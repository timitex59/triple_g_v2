import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import pandas as pd

# Allow direct execution: python tests/test_renko_score_vivier.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from renko_score_29pairs_v16 import (
    append_vivier_event_journal,
    build_telegram_message,
    closed_h1_source,
    collect_vivier_run_events,
    fib_ceiling_label,
    fib_directional_label,
    sar_cross_event,
    update_vivier,
    update_vivier_performance,
    vivier_groups,
)


PARIS = ZoneInfo("Europe/Paris")
NOW = datetime(2026, 6, 27, 12, 0, tzinfo=PARIS)


def row(pair, monthly, weekly, daily, weighted_pct=0.0, fib_pct=None,
        sar_event=None, closed_extreme=None):
    if fib_pct is None:
        fib_pct = 40.0 if monthly == 1 else 60.0
    h1_fib = {"pct_of_range": fib_pct}
    if sar_event is not None:
        h1_fib["sar_cross_event"] = sar_event
    if closed_extreme is not None:
        h1_fib["closed_extreme"] = closed_extreme
    return {
        "pair": pair,
        "px": {"M": monthly, "W": weekly, "D": daily},
        "weighted_pct": weighted_pct,
        "h1_fib": h1_fib,
    }


def event(direction, sar_value, fib50, time_utc):
    return {
        "direction": direction,
        "sar_value": sar_value,
        "fib50": fib50,
        "time_utc": time_utc,
    }


def extreme(time_utc, high, low, fib1_before, fib0_before):
    return {
        "time_utc": time_utc,
        "high": high,
        "low": low,
        "fib1_before": fib1_before,
        "fib0_before": fib0_before,
    }


class VivierStateTests(unittest.TestCase):
    def test_closed_h1_source_excludes_only_open_hour(self):
        index = pd.to_datetime(["2026-06-27T09:00:00Z", "2026-06-27T10:00:00Z"])
        df = pd.DataFrame({"close": [1.0, 2.0]}, index=index)

        open_hour = closed_h1_source(df, pd.Timestamp("2026-06-27T10:30:00Z"))
        closed_hour = closed_h1_source(df, pd.Timestamp("2026-06-27T11:00:00Z"))

        self.assertEqual(len(open_hour), 1)
        self.assertEqual(len(closed_hour), 2)

    def test_standard_sar_cross_uses_closed_prices(self):
        index = pd.to_datetime(["2026-06-27T09:00:00Z", "2026-06-27T10:00:00Z"])
        bull_df = pd.DataFrame({"close": [10.0, 12.0]}, index=index)
        bear_df = pd.DataFrame({"close": [12.0, 10.0]}, index=index)

        bull = sar_cross_event(bull_df, {"trend": [-1, 1], "sar": [11.0, 9.0]})
        bear = sar_cross_event(bear_df, {"trend": [1, -1], "sar": [11.0, 13.0]})

        self.assertEqual(bull["direction"], 1)
        self.assertEqual(bull["sar_value"], 9.0)
        self.assertEqual(bull["time_utc"], "2026-06-27T11:00:00+00:00")
        self.assertEqual(bear["direction"], -1)
        self.assertEqual(bear["sar_value"], 13.0)

    def test_fibonacci_ceiling_uses_next_level_above_price(self):
        self.assertEqual(fib_ceiling_label({"pct_of_range": 34.0}), "Fibo <0.382")
        self.assertEqual(fib_ceiling_label({"pct_of_range": 15.0}), "Fibo <0.236")
        self.assertEqual(fib_ceiling_label({"pct_of_range": 110.0}), "Fibo >1")

    def test_fibonacci_level_follows_vivier_direction(self):
        fib = {"pct_of_range": 60.0}
        self.assertEqual(fib_directional_label(fib, 1), "Fibo <0.618")
        self.assertEqual(fib_directional_label(fib, -1), "Fibo >0.500")

    def test_telegram_vivier_line_is_compact(self):
        state = {
            "pairs": {
                "GBPJPY": {
                    "direction": 1,
                    "last_px": {"M": 1, "W": 1, "D": 0},
                    "fib_position": "Fibo <0.382",
                }
            }
        }

        message = build_telegram_message([], [], vivier_state=state)

        self.assertIn("🟢 GBPJPY (+83% | <0.382)", message)
        self.assertNotIn("M+ W+ D0", message)

    def test_telegram_adds_flame_only_for_current_record_event(self):
        state = {
            "pairs": {
                "GBPJPY": {
                    "direction": 1,
                    "last_px": {"M": 1, "W": 1, "D": 0},
                    "fib_position": "Fibo <0.382",
                    "sar_flame": True,
                }
            }
        }

        message = build_telegram_message([], [], vivier_state=state)

        self.assertIn("🟢 GBPJPY (+83% | <0.382) 🔥", message)

    def test_direct_vivier_to_renko_fibo_transition_is_shown_once(self):
        renko_row = {
            "pair": "GBPJPY",
            "signal_state": 1,
            "weighted_pct": 80.0,
            "h1_fib": {"position": "ABOVE", "sar_dir": 1, "sar_flipped": False},
            "streak_position": {"counts": {1: 3, -1: 0, 0: 0}},
            "streak_tag": "🟢3",
        }
        signal = {
            "pair": "GBPJPY",
            "direction": 1,
            "weighted_pct": 80.0,
            "fib_position": "Fibo <0.786",
        }

        message = build_telegram_message(
            [renko_row], [], vivier_state={"pairs": {}}, vivier_signals=[signal]
        )

        self.assertIn("🚀 VIVIER → RENKO FIBO", message)
        self.assertIn("🟢 GBPJPY (+80% | <0.786)", message)
        self.assertEqual(message.count("GBPJPY"), 1)
        self.assertNotIn("SIGNAL VIVIER", message)

    def test_entry_requires_strict_opposition(self):
        rows = [
            row("BULL01", 1, 0, -1),
            row("BEAR01", -1, 0, 1),
            row("LOWBULL", 1, -1, 0),
            row("LOWBEAR", -1, 1, 0),
            row("INSIDE", 0, -1, -1),
            row("NOOPPO", 1, 0, 1),
        ]

        state, signals = update_vivier(rows, {}, NOW)

        self.assertEqual(set(state["pairs"]), {"BULL01", "BEAR01"})
        self.assertEqual(state["pairs"]["BULL01"]["direction"], 1)
        self.assertEqual(state["pairs"]["BEAR01"]["direction"], -1)
        self.assertEqual(signals, [])

    def test_tracked_pair_survives_inside_transition(self):
        state, _ = update_vivier([row("EURUSD", 1, 0, -1)], {}, NOW)

        next_state, signals = update_vivier(
            [row("EURUSD", 1, 0, 0)], state, NOW
        )

        self.assertIn("EURUSD", next_state["pairs"])
        self.assertEqual(next_state["pairs"]["EURUSD"]["last_px"]["W"], 0)
        self.assertEqual(signals, [])

    def test_full_price_renko_alignment_emits_once_and_removes_pair(self):
        state, _ = update_vivier([row("NZDUSD", -1, 0, 1)], {}, NOW)

        next_state, signals = update_vivier(
            [row("NZDUSD", -1, -1, -1, -80.0)], state, NOW
        )

        self.assertNotIn("NZDUSD", next_state["pairs"])
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["pair"], "NZDUSD")
        self.assertEqual(signals[0]["direction"], -1)
        self.assertEqual(signals[0]["weighted_pct"], -80.0)

        final_state, repeated = update_vivier(
            [row("NZDUSD", -1, -1, -1, -80.0)], next_state, NOW
        )
        self.assertEqual(repeated, [])
        self.assertNotIn("NZDUSD", final_state["pairs"])

    def test_monthly_inside_invalidates_existing_entry(self):
        state, _ = update_vivier([row("GBPJPY", 1, 0, -1)], {}, NOW)

        next_state, signals = update_vivier([row("GBPJPY", 0, 1, -1)], state, NOW)

        self.assertNotIn("GBPJPY", next_state["pairs"])
        self.assertEqual(signals, [])

    def test_missing_market_row_does_not_erase_tracking(self):
        state, _ = update_vivier([row("AUDJPY", 1, 0, -1)], {}, NOW)

        next_state, signals = update_vivier([], state, NOW)

        self.assertIn("AUDJPY", next_state["pairs"])
        self.assertEqual(signals, [])

    def test_groups_are_ranked_by_absolute_base_score(self):
        initial_rows = [
            row("BULL67", 1, 0, -1), row("BULL83", 1, 0, -1),
            row("BULL33", 1, 0, -1), row("BEAR67", -1, 0, 1),
            row("BEAR83", -1, 0, 1), row("BEAR33", -1, 0, 1),
        ]
        state, _ = update_vivier(initial_rows, {}, NOW)
        current_rows = [
            row("BULL67", 1, 1, -1), row("BULL83", 1, 1, 0),
            row("BULL33", 1, 0, -1), row("BEAR67", -1, -1, 1),
            row("BEAR83", -1, -1, 0), row("BEAR33", -1, 0, 1),
        ]
        state, _ = update_vivier(current_rows, state, NOW)

        bull, bear = vivier_groups(state)

        self.assertEqual([pair for pair, _ in bull], ["BULL83", "BULL67", "BULL33"])
        self.assertEqual([pair for pair, _ in bear], ["BEAR83", "BEAR67", "BEAR33"])

    def test_tracked_pair_is_removed_below_minimum_score(self):
        state, _ = update_vivier([row("EURGBP", 1, 0, -1)], {}, NOW)

        next_state, signals = update_vivier([row("EURGBP", 1, -1, -1)], state, NOW)

        self.assertNotIn("EURGBP", next_state["pairs"])
        self.assertEqual(signals, [])

    def test_entry_requires_directional_fibonacci_half(self):
        rows = [
            row("BULL_OK", 1, 0, -1, fib_pct=50.0),
            row("BULL_BAD", 1, 0, -1, fib_pct=50.1),
            row("BEAR_OK", -1, 0, 1, fib_pct=50.0),
            row("BEAR_BAD", -1, 0, 1, fib_pct=49.9),
        ]

        state, _ = update_vivier(rows, {}, NOW)

        self.assertEqual(set(state["pairs"]), {"BULL_OK", "BEAR_OK"})

    def test_tracked_pair_is_not_removed_after_crossing_fibonacci_midpoint(self):
        state, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, fib_pct=40.0)], {}, NOW
        )

        next_state, signals = update_vivier(
            [row("GBPJPY", 1, 1, 0, fib_pct=70.0)], state, NOW
        )

        self.assertIn("GBPJPY", next_state["pairs"])
        self.assertEqual(signals, [])

    def test_bull_sar_flame_requires_new_record_low_below_fib50(self):
        first = event(1, 0.4590, 0.4620, "2026-06-27T10:00:00+00:00")
        state, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, sar_event=first)], {}, NOW
        )
        entry = state["pairs"]["GBPJPY"]
        self.assertTrue(entry["sar_flame"])
        self.assertEqual(entry["sar_record_value"], 0.4590)

        same_state, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, sar_event=first)], state, NOW
        )
        self.assertFalse(same_state["pairs"]["GBPJPY"]["sar_flame"])

        higher = event(1, 0.4600, 0.4620, "2026-06-27T11:00:00+00:00")
        higher_state, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, sar_event=higher)], same_state, NOW
        )
        self.assertFalse(higher_state["pairs"]["GBPJPY"]["sar_flame"])

        lower = event(1, 0.4580, 0.4620, "2026-06-27T12:00:00+00:00")
        lower_state, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, sar_event=lower)], higher_state, NOW
        )
        self.assertTrue(lower_state["pairs"]["GBPJPY"]["sar_flame"])
        self.assertEqual(lower_state["pairs"]["GBPJPY"]["sar_record_value"], 0.4580)

    def test_bear_sar_flame_requires_new_record_high_above_fib50(self):
        first = event(-1, 0.4660, 0.4620, "2026-06-27T10:00:00+00:00")
        state, _ = update_vivier(
            [row("NZDCHF", -1, 0, 1, sar_event=first)], {}, NOW
        )
        self.assertTrue(state["pairs"]["NZDCHF"]["sar_flame"])

        lower = event(-1, 0.4650, 0.4620, "2026-06-27T11:00:00+00:00")
        lower_state, _ = update_vivier(
            [row("NZDCHF", -1, 0, 1, sar_event=lower)], state, NOW
        )
        self.assertFalse(lower_state["pairs"]["NZDCHF"]["sar_flame"])

        higher = event(-1, 0.4670, 0.4620, "2026-06-27T12:00:00+00:00")
        higher_state, _ = update_vivier(
            [row("NZDCHF", -1, 0, 1, sar_event=higher)], lower_state, NOW
        )
        self.assertTrue(higher_state["pairs"]["NZDCHF"]["sar_flame"])
        self.assertEqual(higher_state["pairs"]["NZDCHF"]["sar_record_value"], 0.4670)

    def test_sar_event_on_wrong_fibonacci_half_does_not_start_record(self):
        bull_above = event(1, 0.4630, 0.4620, "2026-06-27T10:00:00+00:00")
        bear_below = event(-1, 0.4610, 0.4620, "2026-06-27T10:00:00+00:00")
        state, _ = update_vivier([
            row("BULL", 1, 0, -1, sar_event=bull_above),
            row("BEAR", -1, 0, 1, sar_event=bear_below),
        ], {}, NOW)

        self.assertFalse(state["pairs"]["BULL"]["sar_flame"])
        self.assertNotIn("sar_record_value", state["pairs"]["BULL"])
        self.assertFalse(state["pairs"]["BEAR"]["sar_flame"])
        self.assertNotIn("sar_record_value", state["pairs"]["BEAR"])

    def test_bull_fib1_touch_resets_record_and_rearms_first_flame(self):
        first = event(1, 0.4590, 0.4620, "2026-06-27T10:00:00+00:00")
        state, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, sar_event=first)], {}, NOW
        )
        touch = extreme(
            "2026-06-27T11:00:00+00:00", high=1.01, low=0.95,
            fib1_before=1.00, fib0_before=0.90,
        )
        reset_state, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, closed_extreme=touch)], state, NOW
        )
        entry = reset_state["pairs"]["GBPJPY"]
        self.assertNotIn("sar_record_value", entry)
        self.assertEqual(entry["sar_last_fib_reset_reason"], "FIB1_TOUCH")

        # Higher than the old minimum, but first record of the rearmed cycle.
        next_event = event(1, 0.4600, 0.4620, "2026-06-27T12:00:00+00:00")
        rearmed, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, sar_event=next_event)], reset_state, NOW
        )
        self.assertTrue(rearmed["pairs"]["GBPJPY"]["sar_flame"])
        self.assertEqual(rearmed["pairs"]["GBPJPY"]["sar_record_value"], 0.4600)

    def test_bear_fib0_touch_resets_record_and_rearms_first_flame(self):
        first = event(-1, 0.4660, 0.4620, "2026-06-27T10:00:00+00:00")
        state, _ = update_vivier(
            [row("NZDCHF", -1, 0, 1, sar_event=first)], {}, NOW
        )
        touch = extreme(
            "2026-06-27T11:00:00+00:00", high=0.47, low=0.44,
            fib1_before=0.48, fib0_before=0.45,
        )
        reset_state, _ = update_vivier(
            [row("NZDCHF", -1, 0, 1, sar_event=None, closed_extreme=touch)], state, NOW
        )
        entry = reset_state["pairs"]["NZDCHF"]
        self.assertNotIn("sar_record_value", entry)
        self.assertEqual(entry["sar_last_fib_reset_reason"], "FIB0_TOUCH")

        # Lower than the old maximum, but first record of the rearmed cycle.
        next_event = event(-1, 0.4650, 0.4620, "2026-06-27T12:00:00+00:00")
        rearmed, _ = update_vivier(
            [row("NZDCHF", -1, 0, 1, sar_event=next_event)], reset_state, NOW
        )
        self.assertTrue(rearmed["pairs"]["NZDCHF"]["sar_flame"])
        self.assertEqual(rearmed["pairs"]["NZDCHF"]["sar_record_value"], 0.4650)

    def test_fib_extreme_touch_without_record_does_not_create_reset(self):
        touch = extreme(
            "2026-06-27T11:00:00+00:00", high=1.01, low=0.95,
            fib1_before=1.00, fib0_before=0.90,
        )
        state, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, closed_extreme=touch)], {}, NOW
        )

        self.assertNotIn("sar_last_fib_reset_time_utc", state["pairs"]["GBPJPY"])

    def test_event_journal_deduplicates_event_ids(self):
        tracked_event = {
            "event_id": "event-1",
            "event_type": "VIVIER_ENTRY",
            "pair": "GBPJPY",
            "direction": 1,
            "time_utc": "2026-06-27T10:00:00+00:00",
            "price": 100.0,
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "events.jsonl"

            first = append_vivier_event_journal([tracked_event], str(path))
            second = append_vivier_event_journal([tracked_event], str(path))
            lines = path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(first, 1)
        self.assertEqual(second, 0)
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["event_id"], "event-1")

    def test_direct_vivier_transition_creates_single_tracker_event(self):
        market_row = {
            "pair": "GBPJPY",
            "signal_state": 1,
            "base_pct": 100.0,
            "weighted_pct": 80.0,
            "px": {"M": 1, "W": 1, "D": 1},
            "h1_fib": {
                "pct_of_range": 70.0,
                "sar_dir": 1,
                "h1_closed_time_utc": "2026-06-27T11:00:00+00:00",
                "h1_closed_price": 213.5,
                "_closed_h1_bars": [{
                    "time_utc": "2026-06-27T11:00:00+00:00",
                    "high": 213.7,
                    "low": 213.3,
                    "close": 213.5,
                }],
            },
            "streak_position": {"counts": {1: 3, -1: 0, 0: 0}},
        }
        previous = {"pairs": {"GBPJPY": {"direction": 1}}}
        signal = {"pair": "GBPJPY", "direction": 1, "weighted_pct": 80.0}

        events = collect_vivier_run_events(
            previous, {"pairs": {}}, [signal], [market_row], [market_row]
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_type"], "VIVIER_TO_RENKO_FIBO")
        self.assertEqual(events[0]["price"], 213.5)

    def test_performance_tracker_fills_horizons_and_excursions(self):
        tracked_event = {
            "event_id": "bull-entry-1",
            "event_type": "VIVIER_ENTRY",
            "pair": "GBPJPY",
            "direction": 1,
            "time_utc": "2026-06-27T10:00:00+00:00",
            "price": 100.0,
        }
        # The first future candle is separated by a weekend gap. Horizons
        # must count traded H1 candles, not elapsed calendar hours.
        first_bar = pd.Timestamp("2026-06-28T22:00:00Z")
        closes = {1: 102.0, 4: 104.0, 12: 101.0, 24: 107.0, 72: 110.0}
        bars = []
        for ordinal in range(1, 73):
            close = closes.get(ordinal, 100.0)
            bars.append({
                "time_utc": (first_bar + pd.Timedelta(hours=ordinal - 1)).isoformat(),
                "high": 112.0 if ordinal == 72 else max(100.0, close),
                "low": 95.0 if ordinal == 4 else min(100.0, close),
                "close": close,
            })
        market_rows = [{"pair": "GBPJPY", "h1_fib": {"_closed_h1_bars": bars}}]

        result = update_vivier_performance({}, [tracked_event], market_rows, NOW)
        performance = result["events"][0]

        self.assertEqual(performance["horizons"]["1h"]["directional_pct"], 2.0)
        self.assertEqual(performance["horizons"]["72h"]["directional_pct"], 10.0)
        self.assertEqual(
            performance["horizons"]["1h"]["time_utc"],
            "2026-06-28T22:00:00+00:00",
        )
        self.assertEqual(
            performance["horizons"]["4h"]["time_utc"],
            "2026-06-29T01:00:00+00:00",
        )
        self.assertEqual(performance["mfe_72h_pct"], 12.0)
        self.assertEqual(performance["mae_72h_pct"], -5.0)
        self.assertEqual(performance["horizon_basis"], "closed_h1_bars")
        self.assertTrue(performance["complete"])
        self.assertEqual(result["version"], 2)
        self.assertEqual(result["summary"]["complete"], 1)
        horizon = result["summary"]["overall"]["horizons"]["72h"]
        self.assertEqual(horizon["samples"], 1)
        self.assertEqual(horizon["win_rate_pct"], 100.0)
        self.assertEqual(horizon["avg_directional_pct"], 10.0)

    def test_performance_tracker_replaces_calendar_time_horizons(self):
        old_event = {
            "event_id": "legacy-entry-1",
            "event_type": "VIVIER_ENTRY",
            "pair": "AUDCAD",
            "direction": 1,
            "time_utc": "2026-06-26T21:00:00+00:00",
            "price": 100.0,
            "horizons": {
                "1h": {"directional_pct": 99.0},
                "4h": {"directional_pct": 99.0},
            },
        }
        bars = [{
            "time_utc": "2026-06-28T22:00:00+00:00",
            "high": 103.0,
            "low": 99.0,
            "close": 102.0,
        }]

        result = update_vivier_performance(
            {"version": 1, "events": [old_event]},
            [],
            [{"pair": "AUDCAD", "h1_fib": {"_closed_h1_bars": bars}}],
            NOW,
        )
        migrated = result["events"][0]

        self.assertEqual(migrated["horizons"]["1h"]["directional_pct"], 2.0)
        self.assertNotIn("4h", migrated["horizons"])
        self.assertEqual(migrated["horizon_basis"], "closed_h1_bars")


if __name__ == "__main__":
    unittest.main()
