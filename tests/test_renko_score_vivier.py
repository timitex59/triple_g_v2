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
    daily_chg_sar_icon,
    fib_ceiling_label,
    fibo_currency_coefficient,
    fibo_currency_sar_adjusted_coefficient,
    fibo_currency_strength,
    fibo_theoretical_pairs,
    fib_directional_label,
    monthly_fib_transition_context,
    sar_cross_event,
    telegram_body_hash,
    update_vivier,
    update_vivier_pip_tracker,
    update_vivier_performance,
    vivier_pip_intraday_lines,
    vivier_pip_period_lines,
    vivier_pip_size,
    vivier_groups,
)


PARIS = ZoneInfo("Europe/Paris")
NOW = datetime(2026, 6, 27, 12, 0, tzinfo=PARIS)


def row(pair, monthly, weekly, daily, weighted_pct=0.0, fib_pct=None,
        sar_event=None, closed_extreme=None, month_high=1.0, month_low=0.45,
        month_utc="2026-06", closed_time="2026-06-27T10:00:00+00:00"):
    if fib_pct is None:
        fib_pct = 40.0 if monthly == 1 else 60.0
    h1_fib = {
        "pct_of_range": fib_pct,
        "month_high": month_high,
        "month_low": month_low,
        "month_utc": month_utc,
        "h1_closed_month_utc": month_utc,
        "h1_closed_time_utc": closed_time,
    }
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


def extreme(time_utc, high, low, fib1_before, fib0_before, month_utc="2026-06"):
    return {
        "time_utc": time_utc,
        "month_utc": month_utc,
        "high": high,
        "low": low,
        "fib1_before": fib1_before,
        "fib0_before": fib0_before,
    }


def pip_row(pair, price, paris_time):
    ts = pd.Timestamp(paris_time)
    if ts.tzinfo is None:
        ts = ts.tz_localize(PARIS)
    return {
        "pair": pair,
        "h1_fib": {
            "h1_closed_price": price,
            "h1_closed_time_utc": ts.tz_convert("UTC").isoformat(),
            "_closed_h1_bars": [{
                "time_utc": ts.tz_convert("UTC").isoformat(),
                "high": price,
                "low": price,
                "close": price,
            }],
        },
    }


class VivierStateTests(unittest.TestCase):
    def test_pine_uses_confirmed_non_repainting_htf_requests(self):
        pine = (PROJECT_ROOT / "renko_forex_V17_vivier.pine").read_text(
            encoding="utf-8"
        )

        self.assertIn('indicator("renko_forex_V17_VIVIER_NR"', pine)
        for timeframe in ("M", "W", "D"):
            request_start = f'request.security(renkoTicker, "{timeframe}", [open[1], close[1]'
            self.assertIn(request_start, pine)
        self.assertGreaterEqual(pine.count("barmerge.lookahead_on"), 5)
        self.assertNotIn('[open, close, f_green_streak(maxStreakBars)', pine)
        self.assertIn(
            'request.security(baseTicker, "D", close[1], '
            'barmerge.gaps_off, barmerge.lookahead_on)',
            pine,
        )

    def test_pine_fib_midpoint_is_always_emphasized(self):
        pine = (PROJECT_ROOT / "renko_forex_V17_vivier.pine").read_text(
            encoding="utf-8"
        )

        self.assertIn(
            'fibLine500 = line.new(na, na, na, na, xloc=xloc.bar_time, '
            'color=fibMidColor, style=line.style_solid, width=3)',
            pine,
        )
        self.assertIn(
            'fibLabel500 = label.new(na, na, "", xloc=xloc.bar_time, '
            'style=label.style_label_left, color=color.new(color.white, 100), '
            'size=size.normal)',
            pine,
        )
        self.assertIn(
            'f_update_fib(fibLine500, fibLabel500, true,',
            pine,
        )
        self.assertIn('label.set_textcolor(fibLabel500, fibMidColor)', pine)

    def test_pine_marks_confirmed_sar_fib_midpoint_crosses(self):
        pine = (PROJECT_ROOT / "renko_forex_V17_vivier.pine").read_text(
            encoding="utf-8"
        )

        self.assertIn(
            'showSarFibCrosses = input.bool(true, '
            '"Show SAR / Fibo 0.500 Crosses"',
            pine,
        )
        self.assertIn(
            'sarFibCrossUp = sameFibContext and '
            'ta.crossover(sarValue, fib500) and sarValue > sarValue[1]',
            pine,
        )
        self.assertIn(
            'sarFibCrossDown = sameFibContext and '
            'ta.crossunder(sarValue, fib500) and sarValue < sarValue[1]',
            pine,
        )
        self.assertIn(
            'barstate.isconfirmed and vivierDirection == -1 and '
            'sarFibCrossUp ? fib500 : na',
            pine,
        )
        self.assertIn(
            'barstate.isconfirmed and vivierDirection == 1 and '
            'sarFibCrossDown ? fib500 : na',
            pine,
        )

    def test_monthly_fib_keeps_previous_range_during_early_disagreement(self):
        index = pd.to_datetime([
            "2026-06-03 10:00Z",
            "2026-06-20 10:00Z",
            "2026-07-01 10:00Z",
            "2026-07-02 10:00Z",
        ])
        frame = pd.DataFrame({
            "open": [90, 110, 103, 103],
            "high": [100, 120, 110, 108],
            "low": [80, 100, 100, 102],
            "close": [90, 110, 103, 104],
        }, index=index)

        context = monthly_fib_transition_context(frame)

        self.assertEqual(context["effective_fib_source"], "PREVIOUS")
        self.assertTrue(context["transition_active"])
        self.assertAlmostEqual(context["current_pct_of_range"], 40.0)
        self.assertAlmostEqual(context["previous_pct_of_range"], 60.0)
        self.assertAlmostEqual(context["pct_of_range"], 60.0)
        self.assertEqual(context["month_utc"], "2026-06")

    def test_monthly_fib_relays_after_maturity_and_agreement(self):
        index = pd.to_datetime([
            "2026-06-03 10:00Z",
            "2026-06-20 10:00Z",
            "2026-07-01 10:00Z",
            "2026-07-02 10:00Z",
            "2026-07-03 10:00Z",
            "2026-07-06 10:00Z",
            "2026-07-07 10:00Z",
        ])
        frame = pd.DataFrame({
            "open": [90, 110, 90, 91, 92, 91, 90],
            "high": [100, 120, 100, 105, 110, 100, 95],
            "low": [80, 100, 80, 85, 90, 88, 86],
            "close": [90, 110, 90, 92, 95, 92, 90],
        }, index=index)

        context = monthly_fib_transition_context(frame)

        self.assertEqual(context["transition_trading_days"], 5)
        self.assertGreaterEqual(context["transition_range_ratio"], 0.60)
        self.assertTrue(context["transition_same_side"])
        self.assertEqual(context["effective_fib_source"], "CURRENT")
        self.assertEqual(context["transition_relay_reason"], "MATURE_AGREEMENT")

    def test_monthly_fib_forces_current_range_from_day_11(self):
        index = pd.to_datetime([
            "2026-06-03 10:00Z",
            "2026-06-20 10:00Z",
            "2026-07-01 10:00Z",
            "2026-07-13 10:00Z",
        ])
        frame = pd.DataFrame({
            "open": [90, 110, 103, 103],
            "high": [100, 120, 110, 108],
            "low": [80, 100, 100, 102],
            "close": [90, 110, 103, 104],
        }, index=index)

        context = monthly_fib_transition_context(frame)

        self.assertEqual(context["effective_fib_source"], "CURRENT")
        self.assertEqual(context["transition_relay_reason"], "FORCED_DAY_11")

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

    def test_fibo_currency_coefficient_weights_distance_from_midpoint(self):
        self.assertEqual(fibo_currency_coefficient({"pct_of_range": -5.0}), -4)
        self.assertEqual(fibo_currency_coefficient({"pct_of_range": 20.0}), -3)
        self.assertEqual(fibo_currency_coefficient({"pct_of_range": 35.0}), -2)
        self.assertEqual(fibo_currency_coefficient({"pct_of_range": 45.0}), -1)
        self.assertEqual(fibo_currency_coefficient({"pct_of_range": 50.0}), 0)
        self.assertEqual(fibo_currency_coefficient({"pct_of_range": 55.0}), 1)
        self.assertEqual(fibo_currency_coefficient({"pct_of_range": 70.0}), 2)
        self.assertEqual(fibo_currency_coefficient({"pct_of_range": 90.0}), 3)
        self.assertEqual(fibo_currency_coefficient({"pct_of_range": 110.0}), 4)

    def test_fibo_sar_strict_coefficient_halves_only_contradictions(self):
        self.assertEqual(
            fibo_currency_sar_adjusted_coefficient({"pct_of_range": 20.0, "sar_dir": 1}),
            -1.5,
        )
        self.assertEqual(
            fibo_currency_sar_adjusted_coefficient({"pct_of_range": 20.0, "sar_dir": -1}),
            -3.0,
        )
        self.assertEqual(
            fibo_currency_sar_adjusted_coefficient({"pct_of_range": 70.0, "sar_dir": -1}),
            1.0,
        )
        self.assertEqual(
            fibo_currency_sar_adjusted_coefficient({"pct_of_range": 70.0, "sar_dir": 1}),
            2.0,
        )

    def test_fibo_currency_strength_uses_base_and_quote_inverse(self):
        rows = [
            {"pair": "GBPCAD", "h1_fib": {"pct_of_range": 90.0}},
            {"pair": "EURGBP", "h1_fib": {"pct_of_range": 20.0}},
            {"pair": "AUDCAD", "h1_fib": {"pct_of_range": 55.0}},
        ]

        ranked = fibo_currency_strength(rows, min_pairs=2)
        scores = {item["currency"]: item["score"] for item in ranked}

        self.assertEqual([item["currency"] for item in ranked], ["GBP", "CAD"])
        self.assertEqual(scores["GBP"], 3.0)
        self.assertEqual(scores["CAD"], -2.0)

    def test_fibo_currency_strength_strict_sar_halves_pair_contradictions(self):
        rows = [
            {"pair": "GBPCAD", "h1_fib": {"pct_of_range": 90.0, "sar_dir": -1}},
            {"pair": "EURGBP", "h1_fib": {"pct_of_range": 20.0, "sar_dir": 1}},
            {"pair": "AUDCAD", "h1_fib": {"pct_of_range": 55.0, "sar_dir": 1}},
        ]

        ranked = fibo_currency_strength(rows, min_pairs=2, strict_sar=True)
        scores = {item["currency"]: item["score"] for item in ranked}

        self.assertEqual(scores["GBP"], 1.5)
        self.assertEqual(scores["CAD"], -1.25)

    def test_fibo_theoretical_pairs_cross_strong_against_weak(self):
        rows = [
            {"pair": "GBPNZD", "h1_fib": {"pct_of_range": 90.0, "sar_dir": 1}},
            {"pair": "GBPAUD", "h1_fib": {"pct_of_range": 90.0, "sar_dir": 1}},
            {"pair": "NZDUSD", "h1_fib": {"pct_of_range": 20.0, "sar_dir": -1}},
            {"pair": "AUDUSD", "h1_fib": {"pct_of_range": 20.0, "sar_dir": -1}},
        ]

        ideas = fibo_theoretical_pairs(
            rows,
            available_pairs={"GBPNZD", "GBPAUD", "NZDUSD", "AUDUSD"},
        )
        by_pair = {idea["pair"]: idea for idea in ideas}

        self.assertEqual(set(by_pair), {"GBPNZD", "GBPAUD", "NZDUSD", "AUDUSD"})
        self.assertEqual(by_pair["GBPNZD"]["direction"], 1)
        self.assertEqual(by_pair["GBPAUD"]["direction"], 1)
        self.assertEqual(by_pair["NZDUSD"]["direction"], -1)
        self.assertEqual(by_pair["AUDUSD"]["direction"], -1)
        self.assertEqual(by_pair["AUDUSD"]["strong"], "USD")
        self.assertEqual(by_pair["AUDUSD"]["weak"], "AUD")

    def test_fibo_theoretical_pairs_reject_vivier_role_conflicts(self):
        rows = [
            {"pair": "GBPNZD", "h1_fib": {"pct_of_range": 90.0, "sar_dir": 1}},
            {"pair": "GBPAUD", "h1_fib": {"pct_of_range": 90.0, "sar_dir": 1}},
            {"pair": "NZDUSD", "h1_fib": {"pct_of_range": 20.0, "sar_dir": -1}},
            {"pair": "AUDUSD", "h1_fib": {"pct_of_range": 20.0, "sar_dir": -1}},
        ]
        vivier_entries = {
            "AUDJPY": {"direction": 1},
            "AUDCAD": {"direction": 1},
        }

        ideas = fibo_theoretical_pairs(
            rows,
            available_pairs={"GBPNZD", "GBPAUD", "NZDUSD", "AUDUSD"},
            vivier_entries=vivier_entries,
        )

        self.assertEqual(ideas, [])

    def test_fibo_theoretical_pairs_require_both_vivier_roles(self):
        rows = [
            {"pair": "GBPCAD", "h1_fib": {"pct_of_range": 90.0, "sar_dir": 1}},
            {"pair": "GBPNZD", "h1_fib": {"pct_of_range": 90.0, "sar_dir": 1}},
            {"pair": "USDCAD", "h1_fib": {"pct_of_range": 90.0, "sar_dir": 1}},
            {"pair": "NZDUSD", "h1_fib": {"pct_of_range": 20.0, "sar_dir": -1}},
        ]
        vivier_entries = {
            "USDJPY": {"direction": 1},  # USD fort, JPY faible
            "AUDCAD": {"direction": 1},  # AUD fort, CAD faible
        }

        ideas = fibo_theoretical_pairs(
            rows,
            available_pairs={"GBPCAD", "GBPNZD", "USDCAD", "NZDUSD"},
            vivier_entries=vivier_entries,
        )

        self.assertEqual([idea["pair"] for idea in ideas], ["USDCAD"])
        self.assertEqual(ideas[0]["direction"], 1)
        self.assertEqual(ideas[0]["strong"], "USD")
        self.assertEqual(ideas[0]["weak"], "CAD")

    def test_telegram_vivier_line_is_compact(self):
        state = {
            "pairs": {
                "GBPJPY": {
                    "direction": 1,
                    "last_px": {"M": 1, "W": 1, "D": 0},
                    "fib_position": "Fibo <0.382",
                    "fib_source": "PREVIOUS",
                    "sar_dir": 1,
                    "daily_chg": -0.12,
                    "daily_sar_dir": -1,
                }
            }
        }

        message = build_telegram_message([], [], vivier_state=state)

        self.assertIn("🟢🔴 GBPJPY (+83% | <0.382)", message)
        self.assertNotIn("M-1", message)
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

        self.assertIn("🟢⚪ GBPJPY (+83% | <0.382) 🔥", message)

    def test_telegram_shows_current_fibo_and_flame_kind(self):
        state = {
            "pairs": {
                "EURJPY": {
                    "direction": 1,
                    "last_px": {"M": 1, "W": 1, "D": 0},
                    "entry_fib_position": "Fibo <0.236",
                    "fib_position": "Fibo <0.500",
                    "sar_flame": True,
                    "sar_flame_kind": "RECORD",
                }
            }
        }

        message = build_telegram_message([], [], vivier_state=state)

        self.assertIn("EURJPY (+83% | <0.500)", message)
        self.assertNotIn("<0.236→", message)
        self.assertIn("🔥R", message)

    def test_telegram_hides_near_alignment_section(self):
        state = {
            "pairs": {
                "GBPJPY": {
                    "direction": 1,
                    "last_px": {"M": 1, "W": 1, "D": 0},
                    "fib_position": "Fibo <0.500",
                }
            }
        }

        message = build_telegram_message([], [], vivier_state=state)

        self.assertIn("VIVIER BULL", message)
        self.assertIn("GBPJPY (+83% | <0.500)", message)
        self.assertNotIn("PROCHE ALIGNEMENT", message)
        self.assertNotIn("D restant", message)

    def test_telegram_shows_post_signal_tracking(self):
        state = {
            "pairs": {},
            "pending_objectives": {
                "GBPJPY": {
                    "direction": 1,
                    "value": 215.614,
                    "signal_price": 213.0,
                }
            },
        }
        current = {
            "pair": "GBPJPY",
            "h1_price": 214.065,
            "h1_fib": {"sar_dir": -1},
            "daily_chg": 0.21,
            "daily_sar_dir": 1,
        }

        message = build_telegram_message([], [current], vivier_state=state)

        self.assertIn("SUIVI SIGNAL", message)
        self.assertIn("🟢🟢 GBPJPY (+0.50%)", message)
        self.assertNotIn("depuis signal", message)
        self.assertNotIn("F1 215.614", message)

    def test_telegram_daily_chg_icon_requires_daily_sar_confirmation(self):
        state = {
            "pairs": {
                "GBPJPY": {
                    "direction": 1,
                    "last_px": {"M": 1, "W": 1, "D": 0},
                    "fib_position": "Fibo <0.382",
                    "daily_chg": 0.12,
                    "daily_sar_dir": -1,
                },
                "CADCHF": {
                    "direction": -1,
                    "last_px": {"M": -1, "W": -1, "D": 0},
                    "fib_position": "Fibo >0.618",
                    "daily_chg": -0.15,
                    "daily_sar_dir": -1,
                },
            }
        }

        message = build_telegram_message([], [], vivier_state=state)

        self.assertIn("🟢⚪ GBPJPY (+83% | <0.382)", message)
        self.assertIn("🔴🔴 CADCHF (-83% | >0.618)", message)

    def test_daily_chg_sar_icon_requires_strict_chg_buffer(self):
        self.assertEqual(daily_chg_sar_icon(0.0500, 1), "⚪")
        self.assertEqual(daily_chg_sar_icon(0.0501, 1), "🟢")
        self.assertEqual(daily_chg_sar_icon(-0.0500, -1), "⚪")
        self.assertEqual(daily_chg_sar_icon(-0.0501, -1), "🔴")
        self.assertEqual(daily_chg_sar_icon(0.0600, -1), "⚪")
        self.assertEqual(daily_chg_sar_icon(-0.0600, 1), "⚪")

    def test_telegram_shows_compact_theoretical_pairs_without_strength_blocks(self):
        state = {
            "pairs": {
                "USDJPY": {
                    "direction": 1,
                    "last_px": {"M": 1, "W": 1, "D": 0},
                    "fib_position": "Fibo <0.500",
                },
                "AUDCAD": {
                    "direction": 1,
                    "last_px": {"M": 1, "W": 1, "D": 0},
                    "fib_position": "Fibo <0.500",
                },
            }
        }
        all_rows = [
            {"pair": "GBPCAD", "h1_fib": {"pct_of_range": 90.0, "sar_dir": 1}},
            {"pair": "GBPNZD", "h1_fib": {"pct_of_range": 90.0, "sar_dir": 1}},
            {"pair": "USDCAD", "h1_fib": {"pct_of_range": 90.0, "sar_dir": 1}},
            {"pair": "NZDUSD", "h1_fib": {"pct_of_range": 20.0, "sar_dir": -1}},
        ]

        message = build_telegram_message([], all_rows, vivier_state=state)

        self.assertNotIn("FORCE FIBO 0.5", message)
        self.assertNotIn("FORCE FIBO+SAR", message)
        self.assertIn("PAIRES FORT/FAIBLE", message)
        self.assertIn("🟢 USDCAD 🌱USD/CAD", message)
        self.assertNotIn("GBPCAD", message)
        self.assertNotIn("NZDUSD", message)
        self.assertNotIn("PROCHE ALIGNEMENT", message)
        self.assertLess(message.index("VIVIER BULL"), message.index("PAIRES FORT/FAIBLE"))

    def test_telegram_body_hash_ignores_timestamp_only(self):
        first = "📊 VIVIER\n\n🟢 GBPJPY\n\n⏰ 2026-06-29 08:16 Paris"
        second = "📊 VIVIER\n\n🟢 GBPJPY\n\n⏰ 2026-06-29 09:16 Paris"
        changed = "📊 VIVIER\n\n🟢 EURJPY\n\n⏰ 2026-06-29 09:16 Paris"

        self.assertEqual(telegram_body_hash(first), telegram_body_hash(second))
        self.assertNotEqual(telegram_body_hash(first), telegram_body_hash(changed))

    def test_direct_vivier_transition_is_kept_as_vivier_signal(self):
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

        self.assertIn("📊 VIVIER", message)
        self.assertIn("🚨 SIGNAL VIVIER", message)
        self.assertIn("🟢 GBPJPY · M/W/D alignés · +80%", message)
        self.assertEqual(message.count("GBPJPY"), 1)
        self.assertNotIn("RENKO FIBO", message)

    def test_standalone_renko_fibo_signal_is_silent(self):
        renko_row = {
            "pair": "GBPCAD",
            "signal_state": 1,
            "weighted_pct": 90.0,
            "h1_fib": {"position": "ABOVE", "sar_dir": 1},
            "streak_position": {"counts": {1: 3, -1: 0, 0: 0}},
            "streak_tag": "🟢3",
        }

        message = build_telegram_message(
            [renko_row], [renko_row], vivier_state={"pairs": {}}
        )

        self.assertIsNone(message)

    def test_vivier_pips_track_bull_bear_and_freeze_exits(self):
        self.assertEqual(vivier_pip_size("EURUSD"), 0.0001)
        self.assertEqual(vivier_pip_size("USDJPY"), 0.01)
        self.assertEqual(vivier_pip_size("XAUUSD"), 0.01)
        active = {"pairs": {
            "EURUSD": {"direction": 1},
            "USDJPY": {"direction": -1},
        }}
        start = datetime(2026, 7, 6, 7, 17, tzinfo=PARIS)
        state, report = update_vivier_pip_tracker(
            {}, active,
            [
                pip_row("EURUSD", 1.1000, "2026-07-06 07:00+02:00"),
                pip_row("USDJPY", 160.00, "2026-07-06 07:00+02:00"),
            ],
            now=start,
        )
        self.assertAlmostEqual(report["intraday"]["total_pips"], 0.0)

        state, report = update_vivier_pip_tracker(
            state, active,
            [
                pip_row("EURUSD", 1.1015, "2026-07-06 10:00+02:00"),
                pip_row("USDJPY", 159.80, "2026-07-06 10:00+02:00"),
            ],
            now=datetime(2026, 7, 6, 10, 17, tzinfo=PARIS),
        )
        self.assertAlmostEqual(report["intraday"]["bull_pips"], 15.0)
        self.assertAlmostEqual(report["intraday"]["bear_pips"], 20.0)
        self.assertAlmostEqual(report["intraday"]["total_pips"], 35.0)

        # EURUSD leaves the vivier: its +20 pips are frozen while USDJPY
        # remains marked to market.
        state, report = update_vivier_pip_tracker(
            state, {"pairs": {"USDJPY": {"direction": -1}}},
            [
                pip_row("EURUSD", 1.1020, "2026-07-06 12:00+02:00"),
                pip_row("USDJPY", 159.90, "2026-07-06 12:00+02:00"),
            ],
            now=datetime(2026, 7, 6, 12, 17, tzinfo=PARIS),
        )
        self.assertNotIn("EURUSD", state["open_segments"])
        self.assertAlmostEqual(report["intraday"]["bull_pips"], 20.0)
        self.assertAlmostEqual(report["intraday"]["bear_pips"], 10.0)
        self.assertAlmostEqual(report["intraday"]["total_pips"], 30.0)
        lines = vivier_pip_intraday_lines(report)
        self.assertIn("📈 DAILY : +30.0 pips / +0.0 pips", lines)
        self.assertIn("🟢 +20.0 | 🔴 +10.0", lines)
        self.assertIn("🟢 +0.0 | 🔴 +0.0", lines)
        self.assertIn("📊 CUMULS", lines)
        self.assertIn("Weekly : +30.0 pips / +0.0 pips", lines)
        self.assertIn("Monthly : +30.0 pips / +0.0 pips", lines)

    def test_vivier_pips_track_confirmed_book_separately(self):
        active_confirmed = {"pairs": {
            "EURUSD": {
                "direction": 1,
                "daily_chg": 0.10,
                "daily_sar_dir": 1,
            },
        }}
        active_neutral = {"pairs": {
            "EURUSD": {
                "direction": 1,
                "daily_chg": 0.02,
                "daily_sar_dir": 1,
            },
        }}

        state, _ = update_vivier_pip_tracker(
            {}, active_confirmed,
            [pip_row("EURUSD", 1.1000, "2026-07-06 07:00+02:00")],
            now=datetime(2026, 7, 6, 7, 17, tzinfo=PARIS),
        )
        state, report = update_vivier_pip_tracker(
            state, active_confirmed,
            [pip_row("EURUSD", 1.1015, "2026-07-06 10:00+02:00")],
            now=datetime(2026, 7, 6, 10, 17, tzinfo=PARIS),
        )

        self.assertAlmostEqual(report["intraday"]["total_pips"], 15.0)
        self.assertAlmostEqual(report["intraday"]["confirmed_total_pips"], 15.0)

        state, report = update_vivier_pip_tracker(
            state, active_neutral,
            [pip_row("EURUSD", 1.1020, "2026-07-06 12:00+02:00")],
            now=datetime(2026, 7, 6, 12, 17, tzinfo=PARIS),
        )

        self.assertAlmostEqual(report["intraday"]["total_pips"], 20.0)
        self.assertAlmostEqual(report["intraday"]["confirmed_total_pips"], 20.0)
        self.assertNotIn("EURUSD", state["open_confirmed_segments"])
        self.assertEqual(
            state["days"]["2026-07-06"]["confirmed_segments"][0]["close_reason"],
            "CONFIRMED_EXIT",
        )

        state, _ = update_vivier_pip_tracker(
            state, active_confirmed,
            [pip_row("EURUSD", 1.1025, "2026-07-06 13:00+02:00")],
            now=datetime(2026, 7, 6, 13, 17, tzinfo=PARIS),
        )
        state, report = update_vivier_pip_tracker(
            state, active_confirmed,
            [pip_row("EURUSD", 1.1030, "2026-07-06 14:00+02:00")],
            now=datetime(2026, 7, 6, 14, 17, tzinfo=PARIS),
        )

        self.assertAlmostEqual(report["intraday"]["total_pips"], 30.0)
        self.assertAlmostEqual(report["intraday"]["confirmed_total_pips"], 25.0)
        lines = vivier_pip_intraday_lines(report)
        self.assertIn("📈 DAILY : +30.0 pips / +25.0 pips", lines)
        self.assertIn("🟢 +30.0 | 🔴 +0.0", lines)
        self.assertIn("🟢 +25.0 | 🔴 +0.0", lines)

    def test_vivier_pips_first_run_backfills_from_07h(self):
        current = pip_row("EURUSD", 1.1015, "2026-07-06 10:00+02:00")
        current["h1_fib"]["_closed_h1_bars"] = [
            pip_row("EURUSD", 1.1000, "2026-07-06 07:00+02:00")["h1_fib"]["_closed_h1_bars"][0],
            current["h1_fib"]["_closed_h1_bars"][0],
        ]
        active = {"pairs": {"EURUSD": {
            "direction": 1,
            "entered_at_paris": "2026-07-05 18:00",
        }}}

        state, report = update_vivier_pip_tracker(
            {}, active, [current],
            now=datetime(2026, 7, 6, 10, 17, tzinfo=PARIS),
        )

        segment = state["open_segments"]["EURUSD"]
        self.assertEqual(segment["start_time_paris"][:16], "2026-07-06T07:00")
        self.assertAlmostEqual(report["intraday"]["bull_pips"], 15.0)

    def test_vivier_pips_day_close_is_idempotent(self):
        active = {"pairs": {"EURUSD": {"direction": 1}}}
        state, _ = update_vivier_pip_tracker(
            {}, active, [pip_row("EURUSD", 1.1000, "2026-07-06 07:00+02:00")],
            now=datetime(2026, 7, 6, 7, 17, tzinfo=PARIS),
        )
        state, report = update_vivier_pip_tracker(
            state, active, [pip_row("EURUSD", 1.1010, "2026-07-06 23:00+02:00")],
            now=datetime(2026, 7, 6, 23, 2, tzinfo=PARIS),
        )
        self.assertAlmostEqual(report["intraday"]["total_pips"], 10.0)
        self.assertEqual(len(state["days"]["2026-07-06"]["segments"]), 1)
        lines = vivier_pip_intraday_lines(report)
        self.assertIn("📈 DAILY : +10.0 pips / +0.0 pips", lines)
        self.assertIn("🟢 +10.0 | 🔴 +0.0", lines)
        self.assertIn("🟢 +0.0 | 🔴 +0.0", lines)
        self.assertIn("📊 CUMULS", lines)
        self.assertIn("Weekly : +10.0 pips / +0.0 pips", lines)
        self.assertIn("Monthly : +10.0 pips / +0.0 pips", lines)
        self.assertIn("🟢 JOUR GAGNANT", lines)

        state, report = update_vivier_pip_tracker(
            state, active, [pip_row("EURUSD", 1.1020, "2026-07-06 23:00+02:00")],
            now=datetime(2026, 7, 6, 23, 30, tzinfo=PARIS),
        )

        self.assertAlmostEqual(report["intraday"]["total_pips"], 10.0)
        self.assertEqual(len(state["days"]["2026-07-06"]["segments"]), 1)
        self.assertEqual(state["open_segments"], {})

    def test_vivier_pips_build_weekly_and_monthly_reports(self):
        state = {}
        active = {"pairs": {"EURUSD": {"direction": 1}}}
        end_prices = [1.1010, 1.0990, 1.1000, 1.1005, 1.0995]
        for offset, end_price in enumerate(end_prices):
            day = pd.Timestamp("2026-07-06", tz=PARIS) + pd.Timedelta(days=offset)
            start = day + pd.Timedelta(hours=7)
            end = day + pd.Timedelta(hours=23)
            state, _ = update_vivier_pip_tracker(
                state, active, [pip_row("EURUSD", 1.1000, start)],
                now=start.to_pydatetime(),
            )
            state, report = update_vivier_pip_tracker(
                state, active, [pip_row("EURUSD", end_price, end)],
                now=end.to_pydatetime(),
            )

        self.assertIsNotNone(report["weekly"])
        self.assertAlmostEqual(report["weekly"]["total_pips"], 0.0)
        self.assertEqual(report["weekly"]["winning_days"], 2)
        self.assertEqual(report["weekly"]["losing_days"], 2)
        self.assertEqual(report["weekly"]["flat_days"], 1)
        weekly_lines = vivier_pip_period_lines(report)
        self.assertIn("📅 BILAN HEBDOMADAIRE", weekly_lines)
        self.assertIn("JOURS : 🟢 2 gagnants / 🔴 2 perdants / ⚪ 1 neutres", weekly_lines)
        self.assertIn("TOTAL : +0.0 pips", weekly_lines)

        state, monthly_midmonth = update_vivier_pip_tracker(
            state, {"pairs": {}}, [],
            now=datetime(2026, 8, 1, 7, 17, tzinfo=PARIS),
        )
        self.assertIsNone(monthly_midmonth["monthly"])

        july_last_friday = pd.Timestamp("2026-07-31", tz=PARIS)
        state, _ = update_vivier_pip_tracker(
            state, active,
            [pip_row("EURUSD", 1.1000, july_last_friday + pd.Timedelta(hours=7))],
            now=(july_last_friday + pd.Timedelta(hours=7)).to_pydatetime(),
        )
        state, monthly = update_vivier_pip_tracker(
            state, active,
            [pip_row("EURUSD", 1.1007, july_last_friday + pd.Timedelta(hours=23))],
            now=(july_last_friday + pd.Timedelta(hours=23)).to_pydatetime(),
        )
        self.assertIsNotNone(monthly["monthly"])
        self.assertEqual(monthly["monthly"]["label"], "JUILLET")
        self.assertEqual(monthly["monthly"]["days"], 6)
        self.assertEqual(monthly["monthly"]["winning_days"], 3)
        self.assertEqual(monthly["monthly"]["losing_days"], 2)
        self.assertEqual(monthly["monthly"]["flat_days"], 1)
        self.assertAlmostEqual(monthly["monthly"]["total_pips"], 7.0)
        message = build_telegram_message(
            [], [], vivier_state={"pairs": {}}, pip_report=monthly
        )
        self.assertIn("🗓 BILAN MENSUEL — JUILLET", message)
        self.assertIn("JOURS : 🟢 3 gagnants / 🔴 2 perdants / ⚪ 1 neutres", message)
        self.assertIn("Σ TOTAL : +7.0 pips", message)

    def test_transition_revalidates_only_entries_created_in_current_month(self):
        current = row("CURRENT", 1, 1, 0, fib_pct=60.0)
        current["h1_fib"].update({
            "transition_active": True,
            "effective_fib_source": "PREVIOUS",
            "current_month_utc": "2026-07",
        })
        older = row("OLDER", 1, 1, 0, fib_pct=60.0)
        older["h1_fib"].update({
            "transition_active": True,
            "effective_fib_source": "PREVIOUS",
            "current_month_utc": "2026-07",
        })
        validated = row("VALIDATED", -1, 1, -1, fib_pct=49.0)
        validated["h1_fib"].update({
            "transition_active": True,
            "effective_fib_source": "PREVIOUS",
            "current_month_utc": "2026-07",
        })
        previous = {
            "pairs": {
                "CURRENT": {
                    "direction": 1,
                    "entered_at_paris": "2026-07-01 07:16",
                    "last_px": {"M": 1, "W": 1, "D": 0},
                },
                "OLDER": {
                    "direction": 1,
                    "entered_at_paris": "2026-06-27 18:38",
                    "last_px": {"M": 1, "W": 1, "D": 0},
                },
                "VALIDATED": {
                    "direction": -1,
                    "entered_at_paris": "2026-07-02 20:23",
                    "entry_fib_source": "PREVIOUS",
                    "last_px": {"M": -1, "W": 1, "D": -1},
                },
            }
        }

        state, signals = update_vivier(
            [current, older, validated],
            previous,
            now=datetime(2026, 7, 2, 19, 0, tzinfo=PARIS),
        )

        self.assertNotIn("CURRENT", state["pairs"])
        self.assertIn("OLDER", state["pairs"])
        self.assertIn("VALIDATED", state["pairs"])
        self.assertEqual(state["pairs"]["OLDER"]["fib_source"], "PREVIOUS")
        self.assertEqual(state["pairs"]["VALIDATED"]["fib_source"], "PREVIOUS")
        self.assertEqual(signals, [])

    def test_entry_accepts_opposition_or_aligned_weekly_with_daily_inside(self):
        rows = [
            row("BULL01", 1, 0, -1),
            row("BEAR01", -1, 0, 1),
            row("LOWBULL", 1, -1, 0),
            row("LOWBEAR", -1, 1, 0),
            row("READYBULL", 1, 1, 0),
            row("READYBEAR", -1, -1, 0),
            row("INSIDE", 0, -1, -1),
            row("NOOPPO", 1, 0, 1),
        ]

        state, signals = update_vivier(rows, {}, NOW)

        self.assertEqual(
            set(state["pairs"]),
            {"BULL01", "BEAR01", "READYBULL", "READYBEAR"},
        )
        self.assertEqual(state["pairs"]["BULL01"]["direction"], 1)
        self.assertEqual(state["pairs"]["BEAR01"]["direction"], -1)
        self.assertEqual(state["pairs"]["READYBULL"]["direction"], 1)
        self.assertEqual(state["pairs"]["READYBEAR"]["direction"], -1)
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
        self.assertEqual(entry["sar_flame_kind"], "FIRST")
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
        self.assertEqual(lower_state["pairs"]["GBPJPY"]["sar_flame_kind"], "RECORD")
        self.assertEqual(lower_state["pairs"]["GBPJPY"]["sar_record_value"], 0.4580)

    def test_bear_sar_flame_requires_new_record_high_above_fib50(self):
        first = event(-1, 0.4660, 0.4620, "2026-06-27T10:00:00+00:00")
        state, _ = update_vivier(
            [row("NZDCHF", -1, 0, 1, sar_event=first)], {}, NOW
        )
        self.assertTrue(state["pairs"]["NZDCHF"]["sar_flame"])
        self.assertEqual(state["pairs"]["NZDCHF"]["sar_flame_kind"], "FIRST")

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
        self.assertEqual(higher_state["pairs"]["NZDCHF"]["sar_flame_kind"], "RECORD")
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
        same_bar_cross = event(1, 0.4585, 0.75, "2026-06-27T11:00:00+00:00")
        reset_state, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, sar_event=same_bar_cross,
                 closed_extreme=touch)], state, NOW
        )
        entry = reset_state["pairs"]["GBPJPY"]
        self.assertNotIn("sar_record_value", entry)
        self.assertEqual(entry["sar_last_fib_reset_reason"], "FIB1_TOUCH")
        self.assertEqual(
            entry["sar_last_processed_time_utc"],
            "2026-06-27T11:00:00+00:00",
        )

        repeated, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, sar_event=same_bar_cross,
                 closed_extreme=touch)], reset_state, NOW
        )
        self.assertFalse(repeated["pairs"]["GBPJPY"]["sar_flame"])
        self.assertNotIn("sar_record_value", repeated["pairs"]["GBPJPY"])

        # Higher than the old minimum, but first record of the rearmed cycle.
        next_event = event(1, 0.4600, 0.4620, "2026-06-27T12:00:00+00:00")
        rearmed, _ = update_vivier(
            [row("GBPJPY", 1, 0, -1, sar_event=next_event)], repeated, NOW
        )
        self.assertTrue(rearmed["pairs"]["GBPJPY"]["sar_flame"])
        self.assertEqual(rearmed["pairs"]["GBPJPY"]["sar_flame_kind"], "RESET")
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
        self.assertEqual(rearmed["pairs"]["NZDCHF"]["sar_flame_kind"], "RESET")
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

    def test_unreached_objective_is_carried_across_months(self):
        initial_rows = [
            row("BULL", 1, 0, -1, month_high=1.0, month_low=0.5),
            row("BEAR", -1, 0, 1, month_high=1.0, month_low=0.5),
        ]
        state, _ = update_vivier(initial_rows, {}, NOW)

        july_rows = [
            row("BULL", 1, 0, -1, month_high=0.9, month_low=0.7,
                month_utc="2026-07", closed_time="2026-07-01T01:00:00+00:00"),
            row("BEAR", -1, 0, 1, month_high=1.1, month_low=0.6,
                month_utc="2026-07", closed_time="2026-07-01T01:00:00+00:00"),
        ]
        carried, _ = update_vivier(july_rows, state, NOW)

        bull = carried["pairs"]["BULL"]
        bear = carried["pairs"]["BEAR"]
        self.assertEqual(bull["fib_objective_value"], 1.0)
        self.assertEqual(bear["fib_objective_value"], 0.5)
        self.assertTrue(bull["fib_objective_carried"])
        self.assertTrue(bear["fib_objective_carried"])
        self.assertEqual(bull["fib_objective_origin_month_utc"], "2026-06")

        august, _ = update_vivier([
            row("BULL", 1, 0, -1, month_high=0.95, month_low=0.8,
                month_utc="2026-08", closed_time="2026-08-03T01:00:00+00:00"),
            row("BEAR", -1, 0, 1, month_high=1.2, month_low=0.55,
                month_utc="2026-08", closed_time="2026-08-03T01:00:00+00:00"),
        ], carried, NOW)
        self.assertEqual(august["pairs"]["BULL"]["fib_objective_value"], 1.0)
        self.assertEqual(august["pairs"]["BEAR"]["fib_objective_value"], 0.5)
        self.assertEqual(
            august["pairs"]["BULL"]["fib_objective_carried_since_month_utc"],
            "2026-07",
        )

    def test_carried_objective_touch_rearms_and_next_flame_sets_new_target(self):
        first = event(1, 0.4590, 0.75, "2026-06-27T10:00:00+00:00")
        state, _ = update_vivier([
            row("GBPJPY", 1, 0, -1, sar_event=first,
                month_high=1.0, month_low=0.5),
        ], {}, NOW)
        july_row = row(
            "GBPJPY", 1, 0, -1, month_high=0.9, month_low=0.7,
            month_utc="2026-07", closed_time="2026-07-01T01:00:00+00:00",
        )
        carried, _ = update_vivier([july_row], state, NOW)
        self.assertEqual(carried["pairs"]["GBPJPY"]["fib_objective_value"], 1.0)

        touch = extreme(
            "2026-07-02T11:00:00+00:00", high=1.01, low=0.80,
            fib1_before=0.99, fib0_before=0.70, month_utc="2026-07",
        )
        touch_row = row(
            "GBPJPY", 1, 0, -1, closed_extreme=touch,
            month_high=1.01, month_low=0.7, month_utc="2026-07",
            closed_time="2026-07-02T11:00:00+00:00",
        )
        touched, _ = update_vivier([touch_row], carried, NOW)
        entry = touched["pairs"]["GBPJPY"]
        self.assertFalse(entry["fib_objective_active"])
        self.assertNotIn("fib_objective_value", entry)
        self.assertTrue(entry["fib_last_objective_touch_was_carried"])
        self.assertEqual(entry["fib_last_objective_touch_value"], 1.0)
        self.assertNotIn("sar_record_value", entry)
        self.assertEqual(entry["sar_last_fib_reset_reason"], "FIB1_TOUCH")

        next_event = event(1, 0.4600, 0.85, "2026-07-02T12:00:00+00:00")
        rearmed, _ = update_vivier([
            row("GBPJPY", 1, 0, -1, sar_event=next_event,
                month_high=1.02, month_low=0.7, month_utc="2026-07",
                closed_time="2026-07-02T12:00:00+00:00"),
        ], touched, NOW)
        rearmed_entry = rearmed["pairs"]["GBPJPY"]
        self.assertTrue(rearmed_entry["sar_flame"])
        self.assertEqual(rearmed_entry["sar_flame_kind"], "RESET")
        self.assertTrue(rearmed_entry["fib_objective_active"])
        self.assertFalse(rearmed_entry["fib_objective_carried"])
        self.assertEqual(rearmed_entry["fib_objective_value"], 1.02)

        carry_events = collect_vivier_run_events(
            state, carried, [], [], [july_row]
        )
        self.assertIn("FIB_OBJECTIVE_CARRIED", {
            item["event_type"] for item in carry_events
        })
        touch_events = collect_vivier_run_events(
            carried, touched, [], [], [touch_row]
        )
        self.assertEqual(
            {item["event_type"] for item in touch_events},
            {"FIB_OBJECTIVE_TOUCH", "SAR_RECORD_RESET"},
        )

    def test_alignment_moves_unreached_objective_to_post_signal_tracking(self):
        state, _ = update_vivier([
            row("GBPJPY", 1, 0, -1, month_high=1.0, month_low=0.5),
        ], {}, NOW)
        aligned_row = row(
            "GBPJPY", 1, 1, 1, weighted_pct=80.0,
            month_high=1.0, month_low=0.5,
        )

        aligned, signals = update_vivier([aligned_row], state, NOW)

        self.assertNotIn("GBPJPY", aligned["pairs"])
        self.assertEqual(len(signals), 1)
        pending = aligned["pending_objectives"]["GBPJPY"]
        self.assertEqual(pending["direction"], 1)
        self.assertEqual(pending["value"], 1.0)
        self.assertEqual(
            aligned["objective_events"][0]["event_type"],
            "POST_ALIGNMENT_OBJECTIVE_CREATED",
        )

        events = collect_vivier_run_events(
            state, aligned, signals, [], [aligned_row]
        )
        self.assertEqual(
            {item["event_type"] for item in events},
            {"POST_ALIGNMENT_OBJECTIVE_CREATED", "SIGNAL_VIVIER"},
        )

    def test_post_alignment_objective_is_reached_outside_vivier(self):
        state, _ = update_vivier([
            row("GBPJPY", 1, 0, -1, month_high=1.0, month_low=0.5),
        ], {}, NOW)
        aligned, _ = update_vivier([
            row("GBPJPY", 1, 1, 1, month_high=1.0, month_low=0.5),
        ], state, NOW)
        target_touch = extreme(
            "2026-06-27T13:00:00+00:00", high=1.01, low=0.8,
            fib1_before=1.0, fib0_before=0.5,
        )
        outside_row = row(
            "GBPJPY", 1, 0, 0, closed_extreme=target_touch,
            month_high=1.01, month_low=0.5,
            closed_time="2026-06-27T13:00:00+00:00",
        )

        reached, signals = update_vivier([outside_row], aligned, NOW)

        self.assertEqual(signals, [])
        self.assertNotIn("GBPJPY", reached["pairs"])
        self.assertNotIn("GBPJPY", reached["pending_objectives"])
        self.assertEqual(
            reached["objective_events"][0]["event_type"],
            "POST_ALIGNMENT_OBJECTIVE_REACHED",
        )

    def test_same_direction_reentry_reattaches_post_alignment_objective(self):
        state, _ = update_vivier([
            row("GBPJPY", 1, 0, -1, month_high=1.0, month_low=0.5),
        ], {}, NOW)
        aligned, _ = update_vivier([
            row("GBPJPY", 1, 1, 1, month_high=1.0, month_low=0.5),
        ], state, NOW)

        reentered, signals = update_vivier([
            row("GBPJPY", 1, 1, 0, month_high=0.9, month_low=0.6),
        ], aligned, NOW)

        self.assertEqual(signals, [])
        self.assertNotIn("GBPJPY", reentered["pending_objectives"])
        entry = reentered["pairs"]["GBPJPY"]
        self.assertEqual(entry["fib_objective_value"], 1.0)
        self.assertTrue(entry["fib_objective_reattached_from_post_alignment"])
        self.assertEqual(
            reentered["objective_events"][0]["event_type"],
            "POST_ALIGNMENT_OBJECTIVE_REATTACHED",
        )

    def test_opposite_monthly_cancels_post_objective_before_new_entry(self):
        state, _ = update_vivier([
            row("GBPJPY", 1, 0, -1, month_high=1.0, month_low=0.5),
        ], {}, NOW)
        aligned, _ = update_vivier([
            row("GBPJPY", 1, 1, 1, month_high=1.0, month_low=0.5),
        ], state, NOW)

        reversed_state, _ = update_vivier([
            row("GBPJPY", -1, -1, 0, month_high=1.1, month_low=0.6),
        ], aligned, NOW)

        self.assertNotIn("GBPJPY", reversed_state["pending_objectives"])
        self.assertEqual(reversed_state["pairs"]["GBPJPY"]["direction"], -1)
        self.assertEqual(
            reversed_state["objective_events"][0]["event_type"],
            "POST_ALIGNMENT_OBJECTIVE_CANCELED",
        )

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
        self.assertEqual(result["version"], 3)
        self.assertEqual(result["summary"]["complete"], 1)
        self.assertEqual(performance["first_profitable_h1_bars"], 1)
        self.assertEqual(performance["peak_h1_bars"], 72)
        self.assertTrue(performance["path_complete"])
        self.assertEqual(
            result["pair_profiles"]["GBPJPY"]["overall"]["status"],
            "LEARNING",
        )
        horizon = result["summary"]["overall"]["horizons"]["72h"]
        self.assertEqual(horizon["samples"], 1)
        self.assertEqual(horizon["win_rate_pct"], 100.0)
        self.assertEqual(horizon["avg_directional_pct"], 10.0)

    def test_pair_profile_learns_profit_and_exit_timing(self):
        tracked_events = [{
            "event_id": f"audjpy-entry-{index}",
            "event_type": "VIVIER_ENTRY",
            "pair": "AUDJPY",
            "direction": 1,
            "time_utc": "2026-06-27T10:00:00+00:00",
            "price": 100.0,
            "objective_value": 108.0,
        } for index in range(20)]
        first_bar = pd.Timestamp("2026-06-27T11:00:00Z")
        bars = []
        closes = {1: 100.01, 2: 100.03, 3: 101.0, 4: 103.0, 5: 105.0}
        for ordinal in range(1, 73):
            close = closes.get(ordinal, 101.0)
            bars.append({
                "time_utc": (first_bar + pd.Timedelta(hours=ordinal - 1)).isoformat(),
                "high": 108.1 if ordinal == 10 else close,
                "low": min(100.0, close),
                "close": close,
                "sar_dir": 1 if ordinal <= 5 else -1,
            })

        result = update_vivier_performance(
            {},
            tracked_events,
            [{"pair": "AUDJPY", "h1_fib": {"_closed_h1_bars": bars}}],
            NOW,
        )
        profile = result["pair_profiles"]["AUDJPY"]["overall"]

        self.assertEqual(profile["status"], "READY")
        self.assertEqual(profile["complete_72h"], 20)
        self.assertEqual(profile["median_first_profitable_h1"], 2.0)
        self.assertEqual(profile["median_peak_h1"], 5.0)
        self.assertEqual(profile["exit_window_h1"], {
            "start_h1": 5.0,
            "end_h1": 5.0,
        })
        self.assertEqual(profile["median_adverse_sar_h1"], 6.0)
        self.assertEqual(profile["objective_hit_rate_pct"], 100.0)
        self.assertEqual(profile["median_objective_h1"], 10.0)

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
