import unittest
from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

# Allow direct execution: python tests/test_renko_score_vivier.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from renko_score_29pairs_v16 import update_vivier, vivier_groups


PARIS = ZoneInfo("Europe/Paris")
NOW = datetime(2026, 6, 27, 12, 0, tzinfo=PARIS)


def row(pair, monthly, weekly, daily, weighted_pct=0.0):
    return {
        "pair": pair,
        "px": {"M": monthly, "W": weekly, "D": daily},
        "weighted_pct": weighted_pct,
    }


class VivierStateTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
