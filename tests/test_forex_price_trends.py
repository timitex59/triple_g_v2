import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from forex_price_trends import load_state, save_state, suffix, update


PARIS = ZoneInfo("Europe/Paris")


class ForexPriceTrendTests(unittest.TestCase):
    def test_daily_baseline_and_previous_run_are_independent(self):
        state, first = update({}, {"EURUSD": 1.1000}, datetime(2026, 8, 13, 7, 15, tzinfo=PARIS))
        state, second = update(state, {"EURUSD": 1.1020}, datetime(2026, 8, 13, 8, 15, tzinfo=PARIS))
        _, third = update(state, {"EURUSD": 1.1010}, datetime(2026, 8, 13, 9, 15, tzinfo=PARIS))

        self.assertEqual(first["EURUSD"]["vs_07h"], "→")
        self.assertEqual(second["EURUSD"]["vs_07h"], "↑")
        self.assertEqual(second["EURUSD"]["vs_previous"], "↑")
        self.assertEqual(third["EURUSD"]["vs_07h"], "↑")
        self.assertEqual(third["EURUSD"]["vs_previous"], "↓")

    def test_format_precision_and_persistence(self):
        state, trends = update({}, {"USDJPY": 159.296}, datetime(2026, 8, 13, 7, 15, tzinfo=PARIS))
        self.assertEqual(suffix("USDJPY", trends), " (159.296) →→")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            save_state(path, state)
            self.assertEqual(load_state(path), state)


if __name__ == "__main__":
    unittest.main()
