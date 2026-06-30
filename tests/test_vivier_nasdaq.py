import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from vivier_nasdaq import (
    analyze_h1_frame,
    build_nasdaq_message,
    load_pipeline_universe,
    pre_entry_candidate,
)


class VivierNasdaqTests(unittest.TestCase):
    def test_load_pipeline_universe_preserves_pipeline_order(self):
        payload = {
            "stocks": [
                {"ticker": "AMD", "name": "AMD", "theme": "Semis", "score": 99.0},
                {"ticker": "WMT", "name": "Walmart", "theme": "Retail", "score": 80.0},
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            assets = load_pipeline_universe(
                path,
                limit=1,
                symbol_lookup={"AMD": "NASDAQ:AMD", "WMT": "NYSE:WMT"},
            )

        self.assertEqual([asset.ticker for asset in assets], ["AMD"])
        self.assertEqual(assets[0].tv_symbol, "NASDAQ:AMD")
        self.assertEqual(assets[0].pipeline_score, 99.0)

    def test_analyze_h1_frame_builds_monthly_fibo(self):
        index = pd.date_range("2026-06-01 13:30", periods=5, freq="h", tz="UTC")
        frame = pd.DataFrame(
            {
                "open": [100, 101, 102, 103, 104],
                "high": [102, 103, 104, 105, 106],
                "low": [99, 100, 101, 102, 103],
                "close": [101, 102, 103, 104, 105],
            },
            index=index,
        )

        result = analyze_h1_frame(frame)

        self.assertIsNotNone(result)
        self.assertEqual(result["month_low"], 99.0)
        self.assertEqual(result["month_high"], 106.0)
        self.assertEqual(result["fib50"], 102.5)
        self.assertEqual(result["position"], "ABOVE")

    def test_pre_entry_candidate_matches_vivier_profiles(self):
        self.assertTrue(pre_entry_candidate({"M": 1, "W": 1, "D": 0}))
        self.assertTrue(pre_entry_candidate({"M": -1, "W": 0, "D": 1}))
        self.assertFalse(pre_entry_candidate({"M": 1, "W": 0, "D": 1}))
        self.assertFalse(pre_entry_candidate({"M": 0, "W": -1, "D": -1}))

    def test_message_uses_nasdaq_timeframes_and_sections(self):
        state = {
            "pairs": {
                "AMD": {
                    "direction": 1,
                    "last_px": {"M": 1, "W": 1, "D": 0},
                    "fib_position": "Fibo <0.500",
                }
            },
            "pending_objectives": {},
        }
        signals = [{"pair": "NVDA", "direction": 1, "weighted_pct": 100.0}]
        now = datetime(2026, 6, 30, 19, 0, tzinfo=ZoneInfo("Europe/Paris"))

        message = build_nasdaq_message(state, signals, [], now=now)

        self.assertIn("VIVIER NASDAQ BULL", message)
        self.assertIn("🟢 AMD (+83% | <0.500)", message)
        self.assertIn("NVDA · 3M/M/W alignés · +100%", message)
        self.assertIn("AMD · W restant · +83%", message)


if __name__ == "__main__":
    unittest.main()
