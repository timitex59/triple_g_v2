#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tests/test_renko_fibo_50.py
----------------------------
Unit tests for renko_fibo_50_strategy.py logic.
"""

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from renko_fibo_50_strategy import (
    Fibo50AnchorState,
    detect_fibo_50_level,
    format_telegram_fibo_50_report,
    save_research_snapshot,
)


class TestRenkoFibo50Strategy(unittest.TestCase):

    def test_research_snapshot_exposes_all_three_sections(self):
        state = Fibo50AnchorState(
            pair="EURUSD", tf="D", direction=1,
            anchor_high=1.2, anchor_low=1.16, fibo_50=1.18,
            last_brick_open=1.17, last_brick_close=1.19,
            px_vs_fibo=1, signal="BULL", three_brick_confirmed=True,
            crossed_50=True, bricks_since_flip=1, live_price=1.195, daily_chg=0.2,
        )
        sections = ([('EURUSD', 'BULL', 0.2)], [('EURUSD', 'BULL', 0.2)], [])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "snapshot.json"
            save_research_snapshot({"EURUSD": {"D": state}}, sections,
                                   datetime(2026, 8, 14, 10,
                                            tzinfo=ZoneInfo("Europe/Paris")), path)
            payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload["sections"]["daily"][0]["pair"], "EURUSD")
        self.assertEqual(payload["sections"]["full_alignment"][0]["direction"], "BULL")
        self.assertEqual(payload["prices"]["EURUSD"], 1.195)

    def test_detect_fibo_50_level_3_bricks_bull(self):
        # 5 red bricks followed by 5 green bricks
        bricks = [
            (1.2100, 1.2000, -1),
            (1.2000, 1.1900, -1),
            (1.1900, 1.1800, -1),
            (1.1800, 1.1700, -1),
            (1.1700, 1.1600, -1), # Swing Low at 1.1600
            (1.1600, 1.1700, 1),
            (1.1700, 1.1800, 1),
            (1.1800, 1.1900, 1),
            (1.1900, 1.2000, 1),
            (1.2000, 1.2100, 1),  # Swing High at 1.2100
        ]
        anchor_high, anchor_low, fibo_50, confirmed, direction, bricks_since, confirm_idx = detect_fibo_50_level(bricks)
        self.assertTrue(confirmed)
        self.assertEqual(direction, 1)

    def test_detect_fibo_50_level_3_bricks_bear(self):
        # 5 green bricks followed by 5 red bricks
        bricks = [
            (1.1600, 1.1700, 1),
            (1.1700, 1.1800, 1),
            (1.1800, 1.1900, 1),
            (1.1900, 1.2000, 1),
            (1.2000, 1.2100, 1), # Swing High at 1.2100
            (1.2100, 1.2000, -1),
            (1.2000, 1.1900, -1),
            (1.1900, 1.1800, -1),
            (1.1800, 1.1700, -1),
            (1.1700, 1.1600, -1), # Swing Low at 1.1600
        ]
        anchor_high, anchor_low, fibo_50, confirmed, direction, bricks_since, confirm_idx = detect_fibo_50_level(bricks)
        self.assertTrue(confirmed)
        self.assertEqual(direction, -1)

    def test_format_telegram_fibo_50_report_bull_and_bear(self):
        state_eur = Fibo50AnchorState(
            pair="EURUSD", tf="D", direction=1,
            anchor_high=1.2000, anchor_low=1.1600, fibo_50=1.1800,
            last_brick_open=1.1750, last_brick_close=1.1850,
            px_vs_fibo=1, signal="BULL", three_brick_confirmed=True,
            crossed_50=True, bricks_since_flip=3, live_price=1.1900, daily_chg=0.5
        )
        state_aud = Fibo50AnchorState(
            pair="AUDJPY", tf="D", direction=1,
            anchor_high=98.00, anchor_low=94.00, fibo_50=96.00,
            last_brick_open=96.50, last_brick_close=97.50,
            px_vs_fibo=1, signal="BULL", three_brick_confirmed=True,
            crossed_50=True, bricks_since_flip=3, live_price=98.00, daily_chg=0.6
        )
        results = {
            "EURUSD": {"M": state_eur, "W": state_eur, "D": state_eur},
            "AUDJPY": {"M": state_aud, "W": state_aud, "D": state_aud}
        }

        report = format_telegram_fibo_50_report(
            results,
            price_trends={
                "EURUSD": {"price": 1.1900, "vs_07h": "↑", "vs_previous": "↓"},
                "AUDJPY": {"price": 98.0, "vs_07h": "↓", "vs_previous": "↑"},
            },
        )
        self.assertIsNotNone(report)
        self.assertIn("RENKO FIBO 50%", report)
        self.assertIn("EURUSD", report)
        self.assertIn("AUDJPY", report)
        self.assertIn("EURUSD (1.19000) ↑↓", report)
        self.assertIn("AUDJPY (98.000) ↓↑", report)
        self.assertNotIn("+0.50%", report)

    def test_format_telegram_fibo_50_report_suppress_empty(self):
        results = {
            "EURUSD": {
                "D": Fibo50AnchorState(
                    pair="EURUSD", tf="D", direction=1,
                    anchor_high=1.2000, anchor_low=1.1600, fibo_50=1.1800,
                    last_brick_open=1.1750, last_brick_close=1.1850,
                    px_vs_fibo=1, signal="NONE", three_brick_confirmed=False,
                    crossed_50=False, bricks_since_flip=0
                )
            }
        }
        report = format_telegram_fibo_50_report(results)
        self.assertIsNone(report)

    def test_daily_signal_alone_does_not_build_telegram_report(self):
        daily = Fibo50AnchorState(
            pair="EURUSD", tf="D", direction=1,
            anchor_high=1.2000, anchor_low=1.1600, fibo_50=1.1800,
            last_brick_open=1.1750, last_brick_close=1.1850,
            px_vs_fibo=1, signal="BULL", three_brick_confirmed=True,
            crossed_50=True, bricks_since_flip=3,
            live_price=1.1900, daily_chg=0.5,
        )

        report = format_telegram_fibo_50_report({"EURUSD": {"D": daily}})

        self.assertIsNone(report)


if __name__ == "__main__":
    unittest.main()
