#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tests/test_renko_fibo_50.py
----------------------------
Unit tests for renko_fibo_50_strategy.py logic.
"""

import unittest
from renko_fibo_50_strategy import (
    Fibo50AnchorState,
    detect_fibo_50_level,
    format_telegram_fibo_50_report,
)


class TestRenkoFibo50Strategy(unittest.TestCase):

    def test_detect_fibo_50_level_3_bricks_bull(self):
        # 3 red bricks followed by 3 green bricks
        bricks = [
            (1.2000, 1.1900, -1),
            (1.1900, 1.1800, -1),
            (1.1800, 1.1700, -1), # Swing Low at 1.1700
            (1.1700, 1.1800, 1),
            (1.1800, 1.1900, 1),
            (1.1900, 1.2000, 1),  # Swing High at 1.2000
        ]
        anchor_high, anchor_low, fibo_50, confirmed, direction = detect_fibo_50_level(bricks)
        self.assertTrue(confirmed)
        self.assertEqual(direction, 1)
        self.assertEqual(anchor_low, 1.1700)
        self.assertEqual(anchor_high, 1.2000)
        self.assertAlmostEqual(fibo_50, (1.2000 + 1.1700) / 2.0)

    def test_detect_fibo_50_level_3_bricks_bear(self):
        # 3 green bricks followed by 3 red bricks
        bricks = [
            (1.2000, 1.2100, 1),
            (1.2100, 1.2200, 1),
            (1.2200, 1.2300, 1), # Swing High at 1.2300
            (1.2300, 1.2200, -1),
            (1.2200, 1.2100, -1),
            (1.2100, 1.2000, -1), # Swing Low at 1.2000
        ]
        anchor_high, anchor_low, fibo_50, confirmed, direction = detect_fibo_50_level(bricks)
        self.assertTrue(confirmed)
        self.assertEqual(direction, -1)
        self.assertEqual(anchor_high, 1.2300)
        self.assertEqual(anchor_low, 1.2000)
        self.assertAlmostEqual(fibo_50, (1.2300 + 1.2000) / 2.0)

    def test_format_telegram_fibo_50_report_bull_and_bear(self):
        results = {
            "EURUSD": {
                "D": Fibo50AnchorState(
                    pair="EURUSD", tf="D", direction=1,
                    anchor_high=1.2000, anchor_low=1.1600, fibo_50=1.1800,
                    last_brick_open=1.1750, last_brick_close=1.1850,
                    px_vs_fibo=1, signal="BULL", three_brick_confirmed=True
                )
            },
            "GBPUSD": {
                "D": Fibo50AnchorState(
                    pair="GBPUSD", tf="D", direction=-1,
                    anchor_high=1.3500, anchor_low=1.3100, fibo_50=1.3300,
                    last_brick_open=1.3350, last_brick_close=1.3250,
                    px_vs_fibo=-1, signal="BEAR", three_brick_confirmed=True
                )
            }
        }

        report = format_telegram_fibo_50_report(results)
        self.assertIsNotNone(report)
        self.assertIn("📊 RENKO FIBO 50% RETRACEMENT", report)
        self.assertIn("🌱 SIGNAUX BULL (> FIBO 50%)", report)
        self.assertIn("EURUSD (D)", report)
        self.assertIn("🔻 SIGNAUX BEAR (< FIBO 50%)", report)
        self.assertIn("GBPUSD (D)", report)

    def test_format_telegram_fibo_50_report_suppress_empty(self):
        results = {
            "EURUSD": {
                "D": Fibo50AnchorState(
                    pair="EURUSD", tf="D", direction=1,
                    anchor_high=1.2000, anchor_low=1.1600, fibo_50=1.1800,
                    last_brick_open=1.1750, last_brick_close=1.1850,
                    px_vs_fibo=1, signal="NONE", three_brick_confirmed=False
                )
            }
        }
        report = format_telegram_fibo_50_report(results)
        self.assertIsNone(report)


if __name__ == "__main__":
    unittest.main()
