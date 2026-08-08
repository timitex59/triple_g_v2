#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tests/test_multilayer_strategy.py
----------------------------------
Unit tests for the Multi-Layer Selection Strategy engine.
"""

import unittest
from fios.multilayer import evaluate_pair_multilayer, compute_multilayer_matrix, format_multilayer_section, MultiLayerScore
from fios.index_scoring import CurrencyScore


class TestMultiLayerStrategy(unittest.TestCase):

    def setUp(self):
        self.composites = {
            "AUD": {"composite": 65.0},
            "USD": {"composite": 40.0},
            "EUR": {"composite": 35.0},
            "CAD": {"composite": 55.0},
            "JPY": {"composite": 45.0},
        }
        self.align_pairs = {
            "AUDUSD": {"px_m": 1, "px_w": 1, "px_d": 1, "daily_chg": 0.50},
            "EURAUD": {"px_m": -1, "px_w": -1, "px_d": -1, "daily_chg": -0.20},
        }
        self.index_scores = {
            "AUD": CurrencyScore("AUD", 88, "ULTRA-FORT", "🟢🟢", 0.55, "M+ W+ D+", 1, 70.0),
            "USD": CurrencyScore("USD", -62, "FAIBLE", "🔴", -0.36, "M0 W- D-", 8, 20.0),
        }

    def test_evaluate_pair_aplus(self):
        class MockFiboState:
            direction = 1
            three_brick_confirmed = True
            px_vs_fibo = 1

        fibo_results = {"AUDUSD": {"D": MockFiboState()}}
        vivier_state = {"pairs": {"AUDUSD": {"direction": 1, "daily_sar_dir": 1}}}

        score = evaluate_pair_multilayer(
            "AUDUSD", self.composites, self.align_pairs, fibo_results, vivier_state, self.index_scores
        )
        self.assertIsNotNone(score)
        self.assertEqual(score.pair, "AUDUSD")
        self.assertGreaterEqual(score.total_score, 80)
        self.assertEqual(score.grade, "A+")
        self.assertIn("Diff Index Max", score.layers_passed)
        self.assertIn("Renko M/W/D", score.layers_passed)
        self.assertIn("Fibo 50%", score.layers_passed)
        self.assertIn("Vivier H1", score.layers_passed)

    def test_format_section(self):
        scores = [
            MultiLayerScore(
                pair="AUDUSD",
                direction=1,
                total_score=100,
                grade="A+",
                layers_passed=["Diff Index Max", "Renko M/W/D", "Fibo 50%", "Vivier H1"],
                index_diff=150,
                base_score=88,
                quote_score=-62,
                base_cur="AUD",
                quote_cur="USD",
                daily_chg=0.50,
                tag_mwd="M+ W+ D+",
            )
        ]
        lines = format_multilayer_section(scores)
        self.assertTrue(any("CLASSEMENT DES MEILLEURES PAIRES" in line for line in lines))
        self.assertTrue(any("🥇 🟢 AUDUSD" in line for line in lines))


if __name__ == "__main__":
    unittest.main()
