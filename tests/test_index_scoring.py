#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tests/test_index_scoring.py
----------------------------
Unit tests for the Currency Index Scoring engine.
"""

import unittest
from fios.index_scoring import compute_currency_index_scores, CurrencyScore


class TestIndexScoring(unittest.TestCase):

    def setUp(self):
        self.composites = {
            "AUD": {"composite": 70.0},
            "USD": {"composite": 20.0},
            "EUR": {"composite": 55.0},
            "GBP": {"composite": 60.0},
            "NZD": {"composite": 65.0},
            "CHF": {"composite": 50.0},
            "CAD": {"composite": 45.0},
            "JPY": {"composite": 35.0},
        }
        self.payload = {
            "indexes": {
                "AUD": {"daily_chg": 0.55, "px_m": 1, "px_w": 1, "px_d": 1},
                "USD": {"daily_chg": -0.36, "px_m": 0, "px_w": -1, "px_d": -1},
            }
        }

    def test_compute_scores(self):
        scores = compute_currency_index_scores(self.composites, self.payload)
        self.assertIn("AUD", scores)
        aud = scores["AUD"]
        self.assertGreaterEqual(aud.total_score, 50)
        self.assertIn(aud.label, ["FORT", "ULTRA-FORT"])

        usd = scores["USD"]
        self.assertLess(usd.total_score, 0)
        self.assertIn(usd.label, ["FAIBLE", "ULTRA-FAIBLE"])


if __name__ == "__main__":
    unittest.main()
