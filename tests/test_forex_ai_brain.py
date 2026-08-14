#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tests/test_forex_ai_brain.py
-----------------------------
Unit tests for the Standalone Forex AI Brain Learning Engine.
"""

import os
import tempfile
import unittest
from forex_ai_brain import (
    load_knowledge_base,
    save_knowledge_base,
    collect_market_snapshot,
    record_run_observation,
    run_eod_analysis,
)


class TestForexAIBrain(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.kb_file = os.path.join(self.tmp_dir.name, "test_kb.json")

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_load_save_knowledge_base(self):
        kb = load_knowledge_base(self.kb_file)
        self.assertEqual(kb.get("total_runs_analyzed"), 0)
        
        kb["accumulated_patterns"].append("USD Sell-Off Pattern")
        save_knowledge_base(kb, self.kb_file)
        
        reloaded = load_knowledge_base(self.kb_file)
        self.assertEqual(reloaded.get("total_runs_analyzed"), 0)
        self.assertIn("USD Sell-Off Pattern", reloaded.get("accumulated_patterns", []))

    def test_record_run_observation(self):
        kb = load_knowledge_base(self.kb_file)
        snapshot = {
            "timestamp": "2026-08-14T16:24:00+02:00",
            "align_pairs": {"AUDUSD": {}},
            "indexes": {"NZD": {"daily_chg": 0.65}, "USD": {"daily_chg": -0.36}},
        }
        obs = record_run_observation(kb, snapshot)
        self.assertEqual(kb.get("total_runs_analyzed"), 1)
        self.assertIn("NZD (+0.65%)", obs["strong_currencies"])
        self.assertIn("USD (-0.36%)", obs["weak_currencies"])

    def test_run_eod_analysis(self):
        kb = load_knowledge_base(self.kb_file)
        report = run_eod_analysis(kb, send_tg=False)
        self.assertIn("🧠 FOREX IA BRAIN", report)
        self.assertIn("RÉTROSPECTIVE 24H", report)
        self.assertIn("SUGGESTIONS D'AMÉLIORATION", report)


if __name__ == "__main__":
    unittest.main()
