#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tests/test_ai_consensus.py
---------------------------
Unit tests for the Dual-AI Consensus Engine (Claude x Codex).
"""

import unittest
from fios.ai_consensus import generate_ai_consensus, _truncate_line, _fallback_synthesis


class TestAIConsensus(unittest.TestCase):

    def test_truncate_line(self):
        long_line = "🔥 Une tres tres tres tres tres longue phrase d'analyse"
        truncated = _truncate_line(long_line, max_chars=25)
        self.assertLessEqual(len(truncated), 25)
        self.assertTrue(truncated.endswith("…"))

    def test_fallback_synthesis(self):
        report = "🟢 XAUUSD (4393.37) ↑↑ 🔥\n🔴 USDCAD (1.38700) ↓↓ 🔥\n🟢 NZDUSD (0.58948) ↑↑ 🔥"
        lines = _fallback_synthesis(report)
        self.assertEqual(len(lines), 2)
        self.assertIn("XAUUSD", lines[0])

    def test_generate_ai_consensus(self):
        report = "🟢 NZDUSD (0.58948) ↑↑ 🔥\n🔴 USDCAD (1.38700) ↓↓ 🔥"
        res = generate_ai_consensus(report, timeout=1.0)
        self.assertIsNotNone(res)
        self.assertGreaterEqual(len(res), 3)
        self.assertTrue(res[0].startswith("🤝") or res[0].startswith("🧠"))


if __name__ == "__main__":
    unittest.main()
