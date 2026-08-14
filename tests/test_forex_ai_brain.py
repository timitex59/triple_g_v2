import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from forex_ai_brain import (
    PARIS,
    _validated_suggestions,
    build_evidence,
    calculate_trade_stats,
    collect_closed_trades,
    collect_market_snapshot,
    load_knowledge_base,
    record_run_observation,
    run_eod_analysis,
    save_knowledge_base,
)


class TestForexAIBrain(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.kb_file = self.root / "kb.json"

    def tearDown(self):
        self.tmp.cleanup()

    def _write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return str(path)

    def test_v1_memory_is_migrated_and_saved_atomically(self):
        self._write("kb.json", {
            "version": 1, "total_runs_analyzed": 3,
            "history": [{"timestamp": "2026-08-14T10:00:00+02:00"}],
            "script_improvements": [{"summary": "ancien"}],
        })
        kb = load_knowledge_base(str(self.kb_file))
        self.assertEqual(kb["version"], 2)
        self.assertEqual(len(kb["observations"]), 1)
        self.assertEqual(kb["suggestions"][0]["summary"], "ancien")
        save_knowledge_base(kb, str(self.kb_file))
        self.assertEqual(json.loads(self.kb_file.read_text(encoding="utf-8"))["version"], 2)

    def test_snapshot_keeps_pair_names_directions_prices_and_sources(self):
        paths = {
            "full_alignment": self._write("full.json", {
                "generated_at": "2026-08-14T10:00:00+02:00",
                "currencies": {"USD": {"daily_chg": -0.4, "full_alignment": -1}},
                "pairs": {"EURUSD": {"daily_chg": 0.2, "live_price": 1.15,
                    "px_m": 1, "px_w": 1, "px_d": 1, "full_alignment": 1}},
            }),
            "vivier_state": self._write("vivier.json", {"pairs": {
                "USDCHF": {"direction": 1, "daily_chg": -0.1, "daily_sar_dir": 1}}}),
            "vivier_pips": self._write("vp.json", {}),
            "fibo_pips": self._write("fp.json", {"open": {
                "GBPCAD": {"direction": 1, "entry_price": 1.88}}}),
            "fios": self._write("fios.json", {"date": "2026-08-14", "sections": {
                "XAUUSD": {"section": "top", "dir": -1}},
                "pairs": {"XAUUSD": {"level": 4300.0}}}),
        }
        snap = collect_market_snapshot(datetime(2026, 8, 14, 10, tzinfo=PARIS), paths)
        self.assertEqual(snap["mwd_raw_alignment"]["EURUSD"]["price"], 1.15)
        self.assertEqual(snap["vivier_pool"]["USDCHF"]["direction"], 1)
        self.assertEqual(snap["fios_sections"]["XAUUSD"]["direction"], -1)
        self.assertEqual(snap["prices"]["XAUUSD"], 4300.0)

    def test_closed_trade_import_is_deduplicated_across_runs(self):
        paths = {
            "full_alignment": self._write("full.json", {}),
            "vivier_state": self._write("vivier.json", {}),
            "vivier_pips": self._write("vp.json", {"days": {"2026-08-14": {
                "confirmed_segments": [{"pair": "EURUSD", "direction": 1,
                    "start_time_paris": "2026-08-14T07:00:00+02:00",
                    "end_time_paris": "2026-08-14T09:00:00+02:00", "pips": 12.0}]}}}),
            "fibo_pips": self._write("fp.json", {"closed": []}),
            "fios": self._write("fios.json", {"tracking": {"days": {}}}),
        }
        trades = collect_closed_trades(paths)
        kb = load_knowledge_base(str(self.kb_file))
        snap = {"timestamp": "2026-08-14T10:00:00+02:00"}
        record_run_observation(kb, snap, trades)
        record_run_observation(kb, snap, trades)
        self.assertEqual(kb["total_runs_analyzed"], 1)
        self.assertEqual(len(kb["trade_ledger"]), 1)

    def test_stats_are_deterministic_and_include_drawdown(self):
        trades = [{"pips": value} for value in (10, -4, -8, 20)]
        stats = calculate_trade_stats(trades)
        self.assertEqual(stats["wins"], 2)
        self.assertEqual(stats["losses"], 2)
        self.assertEqual(stats["net_pips"], 18.0)
        self.assertEqual(stats["profit_factor"], 2.5)
        self.assertEqual(stats["gat"], 2.5)
        self.assertEqual(stats["max_drawdown_pips"], 12.0)
        self.assertFalse(stats["sample_sufficient"])

    def test_24h_window_uses_timestamps_not_last_24_items(self):
        kb = load_knowledge_base(str(self.kb_file))
        kb["observations"] = [
            {"timestamp": "2026-08-12T10:00:00+02:00"},
            {"timestamp": "2026-08-14T09:00:00+02:00"},
        ]
        evidence = build_evidence(kb, datetime(2026, 8, 14, 12, tzinfo=PARIS))
        self.assertEqual(evidence["window_24h"]["runs"], 1)
        self.assertNotIn("observations", evidence["window_24h"])

    def test_ai_suggestion_without_sufficient_named_evidence_is_rejected(self):
        evidence = {"statistics": {"VIVIER_CONFIRMED": {
            "sample_sufficient": True, "trades": 30, "profit_factor": 2.5}}}
        reviews = [{"reviewer": "claude", "suggestions": [
            {"hypothesis": "Changer un seuil", "evidence": "intuition générale"},
            {"hypothesis": "Fausse preuve", "evidence": {
                "strategy": "VIVIER_CONFIRMED", "trades": 29,
                "metric": "profit_factor", "observed_value": 2.5}},
            {"hypothesis": "Tester un filtre", "evidence": {
                "strategy": "VIVIER_CONFIRMED", "trades": 30,
                "metric": "profit_factor", "observed_value": 2.5}},
        ]}]
        accepted = _validated_suggestions(reviews, evidence, "2026-08-14")
        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0]["status"], "PROPOSED")

    @patch("forex_ai_brain.run_dual_review", return_value=[])
    def test_eod_without_api_never_claims_consensus(self, _review):
        kb = load_knowledge_base(str(self.kb_file))
        report = run_eod_analysis(kb, send_tg=False,
                                  now=datetime(2026, 8, 14, 23, tzinfo=PARIS))
        self.assertIn("BILAN FACTUEL", report)
        self.assertIn("0/2 disponibles", report)
        self.assertIn("aucun faux consensus", report)
        self.assertNotIn("CONSENSUS IA", report)
        self.assertFalse(kb["daily_reports"][-1]["telegram_sent"])


if __name__ == "__main__":
    unittest.main()
