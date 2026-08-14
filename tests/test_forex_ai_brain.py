import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from forex_ai_brain import (
    EOD_CLAUDE_MODELS,
    EOD_OPENAI_MODELS,
    FLASH_CLAUDE_MODELS,
    FLASH_OPENAI_MODELS,
    PARIS,
    _query_openai_responses,
    _responses_text,
    _validated_suggestions,
    build_evidence,
    build_flash_features,
    calculate_trade_stats,
    collect_closed_trades,
    collect_market_snapshot,
    load_knowledge_base,
    format_flash_consensus,
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
            "fibo_snapshot": self._write("fs.json", {"sections": {
                "daily": [{"pair": "AUDJPY", "direction": "BEAR"}]}}),
            "fios": self._write("fios.json", {"date": "2026-08-14", "sections": {
                "XAUUSD": {"section": "top", "dir": -1}},
                "pairs": {"XAUUSD": {"level": 4300.0}}}),
        }
        snap = collect_market_snapshot(datetime(2026, 8, 14, 10, tzinfo=PARIS), paths)
        self.assertEqual(snap["mwd_raw_alignment"]["EURUSD"]["price"], 1.15)
        self.assertEqual(snap["vivier_pool"]["USDCHF"]["direction"], 1)
        self.assertEqual(snap["fios_sections"]["XAUUSD"]["direction"], -1)
        self.assertEqual(snap["prices"]["XAUUSD"], 4300.0)
        self.assertEqual(snap["fibo_sections"]["daily"][0]["pair"], "AUDJPY")

    def test_closed_trade_import_is_deduplicated_across_runs(self):
        paths = {
            "full_alignment": self._write("full.json", {}),
            "vivier_state": self._write("vivier.json", {}),
            "vivier_pips": self._write("vp.json", {"days": {"2026-08-14": {
                "confirmed_segments": [{"pair": "EURUSD", "direction": 1,
                    "start_time_paris": "2026-08-14T07:00:00+02:00",
                    "end_time_paris": "2026-08-14T09:00:00+02:00", "pips": 12.0}]}}}),
            "fibo_pips": self._write("fp.json", {"closed": []}),
            "fibo_snapshot": self._write("fs.json", {}),
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

    def test_flash_detects_multi_cross_currency_strength_and_persistence(self):
        kb = load_knowledge_base(str(self.kb_file))
        for hour in (8, 9, 10):
            kb["observations"].append({
                "timestamp": f"2026-08-14T{hour:02d}:00:00+02:00", "date": "2026-08-14",
                "full_selected": {}, "vivier_pool": {}, "fibo_sections": {},
                "fios_sections": {
                    "NZDUSD": {"section": "top", "direction": 1},
                    "NZDCHF": {"section": "top", "direction": 1},
                    "EURNZD": {"section": "top", "direction": -1},
                    "AUDNZD": {"section": "conv", "direction": -1},
                },
            })
        features = build_flash_features(kb)
        self.assertEqual(features["strongest_currency"], "NZD")
        self.assertEqual(features["currency_cross_confirmations"]["NZD"], 4)
        nzdusd = next(item for item in features["candidates"] if item["pair"] == "NZDUSD")
        self.assertEqual(nzdusd["persistence_runs"], 3)

    def test_consensus_label_requires_actual_agreement(self):
        features = {
            "status": "OK", "strongest_currency": "NZD", "weakest_currency": "CHF",
            "currency_scores": {"NZD": 8, "CHF": -5, "USD": -3},
            "currency_cross_confirmations": {"NZD": 4}, "contradictions": [], "gold": [],
            "candidates": [{"pair": "NZDCHF", "direction": "LONG", "score": 8,
                            "sources": ["FIOS_TOP", "FIBO_FULL"],
                            "persistence_runs": 3, "contradiction": False},
                           {"pair": "NZDUSD", "direction": "LONG", "score": 7,
                            "sources": ["FIOS_TOP"], "persistence_runs": 3,
                            "contradiction": False}],
        }
        agreement = [
            {"reviewer": "claude", "strongest_currency": "NZD", "weakest_currency": "CHF",
             "best_pair": "NZDCHF", "direction": "LONG"},
            {"reviewer": "openai", "strongest_currency": "NZD", "weakest_currency": "CHF",
             "best_pair": "NZDCHF", "direction": "LONG"},
        ]
        report = format_flash_consensus(features, agreement)
        self.assertIn("CONSENSUS IA (Claude x GPT-5)", report)
        self.assertIn("NZDCHF LONG · 3 runs · 2 moteurs", report)
        disagreement = [agreement[0], {**agreement[1], "best_pair": "NZDUSD", "weakest_currency": "USD"}]
        report = format_flash_consensus(features, disagreement)
        self.assertNotIn("CONSENSUS IA (Claude x GPT-5)", report)
        self.assertIn("SYNTHÈSE IA MULTI-SCREENER", report)

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

    def test_frontier_models_are_the_default_reviewers(self):
        self.assertTrue(FLASH_CLAUDE_MODELS.startswith("claude-sonnet-4-"))
        self.assertTrue(EOD_CLAUDE_MODELS.startswith("claude-opus-4-1-"))
        self.assertTrue(FLASH_OPENAI_MODELS.startswith("gpt-5.1"))
        self.assertTrue(EOD_OPENAI_MODELS.startswith("gpt-5-pro"))

    def test_responses_api_text_is_extracted(self):
        body = {"output": [{"content": [
            {"type": "output_text", "text": '{"best_pair":"NZDCHF"}'},
        ]}]}
        self.assertEqual(_responses_text(body), '{"best_pair":"NZDCHF"}')

    @patch("forex_ai_brain._post_json")
    def test_gpt5_uses_responses_api_with_reasoning(self, post):
        post.return_value = {"output_text": '{"best_pair":"NZDCHF"}'}
        result = _query_openai_responses(
            "preuves", "instructions", ["gpt-5.1"], "secret",
            max_tokens=300, reasoning="medium", timeout=12,
        )
        self.assertEqual(result["model"], "gpt-5.1")
        url, _headers, payload, _timeout = post.call_args.args
        self.assertEqual(url, "https://api.openai.com/v1/responses")
        self.assertEqual(payload["reasoning"], {"effort": "medium"})
        self.assertFalse(payload["store"])


if __name__ == "__main__":
    unittest.main()
