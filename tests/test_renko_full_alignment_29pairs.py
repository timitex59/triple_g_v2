import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from renko_full_alignment_29pairs import (
    FOREX_INDEX_ASSETS,
    assets_for_scope,
    format_full_alignment_message,
    full_alignment_direction,
    imp_state_from_close_sar,
    raw_alignment_score,
    select_full_alignment_rows,
)


PARIS = ZoneInfo("Europe/Paris")


def row(pair, m, w, d):
    return {
        "pair": pair,
        "px": {"M": m, "W": w, "D": d},
        "asset_type": "PAIR",
        "h1_price": 1.23456,
    }


def index_row(pair, m, w, d):
    item = row(pair, m, w, d)
    item["asset_type"] = "INDEX"
    item["currency"] = {"BXY": "GBP", "JXY": "JPY"}.get(pair)
    return item


class FullAlignmentScannerTests(unittest.TestCase):
    def test_default_universe_includes_forex_indices(self):
        index_symbols = {asset["tv_symbol"] for asset in FOREX_INDEX_ASSETS}

        self.assertEqual(index_symbols, {
            "TVC:DXY",
            "TVC:EXY",
            "TVC:BXY",
            "TVC:JXY",
            "TVC:SXY",
            "TVC:CXY",
            "TVC:AXY",
            "TVC:ZXY",
        })
        self.assertTrue(index_symbols.issubset({
            asset["tv_symbol"] for asset in assets_for_scope("all")
        }))
        self.assertEqual(
            {asset["pair"] for asset in assets_for_scope("indices")},
            {"DXY", "EXY", "BXY", "JXY", "SXY", "CXY", "AXY", "ZXY"},
        )

    def test_detects_strict_bull_and_bear_alignment(self):
        self.assertEqual(full_alignment_direction(row("GBPJPY", 1, 1, 1)), 1)
        self.assertEqual(full_alignment_direction(row("CADCHF", -1, -1, -1)), -1)
        self.assertEqual(raw_alignment_score(row("GBPJPY", 1, 1, 1)), 100.0)
        self.assertEqual(raw_alignment_score(row("CADCHF", -1, -1, -1)), -100.0)

    def test_rejects_inside_or_mixed_alignment(self):
        self.assertEqual(full_alignment_direction(row("AUDJPY", 1, 0, 1)), 0)
        self.assertEqual(full_alignment_direction(row("EURJPY", 1, 1, -1)), 0)

    def test_imp_state_detects_imp_breaks(self):
        state = imp_state_from_close_sar(
            closes=[9.0, 11.0, 11.0, 8.0, 8.0, 13.0, 6.0],
            sar_values=[10.0, 10.0, 9.0, 12.0, 11.0, 7.0, 5.0],
        )

        self.assertEqual(
            [(event["direction"], event["index"]) for event in state["events"]],
            [(-1, 3), (1, 5)],
        )
        self.assertEqual(
            [(event["kind"], event["index"]) for event in state["break_events"]],
            [("IMP BEAR CASSÉ HAUSSE", 5), ("IMP BULL CASSÉ BAISSE", 6)],
        )
        self.assertEqual(state["last_imp_direction"], 1)
        self.assertEqual(state["last_break_direction"], -1)
        self.assertEqual(state["last_bar_break_direction"], -1)
        self.assertEqual(state["last_bar_break_kind"], "IMP BULL CASSÉ BAISSE")

    def test_selects_only_full_alignment_rows(self):
        selected = select_full_alignment_rows([
            row("GBPJPY", 1, 1, 1),
            row("AUDJPY", 1, 0, 1),
            row("CADCHF", -1, -1, -1),
            row("EURJPY", 1, 1, 1),
            index_row("BXY", 1, 1, 1),
            index_row("JXY", -1, -1, -1),
        ])

        self.assertEqual(
            [item["pair"] for item in selected],
            ["EURJPY", "GBPJPY", "CADCHF", "BXY", "JXY"],
        )

    def test_message_is_compact(self):
        selected = select_full_alignment_rows([row("GBPJPY", 1, 1, 1)])
        message = format_full_alignment_message(
            selected,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn("📊 FULL ALIGNMENT M/W/D", message)
        self.assertIn("🟢 GBPJPY", message)
        self.assertNotIn("+100%", message)
        self.assertNotIn("M+/W+/D+", message)
        self.assertIn("2026-07-16 10:00 Paris", message)

    def test_message_marks_imp_breaks_only(self):
        selected = select_full_alignment_rows([
            row("GBPJPY", 1, 1, 1),
            index_row("JXY", -1, -1, -1),
        ])
        selected[0]["imp"] = {"last_bar_break_kind": "IMP BULL CASSÉ BAISSE"}
        selected[1]["imp"] = {"last_bar_break_kind": "IMP BEAR CASSÉ HAUSSE"}
        message = format_full_alignment_message(
            selected,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn("🟢 GBPJPY · IMP BULL cassé ↓", message)
        self.assertIn("🔴 JPY · IMP BEAR cassé ↑", message)

    def test_message_groups_pairs_before_indices(self):
        selected = select_full_alignment_rows([
            index_row("BXY", 1, 1, 1),
            row("GBPJPY", 1, 1, 1),
            index_row("JXY", -1, -1, -1),
            row("EURJPY", 1, 1, 1),
        ])
        message = format_full_alignment_message(
            selected,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn(
            "🟢 EURJPY\n🟢 GBPJPY\n\n🟢 GBP\n🔴 JPY",
            message,
        )
        self.assertNotIn("🟢 BXY", message)
        self.assertNotIn("🔴 JXY", message)


if __name__ == "__main__":
    unittest.main()
