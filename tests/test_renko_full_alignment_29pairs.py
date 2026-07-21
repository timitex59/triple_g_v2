import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from renko_full_alignment_29pairs import (
    FOREX_INDEX_ASSETS,
    assets_for_scope,
    format_full_alignment_message,
    full_alignment_direction,
    raw_alignment_score,
    select_full_alignment_rows,
    sar_break_state_from_close_sar,
    select_full_sar_rows,
    update_full_sar_history,
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

    def test_sar_break_state_detects_sar_crosses(self):
        state = sar_break_state_from_close_sar(
            closes=[9.0, 11.0, 8.0, 12.0],
            sar_values=[10.0, 10.0, 10.0, 10.0],
        )

        self.assertEqual(
            [(event["direction"], event["index"]) for event in state["events"]],
            [(1, 1), (-1, 2), (1, 3)],
        )
        self.assertEqual(state["last_sar_break_direction"], 1)
        self.assertEqual(state["last_sar_break_kind"], "SAR BULL")
        self.assertEqual(state["last_bar_sar_break_direction"], 1)
        self.assertEqual(state["last_bar_sar_break_kind"], "SAR BULL")

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

    def test_full_sar_keeps_only_full_alignment_directional_sar_breaks(self):
        selected = select_full_alignment_rows([
            row("GBPJPY", 1, 1, 1),
            row("EURJPY", 1, 1, 1),
            row("AUDJPY", 1, 0, 1),
            row("CADCHF", -1, -1, -1),
            index_row("JXY", -1, -1, -1),
        ])
        by_pair = {item["pair"]: item for item in selected}
        by_pair["GBPJPY"]["sar_break"] = {"last_bar_sar_break_direction": 1}
        by_pair["EURJPY"]["sar_break"] = {"last_bar_sar_break_direction": -1}
        by_pair["CADCHF"]["sar_break"] = {"last_bar_sar_break_direction": -1}
        by_pair["JXY"]["sar_break"] = {"last_bar_sar_break_direction": -1}

        full_sar_rows = select_full_sar_rows(selected)

        self.assertEqual([item["pair"] for item in full_sar_rows], ["GBPJPY", "CADCHF", "JXY"])

    def test_full_sar_history_tracks_daily_window_without_duplicates(self):
        selected = select_full_alignment_rows([
            row("GBPJPY", 1, 1, 1),
            index_row("JXY", -1, -1, -1),
        ])
        for item in selected:
            item["sar_break"] = {
                "last_bar_sar_break_direction": 1 if item["pair"] == "GBPJPY" else -1
            }
        full_sar_rows = select_full_sar_rows(selected)

        state, today = update_full_sar_history(
            {},
            full_sar_rows,
            datetime(2026, 7, 16, 8, 0, tzinfo=PARIS),
        )
        state, today = update_full_sar_history(
            state,
            full_sar_rows,
            datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertEqual(len(today["events"]), 2)
        by_pair = {event["pair"]: event for event in today["events"]}
        self.assertEqual(by_pair["GBPJPY"]["count"], 2)
        self.assertEqual(by_pair["GBPJPY"]["first_seen"], "2026-07-16T08:00+02:00")
        self.assertEqual(by_pair["GBPJPY"]["last_seen"], "2026-07-16T10:00+02:00")
        self.assertEqual(by_pair["JXY"]["currency"], "JPY")

        state, unchanged = update_full_sar_history(
            state,
            full_sar_rows,
            datetime(2026, 7, 16, 1, 0, tzinfo=PARIS),
        )
        self.assertEqual(len(unchanged["events"]), 2)
        self.assertEqual(by_pair["GBPJPY"]["count"], 2)

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

    def test_message_marks_only_directional_sar_breaks_with_flame(self):
        selected = select_full_alignment_rows([
            row("GBPJPY", 1, 1, 1),
            row("EURJPY", 1, 1, 1),
            index_row("BXY", 1, 1, 1),
            index_row("JXY", -1, -1, -1),
        ])
        by_pair = {item["pair"]: item for item in selected}
        by_pair["GBPJPY"]["sar_break"] = {"last_bar_sar_break_direction": 1}
        by_pair["EURJPY"]["sar_break"] = {"last_bar_sar_break_direction": -1}
        by_pair["BXY"]["sar_break"] = {"last_bar_sar_break_direction": 1}
        by_pair["JXY"]["sar_break"] = {"last_bar_sar_break_direction": -1}
        message = format_full_alignment_message(
            selected,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn("🟢 GBPJPY 🔥", message)
        self.assertIn("🟢 GBP 🔥", message)
        self.assertIn("🔴 JPY 🔥", message)
        self.assertIn("🟢 EURJPY", message)
        self.assertNotIn("🟢 EURJPY 🔥", message)

    def test_message_adds_full_sar_section(self):
        full_rows = select_full_alignment_rows([
            row("GBPJPY", 1, 1, 1),
            index_row("JXY", -1, -1, -1),
        ])
        by_pair = {item["pair"]: item for item in full_rows}
        by_pair["GBPJPY"]["sar_break"] = {"last_bar_sar_break_direction": 1}
        by_pair["JXY"]["sar_break"] = {"last_bar_sar_break_direction": -1}
        full_sar_rows = select_full_sar_rows(full_rows)

        message = format_full_alignment_message(
            full_rows,
            full_sar_rows,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn("⚡ FULL SAR", message)
        self.assertIn("🟢 GBPJPY 🔥", message)
        self.assertIn("🔴 JPY 🔥", message)

    def test_message_adds_full_sar_daily_history(self):
        message = format_full_alignment_message(
            [],
            [],
            {
                "events": [
                    {
                        "pair": "GBPJPY",
                        "asset_type": "PAIR",
                        "direction": 1,
                        "first_seen": "2026-07-16T08:00+02:00",
                        "last_seen": "2026-07-16T10:00+02:00",
                    },
                    {
                        "pair": "JXY",
                        "asset_type": "INDEX",
                        "currency": "JPY",
                        "direction": -1,
                        "first_seen": "2026-07-16T11:00+02:00",
                        "last_seen": "2026-07-16T11:00+02:00",
                    },
                ]
            },
            now=datetime(2026, 7, 16, 12, 0, tzinfo=PARIS),
        )

        self.assertIn("📋 FULL SAR 07H-23H", message)
        self.assertIn("🟢 GBPJPY 🔥 08:00→10:00", message)
        self.assertIn("🔴 JPY 🔥 11:00", message)

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
