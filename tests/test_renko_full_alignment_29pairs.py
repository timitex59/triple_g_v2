import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from renko_full_alignment_29pairs import (
    FOREX_INDEX_ASSETS,
    attach_premium_currency_profiles,
    assets_for_scope,
    format_full_alignment_message,
    full_alignment_direction,
    mid_alignment_candidate,
    raw_alignment_score,
    select_full_alignment_rows,
    select_index_daily_chg_rows,
    sar_break_state_from_close_sar,
    select_mid_alignment_candidates,
    select_mid_sar_rows,
    update_mid_sar_history,
)


PARIS = ZoneInfo("Europe/Paris")


def row(pair, m, w, d, daily_chg=None):
    return {
        "pair": pair,
        "px": {"M": m, "W": w, "D": d},
        "asset_type": "PAIR",
        "h1_price": 1.23456,
        "daily_chg": daily_chg,
    }


def index_row(pair, m, w, d, daily_chg=None):
    item = row(pair, m, w, d, daily_chg=daily_chg)
    item["asset_type"] = "INDEX"
    item["currency"] = {
        "DXY": "USD",
        "EXY": "EUR",
        "BXY": "GBP",
        "JXY": "JPY",
        "SXY": "CHF",
        "CXY": "CAD",
        "AXY": "AUD",
        "ZXY": "NZD",
    }.get(pair)
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

    def test_selects_index_daily_chg_rows_by_daily_chg_descending(self):
        selected = select_index_daily_chg_rows([
            row("GBPJPY", 1, 1, 1),
            index_row("JXY", -1, -1, -1, daily_chg=-0.44),
            index_row("DXY", 1, -1, 1, daily_chg=0.12),
            index_row("BXY", 1, 1, 1, daily_chg=0.31),
        ], exclude_pairs={"JXY"})

        self.assertEqual([item["pair"] for item in selected], ["BXY", "DXY"])

    def test_full_alignment_rows_are_sorted_by_daily_chg_descending(self):
        selected = select_full_alignment_rows([
            row("USDJPY", 1, 1, 1, daily_chg=-0.01),
            row("AUDJPY", 1, 1, 1, daily_chg=0.01),
            row("EURJPY", 1, 1, 1, daily_chg=0.02),
            row("NZDJPY", 1, 1, 1, daily_chg=-0.02),
            index_row("JXY", -1, -1, -1, daily_chg=-0.41),
            index_row("DXY", 1, 1, 1, daily_chg=0.20),
        ])

        self.assertEqual(
            [item["pair"] for item in selected],
            ["EURJPY", "AUDJPY", "USDJPY", "NZDJPY", "DXY", "JXY"],
        )

    def test_premium_currency_profile_marks_strong_weak_full_alignment_pairs(self):
        rows = [
            row("USDJPY", 1, 1, 1, daily_chg=0.01),
            row("USDCAD", 1, 1, 1, daily_chg=0.01),
            row("NZDJPY", 1, 1, 1, daily_chg=-0.03),
            index_row("DXY", 1, -1, 1, daily_chg=0.20),
            index_row("JXY", -1, -1, -1, daily_chg=-0.41),
            index_row("CXY", -1, 1, -1, daily_chg=-0.20),
            index_row("ZXY", -1, 1, -1, daily_chg=-0.37),
        ]
        selected = attach_premium_currency_profiles(select_full_alignment_rows(rows), rows)
        by_pair = {item["pair"]: item for item in selected}

        self.assertTrue(by_pair["USDJPY"]["premium_currency_profile"])
        self.assertFalse(by_pair["USDCAD"]["premium_currency_profile"])
        self.assertFalse(by_pair["NZDJPY"]["premium_currency_profile"])
        self.assertFalse(by_pair["JXY"]["premium_currency_profile"])

    def test_detects_mid_alignment_candidates_with_at_least_two_timeframes(self):
        self.assertEqual(mid_alignment_candidate(row("EURUSD", 1, -1, 1)), (1, "D/M"))
        self.assertEqual(mid_alignment_candidate(row("CADCHF", -1, 1, -1)), (-1, "D/M"))
        self.assertEqual(mid_alignment_candidate(row("AUDJPY", -1, 1, 1)), (1, "D/W"))
        self.assertEqual(mid_alignment_candidate(row("NZDCHF", 1, -1, -1)), (-1, "D/W"))
        self.assertEqual(mid_alignment_candidate(row("GBPUSD", 1, 1, -1)), (1, "W/M"))
        self.assertEqual(mid_alignment_candidate(row("USDCHF", -1, -1, 1)), (-1, "W/M"))
        self.assertEqual(mid_alignment_candidate(row("GBPJPY", 1, 1, 1)), (1, "M/W/D"))
        self.assertEqual(mid_alignment_candidate(row("CHFJPY", -1, -1, -1)), (-1, "M/W/D"))
        self.assertEqual(mid_alignment_candidate(row("EURJPY", 1, 0, -1)), (0, ""))

    def test_mid_sar_keeps_only_mid_alignment_directional_sar_breaks(self):
        candidates = select_mid_alignment_candidates([
            row("GBPJPY", 1, 1, 1),
            row("EURUSD", 1, -1, 1),
            row("EURJPY", 1, -1, 1),
            row("CADCHF", -1, 1, -1),
            index_row("JXY", 1, -1, -1),
        ])
        by_pair = {item["pair"]: item for item in candidates}
        by_pair["GBPJPY"]["sar_break"] = {"last_bar_sar_break_direction": 1}
        by_pair["EURUSD"]["sar_break"] = {"last_bar_sar_break_direction": 1}
        by_pair["EURJPY"]["sar_break"] = {"last_bar_sar_break_direction": -1}
        by_pair["CADCHF"]["sar_break"] = {"last_bar_sar_break_direction": -1}
        by_pair["JXY"]["sar_break"] = {"last_bar_sar_break_direction": -1}

        mid_sar_rows = select_mid_sar_rows(candidates)

        self.assertEqual([item["pair"] for item in mid_sar_rows], ["GBPJPY", "EURUSD", "CADCHF", "JXY"])

    def test_mid_sar_history_tracks_daily_window_without_duplicates(self):
        candidates = select_mid_alignment_candidates([
            row("EURUSD", 1, -1, 1),
            index_row("JXY", 1, -1, -1),
        ])
        for item in candidates:
            item["sar_break"] = {
                "last_bar_sar_break_direction": 1 if item["pair"] == "EURUSD" else -1
            }
        mid_sar_rows = select_mid_sar_rows(candidates)

        state, today = update_mid_sar_history(
            {},
            mid_sar_rows,
            datetime(2026, 7, 16, 8, 0, tzinfo=PARIS),
        )
        state, today = update_mid_sar_history(
            state,
            mid_sar_rows,
            datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertEqual(len(today["events"]), 2)
        by_pair = {event["pair"]: event for event in today["events"]}
        self.assertEqual(by_pair["EURUSD"]["count"], 2)
        self.assertEqual(by_pair["EURUSD"]["first_seen"], "2026-07-16T08:00+02:00")
        self.assertEqual(by_pair["EURUSD"]["last_seen"], "2026-07-16T10:00+02:00")
        self.assertEqual(by_pair["EURUSD"]["tf_pairs"], ["D/M"])
        self.assertEqual(by_pair["JXY"]["currency"], "JPY")
        self.assertEqual(by_pair["JXY"]["tf_pairs"], ["D/W"])

        state, unchanged = update_mid_sar_history(
            state,
            mid_sar_rows,
            datetime(2026, 7, 16, 1, 0, tzinfo=PARIS),
        )
        self.assertEqual(len(unchanged["events"]), 2)
        self.assertEqual(by_pair["EURUSD"]["count"], 2)

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

    def test_message_does_not_mark_full_alignment_sar_breaks_with_flame(self):
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

        self.assertIn("🟢 GBPJPY", message)
        self.assertIn("🟢 GBP", message)
        self.assertIn("🔴 JPY", message)
        self.assertNotIn("🔥", message)

    def test_message_adds_daily_chg_to_full_alignment_rows_and_lists_other_indices(self):
        rows = [
            row("AUDJPY", 1, 1, 1, daily_chg=0.88),
            row("NZDJPY", 1, 1, 1, daily_chg=-0.03),
            row("USDCAD", 1, 1, 1, daily_chg=-0.01),
            row("USDJPY", 1, 1, 1, daily_chg=-0.001),
            index_row("JXY", -1, -1, -1, daily_chg=-0.44),
        ]
        selected = attach_premium_currency_profiles(select_full_alignment_rows(rows), rows)
        other_indices = select_index_daily_chg_rows([
            index_row("DXY", 1, -1, 1, daily_chg=0.12),
            index_row("EXY", 1, 1, -1, daily_chg=-0.05),
            index_row("JXY", -1, -1, -1, daily_chg=-0.44),
        ], exclude_pairs={"JXY"})

        message = format_full_alignment_message(
            selected,
            index_daily_chg_rows=other_indices,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn("🟢 AUDJPY (+0.88%)", message)
        self.assertNotIn("🟢 AUDJPY (+0.88%) ⚠️", message)
        self.assertIn("🟢 NZDJPY (-0.03%) ⚠️", message)
        self.assertIn("🟢 USDCAD (-0.01%) ⚠️", message)
        self.assertIn("🟢 USDJPY (-0.00%) ⚠️", message)
        self.assertNotIn("🌸🌸", message)
        self.assertIn("🔴 JPY (-0.44%)", message)
        self.assertIn("💱 AUTRES INDEX CHG%D", message)
        self.assertIn("🟢 USD +0.12%", message)
        self.assertIn("🔴 EUR -0.05%", message)
        self.assertNotIn("JPY -0.44%", message)

    def test_message_marks_premium_currency_profile_with_double_flower(self):
        rows = [
            row("USDJPY", 1, 1, 1, daily_chg=0.01),
            row("USDCAD", 1, 1, 1, daily_chg=0.01),
            index_row("DXY", 1, -1, 1, daily_chg=0.20),
            index_row("JXY", -1, -1, -1, daily_chg=-0.41),
            index_row("CXY", -1, 1, -1, daily_chg=-0.20),
        ]
        selected = attach_premium_currency_profiles(select_full_alignment_rows(rows), rows)

        message = format_full_alignment_message(
            selected,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn("🟢 USDJPY (+0.01%) 🌸🌸", message)
        self.assertNotIn("🟢 USDCAD (+0.01%) 🌸🌸", message)
        self.assertIn("🔴 JPY (-0.41%)", message)

    def test_message_adds_mid_sar_section(self):
        full_rows = select_full_alignment_rows([
            row("GBPJPY", 1, 1, 1),
            index_row("JXY", -1, -1, -1),
        ])
        mid_rows = select_mid_alignment_candidates([
            row("GBPJPY", 1, 1, 1),
            row("EURUSD", 1, -1, 1),
            index_row("JXY", 1, -1, -1),
        ])
        by_pair = {item["pair"]: item for item in mid_rows}
        by_pair["GBPJPY"]["sar_break"] = {"last_bar_sar_break_direction": 1}
        by_pair["EURUSD"]["sar_break"] = {"last_bar_sar_break_direction": 1}
        by_pair["JXY"]["sar_break"] = {"last_bar_sar_break_direction": -1}
        mid_sar_rows = select_mid_sar_rows(mid_rows)

        message = format_full_alignment_message(
            full_rows,
            mid_sar_rows,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn("⚡ MID SAR", message)
        self.assertIn("🟢 GBPJPY 🔥 M/W/D", message)
        self.assertIn("🟢 EURUSD 🔥 D/M", message)
        self.assertIn("🔴 JPY 🔥 D/W", message)

    def test_message_adds_mid_sar_daily_history(self):
        message = format_full_alignment_message(
            [],
            [],
            {
                "events": [
                    {
                        "pair": "EURUSD",
                        "asset_type": "PAIR",
                        "direction": 1,
                        "tf_pairs": ["D/M"],
                        "first_seen": "2026-07-16T08:00+02:00",
                        "last_seen": "2026-07-16T10:00+02:00",
                    },
                    {
                        "pair": "JXY",
                        "asset_type": "INDEX",
                        "currency": "JPY",
                        "direction": -1,
                        "tf_pairs": ["D/W"],
                        "first_seen": "2026-07-16T11:00+02:00",
                        "last_seen": "2026-07-16T11:00+02:00",
                    },
                ]
            },
            now=datetime(2026, 7, 16, 12, 0, tzinfo=PARIS),
        )

        self.assertIn("📋 MID SAR 07H-23H", message)
        self.assertIn("🟢 EURUSD 🔥 D/M 08:00→10:00", message)
        self.assertIn("🔴 JPY 🔥 D/W 11:00", message)

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
