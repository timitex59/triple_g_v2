import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from renko_full_alignment_29pairs import (
    FOREX_INDEX_ASSETS,
    assets_for_scope,
    fib_extreme_ok,
    format_full_alignment_message,
    full_alignment_direction,
    raw_alignment_score,
    select_full_alignment_rows,
)


PARIS = ZoneInfo("Europe/Paris")


def row(pair, m, w, d, fib_pct):
    return {
        "pair": pair,
        "px": {"M": m, "W": w, "D": d},
        "h1_fib": {"pct_of_range": fib_pct},
        "h1_price": 1.23456,
    }


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
        self.assertEqual(full_alignment_direction(row("GBPJPY", 1, 1, 1, 90.0)), 1)
        self.assertEqual(full_alignment_direction(row("CADCHF", -1, -1, -1, 10.0)), -1)
        self.assertEqual(raw_alignment_score(row("GBPJPY", 1, 1, 1, 90.0)), 100.0)
        self.assertEqual(raw_alignment_score(row("CADCHF", -1, -1, -1, 10.0)), -100.0)

    def test_rejects_inside_or_mixed_alignment(self):
        self.assertEqual(full_alignment_direction(row("AUDJPY", 1, 0, 1, 90.0)), 0)
        self.assertEqual(full_alignment_direction(row("EURJPY", 1, 1, -1, 90.0)), 0)

    def test_fib_extreme_filter_matches_continuation_zone(self):
        self.assertTrue(fib_extreme_ok(row("GBPJPY", 1, 1, 1, 90.0), 1))
        self.assertFalse(fib_extreme_ok(row("GBPJPY", 1, 1, 1, 70.0), 1))
        self.assertTrue(fib_extreme_ok(row("CADCHF", -1, -1, -1, 10.0), -1))
        self.assertFalse(fib_extreme_ok(row("CADCHF", -1, -1, -1, 30.0), -1))

    def test_selects_only_full_alignment_rows(self):
        selected = select_full_alignment_rows([
            row("GBPJPY", 1, 1, 1, 90.0),
            row("AUDJPY", 1, 0, 1, 90.0),
            row("CADCHF", -1, -1, -1, 10.0),
            row("EURJPY", 1, 1, 1, 70.0),
        ])

        self.assertEqual([item["pair"] for item in selected], ["GBPJPY", "CADCHF"])

    def test_message_is_compact(self):
        selected = select_full_alignment_rows([row("GBPJPY", 1, 1, 1, 90.0)])
        message = format_full_alignment_message(
            selected,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn("📊 FULL ALIGNMENT M/W/D", message)
        self.assertIn("🟢 GBPJPY", message)
        self.assertNotIn("+100%", message)
        self.assertNotIn("M+/W+/D+", message)
        self.assertIn("2026-07-16 10:00 Paris", message)


if __name__ == "__main__":
    unittest.main()
