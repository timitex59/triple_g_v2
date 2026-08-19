import unittest

from paire_check import (
    DEFAULT_PAIRS,
    build_message,
    needed_currencies,
    needed_helper_pairs,
)


class PaireCheckTests(unittest.TestCase):
    def test_needed_currencies_unions_base_and_quote_of_every_pair(self):
        self.assertEqual(
            needed_currencies(["EURUSD", "CHFJPY", "USDJPY"]),
            {"EUR", "USD", "CHF", "JPY"},
        )

    def test_needed_currencies_ignores_unrecognized_pairs(self):
        self.assertEqual(needed_currencies(["EURUSD", "NOTAPAIR"]), {"EUR", "USD"})
        self.assertEqual(needed_currencies([]), set())

    def test_needed_helper_pairs_covers_all_7_pairs_of_each_currency(self):
        helper_pairs = needed_helper_pairs({"EUR"})

        self.assertEqual(len(helper_pairs), 7)
        self.assertIn("EURUSD", helper_pairs)
        self.assertIn("EURJPY", helper_pairs)
        self.assertNotIn("AUDNZD", helper_pairs)  # ni EUR ni la devise cherchee

    def test_needed_helper_pairs_dedupes_pairs_shared_by_two_currencies(self):
        # CHFJPY implique CHF et JPY a la fois: ne doit apparaitre qu'une fois.
        helper_pairs = needed_helper_pairs({"CHF", "JPY"})

        self.assertEqual(helper_pairs.count("CHFJPY"), 1)

    def test_needed_helper_pairs_for_default_pairs_matches_manual_union(self):
        # EURUSD, CHFJPY, USDJPY -> devises EUR/USD/CHF/JPY -> 22 des 28 paires
        # (les 6 paires ne touchant que AUD/CAD/GBP/NZD entre elles sont exclues).
        helper_pairs = needed_helper_pairs(needed_currencies(DEFAULT_PAIRS))

        self.assertEqual(len(helper_pairs), 22)
        for excluded in ("AUDCAD", "AUDNZD", "GBPAUD", "GBPCAD", "GBPNZD", "NZDCAD"):
            self.assertNotIn(excluded, helper_pairs)

    def test_build_message_lists_each_pair_check_and_flags_content(self):
        rows_by_pair = {
            "EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"},
            "GBPUSD": {"pair": "GBPUSD", "asset_type": "PAIR"},
        }
        price_trends = {
            "EURUSD": {"pips_vs_06h": 20.0},
            "GBPUSD": {"pips_vs_06h": -5.0},
        }
        import datetime as dt
        from zoneinfo import ZoneInfo
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=ZoneInfo("Europe/Paris"))

        message, has_content = build_message(
            ["EURUSD"], rows_by_pair, price_trends, {}, now,
        )

        self.assertTrue(has_content)
        self.assertIn("📐 PAIRE_CHECK", message)
        self.assertIn("🧭 EURUSD CHECK", message)
        self.assertIn("06h 🟢", message)
        self.assertIn("2026-07-16 10:00 Paris", message)

    def test_build_message_flags_no_content_when_no_pair_scores(self):
        import datetime as dt
        from zoneinfo import ZoneInfo
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=ZoneInfo("Europe/Paris"))

        message, has_content = build_message(["EURUSD"], {}, {}, {}, now)

        self.assertFalse(has_content)
        self.assertIn("📐 PAIRE_CHECK", message)
        self.assertNotIn("🧭 EURUSD CHECK", message)


if __name__ == "__main__":
    unittest.main()
