import datetime as dt
import unittest
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from paire_check import (
    DEFAULT_PAIRS,
    best_pair_lines,
    best_pair_name,
    build_message,
    currency_trend_balls,
    index_chg_lines,
    needed_currencies,
    needed_helper_pairs,
    pair_check_compact_line,
)

PARIS = ZoneInfo("Europe/Paris")


def index_row(pair, currency, m, w, d, daily_chg):
    """Ligne d'indice factice, meme structure que le helper equivalent de
    tests/test_renko_full_alignment_29pairs.py."""
    px = {"M": m, "W": w, "D": d}
    bias = dict(px)
    states = {
        tf: SimpleNamespace(
            green_streak=1 if bias[tf] == 1 else 0,
            red_streak=1 if bias[tf] == -1 else 0,
        )
        for tf in ("M", "W", "D")
    }
    return {
        "pair": pair, "asset_type": "INDEX", "currency": currency,
        "px": px, "bias": bias, "states": states, "daily_chg": daily_chg,
        "live_price": 1.0,
    }


def make_currency_row(
    renko=("BULL", "BULL", "BULL"),
    d1=("BULL", 10.0, 5.0, -2.0),
    h1=("BEAR", -3.0, 0.0, 1.0),
):
    """Ligne factice `compute_currency_imp` (une devise): `d1`/`h1` =
    (imp_status, all_avg_pips, bull_avg_pips, bear_avg_pips)."""
    m, w, d = renko

    def chart(status, all_pips, bull_pips, bear_pips):
        return {
            "imp_status": status,
            "all_avg_pips": all_pips,
            "bull_avg_pips": bull_pips,
            "bear_avg_pips": bear_pips,
        }

    return {
        "pair": "DXY",
        "currency": "USD",
        "renko": {"M": {"status": m}, "W": {"status": w}, "D": {"status": d}},
        "D1": chart(*d1),
        "H1": chart(*h1),
    }


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
        # EURUSD, EURJPY, USDJPY, CHFJPY -> devises EUR/USD/CHF/JPY -> 22 des 28
        # paires (les 6 paires ne touchant que AUD/CAD/GBP/NZD entre elles sont
        # exclues).
        helper_pairs = needed_helper_pairs(needed_currencies(DEFAULT_PAIRS))

        self.assertEqual(len(helper_pairs), 22)
        for excluded in ("AUDCAD", "AUDNZD", "GBPAUD", "GBPCAD", "GBPNZD", "NZDCAD"):
            self.assertNotIn(excluded, helper_pairs)

    def test_pair_check_compact_line_concatenates_icons_with_no_label(self):
        rows_by_pair = {
            "EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"},
            "GBPUSD": {"pair": "GBPUSD", "asset_type": "PAIR"},
        }
        price_trends = {
            "EURUSD": {"pips_vs_06h": 20.0, "pips_vs_previous_run": 3.0},
            "GBPUSD": {"pips_vs_06h": -5.0, "pips_vs_previous_run": -1.0},
        }

        line = pair_check_compact_line("EURUSD", rows_by_pair, price_trends, {})

        self.assertEqual(line, "EURUSD 🟢🟢")

    def test_pair_check_compact_line_none_without_any_exploitable_signal(self):
        self.assertIsNone(pair_check_compact_line("EURUSD", {}, {}, {}))

    def test_index_chg_lines_matches_full_momentum_rendering(self):
        rows = [
            index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
            index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
        ]

        lines = index_chg_lines(rows)

        self.assertEqual(lines[0], "💱 INDEX CHG%D")
        self.assertTrue(lines[1].startswith("🟢 EUR ("))  # score plus fort en tete
        self.assertTrue(lines[2].startswith("🔴 USD ("))

    def test_index_chg_lines_empty_without_index_rows(self):
        self.assertEqual(index_chg_lines([]), [])

    def test_currency_trend_balls_orders_renko_then_d1_then_h1(self):
        # Renko M+ W+ D- ; D1 BULL avec all/bull positifs, bear negatif ;
        # H1 BEAR avec all negatif, bull nul (NEUTRAL), bear positif.
        row = make_currency_row(
            renko=("BULL", "BULL", "BEAR"),
            d1=("BULL", 10.0, 5.0, -2.0),
            h1=("BEAR", -3.0, 0.0, 1.0),
        )
        self.assertEqual(currency_trend_balls(row), "🟢🟢🔴 🟢🟢🟢🔴 🔴🔴⚪🟢")

    def test_index_chg_lines_appends_currency_trend_balls_when_provided(self):
        rows = [
            index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
            index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
        ]
        imp_by_currency = {"EUR": make_currency_row(renko=("BULL", "BULL", "BULL"))}

        with_balls = index_chg_lines(rows, imp_by_currency)
        without_balls = index_chg_lines(rows)

        eur_line = next(line for line in with_balls if line.startswith("🟢 EUR ("))
        usd_line = next(line for line in with_balls if line.startswith("🔴 USD ("))
        base_usd_line = next(line for line in without_balls if line.startswith("🔴 USD ("))

        self.assertIn("🟢🟢🟢", eur_line)  # boules ajoutees pour EUR
        self.assertEqual(usd_line, base_usd_line)  # USD absent de imp_by_currency -> inchangee

    def test_index_chg_lines_without_imp_by_currency_is_unchanged(self):
        rows = [index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57)]
        self.assertEqual(index_chg_lines(rows), index_chg_lines(rows, None))

    def test_best_pair_name_pairs_top_red_with_top_green(self):
        # Meme alignement M/W/D (1,1,1) partout -> le score n'est ordonne que
        # par |daily_chg|: rouge le plus fort = JPY, vert le plus fort = USD,
        # meme si ce ne sont pas les plus extremes en valeur absolue globale.
        rows = [
            index_row("JYX", "JPY", 1, 1, 1, daily_chg=-1.66),
            index_row("AUX", "AUD", 1, 1, 1, daily_chg=-0.43),
            index_row("USX", "USD", 1, 1, 1, daily_chg=0.28),
            index_row("GBX", "GBP", 1, 1, 1, daily_chg=0.10),
        ]

        self.assertEqual(best_pair_name(rows), "USDJPY")

    def test_best_pair_name_none_without_both_colors(self):
        rows = [index_row("USX", "USD", 1, 1, 1, daily_chg=0.28)]

        self.assertIsNone(best_pair_name(rows))

    def test_best_pair_name_none_without_rows(self):
        self.assertIsNone(best_pair_name([]))

    def test_best_pair_lines_renders_the_same_billes_as_a_paires_line(self):
        rows_by_pair = {"EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"}}
        price_trends = {"EURUSD": {"pips_vs_06h": 20.0}}

        lines = best_pair_lines("EURUSD", rows_by_pair, price_trends, {})

        self.assertEqual(
            lines, ["🏆 BEST PAIRE", pair_check_compact_line(
                "EURUSD", rows_by_pair, price_trends, {},
            )],
        )

    def test_best_pair_lines_empty_without_a_pair_name(self):
        self.assertEqual(best_pair_lines(None, {}, {}, {}), [])

    def test_best_pair_lines_empty_without_exploitable_signal(self):
        self.assertEqual(best_pair_lines("EURUSD", {}, {}, {}), [])

    def test_build_message_lists_one_compact_line_per_pair_under_a_paires_label(self):
        rows_by_pair = {
            "EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"},
            "GBPUSD": {"pair": "GBPUSD", "asset_type": "PAIR"},
        }
        price_trends = {
            "EURUSD": {"pips_vs_06h": 20.0},
            "GBPUSD": {"pips_vs_06h": -5.0},
        }
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, has_content = build_message(
            ["EURUSD"], rows_by_pair, price_trends, {}, now,
        )

        self.assertTrue(has_content)
        self.assertEqual(message, "\n".join([
            "📐 PAIRE_CHECK", "",
            "PAIRES",
            "EURUSD 🟢",
            "", "⏰ 2026-07-16 10:00 Paris",
        ]))

    def test_build_message_adds_index_chg_block_before_paires(self):
        rows_by_pair = {"EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"}}
        price_trends = {"EURUSD": {"pips_vs_06h": 20.0}}
        index_by_currency = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
            "USD": index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
        }
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, has_content = build_message(
            ["EURUSD"], rows_by_pair, price_trends, index_by_currency, now,
        )

        self.assertTrue(has_content)
        self.assertIn("💱 INDEX CHG%D", message)
        self.assertIn("🏆 BEST PAIRE", message)
        self.assertIn("PAIRES", message)
        self.assertLess(message.index("💱 INDEX CHG%D"), message.index("🏆 BEST PAIRE"))
        self.assertLess(message.index("🏆 BEST PAIRE"), message.index("PAIRES"))
        # EUR verte, USD rouge ici -> BEST PAIRE = EURUSD, memes billes que la
        # ligne PAIRES (qui suit) puisque c'est la meme paire dans ce test.
        after_paires = message[message.index("PAIRES"):]
        self.assertIn("EURUSD 🟢", after_paires)

    def test_build_message_forwards_imp_by_currency_to_index_chg_lines(self):
        rows_by_pair = {"EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"}}
        price_trends = {"EURUSD": {"pips_vs_06h": 20.0}}
        index_by_currency = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
            "USD": index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
        }
        imp_by_currency = {"EUR": make_currency_row(renko=("BULL", "BULL", "BULL"))}
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, _ = build_message(
            ["EURUSD"], rows_by_pair, price_trends, index_by_currency, now, imp_by_currency,
        )

        self.assertIn("🟢🟢🟢", message)

    def test_build_message_flags_no_content_when_no_pair_scores(self):
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, has_content = build_message(["EURUSD"], {}, {}, {}, now)

        self.assertFalse(has_content)
        self.assertIn("📐 PAIRE_CHECK", message)
        self.assertNotIn("PAIRES", message)
        self.assertNotIn("EURUSD", message)


if __name__ == "__main__":
    unittest.main()
