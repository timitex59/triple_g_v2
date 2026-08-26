"""Tests pour imp_trend_29pairs_v2.py -- verrouille les corrections apportees
au scoring de V1 (cf. module docstring de imp_trend_29pairs_v2.py): 6 votes
de direction reels (au lieu de 11 inflates), rang non exploite -> flag
`tradable`, moyennes sous MIN_AVG_SAMPLE ignorees, et CURRENCY_INDEX comme 6e
vote de direction a part entiere (point 5 du docstring)."""

import unittest

from imp_trend_29pairs import currency_index_vote
from imp_trend_29pairs_v2 import (
    MIN_AVG_SAMPLE,
    currency_exposure_lines,
    select_aligned_pairs_v2,
)


def make_result(
    pair,
    renko=("BULL", "BULL", "BULL"),
    d1_imp=("BULL", 10.0, 15, -5.0, 5),
    h1_imp=("BULL", 10.0, 15, -5.0, 5),
):
    """`renko` = (M, W, D). `d1_imp`/`h1_imp` = (imp_status, bull_avg_pips,
    bull_avg_n, bear_avg_pips, bear_avg_n)."""
    m, w, d = renko

    def chart(status, bull_pips, bull_n, bear_pips, bear_n):
        return {
            "imp_status": status,
            "bull_avg_pips": bull_pips,
            "bull_avg_n": bull_n,
            "bear_avg_pips": bear_pips,
            "bear_avg_n": bear_n,
            "all_avg_pips": bull_pips,
        }

    return {
        "pair": pair,
        "renko": {
            "M": {"status": m}, "W": {"status": w}, "D": {"status": d},
        },
        "D1": chart(*d1_imp),
        "H1": chart(*h1_imp),
    }


class SelectAlignedPairsV2Tests(unittest.TestCase):
    def test_six_of_six_unanimous_is_tier_1_and_tradable(self):
        # Reproduit USDJPY: les 5 votes d'origine unanimes BULL, plus
        # CURRENCY_INDEX qui confirme (USD vert/fort, JPY rouge/faible) --
        # malgre une moyenne H1 bull negative (celle-ci ne compte plus comme
        # vote, juste comme filtre de qualite -- ici encore confirmee par D1).
        result = make_result("USDJPY")
        selected = select_aligned_pairs_v2([result], {"USD": "🟢", "JPY": "🔴"})

        self.assertEqual(len(selected), 1)
        item = selected[0]
        self.assertEqual(item["direction"], "BULL")
        self.assertEqual(item["confirmations"], 6)
        self.assertEqual(item["rank_tier"], 1)
        self.assertTrue(item["tradable"])

    def test_five_of_six_is_tier_2_when_currency_index_abstains(self):
        # Les 5 votes d'origine unanimes BULL, mais aucune donnee devise
        # fournie -> CURRENCY_INDEX reste NEUTRAL (ni pour ni contre): la
        # paire est quand meme retenue (5/6 >= VOTE_THRESHOLD) mais en rang 2,
        # pas 1 -- CURRENCY_INDEX n'a pas confirme.
        result = make_result("USDJPY")
        selected = select_aligned_pairs_v2([result])

        self.assertEqual(len(selected), 1)
        item = selected[0]
        self.assertEqual(item["confirmations"], 5)
        self.assertEqual(item["rank_tier"], 2)
        self.assertEqual(item["CURRENCY_INDEX"], "NEUTRAL")

    def test_one_dissenting_vote_without_currency_confirmation_is_excluded(self):
        # D1_IMP21 dissident (BEAR) fait tomber a 4 votes BULL sur 6 sans
        # donnee devise -- ne passe plus le seuil (contrairement a l'ancien
        # seuil 4/5): la paire n'est plus retenue du tout.
        result = make_result("AUDCAD", renko=("BULL", "BULL", "BULL"),
                              d1_imp=("BEAR", 10.0, 15, -5.0, 5))
        selected = select_aligned_pairs_v2([result])
        self.assertEqual(selected, [])

    def test_one_dissenting_vote_is_tier_2_when_currency_index_confirms(self):
        # Meme paire que ci-dessus, mais CURRENCY_INDEX confirme BULL (AUD
        # vert/fort, CAD rouge/faible) -> compense le vote dissident D1_IMP21
        # et ramene la paire a 5/6, rang 2.
        result = make_result("AUDCAD", renko=("BULL", "BULL", "BULL"),
                              d1_imp=("BEAR", 10.0, 15, -5.0, 5))
        selected = select_aligned_pairs_v2([result], {"AUD": "🟢", "CAD": "🔴"})

        self.assertEqual(len(selected), 1)
        item = selected[0]
        self.assertEqual(item["direction"], "BULL")
        self.assertEqual(item["confirmations"], 5)
        self.assertEqual(item["rank_tier"], 2)
        self.assertEqual(item["CURRENCY_INDEX"], "BULL")

    def test_renko_d_must_match_direction(self):
        # 4/5 en faveur de BULL mais RENKO_D est BEAR -> le garde-fou l'exclut.
        result = make_result("EURUSD", renko=("BULL", "BULL", "BEAR"))
        selected = select_aligned_pairs_v2([result])
        self.assertEqual(selected, [])

    def test_neutral_imp21_excludes_the_pair(self):
        result = make_result("GBPUSD", d1_imp=("NEUTRAL", 10.0, 15, -5.0, 5))
        selected = select_aligned_pairs_v2([result])
        self.assertEqual(selected, [])

    def test_quality_gate_marks_untradable_when_both_averages_contradict(self):
        # CADJPY: score parfait mais D1 et H1 bull_avg_pips negatifs
        # (echantillon suffisant) -> aucune moyenne ne confirme BULL.
        result = make_result(
            "CADJPY",
            d1_imp=("BULL", -10.0, 15, 5.0, 5),
            h1_imp=("BULL", -8.0, 12, 3.0, 5),
        )
        selected = select_aligned_pairs_v2([result])

        self.assertEqual(len(selected), 1)
        item = selected[0]
        self.assertFalse(item["tradable"])
        self.assertIn("MAIS QUALITE", item["rank_reason"])
        self.assertEqual(item["quality_applicable"], 2)
        self.assertEqual(item["quality_confirmations"], 0)

    def test_small_sample_average_is_excluded_from_quality_gate(self):
        # bull_avg_n < MIN_AVG_SAMPLE sur les 2 graphiques -> aucun filtre
        # applicable, donc pas de rejet malgre des moyennes negatives.
        self.assertLess(2, MIN_AVG_SAMPLE)
        result = make_result(
            "NZDJPY",
            d1_imp=("BULL", -10.0, 2, 5.0, 5),
            h1_imp=("BULL", -8.0, 1, 3.0, 5),
        )
        selected = select_aligned_pairs_v2([result])

        self.assertEqual(len(selected), 1)
        item = selected[0]
        self.assertTrue(item["tradable"])
        self.assertEqual(item["quality_applicable"], 0)
        self.assertIn("qualite n/a", item["rank_reason"])

    def test_one_of_two_quality_confirmations_keeps_it_tradable(self):
        # Une seule des deux moyennes confirme (echantillon suffisant sur
        # les deux) -> tradable reste True, seul un echec total (0/N) exclut.
        result = make_result(
            "GBPJPY",
            d1_imp=("BULL", 10.0, 15, -5.0, 5),   # confirme (positif)
            h1_imp=("BULL", -8.0, 12, 3.0, 5),    # contredit (negatif)
        )
        selected = select_aligned_pairs_v2([result])

        item = selected[0]
        self.assertTrue(item["tradable"])
        self.assertEqual(item["quality_confirmations"], 1)
        self.assertEqual(item["quality_applicable"], 2)


class CurrencyIndexVoteTests(unittest.TestCase):
    """Reproduit l'exemple travaille par l'utilisateur (colonne INDEX CHG%D
    d'un PAIRE_CHECK reel): GBP/NZD/CHF/EUR/JPY/CAD rouges, USD/AUD verts.
    AUDCHF (BULL) est confirme -- AUD vert, CHF rouge -- GBPJPY et NZDJPY
    (tous deux BULL) ne le sont pas, les deux devises etant rouges."""

    ICONS = {
        "GBP": "🔴", "NZD": "🔴", "CHF": "🔴", "EUR": "🔴",
        "JPY": "🔴", "USD": "🟢", "AUD": "🟢", "CAD": "🔴",
    }

    def test_base_green_quote_red_confirms_bull(self):
        self.assertEqual(currency_index_vote("AUDCHF", self.ICONS), "BULL")

    def test_both_red_does_not_confirm_bull(self):
        self.assertEqual(currency_index_vote("GBPJPY", self.ICONS), "NEUTRAL")
        self.assertEqual(currency_index_vote("NZDJPY", self.ICONS), "NEUTRAL")

    def test_base_red_quote_green_confirms_bear(self):
        self.assertEqual(currency_index_vote("CHFUSD", self.ICONS), "BEAR")

    def test_missing_currency_is_neutral(self):
        # XAU n'a pas d'indice devise.
        self.assertEqual(currency_index_vote("XAUUSD", self.ICONS), "NEUTRAL")


class CurrencyExposureLinesTests(unittest.TestCase):
    def test_aggregates_bought_and_sold_sides_per_currency(self):
        selected = [
            {"pair": "AUDCAD", "direction": "BULL"},   # AUD achetee, CAD vendue
            {"pair": "AUDCHF", "direction": "BULL"},   # AUD achetee, CHF vendue
            {"pair": "EURAUD", "direction": "BEAR"},   # AUD achetee (quote, BEAR), EUR vendue
        ]
        lines = currency_exposure_lines(selected)

        self.assertIn("📊 EXPOSITION DEVISES", lines)
        aud_line = next(line for line in lines if line.startswith("AUD:"))
        self.assertIn("3 positions (achetée)", aud_line)

    def test_empty_selection_yields_no_lines(self):
        self.assertEqual(currency_exposure_lines([]), [])

    def test_mixed_bought_and_sold_shown_together(self):
        selected = [
            {"pair": "EURUSD", "direction": "BULL"},  # EUR achetee, USD vendue
            {"pair": "USDCHF", "direction": "BULL"},  # USD achetee, CHF vendue
        ]
        lines = currency_exposure_lines(selected)
        usd_line = next(line for line in lines if line.startswith("USD:"))
        self.assertIn("achetée: 1 / vendue: 1", usd_line)


if __name__ == "__main__":
    unittest.main()
