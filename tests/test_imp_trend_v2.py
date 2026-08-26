"""Tests pour imp_trend_29pairs_v2.py -- verrouille les corrections apportees
au scoring de V1 (cf. module docstring de imp_trend_29pairs_v2.py): 6 votes
de direction reels (au lieu de 11 inflates), rang non exploite -> flag
`tradable`, moyennes sous MIN_AVG_SAMPLE ignorees, et CURRENCY_INDEX comme 6e
vote de direction a part entiere (point 5 du docstring)."""

import unittest
from types import SimpleNamespace

from imp_trend_29pairs import (
    CURRENCY_INDEX_MIN_SPREAD,
    currency_index_diverges,
    currency_index_vote,
    invalidation_reason,
)
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


def make_index_row(daily_chg):
    """Ligne d'indice devise minimale (forme `compute_asset_score`) dont le
    `strength_score` vaut exactement `abs(daily_chg)`: seul le Weekly est
    "engage" (streak actif), poids 2/6 renormalise = coefficient 1 -- donc
    `signed_strength` vaut exactement `daily_chg`, ce qui rend les tests
    lisibles directement contre les valeurs affichees par PAIRE_CHECK
    (ex. GBP (+1.20) rouge -> daily_chg=-1.20)."""
    return {
        "daily_chg": daily_chg,
        "states": {"W": SimpleNamespace(green_streak=1, red_streak=1)},
        "bias": {"W": 1},
    }


class SelectAlignedPairsV2Tests(unittest.TestCase):
    def test_six_of_six_unanimous_is_tier_1_and_tradable(self):
        # Reproduit USDJPY: les 5 votes d'origine unanimes BULL, plus
        # CURRENCY_INDEX qui confirme (USD +0.90 vert/fort, JPY -0.90
        # rouge/faible, ecart bien au-dessus de CURRENCY_INDEX_MIN_SPREAD) --
        # malgre une moyenne H1 bull negative (celle-ci ne compte plus comme
        # vote, juste comme filtre de qualite -- ici encore confirmee par D1).
        result = make_result("USDJPY")
        index_by_currency = {"USD": make_index_row(0.90), "JPY": make_index_row(-0.90)}
        selected = select_aligned_pairs_v2([result], index_by_currency)

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
        # +0.60 vert/fort, CAD -0.60 rouge/faible) -> compense le vote
        # dissident D1_IMP21 et ramene la paire a 5/6, rang 2.
        result = make_result("AUDCAD", renko=("BULL", "BULL", "BULL"),
                              d1_imp=("BEAR", 10.0, 15, -5.0, 5))
        index_by_currency = {"AUD": make_index_row(0.60), "CAD": make_index_row(-0.60)}
        selected = select_aligned_pairs_v2([result], index_by_currency)

        self.assertEqual(len(selected), 1)
        item = selected[0]
        self.assertEqual(item["direction"], "BULL")
        self.assertEqual(item["confirmations"], 5)
        self.assertEqual(item["rank_tier"], 2)
        self.assertEqual(item["CURRENCY_INDEX"], "BULL")

    def test_currency_index_active_contradiction_vetoes_the_pair(self):
        # Reproduit GBPJPY: les 5 votes d'origine unanimes BULL suffiraient
        # deja seuls (5/5 >= VOTE_THRESHOLD), mais GBP est nettement plus
        # faible que JPY sur les indices devises (spread BEAR, bien au-dela
        # de CURRENCY_INDEX_MIN_SPREAD) -- contradiction active, pas une
        # simple abstention -> la paire est exclue malgre tout.
        result = make_result("GBPJPY")
        index_by_currency = {"GBP": make_index_row(-1.25), "JPY": make_index_row(-0.32)}
        selected = select_aligned_pairs_v2([result], index_by_currency)
        self.assertEqual(selected, [])

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
    d'un PAIRE_CHECK reel): GBP (+1.20) NZD CHF (+0.54) EUR JPY (+0.29) CAD
    (+0.00) rouges, USD AUD (+0.28) verts.

    Deux devises rouges ne sont PAS equivalentes: GBP (+1.20, tres faible) vs
    CAD (+0.00, quasi neutre) donne quand meme GBPCAD/GBPJPY BEAR -- c'est
    l'ecart qui tranche, pas la couleur. A l'inverse AUD (+0.28) et USD
    (+0.26), verts tous les deux mais trop proches, ne donnent aucun point a
    AUDUSD (cf. CURRENCY_INDEX_MIN_SPREAD)."""

    INDEX_BY_CURRENCY = {
        "GBP": make_index_row(-1.20),
        "NZD": make_index_row(-0.59),
        "CHF": make_index_row(-0.54),
        "EUR": make_index_row(-0.43),
        "JPY": make_index_row(-0.29),
        "USD": make_index_row(0.26),
        "AUD": make_index_row(0.28),
        "CAD": make_index_row(-0.00),
    }

    def test_base_green_quote_red_with_wide_enough_spread_confirms_bull(self):
        # AUD (+0.28) vs CHF (-0.54): ecart 0.82, largement au-dessus du seuil.
        self.assertEqual(currency_index_vote("AUDCHF", self.INDEX_BY_CURRENCY), "BULL")

    def test_two_red_currencies_still_vote_by_spread_not_color(self):
        # GBP (-1.20) est bien plus faible que CAD (-0.00) et que JPY (-0.29)
        # malgre la meme couleur rouge des deux cotes -> BEAR sur les deux.
        self.assertEqual(currency_index_vote("GBPCAD", self.INDEX_BY_CURRENCY), "BEAR")
        self.assertEqual(currency_index_vote("GBPJPY", self.INDEX_BY_CURRENCY), "BEAR")

    def test_two_green_currencies_too_close_together_stay_neutral(self):
        # AUD (+0.28) et USD (+0.26): ecart 0.02, sous CURRENCY_INDEX_MIN_SPREAD.
        self.assertEqual(currency_index_vote("AUDUSD", self.INDEX_BY_CURRENCY), "NEUTRAL")

    def test_spread_just_below_threshold_is_neutral_but_at_threshold_confirms(self):
        just_below = {
            "AUD": make_index_row((CURRENCY_INDEX_MIN_SPREAD - 0.01) / 2),
            "USD": make_index_row(-(CURRENCY_INDEX_MIN_SPREAD - 0.01) / 2),
        }
        self.assertEqual(currency_index_vote("AUDUSD", just_below), "NEUTRAL")

        at_threshold = {
            "AUD": make_index_row(CURRENCY_INDEX_MIN_SPREAD / 2),
            "USD": make_index_row(-CURRENCY_INDEX_MIN_SPREAD / 2),
        }
        self.assertEqual(currency_index_vote("AUDUSD", at_threshold), "BULL")

    def test_base_red_quote_green_confirms_bear(self):
        self.assertEqual(currency_index_vote("CHFUSD", self.INDEX_BY_CURRENCY), "BEAR")

    def test_missing_currency_is_neutral(self):
        # XAU n'a pas d'indice devise.
        self.assertEqual(currency_index_vote("XAUUSD", self.INDEX_BY_CURRENCY), "NEUTRAL")


class CurrencyIndexDivergesTests(unittest.TestCase):
    def test_true_only_on_active_contradiction(self):
        self.assertTrue(currency_index_diverges({"CURRENCY_INDEX": "BEAR"}, "BULL"))
        self.assertTrue(currency_index_diverges({"CURRENCY_INDEX": "BULL"}, "BEAR"))

    def test_false_on_neutral_or_agreement(self):
        self.assertFalse(currency_index_diverges({"CURRENCY_INDEX": "NEUTRAL"}, "BULL"))
        self.assertFalse(currency_index_diverges({"CURRENCY_INDEX": "NEUTRAL"}, "BEAR"))
        self.assertFalse(currency_index_diverges({"CURRENCY_INDEX": "BULL"}, "BULL"))
        self.assertFalse(currency_index_diverges({"CURRENCY_INDEX": "BEAR"}, "BEAR"))


class InvalidationReasonTests(unittest.TestCase):
    def test_currency_index_active_contradiction_is_flagged(self):
        # Meme scenario GBPJPY que test_currency_index_active_contradiction_vetoes_the_pair.
        result = make_result("GBPJPY")
        index_by_currency = {"GBP": make_index_row(-1.25), "JPY": make_index_row(-0.32)}
        self.assertEqual(invalidation_reason(result, index_by_currency), "CURRENCY_INDEX_DIVERGENT")

    def test_currency_index_neutral_is_not_flagged_as_divergent(self):
        result = make_result("GBPJPY")
        self.assertNotEqual(invalidation_reason(result), "CURRENCY_INDEX_DIVERGENT")


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
