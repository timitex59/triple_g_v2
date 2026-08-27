"""Tests pour imp_trend_29pairs_v2.py -- verrouille les corrections apportees
au scoring de V1 (cf. module docstring de imp_trend_29pairs_v2.py): 6 votes
de direction reels (au lieu de 11 inflates), rang non exploite -> flag
`tradable`, moyennes sous MIN_AVG_SAMPLE ignorees, et CURRENCY_INDEX comme 6e
vote de direction a part entiere (point 5 du docstring)."""

import unittest
from types import SimpleNamespace

from imp_trend_29pairs import (
    CURRENCY_DAILY_MOVE_MIN_MAGNITUDE,
    CURRENCY_INDEX_MIN_SPREAD,
    currency_consensus_status,
    currency_daily_status,
    currency_diverges_from_its_own_day,
    currency_index_diverges,
    currency_index_vote,
    currency_trend_confirms,
    invalidation_reason,
    pair_touches_a_divergent_currency,
    select_aligned_pairs,
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


def make_imp_row(d1_status="NEUTRAL", h1_status="NEUTRAL", renko=("NEUTRAL", "NEUTRAL", "NEUTRAL")):
    """Ligne minimale (forme `compute_currency_imp`) pour `currency_trend_confirms`
    (seuls les imp_status D1/H1 lui importent) et pour `currency_consensus_status`
    (a besoin en plus du Renko M/W/D, cf. `screening_votes`) -- Renko NEUTRAL
    par defaut pour ne jamais faire pencher un consensus au-dela de ce qu'un
    test demande explicitement via `renko`."""
    m, w, d = renko

    def chart(status):
        return {"imp_status": status, "all_avg_pips": None, "bull_avg_pips": None, "bear_avg_pips": None}

    return {
        "pair": "XXX",  # code devise factice: len() != 6 -> CURRENCY_INDEX reste NEUTRAL, sans importance ici
        "renko": {"M": {"status": m}, "W": {"status": w}, "D": {"status": d}},
        "D1": chart(d1_status),
        "H1": chart(h1_status),
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

    def test_currency_daily_consensus_contradiction_marks_it_non_tradable(self):
        # AUDCHF autrement parfaite (5/5, moyennes positives), mais AUD a une
        # contradiction jour/consensus (-0.28% aujourd'hui alors que son
        # consensus structurel est BULL, cf. CurrencyDivergesFromItsOwnDayTests)
        # -> tradable=False, meme si le score de la paire elle-meme suffit.
        result = make_result("AUDCHF")
        index_by_currency = {"AUD": make_index_row(-0.28)}
        imp_by_currency = {"AUD": make_result("AUD")}  # consensus BULL par defaut
        selected = select_aligned_pairs_v2([result], index_by_currency, imp_by_currency)

        self.assertEqual(len(selected), 1)
        item = selected[0]
        self.assertFalse(item["tradable"])
        self.assertIn("DEVISE JOUR/CONSENSUS CONTRADICTOIRE", item["rank_reason"])

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

    def test_currency_index_downgraded_to_neutral_by_imp_by_currency(self):
        # Spread confirmerait BULL (AUD +0.60 vs CHF -0.60), mais aucune des
        # deux devises ne trend elle-meme dans le sens attendu -> retrograde
        # NEUTRAL (pas BEAR: currency_index_diverges ne se declenche donc pas
        # non plus). Les 5 votes d'origine unanimes BULL suffisent seuls
        # (5/6 >= VOTE_THRESHOLD), mais en rang 2 puisque CURRENCY_INDEX
        # n'a pas confirme.
        result = make_result("AUDCHF")
        index_by_currency = {"AUD": make_index_row(0.60), "CHF": make_index_row(-0.60)}
        imp_by_currency = {
            "AUD": make_imp_row("BEAR", "BEAR"),
            "CHF": make_imp_row("BULL", "BULL"),
        }
        selected = select_aligned_pairs_v2([result], index_by_currency, imp_by_currency)

        self.assertEqual(len(selected), 1)
        item = selected[0]
        self.assertEqual(item["direction"], "BULL")
        self.assertEqual(item["confirmations"], 5)
        self.assertEqual(item["rank_tier"], 2)
        self.assertEqual(item["CURRENCY_INDEX"], "NEUTRAL")

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

    def test_imp_by_currency_downgrades_to_neutral_on_total_contradiction(self):
        # Spread confirme BULL (AUD +0.28 vs CHF -0.54), mais aucune des deux
        # devises ne trend elle-meme dans le sens attendu par son role (AUD
        # "forte" trend BEAR sur son propre IMP21, CHF "faible" trend BULL)
        # -> retrograde en NEUTRAL plutot que de compter sur un ecart que
        # rien d'autre ne soutient.
        imp_by_currency = {
            "AUD": make_imp_row("BEAR", "BEAR"),
            "CHF": make_imp_row("BULL", "BULL"),
        }
        self.assertEqual(
            currency_index_vote("AUDCHF", self.INDEX_BY_CURRENCY, imp_by_currency), "NEUTRAL",
        )

    def test_imp_by_currency_partial_confirmation_keeps_the_vote(self):
        # Meme spread BULL, mais au moins une verification confirme (AUD
        # trend bien BULL sur D1) -> le vote reste BULL, seul un echec total
        # (0/N) retrograde.
        imp_by_currency = {
            "AUD": make_imp_row("BULL", "BEAR"),
            "CHF": make_imp_row("BULL", "BULL"),
        }
        self.assertEqual(
            currency_index_vote("AUDCHF", self.INDEX_BY_CURRENCY, imp_by_currency), "BULL",
        )

    def test_imp_by_currency_not_provided_keeps_old_spread_only_behavior(self):
        self.assertEqual(
            currency_index_vote("AUDCHF", self.INDEX_BY_CURRENCY, None), "BULL",
        )


class CurrencyTrendConfirmsTests(unittest.TestCase):
    def test_all_four_checks_confirm(self):
        # AUDCHF BULL: AUD (forte) devrait trender BULL, CHF (faible) BEAR.
        imp_by_currency = {
            "AUD": make_imp_row("BULL", "BULL"),
            "CHF": make_imp_row("BEAR", "BEAR"),
        }
        applicable, confirmations = currency_trend_confirms("AUDCHF", "BULL", imp_by_currency)
        self.assertEqual((applicable, confirmations), (4, 4))

    def test_all_four_checks_contradict(self):
        imp_by_currency = {
            "AUD": make_imp_row("BEAR", "BEAR"),
            "CHF": make_imp_row("BULL", "BULL"),
        }
        applicable, confirmations = currency_trend_confirms("AUDCHF", "BULL", imp_by_currency)
        self.assertEqual((applicable, confirmations), (4, 0))

    def test_bear_direction_swaps_the_expected_roles(self):
        # CHFAUD BEAR (base faible, cotee forte): CHF devrait trender BEAR,
        # AUD BULL -- inverse du cas AUDCHF BULL ci-dessus.
        imp_by_currency = {
            "CHF": make_imp_row("BEAR", "BEAR"),
            "AUD": make_imp_row("BULL", "BULL"),
        }
        applicable, confirmations = currency_trend_confirms("CHFAUD", "BEAR", imp_by_currency)
        self.assertEqual((applicable, confirmations), (4, 4))

    def test_neutral_imp_status_is_not_applicable(self):
        imp_by_currency = {
            "AUD": make_imp_row("NEUTRAL", "BULL"),
            "CHF": make_imp_row("NEUTRAL", "NEUTRAL"),
        }
        applicable, confirmations = currency_trend_confirms("AUDCHF", "BULL", imp_by_currency)
        self.assertEqual((applicable, confirmations), (1, 1))

    def test_missing_currency_data_only_checks_the_other_side(self):
        imp_by_currency = {"AUD": make_imp_row("BULL", "BULL")}
        applicable, confirmations = currency_trend_confirms("AUDCHF", "BULL", imp_by_currency)
        self.assertEqual((applicable, confirmations), (2, 2))

    def test_no_data_at_all_is_zero_applicable(self):
        self.assertEqual(currency_trend_confirms("AUDCHF", "BULL", {}), (0, 0))


class CurrencyIndexDivergesTests(unittest.TestCase):
    def test_true_only_on_active_contradiction(self):
        self.assertTrue(currency_index_diverges({"CURRENCY_INDEX": "BEAR"}, "BULL"))
        self.assertTrue(currency_index_diverges({"CURRENCY_INDEX": "BULL"}, "BEAR"))

    def test_false_on_neutral_or_agreement(self):
        self.assertFalse(currency_index_diverges({"CURRENCY_INDEX": "NEUTRAL"}, "BULL"))
        self.assertFalse(currency_index_diverges({"CURRENCY_INDEX": "NEUTRAL"}, "BEAR"))
        self.assertFalse(currency_index_diverges({"CURRENCY_INDEX": "BULL"}, "BULL"))
        self.assertFalse(currency_index_diverges({"CURRENCY_INDEX": "BEAR"}, "BEAR"))


class CurrencyConsensusStatusTests(unittest.TestCase):
    def test_bull_when_four_of_five_real_votes_agree(self):
        row = make_result(
            "GBP", renko=("BULL", "BULL", "BULL"),
            d1_imp=("BULL", 10.0, 15, -5.0, 5), h1_imp=("BEAR", -3.0, 5, 1.0, 5),
        )
        self.assertEqual(currency_consensus_status(row), "BULL")

    def test_bear_when_four_of_five_real_votes_agree(self):
        row = make_result(
            "JPY", renko=("BEAR", "BEAR", "BEAR"),
            d1_imp=("BEAR", -10.0, 15, 5.0, 5), h1_imp=("BULL", 3.0, 5, -1.0, 5),
        )
        self.assertEqual(currency_consensus_status(row), "BEAR")

    def test_neutral_without_a_clear_majority(self):
        row = make_result(
            "EUR", renko=("BULL", "BULL", "BEAR"),
            d1_imp=("BULL", 10.0, 15, -5.0, 5), h1_imp=("BEAR", -3.0, 5, 1.0, 5),
        )
        self.assertEqual(currency_consensus_status(row), "NEUTRAL")


class CurrencyDailyStatusTests(unittest.TestCase):
    def test_bull_above_the_threshold(self):
        self.assertEqual(currency_daily_status(0.64), "BULL")

    def test_bear_above_the_threshold(self):
        self.assertEqual(currency_daily_status(-1.20), "BEAR")

    def test_any_nonzero_move_counts(self):
        # CURRENCY_DAILY_MOVE_MIN_MAGNITUDE = 0 (a la demande explicite de
        # l'utilisateur, cf. commentaire pres de la constante): meme un tout
        # petit mouvement compte, seul un daily_chg exactement nul reste
        # NEUTRAL -- ex. reel: GBP -0.06% avec un consensus BULL doit compter
        # comme une contradiction, pas etre ignore comme du bruit.
        self.assertEqual(currency_daily_status(0.01), "BULL")
        self.assertEqual(currency_daily_status(-0.06), "BEAR")
        self.assertEqual(currency_daily_status(0.0), "NEUTRAL")

    def test_threshold_constant_is_zero(self):
        # Verrouille le choix explicite de l'utilisateur -- si ce test casse,
        # c'est qu'on a change CURRENCY_DAILY_MOVE_MIN_MAGNITUDE sans mettre
        # a jour ce commentaire.
        self.assertEqual(CURRENCY_DAILY_MOVE_MIN_MAGNITUDE, 0.0)

    def test_neutral_when_missing(self):
        self.assertEqual(currency_daily_status(None), "NEUTRAL")


class CurrencyDivergesFromItsOwnDayTests(unittest.TestCase):
    def test_true_when_daily_move_opposes_the_consensus(self):
        # GBP: -0.24% aujourd'hui (BEAR) mais consensus structurel BULL
        # (renko/d1/h1 par defaut de make_result, tous BULL).
        index_by_currency = {"GBP": make_index_row(-0.24)}
        imp_by_currency = {"GBP": make_result("GBP")}
        self.assertTrue(currency_diverges_from_its_own_day("GBP", index_by_currency, imp_by_currency))

    def test_false_when_daily_move_agrees_with_the_consensus(self):
        index_by_currency = {"AUD": make_index_row(0.64)}
        imp_by_currency = {"AUD": make_result("AUD")}  # consensus BULL par defaut
        self.assertFalse(currency_diverges_from_its_own_day("AUD", index_by_currency, imp_by_currency))

    def test_false_when_the_daily_move_is_neutral(self):
        index_by_currency = {"CAD": make_index_row(0.0)}
        imp_by_currency = {"CAD": make_result("CAD")}
        self.assertFalse(currency_diverges_from_its_own_day("CAD", index_by_currency, imp_by_currency))

    def test_false_when_the_consensus_is_neutral(self):
        index_by_currency = {"EUR": make_index_row(-0.06)}
        imp_by_currency = {"EUR": make_result(
            "EUR", renko=("BULL", "BULL", "BEAR"),
            d1_imp=("BULL", 10.0, 15, -5.0, 5), h1_imp=("BEAR", -3.0, 5, 1.0, 5),
        )}  # NEUTRAL, cf. CurrencyConsensusStatusTests
        self.assertFalse(currency_diverges_from_its_own_day("EUR", index_by_currency, imp_by_currency))

    def test_false_when_data_is_missing(self):
        self.assertFalse(currency_diverges_from_its_own_day("GBP", {}, {}))
        self.assertFalse(currency_diverges_from_its_own_day("GBP", {"GBP": make_index_row(-0.24)}, {}))

    def test_true_even_when_daily_move_is_tiny(self):
        # USD +0.01% aujourd'hui alors que le consensus structurel est BEAR:
        # meme un tres petit mouvement compte comme contradiction (aucun
        # seuil minimum, cf. CURRENCY_DAILY_MOVE_MIN_MAGNITUDE = 0) -- choix
        # explicite de l'utilisateur apres le cas reel GBP -0.06%/consensus
        # BULL, qu'il ne voulait pas voir ignore comme du bruit.
        index_by_currency = {"USD": make_index_row(0.01)}
        imp_by_currency = {"USD": make_result(
            "USD", renko=("BEAR", "BEAR", "BEAR"),
            d1_imp=("BEAR", -10.0, 15, 5.0, 5), h1_imp=("BEAR", -3.0, 5, 1.0, 5),
        )}
        self.assertTrue(currency_diverges_from_its_own_day("USD", index_by_currency, imp_by_currency))


class PairTouchesADivergentCurrencyTests(unittest.TestCase):
    def test_true_when_the_base_currency_diverges(self):
        index_by_currency = {"GBP": make_index_row(-0.24), "JPY": make_index_row(0.01)}
        imp_by_currency = {"GBP": make_result("GBP"), "JPY": make_result("JPY")}
        self.assertTrue(pair_touches_a_divergent_currency("GBPJPY", index_by_currency, imp_by_currency))

    def test_true_when_the_quote_currency_diverges(self):
        index_by_currency = {"AUD": make_index_row(0.64), "JPY": make_index_row(0.15)}
        imp_by_currency = {"AUD": make_result("AUD"), "JPY": make_result("JPY")}
        # JPY: +0.15% (BULL) aujourd'hui, consensus BEAR (renko/imp par
        # defaut de make_result bascule en BEAR ici via renko/d1/h1 fournis).
        imp_by_currency["JPY"] = make_result(
            "JPY", renko=("BEAR", "BEAR", "BEAR"),
            d1_imp=("BEAR", -10.0, 15, 5.0, 5), h1_imp=("BULL", 3.0, 5, -1.0, 5),
        )
        self.assertTrue(pair_touches_a_divergent_currency("AUDJPY", index_by_currency, imp_by_currency))

    def test_false_when_neither_currency_diverges(self):
        index_by_currency = {"AUD": make_index_row(0.64), "CHF": make_index_row(0.22)}
        imp_by_currency = {"AUD": make_result("AUD"), "CHF": make_result("CHF")}
        self.assertFalse(pair_touches_a_divergent_currency("AUDCHF", index_by_currency, imp_by_currency))


class SelectAlignedPairsV1Tests(unittest.TestCase):
    """Verrouille le flag `tradable` ajoute a V1 (select_aligned_pairs), nouveau
    pour V1 -- mirroir du mecanisme deja existant en V2 (cf. module docstring
    de imp_trend_29pairs.py, entree 2026-08-27)."""

    def test_tradable_true_by_default(self):
        # AUDCHF via make_result: 9/12 (les 11 votes d'origine donnent 9 BULL,
        # cf. commentaire dans imp_trend_29pairs.py), aucune donnee devise
        # fournie -> pas de contradiction detectable, tradable reste True.
        result = make_result("AUDCHF")
        selected = select_aligned_pairs([result])

        self.assertEqual(len(selected), 1)
        item = selected[0]
        self.assertEqual(item["confirmations"], 9)
        self.assertTrue(item["tradable"])

    def test_currency_daily_consensus_contradiction_marks_it_non_tradable(self):
        # Meme paire, mais AUD a une contradiction jour/consensus (-0.28%
        # aujourd'hui alors que son consensus structurel est BULL) -> reste
        # dans la liste (pas un veto) mais tradable=False et rang retrograde.
        result = make_result("AUDCHF")
        index_by_currency = {"AUD": make_index_row(-0.28)}
        imp_by_currency = {"AUD": make_result("AUD")}  # consensus BULL par defaut
        selected = select_aligned_pairs([result], index_by_currency, imp_by_currency)

        self.assertEqual(len(selected), 1)
        item = selected[0]
        self.assertFalse(item["tradable"])
        self.assertEqual(item["rank_tier"], 4)
        self.assertIn("DEVISE JOUR/CONSENSUS CONTRADICTOIRE", item["rank_reason"])


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
