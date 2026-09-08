import datetime as dt
import unittest
from zoneinfo import ZoneInfo

from imp_count_v2 import (
    currency_trend_lines,
    pair_trend_icon,
    signed_currency_net,
    update_currency_trend_state,
)

PARIS = ZoneInfo("Europe/Paris")


def currency_report(eur_bull, eur_bear, usd_bull, usd_bear):
    """Report factice, meme structure que `imp_count_v2.currency_exposure`."""
    return {
        "EUR": {"bull_pct": eur_bull, "bear_pct": eur_bear},
        "USD": {"bull_pct": usd_bull, "bear_pct": usd_bear},
    }


class SignedCurrencyNetTests(unittest.TestCase):
    def test_none_when_currency_absent(self):
        self.assertIsNone(signed_currency_net("EUR", {}))

    def test_bull_minus_bear(self):
        report = {"EUR": {"bull_pct": 70.0, "bear_pct": 10.0}}
        self.assertAlmostEqual(signed_currency_net("EUR", report), 60.0)


class PairTrendIconTests(unittest.TestCase):
    def test_green_when_base_stronger_than_quote(self):
        report = currency_report(eur_bull=70.0, eur_bear=10.0, usd_bull=10.0, usd_bear=70.0)
        self.assertEqual(pair_trend_icon("EURUSD", report), "🟢")

    def test_red_when_quote_stronger_than_base(self):
        report = currency_report(eur_bull=10.0, eur_bear=70.0, usd_bull=70.0, usd_bear=10.0)
        self.assertEqual(pair_trend_icon("EURUSD", report), "🔴")

    def test_none_when_a_currency_is_missing(self):
        report = {"EUR": {"bull_pct": 70.0, "bear_pct": 10.0}}
        self.assertIsNone(pair_trend_icon("EURUSD", report))


class UpdateCurrencyTrendStateTests(unittest.TestCase):
    def test_accumulates_up_and_down_across_runs(self):
        # Meme `now` a chaque run -> decroissance nulle, comptage simple.
        bull_report = currency_report(70.0, 10.0, 10.0, 70.0)
        bear_report = currency_report(10.0, 70.0, 70.0, 10.0)
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        state = {}
        for report in (bull_report, bull_report, bull_report, bull_report, bull_report, bear_report, bear_report):
            state = update_currency_trend_state(state, ["EURUSD"], report, now)

        counts = state["pairs"]["EURUSD"]
        self.assertAlmostEqual(counts["weighted_up"], 5.0)
        self.assertAlmostEqual(counts["weighted_down"], 2.0)

    def test_decays_older_weight_after_one_half_life(self):
        bull_report = currency_report(70.0, 10.0, 10.0, 70.0)
        bear_report = currency_report(10.0, 70.0, 70.0, 10.0)
        t0 = dt.datetime(2026, 7, 16, 6, 0, tzinfo=PARIS)
        state = update_currency_trend_state({}, ["EURUSD"], bull_report, t0)
        for _ in range(9):
            state = update_currency_trend_state(state, ["EURUSD"], bull_report, t0)
        self.assertAlmostEqual(state["pairs"]["EURUSD"]["weighted_up"], 10.0)

        t1 = t0 + dt.timedelta(hours=4)  # 1 demi-vie (CURRENCY_TREND_HALF_LIFE_HOURS)
        state = update_currency_trend_state(state, ["EURUSD"], bear_report, t1)

        counts = state["pairs"]["EURUSD"]
        self.assertAlmostEqual(counts["weighted_up"], 5.0)
        self.assertAlmostEqual(counts["weighted_down"], 1.0)

    def test_resets_on_a_new_paris_day(self):
        bull_report = currency_report(70.0, 10.0, 10.0, 70.0)
        state = update_currency_trend_state(
            {}, ["EURUSD"], bull_report, dt.datetime(2026, 7, 16, 23, 0, tzinfo=PARIS),
        )
        self.assertAlmostEqual(state["pairs"]["EURUSD"]["weighted_up"], 1.0)

        state = update_currency_trend_state(
            state, ["EURUSD"], bull_report, dt.datetime(2026, 7, 17, 0, 5, tzinfo=PARIS),
        )
        self.assertAlmostEqual(state["pairs"]["EURUSD"]["weighted_up"], 1.0)

    def test_no_balls_on_first_run_of_a_chain(self):
        # Tout premier run du jour (~00h Paris ou plus tard, mais avant 14h):
        # rien a comparer, ni pour l'ancre ni pour le run precedent.
        bull_report = currency_report(70.0, 10.0, 10.0, 70.0)
        now = dt.datetime(2026, 7, 16, 0, 5, tzinfo=PARIS)

        state = update_currency_trend_state({}, ["EURUSD"], bull_report, now)

        counts = state["pairs"]["EURUSD"]
        self.assertEqual(counts["period"], "am")
        self.assertEqual(counts["anchor_up_arrow"], "")
        self.assertEqual(counts["anchor_down_arrow"], "")
        self.assertEqual(counts["up_arrow"], "")
        self.assertEqual(counts["down_arrow"], "")
        # L'ancre se fige sur le %monte/%baisse de ce tout premier run.
        self.assertAlmostEqual(counts["anchor_up_pct"], 100.0)
        self.assertAlmostEqual(counts["anchor_down_pct"], 0.0)

    def test_second_run_matches_both_balls_to_the_same_reference(self):
        # Sur le 2e run d'une chaine, l'ancre ET le run precedent valent tous
        # les deux le 1er run -- les 2 billes sont donc identiques.
        bull_report = currency_report(70.0, 10.0, 10.0, 70.0)
        bear_report = currency_report(10.0, 70.0, 70.0, 10.0)
        t0 = dt.datetime(2026, 7, 16, 0, 5, tzinfo=PARIS)
        state = update_currency_trend_state({}, ["EURUSD"], bull_report, t0)  # 1er run: pas de bille

        t1 = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)
        state = update_currency_trend_state(state, ["EURUSD"], bear_report, t1)

        counts = state["pairs"]["EURUSD"]
        self.assertEqual(counts["period"], "am")
        self.assertEqual(counts["anchor_up_arrow"], "🔴")  # %monte a recule vs l'ancre (=1er run)
        self.assertEqual(counts["up_arrow"], "🔴")  # ... et vs le run precedent (le meme run ici)
        self.assertEqual(counts["anchor_down_arrow"], "🟢")
        self.assertEqual(counts["down_arrow"], "🟢")

    def test_anchor_and_previous_run_diverge_on_a_pullback(self):
        # 1er run bull (ancre), 2e run bull (monte encore), 3e run bear
        # (redescend vs le 2e, mais reste au-dessus de l'ancre du 1er) --
        # l'ancre et le run precedent doivent alors diverger.
        bull_report = currency_report(70.0, 10.0, 10.0, 70.0)
        bear_report = currency_report(10.0, 70.0, 70.0, 10.0)
        t0 = dt.datetime(2026, 7, 16, 6, 0, tzinfo=PARIS)
        state = update_currency_trend_state({}, ["EURUSD"], bull_report, t0)  # ancre: 100%/0%

        t1 = t0 + dt.timedelta(minutes=5)
        state = update_currency_trend_state(state, ["EURUSD"], bull_report, t1)  # reste a 100%/0% (deja sature)

        t2 = t1 + dt.timedelta(minutes=5)
        state = update_currency_trend_state(state, ["EURUSD"], bear_report, t2)  # 1 run bear -> repli local

        counts = state["pairs"]["EURUSD"]
        up_pct = counts["weighted_up"] / (counts["weighted_up"] + counts["weighted_down"]) * 100.0
        self.assertLess(up_pct, 100.0)  # a bien recule depuis l'ancre (100%)
        self.assertEqual(counts["anchor_up_arrow"], "🔴")  # ... donc rouge vs l'ancre du 1er run
        self.assertEqual(counts["up_arrow"], "🔴")  # ... et rouge vs le run precedent (encore a 100%)

    def test_no_balls_right_after_the_14h_reset(self):
        # Au 1er run >= 14h, la chaine repart de zero: pas de bille meme si
        # le %monte/%baisse a change depuis le dernier run du matin.
        bull_report = currency_report(70.0, 10.0, 10.0, 70.0)
        bear_report = currency_report(10.0, 70.0, 70.0, 10.0)
        t0 = dt.datetime(2026, 7, 16, 0, 5, tzinfo=PARIS)
        state = update_currency_trend_state({}, ["EURUSD"], bull_report, t0)

        t1 = dt.datetime(2026, 7, 16, 14, 0, tzinfo=PARIS)
        state = update_currency_trend_state(state, ["EURUSD"], bear_report, t1)
        counts = state["pairs"]["EURUSD"]
        self.assertEqual(counts["period"], "pm")
        self.assertEqual(counts["anchor_up_arrow"], "")
        self.assertEqual(counts["anchor_down_arrow"], "")
        self.assertEqual(counts["up_arrow"], "")
        self.assertEqual(counts["down_arrow"], "")

        # Le run suivant (toujours "pm") compare de nouveau, aux 2 references.
        t2 = dt.datetime(2026, 7, 16, 16, 0, tzinfo=PARIS)
        state = update_currency_trend_state(state, ["EURUSD"], bear_report, t2)
        counts = state["pairs"]["EURUSD"]
        self.assertEqual(counts["period"], "pm")
        self.assertEqual(counts["anchor_up_arrow"], "🔴")  # continue de baisser vs l'ancre 14h
        self.assertEqual(counts["up_arrow"], "🔴")  # ... et vs le run precedent (2e run bear consecutif)


class CurrencyTrendLinesTests(unittest.TestCase):
    def test_reports_both_up_and_down_percentages(self):
        state = {"date": "2026-07-16", "pairs": {"EURUSD": {"weighted_up": 5.0, "weighted_down": 2.0}}}

        self.assertEqual(
            currency_trend_lines(["EURUSD"], state),
            ["📈 TENDANCE", "🟢 EURUSD (71.43%)", "🔴 EURUSD (28.57%)"],
        )

    def test_empty_without_any_decisive_run_yet(self):
        state = {"date": "2026-07-16", "pairs": {"EURUSD": {"weighted_up": 0.0, "weighted_down": 0.0}}}

        self.assertEqual(currency_trend_lines(["EURUSD"], state), [])
        self.assertEqual(currency_trend_lines(["EURUSD"], {}), [])

    def test_renders_both_stored_balls_glued_together(self):
        # currency_trend_lines se contente d'afficher anchor_*_arrow puis
        # *_arrow, colles l'un a l'autre, tels que calcules par
        # update_currency_trend_state (reference de la chaine, puis run
        # precedent).
        state = {
            "date": "2026-07-16",
            "pairs": {
                "EURUSD": {
                    "weighted_up": 5.0, "weighted_down": 2.0,
                    "anchor_up_arrow": "🟢", "up_arrow": "🟢",
                    "anchor_down_arrow": "🔴", "down_arrow": "🔴",
                },
            },
        }

        self.assertEqual(
            currency_trend_lines(["EURUSD"], state),
            ["📈 TENDANCE", "🟢 EURUSD (71.43%) 🟢🟢", "🔴 EURUSD (28.57%) 🔴🔴"],
        )

    def test_renders_a_single_ball_when_only_one_side_diverges(self):
        # L'ancre et le run precedent peuvent diverger (cf. update_currency_
        # trend_state): seule la bille dont la comparaison a quelque chose a
        # montrer s'affiche.
        state = {
            "date": "2026-07-16",
            "pairs": {
                "EURUSD": {
                    "weighted_up": 5.0, "weighted_down": 2.0,
                    "anchor_up_arrow": "", "up_arrow": "🔴",
                    "anchor_down_arrow": "", "down_arrow": "🟢",
                },
            },
        }

        self.assertEqual(
            currency_trend_lines(["EURUSD"], state),
            ["📈 TENDANCE", "🟢 EURUSD (71.43%) 🔴", "🔴 EURUSD (28.57%) 🟢"],
        )

    def test_no_balls_when_absent(self):
        # 1er run d'une chaine (cf. update_currency_trend_state): les 4 cles
        # valent "" -- pas de bille du tout.
        no_arrow_state = {
            "date": "2026-07-16",
            "pairs": {
                "EURUSD": {
                    "weighted_up": 5.0, "weighted_down": 2.0,
                    "anchor_up_arrow": "", "up_arrow": "", "anchor_down_arrow": "", "down_arrow": "",
                },
            },
        }
        self.assertEqual(
            currency_trend_lines(["EURUSD"], no_arrow_state),
            ["📈 TENDANCE", "🟢 EURUSD (71.43%)", "🔴 EURUSD (28.57%)"],
        )

        # Etat sans ces cles du tout (ex. ancien format persiste avant cette
        # fonctionnalite) -- ne doit pas planter, pas de bille non plus.
        no_key_state = {"date": "2026-07-16", "pairs": {"EURUSD": {"weighted_up": 5.0, "weighted_down": 2.0}}}
        self.assertEqual(
            currency_trend_lines(["EURUSD"], no_key_state),
            ["📈 TENDANCE", "🟢 EURUSD (71.43%)", "🔴 EURUSD (28.57%)"],
        )


if __name__ == "__main__":
    unittest.main()
