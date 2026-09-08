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

    def test_captures_reference_on_first_run_of_day(self):
        bull_report = currency_report(70.0, 10.0, 10.0, 70.0)
        now = dt.datetime(2026, 7, 16, 0, 5, tzinfo=PARIS)

        state = update_currency_trend_state({}, ["EURUSD"], bull_report, now)

        counts = state["pairs"]["EURUSD"]
        self.assertEqual(counts["ref_period"], "am")
        self.assertAlmostEqual(counts["ref_up_pct"], 100.0)
        self.assertAlmostEqual(counts["ref_down_pct"], 0.0)

    def test_keeps_the_am_reference_across_the_morning(self):
        bull_report = currency_report(70.0, 10.0, 10.0, 70.0)
        bear_report = currency_report(10.0, 70.0, 70.0, 10.0)
        t0 = dt.datetime(2026, 7, 16, 0, 5, tzinfo=PARIS)
        state = update_currency_trend_state({}, ["EURUSD"], bull_report, t0)  # ref: 100%/0%

        t1 = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)
        state = update_currency_trend_state(state, ["EURUSD"], bear_report, t1)

        counts = state["pairs"]["EURUSD"]
        self.assertEqual(counts["ref_period"], "am")
        self.assertAlmostEqual(counts["ref_up_pct"], 100.0)
        self.assertAlmostEqual(counts["ref_down_pct"], 0.0)

    def test_recaptures_reference_at_14h(self):
        bull_report = currency_report(70.0, 10.0, 10.0, 70.0)
        bear_report = currency_report(10.0, 70.0, 70.0, 10.0)
        t0 = dt.datetime(2026, 7, 16, 0, 5, tzinfo=PARIS)
        state = update_currency_trend_state({}, ["EURUSD"], bull_report, t0)  # ref am: 100%/0%

        t1 = dt.datetime(2026, 7, 16, 14, 0, tzinfo=PARIS)
        state = update_currency_trend_state(state, ["EURUSD"], bear_report, t1)
        counts = state["pairs"]["EURUSD"]
        self.assertEqual(counts["ref_period"], "pm")
        pm_ref_up = counts["ref_up_pct"]
        self.assertLess(pm_ref_up, 100.0)

        t2 = dt.datetime(2026, 7, 16, 16, 0, tzinfo=PARIS)
        state = update_currency_trend_state(state, ["EURUSD"], bear_report, t2)
        counts = state["pairs"]["EURUSD"]
        self.assertEqual(counts["ref_period"], "pm")
        self.assertAlmostEqual(counts["ref_up_pct"], pm_ref_up)


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

    def test_shows_a_green_second_ball_when_above_the_reference(self):
        state = {
            "date": "2026-07-16",
            "pairs": {
                "EURUSD": {
                    "weighted_up": 5.0, "weighted_down": 2.0,
                    "ref_up_pct": 50.0, "ref_down_pct": 50.0,
                },
            },
        }

        self.assertEqual(
            currency_trend_lines(["EURUSD"], state),
            ["📈 TENDANCE", "🟢 EURUSD (71.43%) 🟢", "🔴 EURUSD (28.57%) 🔴"],
        )

    def test_shows_a_red_second_ball_when_below_the_reference(self):
        state = {
            "date": "2026-07-16",
            "pairs": {
                "EURUSD": {
                    "weighted_up": 2.0, "weighted_down": 5.0,
                    "ref_up_pct": 50.0, "ref_down_pct": 50.0,
                },
            },
        }

        self.assertEqual(
            currency_trend_lines(["EURUSD"], state),
            ["📈 TENDANCE", "🟢 EURUSD (28.57%) 🔴", "🔴 EURUSD (71.43%) 🟢"],
        )

    def test_no_second_ball_when_equal_to_or_without_reference(self):
        equal_state = {
            "date": "2026-07-16",
            "pairs": {
                "EURUSD": {
                    "weighted_up": 5.0, "weighted_down": 5.0,
                    "ref_up_pct": 50.0, "ref_down_pct": 50.0,
                },
            },
        }
        self.assertEqual(
            currency_trend_lines(["EURUSD"], equal_state),
            ["📈 TENDANCE", "🟢 EURUSD (50.00%)", "🔴 EURUSD (50.00%)"],
        )

        no_ref_state = {"date": "2026-07-16", "pairs": {"EURUSD": {"weighted_up": 5.0, "weighted_down": 2.0}}}
        self.assertEqual(
            currency_trend_lines(["EURUSD"], no_ref_state),
            ["📈 TENDANCE", "🟢 EURUSD (71.43%)", "🔴 EURUSD (28.57%)"],
        )


if __name__ == "__main__":
    unittest.main()
