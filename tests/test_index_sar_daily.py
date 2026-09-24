import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402

import imp_trend_29pairs as base  # noqa: E402
from index_sar_daily import (  # noqa: E402
    ball,
    build_telegram_message,
    combinations,
    last_cross_since,
    score,
    update_eligible,
)

NOW = datetime(2026, 9, 24, 18, 20, tzinfo=base.PARIS)


class TelegramMessageTests(unittest.TestCase):
    def test_bare_title_icon_lines_and_paris_timestamp(self):
        rows = [dict(index="USD", verdict="BULL", dist=1.05, chg=0.5734, score=0.6),
                dict(index="EUR", verdict="BEAR", dist=-0.99, chg=-0.57, score=-0.5643),
                dict(index="JPY", verdict="BEAR", dist=-1.4, chg=float("nan"), score=float("nan"))]
        message = build_telegram_message(rows, now=datetime(2026, 9, 24, 9, 15))
        self.assertEqual(message, "\U0001f9ed INDEX SAR D\n\n"
                                  "\U0001f7e2USD (+0.60)\n"
                                  "\U0001f534EUR (-0.56)\n"
                                  "\U0001f534JPY (n/a)\n\n⏰ 2026-09-24 09:15 Paris")

    def test_eligible_section_time_only_for_pairs_already_eligible_and_warning(self):
        rows = [dict(index="USD", verdict="BULL", dist=1.0, chg=0.1, score=0.1)]
        eligible = [
            # nouvelle a ce run : pas d'heure
            dict(pair="AUDUSD", direction="BEAR", cross_open=pd.Timestamp("2026-09-24 15:00", tz="UTC"),
                 fresh=True, warning=False),
            # deja eligible aujourd'hui : heure de cloture de la bougie du cross (Paris)
            dict(pair="NZDUSD", direction="BEAR", cross_open=pd.Timestamp("2026-09-24 11:00", tz="UTC"),
                 fresh=False, warning=True),
            # deja eligible un jour precedent : date + heure
            dict(pair="USDJPY", direction="BULL", cross_open=pd.Timestamp("2026-09-23 12:00", tz="UTC"),
                 fresh=False, warning=False),
        ]
        message = build_telegram_message(rows, eligible, now=NOW)
        self.assertEqual(message, "\U0001f9ed INDEX SAR D\n\n\U0001f7e2USD (+0.10)\n\n"
                                  "ELIGIBLE\n"
                                  "\U0001f534AUDUSD\n"
                                  "\U0001f534NZDUSD (14:00) ⚠️\n"
                                  "\U0001f7e2USDJPY (23/09 15:00)\n\n"
                                  "⏰ 2026-09-24 18:20 Paris")


class CombinationTests(unittest.TestCase):
    def test_strong_x_weak_gives_the_pair_and_the_expected_cross(self):
        rows = [dict(index="USD", verdict="BULL", dist=1.0, chg=0.1),
                dict(index="AUD", verdict="BEAR", dist=-1.0, chg=-0.1),
                dict(index="JPY", verdict="BEAR", dist=-1.0, chg=-0.1),
                dict(index="EUR", verdict="BEAR", dist=-1.0, chg=0.1)]   # grise : exclue
        self.assertEqual(combinations(rows), [("AUDUSD", "BEAR"), ("USDJPY", "BULL")])

    def test_no_strong_currency_means_no_combination(self):
        rows = [dict(index="AUD", verdict="BEAR", dist=-1.0, chg=-0.1)]
        self.assertEqual(combinations(rows), [])


TIMES = list(pd.date_range("2026-09-24 10:00", periods=6, freq="h", tz="UTC"))
SAR = [10.0] * 6


class LastCrossSinceTests(unittest.TestCase):
    def test_cross_on_a_bar_closed_after_the_previous_run(self):
        # crossunder sur la bougie de 13:00 (cloturee a 14:00), run precedent a 13:20
        closes = [11, 11, 11, 9, 9, 9]
        self.assertEqual(last_cross_since(TIMES, closes, SAR, pd.Timestamp("2026-09-24 13:20", tz="UTC")), ("BEAR", 3))

    def test_cross_already_seen_by_the_previous_run_is_ignored(self):
        closes = [11, 11, 11, 9, 9, 9]
        self.assertEqual(last_cross_since(TIMES, closes, SAR, pd.Timestamp("2026-09-24 14:20", tz="UTC")), (None, None))

    def test_last_cross_of_the_window_wins(self):
        closes = [11, 9, 11, 11, 11, 11]
        self.assertEqual(last_cross_since(TIMES, closes, SAR, pd.Timestamp("2026-09-24 10:30", tz="UTC"))[0], "BULL")

    def test_without_state_only_the_last_closed_bar_counts(self):
        self.assertEqual(last_cross_since(TIMES, [11, 11, 11, 11, 11, 9], SAR, None)[0], "BEAR")
        self.assertIsNone(last_cross_since(TIMES, [11, 11, 11, 11, 9, 9], SAR, None)[0])


class UpdateEligibleTests(unittest.TestCase):
    SINCE = pd.Timestamp("2026-09-24 12:20", tz="UTC")

    def h1(self, closes, sar=None):
        return (TIMES, closes, sar or SAR)

    def test_crossunder_in_the_combination_direction_enters_with_the_sar_level(self):
        sar = [10.0, 10.0, 10.0, 10.5, 10.4, 10.3]
        entries, state = update_eligible({}, [("AUDUSD", "BEAR")], {"AUDUSD": self.h1([11, 11, 11, 9, 9, 9], sar)}, self.SINCE)
        self.assertEqual([(e["pair"], e["fresh"], e["warning"]) for e in entries], [("AUDUSD", True, False)])
        self.assertEqual(state["AUDUSD"], dict(direction="BEAR", level=10.5, cross_open=TIMES[3].isoformat()))

    def test_cross_against_the_combination_does_not_enter(self):
        entries, state = update_eligible({}, [("USDJPY", "BULL")], {"USDJPY": self.h1([11, 11, 11, 9, 9, 9])}, self.SINCE)
        self.assertEqual((entries, state), ([], {}))

    def test_eligible_pair_stays_until_a_franc_close_beyond_the_cross_level(self):
        previous = {"AUDUSD": dict(direction="BEAR", level=10.5, cross_open=TIMES[3].isoformat())}
        # le SAR H1 se retourne (prix > SAR a 9.5) mais les clotures restent sous 10.5 : toujours eligible
        entries, state = update_eligible(previous, [("AUDUSD", "BEAR")],
                                         {"AUDUSD": self.h1([11, 11, 11, 9, 10.4, 10.5], [10, 10, 10, 10.5, 9.5, 9.6])},
                                         pd.Timestamp("2026-09-24 15:20", tz="UTC"))
        self.assertEqual([(e["pair"], e["fresh"]) for e in entries], [("AUDUSD", False)])
        self.assertEqual(state["AUDUSD"], previous["AUDUSD"])   # heure et niveau d'origine
        # cloture franche au-dessus de 10.5 : sortie
        entries, state = update_eligible(previous, [("AUDUSD", "BEAR")],
                                         {"AUDUSD": self.h1([11, 11, 11, 9, 10.4, 10.51])}, self.SINCE)
        self.assertEqual((entries, state), ([], {}))

    def test_crossover_pair_leaves_on_a_close_below_its_level(self):
        previous = {"USDJPY": dict(direction="BULL", level=9.5, cross_open=TIMES[3].isoformat())}
        entries, _ = update_eligible(previous, [], {"USDJPY": self.h1([9, 9, 9, 11, 9.6, 9.49])}, self.SINCE)
        self.assertEqual(entries, [])

    def test_combination_gone_keeps_the_pair_with_a_warning(self):
        previous = {"AUDUSD": dict(direction="BEAR", level=10.5, cross_open=TIMES[3].isoformat())}
        entries, state = update_eligible(previous, [], {"AUDUSD": self.h1([11, 11, 11, 9, 9, 9])}, self.SINCE)
        self.assertEqual([(e["pair"], e["warning"]) for e in entries], [("AUDUSD", True)])
        self.assertIn("AUDUSD", state)

    def test_pair_whose_fetch_failed_keeps_its_state(self):
        previous = {"AUDUSD": dict(direction="BEAR", level=10.5, cross_open=TIMES[3].isoformat())}
        entries, state = update_eligible(previous, [("AUDUSD", "BEAR")], {}, self.SINCE)
        self.assertEqual(state, previous)
        self.assertEqual([e["fresh"] for e in entries], [False])


class BallTests(unittest.TestCase):
    def test_grey_ball_when_dist_and_chg_have_opposite_signs(self):
        self.assertEqual(ball(dict(verdict="BULL", dist=1.0, chg=-0.2)), "⚪")
        self.assertEqual(ball(dict(verdict="BEAR", dist=-1.0, chg=0.2)), "⚪")
        self.assertEqual(ball(dict(verdict="BULL", dist=1.0, chg=0.2)), "\U0001f7e2")
        self.assertEqual(ball(dict(verdict="BEAR", dist=-1.0, chg=-0.2)), "\U0001f534")


class ScoreTests(unittest.TestCase):
    def test_positive_only_when_both_are_positive(self):
        self.assertAlmostEqual(score(2.0, 0.5), 1.0)
        self.assertAlmostEqual(score(2.0, -0.5), -1.0)
        self.assertAlmostEqual(score(-2.0, 0.5), -1.0)
        self.assertAlmostEqual(score(-2.0, -0.5), -1.0)  # deux negatifs : negatif


if __name__ == "__main__":
    unittest.main()
