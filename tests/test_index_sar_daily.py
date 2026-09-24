import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402

from index_sar_daily import ball, build_telegram_message, combinations, h1_cross_since, score  # noqa: E402


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


    def test_eligible_section_after_the_indices(self):
        rows = [dict(index="USD", verdict="BULL", dist=1.0, chg=0.1, score=0.1)]
        message = build_telegram_message(rows, [("AUDUSD", "BEAR"), ("USDJPY", "BULL")],
                                         now=datetime(2026, 9, 24, 9, 15))
        self.assertEqual(message, "\U0001f9ed INDEX SAR D\n\n\U0001f7e2USD (+0.10)\n\n"
                                  "ELIGIBLE\n\U0001f534AUDUSD\n\U0001f7e2USDJPY\n\n⏰ 2026-09-24 09:15 Paris")


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


class H1CrossSinceTests(unittest.TestCase):
    TIMES = list(pd.date_range("2026-09-24 10:00", periods=5, freq="h", tz="UTC"))
    SAR = [10, 10, 10, 10, 10]

    def test_cross_on_a_bar_closed_after_the_previous_run(self):
        # crossunder sur la bougie de 13:00 (cloturee a 14:00), run precedent a 13:20
        closes = [11, 11, 11, 9, 9]
        self.assertEqual(h1_cross_since(self.TIMES, closes, self.SAR, pd.Timestamp("2026-09-24 13:20", tz="UTC")), "BEAR")

    def test_cross_already_seen_by_the_previous_run_is_ignored(self):
        closes = [11, 11, 11, 9, 9]
        self.assertIsNone(h1_cross_since(self.TIMES, closes, self.SAR, pd.Timestamp("2026-09-24 14:20", tz="UTC")))

    def test_last_cross_of_the_window_wins(self):
        closes = [11, 9, 11, 11, 11]
        self.assertEqual(h1_cross_since(self.TIMES, closes, self.SAR, pd.Timestamp("2026-09-24 10:30", tz="UTC")), "BULL")

    def test_without_state_only_the_last_closed_bar_counts(self):
        self.assertEqual(h1_cross_since(self.TIMES, [11, 11, 11, 11, 9], self.SAR, None), "BEAR")
        self.assertIsNone(h1_cross_since(self.TIMES, [11, 11, 11, 9, 9], self.SAR, None))


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
