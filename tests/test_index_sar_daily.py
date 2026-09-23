import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from index_sar_daily import build_telegram_message, score  # noqa: E402


class TelegramMessageTests(unittest.TestCase):
    def test_bare_title_icon_lines_and_paris_timestamp(self):
        rows = [dict(index="USD", verdict="BULL", dist=1.05, chg=0.5734, score=0.6),
                dict(index="EUR", verdict="BEAR", dist=-0.99, chg=-0.57, score=-0.5643),
                dict(index="JPY", verdict="BEAR", dist=-1.4, chg=float("nan"), score=float("nan"))]
        message = build_telegram_message(rows, now=datetime(2026, 9, 24, 9, 15))
        self.assertEqual(message, "\U0001f9ed INDEX SAR D\n\nINDEX\t\tdist\tCHG%D\tSCORE\n"
                                  "USD\t\U0001f7e2\t+1.05%\t+0.57%\t+0.60\n"
                                  "EUR\t\U0001f534\t-0.99%\t-0.57%\t-0.56\n"
                                  "JPY\t\U0001f534\t-1.40%\tn/a\tn/a\n\n⏰ 2026-09-24 09:15 Paris")


class ScoreTests(unittest.TestCase):
    def test_positive_only_when_both_are_positive(self):
        self.assertAlmostEqual(score(2.0, 0.5), 1.0)
        self.assertAlmostEqual(score(2.0, -0.5), -1.0)
        self.assertAlmostEqual(score(-2.0, 0.5), -1.0)
        self.assertAlmostEqual(score(-2.0, -0.5), -1.0)  # deux negatifs : negatif


if __name__ == "__main__":
    unittest.main()
