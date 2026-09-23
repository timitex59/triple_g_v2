import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from index_sar_daily import build_telegram_message  # noqa: E402


class TelegramMessageTests(unittest.TestCase):
    def test_bare_title_icon_lines_and_paris_timestamp(self):
        rows = [dict(index="DXY", verdict="BULL", dist=1.05, chg=0.5734),
                dict(index="EXY", verdict="BEAR", dist=-0.99, chg=-0.57),
                dict(index="JXY", verdict="BEAR", dist=-1.4, chg=float("nan"))]
        message = build_telegram_message(rows, now=datetime(2026, 9, 24, 9, 15))
        self.assertEqual(message, "\U0001f9ed INDEX SAR D\n\nINDEX\t\tdist\tCHG%D\n"
                                  "DXY\t\U0001f7e2\t+1.05%\t+0.57%\nEXY\t\U0001f534\t-0.99%\t-0.57%\n"
                                  "JXY\t\U0001f534\t-1.40%\tn/a\n\n⏰ 2026-09-24 09:15 Paris")


if __name__ == "__main__":
    unittest.main()
