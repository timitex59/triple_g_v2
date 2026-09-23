import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from index_sar_daily import build_telegram_message  # noqa: E402


class TelegramMessageTests(unittest.TestCase):
    def test_bare_title_icon_lines_and_paris_timestamp(self):
        rows = [dict(index="DXY", verdict="BULL"), dict(index="EXY", verdict="BEAR")]
        message = build_telegram_message(rows, now=datetime(2026, 9, 24, 9, 15))
        self.assertEqual(message, "\U0001f9ed INDEX SAR D\n\nDXY\t\U0001f7e2\nEXY\t\U0001f534\n\n⏰ 2026-09-24 09:15 Paris")


if __name__ == "__main__":
    unittest.main()
