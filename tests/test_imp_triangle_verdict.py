import unittest

import pandas as pd

import datetime as dt
from zoneinfo import ZoneInfo

from imp_triangle_verdict import (
    aligned_verdict,
    build_telegram_message,
    classify,
    count_since,
    timeframe_verdict,
)


class ClassifyTests(unittest.TestCase):
    def test_bull_when_price_is_above_the_bull_level(self):
        # niveau bull 100, niveau bear 90 : prix 105 > 100 ; 105 < 90 faux
        self.assertEqual(classify(105, bull_above=100, bear_below=90, latest_kind="BEAR"), "BULL")

    def test_bear_when_price_is_below_the_bear_level(self):
        self.assertEqual(classify(95, bull_above=110, bear_below=100, latest_kind="BULL"), "BEAR")

    def test_neutral_when_price_sits_between_the_two_levels(self):
        self.assertEqual(classify(100, bull_above=110, bear_below=90, latest_kind="BULL"), "NEUTRE")

    def test_both_true_is_settled_by_the_most_recent_triangle(self):
        self.assertEqual(classify(105, bull_above=100, bear_below=110, latest_kind="BULL"), "BULL")
        self.assertEqual(classify(105, bull_above=100, bear_below=110, latest_kind="BEAR"), "BEAR")

    def test_price_exactly_on_a_level_does_not_qualify(self):
        self.assertEqual(classify(100, bull_above=100, bear_below=None, latest_kind="BEAR"), "NEUTRE")
        self.assertEqual(classify(100, bull_above=None, bear_below=100, latest_kind="BULL"), "NEUTRE")

    def test_missing_level_cannot_qualify_that_side(self):
        self.assertEqual(classify(105, bull_above=None, bear_below=None, latest_kind=None), "NEUTRE")
        self.assertEqual(classify(105, bull_above=100, bear_below=None, latest_kind="BEAR"), "BULL")
        self.assertEqual(classify(95, bull_above=None, bear_below=100, latest_kind="BULL"), "BEAR")


def sig(kind, index):
    return dict(kind=kind, index=index, cross_index=index)


def computed_for(signals, opens, closes):
    times = list(pd.date_range("2026-01-04 21:00", periods=len(opens), freq="D", tz="UTC"))
    return dict(times=times, opens=opens, closes=closes, signals=signals)


class CountSinceTests(unittest.TestCase):
    def test_counts_same_color_after_the_last_opposite(self):
        signals = [sig("BULL", 0), sig("BEAR", 1), sig("BULL", 2), sig("BULL", 3)]
        self.assertEqual(count_since(signals, "BEAR", "BULL"), 2)
        self.assertEqual(count_since(signals, "BULL", "BEAR"), 0)

    def test_counts_all_when_there_was_never_an_opposite(self):
        signals = [sig("BULL", 0), sig("BULL", 1), sig("BULL", 2)]
        self.assertEqual(count_since(signals, "BEAR", "BULL"), 3)


class TimeframeVerdictTests(unittest.TestCase):
    def test_default_reference_is_the_open_of_the_last_triangle_of_each_color(self):
        # [BULL 0, BEAR 1, BULL 2] : 1 seul vert depuis le dernier rouge (< far_count 2)
        c = computed_for([sig("BULL", 0), sig("BEAR", 1), sig("BULL", 2)],
                         opens=[10.0, 20.0, 30.0, 40.0], closes=[11.0, 19.0, 31.0, 41.0])
        reading = timeframe_verdict(c, "D", price=25.0)
        self.assertEqual(reading["bull_ref"]["basis"], "OPEN_RED")
        self.assertEqual(reading["bull_ref"]["level"], 20.0)
        self.assertEqual(reading["bear_ref"]["basis"], "OPEN_GREEN")
        self.assertEqual(reading["bear_ref"]["level"], 30.0)  # dernier vert = index 2
        # 25 > 20 (BULL vrai) et 25 < 30 (BEAR vrai) -> le plus recent (vert) departage
        self.assertEqual(reading["verdict"], "BULL")

    def test_far_red_triangle_moves_the_bull_reference_to_the_last_green_close(self):
        # [BEAR 0, BULL 1, BULL 2, BULL 3] : 3 verts depuis le rouge >= far_count 2
        c = computed_for([sig("BEAR", 0), sig("BULL", 1), sig("BULL", 2), sig("BULL", 3)],
                         opens=[100.0, 60.0, 70.0, 80.0], closes=[99.0, 62.0, 72.0, 85.0])
        reading = timeframe_verdict(c, "D", price=90.0)
        self.assertEqual(reading["bull_ref"]["basis"], "CLOSE_GREEN")
        self.assertEqual(reading["bull_ref"]["level"], 85.0)   # close du dernier vert, pas l'open du rouge (100)
        self.assertEqual(reading["verdict"], "BULL")           # 90 > 85 (avec l'open du rouge 100 : NEUTRE)
        self.assertEqual(reading["greens_since_red"], 3)

    def test_price_inside_the_last_green_body_is_neutral_once_the_reference_moved(self):
        c = computed_for([sig("BEAR", 0), sig("BULL", 1), sig("BULL", 2)],
                         opens=[100.0, 60.0, 80.0], closes=[99.0, 62.0, 85.0])
        # bull > close 85 ; bear < open du dernier vert 80 : 82 n'est ni l'un ni l'autre
        self.assertEqual(timeframe_verdict(c, "D", price=82.0)["verdict"], "NEUTRE")
        self.assertEqual(timeframe_verdict(c, "D", price=79.0)["verdict"], "BEAR")

    def test_far_green_triangle_moves_the_bear_reference_to_the_last_red_close(self):
        # [BULL 0, BEAR 1, BEAR 2, BEAR 3] : 3 rouges depuis le vert >= far_count 2
        c = computed_for([sig("BULL", 0), sig("BEAR", 1), sig("BEAR", 2), sig("BEAR", 3)],
                         opens=[10.0, 60.0, 50.0, 45.0], closes=[11.0, 58.0, 48.0, 40.0])
        reading = timeframe_verdict(c, "D", price=35.0)
        self.assertEqual(reading["bear_ref"]["basis"], "CLOSE_RED")
        self.assertEqual(reading["bear_ref"]["level"], 40.0)   # close du dernier rouge, pas l'open du vert (10)
        self.assertEqual(reading["verdict"], "BEAR")           # 35 < 40 (avec l'open du vert 10 : NEUTRE)
        self.assertEqual(reading["reds_since_green"], 3)

    def test_below_the_threshold_the_original_rule_applies(self):
        c = computed_for([sig("BEAR", 0), sig("BULL", 1), sig("BULL", 2)],
                         opens=[100.0, 60.0, 80.0], closes=[99.0, 62.0, 85.0])
        reading = timeframe_verdict(c, "D", price=90.0, far_count=3)  # 2 verts < 3
        self.assertEqual(reading["bull_ref"]["basis"], "OPEN_RED")
        self.assertEqual(reading["verdict"], "NEUTRE")               # 90 < open du rouge 100

    def test_no_red_at_all_with_enough_greens_uses_the_green_close(self):
        c = computed_for([sig("BULL", 0), sig("BULL", 1)], opens=[10.0, 20.0], closes=[12.0, 25.0])
        reading = timeframe_verdict(c, "D", price=30.0)
        self.assertEqual(reading["bull_ref"]["basis"], "CLOSE_GREEN")
        self.assertEqual(reading["verdict"], "BULL")

    def test_no_triangle_at_all_is_neutral(self):
        c = computed_for([], opens=[10.0, 20.0], closes=[11.0, 21.0])
        reading = timeframe_verdict(c, "D", price=25.0)
        self.assertEqual(reading["verdict"], "NEUTRE")
        self.assertIsNone(reading["bull_ref"])
        self.assertIsNone(reading["bear_ref"])


class AlignedVerdictTests(unittest.TestCase):
    def result(self, **verdicts):
        return dict(timeframes={tf: dict(verdict=v) for tf, v in verdicts.items()})

    def test_aligned_when_all_timeframes_agree(self):
        self.assertEqual(aligned_verdict(self.result(D="BULL", W="BULL", M="BULL")), "BULL")
        self.assertEqual(aligned_verdict(self.result(D="BEAR", W="BEAR")), "BEAR")

    def test_not_aligned_on_disagreement_or_all_neutral(self):
        self.assertIsNone(aligned_verdict(self.result(D="BULL", W="BEAR", M="BULL")))
        self.assertIsNone(aligned_verdict(self.result(D="BULL", W="NEUTRE", M="BULL")))
        self.assertIsNone(aligned_verdict(self.result(D="NEUTRE", W="NEUTRE")))


class TelegramMessageTests(unittest.TestCase):
    NOW = dt.datetime(2026, 9, 20, 3, 45, tzinfo=ZoneInfo("Europe/Paris"))

    def result(self, pair, **verdicts):
        return dict(pair=pair, timeframes={tf: dict(verdict=v) for tf, v in verdicts.items()})

    def test_one_icon_per_aligned_pair_bull_first_then_bear_alphabetical(self):
        results = [
            self.result("NZDUSD", D="BEAR", W="BEAR", M="BEAR"),
            self.result("GBPNZD", D="BULL", W="BULL", M="BULL"),
            self.result("EURAUD", D="BEAR", W="BEAR", M="BEAR"),
            self.result("AUDCAD", D="BULL", W="BULL", M="BULL"),
            self.result("CHFJPY", D="BEAR", W="NEUTRE", M="BULL"),  # non alignee : absente
        ]
        self.assertEqual(
            build_telegram_message(results, now=self.NOW),
            "\U0001f53a EARLY IMP\n\n"
            "AUDCAD\t\U0001f7e2\nGBPNZD\t\U0001f7e2\n"
            "EURAUD\t\U0001f534\nNZDUSD\t\U0001f534\n\n"
            "⏰ 2026-09-20 03:45 Paris",
        )

    def test_silent_when_no_pair_is_aligned(self):
        results = [self.result("CHFJPY", D="BEAR", W="NEUTRE", M="BULL")]
        self.assertIsNone(build_telegram_message(results, now=self.NOW))

    def test_works_with_a_subset_of_timeframes(self):
        results = [self.result("EURAUD", D="BEAR", W="BEAR")]
        message = build_telegram_message(results, now=self.NOW)
        self.assertIn("EURAUD\t\U0001f534\n", message)
        self.assertNotIn("\U0001f7e2", message)


if __name__ == "__main__":
    unittest.main()
