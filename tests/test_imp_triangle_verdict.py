import unittest

import pandas as pd

from imp_triangle_verdict import aligned_verdict, classify, timeframe_verdict


class ClassifyTests(unittest.TestCase):
    def test_bull_when_price_is_above_the_last_red_triangle_open(self):
        # dernier rouge open 100, dernier vert open 90 : prix 105 > 100 ; 105 < 90 faux
        self.assertEqual(classify(105, green_open=90, red_open=100, latest_kind="BEAR"), "BULL")

    def test_bear_when_price_is_below_the_last_green_triangle_open(self):
        # dernier vert open 100, dernier rouge open 110 : prix 95 < 100 ; 95 > 110 faux
        self.assertEqual(classify(95, green_open=100, red_open=110, latest_kind="BULL"), "BEAR")

    def test_neutral_when_price_sits_between_the_two_references(self):
        # vert open 90, rouge open 110 : prix 100 n'est ni > 110 ni < 90
        self.assertEqual(classify(100, green_open=90, red_open=110, latest_kind="BULL"), "NEUTRE")

    def test_both_true_is_settled_by_the_most_recent_triangle(self):
        # rouge open 100, vert open 110, prix 105 : > 100 ET < 110
        self.assertEqual(classify(105, green_open=110, red_open=100, latest_kind="BULL"), "BULL")
        self.assertEqual(classify(105, green_open=110, red_open=100, latest_kind="BEAR"), "BEAR")

    def test_price_exactly_on_a_reference_open_does_not_qualify(self):
        self.assertEqual(classify(100, green_open=None, red_open=100, latest_kind="BEAR"), "NEUTRE")
        self.assertEqual(classify(100, green_open=100, red_open=None, latest_kind="BULL"), "NEUTRE")

    def test_missing_reference_triangle_cannot_qualify_that_side(self):
        self.assertEqual(classify(105, green_open=None, red_open=None, latest_kind=None), "NEUTRE")
        self.assertEqual(classify(105, green_open=None, red_open=100, latest_kind="BEAR"), "BULL")
        self.assertEqual(classify(95, green_open=100, red_open=None, latest_kind="BULL"), "BEAR")


class TimeframeVerdictTests(unittest.TestCase):
    def computed(self):
        times = list(pd.to_datetime(
            ["2026-09-10 21:00", "2026-09-13 21:00", "2026-09-14 21:00", "2026-09-15 21:00"], utc=True))
        return dict(
            times=times, opens=[10.0, 20.0, 30.0, 40.0],
            signals=[
                dict(kind="BULL", index=0, cross_index=0),
                dict(kind="BEAR", index=1, cross_index=0),
                dict(kind="BULL", index=2, cross_index=1),
            ],
        )

    def test_reference_is_the_open_of_the_last_triangle_of_each_color(self):
        reading = timeframe_verdict(self.computed(), "D", price=25.0)
        self.assertEqual(reading["green"]["open"], 30.0)  # dernier vert = index 2, pas l'index 0
        self.assertEqual(reading["red"]["open"], 20.0)
        self.assertEqual(reading["latest_kind"], "BULL")
        # 25 > rouge 20 (BULL vrai) et 25 < vert 30 (BEAR vrai) -> depart. par le plus recent : vert = BULL
        self.assertEqual(reading["verdict"], "BULL")

    def test_no_triangle_at_all_is_neutral(self):
        computed = self.computed() | dict(signals=[])
        reading = timeframe_verdict(computed, "D", price=25.0)
        self.assertEqual(reading["verdict"], "NEUTRE")
        self.assertIsNone(reading["green"])
        self.assertIsNone(reading["red"])


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


if __name__ == "__main__":
    unittest.main()
