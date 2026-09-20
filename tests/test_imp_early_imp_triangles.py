import unittest

import pandas as pd

from imp_early_imp_triangles import drop_unconfirmed, early_imp_signals, find_crosses


def flags(n, *indexes):
    return [i in indexes for i in range(n)]


class EarlyImpSignalsTests(unittest.TestCase):
    def test_triggers_on_first_green_candle_after_the_cross_not_on_the_cross_candle(self):
        # cross bull en 2 (bougie verte elle-meme), 3 rouge, 4 verte -> triangle en 4
        opens = [10, 10, 10, 12, 11, 11]
        closes = [10, 10, 12, 11, 12, 11]
        signals, armed = early_imp_signals(opens, closes, flags(6, 2), flags(6))
        self.assertEqual(signals, [dict(kind="BULL", index=4, cross_index=2)])
        self.assertIsNone(armed)

    def test_bear_triggers_on_first_red_candle(self):
        opens = [10, 10, 10, 8, 9, 9]
        closes = [10, 10, 8, 9, 8, 9]
        signals, _ = early_imp_signals(opens, closes, flags(6), flags(6, 2))
        self.assertEqual(signals, [dict(kind="BEAR", index=4, cross_index=2)])

    def test_no_signal_without_a_cross(self):
        opens = [10, 10, 10, 10]
        closes = [11, 9, 11, 9]
        signals, armed = early_imp_signals(opens, closes, flags(4), flags(4))
        self.assertEqual(signals, [])
        self.assertIsNone(armed)

    def test_opposite_cross_cancels_the_pending_wait(self):
        # cross bull en 2, cross bear en 3 (rouge) : l'attente bull est annulee,
        # la verte en 4 ne declenche rien ; la rouge en 5 declenche le BEAR.
        opens = [10, 10, 10, 12, 8, 9]
        closes = [10, 10, 12, 8, 9, 7]
        signals, armed = early_imp_signals(opens, closes, flags(6, 2), flags(6, 3))
        self.assertEqual(signals, [dict(kind="BEAR", index=5, cross_index=3)])
        self.assertIsNone(armed)

    def test_pending_wait_is_reported_when_nothing_triggered_yet(self):
        opens = [10, 10, 10, 12]
        closes = [10, 10, 12, 11]
        signals, armed = early_imp_signals(opens, closes, flags(4, 2), flags(4))
        self.assertEqual(signals, [])
        self.assertEqual(armed, dict(kind="BULL", cross_index=2))

    def test_trigger_is_evaluated_before_the_new_cross_rearms(self):
        # attente BEAR armee en 1 ; en 2, bougie rouge qui est aussi un cross bull :
        # le BEAR declenche (etat d'avant), puis le cross bull arme l'attente BULL.
        opens = [10, 10, 10, 9]
        closes = [10, 9, 9.5, 10]
        signals, armed = early_imp_signals(opens, closes, flags(4, 2), flags(4, 1))
        self.assertEqual(signals[0], dict(kind="BEAR", index=2, cross_index=1))
        self.assertEqual(signals[1], dict(kind="BULL", index=3, cross_index=2))
        self.assertIsNone(armed)

    def test_signal_is_a_rising_edge_only(self):
        # cross bull en 2 et de nouveau en 3 (SAR touche, pas de cross bear entre) :
        # 3 declenche, le cross en 3 rearme, 4 verte ne doit PAS re-signaler.
        opens = [10, 10, 10, 10, 10]
        closes = [10, 10, 11, 11, 11]
        signals, _ = early_imp_signals(opens, closes, flags(5, 2, 3), flags(5))
        self.assertEqual(signals, [dict(kind="BULL", index=3, cross_index=2)])


class FindCrossesTests(unittest.TestCase):
    def test_crossover_and_crossunder_use_previous_bar(self):
        closes = [9, 11, 12, 8]
        sar = [float("nan"), 10, 10, 10]
        bull, bear = find_crosses(closes, sar)
        self.assertEqual(bull, [False, False, False, False])  # sar[0] nan : pas de cross en 1
        self.assertEqual(bear, [False, False, False, True])

    def test_bull_cross(self):
        closes = [9, 9, 11]
        sar = [10, 10, 10]
        bull, bear = find_crosses(closes, sar)
        self.assertEqual(bull, [False, False, True])
        self.assertEqual(bear, [False, False, False])


class DropUnconfirmedTests(unittest.TestCase):
    def frame(self):
        times = pd.to_datetime(["2026-09-16 21:00", "2026-09-17 21:00", "2026-09-18 21:00"], utc=True)
        return pd.DataFrame({"time": times, "open": [1, 1, 1], "close": [1, 1, 1]})

    def test_developing_candle_is_dropped(self):
        # 19/09 12:00 UTC : la bougie ouverte le 18/09 21:00 est encore en cours
        out = drop_unconfirmed(self.frame(), pd.Timestamp("2026-09-19 12:00", tz="UTC"))
        self.assertEqual(len(out), 2)

    def test_closed_last_candle_is_kept_over_the_weekend(self):
        out = drop_unconfirmed(self.frame(), pd.Timestamp("2026-09-20 12:00", tz="UTC"))
        self.assertEqual(len(out), 3)


if __name__ == "__main__":
    unittest.main()
