"""Regression test for the IMP TREND session pips tracking (cf.
`update_session_tracking` in imp_trend_29pairs.py), locking in the "jour"
vs "cumul" semantics validated with the user against a worked example:

- "jour" (daily_pips): result of the CURRENT/most recent occurrence of a
  given session. Resets to 0.0 exactly when the session opens, fluctuates
  live while open (realized + floating), freezes at close and stays frozen
  until that same session's NEXT opening -- not until midnight. So a frozen
  value from a previous occurrence legitimately still shows before today's
  open; that's intended, not stale data.

- "cumul" (total_pips): lifetime-cumulative total for that session --
  every close adds to it, forever, never resets. Tracks the strategy's
  overall effectiveness on that session over time, independent of the
  other 3 sessions and of any single day's freeze/reset cycle.

(An earlier attempt "fixed" this by making cumul mirror jour, reset nightly
-- that broke the intended behavior and was reverted once the exact
semantics were pinned down with a concrete example.)"""

import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from imp_trend_29pairs import PARIS, update_session_tracking


def paris(y, m, d, hh, mm):
    return datetime(y, m, d, hh, mm, tzinfo=PARIS)


class ImpTrendSessionTests(unittest.TestCase):
    def setUp(self):
        self._tmpdir = TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.state_path = Path(self._tmpdir.name) / "imp_trend_sessions_state.json"

    def _sydney(self, state):
        return state["sessions"]["Sydney"]

    def test_daily_freezes_on_close_and_cumul_carries_across_the_next_cycle(self):
        # Lundi 22h Paris: 1er lancement, Sydney ouvre, position EURUSD BULL.
        state = update_session_tracking(
            self.state_path,
            selected=[{"pair": "EURUSD", "direction": "BULL"}],
            results=[{"pair": "EURUSD", "reference_price": 1.1000}],
            now=paris(2026, 8, 24, 22, 0),
        )
        sydney = self._sydney(state)
        self.assertEqual(sydney["daily_pips"], 0.0)
        self.assertEqual(sydney["total_pips"], 0.0)
        self.assertTrue(sydney["is_active"])

        # Lundi 23h: +12 pips flottants -- jour et cumul montent ensemble.
        state = update_session_tracking(
            self.state_path,
            selected=[{"pair": "EURUSD", "direction": "BULL"}],
            results=[{"pair": "EURUSD", "reference_price": 1.1012}],
            now=paris(2026, 8, 24, 23, 0),
        )
        sydney = self._sydney(state)
        self.assertAlmostEqual(sydney["daily_pips"], 12.0, places=6)
        self.assertAlmostEqual(sydney["total_pips"], 12.0, places=6)

        # Mardi 7h: cloture de session (+45 pips realises sur le cycle).
        state = update_session_tracking(
            self.state_path, selected=[],
            results=[{"pair": "EURUSD", "reference_price": 1.1045}],
            now=paris(2026, 8, 25, 7, 0),
        )
        sydney = self._sydney(state)
        self.assertAlmostEqual(sydney["daily_pips"], 45.0, places=6)
        self.assertAlmostEqual(sydney["total_pips"], 45.0, places=6)
        self.assertFalse(sydney["is_active"])

        # Mardi 14h: session toujours fermee -- fige, pas remis a 0.
        state = update_session_tracking(
            self.state_path, selected=[], results=[], now=paris(2026, 8, 25, 14, 0),
        )
        sydney = self._sydney(state)
        self.assertAlmostEqual(sydney["daily_pips"], 45.0, places=6)
        self.assertAlmostEqual(sydney["total_pips"], 45.0, places=6)

        # Mardi 22h: reouverture -- jour retombe a 0, cumul garde le +45.
        state = update_session_tracking(
            self.state_path,
            selected=[{"pair": "GBPUSD", "direction": "BULL"}],
            results=[{"pair": "GBPUSD", "reference_price": 1.2000}],
            now=paris(2026, 8, 25, 22, 0),
        )
        sydney = self._sydney(state)
        self.assertEqual(sydney["daily_pips"], 0.0)
        self.assertAlmostEqual(sydney["total_pips"], 45.0, places=6)

        # Mardi 23h: +12 flottants sur ce nouveau cycle -> jour=12, cumul=57.
        state = update_session_tracking(
            self.state_path,
            selected=[{"pair": "GBPUSD", "direction": "BULL"}],
            results=[{"pair": "GBPUSD", "reference_price": 1.2012}],
            now=paris(2026, 8, 25, 23, 0),
        )
        sydney = self._sydney(state)
        self.assertAlmostEqual(sydney["daily_pips"], 12.0, places=6)
        self.assertAlmostEqual(sydney["total_pips"], 57.0, places=6)

    def test_other_sessions_never_affect_this_ones_cumul(self):
        # Sydney ferme avec +45 (comme ci-dessus, condense).
        update_session_tracking(
            self.state_path,
            selected=[{"pair": "EURUSD", "direction": "BULL"}],
            results=[{"pair": "EURUSD", "reference_price": 1.1000}],
            now=paris(2026, 8, 24, 22, 0),
        )
        state = update_session_tracking(
            self.state_path, selected=[],
            results=[{"pair": "EURUSD", "reference_price": 1.1045}],
            now=paris(2026, 8, 25, 7, 0),
        )
        self.assertAlmostEqual(self._sydney(state)["total_pips"], 45.0, places=6)

        # Tokyo ouvre et cloture avec +20 -- Sydney (fermee) ne doit pas bouger.
        # (Horaires apres 7h pour rester chronologiquement APRES la cloture
        # de Sydney ci-dessus -- Tokyo et Sydney chevauchent 0h-7h, un "now"
        # dans ce chevauchement rouvrirait artificiellement la fenetre
        # nocturne de Sydney puisque le temps ne peut pas revenir en arriere.)
        state = update_session_tracking(
            self.state_path,
            selected=[{"pair": "USDJPY", "direction": "BULL"}],
            results=[{"pair": "USDJPY", "reference_price": 150.00}],
            now=paris(2026, 8, 25, 7, 30),
        )
        state = update_session_tracking(
            self.state_path, selected=[],
            results=[{"pair": "USDJPY", "reference_price": 150.20}],
            now=paris(2026, 8, 25, 8, 30),
        )
        sydney = self._sydney(state)
        self.assertAlmostEqual(sydney["daily_pips"], 45.0, places=6)
        self.assertAlmostEqual(sydney["total_pips"], 45.0, places=6)
        self.assertAlmostEqual(state["sessions"]["Tokyo"]["total_pips"], 20.0, places=6)


if __name__ == "__main__":
    unittest.main()
