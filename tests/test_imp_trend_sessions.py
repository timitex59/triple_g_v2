"""Regression tests for the IMP TREND session pips tracking
(cf. `update_session_tracking` in imp_trend_29pairs.py).

Bug fixed here: "cumul" (`total_pips`) used to be a lifetime-cumulative
counter (`realized_pips`, never reset) rather than scoped to the current
Paris calendar day, so a session that had not opened yet today (e.g.
Londres/New York checked before their window starts) displayed the frozen
result of its last occurrence -- from a previous day -- as if it were
today's figure. Fixed by gating on a real close timestamp
(`last_session_closed_at`) compared against midnight Paris, rather than on
the session's start-date id (which for an overnight session like Sydney
does not match the calendar day it actually closes on)."""

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from imp_trend_29pairs import PARIS, update_session_tracking


def paris(y, m, d, hh, mm):
    return datetime(y, m, d, hh, mm, tzinfo=PARIS)


class ImpTrendSessionTests(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.state_path = Path(self._tmpdir.name) / "imp_trend_sessions_state.json"

    def test_session_not_yet_started_today_shows_zero_not_yesterdays_carryover(self):
        # Londres a cloture hier (23/08) avec -91.8 pips, jamais rouverte
        # depuis -- exactement le scenario rapporte (bille figee affichee
        # comme si c'etait le cumul du jour).
        self.state_path.write_text(json.dumps({"version": 1, "sessions": {
            "Londres": {
                "realized_pips": -91.8,
                "current_session_realized_pips": -91.8,
                "last_session_pips": -91.8,
                "last_session_closed_at": "2026-08-23T15:00:00+00:00",
                "active_session_id": None,
                "positions": {},
                "events": [],
            },
        }}), encoding="utf-8")

        # Verification a 07:53 le 24/08, avant l'ouverture de Londres (8h).
        state = update_session_tracking(
            self.state_path, selected=[], results=[], now=paris(2026, 8, 24, 7, 53),
        )

        londres = state["sessions"]["Londres"]
        self.assertEqual(londres["daily_pips"], 0.0)
        self.assertEqual(londres["total_pips"], 0.0)
        self.assertFalse(londres["is_active"])
        # L'historique brut reste disponible, seul l'affichage est gate.
        self.assertEqual(londres["last_session_pips"], -91.8)

    def test_overnight_session_still_shows_todays_result_after_it_closes(self):
        # Sydney (22h-7h Paris) chevauche minuit: son occurrence "commence"
        # la veille au soir mais cloture ce matin -- doit rester affichee
        # comme resultat du jour tant qu'on est le meme jour calendaire.
        update_session_tracking(
            self.state_path,
            selected=[{"pair": "EURUSD", "direction": "BULL"}],
            results=[{"pair": "EURUSD", "reference_price": 1.1000}],
            now=paris(2026, 8, 23, 23, 0),
        )
        state = update_session_tracking(
            self.state_path, selected=[], results=[{"pair": "EURUSD", "reference_price": 1.1050}],
            now=paris(2026, 8, 24, 7, 30),
        )
        sydney_just_closed = state["sessions"]["Sydney"]
        self.assertAlmostEqual(sydney_just_closed["daily_pips"], 50.0, places=6)
        self.assertAlmostEqual(sydney_just_closed["total_pips"], 50.0, places=6)

        # Re-verification plus tard le meme jour (14h): toujours le resultat
        # du jour, pas remis a 0 avant la reouverture de ce soir 22h.
        state = update_session_tracking(
            self.state_path, selected=[], results=[], now=paris(2026, 8, 24, 14, 0),
        )
        sydney_later = state["sessions"]["Sydney"]
        self.assertAlmostEqual(sydney_later["daily_pips"], 50.0, places=6)
        self.assertAlmostEqual(sydney_later["total_pips"], 50.0, places=6)
        self.assertFalse(sydney_later["is_active"])


if __name__ == "__main__":
    unittest.main()
