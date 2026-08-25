"""Tests pour la trajectoire "jour" heure par heure (cf. `daily_pips_history`
dans `update_session_tracking` et `session_trajectory_line`/`session_lines`
dans imp_trend_29pairs.py) -- demandee par l'utilisateur a la place de la
section EXPOSITION DEVISES qui ne l'interessait pas sur Telegram."""

import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from imp_trend_29pairs import (
    PARIS,
    session_lines,
    session_trajectory_line,
    update_session_tracking,
)


def paris(y, m, d, hh, mm):
    return datetime(y, m, d, hh, mm, tzinfo=PARIS)


class SessionTrajectoryTests(unittest.TestCase):
    def setUp(self):
        self._tmpdir = TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.state_path = Path(self._tmpdir.name) / "imp_trend_sessions_state.json"

    def _sydney(self, state):
        return state["sessions"]["Sydney"]

    def test_no_history_yields_no_trajectory_line(self):
        self.assertIsNone(session_trajectory_line({}))
        self.assertIsNone(session_trajectory_line({"daily_pips_history": []}))

    def test_trajectory_records_one_point_per_run_while_open_plus_close(self):
        steps = [
            (paris(2026, 8, 24, 22, 29), 1.1000),
            (paris(2026, 8, 24, 23, 19), 1.0992),  # -8 pips
            (paris(2026, 8, 25, 1, 36), 1.1005),   # +5 pips
            (paris(2026, 8, 25, 5, 14), 1.1033),   # +33 pips
        ]
        state = None
        for now, price in steps:
            state = update_session_tracking(
                self.state_path,
                selected=[{"pair": "EURUSD", "direction": "BULL"}],
                results=[{"pair": "EURUSD", "reference_price": price}],
                now=now,
            )
        # cloture: plus selectionnee, prix de sortie 1.1019 -> +19 pips
        state = update_session_tracking(
            self.state_path, selected=[],
            results=[{"pair": "EURUSD", "reference_price": 1.1019}],
            now=paris(2026, 8, 25, 7, 19),
        )

        sydney = self._sydney(state)
        line = session_trajectory_line(sydney)
        self.assertEqual(
            line,
            "  ↳ depuis 22h29: +0.0 → 23h19 -8.0 → 01h36 +5.0 → 05h14 +33.0 → 07h19 +19.0 (clôturée)",
        )

    def test_history_clears_on_the_next_cycles_opening(self):
        # 1er cycle: ouverture, +45 a la cloture.
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
        self.assertEqual(len(self._sydney(state)["daily_pips_history"]), 2)

        # Session fermee toute la journee: aucun nouveau point ajoute.
        state = update_session_tracking(
            self.state_path, selected=[], results=[], now=paris(2026, 8, 25, 14, 0),
        )
        self.assertEqual(len(self._sydney(state)["daily_pips_history"]), 2)

        # 2e cycle: reouverture -> l'historique du 1er cycle est efface, ne
        # garde que le point du nouveau cycle (jour reparti a 0).
        state = update_session_tracking(
            self.state_path,
            selected=[{"pair": "GBPUSD", "direction": "BULL"}],
            results=[{"pair": "GBPUSD", "reference_price": 1.2000}],
            now=paris(2026, 8, 25, 22, 0),
        )
        history = self._sydney(state)["daily_pips_history"]
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["daily_pips"], 0.0)

    def test_open_session_has_no_cloturee_suffix(self):
        state = update_session_tracking(
            self.state_path,
            selected=[{"pair": "EURUSD", "direction": "BULL"}],
            results=[{"pair": "EURUSD", "reference_price": 1.1000}],
            now=paris(2026, 8, 24, 22, 0),
        )
        line = session_trajectory_line(self._sydney(state))
        self.assertNotIn("clôturée", line)

    def test_session_lines_interleaves_summary_and_trajectory(self):
        state = update_session_tracking(
            self.state_path,
            selected=[{"pair": "EURUSD", "direction": "BULL"}],
            results=[{"pair": "EURUSD", "reference_price": 1.1000}],
            now=paris(2026, 8, 24, 22, 0),
        )
        lines = session_lines(state)

        sydney_idx = next(i for i, l in enumerate(lines) if l.startswith("Sydney :"))
        self.assertTrue(lines[sydney_idx + 1].startswith("  ↳ depuis"))


if __name__ == "__main__":
    unittest.main()
