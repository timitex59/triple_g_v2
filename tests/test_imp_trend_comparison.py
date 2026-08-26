"""Tests pour `comparison_pips` (cf. update_session_tracking dans
imp_trend_29pairs.py) -- demande par l'utilisateur en constatant que le
"cumul" classique de V1 (a vie, cf. test_imp_trend_sessions.py) partait
avec 2 jours d'avance sur V2, faussant toute comparaison de performance
entre les deux strategies tant que V2 est jeune. "cmp" s'ancre a 0 pour
n'importe quel script qui reutilise update_session_tracking, au moment de
son premier run apres le deploiement -- donc a 0 pour les DEUX (V1 malgre
son passif, V2 tout neuf) sur ce premier run, puis evolue en parallele."""

import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from imp_trend_29pairs import PARIS, update_session_tracking


def paris(y, m, d, hh, mm):
    return datetime(y, m, d, hh, mm, tzinfo=PARIS)


class ComparisonPipsTests(unittest.TestCase):
    def setUp(self):
        self._tmpdir = TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)

    def _path(self, name):
        return Path(self._tmpdir.name) / name

    def test_first_run_anchors_to_zero_even_with_preexisting_cumul(self):
        # Simule un fichier V1 avec un historique deja accumule (comme en
        # production): cumul a vie non nul avant meme ce nouveau champ.
        path = self._path("v1_like.json")
        import json
        path.write_text(json.dumps({"version": 1, "sessions": {
            "Londres": {
                "realized_pips": 42.4, "current_session_realized_pips": 0.0,
                "last_session_pips": 42.4, "active_session_id": None,
                "positions": {}, "events": [],
            },
        }}), encoding="utf-8")

        state = update_session_tracking(path, [], [], now=paris(2026, 8, 27, 8, 0))
        londres = state["sessions"]["Londres"]

        self.assertEqual(londres["total_pips"], 42.4)  # cumul inchange, garde son passif
        self.assertEqual(londres["comparison_pips"], 0.0)  # cmp reparti a 0 malgre le passif

    def test_fresh_session_also_starts_comparison_at_zero(self):
        path = self._path("v2_like.json")
        state = update_session_tracking(path, [], [], now=paris(2026, 8, 27, 8, 0))
        self.assertEqual(state["sessions"]["Londres"]["comparison_pips"], 0.0)

    def test_identical_results_give_identical_comparison_pips_despite_different_cumul(self):
        # V1 (avec passif de 42.4) et V2 (vierge) subissent le meme cycle
        # +10 pips -> leurs "cumul" divergent (42.4 d'ecart), mais leur "cmp"
        # doit etre rigoureusement identique.
        import json
        v1_path = self._path("v1.json")
        v1_path.write_text(json.dumps({"version": 1, "sessions": {
            "Londres": {
                "realized_pips": 42.4, "current_session_realized_pips": 0.0,
                "last_session_pips": 42.4, "active_session_id": None,
                "positions": {}, "events": [],
            },
        }}), encoding="utf-8")
        v2_path = self._path("v2.json")

        # 1er run post-deploiement (ancrage) pour les deux, meme instant.
        update_session_tracking(v1_path, [], [], now=paris(2026, 8, 27, 8, 0))
        update_session_tracking(v2_path, [], [], now=paris(2026, 8, 27, 8, 0))

        # Meme cycle +10 pips sur EURUSD pour les deux.
        for path in (v1_path, v2_path):
            update_session_tracking(
                path, [{"pair": "EURUSD", "direction": "BULL"}],
                [{"pair": "EURUSD", "reference_price": 1.1000}],
                now=paris(2026, 8, 27, 8, 10),
            )
        state_v1 = update_session_tracking(
            v1_path, [], [{"pair": "EURUSD", "reference_price": 1.1010}],
            now=paris(2026, 8, 27, 17, 0),
        )
        state_v2 = update_session_tracking(
            v2_path, [], [{"pair": "EURUSD", "reference_price": 1.1010}],
            now=paris(2026, 8, 27, 17, 0),
        )

        londres_v1 = state_v1["sessions"]["Londres"]
        londres_v2 = state_v2["sessions"]["Londres"]

        self.assertNotAlmostEqual(londres_v1["total_pips"], londres_v2["total_pips"], places=1)
        self.assertAlmostEqual(londres_v1["comparison_pips"], 10.0, places=6)
        self.assertAlmostEqual(londres_v2["comparison_pips"], 10.0, places=6)
        self.assertAlmostEqual(
            londres_v1["comparison_pips"], londres_v2["comparison_pips"], places=6,
        )

    def test_comparison_baseline_is_set_only_once(self):
        path = self._path("anchor_once.json")
        state = update_session_tracking(
            path, [{"pair": "EURUSD", "direction": "BULL"}],
            [{"pair": "EURUSD", "reference_price": 1.1000}],
            now=paris(2026, 8, 27, 8, 0),
        )
        baseline_after_first_run = state["sessions"]["Sydney"]["comparison_baseline"]

        # Run suivant: le prix bouge, mais le baseline ne doit pas re-sauter.
        state = update_session_tracking(
            path, [{"pair": "EURUSD", "direction": "BULL"}],
            [{"pair": "EURUSD", "reference_price": 1.1050}],
            now=paris(2026, 8, 27, 9, 0),
        )
        self.assertEqual(state["sessions"]["Sydney"]["comparison_baseline"], baseline_after_first_run)


if __name__ == "__main__":
    unittest.main()
