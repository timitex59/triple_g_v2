"""Tests pour le mecanisme d'instantane devise partage (cf. module docstring
de imp_trend_29pairs.py, entree 2026-08-27 'later'): V1, V2 et paire_check.py
tournent dans le meme job CI (.github/workflows/triple_g_workflow.yml) mais
chacun fetchait `index_by_currency`/`imp_by_currency` independamment, a
quelques minutes d'ecart -- assez pour qu'une devise a la limite d'un seuil
(daily_chg, consensus) soit lue differemment d'un script a l'autre sur le
meme run. `save_currency_snapshot`/`load_currency_snapshot`/
`fetch_or_load_currency_data` font que le premier script a fetcher laisse un
instantane que les suivants reutilisent verbatim."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from imp_trend_29pairs import (
    CURRENCY_SNAPSHOT_MAX_AGE,
    TFState,
    _deserialize_currency_index_row,
    _serialize_currency_index_row,
    fetch_or_load_currency_data,
    load_currency_snapshot,
    save_currency_snapshot,
)


def make_index_row(daily_chg=0.5):
    """Ligne `compute_asset_score`-shaped complete, avec un vrai `TFState`
    (pas un SimpleNamespace) pour verifier le round-trip de serialisation."""
    state = TFState(
        px_state=1, bias=1, direction=1, green_streak=3, red_streak=0,
        renko_open=1.1000, renko_close=1.1050, streak_count=3,
        streak_low=1.0950, streak_high=1.1050,
    )
    return {
        "pair": "DXY", "tv_symbol": "TVC:DXY", "asset_type": "INDEX", "currency": "USD",
        "live_price": 99.15, "daily_chg": daily_chg,
        "states": {"M": state, "W": state, "D": state},
        "px": {"M": 1, "W": 1, "D": 1},
        "bias": {"M": 1, "W": 1, "D": 1},
    }


def make_imp_row():
    """Ligne `compute_currency_imp`-shaped: deja entierement JSON-safe."""
    return {
        "pair": "DXY", "currency": "USD", "reference_price": 99.15,
        "renko": {"M": {"status": "BULL"}, "W": {"status": "BULL"}, "D": {"status": "BULL"}},
        "D1": {"imp_status": "BULL", "all_avg_pips": 10.0, "bull_avg_pips": 10.0, "bear_avg_pips": -5.0},
        "H1": {"imp_status": "BULL", "all_avg_pips": 5.0, "bull_avg_pips": 5.0, "bear_avg_pips": -2.0},
    }


class SerializeCurrencyIndexRowTests(unittest.TestCase):
    def test_round_trip_preserves_tfstate_fields(self):
        row = make_index_row()
        restored = _deserialize_currency_index_row(_serialize_currency_index_row(row))

        self.assertEqual(restored["daily_chg"], row["daily_chg"])
        self.assertIsInstance(restored["states"]["M"], TFState)
        self.assertEqual(restored["states"]["M"], row["states"]["M"])

    def test_serialized_form_is_json_safe(self):
        serialized = _serialize_currency_index_row(make_index_row())
        # Ne doit pas lever: uniquement des types JSON natifs.
        json.dumps(serialized)


class SaveLoadCurrencySnapshotTests(unittest.TestCase):
    def test_round_trip_returns_equivalent_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "currency_snapshot.json"
            index_by_currency = {"USD": make_index_row()}
            imp_by_currency = {"USD": make_imp_row()}
            save_currency_snapshot(path, index_by_currency, imp_by_currency)

            loaded = load_currency_snapshot(path)

            self.assertIsNotNone(loaded)
            loaded_index, loaded_imp = loaded
            self.assertEqual(loaded_index["USD"]["daily_chg"], 0.5)
            self.assertIsInstance(loaded_index["USD"]["states"]["M"], TFState)
            self.assertEqual(loaded_imp, imp_by_currency)

    def test_missing_file_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(load_currency_snapshot(Path(tmp) / "absent.json"))

    def test_corrupt_file_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "currency_snapshot.json"
            path.write_text("{not valid json", encoding="utf-8")
            self.assertIsNone(load_currency_snapshot(path))

    def test_stale_snapshot_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "currency_snapshot.json"
            old = datetime.now(timezone.utc) - timedelta(seconds=CURRENCY_SNAPSHOT_MAX_AGE + 60)
            path.write_text(json.dumps({
                "generated_at": old.isoformat(), "index_by_currency": {}, "imp_by_currency": {},
            }), encoding="utf-8")
            self.assertIsNone(load_currency_snapshot(path))

    def test_snapshot_just_within_max_age_is_reused(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "currency_snapshot.json"
            recent = datetime.now(timezone.utc) - timedelta(seconds=CURRENCY_SNAPSHOT_MAX_AGE - 30)
            path.write_text(json.dumps({
                "generated_at": recent.isoformat(), "index_by_currency": {}, "imp_by_currency": {},
            }), encoding="utf-8")
            self.assertIsNotNone(load_currency_snapshot(path))

    def test_future_timestamp_is_rejected(self):
        # Horloge desynchronisee entre deux etapes du meme job CI: mieux vaut
        # refetcher que de faire confiance a un instantane "du futur".
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "currency_snapshot.json"
            future = datetime.now(timezone.utc) + timedelta(seconds=60)
            path.write_text(json.dumps({
                "generated_at": future.isoformat(), "index_by_currency": {}, "imp_by_currency": {},
            }), encoding="utf-8")
            self.assertIsNone(load_currency_snapshot(path))

    def test_custom_max_age_is_honored(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "currency_snapshot.json"
            ten_seconds_old = datetime.now(timezone.utc) - timedelta(seconds=10)
            path.write_text(json.dumps({
                "generated_at": ten_seconds_old.isoformat(), "index_by_currency": {}, "imp_by_currency": {},
            }), encoding="utf-8")
            self.assertIsNone(load_currency_snapshot(path, max_age_seconds=5))
            self.assertIsNotNone(load_currency_snapshot(path, max_age_seconds=30))


class FetchOrLoadCurrencyDataTests(unittest.TestCase):
    def test_reuses_a_fresh_snapshot_without_fetching_live(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "currency_snapshot.json"
            save_currency_snapshot(path, {"USD": make_index_row()}, {"USD": make_imp_row()})

            with mock.patch("imp_trend_29pairs.fetch_currency_index_rows") as fetch_index, \
                 mock.patch("imp_trend_29pairs.fetch_currency_imp_rows") as fetch_imp:
                index_by_currency, imp_by_currency = fetch_or_load_currency_data(
                    path, CURRENCY_SNAPSHOT_MAX_AGE, 14, 300, 50, 5000, 2500, 2500, 14, 50,
                )

            fetch_index.assert_not_called()
            fetch_imp.assert_not_called()
            self.assertIn("USD", index_by_currency)
            self.assertIn("USD", imp_by_currency)

    def test_fetches_live_and_writes_a_snapshot_when_none_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "currency_snapshot.json"

            with mock.patch(
                "imp_trend_29pairs.fetch_currency_index_rows", return_value={"USD": make_index_row()},
            ) as fetch_index, mock.patch(
                "imp_trend_29pairs.fetch_currency_imp_rows", return_value={"USD": make_imp_row()},
            ) as fetch_imp:
                index_by_currency, imp_by_currency = fetch_or_load_currency_data(
                    path, CURRENCY_SNAPSHOT_MAX_AGE, 14, 300, 50, 5000, 2500, 2500, 14, 50,
                )

            fetch_index.assert_called_once()
            fetch_imp.assert_called_once()
            self.assertIn("USD", index_by_currency)
            self.assertTrue(path.exists())  # ecrit pour les scripts suivants du meme run CI

    def test_snapshot_disabled_always_fetches_live(self):
        with mock.patch(
            "imp_trend_29pairs.fetch_currency_index_rows", return_value={"USD": make_index_row()},
        ) as fetch_index, mock.patch(
            "imp_trend_29pairs.fetch_currency_imp_rows", return_value={"USD": make_imp_row()},
        ) as fetch_imp:
            fetch_or_load_currency_data(None, CURRENCY_SNAPSHOT_MAX_AGE, 14, 300, 50, 5000, 2500, 2500, 14, 50)

        fetch_index.assert_called_once()
        fetch_imp.assert_called_once()


if __name__ == "__main__":
    unittest.main()
