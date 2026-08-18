import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from renko_full_alignment_29pairs import (
    FOREX_INDEX_ASSETS,
    _streak_note,
    FOREX_PAIR_ASSETS,
    all_index_status_rows,
    all_pair_status_rows,
    currency_spread,
    signed_strength,
    signed_sync_product,
    sync_marker,
    attach_premium_currency_profiles,
    assets_for_scope,
    format_full_alignment_message,
    full_alignment_direction,
    strength_score,
    mid_alignment_candidate,
    raw_alignment_score,
    select_full_alignment_rows,
    select_index_daily_chg_rows,
    sar_break_state_from_close_sar,
    select_mid_alignment_candidates,
    select_mid_sar_rows,
    update_mid_sar_history,
    update_price_trends,
    write_index_sidecar,
)


PARIS = ZoneInfo("Europe/Paris")


def no_h1_backfill(_tv_symbol, _clock):
    """Stub `h1_close_lookup`: simule une cloture H1 indisponible, pour garder
    `update_price_trends` deterministe en test (aucun appel reseau) et
    reproduire l'ancien comportement (repli sur le prix live du run)."""
    return None


def row(pair, m, w, d, daily_chg=None, streaks=None, bias=None):
    """Ligne de scan factice.

    `bias` = STATUS BULL/BEAR par TF (defaut: le PX lui-meme) et `streaks` =
    longueur du streak Renko dans le sens du STATUS (defaut: 1, donc engage).
    Passer 0 sur un TF reproduit un `W+(0)`: statut haussier mais derniere
    brique opposee."""
    px = {"M": m, "W": w, "D": d}
    bias = bias or dict(px)
    streaks = {"M": 1, "W": 1, "D": 1} | (streaks or {})
    states = {}
    for tf in ("M", "W", "D"):
        engaged = int(streaks[tf])
        states[tf] = SimpleNamespace(
            green_streak=engaged if bias[tf] == 1 else 0,
            red_streak=engaged if bias[tf] == -1 else 0,
        )
    return {
        "pair": pair,
        "px": px,
        "bias": bias,
        "states": states,
        "asset_type": "PAIR",
        "h1_price": 1.23456,
        "live_price": 1.23456,
        "daily_chg": daily_chg,
    }


def index_row(pair, m, w, d, daily_chg=None, streaks=None, bias=None):
    item = row(pair, m, w, d, daily_chg=daily_chg, streaks=streaks, bias=bias)
    item["asset_type"] = "INDEX"
    item["currency"] = {
        "DXY": "USD",
        "EXY": "EUR",
        "BXY": "GBP",
        "JXY": "JPY",
        "SXY": "CHF",
        "CXY": "CAD",
        "AXY": "AUD",
        "ZXY": "NZD",
    }.get(pair)
    return item


class FullAlignmentScannerTests(unittest.TestCase):

    def test_research_sidecar_exposes_only_final_selected_pairs(self):
        rows = [row("EURUSD", 1, 1, 1, daily_chg=0.2),
                row("GBPUSD", 1, 1, -1, daily_chg=0.1)]
        selected = [rows[0]]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sidecar.json"
            write_index_sidecar(path, {}, rows,
                                datetime(2026, 8, 14, 10, tzinfo=PARIS), selected)
            payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(list(payload["selected_pairs"]), ["EURUSD"])
        self.assertEqual(payload["selected_pairs"]["EURUSD"]["direction"], 1)
    def test_live_price_trends_compare_with_06h_and_previous_run(self):
        first = row("AUDCHF", 1, 1, 1)
        first["live_price"] = 0.57000
        state, trends = update_price_trends(
            {}, [first], datetime(2026, 7, 16, 6, 15, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )
        self.assertEqual(trends["AUDCHF"]["vs_06h"], "→")
        self.assertEqual(trends["AUDCHF"]["vs_previous"], "→")

        second = row("AUDCHF", 1, 1, 1)
        second["live_price"] = 0.57120
        state, trends = update_price_trends(
            state, [second], datetime(2026, 7, 16, 7, 15, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )
        self.assertEqual(trends["AUDCHF"]["vs_06h"], "↑")
        self.assertEqual(trends["AUDCHF"]["vs_previous"], "↑")

        third = row("AUDCHF", 1, 1, 1)
        third["live_price"] = 0.57080
        _, trends = update_price_trends(
            state, [third], datetime(2026, 7, 16, 8, 15, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )
        self.assertEqual(trends["AUDCHF"]["vs_06h"], "↑")
        self.assertEqual(trends["AUDCHF"]["vs_previous"], "↓")

    def test_live_price_trends_reset_06h_reference_each_day(self):
        previous = {
            "date": "2026-07-15",
            "pairs": {"AUDJPY": {
                "baseline_price": 110.0, "baseline_ready": True,
                "baseline_direction": 1, "previous": 111.0,
            }},
        }
        current = row("AUDJPY", 1, 1, 1)
        current["live_price"] = 112.386
        state, trends = update_price_trends(
            previous, [current], datetime(2026, 7, 16, 6, 15, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )
        self.assertEqual(state["pairs"]["AUDJPY"]["baseline_price"], 112.386)
        self.assertEqual(trends["AUDJPY"]["vs_06h"], "→")
        self.assertEqual(trends["AUDJPY"]["vs_previous"], "→")

    def test_stale_baseline_ready_without_a_price_self_heals(self):
        """Regression: un etat marque `baseline_ready` mais sans
        `baseline_price` exploitable (ex: reliquat de l'ancien champ
        `baseline_07h` avant renommage) ne doit pas rester bloque sans
        reference pour le reste de la journee: le code doit recapturer."""
        previous = {
            "date": "2026-08-18",
            "pairs": {"AUDJPY": {
                "baseline_ready": True, "baseline_price": None,
                "baseline_direction": 0, "previous": 112.0,
            }},
        }
        current = row("AUDJPY", 1, 1, 1, daily_chg=0.20)
        current["live_price"] = 112.500
        state, trends = update_price_trends(
            previous, [current], datetime(2026, 8, 18, 10, 0, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )
        self.assertEqual(state["pairs"]["AUDJPY"]["baseline_price"], 112.500)
        self.assertEqual(state["pairs"]["AUDJPY"]["baseline_direction"], 1)
        self.assertEqual(trends["AUDJPY"]["trend_icon"], "✅")

    def test_baseline_backfills_the_h1_close_near_6h_when_capture_is_delayed(self):
        """Meme si la capture n'a lieu qu'en fin de journee (run du matin
        manque, etat corrompu repare plus tard...), la reference doit etre la
        cloture H1 proche de 6H fournie par `h1_close_lookup`, pas le prix du
        moment de la reparation."""
        calls = []

        def stub_h1(tv_symbol, clock):
            calls.append((tv_symbol, clock))
            return 0.56800

        now = datetime(2026, 8, 18, 21, 0, tzinfo=PARIS)
        late = row("AUDCHF", 1, 1, 1, daily_chg=0.20)
        late["tv_symbol"] = "OANDA:AUDCHF"
        late["live_price"] = 0.58000  # tres different de la cloture H1 de 6H
        state, _ = update_price_trends({}, [late], now, h1_close_lookup=stub_h1)

        self.assertEqual(state["pairs"]["AUDCHF"]["baseline_price"], 0.56800)
        self.assertEqual(calls, [("OANDA:AUDCHF", now)])

    def test_baseline_falls_back_to_the_live_price_without_h1_data(self):
        """Si `h1_close_lookup` ne trouve rien (feed en panne, historique
        trop court), la capture retombe sur le prix live du run plutot que
        de rester sans reference."""
        current = row("AUDCHF", 1, 1, 1, daily_chg=0.20)
        current["tv_symbol"] = "OANDA:AUDCHF"
        current["live_price"] = 0.58000
        state, _ = update_price_trends(
            {}, [current], datetime(2026, 8, 18, 21, 0, tzinfo=PARIS),
            h1_close_lookup=lambda *_: None,
        )

        self.assertEqual(state["pairs"]["AUDCHF"]["baseline_price"], 0.58000)

    def test_trend_icon_marks_continuation_and_reversal_with_pip_threshold(self):
        # Premier signal a 6H: CHG%D haussier -> reference haussiere figee.
        first = row("AUDCHF", 1, 1, 1, daily_chg=0.20)
        first["live_price"] = 0.57000
        state, trends = update_price_trends(
            {}, [first], datetime(2026, 7, 16, 6, 15, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )
        self.assertEqual(trends["AUDCHF"]["trend_icon"], "✅")

        # +8 pips depuis la reference: toujours dans le sens du signal.
        up = row("AUDCHF", 1, 1, 1, daily_chg=0.20)
        up["live_price"] = 0.57080
        state, trends = update_price_trends(
            state, [up], datetime(2026, 7, 16, 7, 0, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )
        self.assertEqual(trends["AUDCHF"]["trend_icon"], "✅")

        # -3 pips: repli sous le seuil de 5 pips -> reste aligne.
        small_pullback = row("AUDCHF", 1, 1, 1, daily_chg=0.20)
        small_pullback["live_price"] = 0.57050
        state, trends = update_price_trends(
            state, [small_pullback], datetime(2026, 7, 16, 7, 15, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )
        self.assertEqual(trends["AUDCHF"]["trend_icon"], "✅")

        # -9 pips depuis la reference (0.57000): au-dela du seuil -> retourne.
        reversal = row("AUDCHF", 1, 1, 1, daily_chg=0.20)
        reversal["live_price"] = 0.56910
        _, trends = update_price_trends(
            state, [reversal], datetime(2026, 7, 16, 7, 30, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )
        self.assertEqual(trends["AUDCHF"]["trend_icon"], "❌")

    def test_trend_icon_handles_bearish_first_signal_and_jpy_pip_size(self):
        # Premier signal baissier sur une paire JPY (pip = 0.01).
        first = row("AUDJPY", -1, -1, -1, daily_chg=-0.15)
        first["live_price"] = 113.500
        state, _ = update_price_trends(
            {}, [first], datetime(2026, 7, 16, 6, 5, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )

        # +3 pips (0.03): sous le seuil -> reste aligne.
        small_bounce = row("AUDJPY", -1, -1, -1, daily_chg=-0.15)
        small_bounce["live_price"] = 113.530
        state, trends = update_price_trends(
            state, [small_bounce], datetime(2026, 7, 16, 6, 20, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )
        self.assertEqual(trends["AUDJPY"]["trend_icon"], "✅")

        # +7 pips (0.07): au-dela du seuil, a l'oppose du signal baissier.
        _, trends = update_price_trends(
            state, [{**small_bounce, "live_price": 113.570}],
            datetime(2026, 7, 16, 6, 35, tzinfo=PARIS),
            h1_close_lookup=no_h1_backfill,
        )
        self.assertEqual(trends["AUDJPY"]["trend_icon"], "❌")

    def test_trend_icon_stays_empty_without_a_directional_first_signal(self):
        neutral = row("AUDCHF", 1, 1, 1, daily_chg=0.0)
        neutral["live_price"] = 0.57000
        _, trends = update_price_trends(
            {}, [neutral], datetime(2026, 7, 16, 6, 15, tzinfo=PARIS), h1_close_lookup=no_h1_backfill
        )
        self.assertEqual(trends["AUDCHF"]["trend_icon"], "")

    def test_message_shows_the_trend_icon_on_pair_lines(self):
        audnzd = row("AUDNZD", 1, 1, 1, daily_chg=0.10)
        audnzd["live_price"] = 1.20822

        message = format_full_alignment_message(
            [],
            pair_status_rows=all_pair_status_rows([audnzd]),
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
            price_trends={"AUDNZD": {"price": 1.20822, "trend_icon": "✅"}},
        )

        self.assertIn("🟢 AUDNZD (1.20822) ✅", message)

    def test_message_no_longer_renders_the_strict_alignment_list(self):
        """La liste des paires en alignement strict a ete retiree du message:
        elle faisait doublon avec la section PAIRES, ou ces paires figurent
        avec leur score d'intensite."""
        pair = row("AUDCHF", 1, 1, 1, daily_chg=0.34)
        pair["live_price"] = 0.57150
        selected = select_full_alignment_rows([
            pair,
            index_row("AXY", 1, 1, 1, daily_chg=0.03),
        ])

        message = format_full_alignment_message(
            selected,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
            price_trends={"AUDCHF": {
                "price": 0.57150, "vs_07h": "↑", "vs_previous": "↓",
            }},
        )

        self.assertIn("📊 FULL MOMENTUM", message)
        self.assertNotIn("FULL ALIGNMENT", message)
        # Ni prix live, ni fleches de tendance, ni CHG%D brut.
        self.assertNotIn("0.57150", message)
        self.assertNotIn("↑↓", message)
        self.assertNotIn("(+0.03%)", message)

    def test_universe_excludes_gold_and_keeps_28_pairs(self):
        pairs = [asset["pair"] for asset in FOREX_PAIR_ASSETS]

        self.assertNotIn("XAUUSD", pairs)
        self.assertEqual(len(pairs), 28)
        self.assertNotIn("XAUUSD", [a["pair"] for a in assets_for_scope("all")])
        self.assertNotIn("XAUUSD", [a["pair"] for a in assets_for_scope("pairs")])

    def test_default_universe_includes_forex_indices(self):
        index_symbols = {asset["tv_symbol"] for asset in FOREX_INDEX_ASSETS}

        self.assertEqual(index_symbols, {
            "TVC:DXY",
            "TVC:EXY",
            "TVC:BXY",
            "TVC:JXY",
            "TVC:SXY",
            "TVC:CXY",
            "TVC:AXY",
            "TVC:ZXY",
        })
        self.assertTrue(index_symbols.issubset({
            asset["tv_symbol"] for asset in assets_for_scope("all")
        }))
        self.assertEqual(
            {asset["pair"] for asset in assets_for_scope("indices")},
            {"DXY", "EXY", "BXY", "JXY", "SXY", "CXY", "AXY", "ZXY"},
        )

    def test_detects_strict_bull_and_bear_alignment(self):
        self.assertEqual(full_alignment_direction(row("GBPJPY", 1, 1, 1)), 1)
        self.assertEqual(full_alignment_direction(row("CADCHF", -1, -1, -1)), -1)
        self.assertEqual(raw_alignment_score(row("GBPJPY", 1, 1, 1)), 100.0)
        self.assertEqual(raw_alignment_score(row("CADCHF", -1, -1, -1)), -100.0)

    def test_rejects_inside_or_mixed_alignment(self):
        self.assertEqual(full_alignment_direction(row("AUDJPY", 1, 0, 1)), 0)
        self.assertEqual(full_alignment_direction(row("EURJPY", 1, 1, -1)), 0)

    def test_sar_break_state_detects_sar_crosses(self):
        state = sar_break_state_from_close_sar(
            closes=[9.0, 11.0, 8.0, 12.0],
            sar_values=[10.0, 10.0, 10.0, 10.0],
        )

        self.assertEqual(
            [(event["direction"], event["index"]) for event in state["events"]],
            [(1, 1), (-1, 2), (1, 3)],
        )
        self.assertEqual(state["last_sar_break_direction"], 1)
        self.assertEqual(state["last_sar_break_kind"], "SAR BULL")
        self.assertEqual(state["last_bar_sar_break_direction"], 1)
        self.assertEqual(state["last_bar_sar_break_kind"], "SAR BULL")

    def test_selects_only_full_alignment_rows(self):
        selected = select_full_alignment_rows([
            row("GBPJPY", 1, 1, 1),
            row("AUDJPY", 1, 0, 1),
            row("CADCHF", -1, -1, -1),
            row("EURJPY", 1, 1, 1),
            index_row("BXY", 1, 1, 1),
            index_row("JXY", -1, -1, -1),
        ])

        self.assertEqual(
            [item["pair"] for item in selected],
            ["EURJPY", "GBPJPY", "CADCHF", "BXY", "JXY"],
        )

    def test_selects_index_daily_chg_rows_by_daily_chg_descending(self):
        selected = select_index_daily_chg_rows([
            row("GBPJPY", 1, 1, 1),
            index_row("JXY", -1, -1, -1, daily_chg=-0.44),
            index_row("DXY", 1, -1, 1, daily_chg=0.12),
            index_row("BXY", 1, 1, 1, daily_chg=0.31),
        ], exclude_pairs={"JXY"})

        self.assertEqual([item["pair"] for item in selected], ["BXY", "DXY"])

    def test_full_alignment_rows_are_sorted_by_daily_chg_descending(self):
        selected = select_full_alignment_rows([
            row("USDJPY", 1, 1, 1, daily_chg=-0.01),
            row("AUDJPY", 1, 1, 1, daily_chg=0.01),
            row("EURJPY", 1, 1, 1, daily_chg=0.02),
            row("NZDJPY", 1, 1, 1, daily_chg=-0.02),
            index_row("JXY", -1, -1, -1, daily_chg=-0.41),
            index_row("DXY", 1, 1, 1, daily_chg=0.20),
        ])

        self.assertEqual(
            [item["pair"] for item in selected],
            ["EURJPY", "AUDJPY", "USDJPY", "NZDJPY", "DXY", "JXY"],
        )

    def test_premium_currency_profile_marks_strong_weak_full_alignment_pairs(self):
        rows = [
            row("USDJPY", 1, 1, 1, daily_chg=0.01),
            row("USDCAD", 1, 1, 1, daily_chg=0.01),
            row("NZDJPY", 1, 1, 1, daily_chg=-0.03),
            index_row("DXY", 1, -1, 1, daily_chg=0.20),
            index_row("JXY", -1, -1, -1, daily_chg=-0.41),
            index_row("CXY", -1, 1, -1, daily_chg=-0.20),
            index_row("ZXY", -1, 1, -1, daily_chg=-0.37),
        ]
        selected = attach_premium_currency_profiles(select_full_alignment_rows(rows), rows)
        by_pair = {item["pair"]: item for item in selected}

        self.assertTrue(by_pair["USDJPY"]["premium_currency_profile"])
        self.assertFalse(by_pair["USDCAD"]["premium_currency_profile"])
        self.assertFalse(by_pair["NZDJPY"]["premium_currency_profile"])
        self.assertFalse(by_pair["JXY"]["premium_currency_profile"])

    def test_detects_mid_alignment_candidates_with_at_least_two_timeframes(self):
        self.assertEqual(mid_alignment_candidate(row("EURUSD", 1, -1, 1)), (1, "D/M"))
        self.assertEqual(mid_alignment_candidate(row("CADCHF", -1, 1, -1)), (-1, "D/M"))
        self.assertEqual(mid_alignment_candidate(row("AUDJPY", -1, 1, 1)), (1, "D/W"))
        self.assertEqual(mid_alignment_candidate(row("NZDCHF", 1, -1, -1)), (-1, "D/W"))
        self.assertEqual(mid_alignment_candidate(row("GBPUSD", 1, 1, -1)), (1, "W/M"))
        self.assertEqual(mid_alignment_candidate(row("USDCHF", -1, -1, 1)), (-1, "W/M"))
        self.assertEqual(mid_alignment_candidate(row("GBPJPY", 1, 1, 1)), (1, "M/W/D"))
        self.assertEqual(mid_alignment_candidate(row("CHFJPY", -1, -1, -1)), (-1, "M/W/D"))
        self.assertEqual(mid_alignment_candidate(row("EURJPY", 1, 0, -1)), (0, ""))

    def test_mid_sar_keeps_only_mid_alignment_directional_sar_breaks(self):
        candidates = select_mid_alignment_candidates([
            row("GBPJPY", 1, 1, 1),
            row("GBPUSD", 1, 1, -1),
            row("USDCHF", -1, -1, 1),
            row("EURUSD", 1, -1, 1),
            row("EURJPY", 1, -1, 1),
            row("CADCHF", -1, 1, -1),
            index_row("JXY", 1, -1, -1),
        ])
        by_pair = {item["pair"]: item for item in candidates}
        by_pair["GBPJPY"]["sar_break"] = {"last_bar_sar_break_direction": 1}
        by_pair["GBPUSD"]["sar_break"] = {"last_bar_sar_break_direction": 1}
        by_pair["USDCHF"]["sar_break"] = {"last_bar_sar_break_direction": -1}
        by_pair["EURUSD"]["sar_break"] = {"last_bar_sar_break_direction": 1}
        by_pair["EURJPY"]["sar_break"] = {"last_bar_sar_break_direction": -1}
        by_pair["CADCHF"]["sar_break"] = {"last_bar_sar_break_direction": -1}
        by_pair["JXY"]["sar_break"] = {"last_bar_sar_break_direction": -1}

        mid_sar_rows = select_mid_sar_rows(candidates)

        self.assertEqual([item["pair"] for item in mid_sar_rows], ["GBPJPY", "GBPUSD", "USDCHF"])

    def test_mid_sar_history_tracks_daily_window_without_duplicates(self):
        candidates = select_mid_alignment_candidates([
            row("GBPUSD", 1, 1, -1),
            row("EURUSD", 1, -1, 1),
            index_row("JXY", -1, -1, 1),
            index_row("SXY", 1, -1, -1),
        ])
        for item in candidates:
            item["sar_break"] = {
                "last_bar_sar_break_direction": 1 if item["pair"] in ("GBPUSD", "EURUSD") else -1
            }
        mid_sar_rows = select_mid_sar_rows(candidates)

        state, today = update_mid_sar_history(
            {},
            mid_sar_rows,
            datetime(2026, 7, 16, 8, 0, tzinfo=PARIS),
        )
        state, today = update_mid_sar_history(
            state,
            mid_sar_rows,
            datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertEqual(len(today["events"]), 2)
        by_pair = {event["pair"]: event for event in today["events"]}
        self.assertEqual(by_pair["GBPUSD"]["count"], 2)
        self.assertEqual(by_pair["GBPUSD"]["first_seen"], "2026-07-16T08:00+02:00")
        self.assertEqual(by_pair["GBPUSD"]["last_seen"], "2026-07-16T10:00+02:00")
        self.assertEqual(by_pair["GBPUSD"]["tf_pairs"], ["W/M"])
        self.assertEqual(by_pair["JXY"]["currency"], "JPY")
        self.assertEqual(by_pair["JXY"]["tf_pairs"], ["W/M"])

        state, unchanged = update_mid_sar_history(
            state,
            mid_sar_rows,
            datetime(2026, 7, 16, 1, 0, tzinfo=PARIS),
        )
        self.assertEqual(len(unchanged["events"]), 2)
        self.assertEqual(by_pair["GBPUSD"]["count"], 2)

    def test_message_is_compact(self):
        message = format_full_alignment_message(
            select_full_alignment_rows([row("GBPJPY", 1, 1, 1)]),
            pair_status_rows=all_pair_status_rows([row("GBPJPY", 1, 1, 1, daily_chg=0.10)]),
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn("📊 FULL MOMENTUM", message)
        self.assertIn("🟢 GBPJPY (1.23456)", message)
        self.assertNotIn("+100%", message)
        self.assertNotIn("M+/W+/D+", message)
        self.assertIn("2026-07-16 10:00 Paris", message)

    def test_message_never_marks_sar_breaks_with_flame(self):
        scanned = [
            row("GBPJPY", 1, 1, 1, daily_chg=0.10),
            index_row("JXY", -1, -1, -1, daily_chg=-0.44),
        ]
        for item in scanned:
            item["sar_break"] = {"last_bar_sar_break_direction": 1}

        message = format_full_alignment_message(
            select_full_alignment_rows(scanned),
            index_status_rows=all_index_status_rows(scanned),
            pair_status_rows=all_pair_status_rows(scanned),
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn("🟢 GBPJPY", message)
        self.assertIn("🔴 JPY", message)
        self.assertNotIn("🔥", message)

    def test_message_adds_daily_chg_to_full_alignment_rows_and_lists_all_indices(self):
        rows = [
            row("AUDJPY", 1, 1, 1, daily_chg=0.88),
            row("NZDJPY", 1, 1, 1, daily_chg=-0.03),
            row("USDCAD", 1, 1, 1, daily_chg=-0.01),
            row("USDJPY", 1, 1, 1, daily_chg=-0.001),
            index_row("JXY", -1, -1, -1, daily_chg=-0.44),
        ]
        selected = attach_premium_currency_profiles(select_full_alignment_rows(rows), rows)
        # La section 💱 montre le statut de TOUS les indices scannes, meme ceux
        # que le filtre de qualite ecarte (ici DXY: M+/W-/D+ incoherent) et ceux
        # deja listes en alignement strict (ici JXY).
        all_indices = all_index_status_rows([
            index_row("DXY", 1, -1, 1, daily_chg=0.12),
            index_row("EXY", 1, 1, -1, daily_chg=-0.05),
            index_row("JXY", -1, -1, -1, daily_chg=-0.44),
        ])

        message = format_full_alignment_message(
            selected,
            index_status_rows=all_indices,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertNotIn("🌸🌸", message)
        self.assertIn("💱 INDEX CHG%D", message)
        # Ligne compacte: seul le score d'intensite, sans CHG%D ni M/W/D.
        self.assertIn("🟢 USD (+0.12)", message)   # (3-2+1)/2 = 1.0 x 0.12
        self.assertIn("🔴 EUR (+0.10)", message)   # (3+2-1)/2 = 2.0 x 0.05
        self.assertIn("🔴 JPY (+1.32)", message)   # 3.0 x 0.44
        self.assertNotIn("USD +0.12%", message)
        self.assertNotIn("(M+ W- D+)", message)

    def test_streak_note_only_counts_timeframes_backed_by_a_streak(self):
        # GBPUSD sur TradingView: M+(3) W+(0) D+(1), STATUS BULL partout.
        # (3x1 + 2x0 + 1x1) / 2 = 2.0
        gbpusd = row("GBPUSD", 1, 1, 1, streaks={"M": 3, "W": 0, "D": 1})
        self.assertEqual(_streak_note(gbpusd), 2.0)

        # Monthly BEAR malgre un PX a 0, avec un streak rouge engage: M0(1) = -1.
        # Le Monthly pese 3 contre 1 au Daily: (-3 + 0 + 1) / 2 = -1.0, la
        # contradiction M vs D ne s'annule plus.
        mixed = row(
            "XAUUSD", 0, 1, 1,
            bias={"M": -1, "W": 1, "D": 1},
            streaks={"M": 1, "W": 0, "D": 3},
        )
        self.assertEqual(_streak_note(mixed), -1.0)

        # Aucun streak engage -> note nulle malgre un alignement PX parfait.
        self.assertEqual(
            _streak_note(row("EURUSD", 1, 1, 1, streaks={"M": 0, "W": 0, "D": 0})),
            0.0,
        )
        # Alignement complet: la normalisation ramene bien la note a 3.
        self.assertEqual(_streak_note(row("AUDJPY", 1, 1, 1)), 3.0)

    def test_streak_note_weights_monthly_above_daily(self):
        monthly_only = row("EURUSD", 1, 0, 0, streaks={"W": 0, "D": 0})
        daily_only = row("EURUSD", 0, 0, 1, streaks={"M": 0, "W": 0})

        self.assertEqual(_streak_note(monthly_only), 1.5)   # 3 / 2
        self.assertEqual(_streak_note(daily_only), 0.5)     # 1 / 2

    def test_strength_score_multiplies_streak_note_by_absolute_daily_chg(self):
        def score(*args, **kwargs):
            value = strength_score(index_row(*args, **kwargs))
            return round(value, 4) if value is not None else None

        self.assertEqual(score("AXY", 1, 1, 1, daily_chg=0.23), 0.69)
        # Magnitude: un CHG%D negatif ne rend pas le score negatif.
        self.assertEqual(score("CXY", 1, 1, 1, daily_chg=-0.03), 0.09)
        self.assertEqual(score("SXY", 0, -1, 0, daily_chg=0.26), 0.26)
        # Un TF dont le streak ne confirme pas ne compte plus dans le score.
        self.assertEqual(
            score("BXY", 1, 1, 1, daily_chg=0.10, streaks={"W": 0}), 0.20,
        )
        # Aucun TF engage -> score nul quel que soit le CHG%D.
        self.assertEqual(score("DXY", 0, 0, 0, daily_chg=0.40), 0.0)
        self.assertIsNone(score("EXY", 1, 1, 1))

    def test_index_status_rows_are_sorted_by_strength_score(self):
        scanned = [
            index_row("SXY", 0, -1, 0, daily_chg=0.26),   # 1 x 0.26 = 0.26
            index_row("AXY", 1, 1, 1, daily_chg=0.23),    # 3 x 0.23 = 0.69
            index_row("ZXY", 0, 1, 1, daily_chg=0.19),    # 2 x 0.19 = 0.38
            index_row("BXY", 1, 1, 1, daily_chg=0.08),    # 3 x 0.08 = 0.24
            index_row("CXY", 1, 1, 1, daily_chg=-0.03),   # 3 x 0.03 = 0.09
            index_row("JXY", -1, 0, -1, daily_chg=-0.11), # 2 x 0.11 = 0.22
        ]

        status = all_index_status_rows(scanned)

        # SXY (1 seul TF mais gros CHG%D) devance BXY (3 TF alignes, CHG%D faible).
        self.assertEqual(
            [item["pair"] for item in status],
            ["AXY", "ZXY", "SXY", "BXY", "JXY", "CXY"],
        )

    def test_index_status_rows_without_daily_chg_go_last(self):
        scanned = [
            index_row("EXY", 1, 1, 1),
            index_row("CXY", 1, 1, 1, daily_chg=-0.03),
        ]

        status = all_index_status_rows(scanned)

        self.assertEqual([item["pair"] for item in status], ["CXY", "EXY"])

    def test_pair_status_rows_cover_every_pair_sorted_by_strength_score(self):
        scanned = [
            row("EURUSD", 0, 1, 1, daily_chg=0.19),    # 2 x 0.19 = 0.38
            row("GBPJPY", 1, 1, 1, daily_chg=0.23),    # 3 x 0.23 = 0.69
            row("USDCHF", 0, 0, 0, daily_chg=0.42),    # 0 x 0.42 = 0.00
            row("AUDCAD", -1, 0, -1, daily_chg=-0.11), # 2 x 0.11 = 0.22
            index_row("AXY", 1, 1, 1, daily_chg=0.28), # ecarte: c'est un indice
        ]

        status = all_pair_status_rows(scanned)

        self.assertEqual(
            [item["pair"] for item in status],
            ["GBPJPY", "EURUSD", "AUDCAD", "USDCHF"],
        )

    def test_currency_spread_cumulates_a_strong_base_and_a_weak_quote(self):
        """AUD +0.60 face a un JPY faible (-0.15): les deux effets poussent
        AUDJPY a la hausse, donc 0.60 - (-0.15) = +0.75."""
        aud = index_row("AXY", 1, 1, 1, daily_chg=0.20)   # 3.0 x 0.20 = +0.60
        jpy = index_row("JXY", -1, -1, -1, daily_chg=-0.05)  # 3.0 x 0.05 = -0.15
        chf = index_row("SXY", 1, 1, 1, daily_chg=0.11)   # 3.0 x 0.11 = +0.33
        index_by_currency = {"AUD": aud, "JPY": jpy, "CHF": chf}

        self.assertAlmostEqual(signed_strength(aud) or 0.0, 0.60)
        self.assertAlmostEqual(signed_strength(jpy) or 0.0, -0.15)

        audjpy = currency_spread(row("AUDJPY", 1, 1, 1, daily_chg=0.08), index_by_currency)
        self.assertAlmostEqual(audjpy or 0.0, 0.75)

        # Deux devises de meme couleur: le cumul se ramene a un simple ecart.
        audchf = currency_spread(row("AUDCHF", 1, 1, 1, daily_chg=0.04), index_by_currency)
        self.assertAlmostEqual(audchf or 0.0, 0.27)

        # Sens inverse pour la paire miroir.
        jpyaud = currency_spread(row("JPYAUD", 1, 1, 1, daily_chg=0.01), index_by_currency)
        self.assertAlmostEqual(jpyaud or 0.0, -0.75)

    def test_currency_spread_is_absent_without_a_currency_index(self):
        index_by_currency = {"USD": index_row("DXY", 1, 1, 1, daily_chg=0.10)}

        # Cas defensif: un actif sans indice devise (l'or est desormais hors
        # univers, mais la fonction doit rester robuste).
        self.assertIsNone(
            currency_spread(row("XAUUSD", 1, 1, 1, daily_chg=0.50), index_by_currency),
        )
        # Un indice n'a pas de spread non plus.
        self.assertIsNone(
            currency_spread(index_row("DXY", 1, 1, 1, daily_chg=0.10), index_by_currency),
        )

    def test_message_shows_only_the_close_price_on_pair_lines(self):
        """La ligne d'une paire se limite au prix de cloture: plus de score,
        de produit ni de marqueur de synchronisation."""
        aud = index_row("AXY", 1, 1, 1, daily_chg=0.20)      # +0.60
        nzd = index_row("ZXY", 1, 1, 1, daily_chg=0.06)      # +0.18
        by_currency = {"AUD": aud, "NZD": nzd}
        # attendu +0.42, realise 3.0 x 0.13 = 0.39 -> produit 0.1638 (filtre
        # de selection uniquement, ne doit plus apparaitre sur la ligne).
        audnzd = row("AUDNZD", 1, 1, 1, daily_chg=0.13)

        message = format_full_alignment_message(
            [],
            index_status_rows=all_index_status_rows([aud, nzd]),
            pair_status_rows=all_pair_status_rows([audnzd], by_currency),
            index_by_currency=by_currency,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )
        lines = message.splitlines()

        self.assertIn("🟢 AUDNZD (1.23456)", lines)
        self.assertNotIn("0.1638", message)
        self.assertNotIn("🎯", message)
        self.assertNotIn("/(", message)
        # Les indices gardent leur score d'intensite.
        self.assertIn("🟢 AUD (+0.60)", lines)

    def test_signed_product_is_negative_when_the_pair_fights_its_currencies(self):
        eur = index_row("EXY", 1, 1, 1, daily_chg=0.06)   # +0.18
        nzd = index_row("ZXY", 1, 1, 1, daily_chg=0.065)  # +0.195
        by_currency = {"EUR": eur, "NZD": nzd}
        # EURNZD monte alors que ses devises impliquent une baisse.
        eurnzd = row("EURNZD", 1, 1, 1, daily_chg=0.01)

        product = signed_sync_product(eurnzd, by_currency)

        self.assertIsNotNone(product)
        self.assertLess(product or 0.0, 0.0)
        # Et la paire est donc retiree de la section.
        self.assertEqual(all_pair_status_rows([eurnzd], by_currency), [])

    def test_sync_marker_grades_the_two_engines(self):
        aud = index_row("AXY", 1, 1, 1, daily_chg=0.20)      # +0.60
        jpy = index_row("JXY", -1, -1, -1, daily_chg=-0.05)  # -0.15
        nzd = index_row("ZXY", 1, 1, 1, daily_chg=0.06)      # +0.18
        by_currency = {"AUD": aud, "JPY": jpy, "NZD": nzd}

        # Attendu +0.75, paire haussière et engagée -> synchronisée.
        engaged = row("AUDJPY", 1, 1, 1, daily_chg=0.09)
        self.assertEqual(sync_marker(engaged, by_currency), " 🎯")

        # Même carburant, mais aucun streak ne confirme -> en attente.
        waiting = row(
            "AUDJPY", 1, 1, 1, daily_chg=0.09,
            streaks={"M": 0, "W": 0, "D": 0},
        )
        self.assertEqual(sync_marker(waiting, by_currency), " ⏳")

        # La paire baisse alors que ses devises impliquent une hausse: elle
        # est filtree en amont, donc jamais marquee.
        against = row("AUDJPY", 1, 1, 1, daily_chg=-0.09)
        self.assertEqual(sync_marker(against, by_currency), "")

        # AUD +0.60 contre NZD +0.18: 0.42 d'ecart, deux devises fortes mais
        # l'ecart reste au-dessus du seuil.
        self.assertEqual(
            sync_marker(row("AUDNZD", 1, 1, 1, daily_chg=0.10), by_currency), " 🎯",
        )

    def test_sync_marker_stays_silent_without_enough_spread(self):
        nzd = index_row("ZXY", 1, 1, 1, daily_chg=0.06)   # +0.18
        chf = index_row("SXY", 1, 1, 1, daily_chg=0.11)   # +0.33
        by_currency = {"NZD": nzd, "CHF": chf}

        # Ecart de 0.15 seulement: pas de setup, donc pas de marqueur.
        self.assertEqual(
            sync_marker(row("NZDCHF", 1, 1, 1, daily_chg=-0.02), by_currency), "",
        )
        # Un actif sans indice devise n'est jamais marque.
        self.assertEqual(
            sync_marker(row("XAUUSD", 1, 1, 1, daily_chg=0.50), by_currency), "",
        )
        # Un indice non plus.
        self.assertEqual(sync_marker(chf, by_currency), "")

    def test_pair_section_drops_divergent_and_empty_pairs(self):
        aud = index_row("AXY", 1, 1, 1, daily_chg=0.20)      # +0.60
        jpy = index_row("JXY", -1, -1, -1, daily_chg=-0.05)  # -0.15
        nzd = index_row("ZXY", 1, 1, 1, daily_chg=0.06)      # +0.18
        by_currency = {"AUD": aud, "JPY": jpy, "NZD": nzd}

        scanned = [
            # attendu +0.75, realise 3.0 x 0.09 = 0.27 -> produit 0.2025
            row("AUDJPY", 1, 1, 1, daily_chg=0.09),
            # attendu +0.42, realise 3.0 x 0.10 = 0.30 -> produit 0.126
            row("AUDNZD", 1, 1, 1, daily_chg=0.10),
            # baissiere alors que ses devises impliquent une hausse -> retiree
            row("NZDJPY", 1, 1, 1, daily_chg=-0.09),
            # aucun streak confirme: produit nul -> retiree
            row("AUDNZD", 1, 1, 1, daily_chg=0.10, streaks={"M": 0, "W": 0, "D": 0}),
            # realise non nul mais qui s'affiche +0.00 -> retiree aussi
            row("AUDJPY", 1, 1, 1, daily_chg=0.001),
        ]

        kept = all_pair_status_rows(scanned, by_currency)

        self.assertEqual([item["pair"] for item in kept], ["AUDJPY", "AUDNZD"])

    def test_pair_section_keeps_every_pair_without_currency_indices(self):
        scanned = [
            row("AUDJPY", 1, 1, 1, daily_chg=0.09),
            row("NZDJPY", 1, 1, 1, daily_chg=-0.09),
        ]

        self.assertEqual(
            [item["pair"] for item in all_pair_status_rows(scanned)],
            ["AUDJPY", "NZDJPY"],
        )

    def test_message_keeps_only_the_top_pairs(self):
        by_currency = {
            "AUD": index_row("AXY", 1, 1, 1, daily_chg=0.20),
            "JPY": index_row("JXY", -1, -1, -1, daily_chg=-0.05),
            "CAD": index_row("CXY", -1, -1, -1, daily_chg=-0.04),
            "USD": index_row("DXY", -1, -1, -1, daily_chg=-0.03),
            "CHF": index_row("SXY", -1, -1, -1, daily_chg=-0.02),
            "NZD": index_row("ZXY", -1, -1, -1, daily_chg=-0.01),
            "GBP": index_row("BXY", 1, 1, 1, daily_chg=0.08),
        }
        # Sept paires eligibles, d'intensite decroissante.
        names = ["AUDJPY", "AUDCAD", "AUDUSD", "AUDCHF", "AUDNZD", "GBPJPY", "GBPCAD"]
        scanned = [
            row(name, 1, 1, 1, daily_chg=0.20 - index * 0.02)
            for index, name in enumerate(names)
        ]

        message = format_full_alignment_message(
            [],
            pair_status_rows=all_pair_status_rows(scanned, by_currency),
            index_by_currency=by_currency,
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )
        lines = message.splitlines()
        header = lines.index("💹 PAIRES CHG%D")
        pair_lines = [
            line for line in lines[header + 1:]
            if line and not line.startswith("⏰")
        ]

        self.assertEqual(len(pair_lines), 5)
        self.assertNotIn("GBPCAD", message)

    def test_message_lists_pair_status_section(self):
        scanned = [
            row("GBPJPY", 1, 1, 1, daily_chg=0.23),
            row("USDCHF", 0, 0, 0, daily_chg=0.42),
        ]

        message = format_full_alignment_message(
            [],
            pair_status_rows=all_pair_status_rows(scanned),
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
        )

        self.assertIn("💹 PAIRES CHG%D", message)
        self.assertIn("🟢 GBPJPY (1.23456)", message)
        self.assertIn("🟢 USDCHF (1.23456)", message)

    def test_index_status_rows_keep_every_scanned_index(self):
        scanned = [
            row("AUDJPY", 1, 1, 1, daily_chg=0.88),
            index_row("DXY", 1, -1, 1, daily_chg=0.002),
            index_row("JXY", -1, -1, -1, daily_chg=-0.44),
            index_row("AXY", 1, 1, 1, daily_chg=0.28),
        ]

        status = all_index_status_rows(scanned)

        # Cible: l'exhaustivite, pas l'ordre (couvert par le test de tri).
        self.assertEqual(sorted(item["pair"] for item in status), ["AXY", "DXY", "JXY"])
        # Le filtre de qualite reste plus strict: il alimente encore la selection
        # des paires via valid_index_currencies.
        self.assertEqual(
            [item["pair"] for item in select_index_daily_chg_rows(scanned)],
            ["AXY", "JXY"],
        )

    def test_message_drops_the_sections_that_were_removed(self):
        """MID SAR, profil premium et avertissement de divergence ne sont plus
        rendus: le message ne porte plus que les deux sections d'intensite."""
        scanned = [
            row("USDJPY", 1, 1, 1, daily_chg=0.01),
            index_row("DXY", 1, -1, 1, daily_chg=0.20),
            index_row("JXY", -1, -1, -1, daily_chg=-0.41),
        ]
        selected = attach_premium_currency_profiles(
            select_full_alignment_rows(scanned), scanned,
        )

        message = format_full_alignment_message(
            selected,
            select_mid_sar_rows(select_mid_alignment_candidates(scanned)),
            {"events": [{
                "pair": "GBPUSD", "asset_type": "PAIR", "direction": 1,
                "tf_pairs": ["W/M"],
                "first_seen": "2026-07-16T08:00+02:00",
                "last_seen": "2026-07-16T10:00+02:00",
            }]},
            index_status_rows=all_index_status_rows(scanned),
            pair_status_rows=all_pair_status_rows(scanned),
            now=datetime(2026, 7, 16, 12, 0, tzinfo=PARIS),
        )

        self.assertNotIn("MID SAR", message)
        self.assertNotIn("🌸🌸", message)
        self.assertNotIn("⚠️", message)
        self.assertNotIn("🔥", message)
        self.assertEqual(
            [line for line in message.splitlines() if line.startswith(("📊", "💱", "💹"))],
            ["📊 FULL MOMENTUM", "💱 INDEX CHG%D", "💹 PAIRES CHG%D"],
        )


if __name__ == "__main__":
    unittest.main()
