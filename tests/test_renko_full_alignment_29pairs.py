import json
import math
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from renko_full_alignment_29pairs import (
    FOREX_INDEX_ASSETS,
    _streak_note,
    FOREX_PAIR_ASSETS,
    _performance_general_bilan,
    all_index_status_rows,
    all_pair_status_rows,
    currency_pip_sum,
    currency_spread,
    eurusd_cross_check_lines,
    signed_strength,
    signed_sync_product,
    sync_marker,
    attach_premium_currency_profiles,
    assets_for_scope,
    format_full_alignment_message,
    full_alignment_direction,
    performance_report,
    performance_report_lines,
    strength_score,
    mid_alignment_candidate,
    pair_check_lines,
    pair_check_signals,
    raw_alignment_score,
    select_full_alignment_rows,
    select_index_daily_chg_rows,
    sar_break_state_from_close_sar,
    select_mid_alignment_candidates,
    select_mid_sar_rows,
    streaky_pairs,
    streaky_sar_confirmed,
    update_mid_sar_history,
    update_pairs_out_state,
    update_performance_state,
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

    def test_trend_pips_are_signed_in_favor_of_the_baseline_direction(self):
        # Signal BEAR a 6H, prix qui baisse ensuite de 14 pips: en faveur du
        # signal -> pips positifs malgre la baisse du prix brut.
        bear_first = row("AUDUSD", -1, -1, -1, daily_chg=-0.20)
        bear_first["live_price"] = 0.71004
        state, _ = update_price_trends(
            {}, [bear_first], datetime(2026, 7, 16, 6, 15, tzinfo=PARIS),
            h1_close_lookup=no_h1_backfill,
        )
        bear_later = row("AUDUSD", -1, -1, -1, daily_chg=-0.20)
        bear_later["live_price"] = 0.70864
        _, trends = update_price_trends(
            state, [bear_later], datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
            h1_close_lookup=no_h1_backfill,
        )
        self.assertAlmostEqual(trends["AUDUSD"]["trend_pips"], 14.0, places=6)
        self.assertEqual(trends["AUDUSD"]["trend_icon"], "✅")

        # Signal BULL a 6H, prix qui monte ensuite de 8 pips: pips positifs.
        bull_first = row("EURUSD", 1, 1, 1, daily_chg=0.20)
        bull_first["live_price"] = 1.15000
        state2, _ = update_price_trends(
            {}, [bull_first], datetime(2026, 7, 16, 6, 15, tzinfo=PARIS),
            h1_close_lookup=no_h1_backfill,
        )
        bull_later = row("EURUSD", 1, 1, 1, daily_chg=0.20)
        bull_later["live_price"] = 1.15080
        _, trends2 = update_price_trends(
            state2, [bull_later], datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
            h1_close_lookup=no_h1_backfill,
        )
        self.assertAlmostEqual(trends2["EURUSD"]["trend_pips"], 8.0, places=6)
        self.assertEqual(trends2["EURUSD"]["trend_icon"], "✅")

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

    def test_message_shows_the_trend_pips_on_pair_lines(self):
        audusd = row("AUDUSD", -1, -1, -1, daily_chg=-0.20)
        audusd["live_price"] = 0.70864

        message = format_full_alignment_message(
            [],
            pair_status_rows=all_pair_status_rows([audusd]),
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
            price_trends={
                "AUDUSD": {"price": 0.70864, "trend_icon": "✅", "trend_pips": 14.0},
            },
        )

        self.assertIn("🔴 AUDUSD (0.70864) ✅ +14.0 pips", message)

        # Sans trend_icon (pas de sens exploitable), aucun texte de pips.
        message_no_trend = format_full_alignment_message(
            [],
            pair_status_rows=all_pair_status_rows([audusd]),
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
            price_trends={"AUDUSD": {"price": 0.70864, "trend_icon": ""}},
        )
        self.assertIn("🔴 AUDUSD (0.70864)", message_no_trend)
        self.assertNotIn("pips", message_no_trend)

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
        # La derniere cassure (index 3) est la bougie la plus recente (index
        # 3 sur 4 bougies): 0 bougie d'ecart.
        self.assertEqual(state["bars_since_last_break"], 0)

    def test_sar_break_state_bars_since_last_break_counts_backwards(self):
        # Cassure a l'index 1 (haussiere), puis deux bougies calmes (2, 3)
        # qui ne traversent plus le SAR: la derniere cassure remonte a 2
        # bougies (index 3 - index 1).
        state = sar_break_state_from_close_sar(
            closes=[9.0, 11.0, 10.5, 10.8],
            sar_values=[10.0, 10.0, 10.0, 10.0],
        )
        self.assertEqual(state["bars_since_last_break"], 2)

        # Aucune cassure du tout -> None.
        no_break = sar_break_state_from_close_sar(
            closes=[10.5, 10.6, 10.7], sar_values=[10.0, 10.0, 10.0],
        )
        self.assertIsNone(no_break["bars_since_last_break"])

    def test_streaky_sar_confirmed_stays_true_within_the_window(self):
        fresh = {"last_sar_break_direction": 1, "bars_since_last_break": 0}
        within_window = {"last_sar_break_direction": 1, "bars_since_last_break": 2}
        outside_window = {"last_sar_break_direction": 1, "bars_since_last_break": 3}
        wrong_direction = {"last_sar_break_direction": -1, "bars_since_last_break": 0}
        no_break = {"last_sar_break_direction": 0, "bars_since_last_break": None}

        self.assertTrue(streaky_sar_confirmed(fresh, 1))
        self.assertTrue(streaky_sar_confirmed(within_window, 1))
        self.assertFalse(streaky_sar_confirmed(outside_window, 1))
        self.assertFalse(streaky_sar_confirmed(wrong_direction, 1))
        self.assertFalse(streaky_sar_confirmed(no_break, 1))
        self.assertFalse(streaky_sar_confirmed(None, 1))

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

    def test_pairs_out_lists_rank_dropouts_without_warning(self):
        day = datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)
        run1 = [{"pair": f"P{i}"} for i in range(5)]  # P0..P4, tous dans le TOP5
        state, entries1 = update_pairs_out_state({}, run1, day)
        self.assertEqual(entries1, [])  # rien n'est encore sorti

        # P4 retombe au 6e rang, remplace par P5 en tete du TOP5. Il reste
        # present dans la liste filtree: pas de warning.
        run2 = [{"pair": "P0"}, {"pair": "P1"}, {"pair": "P2"}, {"pair": "P3"},
                {"pair": "P5"}, {"pair": "P4"}]
        _, entries2 = update_pairs_out_state(state, run2, day + timedelta(minutes=30))

        self.assertEqual([(e["pair"], e["warning"]) for e in entries2], [("P4", False)])
        self.assertEqual(entries2[0]["out_since_paris"], "10:30")

    def test_pairs_out_marks_warning_when_the_pair_leaves_the_filter_entirely(self):
        day = datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)
        first_run = [{"pair": f"P{i}"} for i in range(5)]
        state, _ = update_pairs_out_state({}, first_run, day)

        # P2 disparait completement du filtre (marge cassee), plus seulement
        # classe plus bas: absent de la liste filtree elle-meme.
        second_run = [{"pair": "P0"}, {"pair": "P1"}, {"pair": "P3"}, {"pair": "P4"}]
        _, entries = update_pairs_out_state(state, second_run, day + timedelta(minutes=30))

        self.assertEqual([e["pair"] for e in entries], ["P2"])
        self.assertTrue(entries[0]["warning"])

    def test_pairs_out_since_persists_until_return_then_resets_on_new_exit(self):
        day = datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)
        # P5 en tete du TOP5, P4 hors classement ce run-la.
        run_p5_in = [{"pair": "P5"}, {"pair": "P0"}, {"pair": "P1"},
                     {"pair": "P2"}, {"pair": "P3"}]
        state, entries0 = update_pairs_out_state({}, run_p5_in, day)
        self.assertEqual(entries0, [])

        # P5 tombe au 6e rang, remplace par P4 en tete: toujours filtre,
        # juste classe plus bas.
        run_p5_out = [{"pair": "P0"}, {"pair": "P1"}, {"pair": "P2"},
                      {"pair": "P3"}, {"pair": "P4"}, {"pair": "P5"}]
        state, entries1 = update_pairs_out_state(state, run_p5_out, day + timedelta(minutes=15))
        self.assertEqual(entries1[0]["pair"], "P5")
        self.assertEqual(entries1[0]["out_since_paris"], "10:15")

        # 30 min plus tard, toujours hors TOP5 -> horodatage inchange.
        state, entries2 = update_pairs_out_state(state, run_p5_out, day + timedelta(minutes=45))
        self.assertEqual(entries2[0]["out_since_paris"], "10:15")

        # P5 revient dans le TOP5 -> disparait de PAIRES OUT.
        state, entries3 = update_pairs_out_state(state, run_p5_in, day + timedelta(hours=1))
        self.assertNotIn("P5", [e["pair"] for e in entries3])

        # P5 ressort plus tard -> nouvel horodatage, pas l'ancien 10:15.
        _, entries4 = update_pairs_out_state(state, run_p5_out, day + timedelta(hours=2))
        p5_entry = next(e for e in entries4 if e["pair"] == "P5")
        self.assertEqual(p5_entry["out_since_paris"], "12:00")

    def test_pairs_out_state_resets_each_day(self):
        previous = {
            "date": "2026-07-15",
            "ever_top5": ["P0", "P1", "P2", "P3", "P4", "P5"],
            "out_since": {"P5": "10:00"},
        }
        run = [{"pair": f"P{i}"} for i in range(5)]  # P5 absent aujourd'hui

        state, entries = update_pairs_out_state(
            previous, run, datetime(2026, 7, 16, 9, 0, tzinfo=PARIS),
        )

        self.assertEqual(entries, [])
        self.assertEqual(state["ever_top5"], ["P0", "P1", "P2", "P3", "P4"])

    def test_message_renders_pairs_out_section(self):
        rows_by_pair = {
            "GBPCAD": {"daily_chg": -0.12, "live_price": 1.90500},
            "NZDCAD": {"daily_chg": 0.05, "live_price": 0.81600},
        }

        message = format_full_alignment_message(
            [],
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
            rows_by_pair=rows_by_pair,
            pairs_out=[
                {"pair": "GBPCAD", "warning": False, "out_since_paris": "09:15"},
                {"pair": "NZDCAD", "warning": True, "out_since_paris": "08:40"},
            ],
        )

        self.assertIn("📤 PAIRES OUT", message)
        self.assertIn("🔴 GBPCAD (1.90500) (depuis 09:15)", message)
        self.assertIn("🟢 NZDCAD (0.81600) ⚠️ (depuis 08:40)", message)

    def test_performance_state_anchors_entry_to_first_top5_appearance_not_6h(self):
        """Cas 1 (USDJPY, 19/08/2026, cf. l'analyse Antigravity): une paire
        qui entre en cours de journee avec deja +23.4 pips accumules depuis
        6H (CHG%D) ne doit pas compter ces +23.4 pips -- seul le mouvement
        depuis son ENTREE reelle dans le TOP5 est un gain 'copy-trading'."""
        entry_run = datetime(2026, 8, 19, 14, 18, tzinfo=PARIS)
        state, newly_closed = update_performance_state(
            {}, ["USDJPY"], ["USDJPY"],
            {"USDJPY": {"trend_pips": 23.4, "trend_icon": "✅"}}, entry_run,
        )
        self.assertEqual(newly_closed, [])
        self.assertEqual(state["open_trades"]["USDJPY"]["entry_pips"], 23.4)
        self.assertEqual(state["open_trades"]["USDJPY"]["last_pips"], 0.0)

        later_run = datetime(2026, 8, 19, 15, 18, tzinfo=PARIS)
        state, newly_closed2 = update_performance_state(
            state, ["USDJPY"], ["USDJPY"],
            {"USDJPY": {"trend_pips": 93.6, "trend_icon": "✅"}}, later_run,
        )
        self.assertEqual(newly_closed2, [])
        # Gain reel depuis 14h18: 93.6 - 23.4 = +70.2, PAS +93.6 (le brut du bot).
        self.assertAlmostEqual(state["open_trades"]["USDJPY"]["last_pips"], 70.2, places=6)

    def test_performance_state_ignores_a_pair_entering_already_invalidated(self):
        """Cas 2 (AUDUSD): une paire qui apparait pour la 1ere fois deja en
        ❌ n'est jamais ouverte -- un trader reel ne prend pas un signal deja
        casse. Impact 0.0 pip, et ca reste vrai meme si elle repasse en ✅
        plus tard (une seule entree possible par paire et par jour)."""
        run = datetime(2026, 8, 19, 15, 9, tzinfo=PARIS)
        state, newly_closed = update_performance_state(
            {}, ["AUDUSD"], ["AUDUSD"],
            {"AUDUSD": {"trend_pips": -43.4, "trend_icon": "❌"}}, run,
        )
        self.assertEqual(newly_closed, [])
        self.assertEqual(state["open_trades"]["AUDUSD"], {"status": "ignored", "last_pips": 0.0})

        later = datetime(2026, 8, 19, 16, 0, tzinfo=PARIS)
        state, newly_closed2 = update_performance_state(
            state, ["AUDUSD"], ["AUDUSD"],
            {"AUDUSD": {"trend_pips": 10.0, "trend_icon": "✅"}}, later,
        )
        self.assertEqual(newly_closed2, [])
        self.assertEqual(state["open_trades"]["AUDUSD"]["status"], "ignored")

    def test_performance_state_stops_on_first_cross_after_entry(self):
        """Cas 3 (GBPAUD): plusieurs runs sans croix, puis une 1ere ❌ ->
        cloture immediate, sur l'ecart reel depuis l'entree (pas depuis 6H)."""
        t0 = datetime(2026, 8, 19, 6, 52, tzinfo=PARIS)
        state, _ = update_performance_state(
            {}, ["GBPAUD"], ["GBPAUD"],
            {"GBPAUD": {"trend_pips": -1.6, "trend_icon": "✅"}}, t0,
        )
        for minutes, pips in [(26, -2.2), (60, 0.5), (86, -0.5)]:
            t = t0 + timedelta(minutes=minutes)
            state, newly_closed = update_performance_state(
                state, ["GBPAUD"], ["GBPAUD"],
                {"GBPAUD": {"trend_pips": pips, "trend_icon": "✅"}}, t,
            )
            self.assertEqual(newly_closed, [])
            self.assertEqual(state["open_trades"]["GBPAUD"]["status"], "open")

        t_stop = t0 + timedelta(hours=2, minutes=14)  # 09:06
        state, newly_closed = update_performance_state(
            state, ["GBPAUD"], ["GBPAUD"],
            {"GBPAUD": {"trend_pips": -6.5, "trend_icon": "❌"}}, t_stop,
        )
        self.assertEqual(len(newly_closed), 1)
        self.assertEqual(newly_closed[0]["pair"], "GBPAUD")
        self.assertAlmostEqual(newly_closed[0]["pips"], -4.9, places=6)  # -6.5 - (-1.6)
        self.assertEqual(newly_closed[0]["reason"], "STOP_FIRST_CROSS")
        self.assertEqual(state["open_trades"]["GBPAUD"]["status"], "closed")

    def test_performance_state_closes_on_top5_exit_before_any_cross(self):
        """Cas 5 (USDCAD): la paire quitte le TOP5 avant toute ❌ -> cloture
        immediate a ce run-la, pas d'attente de fin de journee."""
        t0 = datetime(2026, 8, 19, 15, 58, tzinfo=PARIS)
        state, _ = update_performance_state(
            {}, ["USDCAD"], ["USDCAD"],
            {"USDCAD": {"trend_pips": 59.0, "trend_icon": "✅"}}, t0,
        )
        t1 = t0 + timedelta(minutes=55)
        state, newly_closed = update_performance_state(
            state, [], ["USDCAD"],  # top5_now ne contient plus USDCAD
            {"USDCAD": {"trend_pips": 68.0, "trend_icon": "✅"}}, t1,
        )
        self.assertEqual(len(newly_closed), 1)
        self.assertAlmostEqual(newly_closed[0]["pips"], 9.0, places=6)  # 68.0 - 59.0
        self.assertEqual(newly_closed[0]["reason"], "TOP5_EXIT")

    def test_performance_state_closes_all_open_trades_at_session_end(self):
        """Cas 4 (USDCHF): jamais de ❌, jamais sortie du TOP5 -> cloture au
        changement de date, sur le dernier ecart connu depuis l'entree."""
        t0 = datetime(2026, 8, 19, 15, 9, tzinfo=PARIS)
        state, _ = update_performance_state(
            {}, ["USDCHF"], ["USDCHF"],
            {"USDCHF": {"trend_pips": 50.6, "trend_icon": "✅"}}, t0,
        )
        t1 = datetime(2026, 8, 19, 18, 21, tzinfo=PARIS)
        state, newly_closed = update_performance_state(
            state, ["USDCHF"], ["USDCHF"],
            {"USDCHF": {"trend_pips": 118.4, "trend_icon": "✅"}}, t1,
        )
        self.assertEqual(newly_closed, [])
        self.assertAlmostEqual(state["open_trades"]["USDCHF"]["last_pips"], 67.8, places=6)

        next_day = datetime(2026, 8, 20, 6, 15, tzinfo=PARIS)
        state, newly_closed2 = update_performance_state(state, [], [], {}, next_day)
        self.assertEqual(len(newly_closed2), 1)
        self.assertEqual(newly_closed2[0]["pair"], "USDCHF")
        self.assertAlmostEqual(newly_closed2[0]["pips"], 67.8, places=6)
        self.assertEqual(newly_closed2[0]["reason"], "SESSION_END")
        self.assertEqual(state["open_trades"], {})
        self.assertEqual(state["tracking_date"], "2026-08-20")

    def test_performance_general_bilan_matches_vivier_formulas(self):
        trades = [
            {"pair": "A", "pips": 10.0, "date": "2026-01-01",
             "closed_at_paris": "2026-01-01T23:00:00+01:00"},
            {"pair": "B", "pips": 5.0, "date": "2026-01-02",
             "closed_at_paris": "2026-01-02T23:00:00+01:00"},
            {"pair": "C", "pips": -8.0, "date": "2026-01-03",
             "closed_at_paris": "2026-01-03T23:00:00+01:00"},
            {"pair": "D", "pips": -2.0, "date": "2026-01-04",
             "closed_at_paris": "2026-01-04T23:00:00+01:00"},
        ]
        bilan = _performance_general_bilan(trades, 2026)

        self.assertEqual(bilan["closed_trades"], 4)
        self.assertEqual(bilan["winning_trades"], 2)
        self.assertEqual(bilan["losing_trades"], 2)
        self.assertAlmostEqual(bilan["win_rate_pct"], 50.0)
        self.assertAlmostEqual(bilan["winning_pips"], 15.0)
        self.assertAlmostEqual(bilan["losing_pips"], -10.0)
        self.assertAlmostEqual(bilan["profit_factor"], 1.5)
        self.assertAlmostEqual(bilan["pip_rate_pct"], (15.0 - 10.0) / 15.0 * 100.0)
        self.assertAlmostEqual(
            bilan["ns_score_pct"], math.sqrt(50.0 * bilan["pip_rate_pct"]),
        )
        self.assertAlmostEqual(bilan["average_win_pips"], 7.5)
        self.assertAlmostEqual(bilan["average_loss_pips"], 5.0)
        self.assertAlmostEqual(bilan["gain_loss_ratio"], 1.5)
        self.assertAlmostEqual(bilan["closed_pips"], 5.0)
        # Courbe d'equite ordonnee: +10(peak10,dd0) +5(peak15,dd0)
        # -8(equity7,dd8) -2(equity5,dd10).
        self.assertAlmostEqual(bilan["max_drawdown_pips"], 10.0)

    def test_performance_general_bilan_handles_no_losses(self):
        trades = [{"pair": "A", "pips": 10.0, "date": "2026-01-01",
                   "closed_at_paris": "2026-01-01T23:00:00+01:00"}]
        bilan = _performance_general_bilan(trades, 2026)
        self.assertIsNone(bilan["profit_factor"])
        self.assertIsNone(bilan["gain_loss_ratio"])
        self.assertAlmostEqual(bilan["win_rate_pct"], 100.0)

    def test_performance_report_aggregates_daily_weekly_monthly_yearly(self):
        state = {
            "tracking_date": "2026-08-19",
            "open_trades": {"EURUSD": {"status": "open", "last_pips": 3.0}},
            "closed_trades": [
                {"date": "2026-08-18", "pair": "AUDNZD", "pips": 12.3,
                 "closed_at_paris": "2026-08-18T23:59:00+02:00"},
                {"date": "2026-08-17", "pair": "GBPJPY", "pips": -5.0,
                 "closed_at_paris": "2026-08-17T23:59:00+02:00"},
                {"date": "2026-01-05", "pair": "USDJPY", "pips": 4.0,
                 "closed_at_paris": "2026-01-05T23:59:00+01:00"},
            ],
        }
        report = performance_report(state, datetime(2026, 8, 19, 10, 0, tzinfo=PARIS))

        self.assertEqual(report["daily"]["trades"], 0)  # rien cloture le 19 lui-meme
        self.assertAlmostEqual(report["weekly"]["total_pips"], 12.3 - 5.0)
        self.assertAlmostEqual(report["monthly"]["total_pips"], 12.3 - 5.0)
        self.assertAlmostEqual(report["yearly"]["total_pips"], 12.3 - 5.0 + 4.0)
        self.assertAlmostEqual(report["open_pips_total"], 3.0)

    def test_performance_lines_show_bilan_only_when_trades_just_closed(self):
        state_before_close = {
            "tracking_date": "2026-08-18",
            "open_trades": {"AUDNZD": {"status": "open", "last_pips": 12.3}},
            "closed_trades": [],
        }
        report = performance_report(
            state_before_close, datetime(2026, 8, 18, 15, 0, tzinfo=PARIS),
        )
        lines_no_close = performance_report_lines(report, 0)
        self.assertFalse(any("BILAN GÉNÉRAL" in line for line in lines_no_close))
        self.assertTrue(any("DAILY" in line for line in lines_no_close))

        state_after_close = {
            "tracking_date": "2026-08-19",
            "open_trades": {},
            "closed_trades": [
                {"date": "2026-08-18", "pair": "AUDNZD", "pips": 12.3,
                 "closed_at_paris": "2026-08-18T23:59:00+02:00"},
            ],
        }
        report2 = performance_report(
            state_after_close, datetime(2026, 8, 19, 6, 15, tzinfo=PARIS),
        )
        lines_close = performance_report_lines(report2, 1)
        self.assertTrue(any("BILAN GÉNÉRAL" in line for line in lines_close))
        self.assertIn("PF · Profit Factor : N/D", lines_close)

    def test_performance_lines_show_open_position_line_every_run(self):
        """Sans cette ligne, 'aucune position en cours' et 'plusieurs
        positions ouvertes mais rien clôturé' affichent toutes deux
        +0.0 pips (0 trades) a l'identique -- elle doit apparaitre a CHAQUE
        run (pas seulement au run qui vient de clore, contrairement au
        BILAN GENERAL)."""
        state = {
            "tracking_date": "2026-08-19",
            "open_trades": {
                "AUDUSD": {"status": "ignored", "last_pips": 0.0},
                "CADCHF": {"status": "open", "entry_pips": 55.0, "last_pips": 1.7},
                "EURUSD": {"status": "open", "entry_pips": 82.8, "last_pips": -3.6},
                "GBPUSD": {"status": "open", "entry_pips": 67.4, "last_pips": -6.7},
                "USDCHF": {"status": "open", "entry_pips": 115.2, "last_pips": -0.5},
            },
            "closed_trades": [],
        }
        report = performance_report(state, datetime(2026, 8, 19, 19, 15, tzinfo=PARIS))

        self.assertEqual(report["open_trades_count"], 4)  # AUDUSD ignore exclu
        self.assertAlmostEqual(report["open_pips_total"], -9.1, places=6)

        lines = performance_report_lines(report, 0)
        self.assertIn("🔓 Position ouverte : -9.1 pips (4 positions)", lines)

    def test_performance_lines_omit_open_position_line_without_open_trades(self):
        state = {"tracking_date": "2026-08-19", "open_trades": {}, "closed_trades": []}
        report = performance_report(state, datetime(2026, 8, 19, 19, 15, tzinfo=PARIS))

        lines = performance_report_lines(report, 0)

        self.assertFalse(any("Position ouverte" in line for line in lines))

    def test_performance_lines_use_singular_for_a_single_open_position(self):
        state = {
            "tracking_date": "2026-08-19",
            "open_trades": {"EURUSD": {"status": "open", "entry_pips": 10.0, "last_pips": 4.0}},
            "closed_trades": [],
        }
        report = performance_report(state, datetime(2026, 8, 19, 19, 15, tzinfo=PARIS))

        lines = performance_report_lines(report, 0)

        self.assertIn("🔓 Position ouverte : +4.0 pips (1 position)", lines)

    def test_message_shows_performance_section(self):
        message = format_full_alignment_message(
            [],
            now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
            performance_lines=[
                "📈 PERF TOP5 — DAILY : +12.3 pips (1 trades)",
                "🟢 +12.3 pips | 🔴 +0.0 pips",
            ],
        )
        self.assertIn("📈 PERF TOP5 — DAILY : +12.3 pips (1 trades)", message)

    def test_streaky_pairs_requires_aligned_bias_and_nonzero_streak_on_all_tf(self):
        aligned_bull = row("AUDNZD", 1, 1, 1, streaks={"M": 4, "W": 8, "D": 1})
        aligned_bear = row("GBPAUD", -1, -1, -1, streaks={"M": 2, "W": 7, "D": 1})
        # Biais haussier herite mais aucun streak vert en cours sur W (le
        # fameux 'W+(0)'): doit etre exclu malgre le biais aligne.
        zero_streak = row("EURUSD", 1, 1, 1, streaks={"W": 0})
        # Biais non aligne (D oppose): exclu.
        mixed = row("USDCHF", 1, -1, 1)
        # Les indices ne sont pas concernes par STREAKY.
        index = index_row("AXY", 1, 1, 1, streaks={"M": 3, "W": 3, "D": 3})

        matches = streaky_pairs([aligned_bull, aligned_bear, zero_streak, mixed, index])

        self.assertEqual(
            [(m["pair"], m["direction"]) for m in matches],
            [("AUDNZD", 1), ("GBPAUD", -1)],
        )
        self.assertEqual(matches[0]["streaks"], {"M": 4, "W": 8, "D": 1})
        self.assertEqual(matches[1]["streaks"], {"M": 2, "W": 7, "D": 1})
        # Les deux sont confirmees par le prix live sur les trois TF (px
        # egal au biais partout): aucun TF "porte" par l'historique seul.
        self.assertEqual(matches[0]["px"], {"M": 1, "W": 1, "D": 1})
        self.assertEqual(matches[0]["carried_tfs"], [])
        self.assertEqual(matches[1]["px"], {"M": -1, "W": -1, "D": -1})
        self.assertEqual(matches[1]["carried_tfs"], [])

    def test_streaky_pairs_flags_tf_carried_by_streak_without_fresh_px(self):
        # Biais et streak alignes sur M, mais px_state a 0: le prix n'a pas
        # (encore) casse au-dela de la derniere brique sur ce TF, le biais y
        # est seulement "porte" par le streak historique -> TF a risque.
        carried = row(
            "USDCAD", 0, 1, 1,
            bias={"M": 1, "W": 1, "D": 1}, streaks={"M": 3, "W": 3, "D": 3},
        )

        matches = streaky_pairs([carried])

        self.assertEqual(matches[0]["px"], {"M": 0, "W": 1, "D": 1})
        self.assertEqual(matches[0]["carried_tfs"], ["M"])

    def test_streaky_pairs_sorted_by_total_streak_descending(self):
        weaker = row("USDCAD", 1, 1, 1, streaks={"M": 1, "W": 1, "D": 1})
        stronger = row("AUDJPY", 1, 1, 1, streaks={"M": 2, "W": 8, "D": 1})

        matches = streaky_pairs([weaker, stronger])

        self.assertEqual([m["pair"] for m in matches], ["AUDJPY", "USDCAD"])

    def test_message_shows_streaky_section(self):
        entries = [
            {"pair": "AUDNZD", "direction": 1, "streaks": {"M": 4, "W": 8, "D": 1},
             "px": {"M": 1, "W": 1, "D": 1}, "carried_tfs": []},
            {"pair": "GBPAUD", "direction": -1, "streaks": {"M": 2, "W": 7, "D": 1},
             "px": {"M": -1, "W": 0, "D": -1}, "carried_tfs": ["W"],
             "sar_confirmed": True},
        ]

        message = format_full_alignment_message(
            [], now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS), streaky=entries,
        )

        self.assertIn("🔥 STREAKY", message)
        self.assertIn("🟢 AUDNZD M+(4) W+(8) D+(1)", message)
        # GBPAUD: W "porte" (0) -> warning; SAR H1 frais confirme -> 🎯.
        self.assertIn("🔴 GBPAUD M-(2) W0(7) D-(1) ⚠️ 🎯", message)

    def test_update_price_trends_exposes_pips_vs_06h_signed_normally(self):
        """`pips_vs_06h` est signe dans le sens normal du prix (monte =
        positif), contrairement a `trend_pips` qui est oriente vers le sens du
        premier signal du jour (cf. `test_trend_pips_are_signed_in_favor_of_the_baseline_direction`)."""
        bear_first = row("AUDUSD", -1, -1, -1, daily_chg=-0.20)
        bear_first["live_price"] = 0.71004
        state, _ = update_price_trends(
            {}, [bear_first], datetime(2026, 7, 16, 6, 15, tzinfo=PARIS),
            h1_close_lookup=no_h1_backfill,
        )
        bear_later = row("AUDUSD", -1, -1, -1, daily_chg=-0.20)
        bear_later["live_price"] = 0.70864
        _, trends = update_price_trends(
            state, [bear_later], datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
            h1_close_lookup=no_h1_backfill,
        )
        # Prix a baisse de 14 pips: trend_pips = +14 (favorable au signal
        # BEAR), pips_vs_06h = -14 (le prix a bel et bien baisse).
        self.assertAlmostEqual(trends["AUDUSD"]["trend_pips"], 14.0, places=6)
        self.assertAlmostEqual(trends["AUDUSD"]["pips_vs_06h"], -14.0, places=6)

    def test_update_price_trends_exposes_pips_vs_previous_run(self):
        """Le T0 du run suivant est le prix de celui-ci: `pips_vs_previous_run`
        compare au run precedent (pas a 06h), et vaut None au tout premier run
        du jour faute de run precedent."""
        first = row("AUDUSD", -1, -1, -1, daily_chg=-0.20)
        first["live_price"] = 0.71004
        state, first_trends = update_price_trends(
            {}, [first], datetime(2026, 7, 16, 6, 15, tzinfo=PARIS),
            h1_close_lookup=no_h1_backfill,
        )
        self.assertIsNone(first_trends["AUDUSD"]["pips_vs_previous_run"])

        second = row("AUDUSD", -1, -1, -1, daily_chg=-0.20)
        second["live_price"] = 0.70864  # -14 pips vs le run precedent
        _, second_trends = update_price_trends(
            state, [second], datetime(2026, 7, 16, 6, 30, tzinfo=PARIS),
            h1_close_lookup=no_h1_backfill,
        )
        self.assertAlmostEqual(second_trends["AUDUSD"]["pips_vs_previous_run"], -14.0, places=6)

    def test_currency_pip_sum_orients_pips_by_base_or_quote_role(self):
        """USD base (USDJPY) qui monte renforce l'USD (+); USD quote (EURUSD,
        AUDUSD) l'affaiblit quand elle monte et le renforce quand elle baisse."""
        rows_by_pair = {
            "USDJPY": {"pair": "USDJPY", "asset_type": "PAIR"},
            "EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"},
            "AUDUSD": {"pair": "AUDUSD", "asset_type": "PAIR"},
        }
        price_trends = {
            "USDJPY": {"pips_vs_06h": 50.0},    # USD base, +50 pips -> +50
            "EURUSD": {"pips_vs_06h": 50.0},    # USD quote, +50 pips -> -50
            "AUDUSD": {"pips_vs_06h": -100.0},  # USD quote, -100 pips -> +100
        }

        total, count = currency_pip_sum("USD", rows_by_pair, price_trends)

        self.assertEqual(count, 3)
        self.assertAlmostEqual(total, 100.0, places=6)

    def test_currency_pip_sum_ignores_indices_and_unrelated_pairs(self):
        rows_by_pair = {
            "EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"},
            "GBPJPY": {"pair": "GBPJPY", "asset_type": "PAIR"},  # ni USD ni EUR
            "DXY": index_row("DXY", 1, 1, 1, daily_chg=0.42),  # indice, exclu
        }
        price_trends = {
            "EURUSD": {"pips_vs_06h": 50.0},
            "GBPJPY": {"pips_vs_06h": 30.0},
            "DXY": {"pips_vs_06h": 10.0},
        }

        total, count = currency_pip_sum("USD", rows_by_pair, price_trends)

        self.assertEqual(count, 1)
        self.assertAlmostEqual(total, -50.0, places=6)

    def test_currency_pip_sum_skips_pairs_without_a_price_trend_entry(self):
        rows_by_pair = {
            "EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"},
            "USDJPY": {"pair": "USDJPY", "asset_type": "PAIR"},
        }
        price_trends = {"EURUSD": {"pips_vs_06h": 50.0}}  # USDJPY absente

        total, count = currency_pip_sum("USD", rows_by_pair, price_trends)

        self.assertEqual(count, 1)
        self.assertAlmostEqual(total, -50.0, places=6)

    def test_currency_pip_sum_can_target_the_previous_run_field(self):
        rows_by_pair = {
            "USDJPY": {"pair": "USDJPY", "asset_type": "PAIR"},
            "EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"},
        }
        price_trends = {
            "USDJPY": {"pips_vs_06h": 50.0, "pips_vs_previous_run": 4.0},
            "EURUSD": {"pips_vs_06h": 50.0, "pips_vs_previous_run": -3.0},
        }

        total, count = currency_pip_sum(
            "USD", rows_by_pair, price_trends, field="pips_vs_previous_run",
        )

        self.assertEqual(count, 2)
        # USD base (USDJPY) +4 -> +4; USD quote (EURUSD) -3 -> +3.
        self.assertAlmostEqual(total, 7.0, places=6)

    def _eurusd_check_fixture(self):
        """7 paires USD + 7 paires EUR, pips depuis 06h Paris orchestres pour
        un EUR fort / USD faible (valeurs reprises d'un run reel du
        2026-08-19)."""
        pairs = [
            "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
            "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURNZD", "EURCAD",
        ]
        rows_by_pair = {pair: {"pair": pair, "asset_type": "PAIR"} for pair in pairs}
        price_trends = {
            "EURUSD": {"pips_vs_06h": 90.1},
            "GBPUSD": {"pips_vs_06h": 87.4},
            "AUDUSD": {"pips_vs_06h": 53.0},
            "NZDUSD": {"pips_vs_06h": 64.8},
            "USDCAD": {"pips_vs_06h": -74.9},
            "USDCHF": {"pips_vs_06h": -119.2},
            "USDJPY": {"pips_vs_06h": -122.6},
            "EURGBP": {"pips_vs_06h": 11.3},
            "EURJPY": {"pips_vs_06h": 0.7},
            "EURCHF": {"pips_vs_06h": -67.2},
            "EURAUD": {"pips_vs_06h": 3.7},
            "EURNZD": {"pips_vs_06h": -63.8},
            "EURCAD": {"pips_vs_06h": 37.2},
        }
        return rows_by_pair, price_trends

    def test_eurusd_cross_check_lines_shows_only_the_06h_segment_by_default(self):
        """Sans `index_by_currency` ni `pips_vs_previous_run` (aucun run
        precedent aujourd'hui), seul le segment 06h -- le seul obligatoire --
        apparait: pas de valeurs, une seule bille."""
        rows_by_pair, price_trends = self._eurusd_check_fixture()

        lines = eurusd_cross_check_lines(rows_by_pair, price_trends)

        self.assertEqual(lines, ["🧭 EURUSD CHECK", "06h 🟢"])

    def test_eurusd_cross_check_lines_adds_the_run_segment_when_available(self):
        """Le run precedent devient le T0 de celui-ci: chaque paire porte son
        propre `pips_vs_previous_run`, agrege par devise comme pour 06h."""
        rows_by_pair, price_trends = self._eurusd_check_fixture()
        deltas = {
            "EURUSD": 3.0, "GBPUSD": -1.0, "AUDUSD": 2.0, "NZDUSD": -2.0,
            "USDCAD": 5.0, "USDCHF": -1.0, "USDJPY": 4.0,
            "EURGBP": 1.0, "EURJPY": -3.0, "EURCHF": 0.0, "EURAUD": -2.0,
            "EURNZD": 1.0, "EURCAD": -4.0,
        }
        for pair, delta in deltas.items():
            price_trends[pair]["pips_vs_previous_run"] = delta

        lines = eurusd_cross_check_lines(rows_by_pair, price_trends)

        # 06h toujours EUR fort (🟢), mais l'USD regagne du terrain plus vite
        # que l'EUR depuis le run precedent (🔴 RUN): repli en cours malgre
        # une tendance journaliere toujours engagee.
        self.assertEqual(lines[1], "06h 🟢 · RUN 🔴")

    def test_eurusd_cross_check_lines_adds_the_index_segment_first(self):
        """`index_by_currency` ajoute un segment INDEX, place en tete."""
        rows_by_pair, price_trends = self._eurusd_check_fixture()
        index_by_currency = {
            "EUR": index_row("EXY", 1, 1, 1, daily_chg=1.44),
            "USD": index_row("DXY", -1, -1, -1, daily_chg=-1.43),
        }

        lines = eurusd_cross_check_lines(rows_by_pair, price_trends, index_by_currency)

        self.assertEqual(lines, ["🧭 EURUSD CHECK", "INDEX 🟢 · 06h 🟢"])

    def test_eurusd_cross_check_lines_combines_all_three_segments(self):
        rows_by_pair, price_trends = self._eurusd_check_fixture()
        price_trends["EURUSD"]["pips_vs_previous_run"] = -3.0
        price_trends["USDJPY"]["pips_vs_previous_run"] = 4.0
        for pair in [
            "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
            "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURNZD", "EURCAD",
        ]:
            price_trends[pair]["pips_vs_previous_run"] = 0.0
        index_by_currency = {
            "EUR": index_row("EXY", 1, 1, 1, daily_chg=1.44),
            "USD": index_row("DXY", -1, -1, -1, daily_chg=-1.43),
        }

        lines = eurusd_cross_check_lines(rows_by_pair, price_trends, index_by_currency)

        self.assertEqual(lines, ["🧭 EURUSD CHECK", "INDEX 🟢 · 06h 🟢 · RUN 🔴"])

    def test_eurusd_cross_check_lines_empty_without_price_trends(self):
        rows_by_pair, _ = self._eurusd_check_fixture()

        self.assertEqual(eurusd_cross_check_lines(None, None), [])
        self.assertEqual(eurusd_cross_check_lines(rows_by_pair, None), [])
        self.assertEqual(eurusd_cross_check_lines(rows_by_pair, {}), [])

    def test_message_shows_eurusd_check_section_after_index_block(self):
        rows_by_pair, price_trends = self._eurusd_check_fixture()
        dxy = index_row("DXY", -1, -1, -1, daily_chg=-1.42)
        exy = index_row("EXY", 1, 1, 1, daily_chg=1.35)
        indices = [dxy, exy]

        message = format_full_alignment_message(
            [], now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
            index_status_rows=all_index_status_rows(indices),
            rows_by_pair=rows_by_pair,
            price_trends=price_trends,
            index_by_currency={"USD": dxy, "EUR": exy},
        )

        self.assertIn("💱 INDEX CHG%D", message)
        self.assertIn("🧭 EURUSD CHECK", message)
        self.assertIn("INDEX 🟢 · 06h 🟢", message)
        self.assertLess(
            message.index("💱 INDEX CHG%D"), message.index("🧭 EURUSD CHECK"),
        )
        self.assertLess(
            message.index("🧭 EURUSD CHECK"), message.index("INDEX 🟢 · 06h 🟢"),
        )

    def test_message_omits_eurusd_check_without_price_trends(self):
        """Avant la 1ere baseline 06h du jour (ou run isole sans price_trends
        injecte), la section ne doit pas s'afficher a moitie vide."""
        rows_by_pair, _ = self._eurusd_check_fixture()

        message = format_full_alignment_message(
            [], now=datetime(2026, 7, 16, 10, 0, tzinfo=PARIS),
            rows_by_pair=rows_by_pair,
        )

        self.assertNotIn("🧭 EURUSD CHECK", message)

    def test_pair_check_lines_generalizes_to_a_non_eurusd_pair(self):
        """`pair_check_lines` est la version generalisee derriere EURUSD
        CHECK: verifie qu'elle marche identiquement pour CHFJPY (base CHF,
        quote JPY), utilisee par PAIRE_CHECK."""
        rows_by_pair = {
            "CHFJPY": {"pair": "CHFJPY", "asset_type": "PAIR"},
            "USDCHF": {"pair": "USDCHF", "asset_type": "PAIR"},
            "USDJPY": {"pair": "USDJPY", "asset_type": "PAIR"},
        }
        price_trends = {
            "CHFJPY": {"pips_vs_06h": 40.0},   # CHF base +40 -> +40 ; JPY quote +40 -> -40
            "USDCHF": {"pips_vs_06h": -10.0},  # CHF quote -10 -> +10
            "USDJPY": {"pips_vs_06h": 20.0},   # JPY quote +20 -> -20
        }
        # CHF: +40 (CHFJPY) + 10 (USDCHF) = +50. JPY: -40 (CHFJPY) - 20 (USDJPY) = -60.
        # CHF > JPY -> bille verte.

        lines = pair_check_lines("CHFJPY", rows_by_pair, price_trends)

        self.assertEqual(lines, ["🧭 CHFJPY CHECK", "06h 🟢"])

    def test_pair_check_signals_is_the_labels_and_icons_behind_pair_check_lines(self):
        rows_by_pair, price_trends = self._eurusd_check_fixture()
        index_by_currency = {
            "EUR": index_row("EXY", 1, 1, 1, daily_chg=1.44),
            "USD": index_row("DXY", -1, -1, -1, daily_chg=-1.43),
        }

        self.assertEqual(
            pair_check_signals("EURUSD", rows_by_pair, price_trends, index_by_currency),
            [("INDEX", "🟢"), ("06h", "🟢")],
        )
        self.assertEqual(pair_check_signals("NOTAPAIR", rows_by_pair, price_trends), [])

    def test_pair_check_lines_empty_for_an_unrecognized_pair(self):
        rows_by_pair = {"EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"}}
        price_trends = {"EURUSD": {"pips_vs_06h": 10.0}}

        self.assertEqual(pair_check_lines("XAUUSD", rows_by_pair, price_trends), [])
        self.assertEqual(pair_check_lines("NOTAPAIR", rows_by_pair, price_trends), [])

    def test_eurusd_cross_check_lines_delegates_to_pair_check_lines(self):
        rows_by_pair, price_trends = self._eurusd_check_fixture()

        self.assertEqual(
            eurusd_cross_check_lines(rows_by_pair, price_trends),
            pair_check_lines("EURUSD", rows_by_pair, price_trends),
        )


if __name__ == "__main__":
    unittest.main()
