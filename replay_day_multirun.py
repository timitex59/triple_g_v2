# -*- coding: utf-8 -*-
"""
Replay Multi-Run (Consolidé 6 Portefeuilles & Mode Verbose C1)
============================================================
Rejoue n'importe quelle journée passée (cadence 15m si disponible < 60d, ou H1).
Alimente les 6 portefeuilles d'exécution (A, B, C1, C2, C3, C4) et affiche la matrice de décision consolidée.
Option --verbose-trades pour journaliser chaque ENTRÉE et FERMETURE de C1.
Option --timeframe (auto, 15m, 1h) pour forcer la résolution souhaitée.
"""

import sys
import os
import copy
import argparse
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from typing import Dict, List, Any

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from relative_strength.config import ALL_28_PAIRS, PARIS_TZ
from relative_strength.models import RelativeStrengthState
from relative_strength.pip_engine import get_pip_size, calculate_pips, determine_directional_pips, update_excursion_metrics
from relative_strength.threshold import evaluate_threshold_state, evaluate_entry_zone, evaluate_independent_flags, evaluate_trade_eligibility
from relative_strength.persistence import update_persistence_counters
from relative_strength.velocity import calculate_velocity_and_acceleration
from relative_strength.breadth import compute_currency_strengths_from_pips, compute_currency_breadth, select_best_currency_expression
from relative_strength.scoring import compute_quality_score, compute_opportunity_score
from relative_strength.ranking import apply_triple_ranking
from relative_strength.execution import ExecutionManagerA, ExecutionManagerB, ExecutionManagerC, TradePosition, RejectedSignal
from relative_strength.alerts import format_consolidated_eligible_alert


def fetch_all_history_yfinance(day_str: str, requested_tf: str = "auto") -> Dict[str, pd.DataFrame]:
    """Télécharge les données 15m (si disponible < 60d) ou H1 (fallback) pour la date cible exacte."""
    target_dt = pd.to_datetime(day_str)
    start_dt = target_dt - timedelta(days=2)
    end_dt = target_dt + timedelta(days=2)
    tickers = [f"{p}=X" for p in ALL_28_PAIRS]

    if requested_tf.lower() == "1h":
        df_batch = yf.download(tickers, start=start_dt.strftime("%Y-%m-%d"), end=end_dt.strftime("%Y-%m-%d"), interval="1h", progress=False)
        interval_used = "1h"
    elif requested_tf.lower() == "15m":
        df_batch = yf.download(tickers, start=start_dt.strftime("%Y-%m-%d"), end=end_dt.strftime("%Y-%m-%d"), interval="15m", progress=False)
        interval_used = "15m"
    else:
        # Auto: Essai 15m d'abord
        df_batch = yf.download(tickers, start=start_dt.strftime("%Y-%m-%d"), end=end_dt.strftime("%Y-%m-%d"), interval="15m", progress=False)
        interval_used = "15m"
        if df_batch.empty or df_batch["Close"].dropna().empty:
            df_batch = yf.download(tickers, start=start_dt.strftime("%Y-%m-%d"), end=end_dt.strftime("%Y-%m-%d"), interval="1h", progress=False)
            interval_used = "1h"
    
    m_map: Dict[str, pd.DataFrame] = {}
    for pair in ALL_28_PAIRS:
        ticker = f"{pair}=X"
        try:
            pair_df = pd.DataFrame({
                "Open": df_batch["Open"][ticker],
                "High": df_batch["High"][ticker],
                "Low": df_batch["Low"][ticker],
                "Close": df_batch["Close"][ticker],
            }).dropna()
            pair_df.index = pair_df.index.tz_convert(PARIS_TZ)
            m_map[pair] = pair_df
        except Exception:
            pass
            
    return m_map, interval_used


def run_multi_run_replay():
    parser = argparse.ArgumentParser(description="Replay Multi-Run RSB Engine")
    parser.add_argument("--date", type=str, default="2026-08-14", help="Date de replay (YYYY-MM-DD)")
    parser.add_argument("--timeframe", type=str, default="auto", choices=["auto", "15m", "1h"], help="Forcer le timeframe (auto, 15m, 1h)")
    parser.add_argument("--verbose-trades", action="store_true", help="Affiche le detail trade par trade (ENTRY/CLOSED) pour C1")
    args = parser.parse_args()

    target_date_str = args.date
    verbose_trades = args.verbose_trades
    requested_tf = args.timeframe

    m_map, interval_used = fetch_all_history_yfinance(target_date_str, requested_tf)
    
    if not m_map or len(m_map) < len(ALL_28_PAIRS):
        print(f"❌ Données insuffisantes pour la date {target_date_str}.")
        return

    sample_df = list(m_map.values())[0]
    # Borner strictement la fenêtre de trading à [07:00, 22:00] Paris
    run_timestamps = [dt for dt in sample_df.index if dt.strftime("%Y-%m-%d") == target_date_str and 7 <= dt.hour <= 22]
    run_timestamps.sort()

    if not run_timestamps:
        print(f"⚠️ Aucune bougie ({interval_used}) trouvée pour la date {target_date_str} entre 07:00 et 22:00 Paris.")
        return

    print(f"\n🚀 REPLAY MULTI-RUN ({interval_used.upper()}) DU {target_date_str} ({len(run_timestamps)} RUNS)")
    print("=" * 145)

    ref_dt = run_timestamps[0]
    reference_prices = {}
    for pair, df in m_map.items():
        if ref_dt in df.index:
            reference_prices[pair] = float(df.loc[ref_dt]["Open"])
        else:
            reference_prices[pair] = float(df.iloc[0]["Open"])

    prev_pairs_data: Dict[str, Dict[str, Any]] = {}
    prev_dt_paris = None

    managers = {
        "RSB-A": ExecutionManagerA(),
        "RSB-B": ExecutionManagerB(),
        "RSB-C1": ExecutionManagerC("C1", max_capacity=15),
        "RSB-C2": ExecutionManagerC("C2"),
        "RSB-C3": ExecutionManagerC("C3", max_currency_exposure=2),
        "RSB-C4": ExecutionManagerC("C4"),
    }

    c1_closed_before_count = 0

    for run_idx, dt_paris in enumerate(run_timestamps, start=1):
        run_time_str = dt_paris.strftime("%H:%M")
        
        if run_idx == 1:
            current_prices_map = {pair: reference_prices[pair] for pair in ALL_28_PAIRS}
            delta_hours = 0.25 if interval_used == "15m" else 1.0
        else:
            delta_hours = (dt_paris - prev_dt_paris).total_seconds() / 3600.0 if prev_dt_paris else (0.25 if interval_used == "15m" else 1.0)
            current_prices_map = {
                pair: float(df.loc[dt_paris]["Close"]) if dt_paris in df.index else reference_prices[pair]
                for pair, df in m_map.items()
            }
            
        prev_dt_paris = dt_paris

        raw_pips_map = {}
        for pair in ALL_28_PAIRS:
            price = current_prices_map[pair]
            ref_p = reference_prices[pair]
            pip_s = get_pip_size(pair)
            raw_p, _, _ = calculate_pips(price, ref_p, pip_s)
            raw_pips_map[pair] = raw_p

        currency_strengths = compute_currency_strengths_from_pips(raw_pips_map)
        states_list: List[RelativeStrengthState] = []

        for pair in ALL_28_PAIRS:
            cur_price = current_prices_map[pair]
            ref_p = reference_prices[pair]
            pip_s = get_pip_size(pair)
            
            raw_market, long_pips, short_pips = calculate_pips(cur_price, ref_p, pip_s)
            p_prev = prev_pairs_data.get(pair, {})
            
            trade_dir = "LONG" if raw_market > 0 else ("SHORT" if raw_market < 0 else "NEUTRAL")
            directional_pips = determine_directional_pips(raw_market, trade_dir)
            
            prev_mfe = float(p_prev.get("mfe_pips", 0.0))
            prev_mae = float(p_prev.get("mae_pips", 0.0))
            mfe, mae, dd_pips, dd_pct, eff = update_excursion_metrics(directional_pips, prev_mfe, prev_mae)
            
            prev_vel = float(p_prev.get("velocity_pips_per_hour", 0.0))
            prev_dir_pips = float(p_prev.get("directional_pips", 0.0))
            velocity_raw, accel_raw = calculate_velocity_and_acceleration(
                directional_pips, prev_dir_pips, delta_hours, prev_vel
            )
            
            prev_persistence_map = {
                "confirmation_runs": int(p_prev.get("confirmation_runs", 0)),
                "above_threshold_runs": int(p_prev.get("above_threshold_runs", 0)),
                "signal_direction_runs": int(p_prev.get("signal_direction_runs", 0)),
                "warning_runs": int(p_prev.get("warning_runs", 0)),
                "last_trade_direction": p_prev.get("trade_direction", "NEUTRAL")
            }
            
            align_symbol = "↑↑" if trade_dir == "LONG" else ("↓↓" if trade_dir == "SHORT" else "⚪")
            has_tech_warning = False
            conf_runs, above_runs, dir_runs, warn_runs = update_persistence_counters(
                directional_pips, trade_dir, has_tech_warning, prev_persistence_map
            )
            
            is_exp, is_tr, is_exh, rev_warn, has_warn, is_ch = evaluate_independent_flags(
                directional_pips, prev_dir_pips, conf_runs, mfe, dd_pct, velocity_raw, accel_raw, has_tech_warning
            )
            
            exclusive_state = evaluate_threshold_state(directional_pips, conf_runs)
            entry_zone = evaluate_entry_zone(directional_pips)
            trade_eligibility, is_eligible = evaluate_trade_eligibility(exclusive_state, entry_zone)
            
            base_curr = pair[:3]
            quote_curr = pair[3:]
            base_str = currency_strengths.get(base_curr, 50.0)
            quote_str = currency_strengths.get(quote_curr, 50.0)
            strength_diff = base_str - quote_str
            
            conf_crosses, contrad_crosses, avail_crosses, b_ratio = compute_currency_breadth(
                base_curr, "BULL" if trade_dir == "LONG" else "BEAR", raw_pips_map
            )
            
            state = RelativeStrengthState(
                pair=pair, timestamp=dt_paris.isoformat(), reference_time="07:00:00", reference_price=ref_p,
                reference_source=f"yfinance_{interval_used}", current_price=cur_price, trade_direction=trade_dir,
                raw_market_pips=raw_market, long_pips=long_pips, short_pips=short_pips,
                directional_pips=directional_pips, threshold_state=exclusive_state,
                trade_eligibility=trade_eligibility, is_eligible=is_eligible,
                above_threshold_runs=above_runs, confirmation_runs=conf_runs,
                mfe_pips=mfe, mae_pips=mae, drawdown_from_mfe=dd_pips, drawdown_pct=dd_pct,
                efficiency=eff, velocity_pips_per_hour=velocity_raw, acceleration_pips_per_hour2=accel_raw,
                entry_zone=entry_zone, base_currency=base_curr, quote_currency=quote_curr,
                base_strength=base_str, quote_strength=quote_str, strength_differential=strength_diff,
                confirming_crosses=conf_crosses, contradicting_crosses=contrad_crosses, available_crosses=avail_crosses,
                breadth_ratio=b_ratio, technical_alignment=align_symbol
            )
            
            state.quality_score = compute_quality_score(state)
            state.opportunity_score = compute_opportunity_score(state)
            states_list.append(state)
            prev_pairs_data[pair] = state.to_dict()

        apply_triple_ranking(states_list)

        is_last_run = (run_idx == len(run_timestamps))

        # Traitement parallèle des 6 portefeuilles
        eligible_states = [s for s in states_list if s.is_eligible]
        
        if not verbose_trades and run_time_str in ["07:45", "08:15", "09:45", "11:00", "15:00"]:
            for s in eligible_states[:2]:
                decisions = {}
                for v_id, mgr in managers.items():
                    entries, rejections = mgr.process_run(copy.deepcopy(states_list), is_last_run_of_day=is_last_run)
                    accepted = any(e.pair == s.pair for e in entries)
                    reason = ""
                    if not accepted:
                        matching_rej = [r for r in rejections if r.pair == s.pair]
                        reason = matching_rej[0].rejection_reason if matching_rej else "REJECTED"
                    decisions[v_id] = (accepted, reason)

                msg = format_consolidated_eligible_alert(s, decisions)
                print("\n" + msg)
        else:
            for v_id, mgr in managers.items():
                entries, rejections = mgr.process_run(copy.deepcopy(states_list), is_last_run_of_day=is_last_run)
                
                # Mode Verbose C1 : Affichage SÉQUENTIEL exact du compteur Active x/15
                if verbose_trades and v_id == "RSB-C1":
                    active_start = len(mgr.active_positions) - len(entries)
                    for i, entry in enumerate(entries, start=1):
                        seq_active = active_start + i
                        print(f"🟢 [ENTRY RSB-C1] {run_time_str} | {entry.pair:<7} {entry.direction:<5} | Price: {entry.entry_price:.5f} | 07h Pips: {entry.entry_pips_07h:+.1f}p | Active: {seq_active}/15")
                    
                    new_closed = mgr.closed_trades[c1_closed_before_count:]
                    for cl in new_closed:
                        sign = "+" if cl.exit_pips > 0 else ""
                        print(f"🏁 [CLOSED RSB-C1] {run_time_str} | {cl.pair:<7} {cl.direction:<5} | Exit: {cl.exit_reason:<13} | Net Pips: {sign}{cl.exit_pips:.1f}p | MFE: +{cl.mfe_pips:.1f}p | MAE: {cl.mae_pips:.1f}p")
                    c1_closed_before_count = len(mgr.closed_trades)

    print("\n" + "=" * 145)
    print(f"🏆 SUMMARY FINAL 6 PORTEFEUILLES SUR REPLAY DU {target_date_str} ({interval_used.upper()}) :")
    for v_id, mgr in managers.items():
        print(f"  {v_id:<10} -> {len(mgr.closed_trades)} trades fermés, Pips totaux: {sum(t.exit_pips for t in mgr.closed_trades):+.1f}p")

    if verbose_trades:
        c1_mgr = managers["RSB-C1"]
        print(f"\n📋 DÉTAIL COMPLET DES {len(c1_mgr.closed_trades)} TRADES EXÉCUTÉS SUR RSB-C1 LE {target_date_str} :")
        print("=" * 125)
        print(f"{'#':<3} | {'PAIRE':<7} | {'DIR':<5} | {'ENTRÉE':<6} | {'PRIX ENTRÉE':<12} | {'PIPS 07H':<10} | {'RAISON SORTIE':<14} | {'PIPS NET':<10} | {'MFE':<8} | {'MAE':<8}")
        print("-" * 125)
        for idx, t in enumerate(c1_mgr.closed_trades, start=1):
            sign = "+" if t.exit_pips > 0 else ""
            e_time = t.entry_time.split("T")[1][:5] if "T" in t.entry_time else t.entry_time
            print(f"{idx:<3} | {t.pair:<7} | {t.direction:<5} | {e_time:<6} | {t.entry_price:<12.5f} | {t.entry_pips_07h:<+9.1f}p | {t.exit_reason:<14} | {sign}{t.exit_pips:<+8.1f}p | +{t.mfe_pips:<6.1f}p | {t.mae_pips:<6.1f}p")
        print("=" * 125)


if __name__ == "__main__":
    run_multi_run_replay()
