# -*- coding: utf-8 -*-
"""
Replay Multi-Run 15-Minutes — 14 Août 2026 (Consolidé 6 Portefeuilles)
=====================================================================
Rejoue la journée du 14 août 2026 à la cadence réelle de 15 minutes (66 runs) depuis 07:00 Europe/Paris.
Alimente les 6 portefeuilles d'exécution (A, B, C1, C2, C3, C4) et affiche la matrice de décision consolidée.
"""

import sys
import os
import copy
import pandas as pd
import yfinance as yf
from datetime import datetime
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
from relative_strength.alerts import format_consolidated_eligible_alert, format_daily_comparison_report


def fetch_all_15m_history_yfinance(day_str: str = "2026-08-14") -> Dict[str, pd.DataFrame]:
    tickers = [f"{p}=X" for p in ALL_28_PAIRS]
    df_batch = yf.download(tickers, period="5d", interval="15m", progress=False)
    
    m15_map: Dict[str, pd.DataFrame] = {}
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
            m15_map[pair] = pair_df
        except Exception:
            pass
            
    return m15_map


import argparse

def run_multi_run_replay():
    parser = argparse.ArgumentParser(description="Replay Multi-Run 15M RSB Engine")
    parser.add_argument("--date", type=str, default="2026-08-14", help="Date de replay (YYYY-MM-DD)")
    args = parser.parse_args()

    target_date_str = args.date
    m15_map = fetch_all_15m_history_yfinance(target_date_str)
    
    if not m15_map or len(m15_map) < len(ALL_28_PAIRS):
        print("❌ Données 15M insuffisantes.")
        return

    sample_df = list(m15_map.values())[0]
    m15_timestamps = [dt for dt in sample_df.index if dt.strftime("%Y-%m-%d") == target_date_str and dt.hour >= 7]
    m15_timestamps.sort()

    print(f"\n🚀 REPLAY MULTI-RUN 15M CONSOLIDÉ DU {target_date_str} ({len(m15_timestamps)} RUNS)")
    print("=" * 145)

    ref_dt = m15_timestamps[0]
    reference_prices = {}
    for pair, df in m15_map.items():
        if ref_dt in df.index:
            reference_prices[pair] = float(df.loc[ref_dt]["Open"])
        else:
            reference_prices[pair] = float(df.iloc[0]["Open"])

    prev_pairs_data: Dict[str, Dict[str, Any]] = {}
    prev_dt_paris = None

    managers = {
        "RSB-A": ExecutionManagerA(),
        "RSB-B": ExecutionManagerB(),
        "RSB-C1": ExecutionManagerC("C1"),
        "RSB-C2": ExecutionManagerC("C2"),
        "RSB-C3": ExecutionManagerC("C3", max_currency_exposure=2),
        "RSB-C4": ExecutionManagerC("C4"),
    }

    for run_idx, dt_paris in enumerate(m15_timestamps, start=1):
        run_time_str = dt_paris.strftime("%H:%M")
        
        if run_idx == 1:
            current_prices_map = {pair: reference_prices[pair] for pair in ALL_28_PAIRS}
            delta_hours = 0.25
        else:
            delta_hours = (dt_paris - prev_dt_paris).total_seconds() / 3600.0 if prev_dt_paris else 0.25
            current_prices_map = {
                pair: float(df.loc[dt_paris]["Close"]) if dt_paris in df.index else reference_prices[pair]
                for pair, df in m15_map.items()
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
                reference_source="yfinance_15m", current_price=cur_price, trade_direction=trade_dir,
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

        is_last_run = (run_idx == len(m15_timestamps))

        # Traitement parallèle des 6 portefeuilles
        eligible_states = [s for s in states_list if s.is_eligible]
        
        # Pour les timestamps clés de la journée, afficher le message consolidé
        if run_time_str in ["07:45", "08:15", "09:45", "11:00", "15:00"]:
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
                mgr.process_run(copy.deepcopy(states_list), is_last_run_of_day=is_last_run)

    print("\n" + "=" * 145)
    print("🏆 SUMMARY FINAL 6 PORTEFEUILLES SUR REPLAY 14 AOÛT 2026 :")
    for v_id, mgr in managers.items():
        print(f"  {v_id:<10} -> {len(mgr.closed_trades)} trades fermés, Pips totaux: {sum(t.exit_pips for t in mgr.closed_trades):+.1f}p")

if __name__ == "__main__":
    run_multi_run_replay()
