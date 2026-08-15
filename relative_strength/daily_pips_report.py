# -*- coding: utf-8 -*-
"""
Rapport Quotidien des Pips RSB-C1 (Support d'Intervalle Spécifique & Timeframe)
=============================================================================
Exécute le replay RSB-C1 (Cap 15) jour par jour sur un intervalle de dates spécifique.
Format : YYYY-MM-DD : X trades fermés (+YYY.Y pips)
"""

import sys
import os
import argparse
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from typing import Dict, List, Any

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from relative_strength.config import ALL_28_PAIRS, PARIS_TZ
from relative_strength.models import RelativeStrengthState
from relative_strength.pip_engine import get_pip_size, calculate_pips, determine_directional_pips, update_excursion_metrics
from relative_strength.threshold import evaluate_threshold_state, evaluate_entry_zone, evaluate_independent_flags, evaluate_trade_eligibility
from relative_strength.persistence import update_persistence_counters
from relative_strength.velocity import calculate_velocity_and_acceleration
from relative_strength.breadth import compute_currency_strengths_from_pips, compute_currency_breadth
from relative_strength.scoring import compute_quality_score, compute_opportunity_score
from relative_strength.ranking import apply_triple_ranking
from relative_strength.execution import ExecutionManagerC


def run_single_day_c1(date_str: str, df_batch_cache: Dict[str, pd.DataFrame], timeframe: str) -> List[Any]:
    m_map: Dict[str, pd.DataFrame] = {}
    for pair in ALL_28_PAIRS:
        ticker = f"{pair}=X"
        if ticker in df_batch_cache:
            m_map[pair] = df_batch_cache[ticker]

    if len(m_map) < len(ALL_28_PAIRS):
        return []

    sample_df = list(m_map.values())[0]
    run_timestamps = [dt for dt in sample_df.index if dt.strftime("%Y-%m-%d") == date_str and 7 <= dt.hour <= 22]
    run_timestamps.sort()

    if not run_timestamps:
        return []

    ref_dt = run_timestamps[0]
    reference_prices = {}
    for pair, df in m_map.items():
        if ref_dt in df.index:
            reference_prices[pair] = float(df.loc[ref_dt]["Open"])
        else:
            reference_prices[pair] = float(df.iloc[0]["Open"])

    prev_pairs_data: Dict[str, Dict[str, Any]] = {}
    prev_dt_paris = None
    manager = ExecutionManagerC("C1", max_capacity=15)

    for run_idx, dt_paris in enumerate(run_timestamps, start=1):
        if run_idx == 1:
            current_prices_map = {pair: reference_prices[pair] for pair in ALL_28_PAIRS}
            delta_hours = 0.25 if timeframe == "15m" else 1.0
        else:
            delta_hours = (dt_paris - prev_dt_paris).total_seconds() / 3600.0 if prev_dt_paris else (0.25 if timeframe == "15m" else 1.0)
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
            conf_runs, above_runs, dir_runs, warn_runs = update_persistence_counters(
                directional_pips, trade_dir, False, prev_persistence_map
            )
            
            is_exp, is_tr, is_exh, rev_warn, has_warn, is_ch = evaluate_independent_flags(
                directional_pips, prev_dir_pips, conf_runs, mfe, dd_pct, velocity_raw, accel_raw, False
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
                reference_source=f"yfinance_{timeframe}", current_price=cur_price, trade_direction=trade_dir,
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
        manager.process_run(states_list, is_last_run_of_day=is_last_run)

    return manager.closed_trades


def parse_args():
    parser = argparse.ArgumentParser(description="Rapport Quotidien des Pips RSB-C1")
    parser.add_argument("--start", type=str, default="2026-01-01", help="Date de début (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default="2026-08-14", help="Date de fin (YYYY-MM-DD)")
    parser.add_argument("--timeframe", type=str, default="1h", choices=["1h", "15m", "auto"], help="Intervalle (1h, 15m, auto)")
    return parser.parse_args()


def generate_daily_report():
    args = parse_args()
    start_date = pd.to_datetime(args.start)
    end_date = pd.to_datetime(args.end)
    tf_choice = args.timeframe

    print(f"📊 RAPPORT QUOTIDIEN RSB-C1 ({args.start} AU {args.end}) — TIMEFRAME: {tf_choice.upper()}")
    print("=" * 70)

    tickers = [f"{p}=X" for p in ALL_28_PAIRS]

    print("⏳ Téléchargement des données de marché...")
    download_tf = "1h" if tf_choice == "1h" else "15m"
    df_batch = yf.download(tickers, start=start_date.strftime("%Y-%m-%d"), end=(end_date + timedelta(days=2)).strftime("%Y-%m-%d"), interval=download_tf, progress=False)

    if df_batch.empty or df_batch["Close"].dropna().empty:
        if tf_choice == "15m" or tf_choice == "auto":
            print("⚠️ 15m non disponible sur cette période, bascule automatique vers 1h...")
            download_tf = "1h"
            df_batch = yf.download(tickers, start=start_date.strftime("%Y-%m-%d"), end=(end_date + timedelta(days=2)).strftime("%Y-%m-%d"), interval="1h", progress=False)

    df_cache: Dict[str, pd.DataFrame] = {}
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
            df_cache[ticker] = pair_df
        except Exception:
            pass

    curr_dt = start_date
    total_trades_all = 0
    total_pips_all = 0.0
    active_days_count = 0
    winning_trades_all = 0

    while curr_dt <= end_date:
        if curr_dt.weekday() < 5:  # Du lundi au vendredi
            date_str = curr_dt.strftime("%Y-%m-%d")
            trades = run_single_day_c1(date_str, df_cache, download_tf)
            count = len(trades)
            net_pips = sum(t.exit_pips for t in trades)
            sign = "+" if net_pips > 0 else ""
            
            print(f"{date_str} : {count} trades fermés ({sign}{net_pips:.1f} pips)")
            
            total_trades_all += count
            total_pips_all += net_pips
            if count > 0:
                active_days_count += 1
                winning_trades_all += sum(1 for t in trades if t.exit_pips > 0)
                
        curr_dt += timedelta(days=1)

    print("=" * 70)
    total_sign = "+" if total_pips_all > 0 else ""
    win_rate = (winning_trades_all / total_trades_all * 100) if total_trades_all > 0 else 0.0
    avg_pips_day = (total_pips_all / active_days_count) if active_days_count > 0 else 0.0
    
    print(f"🏆 CUMUL TOTAL RSB-C1 : {total_trades_all} trades fermés ({total_sign}{total_pips_all:.1f} pips)")
    print(f"📊 Win Rate : {win_rate:.1f}% | Moyenne : {total_sign}{avg_pips_day:.1f} pips / jour de trading ({active_days_count} jours)")
    print("=" * 70)


if __name__ == "__main__":
    generate_daily_report()
