# -*- coding: utf-8 -*-
"""
RSB-C1 Portfolio Risk & Drawdown Audit Engine
=============================================
Analyse approfondie du risque portefeuille pour la variante gagnante RSB-C1 :
- Equité temporelle & Max Drawdown (en pips et en % avec risque 1% par trade).
- Distribution de la simultanéité des positions (histogramme 1..25 positions).
- Performance détaillée par paire (les 28 paires Forex) et par devise (8 devises majeures).
- Impact du plafonnement du nombre de positions simultanées (Cap = 5, 8, 10, 15 vs Uncapped 25).
"""

import sys
import os
import copy
import statistics
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Tuple

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from relative_strength.config import ALL_28_PAIRS, PARIS_TZ
from relative_strength.models import RelativeStrengthState
from relative_strength.pip_engine import get_pip_size, calculate_pips, determine_directional_pips, update_excursion_metrics
from relative_strength.threshold import evaluate_threshold_state, evaluate_entry_zone, evaluate_independent_flags, evaluate_trade_eligibility
from relative_strength.persistence import update_persistence_counters
from relative_strength.velocity import calculate_velocity_and_acceleration
from relative_strength.breadth import compute_currency_strengths_from_pips, compute_currency_breadth
from relative_strength.scoring import compute_quality_score, compute_opportunity_score
from relative_strength.ranking import apply_triple_ranking
from relative_strength.execution import ExecutionManagerC, TradePosition, get_directional_currency_exposures
from relative_strength.multi_policy_backtest import fetch_multiday_h1_data


def run_portfolio_risk_audit_c1(period: str = "60d"):
    h1_map = fetch_multiday_h1_data(period)
    if not h1_map or len(h1_map) < len(ALL_28_PAIRS):
        print("❌ Données insuffisantes.")
        return

    sample_df = list(h1_map.values())[0]
    unique_dates = sorted(list(set(dt.strftime("%Y-%m-%d") for dt in sample_df.index if dt.weekday() < 5)))
    n_days = len(unique_dates)

    print(f"\n🔬 AUDIT DU RISQUE PORTEFEUILLE RSB-C1 SUR {n_days} JOURS DE TRADING")
    print("=" * 145)

    # Test avec C1 Uncapped (max 25) et C1 Capped (Cap 5, Cap 8, Cap 10, Cap 15)
    caps = [None, 5, 8, 10, 15]
    cap_results = {}

    for cap in caps:
        cap_name = f"C1 (Cap {cap})" if cap else "C1 (Uncapped)"
        mgr = ExecutionManagerC("C1", strict_rearm=True, use_invalidation_exit=True, use_pips_07h_pnl=True)

        equity_curve_pips = []
        active_counts_history = []
        timestamps_history = []
        cum_pips = 0.0

        for day_str in unique_dates:
            day_timestamps = [
                dt for dt in sample_df.index 
                if dt.strftime("%Y-%m-%d") == day_str and 7 <= dt.hour <= 22
            ]
            day_timestamps.sort()
            if len(day_timestamps) < 4:
                continue

            ref_dt = day_timestamps[0]
            reference_prices = {}
            for pair, df in h1_map.items():
                if ref_dt in df.index:
                    reference_prices[pair] = float(df.loc[ref_dt]["Open"])
                else:
                    day_sub = df[df.index.strftime("%Y-%m-%d") == day_str]
                    reference_prices[pair] = float(day_sub.iloc[0]["Open"]) if not day_sub.empty else 1.0

            prev_pairs_data: Dict[str, Dict[str, Any]] = {}
            prev_dt_paris = None

            for run_idx, dt_paris in enumerate(day_timestamps, start=1):
                if run_idx == 1:
                    current_prices_map = {pair: reference_prices[pair] for pair in ALL_28_PAIRS}
                    delta_hours = 1.0
                else:
                    delta_hours = (dt_paris - prev_dt_paris).total_seconds() / 3600.0 if prev_dt_paris else 1.0
                    current_prices_map = {
                        pair: float(df.loc[dt_paris]["Close"]) if dt_paris in df.index else reference_prices[pair]
                        for pair, df in h1_map.items()
                    }
                prev_dt_paris = dt_paris

                raw_pips_map = {
                    pair: calculate_pips(current_prices_map[pair], reference_prices[pair], get_pip_size(pair))[0]
                    for pair in ALL_28_PAIRS
                }

                currency_strengths = compute_currency_strengths_from_pips(raw_pips_map)
                states_list = []

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
                    
                    exclusive_state = evaluate_threshold_state(directional_pips, conf_runs)
                    entry_zone = evaluate_entry_zone(directional_pips)
                    trade_eligibility, is_eligible = evaluate_trade_eligibility(exclusive_state, entry_zone)
                    
                    base_curr, quote_curr = pair[:3], pair[3:]
                    base_str = currency_strengths.get(base_curr, 50.0)
                    quote_str = currency_strengths.get(quote_curr, 50.0)
                    strength_diff = base_str - quote_str
                    
                    conf_crosses, contrad_crosses, avail_crosses, b_ratio = compute_currency_breadth(
                        base_curr, "BULL" if trade_dir == "LONG" else "BEAR", raw_pips_map
                    )
                    
                    state = RelativeStrengthState(
                        pair=pair, timestamp=dt_paris.isoformat(), reference_time="07:00:00", reference_price=ref_p,
                        reference_source="yfinance_h1", current_price=cur_price, trade_direction=trade_dir,
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
                is_last_run = (run_idx == len(day_timestamps))

                # Si Cap appliqué, limiter active_positions dans process_run
                if cap and len(mgr.active_positions) >= cap:
                    mgr.update_positions_and_check_exits(states_list, is_last_run_of_day=is_last_run)
                else:
                    mgr.process_run(copy.deepcopy(states_list), is_last_run_of_day=is_last_run)

                # Tracking courbe d'équité pips
                closed_now = mgr.closed_trades
                cum_net_pips = sum(t.exit_pips - 1.0 for t in closed_now)  # Net @ 1.0p
                equity_curve_pips.append(cum_net_pips)
                active_counts_history.append(len(mgr.active_positions))
                timestamps_history.append(dt_paris)

        # Calcul du Max Drawdown en pips
        peak = -999999.0
        max_dd_pips = 0.0
        for eq in equity_curve_pips:
            if eq > peak:
                peak = eq
            dd = peak - eq
            if dd > max_dd_pips:
                max_dd_pips = dd

        # Calcul Max Drawdown en % (en supposant un capital de 10 000€ et risque 1.0% par trade, soit 100€ = 10 pips -> 10€/pip)
        # Equity de départ = 10,000€. Capital PnL = cum_net_pips * 10€.
        equity_eur = [10000.0 + (p * 10.0) for p in equity_curve_pips]
        peak_eur = 10000.0
        max_dd_pct = 0.0
        for eq_e in equity_eur:
            if eq_e > peak_eur:
                peak_eur = eq_e
            dd_p = (peak_eur - eq_e) / peak_eur * 100.0
            if dd_p > max_dd_pct:
                max_dd_pct = dd_p

        all_t = mgr.closed_trades
        n_t = len(all_t)
        net_1_tot = sum(t.exit_pips - 1.0 for t in all_t)
        win_r = (sum(1 for t in all_t if (t.exit_pips - 1.0) > 0) / n_t * 100.0) if n_t > 0 else 0.0
        gross_exp = sum(t.exit_pips for t in all_t) / n_t if n_t > 0 else 0.0

        cap_results[cap_name] = {
            "trades": n_t,
            "win_rate": win_r,
            "gross_exp": gross_exp,
            "net_1.0": net_1_tot,
            "max_dd_pips": max_dd_pips,
            "max_dd_pct": max_dd_pct,
            "active_counts": active_counts_history,
            "trades_list": all_t
        }

    # -----------------------------------------------------------------
    # TABLEAU 1 : IMPACT DU PLAFONNEMENT DES POSITIONS SIMULTANÉES
    # -----------------------------------------------------------------
    print("\n1️⃣ IMPACT DU PLAFONNEMENT DU NOMBRE DE POSITIONS SIMULTANÉES (RISK CONTROL)")
    print("-" * 130)
    print(f"{'CONFIGURATION':<20} | {'TRADES':<7} | {'WIN %':<7} | {'NET @ 1.0p':<12} | {'GROSS EXP/t':<12} | {'MAX DD (PIPS)':<15} | {'MAX DD (%) @1% Risk':<22}")
    print("-" * 130)
    for c_name, res in cap_results.items():
        print(f"{c_name:<20} | {res['trades']:<7} | {res['win_rate']:<6.1f}% | {res['net_1.0']:<+11.1f}p | {res['gross_exp']:<+11.2f}p | -{res['max_dd_pips']:<14.1f}p | -{res['max_dd_pct']:<21.2f}%")
    print("-" * 130)

    # -----------------------------------------------------------------
    # TABLEAU 2 : DISTRIBUTION DE LA SIMULTANÉITÉ (C1 UNCAPPED)
    # -----------------------------------------------------------------
    c1_uncapped = cap_results["C1 (Uncapped)"]
    active_hist = c1_uncapped["active_counts"]
    total_runs = len(active_hist)

    print("\n2️⃣ DISTRIBUTION DÉTAILLÉE DE LA SIMULTANÉITÉ DES POSITIONS (RSB-C1 UNCAPPED)")
    print("-" * 90)
    print(f"{'TRANCHE POSITIONS SIMULTANÉES':<35} | {'HEURES DE PRÉSENCE':<20} | {'% DU TEMPS TOTAL':<20}")
    print("-" * 90)

    bins = [(0, 0), (1, 5), (6, 10), (11, 15), (16, 20), (21, 25)]
    for low, high in bins:
        if low == high:
            cnt = sum(1 for x in active_hist if x == low)
            label = "0 position (Marché neutre)"
        else:
            cnt = sum(1 for x in active_hist if low <= x <= high)
            label = f"{low} à {high} positions simultanées"
        pct = (cnt / total_runs) * 100.0
        print(f"{label:<35} | {cnt:<20} | {pct:<19.1f}%")
    print("-" * 90)
    print(f"Médiane des positions simultanées : {int(statistics.median(active_hist))}")
    print(f"95ème percentile des positions    : {int(np.percentile(active_hist, 95))}")

    # -----------------------------------------------------------------
    # TABLEAU 3 : BREAKDOWN PAR PAIRE (TOP 5 BEST VS BOTTOM 5 WORST)
    # -----------------------------------------------------------------
    c1_trades = c1_uncapped["trades_list"]
    pair_stats: Dict[str, Dict[str, Any]] = {}
    for p in ALL_28_PAIRS:
        p_trades = [t for t in c1_trades if t.pair == p]
        if not p_trades:
            continue
        p_net = sum(t.exit_pips - 1.0 for t in p_trades)
        p_win = (sum(1 for t in p_trades if (t.exit_pips - 1.0) > 0) / len(p_trades)) * 100.0
        pair_stats[p] = {"pair": p, "trades": len(p_trades), "net_pips": p_net, "win_rate": p_win}

    sorted_pairs = sorted(pair_stats.values(), key=lambda x: x["net_pips"], reverse=True)

    print("\n3️⃣ PERFORMANCE DÉTAILLÉE PAR PAIRE (TOP 5 ET BOTTOM 5 SUR RSB-C1)")
    print("-" * 90)
    print("🏆 TOP 5 PAIRES LES PLUS PROFITABLES :")
    for item in sorted_pairs[:5]:
        print(f"  🟢 {item['pair']:<8} -> Trades: {item['trades']:<4} | WinRate: {item['win_rate']:<5.1f}% | Net @ 1.0p: {item['net_pips']:+.1f} pips")

    print("\n⚠️ BOTTOM 5 PAIRES LES MOINS PROFITABLES :")
    for item in sorted_pairs[-5:]:
        print(f"  🔴 {item['pair']:<8} -> Trades: {item['trades']:<4} | WinRate: {item['win_rate']:<5.1f}% | Net @ 1.0p: {item['net_pips']:+.1f} pips")
    print("-" * 90)

    # -----------------------------------------------------------------
    # TABLEAU 4 : BREAKDOWN PAR DEVISE (8 DEVISES MAJEURES)
    # -----------------------------------------------------------------
    currencies = ["EUR", "USD", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"]
    curr_stats = {}
    for curr in currencies:
        c_trades = [t for t in c1_trades if t.base_currency == curr or t.quote_currency == curr]
        c_net = sum(t.exit_pips - 1.0 for t in c_trades)
        c_win = (sum(1 for t in c_trades if (t.exit_pips - 1.0) > 0) / len(c_trades)) * 100.0 if c_trades else 0.0
        curr_stats[curr] = {"curr": curr, "trades": len(c_trades), "net_pips": c_net, "win_rate": c_win}

    sorted_curr = sorted(curr_stats.values(), key=lambda x: x["net_pips"], reverse=True)

    print("\n4️⃣ PERFORMANCE PAR DEVISE MAJEURE (CONTRIBUTION NETTE EN PIPS)")
    print("-" * 90)
    print(f"{'DEVISE':<10} | {'TRADES IMPLIQUÉS':<20} | {'WIN %':<10} | {'NET PIPS @ 1.0p':<15}")
    print("-" * 90)
    for c_info in sorted_curr:
        print(f"<code>{c_info['curr']:<10} | {c_info['trades']:<20} | {c_info['win_rate']:<9.1f}% | {c_info['net_pips']:<+14.1f}p</code>")
    print("-" * 90)

if __name__ == "__main__":
    run_portfolio_risk_audit_c1(period="60d")
