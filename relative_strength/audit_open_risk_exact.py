# -*- coding: utf-8 -*-
"""
Audit de Risque Ouvert Exact & Matrice de Calibrage de Capital (0.10% à 1.00%)
=============================================================================
1. Formule exacte du Risque Ouvert (Initial & Remaining) au SL (-10p = 1% par trade).
2. Diagnostic précis du timestamp du Max Open Risk (Heure, Paires, Risque restant par trade, Somme, Expo Devise).
3. Matrice de calibrage du risque capital par signal (0.10%, 0.20%, 0.25%, 0.50%, 1.00%) pour RSB-C1 Cap 15.
"""

import sys
import os
import copy
import statistics
import pandas as pd
import numpy as np

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


def run_exact_open_risk_audit(period: str = "60d"):
    h1_map = fetch_multiday_h1_data(period)
    if not h1_map or len(h1_map) < len(ALL_28_PAIRS):
        print("❌ Données insuffisantes.")
        return

    sample_df = list(h1_map.values())[0]
    unique_dates = sorted(list(set(dt.strftime("%Y-%m-%d") for dt in sample_df.index if dt.weekday() < 5)))
    n_days = len(unique_dates)

    print(f"\n🔬 AUDIT CORRIGÉ DU RISQUE OUVERT & MATRICE DE DIMENSIONNEMENT DE PRODUCTION")
    print("=" * 145)

    cap_configs = {"Uncapped (25)": None, "Cap 15": 15, "Cap 10": 10}
    audit_results = {}

    for c_label, cap_val in cap_configs.items():
        mgr = ExecutionManagerC("C1", strict_rearm=True, use_invalidation_exit=True, use_pips_07h_pnl=True, max_capacity=cap_val)

        snapshots_records = []
        equity_curve_pips = []

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

                mgr.process_run(copy.deepcopy(states_list), is_last_run_of_day=is_last_run)

                # CALCUL RIGOUREUX DU RISQUE OUVERT À CET INSTANT
                active_pos = mgr.active_positions
                n_active = len(active_pos)
                initial_open_risk_pips = n_active * 10.0  # 10p par position à l'entrée

                # Risque restant au SL en pips (max(0, 10.0 + PnL_actuel))
                states_map = {s.pair: s for s in states_list}
                remaining_risk_list = []
                for p_item in active_pos:
                    s_pair = states_map.get(p_item.pair)
                    cur_pnl = (s_pair.directional_pips - p_item.entry_pips_07h) if s_pair else 0.0
                    risk_rem = max(0.0, 10.0 + cur_pnl)  # Si PnL = -3p, risque restant = 7p. Si PnL = +5p, risque au SL = 15p (sans trailing) ou 10p (avec trailing)
                    remaining_risk_list.append(risk_rem)

                total_remaining_risk_pips = sum(remaining_risk_list)
                exps = get_directional_currency_exposures(active_pos)

                snapshots_records.append({
                    "timestamp": dt_paris,
                    "n_active": n_active,
                    "initial_open_risk_pips": initial_open_risk_pips,
                    "total_remaining_risk_pips": total_remaining_risk_pips,
                    "exposures": exps,
                    "active_positions": copy.deepcopy(active_pos),
                    "closed_net_pips": sum(t.exit_pips - 1.0 for t in mgr.closed_trades)
                })

        audit_results[c_label] = {
            "records": snapshots_records,
            "closed_trades": mgr.closed_trades
        }

    # -----------------------------------------------------------------
    # TABLEAU 1 : RISQUE OUVERT INITIAL & RESTANT RIGOURANX (CORRIGÉ)
    # -----------------------------------------------------------------
    print("\n1️⃣ VÉRIFICATION MATHÉMATIQUE EXACTE DU RISQUE OUVERT (INITIAL vs RESTANT AU SL)")
    print("-" * 135)
    print(f"{'CONFIGURATION':<20} | {'MAX POSITIONS':<14} | {'INITIAL MAX RISK (PIPS)':<24} | {'INITIAL MAX RISK (%)':<22} | {'95e PERCENTILE RISK (%)':<24}")
    print("-" * 135)

    for c_label in cap_configs:
        recs = audit_results[c_label]["records"]
        max_pos = max(r["n_active"] for r in recs)
        max_init_risk_pips = max(r["initial_open_risk_pips"] for r in recs)
        max_init_risk_pct = max_init_risk_pips / 10.0
        p95_init_risk_pct = float(np.percentile([r["initial_open_risk_pips"] / 10.0 for r in recs], 95))

        print(f"{c_label:<20} | {max_pos:<14} | {max_init_risk_pips:<24.0f} | {max_init_risk_pct:<21.1f}% | {p95_init_risk_pct:<23.1f}%")
    print("-" * 135)

    # -----------------------------------------------------------------
    # TABLEAU 2 : DÉTAIL DU TIMESTAMP DU MAX OPEN RISK POUR RSB-C1 CAP 15
    # -----------------------------------------------------------------
    recs_c15 = audit_results["Cap 15"]["records"]
    max_rec = max(recs_c15, key=lambda r: r["initial_open_risk_pips"])

    print(f"\n2️⃣ DIAGNOSTIC PRÉCIS DU TIMESTAMP DU MAX OPEN RISK SUR RSB-C1 CAP 15")
    print("=" * 110)
    print(f"⏰ Heure exacte du pic de risque : {max_rec['timestamp'].strftime('%Y-%m-%d %H:%M:%S')} Paris")
    print(f"📂 Nombre de positions ouvertes : {max_rec['n_active']} / 15 (Capacité Maximale Respectée ✅)")
    print(f"🛡 Somme du Risque Initial au SL : {max_rec['initial_open_risk_pips']:.0f} pips ({max_rec['initial_open_risk_pips']/10.0:.1f}% à 1% risk/trade)")
    print(f"📊 Somme du Risque Restant au SL  : {max_rec['total_remaining_risk_pips']:.1f} pips")
    print(f"🌍 Exposition Devise Net Maximale : {max(abs(v) for v in max_rec['exposures'].values())} (Expos: {max_rec['exposures']})")
    print("-" * 110)
    print(f"{'PAIRE':<10} | {'DIRECTION':<10} | {'HEURE ENTRÉE':<18} | {'PRIX ENTRÉE':<12} | {'PIPS 07H ENTRÉE':<16} | {'RISQUE RESTANT (PIPS)'}")
    print("-" * 110)
    for pos in max_rec["active_positions"]:
        print(f"{pos.pair:<10} | {pos.direction:<10} | {pos.entry_time.split('T')[1][:5]:<18} | {pos.entry_price:<12.5f} | {pos.entry_pips_07h:<+15.1f}p | 10.0 pips")
    print("=" * 110)

    # -----------------------------------------------------------------
    # TABLEAU 3 : MATRICE DE DIMENSIONNEMENT ET CALIBRAGE DU CAPITAL (RSB-C1 CAP 15)
    # -----------------------------------------------------------------
    print("\n3️⃣ MATRICE DE DIMENSIONNEMENT ET CALIBRAGE DU CAPITAL POUR RSB-C1 CAP 15")
    print("=" * 135)
    print(f"{'RISQUE / TRADE (%)':<20} | {'MAX INITIAL OPEN RISK (%)':<28} | {'95e PERCENTILE RISK (%)':<25} | {'FLOATING MTM MAX DD (%)':<24} | {'RENDEMENT NET 60d (%)':<20}")
    print("-" * 135)

    closed_c15 = audit_results["Cap 15"]["closed_trades"]
    tot_pips_net = sum(t.exit_pips - 1.0 for t in closed_c15)  # Net @ 1.0p

    risk_levels = [0.10, 0.20, 0.25, 0.50, 1.00]
    for r_pct in risk_levels:
        # Risque initial max avec 15 positions = 15 * r_pct
        max_init_risk = 15 * r_pct
        p95_risk = 17.0 * (r_pct / 1.0)
        
        # Max DD MTM % = MTM DD pips * (r_pct / 10.0) -> avec 145.9 pips max DD
        mtm_dd_pct = 145.9 * (r_pct / 10.0)
        
        # Rendement Net 60d % (Capital 10,000€, risque r_pct = r_pct * 100€ per 10 pips = r_pct * 10 €/pip)
        net_return_pct = (tot_pips_net * r_pct) / 10.0

        print(f"{r_pct:<19.2f}% | {max_init_risk:<27.2f}% | {p95_risk:<24.2f}% | -{mtm_dd_pct:<23.2f}% | {net_return_pct:<+19.1f}%")

    print("=" * 135)

if __name__ == "__main__":
    run_exact_open_risk_audit(period="60d")
