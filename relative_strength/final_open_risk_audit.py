# -*- coding: utf-8 -*-
"""
Final Open Risk & Mark-to-Market Equity Audit Engine
====================================================
Analyse comparative du Risque Ouvert (Open Risk) et du Drawdown Floating MTM pour :
- C1 Uncapped (Max 25)
- C1 Cap 10 (Portfolio Capacity 10)
- C1 Cap 15 (Portfolio Capacity 15)

Calcule :
1. Maximum Open Risk (Pips & % à 1% par trade).
2. 95th Percentile Open Risk.
3. Currency-Adjusted Open Risk (Exposition directionnelle nette max par devise).
4. Mark-to-Market (MTM) Floating Drawdown (Equity temporelle incluant le PnL latent des trades en cours à chaque heure).
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


def run_final_open_risk_audit(period: str = "60d"):
    h1_map = fetch_multiday_h1_data(period)
    if not h1_map or len(h1_map) < len(ALL_28_PAIRS):
        print("❌ Données insuffisantes.")
        return

    sample_df = list(h1_map.values())[0]
    unique_dates = sorted(list(set(dt.strftime("%Y-%m-%d") for dt in sample_df.index if dt.weekday() < 5)))
    n_days = len(unique_dates)

    print(f"\n🔬 AUDIT FINAL DU RISQUE OUVERT & FLOATING MTM DRAWDOWN ({n_days} JOURS)")
    print("=" * 145)

    cap_configs = {"Uncapped (25)": None, "Cap 15": 15, "Cap 10": 10}
    final_audit_results = {}

    for c_label, cap_val in cap_configs.items():
        mgr = ExecutionManagerC("C1", strict_rearm=True, use_invalidation_exit=True, use_pips_07h_pnl=True)

        open_risk_pips_history = []
        open_risk_pct_history = []
        currency_open_risk_history = []
        mtm_equity_pips_history = []
        mtm_equity_eur_history = []
        
        cum_closed_net_pips = 0.0

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

                if cap_val and len(mgr.active_positions) >= cap_val:
                    mgr.update_positions_and_check_exits(states_list, is_last_run_of_day=is_last_run)
                else:
                    mgr.process_run(copy.deepcopy(states_list), is_last_run_of_day=is_last_run)

                # -------------------------------------------------------------
                # CALCUL DE L'OPEN RISK ET MARK-TO-MARKET (FLOATING EQUITY)
                # -------------------------------------------------------------
                active_pos = mgr.active_positions
                n_active = len(active_pos)
                
                # 1. Open Risk total au SL (-10p par position)
                open_risk_pips = n_active * 10.0  # 10 pips par position
                open_risk_pct = n_active * 1.0    # 1.0% de risque capital par position
                
                # 2. Currency-Adjusted Open Risk (Exposition directionnelle max par devise * 1%)
                exps = get_directional_currency_exposures(active_pos)
                curr_max_exp = max([abs(v) for v in exps.values()]) if exps else 0
                curr_adj_open_risk_pct = curr_max_exp * 1.0
                
                # 3. PnL latent floating des positions en cours à cette heure
                states_map = {s.pair: s for s in states_list}
                floating_pips = 0.0
                for pos in active_pos:
                    s_pair = states_map.get(pos.pair)
                    if s_pair:
                        floating_pips += (s_pair.directional_pips - pos.entry_pips_07h)

                # 4. Equité Mark-to-Market (Closed PnL Net + Floating PnL)
                closed_pips_net = sum(t.exit_pips - 1.0 for t in mgr.closed_trades)
                mtm_pips = closed_pips_net + floating_pips
                mtm_eur = 10000.0 + (mtm_pips * 10.0)

                open_risk_pips_history.append(open_risk_pips)
                open_risk_pct_history.append(open_risk_pct)
                currency_open_risk_history.append(curr_adj_open_risk_pct)
                mtm_equity_pips_history.append(mtm_pips)
                mtm_equity_eur_history.append(mtm_eur)

        # Calcul MTM Floating Drawdown Peak to Trough
        peak_mtm_pips = -999999.0
        max_mtm_dd_pips = 0.0
        for m_pip in mtm_equity_pips_history:
            if m_pip > peak_mtm_pips:
                peak_mtm_pips = m_pip
            dd_p = peak_mtm_pips - m_pip
            if dd_p > max_mtm_dd_pips:
                max_mtm_dd_pips = dd_p

        peak_mtm_eur = 10000.0
        max_mtm_dd_pct = 0.0
        for m_eur in mtm_equity_eur_history:
            if m_eur > peak_mtm_eur:
                peak_mtm_eur = m_eur
            dd_pct = (peak_mtm_eur - m_eur) / peak_mtm_eur * 100.0
            if dd_pct > max_mtm_dd_pct:
                max_mtm_dd_pct = dd_pct

        all_closed = mgr.closed_trades
        n_closed = len(all_closed)
        net_1_total = sum(t.exit_pips - 1.0 for t in all_closed)

        final_audit_results[c_label] = {
            "trades": n_closed,
            "win_rate": (sum(1 for t in all_closed if (t.exit_pips - 1.0) > 0) / n_closed * 100.0) if n_closed > 0 else 0.0,
            "net_1.0": net_1_total,
            "gross_exp": (sum(t.exit_pips for t in all_closed) / n_closed) if n_closed > 0 else 0.0,
            "max_open_risk_pips": max(open_risk_pips_history),
            "max_open_risk_pct": max(open_risk_pct_history),
            "p95_open_risk_pct": float(np.percentile(open_risk_pct_history, 95)),
            "max_curr_adj_open_risk_pct": max(currency_open_risk_history),
            "max_mtm_dd_pips": max_mtm_dd_pips,
            "max_mtm_dd_pct": max_mtm_dd_pct,
            "shadow_signals_blocked": len(mgr.rejected_signals) if cap_val else 0
        }

    # -----------------------------------------------------------------
    # TABLEAU COMPARATIF : UNCAPPED VS CAP 15 VS CAP 10
    # -----------------------------------------------------------------
    print(f"\n{'MÉTRIQUE DE RISQUE PORTEFEUILLE':<35} | {'RSB-C1 (UNCAPPED 25)':<25} | {'RSB-C1 (CAP 15)':<25} | {'RSB-C1 (CAP 10)':<25}")
    print("=" * 115)
    
    r_un = final_audit_results["Uncapped (25)"]
    r_c15 = final_audit_results["Cap 15"]
    r_c10 = final_audit_results["Cap 10"]

    print(f"{'Nombre de trades fermés':<35} | {r_un['trades']:<25} | {r_c15['trades']:<25} | {r_c10['trades']:<25}")
    print(f"{'Taux de réussite (Win Rate %)':<35} | {r_un['win_rate']:.1f}%{'':<20} | {r_c15['win_rate']:.1f}%{'':<20} | {r_c10['win_rate']:.1f}%{'':<20}")
    print(f"{'Espérance brute (pips/trade)':<35} | {r_un['gross_exp']:+.2f} pips{'':<15} | {r_c15['gross_exp']:+.2f} pips{'':<15} | {r_c10['gross_exp']:+.2f} pips{'':<15}")
    print(f"{'Pips Net cumulés @ 1.0p':<35} | {r_un['net_1.0']:+.1f} pips{'':<14} | {r_c15['net_1.0']:+.1f} pips{'':<14} | {r_c10['net_1.0']:+.1f} pips{'':<14}")
    print("-" * 115)
    print(f"{'MAX OPEN RISK (Pips au SL)':<35} | {r_un['max_open_risk_pips']:.0f} pips{'':<18} | {r_c15['max_open_risk_pips']:.0f} pips{'':<18} | {r_c10['max_open_risk_pips']:.0f} pips{'':<18}")
    print(f"{'MAX OPEN RISK (% Capital)':<35} | {r_un['max_open_risk_pct']:.1f}%{'':<20} | {r_c15['max_open_risk_pct']:.1f}%{'':<20} | {r_c10['max_open_risk_pct']:.1f}%{'':<20}")
    print(f"{'95e Percentile Open Risk (%)':<35} | {r_un['p95_open_risk_pct']:.1f}%{'':<20} | {r_c15['p95_open_risk_pct']:.1f}%{'':<20} | {r_c10['p95_open_risk_pct']:.1f}%{'':<20}")
    print(f"{'Currency-Adjusted Open Risk Max':<35} | {r_un['max_curr_adj_open_risk_pct']:.1f}% (Net Expo 7){'':<7} | {r_c15['max_curr_adj_open_risk_pct']:.1f}% (Net Expo 7){'':<7} | {r_c10['max_curr_adj_open_risk_pct']:.1f}% (Net Expo 5){'':<7}")
    print("-" * 115)
    print(f"{'FLOATING MTM MAX DRAWDOWN (Pips)':<35} | -{r_un['max_mtm_dd_pips']:.1f} pips{'':<14} | -{r_c15['max_mtm_dd_pips']:.1f} pips{'':<14} | -{r_c10['max_mtm_dd_pips']:.1f} pips{'':<14}")
    print(f"{'FLOATING MTM MAX DRAWDOWN (%)':<35} | -{r_un['max_mtm_dd_pct']:.2f}%{'':<18} | -{r_c15['max_mtm_dd_pct']:.2f}%{'':<18} | -{r_c10['max_mtm_dd_pct']:.2f}%{'':<18}")
    print(f"{'Signaux enregistrés en Shadow':<35} | {r_un['shadow_signals_blocked']:<25} | {r_c15['shadow_signals_blocked']:<25} | {r_c10['shadow_signals_blocked']:<25}")
    print("=" * 115)

if __name__ == "__main__":
    run_final_open_risk_audit(period="60d")
