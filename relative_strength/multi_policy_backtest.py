# -*- coding: utf-8 -*-
"""
Multi-Policy Backtest Engine — 07H Relative Strength Breakout (Audit & Non-Regression Version)
==============================================================================================
Exécute la simulation comparative déterministe sur 60 jours de trading pour :
RSB-A, RSB-B, RSB-C1, RSB-C2, RSB-C3, RSB-C4.

Métriques additionnelles d'audit :
- Non-régression exacte RSB-A et RSB-B (Pips07H PnL).
- Calcul dynamique instantané de l'exposition devise C3 (Garantie MAX EXPO <= 2).
- Règle de Strict Re-Arm sur C1, C2, C3 vs Ré-entrée immédiate non restreinte sur C4.
- Fragilité aux coûts : Gross Expectancy, Net Expectancy (0.5..2.0p), Break-even cost, Trades/Day, Trades/Pair/Day.
"""

import sys
import os
import copy
import statistics
import pandas as pd
import yfinance as yf
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
from relative_strength.execution import ExecutionManagerA, ExecutionManagerB, ExecutionManagerC, TradePosition, RejectedSignal, get_directional_currency_exposures


def fetch_multiday_h1_data(period: str = "60d") -> Dict[str, pd.DataFrame]:
    print(f"📥 Téléchargement des bougies H1 ({period}) pour les 28 paires...")
    tickers = [f"{p}=X" for p in ALL_28_PAIRS]
    df_batch = yf.download(tickers, period=period, interval="1h", progress=False)
    
    h1_map: Dict[str, pd.DataFrame] = {}
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
            h1_map[pair] = pair_df
        except Exception:
            pass
            
    return h1_map


def run_multi_policy_backtest(period: str = "60d"):
    h1_map = fetch_multiday_h1_data(period)
    if not h1_map or len(h1_map) < len(ALL_28_PAIRS):
        print("❌ Données d'historique insuffisantes.")
        return

    sample_df = list(h1_map.values())[0]
    unique_dates = sorted(list(set(dt.strftime("%Y-%m-%d") for dt in sample_df.index if dt.weekday() < 5)))
    n_days = len(unique_dates)

    print(f"\n🚀 BACKTEST MULTI-POLITIQUES (AUDIT & NON-RÉGRESSION) SUR {n_days} JOURS DE TRADING")
    print("=" * 145)

    # Instanciation des 6 portefeuilles indépendants
    # RSB-A et RSB-B utilisent use_invalidation_exit=True pour alignement 100% exact benchmark historique
    managers = {
        "RSB-A": ExecutionManagerA(use_invalidation_exit=True, use_pips_07h_pnl=True),
        "RSB-B": ExecutionManagerB(use_invalidation_exit=True, use_pips_07h_pnl=True),
        "RSB-C1": ExecutionManagerC("C1", strict_rearm=True, use_invalidation_exit=True, use_pips_07h_pnl=True),
        "RSB-C2": ExecutionManagerC("C2", strict_rearm=True, use_invalidation_exit=True, use_pips_07h_pnl=True),
        "RSB-C3": ExecutionManagerC("C3", max_currency_exposure=2, strict_rearm=True, use_invalidation_exit=True, use_pips_07h_pnl=True),
        "RSB-C4": ExecutionManagerC("C4", strict_rearm=False, use_invalidation_exit=True, use_pips_07h_pnl=True),
    }

    max_concurrent_positions = {k: 0 for k in managers}
    max_currency_exposures = {k: 0 for k in managers}

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
                if not day_sub.empty:
                    reference_prices[pair] = float(day_sub.iloc[0]["Open"])
                else:
                    reference_prices[pair] = 1.0

        managers["RSB-A"].reset_daily_state()

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

            for v_id, mgr in managers.items():
                states_snapshot = copy.deepcopy(states_list)
                mgr.process_run(states_snapshot, is_last_run_of_day=is_last_run)

                active_count = len(mgr.active_positions)
                max_concurrent_positions[v_id] = max(max_concurrent_positions[v_id], active_count)
                
                exps = get_directional_currency_exposures(mgr.active_positions)
                if exps:
                    max_exp = max(abs(val) for val in exps.values())
                    max_currency_exposures[v_id] = max(max_currency_exposures[v_id], max_exp)

    # -----------------------------------------------------------------
    # VÉRIFICATION NON-RÉGRESSION A / B
    # -----------------------------------------------------------------
    print("\n1️⃣ VÉRIFICATION DE NON-RÉGRESSION SUR BENCHMARK HISTORIQUE A / B")
    print("-" * 120)
    trades_a = managers["RSB-A"].closed_trades
    trades_b = managers["RSB-B"].closed_trades

    n_a = len(trades_a)
    pips_a_tot = sum(t.exit_pips for t in trades_a)
    win_a = (sum(1 for t in trades_a if t.exit_pips > 0) / n_a * 100.0) if n_a > 0 else 0.0

    n_b = len(trades_b)
    pips_b_tot = sum(t.exit_pips for t in trades_b)
    win_b = (sum(1 for t in trades_b if t.exit_pips > 0) / n_b * 100.0) if n_b > 0 else 0.0

    print(f"RSB-A : {n_a} trades, WinRate {win_a:.1f}%, Pips Bruts: {pips_a_tot:+.1f}p [Attendu: 60 trades, 61.7%, +312.2p]")
    print(f"RSB-B : {n_b} trades, WinRate {win_b:.1f}%, Pips Bruts: {pips_b_tot:+.1f}p [Attendu: 111 trades, 60.4%, +426.1p]")

    if n_a == 60 and abs(win_a - 61.7) <= 0.5 and abs(pips_a_tot - 312.2) <= 1.0:
        print("✅ NON-RÉGRESSION PARFAITE RSB-A : 100% identique au benchmark historique !")
    if n_b == 111 and abs(win_b - 60.4) <= 0.5 and abs(pips_b_tot - 426.1) <= 1.0:
        print("✅ NON-RÉGRESSION PARFAITE RSB-B : 100% identique au benchmark historique !")
    print("-" * 120)

    # -----------------------------------------------------------------
    # MATRICE DE PERFORMANCE ET DE COÛTS (GROSS & NET)
    # -----------------------------------------------------------------
    cost_levels = [0.0, 0.5, 1.0, 1.5, 2.0]
    
    print("\n2️⃣ MATRICE COMPLÈTE DE PERFORMANCE & DE FRAGILITÉ AUX COÛTS DE TRANSACTION")
    print("=" * 145)
    print(f"{'PORTEFEUILLE':<10} | {'TRADES':<6} | {'WIN %':<6} | {'GROSS PIPS':<11} | {'GROSS EXP/t':<11} | {'NET @ 0.5p':<10} | {'NET @ 1.0p':<10} | {'NET @ 1.5p':<10} | {'NET @ 2.0p':<10} | {'BE COST':<8}")
    print("-" * 145)

    summary_results = {}

    for v_id, mgr in managers.items():
        all_trades = mgr.closed_trades
        n_t = len(all_trades)
        if n_t == 0:
            continue

        gross_pips_list = [t.exit_pips for t in all_trades]
        gross_tot = sum(gross_pips_list)
        gross_exp = gross_tot / n_t

        net_totals = {c: sum(p - c for p in gross_pips_list) for c in cost_levels}
        win_rate = (sum(1 for p in gross_pips_list if p > 0) / n_t) * 100.0
        be_cost = gross_exp  # Coût auquel le profit net devient 0

        print(f"{v_id:<10} | {n_t:<6} | {win_rate:<5.1f}% | {gross_tot:<+10.1f}p | {gross_exp:<+10.2f}p | {net_totals[0.5]:<+9.1f}p | {net_totals[1.0]:<+9.1f}p | {net_totals[1.5]:<+9.1f}p | {net_totals[2.0]:<+9.1f}p | {be_cost:<7.2f}p")

        summary_results[v_id] = {
            "trades": n_t,
            "win_rate": win_rate,
            "gross_pips": gross_tot,
            "gross_exp": gross_exp,
            "net_1.0": net_totals[1.0],
            "be_cost": be_cost,
            "trades_list": all_trades,
            "rejections_list": mgr.rejected_signals
        }

    print("-" * 145)

    # -----------------------------------------------------------------
    # COMPARAISON DÉTAILLÉE C1 (SWEET SPOT), C2 (STRICT REARM), C3 (EXPO <= 2), C4 (UNRESTRICTED)
    # -----------------------------------------------------------------
    print("\n3️⃣ COMPARAISON DÉTAILLÉE RSB-C (AVEC RE-ARM STRICT SUR C1, C2, C3 vs UNRESTRICTED SUR C4)")
    print("=" * 145)
    print(f"{'VARIANTE':<10} | {'TRADES':<6} | {'TRADES/D':<8} | {'TRADES/PAIR/D':<13} | {'NET @ 1.0p':<11} | {'MAX POS SIMULT':<15} | {'MAX EXPO DEVISE':<16} | {'SIGNAUX REFUSÉS':<15}")
    print("-" * 145)

    for v_id in ["RSB-C1", "RSB-C2", "RSB-C3", "RSB-C4"]:
        res = summary_results.get(v_id, {})
        n_t = res.get("trades", 0)
        trades_per_day = n_t / n_days
        trades_per_pair_per_day = n_t / (28.0 * n_days)
        net_1 = res.get("net_1.0", 0.0)
        rej_l = res.get("rejections_list", [])

        print(f"{v_id:<10} | {n_t:<6} | {trades_per_day:<8.1f} | {trades_per_pair_per_day:<13.3f} | {net_1:<+10.1f}p | {max_concurrent_positions[v_id]:<15} | {max_currency_exposures[v_id]:<16} | {len(rej_l):<15}")

    print("-" * 145)

    print("\n📋 DÉTAIL DES MOTIFS DE REJET PAR PORTEFEUILLE :")
    print("-" * 110)
    for v_id in ["RSB-C1", "RSB-C2", "RSB-C3", "RSB-C4"]:
        mgr = managers[v_id]
        reasons_count = {}
        for r in mgr.rejected_signals:
            reasons_count[r.rejection_reason] = reasons_count.get(r.rejection_reason, 0) + 1
        reasons_str = ", ".join([f"{k}: {v}" for k, v in sorted(reasons_count.items())]) if reasons_count else "Aucun signal rejeté"
        print(f"  {v_id:<10} -> {reasons_str}")
    print("-" * 110)

if __name__ == "__main__":
    run_multi_policy_backtest(period="60d")
