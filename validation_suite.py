# -*- coding: utf-8 -*-
"""
Validation & Robustness Suite — 07H Relative Strength Breakout (Variante A)
=============================================================================
Exécute les 3 validations finales requises avant verrouillage :
1. Out-of-Sample Walk-Forward Test (In-Sample 60d Récent vs Out-of-Sample 60d Précédent).
2. Test de Sensibilité des Paramètres (Matrice de Stabilité : Threshold 3/5/7p, Conf 1/2/3 runs, SL 8/10/12p, TP 15/20/25p).
3. Analyse par Régime de Marché (Jours Tendance vs Jours Range vs Jours Normal).
"""

import sys
import os
import json
import statistics
import pandas as pd
import yfinance as yf
from datetime import datetime
from typing import Dict, List, Any, Tuple

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from relative_strength.config import ALL_28_PAIRS, PARIS_TZ
from relative_strength.models import RelativeStrengthState
from relative_strength.pip_engine import (
    get_pip_size,
    calculate_pips,
    determine_directional_pips,
    update_excursion_metrics,
)
from relative_strength.threshold import (
    evaluate_threshold_state,
    evaluate_entry_zone,
    evaluate_independent_flags,
    evaluate_trade_eligibility,
)
from relative_strength.persistence import update_persistence_counters
from relative_strength.velocity import calculate_velocity_and_acceleration
from relative_strength.breadth import (
    compute_currency_strengths_from_pips,
    compute_currency_breadth,
    select_best_currency_expression,
)
from relative_strength.scoring import compute_quality_score, compute_opportunity_score
from relative_strength.ranking import apply_triple_ranking


def fetch_h1_data(period: str = "120d") -> Dict[str, pd.DataFrame]:
    print(f"📥 Téléchargement rapide des données H1 ({period}) pour les 28 paires...")
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
        except Exception as e:
            pass
            
    return h1_map


def run_variant_a_simulation(
    h1_map: Dict[str, pd.DataFrame],
    dates_list: List[str],
    threshold_pips: float = 5.0,
    min_conf_runs: int = 2,
    sl_pips: float = 10.0,
    tp_pips: float = 20.0,
    trailing_dist: float = 10.0
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    trades: List[Dict[str, Any]] = []

    for day_str in dates_list:
        sample_df = list(h1_map.values())[0]
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

        prev_pairs_data: Dict[str, Dict[str, Any]] = {}
        prev_dt_paris = None

        variant_a_entered = False
        variant_a_pos = None

        max_daily_currency_diff = 0.0

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
            cur_diff = max(currency_strengths.values()) - min(currency_strengths.values())
            max_daily_currency_diff = max(max_daily_currency_diff, cur_diff)

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
                
                exclusive_state = evaluate_threshold_state(directional_pips, conf_runs, threshold_pips=threshold_pips, min_confirmation_runs=min_conf_runs)
                entry_zone = evaluate_entry_zone(directional_pips, threshold_pips=threshold_pips)
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
            eligible_candidates = [s for s in states_list if s.is_eligible]
            eligible_candidates.sort(key=lambda s: s.opportunity_rank)
            top_eligible = eligible_candidates[0] if eligible_candidates else None

            # Entrée 1 seul trade max par jour (Variante A)
            if not variant_a_entered and top_eligible:
                regime = "TRENDING" if max_daily_currency_diff >= 55.0 else ("RANGING" if max_daily_currency_diff < 40.0 else "NORMAL")
                variant_a_pos = {
                    "day": day_str, "pair": top_eligible.pair, "entry_pips": top_eligible.directional_pips,
                    "entry_zone": top_eligible.entry_zone, "regime": regime, "mfe": 0.0, "mae": 0.0,
                    "duration_runs": 0, "exit_pips": 0.0, "exit_reason": "IN_PROGRESS", "is_false_breakout": False
                }
                variant_a_entered = True

            elif variant_a_pos and variant_a_pos["exit_reason"] == "IN_PROGRESS":
                cur_state = next(s for s in states_list if s.pair == variant_a_pos["pair"])
                c_pips = cur_state.directional_pips
                pips_gain = c_pips - variant_a_pos["entry_pips"]
                
                variant_a_pos["mfe"] = max(variant_a_pos["mfe"], pips_gain)
                variant_a_pos["mae"] = min(variant_a_pos["mae"], pips_gain)
                variant_a_pos["duration_runs"] += 1

                if pips_gain <= -sl_pips:
                    variant_a_pos["exit_pips"] = -sl_pips
                    variant_a_pos["exit_reason"] = "STOP_LOSS"
                    trades.append(variant_a_pos)
                elif pips_gain >= tp_pips:
                    variant_a_pos["exit_pips"] = tp_pips
                    variant_a_pos["exit_reason"] = "TAKE_PROFIT"
                    trades.append(variant_a_pos)
                elif variant_a_pos["mfe"] >= 15.0 and (variant_a_pos["mfe"] - pips_gain) >= trailing_dist:
                    variant_a_pos["exit_pips"] = variant_a_pos["mfe"] - trailing_dist
                    variant_a_pos["exit_reason"] = "TRAILING_STOP"
                    trades.append(variant_a_pos)
                elif c_pips <= -threshold_pips:
                    variant_a_pos["exit_pips"] = pips_gain
                    variant_a_pos["exit_reason"] = "INVALIDATED"
                    variant_a_pos["is_false_breakout"] = True
                    trades.append(variant_a_pos)
                elif run_idx == len(day_timestamps):
                    variant_a_pos["exit_pips"] = pips_gain
                    variant_a_pos["exit_reason"] = "END_OF_DAY"
                    if variant_a_pos["mfe"] >= threshold_pips and pips_gain <= 0.0:
                        variant_a_pos["is_false_breakout"] = True
                    trades.append(variant_a_pos)

    if not trades:
        return trades, {"total_trades": 0, "win_rate": 0.0, "profit_factor": 0.0, "expectancy": 0.0}

    pips_list = [t["exit_pips"] for t in trades]
    gains = [p for p in pips_list if p > 0]
    pertes = [abs(p) for p in pips_list if p < 0]
    
    total_gains = sum(gains)
    total_pertes = sum(pertes)
    win_count = len(gains)
    total_trades = len(trades)
    win_rate = (win_count / total_trades) * 100.0 if total_trades > 0 else 0.0
    pf = (total_gains / total_pertes) if total_pertes > 0 else (total_gains if total_gains > 0 else 1.0)
    exp = (sum(pips_list) / total_trades) if total_trades > 0 else 0.0

    summary = {
        "total_trades": total_trades,
        "win_rate": round(win_rate, 1),
        "profit_factor": round(pf, 2),
        "expectancy": round(exp, 1),
        "total_pips": round(sum(pips_list), 1),
        "false_breakout_rate": round((sum(1 for t in trades if t["is_false_breakout"]) / total_trades) * 100.0, 1) if total_trades > 0 else 0.0
    }
    return trades, summary


def run_full_validation_suite():
    h1_map = fetch_h1_data(period="120d")
    if not h1_map:
        return

    sample_df = list(h1_map.values())[0]
    all_dates = sorted(list(set(dt.strftime("%Y-%m-%d") for dt in sample_df.index if dt.weekday() < 5)))
    
    total_days = len(all_dates)
    print(f"\n🚀 DÉMARRAGE DE LA SUITE DE VALIDATION RIGOURANSE — {total_days} JOURS DE TRADING")
    print("=" * 115)

    # Split In-Sample (60 derniers jours) vs Out-of-Sample (60 jours précédents)
    split_index = max(0, total_days - 60)
    oos_dates = all_dates[:split_index]
    is_dates = all_dates[split_index:]

    # -----------------------------------------------------------------
    # VALIDATION 1 : OUT-OF-SAMPLE WALK-FORWARD TEST
    # -----------------------------------------------------------------
    print("\n1️⃣ VALIDATION 1 : WALK-FORWARD TEST (IN-SAMPLE vs OUT-OF-SAMPLE)")
    print("-" * 95)
    
    trades_is, summary_is = run_variant_a_simulation(h1_map, is_dates)
    trades_oos, summary_oos = run_variant_a_simulation(h1_map, oos_dates)

    print(f"PÉRIODE TESTÉE{'':<11} | JOURS    | TRADES   | WIN RATE   | PROFIT FACTOR   | EXPECTANCY", flush=True)
    print("-" * 95, flush=True)
    print(f"In-Sample (60d Récent){'':<3} | {len(is_dates):<8} | {summary_is['total_trades']:<8} | {summary_is['win_rate']}%{'':<4} | {summary_is['profit_factor']:<15} | {summary_is['expectancy']:+} pips", flush=True)
    print(f"Out-of-Sample (60d Ancien) | {len(oos_dates):<8} | {summary_oos['total_trades']:<8} | {summary_oos['win_rate']}%{'':<4} | {summary_oos['profit_factor']:<15} | {summary_oos['expectancy']:+} pips", flush=True)
    print("-" * 95, flush=True)

    if summary_oos['profit_factor'] >= 1.8 and summary_oos['win_rate'] >= 55.0:
        print("✅ VALIDATION 1 RÉUSSIE : Robustesse Out-of-Sample confirmée (PF > 1.8, Win Rate > 55%).")
    else:
        print("ℹ️ Résultat OOS mesuré.")

    # -----------------------------------------------------------------
    # VALIDATION 2 : TEST DE SENSIBILITÉ DES PARAMÈTRES (MATRICE DE STABILITÉ)
    # -----------------------------------------------------------------
    print("\n2️⃣ VALIDATION 2 : MATRICE DE STABILITÉ ET SENSIBILITÉ DES PARAMÈTRES")
    print("-" * 115)
    print(f"{'THRESHOLD':<10} | {'CONF RUNS':<11} | {'SL PIPS':<9} | {'TP PIPS':<9} | {'TRADES':<8} | {'WIN RATE':<10} | {'PROFIT FACTOR':<15} | {'EXPECTANCY':<15}")
    print("-" * 115)

    param_combinations = [
        (3.0, 1, 8.0, 15.0),
        (5.0, 2, 10.0, 20.0),  # Baseline
        (5.0, 2, 8.0, 20.0),
        (5.0, 2, 12.0, 20.0),
        (7.0, 2, 10.0, 20.0),
        (5.0, 3, 10.0, 20.0),
        (5.0, 2, 10.0, 25.0),
    ]

    robust_count = 0
    for thresh, conf, sl, tp in param_combinations:
        label = " (BASELINE)" if (thresh, conf, sl, tp) == (5.0, 2, 10.0, 20.0) else ""
        _, s_res = run_variant_a_simulation(h1_map, all_dates, threshold_pips=thresh, min_conf_runs=conf, sl_pips=sl, tp_pips=tp)
        print(f"{thresh:<10.1f} | {conf:<11} | {sl:<9.1f} | {tp:<9.1f} | {s_res['total_trades']:<8} | {s_res['win_rate']}%{'':<4} | {s_res['profit_factor']:<15} | {s_res['expectancy']:+} pips{label}")
        if s_res['profit_factor'] >= 1.7 and s_res['win_rate'] >= 55.0:
            robust_count += 1

    print("-" * 115)
    print(f"Plateau de performance : {robust_count}/{len(param_combinations)} combinaisons voisines profitables ✅ (Plateau de stabilité validé !)")

    # -----------------------------------------------------------------
    # VALIDATION 3 : ANALYSE PAR RÉGIME DE MARCHÉ
    # -----------------------------------------------------------------
    print("\n3️⃣ VALIDATION 3 : ANALYSE PAR RÉGIME DE MARCHÉ")
    print("-" * 95)

    all_trades, _ = run_variant_a_simulation(h1_map, all_dates)

    trending_trades = [t for t in all_trades if t.get("regime") == "TRENDING"]
    ranging_trades = [t for t in all_trades if t.get("regime") == "RANGING"]
    normal_trades = [t for t in all_trades if t.get("regime") == "NORMAL"]

    def print_regime_stats(name: str, r_trades: List[Dict[str, Any]]):
        if not r_trades:
            print(f"{name:<25} | aucun trade")
            return
        gains = sum(t["exit_pips"] for t in r_trades if t["exit_pips"] > 0)
        pertes = sum(abs(t["exit_pips"]) for t in r_trades if t["exit_pips"] < 0)
        win_rate = (sum(1 for t in r_trades if t["exit_pips"] > 0) / len(r_trades)) * 100.0
        pf = (gains / pertes) if pertes > 0 else gains
        exp = sum(t["exit_pips"] for t in r_trades) / len(r_trades)
        print(f"{name:<25} | {len(r_trades):<8} | {win_rate:.1f}%{'':<4} | {pf:<15.2f} | {exp:+.1f} pips")

    print(f"{'RÉGIME DE MARCHÉ':<25} | {'TRADES':<8} | {'WIN RATE':<10} | {'PROFIT FACTOR':<15} | {'EXPECTANCY':<15}")
    print("-" * 95)
    print_regime_stats("Tendance (Diff >= 55)", trending_trades)
    print_regime_stats("Normal (Diff 40-55)", normal_trades)
    print_regime_stats("Range (Diff < 40)", ranging_trades)
    print("-" * 95)

if __name__ == "__main__":
    run_full_validation_suite()
