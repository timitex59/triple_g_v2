# -*- coding: utf-8 -*-
"""
Multi-Day Backtest Engine — 07H Relative Strength Breakout
==========================================================
Backtest multi-journées déterministe comparant Variant A (Setup Unique Quotidien) 
et Variant B (Rotation Intraday) avec la Porte d'Éligibilité (Trade Eligibility Gate).

Mesure les 16 métriques de performance :
- Nombre de trades, Win Rate, Profit Factor, Expectancy.
- Gain moyen, Perte moyenne, Médiane MFE, Médiane MAE.
- Taux de faux breakouts, Taux de survie au threshold, Durée moyenne de rétention.
- Performance séparée : SWEET_SPOT vs ELIGIBLE_WITH_PENALTY.
"""

import sys
import os
import json
import statistics
import pandas as pd
import yfinance as yf
from datetime import datetime, time
from typing import Dict, List, Any, Tuple

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from relative_strength.config import ALL_28_PAIRS, PARIS_TZ, THRESHOLD_PIPS
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


def fetch_multiday_h1_data(period: str = "60d") -> Dict[str, pd.DataFrame]:
    """
    Télécharge l'historique H1 multi-journées pour les 28 paires.
    """
    print(f"📥 Téléchargement des données H1 multi-journées ({period}) pour les 28 paires...")
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
            print(f"⚠️ Erreur extraction {pair}: {e}")
            
    return h1_map


def run_multiday_backtest(period: str = "60d"):
    h1_map = fetch_multiday_h1_data(period)
    if not h1_map or len(h1_map) < len(ALL_28_PAIRS):
        print("❌ Données d'historique insuffisantes.")
        return

    # Extraire les dates uniques disponibles (du lundi au vendredi)
    sample_df = list(h1_map.values())[0]
    unique_dates = sorted(list(set(dt.strftime("%Y-%m-%d") for dt in sample_df.index if dt.weekday() < 5)))
    
    print(f"\n📊 DÉMARRAGE DU BACKTEST MULTI-JOURNÉES : {len(unique_dates)} JOURS DE TRADING")
    print("=" * 110)

    trades_variant_a: List[Dict[str, Any]] = []
    trades_variant_b: List[Dict[str, Any]] = []

    for day_str in unique_dates:
        # Récupération des timestamps pour la journée >= 07:00 Paris
        day_timestamps = [
            dt for dt in sample_df.index 
            if dt.strftime("%Y-%m-%d") == day_str and 7 <= dt.hour <= 22
        ]
        day_timestamps.sort()
        
        if len(day_timestamps) < 4:
            continue  # Journée incomplète

        # Prix de référence 07h00 Paris (Open de la première bougie >= 07:00)
        ref_dt = day_timestamps[0]
        reference_prices = {}
        for pair, df in h1_map.items():
            if ref_dt in df.index:
                reference_prices[pair] = float(df.loc[ref_dt]["Open"])
            else:
                # Premier prix dispo pour ce jour
                day_sub = df[df.index.strftime("%Y-%m-%d") == day_str]
                if not day_sub.empty:
                    reference_prices[pair] = float(day_sub.iloc[0]["Open"])
                else:
                    reference_prices[pair] = 1.0

        prev_pairs_data: Dict[str, Dict[str, Any]] = {}
        prev_dt_paris = None

        # Tracking des trades pour la journée
        variant_a_entered = False
        variant_a_position = None
        
        variant_b_position = None

        for run_idx, dt_paris in enumerate(day_timestamps, start=1):
            if run_idx == 1:
                current_prices_map = {pair: reference_prices[pair] for pair in ALL_28_PAIRS}
                delta_hours = 1.0
            else:
                if prev_dt_paris:
                    delta_hours = (dt_paris - prev_dt_paris).total_seconds() / 3600.0
                else:
                    delta_hours = 1.0
                current_prices_map = {
                    pair: float(df.loc[dt_paris]["Close"]) if dt_paris in df.index else reference_prices[pair]
                    for pair, df in h1_map.items()
                }
                
            prev_dt_paris = dt_paris

            raw_pips_map: Dict[str, float] = {}
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
                
                if raw_market > 0:
                    trade_dir = "LONG"
                    align_symbol = "↑↑"
                elif raw_market < 0:
                    trade_dir = "SHORT"
                    align_symbol = "↓↓"
                else:
                    trade_dir = "NEUTRAL"
                    align_symbol = "⚪"
                    
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
                
                has_tech_warning = (align_symbol in ["↑↓", "↓↑"])
                conf_runs, above_runs, dir_runs, warn_runs = update_persistence_counters(
                    directional_pips, trade_dir, has_tech_warning, prev_persistence_map
                )
                
                is_exp, is_tr, is_exh, rev_warn, has_warn, is_ch = evaluate_independent_flags(
                    directional_pips, prev_dir_pips, conf_runs, mfe, dd_pct, velocity_raw, accel_raw, has_tech_warning
                )
                
                exclusive_state = evaluate_threshold_state(directional_pips, conf_runs)
                entry_zone = evaluate_entry_zone(directional_pips)
                
                # Trade Eligibility Gate
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
                    pair=pair,
                    timestamp=dt_paris.isoformat(),
                    reference_time="07:00:00",
                    reference_price=ref_p,
                    reference_source="yfinance_h1",
                    current_price=cur_price,
                    trade_direction=trade_dir,
                    raw_market_pips=raw_market,
                    long_pips=long_pips,
                    short_pips=short_pips,
                    directional_pips=directional_pips,
                    threshold_state=exclusive_state,
                    trade_eligibility=trade_eligibility,
                    is_eligible=is_eligible,
                    is_expanding=is_exp,
                    is_trending=is_tr,
                    is_exhausting=is_exh,
                    reversal_warning=rev_warn,
                    has_warning=has_warn,
                    is_chasing=is_ch,
                    above_threshold_runs=above_runs,
                    confirmation_runs=conf_runs,
                    signal_direction_runs=dir_runs,
                    warning_runs=warn_runs,
                    mfe_pips=mfe,
                    mae_pips=mae,
                    drawdown_from_mfe=dd_pips,
                    drawdown_pct=dd_pct,
                    efficiency=eff,
                    velocity_pips_per_hour=velocity_raw,
                    acceleration_pips_per_hour2=accel_raw,
                    entry_zone=entry_zone,
                    base_currency=base_curr,
                    quote_currency=quote_curr,
                    base_strength=base_str,
                    quote_strength=quote_str,
                    strength_differential=strength_diff,
                    confirming_crosses=conf_crosses,
                    contradicting_crosses=contrad_crosses,
                    available_crosses=avail_crosses,
                    breadth_ratio=b_ratio,
                    technical_alignment=align_symbol,
                    has_fire_signal=False
                )
                
                state.quality_score = compute_quality_score(state)
                state.opportunity_score = compute_opportunity_score(state)
                
                states_list.append(state)
                prev_pairs_data[pair] = state.to_dict()

            apply_triple_ranking(states_list)

            eligible_candidates = [s for s in states_list if s.is_eligible]
            eligible_candidates.sort(key=lambda s: s.opportunity_rank)

            top_eligible = eligible_candidates[0] if eligible_candidates else None

            # -----------------------------------------------------------------
            # SIMULATION VARIANT A (SETUP UNIQUE QUOTIDIEN)
            # -----------------------------------------------------------------
            if not variant_a_entered and top_eligible:
                variant_a_position = {
                    "day": day_str,
                    "pair": top_eligible.pair,
                    "direction": top_eligible.trade_direction,
                    "entry_time": dt_paris.strftime("%H:%M"),
                    "entry_pips": top_eligible.directional_pips,
                    "entry_zone": top_eligible.entry_zone,
                    "opportunity_score": top_eligible.opportunity_score,
                    "mfe": top_eligible.directional_pips,
                    "mae": top_eligible.directional_pips,
                    "exit_pips": top_eligible.directional_pips,
                    "exit_reason": "IN_PROGRESS",
                    "duration_runs": 1,
                    "is_false_breakout": False
                }
                variant_a_entered = True

            elif variant_a_position and variant_a_position["exit_reason"] == "IN_PROGRESS":
                cur_state = next(s for s in states_list if s.pair == variant_a_position["pair"])
                c_pips = cur_state.directional_pips
                
                variant_a_position["mfe"] = max(variant_a_position["mfe"], c_pips)
                variant_a_position["mae"] = min(variant_a_position["mae"], c_pips)
                variant_a_position["duration_runs"] += 1
                variant_a_position["exit_pips"] = c_pips

                # Traitement des Invalidations / Prises de Profit
                if c_pips <= -THRESHOLD_PIPS or cur_state.threshold_state == "INVALIDATED":
                    variant_a_position["exit_reason"] = "INVALIDATED"
                    variant_a_position["is_false_breakout"] = True
                    trades_variant_a.append(variant_a_position)
                elif run_idx == len(day_timestamps):
                    variant_a_position["exit_reason"] = "END_OF_DAY"
                    if variant_a_position["mfe"] >= THRESHOLD_PIPS and c_pips <= THRESHOLD_PIPS:
                        variant_a_position["is_false_breakout"] = True
                    trades_variant_a.append(variant_a_position)

            # -----------------------------------------------------------------
            # SIMULATION VARIANT B (ROTATION INTRADAY)
            # -----------------------------------------------------------------
            if not variant_b_position and top_eligible:
                variant_b_position = {
                    "day": day_str,
                    "pair": top_eligible.pair,
                    "direction": top_eligible.trade_direction,
                    "entry_time": dt_paris.strftime("%H:%M"),
                    "entry_pips": top_eligible.directional_pips,
                    "entry_zone": top_eligible.entry_zone,
                    "opportunity_score": top_eligible.opportunity_score,
                    "mfe": top_eligible.directional_pips,
                    "mae": top_eligible.directional_pips,
                    "exit_pips": top_eligible.directional_pips,
                    "exit_reason": "IN_PROGRESS",
                    "duration_runs": 1,
                    "is_false_breakout": False
                }
            elif variant_b_position and variant_b_position["exit_reason"] == "IN_PROGRESS":
                cur_state = next(s for s in states_list if s.pair == variant_b_position["pair"])
                c_pips = cur_state.directional_pips
                
                variant_b_position["mfe"] = max(variant_b_position["mfe"], c_pips)
                variant_b_position["mae"] = min(variant_b_position["mae"], c_pips)
                variant_b_position["duration_runs"] += 1
                variant_b_position["exit_pips"] = c_pips

                if c_pips <= -THRESHOLD_PIPS or cur_state.threshold_state == "INVALIDATED" or cur_state.entry_zone == "CHASING":
                    variant_b_position["exit_reason"] = "INVALIDATED" if c_pips <= -THRESHOLD_PIPS else "CHASING_EXIT"
                    if variant_b_position["mfe"] >= THRESHOLD_PIPS and c_pips <= THRESHOLD_PIPS:
                        variant_b_position["is_false_breakout"] = True
                    trades_variant_b.append(variant_b_position)
                    variant_b_position = None  # Prêt pour le prochain trade éligible !
                elif run_idx == len(day_timestamps):
                    variant_b_position["exit_reason"] = "END_OF_DAY"
                    if variant_b_position["mfe"] >= THRESHOLD_PIPS and c_pips <= THRESHOLD_PIPS:
                        variant_b_position["is_false_breakout"] = True
                    trades_variant_b.append(variant_b_position)
                    variant_b_position = None

    # -----------------------------------------------------------------
    # CALCUL DES MÉTRIQUES STATISTIQUES
    # -----------------------------------------------------------------
    def calculate_stats(trades: List[Dict[str, Any]], name: str) -> Dict[str, Any]:
        if not trades:
            return {"name": name, "total_trades": 0}

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

        mfes = [t["mfe"] for t in trades]
        maes = [t["mae"] for t in trades]
        durations = [t["duration_runs"] for t in trades]
        false_breakouts = sum(1 for t in trades if t["is_false_breakout"])

        # Breakdown par zone
        sweet_trades = [t for t in trades if t["entry_zone"] == "SWEET_SPOT"]
        penalty_trades = [t for t in trades if t["entry_zone"] == "EXTENSION"]

        sweet_win_rate = (sum(1 for t in sweet_trades if t["exit_pips"] > 0) / len(sweet_trades) * 100.0) if sweet_trades else 0.0
        penalty_win_rate = (sum(1 for t in penalty_trades if t["exit_pips"] > 0) / len(penalty_trades) * 100.0) if penalty_trades else 0.0

        return {
            "name": name,
            "total_trades": total_trades,
            "win_rate": round(win_rate, 1),
            "profit_factor": round(pf, 2),
            "expectancy": round(exp, 1),
            "total_pips": round(sum(pips_list), 1),
            "avg_gain": round(statistics.mean(gains), 1) if gains else 0.0,
            "avg_loss": round(statistics.mean(pertes), 1) if pertes else 0.0,
            "median_mfe": round(statistics.median(mfes), 1) if mfes else 0.0,
            "median_mae": round(statistics.median(maes), 1) if maes else 0.0,
            "avg_duration_hours": round(statistics.mean(durations), 1) if durations else 0.0,
            "false_breakout_rate": round((false_breakouts / total_trades) * 100.0, 1) if total_trades > 0 else 0.0,
            "sweet_spot_trades": len(sweet_trades),
            "sweet_spot_win_rate": round(sweet_win_rate, 1),
            "extension_trades": len(penalty_trades),
            "extension_win_rate": round(penalty_win_rate, 1),
        }

    stats_a = calculate_stats(trades_variant_a, "Variante A (Setup Unique Quotidien)")
    stats_b = calculate_stats(trades_variant_b, "Variante B (Rotation Intraday)")

    # -----------------------------------------------------------------
    # PRESENTATION COMPARATIVE
    # -----------------------------------------------------------------
    print(f"\n🏆 COMPARAISON STATISTIQUE DES 2 VARIANTES ({len(unique_dates)} JOUURS DE TRADING)")
    print("=" * 110)
    print(f"{'MÉTRIQUE / PERFORMANCE':<35} | {'VARIANTE A (SETUP UNIQUE)':<32} | {'VARIANTE B (ROTATION INTRADAY)':<32}")
    print("-" * 110)
    print(f"{'Nombre total de trades':<35} | {stats_a['total_trades']:<32} | {stats_b['total_trades']:<32}")
    print(f"{'Taux de réussite (Win Rate %)':<35} | {stats_a['win_rate']}%{'':<26} | {stats_b['win_rate']}%{'':<26}")
    print(f"{'Profit Factor (PF)':<35} | {stats_a['profit_factor']:<32} | {stats_b['profit_factor']:<32}")
    print(f"{'Espérance (Expectancy pips/trade)':<35} | {stats_a['expectancy']:+} pips{'':<24} | {stats_b['expectancy']:+} pips{'':<24}")
    print(f"{'Pips cumulés totaux':<35} | {stats_a['total_pips']:+} pips{'':<24} | {stats_b['total_pips']:+} pips{'':<24}")
    print(f"{'Gain moyen sur trades gagnants':<35} | {stats_a['avg_gain']:+} pips{'':<24} | {stats_b['avg_gain']:+} pips{'':<24}")
    print(f"{'Perte moyenne sur trades perdants':<35} | -{stats_a['avg_loss']} pips{'':<25} | -{stats_b['avg_loss']} pips{'':<25}")
    print(f"{'Médiane MFE (Max Excursion Fav.)':<35} | {stats_a['median_mfe']:+} pips{'':<24} | {stats_b['median_mfe']:+} pips{'':<24}")
    print(f"{'Médiane MAE (Max Excursion Adv.)':<35} | {stats_a['median_mae']:+} pips{'':<24} | {stats_b['median_mae']:+} pips{'':<24}")
    print(f"{'Taux de Faux Breakouts (%)':<35} | {stats_a['false_breakout_rate']}%{'':<26} | {stats_b['false_breakout_rate']}%{'':<26}")
    print(f"{'Durée moyenne par trade':<35} | {stats_a['avg_duration_hours']} heures{'':<23} | {stats_b['avg_duration_hours']} heures{'':<23}")
    print("-" * 110)
    print(f"{'SWEET_SPOT Win Rate (5-15p)':<35} | {stats_a['sweet_spot_win_rate']}% ({stats_a['sweet_spot_trades']} trades){'':<14} | {stats_b['sweet_spot_win_rate']}% ({stats_b['sweet_spot_trades']} trades){'':<14}")
    print(f"{'EXTENSION Win Rate (15-25p)':<35} | {stats_a['extension_win_rate']}% ({stats_a['extension_trades']} trades){'':<14} | {stats_b['extension_win_rate']}% ({stats_b['extension_trades']} trades){'':<14}")
    print("=" * 110)

if __name__ == "__main__":
    run_multiday_backtest(period="60d")
