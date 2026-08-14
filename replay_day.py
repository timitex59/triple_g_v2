# -*- coding: utf-8 -*-
"""
Replay 07H-RSB Engine — 14 Août 2026
====================================
Rejoue la journée du 14 août 2026 heure par heure depuis 07:00 Europe/Paris.
Produit le tableau synthétique run par run demandé :
- PIP RANK, QUALITY RANK, OPPORTUNITY RANK
- Best Expression (NZD, CAD, etc.)
- Détails clés (USDCAD, NZDUSD, EURJPY, etc.) : pips, MFE, MAE, velocity, breadth, entry zone, scores.
"""

import sys
import os
import json
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
from typing import Dict, List, Any
from zoneinfo import ZoneInfo

# Force UTF-8 output for Windows console
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


def fetch_all_h1_history_yfinance(day_str: str = "2026-08-14") -> Dict[str, pd.DataFrame]:
    """
    Télécharge l'historique H1 de toutes les 28 paires en 1 seconde via yfinance batch.
    """
    print(f"📥 Téléchargement ultra-rapide des bougies H1 du {day_str} pour les 28 paires...")
    tickers = [f"{p}=X" for p in ALL_28_PAIRS]
    
    df_batch = yf.download(tickers, period="5d", interval="1h", progress=False)
    
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
            
            # Conversion timezone Paris
            pair_df.index = pair_df.index.tz_convert(PARIS_TZ)
            h1_map[pair] = pair_df
        except Exception as e:
            print(f"⚠️ Erreur extraction {pair}: {e}")
            
    return h1_map


def replay_august_14():
    target_date_str = "2026-08-14"
    h1_map = fetch_all_h1_history_yfinance(target_date_str)
    
    if not h1_map or len(h1_map) < len(ALL_28_PAIRS):
        print("❌ Données manquantes.")
        return

    # Identification des timestamps H1 disponibles pour le 14 août 2026 >= 07:00 Paris
    sample_df = list(h1_map.values())[0]
    
    hours_timestamps = []
    for dt_index in sample_df.index:
        if dt_index.strftime("%Y-%m-%d") == target_date_str and dt_index.hour >= 7:
            hours_timestamps.append(dt_index)

    hours_timestamps.sort()

    if not hours_timestamps:
        print(f"⚠️ Aucun timestamp H1 trouvé pour le {target_date_str} >= 07:00 Paris.")
        return

    print(f"\n🚀 REPLAY DU {target_date_str} DEPUIS 07:00 PARIS ({len(hours_timestamps)} RUNS HORAIRES)")
    print("=" * 140)

    # 1. Prix de Référence à 07:00 Paris
    ref_dt = hours_timestamps[0]
    reference_prices: Dict[str, float] = {}
    
    for pair, df in h1_map.items():
        if ref_dt in df.index:
            reference_prices[pair] = float(df.loc[ref_dt]["Open"])
        else:
            reference_prices[pair] = float(df.iloc[0]["Open"])

    prev_pairs_data: Dict[str, Dict[str, Any]] = {}
    prev_dt_paris = None

    table_rows = []

    # 2. Replay Run par Run (07:00 à 22:00 / 23:00 Paris)
    for run_idx, dt_paris in enumerate(hours_timestamps, start=1):
        run_time_str = dt_paris.strftime("%H:%M")
        
        if prev_dt_paris:
            delta_hours = (dt_paris - prev_dt_paris).total_seconds() / 3600.0
        else:
            delta_hours = 1.0
            
        prev_dt_paris = dt_paris

        raw_pips_map: Dict[str, float] = {}
        current_prices_map: Dict[str, float] = {}

        for pair, df in h1_map.items():
            if dt_paris in df.index:
                price = float(df.loc[dt_paris]["Close"])
            else:
                price = reference_prices[pair]
                
            current_prices_map[pair] = price
            ref_p = reference_prices[pair]
            pip_s = get_pip_size(pair)
            raw_p, _, _ = calculate_pips(price, ref_p, pip_s)
            raw_pips_map[pair] = raw_p

        # Force des devises
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
                reference_time="07:00",
                reference_price=ref_p,
                reference_source="yfinance_h1",
                current_price=cur_price,
                trade_direction=trade_dir,
                raw_market_pips=raw_market,
                long_pips=long_pips,
                short_pips=short_pips,
                directional_pips=directional_pips,
                threshold_state=exclusive_state,
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

        # Application du Triple Ranking
        apply_triple_ranking(states_list)

        # Détection de la devise dominante et Best Expression tradable
        strongest_curr = max(currency_strengths, key=currency_strengths.get)
        best_expr = select_best_currency_expression(states_list, strongest_curr, "LONG")
        best_expr_str = f"{best_expr.pair} {best_expr.trade_direction}" if best_expr else "NONE (CHASING)"

        # Extraction des TOP #1 pour chaque ranking
        pip_leader = sorted(states_list, key=lambda s: s.pip_rank)[0]
        qual_leader = sorted(states_list, key=lambda s: s.quality_rank)[0]
        opp_leader = sorted(states_list, key=lambda s: s.opportunity_rank)[0]

        # Paires clés
        usdcad_state = next(s for s in states_list if s.pair == "USDCAD")
        nzdusd_state = next(s for s in states_list if s.pair == "NZDUSD")

        row = {
            "time": run_time_str,
            "strongest_curr": strongest_curr,
            "pip_leader": f"{pip_leader.pair} ({pip_leader.directional_pips:+.1f}p)",
            "qual_leader": f"{qual_leader.pair} ({qual_leader.quality_score:.0f})",
            "opp_leader": f"{opp_leader.pair} ({opp_leader.opportunity_score:.0f})",
            "best_expr": best_expr_str,
            "usdcad_opp_rank": usdcad_state.opportunity_rank,
            "usdcad_pips": usdcad_state.directional_pips,
            "usdcad_zone": usdcad_state.entry_zone,
            "nzdusd_opp_rank": nzdusd_state.opportunity_rank,
            "nzdusd_pips": nzdusd_state.directional_pips,
            "nzdusd_zone": nzdusd_state.entry_zone,
        }
        table_rows.append(row)

    # 3. Impression du Tableau Synthétique Run par Run
    print(f"\n📊 TABLEAU SYNTHÉTIQUE REPLAY HEURE PAR HEURE ({target_date_str})")
    print("-" * 140)
    print(f"{'HEURE':<7} | {'DOMINANTE':<9} | {'PIP RANK #1 (Perf)':<22} | {'QUALITY RANK #1 (Qualité)':<25} | {'OPPORTUNITY RANK #1 (Entrée)':<26} | {'BEST EXPRESSION':<20}")
    print("-" * 140)

    for r in table_rows:
        print(f"{r['time']:<7} | {r['strongest_curr']:<9} | {r['pip_leader']:<22} | {r['qual_leader']:<25} | {r['opp_leader']:<26} | {r['best_expr']:<20}")

    print("-" * 140)
    print("\n🔍 ZOOM SUR LES PAIRES D'INTERÊT CLEFS (USDCAD & NZDUSD) AU FIL DE LA JOURNÉE :")
    print("-" * 120)
    print(f"{'HEURE':<7} | {'USDCAD Pips':<12} | {'USDCAD Zone':<12} | {'USDCAD Opp Rank':<16} | {'NZDUSD Pips':<12} | {'NZDUSD Zone':<12} | {'NZDUSD Opp Rank':<16}")
    print("-" * 120)

    for r in table_rows:
        print(f"{r['time']:<7} | {r['usdcad_pips']:+12.1f} | {r['usdcad_zone']:<12} | Opp #{r['usdcad_opp_rank']:<11} | {r['nzdusd_pips']:+12.1f} | {r['nzdusd_zone']:<12} | Opp #{r['nzdusd_opp_rank']:<11}")
    print("-" * 120)

if __name__ == "__main__":
    replay_august_14()
