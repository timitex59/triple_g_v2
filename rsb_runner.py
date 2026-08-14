#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
07H RELATIVE STRENGTH BREAKOUT RUNNER (RSB-C1 Cap 15 Production Engine)
========================================================================
Exécuteur temps réel pour la stratégie officielle de production :
RSB-C1 (Sweet Spot 5-15p + Strict Re-Arm + Portfolio Capacity Cap 15).

Règles de Fixation du Niveau 0 (07h00 Paris) :
- Avant 07h00 Paris (ex: 04h15 UTC = 06h15 Paris) : le script s'exécute en mode attente, sans émettre de signaux et sans figer le niveau 0 officiel.
- Au premier run >= 07h00 Paris (ex: 07h15 Paris) : le niveau 0 officiel de 07h00 est verrouillé pour toute la journée.
"""

import sys
import os
import copy
import argparse
from datetime import datetime
from typing import Dict, List, Any

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from relative_strength.config import ALL_28_PAIRS, PARIS_TZ, THRESHOLD_PIPS
from relative_strength.models import RelativeStrengthState
from relative_strength.reference import update_pair_reference, is_at_or_after_reference_time
from relative_strength.pip_engine import get_pip_size, calculate_pips, determine_directional_pips, update_excursion_metrics
from relative_strength.threshold import evaluate_threshold_state, evaluate_entry_zone, evaluate_independent_flags, evaluate_trade_eligibility
from relative_strength.persistence import update_persistence_counters
from relative_strength.velocity import calculate_velocity_and_acceleration
from relative_strength.breadth import compute_currency_strengths_from_pips, compute_currency_breadth
from relative_strength.scoring import compute_quality_score, compute_opportunity_score
from relative_strength.ranking import apply_triple_ranking
from relative_strength.execution import ExecutionManagerC, ExecutionManagerA, ExecutionManagerB
from relative_strength.alerts import format_production_c1_alert
from relative_strength.persistence_store import load_rsb_state, save_rsb_state
from chfjpy_strategy import send_telegram_message, fetch_pair_data_smart


def parse_args():
    parser = argparse.ArgumentParser(description="07H RSB Engine Production Runner (RSB-C1 Cap 15)")
    parser.add_argument("--no-telegram", action="store_true", help="Desactive l'envoi des messages Telegram.")
    parser.add_argument("--capacity", type=int, default=15, help="Capacité maximale du portefeuille (default=15).")
    return parser.parse_args()


def run_07h_rsb_engine(send_telegram: bool = True, max_capacity: int = 15) -> List[RelativeStrengthState]:
    now_paris = datetime.now(PARIS_TZ)
    timestamp_iso = now_paris.isoformat()
    
    print(f"🚀 [07H-RSB] Production Engine (RSB-C1 Cap {max_capacity}) — {now_paris.strftime('%Y-%m-%d %H:%M:%S')} Paris")
    print("=" * 80)
    
    is_post_07h = is_at_or_after_reference_time(now_paris)
    if not is_post_07h:
        print("⏰ Avant 07:00 Paris : En attente du premier run >= 07:00 Paris pour figer le niveau 0 officiel. Aucun signal émis.")
    
    previous_snapshot = load_rsb_state()
    prev_references = previous_snapshot.get("references", {})
    prev_pairs_data = previous_snapshot.get("pairs", {})
    prev_timestamp_str = previous_snapshot.get("timestamp")
    
    delta_time_hours = 0.5
    if prev_timestamp_str:
        try:
            prev_dt = datetime.fromisoformat(prev_timestamp_str)
            elapsed_sec = (now_paris - prev_dt).total_seconds()
            if elapsed_sec > 0:
                delta_time_hours = elapsed_sec / 3600.0
        except Exception:
            pass
            
    raw_pips_map: Dict[str, float] = {}
    prices_map: Dict[str, float] = {}
    references_store = prev_references
    
    for pair in ALL_28_PAIRS:
        yahoo_symbol = f"{pair}=X"
        res = fetch_pair_data_smart(yahoo_symbol)
        
        cur_price = float(res["close"]) if (res and res.get("close", 0) > 0) else 1.0
        source = res.get("method", "TradingView_Socket") if res else "Fallback_Default"
            
        prices_map[pair] = cur_price
        ref_info = update_pair_reference(pair, now_paris, cur_price, source, references_store)
        ref_price = ref_info["reference_price"]
        pip_size = get_pip_size(pair)
        raw_market, _, _ = calculate_pips(cur_price, ref_price, pip_size)
        raw_pips_map[pair] = raw_market
        
    currency_strengths = compute_currency_strengths_from_pips(raw_pips_map)
    states_list: List[RelativeStrengthState] = []
    
    for pair in ALL_28_PAIRS:
        cur_price = prices_map[pair]
        ref_info = references_store[pair]
        ref_price = ref_info["reference_price"]
        pip_size = get_pip_size(pair)
        
        raw_market, long_pips, short_pips = calculate_pips(cur_price, ref_price, pip_size)
        p_prev = prev_pairs_data.get(pair, {})
        
        trade_dir = "LONG" if raw_market > 0 else ("SHORT" if raw_market < 0 else "NEUTRAL")
        directional_pips = determine_directional_pips(raw_market, trade_dir)
        
        prev_mfe = float(p_prev.get("mfe_pips", 0.0))
        prev_mae = float(p_prev.get("mae_pips", 0.0))
        mfe, mae, dd_pips, dd_pct, eff = update_excursion_metrics(directional_pips, prev_mfe, prev_mae)
        
        prev_vel = float(p_prev.get("velocity_pips_per_hour", 0.0))
        prev_dir_pips = float(p_prev.get("directional_pips", 0.0))
        velocity_raw, accel_raw = calculate_velocity_and_acceleration(
            directional_pips, prev_dir_pips, delta_time_hours, prev_vel
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
        
        # Si on est avant 07h00 Paris, forcer l'éligibilité à False
        if not is_post_07h:
            is_eligible = False
            trade_eligibility = "PRE_07H_WAITING"
        
        base_curr = pair[:3]
        quote_curr = pair[3:]
        base_str = currency_strengths.get(base_curr, 50.0)
        quote_str = currency_strengths.get(quote_curr, 50.0)
        strength_diff = base_str - quote_str
        
        conf_crosses, contrad_crosses, avail_crosses, b_ratio = compute_currency_breadth(
            base_curr, "BULL" if trade_dir == "LONG" else "BEAR", raw_pips_map
        )
        
        state = RelativeStrengthState(
            pair=pair, timestamp=timestamp_iso, reference_time=ref_info["reference_time"],
            reference_price=ref_price, reference_source=ref_info["reference_source"], current_price=cur_price,
            trade_direction=trade_dir, raw_market_pips=raw_market, long_pips=long_pips, short_pips=short_pips,
            directional_pips=directional_pips, threshold_state=exclusive_state, trade_eligibility=trade_eligibility,
            is_eligible=is_eligible, is_expanding=is_exp, is_trending=is_tr, is_exhausting=is_exh,
            reversal_warning=rev_warn, has_warning=has_warn, is_chasing=is_ch, above_threshold_runs=above_runs,
            confirmation_runs=conf_runs, signal_direction_runs=dir_runs, warning_runs=warn_runs,
            mfe_pips=mfe, mae_pips=mae, drawdown_from_mfe=dd_pips, drawdown_pct=dd_pct, efficiency=eff,
            velocity_pips_per_hour=velocity_raw, acceleration_pips_per_hour2=accel_raw, entry_zone=entry_zone,
            base_currency=base_curr, quote_currency=quote_curr, base_strength=base_str, quote_strength=quote_str,
            strength_differential=strength_diff, confirming_crosses=conf_crosses, contradicting_crosses=contrad_crosses,
            available_crosses=avail_crosses, breadth_ratio=b_ratio, technical_alignment=align_symbol
        )
        
        state.quality_score = compute_quality_score(state)
        state.opportunity_score = compute_opportunity_score(state)
        states_list.append(state)
        
    apply_triple_ranking(states_list)
    save_rsb_state(timestamp_iso, references_store, states_list, currency_strengths)

    # -------------------------------------------------------------
    # PORTEFEUILLE PRINCIPAL DE PRODUCTION (RSB-C1 CAP 15)
    # -------------------------------------------------------------
    if is_post_07h:
        manager_c1 = ExecutionManagerC("C1", strict_rearm=True, use_invalidation_exit=True, use_pips_07h_pnl=True)

        eligible_c1_states = [s for s in states_list if s.is_eligible and s.entry_zone == "SWEET_SPOT"]
        
        for s in eligible_c1_states:
            active_cnt = len(manager_c1.active_positions)
            if active_cnt < max_capacity:
                entries, rejections = manager_c1.process_run(copy.deepcopy(states_list), is_last_run_of_day=False)
                accepted = any(e.pair == s.pair for e in entries)
                if accepted:
                    msg = format_production_c1_alert(s, active_cnt, max_capacity=max_capacity)
                    print("\n" + msg)
                    if send_telegram and send_telegram_message:
                        send_telegram_message(msg)
            else:
                msg = format_production_c1_alert(s, active_cnt, max_capacity=max_capacity)
                print("\n" + msg)
                if send_telegram and send_telegram_message:
                    send_telegram_message(msg)

    print(f"\n✅ Run RSB Production (RSB-C1 Cap {max_capacity}) terminé avec succès.")
    return states_list


if __name__ == "__main__":
    args = parse_args()
    run_07h_rsb_engine(send_telegram=not args.no_telegram, max_capacity=args.capacity)
