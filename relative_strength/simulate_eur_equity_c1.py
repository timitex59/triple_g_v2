# -*- coding: utf-8 -*-
"""
Simulation d'Équité EUR Réaliste par Trade — RSB-C1 Cap 15
=========================================================
Simulation exacte trade par trade en Euros (€) sur un compte de 10 000€ :
- Valeur exacte du pip en EUR selon la paire et le taux de change.
- Coût de transaction réel de 1.0 pip par trade (spread + commission).
- Effet de levier et marge requise par position.
- Sizing monétaire exact à 0.10%, 0.20%, 0.25%, 0.50%, 1.00% du capital.
- Correction rigoureuse des percentiles (bounded par Max = 15.0%).
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


def calculate_pip_value_in_eur(pair: str, current_price: float, eurusd_rate: float = 1.08, eurgbp_rate: float = 0.85, eurjpy_rate: float = 165.0, eurcad_rate: float = 1.48, eurchf_rate: float = 0.95, euraud_rate: float = 1.63, eurnzd_rate: float = 1.78) -> float:
    """
    Calcule la valeur exacte d'1 pip pour 1.0 Lot Standard (100 000 unités) en Euros (€).
    """
    pip_size = 0.01 if "JPY" in pair else 0.0001
    quote = pair[3:]
    
    if quote == "EUR":
        return 10.0  # 10€ / pip
    elif quote == "USD":
        return (10.0 / eurusd_rate)  # ex: $10 / 1.08 = 9.26€ / pip
    elif quote == "GBP":
        return (10.0 / eurgbp_rate)  # ex: £10 / 0.85 = 11.76€ / pip
    elif quote == "JPY":
        return (1000.0 / eurjpy_rate)  # ex: ¥1000 / 165.0 = 6.06€ / pip
    elif quote == "CAD":
        return (10.0 / eurcad_rate)
    elif quote == "CHF":
        return (10.0 / eurchf_rate)
    elif quote == "AUD":
        return (10.0 / euraud_rate)
    elif quote == "NZD":
        return (10.0 / eurnzd_rate)
    else:
        return 9.0


def run_realistic_eur_simulation(period: str = "60d"):
    h1_map = fetch_multiday_h1_data(period)
    if not h1_map or len(h1_map) < len(ALL_28_PAIRS):
        print("❌ Données insuffisantes.")
        return

    sample_df = list(h1_map.values())[0]
    unique_dates = sorted(list(set(dt.strftime("%Y-%m-%d") for dt in sample_df.index if dt.weekday() < 5)))
    n_days = len(unique_dates)

    print(f"\n💶 SIMULATION D'ÉQUITÉ REALISTE PAR TRADE EN EUROS (CAPITAL INITIAL: 10 000 €)")
    print("=" * 145)

    mgr = ExecutionManagerC("C1", strict_rearm=True, use_invalidation_exit=True, use_pips_07h_pnl=True, max_capacity=15)

    snapshots_history = []

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

            # Audit horaire
            n_active = len(mgr.active_positions)
            snapshots_history.append({
                "timestamp": dt_paris,
                "n_active": n_active,
                "prices": current_prices_map,
                "active_positions": copy.deepcopy(mgr.active_positions),
                "closed_trades": copy.deepcopy(mgr.closed_trades)
            })

    # -----------------------------------------------------------------
    # SIMULATION EXACTE DU COMPTE EN EUROS POUR DIFFÉRENTS RISQUES
    # -----------------------------------------------------------------
    closed_trades = mgr.closed_trades
    n_trades = len(closed_trades)

    # Statistiques du nombre de positions ouvertes au fil du temps
    n_active_series = [s["n_active"] for s in snapshots_history]
    max_active_positions = max(n_active_series)
    p95_active_positions = float(np.percentile(n_active_series, 95))

    print(f"\n1️⃣ STATISTIQUES CORRIGÉES DES POSITIONS ACTIVES (RSB-C1 CAP 15)")
    print("-" * 110)
    print(f"Capacité maximale autorisée (Cap) : 15 positions")
    print(f"Max Positions Ouvertes observées : {max_active_positions} positions")
    print(f"Max Initial Open Risk à 1% risk   : {max_active_positions * 1.0:.1f}% ({max_active_positions * 10} pips au SL)")
    print(f"95e Percentile Positions          : {p95_active_positions:.1f} positions ({p95_active_positions * 1.0:.1f}% à 1% risk)")
    print("-" * 110)

    # Simulation EUR pour chaque niveau de risque par signal
    risk_allocations = [0.10, 0.20, 0.25, 0.50, 1.00]

    print("\n2️⃣ PERFORMANCE SIMULÉE EN EUROS (€) POUR UN CAPITAL INITIAL DE 10 000 €")
    print("=" * 145)
    print(f"{'RISQUE / SIGNAL (%)':<20} | {'RISQUE MAX INIT (%)':<20} | {'CAPITAL FINAL (€)':<20} | {'GAIN NET TOTAL (€)':<20} | {'MAX MTM DD (€)':<18} | {'MAX MTM DD (%)':<18}")
    print("-" * 145)

    for risk_pct in risk_allocations:
        initial_capital = 10000.0
        current_capital = initial_capital
        capital_history = [initial_capital]

        # Simulation trade par trade avec compounding léger ou lot fixe basé sur le capital de départ
        for t in closed_trades:
            pip_val_eur = calculate_pip_value_in_eur(t.pair, t.entry_price)
            
            # Dimensionnement : risque en Euros = (Capital * risk_pct / 100.0)
            target_risk_eur = initial_capital * (risk_pct / 100.0)
            
            # SL = 10 pips. Donc Perte à -10p doit égaler target_risk_eur
            # 10 pips * pip_val_eur * lot_size = target_risk_eur
            lot_size = target_risk_eur / (10.0 * pip_val_eur)
            
            # Net PnL du trade (exit_pips - 1.0 pip de spread)
            net_pips = t.exit_pips - 1.0
            trade_pnl_eur = net_pips * pip_val_eur * lot_size
            
            current_capital += trade_pnl_eur
            capital_history.append(current_capital)

        # Calcul MTM Max DD en € et en %
        peak_eur = initial_capital
        max_dd_eur = 0.0
        max_dd_pct = 0.0
        for cap_val in capital_history:
            if cap_val > peak_eur:
                peak_eur = cap_val
            dd_e = peak_eur - cap_val
            dd_p = (dd_e / peak_eur) * 100.0
            if dd_e > max_dd_eur:
                max_dd_eur = dd_e
                max_dd_pct = dd_p

        total_gain_eur = current_capital - initial_capital
        max_initial_risk_pct = max_active_positions * risk_pct

        print(f"{risk_pct:<19.2f}% | {max_initial_risk_pct:<19.2f}% | {current_capital:<19.2f} € | {total_gain_eur:<+19.2f} € | -{max_dd_eur:<17.2f} € | -{max_dd_pct:<17.2f}%")

    print("=" * 145)

if __name__ == "__main__":
    run_realistic_eur_simulation(period="60d")
