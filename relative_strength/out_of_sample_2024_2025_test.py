# -*- coding: utf-8 -*-
"""
Test Out-of-Sample (Hors-Échantillon) sur Données Historiques (15/08/2024 au 31/12/2025)
========================================================================================
Valide les 3 Universes définies par Codex sans modifier aucun paramètre :
1. U28 CONTROL : 28 paires complets
2. U7 DISCOVERED : GBPJPY, NZDJPY, NZDUSD, EURUSD, EURJPY, GBPUSD, USDJPY
3. U11 STRUCTURAL HYPOTHESIS : USD Majors + JPY Crosses (11 paires)

Règles Causales Stricte Gelées :
- Signal à la clôture du bar t -> Entrée au Open du bar t+1
- Sweet spot [5.0p, 15.0p]
- Stop Loss: -10.0p | Take Profit: +20.0p | Trailing: MFE >= 15p & Recul 10p | EOD: 22h00
"""

import sys
import os
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
from relative_strength.pip_engine import get_pip_size, calculate_pips

# Définition formelle des 3 Universes Gelés
U28_CONTROL = ALL_28_PAIRS
U7_DISCOVERED = ["GBPJPY", "NZDJPY", "NZDUSD", "EURUSD", "EURJPY", "GBPUSD", "USDJPY"]
U11_STRUCTURAL = ["GBPJPY", "NZDJPY", "EURJPY", "USDJPY", "CADJPY", "AUDJPY", "CHFJPY", "EURUSD", "GBPUSD", "NZDUSD", "AUDUSD"]


def run_causal_simulation_for_universe(universe_pairs: List[str], df_cache: Dict[str, pd.DataFrame], trading_days: List[str]) -> List[Dict[str, Any]]:
    """Simule l'exécution causale stricte sur un univers spécifique."""
    all_universe_trades = []

    for date_str in trading_days:
        available_pairs = [p for p in universe_pairs if p in df_cache]
        if not available_pairs:
            continue

        sample_pair = available_pairs[0]
        sample_df = df_cache[sample_pair]
        run_timestamps = [dt for dt in sample_df.index if dt.strftime("%Y-%m-%d") == date_str and 7 <= dt.hour <= 22]
        run_timestamps.sort()

        if len(run_timestamps) < 2:
            continue

        ref_dt = run_timestamps[0]
        ref_prices = {pair: float(df_cache[pair].loc[ref_dt]["Open"]) for pair in available_pairs if ref_dt in df_cache[pair].index}

        if len(ref_prices) < len(available_pairs):
            continue

        pending_signals = []
        active_positions = []
        disarmed_pairs = {}
        prev_persistence = {}

        for idx, t_stamp in enumerate(run_timestamps):
            is_last_run = (idx == len(run_timestamps) - 1)

            # 1. ENTRÉE au OPEN du bar t_stamp
            for sig in pending_signals:
                pair = sig["pair"]
                if pair in [p["pair"] for p in active_positions]:
                    continue
                if len(active_positions) >= 15:
                    continue

                if pair not in df_cache or t_stamp not in df_cache[pair].index:
                    continue

                entry_open_price = float(df_cache[pair].loc[t_stamp]["Open"])
                pip_s = get_pip_size(pair)
                open_raw_pips, _, _ = calculate_pips(entry_open_price, ref_prices[pair], pip_s)
                open_dir_pips = open_raw_pips if sig["direction"] == "LONG" else -open_raw_pips

                if 5.0 <= open_dir_pips <= 15.0:
                    pos = {
                        "pair": pair,
                        "direction": sig["direction"],
                        "entry_time": t_stamp,
                        "entry_price": entry_open_price,
                        "mfe": 0.0,
                        "mae": 0.0,
                        "exit_pips": 0.0,
                        "exit_reason": None,
                        "date_str": date_str
                    }
                    active_positions.append(pos)

            pending_signals = []

            # 2. GESTION DES POSITIONS
            still_active = []
            for pos in active_positions:
                pair = pos["pair"]
                if pair not in df_cache or t_stamp not in df_cache[pair].index:
                    still_active.append(pos)
                    continue

                pip_s = get_pip_size(pair)
                bar_high = float(df_cache[pair].loc[t_stamp]["High"])
                bar_low = float(df_cache[pair].loc[t_stamp]["Low"])
                bar_close = float(df_cache[pair].loc[t_stamp]["Close"])

                if pos["direction"] == "LONG":
                    max_bar_pips = (bar_high - pos["entry_price"]) / pip_s
                    min_bar_pips = (bar_low - pos["entry_price"]) / pip_s
                    cur_pips = (bar_close - pos["entry_price"]) / pip_s
                else:
                    max_bar_pips = (pos["entry_price"] - bar_low) / pip_s
                    min_bar_pips = (pos["entry_price"] - bar_high) / pip_s
                    cur_pips = (pos["entry_price"] - bar_close) / pip_s

                pos["mfe"] = max(pos["mfe"], max_bar_pips)
                pos["mae"] = min(pos["mae"], min_bar_pips)

                sl_hit = (min_bar_pips <= -10.0)
                tp_hit = (max_bar_pips >= 20.0)

                if sl_hit and tp_hit:
                    pos["exit_pips"] = -10.0
                    pos["exit_reason"] = "STOP_LOSS"
                    all_universe_trades.append(pos)
                    disarmed_pairs[pair] = True
                elif sl_hit:
                    pos["exit_pips"] = -10.0
                    pos["exit_reason"] = "STOP_LOSS"
                    all_universe_trades.append(pos)
                    disarmed_pairs[pair] = True
                elif tp_hit:
                    pos["exit_pips"] = 20.0
                    pos["exit_reason"] = "TAKE_PROFIT"
                    all_universe_trades.append(pos)
                    disarmed_pairs[pair] = True
                elif pos["mfe"] >= 15.0 and (pos["mfe"] - cur_pips) >= 10.0:
                    pos["exit_pips"] = pos["mfe"] - 10.0
                    pos["exit_reason"] = "TRAILING_STOP"
                    all_universe_trades.append(pos)
                    disarmed_pairs[pair] = True
                elif is_last_run:
                    pos["exit_pips"] = cur_pips
                    pos["exit_reason"] = "END_OF_DAY"
                    all_universe_trades.append(pos)
                    disarmed_pairs[pair] = True
                else:
                    still_active.append(pos)

            active_positions = still_active

            # 3. SIGNAUX à la CLÔTURE
            if not is_last_run:
                raw_pips_map = {}
                for pair in available_pairs:
                    if pair in df_cache and t_stamp in df_cache[pair].index:
                        c_p = float(df_cache[pair].loc[t_stamp]["Close"])
                        pip_s = get_pip_size(pair)
                        rp, _, _ = calculate_pips(c_p, ref_prices[pair], pip_s)
                        raw_pips_map[pair] = rp

                for pair in available_pairs:
                    if pair not in raw_pips_map:
                        continue

                    rp = raw_pips_map[pair]
                    tdir = "LONG" if rp > 0 else ("SHORT" if rp < 0 else "NEUTRAL")
                    dpips = rp if tdir == "LONG" else -rp

                    prev_p = prev_persistence.get(pair, {"conf": 0, "dir": "NEUTRAL"})
                    conf = prev_p["conf"] + 1 if tdir == prev_p["dir"] and dpips >= 5.0 else (1 if dpips >= 5.0 else 0)
                    prev_persistence[pair] = {"conf": conf, "dir": tdir}

                    is_confirmed = (conf >= 2)
                    is_sweet = (5.0 <= dpips <= 15.0)
                    is_disarmed = disarmed_pairs.get(pair, False)

                    if is_confirmed and is_sweet and not is_disarmed:
                        pending_signals.append({
                            "pair": pair,
                            "direction": tdir,
                            "signal_time": t_stamp,
                            "pips_close": dpips
                        })

    return all_universe_trades


def calculate_metrics(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calcule les 4 métriques de performance exigées par Codex."""
    count = len(trades)
    if count == 0:
        return {"trades": 0, "win_rate": 0.0, "net_pips": 0.0, "expectancy": 0.0, "profit_factor": 0.0, "max_dd": 0.0, "win_months_pct": 0.0, "pos_months": 0, "total_months": 0}

    net_pips = sum(t["exit_pips"] for t in trades)
    winners = [t for t in trades if t["exit_pips"] > 0]
    losers = [t for t in trades if t["exit_pips"] <= 0]

    gross_gains = sum(t["exit_pips"] for t in winners)
    gross_losses = abs(sum(t["exit_pips"] for t in losers))
    profit_factor = (gross_gains / gross_losses) if gross_losses > 0 else 99.9
    expectancy = net_pips / count
    win_rate = (len(winners) / count) * 100

    # Max Drawdown
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in trades:
        cum += t["exit_pips"]
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    # Stabilité Mensuelle
    monthly_pips = {}
    for t in trades:
        m_key = t["date_str"][:7]
        monthly_pips[m_key] = monthly_pips.get(m_key, 0.0) + t["exit_pips"]

    pos_months = sum(1 for p in monthly_pips.values() if p > 0)
    total_months = len(monthly_pips)
    win_months_pct = (pos_months / total_months * 100) if total_months > 0 else 0.0

    return {
        "trades": count,
        "win_rate": win_rate,
        "net_pips": net_pips,
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "max_dd": max_dd,
        "win_months_pct": win_months_pct,
        "pos_months": pos_months,
        "total_months": total_months,
        "monthly_map": monthly_pips
    }


def run_out_of_sample_test():
    print("🧪 TEST OUT-OF-SAMPLE (HORS-ÉCHANTILLON) SUR DONNÉES HISTORIQUES (15/08/2024 AU 31/12/2025)")
    print("=" * 125)
    print("Évaluation comparative des 3 Universes GELÉS sans aucune modification de règles :")
    print("  1. U28 CONTROL : Univers complet (28 paires)")
    print("  2. U7 DISCOVERED : GBPJPY, NZDJPY, NZDUSD, EURUSD, EURJPY, GBPUSD, USDJPY")
    print("  3. U11 STRUCTURAL HYPOTHESIS : USD Majors + JPY Crosses (11 paires)")
    print("=" * 125)

    start_date = datetime(2024, 9, 1)
    end_date = datetime(2025, 12, 31)
    tickers = [f"{p}=X" for p in ALL_28_PAIRS]

    print("⏳ Téléchargement des données H1 (Août 2024 à Décembre 2025)...")
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
            df_cache[pair] = pair_df
        except Exception:
            pass

    trading_days = []
    curr_dt = start_date
    while curr_dt <= end_date:
        if curr_dt.weekday() < 5:
            trading_days.append(curr_dt.strftime("%Y-%m-%d"))
        curr_dt += timedelta(days=1)

    print(f"⚙️ Simulation causale Out-Of-Sample sur {len(trading_days)} séances de trading...")

    # 1. Simuler U28 CONTROL
    u28_trades = run_causal_simulation_for_universe(U28_CONTROL, df_cache, trading_days)
    u28_metrics = calculate_metrics(u28_trades)

    # 2. Simuler U7 DISCOVERED
    u7_trades = run_causal_simulation_for_universe(U7_DISCOVERED, df_cache, trading_days)
    u7_metrics = calculate_metrics(u7_trades)

    # 3. Simuler U11 STRUCTURAL HYPOTHESIS
    u11_trades = run_causal_simulation_for_universe(U11_STRUCTURAL, df_cache, trading_days)
    u11_metrics = calculate_metrics(u11_trades)

    print("\n📋 TABLEAU COMPARATIF OUT-OF-SAMPLE (15/08/2024 - 31/12/2025) :")
    print("=" * 125)
    print(f"{'UNIVERS':<28} | {'TRADES':<7} | {'WIN %':<6} | {'PROFIT FACTOR':<13} | {'EXPECTANCY':<12} | {'NET PIPS':<11} | {'MAX DD':<10} | {'MOIS GAGNANTS':<13}")
    print("-" * 125)

    def print_row(label, m):
        exp_sign = "+" if m["expectancy"] > 0 else ""
        net_sign = "+" if m["net_pips"] > 0 else ""
        print(f"{label:<28} | {m['trades']:<7} | {m['win_rate']:<5.1f}% | {m['profit_factor']:<13.2f} | {exp_sign}{m['expectancy']:<+11.2f}p | {net_sign}{m['net_pips']:<+10.1f}p | -{m['max_dd']:<9.1f}p | {m['pos_months']}/{m['total_months']} ({m['win_months_pct']:.0f}%)")

    print_row("U28 CONTROL (Toutes Paires)", u28_metrics)
    print_row("U7 DISCOVERED (Top 2026)", u7_metrics)
    print_row("U11 STRUCTURAL (USD/JPY)", u11_metrics)
    print("=" * 125)


if __name__ == "__main__":
    run_out_of_sample_test()
