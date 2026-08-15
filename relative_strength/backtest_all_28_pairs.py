# -*- coding: utf-8 -*-
"""
Audit Causal des 28 Paires Forex Individuellement (2026-01-01 au 2026-08-14)
==========================================================================
Évalue la performance causale (Signal sur Bar Close -> Entrée sur Next Bar Open)
paire par paire sur les 28 paires du portefeuille Forex.
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
from relative_strength.daily_pips_report import run_causal_single_day


def run_full_28_pairs_audit():
    print("📊 AUDIT CAUSAL INDIVIDUEL DES 28 PAIRES FOREX (01/01/2026 AU 14/08/2026)")
    print("=" * 115)
    print("Condition Causale : Signal à la CLÔTURE du bar t  ➔  ENTRÉE au OPEN du bar t+1")
    print("=" * 115)

    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 8, 14)
    tickers = [f"{p}=X" for p in ALL_28_PAIRS]

    print("⏳ Téléchargement unique des données H1 pour les 28 paires...")
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

    # Pré-calculer la liste des jours ouvrés
    trading_days = []
    curr_dt = start_date
    while curr_dt <= end_date:
        if curr_dt.weekday() < 5:
            trading_days.append(curr_dt.strftime("%Y-%m-%d"))
        curr_dt += timedelta(days=1)

    print(f"⚙️ Calcul en mémoire pour les 28 paires sur {len(trading_days)} jours de trading...")

    pair_results = []

    for pair_idx, target_pair in enumerate(ALL_28_PAIRS, start=1):
        trades_all = []
        for date_str in trading_days:
            t_day = run_causal_single_day(date_str, df_cache, "1h", target_pair)
            trades_all.extend(t_day)

        count = len(trades_all)
        net_pips = sum(t["exit_pips"] for t in trades_all)
        wins = sum(1 for t in trades_all if t["exit_pips"] > 0)
        losses = count - wins
        win_rate = (wins / count * 100) if count > 0 else 0.0
        avg_pips_trade = (net_pips / count) if count > 0 else 0.0

        pair_results.append({
            "pair": target_pair,
            "trades": count,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "net_pips": net_pips,
            "avg_pips": avg_pips_trade
        })

    # Trier par Net Pips décroissant
    pair_results.sort(key=lambda x: x["net_pips"], reverse=True)

    print("\n📋 CLASSEMENT DE PERFORMANCE CAUSALE PAR PAIRE (01/01/2026 - 14/08/2026) :")
    print("=" * 115)
    print(f"{'#':<3} | {'PAIRE':<8} | {'TRADES':<8} | {'GAGNANTS':<9} | {'PERDANTS':<9} | {'WIN RATE':<10} | {'NET PIPS':<12} | {'MOY / TRADE':<12}")
    print("-" * 115)

    tot_trades = 0
    tot_pips = 0.0
    tot_wins = 0
    tot_losses = 0

    for idx, r in enumerate(pair_results, start=1):
        sign = "+" if r["net_pips"] > 0 else ""
        avg_sign = "+" if r["avg_pips"] > 0 else ""
        print(f"{idx:<3} | {r['pair']:<8} | {r['trades']:<8} | {r['wins']:<9} | {r['losses']:<9} | {r['win_rate']:<9.1f}% | {sign}{r['net_pips']:<+11.1f}p | {avg_sign}{r['avg_pips']:<+11.1f}p")
        
        tot_trades += r["trades"]
        tot_pips += r["net_pips"]
        tot_wins += r["wins"]
        tot_losses += r["losses"]

    print("=" * 115)
    total_sign = "+" if tot_pips > 0 else ""
    total_wr = (tot_wins / tot_trades * 100) if tot_trades > 0 else 0.0
    print(f"🏆 CUMUL GLOBAL INDIVIDUEL 28 PAIRES : {tot_trades} trades | {tot_wins}W / {tot_losses}L | Win Rate: {total_wr:.1f}% | Total: {total_sign}{tot_pips:.1f} pips")
    print("=" * 115)


if __name__ == "__main__":
    run_full_28_pairs_audit()
