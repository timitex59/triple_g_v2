# -*- coding: utf-8 -*-
"""
Audit Approfondi par Paire : Métriques de Risque & Stabilité Mensuelle (2026-01-01 au 2026-08-14)
============================================================================================
Répond aux exigence quantitatives de Codex :
1. Métriques complètes : Win Rate, Profit Factor, Expectancy, Avg Winner, Avg Loser, Max DD, Exits % (TP/SL/Trailing/EOD).
2. Stabilité mensuelle (Janvier à Août 2026).
3. Transparence du comptage des jours (Calendriers / Métiers / Avec Trades).
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


def run_causal_full_year_all_pairs(df_cache: Dict[str, pd.DataFrame], trading_days: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Exécute la simulation causale sur TOUTES les paires en un seul passage ultra rapide."""
    trades_by_pair = {pair: [] for pair in ALL_28_PAIRS}

    for date_str in trading_days:
        sample_df = list(df_cache.values())[0]
        run_timestamps = [dt for dt in sample_df.index if dt.strftime("%Y-%m-%d") == date_str and 7 <= dt.hour <= 22]
        run_timestamps.sort()

        if len(run_timestamps) < 2:
            continue

        ref_dt = run_timestamps[0]
        ref_prices = {pair: float(df_cache[pair].loc[ref_dt]["Open"]) for pair in ALL_28_PAIRS if pair in df_cache and ref_dt in df_cache[pair].index}

        if len(ref_prices) < len(ALL_28_PAIRS):
            continue

        pending_signals = []
        active_positions = []
        disarmed_pairs = {}
        prev_persistence = {}

        for idx, t_stamp in enumerate(run_timestamps):
            is_last_run = (idx == len(run_timestamps) - 1)

            # 1. Traitement des ENTRÉES PENDING au OPEN du bar t_stamp
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
                        "entry_pips_07h": open_dir_pips,
                        "mfe": 0.0,
                        "mae": 0.0,
                        "exit_pips": 0.0,
                        "exit_reason": None,
                        "date_str": date_str
                    }
                    active_positions.append(pos)

            pending_signals = []

            # 2. Mise à jour des EXCURSIONS & GESTION DES SORTIES sur le bar t_stamp
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
                    trades_by_pair[pair].append(pos)
                    disarmed_pairs[pair] = True
                elif sl_hit:
                    pos["exit_pips"] = -10.0
                    pos["exit_reason"] = "STOP_LOSS"
                    trades_by_pair[pair].append(pos)
                    disarmed_pairs[pair] = True
                elif tp_hit:
                    pos["exit_pips"] = 20.0
                    pos["exit_reason"] = "TAKE_PROFIT"
                    trades_by_pair[pair].append(pos)
                    disarmed_pairs[pair] = True
                elif pos["mfe"] >= 15.0 and (pos["mfe"] - cur_pips) >= 10.0:
                    pos["exit_pips"] = pos["mfe"] - 10.0
                    pos["exit_reason"] = "TRAILING_STOP"
                    trades_by_pair[pair].append(pos)
                    disarmed_pairs[pair] = True
                elif is_last_run:
                    pos["exit_pips"] = cur_pips
                    pos["exit_reason"] = "END_OF_DAY"
                    trades_by_pair[pair].append(pos)
                    disarmed_pairs[pair] = True
                else:
                    still_active.append(pos)

            active_positions = still_active

            # 3. Signaux du bar t_stamp à la CLÔTURE
            if not is_last_run:
                raw_pips_map = {}
                for pair in ALL_28_PAIRS:
                    if pair in df_cache and t_stamp in df_cache[pair].index:
                        c_p = float(df_cache[pair].loc[t_stamp]["Close"])
                        pip_s = get_pip_size(pair)
                        rp, _, _ = calculate_pips(c_p, ref_prices[pair], pip_s)
                        raw_pips_map[pair] = rp

                for pair in ALL_28_PAIRS:
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

    return trades_by_pair


def run_deep_metrics_audit():
    print("📊 AUDIT PAR PAIRE : MÉTRIQUES DE RISQUE ET STABILITÉ MENSUELLE (01/01/2026 AU 14/08/2026)")
    print("=" * 145)

    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 8, 14)
    tickers = [f"{p}=X" for p in ALL_28_PAIRS]

    print("⏳ Téléchargement des données H1...")
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

    calendar_days = (end_date - start_date).days + 1
    trading_days = []
    curr_dt = start_date
    while curr_dt <= end_date:
        if curr_dt.weekday() < 5:
            trading_days.append(curr_dt.strftime("%Y-%m-%d"))
        curr_dt += timedelta(days=1)

    print(f"\n📅 TRANSPARENCE COMPTABILITÉ DES JOURS :")
    print(f"  • Total Jours Calendaires : {calendar_days} jours (du 01/01/2026 au 14/08/2026)")
    print(f"  • Total Jours Ouvrés (Lundi-Vendredi) : {len(trading_days)} séances de trading")

    print(f"⚙️ Calcul en mémoire des métriques par paire...")
    trades_by_pair = run_causal_full_year_all_pairs(df_cache, trading_days)

    months = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06", "2026-07", "2026-08"]
    pair_metrics = []

    for target_pair in ALL_28_PAIRS:
        all_trades = trades_by_pair.get(target_pair, [])
        monthly_pips = {m: 0.0 for m in months}

        for t in all_trades:
            m_key = t["date_str"][:7]
            if m_key in monthly_pips:
                monthly_pips[m_key] += t["exit_pips"]

        count = len(all_trades)
        if count == 0:
            continue

        net_pips = sum(t["exit_pips"] for t in all_trades)
        winners = [t for t in all_trades if t["exit_pips"] > 0]
        losers = [t for t in all_trades if t["exit_pips"] <= 0]
        
        gross_gains = sum(t["exit_pips"] for t in winners)
        gross_losses = abs(sum(t["exit_pips"] for t in losers))
        profit_factor = (gross_gains / gross_losses) if gross_losses > 0 else 99.9
        expectancy = net_pips / count

        avg_winner = (gross_gains / len(winners)) if len(winners) > 0 else 0.0
        avg_loser = (-gross_losses / len(losers)) if len(losers) > 0 else 0.0
        win_rate = (len(winners) / count) * 100

        tp_count = sum(1 for t in all_trades if t["exit_reason"] == "TAKE_PROFIT")
        sl_count = sum(1 for t in all_trades if t["exit_reason"] == "STOP_LOSS")
        trail_count = sum(1 for t in all_trades if t["exit_reason"] == "TRAILING_STOP")
        eod_count = sum(1 for t in all_trades if t["exit_reason"] == "END_OF_DAY")

        pct_tp = (tp_count / count) * 100
        pct_sl = (sl_count / count) * 100
        pct_trail = (trail_count / count) * 100
        pct_eod = (eod_count / count) * 100

        cum = 0.0
        peak = 0.0
        max_dd = 0.0
        for t in all_trades:
            cum += t["exit_pips"]
            peak = max(peak, cum)
            max_dd = max(max_dd, peak - cum)

        pair_metrics.append({
            "pair": target_pair,
            "trades": count,
            "win_rate": win_rate,
            "net_pips": net_pips,
            "expectancy": expectancy,
            "profit_factor": profit_factor,
            "avg_winner": avg_winner,
            "avg_loser": avg_loser,
            "max_dd": max_dd,
            "pct_tp": pct_tp,
            "pct_sl": pct_sl,
            "pct_trail": pct_trail,
            "pct_eod": pct_eod,
            "monthly": monthly_pips
        })

    pair_metrics.sort(key=lambda x: x["net_pips"], reverse=True)

    print("\n📋 MÉTRIQUES DE RISQUE ET DE PROFIL DE SORTIE PAR PAIRE :")
    print("=" * 145)
    print(f"{'PAIRE':<7} | {'TRADES':<6} | {'WIN %':<6} | {'PF':<5} | {'EXPECT':<8} | {'AVG WIN':<8} | {'AVG LOSS':<8} | {'MAX DD':<8} | {'% TP':<6} | {'% SL':<6} | {'% TRAIL':<7} | {'% EOD':<6}")
    print("-" * 145)

    for m in pair_metrics:
        sign_exp = "+" if m["expectancy"] > 0 else ""
        print(f"{m['pair']:<7} | {m['trades']:<6} | {m['win_rate']:<5.1f}% | {m['profit_factor']:<5.2f} | {sign_exp}{m['expectancy']:<+7.1f}p | +{m['avg_winner']:<6.1f}p | {m['avg_loser']:<7.1f}p | -{m['max_dd']:<6.1f}p | {m['pct_tp']:<5.1f}% | {m['pct_sl']:<5.1f}% | {m['pct_trail']:<6.1f}% | {m['pct_eod']:<5.1f}%")

    print("\n" + "=" * 145)
    print("🗓️ STABILITÉ MENSUELLE DES PIPS NETS PAR PAIRE (JANVIER - AOÛT 2026) :")
    print("=" * 145)
    print(f"{'PAIRE':<7} | {'JAN 26':<9} | {'FÉV 26':<9} | {'MAR 26':<9} | {'AVR 26':<9} | {'MAI 26':<9} | {'JUN 26':<9} | {'JUL 26':<9} | {'AOÛ 26':<9} | {'CUMUL':<10}")
    print("-" * 145)

    for m in pair_metrics:
        mp = m["monthly"]
        c_sign = "+" if m["net_pips"] > 0 else ""
        def fmt(val):
            s = "+" if val > 0 else ""
            return f"{s}{val:.1f}p"

        print(f"{m['pair']:<7} | {fmt(mp['2026-01']):<9} | {fmt(mp['2026-02']):<9} | {fmt(mp['2026-03']):<9} | {fmt(mp['2026-04']):<9} | {fmt(mp['2026-05']):<9} | {fmt(mp['2026-06']):<9} | {fmt(mp['2026-07']):<9} | {fmt(mp['2026-08']):<9} | {c_sign}{m['net_pips']:<+9.1f}p")

    print("=" * 145)


if __name__ == "__main__":
    run_deep_metrics_audit()
