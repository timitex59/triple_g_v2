# -*- coding: utf-8 -*-
"""
Audit Anti-Lookahead Stricte : Backtest Causal RSB-C1 (SIGNAL ON BAR CLOSE -> ENTRY ON NEXT BAR OPEN)
===================================================================================================
Élimine 100% de tout same-bar lookahead / leakage :
1. Une bougie t (ex: 09h00-10h00) confirme le signal à sa CLÔTURE (à 10h00).
2. L'entrée s'effectue STRICTEMENT au prix d'OUVERTURE (Open) de la bougie SUIVANTE t+1 (à 10h00:00).
3. Si l'ouverture t+1 est déjà > 15.0 pips (Extension), le trade est refusé (CHASING).
4. Les sorties SL/TP/Trailing/EOD s'évaluent STRICTEMENT sur les bougies t+1 et suivantes.
5. Assertions anti-lookahead intégrées.
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
from relative_strength.models import RelativeStrengthState
from relative_strength.pip_engine import get_pip_size, calculate_pips, determine_directional_pips
from relative_strength.threshold import evaluate_threshold_state, evaluate_entry_zone, evaluate_independent_flags, evaluate_trade_eligibility
from relative_strength.persistence import update_persistence_counters
from relative_strength.breadth import compute_currency_strengths_from_pips
from relative_strength.execution import ExecutionManagerC, TradePosition


def run_causal_replay_2026():
    print("🛡️ AUDIT ANTI-LOOKAHEAD STRICT : REPLAY CAUSAL RSB-C1 (01/01/2026 AU 14/08/2026)")
    print("=" * 115)
    print("Condition Causale : Signal à la CLÔTURE du bar t  ➔  ENTRÉE au OPEN du bar t+1")
    print("=" * 115)

    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 8, 14)
    tickers = [f"{p}=X" for p in ALL_28_PAIRS]

    print("⏳ Téléchargement des données de marché H1...")
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

    curr_dt = start_date
    total_trades_all = 0
    total_pips_all = 0.0
    active_days_count = 0
    positive_days_count = 0
    negative_days_count = 0
    zero_days_count = 0
    winning_trades_all = 0
    losing_trades_all = 0

    while curr_dt <= end_date:
        if curr_dt.weekday() < 5:
            date_str = curr_dt.strftime("%Y-%m-%d")
            
            sample_df = list(df_cache.values())[0]
            run_timestamps = [dt for dt in sample_df.index if dt.strftime("%Y-%m-%d") == date_str and 7 <= dt.hour <= 22]
            run_timestamps.sort()

            if len(run_timestamps) >= 2:
                ref_dt = run_timestamps[0]
                ref_prices = {pair: float(df_cache[pair].loc[ref_dt]["Open"]) for pair in ALL_28_PAIRS if pair in df_cache and ref_dt in df_cache[pair].index}

                if len(ref_prices) == len(ALL_28_PAIRS):
                    pending_signals = []
                    active_positions = []
                    closed_trades = []
                    disarmed_pairs = {}
                    prev_persistence = {}

                    for idx, t_stamp in enumerate(run_timestamps):
                        is_last_run = (idx == len(run_timestamps) - 1)
                        t_str = t_stamp.strftime("%H:%M")

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

                            # ASSERTION CAUSALE 1 : L'entrée a lieu strictement après l'émission du signal
                            assert t_stamp > sig["signal_time"], f"Violation Causalité: Entry {t_stamp} <= Signal {sig['signal_time']}"

                            # Règle de sécurité : Vérifier si l'Open du bar t+1 est toujours dans le Sweet Spot (5 à 15 pips)
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
                                    "exit_reason": None
                                }
                                active_positions.append(pos)

                        pending_signals = [] # Reset pending queue

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

                            # ASSERTION CAUSALE 2 : L'évaluation de sortie a lieu strictement sur/après le bar d'entrée
                            assert t_stamp >= pos["entry_time"], f"Violation Causalité: Exit eval {t_stamp} < Entry {pos['entry_time']}"

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
                                closed_trades.append(pos)
                                disarmed_pairs[pair] = True
                            elif sl_hit:
                                pos["exit_pips"] = -10.0
                                pos["exit_reason"] = "STOP_LOSS"
                                closed_trades.append(pos)
                                disarmed_pairs[pair] = True
                            elif tp_hit:
                                pos["exit_pips"] = 20.0
                                pos["exit_reason"] = "TAKE_PROFIT"
                                closed_trades.append(pos)
                                disarmed_pairs[pair] = True
                            elif pos["mfe"] >= 15.0 and (pos["mfe"] - cur_pips) >= 10.0:
                                pos["exit_pips"] = pos["mfe"] - 10.0
                                pos["exit_reason"] = "TRAILING_STOP"
                                closed_trades.append(pos)
                                disarmed_pairs[pair] = True
                            elif is_last_run:
                                pos["exit_pips"] = cur_pips
                                pos["exit_reason"] = "END_OF_DAY"
                                closed_trades.append(pos)
                                disarmed_pairs[pair] = True
                            else:
                                still_active.append(pos)

                        active_positions = still_active

                        # 3. Calcul des signaux du bar t_stamp à la CLÔTURE (pour entrée au bar t_stamp + 1)
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

                    # Bilan de la journée causale
                    count = len(closed_trades)
                    net_pips = sum(t["exit_pips"] for t in closed_trades)
                    sign = "+" if net_pips > 0 else ""

                    if count > 0:
                        active_days_count += 1
                        total_trades_all += count
                        total_pips_all += net_pips
                        wins = sum(1 for t in closed_trades if t["exit_pips"] > 0)
                        winning_trades_all += wins
                        losing_trades_all += (count - wins)

                        if net_pips > 0:
                            positive_days_count += 1
                        elif net_pips < 0:
                            negative_days_count += 1
                        else:
                            zero_days_count += 1

                    print(f"{date_str} : {count:<2} trades fermés ({sign}{net_pips:<+6.1f} pips)")

        curr_dt += timedelta(days=1)

    print("=" * 115)
    total_sign = "+" if total_pips_all > 0 else ""
    win_rate = (winning_trades_all / total_trades_all * 100) if total_trades_all > 0 else 0.0
    avg_pips_day = (total_pips_all / active_days_count) if active_days_count > 0 else 0.0
    pct_pos_days = (positive_days_count / active_days_count * 100) if active_days_count > 0 else 0.0

    print(f"🏆 CUMUL TOTAL BACKTEST CAUSAL STRICT : {total_trades_all} trades fermés ({total_sign}{total_pips_all:.1f} pips)")
    print(f"📊 Win Rate Trades : {win_rate:.1f}% ({winning_trades_all}W / {losing_trades_all}L)")
    print(f"📅 Journées Gagnantes : {positive_days_count} / {active_days_count} ({pct_pos_days:.1f}% des jours)")
    print(f"🔻 Journées Perdantes : {negative_days_count} jours | Neutres: {zero_days_count} jours")
    print(f"📈 Moyenne par jour : {total_sign}{avg_pips_day:.1f} pips / jour de trading")
    print("=" * 115)


if __name__ == "__main__":
    run_causal_replay_2026()
