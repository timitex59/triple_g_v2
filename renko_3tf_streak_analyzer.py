# -*- coding: utf-8 -*-
"""
renko_3tf_streak_analyzer.py
============================
Analyse Renko 3 Timeframes (Monthly, Weekly, Daily) sur les 29 Paires Forex & Gold
avec détermination exacte des streaks entre parenthèses :
  - PX : M+(streak), W0(streak), D+(streak)
    • '+' : Prix au-dessus de la brique (Breakout Bullish)
    • '-' : Prix en-dessous de la brique (Breakout Bearish)
    • '0' : Prix à l'intérieur de la brique (Inside / Neutral)

Usage :
  - python renko_3tf_streak_analyzer.py            -> Analyse les 29 Paires complets
  - python renko_3tf_streak_analyzer.py GBPJPY     -> Analyse détaillée d'une paire spécifique
"""

import sys
import os
import warnings
import pandas as pd
import yfinance as yf
from datetime import datetime
from typing import Dict, List, Tuple

warnings.filterwarnings('ignore')

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from relative_strength.config import ALL_28_PAIRS

PAIRS_29 = ALL_28_PAIRS + ["XAUUSD"]


def build_renko_bricks(df_tf: pd.DataFrame, atr_pips: float = 20.0) -> Tuple[List[Tuple[float, float, int]], int, int]:
    """Construit les briques Renko et calcule les streaks vert/rouge."""
    if df_tf.empty or len(df_tf) < 2:
        return [], 0, 0

    bricks = []
    prices = df_tf["Close"].values.flatten()

    ref_close = float(prices[0])
    green_streak = 0
    red_streak = 0

    for p_val in prices[1:]:
        p = float(p_val)
        diff = p - ref_close
        if abs(diff) >= atr_pips:
            num_bricks = int(abs(diff) // atr_pips)
            for _ in range(num_bricks):
                if diff > 0:
                    b_open = ref_close
                    b_close = ref_close + atr_pips
                    direction = 1
                    green_streak += 1
                    red_streak = 0
                else:
                    b_open = ref_close
                    b_close = ref_close - atr_pips
                    direction = -1
                    red_streak += 1
                    green_streak = 0

                bricks.append((b_open, b_close, direction))
                ref_close = b_close

    return bricks, green_streak, red_streak


def compute_px_state(b_open: float, b_close: float, price: float) -> Tuple[str, str]:
    hi = max(b_open, b_close)
    lo = min(b_open, b_close)

    direction = "BULL" if b_close > b_open else ("BEAR" if b_close < b_open else "NEUTRAL")

    if price > hi:
        px_sym = "+"
    elif price < lo:
        px_sym = "-"
    else:
        px_sym = "0"
        direction = "INSIDE"

    return direction, px_sym


def analyze_single_pair(pair: str) -> Dict[str, str]:
    ticker = f"{pair}=X" if pair != "XAUUSD" else "GC=F"
    df_d = yf.download(ticker, period="2y", interval="1d", progress=False)
    df_w = yf.download(ticker, period="5y", interval="1wk", progress=False)
    df_m = yf.download(ticker, period="10y", interval="1mo", progress=False)

    res = {"pair": pair, "M": "M0(0)", "W": "W0(0)", "D": "D0(0)", "align": "NEUTRAL"}
    timeframes = [("MONTHLY", "M", df_m), ("WEEKLY", "W", df_w), ("DAILY", "D", df_d)]

    dir_scores = []

    for name, code, df in timeframes:
        if df.empty:
            continue

        last_price = float(df["Close"].iloc[-1])
        brick_size = 0.50 if "JPY" in pair else (10.0 if pair == "XAUUSD" else 0.0050)

        bricks, g_streak, r_streak = build_renko_bricks(df, atr_pips=brick_size)
        if not bricks:
            continue

        last_b_open, last_b_close, last_dir = bricks[-1]
        status, px_sym = compute_px_state(last_b_open, last_b_close, last_price)
        streak = g_streak if last_dir == 1 else r_streak

        res[code] = f"{code}{px_sym}({streak})"
        
        if px_sym == "+":
            dir_scores.append(1)
        elif px_sym == "-":
            dir_scores.append(-1)
        else:
            dir_scores.append(0)

    if dir_scores == [1, 1, 1]:
        res["align"] = "🔥 3/3 FULL BULL"
    elif dir_scores == [-1, -1, -1]:
        res["align"] = "❄️ 3/3 FULL BEAR"
    elif sum(1 for x in dir_scores if x == 1) >= 2:
        res["align"] = "🟢 2/3 BULL"
    elif sum(1 for x in dir_scores if x == -1) >= 2:
        res["align"] = "🔴 2/3 BEAR"
    else:
        res["align"] = "⚪ NEUTRAL / INSIDE"

    return res


def scan_all_29_pairs():
    print(f"\n📊 ANALYSE RENKO 3 TIMEFRAMES & STREAKS SUR LES 29 PAIRES ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
    print("=" * 75)
    print(f"{'#':<3} | {'PAIRE':<8} | {'MONTHLY':<10} | {'WEEKLY':<10} | {'DAILY':<10} | {'ALIGNEMENT':<18}")
    print("-" * 75)

    results = []
    for idx, pair in enumerate(PAIRS_29, start=1):
        try:
            r = analyze_single_pair(pair)
            results.append(r)
            print(f"{idx:<3} | {r['pair']:<8} | {r['M']:<10} | {r['W']:<10} | {r['D']:<10} | {r['align']:<18}")
        except Exception as e:
            print(f"{idx:<3} | {pair:<8} | ERROR: {e}")

    print("=" * 75)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].upper() not in ["ALL", "--ALL"]:
        target = sys.argv[1].upper()
        ticker = f"{target}=X" if target != "XAUUSD" else "GC=F"
        print(f"\n📊 RENKO 3 TIMEFRAMES TABLE FOR {target} ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
        print("=" * 45)
        print(f"{'RENKO':<10} | {'STATUS':<12} | {'PX':<10}")
        print("-" * 45)
        df_d = yf.download(ticker, period="2y", interval="1d", progress=False)
        df_w = yf.download(ticker, period="5y", interval="1wk", progress=False)
        df_m = yf.download(ticker, period="10y", interval="1mo", progress=False)
        timeframes = [("MONTHLY", "M", df_m), ("WEEKLY", "W", df_w), ("DAILY", "D", df_d)]
        for name, code, df in timeframes:
            if df.empty:
                print(f"{name:<10} | {'N/A':<12} | {code}?(0)")
                continue
            last_price = float(df["Close"].iloc[-1])
            brick_size = 0.50 if "JPY" in target else (10.0 if target == "XAUUSD" else 0.0050)
            bricks, g_streak, r_streak = build_renko_bricks(df, atr_pips=brick_size)
            if not bricks:
                print(f"{name:<10} | {'NEUTRAL':<12} | {code}0(0)")
                continue
            last_b_open, last_b_close, last_dir = bricks[-1]
            status, px_sym = compute_px_state(last_b_open, last_b_close, last_price)
            streak = g_streak if last_dir == 1 else r_streak
            print(f"{name:<10} | {status:<12} | {f'{code}{px_sym}({streak})':<10}")
        print("=" * 45)
    else:
        scan_all_29_pairs()
