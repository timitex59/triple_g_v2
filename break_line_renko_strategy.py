#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Strategy screener inspired by break_line.py with Renko filters.

Bull signal:
- H1 close crosses above the latest bear_vw0, or H1 price crosses above PSAR
- W1 Renko ATR(14) is bullish
- D1 Renko ATR(14) is bullish
- D1 CHG% close/close > +0.15

Bear signal:
- H1 close crosses below the latest bull_vw0, or H1 price crosses below PSAR
- W1 Renko ATR(14) is bearish
- D1 Renko ATR(14) is bearish
- D1 CHG% close/close < -0.15
"""

import argparse
from dataclasses import dataclass

import pandas as pd

from break_line import (
    PAIRS_29,
    calculate_psar,
    compute_break_line_state,
    daily_chg_cc,
    fetch_tv_ohlc,
)


@dataclass
class RenkoState:
    direction: int
    color: str
    box_size: float | None


def parse_args():
    parser = argparse.ArgumentParser(description="Break line + Renko strategy screener")
    parser.add_argument("--pair", default=None, help="Analyze a single pair, e.g. EURUSD")
    parser.add_argument("--scan29", action="store_true", help="Scan the 29 configured instruments")
    parser.add_argument("--renko-length", type=int, default=14, help="ATR length for Renko box size")
    parser.add_argument("--w-candles", type=int, default=220, help="Weekly candles fetched")
    parser.add_argument("--d-candles", type=int, default=320, help="Daily candles fetched")
    parser.add_argument("--h-candles", type=int, default=900, help="H1 candles fetched")
    return parser.parse_args()


def atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()


def renko_direction_from_close(close: pd.Series, box_size: float) -> int:
    if close.empty or box_size <= 0:
        return 0

    anchor = float(close.iloc[0])
    direction = 0

    for price in close.iloc[1:]:
        price = float(price)

        if direction == 0:
            if price >= anchor + box_size:
                move = int((price - anchor) // box_size)
                if move > 0:
                    anchor += move * box_size
                    direction = 1
            elif price <= anchor - box_size:
                move = int((anchor - price) // box_size)
                if move > 0:
                    anchor -= move * box_size
                    direction = -1
            continue

        if direction == 1:
            if price >= anchor + box_size:
                move = int((price - anchor) // box_size)
                if move > 0:
                    anchor += move * box_size
            elif price <= anchor - (2.0 * box_size):
                move = int((anchor - price) // box_size) - 1
                if move > 0:
                    anchor -= move * box_size
                    direction = -1
            continue

        if price <= anchor - box_size:
            move = int((anchor - price) // box_size)
            if move > 0:
                anchor -= move * box_size
        elif price >= anchor + (2.0 * box_size):
            move = int((price - anchor) // box_size) - 1
            if move > 0:
                anchor += move * box_size
                direction = 1

    return direction


def compute_renko_state(df: pd.DataFrame, length: int = 14) -> RenkoState:
    if df is None or df.empty:
        return RenkoState(0, "NEUTRAL", None)

    atr_series = atr(df, length)
    box_size = float(atr_series.iloc[-1]) if not atr_series.empty else None
    if box_size is None or pd.isna(box_size) or box_size <= 0:
        return RenkoState(0, "NEUTRAL", None)

    direction = renko_direction_from_close(df["close"].astype(float), box_size)
    color = "BULL" if direction == 1 else "BEAR" if direction == -1 else "NEUTRAL"
    return RenkoState(direction, color, box_size)


def price_crosses_above_level(df: pd.DataFrame, level: float | None) -> bool:
    if df is None or df.empty or len(df) < 2 or level is None:
        return False
    prev_close = float(df["close"].iloc[-2])
    last_close = float(df["close"].iloc[-1])
    return prev_close <= level < last_close


def price_crosses_below_level(df: pd.DataFrame, level: float | None) -> bool:
    if df is None or df.empty or len(df) < 2 or level is None:
        return False
    prev_close = float(df["close"].iloc[-2])
    last_close = float(df["close"].iloc[-1])
    return prev_close >= level > last_close


def psar_cross_signal(df: pd.DataFrame) -> tuple[bool, bool]:
    if df is None or df.empty or len(df) < 2:
        return False, False

    psar = calculate_psar(df)
    if psar is None or psar.empty or len(psar) < 2:
        return False, False

    close = df["close"].astype(float)
    bull_cross = bool((close.iloc[-1] > psar.iloc[-1]) and (close.iloc[-2] <= psar.iloc[-2]))
    bear_cross = bool((close.iloc[-1] < psar.iloc[-1]) and (close.iloc[-2] >= psar.iloc[-2]))
    return bull_cross, bear_cross


def analyze_pair(pair: str, renko_length: int, w_candles: int, d_candles: int, h_candles: int) -> dict:
    symbol = f"OANDA:{pair}"
    w1 = fetch_tv_ohlc(symbol, "W", w_candles)
    d1 = fetch_tv_ohlc(symbol, "D", d_candles)
    h1 = fetch_tv_ohlc(symbol, "60", h_candles)
    if w1 is None or d1 is None or h1 is None:
        return {"pair": pair, "error": True}

    break_h1 = compute_break_line_state(h1)
    renko_w1 = compute_renko_state(w1, renko_length)
    renko_d1 = compute_renko_state(d1, renko_length)
    chg = daily_chg_cc(d1)

    break_above_bear_vw0 = price_crosses_above_level(h1, break_h1.bear_vw0)
    break_below_bull_vw0 = price_crosses_below_level(h1, break_h1.bull_vw0)
    sar_bull_cross, sar_bear_cross = psar_cross_signal(h1)

    bull_filters = renko_w1.direction == 1 and renko_d1.direction == 1 and (chg is not None and chg > 0.15)
    bear_filters = renko_w1.direction == -1 and renko_d1.direction == -1 and (chg is not None and chg < -0.15)

    bull_trigger = break_above_bear_vw0 or sar_bull_cross
    bear_trigger = break_below_bull_vw0 or sar_bear_cross

    bull_signal = bull_trigger and bull_filters
    bear_signal = bear_trigger and bear_filters

    return {
        "pair": pair,
        "error": False,
        "close_h1": break_h1.close,
        "bear_vw0": break_h1.bear_vw0,
        "bull_vw0": break_h1.bull_vw0,
        "renko_w1": renko_w1.color,
        "renko_d1": renko_d1.color,
        "renko_w1_box": renko_w1.box_size,
        "renko_d1_box": renko_d1.box_size,
        "chg_cc_d1": chg,
        "break_above_bear_vw0": break_above_bear_vw0,
        "break_below_bull_vw0": break_below_bull_vw0,
        "sar_bull_cross": sar_bull_cross,
        "sar_bear_cross": sar_bear_cross,
        "bull_filters_ok": bull_filters,
        "bear_filters_ok": bear_filters,
        "bull_signal": bull_signal,
        "bear_signal": bear_signal,
        "signal_direction": "BULL" if bull_signal else "BEAR" if bear_signal else "NONE",
    }


def print_pair_report(row: dict) -> None:
    if row.get("error"):
        print(f"{row['pair']}: data unavailable")
        return

    chg_txt = "N/A" if row.get("chg_cc_d1") is None else f"{row['chg_cc_d1']:+.2f}%"
    print(f"PAIR           : {row['pair']}")
    print(f"H1 CLOSE       : {row['close_h1']}")
    print(f"bear_vw0       : {row['bear_vw0']}")
    print(f"bull_vw0       : {row['bull_vw0']}")
    print(f"RENKO W1       : {row['renko_w1']}")
    print(f"RENKO D1       : {row['renko_d1']}")
    print(f"CHG% CC D1     : {chg_txt}")
    print(f"BULL trigger   : break_above_bear_vw0={row['break_above_bear_vw0']} sar_crossover={row['sar_bull_cross']}")
    print(f"BEAR trigger   : break_below_bull_vw0={row['break_below_bull_vw0']} sar_crossunder={row['sar_bear_cross']}")
    print(f"BULL filters   : {row['bull_filters_ok']}")
    print(f"BEAR filters   : {row['bear_filters_ok']}")
    print(f"SIGNAL         : {row['signal_direction']}")


def scan_pairs(args) -> int:
    rows = []
    for pair in PAIRS_29:
        row = analyze_pair(pair, args.renko_length, args.w_candles, args.d_candles, args.h_candles)
        rows.append(row)
        if row.get("error"):
            print(f"{pair:<8} ERROR")
            continue
        chg_txt = "N/A" if row.get("chg_cc_d1") is None else f"{row['chg_cc_d1']:+.2f}%"
        print(
            f"{pair:<8} SIG={row['signal_direction']:<4} "
            f"RENKO={row['renko_w1']}/{row['renko_d1']} "
            f"CHG={chg_txt}"
        )

    bull_rows = [r for r in rows if not r.get("error") and r.get("bull_signal")]
    bear_rows = [r for r in rows if not r.get("error") and r.get("bear_signal")]

    print("\nBULL SIGNALS")
    if not bull_rows:
        print("None")
    else:
        for row in bull_rows:
            triggers = []
            if row["break_above_bear_vw0"]:
                triggers.append("break_above_bear_vw0")
            if row["sar_bull_cross"]:
                triggers.append("sar_crossover")
            print(f"  {row['pair']:<8} {','.join(triggers)}")

    print("\nBEAR SIGNALS")
    if not bear_rows:
        print("None")
    else:
        for row in bear_rows:
            triggers = []
            if row["break_below_bull_vw0"]:
                triggers.append("break_below_bull_vw0")
            if row["sar_bear_cross"]:
                triggers.append("sar_crossunder")
            print(f"  {row['pair']:<8} {','.join(triggers)}")

    return 0


def main() -> int:
    args = parse_args()
    if args.scan29 or not args.pair:
        return scan_pairs(args)

    row = analyze_pair(args.pair.upper(), args.renko_length, args.w_candles, args.d_candles, args.h_candles)
    print_pair_report(row)
    return 0 if not row.get("error") else 1


if __name__ == "__main__":
    raise SystemExit(main())
