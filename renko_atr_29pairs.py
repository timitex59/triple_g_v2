#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Compute TradingView-style Renko ATR(14) box sizes for the 29 tracked instruments.

The box size is approximated from the source timeframe OHLC using:
- Source = close
- Box assignment method = ATR
- ATR length = 14

Timeframes default to:
- W   (weekly)
- D   (daily)
- 60  (hourly)
"""

import argparse
from typing import Iterable

import pandas as pd

from ichimoku_v4 import PAIRS_29, fetch_tv_ohlc


DEFAULT_INTERVALS = ("W", "D", "60")


def parse_args():
    parser = argparse.ArgumentParser(description="Compute ATR(14) Renko box sizes for the 29 TradingView instruments.")
    parser.add_argument("--length", type=int, default=14, help="ATR length.")
    parser.add_argument(
        "--intervals",
        nargs="+",
        default=list(DEFAULT_INTERVALS),
        help="TradingView source intervals, e.g. W D 60",
    )
    parser.add_argument(
        "--candles",
        type=int,
        default=120,
        help="Number of candles fetched per symbol and timeframe.",
    )
    return parser.parse_args()


def atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()


def fetch_box_size(symbol: str, interval: str, length: int, candles: int) -> dict:
    df = fetch_tv_ohlc(f"OANDA:{symbol}", interval, candles)
    if df is None or df.empty:
        raise RuntimeError(f"Unable to fetch OHLC for {symbol} on {interval}")

    atr_series = atr(df, length)
    last_atr = atr_series.iloc[-1]
    if pd.isna(last_atr):
        raise RuntimeError(f"Not enough data to compute ATR({length}) for {symbol} on {interval}")

    return {
        "pair": symbol,
        "interval": interval,
        "close": float(df["close"].iloc[-1]),
        "box_size": float(last_atr),
    }


def build_rows(pairs: Iterable[str], intervals: Iterable[str], length: int, candles: int) -> list[dict]:
    rows: list[dict] = []
    for pair in pairs:
        for interval in intervals:
            rows.append(fetch_box_size(pair, interval, length, candles))
    return rows


def print_report(rows: list[dict], length: int) -> None:
    print(f"Renko box size approximation using ATR({length})")
    print("")
    print(f"{'PAIR':<8} {'TF':<6} {'CLOSE':>12} {'BOX_SIZE':>12}")
    print("-" * 42)
    for row in rows:
        print(
            f"{row['pair']:<8} "
            f"{row['interval']:<6} "
            f"{row['close']:>12.5f} "
            f"{row['box_size']:>12.5f}"
        )


def main() -> int:
    args = parse_args()
    rows = build_rows(PAIRS_29, args.intervals, args.length, args.candles)
    print_report(rows, args.length)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
