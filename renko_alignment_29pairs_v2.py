#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Screen the 29 instruments for two specific Renko groups:
- D1 + W1 GREEN with H1 in opposition
- D1 + W1 RED with H1 in opposition

Extra filter:
- the latest confirmed Renko D1 brick must be in the current Paris week
- the latest confirmed Renko W1 brick must be in the current Paris week
"""

import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo

import pandas as pd

from ichimoku_v4 import PAIRS_29, fetch_tv_ohlc


PARIS_TZ = ZoneInfo("Europe/Paris")

TIMEFRAME_LABELS = {
    "60": "H1",
    "D": "D1",
    "W": "W1",
}

DEFAULT_INTERVALS = ("60", "D", "W")


@dataclass
class RenkoState:
    pair: str
    interval: str
    label: str
    close: float
    box_size: float
    direction: int
    color: str
    last_brick_at: pd.Timestamp | None
    source_last_at: pd.Timestamp | None


def parse_args():
    parser = argparse.ArgumentParser(description="Screen Renko D1/W1 alignments with H1 opposition across the 29 TradingView instruments.")
    parser.add_argument("--length", type=int, default=14, help="ATR length.")
    parser.add_argument("--candles", type=int, default=260, help="Number of candles fetched per symbol and timeframe.")
    parser.add_argument(
        "--intervals",
        nargs="+",
        default=list(DEFAULT_INTERVALS),
        help="TradingView intervals. Defaults to 60 D W",
    )
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


def compute_renko_state(pair: str, interval: str, length: int, candles: int) -> RenkoState:
    df = fetch_tv_ohlc(f"OANDA:{pair}", interval, candles)
    if df is None or df.empty:
        raise RuntimeError(f"Unable to fetch OHLC for {pair} on {interval}")

    atr_series = atr(df, length)
    box_size = float(atr_series.iloc[-1])
    if pd.isna(box_size) or box_size <= 0:
        raise RuntimeError(f"Not enough data to compute ATR({length}) for {pair} on {interval}")

    close = df["close"].astype(float)
    anchor = float(close.iloc[0])
    direction = 0
    last_brick_at = None

    for ts, price in close.iloc[1:].items():
        price = float(price)
        formed = 0
        new_direction = direction

        if direction == 0:
            if price >= anchor + box_size:
                move = int((price - anchor) // box_size)
                if move > 0:
                    anchor += move * box_size
                    formed = move
                    new_direction = 1
            elif price <= anchor - box_size:
                move = int((anchor - price) // box_size)
                if move > 0:
                    anchor -= move * box_size
                    formed = move
                    new_direction = -1
        elif direction == 1:
            if price >= anchor + box_size:
                move = int((price - anchor) // box_size)
                if move > 0:
                    anchor += move * box_size
                    formed = move
                    new_direction = 1
            elif price <= anchor - (2.0 * box_size):
                move = int((anchor - price) // box_size) - 1
                if move > 0:
                    anchor -= move * box_size
                    formed = move
                    new_direction = -1
        else:
            if price <= anchor - box_size:
                move = int((anchor - price) // box_size)
                if move > 0:
                    anchor -= move * box_size
                    formed = move
                    new_direction = -1
            elif price >= anchor + (2.0 * box_size):
                move = int((price - anchor) // box_size) - 1
                if move > 0:
                    anchor += move * box_size
                    formed = move
                    new_direction = 1

        if formed > 0:
            direction = new_direction
            last_brick_at = ts

    color = {1: "GREEN", -1: "RED", 0: "NEUTRAL"}[direction]
    return RenkoState(
        pair=pair,
        interval=interval,
        label=TIMEFRAME_LABELS.get(interval, interval),
        close=float(close.iloc[-1]),
        box_size=box_size,
        direction=direction,
        color=color,
        last_brick_at=last_brick_at,
        source_last_at=df.index[-1],
    )


def build_states(pairs: Iterable[str], intervals: Iterable[str], length: int, candles: int) -> dict[str, dict[str, RenkoState]]:
    out: dict[str, dict[str, RenkoState]] = {}
    for pair in pairs:
        out[pair] = {}
        for interval in intervals:
            out[pair][interval] = compute_renko_state(pair, interval, length, candles)
    return out


def market_week_start_from_states(states: dict[str, RenkoState]) -> pd.Timestamp | None:
    w1 = states.get("W")
    if not w1:
        return None
    return w1.source_last_at


def brick_in_current_week(last_brick_at: pd.Timestamp | None, market_week_start: pd.Timestamp | None) -> bool:
    if last_brick_at is None or market_week_start is None:
        return False
    return last_brick_at >= market_week_start


def group_alignments_v2(all_states: dict[str, dict[str, RenkoState]]) -> dict[str, list[str]]:
    groups = {
        "d1_w1_green_h1_red": [],
        "d1_w1_red_h1_green": [],
    }

    for pair, states in all_states.items():
        h1 = states.get("60")
        d1 = states.get("D")
        w1 = states.get("W")
        if not h1 or not d1 or not w1:
            continue

        market_week_start = market_week_start_from_states(states)
        d1_fresh = brick_in_current_week(d1.last_brick_at, market_week_start)
        w1_fresh = brick_in_current_week(w1.last_brick_at, market_week_start)
        if not (d1_fresh and w1_fresh):
            continue

        if d1.direction == 1 and w1.direction == 1 and h1.direction == -1:
            groups["d1_w1_green_h1_red"].append(pair)
        elif d1.direction == -1 and w1.direction == -1 and h1.direction == 1:
            groups["d1_w1_red_h1_green"].append(pair)

    return groups


def print_state_table(all_states: dict[str, dict[str, RenkoState]]) -> None:
    print(
        f"{'PAIR':<8} {'H1':<8} {'D1':<8} {'W1':<8} "
        f"{'D1_FRESH':<9} {'W1_FRESH':<9} {'D1_LAST':<18} {'W1_LAST':<18}"
    )
    print("-" * 100)
    for pair in sorted(all_states):
        states = all_states[pair]
        h1 = states["60"]
        d1 = states["D"]
        w1 = states["W"]
        market_week_start = market_week_start_from_states(states)
        d1_fresh = brick_in_current_week(d1.last_brick_at, market_week_start)
        w1_fresh = brick_in_current_week(w1.last_brick_at, market_week_start)
        d1_last = "-" if d1.last_brick_at is None else d1.last_brick_at.tz_convert(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
        w1_last = "-" if w1.last_brick_at is None else w1.last_brick_at.tz_convert(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
        print(
            f"{pair:<8} "
            f"{h1.color:<8} "
            f"{d1.color:<8} "
            f"{w1.color:<8} "
            f"{str(d1_fresh):<9} "
            f"{str(w1_fresh):<9} "
            f"{d1_last:<18} "
            f"{w1_last:<18}"
        )


def print_group(title: str, pairs: list[str]) -> None:
    print(title)
    if pairs:
        print(", ".join(sorted(pairs)))
    else:
        print("(none)")
    print("")


def print_groups(groups: dict[str, list[str]]) -> None:
    print("")
    print_group("D1 + W1 GREEN | H1 RED", groups["d1_w1_green_h1_red"])
    print_group("D1 + W1 RED | H1 GREEN", groups["d1_w1_red_h1_green"])


def main() -> int:
    args = parse_args()
    all_states = build_states(PAIRS_29, args.intervals, args.length, args.candles)
    print_state_table(all_states)
    print_groups(group_alignments_v2(all_states))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
