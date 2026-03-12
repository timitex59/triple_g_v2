#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Screen the 29 instruments for Renko ATR(14) color alignment on H1, D1, and W1.

Assumptions:
- TradingView source price: close
- Box assignment method: ATR
- Last confirmed brick color only

This is a practical approximation of TradingView Renko built from source OHLC data.
"""

import argparse
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from ichimoku_v4 import PAIRS_29, fetch_tv_ohlc


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
    bricks: int
    last_brick_close: float | None


def parse_args():
    parser = argparse.ArgumentParser(description="Screen Renko ATR(14) alignment across the 29 TradingView instruments.")
    parser.add_argument("--length", type=int, default=14, help="ATR length.")
    parser.add_argument("--candles", type=int, default=220, help="Number of candles fetched per symbol and timeframe.")
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


def renko_direction_from_close(close: pd.Series, box_size: float) -> tuple[int, int, float | None]:
    if close.empty or box_size <= 0:
        return 0, 0, None

    anchor = float(close.iloc[0])
    last_brick_close: float | None = None
    direction = 0
    bricks = 0

    for price in close.iloc[1:]:
        price = float(price)

        if direction == 0:
            if price >= anchor + box_size:
                move = int((price - anchor) // box_size)
                if move > 0:
                    bricks += move
                    anchor += move * box_size
                    last_brick_close = anchor
                    direction = 1
            elif price <= anchor - box_size:
                move = int((anchor - price) // box_size)
                if move > 0:
                    bricks += move
                    anchor -= move * box_size
                    last_brick_close = anchor
                    direction = -1
            continue

        if direction == 1:
            if price >= anchor + box_size:
                move = int((price - anchor) // box_size)
                if move > 0:
                    bricks += move
                    anchor += move * box_size
                    last_brick_close = anchor
            elif price <= anchor - (2.0 * box_size):
                move = int((anchor - price) // box_size) - 1
                if move > 0:
                    bricks += move
                    anchor -= move * box_size
                    last_brick_close = anchor
                    direction = -1
            continue

        if price <= anchor - box_size:
            move = int((anchor - price) // box_size)
            if move > 0:
                bricks += move
                anchor -= move * box_size
                last_brick_close = anchor
        elif price >= anchor + (2.0 * box_size):
            move = int((price - anchor) // box_size) - 1
            if move > 0:
                bricks += move
                anchor += move * box_size
                last_brick_close = anchor
                direction = 1

    return direction, bricks, last_brick_close


def fetch_renko_state(pair: str, interval: str, length: int, candles: int) -> RenkoState:
    df = fetch_tv_ohlc(f"OANDA:{pair}", interval, candles)
    if df is None or df.empty:
        raise RuntimeError(f"Unable to fetch OHLC for {pair} on {interval}")

    atr_series = atr(df, length)
    box_size = float(atr_series.iloc[-1])
    if pd.isna(box_size) or box_size <= 0:
        raise RuntimeError(f"Not enough data to compute ATR({length}) for {pair} on {interval}")

    direction, bricks, last_brick_close = renko_direction_from_close(df["close"].astype(float), box_size)
    color = {1: "GREEN", -1: "RED", 0: "NEUTRAL"}[direction]

    return RenkoState(
        pair=pair,
        interval=interval,
        label=TIMEFRAME_LABELS.get(interval, interval),
        close=float(df["close"].iloc[-1]),
        box_size=box_size,
        direction=direction,
        color=color,
        bricks=bricks,
        last_brick_close=last_brick_close,
    )


def build_states(pairs: Iterable[str], intervals: Iterable[str], length: int, candles: int) -> dict[str, dict[str, RenkoState]]:
    out: dict[str, dict[str, RenkoState]] = {}
    for pair in pairs:
        out[pair] = {}
        for interval in intervals:
            out[pair][interval] = fetch_renko_state(pair, interval, length, candles)
    return out


def aligned_pair(states: dict[str, RenkoState], left: str, right: str) -> bool:
    return (
        left in states
        and right in states
        and states[left].direction != 0
        and states[left].direction == states[right].direction
    )


def group_alignments(all_states: dict[str, dict[str, RenkoState]]) -> dict[str, list[str]]:
    groups = {
        "triple_green": [],
        "triple_red": [],
        "h1_d1_green": [],
        "h1_d1_red": [],
        "d1_w1_green": [],
        "d1_w1_red": [],
    }

    for pair, states in all_states.items():
        h1 = states.get("60")
        d1 = states.get("D")
        w1 = states.get("W")
        if h1 and d1 and w1 and h1.direction == d1.direction == w1.direction != 0:
            if h1.direction == 1:
                groups["triple_green"].append(pair)
            else:
                groups["triple_red"].append(pair)

        if aligned_pair(states, "60", "D"):
            if states["60"].direction == 1:
                groups["h1_d1_green"].append(pair)
            else:
                groups["h1_d1_red"].append(pair)

        if aligned_pair(states, "D", "W"):
            if states["D"].direction == 1:
                groups["d1_w1_green"].append(pair)
            else:
                groups["d1_w1_red"].append(pair)

    return groups


def print_state_table(all_states: dict[str, dict[str, RenkoState]]) -> None:
    print(f"{'PAIR':<8} {'H1':<8} {'D1':<8} {'W1':<8} {'H1_BOX':>10} {'D1_BOX':>10} {'W1_BOX':>10}")
    print("-" * 70)
    for pair in sorted(all_states):
        states = all_states[pair]
        h1 = states["60"]
        d1 = states["D"]
        w1 = states["W"]
        print(
            f"{pair:<8} "
            f"{h1.color:<8} "
            f"{d1.color:<8} "
            f"{w1.color:<8} "
            f"{h1.box_size:>10.5f} "
            f"{d1.box_size:>10.5f} "
            f"{w1.box_size:>10.5f}"
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
    print_group("H1 + D1 + W1 GREEN", groups["triple_green"])
    print_group("H1 + D1 + W1 RED", groups["triple_red"])
    print_group("H1 + D1 GREEN", groups["h1_d1_green"])
    print_group("H1 + D1 RED", groups["h1_d1_red"])
    print_group("D1 + W1 GREEN", groups["d1_w1_green"])
    print_group("D1 + W1 RED", groups["d1_w1_red"])


def main() -> int:
    args = parse_args()
    all_states = build_states(PAIRS_29, args.intervals, args.length, args.candles)
    print_state_table(all_states)
    print_groups(group_alignments(all_states))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
