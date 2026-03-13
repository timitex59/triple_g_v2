#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build two Renko lists across the 29 pairs:

LIST ALIGNED
- W1 / D1 / H1 GREEN with daily CHG% > 0
- W1 / D1 / H1 RED with daily CHG% < 0
- W1 / D1 / H1 latest Renko bricks must belong to the current market week

LIST RETRACE
- D1 + W1 GREEN with H1 RED
- D1 + W1 RED with H1 GREEN
- D1 / W1 latest Renko bricks must belong to the current market week
"""

import argparse
from dataclasses import dataclass

import pandas as pd

from break_line import PAIRS_29, daily_chg_cc, fetch_tv_ohlc
from break_line_renko_strategy import compute_renko_state, market_week_start_from_df, renko_brick_in_week


@dataclass
class PairState:
    pair: str
    renko_h1: str
    renko_d1: str
    renko_w1: str
    hourly_fresh: bool
    daily_fresh: bool
    weekly_fresh: bool
    chg_cc_d1: float | None
    d1_last: pd.Timestamp | None
    w1_last: pd.Timestamp | None
    h1_last: pd.Timestamp | None


def parse_args():
    parser = argparse.ArgumentParser(description="Build LIST ALIGNED and LIST RETRACE from Renko states across 29 pairs.")
    parser.add_argument("--renko-length", type=int, default=14, help="ATR length for Renko box size")
    parser.add_argument("--w-candles", type=int, default=260, help="Weekly candles fetched")
    parser.add_argument("--d-candles", type=int, default=320, help="Daily candles fetched")
    parser.add_argument("--h-candles", type=int, default=900, help="Hourly candles fetched")
    return parser.parse_args()


def build_pair_state(pair: str, renko_length: int, w_candles: int, d_candles: int, h_candles: int) -> PairState:
    symbol = f"OANDA:{pair}"
    w1 = fetch_tv_ohlc(symbol, "W", w_candles)
    d1 = fetch_tv_ohlc(symbol, "D", d_candles)
    h1 = fetch_tv_ohlc(symbol, "60", h_candles)
    if w1 is None or d1 is None or h1 is None:
        raise RuntimeError(f"Unable to fetch data for {pair}")

    renko_w1 = compute_renko_state(w1, renko_length)
    renko_d1 = compute_renko_state(d1, renko_length)
    renko_h1 = compute_renko_state(h1, renko_length)
    chg = daily_chg_cc(d1)
    market_week_start = market_week_start_from_df(w1)

    return PairState(
        pair=pair,
        renko_h1=renko_h1.color,
        renko_d1=renko_d1.color,
        renko_w1=renko_w1.color,
        hourly_fresh=renko_brick_in_week(renko_h1.last_brick_at, market_week_start),
        daily_fresh=renko_brick_in_week(renko_d1.last_brick_at, market_week_start),
        weekly_fresh=renko_brick_in_week(renko_w1.last_brick_at, market_week_start),
        chg_cc_d1=chg,
        d1_last=renko_d1.last_brick_at,
        w1_last=renko_w1.last_brick_at,
        h1_last=renko_h1.last_brick_at,
    )


def build_states(args) -> list[PairState]:
    states = []
    for pair in PAIRS_29:
        states.append(build_pair_state(pair, args.renko_length, args.w_candles, args.d_candles, args.h_candles))
    return states


def list_aligned(states: list[PairState]) -> tuple[list[PairState], list[PairState]]:
    green = []
    red = []
    for state in states:
        fresh_ok = state.weekly_fresh and state.daily_fresh and state.hourly_fresh
        if not fresh_ok:
            continue
        if state.chg_cc_d1 is None or abs(state.chg_cc_d1) <= 0.10:
            continue

        if state.renko_w1 == state.renko_d1 == state.renko_h1 == "BULL" and state.chg_cc_d1 > 0:
            green.append(state)
        elif state.renko_w1 == state.renko_d1 == state.renko_h1 == "BEAR" and state.chg_cc_d1 < 0:
            red.append(state)
    return green, red


def list_retrace(states: list[PairState]) -> tuple[list[PairState], list[PairState]]:
    green = []
    red = []
    for state in states:
        fresh_ok = state.weekly_fresh and state.daily_fresh
        if not fresh_ok:
            continue

        if state.renko_w1 == "BULL" and state.renko_d1 == "BULL" and state.renko_h1 == "BEAR":
            green.append(state)
        elif state.renko_w1 == "BEAR" and state.renko_d1 == "BEAR" and state.renko_h1 == "BULL":
            red.append(state)
    return green, red


def fmt_chg(value: float | None) -> str:
    return "N/A" if value is None else f"{value:+.2f}%"


def print_group(title: str, rows: list[PairState]) -> None:
    print(title)
    if not rows:
        print("(none)")
        print("")
        return

    for state in rows:
        print(
            f"{state.pair:<8} "
            f"RENKO={state.renko_w1}/{state.renko_d1}/{state.renko_h1} "
            f"CHG={fmt_chg(state.chg_cc_d1)} "
            f"FRESH={state.weekly_fresh}/{state.daily_fresh}/{state.hourly_fresh}"
        )
    print("")


def main() -> int:
    args = parse_args()
    states = build_states(args)
    aligned_green, aligned_red = list_aligned(states)
    retrace_green, retrace_red = list_retrace(states)

    print_group("LIST ALIGNED | GREEN/GREEN/GREEN", aligned_green)
    print_group("LIST ALIGNED | RED/RED/RED", aligned_red)
    print_group("LIST RETRACE | D1 + W1 GREEN | H1 RED", retrace_green)
    print_group("LIST RETRACE | D1 + W1 RED | H1 GREEN", retrace_red)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
