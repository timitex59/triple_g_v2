#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Validate a trend when the following are aligned:
- Heikin Ashi Weekly body color
- Heikin Ashi Daily body color
- Renko Daily direction
"""

import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from ichimoku_v4 import PAIRS_29, fetch_tv_ohlc


PARIS_TZ = ZoneInfo("Europe/Paris")

TIMEFRAME_LABELS = {
    "D": "D1",
    "W": "W1",
}

DEFAULT_INTERVALS = ("D",)

PSAR_H1_START = 0.1
PSAR_H1_INCREMENT = 0.1
PSAR_H1_MAXIMUM = 0.2

H1_CANDLES = 260


def current_paris_week_start() -> pd.Timestamp:
    now_paris = datetime.now(PARIS_TZ)
    monday = now_paris.replace(hour=0, minute=0, second=0, microsecond=0) - pd.Timedelta(days=now_paris.weekday())
    return pd.Timestamp(monday)


def is_in_current_paris_week(ts: pd.Timestamp | None) -> bool:
    if ts is None:
        return False
    try:
        ts_paris = ts.tz_convert(PARIS_TZ)
    except (TypeError, AttributeError):
        ts_paris = ts.tz_localize("UTC").tz_convert(PARIS_TZ)
    return ts_paris >= current_paris_week_start()


def calculate_psar(df: pd.DataFrame, start: float, increment: float, maximum: float) -> pd.Series | None:
    if df is None or df.empty or len(df) < 3:
        return None

    highs = df["high"].astype(float).values
    lows = df["low"].astype(float).values
    closes = df["close"].astype(float).values

    psar = np.zeros(len(df), dtype=float)
    bull = closes[1] >= closes[0]
    af = float(start)
    ep = float(highs[0] if bull else lows[0])
    psar[0] = float(lows[0] if bull else highs[0])

    for i in range(1, len(df)):
        psar[i] = psar[i - 1] + af * (ep - psar[i - 1])
        if bull:
            if lows[i] < psar[i]:
                bull = False
                psar[i] = ep
                ep = float(lows[i])
                af = float(start)
            else:
                if highs[i] > ep:
                    ep = float(highs[i])
                    af = min(af + float(increment), float(maximum))
                psar[i] = min(psar[i], lows[i - 1], lows[i - 2]) if i >= 2 else min(psar[i], lows[i - 1])
        else:
            if highs[i] > psar[i]:
                bull = True
                psar[i] = ep
                ep = float(highs[i])
                af = float(start)
            else:
                if lows[i] < ep:
                    ep = float(lows[i])
                    af = min(af + float(increment), float(maximum))
                psar[i] = max(psar[i], highs[i - 1], highs[i - 2]) if i >= 2 else max(psar[i], highs[i - 1])

    return pd.Series(psar, index=df.index, dtype="float64")


def h1_psar_bias(pair: str) -> tuple[str, float | None, float | None, pd.Timestamp | None]:
    df_h1 = fetch_tv_ohlc(f"OANDA:{pair}", "60", H1_CANDLES)
    if df_h1 is None or df_h1.empty or len(df_h1) < 3:
        return "N/A", None, None, None

    psar = calculate_psar(df_h1, PSAR_H1_START, PSAR_H1_INCREMENT, PSAR_H1_MAXIMUM)
    if psar is None or psar.empty:
        return "N/A", None, None, None

    idx = -2 if len(df_h1) >= 2 else -1
    close = float(df_h1["close"].iloc[idx])
    sar = float(psar.iloc[idx])
    ts = df_h1.index[idx]

    if close > sar:
        return "BULL", close, sar, ts
    if close < sar:
        return "BEAR", close, sar, ts
    return "NEUTRAL", close, sar, ts


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


@dataclass
class HeikinAshiState:
    interval: str
    label: str
    color: str
    last_at: pd.Timestamp | None


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Validate trends across the 29 TradingView instruments when HA(W1)+HA(D1)+Renko(D1) are aligned."
        )
    )
    parser.add_argument("--length", type=int, default=14, help="ATR length.")
    parser.add_argument("--candles", type=int, default=260, help="Number of candles fetched per symbol and timeframe.")
    parser.add_argument(
        "--no-wicks",
        action="store_true",
        help="Disable wick processing and build Renko using close only.",
    )
    parser.add_argument(
        "--intervals",
        nargs="+",
        default=list(DEFAULT_INTERVALS),
        help="Renko intervals. Defaults to D",
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


def compute_renko_state(pair: str, interval: str, length: int, candles: int, use_wicks: bool) -> RenkoState:
    df = fetch_tv_ohlc(f"OANDA:{pair}", interval, candles)
    if df is None or df.empty:
        raise RuntimeError(f"Unable to fetch OHLC for {pair} on {interval}")

    atr_series = atr(df, length)
    box_size = float(atr_series.iloc[-1])
    if pd.isna(box_size) or box_size <= 0:
        raise RuntimeError(f"Not enough data to compute ATR({length}) for {pair} on {interval}")

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    anchor = float(close.iloc[0])
    direction = 0
    last_brick_at = None

    def apply_price(price: float, ts: pd.Timestamp) -> None:
        nonlocal anchor, direction, last_brick_at

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

    for ts in df.index[1:]:
        if use_wicks:
            hi = float(high.loc[ts])
            lo = float(low.loc[ts])
            if direction >= 0:
                apply_price(hi, ts)
                apply_price(lo, ts)
            else:
                apply_price(lo, ts)
                apply_price(hi, ts)
        else:
            apply_price(float(close.loc[ts]), ts)

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


def compute_heikin_ashi_state(pair: str, interval: str, candles: int) -> HeikinAshiState:
    df = fetch_tv_ohlc(f"OANDA:{pair}", interval, candles)
    if df is None or df.empty:
        raise RuntimeError(f"Unable to fetch OHLC for {pair} on {interval}")

    o = df["open"].astype(float)
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)

    ha_close = (o + h + l + c) / 4.0

    ha_open = pd.Series(index=df.index, dtype=float)
    if len(df) == 0:
        last_at = None
        color = "NEUTRAL"
    else:
        ha_open.iloc[0] = (float(o.iloc[0]) + float(c.iloc[0])) / 2.0
        for i in range(1, len(df)):
            ha_open.iloc[i] = (float(ha_open.iloc[i - 1]) + float(ha_close.iloc[i - 1])) / 2.0

        last_at = df.index[-1]
        last_ho = float(ha_open.iloc[-1])
        last_hc = float(ha_close.iloc[-1])
        if last_hc > last_ho:
            color = "GREEN"
        elif last_hc < last_ho:
            color = "RED"
        else:
            color = "NEUTRAL"

    return HeikinAshiState(
        interval=interval,
        label=TIMEFRAME_LABELS.get(interval, interval),
        color=color,
        last_at=last_at,
    )


def build_renko_states(
    pairs: Iterable[str], intervals: Iterable[str], length: int, candles: int, use_wicks: bool
) -> dict[str, dict[str, RenkoState]]:
    out: dict[str, dict[str, RenkoState]] = {}
    for pair in pairs:
        out[pair] = {}
        for interval in intervals:
            out[pair][interval] = compute_renko_state(pair, interval, length, candles, use_wicks)
    return out


def build_ashi_states(pairs: Iterable[str], intervals: Iterable[str], candles: int) -> dict[str, dict[str, HeikinAshiState]]:
    out: dict[str, dict[str, HeikinAshiState]] = {}
    for pair in pairs:
        out[pair] = {}
        for interval in intervals:
            out[pair][interval] = compute_heikin_ashi_state(pair, interval, candles)
    return out


def ashi_confirms(direction: int, ashi_color: str) -> bool:
    if direction == 1:
        return ashi_color == "GREEN"
    if direction == -1:
        return ashi_color == "RED"
    return False


def group_trends_v3(
    renko_states: dict[str, dict[str, RenkoState]],
    ashi_states: dict[str, dict[str, HeikinAshiState]],
) -> dict[str, list[str]]:
    groups = {
        "trend_bullish": [],
        "trend_bearish": [],
    }

    for pair, states in renko_states.items():
        d1 = states.get("D")
        if not d1:
            continue

        if not is_in_current_paris_week(d1.last_brick_at):
            continue

        d1_ashi = ashi_states.get(pair, {}).get("D")
        w1_ashi = ashi_states.get(pair, {}).get("W")
        if not d1_ashi or not w1_ashi:
            continue

        if d1.direction == 1 and d1_ashi.color == "GREEN" and w1_ashi.color == "GREEN":
            groups["trend_bullish"].append(pair)
        elif d1.direction == -1 and d1_ashi.color == "RED" and w1_ashi.color == "RED":
            groups["trend_bearish"].append(pair)

    return groups


def print_state_table(
    renko_states: dict[str, dict[str, RenkoState]],
    ashi_states: dict[str, dict[str, HeikinAshiState]],
) -> None:
    print(
        f"{'PAIR':<8} {'RENKO_D1':<10} {'HA_D1':<8} {'HA_W1':<8} {'D1_LAST':<18}"
    )
    print("-" * 60)

    for pair in sorted(renko_states):
        states = renko_states[pair]
        d1 = states.get("D")
        if not d1:
            continue

        if not is_in_current_paris_week(d1.last_brick_at):
            continue

        d1_ashi = ashi_states.get(pair, {}).get("D")
        w1_ashi = ashi_states.get(pair, {}).get("W")
        d1_ashi_color = d1_ashi.color if d1_ashi else "-"
        w1_ashi_color = w1_ashi.color if w1_ashi else "-"

        d1_last = "-" if d1.last_brick_at is None else d1.last_brick_at.tz_convert(PARIS_TZ).strftime("%Y-%m-%d %H:%M")

        print(
            f"{pair:<8} "
            f"{d1.color:<10} "
            f"{d1_ashi_color:<8} "
            f"{w1_ashi_color:<8} "
            f"{d1_last:<18}"
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
    print_group("TREND BULLISH (RENKO_D1 + HA_D1 + HA_W1)", groups["trend_bullish"])
    print_group("TREND BEARISH (RENKO_D1 + HA_D1 + HA_W1)", groups["trend_bearish"])


def main() -> int:
    args = parse_args()

    renko_states = build_renko_states(PAIRS_29, args.intervals, args.length, args.candles, use_wicks=not args.no_wicks)
    ashi_states = build_ashi_states(PAIRS_29, ("D", "W"), max(80, int(args.candles)))

    print_state_table(renko_states, ashi_states)

    trend_groups = group_trends_v3(renko_states, ashi_states)
    print_groups(trend_groups)

    trending: list[str] = []
    retracing: list[str] = []
    trend_pairs = sorted(set(trend_groups.get("trend_bullish", []) + trend_groups.get("trend_bearish", [])))
    if trend_pairs:
        for pair in trend_pairs:
            d1 = renko_states.get(pair, {}).get("D")
            if not d1:
                continue
            bias, _, _, _ = h1_psar_bias(pair)
            if bias == "N/A" or bias == "NEUTRAL":
                continue

            expected = "BULL" if d1.direction == 1 else "BEAR" if d1.direction == -1 else "N/A"
            if expected == "N/A":
                continue
            if bias == expected:
                trending.append(pair)
            else:
                retracing.append(pair)

    print_group("TRENDING (H1 PSAR in trend direction)", trending)
    print_group("RETRACING (H1 PSAR against trend)", retracing)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
