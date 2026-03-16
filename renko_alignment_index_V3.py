#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scan the 29 pairs directly, without index pre-filtering.

For each pair:
- derive the expected direction from the pair's own H1 Renko state
- validate with H1 Ichimoku, D1 close-to-close, and D1 Heikin Ashi color
"""

import argparse
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from ichimoku_v4 import PAIRS_29, compute_ichimoku_bias_state, fetch_tv_ohlc, send_telegram_message


PARIS_TZ = ZoneInfo("Europe/Paris")
HOURLY_INTERVAL = "60"
DAILY_INTERVAL = "D"


@dataclass
class RenkoState:
    pair: str
    interval: str
    close: float
    box_size: float
    direction: int
    color: str
    last_brick_at: pd.Timestamp | None
    source_last_at: pd.Timestamp | None


@dataclass
class PairCheck:
    pair: str
    expected: str
    renko_h1: str
    chg_cc_h1: float | None
    chg_cc_daily: float | None
    h1_last_at: pd.Timestamp | None
    cloud_confirms: bool
    daily_confirms: bool
    ha_daily_confirms: bool
    daily_alignment_confirms: bool
    confirms: bool | None


def parse_args():
    parser = argparse.ArgumentParser(description="Scan the 29 pairs directly from their own H1 Renko state.")
    parser.add_argument("--length", type=int, default=14, help="ATR length.")
    parser.add_argument("--candles", type=int, default=260, help="Number of H1 candles fetched per pair.")
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
    symbol = f"OANDA:{pair}"
    df = fetch_tv_ohlc(symbol, interval, candles)
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
        close=float(close.iloc[-1]),
        box_size=box_size,
        direction=direction,
        color=color,
        last_brick_at=last_brick_at,
        source_last_at=df.index[-1],
    )


def pct_change_last_bar(df: pd.DataFrame | None) -> float | None:
    if df is None or df.empty or len(df) < 2:
        return None
    close = df["close"].astype(float)
    prev_close = float(close.iloc[-2])
    last_close = float(close.iloc[-1])
    if prev_close == 0:
        return None
    return ((last_close - prev_close) / prev_close) * 100.0


def daily_chg_cc(df_daily: pd.DataFrame | None) -> float | None:
    if df_daily is None or df_daily.empty or len(df_daily) < 2:
        return None
    close = df_daily["close"].astype(float)
    prev_close = float(close.iloc[-2])
    last_close = float(close.iloc[-1])
    if prev_close == 0:
        return None
    return ((last_close - prev_close) / prev_close) * 100.0


def heikin_ashi_daily_confirms(df_daily: pd.DataFrame | None, expected: str) -> bool:
    if df_daily is None or df_daily.empty:
        return False

    o = df_daily["open"].astype(float)
    h = df_daily["high"].astype(float)
    l = df_daily["low"].astype(float)
    c = df_daily["close"].astype(float)

    ha_close = (o + h + l + c) / 4.0
    ha_open = pd.Series(index=df_daily.index, dtype=float)
    ha_open.iloc[0] = (float(o.iloc[0]) + float(c.iloc[0])) / 2.0

    for i in range(1, len(df_daily)):
        ha_open.iloc[i] = (float(ha_open.iloc[i - 1]) + float(ha_close.iloc[i - 1])) / 2.0

    last_ha_open = float(ha_open.iloc[-1])
    last_ha_close = float(ha_close.iloc[-1])

    if expected == "BULLISH":
        return last_ha_close > last_ha_open
    return last_ha_close < last_ha_open


def price_confirms_cloud(df_h1: pd.DataFrame | None, expected: str) -> bool:
    if df_h1 is None or df_h1.empty:
        return False

    h1_state = compute_ichimoku_bias_state(df_h1)
    if (
        h1_state.close is None
        or h1_state.visible_cloud_top is None
        or h1_state.visible_cloud_bottom is None
    ):
        return False

    if expected == "BULLISH":
        return h1_state.close > h1_state.visible_cloud_top
    return h1_state.close < h1_state.visible_cloud_bottom


def compute_pair_check(pair: str, length: int, candles: int) -> PairCheck:
    renko_h1 = compute_renko_state(pair, HOURLY_INTERVAL, length, candles)
    if renko_h1.direction == 1:
        expected = "BULLISH"
    elif renko_h1.direction == -1:
        expected = "BEARISH"
    else:
        expected = "NEUTRAL"

    df_h1 = fetch_tv_ohlc(f"OANDA:{pair}", HOURLY_INTERVAL, max(candles, 160))
    df_d1 = fetch_tv_ohlc(f"OANDA:{pair}", DAILY_INTERVAL, 160)
    h1_last_at = None if df_h1 is None or df_h1.empty else df_h1.index[-1]
    chg_h1 = pct_change_last_bar(df_h1)
    chg_d1 = daily_chg_cc(df_d1)

    if expected == "NEUTRAL":
        return PairCheck(
            pair=pair,
            expected=expected,
            renko_h1=renko_h1.color,
            chg_cc_h1=chg_h1,
            chg_cc_daily=chg_d1,
            h1_last_at=h1_last_at,
            cloud_confirms=False,
            daily_confirms=False,
            ha_daily_confirms=False,
            daily_alignment_confirms=False,
            confirms=False,
        )

    cloud_confirms = price_confirms_cloud(df_h1, expected)
    daily_confirms = False if chg_d1 is None else (chg_d1 > 0 if expected == "BULLISH" else chg_d1 < 0)
    ha_daily_confirms = heikin_ashi_daily_confirms(df_d1, expected)
    daily_alignment_confirms = daily_confirms == ha_daily_confirms
    confirms = cloud_confirms and daily_confirms and ha_daily_confirms and daily_alignment_confirms

    return PairCheck(
        pair=pair,
        expected=expected,
        renko_h1=renko_h1.color,
        chg_cc_h1=chg_h1,
        chg_cc_daily=chg_d1,
        h1_last_at=h1_last_at,
        cloud_confirms=cloud_confirms,
        daily_confirms=daily_confirms,
        ha_daily_confirms=ha_daily_confirms,
        daily_alignment_confirms=daily_alignment_confirms,
        confirms=confirms,
    )


def compute_pair_checks(pairs: list[str], length: int, candles: int) -> list[PairCheck]:
    return [compute_pair_check(pair, length, candles) for pair in pairs]


def print_pair_checks(checks: list[PairCheck]) -> None:
    print("Direct 29-Pair Scan")
    if not checks:
        print("(none)")
        print("")
        return

    print(
        f"{'PAIR':<8} {'RENKO_H1':<9} {'EXPECTED':<8} {'CHG%H1':<9} "
        f"{'CHG%D':<9} {'D1':<7} {'CLOUD':<7} {'HA_D':<7} {'D1=HA':<7} {'H1_LAST':<18} {'CONFIRMS':<8}"
    )
    print("-" * 118)
    for row in sorted(checks, key=lambda x: (x.confirms is False, x.expected == "NEUTRAL", x.pair)):
        chg_h1_txt = "N/A" if row.chg_cc_h1 is None else f"{row.chg_cc_h1:+.2f}%"
        chg_d1_txt = "N/A" if row.chg_cc_daily is None else f"{row.chg_cc_daily:+.2f}%"
        d1_txt = "YES" if row.daily_confirms else "NO"
        cloud_txt = "YES" if row.cloud_confirms else "NO"
        ha_txt = "YES" if row.ha_daily_confirms else "NO"
        align_txt = "YES" if row.daily_alignment_confirms else "NO"
        h1_last_txt = "-" if row.h1_last_at is None else row.h1_last_at.tz_convert(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
        confirms_txt = "YES" if row.confirms else "NO"
        print(
            f"{row.pair:<8} "
            f"{row.renko_h1:<9} "
            f"{row.expected:<8} "
            f"{chg_h1_txt:<9} "
            f"{chg_d1_txt:<9} "
            f"{d1_txt:<7} "
            f"{cloud_txt:<7} "
            f"{ha_txt:<7} "
            f"{align_txt:<7} "
            f"{h1_last_txt:<18} "
            f"{confirms_txt:<8}"
        )
    print("")


def build_telegram_message(checks: list[PairCheck]) -> str:
    lines = ["RENKO INDEX V3", ""]
    selected = [row for row in checks if row.confirms]

    if not selected:
        lines.append("NO DEAL")
    else:
        for row in sorted(selected, key=lambda x: (abs(float(x.chg_cc_daily or 0.0)), x.pair), reverse=True):
            direction = "BULL" if row.expected == "BULLISH" else "BEAR"
            chg_txt = "N/A" if row.chg_cc_daily is None else f"{row.chg_cc_daily:+.2f}%"
            lines.append(f"{direction} {row.pair} ({chg_txt}) [CLOUD D1 HA_D]")

    lines.append("")
    lines.append(f"TIME {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M Paris')}")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    pair_checks = compute_pair_checks(PAIRS_29, args.length, args.candles)
    print_pair_checks(pair_checks)
    send_telegram_message(build_telegram_message(pair_checks))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
