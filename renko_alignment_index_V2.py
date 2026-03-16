#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Screen the currency indices for simple Renko alignment on H1 only.

Then validate the FX pairs implied by GREEN vs RED currencies
using H1 Ichimoku, D1 close-to-close and D1 Heikin Ashi confirmations.
"""

import argparse
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from zoneinfo import ZoneInfo

from ichimoku_v4 import compute_ichimoku_bias_state, fetch_tv_ohlc, send_telegram_message


PARIS_TZ = ZoneInfo("Europe/Paris")
HOURLY_INTERVAL = "60"
HOURLY_LABEL = "H1"
DAILY_INTERVAL = "D"

INDICES = {
    "DXY": "TVC:DXY",
    "EXY": "TVC:EXY",
    "BXY": "TVC:BXY",
    "JXY": "TVC:JXY",
    "SXY": "TVC:SXY",
    "CXY": "TVC:CXY",
    "AXY": "TVC:AXY",
    "ZXY": "TVC:ZXY",
}

INDEX_TO_CCY = {
    "DXY": "USD",
    "EXY": "EUR",
    "BXY": "GBP",
    "JXY": "JPY",
    "SXY": "CHF",
    "CXY": "CAD",
    "AXY": "AUD",
    "ZXY": "NZD",
}

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURNZD", "EURCHF",
    "GBPAUD", "GBPCAD", "GBPNZD", "GBPCHF",
    "AUDNZD", "AUDCAD", "AUDCHF",
    "NZDCAD", "NZDCHF",
    "CADCHF",
]


@dataclass
class RenkoState:
    index_code: str
    symbol: str
    interval: str
    label: str
    close: float
    box_size: float
    direction: int
    color: str
    last_brick_at: pd.Timestamp | None
    source_last_at: pd.Timestamp | None


@dataclass
class PairCheck:
    pair: str
    base: str
    quote: str
    expected: str
    chg_cc_h1: float | None
    chg_cc_daily: float | None
    h1_last_at: pd.Timestamp | None
    cloud_confirms: bool
    daily_confirms: bool
    ha_daily_confirms: bool
    daily_alignment_confirms: bool
    confirms: bool | None


def parse_args():
    parser = argparse.ArgumentParser(description="Screen Renko H1 alignments across the currency indices.")
    parser.add_argument("--length", type=int, default=14, help="ATR length.")
    parser.add_argument("--candles", type=int, default=260, help="Number of H1 candles fetched per symbol.")
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


def compute_renko_state(index_code: str, symbol: str, interval: str, length: int, candles: int) -> RenkoState:
    df = fetch_tv_ohlc(symbol, interval, candles)
    if df is None or df.empty:
        raise RuntimeError(f"Unable to fetch OHLC for {index_code} ({symbol}) on {interval}")

    atr_series = atr(df, length)
    box_size = float(atr_series.iloc[-1])
    if pd.isna(box_size) or box_size <= 0:
        raise RuntimeError(f"Not enough data to compute ATR({length}) for {index_code} ({symbol}) on {interval}")

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
        index_code=index_code,
        symbol=symbol,
        interval=interval,
        label=HOURLY_LABEL,
        close=float(close.iloc[-1]),
        box_size=box_size,
        direction=direction,
        color=color,
        last_brick_at=last_brick_at,
        source_last_at=df.index[-1],
    )


def build_states(indices: dict[str, str], length: int, candles: int) -> dict[str, RenkoState]:
    out: dict[str, RenkoState] = {}
    for index_code, symbol in indices.items():
        out[index_code] = compute_renko_state(index_code, symbol, HOURLY_INTERVAL, length, candles)
    return out


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


def group_alignments_hourly(all_states: dict[str, RenkoState]) -> dict[str, list[str]]:
    groups = {
        "h1_green": [],
        "h1_red": [],
    }

    for index_code, state in all_states.items():
        label = f"{index_code}({INDEX_TO_CCY.get(index_code, '?')})"
        if state.direction == 1:
            groups["h1_green"].append(label)
        elif state.direction == -1:
            groups["h1_red"].append(label)

    return groups


def print_state_table(all_states: dict[str, RenkoState]) -> None:
    print(f"{'INDEX':<8} {'CCY':<5} {'H1':<8} {'H1_LAST':<18} {'SRC_LAST':<18}")
    print("-" * 62)
    for index_code in sorted(all_states):
        state = all_states[index_code]
        h1_last = "-" if state.last_brick_at is None else state.last_brick_at.tz_convert(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
        src_last = "-" if state.source_last_at is None else state.source_last_at.tz_convert(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
        print(
            f"{index_code:<8} "
            f"{INDEX_TO_CCY.get(index_code, '?'):<5} "
            f"{state.color:<8} "
            f"{h1_last:<18} "
            f"{src_last:<18}"
        )


def print_group(title: str, items: list[str]) -> None:
    print(title)
    if items:
        print(", ".join(sorted(items)))
    else:
        print("(none)")
    print("")


def print_groups(groups: dict[str, list[str]]) -> None:
    print("")
    print_group("H1 GREEN", groups["h1_green"])
    print_group("H1 RED", groups["h1_red"])


def aligned_currencies(all_states: dict[str, RenkoState]) -> tuple[set[str], set[str]]:
    greens: set[str] = set()
    reds: set[str] = set()
    for index_code, state in all_states.items():
        ccy = INDEX_TO_CCY.get(index_code)
        if not ccy:
            continue
        if state.direction == 1:
            greens.add(ccy)
        elif state.direction == -1:
            reds.add(ccy)
    return greens, reds


def compute_pair_checks(pairs: list[str], green_ccy: set[str], red_ccy: set[str]) -> list[PairCheck]:
    out: list[PairCheck] = []
    for pair in pairs:
        base = pair[:3]
        quote = pair[3:6]
        expected = None
        if base in green_ccy and quote in red_ccy:
            expected = "BULLISH"
        elif base in red_ccy and quote in green_ccy:
            expected = "BEARISH"
        if expected is None:
            continue

        df_h1 = fetch_tv_ohlc(f"OANDA:{pair}", HOURLY_INTERVAL, 160)
        df_d1 = fetch_tv_ohlc(f"OANDA:{pair}", DAILY_INTERVAL, 160)
        h1_last_at = None if df_h1 is None or df_h1.empty else df_h1.index[-1]
        chg = pct_change_last_bar(df_h1)
        chg_daily = daily_chg_cc(df_d1)
        cloud_confirms = price_confirms_cloud(df_h1, expected)
        daily_confirms = None if chg_daily is None else (chg_daily > 0 if expected == "BULLISH" else chg_daily < 0)
        ha_daily_confirms = heikin_ashi_daily_confirms(df_d1, expected)
        daily_alignment_confirms = daily_confirms == ha_daily_confirms
        confirms = (
            None
            if daily_confirms is None
            else (
                cloud_confirms
                and daily_confirms
                and ha_daily_confirms
                and daily_alignment_confirms
            )
        )
        out.append(
            PairCheck(
                pair=pair,
                base=base,
                quote=quote,
                expected=expected,
                chg_cc_h1=chg,
                chg_cc_daily=chg_daily,
                h1_last_at=h1_last_at,
                cloud_confirms=cloud_confirms,
                daily_confirms=daily_confirms,
                ha_daily_confirms=ha_daily_confirms,
                daily_alignment_confirms=daily_alignment_confirms,
                confirms=confirms,
            )
        )
    return out


def print_pair_checks(checks: list[PairCheck]) -> None:
    print("Index Strength vs H1 Change + Ichimoku + HA Daily")
    if not checks:
        print("(none)")
        print("")
        return

    print(
        f"{'PAIR':<8} {'BASE':<5} {'QUOTE':<5} {'EXPECTED':<8} "
        f"{'CHG%H1':<9} {'CHG%D':<9} {'D1':<7} {'CLOUD':<7} {'HA_D':<7} {'D1=HA':<7} {'H1_LAST':<18} {'CONFIRMS':<8}"
    )
    print("-" * 111)
    for row in sorted(checks, key=lambda x: (x.confirms is False, x.pair)):
        chg_txt = "N/A" if row.chg_cc_h1 is None else f"{row.chg_cc_h1:+.2f}%"
        chg_daily_txt = "N/A" if row.chg_cc_daily is None else f"{row.chg_cc_daily:+.2f}%"
        daily_txt = "YES" if row.daily_confirms else "NO"
        cloud_txt = "YES" if row.cloud_confirms else "NO"
        ha_txt = "YES" if row.ha_daily_confirms else "NO"
        daily_align_txt = "YES" if row.daily_alignment_confirms else "NO"
        h1_last_txt = "-" if row.h1_last_at is None else row.h1_last_at.tz_convert(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
        confirms_txt = "YES" if row.confirms is True else "NO" if row.confirms is False else "N/A"
        print(
            f"{row.pair:<8} "
            f"{row.base:<5} "
            f"{row.quote:<5} "
            f"{row.expected:<8} "
            f"{chg_txt:<9} "
            f"{chg_daily_txt:<9} "
            f"{daily_txt:<7} "
            f"{cloud_txt:<7} "
            f"{ha_txt:<7} "
            f"{daily_align_txt:<7} "
            f"{h1_last_txt:<18} "
            f"{confirms_txt:<8}"
        )
    print("")


def build_telegram_message(checks: list[PairCheck]) -> str:
    lines = ["RENKO INDEX H1", ""]
    sorted_checks = sorted(
        [row for row in checks if row.confirms is True and row.chg_cc_h1 is not None],
        key=lambda row: abs(float(row.chg_cc_h1)),
        reverse=True,
    )

    if not sorted_checks:
        lines.append("NO DEAL")
    else:
        for row in sorted_checks:
            direction = "BULL" if row.expected == "BULLISH" else "BEAR"
            lines.append(f"{direction} {row.pair} ({row.chg_cc_h1:+.2f}%) [CLOUD HA_D]")

    lines.append("")
    lines.append(f"TIME {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M Paris')}")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    all_states = build_states(INDICES, args.length, args.candles)
    green_ccy, red_ccy = aligned_currencies(all_states)
    pair_checks = compute_pair_checks(PAIRS, green_ccy, red_ccy)
    print_state_table(all_states)
    print_groups(group_alignments_hourly(all_states))
    print_pair_checks(pair_checks)
    send_telegram_message(build_telegram_message(pair_checks))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
