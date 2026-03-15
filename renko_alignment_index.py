#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Screen the currency indices for simple Renko alignment:
- H1 + D1 GREEN
- H1 + D1 RED

Then validate the FX pairs implied by GREEN vs RED currencies
using CHG% CC DAILY.
"""

import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from ichimoku_v4 import compute_ichimoku_bias_state, fetch_tv_ohlc, send_telegram_message


PARIS_TZ = ZoneInfo("Europe/Paris")

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

TIMEFRAME_LABELS = {
    "60": "H1",
    "D": "D1",
}

DEFAULT_INTERVALS = ("60", "D")


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
    chg_cc_daily: float | None
    d1_last_at: pd.Timestamp | None
    cloud_confirms: bool
    sar_confirms: bool
    confirms: bool | None


def parse_args():
    parser = argparse.ArgumentParser(description="Screen Renko H1/D1 alignments across the currency indices.")
    parser.add_argument("--length", type=int, default=14, help="ATR length.")
    parser.add_argument("--candles", type=int, default=260, help="Number of candles fetched per symbol and timeframe.")
    parser.add_argument(
        "--intervals",
        nargs="+",
        default=list(DEFAULT_INTERVALS),
        help="TradingView intervals. Defaults to 60 D",
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
        label=TIMEFRAME_LABELS.get(interval, interval),
        close=float(close.iloc[-1]),
        box_size=box_size,
        direction=direction,
        color=color,
        last_brick_at=last_brick_at,
        source_last_at=df.index[-1],
    )


def build_states(
    indices: dict[str, str], intervals: Iterable[str], length: int, candles: int
) -> dict[str, dict[str, RenkoState]]:
    out: dict[str, dict[str, RenkoState]] = {}
    for index_code, symbol in indices.items():
        out[index_code] = {}
        for interval in intervals:
            out[index_code][interval] = compute_renko_state(index_code, symbol, interval, length, candles)
    return out


def current_paris_week_start() -> pd.Timestamp:
    now_paris = datetime.now(PARIS_TZ)
    monday = now_paris.replace(hour=0, minute=0, second=0, microsecond=0) - pd.Timedelta(days=now_paris.weekday())
    return pd.Timestamp(monday)


def is_in_current_paris_week(ts: pd.Timestamp | None) -> bool:
    if ts is None:
        return False
    ts_paris = ts.tz_convert(PARIS_TZ)
    return ts_paris >= current_paris_week_start()


def daily_chg_cc(df_daily: pd.DataFrame) -> float | None:
    if df_daily is None or df_daily.empty or len(df_daily) < 2:
        return None
    close = df_daily["close"].astype(float)
    prev_close = float(close.iloc[-2])
    last_close = float(close.iloc[-1])
    if prev_close == 0:
        return None
    return ((last_close - prev_close) / prev_close) * 100.0


def calculate_psar(
    df: pd.DataFrame,
    start: float = 0.1,
    increment: float = 0.1,
    maximum: float = 0.2,
) -> pd.Series | None:
    if df is None or df.empty or len(df) < 2:
        return None

    highs = df["high"].astype(float).to_numpy()
    lows = df["low"].astype(float).to_numpy()
    closes = df["close"].astype(float).to_numpy()

    psar = np.zeros(len(df), dtype=float)
    bull = closes[1] >= closes[0]
    af = start
    ep = highs[0] if bull else lows[0]
    psar[0] = lows[0] if bull else highs[0]

    for i in range(1, len(df)):
        psar[i] = psar[i - 1] + af * (ep - psar[i - 1])
        if bull:
            if lows[i] < psar[i]:
                bull = False
                psar[i] = ep
                ep = lows[i]
                af = start
            else:
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + increment, maximum)
                psar[i] = min(psar[i], lows[i - 1], lows[i - 2]) if i >= 2 else min(psar[i], lows[i - 1])
        else:
            if highs[i] > psar[i]:
                bull = True
                psar[i] = ep
                ep = highs[i]
                af = start
            else:
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + increment, maximum)
                psar[i] = max(psar[i], highs[i - 1], highs[i - 2]) if i >= 2 else max(psar[i], highs[i - 1])

    return pd.Series(psar, index=df.index)


def build_renko_brick_frame(df: pd.DataFrame | None, length: int = 14) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None

    atr_series = atr(df, length)
    if atr_series.empty:
        return None
    box_size = float(atr_series.iloc[-1])
    if pd.isna(box_size) or box_size <= 0:
        return None

    close = df["close"].astype(float)
    anchor = float(close.iloc[0])
    direction = 0
    bricks: list[dict] = []

    for ts, price in close.iloc[1:].items():
        price = float(price)
        formed = 0
        new_direction = direction

        if direction == 0:
            if price >= anchor + box_size:
                formed = int((price - anchor) // box_size)
                new_direction = 1 if formed > 0 else direction
            elif price <= anchor - box_size:
                formed = int((anchor - price) // box_size)
                new_direction = -1 if formed > 0 else direction
        elif direction == 1:
            if price >= anchor + box_size:
                formed = int((price - anchor) // box_size)
                new_direction = 1 if formed > 0 else direction
            elif price <= anchor - (2.0 * box_size):
                formed = int((anchor - price) // box_size) - 1
                new_direction = -1 if formed > 0 else direction
        else:
            if price <= anchor - box_size:
                formed = int((anchor - price) // box_size)
                new_direction = -1 if formed > 0 else direction
            elif price >= anchor + (2.0 * box_size):
                formed = int((price - anchor) // box_size) - 1
                new_direction = 1 if formed > 0 else direction

        if formed <= 0:
            continue

        step = box_size if new_direction == 1 else -box_size
        for _ in range(formed):
            prev_anchor = anchor
            anchor += step
            bricks.append(
                {
                    "open": prev_anchor,
                    "high": max(prev_anchor, anchor),
                    "low": min(prev_anchor, anchor),
                    "close": anchor,
                    "timestamp": ts,
                }
            )
        direction = new_direction

    if len(bricks) < 2:
        return None

    brick_df = pd.DataFrame(bricks)
    brick_df.index = pd.Index(brick_df.pop("timestamp"))
    return brick_df


def psar_confirms_trend(df_h1: pd.DataFrame | None, df_d1: pd.DataFrame | None, expected: str) -> bool:
    if df_h1 is None or df_h1.empty or df_d1 is None or df_d1.empty:
        return False

    renko_h1 = build_renko_brick_frame(df_h1)
    renko_d1 = build_renko_brick_frame(df_d1)
    if renko_h1 is None or renko_d1 is None:
        return False

    psar_h1 = calculate_psar(renko_h1)
    psar_d1 = calculate_psar(renko_d1)
    if psar_h1 is None or psar_h1.empty or psar_d1 is None or psar_d1.empty:
        return False

    h1_close = float(renko_h1["close"].iloc[-1])
    d1_close = float(renko_d1["close"].iloc[-1])
    h1_psar = float(psar_h1.iloc[-1])
    d1_psar = float(psar_d1.iloc[-1])

    if expected == "BULLISH":
        return h1_close > h1_psar and d1_close > d1_psar
    return h1_close < h1_psar and d1_close < d1_psar


def price_confirms_cloud(pair: str, expected: str) -> bool:
    df_h1 = fetch_tv_ohlc(f"OANDA:{pair}", "60", 160)
    df_d1 = fetch_tv_ohlc(f"OANDA:{pair}", "D", 160)
    if df_h1 is None or df_h1.empty or df_d1 is None or df_d1.empty:
        return False

    h1_state = compute_ichimoku_bias_state(df_h1)
    d1_state = compute_ichimoku_bias_state(df_d1)
    if (
        h1_state.close is None
        or d1_state.close is None
        or h1_state.visible_cloud_top is None
        or h1_state.visible_cloud_bottom is None
        or d1_state.visible_cloud_top is None
        or d1_state.visible_cloud_bottom is None
    ):
        return False

    if expected == "BULLISH":
        return h1_state.close > h1_state.visible_cloud_top and d1_state.close > d1_state.visible_cloud_top
    return h1_state.close < h1_state.visible_cloud_bottom and d1_state.close < d1_state.visible_cloud_bottom


def group_alignments_v2(all_states: dict[str, dict[str, RenkoState]]) -> dict[str, list[str]]:
    groups = {
        "h1_d1_green": [],
        "h1_d1_red": [],
    }

    for index_code, states in all_states.items():
        h1 = states.get("60")
        d1 = states.get("D")
        if not h1 or not d1:
            continue

        label = f"{index_code}({INDEX_TO_CCY.get(index_code, '?')})"
        if d1.direction == 1 and h1.direction == 1:
            groups["h1_d1_green"].append(label)
        elif d1.direction == -1 and h1.direction == -1:
            groups["h1_d1_red"].append(label)

    return groups


def print_state_table(all_states: dict[str, dict[str, RenkoState]]) -> None:
    print(
        f"{'INDEX':<8} {'CCY':<5} {'H1':<8} {'D1':<8} {'D1_OK':<6} {'H1_LAST':<18} {'D1_LAST':<18}"
    )
    print("-" * 80)
    for index_code in sorted(all_states):
        states = all_states[index_code]
        h1 = states["60"]
        d1 = states["D"]
        d1_ok = is_in_current_paris_week(d1.last_brick_at)
        h1_last = "-" if h1.last_brick_at is None else h1.last_brick_at.tz_convert(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
        d1_last = "-" if d1.last_brick_at is None else d1.last_brick_at.tz_convert(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
        print(
            f"{index_code:<8} "
            f"{INDEX_TO_CCY.get(index_code, '?'):<5} "
            f"{h1.color:<8} "
            f"{d1.color:<8} "
            f"{str(d1_ok):<6} "
            f"{h1_last:<18} "
            f"{d1_last:<18} "
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
    print_group("H1 + D1 GREEN", groups["h1_d1_green"])
    print_group("H1 + D1 RED", groups["h1_d1_red"])


def aligned_currencies(all_states: dict[str, dict[str, RenkoState]]) -> tuple[set[str], set[str]]:
    greens: set[str] = set()
    reds: set[str] = set()
    for index_code, states in all_states.items():
        h1 = states.get("60")
        d1 = states.get("D")
        if not h1 or not d1:
            continue
        if not is_in_current_paris_week(d1.last_brick_at):
            continue
        ccy = INDEX_TO_CCY.get(index_code)
        if not ccy:
            continue
        if h1.direction == 1 and d1.direction == 1:
            greens.add(ccy)
        elif h1.direction == -1 and d1.direction == -1:
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

        df_h1 = fetch_tv_ohlc(f"OANDA:{pair}", "60", 160)
        df_d1 = fetch_tv_ohlc(f"OANDA:{pair}", "D", 160)
        d1_last_at = None if df_d1 is None or df_d1.empty else df_d1.index[-1]
        if not is_in_current_paris_week(d1_last_at):
            continue
        chg = daily_chg_cc(df_d1)
        confirms = None if chg is None else (chg > 0 if expected == "BULLISH" else chg < 0)
        cloud_confirms = price_confirms_cloud(pair, expected)
        sar_confirms = psar_confirms_trend(df_h1, df_d1, expected)
        out.append(
            PairCheck(
                pair=pair,
                base=base,
                quote=quote,
                expected=expected,
                chg_cc_daily=chg,
                d1_last_at=d1_last_at,
                cloud_confirms=cloud_confirms,
                sar_confirms=sar_confirms,
                confirms=confirms,
            )
        )
    return out


def print_pair_checks(checks: list[PairCheck]) -> None:
    print("Index Strength vs CHG% CC DAILY")
    if not checks:
        print("(none)")
        print("")
        return

    print(f"{'PAIR':<8} {'BASE':<5} {'QUOTE':<5} {'EXPECTED':<8} {'CHG%D':<9} {'D1_LAST':<18} {'CONFIRMS':<8}")
    print("-" * 73)
    for row in sorted(checks, key=lambda x: (x.confirms is False, x.pair)):
        chg_txt = "N/A" if row.chg_cc_daily is None else f"{row.chg_cc_daily:+.2f}%"
        d1_last_txt = "-" if row.d1_last_at is None else row.d1_last_at.tz_convert(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
        confirms_txt = "YES" if row.confirms is True else "NO" if row.confirms is False else "N/A"
        print(
            f"{row.pair:<8} "
            f"{row.base:<5} "
            f"{row.quote:<5} "
            f"{row.expected:<8} "
            f"{chg_txt:<9} "
            f"{d1_last_txt:<18} "
            f"{confirms_txt:<8}"
        )
    print("")


def build_telegram_message(checks: list[PairCheck]) -> str:
    lines = ["RENKO INDEX", ""]
    sorted_checks = sorted(
        [row for row in checks if row.confirms is True and row.chg_cc_daily is not None],
        key=lambda row: abs(float(row.chg_cc_daily)),
        reverse=True,
    )

    if not sorted_checks:
        lines.append("NO DEAL 😞")
    else:
        for row in sorted_checks:
            base_icon = "🟢" if row.expected == "BULLISH" else "🔴"
            confirm_icon = base_icon if row.sar_confirms else "⚪"
            cloud_marker = " ☁️" if row.cloud_confirms else ""
            lines.append(f"{base_icon}{confirm_icon} {row.pair} ({row.chg_cc_daily:+.2f}%){cloud_marker}")

    lines.append("")
    lines.append(f"⏰ {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M Paris')}")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    all_states = build_states(INDICES, args.intervals, args.length, args.candles)
    green_ccy, red_ccy = aligned_currencies(all_states)
    pair_checks = compute_pair_checks(PAIRS, green_ccy, red_ccy)
    print_state_table(all_states)
    print_groups(group_alignments_v2(all_states))
    print_pair_checks(pair_checks)
    send_telegram_message(build_telegram_message(pair_checks))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
