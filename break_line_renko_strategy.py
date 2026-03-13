#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Strategy screener inspired by break_line.py with Renko filters.

Bull signal:
- H1 close crosses above the latest bear_vw0, or H1 price crosses above PSAR
- W1 Renko ATR(14) is bullish
- D1 Renko ATR(14) is bullish
- H1 Renko ATR(14) is bullish

Bear signal:
- H1 close crosses below the latest bull_vw0, or H1 price crosses below PSAR
- W1 Renko ATR(14) is bearish
- D1 Renko ATR(14) is bearish
- H1 Renko ATR(14) is bearish
"""

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from break_line import (
    PAIRS_29,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    calculate_psar,
    compute_break_line_state,
    daily_chg_cc,
    fetch_tv_ohlc,
    send_telegram_message,
)

PARIS_TZ = ZoneInfo("Europe/Paris")
TRACKING_PATH = os.path.join(os.path.dirname(__file__), "break_line_renko_state.json")
SCAN_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "break_line_renko_scan.json")


@dataclass
class RenkoState:
    direction: int
    color: str
    box_size: float | None
    last_brick_at: pd.Timestamp | None


def parse_args():
    parser = argparse.ArgumentParser(description="Break line + Renko strategy screener")
    parser.add_argument("--pair", default=None, help="Analyze a single pair, e.g. EURUSD")
    parser.add_argument("--scan29", action="store_true", help="Scan the 29 configured instruments")
    parser.add_argument("--renko-length", type=int, default=14, help="ATR length for Renko box size")
    parser.add_argument("--w-candles", type=int, default=260, help="Weekly candles fetched")
    parser.add_argument("--d-candles", type=int, default=320, help="Daily candles fetched")
    parser.add_argument("--h-candles", type=int, default=900, help="H1 candles fetched")
    parser.add_argument("--telegram", action="store_true", help="Send Telegram update when active persisted signals change")
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
        return RenkoState(0, "NEUTRAL", None, None)

    atr_series = atr(df, length)
    box_size = float(atr_series.iloc[-1]) if not atr_series.empty else None
    if box_size is None or pd.isna(box_size) or box_size <= 0:
        return RenkoState(0, "NEUTRAL", None, None)

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

    color = "BULL" if direction == 1 else "BEAR" if direction == -1 else "NEUTRAL"
    return RenkoState(direction, color, box_size, last_brick_at)


def market_week_start_from_df(df: pd.DataFrame) -> pd.Timestamp | None:
    if df is None or df.empty:
        return None
    return df.index[-1]


def renko_brick_in_week(last_brick_at: pd.Timestamp | None, market_week_start: pd.Timestamp | None) -> bool:
    if last_brick_at is None or market_week_start is None:
        return False
    return last_brick_at >= market_week_start


def load_tracking_state(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return data
    except Exception:
        return {}
    return {}


def save_tracking_state(path: str, active_rows: list[dict]) -> None:
    payload = {
        "updated_at_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "active_signals": {
            row["pair"]: {
                "pair": row["pair"],
                "signal_direction": row["signal_direction"],
                "chg_cc_d1": row.get("chg_cc_d1"),
            }
            for row in active_rows
        },
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)


def save_scan_snapshot(path: str, rows: list[dict], active_rows: list[dict]) -> None:
    payload = {
        "updated_at_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rows": rows,
        "active_rows": active_rows,
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)


def build_telegram_message(active_rows: list[dict]) -> str:
    lines = ["RENKO", ""]
    if not active_rows:
        lines.append("NO DEAL \U0001F61E")
    else:
        for row in active_rows:
            icon = "\U0001F7E2" if row["signal_direction"] == "BULL" else "\U0001F534"
            chg = row.get("chg_cc_d1")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            lines.append(f"{icon} {row['pair']} ({chg_txt})")
    now_txt = datetime.now(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
    lines.extend(["", f"\u23F0 {now_txt} Paris"])
    return "\n".join(lines)


def row_invalidation_direction(row: dict, active_direction: str) -> bool:
    renko_ok = (
        row["renko_h1"] == active_direction
        and row["renko_d1"] == active_direction
        and row["renko_w1"] == active_direction
    )
    fresh_ok = row["hourly_fresh"] and row["daily_fresh"] and row["weekly_fresh"]
    if active_direction == "BULL":
        return row["sar_bear_cross"] or row["break_below_bull_vw0"] or not (renko_ok and fresh_ok)
    return row["sar_bull_cross"] or row["break_above_bear_vw0"] or not (renko_ok and fresh_ok)


def compute_persisted_signals(rows: list[dict], previous_state: dict) -> list[dict]:
    previous_active = previous_state.get("active_signals", {}) if isinstance(previous_state, dict) else {}
    active_rows = []

    for row in rows:
        if row.get("error"):
            continue

        previous = previous_active.get(row["pair"])
        current_direction = row["signal_direction"]

        if current_direction in {"BULL", "BEAR"}:
            active_rows.append(row)
            continue

        if not previous:
            continue

        previous_direction = previous.get("signal_direction")
        if previous_direction not in {"BULL", "BEAR"}:
            continue

        if row_invalidation_direction(row, previous_direction):
            continue

        persisted = dict(row)
        persisted["signal_direction"] = previous_direction
        persisted["persisted"] = True
        active_rows.append(persisted)

    active_rows.sort(key=lambda r: abs(r.get("chg_cc_d1") or 0.0), reverse=True)
    return active_rows


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
    renko_h1 = compute_renko_state(h1, renko_length)
    chg_cc_d1 = daily_chg_cc(d1)
    market_week_start = market_week_start_from_df(w1)

    break_above_bear_vw0 = price_crosses_above_level(h1, break_h1.bear_vw0)
    break_below_bull_vw0 = price_crosses_below_level(h1, break_h1.bull_vw0)
    sar_bull_cross, sar_bear_cross = psar_cross_signal(h1)

    weekly_fresh = renko_brick_in_week(renko_w1.last_brick_at, market_week_start)
    hourly_fresh = renko_brick_in_week(renko_h1.last_brick_at, market_week_start)
    daily_fresh = renko_brick_in_week(renko_d1.last_brick_at, market_week_start)
    freshness_ok = weekly_fresh and hourly_fresh and daily_fresh

    bull_filters = renko_w1.direction == 1 and renko_h1.direction == 1 and renko_d1.direction == 1 and freshness_ok
    bear_filters = renko_w1.direction == -1 and renko_h1.direction == -1 and renko_d1.direction == -1 and freshness_ok

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
        "renko_h1": renko_h1.color,
        "renko_d1": renko_d1.color,
        "renko_w1_box": renko_w1.box_size,
        "renko_h1_box": renko_h1.box_size,
        "renko_d1_box": renko_d1.box_size,
        "renko_w1_last_brick_at": None if renko_w1.last_brick_at is None else renko_w1.last_brick_at.tz_convert(PARIS_TZ),
        "renko_h1_last_brick_at": None if renko_h1.last_brick_at is None else renko_h1.last_brick_at.tz_convert(PARIS_TZ),
        "renko_d1_last_brick_at": None if renko_d1.last_brick_at is None else renko_d1.last_brick_at.tz_convert(PARIS_TZ),
        "chg_cc_d1": chg_cc_d1,
        "break_above_bear_vw0": break_above_bear_vw0,
        "break_below_bull_vw0": break_below_bull_vw0,
        "sar_bull_cross": sar_bull_cross,
        "sar_bear_cross": sar_bear_cross,
        "bull_filters_ok": bull_filters,
        "bear_filters_ok": bear_filters,
        "weekly_fresh": weekly_fresh,
        "hourly_fresh": hourly_fresh,
        "daily_fresh": daily_fresh,
        "bull_signal": bull_signal,
        "bear_signal": bear_signal,
        "signal_direction": "BULL" if bull_signal else "BEAR" if bear_signal else "NONE",
        "persisted": False,
    }


def print_pair_report(row: dict) -> None:
    if row.get("error"):
        print(f"{row['pair']}: data unavailable")
        return

    print(f"PAIR           : {row['pair']}")
    print(f"H1 CLOSE       : {row['close_h1']}")
    print(f"bear_vw0       : {row['bear_vw0']}")
    print(f"bull_vw0       : {row['bull_vw0']}")
    print(f"RENKO W1       : {row['renko_w1']}")
    print(f"RENKO H1       : {row['renko_h1']}")
    print(f"RENKO D1       : {row['renko_d1']}")
    print(f"RENKO W1 LAST  : {row['renko_w1_last_brick_at']}")
    print(f"RENKO H1 LAST  : {row['renko_h1_last_brick_at']}")
    print(f"RENKO D1 LAST  : {row['renko_d1_last_brick_at']}")
    print(f"BULL trigger   : break_above_bear_vw0={row['break_above_bear_vw0']} sar_crossover={row['sar_bull_cross']}")
    print(f"BEAR trigger   : break_below_bull_vw0={row['break_below_bull_vw0']} sar_crossunder={row['sar_bear_cross']}")
    print(f"RENKO FRESH    : W1={row['weekly_fresh']} H1={row['hourly_fresh']} D1={row['daily_fresh']}")
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
        print(
            f"{pair:<8} SIG={row['signal_direction']:<4} "
            f"RENKO={row['renko_w1']}/{row['renko_h1']}/{row['renko_d1']} "
            f"FRESH={row['weekly_fresh']}/{row['hourly_fresh']}/{row['daily_fresh']}"
        )

    previous_state = load_tracking_state(TRACKING_PATH)
    active_rows = compute_persisted_signals(rows, previous_state)
    bull_rows = [r for r in active_rows if r.get("signal_direction") == "BULL"]
    bear_rows = [r for r in active_rows if r.get("signal_direction") == "BEAR"]

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
            suffix = " persisted" if row.get("persisted") else ""
            print(f"  {row['pair']:<8} {','.join(triggers) if triggers else '-'}{suffix}")

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
            suffix = " persisted" if row.get("persisted") else ""
            print(f"  {row['pair']:<8} {','.join(triggers) if triggers else '-'}{suffix}")

    save_tracking_state(TRACKING_PATH, active_rows)
    save_scan_snapshot(SCAN_OUTPUT_PATH, rows, active_rows)

    if args.telegram and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_telegram_message(build_telegram_message(active_rows))

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
