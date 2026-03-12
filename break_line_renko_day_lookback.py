#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Replay the break_line + Renko strategy across a single day and report when the
signal was true between 00:00 and 23:59.
"""

import argparse
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pandas as pd

from break_line import compute_break_line_state, fetch_tv_ohlc
from break_line_renko_strategy import (
    PAIRS_29,
    compute_renko_state,
    price_crosses_above_level,
    price_crosses_below_level,
    psar_cross_signal,
    renko_brick_in_week,
)


PARIS_TZ = ZoneInfo("Europe/Paris")


def parse_args():
    parser = argparse.ArgumentParser(description="Look back within one day for break_line + Renko signals")
    parser.add_argument("--pair", default=None, help="Pair to analyze, e.g. EURUSD")
    parser.add_argument("--scan29", action="store_true", help="Analyze all 29 pairs")
    parser.add_argument("--date", required=True, help="Day to replay in YYYY-MM-DD format, Paris timezone")
    parser.add_argument("--tf", default="60", help="Signal timeframe, e.g. 60, 15, 5")
    parser.add_argument("--renko-length", type=int, default=14, help="ATR length for Renko box size")
    parser.add_argument("--signal-candles", type=int, default=1500, help="Number of signal timeframe candles fetched")
    parser.add_argument("--daily-candles", type=int, default=500, help="Number of daily candles fetched")
    parser.add_argument("--hourly-candles", type=int, default=900, help="Number of hourly candles fetched for Renko H1")
    parser.add_argument("--show-all-bars", action="store_true", help="Print every bar of the selected day, not only true signals")
    return parser.parse_args()


def parse_day(day_str: str) -> tuple[datetime, datetime]:
    target_day = date.fromisoformat(day_str)
    start_local = datetime.combine(target_day, time(0, 0), tzinfo=PARIS_TZ)
    end_local = datetime.combine(target_day, time(23, 59, 59), tzinfo=PARIS_TZ)
    return start_local, end_local


def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.index = out.index.tz_convert(PARIS_TZ)
    return out.sort_index()


def intraday_daily_view(signal_slice: pd.DataFrame) -> pd.DataFrame:
    if signal_slice.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close"])

    daily = signal_slice.resample("1D").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
        }
    )
    return daily.dropna()


def intraday_hourly_view(signal_slice: pd.DataFrame) -> pd.DataFrame:
    if signal_slice.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close"])

    hourly = signal_slice.resample("1h").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
        }
    )
    return hourly.dropna()


def combine_higher_tf(base_df: pd.DataFrame, intraday_df: pd.DataFrame) -> pd.DataFrame:
    if base_df is None or base_df.empty:
        return intraday_df.copy()
    if intraday_df is None or intraday_df.empty:
        return prepare_df(base_df)

    base = prepare_df(base_df)
    merged = pd.concat([base, intraday_df])
    merged = merged[~merged.index.duplicated(keep="last")]
    return merged.sort_index()


def previous_official_daily_close(daily_df: pd.DataFrame, start_local: datetime) -> float | None:
    if daily_df is None or daily_df.empty:
        return None

    official_daily = prepare_df(daily_df)
    previous_days = official_daily[official_daily.index < start_local]
    if previous_days.empty:
        return None
    return float(previous_days["close"].iloc[-1])


def live_daily_chg_cc(prev_daily_close: float | None, current_close: float | None) -> float | None:
    if prev_daily_close is None or current_close is None or prev_daily_close == 0:
        return None
    return ((current_close - prev_daily_close) / prev_daily_close) * 100.0


def evaluate_bar(
    signal_slice: pd.DataFrame,
    daily_slice: pd.DataFrame,
    hourly_slice: pd.DataFrame,
    prev_daily_close: float | None,
    ref_dt: datetime,
    renko_length: int,
) -> dict:
    break_state = compute_break_line_state(signal_slice)
    renko_d1 = compute_renko_state(daily_slice, renko_length)
    renko_h1 = compute_renko_state(hourly_slice, renko_length)
    chg = live_daily_chg_cc(prev_daily_close, break_state.close)

    break_above_bear_vw0 = price_crosses_above_level(signal_slice, break_state.bear_vw0)
    break_below_bull_vw0 = price_crosses_below_level(signal_slice, break_state.bull_vw0)
    sar_bull_cross, sar_bear_cross = psar_cross_signal(signal_slice)

    hourly_fresh = renko_brick_in_week(renko_h1.last_brick_at, ref_dt)
    daily_fresh = renko_brick_in_week(renko_d1.last_brick_at, ref_dt)
    freshness_ok = hourly_fresh and daily_fresh

    bull_filters_ok = renko_h1.direction == 1 and renko_d1.direction == 1 and freshness_ok
    bear_filters_ok = renko_h1.direction == -1 and renko_d1.direction == -1 and freshness_ok
    bull_signal = bull_filters_ok and (break_above_bear_vw0 or sar_bull_cross)
    bear_signal = bear_filters_ok and (break_below_bull_vw0 or sar_bear_cross)

    triggers = []
    if break_above_bear_vw0:
        triggers.append("break_above_bear_vw0")
    if sar_bull_cross:
        triggers.append("sar_crossover")
    if break_below_bull_vw0:
        triggers.append("break_below_bull_vw0")
    if sar_bear_cross:
        triggers.append("sar_crossunder")

    return {
        "close": break_state.close,
        "bear_vw0": break_state.bear_vw0,
        "bull_vw0": break_state.bull_vw0,
        "renko_d1": renko_d1.color,
        "renko_h1": renko_h1.color,
        "chg_cc_d1": chg,
        "renko_h1_last_brick_at": renko_h1.last_brick_at,
        "renko_d1_last_brick_at": renko_d1.last_brick_at,
        "hourly_fresh": hourly_fresh,
        "daily_fresh": daily_fresh,
        "bull_filters_ok": bull_filters_ok,
        "bear_filters_ok": bear_filters_ok,
        "bull_signal": bull_signal,
        "bear_signal": bear_signal,
        "signal_direction": "BULL" if bull_signal else "BEAR" if bear_signal else "NONE",
        "triggers": triggers,
    }


def lookback_day(args) -> int:
    pair = args.pair.upper()
    if pair not in PAIRS_29:
        print(f"{pair}: unsupported pair")
        return 1

    start_local, end_local = parse_day(args.date)
    symbol = f"OANDA:{pair}"

    signal_df = fetch_tv_ohlc(symbol, args.tf, args.signal_candles)
    daily_df = fetch_tv_ohlc(symbol, "D", args.daily_candles)
    hourly_df = fetch_tv_ohlc(symbol, "60", args.hourly_candles)
    if signal_df is None or daily_df is None or hourly_df is None:
        print(f"{pair}: data unavailable")
        return 1

    signal_df = prepare_df(signal_df)
    signal_day = signal_df[(signal_df.index >= start_local) & (signal_df.index <= end_local)]
    if signal_day.empty:
        print(f"{pair}: no {args.tf} bars found on {args.date} (Paris time)")
        return 1

    combined_daily = combine_higher_tf(daily_df, intraday_daily_view(signal_df))
    combined_hourly = combine_higher_tf(hourly_df, intraday_hourly_view(signal_df))
    prev_daily_close = previous_official_daily_close(daily_df, start_local)

    print(f"PAIR   : {pair}")
    print(f"DATE   : {args.date}")
    print(f"TF     : {args.tf}")
    print(f"BARS   : {len(signal_day)}")
    print(f"PREV D1 CLOSE : {prev_daily_close}")
    print("")

    true_rows = []
    for ts, _ in signal_day.iterrows():
        signal_slice = signal_df.loc[:ts]
        daily_slice = combined_daily.loc[:ts]
        hourly_slice = combined_hourly.loc[:ts]
        result = evaluate_bar(signal_slice, daily_slice, hourly_slice, prev_daily_close, ts.to_pydatetime(), args.renko_length)
        row = {
            "ts": ts,
            **result,
        }
        if args.show_all_bars or row["signal_direction"] != "NONE":
            chg_txt = "N/A" if row["chg_cc_d1"] is None else f"{row['chg_cc_d1']:+.2f}%"
            trigger_txt = ",".join(row["triggers"]) if row["triggers"] else "-"
            print(
                f"{ts.strftime('%Y-%m-%d %H:%M')} "
                f"SIG={row['signal_direction']:<4} "
                f"RENKO={row['renko_h1']}/{row['renko_d1']} "
                f"FRESH={row['hourly_fresh']}/{row['daily_fresh']} "
                f"CHG={chg_txt} "
                f"TRIG={trigger_txt}"
            )
        if row["signal_direction"] != "NONE":
            true_rows.append(row)

    print("")
    print("SIGNAL WINDOWS")
    if not true_rows:
        print("None")
        return 0

    for row in true_rows:
        trigger_txt = ",".join(row["triggers"]) if row["triggers"] else "-"
        print(f"{row['ts'].strftime('%H:%M')} {row['signal_direction']} {trigger_txt}")
    return 0


def lookback_all_pairs(args) -> int:
    any_signal = False
    for pair in PAIRS_29:
        pair_args = argparse.Namespace(**vars(args))
        pair_args.pair = pair
        print(f"\n===== {pair} =====")
        result = lookback_day(pair_args)
        if result == 0:
            any_signal = True
    return 0 if any_signal else 0


def main() -> int:
    args = parse_args()
    if args.scan29:
        return lookback_all_pairs(args)
    if not args.pair:
        raise SystemExit("--pair or --scan29 is required")
    return lookback_day(args)


if __name__ == "__main__":
    raise SystemExit(main())
