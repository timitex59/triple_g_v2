#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MiniTrend scanner

Rules:
- bullish mode:
  - weekly and daily current candle is above all 8 EMA
  - weekly and daily 8 EMA are perfectly aligned bullish
  - latest H1 IMP signal is bullish
- bearish mode: exact opposite
"""

import argparse
import json
import os
from typing import Dict, List

import pandas as pd
import requests

from sms_tim_V2 import EMA_LENGTHS, PAIRS, _parabolic_sar, fetch_tv_ohlc


def _current_pos(df: pd.DataFrame) -> int:
    # Always use the currently forming candle.
    if df is None or df.empty:
        return -1
    return -1


def _ema_values_at(df: pd.DataFrame, pos: int) -> List[float]:
    close = df["Close"]
    return [float(close.ewm(span=length, adjust=False).mean().iloc[pos]) for length in EMA_LENGTHS]


def _chg_cc_daily_current(df_d1: pd.DataFrame) -> float:
    if df_d1 is None or df_d1.empty or len(df_d1) < 2:
        return float("nan")
    curr = float(df_d1["Close"].iloc[-1])
    prev = float(df_d1["Close"].iloc[-2])
    if prev == 0.0:
        return float("nan")
    return (curr - prev) / prev * 100.0


def _ema_aligned(ema_vals: List[float], bias: str) -> bool:
    if bias == "bullish":
        return all(ema_vals[i] > ema_vals[i + 1] for i in range(len(ema_vals) - 1))
    return all(ema_vals[i] < ema_vals[i + 1] for i in range(len(ema_vals) - 1))


def _candle_side(open_v: float, close_v: float, bias: str) -> bool:
    if bias == "bullish":
        return close_v > open_v
    return close_v < open_v


def _frame_eval(df: pd.DataFrame, bias: str) -> Dict[str, object]:
    if df is None or df.empty or len(df) < max(EMA_LENGTHS):
        return {
            "ok": False,
            "candle_ok": False,
            "ema_aligned": False,
            "above_sar": False,
            "above_all_ema": False,
            "open": None,
            "close": None,
            "position": None,
        }

    pos = _current_pos(df)
    sar = _parabolic_sar(df)
    if sar is None or sar.empty or pd.isna(sar.iloc[pos]):
        return {
            "ok": False,
            "candle_ok": False,
            "ema_aligned": False,
            "above_sar": False,
            "above_all_ema": False,
            "open": None,
            "close": None,
            "position": int(pos),
        }

    low_v = float(df["Low"].iloc[pos])
    high_v = float(df["High"].iloc[pos])
    open_v = float(df["Open"].iloc[pos])
    close_v = float(df["Close"].iloc[pos])
    sar_v = float(sar.iloc[pos])
    ema_vals = _ema_values_at(df, pos)

    candle_ok = _candle_side(open_v, close_v, bias)
    ema_aligned = _ema_aligned(ema_vals, bias)
    above_sar = low_v > sar_v if bias == "bullish" else high_v < sar_v
    above_all_ema = all(low_v > ema for ema in ema_vals) if bias == "bullish" else all(high_v < ema for ema in ema_vals)
    if bias == "bullish":
        ok = (
            above_all_ema
            and ema_aligned
        )
    else:
        ok = (
            above_all_ema
            and ema_aligned
        )

    return {
        "ok": bool(ok),
        "candle_ok": bool(candle_ok),
        "ema_aligned": bool(ema_aligned),
        "above_sar": bool(above_sar),
        "above_all_ema": bool(above_all_ema),
        "open": open_v,
        "close": close_v,
        "position": int(pos),
    }


def _imp_state_h1(df_h1: pd.DataFrame) -> Dict[str, object]:
    """
    Rebuild IMP appearance logic from Pine on H1:
    - track latest bull/bear PSAR cross levels
    - IMP bear appears when close < last_bull_level after a bear cross cycle
    - IMP bull appears when close > last_bear_level after a bull cross cycle
    """
    out = {
        "last_imp_signal_h1": None,
        "last_imp_bars_ago_h1": None,
        "last_imp_level_h1": None,
        "last_imp_time_h1": None,
    }
    if df_h1 is None or df_h1.empty or len(df_h1) < 3:
        return out

    sar = _parabolic_sar(df_h1, start=0.1, increment=0.1, max_af=0.2)
    if sar is None or sar.empty:
        return out

    close = df_h1["Close"].astype(float).tolist()
    sar_vals = sar.astype(float).tolist()
    n = len(df_h1)

    last_bull_level = None
    last_bear_level = None
    tracked_bull_level = None
    tracked_bear_level = None
    tracked_bull_important = False
    tracked_bear_important = False

    last_imp_signal = None
    last_imp_idx = None
    last_imp_level = None

    for i in range(1, n):
        bull = close[i] > sar_vals[i] and close[i - 1] <= sar_vals[i - 1]
        bear = close[i] < sar_vals[i] and close[i - 1] >= sar_vals[i - 1]

        if bull:
            last_bull_level = sar_vals[i]
            tracked_bull_level = sar_vals[i]
            tracked_bull_important = False
        if bear:
            last_bear_level = sar_vals[i]
            tracked_bear_level = sar_vals[i]
            tracked_bear_important = False

        if (not tracked_bear_important) and (tracked_bear_level is not None) and (last_bull_level is not None) and (close[i] < last_bull_level):
            tracked_bear_important = True
            last_imp_signal = "BEAR"
            last_imp_idx = i
            last_imp_level = tracked_bear_level

        if (not tracked_bull_important) and (tracked_bull_level is not None) and (last_bear_level is not None) and (close[i] > last_bear_level):
            tracked_bull_important = True
            last_imp_signal = "BULL"
            last_imp_idx = i
            last_imp_level = tracked_bull_level

    if last_imp_signal is not None and last_imp_idx is not None:
        out["last_imp_signal_h1"] = last_imp_signal
        out["last_imp_bars_ago_h1"] = int((n - 1) - last_imp_idx)
        out["last_imp_level_h1"] = round(float(last_imp_level), 6) if last_imp_level is not None else None
        out["last_imp_time_h1"] = str(df_h1.index[last_imp_idx])
    return out


def _daily_sar_retracement_ok(df_d1: pd.DataFrame, imp_time_str: str, imp_signal: str) -> Dict[str, object]:
    out = {
        "ok": False,
        "daily_close_at_imp": None,
        "daily_sar_at_imp": None,
        "daily_bar_time_at_imp": None,
    }
    if df_d1 is None or df_d1.empty or not imp_time_str or imp_signal not in ("BULL", "BEAR"):
        return out

    try:
        imp_time = pd.Timestamp(imp_time_str)
        if imp_time.tzinfo is None:
            imp_time = imp_time.tz_localize("UTC")
        else:
            imp_time = imp_time.tz_convert("UTC")
    except Exception:
        return out

    sar_d1 = _parabolic_sar(df_d1, start=0.1, increment=0.1, max_af=0.2)
    if sar_d1 is None or sar_d1.empty:
        return out

    candidates = df_d1.index[df_d1.index <= imp_time]
    if len(candidates) == 0:
        return out
    dbar_ts = candidates[-1]
    pos = int(df_d1.index.get_indexer([dbar_ts])[0])
    if pos < 0:
        return out

    close_d = float(df_d1["Close"].iloc[pos])
    sar_d = float(sar_d1.iloc[pos])
    if pd.isna(close_d) or pd.isna(sar_d):
        return out

    # Confirm retracement context at IMP time:
    # - BEAR IMP requires price above daily SAR
    # - BULL IMP requires price below daily SAR
    ok = (imp_signal == "BEAR" and close_d > sar_d) or (imp_signal == "BULL" and close_d < sar_d)
    out["ok"] = bool(ok)
    out["daily_close_at_imp"] = round(close_d, 6)
    out["daily_sar_at_imp"] = round(sar_d, 6)
    out["daily_bar_time_at_imp"] = str(dbar_ts)
    return out


def analyze_symbol(symbol: str, bars: int) -> Dict:
    df_d1 = fetch_tv_ohlc(symbol=symbol, interval="1D", bars=bars)
    df_w1 = fetch_tv_ohlc(symbol=symbol, interval="1W", bars=bars)
    df_h1 = fetch_tv_ohlc(symbol=symbol, interval="60", bars=bars)
    if df_d1 is None or df_d1.empty:
        raise RuntimeError(f"No D1 data for {symbol}")
    if df_w1 is None or df_w1.empty:
        raise RuntimeError(f"No W1 data for {symbol}")
    if df_h1 is None or df_h1.empty:
        raise RuntimeError(f"No H1 data for {symbol}")
    chg_cc_daily = _chg_cc_daily_current(df_d1)

    bull_daily = _frame_eval(df_d1, "bullish")
    bull_weekly = _frame_eval(df_w1, "bullish")
    bear_daily = _frame_eval(df_d1, "bearish")
    bear_weekly = _frame_eval(df_w1, "bearish")
    imp_h1 = _imp_state_h1(df_h1)
    imp_signal = imp_h1.get("last_imp_signal_h1")
    retr_ok = _daily_sar_retracement_ok(
        df_d1=df_d1,
        imp_time_str=imp_h1.get("last_imp_time_h1"),
        imp_signal=imp_signal,
    )
    imp_bull_ok = (imp_signal == "BULL") and bool(retr_ok.get("ok"))
    imp_bear_ok = (imp_signal == "BEAR") and bool(retr_ok.get("ok"))

    return {
        "symbol": symbol,
        "pair": symbol.replace("OANDA:", ""),
        "bullish_daily_ok": bull_daily["ok"],
        "bullish_weekly_ok": bull_weekly["ok"],
        "bullish_imp_h1_ok": imp_bull_ok,
        "bullish_selected": bool(bull_daily["ok"] and bull_weekly["ok"] and imp_bull_ok),
        "bearish_daily_ok": bear_daily["ok"],
        "bearish_weekly_ok": bear_weekly["ok"],
        "bearish_imp_h1_ok": imp_bear_ok,
        "bearish_selected": bool(bear_daily["ok"] and bear_weekly["ok"] and imp_bear_ok),
        "last_imp_signal_h1": imp_h1.get("last_imp_signal_h1"),
        "last_imp_bars_ago_h1": imp_h1.get("last_imp_bars_ago_h1"),
        "last_imp_level_h1": imp_h1.get("last_imp_level_h1"),
        "last_imp_time_h1": imp_h1.get("last_imp_time_h1"),
        "daily_retracement_ok_at_imp": bool(retr_ok.get("ok")),
        "daily_close_at_imp": retr_ok.get("daily_close_at_imp"),
        "daily_sar_at_imp": retr_ok.get("daily_sar_at_imp"),
        "daily_bar_time_at_imp": retr_ok.get("daily_bar_time_at_imp"),
        "daily_candle_bullish": bool(bull_daily["candle_ok"]),
        "daily_candle_bearish": bool(bear_daily["candle_ok"]),
        "weekly_candle_bullish": bool(bull_weekly["candle_ok"]),
        "weekly_candle_bearish": bool(bear_weekly["candle_ok"]),
        "daily_ema_aligned_bullish": bool(bull_daily["ema_aligned"]),
        "daily_ema_aligned_bearish": bool(bear_daily["ema_aligned"]),
        "weekly_ema_aligned_bullish": bool(bull_weekly["ema_aligned"]),
        "weekly_ema_aligned_bearish": bool(bear_weekly["ema_aligned"]),
        "daily_above_sar_bullish": bool(bull_daily["above_sar"]),
        "daily_above_sar_bearish": bool(bear_daily["above_sar"]),
        "weekly_above_sar_bullish": bool(bull_weekly["above_sar"]),
        "weekly_above_sar_bearish": bool(bear_weekly["above_sar"]),
        "daily_above_ema_bullish": bool(bull_daily["above_all_ema"]),
        "daily_above_ema_bearish": bool(bear_daily["above_all_ema"]),
        "weekly_above_ema_bullish": bool(bull_weekly["above_all_ema"]),
        "weekly_above_ema_bearish": bool(bear_weekly["above_all_ema"]),
        "daily_open": bull_daily["open"],
        "daily_close": bull_daily["close"],
        "weekly_open": bull_weekly["open"],
        "weekly_close": bull_weekly["close"],
        "daily_bar_pos_used": bull_daily["position"],
        "weekly_bar_pos_used": bull_weekly["position"],
        "chg_cc_daily": None if pd.isna(chg_cc_daily) else round(float(chg_cc_daily), 2),
    }


def _telegram_lines(rows: List[Dict], bias: str) -> List[str]:
    lines = ["MINITREND"]
    to_send: List[Dict] = []

    if bias in ("both", "bullish"):
        to_send.extend(sorted([r for r in rows if r["bullish_selected"]], key=lambda r: r["pair"]))
    if bias in ("both", "bearish"):
        to_send.extend(sorted([r for r in rows if r["bearish_selected"]], key=lambda r: r["pair"]))

    if not to_send:
        lines.append("NO DEAL")
        return lines

    for row in to_send:
        pair = row["pair"]
        chg = row.get("chg_cc_daily")
        chg_txt = "N/A" if chg is None else f"{float(chg):+0.2f}%"
        dot = "🟢" if row.get("bullish_selected") else "🔴"
        lines.append(f"{dot} {pair} ({chg_txt})")
    return lines


def send_telegram_message(lines: List[str]) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": "\n".join(lines)}, timeout=10)
        return True
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description="MiniTrend D1+W1 scanner")
    parser.add_argument(
        "--bias",
        choices=["both", "bullish", "bearish"],
        default="both",
        help="Filter output side. Default: both.",
    )
    parser.add_argument("--bars", type=int, default=300, help="Bars to request per timeframe.")
    parser.add_argument("--debug-all", action="store_true", help="Print all pairs with per-timeframe flags.")
    args = parser.parse_args()

    rows: List[Dict] = []
    errors: List[Dict] = []

    for pair in PAIRS:
        symbol = f"OANDA:{pair.replace('=X', '')}"
        try:
            rows.append(analyze_symbol(symbol=symbol, bars=args.bars))
        except Exception as exc:
            errors.append({"pair": pair, "symbol": symbol, "error": str(exc)})

    bullish_selected = sorted([r for r in rows if r["bullish_selected"]], key=lambda r: r["pair"])
    bearish_selected = sorted([r for r in rows if r["bearish_selected"]], key=lambda r: r["pair"])

    if args.bias in ("both", "bullish"):
        print("=== MINITREND BULLISH SELECTED ===")
        print(json.dumps(bullish_selected, indent=2))
    if args.bias in ("both", "bearish"):
        print("=== MINITREND BEARISH SELECTED ===")
        print(json.dumps(bearish_selected, indent=2))
    print("=== SUMMARY ===")
    selected_count = (
        len(bullish_selected)
        if args.bias == "bullish"
        else len(bearish_selected)
        if args.bias == "bearish"
        else (len(bullish_selected) + len(bearish_selected))
    )
    print(
        json.dumps(
            {
                "bias": args.bias,
                "total_pairs": len(PAIRS),
                "computed": len(rows),
                "bullish_selected": len(bullish_selected),
                "bearish_selected": len(bearish_selected),
                "selected_count": selected_count,
                "errors": len(errors),
            },
            indent=2,
        )
    )

    if args.debug_all:
        all_rows = sorted(rows, key=lambda r: r["pair"])
        print("=== DEBUG ALL ===")
        print(json.dumps(all_rows, indent=2))

    if errors:
        print("=== ERRORS ===")
        print(json.dumps(errors, indent=2))

    telegram_lines = _telegram_lines(rows, args.bias)
    sent = send_telegram_message(telegram_lines)
    print("=== TELEGRAM ===")
    print("sent" if sent else "not_sent (missing token/chat_id or request failed)")
    if not sent:
        print(json.dumps(telegram_lines, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
