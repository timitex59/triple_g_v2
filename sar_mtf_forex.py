#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sar_mtf_forex.py
SAR + Ichimoku MTF signal scanner for Forex pairs.
Run hourly (GitHub Actions or local cron).

Logic mirrors sar_mtf.pine:
  - BULL state  : price above Ichimoku cloud AND above SAR (on W and D)
  - BEAR state  : price below cloud AND below SAR (on W and D)
  - LONG signal : W+D both BULL + crossover(close, bear_vw0) on latest H1 bar
  - SHORT signal: W+D both BEAR + crossunder(close, bull_vw0) on latest H1 bar
"""

import os
import sys
import argparse
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

import pytz
import requests as http_requests
from dotenv import load_dotenv

load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

from renko_nasdaq import fetch_tv_ohlc

# ─── Constants ────────────────────────────────────────────────────────────────

SAR_START     = 0.1
SAR_INCREMENT = 0.1
SAR_MAXIMUM   = 0.2

CONV_LEN   = 9
BASE_LEN   = 26
SPANB_LEN  = 52

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURNZD", "EURCHF",
    "GBPAUD", "GBPCAD", "GBPNZD", "GBPCHF",
    "AUDNZD", "AUDCAD", "AUDCHF",
    "NZDCAD", "NZDCHF",
    "CADCHF",
]

INDEX_TO_CCY = {
    "DXY": "USD", "EXY": "EUR", "BXY": "GBP", "JXY": "JPY",
    "SXY": "CHF", "CXY": "CAD", "AXY": "AUD", "ZXY": "NZD",
}

INDEX_SYMBOLS = {
    "DXY": "TVC:DXY", "EXY": "TVC:EXY", "BXY": "TVC:BXY", "JXY": "TVC:JXY",
    "SXY": "TVC:SXY", "CXY": "TVC:CXY", "AXY": "TVC:AXY", "ZXY": "TVC:ZXY",
}


# ─── Indicators ───────────────────────────────────────────────────────────────

def compute_sar(bars: list[dict],
                start: float = SAR_START,
                increment: float = SAR_INCREMENT,
                maximum: float = SAR_MAXIMUM) -> list[float]:
    """Parabolic SAR — mirrors TradingView ta.sar()."""
    highs  = [float(b["high"])  for b in bars]
    lows   = [float(b["low"])   for b in bars]
    closes = [float(b["close"]) for b in bars]
    n = len(bars)
    sar = [float("nan")] * n
    if n < 2:
        return sar

    bull  = closes[1] >= closes[0]
    af    = start
    ep    = highs[1] if bull else lows[1]
    sar[0] = lows[0]  if bull else highs[0]
    sar[1] = sar[0]

    for i in range(2, n):
        prev_sar = sar[i - 1]
        if bull:
            new_sar = prev_sar + af * (ep - prev_sar)
            new_sar = min(new_sar, lows[i - 1], lows[i - 2])
            if lows[i] < new_sar:
                bull    = False
                new_sar = ep
                ep      = lows[i]
                af      = start
            else:
                if highs[i] > ep:
                    ep  = highs[i]
                    af  = min(af + increment, maximum)
        else:
            new_sar = prev_sar + af * (ep - prev_sar)
            new_sar = max(new_sar, highs[i - 1], highs[i - 2])
            if highs[i] > new_sar:
                bull    = True
                new_sar = ep
                ep      = highs[i]
                af      = start
            else:
                if lows[i] < ep:
                    ep  = lows[i]
                    af  = min(af + increment, maximum)
        sar[i] = new_sar
    return sar


def compute_ichimoku_cloud(bars: list[dict],
                           conv_len: int = CONV_LEN,
                           base_len: int = BASE_LEN,
                           spanb_len: int = SPANB_LEN) -> tuple[list[float], list[float]]:
    """Returns (span_a, span_b) — current values (no displacement offset)."""
    highs  = [float(b["high"])  for b in bars]
    lows   = [float(b["low"])   for b in bars]
    n = len(bars)

    def donchian(i, length):
        start = max(0, i - length + 1)
        return (max(highs[start:i+1]) + min(lows[start:i+1])) / 2

    span_a = [float("nan")] * n
    span_b = [float("nan")] * n
    for i in range(n):
        conv = donchian(i, conv_len)
        base = donchian(i, base_len)
        span_a[i] = (conv + base) / 2
        span_b[i] = donchian(i, spanb_len)
    return span_a, span_b


def cloud_state(bars: list[dict]) -> int:
    """1 = price above cloud, -1 = below, 0 = inside.
    Mirrors Pine f_cloud_state: compare close against current leadLine1/leadLine2
    (no displacement — displacement only affects visual plot, not MTF security values)."""
    if not bars:
        return 0
    span_a, span_b = compute_ichimoku_cloud(bars)
    close = float(bars[-1]["close"])
    top = max(span_a[-1], span_b[-1])
    bot = min(span_a[-1], span_b[-1])
    if close > top:
        return 1
    if close < bot:
        return -1
    return 0


def sar_state(bars: list[dict]) -> int:
    """1 = price above SAR, -1 = below."""
    if not bars:
        return 0
    sar = compute_sar(bars)
    close = float(bars[-1]["close"])
    s = sar[-1]
    if float("nan") == s or s != s:
        return 0
    return 1 if close > s else -1


def tf_state(bars: list[dict]) -> int:
    """BULL=1 if cloud+SAR both bull, BEAR=-1 if both bear, else 0."""
    c = cloud_state(bars)
    s = sar_state(bars)
    if c == 1 and s == 1:
        return 1
    if c == -1 and s == -1:
        return -1
    return 0


def valuewhen_series(condition: list[bool], values: list[float]) -> list[float]:
    """Returns a series: at each bar i, the value at the most recent True in condition up to i."""
    n = len(condition)
    result = [float("nan")] * n
    last = float("nan")
    for i in range(n):
        if condition[i]:
            last = values[i]
        result[i] = last
    return result


LOOKBACK_HOURS = 24


# ─── Scan ─────────────────────────────────────────────────────────────────────

def scan_pair(pair: str, debug: bool = False) -> dict:
    symbol = f"OANDA:{pair}"

    bars_h1 = fetch_tv_ohlc(symbol, "60", 300, session="extended")
    bars_d  = fetch_tv_ohlc(symbol, "D",  200, session="extended")
    bars_w  = fetch_tv_ohlc(symbol, "W",  200, session="extended")

    if not bars_h1 or not bars_d or not bars_w:
        return {"pair": pair, "signal": None, "past_signals": [], "status": "no data", "global_state": 0}

    state_d = tf_state(bars_d)
    state_w = tf_state(bars_w)

    global_state = 0
    if state_w == 1 and state_d == 1:
        global_state = 1
    elif state_w == -1 and state_d == -1:
        global_state = -1

    if global_state == 0:
        label_w = "BULL" if state_w == 1 else "BEAR" if state_w == -1 else "MIX"
        label_d = "BULL" if state_d == 1 else "BEAR" if state_d == -1 else "MIX"
        return {"pair": pair, "signal": None, "past_signals": [], "status": f"MIXED W={label_w} D={label_d}", "global_state": 0}

    # Full H1 series
    sar_h1    = compute_sar(bars_h1)
    closes_h1 = [float(b["close"]) for b in bars_h1]
    n = len(bars_h1)

    cross_over  = [i > 0 and closes_h1[i-1] <= sar_h1[i-1] and closes_h1[i] > sar_h1[i] for i in range(n)]
    cross_under = [i > 0 and closes_h1[i-1] >= sar_h1[i-1] and closes_h1[i] < sar_h1[i] for i in range(n)]

    # Compute valuewhen as a full series (needed for retroactive crossover detection)
    bull_vw0_series = valuewhen_series(cross_over,  sar_h1)
    bear_vw0_series = valuewhen_series(cross_under, sar_h1)

    # Detect signal at each bar i using bar i-1 and i
    paris_tz = pytz.timezone("Europe/Paris")
    past_signals = []
    signal_now   = None

    for i in range(1, n):
        bv = bull_vw0_series[i]
        bev = bear_vw0_series[i]
        c_prev = closes_h1[i - 1]
        c_now  = closes_h1[i]

        sig = None
        if global_state == 1 and bev == bev:
            if c_prev <= bev and c_now > bev:
                sig = "LONG"
        if global_state == -1 and bv == bv:
            if c_prev >= bv and c_now < bv:
                sig = "SHORT"

        if sig is None:
            continue

        ts_raw = bars_h1[i].get("time") or bars_h1[i].get("timestamp")
        if ts_raw:
            ts_dt = datetime.fromtimestamp(int(ts_raw), tz=paris_tz)
            ts_str = ts_dt.strftime("%H:%M")
        else:
            ts_str = "?"

        if i >= n - LOOKBACK_HOURS:
            entry = {"signal": sig, "time": ts_str, "bar_idx": i}
            if i == n - 1:
                signal_now = sig
            past_signals.append(entry)

    close_now  = closes_h1[-1]
    bull_vw0   = bull_vw0_series[-1]
    bear_vw0   = bear_vw0_series[-1]

    direction = "BULL" if global_state == 1 else "BEAR"
    level     = bear_vw0 if global_state == 1 else bull_vw0
    level_lbl = "bear_vw0" if global_state == 1 else "bull_vw0"
    status = f"{direction} | close={close_now:.5f} | {level_lbl}={level:.5f}"
    if signal_now:
        status += f" → {signal_now} ✅"
    elif past_signals:
        last = past_signals[-1]
        status += f" → passé {last['signal']} @ {last['time']}"
    else:
        status += " → no signal"

    return {
        "pair":         pair,
        "signal":       signal_now,
        "past_signals": past_signals,
        "status":       status,
        "close":        close_now,
        "global_state": global_state,
        "state_w":      state_w,
        "state_d":      state_d,
        "bull_vw0":     bull_vw0,
        "bear_vw0":     bear_vw0,
    }


# ─── Telegram ─────────────────────────────────────────────────────────────────

def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  [Telegram] token/chat_id manquant")
        return False
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = http_requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        return bool(resp.json().get("ok", False))
    except Exception as e:
        print(f"  [Telegram] erreur: {e}")
        return False


# ─── Main ─────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="SAR MTF Forex signal scanner")
    p.add_argument("--no-telegram", action="store_true")
    p.add_argument("--debug",       action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    paris_tz = pytz.timezone("Europe/Paris")
    now_str  = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M")

    print(f"Scanning {len(PAIRS)} pairs...\n")

    all_results = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(scan_pair, pair, args.debug): pair for pair in PAIRS}
        for future, pair in futures.items():
            try:
                result = future.result(timeout=90)
            except FuturesTimeoutError:
                future.cancel()
                print(f"  {pair}: timeout — skipped")
                continue
            except Exception as e:
                print(f"  {pair}: error — {e}")
                continue
            if result:
                all_results.append(result)

    # ── Terminal detail ────────────────────────────────────────────────────────
    all_results.sort(key=lambda r: r["pair"])
    print("\n── Détail des paires ─────────────────────────────────────────────────")
    for r in all_results:
        print(f"  {r['pair']:<10} {r['status']}")
    print()

    # Collect all signals from 07:00 to 23:00, sorted by time
    all_day_signals = []
    for r in all_results:
        for ps in r.get("past_signals", []):
            hour = int(ps["time"].split(":")[0]) if ps["time"] != "?" else -1
            if 7 <= hour <= 23:
                all_day_signals.append({"pair": r["pair"], **ps})
    all_day_signals.sort(key=lambda x: x["bar_idx"])

    lines = ["📡 SAR MTF FOREX", ""]

    if all_day_signals:
        for s in all_day_signals:
            emoji = "🟢" if s["signal"] == "LONG" else "🔴"
            lines.append(f"  {emoji} {s['pair']:<10} @ {s['time']}")
    else:
        lines.append("  (aucun signal)")

    lines += ["", f"⏰ {now_str} Paris"]
    msg = "\n".join(lines)

    print(f"{msg}\n")

    if not args.no_telegram:
        send_telegram(msg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
