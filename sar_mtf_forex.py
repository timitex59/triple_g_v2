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


DISPLACEMENT = 26

def cloud_state(bars: list[dict]) -> int:
    """1 = price above cloud, -1 = below, 0 = inside.
    Uses displaced cloud (offset=25) matching TradingView visual."""
    if not bars:
        return 0
    span_a, span_b = compute_ichimoku_cloud(bars)
    close = float(bars[-1]["close"])
    # The visual cloud at current bar = values calculated DISPLACEMENT-1 bars ago
    idx = -(DISPLACEMENT - 1) - 1  # = -26
    if len(span_a) < abs(idx):
        return 0
    top = max(span_a[idx], span_b[idx])
    bot = min(span_a[idx], span_b[idx])
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


def valuewhen(condition: list[bool], values: list[float], occurrence: int = 0) -> float:
    """Pine Script ta.valuewhen equivalent."""
    count = 0
    for i in range(len(condition) - 1, -1, -1):
        if condition[i]:
            if count == occurrence:
                return values[i]
            count += 1
    return float("nan")


# ─── Scan ─────────────────────────────────────────────────────────────────────

def scan_pair(pair: str, debug: bool = False) -> dict:
    symbol = f"OANDA:{pair}"

    bars_h1 = fetch_tv_ohlc(symbol, "60", 300)
    bars_d  = fetch_tv_ohlc(symbol, "D",  200)
    bars_w  = fetch_tv_ohlc(symbol, "W",  200)

    if not bars_h1 or not bars_d or not bars_w:
        return {"pair": pair, "signal": None, "status": "no data", "global_state": 0}

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
        return {"pair": pair, "signal": None, "status": f"MIXED W={label_w} D={label_d}", "global_state": 0}

    # Compute SAR on H1
    sar_h1    = compute_sar(bars_h1)
    closes_h1 = [float(b["close"]) for b in bars_h1]
    opens_h1  = [float(b["open"])  for b in bars_h1]

    cross_over  = [i > 0 and closes_h1[i - 1] <= sar_h1[i - 1] and closes_h1[i] > sar_h1[i]
                   for i in range(len(bars_h1))]
    cross_under = [i > 0 and closes_h1[i - 1] >= sar_h1[i - 1] and closes_h1[i] < sar_h1[i]
                   for i in range(len(bars_h1))]

    bull_vw0 = valuewhen(cross_over,  sar_h1, 0)
    bear_vw0 = valuewhen(cross_under, sar_h1, 0)

    open_prev  = opens_h1[-2]
    close_now  = closes_h1[-1]

    signal = None
    # LONG : bougie précédente ouvre sous bear_vw0, bougie actuelle ferme au-dessus
    if global_state == 1 and bear_vw0 == bear_vw0:
        if open_prev <= bear_vw0 and close_now > bear_vw0:
            signal = "LONG"
    # SHORT : bougie précédente ouvre au-dessus de bull_vw0, bougie actuelle ferme en-dessous
    if global_state == -1 and bull_vw0 == bull_vw0:
        if open_prev >= bull_vw0 and close_now < bull_vw0:
            signal = "SHORT"

    direction = "BULL" if global_state == 1 else "BEAR"
    level     = bear_vw0 if global_state == 1 else bull_vw0
    level_lbl = "bear_vw0" if global_state == 1 else "bull_vw0"
    status = f"{direction} | close={close_now:.5f} | {level_lbl}={level:.5f} | open[-2]={open_prev:.5f}"
    if signal:
        status += f" → {signal} ✅"
    else:
        status += " → no crossover"

    return {
        "pair":         pair,
        "signal":       signal,
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
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(scan_pair, pair, args.debug): pair for pair in PAIRS}
        for future, pair in futures.items():
            try:
                result = future.result(timeout=20)
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

    signals = [r for r in all_results if r["signal"]]
    longs   = [s for s in signals if s["signal"] == "LONG"]
    shorts  = [s for s in signals if s["signal"] == "SHORT"]

    lines = ["📡 SAR MTF FOREX", ""]

    if longs:
        lines.append("🟢 LONG")
        for s in longs:
            lines.append(f"  {s['pair']:<10} close={s['close']:.5f}")
    else:
        lines.append("🟢 LONG\n  (aucun)")

    lines.append("")

    if shorts:
        lines.append("🔴 SHORT")
        for s in shorts:
            lines.append(f"  {s['pair']:<10} close={s['close']:.5f}")
    else:
        lines.append("🔴 SHORT\n  (aucun)")

    lines += ["", f"⏰ {now_str} Paris"]
    msg = "\n".join(lines)

    print(f"{msg}\n")

    if not args.no_telegram:
        send_telegram(msg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
