#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sarenki_V1_forex.py
Python mirror of sarenki_V1.pine for 28 Forex pairs.
Scans hourly and sends Telegram alerts.

Signal conditions:
  TREND = BULL  → State + Momentum + MTF all BULL
  TREND = BEAR  → State + Momentum + MTF all BEAR
  LONG signal   → TREND BULL + crossover(close, bear_vw0) on last confirmed H1 bar
  SHORT signal  → TREND BEAR + crossunder(close, bull_vw0) on last confirmed H1 bar
"""

import os
import sys
import json
import math
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

from renko_nasdaq import fetch_tv_ohlc, _fetch_renko_with_fallback

# ─── Constants ────────────────────────────────────────────────────────────────

SAR_START     = 0.1
SAR_INCREMENT = 0.1
SAR_MAXIMUM   = 0.2

CONV_LEN     = 9
BASE_LEN     = 26
SPANB_LEN    = 52
DISPLACEMENT = 26

ROC_LEN_H1 = 23
ROC_LEN_D1 = 5

W_W1 = 1.0
W_D1 = 0.75
W_H4 = 0.25
SCORE_BULL_THRESHOLD =  0.01
SCORE_BEAR_THRESHOLD = -0.01

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURNZD", "EURCHF",
    "GBPAUD", "GBPCAD", "GBPNZD", "GBPCHF",
    "AUDNZD", "AUDCAD", "AUDCHF",
    "NZDCAD", "NZDCHF",
    "CADCHF",
]

NAN = float("nan")

def isnan(v):
    return v != v


# ─── Indicators ───────────────────────────────────────────────────────────────

def compute_sar(bars, start=SAR_START, increment=SAR_INCREMENT, maximum=SAR_MAXIMUM):
    highs  = [float(b["high"])  for b in bars]
    lows   = [float(b["low"])   for b in bars]
    closes = [float(b["close"]) for b in bars]
    n = len(bars)
    sar = [NAN] * n
    if n < 2:
        return sar
    bull = closes[1] >= closes[0]
    af   = start
    ep   = highs[1] if bull else lows[1]
    sar[0] = lows[0] if bull else highs[0]
    sar[1] = sar[0]
    for i in range(2, n):
        prev_sar = sar[i - 1]
        if bull:
            new_sar = prev_sar + af * (ep - prev_sar)
            new_sar = min(new_sar, lows[i - 1], lows[i - 2])
            if lows[i] < new_sar:
                bull = False; new_sar = ep; ep = lows[i]; af = start
            else:
                if highs[i] > ep:
                    ep = highs[i]; af = min(af + increment, maximum)
        else:
            new_sar = prev_sar + af * (ep - prev_sar)
            new_sar = max(new_sar, highs[i - 1], highs[i - 2])
            if highs[i] > new_sar:
                bull = True; new_sar = ep; ep = highs[i]; af = start
            else:
                if lows[i] < ep:
                    ep = lows[i]; af = min(af + increment, maximum)
        sar[i] = new_sar
    return sar


def compute_ichimoku(bars):
    """Returns (span_a, span_b) lists — no displacement applied."""
    highs = [float(b["high"]) for b in bars]
    lows  = [float(b["low"])  for b in bars]
    n = len(bars)

    def donchian(i, length):
        s = max(0, i - length + 1)
        return (max(highs[s:i+1]) + min(lows[s:i+1])) / 2

    span_a = [NAN] * n
    span_b = [NAN] * n
    for i in range(n):
        conv    = donchian(i, CONV_LEN)
        base    = donchian(i, BASE_LEN)
        span_a[i] = (conv + base) / 2
        span_b[i] = donchian(i, SPANB_LEN)
    return span_a, span_b


def cloud_state_displaced(bars):
    """Price vs visual cloud (displacement-1 bars back)."""
    span_a, span_b = compute_ichimoku(bars)
    idx = -(DISPLACEMENT - 1) - 1  # = -26
    if len(span_a) < abs(idx):
        return 0
    top = max(span_a[idx], span_b[idx])
    bot = min(span_a[idx], span_b[idx])
    close = float(bars[-1]["close"])
    return 1 if close > top else -1 if close < bot else 0


def sar_state(bars):
    sar = compute_sar(bars)
    close = float(bars[-1]["close"])
    s = sar[-1]
    if isnan(s):
        return 0
    return 1 if close > s else -1


def valuewhen(condition, values, occurrence=0):
    count = 0
    for i in range(len(condition) - 1, -1, -1):
        if condition[i]:
            if count == occurrence:
                return values[i]
            count += 1
    return NAN


def sign(v):
    if v is None or isnan(v):
        return 0
    return 1 if v > 0 else -1 if v < 0 else 0


def roc(closes, length):
    if len(closes) <= length:
        return None
    prev = closes[-length - 1]
    if prev == 0:
        return None
    return (closes[-1] - prev) / abs(prev) * 100


def chg(closes):
    if len(closes) < 2 or closes[-2] == 0:
        return None
    return (closes[-1] - closes[-2]) / closes[-2] * 100


# ─── Component states ─────────────────────────────────────────────────────────

def renko_state(symbol, tf, current_close, atr_length=14, debug=False):
    bricks = _fetch_renko_with_fallback(symbol, tf, atr_length, 50, debug)
    if not bricks:
        return 0
    last   = bricks[-1]
    r_open = float(last["open"])
    r_cls  = float(last["close"])
    r_hi   = max(r_open, r_cls)
    r_lo   = min(r_open, r_cls)
    return 1 if current_close > r_hi else -1 if current_close < r_lo else 0


def momentum_state(bars_h1):
    sar    = compute_sar(bars_h1)
    closes = [float(b["close"]) for b in bars_h1]

    cross_over  = [i > 0 and closes[i-1] <= sar[i-1] and closes[i] > sar[i] for i in range(len(bars_h1))]
    cross_under = [i > 0 and closes[i-1] >= sar[i-1] and closes[i] < sar[i] for i in range(len(bars_h1))]

    bull_vw0      = valuewhen(cross_over,  sar, 0)
    bull_vw0_prev = valuewhen(cross_over,  sar, 1)
    bear_vw0      = valuewhen(cross_under, sar, 0)
    bear_vw0_prev = valuewhen(cross_under, sar, 1)

    close_now = closes[-1]

    mom_bull_core = (
        (not isnan(bull_vw0) and not isnan(bull_vw0_prev) and bull_vw0 > bull_vw0_prev) or
        (not isnan(bull_vw0) and not isnan(bull_vw0_prev) and bull_vw0 < bull_vw0_prev
         and not isnan(bear_vw0) and close_now > bear_vw0)
    )
    mom_bull = mom_bull_core and not isnan(bull_vw0) and close_now >= bull_vw0

    mom_bear_core = (
        (not isnan(bear_vw0) and not isnan(bear_vw0_prev) and bear_vw0 < bear_vw0_prev) or
        (not isnan(bear_vw0) and not isnan(bear_vw0_prev) and bear_vw0 > bear_vw0_prev
         and not isnan(bull_vw0) and close_now < bull_vw0)
    )
    mom_bear = mom_bear_core and not isnan(bear_vw0) and close_now <= bear_vw0

    return 1 if mom_bull else -1 if mom_bear else 0


def mtf_state(bars_tf):
    """SAR + displaced cloud on a given TF."""
    c = cloud_state_displaced(bars_tf)
    s = sar_state(bars_tf)
    if c == 1 and s == 1:
        return 1
    if c == -1 and s == -1:
        return -1
    return 0


def global_state(symbol, bars_h1, bars_d, current_close, debug=False):
    """
    Renko W/D/H4 weighted score + Ichimoku H1/D cloud + ROC + CHG
    filtered by SAR Daily.
    """
    closes_h1 = [float(b["close"]) for b in bars_h1]
    closes_d  = [float(b["close"]) for b in bars_d]

    # Renko
    px_renko_h4 = renko_state(symbol, "240", current_close, debug=debug)
    px_renko_d  = renko_state(symbol, "D",   current_close, debug=debug)
    px_renko_w  = renko_state(symbol, "W",   current_close, debug=debug)

    renko_score    = px_renko_w * W_W1 + px_renko_d * W_D1 + px_renko_h4 * W_H4
    renko_state_now = 1 if renko_score >= SCORE_BULL_THRESHOLD else -1 if renko_score <= SCORE_BEAR_THRESHOLD else 0

    # Ichimoku cloud H1 + D (with displacement)
    px_cloud_h1 = cloud_state_displaced(bars_h1)
    px_cloud_d  = cloud_state_displaced(bars_d)

    # ROC
    roc_h1 = roc(closes_h1, ROC_LEN_H1)
    roc_d1 = roc(closes_d,  ROC_LEN_D1)

    # CHG bar-over-bar
    chg_h1 = chg(closes_h1)
    chg_d1 = chg(closes_d)

    bias_score   = renko_state_now + px_cloud_h1 + px_cloud_d
    pnl_score    = renko_state_now + sign(roc_h1) + sign(roc_d1)
    chg_score    = sign(chg_h1) + sign(chg_d1)
    global_score = bias_score + pnl_score + chg_score

    raw = 1 if global_score >= 3 else -1 if global_score <= -3 else 0

    # Filter by SAR Daily
    sar_d = sar_state(bars_d)
    if raw == 1 and sar_d == 1:
        return 1, global_score, bias_score, pnl_score, chg_score
    if raw == -1 and sar_d == -1:
        return -1, global_score, bias_score, pnl_score, chg_score
    return 0, global_score, bias_score, pnl_score, chg_score


# ─── Scan ─────────────────────────────────────────────────────────────────────

def state_label(s):
    return "BULL" if s == 1 else "BEAR" if s == -1 else "MIX"


def scan_pair(pair, debug=False):
    symbol = f"OANDA:{pair}"

    bars_h1 = fetch_tv_ohlc(symbol, "60", 300)
    bars_h4 = fetch_tv_ohlc(symbol, "240", 200)
    bars_d  = fetch_tv_ohlc(symbol, "D",   200)
    bars_w  = fetch_tv_ohlc(symbol, "W",   200)

    if not bars_h1 or not bars_h4 or not bars_d or not bars_w:
        return {"pair": pair, "signal": None, "status": "no data"}

    current_close = float(bars_h1[-1]["close"])

    # ── Global state ──────────────────────────────────────────────────────────
    g_state, g_score, bias, pnl, chg_ = global_state(symbol, bars_h1, bars_d, current_close, debug)

    # ── Momentum ──────────────────────────────────────────────────────────────
    mom = momentum_state(bars_h1)

    # ── MTF (W + D) ───────────────────────────────────────────────────────────
    mtf_w = mtf_state(bars_w)
    mtf_d = mtf_state(bars_d)
    mtf   = 1 if mtf_w == 1 and mtf_d == 1 else -1 if mtf_w == -1 and mtf_d == -1 else 0

    # ── TREND ─────────────────────────────────────────────────────────────────
    trend = 1 if g_state == 1 and mom == 1 and mtf == 1 else \
           -1 if g_state == -1 and mom == -1 and mtf == -1 else 0

    # ── Signal (crossover on H1) ──────────────────────────────────────────────
    sar_h1    = compute_sar(bars_h1)
    closes_h1 = [float(b["close"]) for b in bars_h1]
    opens_h1  = [float(b["open"])  for b in bars_h1]

    cross_over  = [i > 0 and closes_h1[i-1] <= sar_h1[i-1] and closes_h1[i] > sar_h1[i]
                   for i in range(len(bars_h1))]
    cross_under = [i > 0 and closes_h1[i-1] >= sar_h1[i-1] and closes_h1[i] < sar_h1[i]
                   for i in range(len(bars_h1))]

    bull_vw0 = valuewhen(cross_over,  sar_h1, 0)
    bear_vw0 = valuewhen(cross_under, sar_h1, 0)

    open_prev = opens_h1[-2]
    close_now = closes_h1[-1]

    signal = None
    if trend == 1 and not isnan(bear_vw0):
        if open_prev <= bear_vw0 and close_now > bear_vw0:
            signal = "LONG"
    if trend == -1 and not isnan(bull_vw0):
        if open_prev >= bull_vw0 and close_now < bull_vw0:
            signal = "SHORT"

    # ── Status line ───────────────────────────────────────────────────────────
    level     = bear_vw0 if trend == 1 else bull_vw0 if trend == -1 else NAN
    level_lbl = "bear_vw0" if trend == 1 else "bull_vw0" if trend == -1 else "—"
    level_str = f"{level:.5f}" if not isnan(level) else "nan"

    status = (
        f"Global={state_label(g_state)}(score={g_score:+d}) "
        f"Mom={state_label(mom)} "
        f"MTF={state_label(mtf)}(W={state_label(mtf_w)} D={state_label(mtf_d)}) "
        f"TREND={state_label(trend)}"
    )
    if trend != 0:
        status += f" | close={close_now:.5f} {level_lbl}={level_str}"
    if signal:
        status += f" → {signal} ✅"
    elif trend != 0:
        status += " → no crossover"

    return {
        "pair":       pair,
        "signal":     signal,
        "status":     status,
        "close":      close_now,
        "trend":      trend,
        "g_state":    g_state,
        "g_score":    g_score,
        "mom":        mom,
        "mtf":        mtf,
        "bull_vw0":   bull_vw0,
        "bear_vw0":   bear_vw0,
    }


# ─── Telegram ─────────────────────────────────────────────────────────────────

def send_telegram(text):
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


# ─── Backfill ─────────────────────────────────────────────────────────────────

def backfill_today(pairs, paris_tz, start_hour=7, debug=False):
    """
    For each pair with an active TREND, scan H1 bars since start_hour today
    and collect crossovers that would have been signals.
    Returns list of {time, pair, signal, close} sorted chronologically.
    """
    today_date = datetime.now(paris_tz).date()
    start_ts   = paris_tz.localize(
        datetime(today_date.year, today_date.month, today_date.day, start_hour, 0)
    ).timestamp()

    found = []

    for pair in pairs:
        symbol = f"OANDA:{pair}"
        bars_h1 = fetch_tv_ohlc(symbol, "60", 300)
        bars_d  = fetch_tv_ohlc(symbol, "D",   200)
        bars_w  = fetch_tv_ohlc(symbol, "W",   200)

        if not bars_h1 or not bars_d or not bars_w:
            continue

        current_close = float(bars_h1[-1]["close"])

        # Compute current trend state
        try:
            g_state, *_ = global_state(symbol, bars_h1, bars_d, current_close, debug)
        except Exception:
            continue
        mom  = momentum_state(bars_h1)
        mtf_w = mtf_state(bars_w)
        mtf_d = mtf_state(bars_d)
        mtf   = 1 if mtf_w == 1 and mtf_d == 1 else -1 if mtf_w == -1 and mtf_d == -1 else 0
        trend = 1 if g_state == 1 and mom == 1 and mtf == 1 else \
               -1 if g_state == -1 and mom == -1 and mtf == -1 else 0

        if trend == 0:
            continue

        # Compute SAR + crossovers on full H1 history
        sar_vals  = compute_sar(bars_h1)
        closes_h1 = [float(b["close"]) for b in bars_h1]
        opens_h1  = [float(b["open"])  for b in bars_h1]
        times_h1  = [int(b["time"])    for b in bars_h1]

        cross_over  = [i > 0 and closes_h1[i-1] <= sar_vals[i-1] and closes_h1[i] > sar_vals[i]
                       for i in range(len(bars_h1))]
        cross_under = [i > 0 and closes_h1[i-1] >= sar_vals[i-1] and closes_h1[i] < sar_vals[i]
                       for i in range(len(bars_h1))]

        # Scan historical bars since start_hour (exclude last bar = current open)
        for i in range(1, len(bars_h1) - 1):
            bar_ts = times_h1[i]
            if bar_ts < start_ts:
                continue

            bull_vw0 = valuewhen(cross_over[:i+1],  sar_vals[:i+1], 0)
            bear_vw0 = valuewhen(cross_under[:i+1], sar_vals[:i+1], 0)

            open_prev = opens_h1[i - 1]
            close_now = closes_h1[i]

            sig = None
            if trend == 1 and not isnan(bear_vw0):
                if open_prev <= bear_vw0 and close_now > bear_vw0:
                    sig = "LONG"
            if trend == -1 and not isnan(bull_vw0):
                if open_prev >= bull_vw0 and close_now < bull_vw0:
                    sig = "SHORT"

            if sig:
                bar_dt   = datetime.fromtimestamp(bar_ts, tz=paris_tz)
                hour_str = bar_dt.strftime("%H:%M")
                found.append({
                    "time":   hour_str,
                    "pair":   pair,
                    "signal": sig,
                    "close":  close_now,
                })

    found.sort(key=lambda x: x["time"])
    return found


# ─── Index filter ────────────────────────────────────────────────────────────

CCY_INDEX = {
    "USD": "TVC:DXY", "EUR": "TVC:EXY", "GBP": "TVC:BXY", "JPY": "TVC:JXY",
    "CHF": "TVC:SXY", "CAD": "TVC:CXY", "AUD": "TVC:AXY", "NZD": "TVC:ZXY",
}

def _scan_one_index(ccy: str, symbol: str) -> tuple[str, int]:
    bars_d = fetch_tv_ohlc(symbol, "D", 200)
    bars_w = fetch_tv_ohlc(symbol, "W", 200)
    if bars_d and bars_w:
        s_d = mtf_state(bars_d)
        s_w = mtf_state(bars_w)
        score = s_w + s_d  # -2/-1 → BEAR, +1/+2 → BULL, 0 → MIX
        return ccy, (1 if score >= 1 else -1 if score <= -1 else 0)
    return ccy, 0

def scan_index_states() -> dict[str, int]:
    """Returns {ccy: mtf_state} for each currency index (D+W cloud+SAR)."""
    states = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_scan_one_index, ccy, sym): ccy for ccy, sym in CCY_INDEX.items()}
        for future in futures:
            try:
                ccy, state = future.result(timeout=25)
                states[ccy] = state
            except Exception:
                states[futures[future]] = 0
    return states

def index_confirms(pair: str, signal: str, idx_states: dict[str, int]) -> bool:
    base, quote = pair[:3], pair[3:]
    s_base  = idx_states.get(base,  0)
    s_quote = idx_states.get(quote, 0)
    if signal == "LONG":
        return s_base == 1 and s_quote == -1
    if signal == "SHORT":
        return s_base == -1 and s_quote == 1
    return False


# ─── Daily log ────────────────────────────────────────────────────────────────

LOG_FILE = os.path.join(os.path.dirname(__file__), "sarenki_V1_forex_log.json")

def load_log(today: str) -> list:
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("date") == today:
            return data.get("signals", [])
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return []

def save_log(today: str, signals: list):
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump({"date": today, "signals": signals}, f, ensure_ascii=False)


# ─── Main ─────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Sarenki V1 Forex signal scanner")
    p.add_argument("--no-telegram", action="store_true")
    p.add_argument("--debug",       action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    paris_tz = pytz.timezone("Europe/Paris")
    now      = datetime.now(paris_tz)
    today    = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H:%M")
    now_str  = now.strftime("%Y-%m-%d %H:%M")

    print("Scanning currency indices...")
    idx_states = scan_index_states()
    print(f"  {', '.join(f'{c}={v:+d}' for c, v in idx_states.items())}\n")

    print(f"Scanning {len(PAIRS)} pairs...\n")

    all_results = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(scan_pair, pair, args.debug): pair for pair in PAIRS}
        for future, pair in futures.items():
            try:
                r = future.result(timeout=30)
            except FuturesTimeoutError:
                future.cancel()
                print(f"  {pair}: timeout — skipped")
                continue
            except Exception as e:
                print(f"  {pair}: error — {e}")
                continue
            if r:
                all_results.append(r)

    # ── Terminal detail ────────────────────────────────────────────────────────
    all_results.sort(key=lambda r: r["pair"])
    print("── Détail des paires ─────────────────────────────────────────────────")
    for r in all_results:
        print(f"  {r['pair']:<10} {r['status']}")
    print()

    # ── Accumulate daily log ───────────────────────────────────────────────────
    daily_log   = load_log(today)
    is_first_run = len(daily_log) == 0

    # Backfill signals since 07:00 on first run of the day
    if is_first_run:
        print("Premier run de la journée — backfill depuis 07:00...\n")
        historical = backfill_today(PAIRS, paris_tz, start_hour=7, debug=args.debug)
        daily_log.extend(historical)
        if historical:
            print(f"  {len(historical)} signal(s) historique(s) retrouvé(s):")
            for e in historical:
                emoji = "🟢" if e["signal"] == "LONG" else "🔴"
                print(f"    {emoji} {e['pair']} @ {e['time']}")
        else:
            print("  Aucun signal historique depuis 07:00.")
        print()

    # Add current hour signals (avoid duplicates: same pair+signal+hour)
    existing_keys = {(e["pair"], e["signal"], e["time"]) for e in daily_log}
    for r in all_results:
        if not r["signal"]:
            continue
        key = (r["pair"], r["signal"], hour_str)
        if key not in existing_keys:
            daily_log.append({
                "time":   hour_str,
                "pair":   r["pair"],
                "signal": r["signal"],
                "close":  r["close"],
            })
            existing_keys.add(key)

    save_log(today, daily_log)

    # ── Build Telegram message (full day log, deduplicated by pair+signal → last occurrence) ──
    seen = {}
    for entry in daily_log:
        seen[(entry["pair"], entry["signal"])] = entry
    deduped = sorted(seen.values(), key=lambda e: e["time"])

    lines = ["📡 SARENKI V1 FOREX", ""]

    if deduped:
        for entry in deduped:
            emoji = "🟢" if entry["signal"] == "LONG" else "🔴"
            flame = " 🔥" if index_confirms(entry["pair"], entry["signal"], idx_states) else ""
            lines.append(f"  {emoji} {entry['pair']:<10} @ {entry['time']}{flame}")
    else:
        lines.append("  (aucun signal aujourd'hui)")

    lines += ["", f"⏰ {now_str} Paris"]
    msg = "\n".join(lines)

    print(f"{msg}\n")

    if not args.no_telegram:
        send_telegram(msg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
