#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
renko_forex_V4.py
Renko ATR(14) W/D alignment for selected OANDA Forex pairs.
Logic mirrors renko_forex_V4.pine: pair W/D signal + currency-index W/D confirmation.
"""

import argparse
import json
import math
import os
import random
import string
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import datetime

def _configure_console_encoding() -> None:
    """Keep emoji output readable in Windows PowerShell."""
    if os.name == "nt" and sys.stdout.isatty():
        os.system("chcp 65001 > nul")
    if sys.stdout.encoding and sys.stdout.encoding.lower().replace("-", "") != "utf8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


_configure_console_encoding()

import pytz
import requests as http_requests
from dotenv import load_dotenv
from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException, create_connection

load_dotenv()

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
STATE_PATH    = os.path.join(SCRIPT_DIR, "renko_forex_v4_state.json")
TRACKER_PATH  = os.path.join(SCRIPT_DIR, "renko_forex_v4_tracker.json")
HISTORY_PATH  = os.path.join(SCRIPT_DIR, "renko_forex_v4_pnl_history.json")
PORTFOLIO_PATH = os.path.join(SCRIPT_DIR, "renko_forex_v4_portfolio.json")
SPREAD_CACHE_PATH = os.path.join(SCRIPT_DIR, "renko_forex_v4_spreads.json")
CLOSE_THRESHOLD = -0.05  # % threshold for close alert
PORTFOLIO_INITIAL_CAPITAL = 10000.0
PORTFOLIO_POSITION_BUDGET = 1000.0
ACCOUNT_CURRENCY = "EUR"
OANDA_SPREAD_DIVISION = os.getenv("OANDA_SPREAD_DIVISION", "OEL")
OANDA_SPREAD_FIELD = os.getenv("OANDA_SPREAD_FIELD", "averageBusinessHours")
OANDA_PRICING_MODEL = os.getenv("OANDA_PRICING_MODEL", "spread").strip().lower()
OANDA_CORE_COMMISSION_EUR_PER_10K = float(os.getenv("OANDA_CORE_COMMISSION_EUR_PER_10K", "0.70"))

WS_URL     = "wss://prodata.tradingview.com/socket.io/websocket"
WS_HEADERS = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

PAIRS_29 = [
    "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "AUDUSD",
    "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD",
    "EURCHF", "EURGBP", "EURJPY", "EURNZD", "EURUSD",
    "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD",
    "GBPUSD", "NZDCAD", "NZDCHF", "NZDJPY", "NZDUSD",
    "USDCAD", "USDCHF", "USDJPY", "XAUUSD",
]

FOREX_ASSETS: list[tuple[str, str]] = [
    (f"OANDA:{pair}", pair) for pair in PAIRS_29
]

CURRENCY_INDICES: list[tuple[str, str, str]] = [
    ("DXY", "TVC:DXY", "USD"),
    ("EXY", "TVC:EXY", "EUR"),
    ("BXY", "TVC:BXY", "GBP"),
    ("JXY", "TVC:JXY", "JPY"),
    ("SXY", "TVC:SXY", "CHF"),
    ("CXY", "TVC:CXY", "CAD"),
    ("AXY", "TVC:AXY", "AUD"),
    ("ZXY", "TVC:ZXY", "NZD"),
]

INDEX_BY_CURRENCY = {currency: symbol for _ticker, symbol, currency in CURRENCY_INDICES}


# ─── WebSocket helpers ────────────────────────────────────────────────────────

def _gen_sid() -> str:
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))

def _msg(func: str, args: list) -> str:
    payload = json.dumps({"m": func, "p": args})
    return f"~m~{len(payload)}~m~{payload}"

def _frames(raw: str) -> list[str]:
    if raw in ("~h", "h"):
        return [raw]
    out, i = [], 0
    while raw.startswith("~m~", i):
        i += 3
        j = raw.find("~m~", i)
        if j == -1:
            break
        size = int(raw[i:j])
        i = j + 3
        out.append(raw[i: i + size])
        i += size
    return out if out else [raw]


# ─── OHLC fetch ───────────────────────────────────────────────────────────────

def fetch_tv_ohlc(symbol: str, interval: str, n_candles: int,
                  timeout_s: int = 20, retries: int = 2) -> list[dict] | None:
    for attempt in range(retries + 1):
        ws = None
        try:
            ws = create_connection(WS_URL, header=WS_HEADERS, timeout=timeout_s)
            ws.settimeout(timeout_s)
            sid = _gen_sid()
            ws.send(_msg("chart_create_session", [sid, ""]))
            ws.send(_msg("resolve_symbol", [
                sid, "sds_sym_1",
                f'={{"symbol":"{symbol}","adjustment":"splits","session":"regular"}}'
            ]))
            ws.send(_msg("create_series", [sid, "sds_1", "s1", "sds_sym_1", interval, n_candles, ""]))

            points: list[dict] = []
            t0 = time.time()
            while time.time() - t0 < 18:
                try:
                    raw = ws.recv()
                except WebSocketTimeoutException:
                    continue
                except WebSocketConnectionClosedException:
                    break
                for frame in _frames(raw):
                    if frame in ("~h", "h"):
                        ws.send("~h")
                        continue
                    if '"m":"timescale_update"' in frame:
                        payload = json.loads(frame)
                        series = payload.get("p", [None, {}])[1]
                        if isinstance(series, dict) and "sds_1" in series:
                            points = series["sds_1"].get("s", []) or points
                    if "series_completed" in frame:
                        break
                if "series_completed" in raw:
                    break

            if not points:
                continue
            rows = []
            for item in points:
                v = item.get("v", [])
                if len(v) >= 5:
                    rows.append({"time": v[0], "open": v[1], "high": v[2], "low": v[3], "close": v[4]})
            if rows:
                return rows
        except Exception:
            if attempt >= retries:
                break
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
    return None


# ─── Renko fetch (native TV) ──────────────────────────────────────────────────

def fetch_tv_renko_ohlc(symbol: str, interval: str, atr_length: int = 14,
                        n_bricks: int = 20, timeout_s: int = 25, retries: int = 2,
                        debug: bool = False) -> dict | None:
    wait_s = 18  # M/W/D are non-intraday
    for attempt in range(retries + 1):
        ws = None
        symbol_error = False
        try:
            ws = create_connection(WS_URL, header=WS_HEADERS, timeout=timeout_s)
            ws.settimeout(timeout_s)
            sid = _gen_sid()
            ws.send(_msg("chart_create_session", [sid, ""]))
            sym_payload = json.dumps({
                "symbol": {"symbol": symbol, "adjustment": "splits", "session": "regular"},
                "type": "BarSetRenko@tv-prostudies-40!",
                "inputs": {
                    "source": "close", "sources": "Close",
                    "boxSize": atr_length, "style": "ATR",
                    "atrLength": atr_length, "wicks": True,
                },
            })
            ws.send(_msg("resolve_symbol", [sid, "sds_sym_1", f"={sym_payload}"]))
            ws.send(_msg("create_series", [sid, "sds_1", "s1", "sds_sym_1", interval, n_bricks, ""]))

            points: list[dict] = []
            t0 = time.time()
            while time.time() - t0 < wait_s:
                try:
                    raw = ws.recv()
                except WebSocketTimeoutException:
                    continue
                except WebSocketConnectionClosedException:
                    break
                for frame in _frames(raw):
                    if frame in ("~h", "h"):
                        ws.send("~h")
                        continue
                    if '"m":"symbol_error"' in frame or '"m":"series_error"' in frame:
                        symbol_error = True
                        break
                    if '"m":"timescale_update"' in frame:
                        payload = json.loads(frame)
                        series = payload.get("p", [None, {}])[1]
                        if isinstance(series, dict) and "sds_1" in series:
                            points = series["sds_1"].get("s", []) or points
                    if "series_completed" in frame:
                        break
                if symbol_error:
                    break
                if "series_completed" in raw:
                    break

            if debug:
                print(f"  [renko {interval}] attempt={attempt} symbol_error={symbol_error} points={len(points)}")
            if symbol_error:
                break
            if points:
                bricks = []
                for item in points:
                    v = item.get("v", [])
                    if len(v) >= 5:
                        bricks.append({"open": v[1], "close": v[4]})
                if bricks:
                    return bricks
        except Exception as e:
            if debug:
                print(f"  [renko {interval}] exception: {e}")
            if attempt >= retries:
                break
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
    return None


# ─── Streak helper ───────────────────────────────────────────────────────────

def green_streak(bricks: list[dict]) -> int:
    """Count consecutive green (close > open) bricks from the most recent."""
    count = 0
    for b in reversed(bricks):
        if float(b["close"]) > float(b["open"]):
            count += 1
        else:
            break
    return count


def red_streak(bricks: list[dict]) -> int:
    """Count consecutive red (close < open) bricks from the most recent."""
    count = 0
    for b in reversed(bricks):
        if float(b["close"]) < float(b["open"]):
            count += 1
        else:
            break
    return count


# ─── Local Renko fallback ─────────────────────────────────────────────────────

def _atr_from_ohlc(bars: list[dict], length: int) -> float | None:
    if not bars or len(bars) < length + 1:
        return None
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = float(bars[i]["high"]), float(bars[i]["low"]), float(bars[i - 1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < length:
        return None
    return sum(trs[-length:]) / float(length)


def _last_renko_brick_from_closes(closes: list[float], box_size: float) -> dict | None:
    if not closes or box_size <= 0:
        return None
    ref = float(closes[0])
    last_open = last_close = None
    for px in closes[1:]:
        px = float(px)
        while abs(px - ref) >= box_size:
            direction = 1.0 if px > ref else -1.0
            last_open = ref
            last_close = ref + direction * box_size
            ref = last_close
    if last_open is None or last_close is None:
        return {"open": ref, "high": ref, "low": ref, "close": ref}
    return {"open": last_open, "high": max(last_open, last_close),
            "low": min(last_open, last_close), "close": last_close}


def _renko_brick_series(bars: list[dict], box_size: float) -> list[dict]:
    result = []
    if not bars or box_size <= 0:
        return result
    ref = float(bars[0]["close"])
    for bar in bars[1:]:
        px, t = float(bar["close"]), int(bar["time"])
        while abs(px - ref) >= box_size:
            direction = 1.0 if px > ref else -1.0
            b_open, b_close = ref, ref + direction * box_size
            result.append({"time": t, "open": b_open, "close": b_close})
            ref = b_close
    return result


def _brick_at(series: list[dict], t: int) -> dict | None:
    result = None
    for b in series:
        if b["time"] <= t:
            result = b
        else:
            break
    return result


def px_state(renko_open: float, renko_close: float, price: float) -> int:
    h, l = max(renko_open, renko_close), min(renko_open, renko_close)
    return 1 if price > h else (-1 if price < l else 0)


def _compute_psar(bars: list[dict], start: float = 0.1, increment: float = 0.1, maximum: float = 0.2) -> list[float]:
    """Parabolic SAR — same defaults as sarenki_V3.pine."""
    n = len(bars)
    if n < 2:
        return [float("nan")] * n
    sar = [float("nan")] * n
    highs = [float(b["high"]) for b in bars]
    lows  = [float(b["low"])  for b in bars]
    closes = [float(b["close"]) for b in bars]
    bull = closes[1] >= closes[0]
    af = start
    ep = highs[1] if bull else lows[1]
    sar[0] = lows[0] if bull else highs[0]
    sar[1] = lows[0] if bull else highs[0]
    for i in range(2, n):
        prev_sar = sar[i - 1]
        if bull:
            new_sar = prev_sar + af * (ep - prev_sar)
            new_sar = min(new_sar, lows[i - 1], lows[i - 2])
            if lows[i] < new_sar:
                bull = False
                new_sar = ep
                ep = lows[i]
                af = start
            else:
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + increment, maximum)
        else:
            new_sar = prev_sar + af * (ep - prev_sar)
            new_sar = max(new_sar, highs[i - 1], highs[i - 2])
            if highs[i] > new_sar:
                bull = True
                new_sar = ep
                ep = highs[i]
                af = start
            else:
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + increment, maximum)
        sar[i] = new_sar
    return sar


def sar_h1_breakout(symbol: str, direction: int) -> bool:
    """Return True if current H1 close crosses one of the 3 last opposite SAR levels."""
    bars = fetch_tv_ohlc(symbol, "60", 150)
    if not bars or len(bars) < 10:
        return False
    sar_vals = _compute_psar(bars)
    closes = [float(b["close"]) for b in bars]
    n = len(bars)
    # Identify last 3 crossunder (close goes below SAR → bearish flip → bear_vw0)
    # and last 3 crossover (close goes above SAR → bullish flip → bull_vw0)
    bear_levels: list[float] = []  # SAR at last crossunders
    bull_levels: list[float] = []  # SAR at last crossovers
    for i in range(1, n):
        if float("nan") in (sar_vals[i], sar_vals[i - 1]):
            continue
        cross_over  = closes[i] > sar_vals[i] and closes[i - 1] <= sar_vals[i - 1]
        cross_under = closes[i] < sar_vals[i] and closes[i - 1] >= sar_vals[i - 1]
        if cross_over:
            bull_levels.append(sar_vals[i])
        if cross_under:
            bear_levels.append(sar_vals[i])
    # Keep last 3 of each
    bear_levels = bear_levels[-3:]
    bull_levels = bull_levels[-3:]
    curr  = closes[-1]
    prev  = closes[-2]
    if direction == 1:   # LONG: price crosses above any bear level
        return any(prev <= lvl < curr for lvl in bear_levels)
    elif direction == -1:  # SHORT: price crosses below any bull level
        return any(curr < lvl <= prev for lvl in bull_levels)
    return False


def sar_h1_cross_events_24h(symbol: str, direction: int, paris_tz) -> list[dict]:
    """Return SAR H1 cross events (closed candles only) in the last 24h, filtered by bias direction.

    direction=1  → LONG  : crossover ↑ only (price closes above SAR after being below)
    direction=-1 → SHORT : crossunder ↓ only (price closes below SAR after being above)
    """
    bars = fetch_tv_ohlc(symbol, "60", 150)
    if not bars or len(bars) < 10:
        return []
    sar_vals = _compute_psar(bars)
    closes = [float(b["close"]) for b in bars]
    n = len(bars)
    cutoff = time.time() - 86400
    events = []
    # Stop at n-1 to exclude the current unclosed candle
    for i in range(1, n - 1):
        bar_time = int(bars[i]["time"])
        if bar_time < cutoff:
            continue
        if math.isnan(sar_vals[i]) or math.isnan(sar_vals[i - 1]):
            continue
        cross_over  = closes[i] > sar_vals[i] and closes[i - 1] <= sar_vals[i - 1]
        cross_under = closes[i] < sar_vals[i] and closes[i - 1] >= sar_vals[i - 1]
        if direction == 1 and cross_over:
            dt_paris = datetime.fromtimestamp(bar_time, tz=paris_tz).strftime("%H:%M")
            events.append({"time_paris": dt_paris, "type": "crossover ↑", "sar": sar_vals[i], "close": closes[i]})
        elif direction == -1 and cross_under:
            dt_paris = datetime.fromtimestamp(bar_time, tz=paris_tz).strftime("%H:%M")
            events.append({"time_paris": dt_paris, "type": "crossunder ↓", "sar": sar_vals[i], "close": closes[i]})
    return events


def _fetch_renko_with_fallback(symbol: str, interval: str, atr_length: int,
                                n_candles: int, debug: bool = False) -> list[dict] | None:
    """Return list of renko bricks [{open, close}, ...] or None."""
    bricks = fetch_tv_renko_ohlc(symbol, interval, atr_length=atr_length, n_bricks=n_candles, debug=debug)
    if bricks:
        return bricks
    bars = fetch_tv_ohlc(symbol, interval, n_candles)
    if not bars:
        if debug:
            print(f"  [{symbol}] no OHLC for {interval}")
        return None
    atr = _atr_from_ohlc(bars, atr_length)
    if not atr or atr <= 0:
        if debug:
            print(f"  [{symbol}] ATR failed for {interval} ({len(bars)} bars)")
        return None
    closes = [float(b["close"]) for b in bars]
    series = _renko_brick_series([{"close": c, "time": i} for i, c in enumerate(closes)], atr)
    if debug and series:
        print(f"  [{symbol}] renko {interval} fallback box={atr:.4f} bricks={len(series)}")
    return series if series else None


def scan_currency_indices(atr_length: int = 14, debug: bool = False) -> list[str]:
    """Scan currency indices on M/W/D Renko and return compact report lines."""
    results = []
    for ticker, tv_sym, currency in CURRENCY_INDICES:
        bm = _fetch_renko_with_fallback(tv_sym, "M", atr_length, 200)
        bw = _fetch_renko_with_fallback(tv_sym, "W", atr_length, 200)
        bd = _fetch_renko_with_fallback(tv_sym, "D", atr_length, 200)
        if not bm or not bw or not bd:
            continue
        bars = fetch_tv_ohlc(tv_sym, "D", 30)
        if not bars:
            continue
        price = float(bars[-1]["close"])
        sm = px_state(bm[-1]["open"], bm[-1]["close"], price)
        sw = px_state(bw[-1]["open"], bw[-1]["close"], price)
        sd = px_state(bd[-1]["open"], bd[-1]["close"], price)
        if debug:
            print(
                f"  INDEX {currency:<3} | M={sm:+d}({green_streak(bm)}/{red_streak(bm)}) "
                f"W={sw:+d}({green_streak(bw)}/{red_streak(bw)}) "
                f"D={sd:+d}({green_streak(bd)}/{red_streak(bd)})"
            )
        bull_count = sum(1 for s in (sm, sw, sd) if s == 1)
        bear_count = sum(1 for s in (sm, sw, sd) if s == -1)
        if bull_count >= 2 and sd == 1:
            stm = green_streak(bm)
            stw = green_streak(bw)
            std = green_streak(bd)
            if std >= 1:
                results.append((bull_count, 1, currency, stm, stw, std))
        elif bear_count >= 2 and sd == -1:
            stm = red_streak(bm)
            stw = red_streak(bw)
            std = red_streak(bd)
            if std >= 1:
                results.append((bear_count, -1, currency, stm, stw, std))

    bull = sorted([(c, cur, sm, sw, sd) for c, d, cur, sm, sw, sd in results if d == 1], reverse=True)
    bear = sorted([(c, cur, sm, sw, sd) for c, d, cur, sm, sw, sd in results if d == -1], reverse=True)
    lines = []
    for count, currency, sm, sw, sd in bull:
        lines.append(f"🟢 {currency} ({count})")
    for count, currency, sm, sw, sd in bear:
        lines.append(f"🔴 {currency} ({count})")

    express_bull = []
    express_bear = []
    for ticker, tv_sym, currency in CURRENCY_INDICES:
        bw = _fetch_renko_with_fallback(tv_sym, "W", atr_length, 200)
        bd = _fetch_renko_with_fallback(tv_sym, "D", atr_length, 200)
        if not bw or not bd:
            continue
        bars = fetch_tv_ohlc(tv_sym, "D", 30)
        if not bars:
            continue
        price = float(bars[-1]["close"])
        sw = px_state(bw[-1]["open"], bw[-1]["close"], price)
        sd = px_state(bd[-1]["open"], bd[-1]["close"], price)
        if sw == 1 and sd == 1 and green_streak(bd) >= 1:
            express_bull.append(currency)
        elif sw == -1 and sd == -1 and red_streak(bd) >= 1:
            express_bear.append(currency)

    if express_bull or express_bear:
        lines.append("")
        lines.append("⚡️ EXPRESS")
        for currency in express_bull:
            lines.append(f"🟢 {currency}")
        for currency in express_bear:
            lines.append(f"🔴 {currency}")

    return lines


def scan_currency_index_daily_chg_extremes(debug: bool = False) -> list[str]:
    """Return strongest positive and negative daily CHG% among currency indices."""
    changes = scan_currency_index_daily_changes(debug=debug)
    if not changes:
        return []

    lines = []
    positive = [item for item in changes if item[0] > 0]
    negative = [item for item in changes if item[0] < 0]
    if positive:
        chg_pct, currency = max(positive, key=lambda item: item[0])
        lines.append(f"🟢 {currency} {chg_pct:+.2f}%")
    if negative:
        chg_pct, currency = min(negative, key=lambda item: item[0])
        lines.append(f"🔴 {currency} {chg_pct:+.2f}%")
    if not lines:
        chg_pct, currency = max(changes, key=lambda item: abs(item[0]))
        icon = "🟢" if chg_pct >= 0 else "🔴"
        lines.append(f"{icon} {currency} {chg_pct:+.2f}%")
    return lines


def scan_currency_index_daily_changes(debug: bool = False) -> list[tuple[float, str]]:
    """Return daily CHG% for all currency indices as (chg_pct, currency)."""
    changes = []
    for _ticker, tv_sym, currency in CURRENCY_INDICES:
        bars = fetch_tv_ohlc(tv_sym, "D", 30)
        if not bars:
            continue
        last = bars[-1]
        try:
            open_px = float(last["open"])
            close_px = float(last["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if open_px == 0:
            continue
        chg_pct = (close_px - open_px) / open_px * 100.0
        changes.append((chg_pct, currency))
        if debug:
            print(f"  INDEX CHG {currency:<3} | {chg_pct:+.2f}%")

    return changes


def scan_extreme_index_pair_confirmation(snaps: list["ForexSnapshot"], debug: bool = False) -> list[str]:
    """Check whether the strongest/weakest index pair has a confirming daily CHG%."""
    changes = scan_currency_index_daily_changes(debug=debug)
    positive = [item for item in changes if item[0] > 0]
    negative = [item for item in changes if item[0] < 0]
    if not positive or not negative:
        return []

    strong_chg, strong_ccy = max(positive, key=lambda item: item[0])
    weak_chg, weak_ccy = min(negative, key=lambda item: item[0])
    snap_by_name = {s.name.upper(): s for s in snaps}

    direct_pair = f"{strong_ccy}{weak_ccy}"
    inverse_pair = f"{weak_ccy}{strong_ccy}"
    expected_pair = ""
    expected_side = ""
    snap = None
    if direct_pair in snap_by_name:
        expected_pair = direct_pair
        expected_side = "LONG"
        snap = snap_by_name[direct_pair]
    elif inverse_pair in snap_by_name:
        expected_pair = inverse_pair
        expected_side = "SHORT"
        snap = snap_by_name[inverse_pair]

    if snap is None:
        return [f"⚪ {strong_ccy}/{weak_ccy}: paire absente de la watchlist"]

    chg = snap.chg_daily_pct
    if chg is None:
        return [f"⚪ {expected_pair} {expected_side} attendu | CHG D N/A"]

    confirms = chg > 0.1 if expected_side == "LONG" else chg < -0.1
    icon = ("🟢" if expected_side == "LONG" else "🔴") if confirms else "❌"
    return [f"{icon} {expected_pair}"]


def index_renko_state(symbol: str, atr_length: int = 14, debug: bool = False) -> tuple[int, int]:
    bw = _fetch_renko_with_fallback(symbol, "W", atr_length, 200, debug)
    bd = _fetch_renko_with_fallback(symbol, "D", atr_length, 200, debug)
    bars = fetch_tv_ohlc(symbol, "D", 30)
    if not bw or not bd or not bars:
        return 0, 0
    price = float(bars[-1]["close"])
    return (
        px_state(bw[-1]["open"], bw[-1]["close"], price),
        px_state(bd[-1]["open"], bd[-1]["close"], price),
    )


def fetch_index_state_map(atr_length: int = 14, debug: bool = False) -> dict[str, tuple[int, int]]:
    states: dict[str, tuple[int, int]] = {}
    for _ticker, tv_sym, currency in CURRENCY_INDICES:
        states[currency] = index_renko_state(tv_sym, atr_length, debug)
    return states


def any_bull(state: tuple[int, int] | None) -> bool:
    return bool(state) and (state[0] == 1 or state[1] == 1)


def any_bear(state: tuple[int, int] | None) -> bool:
    return bool(state) and (state[0] == -1 or state[1] == -1)


def split_pair(symbol: str) -> tuple[str, str]:
    compact = compact_symbol(symbol)
    return compact[:3], compact[3:6]


def index_confirms_trade(symbol: str, direction: int, index_state_map: dict[str, tuple[int, int]]) -> bool:
    base, quote = split_pair(symbol)
    base_state = index_state_map.get(base, (0, 0))
    quote_state = index_state_map.get(quote, (0, 0))
    if direction == 1:
        return any_bull(base_state) and any_bear(quote_state)
    if direction == -1:
        return any_bear(base_state) and any_bull(quote_state)
    return False


# ─── State / tracker persistence ─────────────────────────────────────────────

def _load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_json(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _get(state: dict, sym: str, key: str, default=None):
    return state.get(sym, {}).get(key, default)

def _set(state: dict, sym: str, **kwargs):
    state.setdefault(sym, {}).update(kwargs)
    state[sym]["updated_at"] = datetime.now(pytz.UTC).isoformat()


# ─── Snapshot ─────────────────────────────────────────────────────────────────

@dataclass
class ForexSnapshot:
    symbol: str
    name: str
    bar_time: int
    px_mn: int
    px_w1: int
    px_d1: int  # = daily in M/W/D mode
    streak_3m: int
    streak_m: int
    streak_w: int
    aligned: int
    bias_state_before: int
    bias_state_after: int
    bias_entry: float | None
    pnl_pct: float | None
    price_roc7: float | None
    price_roc14: float | None
    price_roc21: float | None
    chg_daily_pct: float | None
    index_confirmed: bool
    base_index_state: tuple[int, int]
    quote_index_state: tuple[int, int]
    last_event_time: int | None
    last_event_price: float | None
    close: float
    ts_utc: int


# ─── Scan one symbol ──────────────────────────────────────────────────────────

def scan_symbol(symbol: str, name: str, atr_length: int,
                start_date_utc: int | None, state: dict,
                index_state_map: dict[str, tuple[int, int]],
                debug: bool = False) -> ForexSnapshot | None:

    # Timeframes: W (weekly) / D (daily), matching renko_forex_V4.pine.
    brick_w1 = _fetch_renko_with_fallback(symbol, "W", atr_length, 200, debug)
    if not brick_w1:
        if debug:
            print(f"  [{symbol}] no renko W")
        return None

    brick_d1 = _fetch_renko_with_fallback(symbol, "D", atr_length, 200, debug)
    if not brick_d1:
        if debug:
            print(f"  [{symbol}] no renko D")
        return None

    bars_d = fetch_tv_ohlc(symbol, "D", 30)
    if not bars_d:
        if debug:
            print(f"  [{symbol}] no daily OHLC for current price")
        return None

    curr_close = float(bars_d[-1]["close"])
    bar_time   = int(bars_d[-1]["time"])
    daily_open = float(bars_d[-1]["open"])
    chg_daily_pct: float | None = (curr_close - daily_open) / daily_open * 100 if daily_open else None
    price_roc7: float | None = None
    price_roc14: float | None = None
    price_roc21: float | None = None
    bars_roc = fetch_tv_ohlc(symbol, "D", 24)
    if bars_roc and len(bars_roc) >= 8:
        roc_close = float(bars_roc[-1]["close"])
        prev7  = float(bars_roc[-8]["close"])
        if prev7:
            price_roc7 = (roc_close - prev7) / prev7 * 100
        if len(bars_roc) >= 15:
            prev14 = float(bars_roc[-15]["close"])
            if prev14:
                price_roc14 = (roc_close - prev14) / prev14 * 100
        if len(bars_roc) >= 22:
            prev21 = float(bars_roc[-22]["close"])
            if prev21:
                price_roc21 = (roc_close - prev21) / prev21 * 100

    last_m  = brick_w1[-1]
    last_w  = brick_d1[-1]

    px_mn = 0
    px_w1 = px_state(last_m["open"],  last_m["close"],  curr_close)
    px_d1 = px_state(last_w["open"],  last_w["close"],  curr_close)

    streak_3m = 0
    streak_m  = green_streak(brick_w1) if px_w1 == 1 else red_streak(brick_w1) if px_w1 == -1 else 0
    streak_w  = green_streak(brick_d1) if px_d1 == 1 else red_streak(brick_d1) if px_d1 == -1 else 0

    aligned_bull = px_w1 == 1 and px_d1 == 1
    aligned_bear = px_w1 == -1 and px_d1 == -1
    aligned = 1 if aligned_bull else (-1 if aligned_bear else 0)
    base_ccy, quote_ccy = split_pair(symbol)
    base_index_state = index_state_map.get(base_ccy, (0, 0))
    quote_index_state = index_state_map.get(quote_ccy, (0, 0))
    index_confirmed = index_confirms_trade(symbol, aligned, index_state_map)

    bias_before = int(_get(state, symbol, "biasState", 0))
    bias_entry  = _get(state, symbol, "biasEntry")
    last_t      = _get(state, symbol, "lastEventTime")
    last_p      = _get(state, symbol, "lastEventPrice")

    stored_start = _get(state, symbol, "startDate")
    if start_date_utc is not None and stored_start != start_date_utc:
        bias_before, bias_entry, last_t, last_p = 0, None, None, None

    if start_date_utc is not None and bar_time < start_date_utc:
        bias_after = 0
        bias_entry = last_t = last_p = None
        _set(state, symbol, biasState=0, biasEntry=None,
             lastEventTime=None, lastEventPrice=None, startDate=start_date_utc)
    else:
        bias_after = bias_before
        if aligned_bull and bias_before != 1:
            bias_after = 1
            bias_entry = curr_close
            last_t, last_p = bar_time, curr_close
        elif aligned_bear and bias_before != -1:
            bias_after = -1
            bias_entry = curr_close
            last_t, last_p = bar_time, curr_close
        _set(state, symbol, biasState=bias_after, biasEntry=bias_entry,
             lastEventTime=last_t, lastEventPrice=last_p, startDate=start_date_utc)

    pnl_pct: float | None = None
    if bias_after == 1 and bias_entry:
        pnl_pct = (curr_close - bias_entry) / bias_entry * 100
    elif bias_after == -1 and bias_entry:
        pnl_pct = (bias_entry - curr_close) / bias_entry * 100

    return ForexSnapshot(
        symbol=symbol, name=name, bar_time=bar_time,
        px_mn=px_mn, px_w1=px_w1, px_d1=px_d1,
        streak_3m=streak_3m, streak_m=streak_m, streak_w=streak_w,
        aligned=aligned,
        bias_state_before=bias_before, bias_state_after=bias_after,
        bias_entry=bias_entry, pnl_pct=pnl_pct, price_roc7=price_roc7, price_roc14=price_roc14, price_roc21=price_roc21,
        chg_daily_pct=chg_daily_pct,
        index_confirmed=index_confirmed,
        base_index_state=base_index_state,
        quote_index_state=quote_index_state,
        last_event_time=last_t, last_event_price=last_p,
        close=curr_close, ts_utc=bar_time,
    )


# ─── Historical init ──────────────────────────────────────────────────────────

def init_bias_from_history(symbol: str, start_date_utc: int,
                            atr_length: int, debug: bool = False) -> dict:
    # Timeframes: W / D
    bars_mn = fetch_tv_ohlc(symbol, "W", 200)
    bars_w1 = fetch_tv_ohlc(symbol, "D", 300)

    if not bars_mn or not bars_w1:
        if debug:
            print(f"  [{symbol}] init: missing OHLC")
        return {}

    atr_mn = _atr_from_ohlc(bars_mn, atr_length)
    atr_w1 = _atr_from_ohlc(bars_w1, atr_length)

    if not atr_mn or not atr_w1:
        if debug:
            print(f"  [{symbol}] init: ATR failed")
        return {}

    series_mn = _renko_brick_series(bars_mn, atr_mn)
    series_w1 = _renko_brick_series(bars_w1, atr_w1)

    bias_state = 0
    bias_entry: float | None = None
    last_event_time: int | None = None
    last_event_price: float | None = None

    # Walk daily bars to replay alignment events
    for bar in bars_w1:
        t = int(bar["time"])
        if t < start_date_utc:
            continue
        price = float(bar["close"])
        b_mn  = _brick_at(series_mn, t)
        b_w1  = _brick_at(series_w1, t)
        if not b_mn or not b_w1:
            continue

        pw1 = px_state(b_mn["open"], b_mn["close"], price)
        pd1 = px_state(b_w1["open"], b_w1["close"], price)

        if pw1 == 1 and pd1 == 1 and bias_state != 1:
            bias_state, bias_entry, last_event_time, last_event_price = 1, price, t, price
        elif pw1 == -1 and pd1 == -1 and bias_state != -1:
            bias_state, bias_entry, last_event_time, last_event_price = -1, price, t, price

    if debug:
        label = "BULL" if bias_state == 1 else "BEAR" if bias_state == -1 else "MIXED"
        dt = datetime.fromtimestamp(last_event_time, tz=pytz.UTC).strftime("%Y-%m-%d") if last_event_time else "---"
        print(f"  [{symbol}] init => {label} entry={bias_entry} last={dt}")

    return {"biasState": bias_state, "biasEntry": bias_entry,
            "lastEventTime": last_event_time, "lastEventPrice": last_event_price}


# ─── Tracker ──────────────────────────────────────────────────────────────────

def update_tracker(tracker: dict, snaps: list[ForexSnapshot], paris_tz) -> list[str]:
    now_paris = datetime.now(paris_tz)
    today_str = now_paris.strftime("%Y-%m-%d")
    close_alerts = []

    # Track the global top-10, LONG and SHORT included.
    candidates = sorted(
        [s for s in snaps if is_trade_candidate(s)],
        key=lambda s: s.pnl_pct if s.pnl_pct is not None else float("-inf"),
        reverse=True,
    )
    active_positive = {s.symbol for s in candidates[:10]}

    for sym in active_positive:
        if sym not in tracker or tracker[sym].get("removed"):
            snap = next(s for s in snaps if s.symbol == sym)
            tracker[sym] = {
                "name": snap.name,
                "side": trade_side_label(snap),
                "added_at": now_paris.isoformat(),
                "added_pnl": round(snap.pnl_pct, 4),
                "current_pnl": round(snap.pnl_pct, 4),
                "alert_triggered": False,
                "alert_at": None,
                "alert_day": None,
                "removed": False,
            }

    for sym, entry in list(tracker.items()):
        if entry.get("removed"):
            continue
        snap = next((s for s in snaps if s.symbol == sym), None)
        if snap is None or snap.pnl_pct is None:
            continue
        entry["current_pnl"] = round(snap.pnl_pct, 4)

        if not entry["alert_triggered"] and snap.pnl_pct < CLOSE_THRESHOLD:
            entry["alert_triggered"] = True
            entry["alert_at"]  = now_paris.isoformat()
            entry["alert_day"] = today_str
            close_alerts.append(f"⚠️ CLOSE {trade_side_label(snap)} {snap.name} | PnL {snap.pnl_pct:+.2f}%")

        if entry["alert_triggered"] and entry.get("alert_day"):
            if now_paris.hour >= 23 and today_str == entry["alert_day"]:
                if snap.pnl_pct <= 0:
                    entry["removed"]    = True
                    entry["removed_at"] = now_paris.isoformat()
                else:
                    entry["alert_triggered"] = False
                    entry["alert_at"]  = None
                    entry["alert_day"] = None

    return close_alerts


def update_rank_exit_alerts(
    tracker: dict,
    snaps: list[ForexSnapshot],
    current_ranked: list[ForexSnapshot],
    paris_tz,
) -> list[str]:
    """Alert when a symbol from the previous TOP 15 display drops out on this run."""
    now_paris = datetime.now(paris_tz)
    meta = tracker.setdefault("_meta", {})
    previous_ranked = meta.get("last_ranked_symbols", [])

    current_symbols = {s.symbol for s in current_ranked}
    scanned = {s.symbol: s for s in snaps}
    exit_alerts = []

    for item in previous_ranked:
        sym = item.get("symbol")
        if not sym or sym in current_symbols or sym not in scanned:
            continue

        snap = scanned[sym]
        pnl_txt = "---" if snap.pnl_pct is None else f"{snap.pnl_pct:+.2f}%"
        prev_rank = item.get("rank")
        prev_txt = f" | prev #{prev_rank}" if prev_rank else ""
        exit_alerts.append(f"⚠️ OUT TOP15 {trade_side_label(snap)} {snap.name} | PnL {pnl_txt}{prev_txt}")

    meta["last_ranked_at"] = now_paris.isoformat()
    meta["last_ranked_symbols"] = [
        {
            "symbol": s.symbol,
            "name": s.name,
            "rank": i,
            "pnl": round(s.pnl_pct, 4) if s.pnl_pct is not None else None,
        }
        for i, s in enumerate(current_ranked, 1)
    ]

    return exit_alerts


# ─── Portfolio simulation ────────────────────────────────────────────────────

def trade_direction(s: ForexSnapshot) -> int:
    if s.bias_state_after == 1 and s.aligned == 1:
        return 1
    if s.bias_state_after == -1 and s.aligned == -1:
        return -1
    return 0


def trade_side_label(s: ForexSnapshot) -> str:
    direction = trade_direction(s)
    if direction == 1:
        return "LONG"
    if direction == -1:
        return "SHORT"
    return "MIXED"


def is_trade_candidate(s: ForexSnapshot) -> bool:
    direction = trade_direction(s)
    if direction == 0:
        return False
    if s.streak_w < 1:
        return False
    if not s.index_confirmed:
        return False
    if s.chg_daily_pct is None:
        return False
    if direction == 1 and s.chg_daily_pct <= 0.1:
        return False
    if direction == -1 and s.chg_daily_pct >= -0.1:
        return False
    return True


def compact_symbol(symbol: str) -> str:
    return symbol.split(":", 1)[-1].replace("/", "").replace("_", "").upper()


def oanda_instrument(symbol: str) -> str:
    compact = compact_symbol(symbol)
    if compact == "XAUUSD":
        return "XAU_USD"
    return f"{compact[:3]}_{compact[3:]}"


def pip_size(symbol: str) -> float:
    compact = compact_symbol(symbol)
    if compact.endswith("JPY") or compact == "XAUUSD":
        return 0.01
    return 0.0001


def quote_currency(symbol: str) -> str:
    return compact_symbol(symbol)[-3:]


def quote_to_eur_rate(quote: str, price_map: dict[str, float]) -> float | None:
    quote = quote.upper()
    if quote == ACCOUNT_CURRENCY:
        return 1.0
    eur_quote = f"{ACCOUNT_CURRENCY}{quote}"
    quote_eur = f"{quote}{ACCOUNT_CURRENCY}"
    if eur_quote in price_map and price_map[eur_quote]:
        return 1.0 / float(price_map[eur_quote])
    if quote_eur in price_map and price_map[quote_eur]:
        return float(price_map[quote_eur])
    return None


def load_oanda_spreads(symbols: list[str], max_age_hours: int = 24) -> dict[str, dict]:
    """Fetch OANDA historical spread stats. Values are spreads in pips."""
    now = time.time()
    requested = sorted({compact_symbol(s) for s in symbols})
    cached = _load_json(SPREAD_CACHE_PATH)
    cached_spreads = cached.get("spreads", {}) if cached else {}
    cache_fresh = (
        cached
        and cached.get("division") == OANDA_SPREAD_DIVISION
        and cached.get("field") == OANDA_SPREAD_FIELD
        and cached.get("pricing_model") == OANDA_PRICING_MODEL
        and now - float(cached.get("fetched_at", 0) or 0) < max_age_hours * 3600
    )
    if cache_fresh and all(sym in cached_spreads for sym in requested):
        return cached_spreads

    spreads: dict[str, dict] = dict(cached_spreads) if cache_fresh else {}
    missing = [sym for sym in requested if sym not in spreads]
    for sym in missing:
        inst = oanda_instrument(sym)
        try:
            resp = http_requests.get(
                "https://labs-api.oanda.com/v1/spreads/spread-stability-and-average",
                params={"division": OANDA_SPREAD_DIVISION, "instrument": inst},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            if not resp.ok:
                continue
            payload = resp.json().get("spreadStabilityAndAverages", {})
            last_month = payload.get("spreadStabilityAndAverageLastMonth", {}) or {}
            last_3m = payload.get("spreadStabilityAndAverageLastThreeMonths", {}) or {}
            spread_pips = last_month.get(OANDA_SPREAD_FIELD)
            if spread_pips is None:
                spread_pips = last_month.get("averageBusinessHours")
            if spread_pips is None:
                spread_pips = last_3m.get("averageBusinessHours")
            if spread_pips is None:
                continue
            spreads[sym] = {
                "instrument": payload.get("instrument", inst),
                "spread_pips": float(spread_pips),
                "field": OANDA_SPREAD_FIELD,
                "date": payload.get("date"),
                "last_month": last_month,
                "last_3m": last_3m,
            }
        except Exception:
            continue

    if spreads:
        _save_json(SPREAD_CACHE_PATH, {
            "fetched_at": now,
            "fetched_at_utc": datetime.now(pytz.UTC).isoformat(),
            "division": OANDA_SPREAD_DIVISION,
            "field": OANDA_SPREAD_FIELD,
            "pricing_model": OANDA_PRICING_MODEL,
            "spreads": spreads,
        })
        return spreads
    return cached.get("spreads", {}) if cached else {}


def estimate_execution_cost_eur(
    symbol: str,
    qty: int,
    price_map: dict[str, float],
    spread_map: dict[str, dict],
) -> tuple[float, dict]:
    """Estimate one execution cost in EUR: half spread + optional OANDA Core commission."""
    if qty <= 0:
        return 0.0, {"spread_pips": None, "spread_cost_eur": 0.0, "commission_eur": 0.0}
    compact = compact_symbol(symbol)
    spread_info = spread_map.get(compact, {})
    spread_pips = spread_info.get("spread_pips")
    spread_cost_eur = 0.0
    if spread_pips is not None:
        cost_quote = float(spread_pips) * pip_size(compact) * float(qty) * 0.5
        quote_rate = quote_to_eur_rate(quote_currency(compact), price_map)
        if quote_rate is not None:
            spread_cost_eur = cost_quote * quote_rate
    commission_eur = 0.0
    if OANDA_PRICING_MODEL in {"core", "core_spread", "core_spreads", "commission"}:
        commission_eur = float(qty) / 10000.0 * OANDA_CORE_COMMISSION_EUR_PER_10K
    total = spread_cost_eur + commission_eur
    return total, {
        "spread_pips": spread_pips,
        "spread_cost_eur": spread_cost_eur,
        "commission_eur": commission_eur,
        "pricing_model": OANDA_PRICING_MODEL,
    }


def default_portfolio() -> dict:
    return {
        "initial_capital": PORTFOLIO_INITIAL_CAPITAL,
        "cash": PORTFOLIO_INITIAL_CAPITAL,
        "fees_paid": 0.0,
        "positions": {},
        "history": [],
        "trades": [],
    }


def load_portfolio() -> dict:
    data = _load_json(PORTFOLIO_PATH)
    if not data:
        return default_portfolio()
    base = default_portfolio()
    base.update(data)
    if not isinstance(base.get("positions"), dict):
        base["positions"] = {}
    if not isinstance(base.get("history"), list):
        base["history"] = []
    if not isinstance(base.get("trades"), list):
        base["trades"] = []
    reconstructed_fees = round(
        sum(float(t.get("fee", 0.0) or 0.0) for t in base.get("trades", [])),
        2,
    )
    base["fees_paid"] = max(float(base.get("fees_paid", 0.0) or 0.0), reconstructed_fees)
    if not base.get("first_run"):
        candidates = []
        candidates.extend(str(h.get("time")) for h in base.get("history", []) if h.get("time"))
        candidates.extend(str(t.get("time")) for t in base.get("trades", []) if t.get("time"))
        candidates.extend(str(p.get("entry_time")) for p in base.get("positions", {}).values() if p.get("entry_time"))
        if candidates:
            base["first_run"] = min(candidates)
    return base


def portfolio_equity(portfolio: dict, price_map: dict[str, float]) -> float:
    equity = float(portfolio.get("cash", 0.0) or 0.0)
    for sym, pos in portfolio.get("positions", {}).items():
        px = price_map.get(sym)
        if px is None:
            px = float(pos.get("last_price", 0.0) or 0.0)
        qty = int(pos.get("qty", 0) or 0)
        side = pos.get("side", "LONG")
        avg = float(pos.get("avg_price", px) or px)
        margin = float(pos.get("margin", qty * avg) or 0.0)
        if side == "SHORT":
            equity += margin + (avg - px) * qty
        else:
            equity += qty * px
    return equity


def close_alert_symbols(close_alerts: list[str], snaps: list[ForexSnapshot]) -> set[str]:
    by_name = {s.name: s.symbol for s in snaps}
    symbols = set()
    for alert in close_alerts:
        for marker in ("CLOSE ", "OUT TOP15 "):
            idx = alert.find(marker)
            if idx < 0:
                continue
            name = alert[idx + len(marker):].split("|", 1)[0].strip()
            for prefix in ("LONG ", "SHORT "):
                if name.startswith(prefix):
                    name = name[len(prefix):].strip()
            sym = by_name.get(name)
            if sym:
                symbols.add(sym)
            break
    return symbols


def portfolio_buy_targets(
    positive: list[ForexSnapshot],
    held_symbols: set[str] | None = None,
    limit: int = 10,
    protect_rank: int = 15,
) -> list[ForexSnapshot]:
    """Build portfolio targets, preserving held TOP15 names."""
    held_symbols = held_symbols or set()
    alphabet_symbols = set()
    by_symbol = {s.symbol: s for s in positive}
    rank = {s.symbol: i for i, s in enumerate(positive)}

    protected = {
        s.symbol for s in positive[:protect_rank]
        if s.symbol in held_symbols
    }

    target_symbols: set[str] = set()
    alphabet_selected = False

    def add_symbol(sym: str) -> bool:
        nonlocal alphabet_selected
        snap = by_symbol.get(sym)
        if snap is None or sym in target_symbols:
            return False
        if sym in alphabet_symbols:
            if alphabet_selected:
                return False
            alphabet_selected = True
        target_symbols.add(sym)
        return True

    for snap in positive[:protect_rank]:
        if snap.symbol in protected:
            add_symbol(snap.symbol)
            if len(target_symbols) >= limit:
                break

    for snap in positive:
        if len(target_symbols) >= limit:
            break
        add_symbol(snap.symbol)

    targets = [by_symbol[sym] for sym in target_symbols]
    targets.sort(key=lambda s: rank.get(s.symbol, 10**9))
    return targets[:limit]


def update_portfolio_simulation(
    portfolio: dict,
    buy_targets: list[ForexSnapshot],
    close_alerts: list[str],
    snaps: list[ForexSnapshot],
    now_str: str,
) -> tuple[list[str], dict]:
    price_map = {s.symbol: s.close for s in snaps if s.close is not None}
    price_map.update({compact_symbol(s.symbol): s.close for s in snaps if s.close is not None})
    spread_map = load_oanda_spreads([s.symbol for s in snaps])
    name_map = {s.symbol: s.name for s in snaps}
    snap_map = {s.symbol: s for s in snaps}
    positions = portfolio.setdefault("positions", {})
    if not portfolio.get("first_run"):
        portfolio["first_run"] = now_str
    first_run = str(portfolio.get("first_run") or now_str)
    trades = []

    equity_before = portfolio_equity(portfolio, price_map)

    def sell_position(sym: str, reason: str) -> None:
        pos = positions.get(sym)
        px = price_map.get(sym)
        if not pos or px is None:
            return
        qty = int(pos.get("qty", 0) or 0)
        if qty <= 0:
            return
        avg = float(pos.get("avg_price", px) or px)
        side = pos.get("side", "LONG")
        margin = float(pos.get("margin", qty * avg) or 0.0)
        gross = qty * px
        fee, fee_detail = estimate_execution_cost_eur(sym, qty, price_map, spread_map)
        if side == "SHORT":
            proceeds = max(0.0, margin + (avg - px) * qty - fee)
            realized = (avg - px) * qty - fee
            trade_side = "COVER"
        else:
            proceeds = max(0.0, gross - fee)
            realized = (px - avg) * qty - fee
            trade_side = "SELL"
        portfolio["cash"] = float(portfolio.get("cash", 0.0) or 0.0) + proceeds
        portfolio["fees_paid"] = float(portfolio.get("fees_paid", 0.0) or 0.0) + fee
        positions.pop(sym, None)
        trades.append({
            "time": now_str,
            "side": trade_side,
            "position_side": side,
            "symbol": sym,
            "name": name_map.get(sym, sym),
            "qty": qty,
            "price": round(px, 4),
            "gross": round(gross, 2),
            "fee": round(fee, 4),
            "fee_detail": fee_detail,
            "notional": round(proceeds, 2),
            "realized_pnl": round(realized, 2),
            "cash_after": round(float(portfolio.get("cash", 0.0) or 0.0), 2),
            "reason": reason,
        })

    sell_symbols = close_alert_symbols(close_alerts, snaps)
    over_budget_symbols = set()
    for sym in list(positions.keys()):
        snap = snap_map.get(sym)
        pos_side = positions[sym].get("side", "LONG")
        snap_side = trade_side_label(snap) if snap else "MIXED"
        if snap and snap_side != "MIXED" and snap_side != pos_side:
            sell_symbols.add(sym)
        elif snap:
            rocs = [r for r in (snap.price_roc7, snap.price_roc14, snap.price_roc21) if r is not None]
            if rocs:
                if pos_side == "LONG" and not all(r >= 0 for r in rocs):
                    sell_symbols.add(sym)
                elif pos_side == "SHORT" and not all(r <= 0 for r in rocs):
                    sell_symbols.add(sym)
        margin = float(positions[sym].get("margin", 0.0) or 0.0)
        if margin <= 0:
            qty = int(positions[sym].get("qty", 0) or 0)
            avg = float(positions[sym].get("avg_price", price_map.get(sym, 0.0)) or 0.0)
            margin = qty * avg
        if margin > PORTFOLIO_POSITION_BUDGET:
            sell_symbols.add(sym)
            over_budget_symbols.add(sym)

    def sell_reason(sym: str) -> str:
        snap = snap_map.get(sym)
        pos = positions.get(sym)
        if sym in over_budget_symbols:
            return "BUDGET > 1000"
        if snap and pos:
            pos_side = pos.get("side", "LONG")
            snap_side = trade_side_label(snap)
            if snap_side != "MIXED" and snap_side != pos_side:
                return "SIDE CHANGED"
            rocs = [r for r in (snap.price_roc7, snap.price_roc14, snap.price_roc21) if r is not None]
            if rocs:
                if (pos_side == "LONG" and not all(r >= 0 for r in rocs)) or \
                   (pos_side == "SHORT" and not all(r <= 0 for r in rocs)):
                    return "ROC OPPOSE"
        return "CLOSE ALERT"

    # Close explicit alerts and any open position whose signal or ROC turns against its side.
    for sym in sorted(sell_symbols):
        sell_position(sym, sell_reason(sym))

    # Open missing target symbols with a fixed budget per position.
    missing_top10 = [
        s for s in buy_targets
        if s.symbol not in positions
        and s.close > 0
        and is_trade_candidate(s)
    ]
    cash = float(portfolio.get("cash", 0.0) or 0.0)
    if missing_top10 and cash > 0:
        for s in missing_top10:
            px = float(s.close)
            available = min(PORTFOLIO_POSITION_BUDGET, float(portfolio.get("cash", 0.0) or 0.0))
            qty = int(available // px)
            if qty <= 0:
                continue
            gross = qty * px
            fee, fee_detail = estimate_execution_cost_eur(s.symbol, qty, price_map, spread_map)
            total_cost = gross + fee
            if total_cost > float(portfolio.get("cash", 0.0) or 0.0):
                continue
            positions[s.symbol] = {
                "name": s.name,
                "side": trade_side_label(s),
                "qty": qty,
                "avg_price": px,
                "margin": gross,
                "entry_time": now_str,
                "last_price": px,
            }
            portfolio["cash"] = float(portfolio.get("cash", 0.0) or 0.0) - total_cost
            portfolio["fees_paid"] = float(portfolio.get("fees_paid", 0.0) or 0.0) + fee
            trades.append({
                "time": now_str,
                "side": "OPEN",
                "position_side": trade_side_label(s),
                "symbol": s.symbol,
                "name": s.name,
                "qty": qty,
                "price": round(px, 4),
                "gross": round(gross, 2),
                "fee": round(fee, 4),
                "fee_detail": fee_detail,
                "notional": round(total_cost, 2),
                "cash_after": round(float(portfolio.get("cash", 0.0) or 0.0), 2),
            })

    for sym, pos in positions.items():
        if sym in price_map:
            pos["last_price"] = price_map[sym]

    equity_after = portfolio_equity(portfolio, price_map)
    initial = float(portfolio.get("initial_capital", PORTFOLIO_INITIAL_CAPITAL) or PORTFOLIO_INITIAL_CAPITAL)
    pnl = equity_after - initial
    pnl_pct = pnl / initial * 100 if initial else 0.0
    fees_paid = float(portfolio.get("fees_paid", 0.0) or 0.0)
    gross_pnl = pnl + fees_paid
    fee_drag_pct = fees_paid / initial * 100 if initial else 0.0
    open_rocs = []
    for s in snaps:
        if s.symbol not in positions or s.price_roc7 is None:
            continue
        side = positions[s.symbol].get("side", "LONG")
        open_rocs.append(s.price_roc7 if side == "LONG" else -s.price_roc7)
    avg_roc = sum(open_rocs) / len(open_rocs) if open_rocs else None

    portfolio.setdefault("history", []).append({
        "time": now_str,
        "equity": round(equity_after, 2),
        "cash": round(float(portfolio.get("cash", 0.0) or 0.0), 2),
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl_pct, 2),
        "gross_pnl_before_fees": round(gross_pnl, 2),
        "fees_paid": round(fees_paid, 2),
        "fee_drag_pct": round(fee_drag_pct, 2),
        "positions": len(positions),
    })
    portfolio["history"] = portfolio["history"][-250:]
    portfolio.setdefault("trades", []).extend(trades)
    portfolio["trades"] = portfolio["trades"][-500:]
    portfolio["last_update"] = now_str

    equity_icon = "🟢" if equity_after >= initial else "🔴"
    pnl_icon = "🟢" if pnl >= 0 else "🔴"
    roc_icon = "🟢" if avg_roc is not None and avg_roc >= 0 else "🔴"
    bought_symbols = {t["symbol"] for t in trades if t["side"] == "OPEN"}
    sold_trades = [t for t in trades if t["side"] in ("SELL", "COVER")]
    summary = [
        "💼 FOREX PORTFOLIO SIM",
        f"{equity_icon} Equity {equity_after:.2f}",
        f"{pnl_icon} PnL {pnl:+.2f} ({pnl_pct:+.2f}%)",
        f"📅 First run {first_run}",
        f"💸 Fees {fees_paid:.2f} ({fee_drag_pct:.2f}%) | brut {gross_pnl:+.2f}",
        f"{roc_icon} ROC {avg_roc:+.2f}%" if avg_roc is not None else "⚪ ROC ---",
        f"Cash {float(portfolio.get('cash', 0.0) or 0.0):.2f} | Positions {len(positions)}",
    ]

    ordered_positions = []
    seen_positions = set()
    for target in buy_targets:
        if target.symbol in positions:
            ordered_positions.append((target.symbol, positions[target.symbol]))
            seen_positions.add(target.symbol)
    for sym in sorted(positions):
        if sym not in seen_positions:
            ordered_positions.append((sym, positions[sym]))

    pos_lines = []
    total_pips = 0.0
    for sym, pos in ordered_positions:
        new_txt = " 🆕" if sym in bought_symbols else ""
        side = pos.get("side", "LONG")
        side_icon = "🟢" if side == "LONG" else "🔴"
        entry = float(pos.get("avg_price", 0) or 0)
        last  = float(pos.get("last_price", entry) or entry)
        pip_size = 0.01 if "JPY" in sym or "XAU" in sym else 0.0001
        raw_pips = (last - entry) / pip_size if side == "LONG" else (entry - last) / pip_size
        pips = round(raw_pips, 1)
        total_pips += pips
        pips_txt = f"{pips:+.1f} pips"
        snap = snap_map.get(sym)
        roc7  = snap.price_roc7  if snap else None
        roc14 = snap.price_roc14 if snap else None
        roc21 = snap.price_roc21 if snap else None
        signal_tag = ""
        rocs = [r for r in (roc7, roc14, roc21) if r is not None]
        if rocs:
            is_long = (side == "LONG")
            bulls = [r > 0 for r in rocs]
            aligned_with = [b == is_long for b in bulls]
            score = sum(aligned_with)
            if score == len(rocs):
                signal_tag = " 🔥"
            elif score == 0:
                signal_tag = " ⚠️"
            elif bulls[-1] == is_long:
                signal_tag = " 💚"
            else:
                signal_tag = " 🔶"
        sar_dir = 1 if side == "LONG" else -1
        sar_tag = " ⚡" if sar_h1_breakout(sym, sar_dir) else ""
        pos_lines.append(f"{side_icon}{pos.get('name', sym)} {pips_txt}{new_txt}{signal_tag}{sar_tag}")

    pips_icon = "🟢" if total_pips >= 0 else "🔴"
    summary.insert(3, f"{pips_icon} Pips {total_pips:+.1f}")
    summary += pos_lines

    if sold_trades:
        summary.append("🚨 FOREX CLOSE ALERTS")
        for t in sold_trades:
            reason = f" ({t['reason']})" if t.get("reason") else ""
            summary.append(f"⚠️ {t['name']}{reason}")

    return summary, {
        "equity_before": equity_before,
        "equity_after": equity_after,
        "trades": trades,
    }


# ─── PnL history ─────────────────────────────────────────────────────────────

def update_pnl_history(history: dict, snaps: list, today_str: str) -> None:
    """Append today's PnL% for each symbol (one entry per day, deduplicated)."""
    for s in snaps:
        if s.pnl_pct is None or s.bias_state_after == 0:
            continue
        sym = s.symbol
        entries: list[dict] = history.setdefault(sym, [])
        if entries and entries[-1]["date"] == today_str:
            entries[-1]["pnl"] = round(s.pnl_pct, 4)  # update same-day
        else:
            entries.append({"date": today_str, "pnl": round(s.pnl_pct, 4)})
        # Keep max 60 days
        if len(entries) > 60:
            history[sym] = entries[-60:]


# ─── Telegram ─────────────────────────────────────────────────────────────────

def telegram_window_open(now_paris: datetime) -> bool:
    return 6 <= now_paris.hour < 21


def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = http_requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        return bool(resp.json().get("ok", False))
    except Exception:
        return False


# ─── Args ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Renko W/D + currency index confirmation — OANDA Forex")
    p.add_argument("--length",     type=int,   default=14)
    p.add_argument("--start-date", type=str,   default="2026-04-01",
                   help='Bias start date UTC. Formats: "YYYY-MM-DD" or epoch seconds.')
    p.add_argument("--init",       action="store_true",
                   help="Replay history to initialise bias, then scan.")
    p.add_argument("--no-telegram", action="store_true")
    p.add_argument("--filter-positive", action="store_true",
                   help="Show only instruments with a positive directional PnL%%.")
    p.add_argument("--only", type=str, default="",
                   help="Comma-separated pairs to scan, e.g. NZDJPY,EURNZD.")
    p.add_argument("--allow-weekend", action="store_true",
                   help="Force a live scan on Saturday/Sunday for diagnostics.")
    p.add_argument("--force-telegram", action="store_true",
                   help="Send Telegram messages even outside the 06:00-21:00 Paris window.")
    p.add_argument("--debug",      action="store_true")
    return p.parse_args()


def _parse_date_utc(value: str) -> int | None:
    v = (value or "").strip()
    if not v:
        return None
    if v.isdigit():
        return int(v)
    try:
        fmt = "%Y-%m-%d %H:%M" if len(v) > 10 else "%Y-%m-%d"
        return int(datetime.strptime(v, fmt).replace(tzinfo=pytz.UTC).timestamp())
    except Exception:
        return None


def _is_us_equity_weekend(now_ny: datetime) -> bool:
    """US equities are closed all day Saturday/Sunday in New York time."""
    return now_ny.weekday() >= 5


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_args()
    ny_tz = pytz.timezone("America/New_York")
    now_ny = datetime.now(ny_tz)
    if _is_us_equity_weekend(now_ny) and not args.allow_weekend:
        now_paris = datetime.now(pytz.timezone("Europe/Paris"))
        print(
            "US equities closed: weekend in New York "
            f"({now_ny.strftime('%Y-%m-%d %H:%M')} NY / {now_paris.strftime('%Y-%m-%d %H:%M')} Paris). "
            "Skipping live Forex scan. Use --allow-weekend to force it."
        )
        return 0

    start_date_utc = _parse_date_utc(args.start_date)
    if args.start_date and start_date_utc is None:
        print('Invalid --start-date.')
        return 2

    symbols = FOREX_ASSETS
    if args.only:
        wanted = {x.strip().upper() for x in args.only.split(",") if x.strip()}
        symbols = [(tv_sym, name) for tv_sym, name in FOREX_ASSETS if name.upper() in wanted]
    print(f"Forex universe loaded: {len(symbols)} symbols.")
    print("Loading currency index W/D states...")
    index_state_map = fetch_index_state_map(args.length, debug=args.debug)

    state   = _load_json(STATE_PATH)
    tracker = _load_json(TRACKER_PATH)
    portfolio = load_portfolio()

    # Auto-init if start_date changed
    stored_global_start = state.get("_meta", {}).get("start_date_utc")
    if not args.init and start_date_utc is not None and stored_global_start != start_date_utc:
        print(f"Start date changed → auto-init (start={args.start_date})...")
        args.init = True

    if args.init:
        print(f"Initialising bias from history (start={args.start_date}) for {len(symbols)} symbols...")
        for tv_sym, name in symbols:
            print(f"  {name}...", end=" ", flush=True)
            result = init_bias_from_history(tv_sym, start_date_utc, args.length, debug=args.debug)
            if result:
                state.setdefault(tv_sym, {}).update(result)
                state[tv_sym]["startDate"]  = start_date_utc
                state[tv_sym]["updated_at"] = datetime.now(pytz.UTC).isoformat()
                label = "BULL" if result["biasState"] == 1 else "BEAR" if result["biasState"] == -1 else "MIXED"
                last_dt = ""
                if result.get("lastEventTime"):
                    last_dt = " @ " + datetime.fromtimestamp(result["lastEventTime"], tz=pytz.UTC).strftime("%Y-%m-%d")
                print(f"{label}{last_dt} entry={result.get('biasEntry')}")
            else:
                print("no data")
        state.setdefault("_meta", {})["start_date_utc"] = start_date_utc
        _save_json(STATE_PATH, state)
        print("Done. Continuing with scan...\n")

    # ── Scan — moderate parallelism to avoid TradingView timeouts ──
    snaps: list[ForexSnapshot] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(scan_symbol, tv_sym, name, args.length, start_date_utc, state, index_state_map, args.debug): (tv_sym, name)
            for tv_sym, name in symbols
        }
        for future, (tv_sym, name) in futures.items():
            try:
                snap = future.result(timeout=45)
            except FuturesTimeoutError:
                future.cancel()
                print(f"  {name}: timeout (>45s) — skipped")
                continue
            except Exception as e:
                if args.debug:
                    print(f"  {name}: error — {e}")
                continue
            if not snap:
                if args.debug:
                    print(f"  {name}: no data")
                continue
            snaps.append(snap)

    snaps.sort(key=lambda s: s.pnl_pct if s.pnl_pct is not None else float("-inf"), reverse=True)

    paris_tz  = pytz.timezone("Europe/Paris")
    now_paris = datetime.now(paris_tz)
    today_str = now_paris.strftime("%Y-%m-%d")
    now_str   = now_paris.strftime("%Y-%m-%d %H:%M")
    can_send_telegram = args.force_telegram or telegram_window_open(now_paris)
    if not portfolio.get("first_run"):
        portfolio["first_run"] = now_str
    portfolio_first_run = str(portfolio.get("first_run") or now_str)

    # ── PnL history ──
    history = _load_json(HISTORY_PATH)
    update_pnl_history(history, snaps, today_str)
    _save_json(HISTORY_PATH, history)

    positive = sorted(
        [s for s in snaps if is_trade_candidate(s)],
        key=lambda s: s.pnl_pct if s.pnl_pct is not None else float("-inf"),
        reverse=True,
    )
    top10  = positive[:10]
    top_15 = positive[10:15]
    current_ranked = top10 + top_15
    held_symbols = set(portfolio.get("positions", {}).keys())
    buy_targets = portfolio_buy_targets(positive, held_symbols=held_symbols, limit=10)

    def _price_roc_tag(s: ForexSnapshot) -> tuple[float | None, str]:
        roc = s.price_roc7
        if roc is None:
            return None, ""
        direction = trade_direction(s)
        if abs(roc) >= 50:
            return roc, " 🔥"
        if direction == -1:
            return roc, " 📉" if roc <= 0 else " ⚠️"
        return roc, " ⚠️" if roc < 0 else " 📈"

    def _streak_fmt(n: int, px: int) -> str:
        if n == 0 and px == 1:
            return "↑"   # brick rouge absorbé — prix au-dessus
        return str(n)

    def _streak_str(s) -> str:
        w = _streak_fmt(s.streak_m,  s.px_w1)
        d = _streak_fmt(s.streak_w,  s.px_d1)
        return f" [{w}W/{d}D]"

    # ── TOP 10 ──
    lines_top10 = []
    weakening = []
    for i, s in enumerate(top10, 1):
        dot    = "🟢" if s.bias_state_after == 1 else "🔴"
        roc, tag = _price_roc_tag(s)
        entry = tracker.get(s.symbol, {})
        added_day = (entry.get("added_at") or "")[:10]
        new_tag = " 🆕" if added_day == today_str else ""
        side = trade_side_label(s)
        lines_top10.append(f"{i:>2}. {dot} {s.name} (+{s.pnl_pct:.2f}%){new_tag}{tag}")
        if roc is not None and roc < 0:
            weakening.append(s)

    # ── TOP 11-15 ──
    lines_watch = []
    rising = []
    for i, s in enumerate(top_15, 11):
        dot    = "🟢" if s.bias_state_after == 1 else "🔴"
        roc, tag = _price_roc_tag(s)
        side = trade_side_label(s)
        lines_watch.append(f"{i:>2}. {dot} {side} {s.name} (+{s.pnl_pct:.2f}%){tag}")
        if roc is not None and roc > 0:
            rising.append((s, roc))

    # ── Strong momentum outside TOP 10 ──
    strong_momentum = []
    top10_symbols = {s.symbol for s in top10}
    for s in positive:
        if s.symbol in top10_symbols:
            continue
        roc, tag = _price_roc_tag(s)
        if roc is not None and roc >= 50:
            strong_momentum.append((s, roc, tag))

    strong_momentum.sort(key=lambda x: x[1], reverse=True)
    lines_momentum = [
        f"{trade_side_label(s)} {s.name} (+{s.pnl_pct:.2f}%){tag}"
        for s, _roc, tag in strong_momentum[:5]
    ]

    # ── ROTATIONS ──
    rotation_lines = []
    if weakening and rising:
        rising_sorted = sorted(rising, key=lambda x: x[1], reverse=True)
        for weak in weakening:
            _, weak_tag = _price_roc_tag(weak)
            candidate, roc_up = rising_sorted[0]
            _, up_tag = _price_roc_tag(candidate)
            rotation_lines.append(
                f"🔄 {trade_side_label(candidate)} {candidate.name} (+{candidate.pnl_pct:.2f}%){up_tag}"
                f" → remplace {trade_side_label(weak)} {weak.name} (+{weak.pnl_pct:.2f}%){weak_tag}"
            )

    # ── Build message ──
    parts = []
    if lines_top10:
        parts.append("📊 FOREX TOP 10\n" + "\n".join(lines_top10))
    if lines_watch:
        parts.append("👀 SURVEILLANCE (11-15)\n" + "\n".join(lines_watch))
    if lines_momentum:
        parts.append("🔥 DYNAMIQUE FORTE HORS TOP 10\n" + "\n".join(lines_momentum))
    if rotation_lines:
        parts.append("🔄 ROTATIONS À SURVEILLER\n" + "\n".join(rotation_lines))

    if parts:
        msg = "\n\n".join(parts) + f"\n\n📅 First run {portfolio_first_run}\n⏰ {now_str} Paris"
        print(f"\n{msg}\n")

    # ── Detailed output ──
    if not args.filter_positive:
        for s in snaps:
            ts       = datetime.fromtimestamp(s.ts_utc, tz=pytz.UTC).strftime("%Y-%m-%d")
            pnl_txt  = "---" if s.pnl_pct is None else f"{s.pnl_pct:+.2f}%"
            bias_txt = "BULL" if s.bias_state_after == 1 else "BEAR" if s.bias_state_after == -1 else "MIXED"
            side_txt = trade_side_label(s)
            last_txt = "---"
            if s.last_event_time and s.last_event_price is not None:
                last_dt  = datetime.fromtimestamp(s.last_event_time, tz=pytz.UTC).strftime("%Y-%m-%d")
                last_txt = f"{last_dt}@{s.last_event_price:.2f}"
            roc, tag = _price_roc_tag(s)
            roc_str = f" ROC7{roc:+.2f}%" if roc is not None else ""
            chg_str = "---" if s.chg_daily_pct is None else f"{s.chg_daily_pct:+.2f}%"
            base, quote = split_pair(s.symbol)
            idx_txt = f"{base}{s.base_index_state[0]:+d}/{s.base_index_state[1]:+d} {quote}{s.quote_index_state[0]:+d}/{s.quote_index_state[1]:+d}"
            idx_ok = "IDX OK" if s.index_confirmed else "IDX --"
            print(f"{s.name:<6} | close={s.close:>10.2f} | W={s.px_w1:+d}({s.streak_m}) D={s.px_d1:+d}({s.streak_w}) | "
                  f"Side {side_txt} | "
                  f"Bias {bias_txt} | {idx_ok} {idx_txt} | CHG D {chg_str} | PnL {pnl_txt}{roc_str}{tag} | Last {last_txt} | {ts}")

    # ── SAR H1 retroaction — crossover/crossunder dans les 24 dernières heures ──
    print("\n📡 SAR H1 — événements des 24 dernières heures (paires candidates)")
    for s in snaps:
        if not is_trade_candidate(s):
            continue
        direction = trade_direction(s)
        events = sar_h1_cross_events_24h(s.symbol, direction, paris_tz)
        if not events:
            print(f"  {s.name:<6} | aucun événement SAR H1 dans les 24h")
            continue
        for ev in events:
            cross_type = "crossover ↑" if ev["type"] == "crossover" else "crossunder ↓"
            print(f"  {s.name:<6} | {ev['time_paris']} Paris | {cross_type} | SAR={ev['sar']:.5f} | close={ev['close']:.5f}")

    # ── Tracker + close alerts ──
    close_alerts = update_tracker(tracker, snaps, paris_tz)
    close_alerts.extend(update_rank_exit_alerts(tracker, snaps, current_ranked, paris_tz))
    _save_json(TRACKER_PATH, tracker)

    if close_alerts:
        close_msg = "🚨 FOREX CLOSE ALERTS\n" + "\n".join(close_alerts) + f"\n\n⏰ {now_str} Paris"
        print(f"\n{close_msg}\n")

    index_lines = scan_currency_indices(args.length, debug=args.debug)
    index_chg_lines = scan_currency_index_daily_chg_extremes(debug=args.debug)
    extreme_pair_lines = scan_extreme_index_pair_confirmation(snaps, debug=args.debug)

    eligible_lines = []
    for i, s in enumerate(positive, 1):
        side = trade_side_label(s)
        icon = "🟢" if side == "LONG" else "🔴"
        chg_str = "---" if s.chg_daily_pct is None else f"{s.chg_daily_pct:+.2f}%"
        roc_str = "" if s.price_roc7 is None else f" ROC7{s.price_roc7:+.2f}%"
        pnl_str = "---" if s.pnl_pct is None else f"{s.pnl_pct:+.2f}%"
        eligible_lines.append(
            f"{i}. {icon} {side} {s.name} | PnL {pnl_str} | CHG D {chg_str}{roc_str}"
        )

    portfolio_lines = ["🎯 PAIRES ÉLIGIBLES"] + (eligible_lines if eligible_lines else ["(aucune)"])
    if index_lines:
        express_idx = next((i for i, line in enumerate(index_lines) if line == "⚡️ EXPRESS"), None)
        if express_idx is not None:
            index_lines = index_lines[:express_idx]
            while index_lines and index_lines[-1] == "":
                index_lines.pop()
        portfolio_lines += ["", "📊 INDICES FOREX"] + index_lines
    if index_chg_lines:
        portfolio_lines += ["", "🔥 CHG% DAILY INDICES"] + index_chg_lines
    if extreme_pair_lines:
        portfolio_lines += extreme_pair_lines

    if portfolio_lines:
        portfolio_msg = "\n".join(portfolio_lines) + f"\n\n⏰ {now_str} Paris"
        print(f"\n{portfolio_msg}\n")
        if not args.no_telegram and can_send_telegram:
            send_telegram(portfolio_msg)
        elif not args.no_telegram:
            print("Telegram skipped: outside 06:00-21:00 Paris window. Use --force-telegram to override.")

    _save_json(STATE_PATH, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

