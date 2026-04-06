#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
px_renko_screener_index.py

Python replica of px_renko_alignment_index.pine (100% faithful).

For each of 8 currency indices (DXY, EXY, BXY, JXY, SXY, CXY, AXY, ZXY):
  1. Fetch Renko D1 + W1 bricks → px_state (+1/>close, -1/<open, 0/inside)
  2. Fetch Daily close + prev close → CHG%
  3. Fetch OHLC on chosen timeframe → compute Parabolic SAR state
  4. Compute bias (BULL/BEAR), trigger (LONG/SHORT)
  5. Weighted score = (D1 + W1 + CHG% + SAR) × |CHG%|
  6. Alert: SHORT, BEAR (bias+SAR below), LONG, BULL (bias+SAR above)
"""

import argparse
import json
import os
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta

import pytz
import requests as http_requests
from dotenv import load_dotenv
from websocket import WebSocketConnectionClosedException, create_connection

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCAN_OUTPUT_PATH = os.path.join(SCRIPT_DIR, "px_renko_index_scan.json")

# ── constants ──────────────────────────────────────────────────────────
WS_URL = "wss://prodata.tradingview.com/socket.io/websocket"
WS_HEADERS = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

MIN_CHG_PCT = 0.15
SAR_START = 0.1
SAR_INCREMENT = 0.1
SAR_MAXIMUM = 0.2

INDICES = {
    "DXY": ("TVC:DXY", "USD"),
    "EXY": ("TVC:EXY", "EUR"),
    "BXY": ("TVC:BXY", "GBP"),
    "JXY": ("TVC:JXY", "JPY"),
    "SXY": ("TVC:SXY", "CHF"),
    "CXY": ("TVC:CXY", "CAD"),
    "AXY": ("TVC:AXY", "AUD"),
    "ZXY": ("TVC:ZXY", "NZD"),
}

INDEX_ORDER = ["DXY", "EXY", "BXY", "JXY", "SXY", "CXY", "AXY", "ZXY"]

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURNZD", "EURCHF",
    "GBPAUD", "GBPCAD", "GBPNZD", "GBPCHF",
    "AUDNZD", "AUDCAD", "AUDCHF",
    "NZDCAD", "NZDCHF",
    "CADCHF","XAUUSD"
]
PAIR_SET = set(PAIRS)


# ── dataclass ──────────────────────────────────────────────────────────
@dataclass
class IndexResult:
    name: str           # e.g. "AXY"
    ccy: str            # e.g. "AUD"
    px_d1: int          # +1, -1, 0
    px_w1: int          # +1, -1, 0
    chg_pct: float      # daily CHG%
    sar_state: int      # +2=xover, -2=xunder, +1=above, -1=below, 0=on
    bias: int           # +1=BULL, -1=BEAR, 0=neutral
    trigger: int        # +1=LONG, -1=SHORT, 0=none
    raw_score: int      # D1+W1+CHG%+SAR (-4 to +4)
    weighted_score: float  # raw × |CHG%|
    bl_confirmed: bool = False


@dataclass
class PairResult:
    pair: str             # e.g. "USDCHF"
    expected_bias: int    # +1=BULL, -1=BEAR (from index logic)
    strong_ccy: str       # TOP currency
    weak_ccy: str         # LAST currency
    px_d1: int
    px_w1: int
    chg_pct: float
    sar_state: int
    bias: int             # +1=BULL, -1=BEAR, 0=neutral
    trigger: int          # +1=LONG, -1=SHORT, 0=none
    raw_score: int
    weighted_score: float
    sar15_event: str | None = None
    sar15_value: float | None = None
    sar15_time: datetime | None = None
    sar15_note: str | None = None
    sar15_confirmed: bool = False
    bl_confirmed: bool = False


# ── TradingView websocket helpers ─────────────────────────────────────
def _gen_sid():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


def _msg(func, args):
    payload = json.dumps({"m": func, "p": args})
    return f"~m~{len(payload)}~m~{payload}"


def _frames(raw):
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
        out.append(raw[i : i + size])
        i += size
    return out if out else [raw]


def fetch_tv_ohlc(symbol: str, interval: str, n_candles: int,
                   timeout_s: int = 20, retries: int = 2) -> list[dict] | None:
    """Fetch OHLC bars from TradingView. Returns list of {time,open,high,low,close} or None."""
    for attempt in range(retries + 1):
        ws = None
        try:
            ws = create_connection(WS_URL, header=WS_HEADERS, timeout=timeout_s)
            sid = _gen_sid()
            ws.send(_msg("chart_create_session", [sid, ""]))
            ws.send(_msg("resolve_symbol", [
                sid, "sds_sym_1",
                f'={{"symbol":"{symbol}","adjustment":"splits","session":"regular"}}'
            ]))
            ws.send(_msg("create_series", [sid, "sds_1", "s1", "sds_sym_1", interval, n_candles, ""]))

            points = []
            t0 = time.time()
            while time.time() - t0 < 14:
                try:
                    raw = ws.recv()
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
                    rows.append({
                        "time": v[0],
                        "open": v[1],
                        "high": v[2],
                        "low": v[3],
                        "close": v[4],
                    })
            if rows:
                return rows

        except Exception:
            if attempt >= retries:
                return None
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
    return None


def fetch_tv_renko_ohlc(symbol: str, interval: str, atr_length: int = 14,
                         n_bricks: int = 5, timeout_s: int = 15, retries: int = 2) -> dict | None:
    """Return last Renko brick as dict {open, high, low, close}, or None."""
    for attempt in range(retries + 1):
        ws = None
        try:
            ws = create_connection(WS_URL, header=WS_HEADERS, timeout=timeout_s)
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

            points = []
            t0 = time.time()
            while time.time() - t0 < 12:
                try:
                    raw = ws.recv()
                except WebSocketConnectionClosedException:
                    break
                for frame in _frames(raw):
                    if frame in ("~h", "h"):
                        ws.send("~h")
                        continue
                    if '"m":"symbol_error"' in frame:
                        return None
                    if '"m":"timescale_update"' in frame:
                        payload = json.loads(frame)
                        series = payload.get("p", [None, {}])[1]
                        if isinstance(series, dict) and "sds_1" in series:
                            points = series["sds_1"].get("s", []) or points
                    if "series_completed" in frame:
                        break
                if "series_completed" in raw:
                    break

            if points:
                for item in reversed(points):
                    v = item.get("v", [])
                    if len(v) >= 5:
                        return {"open": v[1], "high": v[2], "low": v[3], "close": v[4]}
        except Exception:
            if attempt >= retries:
                return None
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
    return None


# ── Parabolic SAR ─────────────────────────────────────────────────────
def compute_parabolic_sar(bars: list[dict], start: float = 0.1,
                           increment: float = 0.1, maximum: float = 0.2) -> int:
    """
    Compute Parabolic SAR on OHLC bars.
    Returns SAR state on last bar:
      +2 = crossover, -2 = crossunder, +1 = above, -1 = below, 0 = on
    Matches Pine Script ta.sar(start, increment, maximum).
    """
    n = len(bars)
    if n < 3:
        return 0

    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    closes = [b["close"] for b in bars]

    sar = [0.0] * n
    af = start
    is_long = closes[1] > closes[0]

    if is_long:
        ep = highs[0]
        sar[0] = lows[0]
    else:
        ep = lows[0]
        sar[0] = highs[0]

    for i in range(1, n):
        prev_sar = sar[i - 1]
        sar[i] = prev_sar + af * (ep - prev_sar)

        if is_long:
            # Clamp SAR below previous lows
            if i >= 2:
                sar[i] = min(sar[i], lows[i - 1], lows[i - 2])
            else:
                sar[i] = min(sar[i], lows[i - 1])

            if lows[i] < sar[i]:
                # Reversal → downtrend
                is_long = False
                sar[i] = ep
                ep = lows[i]
                af = start
            else:
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + increment, maximum)
        else:
            # Clamp SAR above previous highs
            if i >= 2:
                sar[i] = max(sar[i], highs[i - 1], highs[i - 2])
            else:
                sar[i] = max(sar[i], highs[i - 1])

            if highs[i] > sar[i]:
                # Reversal → uptrend
                is_long = True
                sar[i] = ep
                ep = highs[i]
                af = start
            else:
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + increment, maximum)

    # Determine state on last bar
    c_last, sar_last = closes[-1], sar[-1]
    c_prev, sar_prev = closes[-2], sar[-2]

    cross_over = c_prev <= sar_prev and c_last > sar_last
    cross_under = c_prev >= sar_prev and c_last < sar_last

    if cross_over:
        return 2
    elif cross_under:
        return -2
    elif c_last > sar_last:
        return 1
    elif c_last < sar_last:
        return -1
    return 0


def compute_parabolic_sar_series(bars: list[dict], start: float = 0.1,
                                 increment: float = 0.1, maximum: float = 0.2) -> list[float]:
    """Return the full Parabolic SAR series for the provided OHLC bars."""
    n = len(bars)
    if n == 0:
        return []
    if n == 1:
        return [bars[0]["low"]]

    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    closes = [b["close"] for b in bars]

    sar = [0.0] * n
    af = start
    is_long = closes[1] > closes[0]

    if is_long:
        ep = highs[0]
        sar[0] = lows[0]
    else:
        ep = lows[0]
        sar[0] = highs[0]

    for i in range(1, n):
        prev_sar = sar[i - 1]
        sar[i] = prev_sar + af * (ep - prev_sar)

        if is_long:
            if i >= 2:
                sar[i] = min(sar[i], lows[i - 1], lows[i - 2])
            else:
                sar[i] = min(sar[i], lows[i - 1])

            if lows[i] < sar[i]:
                is_long = False
                sar[i] = ep
                ep = lows[i]
                af = start
            else:
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + increment, maximum)
        else:
            if i >= 2:
                sar[i] = max(sar[i], highs[i - 1], highs[i - 2])
            else:
                sar[i] = max(sar[i], highs[i - 1])

            if highs[i] > sar[i]:
                is_long = True
                sar[i] = ep
                ep = highs[i]
                af = start
            else:
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + increment, maximum)

    return sar


def compute_break_line_confirmed(bars: list[dict], bias: int,
                                  sar_start: float, sar_inc: float, sar_max: float) -> bool:
    """Return True if price confirms break line direction.
    BULL: current price > bear trend line (red line).
    BEAR: current price < bull trend line (green line).
    Lines are drawn between the last 2 valid SAR cross events of each direction.
    """
    if not bars or len(bars) < 5 or bias == 0:
        return False

    sar = compute_parabolic_sar_series(bars, start=sar_start, increment=sar_inc, maximum=sar_max)
    closes = [b["close"] for b in bars]
    n = len(bars)

    last_bull_line: tuple[int, float, int, float] | None = None
    last_bear_line: tuple[int, float, int, float] | None = None
    bull_events: list[tuple[int, float]] = []
    bear_events: list[tuple[int, float]] = []

    for i in range(1, n):
        c_prev, sar_prev = closes[i - 1], sar[i - 1]
        c_last, sar_last = closes[i], sar[i]
        if c_prev <= sar_prev and c_last > sar_last:
            bull_events.append((i, sar_last))
            if len(bull_events) >= 2:
                (x1, y1), (x2, y2) = bull_events[-2], bull_events[-1]
                if y2 > y1:  # ascending bull line only
                    last_bull_line = (x1, y1, x2, y2)
        elif c_prev >= sar_prev and c_last < sar_last:
            bear_events.append((i, sar_last))
            if len(bear_events) >= 2:
                (x1, y1), (x2, y2) = bear_events[-2], bear_events[-1]
                if y2 < y1:  # descending bear line only
                    last_bear_line = (x1, y1, x2, y2)

    def project_line(line: tuple[int, float, int, float] | None) -> float | None:
        if line is None:
            return None
        x1, y1, x2, y2 = line
        if x2 == x1:
            return None
        return y1 + (y2 - y1) * (n - 1 - x1) / (x2 - x1)

    current_close = closes[-1]

    if bias == 1:  # BULL: price must be above red line (bear trend line)
        bear_line = project_line(last_bear_line)
        return bear_line is not None and current_close > bear_line
    else:          # BEAR: price must be below green line (bull trend line)
        bull_line = project_line(last_bull_line)
        return bull_line is not None and current_close < bull_line


def sar_event_cutoff_paris(now_paris: datetime | None = None) -> datetime:
    tz = pytz.timezone("Europe/Paris")
    now_paris = now_paris or datetime.now(tz)
    # Start of current trading day at 00:00 Paris.
    # On weekends (Sat=5, Sun=6), roll back to last Friday.
    base = now_paris.replace(hour=0, minute=0, second=0, microsecond=0)
    if base.weekday() == 5:   # Saturday → Friday
        base -= timedelta(days=1)
    elif base.weekday() == 6:  # Sunday → Friday
        base -= timedelta(days=2)
    return base


def parse_paris_datetime(value: str) -> datetime:
    """Parse YYYY-MM-DD HH:MM as Europe/Paris local time."""
    tz = pytz.timezone("Europe/Paris")
    dt = datetime.strptime(value, "%Y-%m-%d %H:%M")
    return tz.localize(dt)


def format_sar15_event(pair: PairResult) -> str:
    if pair.sar15_note:
        return pair.sar15_note
    if pair.sar15_value is None or pair.sar15_time is None or not pair.sar15_event:
        return "n/a"
    stamp = pair.sar15_time.strftime("%H:%M")
    return f"{pair.sar15_event} {pair.sar15_value:.5f} @{stamp}"


def find_first_sar15_event(pair: str, weighted_score: float, sar_start: float,
                           sar_inc: float, sar_max: float,
                           cutoff_override: datetime | None = None,
                           tz_name: str = "Europe/Paris") -> tuple[str | None, float | None, datetime | None, str | None, bool]:
    """Find first 15m SAR cross event since cutoff, based on score sign.
    Also checks if the current (last) bar still confirms the direction.
    Returns (event, value, time, note, confirmed)."""
    if weighted_score == 0:
        return None, None, None, "score=0", False

    tz = pytz.timezone(tz_name)
    now_paris = datetime.now(tz)
    cutoff = cutoff_override.astimezone(tz) if cutoff_override else sar_event_cutoff_paris(now_paris)
    # Parabolic SAR is path-dependent. Load a large warm-up window so the
    # computed SAR matches TradingView more closely before scanning from cutoff.
    minutes_back = max(180, int((now_paris - cutoff).total_seconds() // 60) + 60)
    n_candles = max(300, min(500, minutes_back // 15 + 200))

    bars = fetch_tv_ohlc(f"OANDA:{pair}", "15", n_candles)
    if not bars or len(bars) < 3:
        return None, None, None, "no 15m data", False

    sar = compute_parabolic_sar_series(bars, start=sar_start, increment=sar_inc, maximum=sar_max)
    wanted = "X Over" if weighted_score > 0 else "X Under"

    # Find the most extreme cross in the signal direction since cutoff:
    # BULL → X Over with lowest SAR value (strongest support floor)
    # BEAR → X Under with highest SAR value (strongest resistance ceiling)
    extreme_sar: float | None = None
    extreme_ts: datetime | None = None

    for i in range(1, len(bars)):
        ts = datetime.fromtimestamp(bars[i]["time"], tz=pytz.UTC).astimezone(tz)
        if ts < cutoff:
            continue

        c_prev, sar_prev = bars[i - 1]["close"], sar[i - 1]
        c_last, sar_last = bars[i]["close"], sar[i]
        cross_over = c_prev <= sar_prev and c_last > sar_last
        cross_under = c_prev >= sar_prev and c_last < sar_last

        if wanted == "X Over" and cross_over:
            if extreme_sar is None or sar_last < extreme_sar:
                extreme_sar = sar_last
                extreme_ts = ts
        elif wanted == "X Under" and cross_under:
            if extreme_sar is None or sar_last > extreme_sar:
                extreme_sar = sar_last
                extreme_ts = ts

    if extreme_sar is None:
        return None, None, None, "no cross since cutoff", False

    # Confirmation: current price still on the right side of the extreme SAR level
    current_close = bars[-1]["close"]
    confirmed = (current_close > extreme_sar) if weighted_score > 0 else (current_close < extreme_sar)

    return wanted, extreme_sar, extreme_ts, None, confirmed


def list_sar15_events(pair: str, sar_start: float, sar_inc: float, sar_max: float,
                      cutoff_override: datetime | None = None,
                      tz_name: str = "Europe/Paris") -> tuple[list[tuple[str, float, datetime]], str | None]:
    """List all 15m SAR cross events since cutoff for one pair."""
    tz = pytz.timezone(tz_name)
    now_paris = datetime.now(tz)
    cutoff = cutoff_override.astimezone(tz) if cutoff_override else sar_event_cutoff_paris(now_paris)
    minutes_back = max(180, int((now_paris - cutoff).total_seconds() // 60) + 60)
    n_candles = max(300, min(500, minutes_back // 15 + 200))

    bars = fetch_tv_ohlc(f"OANDA:{pair}", "15", n_candles)
    if not bars or len(bars) < 3:
        return [], "no 15m data"

    sar = compute_parabolic_sar_series(bars, start=sar_start, increment=sar_inc, maximum=sar_max)
    events: list[tuple[str, float, datetime]] = []

    for i in range(1, len(bars)):
        ts = datetime.fromtimestamp(bars[i]["time"], tz=pytz.UTC).astimezone(tz)
        if ts < cutoff:
            continue

        c_prev, sar_prev = bars[i - 1]["close"], sar[i - 1]
        c_last, sar_last = bars[i]["close"], sar[i]
        cross_over = c_prev <= sar_prev and c_last > sar_last
        cross_under = c_prev >= sar_prev and c_last < sar_last

        if cross_over:
            events.append(("X Over", sar_last, ts))
        if cross_under:
            events.append(("X Under", sar_last, ts))

    return events, None


def print_sar15_events_for_pairs(pairs: list[PairResult], sar_start: float, sar_inc: float,
                                 sar_max: float, cutoff_override: datetime | None = None,
                                 tz_name: str = "Europe/Paris") -> None:
    """Print all detected SAR15 cross events for each pair in the list."""
    if not pairs:
        return

    print("\n  SAR15 EVENTS BY PAIR")
    print("  " + "-" * 88)
    for pair_result in pairs:
        events, note = list_sar15_events(
            pair_result.pair,
            sar_start=sar_start,
            sar_inc=sar_inc,
            sar_max=sar_max,
            cutoff_override=cutoff_override,
            tz_name=tz_name,
        )
        print(f"  {pair_result.pair} ({score_str(pair_result.weighted_score)})")
        if note:
            print(f"    {note}")
            continue
        if not events:
            print("    no cross since cutoff")
            continue
        for event, sar_value, ts in events:
            print(f"    {ts.strftime('%Y-%m-%d %H:%M')} | {event:<7} | SAR {sar_value:.5f}")


def debug_sar15_pair(pair: str, sar_start: float, sar_inc: float, sar_max: float,
                     cutoff_override: datetime | None = None,
                     tz_name: str = "Europe/Paris") -> None:
    """Print 15m bars, SAR values, and detected crosses for one pair."""
    tz = pytz.timezone(tz_name)
    now_paris = datetime.now(tz)
    cutoff = cutoff_override.astimezone(tz) if cutoff_override else sar_event_cutoff_paris(now_paris)
    minutes_back = max(180, int((now_paris - cutoff).total_seconds() // 60) + 60)
    n_candles = max(300, min(500, minutes_back // 15 + 200))

    bars = fetch_tv_ohlc(f"OANDA:{pair}", "15", n_candles)
    print(f"\n  SAR15 DEBUG {pair}")
    print("  " + "-" * 110)
    print(f"  Cutoff: {cutoff.strftime('%Y-%m-%d %H:%M %Z')}")
    if not bars or len(bars) < 3:
        print("  no 15m data")
        return

    sar = compute_parabolic_sar_series(bars, start=sar_start, increment=sar_inc, maximum=sar_max)
    print(f"  {'Time':<18} {'Close':<12} {'SAR':<12} {'State':<8} {'Cross':<10}")
    print("  " + "-" * 110)

    rows_printed = 0
    for i in range(1, len(bars)):
        ts = datetime.fromtimestamp(bars[i]["time"], tz=pytz.UTC).astimezone(tz)
        if ts < cutoff:
            continue

        c_prev, sar_prev = bars[i - 1]["close"], sar[i - 1]
        c_last, sar_last = bars[i]["close"], sar[i]
        cross_over = c_prev <= sar_prev and c_last > sar_last
        cross_under = c_prev >= sar_prev and c_last < sar_last

        state = "above" if c_last > sar_last else "below" if c_last < sar_last else "on"
        cross = "X Over" if cross_over else "X Under" if cross_under else ""
        print(f"  {ts.strftime('%Y-%m-%d %H:%M'):<18} {c_last:<12.5f} {sar_last:<12.5f} {state:<8} {cross:<10}")
        rows_printed += 1

    if rows_printed == 0:
        print("  no rows since cutoff")


# ── Core logic (mirrors Pine f_px_state / f_trigger / f_score) ────────
def px_state(renko_open: float, renko_close: float, price: float) -> int:
    """Price vs Renko brick: +1 = >close, -1 = <open, 0 = inside."""
    h = max(renko_open, renko_close)
    l = min(renko_open, renko_close)
    if price > h:
        return 1
    elif price < l:
        return -1
    return 0


def compute_trigger(d1_state: int, w1_state: int, chg_pct: float,
                     sar_st: int, min_chg: float) -> tuple[int, int]:
    """Returns (trigger, bias). Mirrors Pine f_trigger."""
    chg_ok = abs(chg_pct) > min_chg
    chg_dir = (d1_state == 1 and chg_pct > 0) or (d1_state == -1 and chg_pct < 0)
    bull = d1_state == 1 and w1_state == 1 and chg_ok and chg_dir
    bear = d1_state == -1 and w1_state == -1 and chg_ok and chg_dir

    trig_long = bull and sar_st == 2
    trig_short = bear and sar_st == -2
    trigger = 1 if trig_long else (-1 if trig_short else 0)
    bias = 1 if bull else (-1 if bear else 0)
    return trigger, bias


def compute_score(d1: int, w1: int, chg_pct: float, sar_st: int) -> tuple[int, float]:
    """Returns (raw_score, weighted_score). Mirrors Pine f_score."""
    s_chg = 1 if chg_pct > 0 else (-1 if chg_pct < 0 else 0)
    s_sar = 1 if sar_st > 0 else (-1 if sar_st < 0 else 0)
    raw = d1 + w1 + s_chg + s_sar
    weighted = raw * abs(chg_pct)
    return raw, weighted


# ── Scan one index ────────────────────────────────────────────────────
def _scan_index(name: str, symbol: str, ccy: str, atr_length: int,
                min_chg: float, sar_tf: str, sar_bars: int,
                sar_start: float, sar_inc: float, sar_max: float) -> IndexResult | None:
    """Scan one currency index: Renko D1+W1, daily CHG%, SAR state."""

    # 1. Renko D1
    brick_d1 = fetch_tv_renko_ohlc(symbol, "D", atr_length=atr_length)
    if not brick_d1:
        return None

    # 2. Renko W1
    brick_w1 = fetch_tv_renko_ohlc(symbol, "W", atr_length=atr_length)
    if not brick_w1:
        return None

    # 3. Daily close + prev close for CHG% + break line history
    daily_bars = fetch_tv_ohlc(symbol, "D", 150)
    if not daily_bars or len(daily_bars) < 2:
        return None
    dc = daily_bars[-1]["close"]
    dp = daily_bars[-2]["close"]
    chg_pct = ((dc - dp) / dp * 100) if dp != 0 else 0.0

    # 4. SAR on chosen timeframe
    sar_ohlc = fetch_tv_ohlc(symbol, sar_tf, sar_bars)
    if not sar_ohlc or len(sar_ohlc) < 3:
        return None
    sar_st = compute_parabolic_sar(sar_ohlc, start=sar_start,
                                    increment=sar_inc, maximum=sar_max)

    # 5. Px vs Renko states (using daily close as reference price)
    d1 = px_state(brick_d1["open"], brick_d1["close"], dc)
    w1 = px_state(brick_w1["open"], brick_w1["close"], dc)

    # 6. Trigger & bias
    trigger, bias = compute_trigger(d1, w1, chg_pct, sar_st, min_chg)

    # 7. Score
    raw_sc, weighted_sc = compute_score(d1, w1, chg_pct, sar_st)

    # 8. Break line confirmation
    bl = compute_break_line_confirmed(daily_bars, bias, sar_start, sar_inc, sar_max)

    return IndexResult(
        name=name, ccy=ccy, px_d1=d1, px_w1=w1,
        chg_pct=chg_pct, sar_state=sar_st,
        bias=bias, trigger=trigger,
        raw_score=raw_sc, weighted_score=weighted_sc,
        bl_confirmed=bl,
    )


def scan_all_indices(atr_length: int, min_chg: float, sar_tf: str,
                     sar_bars: int, sar_start: float, sar_inc: float,
                     sar_max: float, workers: int = 4) -> list[IndexResult]:
    total = len(INDICES)
    results: list[IndexResult] = []
    done = 0

    print(f"\n  Scanning {total} indices (Renko D1+W1, CHG%, SAR on {sar_tf}) ...")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _scan_index, name, sym, ccy, atr_length, min_chg,
                sar_tf, sar_bars, sar_start, sar_inc, sar_max,
            ): name
            for name, (sym, ccy) in INDICES.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            done += 1
            try:
                r = future.result()
                if r:
                    results.append(r)
                    print(f"\r  [{done}/{total}] {name:<4} OK ", end="", flush=True)
                else:
                    print(f"\r  [{done}/{total}] {name:<4} FAIL", end="", flush=True)
            except Exception:
                print(f"\r  [{done}/{total}] {name:<4} ERR ", end="", flush=True)

    print("\r" + " " * 60 + "\r", end="")
    # Sort by INDEX_ORDER
    order_map = {n: i for i, n in enumerate(INDEX_ORDER)}
    results.sort(key=lambda r: order_map.get(r.name, 99))
    return results


# ── Display helpers (match Pine table text) ───────────────────────────
PX_TEXT = {1: "> Close", -1: "< Open", 0: "Inside"}
SAR_TEXT = {2: "X Over", -2: "X Under", 1: "Above", -1: "Below", 0: "---"}
BIAS_LABEL = {1: "BULL", -1: "BEAR", 0: "---"}
TRIGGER_LABEL = {1: "LONG", -1: "SHORT", 0: ""}


def signal_text(trigger: int, bias: int) -> str:
    if trigger == 1:
        return "LONG"
    elif trigger == -1:
        return "SHORT"
    elif bias == 1:
        return "BULL"
    elif bias == -1:
        return "BEAR"
    return "---"


def chg_str(chg: float) -> str:
    return f"{chg:+.2f}%"


def score_str(sc: float) -> str:
    return f"{sc:+.2f}"


# ── Phase 2: derive & scan pairs from TOP2 × LAST2 ──────────────────
def derive_pair_candidates(top2: list[IndexResult], last2: list[IndexResult]) -> list[tuple[str, int, str, str]]:
    """Return list of (pair, expected_bias, strong_ccy, weak_ccy)."""
    candidates = []
    for strong in top2:
        for weak in last2:
            # STRONG base + WEAK quote → BULL
            pair_bull = strong.ccy + weak.ccy
            if pair_bull in PAIR_SET:
                candidates.append((pair_bull, 1, strong.ccy, weak.ccy))
            # WEAK base + STRONG quote → BEAR
            pair_bear = weak.ccy + strong.ccy
            if pair_bear in PAIR_SET:
                candidates.append((pair_bear, -1, strong.ccy, weak.ccy))
    # Deduplicate
    seen = set()
    unique = []
    for c in candidates:
        if c[0] not in seen:
            seen.add(c[0])
            unique.append(c)
    return unique


def _scan_pair(pair: str, expected_bias: int, strong_ccy: str, weak_ccy: str,
               atr_length: int, min_chg: float, sar_tf: str, sar_bars: int,
               sar_start: float, sar_inc: float, sar_max: float,
               sar15_cutoff: datetime | None = None) -> PairResult | None:
    """Scan one FX pair: Renko D1+W1, daily CHG%, SAR state."""
    symbol = f"OANDA:{pair}"

    brick_d1 = fetch_tv_renko_ohlc(symbol, "D", atr_length=atr_length)
    if not brick_d1:
        return None

    brick_w1 = fetch_tv_renko_ohlc(symbol, "W", atr_length=atr_length)
    if not brick_w1:
        return None

    # Fetch 150 D1 bars: enough history for break line + CHG%
    daily_bars = fetch_tv_ohlc(symbol, "D", 150)
    if not daily_bars or len(daily_bars) < 2:
        return None
    dc = daily_bars[-1]["close"]
    dp = daily_bars[-2]["close"]
    chg_pct = ((dc - dp) / dp * 100) if dp != 0 else 0.0

    sar_ohlc = fetch_tv_ohlc(symbol, sar_tf, sar_bars)
    if not sar_ohlc or len(sar_ohlc) < 3:
        return None
    sar_st = compute_parabolic_sar(sar_ohlc, start=sar_start,
                                    increment=sar_inc, maximum=sar_max)

    d1 = px_state(brick_d1["open"], brick_d1["close"], dc)
    w1 = px_state(brick_w1["open"], brick_w1["close"], dc)
    trigger, bias = compute_trigger(d1, w1, chg_pct, sar_st, min_chg)
    raw_sc, weighted_sc = compute_score(d1, w1, chg_pct, sar_st)
    sar15_event, sar15_value, sar15_time, sar15_note, sar15_confirmed = find_first_sar15_event(
        pair, weighted_sc, sar_start, sar_inc, sar_max, cutoff_override=sar15_cutoff
    )
    bl_confirmed = compute_break_line_confirmed(daily_bars, bias, sar_start, sar_inc, sar_max)

    return PairResult(
        pair=pair, expected_bias=expected_bias,
        strong_ccy=strong_ccy, weak_ccy=weak_ccy,
        px_d1=d1, px_w1=w1, chg_pct=chg_pct, sar_state=sar_st,
        bias=bias, trigger=trigger,
        raw_score=raw_sc, weighted_score=weighted_sc,
        sar15_event=sar15_event, sar15_value=sar15_value, sar15_time=sar15_time,
        sar15_note=sar15_note, sar15_confirmed=sar15_confirmed,
        bl_confirmed=bl_confirmed,
    )


def scan_pairs(candidates: list[tuple[str, int, str, str]],
               atr_length: int, min_chg: float, sar_tf: str, sar_bars: int,
               sar_start: float, sar_inc: float, sar_max: float,
               workers: int = 4, phase_label: str = "Phase 2",
               sar15_cutoff: datetime | None = None) -> list[PairResult]:
    """Scan candidate pairs and return only those with BULL/BEAR/LONG/SHORT signal."""
    total = len(candidates)
    results: list[PairResult] = []
    done = 0

    print(f"\n  {phase_label}: Scanning {total} pairs (Renko D1+W1, CHG%, SAR) ...")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _scan_pair, pair, exp_bias, strong, weak,
                atr_length, min_chg, sar_tf, sar_bars,
                sar_start, sar_inc, sar_max, sar15_cutoff,
            ): pair
            for pair, exp_bias, strong, weak in candidates
        }
        for future in as_completed(futures):
            pair = futures[future]
            done += 1
            try:
                r = future.result()
                if r:
                    results.append(r)
                    print(f"\r  [{done}/{total}] {pair:<8} OK ", end="", flush=True)
                else:
                    print(f"\r  [{done}/{total}] {pair:<8} FAIL", end="", flush=True)
            except Exception:
                print(f"\r  [{done}/{total}] {pair:<8} ERR ", end="", flush=True)

    print("\r" + " " * 60 + "\r", end="")

    # Sort by |weighted_score| descending
    results.sort(key=lambda r: abs(r.weighted_score), reverse=True)
    return results


def print_pairs_table(pairs: list[PairResult], title: str = "\U0001f3af PAIRS (TOP2 \u00d7 LAST2)") -> None:
    if not pairs:
        print("  No pairs with BULL/BEAR signal.\n")
        return

    hdr = (f"  {BOLD}{'Pair':<10} {'D1':<10} {'W1':<10} "
           f"{'CHG%':<9} {'SAR':<10} {'Score':<8} {'Signal':<8} {'SAR15 23H':<20} {'BL':<4}{RESET}")
    sep = "  " + "-" * 94
    print(f"\n  {BOLD}{title}{RESET}")
    print(sep)
    print(hdr)
    print(sep)

    for r in pairs:
        d1_c = _color(r.px_d1)
        w1_c = _color(r.px_w1)
        chg_c = _color(r.chg_pct)
        sar_c = _color(r.sar_state)
        sc_c = _color(r.weighted_score)
        sig = signal_text(r.trigger, r.bias)
        sig_c = GREEN if sig in ("LONG", "BULL") else RED if sig in ("SHORT", "BEAR") else GRAY

        print(
            f"  {r.pair:<10} "
            f"{d1_c}{PX_TEXT[r.px_d1]:<10}{RESET} "
            f"{w1_c}{PX_TEXT[r.px_w1]:<10}{RESET} "
            f"{chg_c}{chg_str(r.chg_pct):<9}{RESET} "
            f"{sar_c}{SAR_TEXT.get(r.sar_state, '---'):<10}{RESET} "
            f"{sc_c}{score_str(r.weighted_score):<8}{RESET} "
            f"{sig_c}{BOLD}{sig:<8}{RESET} "
            f"{format_sar15_event(r):<20} "
            f"{GREEN if r.bl_confirmed else GRAY}{'✓' if r.bl_confirmed else '-':<4}{RESET}"
        )
    print(sep)
    print()


# ── Console table ─────────────────────────────────────────────────────
RESET = "\033[0m"
RED = "\033[91m"
GREEN = "\033[92m"
GRAY = "\033[90m"
BOLD = "\033[1m"
BG_RED = "\033[41m"
BG_GREEN = "\033[42m"
BG_GRAY = "\033[100m"
WHITE = "\033[97m"


def _color(val: int | float, pos_col=GREEN, neg_col=RED, zero_col=GRAY) -> str:
    if val > 0:
        return pos_col
    elif val < 0:
        return neg_col
    return zero_col


def print_table(results: list[IndexResult]) -> None:
    n_trig = sum(1 for r in results if r.trigger != 0)
    n_bias = sum(1 for r in results
                 if r.trigger == 0 and (
                     (r.bias == -1 and r.sar_state == -1) or
                     (r.bias == 1 and r.sar_state == 1)
                 ))

    hdr = (f"  {BOLD}{'Index':<12} {'D1':<10} {'W1':<10} "
           f"{'CHG%':<9} {'SAR':<10} {'Score':<8} {'Signal':<8}{RESET}")
    sep = "  " + "-" * 67
    print(f"\n  {BOLD}📊 INDICES SCAN{RESET}")
    print(sep)
    print(hdr)
    print(sep)

    for r in results:
        d1_c = _color(r.px_d1)
        w1_c = _color(r.px_w1)
        chg_c = _color(r.chg_pct)
        sar_c = _color(r.sar_state)
        sc_c = _color(r.weighted_score)
        sig = signal_text(r.trigger, r.bias)
        sig_c = GREEN if sig in ("LONG", "BULL") else RED if sig in ("SHORT", "BEAR") else GRAY

        print(
            f"  {r.name:<4} ({r.ccy:<3})  "
            f"{d1_c}{PX_TEXT[r.px_d1]:<10}{RESET} "
            f"{w1_c}{PX_TEXT[r.px_w1]:<10}{RESET} "
            f"{chg_c}{chg_str(r.chg_pct):<9}{RESET} "
            f"{sar_c}{SAR_TEXT.get(r.sar_state, '---'):<10}{RESET} "
            f"{sc_c}{score_str(r.weighted_score):<8}{RESET} "
            f"{sig_c}{BOLD}{sig:<8}{RESET}"
        )

    print(sep)
    print(f"  {n_trig} triggered / {n_bias} biased")

    # Average score + TOP 2 (score >= 0) / LAST 2 (score <= 0)
    avg_sc = sum(r.weighted_score for r in results) / len(results) if results else 0.0
    above = sorted([r for r in results if r.weighted_score >= 0],
                   key=lambda r: r.weighted_score, reverse=True)
    below = sorted([r for r in results if r.weighted_score <= 0],
                   key=lambda r: r.weighted_score)
    top2 = above[:2]
    last2 = below[:2]

    print(f"  Avg score: {score_str(avg_sc)}")
    if top2:
        print(f"  {GREEN}{BOLD}🔺 TOP 2:{RESET} " + ", ".join(f"{r.ccy} [{score_str(r.weighted_score)}]" for r in top2))
    if last2:
        print(f"  {RED}{BOLD}🔻 LAST 2:{RESET} " + ", ".join(f"{r.ccy} [{score_str(r.weighted_score)}]" for r in last2))
    print()


# ── Telegram message (exact Pine format) ──────────────────────────────
def build_telegram_message(results: list[IndexResult]) -> str | None:
    """Build simplified Telegram message."""
    if not results:
        return None

    sorted_results = sorted(results, key=lambda r: r.weighted_score, reverse=True)

    lines = ["📊 INDICES (score ↓)"]
    for r in sorted_results:
        sig = signal_text(r.trigger, r.bias)
        if sig == "LONG":
            emoji = "🟢"
        elif sig == "SHORT":
            emoji = "🔴"
        elif sig == "BULL":
            emoji = "🔵"
        elif sig == "BEAR":
            emoji = "🟠"
        else:
            emoji = "⚪"
        flame = " 🔥" if r.bl_confirmed else ""
        lines.append(f"{emoji} {r.ccy} ({score_str(r.weighted_score)}){flame}")

    lines.append("")
    now_paris = datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
    lines.append(f"⏰ {now_paris} Paris")
    return "\n".join(lines)


def append_pairs_to_message(base_msg: str, candidate_pairs: list[PairResult],
                            other_pairs: list[PairResult]) -> str:
    """Merge candidate + other pairs into a single PAIRS TO FOLLOW section."""
    all_pairs = list(candidate_pairs) + list(other_pairs)
    if not all_pairs:
        return base_msg

    lines = ["", "PAIRS TO FOLLOW"]
    for r in all_pairs:
        sig = signal_text(r.trigger, r.bias)
        if sig == "LONG":
            emoji = "\U0001f7e2"
        elif sig == "SHORT":
            emoji = "\U0001f534"
        elif sig == "BULL":
            emoji = "\U0001f535"
        elif sig == "BEAR":
            emoji = "\U0001f7e0"
        else:
            emoji = "\u26aa"
        flame = " 🔥" if r.bl_confirmed else ""
        lines.append(f"{emoji} {r.pair} ({score_str(r.weighted_score)}){flame}")

    parts = base_msg.rsplit("\n⏰", 1)
    if len(parts) == 2:
        return parts[0] + "\n".join(lines) + "\n\n⏰" + parts[1]
    return base_msg + "\n" + "\n".join(lines)


# ── Telegram ──────────────────────────────────────────────────────────
def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  [WARN] Telegram credentials not configured (.env)")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = http_requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        ok = resp.json().get("ok", False)
        if ok:
            print("  ✅ Telegram: sent")
        else:
            print(f"  ❌ Telegram: failed ({resp.text[:100]})")
        return ok
    except Exception as e:
        print(f"  ❌ Telegram: error ({e})")
        return False


# ── CLI ───────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="Px/Renko Index Screener – faithful Python replica of PxRenko_index Pine Script."
    )
    p.add_argument("--length", type=int, default=14, help="ATR length for Renko (default: 14).")
    p.add_argument("--min-chg", type=float, default=MIN_CHG_PCT, help="Min |CHG%%| daily (default: 0.15).")
    p.add_argument("--sar-tf", type=str, default="60", help="Timeframe for SAR computation (default: 60 = 1H).")
    p.add_argument("--sar-bars", type=int, default=100, help="Number of bars for SAR computation (default: 100).")
    p.add_argument("--sar-start", type=float, default=SAR_START, help="SAR start (default: 0.1).")
    p.add_argument("--sar-inc", type=float, default=SAR_INCREMENT, help="SAR increment (default: 0.1).")
    p.add_argument("--sar-max", type=float, default=SAR_MAXIMUM, help="SAR maximum (default: 0.2).")
    p.add_argument("--sar15-cutoff", type=str, default=None,
                   help='Force SAR15 search start in Paris time, format "YYYY-MM-DD HH:MM".')
    p.add_argument("--sar15-list-all", action="store_true",
                   help="List all SAR15 X Over/X Under events for each pair in PAIRS TO FOLLOW.")
    p.add_argument("--sar15-debug-pair", type=str, default=None,
                   help="Print 15m close/SAR rows and cross events for one pair, e.g. USDJPY.")
    p.add_argument("--no-telegram", action="store_true", help="Skip Telegram, console only.")
    p.add_argument("--force", action="store_true", help="Force scan even if no changes detected.")
    p.add_argument("--workers", type=int, default=4, help="Parallel threads (default: 4).")
    return p.parse_args()


def save_scan_output(candidate_pairs: list[PairResult], other_pairs: list[PairResult]) -> None:
    """Save bl_confirmed + BULL/BEAR/LONG/SHORT pairs to JSON for the tracker."""
    all_pairs = list(candidate_pairs) + list(other_pairs)
    active = []
    for r in all_pairs:
        if not r.bl_confirmed or not r.sar15_confirmed:
            continue
        sig = signal_text(r.trigger, r.bias)
        if sig not in ("BULL", "BEAR", "LONG", "SHORT"):
            continue
        active.append({
            "pair": r.pair,
            "direction": "BULL" if sig in ("BULL", "LONG") else "BEAR",
            "signal": sig,
            "score": round(r.weighted_score, 4),
            "sar15_confirmed": r.sar15_confirmed,
        })
    output = {
        "updated_at": datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%dT%H:%M:%S"),
        "active_pairs": active,
    }
    with open(SCAN_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)


def main() -> int:
    args = parse_args()
    t_start = time.time()
    sar15_cutoff = parse_paris_datetime(args.sar15_cutoff) if args.sar15_cutoff else None

    # Scan all 8 indices
    results = scan_all_indices(
        atr_length=args.length, min_chg=args.min_chg,
        sar_tf=args.sar_tf, sar_bars=args.sar_bars,
        sar_start=args.sar_start, sar_inc=args.sar_inc,
        sar_max=args.sar_max, workers=args.workers,
    )
    elapsed = time.time() - t_start
    print(f"  Scan complete in {elapsed:.1f}s ({len(results)}/{len(INDICES)} indices)\n")

    if not results:
        print("  No index data retrieved.")
        return 1

    # Console table (Phase 1)
    print_table(results)

    # Compute TOP2 (score >= 0) / LAST2 (score <= 0) for pair derivation
    avg_sc = sum(r.weighted_score for r in results) / len(results) if results else 0.0
    above = sorted([r for r in results if r.weighted_score >= 0],
                   key=lambda r: r.weighted_score, reverse=True)
    below = sorted([r for r in results if r.weighted_score <= 0],
                   key=lambda r: r.weighted_score)
    top2 = above[:2]
    last2 = below[:2]

    # Phase 2: derive & scan candidate pairs from TOP2 × LAST2
    pair_results = []
    candidate_pair_names = set()
    if top2 and last2:
        candidates = derive_pair_candidates(top2, last2)
        if candidates:
            candidate_pair_names = {c[0] for c in candidates}
            print(f"  Candidates: " + ", ".join(c[0] for c in candidates))
            pair_results = scan_pairs(
                candidates, atr_length=args.length, min_chg=args.min_chg,
                sar_tf=args.sar_tf, sar_bars=args.sar_bars,
                sar_start=args.sar_start, sar_inc=args.sar_inc,
                sar_max=args.sar_max, workers=args.workers,
                sar15_cutoff=sar15_cutoff,
            )
            n_sig = sum(1 for r in pair_results if r.bias != 0 or r.trigger != 0)
            elapsed2 = time.time() - t_start
            print(f"  Phase 2 done in {elapsed2:.1f}s — {len(pair_results)} scanned, {n_sig} with signal\n")
            print_pairs_table(pair_results)
        else:
            print("  No valid pair combinations from TOP2 × LAST2.\n")
    else:
        print("  Not enough indices above/below average for pair derivation.\n")

    if args.sar15_list_all:
        print_sar15_events_for_pairs(
            pair_results,
            sar_start=args.sar_start,
            sar_inc=args.sar_inc,
            sar_max=args.sar_max,
            cutoff_override=sar15_cutoff,
            tz_name="Europe/Paris",
        )

    if args.sar15_debug_pair:
        debug_sar15_pair(
            args.sar15_debug_pair.upper(),
            sar_start=args.sar_start,
            sar_inc=args.sar_inc,
            sar_max=args.sar_max,
            cutoff_override=sar15_cutoff,
            tz_name="Europe/Paris",
        )

    # Save scan results for tracker
    save_scan_output(pair_results, [])

    # Telegram
    msg = build_telegram_message(results)
    if msg:
        msg = append_pairs_to_message(msg, pair_results, [])

    if not args.no_telegram:
        if msg:
            print(msg)
            print()
            send_telegram(msg)
        else:
            print("  No signals — no Telegram sent.\n")
    else:
        if msg:
            print(msg)
            print()
        else:
            print("  No signals.\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
