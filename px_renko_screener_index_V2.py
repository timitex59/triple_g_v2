#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
px_renko_screener_index_V2.py

Scans all 29 FX pairs directly (no TOP1×LAST1 index logic).
For each pair: Renko D1+W1, daily CHG%, SAR → bias BULL/BEAR.
Keeps daily accumulation of PAIRS TO FOLLOW with delta score/price.
Sends Telegram with INDICES section + PAIRS TO FOLLOW.
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
DAILY_FOLLOW_PATH = os.path.join(SCRIPT_DIR, "px_renko_daily_follow_v2.json")

DAILY_START_HOUR = 7
DAILY_END_HOUR = 22

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
    "CADCHF", "XAUUSD",
]

RESET  = "\033[0m"
RED    = "\033[91m"
GREEN  = "\033[92m"
GRAY   = "\033[90m"
BOLD   = "\033[1m"


# ── dataclasses ───────────────────────────────────────────────────────
@dataclass
class IndexResult:
    name: str
    ccy: str
    px_d1: int
    px_w1: int
    chg_pct: float
    sar_state: int
    bias: int
    trigger: int
    raw_score: int
    weighted_score: float
    bl_confirmed: bool = False


@dataclass
class PairResult:
    pair: str
    expected_bias: int   # derived from pair bias itself (same as bias)
    px_d1: int
    px_w1: int
    chg_pct: float
    sar_state: int
    bias: int
    trigger: int
    raw_score: int
    weighted_score: float
    bl_confirmed: bool = False
    current_price: float = 0.0


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
        out.append(raw[i: i + size])
        i += size
    return out if out else [raw]


def fetch_tv_ohlc(symbol: str, interval: str, n_candles: int,
                  timeout_s: int = 20, retries: int = 2) -> list[dict] | None:
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
                    rows.append({"time": v[0], "open": v[1], "high": v[2], "low": v[3], "close": v[4]})
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
        ep = highs[0]; sar[0] = lows[0]
    else:
        ep = lows[0]; sar[0] = highs[0]
    for i in range(1, n):
        prev_sar = sar[i - 1]
        sar[i] = prev_sar + af * (ep - prev_sar)
        if is_long:
            sar[i] = min(sar[i], lows[i - 1], lows[i - 2]) if i >= 2 else min(sar[i], lows[i - 1])
            if lows[i] < sar[i]:
                is_long = False; sar[i] = ep; ep = lows[i]; af = start
            else:
                if highs[i] > ep:
                    ep = highs[i]; af = min(af + increment, maximum)
        else:
            sar[i] = max(sar[i], highs[i - 1], highs[i - 2]) if i >= 2 else max(sar[i], highs[i - 1])
            if highs[i] > sar[i]:
                is_long = True; sar[i] = ep; ep = highs[i]; af = start
            else:
                if lows[i] < ep:
                    ep = lows[i]; af = min(af + increment, maximum)
    c_last, sar_last = closes[-1], sar[-1]
    c_prev, sar_prev = closes[-2], sar[-2]
    if c_prev <= sar_prev and c_last > sar_last:
        return 2
    elif c_prev >= sar_prev and c_last < sar_last:
        return -2
    elif c_last > sar_last:
        return 1
    elif c_last < sar_last:
        return -1
    return 0


def compute_parabolic_sar_series(bars: list[dict], start: float = 0.1,
                                 increment: float = 0.1, maximum: float = 0.2) -> list[float]:
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
        ep = highs[0]; sar[0] = lows[0]
    else:
        ep = lows[0]; sar[0] = highs[0]
    for i in range(1, n):
        prev_sar = sar[i - 1]
        sar[i] = prev_sar + af * (ep - prev_sar)
        if is_long:
            sar[i] = min(sar[i], lows[i - 1], lows[i - 2]) if i >= 2 else min(sar[i], lows[i - 1])
            if lows[i] < sar[i]:
                is_long = False; sar[i] = ep; ep = lows[i]; af = start
            else:
                if highs[i] > ep:
                    ep = highs[i]; af = min(af + increment, maximum)
        else:
            sar[i] = max(sar[i], highs[i - 1], highs[i - 2]) if i >= 2 else max(sar[i], highs[i - 1])
            if highs[i] > sar[i]:
                is_long = True; sar[i] = ep; ep = highs[i]; af = start
            else:
                if lows[i] < ep:
                    ep = lows[i]; af = min(af + increment, maximum)
    return sar


def compute_break_line_confirmed(bars: list[dict], bias: int,
                                 sar_start: float, sar_inc: float, sar_max: float) -> bool:
    if not bars or len(bars) < 5 or bias == 0:
        return False
    sar = compute_parabolic_sar_series(bars, start=sar_start, increment=sar_inc, maximum=sar_max)
    closes = [b["close"] for b in bars]
    n = len(bars)
    bull_events: list[tuple[int, float]] = []
    bear_events: list[tuple[int, float]] = []
    last_bull_line = last_bear_line = None
    for i in range(1, n):
        c_prev, sar_prev = closes[i - 1], sar[i - 1]
        c_last, sar_last = closes[i], sar[i]
        if c_prev <= sar_prev and c_last > sar_last:
            bull_events.append((i, sar_last))
            if len(bull_events) >= 2:
                (x1, y1), (x2, y2) = bull_events[-2], bull_events[-1]
                if y2 > y1:
                    last_bull_line = (x1, y1, x2, y2)
        elif c_prev >= sar_prev and c_last < sar_last:
            bear_events.append((i, sar_last))
            if len(bear_events) >= 2:
                (x1, y1), (x2, y2) = bear_events[-2], bear_events[-1]
                if y2 < y1:
                    last_bear_line = (x1, y1, x2, y2)

    def project_line(line):
        if line is None:
            return None
        x1, y1, x2, y2 = line
        if x2 == x1:
            return None
        return y1 + (y2 - y1) * (n - 1 - x1) / (x2 - x1)

    current_close = closes[-1]
    if bias == 1:
        bear_line = project_line(last_bear_line)
        return bear_line is not None and current_close > bear_line
    else:
        bull_line = project_line(last_bull_line)
        return bull_line is not None and current_close < bull_line


# ── Core scoring logic ────────────────────────────────────────────────
def px_state(renko_open: float, renko_close: float, price: float) -> int:
    h = max(renko_open, renko_close)
    l = min(renko_open, renko_close)
    if price > h:
        return 1
    elif price < l:
        return -1
    return 0


def compute_trigger(d1_state: int, w1_state: int, chg_pct: float,
                    sar_st: int, min_chg: float) -> tuple[int, int]:
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
    s_chg = 1 if chg_pct > 0 else (-1 if chg_pct < 0 else 0)
    s_sar = 1 if sar_st > 0 else (-1 if sar_st < 0 else 0)
    raw = d1 + w1 + s_chg + s_sar
    weighted = raw * abs(chg_pct)
    return raw, weighted


# ── Scan one index ────────────────────────────────────────────────────
def _scan_index(name: str, symbol: str, ccy: str, atr_length: int,
                min_chg: float, sar_tf: str, sar_bars: int,
                sar_start: float, sar_inc: float, sar_max: float) -> IndexResult | None:
    brick_d1 = fetch_tv_renko_ohlc(symbol, "D", atr_length=atr_length)
    if not brick_d1:
        return None
    brick_w1 = fetch_tv_renko_ohlc(symbol, "W", atr_length=atr_length)
    if not brick_w1:
        return None
    daily_bars = fetch_tv_ohlc(symbol, "D", 150)
    if not daily_bars or len(daily_bars) < 2:
        return None
    dc = daily_bars[-1]["close"]
    dp = daily_bars[-2]["close"]
    chg_pct = ((dc - dp) / dp * 100) if dp != 0 else 0.0
    sar_ohlc = fetch_tv_ohlc(symbol, sar_tf, sar_bars)
    if not sar_ohlc or len(sar_ohlc) < 3:
        return None
    sar_st = compute_parabolic_sar(sar_ohlc, start=sar_start, increment=sar_inc, maximum=sar_max)
    d1 = px_state(brick_d1["open"], brick_d1["close"], dc)
    w1 = px_state(brick_w1["open"], brick_w1["close"], dc)
    trigger, bias = compute_trigger(d1, w1, chg_pct, sar_st, min_chg)
    raw_sc, weighted_sc = compute_score(d1, w1, chg_pct, sar_st)
    bl = compute_break_line_confirmed(daily_bars, bias, sar_start, sar_inc, sar_max)
    return IndexResult(name=name, ccy=ccy, px_d1=d1, px_w1=w1,
                       chg_pct=chg_pct, sar_state=sar_st,
                       bias=bias, trigger=trigger,
                       raw_score=raw_sc, weighted_score=weighted_sc,
                       bl_confirmed=bl)


def scan_all_indices(atr_length: int, min_chg: float, sar_tf: str,
                     sar_bars: int, sar_start: float, sar_inc: float,
                     sar_max: float, workers: int = 4) -> list[IndexResult]:
    total = len(INDICES)
    results = []
    done = 0
    print(f"\n  Scanning {total} indices ...")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_scan_index, name, sym, ccy, atr_length, min_chg,
                        sar_tf, sar_bars, sar_start, sar_inc, sar_max): name
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
    order_map = {n: i for i, n in enumerate(INDEX_ORDER)}
    results.sort(key=lambda r: order_map.get(r.name, 99))
    return results


# ── Scan one pair ─────────────────────────────────────────────────────
def _scan_pair(pair: str, atr_length: int, min_chg: float, sar_tf: str,
               sar_bars: int, sar_start: float, sar_inc: float, sar_max: float) -> PairResult | None:
    symbol = f"OANDA:{pair}"
    brick_d1 = fetch_tv_renko_ohlc(symbol, "D", atr_length=atr_length)
    if not brick_d1:
        return None
    brick_w1 = fetch_tv_renko_ohlc(symbol, "W", atr_length=atr_length)
    if not brick_w1:
        return None
    daily_bars = fetch_tv_ohlc(symbol, "D", 150)
    if not daily_bars or len(daily_bars) < 2:
        return None
    dc = daily_bars[-1]["close"]
    dp = daily_bars[-2]["close"]
    chg_pct = ((dc - dp) / dp * 100) if dp != 0 else 0.0
    sar_ohlc = fetch_tv_ohlc(symbol, sar_tf, sar_bars)
    if not sar_ohlc or len(sar_ohlc) < 3:
        return None
    sar_st = compute_parabolic_sar(sar_ohlc, start=sar_start, increment=sar_inc, maximum=sar_max)
    d1 = px_state(brick_d1["open"], brick_d1["close"], dc)
    w1 = px_state(brick_w1["open"], brick_w1["close"], dc)
    trigger, bias = compute_trigger(d1, w1, chg_pct, sar_st, min_chg)
    raw_sc, weighted_sc = compute_score(d1, w1, chg_pct, sar_st)
    bl = compute_break_line_confirmed(daily_bars, bias, sar_start, sar_inc, sar_max)
    return PairResult(
        pair=pair, expected_bias=bias,
        px_d1=d1, px_w1=w1, chg_pct=chg_pct, sar_state=sar_st,
        bias=bias, trigger=trigger,
        raw_score=raw_sc, weighted_score=weighted_sc,
        bl_confirmed=bl, current_price=dc,
    )


def scan_all_pairs(atr_length: int, min_chg: float, sar_tf: str,
                   sar_bars: int, sar_start: float, sar_inc: float,
                   sar_max: float, workers: int = 4) -> list[PairResult]:
    total = len(PAIRS)
    results = []
    done = 0
    print(f"\n  Scanning {total} pairs ...")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_scan_pair, pair, atr_length, min_chg, sar_tf,
                        sar_bars, sar_start, sar_inc, sar_max): pair
            for pair in PAIRS
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
    results.sort(key=lambda r: abs(r.weighted_score), reverse=True)
    return results


# ── Display helpers ───────────────────────────────────────────────────
SAR_TEXT = {2: "X Over", -2: "X Under", 1: "Above", -1: "Below", 0: "---"}
PX_TEXT  = {1: "> Close", -1: "< Open", 0: "Inside"}
BIAS_LABEL = {1: "BULL", -1: "BEAR", 0: "---"}


def signal_text(trigger: int, bias: int) -> str:
    if trigger == 1:   return "LONG"
    if trigger == -1:  return "SHORT"
    if bias == 1:      return "BULL"
    if bias == -1:     return "BEAR"
    return "---"


def score_str(sc: float) -> str:
    return f"{sc:+.2f}"


def chg_str(chg: float) -> str:
    return f"{chg:+.2f}%"


def _color(val):
    if val > 0: return GREEN
    if val < 0: return RED
    return GRAY


# ── Daily follow ──────────────────────────────────────────────────────
def load_daily_follow(now_paris: datetime) -> dict:
    today = now_paris.strftime("%Y-%m-%d")
    empty = {"date": today, "pairs": {}}
    if now_paris.hour < DAILY_START_HOUR or now_paris.hour >= DAILY_END_HOUR:
        return empty
    try:
        with open(DAILY_FOLLOW_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("date") != today:
            return empty
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return empty


def save_daily_follow(data: dict) -> None:
    with open(DAILY_FOLLOW_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def update_daily_follow(data: dict, valid_pairs: list[PairResult],
                        all_pair_results: list[PairResult],
                        now_paris: datetime, scan_params: dict) -> dict:
    time_str = now_paris.strftime("%H:%M")

    for r in valid_pairs:
        if r.pair not in data["pairs"]:
            data["pairs"][r.pair] = {
                "first_score": round(r.weighted_score, 4),
                "first_price": round(r.current_price, 6),
                "first_time": time_str,
                "expected_bias": r.expected_bias,
            }
        elif data["pairs"][r.pair].get("first_price") is None:
            data["pairs"][r.pair]["first_price"] = round(r.current_price, 6)

    current_map = {r.pair: r for r in all_pair_results}
    for pair, info in data["pairs"].items():
        r = current_map.get(pair)
        if r is not None:
            info["last_score"] = round(r.weighted_score, 4)
            info["last_price"] = round(r.current_price, 6)
            info["last_time"] = time_str

    # Rescan pairs in daily but absent from current scan
    missing = [p for p in data["pairs"] if p not in current_map]
    if missing:
        print(f"  Rescanning {len(missing)} tracked pair(s): {', '.join(missing)}")
        with ThreadPoolExecutor(max_workers=scan_params["workers"]) as pool:
            futures = {
                pool.submit(_scan_pair, p,
                            scan_params["atr_length"], scan_params["min_chg"],
                            scan_params["sar_tf"], scan_params["sar_bars"],
                            scan_params["sar_start"], scan_params["sar_inc"],
                            scan_params["sar_max"]): p
                for p in missing
            }
            for future in as_completed(futures):
                try:
                    r = future.result()
                    if r and r.pair in data["pairs"]:
                        data["pairs"][r.pair]["last_score"] = round(r.weighted_score, 4)
                        data["pairs"][r.pair]["last_price"] = round(r.current_price, 6)
                        data["pairs"][r.pair]["last_time"] = time_str
                        data["pairs"][r.pair]["expected_bias"] = r.expected_bias
                except Exception:
                    pass

    return data


# ── Telegram builders ─────────────────────────────────────────────────
def build_telegram_indices(results: list[IndexResult]) -> str:
    sorted_results = sorted(results, key=lambda r: r.weighted_score, reverse=True)
    lines = ["📊 INDICES (score ↓)"]
    for r in sorted_results:
        sig = signal_text(r.trigger, r.bias)
        if sig == "LONG":    emoji = "🟢"
        elif sig == "SHORT": emoji = "🔴"
        elif sig == "BULL":  emoji = "🔵"
        elif sig == "BEAR":  emoji = "🟠"
        else:                emoji = "⚪"
        flame = " 🔥" if r.bl_confirmed else ""
        lines.append(f"{emoji} {r.ccy} ({score_str(r.weighted_score)}){flame}")
    lines.append("")
    now_paris = datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
    lines.append(f"⏰ {now_paris} Paris")
    return "\n".join(lines)


def _pair_line_v2(pair: str, r: PairResult | None, info: dict) -> str:
    first_score = info.get("first_score")
    first_price = info.get("first_price")
    last_score  = info.get("last_score")
    last_price  = info.get("last_price")
    expected_bias = info.get("expected_bias", 0)

    # Current signal emoji (1st dot)
    if r is not None:
        sig = signal_text(r.trigger, r.bias)
        if sig == "LONG":    emoji = "🟢"
        elif sig == "SHORT": emoji = "🔴"
        elif sig == "BULL":  emoji = "🔵"
        elif sig == "BEAR":  emoji = "🟠"
        else:                emoji = "⚪"
        cur_score = r.weighted_score
        cur_price = r.current_price
        bl = r.bl_confirmed
    else:
        emoji = "⚪"
        cur_score = last_score
        cur_price = last_price
        bl = False

    # Expected bias emoji (2nd dot)
    if expected_bias == 1:   bias_emoji = "🟢"
    elif expected_bias == -1: bias_emoji = "🔴"
    else:                     bias_emoji = "⚪"

    # Deltas
    if (first_score is not None and first_price and
            cur_score is not None and cur_price):
        delta_score = cur_score - first_score
        delta_price_pct = (cur_price - first_price) / first_price * 100
        score_part = f"({score_str(delta_score)} / {delta_price_pct:+.2f}%)"

        # Warning logic
        warning = False
        if delta_score != 0.0 or delta_price_pct != 0.0:
            if (delta_score > 0 and delta_price_pct < 0) or (delta_score < 0 and delta_price_pct > 0):
                warning = True
            elif expected_bias == 1 and delta_score < 0 and delta_price_pct < 0:
                warning = True
            elif expected_bias == -1 and delta_score > 0 and delta_price_pct > 0:
                warning = True

        suffix = " ⚠️" if warning else (" 🔥" if bl else "")
    else:
        score_part = "(n/a)"
        suffix = ""

    return f"{emoji}{bias_emoji} {pair} {score_part}{suffix}"


def build_pairs_to_follow_message(daily_data: dict, valid_pairs: list[PairResult]) -> str:
    if not daily_data.get("pairs"):
        return ""

    current_map = {r.pair: r for r in valid_pairs}

    # Sort by |delta_score| descending
    def sort_key(item):
        pair, info = item
        r = current_map.get(pair)
        cur_score = r.weighted_score if r else info.get("last_score", 0) or 0
        first_score = info.get("first_score", cur_score) or cur_score
        return abs(cur_score - first_score)

    sorted_pairs = sorted(daily_data["pairs"].items(), key=sort_key, reverse=True)

    lines = ["", "PAIRS TO FOLLOW"]
    for pair, info in sorted_pairs:
        r = current_map.get(pair)
        lines.append(_pair_line_v2(pair, r, info))

    return "\n".join(lines)


# ── Telegram send ─────────────────────────────────────────────────────
def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  [WARN] Telegram credentials not configured")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = http_requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        ok = resp.json().get("ok", False)
        print("  ✅ Telegram: sent" if ok else f"  ❌ Telegram: failed ({resp.text[:100]})")
        return ok
    except Exception as e:
        print(f"  ❌ Telegram: error ({e})")
        return False


# ── CLI ───────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Px/Renko Screener V2 — scans all 29 pairs.")
    p.add_argument("--length",   type=int,   default=14,          help="ATR length for Renko.")
    p.add_argument("--min-chg",  type=float, default=MIN_CHG_PCT,  help="Min |CHG%%| daily.")
    p.add_argument("--sar-tf",   type=str,   default="60",         help="Timeframe for SAR.")
    p.add_argument("--sar-bars", type=int,   default=100,          help="Bars for SAR.")
    p.add_argument("--sar-start",type=float, default=SAR_START)
    p.add_argument("--sar-inc",  type=float, default=SAR_INCREMENT)
    p.add_argument("--sar-max",  type=float, default=SAR_MAXIMUM)
    p.add_argument("--workers",  type=int,   default=4)
    p.add_argument("--no-telegram", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    t_start = time.time()

    # Phase 1 — Indices
    index_results = scan_all_indices(
        atr_length=args.length, min_chg=args.min_chg,
        sar_tf=args.sar_tf, sar_bars=args.sar_bars,
        sar_start=args.sar_start, sar_inc=args.sar_inc,
        sar_max=args.sar_max, workers=args.workers,
    )
    print(f"  Indices done in {time.time() - t_start:.1f}s ({len(index_results)}/8)\n")

    # Phase 2 — All pairs
    all_results = scan_all_pairs(
        atr_length=args.length, min_chg=args.min_chg,
        sar_tf=args.sar_tf, sar_bars=args.sar_bars,
        sar_start=args.sar_start, sar_inc=args.sar_inc,
        sar_max=args.sar_max, workers=args.workers,
    )
    print(f"  Pairs done in {time.time() - t_start:.1f}s ({len(all_results)}/{len(PAIRS)} scanned)\n")

    # Filter pairs with BULL/BEAR signal
    valid_pairs = [r for r in all_results if r.bias != 0 or r.trigger != 0]
    print(f"  {len(valid_pairs)} pairs with signal\n")

    # Daily follow accumulation
    now_paris = datetime.now(pytz.timezone("Europe/Paris"))
    daily_data = load_daily_follow(now_paris)
    if now_paris.hour >= DAILY_START_HOUR and now_paris.hour < DAILY_END_HOUR:
        scan_params = {
            "atr_length": args.length, "min_chg": args.min_chg,
            "sar_tf": args.sar_tf, "sar_bars": args.sar_bars,
            "sar_start": args.sar_start, "sar_inc": args.sar_inc,
            "sar_max": args.sar_max, "workers": args.workers,
        }
        daily_data = update_daily_follow(daily_data, valid_pairs, all_results, now_paris, scan_params)
        save_daily_follow(daily_data)

    # Build Telegram message
    has_daily_pairs = bool(daily_data.get("pairs"))
    msg = build_telegram_indices(index_results)
    if has_daily_pairs:
        pairs_section = build_pairs_to_follow_message(daily_data, valid_pairs)
        if pairs_section:
            parts = msg.rsplit("\n⏰", 1)
            if len(parts) == 2:
                msg = parts[0] + pairs_section + "\n\n⏰" + parts[1]
            else:
                msg = msg + "\n" + pairs_section

    if not args.no_telegram:
        if msg and has_daily_pairs:
            print(msg)
            print()
            send_telegram(msg)
        else:
            print("  No pairs to follow today — no Telegram sent.\n")
    else:
        print(msg)
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
