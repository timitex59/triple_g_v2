#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
renko_nasdaq.py
Renko ATR(14) alignment MN / W1 / D1 for the Nasdaq 100.
Logic mirrors renko_monster_V5.pine — no SAR filter, triple alignment only.
"""

import argparse
import json
import os
import random
import re
import string
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

import pytz
import requests as http_requests
from dotenv import load_dotenv
from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException, create_connection

load_dotenv()

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
STATE_PATH    = os.path.join(SCRIPT_DIR, "renko_nasdaq_state.json")
TRACKER_PATH  = os.path.join(SCRIPT_DIR, "renko_nasdaq_tracker.json")
HISTORY_PATH  = os.path.join(SCRIPT_DIR, "renko_nasdaq_pnl_history.json")
CLOSE_THRESHOLD = -0.05  # % threshold for close alert

WS_URL     = "wss://prodata.tradingview.com/socket.io/websocket"
WS_HEADERS = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# Tickers that trade on NYSE (not NASDAQ) — used by the wiki fetcher
_NYSE_TICKERS = {"LIN", "WMT", "SHOP", "TRI"}

WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"


def fetch_nasdaq_100_from_wiki() -> list[tuple[str, str]] | None:
    """Return [(tv_symbol, ticker), ...] parsed live from Wikipedia, or None on failure.

    Targets the first wikitable whose header row contains 'Ticker' — that is the
    current-components table on the Nasdaq-100 Wikipedia article.
    """
    try:
        resp = http_requests.get(WIKI_URL, timeout=10,
                                 headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        html = resp.text

        # Find each <table class="wikitable ...">...</table> block
        table_pattern = re.compile(
            r'<table[^>]+class="[^"]*wikitable[^"]*"[^>]*>(.*?)</table>',
            re.DOTALL | re.IGNORECASE,
        )
        row_pattern    = re.compile(r'<tr[^>]*>(.*?)</tr>', re.DOTALL | re.IGNORECASE)
        cell_pattern   = re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', re.DOTALL | re.IGNORECASE)
        tag_strip      = re.compile(r'<[^>]+>')

        def _text(cell_html: str) -> str:
            return tag_strip.sub("", cell_html).strip()

        for table_match in table_pattern.finditer(html):
            table_html = table_match.group(1)
            rows = row_pattern.findall(table_html)
            if not rows:
                continue
            # Check header row contains "Ticker"
            header_cells = [_text(c.group(1)) for c in cell_pattern.finditer(rows[0])]
            if "Ticker" not in header_cells:
                continue
            ticker_col = header_cells.index("Ticker")
            company_col = 0  # Company is always first column

            result = []
            for row_html in rows[1:]:
                cells = [_text(c.group(1)) for c in cell_pattern.finditer(row_html)]
                if len(cells) <= max(ticker_col, company_col):
                    continue
                ticker  = cells[ticker_col].strip()
                if not re.match(r"^[A-Z]{1,5}$", ticker):
                    continue
                exchange = "NYSE" if ticker in _NYSE_TICKERS else "NASDAQ"
                result.append((f"{exchange}:{ticker}", ticker))

            if len(result) >= 90:
                return result

        return None
    except Exception:
        return None


# Nasdaq-100 components — fallback if Wikipedia is unreachable
# LIN → NYSE; WMT, SHOP, TRI → NYSE; all others NASDAQ
NASDAQ_100: list[tuple[str, str]] = [
    ("NASDAQ:AAPL",  "AAPL"),  ("NASDAQ:MSFT",  "MSFT"),  ("NASDAQ:NVDA",  "NVDA"),
    ("NASDAQ:AMZN",  "AMZN"),  ("NASDAQ:META",  "META"),  ("NASDAQ:GOOGL", "GOOGL"),
    ("NASDAQ:GOOG",  "GOOG"),  ("NASDAQ:TSLA",  "TSLA"),  ("NASDAQ:AVGO",  "AVGO"),
    ("NASDAQ:COST",  "COST"),  ("NASDAQ:NFLX",  "NFLX"),  ("NASDAQ:ASML",  "ASML"),
    ("NASDAQ:AMD",   "AMD"),   ("NASDAQ:TMUS",  "TMUS"),  ("NYSE:LIN",     "LIN"),
    ("NASDAQ:ISRG",  "ISRG"),  ("NASDAQ:CSCO",  "CSCO"),  ("NASDAQ:INTU",  "INTU"),
    ("NASDAQ:PEP",   "PEP"),   ("NASDAQ:ADBE",  "ADBE"),  ("NASDAQ:QCOM",  "QCOM"),
    ("NASDAQ:TXN",   "TXN"),   ("NASDAQ:AMGN",  "AMGN"),  ("NASDAQ:HON",   "HON"),
    ("NASDAQ:AMAT",  "AMAT"),  ("NASDAQ:BKNG",  "BKNG"),  ("NASDAQ:VRTX",  "VRTX"),
    ("NASDAQ:PANW",  "PANW"),  ("NASDAQ:ADI",   "ADI"),   ("NASDAQ:GILD",  "GILD"),
    ("NASDAQ:SBUX",  "SBUX"),  ("NASDAQ:ADP",   "ADP"),   ("NASDAQ:REGN",  "REGN"),
    ("NASDAQ:LRCX",  "LRCX"),  ("NASDAQ:MU",    "MU"),    ("NASDAQ:KLAC",  "KLAC"),
    ("NASDAQ:MDLZ",  "MDLZ"),  ("NASDAQ:MELI",  "MELI"),  ("NASDAQ:SNPS",  "SNPS"),
    ("NASDAQ:CDNS",  "CDNS"),  ("NASDAQ:CTAS",  "CTAS"),  ("NASDAQ:ORLY",  "ORLY"),
    ("NASDAQ:ABNB",  "ABNB"),  ("NASDAQ:CEG",   "CEG"),   ("NASDAQ:FTNT",  "FTNT"),
    ("NASDAQ:CSX",   "CSX"),   ("NASDAQ:PYPL",  "PYPL"),  ("NASDAQ:MRVL",  "MRVL"),
    ("NASDAQ:WDAY",  "WDAY"),  ("NASDAQ:PCAR",  "PCAR"),  ("NASDAQ:NXPI",  "NXPI"),
    ("NASDAQ:DXCM",  "DXCM"),  ("NASDAQ:PAYX",  "PAYX"),  ("NASDAQ:CRWD",  "CRWD"),
    ("NASDAQ:KDP",   "KDP"),   ("NASDAQ:FAST",  "FAST"),  ("NASDAQ:ODFL",  "ODFL"),
    ("NASDAQ:ROST",  "ROST"),  ("NASDAQ:CPRT",  "CPRT"),  ("NASDAQ:IDXX",  "IDXX"),
    ("NASDAQ:TEAM",  "TEAM"),  ("NASDAQ:EA",    "EA"),    ("NASDAQ:ZS",    "ZS"),
    ("NASDAQ:VRSK",  "VRSK"),  ("NASDAQ:FANG",  "FANG"),  ("NASDAQ:BKR",   "BKR"),
    ("NASDAQ:GEHC",  "GEHC"),  ("NASDAQ:TTWO",  "TTWO"),  ("NASDAQ:DDOG",  "DDOG"),
    ("NASDAQ:CCEP",  "CCEP"),  ("NASDAQ:ARM",   "ARM"),   ("NASDAQ:PLTR",  "PLTR"),
    ("NASDAQ:MCHP",  "MCHP"),  ("NASDAQ:EXC",   "EXC"),   ("NASDAQ:XEL",   "XEL"),
    ("NASDAQ:CSGP",  "CSGP"),  ("NASDAQ:KHC",   "KHC"),   ("NASDAQ:WBD",   "WBD"),
    ("NASDAQ:ROP",   "ROP"),   ("NASDAQ:MNST",  "MNST"),  ("NASDAQ:CHTR",  "CHTR"),
    ("NASDAQ:CTSH",  "CTSH"),  ("NASDAQ:CMCSA", "CMCSA"), ("NASDAQ:APP",   "APP"),
    ("NASDAQ:ADSK",  "ADSK"),  ("NASDAQ:AXON",  "AXON"),  ("NASDAQ:DASH",  "DASH"),
    ("NASDAQ:FER",   "FER"),   ("NASDAQ:INSM",  "INSM"),  ("NASDAQ:INTC",  "INTC"),
    ("NASDAQ:MAR",   "MAR"),   ("NASDAQ:MSTR",  "MSTR"),  ("NASDAQ:MPWR",  "MPWR"),
    ("NASDAQ:PDD",   "PDD"),   ("NASDAQ:STX",   "STX"),   ("NYSE:SHOP",    "SHOP"),
    ("NYSE:TRI",     "TRI"),   ("NYSE:WMT",     "WMT"),   ("NASDAQ:WDC",   "WDC"),
    ("NASDAQ:ALNY",  "ALNY"),  ("NASDAQ:AEP",   "AEP"),
]

SYMBOL_MAP = {tv: name for tv, name in NASDAQ_100}


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


# ─── State / tracker persistence ─────────────────────────────────────────────

def _load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
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
class NasdaqSnapshot:
    symbol: str
    name: str
    bar_time: int
    px_mn: int
    px_w1: int
    px_d1: int  # = weekly in 3M/M/W mode
    streak_3m: int
    streak_m: int
    streak_w: int
    aligned: int
    bias_state_before: int
    bias_state_after: int
    bias_entry: float | None
    pnl_pct: float | None
    last_event_time: int | None
    last_event_price: float | None
    close: float
    ts_utc: int


# ─── Scan one symbol ──────────────────────────────────────────────────────────

def scan_symbol(symbol: str, name: str, atr_length: int,
                start_date_utc: int | None, state: dict,
                debug: bool = False) -> NasdaqSnapshot | None:

    # Timeframes: 3M (quarterly) / M (monthly) / W (weekly)
    brick_mn = _fetch_renko_with_fallback(symbol, "3M", atr_length, 100, debug)
    if not brick_mn:
        if debug:
            print(f"  [{symbol}] no renko 3M")
        return None

    brick_w1 = _fetch_renko_with_fallback(symbol, "M", atr_length, 200, debug)
    if not brick_w1:
        if debug:
            print(f"  [{symbol}] no renko M")
        return None

    brick_d1 = _fetch_renko_with_fallback(symbol, "W", atr_length, 200, debug)
    if not brick_d1:
        if debug:
            print(f"  [{symbol}] no renko W")
        return None

    bars_d = fetch_tv_ohlc(symbol, "W", 2)
    if not bars_d:
        if debug:
            print(f"  [{symbol}] no weekly OHLC for current price")
        return None

    curr_close = float(bars_d[-1]["close"])
    bar_time   = int(bars_d[-1]["time"])

    last_3m = brick_mn[-1]
    last_m  = brick_w1[-1]
    last_w  = brick_d1[-1]

    px_mn = px_state(last_3m["open"], last_3m["close"], curr_close)
    px_w1 = px_state(last_m["open"],  last_m["close"],  curr_close)
    px_d1 = px_state(last_w["open"],  last_w["close"],  curr_close)

    streak_3m = green_streak(brick_mn)
    streak_m  = green_streak(brick_w1)
    streak_w  = green_streak(brick_d1)

    aligned_bull = px_mn == 1 and px_w1 == 1 and px_d1 == 1
    aligned_bear = px_mn == -1 and px_w1 == -1 and px_d1 == -1
    aligned = 1 if aligned_bull else (-1 if aligned_bear else 0)

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

    return NasdaqSnapshot(
        symbol=symbol, name=name, bar_time=bar_time,
        px_mn=px_mn, px_w1=px_w1, px_d1=px_d1,
        streak_3m=streak_3m, streak_m=streak_m, streak_w=streak_w,
        aligned=aligned,
        bias_state_before=bias_before, bias_state_after=bias_after,
        bias_entry=bias_entry, pnl_pct=pnl_pct,
        last_event_time=last_t, last_event_price=last_p,
        close=curr_close, ts_utc=bar_time,
    )


# ─── Historical init ──────────────────────────────────────────────────────────

def init_bias_from_history(symbol: str, start_date_utc: int,
                            atr_length: int, debug: bool = False) -> dict:
    # Timeframes: 3M / M / W
    bars_3m = fetch_tv_ohlc(symbol, "3M", 100)
    bars_mn = fetch_tv_ohlc(symbol, "M",  200)
    bars_w1 = fetch_tv_ohlc(symbol, "W",  200)

    if not bars_3m or not bars_mn or not bars_w1:
        if debug:
            print(f"  [{symbol}] init: missing OHLC")
        return {}

    atr_3m = _atr_from_ohlc(bars_3m, atr_length)
    atr_mn = _atr_from_ohlc(bars_mn, atr_length)
    atr_w1 = _atr_from_ohlc(bars_w1, atr_length)

    if not atr_3m or not atr_mn or not atr_w1:
        if debug:
            print(f"  [{symbol}] init: ATR failed")
        return {}

    series_3m = _renko_brick_series(bars_3m, atr_3m)
    series_mn = _renko_brick_series(bars_mn, atr_mn)
    series_w1 = _renko_brick_series(bars_w1, atr_w1)

    bias_state = 0
    bias_entry: float | None = None
    last_event_time: int | None = None
    last_event_price: float | None = None

    # Walk weekly bars to replay alignment events
    for bar in bars_w1:
        t = int(bar["time"])
        if t < start_date_utc:
            continue
        price = float(bar["close"])
        b_3m  = _brick_at(series_3m, t)
        b_mn  = _brick_at(series_mn, t)
        b_w1  = _brick_at(series_w1, t)
        if not b_3m or not b_mn or not b_w1:
            continue

        pmn = px_state(b_3m["open"], b_3m["close"], price)
        pw1 = px_state(b_mn["open"], b_mn["close"], price)
        pd1 = px_state(b_w1["open"], b_w1["close"], price)

        if pmn == 1 and pw1 == 1 and pd1 == 1 and bias_state != 1:
            bias_state, bias_entry, last_event_time, last_event_price = 1, price, t, price
        elif pmn == -1 and pw1 == -1 and pd1 == -1 and bias_state != -1:
            bias_state, bias_entry, last_event_time, last_event_price = -1, price, t, price

    if debug:
        label = "BULL" if bias_state == 1 else "BEAR" if bias_state == -1 else "MIXED"
        dt = datetime.fromtimestamp(last_event_time, tz=pytz.UTC).strftime("%Y-%m-%d") if last_event_time else "---"
        print(f"  [{symbol}] init => {label} entry={bias_entry} last={dt}")

    return {"biasState": bias_state, "biasEntry": bias_entry,
            "lastEventTime": last_event_time, "lastEventPrice": last_event_price}


# ─── Tracker ──────────────────────────────────────────────────────────────────

def update_tracker(tracker: dict, snaps: list[NasdaqSnapshot], paris_tz) -> list[str]:
    now_paris = datetime.now(paris_tz)
    today_str = now_paris.strftime("%Y-%m-%d")
    close_alerts = []

    # Only track top-10 by PnL%
    positive = sorted(
        [s for s in snaps if s.pnl_pct is not None and s.pnl_pct >= 2.0
         and s.bias_state_after == 1 and s.px_mn == 1 and s.px_w1 == 1 and s.px_d1 == 1
         and s.streak_w >= 1],
        key=lambda s: (s.streak_3m + s.streak_m + s.streak_w) / 3 * (1 + (s.pnl_pct or 0) / 100), reverse=True
    )
    active_positive = {s.symbol for s in positive[:10]}

    for sym in active_positive:
        if sym not in tracker or tracker[sym].get("removed"):
            snap = next(s for s in snaps if s.symbol == sym)
            tracker[sym] = {
                "name": snap.name,
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
            close_alerts.append(f"⚠️ CLOSE {snap.name} | PnL {snap.pnl_pct:+.2f}%")

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


# ─── PnL history & ROC(14) ───────────────────────────────────────────────────

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


def roc14(history: dict, symbol: str) -> float | None:
    """Rate of Change over 14 periods on the daily PnL% series. None if not enough data."""
    entries = history.get(symbol, [])
    if len(entries) < 15:
        return None
    p_now  = entries[-1]["pnl"]
    p_prev = entries[-15]["pnl"]
    if p_prev == 0:
        return None
    return (p_now - p_prev) / abs(p_prev) * 100


# ─── Telegram ─────────────────────────────────────────────────────────────────

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
    p = argparse.ArgumentParser(description="Renko MN/W1/D1 alignment — Nasdaq 100")
    p.add_argument("--length",     type=int,   default=14)
    p.add_argument("--start-date", type=str,   default="2026-04-01",
                   help='Bias start date UTC. Formats: "YYYY-MM-DD" or epoch seconds.')
    p.add_argument("--init",       action="store_true",
                   help="Replay history to initialise bias, then scan.")
    p.add_argument("--no-telegram", action="store_true")
    p.add_argument("--filter-positive", action="store_true",
                   help="Show only stocks with positive PnL%.")
    p.add_argument("--allow-weekend", action="store_true",
                   help="Force a live scan on Saturday/Sunday for diagnostics.")
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
            "Skipping live Nasdaq scan. Use --allow-weekend to force it."
        )
        return 0

    start_date_utc = _parse_date_utc(args.start_date)
    if args.start_date and start_date_utc is None:
        print('Invalid --start-date.')
        return 2

    # Live Nasdaq-100 list from Wikipedia (fallback to hardcoded)
    print("Fetching Nasdaq-100 list from Wikipedia...", end=" ", flush=True)
    symbols = fetch_nasdaq_100_from_wiki()
    if symbols:
        print(f"{len(symbols)} symbols loaded.")
    else:
        symbols = NASDAQ_100
        print(f"failed — using hardcoded list ({len(symbols)} symbols).")

    state   = _load_json(STATE_PATH)
    tracker = _load_json(TRACKER_PATH)

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

    # ── Scan — 8 symbols in parallel, 15s timeout each ──
    snaps: list[NasdaqSnapshot] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(scan_symbol, tv_sym, name, args.length, start_date_utc, state, args.debug): (tv_sym, name)
            for tv_sym, name in symbols
        }
        for future, (tv_sym, name) in futures.items():
            try:
                snap = future.result(timeout=15)
            except FuturesTimeoutError:
                future.cancel()
                print(f"  {name}: timeout (>15s) — skipped")
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

    # ── PnL history + ROC(14) ──
    history = _load_json(HISTORY_PATH)
    update_pnl_history(history, snaps, today_str)
    _save_json(HISTORY_PATH, history)

    positive = [s for s in snaps if s.pnl_pct is not None and s.pnl_pct >= 2.0
                and s.bias_state_after == 1
                and s.px_mn == 1 and s.px_w1 == 1 and s.px_d1 == 1
                and s.streak_w >= 1]
    top10  = positive[:10]
    top_20 = positive[10:20]

    def _roc_tag(sym: str) -> tuple[float | None, str]:
        r = roc14(history, sym)
        if r is None:
            return None, ""
        return r, " ⚠️" if r < 0 else " 📈"

    def _streak_fmt(n: int, px: int) -> str:
        if n == 0 and px == 1:
            return "↑"   # brick rouge absorbé — prix au-dessus
        return str(n)

    def _streak_str(s) -> str:
        q = _streak_fmt(s.streak_3m, s.px_mn)
        m = _streak_fmt(s.streak_m,  s.px_w1)
        w = _streak_fmt(s.streak_w,  s.px_d1)
        return f" [{q}Q/{m}M/{w}W]"

    # ── TOP 10 ──
    lines_top10 = []
    weakening = []
    for i, s in enumerate(top10, 1):
        dot    = "🟢" if s.bias_state_after == 1 else "🔴"
        r, tag = _roc_tag(s.symbol)
        roc_str = f" ROC{r:+.0f}%" if r is not None else ""
        entry = tracker.get(s.symbol, {})
        added_day = (entry.get("added_at") or "")[:10]
        new_tag = " 🆕" if added_day == today_str else ""
        lines_top10.append(f"{i:>2}. {dot} {s.name} (+{s.pnl_pct:.2f}%){new_tag}{roc_str}{tag}")
        if r is not None and r < 0:
            weakening.append(s)

    # ── TOP 11-20 ──
    lines_watch = []
    rising = []
    for i, s in enumerate(top_20, 11):
        dot    = "🟢" if s.bias_state_after == 1 else "🔴"
        r, tag = _roc_tag(s.symbol)
        roc_str = f" ROC{r:+.0f}%" if r is not None else ""
        lines_watch.append(f"{i:>2}. {dot} {s.name} (+{s.pnl_pct:.2f}%){roc_str}{tag}")
        if r is not None and r > 0:
            rising.append((s, r))

    # ── ROTATIONS ──
    rotation_lines = []
    if weakening and rising:
        rising_sorted = sorted(rising, key=lambda x: x[1], reverse=True)
        for weak in weakening:
            r_weak, _ = _roc_tag(weak.symbol)
            candidate, r_up = rising_sorted[0]
            rotation_lines.append(
                f"🔄 {candidate.name} (+{candidate.pnl_pct:.2f}% ROC{r_up:+.0f}%)"
                f" → remplace {weak.name} (+{weak.pnl_pct:.2f}% ROC{r_weak:+.0f}%)"
            )

    # ── Build message ──
    parts = []
    if lines_top10:
        parts.append("📊 NASDAQ TOP 10\n" + "\n".join(lines_top10))
    if lines_watch:
        parts.append("👀 SURVEILLANCE (11-20)\n" + "\n".join(lines_watch))
    if rotation_lines:
        parts.append("🔄 ROTATIONS À SURVEILLER\n" + "\n".join(rotation_lines))

    if parts:
        msg = "\n\n".join(parts) + f"\n\n⏰ {now_str} Paris"
        print(f"\n{msg}\n")
        if not args.no_telegram:
            send_telegram(msg)

    # ── Detailed output ──
    if not args.filter_positive:
        for s in snaps:
            ts       = datetime.fromtimestamp(s.ts_utc, tz=pytz.UTC).strftime("%Y-%m-%d")
            pnl_txt  = "---" if s.pnl_pct is None else f"{s.pnl_pct:+.2f}%"
            bias_txt = "BULL" if s.bias_state_after == 1 else "BEAR" if s.bias_state_after == -1 else "MIXED"
            last_txt = "---"
            if s.last_event_time and s.last_event_price is not None:
                last_dt  = datetime.fromtimestamp(s.last_event_time, tz=pytz.UTC).strftime("%Y-%m-%d")
                last_txt = f"{last_dt}@{s.last_event_price:.2f}"
            r, tag = _roc_tag(s.symbol)
            roc_str = f" ROC{r:+.0f}%" if r is not None else ""
            print(f"{s.name:<6} | close={s.close:>10.2f} | 3M={s.px_mn:+d}({s.streak_3m}) M={s.px_w1:+d}({s.streak_m}) W={s.px_d1:+d}({s.streak_w}) | "
                  f"Bias {bias_txt} | PnL {pnl_txt}{roc_str}{tag} | Last {last_txt} | {ts}")

    # ── Tracker + close alerts ──
    close_alerts = update_tracker(tracker, snaps, paris_tz)
    _save_json(TRACKER_PATH, tracker)

    if close_alerts:
        close_msg = "🚨 NASDAQ CLOSE ALERTS\n" + "\n".join(close_alerts) + f"\n\n⏰ {now_str} Paris"
        print(f"\n{close_msg}\n")
        if not args.no_telegram:
            send_telegram(close_msg)

    _save_json(STATE_PATH, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
