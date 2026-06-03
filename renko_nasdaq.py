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
PORTFOLIO_PATH = os.path.join(SCRIPT_DIR, "renko_nasdaq_portfolio.json")
CLOSE_THRESHOLD = -0.05  # % threshold for close alert
PORTFOLIO_INITIAL_CAPITAL = 10000.0
PORTFOLIO_POSITION_BUDGET = 1000.0
PORTFOLIO_FEE = 1.0

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
    price_roc14: float | None
    price_roc21: float | None
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

    bars_d = fetch_tv_ohlc(symbol, "W", 24)
    if not bars_d:
        if debug:
            print(f"  [{symbol}] no weekly OHLC for current price")
        return None

    curr_close = float(bars_d[-1]["close"])
    bar_time   = int(bars_d[-1]["time"])
    price_roc14: float | None = None
    price_roc21: float | None = None
    if len(bars_d) >= 15:
        prev_close = float(bars_d[-15]["close"])
        if prev_close:
            price_roc14 = (curr_close - prev_close) / prev_close * 100
    if len(bars_d) >= 22:
        prev_close21 = float(bars_d[-22]["close"])
        if prev_close21:
            price_roc21 = (curr_close - prev_close21) / prev_close21 * 100

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
        bias_entry=bias_entry, pnl_pct=pnl_pct, price_roc14=price_roc14, price_roc21=price_roc21,
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


def update_rank_exit_alerts(
    tracker: dict,
    snaps: list[NasdaqSnapshot],
    current_ranked: list[NasdaqSnapshot],
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
        exit_alerts.append(f"⚠️ OUT TOP15 {snap.name} | PnL {pnl_txt}{prev_txt}")

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
        equity += qty * px
    return equity


def close_alert_symbols(close_alerts: list[str], snaps: list[NasdaqSnapshot]) -> set[str]:
    by_name = {s.name: s.symbol for s in snaps}
    symbols = set()
    for alert in close_alerts:
        for marker in ("CLOSE ", "OUT TOP15 "):
            idx = alert.find(marker)
            if idx < 0:
                continue
            name = alert[idx + len(marker):].split("|", 1)[0].strip()
            sym = by_name.get(name)
            if sym:
                symbols.add(sym)
            break
    return symbols


def portfolio_buy_targets(
    positive: list[NasdaqSnapshot],
    held_symbols: set[str] | None = None,
    limit: int = 10,
    protect_rank: int = 15,
) -> list[NasdaqSnapshot]:
    """Build portfolio targets, preserving held TOP15 names and one Alphabet class."""
    held_symbols = held_symbols or set()
    alphabet_symbols = {"NASDAQ:GOOG", "NASDAQ:GOOGL"}
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
    buy_targets: list[NasdaqSnapshot],
    close_alerts: list[str],
    snaps: list[NasdaqSnapshot],
    now_str: str,
) -> tuple[list[str], dict]:
    price_map = {s.symbol: s.close for s in snaps if s.close is not None}
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
        gross = qty * px
        proceeds = max(0.0, gross - PORTFOLIO_FEE)
        realized = (px - avg) * qty - PORTFOLIO_FEE
        portfolio["cash"] = float(portfolio.get("cash", 0.0) or 0.0) + proceeds
        portfolio["fees_paid"] = float(portfolio.get("fees_paid", 0.0) or 0.0) + PORTFOLIO_FEE
        positions.pop(sym, None)
        trades.append({
            "time": now_str,
            "side": "SELL",
            "symbol": sym,
            "name": name_map.get(sym, sym),
            "qty": qty,
            "price": round(px, 4),
            "gross": round(gross, 2),
            "fee": PORTFOLIO_FEE,
            "notional": round(proceeds, 2),
            "realized_pnl": round(realized, 2),
            "cash_after": round(float(portfolio.get("cash", 0.0) or 0.0), 2),
            "reason": reason,
        })

    sell_symbols = close_alert_symbols(close_alerts, snaps)
    over_budget_symbols = set()
    for sym in list(positions.keys()):
        snap = snap_map.get(sym)
        if snap and (
            (snap.price_roc14 is not None and snap.price_roc14 < 0)
            or (snap.price_roc21 is not None and snap.price_roc21 < 0)
        ):
            sell_symbols.add(sym)
        qty = int(positions[sym].get("qty", 0) or 0)
        avg = float(positions[sym].get("avg_price", price_map.get(sym, 0.0)) or 0.0)
        if qty * avg > PORTFOLIO_POSITION_BUDGET:
            sell_symbols.add(sym)
            over_budget_symbols.add(sym)

    # Sell explicit close alerts and any open position with negative ROC14.
    for sym in sorted(sell_symbols):
        if sym in over_budget_symbols:
            reason = "BUDGET > 1000"
        else:
            snap_s = snap_map.get(sym)
        roc_neg = snap_s and (
            (snap_s.price_roc14 is not None and snap_s.price_roc14 < 0)
            or (snap_s.price_roc21 is not None and snap_s.price_roc21 < 0)
        )
        reason = "ROC < 0" if sym in positions and roc_neg else "CLOSE ALERT"
        sell_position(sym, reason)

    # Buy missing target symbols with a fixed budget per position.
    missing_top10 = [
        s for s in buy_targets
        if s.symbol not in positions
        and s.close > 0
        and (s.price_roc14 is None or s.price_roc14 >= 0)
        and (s.price_roc21 is None or s.price_roc21 >= 0)
    ]
    cash = float(portfolio.get("cash", 0.0) or 0.0)
    if missing_top10 and cash > 0:
        for s in missing_top10:
            px = float(s.close)
            available = min(PORTFOLIO_POSITION_BUDGET + PORTFOLIO_FEE, float(portfolio.get("cash", 0.0) or 0.0))
            qty = int((available - PORTFOLIO_FEE) // px)
            if qty <= 0:
                continue
            gross = qty * px
            total_cost = gross + PORTFOLIO_FEE
            if total_cost > float(portfolio.get("cash", 0.0) or 0.0):
                continue
            positions[s.symbol] = {
                "name": s.name,
                "qty": qty,
                "avg_price": px,
                "entry_time": now_str,
                "last_price": px,
            }
            portfolio["cash"] = float(portfolio.get("cash", 0.0) or 0.0) - total_cost
            portfolio["fees_paid"] = float(portfolio.get("fees_paid", 0.0) or 0.0) + PORTFOLIO_FEE
            trades.append({
                "time": now_str,
                "side": "BUY",
                "symbol": s.symbol,
                "name": s.name,
                "qty": qty,
                "price": round(px, 4),
                "gross": round(gross, 2),
                "fee": PORTFOLIO_FEE,
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
    open_rocs = [
        s.price_roc14 for s in snaps
        if s.symbol in positions and s.price_roc14 is not None
    ]
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
    bought_symbols = {t["symbol"] for t in trades if t["side"] == "BUY"}
    sold_trades = [t for t in trades if t["side"] != "BUY"]
    summary = [
        "💼 NASDAQ PORTFOLIO SIM",
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

    for sym, pos in ordered_positions:
        qty = int(pos.get("qty", 0) or 0)
        new_txt = " 🆕" if sym in bought_symbols else ""
        snap = snap_map.get(sym)
        roc14 = snap.price_roc14 if snap else None
        roc21 = snap.price_roc21 if snap else None
        roc_txt = ""
        if roc14 is not None:
            roc_txt += f" R14:{roc14:+.2f}%"
        if roc21 is not None:
            roc_txt += f" R21:{roc21:+.2f}%"
        summary.append(f"🟢{pos.get('name', sym)} ({qty}){roc_txt}{new_txt}")

    if sold_trades:
        summary.append("🚨 NASDAQ CLOSE ALERTS")
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
    if not portfolio.get("first_run"):
        portfolio["first_run"] = now_str
    portfolio_first_run = str(portfolio.get("first_run") or now_str)

    # ── PnL history ──
    history = _load_json(HISTORY_PATH)
    update_pnl_history(history, snaps, today_str)
    _save_json(HISTORY_PATH, history)

    positive = [s for s in snaps if s.pnl_pct is not None and s.pnl_pct >= 2.0
                and s.bias_state_after == 1
                and s.px_mn == 1 and s.px_w1 == 1 and s.px_d1 == 1
                and s.streak_w >= 1]
    top10  = positive[:10]
    top_15 = positive[10:15]
    current_ranked = top10 + top_15
    held_symbols = set(portfolio.get("positions", {}).keys())
    buy_targets = portfolio_buy_targets(positive, held_symbols=held_symbols, limit=10)

    def _price_roc_tag(s: NasdaqSnapshot) -> tuple[float | None, str]:
        roc = s.price_roc14
        if roc is None:
            return None, ""
        if roc >= 50:
            return roc, " 🔥"
        return roc, " ⚠️" if roc < 0 else " 📈"

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
        roc, tag = _price_roc_tag(s)
        entry = tracker.get(s.symbol, {})
        added_day = (entry.get("added_at") or "")[:10]
        new_tag = " 🆕" if added_day == today_str else ""
        lines_top10.append(f"{i:>2}. {dot} {s.name} (+{s.pnl_pct:.2f}%){new_tag}{tag}")
        if roc is not None and roc < 0:
            weakening.append(s)

    # ── TOP 11-15 ──
    lines_watch = []
    rising = []
    for i, s in enumerate(top_15, 11):
        dot    = "🟢" if s.bias_state_after == 1 else "🔴"
        roc, tag = _price_roc_tag(s)
        lines_watch.append(f"{i:>2}. {dot} {s.name} (+{s.pnl_pct:.2f}%){tag}")
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
        f"{s.name} (+{s.pnl_pct:.2f}%){tag}"
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
                f"🔄 {candidate.name} (+{candidate.pnl_pct:.2f}%){up_tag}"
                f" → remplace {weak.name} (+{weak.pnl_pct:.2f}%){weak_tag}"
            )

    # ── Build message ──
    parts = []
    if lines_top10:
        parts.append("📊 NASDAQ TOP 10\n" + "\n".join(lines_top10))
    if lines_watch:
        parts.append("👀 SURVEILLANCE (11-15)\n" + "\n".join(lines_watch))
    if lines_momentum:
        parts.append("🔥 DYNAMIQUE FORTE HORS TOP 10\n" + "\n".join(lines_momentum))
    if rotation_lines:
        parts.append("🔄 ROTATIONS À SURVEILLER\n" + "\n".join(rotation_lines))

    if parts:
        msg = "\n\n".join(parts) + f"\n\n📅 First run {portfolio_first_run}\n⏰ {now_str} Paris"
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
            roc, tag = _price_roc_tag(s)
            roc_str = f" ROC14{roc:+.2f}%" if roc is not None else ""
            if s.price_roc21 is not None:
                roc_str += f" ROC21{s.price_roc21:+.2f}%"
            print(f"{s.name:<6} | close={s.close:>10.2f} | 3M={s.px_mn:+d}({s.streak_3m}) M={s.px_w1:+d}({s.streak_m}) W={s.px_d1:+d}({s.streak_w}) | "
                  f"Bias {bias_txt} | PnL {pnl_txt}{roc_str}{tag} | Last {last_txt} | {ts}")

    # ── Tracker + close alerts ──
    close_alerts = update_tracker(tracker, snaps, paris_tz)
    close_alerts.extend(update_rank_exit_alerts(tracker, snaps, current_ranked, paris_tz))
    _save_json(TRACKER_PATH, tracker)

    if close_alerts:
        close_msg = "🚨 NASDAQ CLOSE ALERTS\n" + "\n".join(close_alerts) + f"\n\n⏰ {now_str} Paris"
        print(f"\n{close_msg}\n")
        if not args.no_telegram:
            send_telegram(close_msg)

    portfolio_lines, _portfolio_info = update_portfolio_simulation(
        portfolio, buy_targets, close_alerts, snaps, now_str
    )
    _save_json(PORTFOLIO_PATH, portfolio)

    if portfolio_lines:
        portfolio_msg = "\n".join(portfolio_lines) + f"\n\n⏰ {now_str} Paris"
        print(f"\n{portfolio_msg}\n")
        if not args.no_telegram:
            send_telegram(portfolio_msg)

    _save_json(STATE_PATH, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
