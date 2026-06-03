#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
renko_etf_strategy_V3.py
Renko ATR W/D scanner for the ETF strategy watchlist.

Logic mirrors renko_forex_V3.pine:
- M/W/D Renko alignment
- Daily directional streak >= 1
- long-only candidates ranked by relative ratio strength over 7/14/21 days
- ROC 7/14/21 filter: at least 2 of 3 positive
- RSI14 displayed as context
"""

import argparse
import json
import os
import random
import string
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime

if sys.stdout.encoding and sys.stdout.encoding.lower().replace("-", "") != "utf8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import pytz
import requests as http_requests
from dotenv import load_dotenv
from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException, create_connection

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(SCRIPT_DIR, "renko_etf_strategy_v3_state.json")
METRICS_PATH = os.path.join(SCRIPT_DIR, "renko_etf_strategy_v3_metrics.json")
# Passerelle: TOP 3 ETF UCITS exportes par nasdaq_sector_pipeline.py, fusionnes
# automatiquement dans l'univers ci-dessous.
TOP_UCITS_PATH = os.path.join(SCRIPT_DIR, "nasdaq_top_ucits_etf.json")
METRICS_ALPHA = 0.25
METRICS_MAX_HISTORY = 120
TREND_THRESHOLD = 0.25
LEADER_ARROW_THRESHOLD = 0.30
LEADER_STRONG_THRESHOLD = 1.00
WATCH_THRESHOLD = 0.50

WS_URL = "wss://prodata.tradingview.com/socket.io/websocket"
WS_HEADERS = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

ETF_STRATEGY_ASSETS: list[tuple[str, str]] = [
    ("EURONEXT:WQTM", "WQTM"),
    ("EURONEXT:LQQ", "LQQ"),
    ("EURONEXT:CL2", "CL2"),
    ("EURONEXT:PUST", "PUST"),
    ("EURONEXT:LWLD", "LWLD"),
    ("EURONEXT:PAEEM", "PAEEM"),
    ("EURONEXT:PSP5", "PSP5"),
    ("EURONEXT:ISRU", "ISRU"),
    ("NASDAQ:JEPQ", "JEPQ"),
    ("EURONEXT:USVE", "USVE"),
    ("EURONEXT:WEBH", "WEBH"),
    ("EURONEXT:LVE", "LVE"),
    ("EURONEXT:ISOE", "ISOE"),
]


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
        out.append(raw[i : i + size])
        i += size
    return out if out else [raw]


def fetch_tv_ohlc(symbol: str, interval: str, n_candles: int, timeout_s: int = 20, retries: int = 2) -> list[dict] | None:
    for attempt in range(retries + 1):
        ws = None
        try:
            ws = create_connection(WS_URL, header=WS_HEADERS, timeout=timeout_s)
            ws.settimeout(timeout_s)
            sid = _gen_sid()
            ws.send(_msg("chart_create_session", [sid, ""]))
            ws.send(_msg("resolve_symbol", [sid, "sds_sym_1", f'={{"symbol":"{symbol}","adjustment":"splits","session":"regular"}}']))
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
                if "series_completed" in raw:
                    break

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


def fetch_tv_renko_ohlc(
    symbol: str,
    interval: str,
    atr_length: int = 14,
    n_bricks: int = 200,
    timeout_s: int = 25,
    retries: int = 2,
    debug: bool = False,
) -> list[dict] | None:
    wait_s = 18
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
                    "source": "close",
                    "sources": "Close",
                    "boxSize": atr_length,
                    "style": "ATR",
                    "atrLength": atr_length,
                    "wicks": True,
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
                if symbol_error:
                    break
                if "series_completed" in raw:
                    break

            if debug:
                print(f"  [renko {interval}] {symbol} attempt={attempt} symbol_error={symbol_error} points={len(points)}")
            if symbol_error:
                break
            rows = []
            for item in points:
                v = item.get("v", [])
                if len(v) >= 5:
                    rows.append({"time": v[0], "open": float(v[1]), "high": float(v[2]), "low": float(v[3]), "close": float(v[4])})
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


def _atr_from_ohlc(bars: list[dict], length: int) -> float | None:
    if len(bars) < length + 1:
        return None
    trs = []
    prev_close = float(bars[0]["close"])
    for b in bars[1:]:
        high, low, close = float(b["high"]), float(b["low"]), float(b["close"])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        prev_close = close
    return sum(trs[-length:]) / length if len(trs) >= length else None


def _renko_brick_series(bars: list[dict], box_size: float) -> list[dict]:
    if not bars or box_size <= 0:
        return []
    ref = float(bars[0]["close"])
    bricks = []
    last_close = ref
    for b in bars[1:]:
        px = float(b["close"])
        t = int(b.get("time", 0))
        while abs(px - last_close) >= box_size:
            direction = 1.0 if px > last_close else -1.0
            b_open = last_close
            b_close = last_close + direction * box_size
            bricks.append({"time": t, "open": b_open, "close": b_close})
            last_close = b_close
    return bricks


def _fetch_renko_with_fallback(symbol: str, interval: str, atr_length: int, n_candles: int, debug: bool = False) -> list[dict] | None:
    bricks = fetch_tv_renko_ohlc(symbol, interval, atr_length=atr_length, n_bricks=n_candles, debug=debug)
    if bricks:
        return bricks
    bars = fetch_tv_ohlc(symbol, interval, n_candles)
    if not bars:
        if debug:
            print(f"  [{symbol}] no OHLC for {interval}")
        return None
    atr = _atr_from_ohlc(bars, atr_length)
    if not atr:
        return None
    series = _renko_brick_series(bars, atr)
    if debug and series:
        print(f"  [{symbol}] renko {interval} fallback box={atr:.4f} bricks={len(series)}")
    return series or None


def px_state(renko_open: float, renko_close: float, price: float) -> int:
    high, low = max(renko_open, renko_close), min(renko_open, renko_close)
    return 1 if price > high else (-1 if price < low else 0)


def green_streak(bricks: list[dict], max_bars: int = 50) -> int:
    count = 0
    for b in reversed(bricks[-max_bars:]):
        if float(b["close"]) > float(b["open"]):
            count += 1
        else:
            break
    return count


def red_streak(bricks: list[dict], max_bars: int = 50) -> int:
    count = 0
    for b in reversed(bricks[-max_bars:]):
        if float(b["close"]) < float(b["open"]):
            count += 1
        else:
            break
    return count


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


def load_dynamic_ucits() -> list[tuple[str, str]]:
    """Lit le TOP 3 ETF UCITS exporte par nasdaq_sector_pipeline.py et renvoie
    une liste (symbole TradingView, nom court) a fusionner dans l'univers.
    Tolerant: fichier absent / illisible -> liste vide."""
    data = _load_json(TOP_UCITS_PATH)
    out: list[tuple[str, str]] = []
    for e in data.get("etfs", []):
        sym = str(e.get("tv_symbol") or "").strip()
        name = str(e.get("ticker") or e.get("name") or "").strip()
        if sym and name:
            out.append((sym, name))
    return out


def _smooth(prev: float | None, value: float | None, alpha: float = METRICS_ALPHA) -> float | None:
    if value is None:
        return prev
    if prev is None:
        return value
    return prev + alpha * (value - prev)


@dataclass
class Snapshot:
    symbol: str
    name: str
    bar_time: int
    px_m: int
    px_w: int
    px_d: int
    streak_m: int
    streak_w: int
    streak_d: int
    aligned: int
    bias_before: int
    bias_after: int
    close: float
    daily_closes: list[float] | None = None
    roc7: float | None = None
    roc14: float | None = None
    roc21: float | None = None
    rsi14: float | None = None
    rs_wins: int = 0
    rs_total: int = 0
    rs_avg_pct: float | None = None


def scan_symbol(symbol: str, name: str, atr_length: int, start_date_utc: int | None, state: dict, debug: bool = False) -> Snapshot | None:
    brick_m = _fetch_renko_with_fallback(symbol, "M", atr_length, 200, debug)
    brick_w = _fetch_renko_with_fallback(symbol, "W", atr_length, 200, debug)
    brick_d = _fetch_renko_with_fallback(symbol, "D", atr_length, 200, debug)
    if not brick_m or not brick_w or not brick_d:
        if debug:
            print(f"  [{symbol}] missing M/W/D Renko")
        return None

    bars_d = fetch_tv_ohlc(symbol, "D", 30)
    if not bars_d:
        if debug:
            print(f"  [{symbol}] no daily OHLC")
        return None

    curr_close = float(bars_d[-1]["close"])
    bar_time = int(bars_d[-1]["time"])
    daily_closes = [float(b["close"]) for b in bars_d]
    roc7 = roc(daily_closes, 7)
    roc14 = roc(daily_closes, 14)
    roc21 = roc(daily_closes, 21)
    rsi14 = rsi(daily_closes, 14)

    px_m = px_state(brick_m[-1]["open"], brick_m[-1]["close"], curr_close)
    px_w = px_state(brick_w[-1]["open"], brick_w[-1]["close"], curr_close)
    px_d = px_state(brick_d[-1]["open"], brick_d[-1]["close"], curr_close)
    aligned_bull = px_m == 1 and px_w == 1 and px_d == 1
    aligned_bear = px_m == -1 and px_w == -1 and px_d == -1
    aligned = 1 if aligned_bull else (-1 if aligned_bear else 0)

    bias_before = int(state.get(symbol, {}).get("biasState", 0))
    stored_start = state.get(symbol, {}).get("startDate")
    if start_date_utc is not None and stored_start != start_date_utc:
        bias_before = 0

    if start_date_utc is not None and bar_time < start_date_utc:
        bias_after = 0
    else:
        bias_after = bias_before
        if aligned_bull and bias_before != 1:
            bias_after = 1
        elif aligned_bear and bias_before != -1:
            bias_after = -1

    state.setdefault(symbol, {}).update({
        "biasState": bias_after,
        "startDate": start_date_utc,
        "updated_at": datetime.now(pytz.UTC).isoformat(),
    })

    return Snapshot(
        symbol=symbol,
        name=name,
        bar_time=bar_time,
        px_m=px_m,
        px_w=px_w,
        px_d=px_d,
        streak_m=green_streak(brick_m) if px_m == 1 else red_streak(brick_m),
        streak_w=green_streak(brick_w) if px_w == 1 else red_streak(brick_w),
        streak_d=green_streak(brick_d) if px_d == 1 else red_streak(brick_d),
        aligned=aligned,
        bias_before=bias_before,
        bias_after=bias_after,
        close=curr_close,
        daily_closes=daily_closes,
        roc7=roc7,
        roc14=roc14,
        roc21=roc21,
        rsi14=rsi14,
    )


def trade_direction(s: Snapshot) -> int:
    if s.bias_after == 1 and s.aligned == 1:
        return 1
    if s.bias_after == -1 and s.aligned == -1:
        return -1
    return 0


def trade_side_label(s: Snapshot) -> str:
    direction = trade_direction(s)
    return "LONG" if direction == 1 else ("SHORT" if direction == -1 else "MIXED")


def is_trade_candidate(s: Snapshot) -> bool:
    direction = trade_direction(s)
    if direction != 1:
        return False
    if s.streak_d < 1:
        return False
    if s.rs_total == 0 or s.rs_avg_pct is None:
        return False
    return s.rs_wins > s.rs_total / 2 and s.rs_avg_pct > 0 and roc_positive_count(s) >= 2


def roc(values: list[float], lookback: int) -> float | None:
    if len(values) <= lookback:
        return None
    past = values[-1 - lookback]
    if past == 0:
        return None
    return (values[-1] / past - 1.0) * 100.0


def rsi(values: list[float], length: int = 14) -> float | None:
    if len(values) <= length:
        return None
    gains = []
    losses = []
    for i in range(-length, 0):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains) / length
    avg_loss = sum(losses) / length
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def roc_positive_count(s: Snapshot) -> int:
    return sum(1 for value in (s.roc7, s.roc14, s.roc21) if value is not None and value > 0)


def fmt_pct(value: float | None) -> str:
    return "---" if value is None else f"{value:+.2f}%"


def update_metrics_tracker(metrics: dict, snaps: list[Snapshot], now_str: str) -> list[dict]:
    by_rank = sorted(
        snaps,
        key=lambda s: (
            s.rs_avg_pct if s.rs_avg_pct is not None else float("-inf"),
            s.rs_wins / s.rs_total if s.rs_total else float("-inf"),
        ),
        reverse=True,
    )
    ranks = {s.symbol: i for i, s in enumerate(by_rank, 1)}
    current = []
    assets = metrics.setdefault("assets", {})

    for snap in snaps:
        symbol_data = assets.setdefault(snap.symbol, {"name": snap.name})
        previous = symbol_data.get("smoothed", {})
        prev_smoothed_rs = previous.get("rs_avg_pct")
        prev_smoothed_rank = previous.get("rank")
        rank = ranks.get(snap.symbol)
        mode = trade_side_label(snap)
        roc_pos = roc_positive_count(snap)

        smoothed = {
            "rank": _smooth(prev_smoothed_rank, float(rank) if rank is not None else None),
            "rs_avg_pct": _smooth(prev_smoothed_rs, snap.rs_avg_pct),
            "rs_win_rate": _smooth(previous.get("rs_win_rate"), snap.rs_wins / snap.rs_total if snap.rs_total else None),
            "roc7": _smooth(previous.get("roc7"), snap.roc7),
            "roc14": _smooth(previous.get("roc14"), snap.roc14),
            "roc21": _smooth(previous.get("roc21"), snap.roc21),
            "rsi14": _smooth(previous.get("rsi14"), snap.rsi14),
            "roc_positive_count": _smooth(previous.get("roc_positive_count"), float(roc_pos)),
        }

        symbol_data["name"] = snap.name
        symbol_data["last"] = {
            "time": now_str,
            "rank": rank,
            "mode": mode,
            "candidate": is_trade_candidate(snap),
            "rs_wins": snap.rs_wins,
            "rs_total": snap.rs_total,
            "rs_avg_pct": snap.rs_avg_pct,
            "roc7": snap.roc7,
            "roc14": snap.roc14,
            "roc21": snap.roc21,
            "rsi14": snap.rsi14,
        }
        symbol_data["smoothed"] = smoothed
        history = symbol_data.setdefault("history", [])
        history.append({"time": now_str, **symbol_data["last"], "smoothed": smoothed})
        if len(history) > METRICS_MAX_HISTORY:
            symbol_data["history"] = history[-METRICS_MAX_HISTORY:]

        rs_trend = None
        rank_trend = None
        if prev_smoothed_rs is not None and smoothed["rs_avg_pct"] is not None:
            rs_trend = smoothed["rs_avg_pct"] - prev_smoothed_rs
        if prev_smoothed_rank is not None and smoothed["rank"] is not None:
            rank_trend = prev_smoothed_rank - smoothed["rank"]

        current.append({
            "name": snap.name,
            "mode": mode,
            "candidate": is_trade_candidate(snap),
            "smoothed_rank": smoothed["rank"],
            "smoothed_rs": smoothed["rs_avg_pct"],
            "rs_trend": rs_trend,
            "rank_trend": rank_trend,
        })

    metrics["last_update"] = now_str
    return current


def build_trend_lines(trends: list[dict], limit: int = 3) -> list[str]:
    improving = sorted(
        [t for t in trends if t.get("rs_trend") is not None and t["rs_trend"] >= WATCH_THRESHOLD],
        key=lambda t: (t["rs_trend"], t.get("rank_trend") or 0.0),
        reverse=True,
    )
    weakening = sorted(
        [t for t in trends if t.get("rs_trend") is not None and t["rs_trend"] <= -WATCH_THRESHOLD],
        key=lambda t: (t["rs_trend"], -(t.get("rank_trend") or 0.0)),
    )
    lines = []
    watch_items = []
    for item in improving[:limit]:
        watch_items.append(f"📈 {item['name']} improving")
    for item in weakening[:limit]:
        watch_items.append(f"📉 {item['name']} weakening")
    if watch_items:
        lines.append("Watch")
        lines.extend(watch_items)
    return lines


def trend_arrow(value: float | None) -> str:
    if value is None:
        return "→"
    if value >= LEADER_STRONG_THRESHOLD:
        return "↑↑"
    if value >= LEADER_ARROW_THRESHOLD:
        return "↑"
    if value <= -LEADER_STRONG_THRESHOLD:
        return "↓↓"
    if value <= -LEADER_ARROW_THRESHOLD:
        return "↓"
    return "→"


def pulse_label(candidates: list[Snapshot], trend_by_name: dict[str, dict]) -> str:
    improving = 0
    weakening = 0
    for snap in candidates:
        trend = trend_by_name.get(snap.name, {}).get("rs_trend")
        arrow = trend_arrow(trend)
        if arrow in ("↑", "↑↑"):
            improving += 1
        elif arrow in ("↓", "↓↓"):
            weakening += 1
    if improving > weakening:
        return "improving"
    if weakening > improving:
        return "weakening"
    return "stable"


def build_telegram_message(candidates: list[Snapshot], trends: list[dict], trend_lines: list[str]) -> str:
    trend_by_name = {item["name"]: item for item in trends}
    leader_lines = ["📊 ETF", f"Pulse: {pulse_label(candidates, trend_by_name)}", "", "Leaders"]
    for snap in candidates:
        rs = "---" if snap.rs_avg_pct is None else f"{snap.rs_avg_pct:+.2f}"
        trend = trend_by_name.get(snap.name, {}).get("rs_trend")
        leader_lines.append(f"🟢 {snap.name} {rs} {trend_arrow(trend)}")
    parts = ["\n".join(leader_lines)]
    if trend_lines:
        parts.append("\n".join(trend_lines))
    return "\n\n".join(parts)


def calculate_relative_strength(snaps: list[Snapshot], lookbacks: tuple[int, ...] = (7, 14, 21)) -> None:
    for snap in snaps:
        wins = 0
        total = 0
        rel_sum = 0.0
        closes_a = snap.daily_closes or []
        if not closes_a:
            continue
        for other in snaps:
            if other.symbol == snap.symbol:
                continue
            closes_b = other.daily_closes or []
            for lookback in lookbacks:
                if len(closes_a) <= lookback or len(closes_b) <= lookback:
                    continue
                a_now, a_then = closes_a[-1], closes_a[-1 - lookback]
                b_now, b_then = closes_b[-1], closes_b[-1 - lookback]
                if a_then == 0 or b_now == 0 or b_then == 0:
                    continue
                ratio_now = a_now / b_now
                ratio_then = a_then / b_then
                if ratio_then == 0:
                    continue
                rel_pct = (ratio_now / ratio_then - 1.0) * 100.0
                rel_sum += rel_pct
                total += 1
                if rel_pct > 0:
                    wins += 1
        snap.rs_wins = wins
        snap.rs_total = total
        snap.rs_avg_pct = rel_sum / total if total else None


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


def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = http_requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        return bool(resp.json().get("ok", False))
    except Exception:
        return False


def telegram_window_open(now_paris: datetime) -> bool:
    return 6 <= now_paris.hour < 21


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Renko M/W/D scanner - ETF strategy watchlist")
    parser.add_argument("--length", type=int, default=14)
    parser.add_argument("--start-date", type=str, default="2026-04-01")
    parser.add_argument("--only", type=str, default="", help="Comma-separated tickers to scan, e.g. LQQ,ISOE,JEPQ.")
    parser.add_argument("--update-metrics-partial", action="store_true", help="Allow metrics history updates when --only is used.")
    parser.add_argument("--force-telegram", action="store_true", help="Send Telegram even outside 06:00-21:00 Paris.")
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start_date_utc = _parse_date_utc(args.start_date)
    if args.start_date and start_date_utc is None:
        print("Invalid --start-date.")
        return 2

    state = _load_json(STATE_PATH)
    metrics = _load_json(METRICS_PATH)

    # Univers = 13 ETF fixes + TOP 3 ETF UCITS dynamiques du pipeline Nasdaq (dedup).
    universe = list(ETF_STRATEGY_ASSETS)
    existing = {sym.upper() for sym, _ in universe}
    dynamic_added: list[tuple[str, str]] = []
    for sym, name in load_dynamic_ucits():
        if sym.upper() not in existing:
            universe.append((sym, name))
            existing.add(sym.upper())
            dynamic_added.append((sym, name))
    if dynamic_added:
        print("+ ETF UCITS dynamiques (NASDAQ pipeline): "
              + ", ".join(f"{name} [{sym}]" for sym, name in dynamic_added))

    symbols = universe
    if args.only:
        wanted = {x.strip().upper() for x in args.only.split(",") if x.strip()}
        symbols = [(symbol, name) for symbol, name in universe if name.upper() in wanted]
    print(f"ETF strategy universe loaded: {len(symbols)} symbols.")

    snaps: list[Snapshot] = []
    skipped: list[str] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(scan_symbol, symbol, name, args.length, start_date_utc, state, args.debug): (symbol, name)
            for symbol, name in symbols
        }
        for future in as_completed(futures):
            symbol, name = futures[future]
            try:
                snap = future.result()
            except Exception as exc:
                if args.debug:
                    print(f"  {name}: error - {exc}")
                skipped.append(name)
                continue
            if snap:
                snaps.append(snap)
            else:
                skipped.append(name)

    _save_json(STATE_PATH, state)
    calculate_relative_strength(snaps)

    paris_tz = pytz.timezone("Europe/Paris")
    now_paris = datetime.now(paris_tz)
    now_str = now_paris.strftime("%Y-%m-%d %H:%M")
    update_metrics = not args.only or args.update_metrics_partial
    trend_lines: list[str] = []
    if update_metrics:
        trend_data = update_metrics_tracker(metrics, snaps, now_str)
        _save_json(METRICS_PATH, metrics)
        trend_lines = build_trend_lines(trend_data)
    candidates = sorted(
        [s for s in snaps if is_trade_candidate(s)],
        key=lambda s: (
            s.rs_avg_pct if s.rs_avg_pct is not None else float("-inf"),
            s.rs_wins / s.rs_total if s.rs_total else float("-inf"),
        ),
        reverse=True,
    )

    print("\nETF STRATEGY - M/W/D RENKO LONG-ONLY ANALYSIS")
    if skipped:
        print("Skipped/no data: " + ", ".join(sorted(skipped)))
    display_snaps = [s for s in snaps if trade_direction(s) != -1]
    for s in sorted(
        display_snaps,
        key=lambda x: (
            x.rs_avg_pct if x.rs_avg_pct is not None else float("-inf"),
            x.rs_wins / x.rs_total if x.rs_total else float("-inf"),
        ),
        reverse=True,
    ):
        side = trade_side_label(s)
        rs = "---" if s.rs_avg_pct is None else f"{s.rs_wins}/{s.rs_total} {s.rs_avg_pct:+.2f}%"
        roc_txt = f"{fmt_pct(s.roc7)}/{fmt_pct(s.roc14)}/{fmt_pct(s.roc21)}"
        rsi_txt = "---" if s.rsi14 is None else f"{s.rsi14:.1f}"
        candidate = "OK" if is_trade_candidate(s) else "--"
        print(
            f"{s.name:<6} | close={s.close:>9.3f} | M={s.px_m:+d}({s.streak_m}) W={s.px_w:+d}({s.streak_w}) D={s.px_d:+d}({s.streak_d}) | "
            f"{side:<5} | RS {rs:>15} | ROC {roc_txt:>24} | RSI {rsi_txt:>5} | {candidate}"
        )

    lines = []
    for i, s in enumerate(candidates, 1):
        dot = "🟢" if trade_direction(s) == 1 else "🔴"
        rs = "---" if s.rs_avg_pct is None else f"{s.rs_wins}/{s.rs_total} {s.rs_avg_pct:+.2f}%"
        roc_txt = f"{fmt_pct(s.roc7)}/{fmt_pct(s.roc14)}/{fmt_pct(s.roc21)}"
        rsi_txt = "---" if s.rsi14 is None else f"{s.rsi14:.1f}"
        lines.append(f"{i:>2}. {dot} {trade_side_label(s)} {s.name} | RS {rs} | ROC {roc_txt} | RSI {rsi_txt}")

    if lines:
        msg = build_telegram_message(candidates, trend_data if update_metrics else [], trend_lines)
        print(f"\n{msg}\n")
        if not args.no_telegram and (telegram_window_open(now_paris) or args.force_telegram):
            send_telegram(msg)
        elif not args.no_telegram:
            print("Telegram skipped: outside 06:00-21:00 Paris window.")
    else:
        print("\nNo ETF strategy candidates.")
        if trend_lines:
            print("\n" + "\n".join(trend_lines))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
