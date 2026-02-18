#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SAR_BREAK_V5

Level 1 (Weekly):
- BULL: weekly close > weekly PSAR
- BEAR: weekly close < weekly PSAR

Level 2 (Daily):
- BULL: daily close > daily PSAR AND daily close > all 8 EMAs
        + EMA alignment: EMA20 > EMA25 > ... > EMA55
- BEAR: daily close < daily PSAR AND daily close < all 8 EMAs
        + EMA alignment: EMA20 < EMA25 < ... < EMA55

Sends a Telegram message for valid signals.
"""

import json
import random
import string
import time
from datetime import timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from websocket import create_connection, WebSocketConnectionClosedException

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

try:
    import annual_V2 as annual
except Exception:
    annual = None

import os
import requests


PSAR_START = 0.1
PSAR_INCREMENT = 0.1
PSAR_MAXIMUM = 0.2

EMA_STACK = [20, 25, 30, 35, 40, 45, 50, 55]

D1_CANDLES = 250
W1_CANDLES = 200
H1_CANDLES = 700

TZ_NAME = "Europe/Paris"
DEBUG_WEEKLY = False
DEBUG_BEST_TRADE = str(os.getenv("BEST_TRADE_DEBUG", "0")).strip().lower() in ("1", "true", "yes", "on")
CHG_CC_STATE_PATH = os.path.join(os.path.dirname(__file__), "chg_cc_indices.json")

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
    "DXY": "USD",
    "EXY": "EUR",
    "BXY": "GBP",
    "JXY": "JPY",
    "SXY": "CHF",
    "CXY": "CAD",
    "AXY": "AUD",
    "ZXY": "NZD",
}

INDEX_SYMBOLS = {
    "DXY": "TVC:DXY",
    "EXY": "TVC:EXY",
    "BXY": "TVC:BXY",
    "JXY": "TVC:JXY",
    "SXY": "TVC:SXY",
    "CXY": "TVC:CXY",
    "AXY": "TVC:AXY",
    "ZXY": "TVC:ZXY",
}

CHG_CC_PAIRS = [
    "USDJPY",
    "EURJPY",
    "GBPJPY",
    "AUDJPY",
    "NZDJPY",
    "CADJPY",
    "CHFJPY",
]


def _gen_session_id():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


def _create_msg(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"


def _parse_frames(raw):
    if raw in ("~h", "h"):
        return [raw]
    frames = []
    i = 0
    while raw.startswith("~m~", i):
        i += 3
        j = raw.find("~m~", i)
        if j == -1:
            break
        size = int(raw[i:j])
        i = j + 3
        frames.append(raw[i : i + size])
        i += size
    return frames if frames else [raw]


def fetch_tv_ohlc(symbol, interval, n_candles, sleep_s=0.05, timeout_s=20, retries=2):
    for attempt in range(retries + 1):
        ws = None
        try:
            ws = create_connection(
                "wss://prodata.tradingview.com/socket.io/websocket",
                header={"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"},
                timeout=timeout_s,
            )
            session_id = _gen_session_id()
            ws.send(_create_msg("chart_create_session", [session_id, ""]))
            ws.send(
                _create_msg(
                    "resolve_symbol",
                    [session_id, "sds_sym_1", f'={{"symbol":"{symbol}","adjustment":"splits","session":"regular"}}'],
                )
            )
            ws.send(_create_msg("create_series", [session_id, "sds_1", "s1", "sds_sym_1", interval, n_candles, ""]))

            points = []
            start_t = time.time()
            while time.time() - start_t < 12:
                try:
                    raw = ws.recv()
                except WebSocketConnectionClosedException:
                    break
                for frame in _parse_frames(raw):
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
                if len(v) < 5:
                    continue
                ts, o, h, l, c = v[:5]
                rows.append({"date": ts, "open": o, "high": h, "low": l, "close": c})
            if not rows:
                continue
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"], unit="s", utc=True)
            df.set_index("date", inplace=True)
            return df.sort_index()
        except Exception:
            if attempt >= retries:
                return None
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
            if sleep_s:
                time.sleep(sleep_s)
    return None


def fetch_pair_d1(pair_code):
    if annual and hasattr(annual, "fetch_data_tv"):
        try:
            df = annual.fetch_data_tv("", "1d", n_candles=D1_CANDLES, tv_symbol=f"OANDA:{pair_code}")
            if df is not None and not df.empty:
                df.columns = [c.lower() for c in df.columns]
                return df
        except Exception:
            pass
    return fetch_tv_ohlc(f"OANDA:{pair_code}", "D", D1_CANDLES)


def fetch_pair_w1(pair_code):
    return fetch_tv_ohlc(f"OANDA:{pair_code}", "W", W1_CANDLES)


def fetch_pair_h1(pair_code):
    if annual and hasattr(annual, "fetch_data_tv"):
        try:
            df = annual.fetch_data_tv("", "1h", n_candles=H1_CANDLES, tv_symbol=f"OANDA:{pair_code}")
            if df is not None and not df.empty:
                df.columns = [c.lower() for c in df.columns]
                return df
        except Exception:
            pass
    return fetch_tv_ohlc(f"OANDA:{pair_code}", "60", H1_CANDLES)


def calculate_psar(df, start, increment, maximum):
    if df is None or len(df) < 3:
        return None
    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values
    psar = np.zeros(len(df))
    bull = closes[1] >= closes[0]
    af = start
    ep = highs[0] if bull else lows[0]
    psar[0] = lows[0] if bull else highs[0]
    for i in range(1, len(df)):
        psar[i] = psar[i - 1] + af * (ep - psar[i - 1])
        if bull:
            if lows[i] < psar[i]:
                bull = False
                psar[i] = ep
                ep = lows[i]
                af = start
            else:
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + increment, maximum)
                psar[i] = min(psar[i], lows[i - 1], lows[i - 2]) if i >= 2 else min(psar[i], lows[i - 1])
        else:
            if highs[i] > psar[i]:
                bull = True
                psar[i] = ep
                ep = highs[i]
                af = start
            else:
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + increment, maximum)
                psar[i] = max(psar[i], highs[i - 1], highs[i - 2]) if i >= 2 else max(psar[i], highs[i - 1])
    return pd.Series(psar, index=df.index)


def ema_series(series, length):
    return series.ewm(span=length, adjust=False).mean()


def ema_alignment(values, direction):
    if direction == "UP":
        return all(values[i] > values[i + 1] for i in range(len(values) - 1))
    return all(values[i] < values[i + 1] for i in range(len(values) - 1))


def compute_ema_stack(df, lengths):
    return [ema_series(df["close"], l) for l in lengths]


def daily_chg_cc(df_d):
    if df_d is None or df_d.empty or len(df_d) < 2:
        return None
    d_close = df_d["close"].iloc[-1]
    d_prev = df_d["close"].iloc[-2]
    if d_prev == 0:
        return None
    return (d_close - d_prev) / d_prev * 100.0


def chg_dot(chg):
    if chg is None:
        return "⚪"
    return "🟢" if chg > 0 else "🔴" if chg < 0 else "⚪"


def _today_key(tz_name=TZ_NAME):
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    return pd.Timestamp.now(tz=tz).strftime("%Y-%m-%d")


def _load_chg_cc_state():
    if not os.path.exists(CHG_CC_STATE_PATH):
        return {}
    try:
        with open(CHG_CC_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_chg_cc_state(state):
    try:
        with open(CHG_CC_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception:
        pass


def _append_chg_cc_state(state, date_key, idx_name, chg_daily, chg_h1):
    day = state.get(date_key, {})
    idx = day.get(idx_name, {"daily": [], "h1": []})
    if chg_daily is not None:
        idx["daily"].append(float(chg_daily))
    if chg_h1 is not None:
        idx["h1"].append(float(chg_h1))
    day[idx_name] = idx
    state[date_key] = day


def _avg_from_state(state, date_key, idx_name, key):
    day = state.get(date_key, {})
    idx = day.get(idx_name, {})
    vals = idx.get(key, [])
    if not vals:
        return None
    return float(sum(vals) / len(vals))


def collect_extra_chg_cc_sections():
    date_key = _today_key()
    state = _load_chg_cc_state()
    index_rows = []
    for idx, sym in INDEX_SYMBOLS.items():
        df_idx_d = fetch_tv_ohlc(sym, "D", D1_CANDLES)
        df_idx_h1 = fetch_tv_ohlc(sym, "60", H1_CANDLES)
        chg = daily_chg_cc(df_idx_d)
        chg_h1 = daily_chg_cc(df_idx_h1)
        _append_chg_cc_state(state, date_key, idx, chg, chg_h1)
        avg_d = _avg_from_state(state, date_key, idx, "daily")
        avg_h1 = _avg_from_state(state, date_key, idx, "h1")
        ccy = INDEX_TO_CCY.get(idx, idx)
        index_rows.append(
            {
                "name": idx,
                "ccy": ccy,
                "chg": chg,
                "chg_h1": chg_h1,
                "avg_d": avg_d,
                "avg_h1": avg_h1,
            }
        )
    _save_chg_cc_state(state)
    index_rows.sort(key=lambda r: (r["chg"] is None, -(r["chg"] if r["chg"] is not None else float("-inf"))))

    lines = ["", "CHG% CC DAILY / 1H", "Indices"]
    for r in index_rows:
        chg_txt = "N/A" if r["chg"] is None else f"{r['chg']:+.2f}%"
        daily_above = r["chg"] is not None and r["avg_d"] is not None and r["chg"] > r["avg_d"]
        daily_below = r["chg"] is not None and r["avg_d"] is not None and r["chg"] < r["avg_d"]
        h1_above = r["chg_h1"] is not None and r["avg_h1"] is not None and r["chg_h1"] > r["avg_h1"]
        h1_below = r["chg_h1"] is not None and r["avg_h1"] is not None and r["chg_h1"] < r["avg_h1"]
        flames = ""
        if (daily_above and h1_above) or (daily_below and h1_below):
            flames = " 🔥🔥"
        elif daily_above or daily_below:
            flames = " 🔥"
        lines.append(f"{chg_dot(r['chg'])}{chg_dot(r['chg_h1'])} {r['name']} ({r['ccy']}) : {chg_txt}{flames}")

    return lines, index_rows, []


def h1_best_trade_filter(pair, direction):
    df_h = fetch_pair_h1(pair)
    if df_h is None or df_h.empty:
        return False
    psar_h = calculate_psar(df_h, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
    if psar_h is None:
        return False
    h_close = df_h["close"].iloc[-1]
    h_psar = psar_h.iloc[-1]
    h_emas = [ema_series(df_h["close"], l).iloc[-1] for l in EMA_STACK]
    if direction == "LONG":
        return h_close > h_psar and h_close > max(h_emas) and ema_alignment(h_emas, "UP")
    return h_close < h_psar and h_close < min(h_emas) and ema_alignment(h_emas, "DOWN")


def daily_best_trade_filter(df_d, direction):
    if df_d is None or df_d.empty:
        return False
    d_close = df_d["close"].iloc[-1]
    d_emas = [ema_series(df_d["close"], l).iloc[-1] for l in EMA_STACK]
    if direction == "LONG":
        return d_close > max(d_emas)
    return d_close < min(d_emas)


def build_best_trade_lines(index_rows, debug_best_trade=DEBUG_BEST_TRADE):
    lines = ["", "BEST TRADE"]
    seen = set()
    debug_lines = []
    debug_seen = set()

    def add_trade(dot, pair, chg_txt):
        key = (dot, pair)
        if key in seen:
            return False
        seen.add(key)
        lines.append(f"{dot} {pair} ({chg_txt})")
        return True

    def add_debug(pair, reason):
        msg = f"- {pair}: {reason}"
        if msg not in debug_seen:
            debug_seen.add(msg)
            debug_lines.append(msg)

    def row_alignment(row):
        d = row.get("chg")
        h = row.get("chg_h1")
        if d is None or h is None:
            return "NONE"
        if d > 0 and h > 0:
            return "GG"
        if d < 0 and h < 0:
            return "RR"
        return "MIXED"

    def resolve_pair(ccy_a, ccy_b):
        p1 = f"{ccy_a}{ccy_b}"
        if p1 in PAIRS:
            return p1
        p2 = f"{ccy_b}{ccy_a}"
        if p2 in PAIRS:
            return p2
        return None

    valid = [r for r in index_rows if r.get("chg") is not None]
    if len(valid) < 2:
        lines.append("NO DEAL")
        lines.append("")
        lines.append("BEST TRADE DEBUG")
        lines.append("- Not enough valid indices (need >= 2 with non-null daily change)")
        return lines

    added_any = False
    top2 = valid[:2]
    bottom2 = valid[-2:] if len(valid) >= 2 else []
    if len(top2) < 2 or len(bottom2) < 2:
        lines.append("NO DEAL 😞")
        lines.append("")
        lines.append("BEST TRADE DEBUG")
        lines.append("- Need at least 2 currencies in top and bottom buckets")
        return lines

    aligned_green = [r for r in valid if row_alignment(r) == "GG"]
    aligned_red = [r for r in valid if row_alignment(r) == "RR"]

    candidate_pairs = []
    for src in aligned_green:
        for tgt in bottom2:
            candidate_pairs.append((src, tgt, "GG->BOTTOM2"))
    for src in aligned_red:
        for tgt in top2:
            candidate_pairs.append((src, tgt, "RR->TOP2"))

    if not candidate_pairs:
        add_debug("GLOBAL", "Rejected: no aligned currencies (GG or RR) to seed combinations")

    for src, tgt, rule in candidate_pairs:
        src_ccy = src["ccy"]
        tgt_ccy = tgt["ccy"]
        if src_ccy == tgt_ccy:
            add_debug(f"{src_ccy}/{tgt_ccy}", f"Rejected ({rule}): same currency")
            continue

        src_align = row_alignment(src)
        tgt_align = row_alignment(tgt)
        if src_align in ("GG", "RR") and tgt_align == src_align:
            add_debug(f"{src_ccy}/{tgt_ccy}", f"Rejected ({rule}): same aligned direction ({src_align})")
            continue

        pair = resolve_pair(src_ccy, tgt_ccy)
        if not pair:
            add_debug(f"{src_ccy}/{tgt_ccy}", f"Rejected ({rule}): no tradable pair in PAIRS list")
            continue

        df_d_pair = fetch_pair_d1(pair)
        chg = daily_chg_cc(df_d_pair)
        if chg is None:
            add_debug(pair, f"Rejected ({rule}): CHG is None")
            continue
        if chg == 0:
            add_debug(pair, f"Rejected ({rule}): CHG is 0")
            continue

        direction = "LONG" if chg > 0 else "SHORT"
        if not daily_best_trade_filter(df_d_pair, direction):
            add_debug(pair, f"Rejected ({rule}): Daily filter failed (price vs 8 EMA)")
            continue
        if not h1_best_trade_filter(pair, direction):
            add_debug(pair, f"Rejected ({rule}): H1 filter failed (PSAR/EMA stack/price vs 8 EMA)")
            continue

        dot = "🟢" if chg > 0 else "🔴"
        chg_txt = f"{chg:+.2f}%"
        if add_trade(dot, pair, chg_txt):
            added_any = True
        else:
            add_debug(pair, f"Skipped ({rule}): duplicate trade")

    if not added_any:
        lines.append("NO DEAL 😞")
    lines.append("")
    lines.append("BEST TRADE DEBUG")
    if debug_lines:
        lines.extend(debug_lines)
    else:
        lines.append("- No rejection recorded")

    return lines


def detect_cross_since_23h(df_h1, psar, direction, ema_stack, tz_name=TZ_NAME):
    if df_h1 is None or psar is None or len(df_h1) < 2:
        return []
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    local_index = df_h1.index.tz_convert(tz) if df_h1.index.tz is not None else df_h1.index.tz_localize("UTC").tz_convert(tz)
    now_local = local_index[-1]
    window_start = now_local.replace(hour=23, minute=0, second=0, microsecond=0)
    if now_local < window_start:
        window_start = window_start - timedelta(days=1)
    window_mask = local_index >= window_start
    if window_mask.sum() < 2:
        return []
    close = df_h1["close"]
    open_ = df_h1["open"]
    close_w = close[window_mask]
    open_w = open_[window_mask]
    psar_w = psar[window_mask]
    ema_w = [ema[window_mask] for ema in ema_stack]
    events = []
    for i in range(1, len(close_w)):
        if direction == "UP":
            if close_w.iloc[i] > psar_w.iloc[i] and close_w.iloc[i - 1] < psar_w.iloc[i - 1]:
                psar_i = psar_w.iloc[i]
                open_i = open_w.iloc[i]
                emas_i = [ema.iloc[i] for ema in ema_w]
                if not ema_alignment(emas_i, "UP"):
                    continue
                if close_w.iloc[i] <= max(emas_i):
                    continue
                fire = psar_i < min(emas_i) and open_i < min(emas_i)
                events.append({"time": close_w.index[i], "fire": fire})
        else:
            if close_w.iloc[i] < psar_w.iloc[i] and close_w.iloc[i - 1] > psar_w.iloc[i - 1]:
                psar_i = psar_w.iloc[i]
                open_i = open_w.iloc[i]
                emas_i = [ema.iloc[i] for ema in ema_w]
                if not ema_alignment(emas_i, "DOWN"):
                    continue
                if close_w.iloc[i] >= min(emas_i):
                    continue
                fire = psar_i > max(emas_i) and open_i > max(emas_i)
                events.append({"time": close_w.index[i], "fire": fire})
    return events


def detect_cross_in_window(df_h1, psar, direction, start_local, end_local, tz_name=TZ_NAME):
    if df_h1 is None or psar is None or len(df_h1) < 2:
        return []
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    local_index = df_h1.index.tz_convert(tz) if df_h1.index.tz is not None else df_h1.index.tz_localize("UTC").tz_convert(tz)
    window_mask = (local_index >= start_local) & (local_index <= end_local)
    if window_mask.sum() < 2:
        return []
    close = df_h1["close"]
    close_w = close[window_mask]
    psar_w = psar[window_mask]
    times = []
    for i in range(1, len(close_w)):
        if direction == "UP":
            if close_w.iloc[i] > psar_w.iloc[i] and close_w.iloc[i - 1] < psar_w.iloc[i - 1]:
                times.append(close_w.index[i])
        else:
            if close_w.iloc[i] < psar_w.iloc[i] and close_w.iloc[i - 1] > psar_w.iloc[i - 1]:
                times.append(close_w.index[i])
    return times


def detect_recent_break(df_h1, psar, direction, ema_stack):
    if df_h1 is None or psar is None or len(df_h1) < 2:
        return None
    close = df_h1["close"]
    open_ = df_h1["open"]
    up_cross = ((close > psar) & (close.shift(1) < psar.shift(1))).fillna(False)
    down_cross = ((close < psar) & (close.shift(1) > psar.shift(1))).fillna(False)

    up_times = df_h1.index[up_cross]
    down_times = df_h1.index[down_cross]

    last_up = up_times[-1] if len(up_times) else None
    last_down = down_times[-1] if len(down_times) else None

    if direction == "UP":
        if last_up is None:
            return None
        if last_down is not None and last_down >= last_up:
            return None
        last_cross = last_up
    else:
        if last_down is None:
            return None
        if last_up is not None and last_up >= last_down:
            return None
        last_cross = last_down

    pos = df_h1.index.get_indexer([last_cross])[0]
    if pos < len(df_h1) - 2:
        return None

    # Keep break only if current H1 still confirms higher-timeframe direction.
    curr_close = close.iloc[-1]
    curr_psar = psar.iloc[-1]
    if direction == "UP" and curr_close <= curr_psar:
        return None
    if direction == "DOWN" and curr_close >= curr_psar:
        return None

    psar_i = psar.iloc[pos]
    open_i = open_.iloc[pos]
    emas_i = [ema.iloc[pos] for ema in ema_stack]
    if direction == "UP":
        if not ema_alignment(emas_i, "UP"):
            return None
        fire = psar_i < min(emas_i) and open_i < min(emas_i)
    else:
        if not ema_alignment(emas_i, "DOWN"):
            return None
        fire = psar_i > max(emas_i) and open_i > max(emas_i)
    return {"time": last_cross, "fire": fire}


def _fmt_local_time(ts, tz_name=TZ_NAME):
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    local_ts = ts.tz_convert(tz) if ts.tzinfo else ts.tz_localize("UTC").tz_convert(tz)
    return local_ts.strftime("%Y-%m-%d %H:%M")


def _now_local_line(tz_name=TZ_NAME):
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    now_local = pd.Timestamp.now(tz=tz)
    return f"⏰ {now_local.strftime('%Y-%m-%d %H:%M')} Paris"


def _strip_debug_for_telegram(lines):
    cleaned = []
    in_best_trade_debug = False
    for line in lines:
        if line == "BEST TRADE DEBUG":
            in_best_trade_debug = True
            continue
        if in_best_trade_debug:
            if line.startswith("- "):
                continue
            if line == "":
                continue
            in_best_trade_debug = False
        cleaned.append(line)
    return cleaned


def main():
    if load_dotenv:
        load_dotenv()

    results = []
    run_breaks = []
    super_bull = []
    super_bear = []
    super_potential_bull = []
    super_potential_bear = []
    for pair in PAIRS:
        df_w = fetch_pair_w1(pair)
        df_d = fetch_pair_d1(pair)
        df_h = fetch_pair_h1(pair)
        if df_w is None or df_d is None or df_h is None or df_w.empty or df_d.empty or df_h.empty:
            continue

        psar_w = calculate_psar(df_w, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        psar_d = calculate_psar(df_d, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        psar_h = calculate_psar(df_h, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        if psar_w is None or psar_d is None or psar_h is None:
            continue

        chg_cc_pair = daily_chg_cc(df_d)

        # SUPER TREND (W1 + D1 + H1 strict alignment, trigger on fresh H1 cross)
        w_close_now = df_w["close"].iloc[-1]
        d_close_now = df_d["close"].iloc[-1]
        h_close_now = df_h["close"].iloc[-1]
        h_close_prev = df_h["close"].iloc[-2] if len(df_h) >= 2 else None
        w_psar_now = psar_w.iloc[-1]
        d_psar_now = psar_d.iloc[-1]
        h_psar_now = psar_h.iloc[-1]
        h_psar_prev = psar_h.iloc[-2] if len(psar_h) >= 2 else None
        w_emas = [ema_series(df_w["close"], l).iloc[-1] for l in EMA_STACK]
        d_emas = [ema_series(df_d["close"], l).iloc[-1] for l in EMA_STACK]
        ema_h1_stack = compute_ema_stack(df_h, EMA_STACK)
        h_emas = [ema.iloc[-1] for ema in ema_h1_stack]

        w_bull = w_close_now > w_psar_now and w_close_now > max(w_emas) and ema_alignment(w_emas, "UP")
        d_bull = d_close_now > d_psar_now and d_close_now > max(d_emas) and ema_alignment(d_emas, "UP")
        h_bull = h_close_now > h_psar_now and h_close_now > max(h_emas) and ema_alignment(h_emas, "UP")

        w_bear = w_close_now < w_psar_now and w_close_now < min(w_emas) and ema_alignment(w_emas, "DOWN")
        d_bear = d_close_now < d_psar_now and d_close_now < min(d_emas) and ema_alignment(d_emas, "DOWN")
        h_bear = h_close_now < h_psar_now and h_close_now < min(h_emas) and ema_alignment(h_emas, "DOWN")

        cross_up = h_close_prev is not None and h_psar_prev is not None and h_close_prev < h_psar_prev and h_close_now > h_psar_now
        cross_down = h_close_prev is not None and h_psar_prev is not None and h_close_prev > h_psar_prev and h_close_now < h_psar_now

        if w_bull and d_bull and h_bull:
            if cross_up:
                super_bull.append((pair, chg_cc_pair))
            else:
                super_potential_bull.append((pair, chg_cc_pair))
        elif w_bear and d_bear and h_bear:
            if cross_down:
                super_bear.append((pair, chg_cc_pair))
            else:
                super_potential_bear.append((pair, chg_cc_pair))

        w_close = df_w["close"].iloc[-1]
        w_psar = psar_w.iloc[-1]

        if DEBUG_WEEKLY:
            w_ts = df_w.index[-1]
            print(f"[W1] {pair} | ts={w_ts} close={w_close:.3f} psar={w_psar:.3f}")

        if w_close > w_psar:
            direction = "UP"
            signal = "LONG"
        elif w_close < w_psar:
            direction = "DOWN"
            signal = "SHORT"
        else:
            continue

        d_close = d_close_now
        d_psar = d_psar_now

        if direction == "UP":
            if not (d_close > d_psar and d_close > max(d_emas)):
                continue
            if not ema_alignment(d_emas, "UP"):
                continue
        else:
            if not (d_close < d_psar and d_close < min(d_emas)):
                continue
            if not ema_alignment(d_emas, "DOWN"):
                continue

        h1_emas_last = h_emas
        if direction == "UP":
            if not ema_alignment(h1_emas_last, "UP"):
                continue
        else:
            if not ema_alignment(h1_emas_last, "DOWN"):
                continue

        recent_break = detect_recent_break(df_h, psar_h, direction, ema_h1_stack)
        if recent_break is not None:
            run_breaks.append(
                {
                    "pair": pair,
                    "signal": signal,
                    "time": recent_break["time"],
                    "fire": recent_break.get("fire", False),
                }
            )
        cross_events = detect_cross_since_23h(df_h, psar_h, direction, ema_h1_stack)
        if not cross_events:
            continue

        fire_count = sum(1 for e in cross_events if e.get("fire"))
        chg_cc = chg_cc_pair
        if chg_cc is None:
            continue
        if signal == "LONG" and chg_cc <= 0:
            continue
        if signal == "SHORT" and chg_cc >= 0:
            continue
        results.append(
            {
                "pair": pair,
                "signal": signal,
                "cross_events": cross_events,
                "fire_count": fire_count,
                "chg_cc": chg_cc,
            }
        )

    print("SAR BREAK V5")
    results.sort(
        key=lambda r: (
            r.get("chg_cc") is None,
            -(abs(r.get("chg_cc")) if r.get("chg_cc") is not None else float("-inf")),
        )
    )
    results = results[:3]
    lines = ["SAR BREAK V5"]
    if not results:
        lines.append("NO DEAL 😞")
    else:
        for r in results:
            cross_count = len(r.get("cross_events", []))
            fire_count = r.get("fire_count", 0)
            fire_tag = " 🔥" if fire_count > 0 else ""
            chg_cc = r.get("chg_cc")
            chg_text = f"{chg_cc:+.2f}%" if chg_cc is not None else "N/A"
            print(f"{r['pair']} | {r['signal']} | CHG% (CC): {chg_text} | crosses: {cross_count}{fire_tag}")
            dot = "🟢" if r["signal"] == "LONG" else "🔴"
            lines.append(f"{dot} {r['pair']} ({chg_text}) : {cross_count}{fire_tag}")

    lines.append("")
    lines.append("RUN BREAK")
    if not run_breaks:
        lines.append("NO BREAK😞")
    else:
        run_breaks.sort(key=lambda r: r.get("time"), reverse=True)
        for rb in run_breaks:
            dot = "🟢" if rb["signal"] == "LONG" else "🔴"
            time_txt = _fmt_local_time(rb["time"])
            fire_tag = " 🔥" if rb.get("fire") else ""
            lines.append(f"{dot} {rb['pair']} : {time_txt}{fire_tag}")

    lines.append("")
    lines.append("SUPER TREND")
    def _chg_txt(v):
        return f"{v:+.2f}%" if v is not None and not pd.isna(v) else "N/A"
    if not super_bull and not super_bear:
        lines.append("NO SIGNAL")
    else:
        if super_bull:
            lines.append("BULL")
            for p, chg in super_bull:
                lines.append(f"🟢 {p} ({_chg_txt(chg)})")
        if super_bear:
            lines.append("BEAR")
            for p, chg in super_bear:
                lines.append(f"🔴 {p} ({_chg_txt(chg)})")
    if super_potential_bull or super_potential_bear:
        lines.append("")
        lines.append("POTENTIAL (waiting H1 cross)")
        if super_potential_bull:
            lines.append("BULL CANDIDATES")
            for p, chg in super_potential_bull:
                lines.append(f"🟢 {p} ({_chg_txt(chg)})")
        if super_potential_bear:
            lines.append("BEAR CANDIDATES")
            for p, chg in super_potential_bear:
                lines.append(f"🔴 {p} ({_chg_txt(chg)})")

    _, index_rows, _ = collect_extra_chg_cc_sections()
    lines.extend(build_best_trade_lines(index_rows))
    lines.append("")
    lines.append(_now_local_line())

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            telegram_lines = _strip_debug_for_telegram(lines)
            requests.post(url, json={"chat_id": chat_id, "text": "\n".join(telegram_lines)}, timeout=10)
        except Exception:
            pass


if __name__ == "__main__":
    main()
