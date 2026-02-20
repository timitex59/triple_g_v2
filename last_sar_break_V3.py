#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
last_sar_break_V3.py

Single script that runs BOTH strategies:
- V1 logic (levels from D1 retracement + H1 breaks)
- V2 logic (H1 PSAR/EMA20 cross trigger)

Telegram sends one unified message with sections: V1 + V2.
"""

import argparse
import json
import os
import random
import string
import time
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from websocket import WebSocketConnectionClosedException, create_connection

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

try:
    import annual_V2 as annual
except Exception:
    annual = None


PSAR_START = 0.1
PSAR_INCREMENT = 0.1
PSAR_MAXIMUM = 0.2
EMA_STACK = [20, 25, 30, 35, 40, 45, 50, 55]

D1_CANDLES = 350
W1_CANDLES = 220
H1_CANDLES = 900

TZ_NAME = "Europe/Paris"
DEBUG = str(os.getenv("LAST_SAR_V3_DEBUG", "0")).strip().lower() in ("1", "true", "yes", "on")
FOREX_TOP_N = 5

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURNZD", "EURCHF",
    "GBPAUD", "GBPCAD", "GBPNZD", "GBPCHF",
    "AUDNZD", "AUDCAD", "AUDCHF",
    "NZDCAD", "NZDCHF",
    "CADCHF",
]

INDICES = {
    "DXY": "TVC:DXY",
    "EXY": "TVC:EXY",
    "BXY": "TVC:BXY",
    "JXY": "TVC:JXY",
    "SXY": "TVC:SXY",
    "CXY": "TVC:CXY",
    "AXY": "TVC:AXY",
    "ZXY": "TVC:ZXY",
}

CCY_TO_INDEX = {
    "USD": "DXY",
    "EUR": "EXY",
    "GBP": "BXY",
    "JPY": "JXY",
    "CHF": "SXY",
    "CAD": "CXY",
    "AUD": "AXY",
    "NZD": "ZXY",
}


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


def _ema_last_values(df):
    return [ema_series(df["close"], l).iloc[-1] for l in EMA_STACK]


def daily_chg_cc(df_d):
    if df_d is None or df_d.empty or len(df_d) < 2:
        return None
    d_close = float(df_d["close"].iloc[-1])
    d_prev = float(df_d["close"].iloc[-2])
    if d_prev == 0:
        return None
    return (d_close - d_prev) / d_prev * 100.0


def _summarize_currencies(items):
    stats = {}
    for item in items:
        pair = item["pair"]
        chg = item["chg_pct"]
        base = pair[:3]
        quote = pair[3:]
        for ccy in (base, quote):
            if ccy not in stats:
                stats[ccy] = {
                    "base_count": 0,
                    "quote_count": 0,
                    "appearances": 0,
                    "bull_count": 0,
                    "bear_count": 0,
                    "bias_score": 0,
                }
        stats[base]["base_count"] += 1
        stats[base]["appearances"] += 1
        stats[quote]["quote_count"] += 1
        stats[quote]["appearances"] += 1

        if chg > 0:
            stats[base]["bull_count"] += 1
            stats[base]["bias_score"] += 1
            stats[quote]["bear_count"] += 1
            stats[quote]["bias_score"] -= 1
        elif chg < 0:
            stats[base]["bear_count"] += 1
            stats[base]["bias_score"] -= 1
            stats[quote]["bull_count"] += 1
            stats[quote]["bias_score"] += 1

    for s in stats.values():
        if s["bias_score"] > 0:
            s["bias"] = "BULL"
        elif s["bias_score"] < 0:
            s["bias"] = "BEAR"
        else:
            s["bias"] = "NEUTRE"

    eligible = [(ccy, v) for ccy, v in stats.items() if v["appearances"] >= 2]
    eligible.sort(key=lambda kv: (-kv[1]["appearances"], kv[0]))
    selected = [ccy for ccy, _ in eligible[:2]]
    return stats, selected


def _collect_bull_bear_pools(top_stats, last_stats, top_selected, last_selected):
    bull_sources = {}
    bear_sources = {}

    def _add(stats, selected, section_name):
        for ccy in selected:
            s = stats.get(ccy)
            if not s:
                continue
            if s.get("bias") == "BULL":
                bull_sources.setdefault(ccy, set()).add(section_name)
            elif s.get("bias") == "BEAR":
                bear_sources.setdefault(ccy, set()).add(section_name)

    _add(top_stats, top_selected, "TOP")
    _add(last_stats, last_selected, "LAST")
    bulls = sorted(
        [{"ccy": ccy, "source": "+".join(sorted(srcs))} for ccy, srcs in bull_sources.items()],
        key=lambda x: x["ccy"],
    )
    bears = sorted(
        [{"ccy": ccy, "source": "+".join(sorted(srcs))} for ccy, srcs in bear_sources.items()],
        key=lambda x: x["ccy"],
    )
    return bulls, bears


def _pair_bias_from_currencies(ccy_a, ccy_b, universe):
    direct = f"{ccy_a}{ccy_b}"
    inverse = f"{ccy_b}{ccy_a}"
    if direct in universe:
        return direct, "BULL"
    if inverse in universe:
        return inverse, "BEAR"
    return None, "N/A"


def _build_associations_from_pools(bulls, bears, universe):
    associations = []
    for b in bulls:
        for s in bears:
            if b["ccy"] == s["ccy"]:
                continue
            pair, pair_bias = _pair_bias_from_currencies(b["ccy"], s["ccy"], universe)
            if not pair:
                continue
            associations.append(
                {
                    "bull_ccy": b["ccy"],
                    "bear_ccy": s["ccy"],
                    "pair": pair,
                    "pair_bias": pair_bias,
                }
            )
    uniq = {}
    for x in associations:
        uniq[(x["pair"], x["pair_bias"])] = x
    return sorted(uniq.values(), key=lambda z: z["pair"])


def _fetch_index_changes_for_associations(associations):
    needed = set()
    for a in associations:
        pair = a["pair"]
        base = pair[:3]
        quote = pair[3:]
        ib = CCY_TO_INDEX.get(base)
        iq = CCY_TO_INDEX.get(quote)
        if ib:
            needed.add(ib)
        if iq:
            needed.add(iq)
    changes = {}
    for idx_code in sorted(needed):
        tv_symbol = INDICES.get(idx_code)
        if not tv_symbol:
            changes[idx_code] = None
            continue
        df = fetch_tv_ohlc(tv_symbol, "D", 8)
        changes[idx_code] = daily_chg_cc(df) if df is not None and not df.empty else None
    return changes


def _validate_associations_with_indices(associations, index_changes):
    out = []
    for a in associations:
        pair = a["pair"]
        base = pair[:3]
        quote = pair[3:]
        ib = CCY_TO_INDEX.get(base)
        iq = CCY_TO_INDEX.get(quote)
        bchg = index_changes.get(ib)
        qchg = index_changes.get(iq)

        valid = False
        if bchg is not None and qchg is not None:
            if a["pair_bias"] == "BULL":
                valid = (bchg > 0) and (qchg < 0)
            elif a["pair_bias"] == "BEAR":
                valid = (bchg < 0) and (qchg > 0)

        row = dict(a)
        row["index_validation"] = valid
        out.append(row)
    return out


def build_forex_screener_telegram_lines(chg_by_pair, top_n=FOREX_TOP_N, tz_name=TZ_NAME):
    ranked = [{"pair": p, "chg_pct": c} for p, c in chg_by_pair.items() if c is not None]
    ranked.sort(key=lambda x: x["chg_pct"], reverse=True)
    if not ranked:
        return ["<b>FOREX SCREENER</b>", "\u26AA Aucun setup valide"]

    k = max(1, min(top_n, len(ranked)))
    top_items = ranked[:k]
    last_items = ranked[-k:]

    top_stats, top_sel = _summarize_currencies(top_items)
    last_stats, last_sel = _summarize_currencies(last_items)
    bulls, bears = _collect_bull_bear_pools(top_stats, last_stats, top_sel, last_sel)
    associations = _build_associations_from_pools(bulls, bears, set(PAIRS))
    idx_changes = _fetch_index_changes_for_associations(associations)
    validated = _validate_associations_with_indices(associations, idx_changes)

    chg_lookup = {r["pair"]: r["chg_pct"] for r in ranked}
    lines = ["<b>FOREX SCREENER</b>"]
    assoc_count = 0
    for v in validated:
        chg = chg_lookup.get(v["pair"])
        chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
        icon = "\U0001F7E2" if v["pair_bias"] == "BULL" else "\U0001F534"
        lines.append(f"{icon} {v['pair']} ({chg_txt})")
        assoc_count += 1

    if assoc_count == 0:
        lines.append("\u26AA Aucun setup valide")
    return lines


def _crossunder(a_prev, a_now, b_prev, b_now):
    return a_prev >= b_prev and a_now < b_now


def _crossover(a_prev, a_now, b_prev, b_now):
    return a_prev <= b_prev and a_now > b_now


def _last_cross_index(close_s, psar_s, mode):
    idx = None
    for i in range(1, len(close_s)):
        prev_close = close_s.iloc[i - 1]
        curr_close = close_s.iloc[i]
        prev_psar = psar_s.iloc[i - 1]
        curr_psar = psar_s.iloc[i]
        if mode == "bear_vw0":
            if _crossunder(prev_close, curr_close, prev_psar, curr_psar):
                idx = i
        else:
            if _crossover(prev_close, curr_close, prev_psar, curr_psar):
                idx = i
    return idx


def collect_bull_levels(df_d, psar_d):
    levels = []
    last_bear_idx = _last_cross_index(df_d["close"], psar_d, "bear_vw0")
    if last_bear_idx is None:
        return levels
    for i in range(last_bear_idx + 1, len(df_d)):
        o = float(df_d["open"].iloc[i])
        h = float(df_d["high"].iloc[i])
        c = float(df_d["close"].iloc[i])
        p = float(psar_d.iloc[i])
        if not h < p:
            continue
        if c < o:
            level = o
            kind = "bear_open"
        elif c > o:
            level = c
            kind = "bull_close"
        else:
            continue
        levels.append({"time": df_d.index[i], "kind": kind, "level": float(level)})
    return levels


def collect_bear_levels(df_d, psar_d):
    levels = []
    last_bull_idx = _last_cross_index(df_d["close"], psar_d, "bull_vw0")
    if last_bull_idx is None:
        return levels
    for i in range(last_bull_idx + 1, len(df_d)):
        o = float(df_d["open"].iloc[i])
        l = float(df_d["low"].iloc[i])
        c = float(df_d["close"].iloc[i])
        p = float(psar_d.iloc[i])
        if not l > p:
            continue
        if c > o:
            level = o
            kind = "bull_open"
        elif c < o:
            level = c
            kind = "bear_close"
        else:
            continue
        levels.append({"time": df_d.index[i], "kind": kind, "level": float(level)})
    return levels


def detect_h1_breaks_v1(df_h, levels, direction):
    signals = []
    if df_h is None or df_h.empty or len(df_h) < 2 or not levels:
        return signals
    close = df_h["close"]
    for lv in levels:
        level = lv["level"]
        level_time = lv["time"]
        for i in range(1, len(close)):
            if df_h.index[i] <= level_time:
                continue
            prev_close = float(close.iloc[i - 1])
            curr_close = float(close.iloc[i])
            if direction == "UP":
                hit = prev_close < level and curr_close > level
            else:
                hit = prev_close > level and curr_close < level
            if hit:
                signals.append(
                    {
                        "time": df_h.index[i],
                        "close": curr_close,
                        "level": level,
                        "level_kind": lv["kind"],
                    }
                )
    signals.sort(key=lambda x: x["time"], reverse=True)
    return signals


def detect_h1_bull_events_v2(df_h, psar_h):
    events = []
    if df_h is None or df_h.empty or psar_h is None or len(df_h) < 2:
        return events
    ema20 = ema_series(df_h["close"], 20)
    ema_stack = [ema_series(df_h["close"], l) for l in EMA_STACK]
    close = df_h["close"]
    for i in range(1, len(df_h)):
        prev_psar = float(psar_h.iloc[i - 1])
        curr_psar = float(psar_h.iloc[i])
        prev_e20 = float(ema20.iloc[i - 1])
        curr_e20 = float(ema20.iloc[i])
        cross_bearish = prev_psar > prev_e20 and curr_psar < curr_e20
        if not cross_bearish:
            continue
        curr_close = float(close.iloc[i])
        curr_emas = [float(e.iloc[i]) for e in ema_stack]
        cond_price = curr_close > curr_psar and curr_close > max(curr_emas)
        cond_ema = ema_alignment(curr_emas, "UP")
        if cond_price and cond_ema:
            events.append({"time": df_h.index[i], "close": curr_close})
    events.sort(key=lambda x: x["time"], reverse=True)
    return events


def detect_h1_bear_events_v2(df_h, psar_h):
    events = []
    if df_h is None or df_h.empty or psar_h is None or len(df_h) < 2:
        return events
    ema20 = ema_series(df_h["close"], 20)
    ema_stack = [ema_series(df_h["close"], l) for l in EMA_STACK]
    close = df_h["close"]
    for i in range(1, len(df_h)):
        prev_psar = float(psar_h.iloc[i - 1])
        curr_psar = float(psar_h.iloc[i])
        prev_e20 = float(ema20.iloc[i - 1])
        curr_e20 = float(ema20.iloc[i])
        cross_bullish = prev_psar < prev_e20 and curr_psar > curr_e20
        if not cross_bullish:
            continue
        curr_close = float(close.iloc[i])
        curr_emas = [float(e.iloc[i]) for e in ema_stack]
        cond_price = curr_close < curr_psar and curr_close < min(curr_emas)
        cond_ema = ema_alignment(curr_emas, "DOWN")
        if cond_price and cond_ema:
            events.append({"time": df_h.index[i], "close": curr_close})
    events.sort(key=lambda x: x["time"], reverse=True)
    return events


def _filter_rows_today(rows, key_time="time", tz_name=TZ_NAME):
    if not rows:
        return []
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    now_local = pd.Timestamp.now(tz=tz)
    day_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + pd.Timedelta(days=1)
    out = []
    for r in rows:
        ts = r.get(key_time)
        if ts is None:
            continue
        local_ts = ts.tz_convert(tz) if getattr(ts, "tzinfo", None) else ts.tz_localize("UTC").tz_convert(tz)
        if day_start <= local_ts < day_end:
            out.append(r)
    out.sort(key=lambda x: x[key_time], reverse=True)
    return out


def _dedupe_latest_pair_side(rows, key_time="time"):
    dedup = {}
    for r in sorted(rows, key=lambda x: x[key_time], reverse=True):
        key = (r.get("pair"), r.get("side"))
        if key not in dedup:
            dedup[key] = r
    out = list(dedup.values())
    out.sort(key=lambda x: x[key_time], reverse=True)
    return out


def _filter_active_v1(rows, latest_h1_close_by_pair):
    active = []
    for r in rows:
        pair = r.get("pair")
        side = r.get("side")
        level = r.get("level")
        last_h1_close = latest_h1_close_by_pair.get(pair)
        if last_h1_close is None or level is None:
            continue
        if side == "BULL" and float(last_h1_close) > float(level):
            active.append(r)
        if side == "BEAR" and float(last_h1_close) < float(level):
            active.append(r)
    active.sort(key=lambda x: x["time"], reverse=True)
    return active


def _now_line(tz_name=TZ_NAME):
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    now_local = pd.Timestamp.now(tz=tz)
    return f"⏰ {now_local.strftime('%Y-%m-%d %H:%M')} Paris"


def _dot(side):
    return "🟢" if side == "BULL" else "🔴"


def build_telegram_lines(v1_rows_today, v2_rows_today, include_timestamp=True):
    lines = ["<b>LAST SAR BREAK V3</b>", "", "<b>V1</b>"]
    if not v1_rows_today:
        lines.append("NO DEAL 😞")
    else:
        for r in v1_rows_today:
            chg = r.get("chg_cc_daily")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            lines.append(f"{_dot(r['side'])} {r['pair']} ({chg_txt})")

    lines.append("")
    lines.append("<b>V2</b>")
    if not v2_rows_today:
        lines.append("NO DEAL 😞")
    else:
        for r in v2_rows_today:
            chg = r.get("chg_cc_daily")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            lines.append(f"{_dot(r['side'])} {r['pair']} ({chg_txt})")

    if include_timestamp:
        lines.append("")
        lines.append(_now_line())
    return lines


def build_console_lines(v1_rows, v2_rows, debug_rows=None):
    lines = ["LAST SAR BREAK V3", "", "V1 RESULTS"]
    if not v1_rows:
        lines.append("NO DEAL")
    else:
        for r in v1_rows:
            chg = r.get("chg_cc_daily")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            lines.append(f"{r['side']} {r['pair']} ({chg_txt}) @ {r['time']}")

    lines.append("")
    lines.append("V2 RESULTS")
    if not v2_rows:
        lines.append("NO DEAL")
    else:
        for r in v2_rows:
            chg = r.get("chg_cc_daily")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            lines.append(f"{r['side']} {r['pair']} ({chg_txt}) @ {r['time']}")

    if debug_rows:
        lines.append("")
        lines.append("DEBUG")
        lines.extend(debug_rows)
    return lines


def main():
    if load_dotenv:
        load_dotenv()

    parser = argparse.ArgumentParser(description="last_sar_break_V3")
    parser.add_argument("--debug", action="store_true", help="Print debug details")
    parser.add_argument("--pair", default=None, help="Run only one pair, e.g. EURUSD")
    parser.add_argument("--no-forex-screener", action="store_true", help="Disable FOREX SCREENER section")
    args = parser.parse_args()

    debug_enabled = bool(args.debug or DEBUG)
    pairs = [args.pair.strip().upper()] if args.pair else PAIRS

    v1_rows_all = []
    v2_rows_all = []
    latest_h1_close_by_pair = {}
    chg_cc_by_pair = {}
    debug_rows = []

    for pair in pairs:
        df_w = fetch_pair_w1(pair)
        df_d = fetch_pair_d1(pair)
        df_h = fetch_pair_h1(pair)
        if (
            df_w is None or df_d is None or df_h is None
            or df_w.empty or df_d.empty or df_h.empty
        ):
            continue

        psar_w = calculate_psar(df_w, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        psar_d = calculate_psar(df_d, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        psar_h = calculate_psar(df_h, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        if psar_w is None or psar_d is None or psar_h is None:
            continue

        latest_h1_close_by_pair[pair] = float(df_h["close"].iloc[-1])

        chg_cc = daily_chg_cc(df_d)
        chg_cc_by_pair[pair] = chg_cc
        d_close = float(df_d["close"].iloc[-1])
        d_psar = float(psar_d.iloc[-1])
        w_close = float(df_w["close"].iloc[-1])
        w_psar = float(psar_w.iloc[-1])

        # V1
        d_emas = _ema_last_values(df_d)
        w_emas = _ema_last_values(df_w)
        v1_daily_bull = ema_alignment(d_emas, "UP") and d_close > d_psar
        v1_daily_bear = ema_alignment(d_emas, "DOWN") and d_close < d_psar
        weekly_bull_ok = w_close > w_psar and ema_alignment(w_emas, "UP")
        weekly_bear_ok = w_close < w_psar and ema_alignment(w_emas, "DOWN")

        if v1_daily_bull and weekly_bull_ok and chg_cc is not None and chg_cc > 0:
            bull_levels = collect_bull_levels(df_d, psar_d)
            bull_breaks = detect_h1_breaks_v1(df_h, bull_levels, "UP")
            for ev in bull_breaks:
                v1_rows_all.append(
                    {
                        "pair": pair,
                        "side": "BULL",
                        "time": ev["time"],
                        "level": ev["level"],
                        "level_kind": ev["level_kind"],
                        "chg_cc_daily": chg_cc,
                    }
                )

        if v1_daily_bear and weekly_bear_ok and chg_cc is not None and chg_cc < 0:
            bear_levels = collect_bear_levels(df_d, psar_d)
            bear_breaks = detect_h1_breaks_v1(df_h, bear_levels, "DOWN")
            for ev in bear_breaks:
                v1_rows_all.append(
                    {
                        "pair": pair,
                        "side": "BEAR",
                        "time": ev["time"],
                        "level": ev["level"],
                        "level_kind": ev["level_kind"],
                        "chg_cc_daily": chg_cc,
                    }
                )

        # V2
        v2_bull_bias = ema_alignment(w_emas, "UP") and ema_alignment(d_emas, "UP") and w_close > w_psar
        v2_bear_bias = ema_alignment(w_emas, "DOWN") and ema_alignment(d_emas, "DOWN") and w_close < w_psar

        if v2_bull_bias and chg_cc is not None and chg_cc > 0:
            for ev in detect_h1_bull_events_v2(df_h, psar_h):
                v2_rows_all.append(
                    {
                        "pair": pair,
                        "side": "BULL",
                        "time": ev["time"],
                        "chg_cc_daily": chg_cc,
                    }
                )

        if v2_bear_bias and chg_cc is not None and chg_cc < 0:
            for ev in detect_h1_bear_events_v2(df_h, psar_h):
                v2_rows_all.append(
                    {
                        "pair": pair,
                        "side": "BEAR",
                        "time": ev["time"],
                        "chg_cc_daily": chg_cc,
                    }
                )

        if debug_enabled:
            debug_rows.append(
                f"{pair} | chg={('N/A' if chg_cc is None else f'{chg_cc:+.2f}%')} "
                f"| V1(bull={v1_daily_bull and weekly_bull_ok},bear={v1_daily_bear and weekly_bear_ok}) "
                f"| V2(bull={v2_bull_bias},bear={v2_bear_bias})"
            )

    # Final selection for display
    v1_rows = _dedupe_latest_pair_side(v1_rows_all, key_time="time")
    v1_rows = _filter_active_v1(v1_rows, latest_h1_close_by_pair)
    v2_rows = _dedupe_latest_pair_side(v2_rows_all, key_time="time")

    lines = build_console_lines(v1_rows, v2_rows, debug_rows if debug_enabled else None)
    forex_section_lines = []
    if not args.no_forex_screener and not args.pair:
        for p in PAIRS:
            if p not in chg_cc_by_pair:
                d1_only = fetch_pair_d1(p)
                chg_cc_by_pair[p] = daily_chg_cc(d1_only) if d1_only is not None and not d1_only.empty else None
        forex_section_lines = build_forex_screener_telegram_lines(chg_cc_by_pair)
        lines += ["", "FOREX SCREENER SECTION"] + forex_section_lines[2:-2]
    print("\n".join(lines))

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try:
            v1_today = _filter_rows_today(v1_rows_all, key_time="time")
            v1_today = _dedupe_latest_pair_side(v1_today, key_time="time")
            v1_today = _filter_active_v1(v1_today, latest_h1_close_by_pair)

            v2_today = _filter_rows_today(v2_rows_all, key_time="time")
            v2_today = _dedupe_latest_pair_side(v2_today, key_time="time")

            telegram_lines = build_telegram_lines(v1_today, v2_today, include_timestamp=False)
            if forex_section_lines:
                telegram_lines += [""] + forex_section_lines
            telegram_lines += ["", _now_line()]
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(
                url,
                json={"chat_id": chat_id, "text": "\n".join(telegram_lines), "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception:
            pass


if __name__ == "__main__":
    main()
