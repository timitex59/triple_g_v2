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


def collect_extra_chg_cc_sections():
    index_rows = []
    for idx, sym in INDEX_SYMBOLS.items():
        df_idx = fetch_tv_ohlc(sym, "D", D1_CANDLES)
        chg = daily_chg_cc(df_idx)
        ccy = INDEX_TO_CCY.get(idx, idx)
        index_rows.append({"name": idx, "ccy": ccy, "chg": chg})
    index_rows.sort(key=lambda r: (r["chg"] is None, -(r["chg"] if r["chg"] is not None else float("-inf"))))

    lines = ["", "CHG% CC DAILY", "Indices"]
    for r in index_rows:
        chg_txt = "N/A" if r["chg"] is None else f"{r['chg']:+.2f}%"
        lines.append(f"{chg_dot(r['chg'])} {r['name']} ({r['ccy']}) : {chg_txt}")

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


def build_best_trade_lines(index_rows):
    lines = ["", "BEST TRADE"]
    seen = set()

    def add_trade(dot, pair, chg_txt):
        key = (dot, pair)
        if key in seen:
            return False
        seen.add(key)
        lines.append(f"{dot} {pair} ({chg_txt})")
        return True

    valid = [r for r in index_rows if r.get("chg") is not None]
    if len(valid) < 2:
        lines.append("NO DEAL")
        return lines

    added_any = False
    strongest = valid[0]
    if strongest["ccy"] != "JPY":
        pair = f"{strongest['ccy']}JPY"
        if pair in PAIRS:
            chg = daily_chg_cc(fetch_pair_d1(pair))
            if chg is not None and chg > 0 and h1_best_trade_filter(pair, "LONG"):
                chg_txt = f"{chg:+.2f}%"
                if add_trade("🟢", pair, chg_txt):
                    added_any = True
    else:
        weakest = valid[-1]
        if weakest["ccy"] != "JPY":
            pair = f"{weakest['ccy']}JPY"
            if pair in PAIRS:
                chg = daily_chg_cc(fetch_pair_d1(pair))
                if chg is not None and chg < 0 and h1_best_trade_filter(pair, "SHORT"):
                    chg_txt = f"{chg:+.2f}%"
                    if add_trade("🔴", pair, chg_txt):
                        added_any = True

    top2 = valid[:2]
    bottom2 = valid[-2:] if len(valid) >= 2 else []
    if len(top2) == 2 and len(bottom2) == 2:
        added = False
        for hi in top2:
            for lo in bottom2:
                if hi["ccy"] == lo["ccy"]:
                    continue
                pair = f"{hi['ccy']}{lo['ccy']}"
                if pair not in PAIRS:
                    pair = f"{lo['ccy']}{hi['ccy']}"
                if pair not in PAIRS:
                    continue
                chg = daily_chg_cc(fetch_pair_d1(pair))
                if chg is None or chg == 0:
                    continue
                direction = "LONG" if chg > 0 else "SHORT"
                if not h1_best_trade_filter(pair, direction):
                    continue
                dot = "🟢" if chg > 0 else "🔴"
                chg_txt = f"{chg:+.2f}%"
                if add_trade(dot, pair, chg_txt):
                    added = True
        if added:
            added_any = True

    if not added_any:
        lines.append("NO DEAL 😞")

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
                fire = psar_i < min(emas_i) and open_i < min(emas_i)
                events.append({"time": close_w.index[i], "fire": fire})
        else:
            if close_w.iloc[i] < psar_w.iloc[i] and close_w.iloc[i - 1] > psar_w.iloc[i - 1]:
                psar_i = psar_w.iloc[i]
                open_i = open_w.iloc[i]
                emas_i = [ema.iloc[i] for ema in ema_w]
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


def main():
    if load_dotenv:
        load_dotenv()

    results = []
    for pair in PAIRS:
        df_w = fetch_pair_w1(pair)
        df_d = fetch_pair_d1(pair)
        df_h = fetch_pair_h1(pair)
        if df_w is None or df_d is None or df_h is None or df_w.empty or df_d.empty or df_h.empty:
            continue

        psar_w = calculate_psar(df_w, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        psar_d = calculate_psar(df_d, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        if psar_w is None or psar_d is None:
            continue

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

        d_close = df_d["close"].iloc[-1]
        d_psar = psar_d.iloc[-1]
        d_emas = [ema_series(df_d["close"], l).iloc[-1] for l in EMA_STACK]

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

        psar_h = calculate_psar(df_h, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        if psar_h is None:
            continue

        ema_h1_stack = compute_ema_stack(df_h, EMA_STACK)
        cross_events = detect_cross_since_23h(df_h, psar_h, direction, ema_h1_stack)
        if not cross_events:
            continue

        fire_count = sum(1 for e in cross_events if e.get("fire"))
        chg_cc = daily_chg_cc(df_d)
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

    if not results:
        print("SAR BREAK V5")
        lines = ["SAR BREAK V5", "NO DEAL 😞"]
        extra_lines, index_rows, _ = collect_extra_chg_cc_sections()
        lines.extend(extra_lines)
        lines.extend(build_best_trade_lines(index_rows))
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat_id:
            try:
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                requests.post(url, json={"chat_id": chat_id, "text": "\n".join(lines)}, timeout=10)
            except Exception:
                pass
        return

    print("SAR BREAK V5")
    results.sort(key=lambda r: (r.get("chg_cc") is None, -(r.get("chg_cc") if r.get("chg_cc") is not None else float("-inf"))))
    lines = ["SAR BREAK V5"]
    for r in results:
        cross_count = len(r.get("cross_events", []))
        fire_count = r.get("fire_count", 0)
        fire_tag = " 🔥" if fire_count > 0 else ""
        chg_cc = r.get("chg_cc")
        chg_text = f"{chg_cc:+.2f}%" if chg_cc is not None else "N/A"
        print(f"{r['pair']} | {r['signal']} | CHG% (CC): {chg_text} | crosses: {cross_count}{fire_tag}")
        dot = "🟢" if r["signal"] == "LONG" else "🔴"
        lines.append(f"{dot} {r['pair']} ({chg_text}) : {cross_count}{fire_tag}")

    extra_lines, index_rows, _ = collect_extra_chg_cc_sections()
    lines.extend(extra_lines)
    lines.extend(build_best_trade_lines(index_rows))

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(url, json={"chat_id": chat_id, "text": "\n".join(lines)}, timeout=10)
        except Exception:
            pass


if __name__ == "__main__":
    main()
