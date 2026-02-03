#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SAR_BREAK_V3
Conditions:
1) CHG% (CC) > 0 AND daily close > PSAR_daily AND H1 close > EMA20
   If H1 price/PSAR cross UP since 23h (Paris), emit LONG signal.
2) CHG% (CC) < 0 AND daily close < PSAR_daily AND H1 close < EMA20
   If H1 price/PSAR cross DOWN since 23h (Paris), emit SHORT signal.

Day resets at 23:00 Europe/Paris (new daily candle start).
Sends Telegram message for valid signals.
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

EMA20_LEN = 20
EMA_STACK = [20, 25, 30, 35, 40, 45, 50, 55]

H1_CANDLES = 700
D1_CANDLES = 250

TZ_NAME = "Europe/Paris"

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURNZD", "EURCHF",
    "GBPAUD", "GBPCAD", "GBPNZD", "GBPCHF",
    "AUDNZD", "AUDCAD", "AUDCHF",
    "NZDCAD", "NZDCHF",
    "CADCHF",
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


def daily_chg_cc(df_d):
    if df_d is None or df_d.empty or len(df_d) < 2:
        return None
    d_close = df_d["close"].iloc[-1]
    d_prev = df_d["close"].iloc[-2]
    if d_prev == 0:
        return None
    return (d_close - d_prev) / d_prev * 100.0


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


def ema_alignment_h1(df_h1, direction):
    if df_h1 is None or df_h1.empty:
        return False
    emas = [ema_series(df_h1["close"], l).iloc[-1] for l in EMA_STACK]
    if direction == "UP":
        return all(emas[i] > emas[i + 1] for i in range(len(emas) - 1))
    return all(emas[i] < emas[i + 1] for i in range(len(emas) - 1))


def detect_cross_since_23h(df_h1, psar, direction, tz_name=TZ_NAME):
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
        df_d = fetch_pair_d1(pair)
        df_h = fetch_pair_h1(pair)
        if df_d is None or df_h is None or df_d.empty or df_h.empty:
            continue

        chg = daily_chg_cc(df_d)
        if chg is None or chg == 0:
            continue

        psar_d = calculate_psar(df_d, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        psar_h = calculate_psar(df_h, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        if psar_d is None or psar_h is None:
            continue

        daily_close = df_d["close"].iloc[-1]
        daily_psar = psar_d.iloc[-1]
        h1_close = df_h["close"].iloc[-1]
        h1_ema20 = ema_series(df_h["close"], EMA20_LEN).iloc[-1]

        if chg > 0:
            if not (daily_close > daily_psar and h1_close > h1_ema20):
                continue
            if not ema_alignment_h1(df_h, "UP"):
                continue
            direction = "UP"
            signal = "LONG"
        else:
            if not (daily_close < daily_psar and h1_close < h1_ema20):
                continue
            if not ema_alignment_h1(df_h, "DOWN"):
                continue
            direction = "DOWN"
            signal = "SHORT"

        cross_times = detect_cross_since_23h(df_h, psar_h, direction)
        if not cross_times:
            continue

        # Filter: at least 2 crossings required
        if len(cross_times) < 2:
            continue

        # Filter: CHG% (CC) must be > 0.2 in absolute value
        if abs(chg) <= 0.2:
            continue

        results.append(
            {
                "pair": pair,
                "chg_cc": chg,
                "signal": signal,
                "cross_times": cross_times,
            }
        )

    if not results:
        print("SAR BREAK V3: no signals")
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat_id:
            try:
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                requests.post(url, json={"chat_id": chat_id, "text": "SAR BREAK V3\nNO SIGNAL 😞"}, timeout=10)
            except Exception:
                pass
        return

    print("SAR BREAK V3")
    lines = ["SAR BREAK V3"]
    results.sort(key=lambda r: abs(r["chg_cc"]), reverse=True)
    for r in results:
        cross_count = len(r.get("cross_times", []))
        flame = " 🔥" if abs(r["chg_cc"]) > 0.15 else ""
        print(f"{r['pair']} | CHG% (CC): {r['chg_cc']:+.2f}% | crosses: {cross_count}{flame}")
        dot = "🟢" if r["chg_cc"] > 0 else "🔴"
        lines.append(f"{dot} {r['pair']} ({r['chg_cc']:+.2f}%) : {cross_count}{flame}")

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
