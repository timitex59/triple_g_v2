#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
INDEX_LONG_TREND
Analyze FX indices on weekly, daily, and H1.
Flags indices that are above the 8 EMAs on all 3 timeframes.
"""

import json
import random
import string
import time

import pandas as pd
from websocket import create_connection, WebSocketConnectionClosedException
import os
import requests

EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]
PSAR_START = 0.1
PSAR_INCREMENT = 0.1
PSAR_MAXIMUM = 0.2

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


def ema_series(close_series, length):
    return close_series.ewm(span=length, adjust=False).mean()


def daily_chg_pct_close_close(df_d):
    if df_d is None or df_d.empty or len(df_d) < 2:
        return None
    d_close = df_d["close"].iloc[-1]
    d_prev = df_d["close"].iloc[-2]
    if d_prev == 0:
        return None
    return (d_close - d_prev) / d_prev * 100.0


def fmt_num(v):
    return "na" if v is None or pd.isna(v) else f"{v:.2f}"


def calculate_psar(df, start, increment, maximum):
    if df is None or len(df) < 3:
        return None
    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values
    psar = [0.0] * len(df)
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
                if i >= 2:
                    psar[i] = min(psar[i], lows[i - 1], lows[i - 2])
                else:
                    psar[i] = min(psar[i], lows[i - 1])
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
                if i >= 2:
                    psar[i] = max(psar[i], highs[i - 1], highs[i - 2])
                else:
                    psar[i] = max(psar[i], highs[i - 1])
    return pd.Series(psar, index=df.index)


def is_above_8_emas(df):
    if df is None or df.empty:
        return None
    close = df["close"].iloc[-1]
    for length in EMA_LENGTHS:
        if close <= ema_series(df["close"], length).iloc[-1]:
            return False
    return True


def is_below_8_emas(df):
    if df is None or df.empty:
        return None
    close = df["close"].iloc[-1]
    for length in EMA_LENGTHS:
        if close >= ema_series(df["close"], length).iloc[-1]:
            return False
    return True


def main():
    rows = []
    for name, sym in INDICES.items():
        df_w = fetch_tv_ohlc(sym, "W", 200)
        df_d = fetch_tv_ohlc(sym, "D", 200)
        df_h = fetch_tv_ohlc(sym, "60", 400)
        chg_cc_d = daily_chg_pct_close_close(df_d)
        above_w = is_above_8_emas(df_w)
        above_d = is_above_8_emas(df_d)
        above_h = is_above_8_emas(df_h)
        below_w = is_below_8_emas(df_w)
        below_d = is_below_8_emas(df_d)
        below_h = is_below_8_emas(df_h)
        base_all_3 = True if above_w and above_d and above_h else False
        base_all_3_below = True if below_w and below_d and below_h else False

        psar_status = None
        if df_d is not None and not df_d.empty and (base_all_3 or base_all_3_below):
            psar_d = calculate_psar(df_d, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
            if psar_d is not None and not psar_d.empty:
                d_close = df_d["close"].iloc[-1]
                d_psar = psar_d.iloc[-1]
                if base_all_3:
                    psar_status = d_close > d_psar
                elif base_all_3_below:
                    psar_status = d_close < d_psar

        all_3 = base_all_3
        all_3_below = base_all_3_below
        if all_3 and (chg_cc_d is None or chg_cc_d <= 0.15):
            all_3 = False
        if all_3_below and (chg_cc_d is None or chg_cc_d >= -0.15):
            all_3_below = False
        rows.append(
            {
                "INDEX": f"{name} ({INDEX_TO_CCY.get(name, name)})",
                "CHG% (CC) DAILY": chg_cc_d,
                "W1_ABOVE_8_EMA": above_w,
                "D1_ABOVE_8_EMA": above_d,
                "H1_ABOVE_8_EMA": above_h,
                "ALL_3": all_3,
                "W1_BELOW_8_EMA": below_w,
                "D1_BELOW_8_EMA": below_d,
                "H1_BELOW_8_EMA": below_h,
                "ALL_3_BELOW": all_3_below,
                "D1_PSAR_OK": psar_status,
            }
        )

    df = pd.DataFrame(rows)
    df_display = df.copy()
    df_display["CHG% (CC) DAILY"] = df_display["CHG% (CC) DAILY"].map(fmt_num)
    df_display["W1_ABOVE_8_EMA"] = df_display["W1_ABOVE_8_EMA"].map(
        lambda v: "YES" if v is True else "NO" if v is False else "na"
    )
    df_display["D1_ABOVE_8_EMA"] = df_display["D1_ABOVE_8_EMA"].map(
        lambda v: "YES" if v is True else "NO" if v is False else "na"
    )
    df_display["H1_ABOVE_8_EMA"] = df_display["H1_ABOVE_8_EMA"].map(
        lambda v: "YES" if v is True else "NO" if v is False else "na"
    )
    df_display["ALL_3"] = df_display["ALL_3"].map(lambda v: "YES" if v else "NO")
    df_display["W1_BELOW_8_EMA"] = df_display["W1_BELOW_8_EMA"].map(
        lambda v: "YES" if v is True else "NO" if v is False else "na"
    )
    df_display["D1_BELOW_8_EMA"] = df_display["D1_BELOW_8_EMA"].map(
        lambda v: "YES" if v is True else "NO" if v is False else "na"
    )
    df_display["H1_BELOW_8_EMA"] = df_display["H1_BELOW_8_EMA"].map(
        lambda v: "YES" if v is True else "NO" if v is False else "na"
    )
    df_display["ALL_3_BELOW"] = df_display["ALL_3_BELOW"].map(lambda v: "YES" if v else "NO")
    df_display["D1_PSAR_OK"] = df_display["D1_PSAR_OK"].map(
        lambda v: "YES" if v is True else "NO" if v is False else "na"
    )

    print("INDEX LONG TREND (W1 + D1 + H1)")
    print(df_display.to_string(index=False))

    winners = df[df["ALL_3"] == True]["INDEX"].tolist()
    if winners:
        print("\nABOVE 8 EMA ON ALL 3 TF:")
        for w in winners:
            print(f"- {w}")
    else:
        print("\nABOVE 8 EMA ON ALL 3 TF: none")

    losers = df[df["ALL_3_BELOW"] == True]["INDEX"].tolist()
    if losers:
        print("\nBELOW 8 EMA ON ALL 3 TF:")
        for w in losers:
            print(f"- {w}")
    else:
        print("\nBELOW 8 EMA ON ALL 3 TF: none")

    # Telegram summary (PSAR OK only)
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        lines = ["INDEX LONG TREND"]
        psar_ok = df[df["D1_PSAR_OK"] == True]["INDEX"].tolist()
        if psar_ok:
            lines.append("PSAR OK (D1):")
            lines.extend([f"✅ {w}" for w in psar_ok])
        else:
            lines.append("PSAR OK (D1): none")
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(url, json={"chat_id": chat_id, "text": "\n".join(lines)}, timeout=10)
        except Exception:
            pass


if __name__ == "__main__":
    main()
