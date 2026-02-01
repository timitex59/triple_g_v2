#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
INDEX_POWER_1H
1) Position each index vs 8 EMAs and compute DIST% EMA20 (H1 only).
2) Score and rank indices using H1 distance only.
"""

import json
import random
import string
import time

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

EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]

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

PAIR_LIST = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURNZD", "EURCHF",
    "GBPAUD", "GBPCAD", "GBPNZD", "GBPCHF",
    "AUDNZD", "AUDCAD", "AUDCHF",
    "NZDCAD", "NZDCHF",
    "CADCHF",
]
PAIR_SET = set(PAIR_LIST)


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


def dist_from_ema20_h1(df_h1):
    if df_h1 is None or df_h1.empty:
        return None
    h1_close = df_h1["close"].iloc[-1]
    e20 = ema_series(df_h1["close"], EMA_LENGTHS[0]).iloc[-1]
    if e20 == 0:
        return None
    return (h1_close - e20) / e20 * 100.0


def is_below_8_emas_h1(df_h1):
    if df_h1 is None or df_h1.empty:
        return None
    h1_close = df_h1["close"].iloc[-1]
    for length in EMA_LENGTHS:
        if h1_close >= ema_series(df_h1["close"], length).iloc[-1]:
            return False
    return True


def fmt_num(v):
    return "na" if v is None or pd.isna(v) else f"{v:.2f}"


def fetch_pair_chg_cc_daily(pair_code, d1_candles=200):
    """
    Fetch daily close/close change % for a forex pair (informational only).
    Tries annual_V2.fetch_data_tv if available, otherwise uses local fetch_tv_ohlc.
    """
    df = None
    if annual and hasattr(annual, "fetch_data_tv"):
        try:
            df = annual.fetch_data_tv("", "1d", n_candles=d1_candles, tv_symbol=f"OANDA:{pair_code}")
        except Exception:
            df = None
    if df is None:
        df = fetch_tv_ohlc(f"OANDA:{pair_code}", "D", d1_candles)
    if df is None or df.empty or len(df) < 2:
        return None
    d_close = df["close"].iloc[-1] if "close" in df.columns else df["Close"].iloc[-1]
    d_prev = df["close"].iloc[-2] if "close" in df.columns else df["Close"].iloc[-2]
    if d_prev == 0:
        return None
    return (d_close - d_prev) / d_prev * 100.0


def main():
    if load_dotenv:
        load_dotenv()

    h1_candles = 200

    rows = []
    for name, sym in INDICES.items():
        h1 = fetch_tv_ohlc(sym, "60", h1_candles)
        dist_h1 = dist_from_ema20_h1(h1)
        below8 = is_below_8_emas_h1(h1)
        rows.append(
            {
                "INDEX": f"{name} ({INDEX_TO_CCY.get(name, name)})",
                "DIST% EMA20 H1": dist_h1,
                "POWER": dist_h1,
                "TREND": None if dist_h1 is None else dist_h1 * dist_h1,
                "DEAL": None,
                "BELOW_8_EMA": below8,
            }
        )

    df = pd.DataFrame(rows)

    # DEAL: sign follows DIST% EMA20 H1
    def deal_value(row):
        b = row.get("DIST% EMA20 H1")
        if pd.isna(b):
            return None
        sign = -1 if b < 0 else 1
        return sign * abs(b) * abs(b)

    df["DEAL"] = df.apply(deal_value, axis=1)
    df_sorted = df.sort_values(by="DEAL", ascending=False)
    df_display = df_sorted.copy()
    df_display["DIST% EMA20 H1"] = df_display["DIST% EMA20 H1"].apply(fmt_num)
    df_display["POWER"] = df_display["POWER"].apply(fmt_num)
    df_display["TREND"] = df_display["TREND"].apply(fmt_num)
    df_display["DEAL"] = df_display["DEAL"].apply(fmt_num)
    df_display["BELOW_8_EMA"] = df_display["BELOW_8_EMA"].map(
        lambda v: "BEAR" if v is True else "BULL" if v is False else "na"
    )

    print("INDEX POWER 1H")
    power_avg = df["POWER"].dropna().mean() if "POWER" in df else None
    if power_avg is None or pd.isna(power_avg):
        print("AVG POWER: na")
    else:
        print(f"AVG POWER: {power_avg:.2f}")
    print(df_display.to_string(index=False))

    # BEST DEAL combinations
    df_deal = df.copy()
    df_deal["CCY"] = df_deal["INDEX"].str.extract(r"\(([^)]+)\)")
    min_abs_dist = 0.1
    df_deal = df_deal[df_deal["DIST% EMA20 H1"].abs() > min_abs_dist]
    top_pos = df_deal[df_deal["DEAL"] > 0].sort_values(by="DEAL", ascending=False).head(2)
    top_neg = df_deal[df_deal["DEAL"] < 0].sort_values(by="DEAL", ascending=True).head(2)

    valid_lines = []
    if not top_pos.empty and not top_neg.empty:
        print("\nBEST DEAL")
        combos = []
        dist_map = {row["CCY"]: row["DIST% EMA20 H1"] for _, row in df_deal.iterrows()}
        for _, p in top_pos.iterrows():
            for _, n in top_neg.iterrows():
                strong = p["CCY"]
                weak = n["CCY"]
                if not isinstance(strong, str) or not isinstance(weak, str):
                    continue
                strong_dist = dist_map.get(strong)
                weak_dist = dist_map.get(weak)
                if pd.isna(strong_dist) or pd.isna(weak_dist):
                    continue
                if (strong_dist >= 0 and weak_dist >= 0) or (strong_dist <= 0 and weak_dist <= 0):
                    continue
                pair = None
                direction = None
                if strong + weak in PAIR_SET:
                    pair = strong + weak
                    direction = "LONG"
                elif weak + strong in PAIR_SET:
                    pair = weak + strong
                    direction = "SHORT"
                if pair:
                    combos.append((pair, strong, weak, direction))
        if combos:
            # Sort by combined strength (abs strong DEAL + abs weak DEAL)
            deal_map = {row["CCY"]: row["DEAL"] for _, row in df_deal.iterrows()}
            def combo_strength(item):
                _, strong, weak, _ = item
                return abs(deal_map.get(strong, 0)) + abs(deal_map.get(weak, 0))

            combos.sort(key=combo_strength, reverse=True)
            for pair, strong, weak, direction in combos:
                chg_cc_d = fetch_pair_chg_cc_daily(pair)
                if chg_cc_d is None:
                    dot = "na"
                    chg_txt = "na"
                else:
                    dot = "🟢" if chg_cc_d > 0 else "🔴"
                    chg_txt = f"{chg_cc_d:+.2f}%"
                print(f"{pair} ({direction}) | {strong} strong vs {weak} weak | CHG% (CC): {chg_txt}")
                if chg_cc_d is not None and abs(chg_cc_d) > 0.2:
                    valid_lines.append(f"{dot} {pair} ({chg_txt})")
        else:
            print("No valid pairs found for best deal combinations.")
    else:
        print("\nBEST DEAL\nNot enough positive/negative DEAL values.")

    # Telegram: send only valid BEST DEAL
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        if valid_lines:
            msg = "INDEX ANALYSIS 1H\n" + "\n".join(valid_lines)
        else:
            msg = "INDEX ANALYSIS 1H\nNO DEAL"
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)
        except Exception:
            pass


if __name__ == "__main__":
    main()
