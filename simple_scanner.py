#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TRIPLE COMBINED V4 (MTF RANGE + TRIPLE G 1H/Daily + GLOBAL)
Data source: TradingView WebSocket with Yahoo fallback (same style as reference).
No repaint: only confirmed bars (previous candle) are used.
"""

import sys
import subprocess
import importlib
import time
import json
import os
import random
import string
from datetime import datetime
from pathlib import Path

def install_and_import(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        return importlib.import_module(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name)

# Dependencies aligned with reference script
install_and_import("pandas")
install_and_import("numpy")
install_and_import("websocket-client", "websocket")
install_and_import("yfinance")
install_and_import("requests")
install_and_import("python-dotenv", "dotenv")

import pandas as pd
import numpy as np
import yfinance as yf
import requests
from dotenv import load_dotenv
from websocket import create_connection

# Load environment variables
load_dotenv()

# ----------------------------
# CONFIG
# ----------------------------
PAIRS = [
    "EURUSD=X", "EURGBP=X", "EURJPY=X", "EURCHF=X", "EURAUD=X", "EURCAD=X", "EURNZD=X",
    "GBPCHF=X", "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPUSD=X", "GBPJPY=X",
    "NZDCHF=X", "NZDCAD=X", "NZDUSD=X", "NZDJPY=X",
    "USDCHF=X", "USDJPY=X", "USDCAD=X",
    "AUDCHF=X", "AUDCAD=X", "AUDUSD=X", "AUDJPY=X", "AUDNZD=X",
    "CHFJPY=X", "CADJPY=X", "CADCHF=X"
]

EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]
LOOKBACK = 50

CACHE_FILE = "market_cache.pkl"
CACHE_MAX_AGE_SECONDS = 300
GLOBAL_CACHE = None

# Telegram Settings
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials not configured.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, json=payload, timeout=10)
        return response.json().get("ok", False)
    except Exception as e:
        print(f"Telegram error: {e}")
        return False

# ----------------------------
# DATA ENGINE (TV + Yahoo + Cache)
# ----------------------------
def load_global_cache():
    global GLOBAL_CACHE
    if GLOBAL_CACHE is not None:
        return
    if os.path.exists(CACHE_FILE):
        try:
            import pickle
            with open(CACHE_FILE, "rb") as f:
                cache_data = pickle.load(f)
            if isinstance(cache_data, dict) and "timestamp" in cache_data:
                cache_age = time.time() - cache_data["timestamp"]
                if cache_age > CACHE_MAX_AGE_SECONDS:
                    GLOBAL_CACHE = {}
                else:
                    GLOBAL_CACHE = cache_data["data"]
            else:
                GLOBAL_CACHE = cache_data
        except Exception:
            GLOBAL_CACHE = {}
    else:
        GLOBAL_CACHE = {}

def generate_session_id():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))

def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"

def fetch_data_yahoo(pair_yahoo, interval_code, n_candles=None):
    try:
        period = "1y"
        if interval_code == "1h":
            period = "1mo"
        elif interval_code == "1wk":
            period = "2y"

        df = yf.Ticker(pair_yahoo).history(period=period, interval=interval_code)
        if df.empty:
            return None

        df.index.name = "datetime"
        if interval_code == "1d":
            df = df[df.index.dayofweek < 5]
        return df
    except Exception:
        return None

def fetch_data_tv(pair_yahoo, interval_code, n_candles=300):
    clean_pair = pair_yahoo.replace("=X", "")
    tv_pair = f"OANDA:{clean_pair}"

    tv_interval = "60"
    if interval_code == "1h":
        tv_interval = "60"
    elif interval_code == "1d":
        tv_interval = "D"
    elif interval_code == "1wk":
        tv_interval = "W"

    ws = None
    extracted_df = None

    try:
        headers = {
            "Origin": "https://www.tradingview.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        ws = create_connection("wss://prodata.tradingview.com/socket.io/websocket", header=headers, timeout=10)
        session_id = generate_session_id()

        ws.send(create_message("chart_create_session", [session_id, ""]))
        ws.send(create_message("resolve_symbol", [session_id, "sds_sym_1",
                                                  f"={{\"symbol\":\"{tv_pair}\",\"adjustment\":\"splits\",\"session\":\"regular\"}}"]))
        ws.send(create_message("create_series", [session_id, "sds_1", "s1", "sds_sym_1", tv_interval, n_candles, ""]))

        start_t = time.time()
        while time.time() - start_t < 8:
            try:
                res = ws.recv()
                if "\"s\":[" in res:
                    start = res.find("\"s\":[")
                    end = res.find("\"ns\":", start)
                    if start != -1 and end != -1:
                        extract_end = end - 1
                        while res[extract_end] not in [",", "}"]:
                            extract_end -= 1

                        raw = res[start + 4:extract_end]
                        data = json.loads(raw)
                        fdata = [item["v"] for item in data]
                        extracted_df = pd.DataFrame(
                            fdata, columns=["timestamp", "open", "high", "low", "close", "volume"]
                        )
                        extracted_df["datetime"] = pd.to_datetime(extracted_df["timestamp"], unit="s", utc=True)
                        extracted_df.set_index("datetime", inplace=True)
                        extracted_df.rename(columns={
                            "open": "Open", "high": "High", "low": "Low",
                            "close": "Close", "volume": "Volume"
                        }, inplace=True)
                        extracted_df.drop(columns=["timestamp"], inplace=True)

                if "series_completed" in res:
                    break
            except Exception:
                pass

        ws.close()
        return extracted_df
    except Exception:
        if ws:
            try:
                ws.close()
            except Exception:
                pass
        return None

def fetch_data_smart(pair_yahoo, interval_code, n_candles=300):
    load_global_cache()
    if GLOBAL_CACHE and pair_yahoo in GLOBAL_CACHE:
        cache_data = GLOBAL_CACHE[pair_yahoo]
        if interval_code in cache_data:
            return cache_data[interval_code]

    df = fetch_data_tv(pair_yahoo, interval_code, n_candles)
    if df is not None and not df.empty:
        return df

    return fetch_data_yahoo(pair_yahoo, interval_code, n_candles)

# ----------------------------
# CORE LOGIC
# ----------------------------
def _confirmed_index(df):
    # Use previous bar to avoid repaint
    if df is None or len(df) < 2:
        return None
    return -2

def calc_mtf_range_signal(df_d, df_w, lookback):
    if df_d is None or df_w is None:
        return "NEUTRAL", "NEUTRAL"

    if len(df_d) < lookback + 1 or len(df_w) < lookback + 1:
        return "NEUTRAL", "NEUTRAL"

    d_mid = (df_d["High"].rolling(lookback).max() + df_d["Low"].rolling(lookback).min()) / 2.0
    w_mid = (df_w["High"].rolling(lookback).max() + df_w["Low"].rolling(lookback).min()) / 2.0

    d_mid_val = d_mid.iloc[-2]
    w_mid_val = w_mid.iloc[-2]

    d_close = df_d["Close"].iloc[-2]
    w_close = df_w["Close"].iloc[-2]

    s2 = 1 if d_close > d_mid_val else -1
    s3 = 1 if w_close > w_mid_val else -1

    score = s2 + s3
    is_buy = score > 0 and s2 > 0
    is_sell = score < 0 and s2 < 0

    mtf_signal = "BULL" if is_buy else "BEAR" if is_sell else "NEUTRAL"
    return mtf_signal, mtf_signal

def calc_triple_g_signal(df):
    if df is None or len(df) < max(EMA_LENGTHS) + 1:
        return "NEUTRAL"

    close = df["Close"]
    emas = [close.ewm(span=l, adjust=False).mean() for l in EMA_LENGTHS]

    idx = _confirmed_index(df)
    if idx is None:
        return "NEUTRAL"

    ema_vals = [ema.iloc[idx] for ema in emas]

    is_bull = all(ema_vals[i] > ema_vals[i + 1] for i in range(len(ema_vals) - 1))
    if is_bull:
        return "BULL"

    is_bear = all(ema_vals[i] < ema_vals[i + 1] for i in range(len(ema_vals) - 1))
    if is_bear:
        return "BEAR"

    return "NEUTRAL"

def calc_global_signal(mtf_signal, triple_h1, triple_d):
    bull_count = (1 if mtf_signal == "BULL" else 0) + (1 if triple_h1 == "BULL" else 0) + (1 if triple_d == "BULL" else 0)
    bear_count = (1 if mtf_signal == "BEAR" else 0) + (1 if triple_h1 == "BEAR" else 0) + (1 if triple_d == "BEAR" else 0)
    neutral_count = (1 if mtf_signal == "NEUTRAL" else 0) + (1 if triple_h1 == "NEUTRAL" else 0) + (1 if triple_d == "NEUTRAL" else 0)

    if bull_count == 3:
        return "BULL100"
    if bear_count == 3:
        return "BEAR100"
    if bull_count == 2 and neutral_count == 1:
        return "BULL67"
    if bear_count == 2 and neutral_count == 1:
        return "BEAR67"
    return "NEUTRAL"

def calc_runner_pct(df_h1, df_d):
    if df_h1 is None or df_h1.empty or df_d is None or df_d.empty:
        return None
    daily_open = df_d["Open"].iloc[-1]
    current_price = df_h1["Close"].iloc[-1]
    if daily_open == 0:
        return None
    return ((current_price - daily_open) / daily_open) * 100.0

def analyze_pair(ticker):
    df_h1 = fetch_data_smart(ticker, "1h", n_candles=500)
    df_d = fetch_data_smart(ticker, "1d", n_candles=300)
    df_w = fetch_data_smart(ticker, "1wk", n_candles=200)

    mtf_signal, _ = calc_mtf_range_signal(df_d, df_w, LOOKBACK)
    triple_h1 = calc_triple_g_signal(df_h1)
    triple_d = calc_triple_g_signal(df_d)
    runner_pct = calc_runner_pct(df_h1, df_d)

    global_signal = calc_global_signal(mtf_signal, triple_h1, triple_d)

    return {
        "pair": ticker.replace("=X", ""),
        "mtf_range": mtf_signal,
        "triple_h1": triple_h1,
        "triple_d": triple_d,
        "global": global_signal,
        "runner_pct": runner_pct
    }

def main():
    print("TRIPLE COMBINED V4 - Scanner")
    print(f"Pairs: {len(PAIRS)}")
    print("")

    results = []
    for pair in PAIRS:
        try:
            res = analyze_pair(pair)
            results.append(res)
            print(".", end="", flush=True)
        except Exception:
            print("x", end="", flush=True)

    print("\n")
    results = [
        r for r in results
        if r["runner_pct"] is not None and abs(r["runner_pct"]) > 0.2
    ]
    results.sort(key=lambda x: abs(x["runner_pct"]), reverse=True)

    print(f"{'PAIR':<10} | {'MTF_RANGE':<8} | {'TRIPLE_1H':<9} | {'TRIPLE_D':<9} | {'GLOBAL':<8} | {'RUNNER':<8}")
    print("-" * 71)
    if not results:
        print("Aucune paire.")
        return
    for r in results:
        runner = "NA"
        if r["runner_pct"] is not None:
            runner = f"{r['runner_pct']:+.2f}%"
        print(f"{r['pair']:<10} | {r['mtf_range']:<8} | {r['triple_h1']:<9} | {r['triple_d']:<9} | {r['global']:<8} | {runner:<8}")

    msg_lines = ["<b>TRIPLE COMBINED V4</b>", ""]
    for r in results:
        runner = "NA"
        if r["runner_pct"] is not None:
            runner = f"{r['runner_pct']:+.2f}%"
        emoji = "🟢" if r["runner_pct"] is not None and r["runner_pct"] > 0 else "🔴"
        msg_lines.append(f"{emoji} {r['pair']} ({runner})")

    msg_lines.append("")
    msg_lines.append(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')} Paris")

    telegram_msg = "\n".join(msg_lines)
    if send_telegram_message(telegram_msg):
        print("Telegram message sent.")
    else:
        print("Telegram message failed.")

if __name__ == "__main__":
    main()
