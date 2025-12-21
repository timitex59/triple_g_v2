#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EMA SCORE (H1 + D1)
For each EMA i, score +1 if EMA_i > EMA_{i+1}, -1 if EMA_i < EMA_{i+1}.
Scores are computed on H1 and D1, then combined per EMA and summed.
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


def install_and_import(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        return importlib.import_module(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name)


install_and_import("pandas")
install_and_import("websocket-client", "websocket")
install_and_import("yfinance")
install_and_import("requests")
install_and_import("python-dotenv", "dotenv")

import pandas as pd
import yfinance as yf
import requests
from dotenv import load_dotenv
from websocket import create_connection

# Load environment variables
load_dotenv(dotenv_path=r"E:\TRADINGVIEW\.env")
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
USE_CONFIRMED = True
TOP_TOTAL_MIN = 10
TOP_AVG_D1_MIN = 0.05
TOP_RATIO_MAX = 0.5
TOP_RUNNER_MIN = 0.2

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
# DATA ENGINE (TV + Yahoo)
# ----------------------------

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
    df = fetch_data_tv(pair_yahoo, interval_code, n_candles)
    if df is not None and not df.empty:
        return df
    return fetch_data_yahoo(pair_yahoo, interval_code, n_candles)

# ----------------------------
# SCORING
# ----------------------------

def _score_index(df):
    if df is None or len(df) < 1:
        return None
    if USE_CONFIRMED:
        return -2 if len(df) >= 2 else None
    return -1


def score_timeframe(df):
    idx = _score_index(df)
    if idx is None or len(df) < max(EMA_LENGTHS) + 1:
        return None

    close = df["Close"]
    ema_vals = [close.ewm(span=l, adjust=False).mean().iloc[idx] for l in EMA_LENGTHS]

    scores = []
    for i in range(len(ema_vals) - 1):
        if ema_vals[i] > ema_vals[i + 1]:
            scores.append(1)
        elif ema_vals[i] < ema_vals[i + 1]:
            scores.append(-1)
        else:
            scores.append(0)

    scores.append(0)
    return scores

def ema_distances_pct(df):
    idx = _score_index(df)
    if idx is None or df is None or len(df) < max(EMA_LENGTHS) + 1:
        return None

    close = df["Close"]
    price = close.iloc[idx]
    if price == 0:
        return None

    ema_vals = [close.ewm(span=l, adjust=False).mean().iloc[idx] for l in EMA_LENGTHS]
    dists = []
    for i in range(len(ema_vals) - 1):
        dist = abs(ema_vals[i] - ema_vals[i + 1])
        dists.append((dist / price) * 100.0)
    return dists


def analyze_pair(ticker):
    df_h1 = fetch_data_smart(ticker, "1h", n_candles=500)
    df_d = fetch_data_smart(ticker, "1d", n_candles=300)

    scores_h1 = score_timeframe(df_h1)
    scores_d = score_timeframe(df_d)
    if scores_h1 is None or scores_d is None:
        return None

    runner_pct = None
    if df_h1 is not None and not df_h1.empty and df_d is not None and not df_d.empty:
        daily_open = df_d["Open"].iloc[-1]
        current_price = df_h1["Close"].iloc[-1]
        if daily_open != 0:
            runner_pct = ((current_price - daily_open) / daily_open) * 100.0

    distances_h1 = ema_distances_pct(df_h1)
    distances_d = ema_distances_pct(df_d)
    avg_gap_h1 = sum(distances_h1) / len(distances_h1) if distances_h1 else None
    avg_gap_d = sum(distances_d) / len(distances_d) if distances_d else None
    ratio_gap = None
    if avg_gap_h1 is not None and avg_gap_d is not None and avg_gap_d != 0:
        ratio_gap = avg_gap_h1 / avg_gap_d

    combined = [scores_h1[i] + scores_d[i] for i in range(len(EMA_LENGTHS))]
    total = sum(combined)

    return {
        "pair": ticker.replace("=X", ""),
        "scores_h1": scores_h1,
        "scores_d": scores_d,
        "scores_global": combined,
        "total": total,
        "runner_pct": runner_pct,
        "dist_h1": distances_h1,
        "dist_d": distances_d,
        "avg_gap_h1": avg_gap_h1,
        "avg_gap_d": avg_gap_d,
        "ratio_gap": ratio_gap
    }


def main():
    results = []
    for pair in PAIRS:
        try:
            res = analyze_pair(pair)
            if res is not None:
                results.append(res)
                print(".", end="", flush=True)
        except Exception:
            print("x", end="", flush=True)

    print("\n")
    results.sort(key=lambda x: abs(x["total"]), reverse=True)

    header = "PAIR       | TOTAL | RUNNER | AVG_H1 | AVG_D1 | RATIO | E1  E2  E3  E4  E5  E6  E7  E8"
    print(header)
    print("-" * len(header))
    for r in results:
        per_ema = " ".join([f"{v:+d}" for v in r["scores_global"]])
        runner = "NA"
        if r["runner_pct"] is not None:
            runner = f"{r['runner_pct']:+.2f}%"
        avg_h1 = f"{r['avg_gap_h1']:.3f}%" if r["avg_gap_h1"] is not None else "NA"
        avg_d1 = f"{r['avg_gap_d']:.3f}%" if r["avg_gap_d"] is not None else "NA"
        ratio = f"{r['ratio_gap']:.2f}" if r["ratio_gap"] is not None else "NA"
        print(f"{r['pair']:<10} | {r['total']:+5d} | {runner:<7} | {avg_h1:<6} | {avg_d1:<6} | {ratio:<5} | {per_ema}")
        if r["dist_h1"] is not None:
            h1_dist = " ".join([f"{v:.3f}%" for v in r["dist_h1"]])
            print(f"{'H1%':<10} | {'':<5} | {'':<7} | {h1_dist}")
        if r["dist_d"] is not None:
            d_dist = " ".join([f"{v:.3f}%" for v in r["dist_d"]])
            print(f"{'D1%':<10} | {'':<5} | {'':<7} | {d_dist}")

    top_setups = []
    for r in results:
        if r["total"] < TOP_TOTAL_MIN:
            continue
        if r["runner_pct"] is None or abs(r["runner_pct"]) <= TOP_RUNNER_MIN:
            continue
        if r["avg_gap_d"] is None or r["avg_gap_d"] < TOP_AVG_D1_MIN:
            continue
        if r["ratio_gap"] is None or r["ratio_gap"] >= TOP_RATIO_MAX:
            continue
        top_setups.append(r)

    if top_setups:
        top_setups_sorted = sorted(
            top_setups,
            key=lambda r: (r["runner_pct"] is not None, r["runner_pct"]),
            reverse=True
        )
        print("\nTOP SETUPS")
        print(f"{'PAIR':<10} | {'TOTAL':<5} | {'RUNNER':<7} | {'AVG_D1':<6} | {'RATIO':<5}")
        print("-" * 47)
        for r in top_setups_sorted:
            runner = "NA"
            if r["runner_pct"] is not None:
                runner = f"{r['runner_pct']:+.2f}%"
            avg_d1 = f"{r['avg_gap_d']:.3f}%"
            ratio = f"{r['ratio_gap']:.2f}"
            print(f"{r['pair']:<10} | {r['total']:+5d} | {runner:<7} | {avg_d1:<6} | {ratio:<5}")

        msg_lines = ["<b>SCORE ANALYSIS</b>", ""]
        for r in top_setups_sorted:
            emoji = "🟢" if r["runner_pct"] is not None and r["runner_pct"] > 0 else "🔴"
            runner = "NA"
            if r["runner_pct"] is not None:
                runner = f"{r['runner_pct']:+.2f}%"
            msg_lines.append(f"{emoji} {r['pair']} ({runner})")
        msg_lines.append("")
        msg_lines.append(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')} Paris")

        if send_telegram_message("\n".join(msg_lines)):
            print("Telegram message sent.")
        else:
            print("Telegram message failed.")
    else:
        print("\nTOP SETUPS")
        print("Aucune paire.")


if __name__ == "__main__":
    main()
