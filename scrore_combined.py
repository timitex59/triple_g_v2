#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Combined scanner:
1) Long-term filter (D1 + W1) from score_long logic.
2) Short-term confirmation (H1 + D1) from score.py logic.
"""

import sys
import subprocess
import importlib
import time
import json
import random
import string
import os
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

# Long-term (D1 + W1)
LONG_TOTAL_ABS = 14
LONG_RUNNER_MIN = 0.2
LONG_AVG_W1_MIN = 0.20

# Short-term (H1 + D1)
SHORT_TOTAL_MIN = 10
SHORT_RUNNER_MIN = 0.2
SHORT_AVG_D1_MIN = 0.05
SHORT_RATIO_MAX = 0.5

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
        elif interval_code == "1d":
            period = "2y"
        elif interval_code == "1wk":
            period = "5y"

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


def analyze_long(pair):
    df_d = fetch_data_smart(pair, "1d", n_candles=500)
    df_w = fetch_data_smart(pair, "1wk", n_candles=300)
    if df_d is None or df_w is None:
        return None

    scores_d = score_timeframe(df_d)
    scores_w = score_timeframe(df_w)
    if scores_d is None or scores_w is None:
        return None

    weekly_open = df_w["Open"].iloc[-1]
    weekly_close = df_w["Close"].iloc[-1]
    if weekly_open == 0:
        return None
    runner_pct = ((weekly_close - weekly_open) / weekly_open) * 100.0

    dist_w = ema_distances_pct(df_w)
    avg_gap_w = sum(dist_w) / len(dist_w) if dist_w else None

    total = sum(scores_d[i] + scores_w[i] for i in range(len(EMA_LENGTHS)))

    if abs(total) != LONG_TOTAL_ABS:
        return None
    if abs(runner_pct) <= LONG_RUNNER_MIN:
        return None
    if avg_gap_w is None or avg_gap_w < LONG_AVG_W1_MIN:
        return None

    return {
        "pair": pair.replace("=X", ""),
        "total": total,
        "runner_pct": runner_pct,
        "avg_gap_w": avg_gap_w
    }


def analyze_short(pair):
    df_h1 = fetch_data_smart(pair, "1h", n_candles=500)
    df_d = fetch_data_smart(pair, "1d", n_candles=300)
    if df_h1 is None or df_d is None:
        return None

    scores_h1 = score_timeframe(df_h1)
    scores_d = score_timeframe(df_d)
    if scores_h1 is None or scores_d is None:
        return None

    daily_open = df_d["Open"].iloc[-1]
    current_price = df_h1["Close"].iloc[-1]
    if daily_open == 0:
        return None
    runner_pct = ((current_price - daily_open) / daily_open) * 100.0

    dist_h1 = ema_distances_pct(df_h1)
    dist_d1 = ema_distances_pct(df_d)
    avg_gap_d1 = sum(dist_d1) / len(dist_d1) if dist_d1 else None
    avg_gap_h1 = sum(dist_h1) / len(dist_h1) if dist_h1 else None
    if avg_gap_d1 is None or avg_gap_h1 is None or avg_gap_d1 == 0:
        return None
    ratio = avg_gap_h1 / avg_gap_d1

    total = sum(scores_h1[i] + scores_d[i] for i in range(len(EMA_LENGTHS)))

    if total < SHORT_TOTAL_MIN:
        return None
    if abs(runner_pct) <= SHORT_RUNNER_MIN:
        return None
    if avg_gap_d1 < SHORT_AVG_D1_MIN:
        return None
    if ratio >= SHORT_RATIO_MAX:
        return None

    return {
        "total": total,
        "runner_pct": runner_pct,
        "avg_gap_d1": avg_gap_d1,
        "ratio": ratio
    }


def main():
    long_candidates = []
    for pair in PAIRS:
        try:
            long_res = analyze_long(pair)
            if long_res:
                long_candidates.append((pair, long_res))
                print(".", end="", flush=True)
            else:
                print("-", end="", flush=True)
        except Exception:
            print("x", end="", flush=True)

    print("\n")
    if not long_candidates:
        print("Aucune paire apres filtre long terme.")
        return

    finals = []
    for pair, long_res in long_candidates:
        try:
            short_res = analyze_short(pair)
            if short_res:
                finals.append({
                    "pair": long_res["pair"],
                    "l_total": long_res["total"],
                    "l_runner": long_res["runner_pct"],
                    "l_avg_w1": long_res["avg_gap_w"],
                    "s_total": short_res["total"],
                    "s_runner": short_res["runner_pct"],
                    "s_avg_d1": short_res["avg_gap_d1"],
                    "s_ratio": short_res["ratio"]
                })
        except Exception:
            pass

    if not finals:
        print("Aucune paire apres filtre court terme.")
        return

    finals.sort(key=lambda x: abs(x["s_total"]), reverse=True)

    header = "PAIR     | L_TOT | L_RUN  | L_W1  | S_TOT | S_RUN  | S_D1  | RATIO"
    print(header)
    print("-" * len(header))
    for r in finals:
        print(
            f"{r['pair']:<8} | {r['l_total']:+5d} | {r['l_runner']:+5.2f}% | {r['l_avg_w1']:.3f}% | "
            f"{r['s_total']:+5d} | {r['s_runner']:+5.2f}% | {r['s_avg_d1']:.3f}% | {r['s_ratio']:.2f}"
        )

    msg_lines = ["<b>SCORE ANALYSIS</b>", ""]
    for r in finals:
        total_emoji = "🟢" if r["l_total"] > 0 else "🔴"
        runner_emoji = "🟢" if r["s_runner"] > 0 else "🔴"
        msg_lines.append(f"{total_emoji}{runner_emoji} {r['pair']} ({r['s_runner']:+.2f}%)")
    msg_lines.append("")
    msg_lines.append(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')} Paris")

    if send_telegram_message("\n".join(msg_lines)):
        print("Telegram message sent.")
    else:
        print("Telegram message failed.")


if __name__ == "__main__":
    main()
