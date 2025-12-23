#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Weekly SAR + EMA Scanner
Check which forex pairs are above all 8 EMAs and above PSAR.
PSAR parameters: start=0.1, increment=0.1, maximum=0.2
"""

import os
import sys
import time
import json
import random
import string
import importlib
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed


def install_and_import(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        return importlib.import_module(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name)


yf = install_and_import("yfinance")
pd = install_and_import("pandas")
np = install_and_import("numpy")
websocket = install_and_import("websocket-client", "websocket")
from websocket import create_connection
requests = install_and_import("requests")
dotenv = install_and_import("python-dotenv", "dotenv")


PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
    "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
    "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPCHF=X",
    "AUDNZD=X", "AUDCAD=X", "AUDCHF=X",
    "NZDCAD=X", "NZDCHF=X",
    "CADCHF=X",
]

EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]

PSAR_START = 0.1
PSAR_INCREMENT = 0.1
PSAR_MAXIMUM = 0.2

TV_CANDLES = 200
MAX_WORKERS = 8
TRACKING_FILE = "trend_follower.json"

dotenv.load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def generate_session_id():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"


def fetch_data_tv(pair_yahoo, interval_code, n_candles=200):
    clean_pair = pair_yahoo.replace("=X", "")
    tv_pair = f"OANDA:{clean_pair}"

    if interval_code == "1wk":
        tv_interval = "W"
    elif interval_code == "1d":
        tv_interval = "D"
    elif interval_code == "1h":
        tv_interval = "60"
    else:
        tv_interval = "W"

    ws = None
    extracted_df = None

    try:
        headers = {
            "Origin": "https://www.tradingview.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

        ws = create_connection("wss://prodata.tradingview.com/socket.io/websocket", header=headers, timeout=10)
        session_id = generate_session_id()

        ws.send(create_message("chart_create_session", [session_id, ""]))
        ws.send(create_message("resolve_symbol", [session_id, "sds_sym_1", f"={{\"symbol\":\"{tv_pair}\",\"adjustment\":\"splits\",\"session\":\"regular\"}}"]))
        ws.send(create_message("create_series", [session_id, "sds_1", "s1", "sds_sym_1", tv_interval, n_candles, ""]))

        start_t = time.time()
        while time.time() - start_t < 8:
            try:
                res = ws.recv()
                if '"s":[' in res:
                    start = res.find('"s":[')
                    end = res.find('"ns":', start)
                    if start != -1 and end != -1:
                        extract_end = end - 1
                        while res[extract_end] not in [",", "}"]:
                            extract_end -= 1

                        raw = res[start + 4 : extract_end]
                        data = json.loads(raw)
                        fdata = [item["v"] for item in data]
                        extracted_df = pd.DataFrame(
                            fdata,
                            columns=["timestamp", "open", "high", "low", "close", "volume"],
                        )

                        extracted_df["datetime"] = pd.to_datetime(extracted_df["timestamp"], unit="s", utc=True)
                        extracted_df.set_index("datetime", inplace=True)
                        extracted_df.rename(
                            columns={
                                "open": "Open",
                                "high": "High",
                                "low": "Low",
                                "close": "Close",
                                "volume": "Volume",
                            },
                            inplace=True,
                        )
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


def fetch_data_yahoo(pair_yahoo, interval_code, period):
    try:
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = yf.Ticker(pair_yahoo).history(period=period, interval=interval_code)
            if df.empty:
                return None
            return df
    except Exception:
        return None


def fetch_data_smart(pair, interval, n_candles, yahoo_period):
    df = fetch_data_tv(pair, interval, n_candles=n_candles)
    if df is not None and not df.empty:
        return df, "TV"

    df = fetch_data_yahoo(pair, interval, yahoo_period)
    if df is not None and not df.empty:
        return df, "YF"

    return None, "NA"


def calculate_psar_series(df, start, increment, maximum):
    if df is None or len(df) < 3:
        return None

    highs = df["High"].values
    lows = df["Low"].values
    closes = df["Close"].values

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

    return psar


def calculate_pct_runner_daily(df):
    if df is None or len(df) < 1:
        return None
    open_price = df["Open"].iloc[-1]
    close_price = df["Close"].iloc[-1]
    if open_price > 0:
        return ((close_price - open_price) / open_price) * 100.0
    return None


def format_runner(value):
    if value is None:
        return "NA"
    return f"{value:+.2f}%"


def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials not configured.")
        return False

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
        }
        response = requests.post(url, json=payload, timeout=10)
        return response.json().get("ok", False)
    except Exception as exc:
        print(f"Telegram error: {exc}")
        return False


def trend_ball(signal):
    if signal == "BULL":
        return "🟢"
    if signal == "BEAR":
        return "🔴"
    return "⚪"


def runner_ball(value):
    if value is None or value == 0:
        return "⚪"
    return "🟢" if value > 0 else "🔴"


def format_alignment_ball(item, main_signal):
    main_ball = trend_ball(main_signal)
    hourly_ball = trend_ball(item["hourly_signal"])
    return main_ball + hourly_ball



def build_telegram_message(bull_results, bear_results, new_flags):
    new_bull = set(new_flags.get("bullish", []))
    new_bear = set(new_flags.get("bearish", []))
    stats_lines = format_currency_strength_stats(bull_results, bear_results)
    lines = []
    if bull_results:
        lines.append("BULLISH W+D")
        for item in sorted(bull_results, key=lambda x: x["pair"]):
            balls = format_alignment_ball(item, "BULL")
            check = " ✅" if item["pair"] in new_bull else ""
            lines.append(f"{balls} {item['pair']}{check}")
    if bear_results:
        if lines:
            lines.append("")
        lines.append("BEARISH W+D")
        for item in sorted(bear_results, key=lambda x: x["pair"]):
            balls = format_alignment_ball(item, "BEAR")
            check = " ✅" if item["pair"] in new_bear else ""
            lines.append(f"{balls} {item['pair']}{check}")
    if stats_lines:
        if lines:
            lines.append("")
        lines.extend(stats_lines)
    return "\n".join(lines)


def tracking_file_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), TRACKING_FILE)


def load_tracking_state(path):
    if not os.path.exists(path):
        return {"current": {"bullish": [], "bearish": []}}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if "current" not in data:
            data["current"] = {"bullish": [], "bearish": []}
        return data
    except Exception:
        return {"current": {"bullish": [], "bearish": []}}


def save_tracking_state(path, bull_results, bear_results):
    previous = load_tracking_state(path).get("current", {})
    prev_bull = set(previous.get("bullish", []))
    prev_bear = set(previous.get("bearish", []))

    curr_bull = [item["pair"] for item in bull_results]
    curr_bear = [item["pair"] for item in bear_results]
    curr_bull_set = set(curr_bull)
    curr_bear_set = set(curr_bear)

    new_bull = sorted(curr_bull_set - prev_bull)
    new_bear = sorted(curr_bear_set - prev_bear)
    exit_bull = sorted(prev_bull - curr_bull_set)
    exit_bear = sorted(prev_bear - curr_bear_set)

    def with_new_flag(pairs, new_set):
        return [{"pair": pair, "new": pair in new_set} for pair in pairs]

    data = {
        "updated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "current": {
            "bullish": curr_bull,
            "bearish": curr_bear,
        },
        "current_with_flags": {
            "bullish": with_new_flag(curr_bull, set(new_bull)),
            "bearish": with_new_flag(curr_bear, set(new_bear)),
        },
        "new_entries": {
            "bullish": new_bull,
            "bearish": new_bear,
        },
        "exits": {
            "bullish": exit_bull,
            "bearish": exit_bear,
        },
    }

    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    return data


def format_currency_strength_stats(bull_results, bear_results):
    total_pairs = len(bull_results) + len(bear_results)
    if total_pairs == 0:
        return []

    strong_counts = {}
    weak_counts = {}

    for item in bull_results:
        pair = item["pair"]
        if len(pair) < 6:
            continue
        base = pair[:3]
        quote = pair[3:]
        strong_counts[base] = strong_counts.get(base, 0) + 1
        weak_counts[quote] = weak_counts.get(quote, 0) + 1

    for item in bear_results:
        pair = item["pair"]
        if len(pair) < 6:
            continue
        base = pair[:3]
        quote = pair[3:]
        strong_counts[quote] = strong_counts.get(quote, 0) + 1
        weak_counts[base] = weak_counts.get(base, 0) + 1

    def top_stats(counts):
        if not counts:
            return [], 0, 0.0
        max_count = max(counts.values())
        top = sorted([k for k, v in counts.items() if v == max_count])
        pct = (max_count / total_pairs) * 100.0
        return top, max_count, pct

    top_strong, strong_count, strong_pct = top_stats(strong_counts)
    top_weak, weak_count, weak_pct = top_stats(weak_counts)

    strong_str = ", ".join(top_strong) if top_strong else "NA"
    weak_str = ", ".join(top_weak) if top_weak else "NA"

    return [
        "STATS",
        f"Strongest: {strong_str} ({strong_pct:.1f}%, {strong_count}/{total_pairs})",
        f"Weakest: {weak_str} ({weak_pct:.1f}%, {weak_count}/{total_pairs})",
    ]


def evaluate_conditions(df):
    if df is None or df.empty:
        return None

    required = {"High", "Low", "Close"}
    if not required.issubset(df.columns):
        return None

    df = df.dropna(subset=["High", "Low", "Close"])
    if len(df) < max(EMA_LENGTHS) + 2:
        return None

    close_last = df["Close"].iloc[-1]
    ema_values = [df["Close"].ewm(span=length, adjust=False).mean().iloc[-1] for length in EMA_LENGTHS]
    above_emas = all(close_last > ema for ema in ema_values)
    below_emas = all(close_last < ema for ema in ema_values)

    psar = calculate_psar_series(df, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
    if psar is None:
        return None

    above_psar = close_last > psar[-1]
    below_psar = close_last < psar[-1]

    if above_emas and above_psar:
        return {
            "signal": "BULL",
            "close": close_last,
            "psar": psar[-1],
        }

    if below_emas and below_psar:
        return {
            "signal": "BEAR",
            "close": close_last,
            "psar": psar[-1],
        }

    return {"signal": "NEUTRAL", "close": close_last, "psar": psar[-1]}


def analyze_pair(pair):
    df_w, source_w = fetch_data_smart(pair, "1wk", n_candles=TV_CANDLES, yahoo_period="3y")
    df_d, source_d = fetch_data_smart(pair, "1d", n_candles=TV_CANDLES, yahoo_period="2y")
    df_h, source_h = fetch_data_smart(pair, "1h", n_candles=TV_CANDLES, yahoo_period="2mo")

    weekly = evaluate_conditions(df_w)
    daily = evaluate_conditions(df_d)
    hourly = evaluate_conditions(df_h)
    daily_runner = calculate_pct_runner_daily(df_d)

    if weekly is None or daily is None or hourly is None:
        return None
    if daily_runner is None or daily_runner == 0:
        return None

    if weekly["signal"] == "BULL" and daily["signal"] == "BULL" and daily_runner > 0:
        return {
            "pair": pair.replace("=X", ""),
            "source": f"{source_w}/{source_d}/{source_h}",
            "close": weekly["close"],
            "psar": weekly["psar"],
            "signal": "BULL",
            "hourly_signal": hourly["signal"],
            "daily_runner": daily_runner,
        }

    if weekly["signal"] == "BEAR" and daily["signal"] == "BEAR" and daily_runner < 0:
        return {
            "pair": pair.replace("=X", ""),
            "source": f"{source_w}/{source_d}/{source_h}",
            "close": weekly["close"],
            "psar": weekly["psar"],
            "signal": "BEAR",
            "hourly_signal": hourly["signal"],
            "daily_runner": daily_runner,
        }

    return None


def main():
    bull_results = []
    bear_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_pair, pair): pair for pair in PAIRS}
        for future in as_completed(futures):
            result = future.result()
            if result:
                if result["signal"] == "BULL":
                    bull_results.append(result)
                elif result["signal"] == "BEAR":
                    bear_results.append(result)

    bull_results.sort(key=lambda x: x["pair"])
    bear_results.sort(key=lambda x: x["pair"])

    print("Weekly SAR + EMA (8) Scanner")
    print(f"PSAR: start={PSAR_START}, increment={PSAR_INCREMENT}, maximum={PSAR_MAXIMUM}")
    print(
        f"Pairs scanned: {len(PAIRS)} | Bullish: {len(bull_results)} | Bearish: {len(bear_results)}"
    )
    print("-" * 64)

    if bull_results:
        bull_aligned = [item for item in bull_results if item["hourly_signal"] == "BULL"]
        bull_unaligned = [item for item in bull_results if item["hourly_signal"] != "BULL"]

        print("Bullish W+D, H1 aligned (above 8 EMA + above PSAR)")
        print(f"{'PAIR':<10} {'SOURCE':<10} {'CLOSE':>12} {'PSAR':>12} {'%RUNNER D':>11}")
        for item in bull_aligned:
            print(
                f"{item['pair']:<10} {item['source']:<10} {item['close']:>12.5f} {item['psar']:>12.5f} {format_runner(item['daily_runner']):>11}"
            )
        if not bull_aligned:
            print("None")

        print("-" * 64)
        print("Bullish W+D, H1 not aligned")
        print(f"{'PAIR':<10} {'SOURCE':<10} {'CLOSE':>12} {'PSAR':>12} {'%RUNNER D':>11}")
        for item in bull_unaligned:
            print(
                f"{item['pair']:<10} {item['source']:<10} {item['close']:>12.5f} {item['psar']:>12.5f} {format_runner(item['daily_runner']):>11}"
            )
        if not bull_unaligned:
            print("None")
    else:
        print("Bullish W+D: none")

    print("-" * 64)

    if bear_results:
        bear_aligned = [item for item in bear_results if item["hourly_signal"] == "BEAR"]
        bear_unaligned = [item for item in bear_results if item["hourly_signal"] != "BEAR"]

        print("Bearish W+D, H1 aligned (below 8 EMA + below PSAR)")
        print(f"{'PAIR':<10} {'SOURCE':<10} {'CLOSE':>12} {'PSAR':>12} {'%RUNNER D':>11}")
        for item in bear_aligned:
            print(
                f"{item['pair']:<10} {item['source']:<10} {item['close']:>12.5f} {item['psar']:>12.5f} {format_runner(item['daily_runner']):>11}"
            )
        if not bear_aligned:
            print("None")

        print("-" * 64)
        print("Bearish W+D, H1 not aligned")
        print(f"{'PAIR':<10} {'SOURCE':<10} {'CLOSE':>12} {'PSAR':>12} {'%RUNNER D':>11}")
        for item in bear_unaligned:
            print(
                f"{item['pair']:<10} {item['source']:<10} {item['close']:>12.5f} {item['psar']:>12.5f} {format_runner(item['daily_runner']):>11}"
            )
        if not bear_unaligned:
            print("None")
    else:
        print("Bearish W+D: none")

    tracking_path = tracking_file_path()
    tracking_data = save_tracking_state(tracking_path, bull_results, bear_results)
    new_flags = tracking_data.get("new_entries", {})
    tg_message = build_telegram_message(bull_results, bear_results, new_flags)
    if tg_message:
        send_telegram_message(tg_message)


if __name__ == "__main__":
    main()
