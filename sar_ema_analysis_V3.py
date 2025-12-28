#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily SAR + EMA Scanner
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
import pickle
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
    "USDJPY=X", "USDCHF=X",
    "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
    "EURCHF=X", "GBPCHF=X", "AUDCHF=X", "NZDCHF=X", "CADCHF=X",
]

EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]

PSAR_START = 0.1
PSAR_INCREMENT = 0.1
PSAR_MAXIMUM = 0.2

TV_CANDLES = 200
MAX_WORKERS = 8
FORCE_TV_ONLY = True
CACHE_FILE = "market_cache.pkl"
USE_CACHE = True
MARKET_CACHE = None
MARKET_CACHE_TIMESTAMP = None
REFRESH_CACHE = True
CACHE_H1_CANDLES = 500
CACHE_D1_CANDLES = 300
CACHE_W1_CANDLES = 150
CACHE_MAX_WORKERS = 5

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
        tv_interval = "D"

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

    if FORCE_TV_ONLY:
        return None, "NA"

    df = fetch_data_yahoo(pair, interval, yahoo_period)
    if df is not None and not df.empty:
        return df, "YF"

    return None, "NA"


def load_market_cache():
    global MARKET_CACHE
    global MARKET_CACHE_TIMESTAMP
    if not USE_CACHE:
        return None
    if MARKET_CACHE is not None:
        return MARKET_CACHE
    try:
        with open(CACHE_FILE, "rb") as f:
            cache_obj = pickle.load(f)
        if isinstance(cache_obj, dict) and "data" in cache_obj:
            MARKET_CACHE = cache_obj["data"]
            MARKET_CACHE_TIMESTAMP = cache_obj.get("timestamp")
        else:
            MARKET_CACHE = cache_obj
    except FileNotFoundError:
        MARKET_CACHE = {}
    except Exception:
        MARKET_CACHE = {}
    return MARKET_CACHE


def get_cached_df(pair, interval):
    cache = load_market_cache()
    if not cache:
        return None
    pair_data = cache.get(pair)
    if not pair_data:
        return None
    df = pair_data.get(interval)
    if df is None or df.empty:
        return None
    return df


def fetch_tv_batch(pair_yahoo):
    data_map = {}
    df_h1 = fetch_data_tv(pair_yahoo, "1h", n_candles=CACHE_H1_CANDLES)
    if df_h1 is not None:
        data_map["1h"] = df_h1

    df_d1 = fetch_data_tv(pair_yahoo, "1d", n_candles=CACHE_D1_CANDLES)
    if df_d1 is not None:
        data_map["1d"] = df_d1

    df_w1 = fetch_data_tv(pair_yahoo, "1wk", n_candles=CACHE_W1_CANDLES)
    if df_w1 is not None:
        data_map["1wk"] = df_w1

    if data_map:
        return pair_yahoo, data_map
    return None


def refresh_market_cache():
    cache_data = {}
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=CACHE_MAX_WORKERS) as executor:
        future_to_pair = {executor.submit(fetch_tv_batch, pair): pair for pair in PAIRS}
        for future in as_completed(future_to_pair):
            result = future.result()
            if result:
                pair, data = result
                cache_data[pair] = data

    cache_with_meta = {
        "timestamp": time.time(),
        "data": cache_data,
    }
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(cache_with_meta, f)
    return time.time() - start_time


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


def calculate_psar_cross_levels(df, start, increment, maximum):
    if df is None or len(df) < 2:
        return None

    psar = calculate_psar_series(df, start, increment, maximum)
    if psar is None:
        return None

    closes = df["Close"].values
    bull_indices = []
    bear_indices = []

    for i in range(1, len(df)):
        if closes[i - 1] <= psar[i - 1] and closes[i] > psar[i]:
            bull_indices.append(i)
        if closes[i - 1] >= psar[i - 1] and closes[i] < psar[i]:
            bear_indices.append(i)

    def pick(indices, n):
        if len(indices) > n:
            return psar[indices[-1 - n]]
        return None

    return {
        "bull_vw0": pick(bull_indices, 0),
        "bull_vw1": pick(bull_indices, 1),
        "bull_vw2": pick(bull_indices, 2),
        "bear_vw0": pick(bear_indices, 0),
        "bear_vw1": pick(bear_indices, 1),
        "bear_vw2": pick(bear_indices, 2),
    }


def calculate_pct_runner_daily(df):
    if df is None or len(df) < 1:
        return None
    open_price = df["Open"].iloc[-1]
    close_price = df["Close"].iloc[-1]
    if open_price > 0:
        return ((close_price - open_price) / open_price) * 100.0
    return None


def get_ema_band_series(close):
    ema_frames = [close.ewm(span=length, adjust=False).mean() for length in EMA_LENGTHS]
    all_emas = pd.concat(ema_frames, axis=1)
    ema_low = all_emas.min(axis=1)
    ema_high = all_emas.max(axis=1)
    return ema_low, ema_high


def get_last_psar_cross_series(psar, closes):
    last_bull = [None] * len(psar)
    last_bear = [None] * len(psar)
    current_bull = None
    current_bear = None

    for i in range(len(psar)):
        if i > 0:
            if closes[i - 1] <= psar[i - 1] and closes[i] > psar[i]:
                current_bull = psar[i]
            if closes[i - 1] >= psar[i - 1] and closes[i] < psar[i]:
                current_bear = psar[i]
        last_bull[i] = current_bull
        last_bear[i] = current_bear

    return last_bull, last_bear


def compute_setup_state_1h(df, trend_signal):
    if df is None or df.empty:
        return None
    if "Close" not in df.columns:
        return None

    psar = calculate_psar_series(df, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
    if psar is None:
        return None

    close = df["Close"]
    ema_low, ema_high = get_ema_band_series(close)
    last_bull, last_bear = get_last_psar_cross_series(psar, close.values)

    setup_valid = []
    for i in range(len(df)):
        bull_vw0 = last_bull[i]
        bear_vw0 = last_bear[i]
        if bull_vw0 is None or bear_vw0 is None:
            setup_valid.append(False)
            continue
        if trend_signal == "BULL":
            setup_valid.append(bull_vw0 < ema_low.iat[i] and bear_vw0 > ema_high.iat[i])
        else:
            setup_valid.append(bull_vw0 > ema_high.iat[i] and bear_vw0 < ema_low.iat[i])

    setup_valid = np.array(setup_valid, dtype=bool)
    last_close = float(close.iloc[-1])

    if setup_valid[-1]:
        bull_vw0 = last_bull[-1]
        bear_vw0 = last_bear[-1]
        if trend_signal == "BULL":
            is_break = last_close > bear_vw0
        else:
            is_break = last_close < bull_vw0
        return {
            "status": "ACTIVE",
            "bull_vw0": bull_vw0,
            "bear_vw0": bear_vw0,
            "is_break": is_break,
            "last_close_1h": last_close,
        }

    valid_indices = np.flatnonzero(setup_valid)
    if len(valid_indices) == 0:
        return None

    last_valid_index = int(valid_indices[-1])
    last_bull_now = last_bull[-1]
    last_bear_now = last_bear[-1]
    if last_bull_now is None or last_bear_now is None:
        return None

    ema_low_last = ema_low.iat[-1]
    ema_high_last = ema_high.iat[-1]
    if trend_signal == "BULL":
        lost = last_bull_now < ema_low_last and last_bear_now < ema_low_last
    else:
        lost = last_bull_now > ema_high_last and last_bear_now > ema_high_last
    if not lost:
        return None

    return {
        "status": "LOST",
        "bull_vw0": last_bull[last_valid_index],
        "bear_vw0": last_bear[last_valid_index],
        "is_break": None,
        "last_close_1h": last_close,
    }


def format_runner(value):
    if value is None:
        return "NA"
    return f"{value:+.2f}%"


def format_level(value):
    if value is None:
        return "NA"
    return f"{value:.5f}"


def format_price(value):
    if value is None:
        return "NA"
    return f"{value:.5f}"


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


def runner_ball(value):
    if value is None or value == 0:
        return "⚪"
    return "🟢" if value > 0 else "🔴"


def format_alignment_ball(item, main_signal):
    main_ball = "🟢" if main_signal == "BULL" else "🔴"
    return main_ball + runner_ball(item.get("daily_runner"))


def build_telegram_message(bull_results, bear_results):
    lines = []
    if bull_results:
        lines.append("BULLISH D")
        for item in sorted(bull_results, key=lambda x: x["pair"]):
            balls = format_alignment_ball(item, "BULL")
            lines.append(f"{balls} {item['pair']}")
    if bear_results:
        if lines:
            lines.append("")
        lines.append("BEARISH D")
        for item in sorted(bear_results, key=lambda x: x["pair"]):
            balls = format_alignment_ball(item, "BEAR")
            lines.append(f"{balls} {item['pair']}")
    return "\n".join(lines)


def evaluate_conditions(df, use_psar=True):
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

    cross_levels = calculate_psar_cross_levels(df, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
    bull_vw0 = cross_levels["bull_vw0"] if cross_levels else None
    bear_vw0 = cross_levels["bear_vw0"] if cross_levels else None

    psar = None
    above_psar = True
    below_psar = True
    if use_psar:
        psar = calculate_psar_series(df, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        if psar is None:
            return None
        above_psar = close_last > psar[-1]
        below_psar = close_last < psar[-1]

    if above_emas and above_psar:
        return {
            "signal": "BULL",
            "close": close_last,
            "psar": psar[-1] if psar is not None else None,
            "bull_vw0": bull_vw0,
            "bear_vw0": bear_vw0,
        }

    if below_emas and below_psar:
        return {
            "signal": "BEAR",
            "close": close_last,
            "psar": psar[-1] if psar is not None else None,
            "bull_vw0": bull_vw0,
            "bear_vw0": bear_vw0,
        }

    return {
        "signal": "NEUTRAL",
        "close": close_last,
        "psar": psar[-1] if psar is not None else None,
        "bull_vw0": bull_vw0,
        "bear_vw0": bear_vw0,
    }


def analyze_pair(pair):
    df_w = get_cached_df(pair, "1wk")
    df_d = get_cached_df(pair, "1d")
    df_h = get_cached_df(pair, "1h")
    source_w = "CACHE" if df_w is not None else None
    source_d = "CACHE" if df_d is not None else None
    source_h = "CACHE" if df_h is not None else None
    if df_w is None:
        df_w, source_w = fetch_data_smart(pair, "1wk", n_candles=CACHE_W1_CANDLES, yahoo_period="3y")
    if df_d is None:
        df_d, source_d = fetch_data_smart(pair, "1d", n_candles=TV_CANDLES, yahoo_period="2y")
    if df_h is None:
        df_h, source_h = fetch_data_smart(pair, "1h", n_candles=TV_CANDLES, yahoo_period="2mo")

    weekly = evaluate_conditions(df_w, use_psar=False)
    daily = evaluate_conditions(df_d, use_psar=False)
    hourly = evaluate_conditions(df_h, use_psar=True)
    daily_runner = calculate_pct_runner_daily(df_d)

    if weekly is None or daily is None or hourly is None:
        return None

    if weekly["signal"] not in ("BULL", "BEAR"):
        return None
    if daily["signal"] not in ("BULL", "BEAR"):
        return None
    if weekly["signal"] != daily["signal"]:
        return None

    setup_state = compute_setup_state_1h(df_h, daily["signal"])
    result = {
        "pair": pair.replace("=X", ""),
        "source": f"{source_w}/{source_d}/{source_h}",
        "close": daily["close"],
        "psar": daily["psar"],
        "signal": daily["signal"],
        "hourly_signal": hourly["signal"],
        "daily_runner": daily_runner,
        "status": None,
    }
    if setup_state is None:
        return result
    result.update(
        {
            "bull_vw0": setup_state["bull_vw0"],
            "bear_vw0": setup_state["bear_vw0"],
            "is_break": setup_state["is_break"],
            "last_close_1h": setup_state["last_close_1h"],
            "status": setup_state["status"],
        }
    )
    return result

    return None


def main():
    if REFRESH_CACHE:
        elapsed = refresh_market_cache()
        print(f"Cache refreshed in {elapsed:.2f}s")
        print("-" * 64)

    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_pair, pair): pair for pair in PAIRS}
        for future in as_completed(futures):
            result = future.result()
            if result:
                if result["status"] == "ACTIVE":
                    all_results.append(result)

    all_results.sort(key=lambda x: x["pair"])

    print("Weekly+Daily SAR + EMA (8) Scanner")
    print(f"PSAR: start={PSAR_START}, increment={PSAR_INCREMENT}, maximum={PSAR_MAXIMUM}")
    if MARKET_CACHE_TIMESTAMP:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(MARKET_CACHE_TIMESTAMP))
        print(f"Cache timestamp: {ts}")
    print(
        f"Pairs scanned: {len(PAIRS)} | Matches: {len(all_results)}"
    )
    print("-" * 64)

    if all_results:
        print("Pairs with BULL_VW0 below EMA and BEAR_VW0 above EMA (1H)")
        print(f"{'PAIR':<10} {'SOURCE':<10} {'DAILY':>8} {'CLOSE':>12} {'PSAR':>12} {'H1_CLOSE':>12} {'BULL_VW0':>12} {'BEAR_VW0':>12} {'%RUNNER D':>11}")
        for item in all_results:
            print(
                f"{item['pair']:<10} {item['source']:<10} {item['signal']:>8} {format_price(item['close']):>12} {format_price(item['psar']):>12} {format_price(item['last_close_1h']):>12} {format_level(item['bull_vw0']):>12} {format_level(item['bear_vw0']):>12} {format_runner(item['daily_runner']):>11}"
            )
    else:
        print("No pairs match the 1H EMA band setup.")

    bull_active = [item for item in all_results if item["signal"] == "BULL"]
    bear_active = [item for item in all_results if item["signal"] == "BEAR"]
    tg_message = build_telegram_message(bull_active, bear_active)
    if tg_message:
        send_telegram_message(tg_message)


if __name__ == "__main__":
    main()
