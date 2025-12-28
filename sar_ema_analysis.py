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
TRACKING_FILE = "trend_follower.json"
CACHE_FILE = "market_cache.pkl"
CACHE_MAX_AGE_SECONDS = 6 * 60 * 60
TELEGRAM_MIN_ABS_RUNNER = 0.15
REFRESH_CACHE = True
REFRESH_MODE = "incremental"
CACHE_H1_CANDLES = 500
CACHE_D1_CANDLES = 300
CACHE_W1_CANDLES = 150
H1_REFRESH_CANDLES = 24
D1_REFRESH_CANDLES = 5
W1_REFRESH_CANDLES = 4
CACHE_MAX_WORKERS = 5
REFRESH_MIN_SECONDS = 600

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
    df = fetch_data_cache(pair, interval, n_candles)
    if df is not None and not df.empty:
        return df, "CACHE"

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



def filter_for_telegram(items):
    return [
        item
        for item in items
        if item.get("daily_runner") is not None and abs(item["daily_runner"]) > TELEGRAM_MIN_ABS_RUNNER
    ]


def build_telegram_message(bull_results, bear_results, new_flags, exits):
    new_bull = set(new_flags.get("bullish", []))
    new_bear = set(new_flags.get("bearish", []))
    bull_results = filter_for_telegram(bull_results)
    bear_results = filter_for_telegram(bear_results)
    exit_bull = exits.get("bullish", [])
    exit_bear = exits.get("bearish", [])
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
    if exit_bull or exit_bear:
        if lines:
            lines.append("")
        lines.append("EXITS")
        if exit_bull:
            lines.append(f"Exit BULL: {', '.join(sorted(exit_bull))}")
        if exit_bear:
            lines.append(f"Exit BEAR: {', '.join(sorted(exit_bear))}")
    return "\n".join(lines)


def tracking_file_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), TRACKING_FILE)


def cache_file_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), CACHE_FILE)


_CACHE_DATA = None
_CACHE_TIMESTAMP = None


def load_market_cache():
    global _CACHE_DATA
    global _CACHE_TIMESTAMP
    if _CACHE_DATA is not None:
        return _CACHE_DATA
    path = cache_file_path()
    if not os.path.exists(path):
        _CACHE_DATA = {}
        return _CACHE_DATA
    try:
        with open(path, "rb") as handle:
            payload = pickle.load(handle)
        timestamp = payload.get("timestamp") if isinstance(payload, dict) else None
        _CACHE_TIMESTAMP = timestamp
        if timestamp and (time.time() - timestamp) > CACHE_MAX_AGE_SECONDS:
            _CACHE_DATA = {}
            return _CACHE_DATA
        _CACHE_DATA = payload.get("data", {}) if isinstance(payload, dict) else {}
    except Exception:
        _CACHE_DATA = {}
    return _CACHE_DATA


def fetch_data_cache(pair, interval_code, n_candles):
    cache = load_market_cache()
    pair_data = cache.get(pair)
    if not pair_data:
        return None
    df = pair_data.get(interval_code)
    if df is None or df.empty:
        return None
    if n_candles and len(df) > n_candles:
        return df.tail(n_candles)
    return df


def normalize_timestamp(ts):
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def should_refresh(last_index, interval_code):
    if last_index is None:
        return True
    last_index = normalize_timestamp(last_index)
    now = pd.Timestamp.now(tz="UTC")
    if interval_code == "1h":
        return (now - last_index) >= pd.Timedelta(hours=1)
    if interval_code == "1d":
        return now.date() > last_index.date()
    if interval_code == "1wk":
        now_iso = now.isocalendar()
        last_iso = last_index.isocalendar()
        return (now_iso.year, now_iso.week) != (last_iso.year, last_iso.week)
    return True


def merge_frames(existing, new_df, max_candles):
    if new_df is None or new_df.empty:
        return existing
    if existing is None or existing.empty:
        merged = new_df
    else:
        merged = pd.concat([existing, new_df])
        merged = merged[~merged.index.duplicated(keep="last")]
        merged.sort_index(inplace=True)
    if max_candles and len(merged) > max_candles:
        merged = merged.iloc[-max_candles:]
    return merged


def fetch_tv_batch(pair_yahoo, h1_candles, d1_candles, w1_candles):
    data_map = {}
    if h1_candles and h1_candles > 0:
        df_h1 = fetch_data_tv(pair_yahoo, "1h", n_candles=h1_candles)
        if df_h1 is not None:
            data_map["1h"] = df_h1

    if d1_candles and d1_candles > 0:
        df_d1 = fetch_data_tv(pair_yahoo, "1d", n_candles=d1_candles)
        if df_d1 is not None:
            data_map["1d"] = df_d1

    if w1_candles and w1_candles > 0:
        df_w1 = fetch_data_tv(pair_yahoo, "1wk", n_candles=w1_candles)
        if df_w1 is not None:
            data_map["1wk"] = df_w1

    if data_map:
        return pair_yahoo, data_map
    return None


def refresh_market_cache():
    start_time = time.time()
    existing_cache = load_market_cache() or {}
    if REFRESH_MODE == "incremental" and _CACHE_TIMESTAMP:
        if (time.time() - _CACHE_TIMESTAMP) < REFRESH_MIN_SECONDS:
            return 0.0, True

    cache_data = {}
    incremental = REFRESH_MODE == "incremental"
    fetch_specs = {}
    for pair in PAIRS:
        if not incremental or pair not in existing_cache:
            fetch_specs[pair] = (CACHE_H1_CANDLES, CACHE_D1_CANDLES, CACHE_W1_CANDLES)
            continue

        pair_cache = existing_cache.get(pair, {})
        h1_last = pair_cache.get("1h")
        d1_last = pair_cache.get("1d")
        w1_last = pair_cache.get("1wk")
        h1_last_idx = h1_last.index[-1] if h1_last is not None and not h1_last.empty else None
        d1_last_idx = d1_last.index[-1] if d1_last is not None and not d1_last.empty else None
        w1_last_idx = w1_last.index[-1] if w1_last is not None and not w1_last.empty else None

        h1_need = H1_REFRESH_CANDLES if should_refresh(h1_last_idx, "1h") else 0
        d1_need = D1_REFRESH_CANDLES if should_refresh(d1_last_idx, "1d") else 0
        w1_need = W1_REFRESH_CANDLES if should_refresh(w1_last_idx, "1wk") else 0
        fetch_specs[pair] = (h1_need, d1_need, w1_need)

    with ThreadPoolExecutor(max_workers=CACHE_MAX_WORKERS) as executor:
        future_to_pair = {}
        for pair, (h1_candles, d1_candles, w1_candles) in fetch_specs.items():
            if h1_candles == 0 and d1_candles == 0 and w1_candles == 0:
                if pair in existing_cache:
                    cache_data[pair] = existing_cache[pair]
                continue
            future_to_pair[
                executor.submit(fetch_tv_batch, pair, h1_candles, d1_candles, w1_candles)
            ] = pair

        for future in as_completed(future_to_pair):
            pair = future_to_pair[future]
            result = future.result()
            if result:
                _, data = result
                if incremental and pair in existing_cache:
                    merged = {}
                    merged["1h"] = merge_frames(
                        existing_cache[pair].get("1h"), data.get("1h"), CACHE_H1_CANDLES
                    )
                    merged["1d"] = merge_frames(
                        existing_cache[pair].get("1d"), data.get("1d"), CACHE_D1_CANDLES
                    )
                    merged["1wk"] = merge_frames(
                        existing_cache[pair].get("1wk"), data.get("1wk"), CACHE_W1_CANDLES
                    )
                    cache_data[pair] = merged
                else:
                    cache_data[pair] = data
            elif incremental and pair in existing_cache:
                cache_data[pair] = existing_cache[pair]

    cache_with_meta = {
        "timestamp": time.time(),
        "data": cache_data,
    }
    with open(cache_file_path(), "wb") as handle:
        pickle.dump(cache_with_meta, handle)
    return time.time() - start_time, False


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


def get_strength_sets(bull_results, bear_results):
    total_pairs = len(bull_results) + len(bear_results)
    if total_pairs == 0:
        return set(), set(), 0

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

    max_strong = max(strong_counts.values()) if strong_counts else 0
    max_weak = max(weak_counts.values()) if weak_counts else 0
    strongest = {k for k, v in strong_counts.items() if v == max_strong} if max_strong else set()
    weakest = {k for k, v in weak_counts.items() if v == max_weak} if max_weak else set()

    return strongest, weakest, total_pairs


def build_best_trade_lines(bull_results, bear_results):
    strongest, weakest, _ = get_strength_sets(bull_results, bear_results)
    if not strongest or not weakest:
        return []

    results_by_pair = {item["pair"]: item for item in (bull_results + bear_results)}
    available_pairs = [p.replace("=X", "") for p in PAIRS]

    best_pairs = []
    for pair in available_pairs:
        if len(pair) < 6:
            continue
        base = pair[:3]
        quote = pair[3:]
        if (base in strongest and quote in weakest) or (base in weakest and quote in strongest):
            best_pairs.append(pair)

    if not best_pairs:
        return []

    lines = ["BEST TRADE"]
    for pair in sorted(best_pairs):
        item = results_by_pair.get(pair)
        if item:
            balls = format_alignment_ball(item, item["signal"])
        else:
            balls = "⚪⚪"
        lines.append(f"{balls} {pair}")
    return lines


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
    if REFRESH_CACHE:
        elapsed, skipped = refresh_market_cache()
        if skipped:
            print(f"Cache refresh skipped (recent cache < {REFRESH_MIN_SECONDS}s)")
        else:
            print(f"Cache refreshed in {elapsed:.2f}s")
        print("-" * 64)

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
    exits = tracking_data.get("exits", {})
    tg_message = build_telegram_message(bull_results, bear_results, new_flags, exits)
    if tg_message:
        send_telegram_message(tg_message)


if __name__ == "__main__":
    main()
