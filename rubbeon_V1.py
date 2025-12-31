#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rubbeon_V3 logic in Python using TradingView/Yahoo data sources with cache fallback.
"""

import argparse
import importlib
import os
import sys
import time
import json
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed


def install_and_import(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        return importlib.import_module(import_name)
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name)


pd = install_and_import("pandas")
np = install_and_import("numpy")
yf = install_and_import("yfinance")
websocket = install_and_import("websocket-client", "websocket")
from websocket import create_connection
requests = install_and_import("requests")
dotenv = install_and_import("python-dotenv", "dotenv")

EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]
PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM = 0.1, 0.1, 0.2
CACHE_FILE = "market_cache.pkl"
USE_CACHE_DEFAULT = False
H1_CANDLES_DEFAULT = 800
D1_CANDLES_DEFAULT = 500
H1_CANDLES_MAX = 1500
D1_CANDLES_MAX = 900
PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
    "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
    "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPCHF=X",
    "AUDNZD=X", "AUDCAD=X", "AUDCHF=X",
    "NZDCAD=X", "NZDCHF=X",
    "CADCHF=X",
]

dotenv.load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
ASIAN_TZ = "Asia/Tokyo"
ASIAN_SESSION_START_HOUR = 9
ASIAN_SESSION_END_HOUR = 15


def generate_session_id():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"


def fetch_data_tv(pair_yahoo, interval_code, n_candles=200):
    clean_pair = pair_yahoo.replace("=X", "")
    tv_pair = f"OANDA:{clean_pair}"
    tv_interval = {"1d": "D", "1h": "60"}.get(interval_code, "D")
    ws, extracted_df = None, None
    try:
        headers = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}
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
                    end = res.find('"ns":', res.find('"s":['))
                    if start != -1 and end != -1:
                        extract_end = end - 1
                        while res[extract_end] not in [",", "}"]:
                            extract_end -= 1
                        data = json.loads(res[start + 4 : extract_end])
                        fdata = [item["v"] for item in data]
                        extracted_df = pd.DataFrame(fdata, columns=["timestamp", "open", "high", "low", "close", "volume"])
                        extracted_df["datetime"] = pd.to_datetime(extracted_df["timestamp"], unit="s", utc=True)
                        extracted_df.set_index("datetime", inplace=True)
                        extracted_df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}, inplace=True)
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
            return df if df is not None and not df.empty else None
    except Exception:
        return None


def cache_file_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), CACHE_FILE)


def load_market_cache():
    path = cache_file_path()
    if not os.path.exists(path):
        return {}
    try:
        import pickle
        with open(path, "rb") as handle:
            payload = pickle.load(handle)
        return payload.get("data", {}) if isinstance(payload, dict) else {}
    except Exception:
        return {}


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


def fetch_data_smart(pair, interval, n_candles, yahoo_period, use_cache=True):
    if use_cache:
        df = fetch_data_cache(pair, interval, n_candles)
        if df is not None and not df.empty:
            return df, "CACHE"
    df = fetch_data_tv(pair, interval, n_candles=n_candles)
    if df is not None and not df.empty:
        return df, "TV"
    df = fetch_data_yahoo(pair, interval, yahoo_period)
    return (df, "YF") if df is not None and not df.empty else (None, "NA")

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
        return response.json().get("ok", False)
    except Exception:
        return False

def format_pair_name(pair):
    return pair.replace("=X", "")

def build_telegram_message(aligned_items):
    lines = ["RUBBEON"]
    for item in aligned_items:
        icon = "🟢" if item["aligned_state"] == "BULL" else "🔴"
        runner = item.get("daily_change_pct")
        runner_text = "NA" if runner is None or not np.isfinite(runner) else f"{runner:+.2f}%"
        lines.append(f"{icon} {format_pair_name(item['pair'])} ({runner_text})")
    return "\n".join(lines)


def normalize_index(df):
    if df is None or df.empty:
        return df
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df


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
                bull, psar[i], ep, af = False, ep, lows[i], start
            else:
                if highs[i] > ep:
                    ep, af = highs[i], min(af + increment, maximum)
                psar[i] = min(psar[i], lows[i - 1], lows[i - 2]) if i >= 2 else min(psar[i], lows[i - 1])
        else:
            if highs[i] > psar[i]:
                bull, psar[i], ep, af = True, ep, highs[i], start
            else:
                if lows[i] < ep:
                    ep, af = lows[i], min(af + increment, maximum)
                psar[i] = max(psar[i], highs[i - 1], highs[i - 2]) if i >= 2 else max(psar[i], highs[i - 1])
    return psar


def compute_ema_series(close_series, lengths):
    return {length: close_series.ewm(span=length, adjust=False).mean() for length in lengths}


def compute_valuewhen_series(closes, psar):
    bull_vw = np.full(len(closes), np.nan)
    bear_vw = np.full(len(closes), np.nan)
    last_bull = np.nan
    last_bear = np.nan
    for i in range(1, len(closes)):
        bull_cross = closes[i] > psar[i] and closes[i - 1] <= psar[i - 1]
        bear_cross = closes[i] < psar[i] and closes[i - 1] >= psar[i - 1]
        if bull_cross:
            last_bull = psar[i]
        if bear_cross:
            last_bear = psar[i]
        bull_vw[i] = last_bull
        bear_vw[i] = last_bear
    return bull_vw, bear_vw


def compute_asian_session_state(df_h1):
    if df_h1 is None or df_h1.empty:
        return "NONE", np.nan, np.nan, np.nan, None
    try:
        local_index = df_h1.index.tz_convert(ASIAN_TZ)
    except Exception:
        local_index = df_h1.index.tz_localize("UTC").tz_convert(ASIAN_TZ)
    hours = local_index.hour
    session_mask = (hours >= ASIAN_SESSION_START_HOUR) & (hours < ASIAN_SESSION_END_HOUR)
    session_df = df_h1.loc[session_mask].copy()
    if session_df.empty:
        return "NONE", np.nan, np.nan, np.nan, None
    session_df["local_date"] = local_index[session_mask].date
    last_date = session_df["local_date"].iloc[-1]
    last_session = session_df[session_df["local_date"] == last_date]
    asian_high = last_session["High"].max()
    asian_low = last_session["Low"].min()
    asian_mid = (asian_high + asian_low) / 2.0
    close_now = df_h1["Close"].iloc[-1]
    if close_now > asian_mid:
        asia_state = "BULL"
    elif close_now < asian_mid:
        asia_state = "BEAR"
    else:
        asia_state = "NEUTRE"
    return asia_state, asian_mid, asian_high, asian_low, last_date


def analyze_pair(pair, h1_candles=H1_CANDLES_DEFAULT, d1_candles=D1_CANDLES_DEFAULT, use_cache=True):
    df_h1, source_h1 = fetch_data_smart(pair, "1h", n_candles=h1_candles, yahoo_period="2mo", use_cache=use_cache)
    df_d, source_d = fetch_data_smart(pair, "1d", n_candles=d1_candles, yahoo_period="2y", use_cache=use_cache)

    if df_h1 is None or df_d is None:
        return None

    df_h1 = normalize_index(df_h1)
    df_d = normalize_index(df_d)

    if source_d == "CACHE":
        df_d_fresh, source_d_fresh = fetch_data_smart(pair, "1d", n_candles=d1_candles, yahoo_period="2y", use_cache=False)
        df_d_fresh = normalize_index(df_d_fresh)
        if df_d_fresh is not None and not df_d_fresh.empty:
            df_d = df_d_fresh
            source_d = source_d_fresh

    if len(df_h1) < max(EMA_LENGTHS) + 3 or len(df_d) < max(EMA_LENGTHS) + 2:
        h1_retry = min(h1_candles * 2, H1_CANDLES_MAX)
        d1_retry = min(d1_candles * 2, D1_CANDLES_MAX)
        if h1_retry > h1_candles or d1_retry > d1_candles:
            df_h1, source_h1 = fetch_data_smart(pair, "1h", n_candles=h1_retry, yahoo_period="2mo", use_cache=use_cache)
            df_d, source_d = fetch_data_smart(pair, "1d", n_candles=d1_retry, yahoo_period="2y", use_cache=use_cache)
            df_h1 = normalize_index(df_h1)
            df_d = normalize_index(df_d)
            if df_h1 is None or df_d is None:
                return None
            if len(df_h1) < max(EMA_LENGTHS) + 3 or len(df_d) < max(EMA_LENGTHS) + 2:
                df_h1, source_h1 = fetch_data_smart(pair, "1h", n_candles=h1_retry, yahoo_period="2mo", use_cache=False)
                df_d, source_d = fetch_data_smart(pair, "1d", n_candles=d1_retry, yahoo_period="2y", use_cache=False)
                df_h1 = normalize_index(df_h1)
                df_d = normalize_index(df_d)
                if df_h1 is None or df_d is None:
                    return None
                if len(df_h1) < max(EMA_LENGTHS) + 3 or len(df_d) < max(EMA_LENGTHS) + 2:
                    return None
        else:
            return None

    psar_h1 = calculate_psar_series(df_h1, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
    if psar_h1 is None:
        return None

    ema_h1 = compute_ema_series(df_h1["Close"], EMA_LENGTHS)
    ema_d = compute_ema_series(df_d["Close"], EMA_LENGTHS)

    ema_max_h1 = pd.concat([ema_h1[l] for l in EMA_LENGTHS], axis=1).max(axis=1)
    ema_min_h1 = pd.concat([ema_h1[l] for l in EMA_LENGTHS], axis=1).min(axis=1)

    ema_max_d = pd.concat([ema_d[l] for l in EMA_LENGTHS], axis=1).max(axis=1)
    ema_min_d = pd.concat([ema_d[l] for l in EMA_LENGTHS], axis=1).min(axis=1)

    # Préparer les données Daily pour le merge sur H1
    daily_df = pd.DataFrame({
        "close_d": df_d["Close"],
        "open_d": df_d["Open"],
        "ema_max_d": ema_max_d,
        "ema_min_d": ema_min_d,
    })
    daily_df["daily_above_all"] = daily_df["close_d"] > daily_df["ema_max_d"]
    daily_df["daily_below_all"] = daily_df["close_d"] < daily_df["ema_min_d"]

    # Réindexer sur H1 avec forward fill
    daily_on_h1 = daily_df.reindex(df_h1.index, method="ffill")
    
    # Calculer le %RUNNER en temps réel : (Close H1 actuel - Open Daily) / Open Daily * 100
    # Cela donne un résultat beaucoup plus réaliste et proche de TradingView
    daily_on_h1["daily_change_pct"] = (df_h1["Close"] - daily_on_h1["open_d"]) / daily_on_h1["open_d"] * 100.0

    bull_vw0_h1, bear_vw0_h1 = compute_valuewhen_series(df_h1["Close"].values, psar_h1)

    hourly_bear_above_all = bear_vw0_h1 > ema_max_h1.values
    hourly_bull_below_all = bull_vw0_h1 < ema_min_h1.values

    bull_bg = (
        daily_on_h1["daily_above_all"].values
        & hourly_bear_above_all
        & hourly_bull_below_all
        & (daily_on_h1["daily_change_pct"].values > 0)
    )
    bear_bg = (
        daily_on_h1["daily_below_all"].values
        & hourly_bear_above_all
        & hourly_bull_below_all
        & (daily_on_h1["daily_change_pct"].values < 0)
    )

    last_bg_state = 0
    bear_min_since = np.nan
    bull_max_since = np.nan
    for i in range(len(df_h1)):
        if bull_bg[i]:
            last_bg_state = 1
            bull_max_since = bull_vw0_h1[i]
        elif bear_bg[i]:
            last_bg_state = -1
            bear_min_since = bear_vw0_h1[i]

        if last_bg_state == -1 and not np.isnan(bear_vw0_h1[i]):
            bear_min_since = bear_vw0_h1[i] if np.isnan(bear_min_since) else min(bear_min_since, bear_vw0_h1[i])
        if last_bg_state == 1 and not np.isnan(bull_vw0_h1[i]):
            bull_max_since = bull_vw0_h1[i] if np.isnan(bull_max_since) else max(bull_max_since, bull_vw0_h1[i])

    close_now = df_h1["Close"].iloc[-1]
    mom_state = "NONE"
    if last_bg_state == -1 and not np.isnan(bear_min_since):
        mom_state = "BEAR" if close_now < bear_min_since else "BULL"
    elif last_bg_state == 1 and not np.isnan(bull_max_since):
        mom_state = "BULL" if close_now > bull_max_since else "BEAR"

    asia_state, asian_mid, asian_high, asian_low, asian_date = compute_asian_session_state(df_h1)

    aligned_state = None
    daily_change_last = daily_on_h1["daily_change_pct"].iloc[-1]
    runner_ok = np.isfinite(daily_change_last) and abs(daily_change_last) > 0.1
    if bool(bull_bg[-1]) and mom_state == "BULL" and daily_change_last > 0 and runner_ok and asia_state == "BULL":
        aligned_state = "BULL"
    elif bool(bear_bg[-1]) and mom_state == "BEAR" and daily_change_last < 0 and runner_ok and asia_state == "BEAR":
        aligned_state = "BEAR"

    return {
        "pair": pair,
        "source": f"H1={source_h1} D1={source_d}",
        "close_now": close_now,
        "daily_change_pct": daily_on_h1["daily_change_pct"].iloc[-1],
        "daily_above_all": bool(daily_on_h1["daily_above_all"].iloc[-1]),
        "daily_below_all": bool(daily_on_h1["daily_below_all"].iloc[-1]),
        "bear_vw0_h1": bear_vw0_h1[-1],
        "bull_vw0_h1": bull_vw0_h1[-1],
        "ema_max_h1": ema_max_h1.iloc[-1],
        "ema_min_h1": ema_min_h1.iloc[-1],
        "bull_bg": bool(bull_bg[-1]),
        "bear_bg": bool(bear_bg[-1]),
        "last_bg_state": last_bg_state,
        "bear_min_since": bear_min_since,
        "bull_max_since": bull_max_since,
        "mom_state": mom_state,
        "asia_state": asia_state,
        "asian_mid": asian_mid,
        "asian_high": asian_high,
        "asian_low": asian_low,
        "asian_date": asian_date,
        "aligned_state": aligned_state,
    }

def print_result(result):
    print("=" * 60)
    print(f"Pair: {result['pair']}")
    print(f"Source: {result['source']}")
    print(f"Close (H1): {result['close_now']:.5f}")
    print(f"Daily % change: {result['daily_change_pct']:.2f}%")
    print(f"Daily above all EMA: {result['daily_above_all']}")
    print(f"Daily below all EMA: {result['daily_below_all']}")
    print(f"bear_vw0_H1: {result['bear_vw0_h1']}")
    print(f"bull_vw0_H1: {result['bull_vw0_h1']}")
    print(f"EMA max H1: {result['ema_max_h1']:.5f}")
    print(f"EMA min H1: {result['ema_min_h1']:.5f}")
    print(f"BG bullish: {result['bull_bg']}")
    print(f"BG bearish: {result['bear_bg']}")
    print(f"Last BG state: {result['last_bg_state']}")
    print(f"bear_min_since: {result['bear_min_since']}")
    print(f"bull_max_since: {result['bull_max_since']}")
    print(f"MOM: {result['mom_state']}")
    print(f"ASIA: {result['asia_state']} (mid={result['asian_mid']}, high={result['asian_high']}, low={result['asian_low']})")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Rubbeon_V3 analysis (H1/D1) with TV/YF data sources.")
    parser.add_argument("--pair", help="Yahoo pair symbol, e.g. EURUSD=X")
    parser.add_argument("--cache", action="store_true", default=USE_CACHE_DEFAULT, help="Enable cache usage (default: False)")
    parser.add_argument("--h1-candles", type=int, default=H1_CANDLES_DEFAULT, help="Number of H1 candles to fetch")
    parser.add_argument("--d1-candles", type=int, default=D1_CANDLES_DEFAULT, help="Number of D1 candles to fetch")
    parser.add_argument("--workers", type=int, default=6, help="Max workers for multi-pair mode")
    args = parser.parse_args()

    pairs = [args.pair] if args.pair else PAIRS
    use_cache = args.cache
    results = []
    errors = []
    start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(
                analyze_pair,
                pair,
                h1_candles=args.h1_candles,
                d1_candles=args.d1_candles,
                use_cache=use_cache,
            ): pair
            for pair in pairs
        }
        for future in as_completed(future_map):
            pair = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                errors.append((pair, str(exc)))
                continue
            if result:
                results.append(result)
            else:
                errors.append((pair, "No data or insufficient history"))

    results.sort(key=lambda x: x["pair"])
    for result in results:
        print_result(result)

    print(f"Pairs analyzed: {len(pairs)} | OK: {len(results)} | Errors: {len(errors)}")
    print(f"Elapsed: {time.time() - start:.2f}s")
    aligned = [r for r in results if r["aligned_state"]]
    if aligned:
        print("Aligned (BG + MOM + Daily % + ASIA):")
        for item in aligned:
            print(f"- {item['pair']}: {item['aligned_state']}")
        tg_message = build_telegram_message(aligned)
        if tg_message:
            sent = send_telegram_message(tg_message)
            if sent:
                print("Telegram: message sent.")
            else:
                print("Telegram: not sent (missing token/chat id or error).")
    if errors:
        print("Errors:")
        for pair, message in errors:
            print(f"- {pair}: {message}")


if __name__ == "__main__":
    main()
