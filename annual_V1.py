#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Annual_V1 - Analyse annuelle des paires de devises
Compare le prix actuel avec l'open annuel et detecte les retracements via PSAR.
"""

import argparse
import importlib
import os
import sys
import time
import json
import random
import string
from datetime import datetime
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
websocket = install_and_import("websocket-client", "websocket")
from websocket import create_connection
requests = install_and_import("requests")
dotenv = install_and_import("python-dotenv", "dotenv")

PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM = 0.1, 0.1, 0.2
D1_CANDLES_DEFAULT = 500
D1_CANDLES_MAX = 900

# PAIRS = [
#     "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X",
#     "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
#     "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
#     "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPCHF=X",
#     "AUDNZD=X", "AUDCAD=X", "AUDCHF=X",
#     "NZDCAD=X", "NZDCHF=X",
#     "CADCHF=X",
# ]



PAIRS = [
    "USDJPY=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
]


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


def get_annual_open(df_d):
    if df_d is None or df_d.empty:
        return np.nan
    current_year = datetime.now().year
    year_data = df_d[df_d.index.year == current_year]
    if year_data.empty:
        return np.nan
    return year_data["Open"].iloc[0]


def compute_ytd_extremes(df_d, bull_vw, bear_vw):
    if df_d is None or df_d.empty:
        return np.nan, np.nan, np.nan, np.nan
    current_year = datetime.now().year
    year_mask = df_d.index.year == current_year
    if not year_mask.any():
        return np.nan, np.nan, np.nan, np.nan
    bull_vw_ytd = bull_vw[year_mask]
    bear_vw_ytd = bear_vw[year_mask]
    lowest_bull = np.nanmin(bull_vw_ytd) if len(bull_vw_ytd) > 0 else np.nan
    lowest_bear = np.nanmin(bear_vw_ytd) if len(bear_vw_ytd) > 0 else np.nan
    highest_bull = np.nanmax(bull_vw_ytd) if len(bull_vw_ytd) > 0 else np.nan
    highest_bear = np.nanmax(bear_vw_ytd) if len(bear_vw_ytd) > 0 else np.nan
    return lowest_bull, lowest_bear, highest_bull, highest_bear


def determine_state(close_now, annual_open, lowest_bull, lowest_bear, highest_bull, highest_bear):
    if np.isnan(annual_open):
        return "UNKNOWN", "UNKNOWN"
    is_bullish = close_now >= annual_open
    is_bearish = close_now < annual_open
    base_state = "BULLISH" if is_bullish else "BEARISH"
    if is_bearish:
        if not np.isnan(lowest_bull) and not np.isnan(lowest_bear):
            if close_now > lowest_bull and close_now > lowest_bear:
                return base_state, "BEARISH_RETRACEMENT"
    elif is_bullish:
        if not np.isnan(highest_bull) and not np.isnan(highest_bear):
            if close_now < highest_bull and close_now < highest_bear:
                return base_state, "BULLISH_RETRACEMENT"
    return base_state, base_state


def analyze_pair(pair, d1_candles=D1_CANDLES_DEFAULT):
    current_d1 = d1_candles
    while True:
        df_d = fetch_data_tv(pair, "1d", n_candles=current_d1)
        if df_d is None:
            return None
        df_d = normalize_index(df_d)
        if len(df_d) < 10:
            d1_retry = min(current_d1 * 2, D1_CANDLES_MAX)
            if d1_retry == current_d1:
                return None
            current_d1 = d1_retry
            continue
        psar_d = calculate_psar_series(df_d, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        if psar_d is None:
            return None
        bull_vw, bear_vw = compute_valuewhen_series(df_d["Close"].values, psar_d)
        annual_open = get_annual_open(df_d)
        lowest_bull, lowest_bear, highest_bull, highest_bear = compute_ytd_extremes(df_d, bull_vw, bear_vw)
        break
    close_now = df_d["Close"].iloc[-1]
    psar_now = psar_d[-1]
    bull_vw0_now = bull_vw[-1]
    bear_vw0_now = bear_vw[-1]
    base_state, final_state = determine_state(close_now, annual_open, lowest_bull, lowest_bear, highest_bull, highest_bear)
    annual_change_pct = ((close_now - annual_open) / annual_open * 100) if not np.isnan(annual_open) else np.nan
    return {
        "pair": pair,
        "close_now": close_now,
        "annual_open": annual_open,
        "annual_change_pct": annual_change_pct,
        "psar": psar_now,
        "bull_vw0": bull_vw0_now,
        "bear_vw0": bear_vw0_now,
        "lowest_bull_ytd": lowest_bull,
        "lowest_bear_ytd": lowest_bear,
        "highest_bull_ytd": highest_bull,
        "highest_bear_ytd": highest_bear,
        "base_state": base_state,
        "final_state": final_state,
    }


def get_state_icon(final_state):
    icons = {"BULLISH": "G", "BEARISH": "R", "BULLISH_RETRACEMENT": "X", "BEARISH_RETRACEMENT": "X", "UNKNOWN": "?"}
    return icons.get(final_state, "?")


def print_result(result):
    icon = get_state_icon(result["final_state"])
    print("=" * 60)
    print(f"[{icon}] Pair: {format_pair_name(result['pair'])}")
    print(f"Close: {result['close_now']:.5f}")
    ao = result['annual_open']
    print(f"Annual Open: {ao:.5f}" if not np.isnan(ao) else "Annual Open: N/A")
    acp = result['annual_change_pct']
    print(f"Annual Change: {acp:+.2f}%" if not np.isnan(acp) else "Annual Change: N/A")
    print(f"Base State: {result['base_state']} | Final State: {result['final_state']}")
    print("=" * 60)


def build_telegram_message(results):
    lines = ["ANNUAL"]
    sorted_results = sorted(results, key=lambda x: x["annual_change_pct"] if not np.isnan(x["annual_change_pct"]) else 0, reverse=True)
    for r in sorted_results:
        pct = r["annual_change_pct"]
        pct_str = f"{pct:+.2f}%" if not np.isnan(pct) else "N/A"
        final_state = r["final_state"]
        if final_state == "BULLISH":
            icon = "🟢"
        elif final_state == "BEARISH":
            icon = "🔴"
        else:
            icon = "⚪"
        lines.append(f"{icon} {format_pair_name(r['pair'])} ({pct_str})")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Annual_V1 analysis using TradingView socket.")
    parser.add_argument("--pair", help="Yahoo pair symbol, e.g. EURUSD=X")
    parser.add_argument("--d1-candles", type=int, default=D1_CANDLES_DEFAULT)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--telegram", action="store_true", help="Send results to Telegram")
    args = parser.parse_args()
    if os.getenv("GITHUB_ACTIONS", "").lower() == "true":
        args.workers = min(args.workers, 2)
    pairs = [args.pair] if args.pair else PAIRS
    results = []
    errors = []
    start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {executor.submit(analyze_pair, pair, d1_candles=args.d1_candles): pair for pair in pairs}
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
    print(f"\nPairs analyzed: {len(pairs)} | OK: {len(results)} | Errors: {len(errors)}")
    print(f"Elapsed: {time.time() - start:.2f}s")
    if results:
        tg_message = build_telegram_message(results)
        sent = send_telegram_message(tg_message)
        print(f"Telegram: {'sent' if sent else 'not sent'}")
    if errors:
        print("\nErrors:")
        for pair, message in errors:
            print(f"  - {pair}: {message}")


if __name__ == "__main__":
    main()
