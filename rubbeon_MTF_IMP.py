#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rubbeon_MTF_IMP logic in Python using TradingView socket data.
Replicates logic from rubbeon_MTF_IMP.pine
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
websocket = install_and_import("websocket-client", "websocket")
from websocket import create_connection
requests = install_and_import("requests")
dotenv = install_and_import("python-dotenv", "dotenv")
colorama = install_and_import("colorama")
from colorama import Fore, Style, init
init(autoreset=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# PSAR Settings from Pine Script
PSAR_START = 0.1
PSAR_INCREMENT = 0.1
PSAR_MAXIMUM = 0.2

# Candle limits
H1_CANDLES_DEFAULT = 1000
D1_CANDLES_DEFAULT = 365
W1_CANDLES_DEFAULT = 100

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
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKING_PATH = os.path.join(SCRIPT_DIR, "rubbeon_MTF_IMP_state.json")

# =============================================================================
# TRADINGVIEW DATA FETCHING
# =============================================================================

def generate_session_id():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))

def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"

def fetch_data_tv(pair_yahoo, interval_code, n_candles=200):
    """
    Fetches data from TradingView via WebSocket.
    interval_code: '1h', '1d', '1w'
    """
    clean_pair = pair_yahoo.replace("=X", "")
    tv_pair = f"OANDA:{clean_pair}"
    
    # Map interval codes to TradingView format
    tv_interval = {
        "1h": "60",
        "1d": "D",
        "1w": "W"
    }.get(interval_code, "D")
    
    ws = None
    extracted_df = None
    
    try:
        headers = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}
        ws = create_connection("wss://prodata.tradingview.com/socket.io/websocket", header=headers, timeout=10)
        session_id = generate_session_id()
        
        ws.send(create_message("chart_create_session", [session_id, ""]))
        ws.send(create_message("resolve_symbol", [session_id, "sds_sym_1", f"={{\"symbol\":\"{tv_pair}\",\"adjustment\":\"splits\",\"session\":\"regular\"}}"]))
        ws.send(create_message("create_series", [session_id, "sds_1", "s1", "sds_sym_1", tv_interval, n_candles, ""]))
        
        start_t = time.time()
        while time.time() - start_t < 10:
            try:
                res = ws.recv()
                
                # Check for series data
                if '"s":[' in res:
                    start = res.find('"s":[')
                    # Find the end of the JSON object that contains "s":[...]
                    # Looking for "ns":... which usually follows "s":... in the packet
                    end = res.find('"ns":', res.find('"s":['))
                    
                    if start != -1 and end != -1:
                        # Backtrack from "ns" to find the closing bracket/brace
                        extract_end = end - 1
                        while res[extract_end] not in [",", "}"]:
                            extract_end -= 1
                            
                        json_str = res[start + 4 : extract_end]
                        data = json.loads(json_str)
                        
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
            except:
                pass
        return None

def normalize_index(df):
    if df is None or df.empty:
        return df
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df

# =============================================================================
# INDICATOR LOGIC
# =============================================================================

def calculate_psar(df, start, increment, maximum):
    """
    Calculates Parabolic SAR.
    Returns a Series with the same index as df.
    """
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
                # Constrain PSAR
                psar[i] = min(psar[i], lows[i - 1], lows[i - 2]) if i >= 2 else min(psar[i], lows[i - 1])
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
                # Constrain PSAR
                psar[i] = max(psar[i], highs[i - 1], highs[i - 2]) if i >= 2 else max(psar[i], highs[i - 1])
                
    return pd.Series(psar, index=df.index)

def calculate_imp_state(df, psar_series):
    """
    Calculates the IMP logic (Bull/Bear tracked important levels).
    Returns (tracked_bull_important, tracked_bear_important) for the last bar.
    """
    closes = df["Close"].values
    psar = psar_series.values
    
    n = len(df)
    
    # State variables
    last_bull_level = np.nan
    last_bear_level = np.nan
    
    tracked_bull_level = np.nan
    tracked_bear_level = np.nan
    
    tracked_bull_important = False
    tracked_bear_important = False
    
    # Iterate through history to build state
    for i in range(1, n):
        close = closes[i]
        prev_close = closes[i-1]
        p = psar[i]
        prev_p = psar[i-1]
        
        # Crossover: Close > PSAR now, Close <= PSAR before
        bull_cross = (close > p) and (prev_close <= prev_p)
        
        # Crossunder: Close < PSAR now, Close >= PSAR before
        bear_cross = (close < p) and (prev_close >= prev_p)
        
        # NOTE: Pine's valuewhen(crossover(close, sar), sar, 0)
        # On the bar of the crossover, the SAR value used is the one OF THAT BAR.
        # But wait, ta.sar() calculates the stop for the CURRENT bar based on PREVIOUS bars.
        # So psar[i] is valid.
        
        if bull_cross:
            # bullLevel in Pine is ta.sar() at crossover
            bullLevel = p 
            last_bull_level = bullLevel
            tracked_bull_level = bullLevel
            tracked_bull_important = False
            
        if bear_cross:
            # bearLevel in Pine is ta.sar() at crossunder
            bearLevel = p
            last_bear_level = bearLevel
            tracked_bear_level = bearLevel
            tracked_bear_important = False
            
        # Importance Logic
        # if not tracked_bear_important and not na(tracked_bear_level) and not na(last_bull_level) and close < last_bull_level
        if (not tracked_bear_important) and (not np.isnan(tracked_bear_level)) and (not np.isnan(last_bull_level)):
            if close < last_bull_level:
                tracked_bear_important = True
                
        # if not tracked_bull_important and not na(tracked_bull_level) and not na(last_bear_level) and close > last_bear_level
        if (not tracked_bull_important) and (not np.isnan(tracked_bull_level)) and (not np.isnan(last_bear_level)):
            if close > last_bear_level:
                tracked_bull_important = True
                
    return tracked_bull_important, tracked_bear_important

def analyze_pair_mtf(pair):
    """
    Analyzes a single pair across H1, D1, W1.
    """
    # Fetch Data
    df_h1 = fetch_data_tv(pair, "1h", n_candles=H1_CANDLES_DEFAULT)
    df_d = fetch_data_tv(pair, "1d", n_candles=D1_CANDLES_DEFAULT)
    df_w = fetch_data_tv(pair, "1w", n_candles=W1_CANDLES_DEFAULT)
    
    if df_h1 is None or df_d is None or df_w is None:
        return None
        
    df_h1 = normalize_index(df_h1)
    df_d = normalize_index(df_d)
    df_w = normalize_index(df_w)
    
    # Calculate PSAR
    psar_h1 = calculate_psar(df_h1, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
    psar_d = calculate_psar(df_d, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
    psar_w = calculate_psar(df_w, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
    
    if psar_h1 is None or psar_d is None or psar_w is None:
        return None
        
    # Calculate IMP States
    h_bull_imp, h_bear_imp = calculate_imp_state(df_h1, psar_h1)
    d_bull_imp, d_bear_imp = calculate_imp_state(df_d, psar_d)
    w_bull_imp, w_bear_imp = calculate_imp_state(df_w, psar_w)
    
    # Conflicts
    h_conflict = h_bull_imp and h_bear_imp
    d_conflict = d_bull_imp and d_bear_imp
    w_conflict = w_bull_imp and w_bear_imp
    
    # Effective Status per Timeframe
    # Hourly
    h_status = "NEUTRE"
    if h_bull_imp and not h_conflict:
        h_status = "BULL"
    elif h_bear_imp and not h_conflict:
        h_status = "BEAR"
        
    # Daily
    d_status = "NEUTRE"
    if d_bull_imp and not d_conflict:
        d_status = "BULL"
    elif d_bear_imp and not d_conflict:
        d_status = "BEAR"
        
    # Weekly
    w_status = "NEUTRE"
    if w_bull_imp and not w_conflict:
        w_status = "BULL"
    elif w_bear_imp and not w_conflict:
        w_status = "BEAR"
        
    # Global Status Logic
    # bool isGlobalBull = (d_tracked_bull_important and not d_conflict) and (w_tracked_bull_important and not w_conflict)
    # bool isGlobalBear = (d_tracked_bear_important and not d_conflict) and (w_tracked_bear_important and not w_conflict)
    
    is_global_bull = (d_status == "BULL") and (w_status == "BULL")
    is_global_bear = (d_status == "BEAR") and (w_status == "BEAR")
    
    global_status = "NEUTRE"
    if is_global_bull:
        global_status = "BULL"
    elif is_global_bear:
        global_status = "BEAR"
        
    return {
        "pair": pair,
        "h_status": h_status,
        "d_status": d_status,
        "w_status": w_status,
        "global_status": global_status
    }

# =============================================================================
# MAIN
# =============================================================================

def print_colored_result(res):
    pair = res["pair"].replace("=X", "")
    
    def get_color(status):
        if status == "BULL":
            return Fore.GREEN
        elif status == "BEAR":
            return Fore.RED
        else:
            return Fore.WHITE

    h_col = get_color(res["h_status"])
    d_col = get_color(res["d_status"])
    w_col = get_color(res["w_status"])
    g_col = get_color(res["global_status"])
    
    print(f"{Fore.CYAN}{pair:<10} "
          f"| H1: {h_col}{res['h_status']:<6} {Style.RESET_ALL} "
          f"| D1: {d_col}{res['d_status']:<6} {Style.RESET_ALL} "
          f"| W1: {w_col}{res['w_status']:<6} {Style.RESET_ALL} "
          f"--> GLOBAL: {g_col}{res['global_status']}{Style.RESET_ALL}")

def send_telegram_summary(results):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    def load_tracking_state(path):
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                return data
        except Exception:
            return {}
        return {}

    def save_tracking_state(path, statuses):
        data = {
            "updated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "statuses": statuses,
        }
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2)
        return data

    def status_icon(status):
        if status == "BULL":
            return "🟢"
        if status == "BEAR":
            return "🔴"
        return "⚪"

    previous = load_tracking_state(TRACKING_PATH)
    prev_statuses = previous.get("statuses", {}) if isinstance(previous, dict) else {}

    current_statuses = {}
    changed_lines = []
    stable_lines = []
    for r in results:
        pair = r["pair"].replace("=X", "")
        status = r["global_status"]
        current_statuses[pair] = status

        prev_status = prev_statuses.get(pair)
        if status in ["BULL", "BEAR"]:
            stable_lines.append(f"{status_icon(status)} {pair}")
        if prev_status in ["BULL", "BEAR"] and prev_status != status:
            changed_lines.append(f"{status_icon(status)} {pair} -> {status}")

    lines = list(stable_lines)
    if changed_lines:
        if lines:
            lines.append("")
            lines.append("ALERTE")
        lines.extend(changed_lines)

    if not lines:
        save_tracking_state(TRACKING_PATH, current_statuses)
        return

    message = "\n".join(lines)

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=10)
        print("Telegram notification sent.")
    except Exception as e:
        print(f"Failed to send Telegram: {e}")
    finally:
        save_tracking_state(TRACKING_PATH, current_statuses)

def main():
    print("Starting Rubbeon MTF IMP Analysis...")
    print(f"Pairs: {len(PAIRS)}")
    
    results = []
    
    # Use ThreadPoolExecutor for parallel fetching
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_map = {executor.submit(analyze_pair_mtf, pair): pair for pair in PAIRS}
        
        for future in as_completed(future_map):
            pair = future_map[future]
            try:
                res = future.result()
                if res:
                    results.append(res)
                    print_colored_result(res)
                else:
                    print(f"{Fore.YELLOW}No data for {pair}{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error analyzing {pair}: {e}{Style.RESET_ALL}")

    # Sort results by pair name
    results.sort(key=lambda x: x["pair"])
    
    # Send Telegram
    send_telegram_summary(results)

if __name__ == "__main__":
    main()
