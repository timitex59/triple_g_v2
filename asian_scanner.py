#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ASIAN STRATEGY SCANNER
======================
Trading Strategy based on:
1. Daily Trend (8 EMA Ribbon + Perfect Alignment + Open vs EMA20 Filter)
2. H1 Asian Session (Deep Retracement)

Asian Hours (Local Time CE(S)T):
- Winter : 02h00 - 07h00
- Summer : 01h00 - 06h00
"""

import sys
import os
import time
import datetime
import pytz
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib
import subprocess
import json
import random
import string

# --- DEPENDENCY MANAGEMENT ---
def install_and_import(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        return importlib.import_module(import_name)
    except ImportError:
        # print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name)

# Load libs
yf = install_and_import("yfinance")
pd = install_and_import("pandas")
np = install_and_import("numpy")
pytz = install_and_import("pytz")
requests = install_and_import("requests")
websocket = install_and_import("websocket-client", "websocket")
from websocket import create_connection

# Import EMA indicators from triple_g_indicators
from triple_g_indicators import calculate_ema_gap_status, calculate_ema_aligned_status

# --- CONFIGURATION ---
EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]

# Load .env variables manually
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(env_path):
    with open(env_path, "r") as f:
        for line in f:
            if line.strip() and not line.startswith("#") and "=" in line:
                key, value = line.strip().split("=", 1)
                if key == "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN = value
                if key == "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID = value

# Forex Pairs List (Yahoo format kept for compatibility, will be converted)
PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
    "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
    "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPCHF=X",
    "AUDNZD=X", "AUDCAD=X", "AUDCHF=X",
    "NZDCAD=X", "NZDCHF=X",
    "CADCHF=X"
]

# --- DATA ENGINE (TV + YAHOO FALLBACK + CACHE) ---
CACHE_FILE = "market_cache.pkl"
CACHE_MAX_AGE_SECONDS = 300  # 5 minutes max
GLOBAL_CACHE = None

def load_global_cache():
    global GLOBAL_CACHE
    if GLOBAL_CACHE is not None: return
    
    if os.path.exists(CACHE_FILE):
        try:
            import pickle
            with open(CACHE_FILE, "rb") as f:
                cache_data = pickle.load(f)
            
            # Check if new format with timestamp
            if isinstance(cache_data, dict) and "timestamp" in cache_data:
                cache_age = time.time() - cache_data["timestamp"]
                if cache_age > CACHE_MAX_AGE_SECONDS:
                    print(f"⚠️ Cache périmé ({cache_age:.0f}s > {CACHE_MAX_AGE_SECONDS}s) - Données fraîches requises")
                    GLOBAL_CACHE = {}
                else:
                    GLOBAL_CACHE = cache_data["data"]
                    print(f"📦 Cache valide ({cache_age:.0f}s) - {len(GLOBAL_CACHE)} paires")
            else:
                # Old format without timestamp - accept but warn
                GLOBAL_CACHE = cache_data
                print(f"⚠️ Cache ancien format (sans timestamp) - {len(GLOBAL_CACHE)} paires")
        except Exception:
            GLOBAL_CACHE = {}
    else:
        GLOBAL_CACHE = {}

def generate_session_id():
    return "cs_" + ''.join(random.choices(string.ascii_letters + string.digits, k=12))

def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"

def fetch_data_yahoo(pair_yahoo, interval_code, n_candles=None):
    """
    Fallback sur Yahoo Finance via yfinance.
    """
    try:
        # Adaptation des périodes pour Asian Scanner
        period = "1y"
        if interval_code == "1h": period = "1mo" # Large buffer pour Asian
        elif interval_code == "1d": period = "2y"
        
        df = yf.Ticker(pair_yahoo).history(period=period, interval=interval_code)
        if df.empty: return None
        
        df.index.name = "datetime"
        
        # Filtrer week-end pour daily
        if interval_code == "1d":
            df = df[df.index.dayofweek < 5]
            
        return df
    except Exception as e:
        # print(f"Yahoo Error {pair_yahoo}: {e}")
        return None

def fetch_data_tv(pair_yahoo, interval_code, n_candles=300):
    """
    Fetches data from TradingView via WebSocket.
    """
    clean_pair = pair_yahoo.replace("=X", "")
    tv_pair = f"OANDA:{clean_pair}"
    
    tv_interval = "60"
    if interval_code == "1d": tv_interval = "D"
    elif interval_code == "1h": tv_interval = "60"
    
    ws = None
    extracted_df = None
    
    try:
        # Headers sécurisés
        headers = {
            "Origin": "https://www.tradingview.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        ws = create_connection("wss://prodata.tradingview.com/socket.io/websocket", header=headers, timeout=10)
        session_id = generate_session_id()
        
        ws.send(create_message("chart_create_session", [session_id, ""]))
        ws.send(create_message("resolve_symbol", [session_id, "sds_sym_1", f"={{\"symbol\":\"{tv_pair}\",\"adjustment\":\"splits\",\"session\":\"regular\"}}"]))
        ws.send(create_message("create_series", [session_id, "sds_1", "s1", "sds_sym_1", tv_interval, n_candles, ""]))
        
        start_t = time.time()
        while time.time() - start_t < 8: # Timeout 8s
            try:
                res = ws.recv()
                if '"s":[' in res:
                    start = res.find('"s":[')
                    end = res.find('"ns":', start)
                    if start != -1 and end != -1:
                        extract_end = end - 1
                        while res[extract_end] not in [',', '}']: extract_end -= 1
                        
                        raw = res[start + 4:extract_end]
                        data = json.loads(raw)
                        fdata = [item["v"] for item in data]
                        extracted_df = pd.DataFrame(fdata, columns=["timestamp", "open", "high", "low", "close", "volume"])
                        
                        extracted_df['datetime'] = pd.to_datetime(extracted_df['timestamp'], unit='s', utc=True)
                        extracted_df.set_index('datetime', inplace=True)
                        extracted_df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}, inplace=True)
                        extracted_df.drop(columns=['timestamp'], inplace=True)
                        
                if "series_completed" in res:
                    break
            except: pass
            
        ws.close()
        return extracted_df
    except Exception:
        if ws: 
            try: ws.close() 
            except: pass
        return None

def fetch_data_smart(pair_yahoo, interval_code, n_candles=300):
    """
    Priorité : Cache > TV > Yahoo
    """
    # 0. Cache Local
    load_global_cache()
    if GLOBAL_CACHE and pair_yahoo in GLOBAL_CACHE:
        cache_data = GLOBAL_CACHE[pair_yahoo]
        if interval_code in cache_data:
            # print(f"⚡ Cache Hit {pair_yahoo} {interval_code}")
            return cache_data[interval_code]

    # 1. TradingView WebSocket
    df = fetch_data_tv(pair_yahoo, interval_code, n_candles)
    if df is not None and not df.empty:
        return df
    
    # 2. Yahoo Finance (Fallback)
    return fetch_data_yahoo(pair_yahoo, interval_code, n_candles)

# --- TIME & DST UTILS ---
def is_dst(dt=None, timezone="Europe/Paris"):
    if dt is None:
        dt = datetime.datetime.now(datetime.timezone.utc)
    tz = pytz.timezone(timezone)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(tz).dst() != datetime.timedelta(0)

def get_asian_hours(target_date_utc):
    dst = is_dst(target_date_utc)
    if dst:
        return 1, 6 # Summer
    else:
        return 2, 7 # Winter

# --- INDICATORS ---
def calculate_emas(df):
    for length in EMA_LENGTHS:
        df[f'EMA_{length}'] = df['Close'].ewm(span=length, adjust=False).mean()
    return df

def check_daily_trend(df, pair_name="Unknown", debug=False):
    """
    Analyzes Daily Trend with Debug option.
    """
    if df is None or len(df) < 60:
        return "NEUTRAL"
    
    last = df.iloc[-1]
    close = last['Close']
    open_price = last['Open']
    date_str = last.name.strftime("%Y-%m-%d")
    
    ema_vals = [last[f'EMA_{l}'] for l in EMA_LENGTHS]
    ema_20 = last['EMA_20'] # Smallest EMA
    
    all_above = all(close > v for v in ema_vals)
    all_below = all(close < v for v in ema_vals)
    
    is_aligned_bull = all(ema_vals[i] > ema_vals[i+1] for i in range(len(ema_vals)-1))
    is_aligned_bear = all(ema_vals[i] < ema_vals[i+1] for i in range(len(ema_vals)-1))
    
    # Filter Open vs EMA20
    is_open_bull = open_price > ema_20
    is_open_bear = open_price < ema_20
    
    trend = "NEUTRAL"
    if all_above and is_aligned_bull and is_open_bull:
        trend = "BULLISH"
    elif all_below and is_aligned_bear and is_open_bear:
        trend = "BEARISH"
    
    # Relaxed trend (only Close vs EMAs, no alignment/open filter)
    relaxed_trend = "NEUTRAL"
    if all_above:
        relaxed_trend = "BULLISH"
    elif all_below:
        relaxed_trend = "BEARISH"

    if debug:
        try:
            with open("e:/asian_strategy/debug_log.txt", "a", encoding="utf-8") as f:
                f.write(f"\nDEBUG DAILY for {pair_name} on {date_str}\n")
                f.write(f"   Open: {open_price:.5f} | Close: {close:.5f}\n")
                f.write(f"   EMA20: {ema_20:.5f}\n")
                f.write(f"   > Align Bull: {is_aligned_bull} | Align Bear: {is_aligned_bear}\n")
                f.write(f"   > Above All: {all_above} | Below All: {all_below}\n")
                f.write(f"   > Open > EMA20: {is_open_bull} | Open < EMA20: {is_open_bear}\n")
                f.write(f"   => RESULT: {trend}\n")
        except: pass
        
    return {
        "trend": trend,
        "relaxed_trend": relaxed_trend,
        "open": open_price,
        "ema20": ema_20
    }

def analyze_pair(pair, debug_mode=False):
    # 1. Daily Analysis (via TradingView Socket + Fallback)
    # We ask for 200 candles to be safe for EMA55 calculation
    df_d = fetch_data_smart(pair, "1d", n_candles=200)
    
    if df_d is None: return None
    
    # === CORRECTION: EXCLUDE WEEKEND CANDLES ===
    # If the last candle is Saturday (5) or Sunday (6), remove it.
    if not df_d.empty and df_d.index[-1].weekday() >= 5:
        df_d = df_d.iloc[:-1]
    
    df_d = calculate_emas(df_d)
    
    # Check trend
    debug_this = debug_mode and (pair == debug_mode or debug_mode == "ALL")
    trend_data = check_daily_trend(df_d, pair, debug_this)
    trend = trend_data["trend"]
    relaxed_trend = trend_data["relaxed_trend"]
    open_price = trend_data["open"]
    ema_20 = trend_data["ema20"]
    
    if trend == "NEUTRAL" and relaxed_trend == "NEUTRAL":
        return None 
    
    # 2. H1 Analysis (via TradingView Socket + Fallback)
    # We fetch 120 candles (5 days * 24h) to cover Asian session
    df_h1 = fetch_data_smart(pair, "1h", n_candles=120)
    
    if df_h1 is None: return None
    
    # Calculate Runner PCT (Variation based on Daily Open)
    current_price_runner = df_h1['Close'].iloc[-1]
    runner_pct = 0.0
    if open_price > 0:
        runner_pct = ((current_price_runner - open_price) / open_price) * 100.0
        
    df_h1 = calculate_emas(df_h1)
    
    paris_tz = pytz.timezone("Europe/Paris")
    df_h1.index = df_h1.index.tz_convert(paris_tz)
    
    last_dt = df_h1.index[-1]
    target_date = last_dt.date()
    start_h, end_h = get_asian_hours(last_dt)
    
    # Session Mask
    session_mask = (df_h1.index.date == target_date) & \
                   (df_h1.index.hour >= start_h) & \
                   (df_h1.index.hour < end_h)
                   
    session_candles = df_h1[session_mask]
    
    # Fallback if session incomplete
    if len(session_candles) < 3:
        target_date = target_date - datetime.timedelta(days=1)
        session_mask = (df_h1.index.date == target_date) & \
                       (df_h1.index.hour >= start_h) & \
                       (df_h1.index.hour < end_h)
        session_candles = df_h1[session_mask]
        
        if len(session_candles) < 3:
            return None 
            
    asian_high_val = session_candles['High'].max()
    asian_low_val = session_candles['Low'].min()
    
    idx_high = session_candles['High'].idxmax()
    idx_low = session_candles['Low'].idxmin()
    
    candle_high = session_candles.loc[idx_high]
    candle_low = session_candles.loc[idx_low]

    # --- FILTRE TRIPLE G V2 : Puissance depuis Asian Range (> 0.2%) ---
    current_price_check = df_h1['Close'].iloc[-1]
    range_progress_pct = 0.0
    
    # Use relaxed_trend for progress calculation if trend is NEUTRAL
    active_trend = trend if trend != "NEUTRAL" else relaxed_trend
    
    if active_trend == "BULLISH":
        if asian_low_val > 0:
            range_progress_pct = ((current_price_check - asian_low_val) / asian_low_val) * 100.0
        if trend != "NEUTRAL" and range_progress_pct < 0.2:
            return None
            
    elif active_trend == "BEARISH":
        if asian_high_val > 0:
            range_progress_pct = ((asian_high_val - current_price_check) / asian_high_val) * 100.0
        if trend != "NEUTRAL" and range_progress_pct < 0.2:
            return None
    # ---------------------------------------------------------------------
    
    setup_valid = False
    setup_type = ""
    trigger_level = 0.0
    stop_loss = 0.0
    
    if trend == "BULLISH":
        emas_at_low = [candle_low[f'EMA_{l}'] for l in EMA_LENGTHS]
        ribbon_min = min(emas_at_low)
        # Condition: Asian Low Close < Ribbon Min
        if candle_low['Close'] < ribbon_min:
            setup_valid = True
            setup_type = "BUY STOP"
            trigger_level = asian_high_val
            stop_loss = asian_low_val
            
    elif trend == "BEARISH":
        emas_at_high = [candle_high[f'EMA_{l}'] for l in EMA_LENGTHS]
        ribbon_max = max(emas_at_high)
        # Condition: Asian High Close > Ribbon Max
        if candle_high['Close'] > ribbon_max:
            setup_valid = True
            setup_type = "SELL STOP"
            trigger_level = asian_low_val
            stop_loss = asian_high_val
            
    if setup_valid:
        # 3. Activation Check (Dynamic based on Current Price)
        current_price = df_h1['Close'].iloc[-1]
        
        # Get Current H1 EMAs values for Strict Waiting Condition
        current_candle = df_h1.iloc[-1]
        emas_h1 = [current_candle[f'EMA_{l}'] for l in EMA_LENGTHS]
        
        status = "PENDING"
        
        if trend == "BULLISH":
            if current_price > asian_high_val:
                status = "TRIGGERED"      # Currently above Entry -> ACTIVE
            elif current_price < asian_low_val:
                status = "INVALIDATED"    # Currently below SL -> INVALID
            else:
                # Strict WAITING Condition: Price > All H1 EMAs
                if all(current_price > ema for ema in emas_h1):
                    status = "PENDING"
                else:
                    status = "IGNORED"    # Not ready yet (inside ribbon)
                
        elif trend == "BEARISH":
            if current_price < asian_low_val:
                status = "TRIGGERED"      # Currently below Entry -> ACTIVE
            elif current_price > asian_high_val:
                status = "INVALIDATED"    # Currently above SL -> INVALID
            else:
                # Strict WAITING Condition: Price < All H1 EMAs
                if all(current_price < ema for ema in emas_h1):
                    status = "PENDING"
                else:
                    status = "IGNORED"    # Not ready yet (inside ribbon)

        return {
            "pair": pair.replace("=X", ""),
            "trend": trend,
            "date": target_date.strftime("%Y-%m-%d"),
            "session": f"{start_h}h-{end_h}h",
            "type": setup_type,
            "trigger": trigger_level,
            "sl": stop_loss,
            "asian_high": asian_high_val,
            "asian_low": asian_low_val,
            "status": status,
            "daily_open": open_price,
            "daily_ema20": ema_20,
            "current_price": current_price,
            "runner_pct": runner_pct,
            "df_d": df_d  # Pass daily dataframe for EMA indicators
        }
    
    # --- ALREADY CHECK: Asian Range entirely above/below EMA35 with perfect H1 alignment ---
    # Get EMA values at the candle where Asian High/Low occurred
    ema35_at_high = candle_high['EMA_35']
    ema35_at_low = candle_low['EMA_35']
    
    # Get current H1 EMAs for alignment check
    current_candle = df_h1.iloc[-1]
    emas_h1 = [current_candle[f'EMA_{l}'] for l in EMA_LENGTHS]
    
    # Check perfect H1 alignment
    is_h1_aligned_bull = all(emas_h1[i] > emas_h1[i+1] for i in range(len(emas_h1)-1))
    is_h1_aligned_bear = all(emas_h1[i] < emas_h1[i+1] for i in range(len(emas_h1)-1))
    
    already_valid = False
    already_type = ""
    trigger_level = 0.0
    stop_loss = 0.0
    
    if trend == "BULLISH":
        # Asian Low must be above EMA35 + Perfect H1 Bull Alignment
        if asian_low_val > ema35_at_low and is_h1_aligned_bull:
            already_valid = True
            already_type = "BUY STOP"
            trigger_level = asian_high_val
            stop_loss = asian_low_val
            
    elif trend == "BEARISH":
        # Asian High must be below EMA35 + Perfect H1 Bear Alignment
        if asian_high_val < ema35_at_high and is_h1_aligned_bear:
            already_valid = True
            already_type = "SELL STOP"
            trigger_level = asian_low_val
            stop_loss = asian_high_val
    
    if already_valid:
        current_price = df_h1['Close'].iloc[-1]
        current_open = df_h1['Open'].iloc[-1]
        
        # Calculate 50% Fibonacci level of Asian Range
        fibo_50 = asian_low_val + (asian_high_val - asian_low_val) * 0.5
        
        # Determine ALREADY status
        status = "ALREADY"
        
        if trend == "BULLISH":
            if current_open > fibo_50:  # H1 Open above 50% Fibo
                status = "ALREADY_TRIGGERED"
            elif current_price < asian_low_val:
                status = "ALREADY_INVALIDATED"
                
        elif trend == "BEARISH":
            if current_open < fibo_50:  # H1 Open below 50% Fibo
                status = "ALREADY_TRIGGERED"
            elif current_price > asian_high_val:
                status = "ALREADY_INVALIDATED"
        
        return {
            "pair": pair.replace("=X", ""),
            "trend": trend,
            "date": target_date.strftime("%Y-%m-%d"),
            "session": f"{start_h}h-{end_h}h",
            "type": already_type,
            "trigger": trigger_level,
            "sl": stop_loss,
            "asian_high": asian_high_val,
            "asian_low": asian_low_val,
            "status": status,
            "daily_open": open_price,
            "daily_ema20": ema_20,
            "current_price": current_price,
            "runner_pct": runner_pct,
            "df_d": df_d  # Pass daily dataframe for EMA indicators
        }
    
    # --- ALREADY LIGHT CHECK: Relaxed Daily (only Close vs EMAs) + H1 alignment ---
    # Only check if strict trend is NEUTRAL but relaxed trend is valid
    if trend == "NEUTRAL" and relaxed_trend != "NEUTRAL":
        # Get EMA values at the candle where Asian High/Low occurred
        ema35_at_high = candle_high['EMA_35']
        ema35_at_low = candle_low['EMA_35']
        
        # Get current H1 EMAs for alignment check
        current_candle = df_h1.iloc[-1]
        emas_h1 = [current_candle[f'EMA_{l}'] for l in EMA_LENGTHS]
        
        # Check perfect H1 alignment
        is_h1_aligned_bull = all(emas_h1[i] > emas_h1[i+1] for i in range(len(emas_h1)-1))
        is_h1_aligned_bear = all(emas_h1[i] < emas_h1[i+1] for i in range(len(emas_h1)-1))
        
        already_light_valid = False
        already_light_type = ""
        trigger_level = 0.0
        stop_loss = 0.0
        
        if relaxed_trend == "BULLISH":
            # Asian Low must be above EMA35 + Perfect H1 Bull Alignment
            if asian_low_val > ema35_at_low and is_h1_aligned_bull:
                already_light_valid = True
                already_light_type = "BUY STOP"
                trigger_level = asian_high_val
                stop_loss = asian_low_val
                
        elif relaxed_trend == "BEARISH":
            # Asian High must be below EMA35 + Perfect H1 Bear Alignment
            if asian_high_val < ema35_at_high and is_h1_aligned_bear:
                already_light_valid = True
                already_light_type = "SELL STOP"
                trigger_level = asian_low_val
                stop_loss = asian_high_val
        
        if already_light_valid:
            current_price = df_h1['Close'].iloc[-1]
            current_open = df_h1['Open'].iloc[-1]
            
            # Calculate 50% Fibonacci level of Asian Range
            fibo_50 = asian_low_val + (asian_high_val - asian_low_val) * 0.5
            
            # Determine ALREADY LIGHT status
            status = "ALREADY_LIGHT"
            
            if relaxed_trend == "BULLISH":
                if current_open > fibo_50:  # H1 Open above 50% Fibo
                    status = "ALREADY_LIGHT_TRIGGERED"
                elif current_price < asian_low_val:
                    status = "ALREADY_LIGHT_INVALIDATED"
                    
            elif relaxed_trend == "BEARISH":
                if current_open < fibo_50:  # H1 Open below 50% Fibo
                    status = "ALREADY_LIGHT_TRIGGERED"
                elif current_price > asian_high_val:
                    status = "ALREADY_LIGHT_INVALIDATED"
            
            return {
                "pair": pair.replace("=X", ""),
                "trend": relaxed_trend,  # Use relaxed trend
                "date": target_date.strftime("%Y-%m-%d"),
                "session": f"{start_h}h-{end_h}h",
                "type": already_light_type,
                "trigger": trigger_level,
                "sl": stop_loss,
                "asian_high": asian_high_val,
                "asian_low": asian_low_val,
                "status": status,
                "daily_open": open_price,
                "daily_ema20": ema_20,
                "current_price": current_price,
                "runner_pct": runner_pct,
            "df_d": df_d  # Pass daily dataframe for EMA indicators
            }
        
    return None

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram credentials not found in .env")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        requests.post(url, json=payload, timeout=10)
        print("📤 Telegram message sent.")
    except Exception as e:
        print(f"⚠️ Error sending Telegram message: {e}")

def is_within_active_hours():
    """
    Check if current time is within active trading hours.
    Active hours: 1 hour after Asian session end until 22h00 (Paris time)
    - Summer: 07h00 - 22h00
    - Winter: 08h00 - 22h00
    """
    paris_tz = pytz.timezone("Europe/Paris")
    now_paris = datetime.datetime.now(paris_tz)
    current_hour = now_paris.hour
    
    # Determine start hour based on DST
    dst = is_dst(now_paris)
    if dst:
        start_hour = 7   # Summer: Asian ends at 6h, active from 7h
    else:
        start_hour = 8   # Winter: Asian ends at 7h, active from 8h
    
    end_hour = 22
    
    return start_hour <= current_hour < end_hour, start_hour, end_hour, current_hour

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", type=str, help="Pair to debug (e.g. NZDCAD=X)")
    parser.add_argument("--force", action="store_true", help="Force scan even outside active hours")
    args = parser.parse_args()
    
    print("ASIAN STRATEGY SCANNER")
    print("======================")
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Time: {current_time}")
    
    # Check if within active hours
    is_active, start_h, end_h, current_h = is_within_active_hours()
    
    if not is_active and not args.force:
        print(f"\n💤 Scanner dormant - Outside active hours")
        print(f"   Current hour: {current_h}h (Paris)")
        print(f"   Active hours: {start_h}h - {end_h}h")
        print(f"\n   Use --force to run anyway.")
        return
    
    # If debug, clean log file
    if args.debug:
        try: os.remove("e:/asian_strategy/debug_log.txt")
        except: pass
        print(f"DEBUG MODE for {args.debug} (Check debug_log.txt)")
    
    debug_target = args.debug if args.debug else False
    scan_list = [debug_target] if debug_target else PAIRS
    
    results = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pair = {executor.submit(analyze_pair, pair, debug_target): pair for pair in scan_list}
        for future in as_completed(future_to_pair):
            res = future.result()
            if res:
                results.append(res)

    print("\n================ RESULTS ================")
    
    # Telegram Report Buffer
    tg_lines = []
    tg_lines.append(f"🌏 *ASIAN SCANNER*")
    
    if not results:
        print("No setup found.")
        tg_lines.append("\nNo setup found today.")
    else:
        # Sort keys:
        # 1. Status Rank (Triggered > Pending > Invalidated) - OPTIONAL if you prefer absolute pct globally
        # 2. Absolute Runner PCT (Descending) - High volatility first
        
        def sort_key(x):
            # Primary: Status Rank
            s_rank = 0
            if x['status'] == "TRIGGERED": s_rank = 9
            elif x['status'] == "ALREADY_TRIGGERED": s_rank = 8
            elif x['status'] == "ALREADY_LIGHT_TRIGGERED": s_rank = 7
            elif x['status'] == "PENDING": s_rank = 6
            elif x['status'] == "ALREADY": s_rank = 5
            elif x['status'] == "ALREADY_LIGHT": s_rank = 4
            elif x['status'] == "INVALIDATED": s_rank = 3
            elif x['status'] == "ALREADY_INVALIDATED": s_rank = 2
            elif x['status'] == "ALREADY_LIGHT_INVALIDATED": s_rank = 1
            
            # Secondary: Absolute PCT
            return (s_rank, abs(x['runner_pct']))
            
        # Sort descending (higher rank first, higher abs pct first)
        results.sort(key=sort_key, reverse=True)
        
        triggered = [r for r in results if r['status'] == "TRIGGERED"]
        pending = [r for r in results if r['status'] == "PENDING"]
        invalidated = [r for r in results if r['status'] == "INVALIDATED"]
        already = [r for r in results if r['status'] == "ALREADY"]
        already_triggered = [r for r in results if r['status'] == "ALREADY_TRIGGERED"]
        already_invalidated = [r for r in results if r['status'] == "ALREADY_INVALIDATED"]
        already_light = [r for r in results if r['status'] == "ALREADY_LIGHT"]
        already_light_triggered = [r for r in results if r['status'] == "ALREADY_LIGHT_TRIGGERED"]
        already_light_invalidated = [r for r in results if r['status'] == "ALREADY_LIGHT_INVALIDATED"]
        
        # Helper for Emoji
        def get_emoji(trend):
            return "🟢" if trend == "BULLISH" else "🔴"
        
        # Helper for EMA indicators
        def get_ema_indicators(r):
            """Get EMA status and aligned status from daily dataframe."""
            df_d = r.get('df_d')
            if df_d is None or df_d.empty:
                return "⚪", "⚪"
            
            ema_status = calculate_ema_gap_status(df_d)
            ema_aligned = calculate_ema_aligned_status(df_d)
            
            # Extract just the emoji part for compact display
            ema_status_short = ema_status.split()[-1] if ema_status else "⚪"
            ema_aligned_short = ema_aligned.split()[-1] if ema_aligned else "⚪"
            
            return ema_status_short, ema_aligned_short
        
        # Helper to check full coherence (runner + EMA)
        def is_fully_coherent(r):
            """Returns True if runner direction matches trend AND EMA indicators.
            🟢 BULLISH → runner > 0 AND EMA not 🔴
            🔴 BEARISH → runner < 0 AND EMA not 🟢
            """
            trend = r.get('trend', 'NEUTRAL')
            runner = r.get('runner_pct', 0)
            
            # Check runner coherence first
            if trend == "BULLISH" and runner <= 0:
                return False
            if trend == "BEARISH" and runner >= 0:
                return False
            
            # Check EMA coherence
            ema_st, ema_al = get_ema_indicators(r)
            if trend == "BULLISH":
                return ema_st != "🔴" and ema_al != "🔴"
            elif trend == "BEARISH":
                return ema_st != "🟢" and ema_al != "🟢"
            return True

        # 1. TRIGGERED
        if triggered:
            print("\n ACTIVATED (TRIGGERED)")
            print("=" * 40)
            tg_lines.append("\n🚀 *ACTIVE*")
            
            for r in triggered:
                emoji = get_emoji(r['trend'])
                pct_str = f"({r['runner_pct']:+.2f}%)"
                
                # Console
                print(f"{emoji} {r['pair']} [{r['date']}] TRIGGERED {pct_str}")
                print(f"   Signal  : {r['type']} @ {r['trigger']:.5f}")
                print(f"   StopLoss: {r['sl']:.5f}")
                print(f"   Note    : Level {r['trigger']:.5f} broken !")
                print(f"   Conf    : Open {r['daily_open']:.5f} / EMA20 {r['daily_ema20']:.5f}")
                print("-------------------------------------------")
                # Telegram Output (Compact) with EMA indicators - only if coherent
                if is_fully_coherent(r):
                    ema_st, ema_al = get_ema_indicators(r)
                    tg_lines.append(f"{emoji} *{r['pair']}* {pct_str} | {ema_st}{ema_al}")

        # 2. PENDING (VALID)
        if pending:
            print("\n WAITING (VALID)")
            print("=" * 40)
            tg_lines.append("\n⏳ *WAITING*")
            
            for r in pending:
                emoji = get_emoji(r['trend'])
                pct_str = f"({r['runner_pct']:+.2f}%)"
                
                # Console
                print(f"{emoji} {r['pair']} [{r['date']}] PENDING {pct_str}")
                print(f"   Signal  : {r['type']} @ {r['trigger']:.5f}")
                print(f"   StopLoss: {r['sl']:.5f}")
                print(f"   Current : {r['current_price']:.5f}")
                print(f"   Conf    : Open {r['daily_open']:.5f} / EMA20 {r['daily_ema20']:.5f}")
                print("-------------------------------------------")
                # Telegram with EMA indicators - only if coherent
                if is_fully_coherent(r):
                    ema_st, ema_al = get_ema_indicators(r)
                    tg_lines.append(f"{emoji} *{r['pair']}* {pct_str} | {ema_st}{ema_al}")

        # 3. INVALIDATED
        if invalidated:
            print("\n INVALIDATED (CURRENTLY INVALID)")
            print("=" * 40)
            tg_lines.append("\n❌ *INVALID*")
            
            for r in invalidated:
                emoji = get_emoji(r['trend']) # Always color based on trend
                pct_str = f"({r['runner_pct']:+.2f}%)"
                
                # Console
                print(f"{emoji} {r['pair']} [{r['date']}] INVALIDATED {pct_str}")
                print(f"   Signal  : {r['type']} @ {r['trigger']:.5f}")
                print(f"   StopLoss: {r['sl']:.5f} (Price beyond SL)")
                print(f"   Current : {r['current_price']:.5f}")
                print("-------------------------------------------")
                # Telegram with EMA indicators - only if coherent
                if is_fully_coherent(r):
                    ema_st, ema_al = get_ema_indicators(r)
                    tg_lines.append(f"{emoji} *{r['pair']}* {pct_str} | {ema_st}{ema_al}")
        
        # 4. ALREADY (Trend continuation - no deep retracement)
        if already:
            print("\n ALREADY (TREND CONTINUATION)")
            print("=" * 40)
            tg_lines.append("\n📈 *ALREADY*")
            
            for r in already:
                emoji = get_emoji(r['trend'])
                pct_str = f"({r['runner_pct']:+.2f}%)"
                
                # Console
                print(f"{emoji} {r['pair']} [{r['date']}] ALREADY {pct_str}")
                print(f"   Signal  : {r['type']} @ {r['trigger']:.5f}")
                print(f"   StopLoss: {r['sl']:.5f}")
                print(f"   Current : {r['current_price']:.5f}")
                print(f"   Note    : Asian range above/below EMA35 + H1 aligned")
                print("-------------------------------------------")
                # Telegram with EMA indicators - only if coherent
                if is_fully_coherent(r):
                    ema_st, ema_al = get_ema_indicators(r)
                    tg_lines.append(f"{emoji} *{r['pair']}* {pct_str} | {ema_st}{ema_al}")
        
        # 5. ALREADY_TRIGGERED
        if already_triggered:
            print("\n ALREADY TRIGGERED (CONTINUATION ACTIVE)")
            print("=" * 40)
            tg_lines.append("\n🔥 *ALREADY ACTIVE*")
            
            for r in already_triggered:
                emoji = get_emoji(r['trend'])
                pct_str = f"({r['runner_pct']:+.2f}%)"
                
                # Console
                print(f"{emoji} {r['pair']} [{r['date']}] ALREADY_TRIGGERED {pct_str}")
                print(f"   Signal  : {r['type']} @ {r['trigger']:.5f}")
                print(f"   StopLoss: {r['sl']:.5f}")
                print(f"   Note    : Level {r['trigger']:.5f} broken (continuation)!")
                print("-------------------------------------------")
                # Telegram with EMA indicators - only if coherent
                if is_fully_coherent(r):
                    ema_st, ema_al = get_ema_indicators(r)
                    tg_lines.append(f"{emoji} *{r['pair']}* {pct_str} | {ema_st}{ema_al}")
        
        # 6. ALREADY_INVALIDATED
        if already_invalidated:
            print("\n ALREADY INVALIDATED")
            print("=" * 40)
            tg_lines.append("\n💨 *ALREADY INVALID*")
            
            for r in already_invalidated:
                emoji = get_emoji(r['trend'])
                pct_str = f"({r['runner_pct']:+.2f}%)"
                
                # Console
                print(f"{emoji} {r['pair']} [{r['date']}] ALREADY_INVALIDATED {pct_str}")
                print(f"   Signal  : {r['type']} @ {r['trigger']:.5f}")
                print(f"   StopLoss: {r['sl']:.5f} (Price beyond SL)")
                print(f"   Current : {r['current_price']:.5f}")
                print("-------------------------------------------")
                # Telegram with EMA indicators - only if coherent
                if is_fully_coherent(r):
                    ema_st, ema_al = get_ema_indicators(r)
                    tg_lines.append(f"{emoji} *{r['pair']}* {pct_str} | {ema_st}{ema_al}")
        
        # 7. ALREADY_LIGHT (Relaxed Daily - trend continuation)
        if already_light:
            print("\n ALREADY LIGHT (RELAXED DAILY)")
            print("=" * 40)
            tg_lines.append("\n📊 *ALREADY LIGHT*")
            
            for r in already_light:
                emoji = get_emoji(r['trend'])
                pct_str = f"({r['runner_pct']:+.2f}%)"
                
                # Console
                print(f"{emoji} {r['pair']} [{r['date']}] ALREADY_LIGHT {pct_str}")
                print(f"   Signal  : {r['type']} @ {r['trigger']:.5f}")
                print(f"   StopLoss: {r['sl']:.5f}")
                print(f"   Current : {r['current_price']:.5f}")
                print(f"   Note    : Relaxed Daily (Close only) + H1 aligned")
                print("-------------------------------------------")
                # Telegram with EMA indicators - only if coherent
                if is_fully_coherent(r):
                    ema_st, ema_al = get_ema_indicators(r)
                    tg_lines.append(f"{emoji} *{r['pair']}* {pct_str} | {ema_st}{ema_al}")
        
        # 8. ALREADY_LIGHT_TRIGGERED
        if already_light_triggered:
            print("\n ALREADY LIGHT TRIGGERED")
            print("=" * 40)
            tg_lines.append("\n✨ *ALREADY LIGHT ACTIVE*")
            
            for r in already_light_triggered:
                emoji = get_emoji(r['trend'])
                pct_str = f"({r['runner_pct']:+.2f}%)"
                
                # Console
                print(f"{emoji} {r['pair']} [{r['date']}] ALREADY_LIGHT_TRIGGERED {pct_str}")
                print(f"   Signal  : {r['type']} @ {r['trigger']:.5f}")
                print(f"   StopLoss: {r['sl']:.5f}")
                print(f"   Note    : Level {r['trigger']:.5f} broken (light)!")
                print("-------------------------------------------")
                # Telegram with EMA indicators - only if coherent
                if is_fully_coherent(r):
                    ema_st, ema_al = get_ema_indicators(r)
                    tg_lines.append(f"{emoji} *{r['pair']}* {pct_str} | {ema_st}{ema_al}")
        
        # 9. ALREADY_LIGHT_INVALIDATED
        if already_light_invalidated:
            print("\n ALREADY LIGHT INVALIDATED")
            print("=" * 40)
            tg_lines.append("\n💤 *ALREADY LIGHT INVALID*")
            
            for r in already_light_invalidated:
                emoji = get_emoji(r['trend'])
                pct_str = f"({r['runner_pct']:+.2f}%)"
                
                # Console
                print(f"{emoji} {r['pair']} [{r['date']}] ALREADY_LIGHT_INVALIDATED {pct_str}")
                print(f"   Signal  : {r['type']} @ {r['trigger']:.5f}")
                print(f"   StopLoss: {r['sl']:.5f} (Price beyond SL)")
                print(f"   Current : {r['current_price']:.5f}")
                print("-------------------------------------------")
                # Telegram with EMA indicators - only if coherent
                if is_fully_coherent(r):
                    ema_st, ema_al = get_ema_indicators(r)
                    tg_lines.append(f"{emoji} *{r['pair']}* {pct_str} | {ema_st}{ema_al}")

    # Footer with Time
    tg_lines.append(f"\n⏰ {current_time} Paris")

    # Send if there are results
    if results:
        send_telegram_message("\n".join(tg_lines))

if __name__ == "__main__":
    main()
