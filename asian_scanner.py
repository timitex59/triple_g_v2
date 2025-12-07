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

# --- DEPENDENCY MANAGEMENT ---
REQUIRED_PKGS = ["yfinance", "pandas", "numpy", "pytz", "requests"]

def install_and_import(package):
    try:
        return importlib.import_module(package)
    except ImportError:
        # print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(package)

# Load libs
yf = install_and_import("yfinance")
pd = install_and_import("pandas")
np = install_and_import("numpy")
pytz = install_and_import("pytz")
requests = install_and_import("requests")

# --- CONFIGURATION ---
EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]

# Load .env variables manually
TELEGRAM_BOT_TOKEN = None
TELEGRAM_CHAT_ID = None

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(env_path):
    with open(env_path, "r") as f:
        for line in f:
            if line.strip() and not line.startswith("#") and "=" in line:
                key, value = line.strip().split("=", 1)
                if key == "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN = value
                if key == "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID = value

# Forex Pairs List
PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
    "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
    "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPCHF=X",
    "AUDNZD=X", "AUDCAD=X", "AUDCHF=X",
    "NZDCAD=X", "NZDCHF=X",
    "CADCHF=X"
]

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
        "open": open_price,
        "ema20": ema_20
    }

# --- CORE ANALYSIS ---
def fetch_data(pair, interval="1d", period="1y"):
    try:
        ticker = yf.Ticker(pair)
        df = ticker.history(period=period, interval=interval)
        if df.empty: return None
        return df
    except: return None

def analyze_pair(pair, debug_mode=False):
    # 1. Daily Analysis
    df_d = fetch_data(pair, "1d", "1y")
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
    open_price = trend_data["open"]
    ema_20 = trend_data["ema20"]
    
    # Calculate Runner PCT (Daily Variation)
    # Using H1 close for latest price if available, otherwise Daily Close
    current_price_runner = df_d['Close'].iloc[-1]
    
    # Try to get more recent price from H1
    df_h1 = fetch_data(pair, "1h", "5d") 
    if df_h1 is not None and not df_h1.empty:
        current_price_runner = df_h1['Close'].iloc[-1]
        
    runner_pct = 0.0
    if open_price > 0:
        runner_pct = ((current_price_runner - open_price) / open_price) * 100.0
    
    if trend == "NEUTRAL":
        return None 
        
    # 2. H1 Analysis (Already fetched above if available, but need EMA)
    if df_h1 is None: return None
    
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
        
        status = "PENDING"
        
        if trend == "BULLISH":
            if current_price > asian_high_val:
                status = "TRIGGERED"      # Currently above Entry -> ACTIVE
            elif current_price < asian_low_val:
                status = "INVALIDATED"    # Currently below SL -> INVALID
            else:
                status = "PENDING"        # Between SL and Entry -> WAITING
                
        elif trend == "BEARISH":
            if current_price < asian_low_val:
                status = "TRIGGERED"      # Currently below Entry -> ACTIVE
            elif current_price > asian_high_val:
                status = "INVALIDATED"    # Currently above SL -> INVALID
            else:
                status = "PENDING"        # Between SL and Entry -> WAITING

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
            "runner_pct": runner_pct
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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", type=str, help="Pair to debug (e.g. NZDCAD=X)")
    args = parser.parse_args()
    
    # If debug, clean log file
    if args.debug:
        try: os.remove("e:/asian_strategy/debug_log.txt")
        except: pass
        print(f"DEBUG MODE for {args.debug} (Check debug_log.txt)")
    
    print("ASIAN STRATEGY SCANNER")
    print("======================")
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Time: {current_time}")
    
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
            # Primary: Status (3=Triggered, 2=Pending, 1=Invalid)
            s_rank = 0
            if x['status'] == "TRIGGERED": s_rank = 3
            elif x['status'] == "PENDING": s_rank = 2
            else: s_rank = 1
            
            # Secondary: Absolute PCT
            return (s_rank, abs(x['runner_pct']))
            
        # Sort descending (higher rank first, higher abs pct first)
        results.sort(key=sort_key, reverse=True)
        
        triggered = [r for r in results if r['status'] == "TRIGGERED"]
        pending = [r for r in results if r['status'] == "PENDING"]
        invalidated = [r for r in results if r['status'] == "INVALIDATED"]
        
        # Helper for Emoji
        def get_emoji(trend):
            return "🟢" if trend == "BULLISH" else "🔴"

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
                # Telegram Output (Compact)
                tg_lines.append(f"{emoji} *{r['pair']}* {pct_str}")

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
                # Telegram
                tg_lines.append(f"{emoji} *{r['pair']}* {pct_str}")

        # 3. INVALIDATED
        if invalidated:
            print("\n INVALIDATED (SL HIT)")
            print("=" * 40)
            tg_lines.append("\n❌ *INVALID*")
            
            for r in invalidated:
                emoji = get_emoji(r['trend']) # Always color based on trend
                pct_str = f"({r['runner_pct']:+.2f}%)"
                
                # Console
                print(f"{emoji} {r['pair']} [{r['date']}] INVALIDATED {pct_str}")
                print(f"   Signal  : {r['type']} @ {r['trigger']:.5f}")
                print(f"   StopLoss: {r['sl']:.5f} HIT")
                print(f"   Current : {r['current_price']:.5f}")
                print("-------------------------------------------")
                # Telegram (Compact)
                tg_lines.append(f"{emoji} *{r['pair']}* {pct_str}")

    # Footer with Time
    tg_lines.append(f"\n⏰ {current_time} Paris")

    # Send if there are results
    if results:
        send_telegram_message("\n".join(tg_lines))

if __name__ == "__main__":
    main()
