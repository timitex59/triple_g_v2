
import sys
import os
import time
import datetime
import importlib
import subprocess
import json
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- DEPENDENCY MANAGEMENT ---
def install_and_import(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        return importlib.import_module(import_name)
    except ImportError:
        print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name)

# Load libs
yf = install_and_import("yfinance")
pd = install_and_import("pandas")
colorama = install_and_import("colorama")
websocket = install_and_import("websocket-client", "websocket")
install_and_import("python-dotenv", "dotenv")
pytz = install_and_import("pytz")  # Added for Asian time handling

from websocket import create_connection
from colorama import Fore, Style, init
from dotenv import load_dotenv
init(autoreset=True)

# Charger les variables d'environnement
load_dotenv()

# --- CONFIGURATION ---
PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
    "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
    "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPCHF=X",
    "AUDNZD=X", "AUDCAD=X", "AUDCHF=X",
    "NZDCAD=X", "NZDCHF=X",
    "CADCHF=X"
]

LOOKBACK = 50
EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]

# --- TV HELPERS ---
def generate_session_id():
    return "cs_" + ''.join(random.choices(string.ascii_letters + string.digits, k=12))

def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"

def fetch_data_tv(pair_yahoo, interval_code, n_candles=300):
    """
    Fetches data from TradingView via WebSocket.
    """
    clean_pair = pair_yahoo.replace("=X", "")
    tv_pair = f"OANDA:{clean_pair}"
    
    # Map Intervals to TV format
    tv_interval = "60"
    if interval_code == "1w": tv_interval = "1W"
    elif interval_code == "1d": tv_interval = "D"
    elif interval_code == "1h": tv_interval = "60"
    elif interval_code == "15m": tv_interval = "15"
    
    ws = None
    extracted_df = None
    
    try:
        # Headers 
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

def fetch_data_yahoo(pair_yahoo, interval_code, period="1mo"):
    try:
        df = yf.Ticker(pair_yahoo).history(period=period, interval=interval_code)
        if df.empty: return None
        return df
    except Exception:
        return None

def fetch_data_smart(pair, interval, period_for_yahoo):
    # 1. Try TV
    df = fetch_data_tv(pair, interval, n_candles=200) # Increased for EMA55
    if df is not None and not df.empty:
        return df, "TV"
        
    # 2. Try Yahoo
    df = fetch_data_yahoo(pair, interval, period_for_yahoo)
    if df is not None:
        return df, "YF"
        
    return None, "NA"

# --- STRATEGY LOGIC ---

def calculate_signal(df, lookback):
    if df is None or len(df) < lookback:
        return 0, 0.0, 0.0 # Neutral, Close, Mid
    
    recent = df.tail(lookback)
    h = recent['High'].max()
    l = recent['Low'].min()
    mid = (h + l) / 2.0
    close = recent['Close'].iloc[-1]
    
    signal = 1 if close > mid else -1
    return signal, close, mid

def calculate_ema_aligned_status(df):
    """Vérifie si les 8 EMAs sont parfaitement alignées."""
    if df is None or len(df) < max(EMA_LENGTHS):
        return "NEUTRAL"
    
    close = df['Close']
    emas = [close.ewm(span=l, adjust=False).mean().iloc[-1] for l in EMA_LENGTHS]
    
    is_bull = all(emas[i] > emas[i+1] for i in range(len(emas)-1))
    if is_bull: return "BULLISH"
        
    is_bear = all(emas[i] < emas[i+1] for i in range(len(emas)-1))
    if is_bear: return "BEARISH"
        
    return "NEUTRAL"

# --- CANDLE TREND LOGIC (FROM PINESCRIPT) ---

def find_last_candle_open(df, target_is_bullish):
    """
    Finds the Open price of the last candle that matches the target direction (Bullish/Bearish).
    PineScript: findLastCandleOpen(tf, isBullish)
    Returns None if not found.
    """
    # Iterate backwards from penultimate candle (since we compare current Close to PAST opens)
    # Actually, PineScript `request.security(..., [open[1], close[1]])` looks at CLOSED candles.
    # So we look at the last N candles.
    
    # We scan up to 20 candles back
    limit = 20
    if len(df) < 2: return None
    
    # Check history (excluding current incomplete candle if using live data, but usually we use closed logic)
    # PineScript logic uses `open[1]`, `close[1]` implies previous candle.
    # But here we pass the whole DF. Let's assume the last row is the "Current" and we look back.
    
    # We iterate from index -2 (previous candle) downwards
    for i in range(len(df) - 2, max(-1, len(df) - 2 - limit), -1):
        row = df.iloc[i]
        c = row['Close']
        o = row['Open']
        
        is_bull = c > o
        
        if target_is_bullish and is_bull:
            return o
        if not target_is_bullish and not is_bull:
            return o
            
    return None

def calculate_candle_trend_status(df):
    """
    Implements the PineScript 'Trend' logic:
    Bullish: Close > Open AND Close > Last Bearish Open
    Bearish: Close < Open AND Close < Last Bullish Open
    """
    if df is None or len(df) < 5:
        return "NEUTRAL"
    
    # Current Candle (Last row)
    last = df.iloc[-1]
    curr_c = last['Close']
    curr_o = last['Open']
    
    basic_bull = curr_c > curr_o
    basic_bear = curr_c < curr_o
    
    # Find Last Opens (from previous candles)
    last_bullish_open = find_last_candle_open(df, True)
    last_bearish_open = find_last_candle_open(df, False)
    
    # Logic
    is_bull = False
    if basic_bull:
        cond1 = (last_bearish_open is None) or (curr_c > last_bearish_open)
        cond2 = (last_bullish_open is None) or (curr_c > last_bullish_open)
        is_bull = cond1 and cond2
        
    is_bear = False
    if basic_bear:
        cond1 = (last_bullish_open is None) or (curr_c < last_bullish_open)
        cond2 = (last_bearish_open is None) or (curr_c < last_bearish_open)
        is_bear = cond1 and cond2
        
    if is_bull: return "BULLISH"
    if is_bear: return "BEARISH"
    return "NEUTRAL"

# --- ASIAN SETUP LOGIC ---

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

def calculate_emas(df):
    for length in EMA_LENGTHS:
        df[f'EMA_{length}'] = df['Close'].ewm(span=length, adjust=False).mean()
    return df

def check_strict_daily_trend(df):
    """
    Checks for STRICT Daily Trend:
    1. Perfect EMA alignment
    2. Open price respecting EMA20 (Open > EMA20 for Bull, Open < EMA20 for Bear)
    """
    if df is None or len(df) < 60:
        return "NEUTRAL"

    # Ensure EMAs are calculated
    df = calculate_emas(df)
    
    last = df.iloc[-1]
    close = last['Close']
    open_price = last['Open']
    
    ema_vals = [last[f'EMA_{l}'] for l in EMA_LENGTHS]
    ema_20 = last['EMA_20'] 
    
    all_above = all(close > v for v in ema_vals)
    all_below = all(close < v for v in ema_vals)
    
    is_aligned_bull = all(ema_vals[i] > ema_vals[i+1] for i in range(len(ema_vals)-1))
    is_aligned_bear = all(ema_vals[i] < ema_vals[i+1] for i in range(len(ema_vals)-1))
    
    is_open_bull = open_price > ema_20
    is_open_bear = open_price < ema_20
    
    if all_above and is_aligned_bull and is_open_bull:
        return "BULLISH"
    elif all_below and is_aligned_bear and is_open_bear:
        return "BEARISH"
    
    return "NEUTRAL"

def check_asian_setup(df_d, df_h1):
    """
    Identifies if the pair matches 'STANDARD' or 'ALREADY' Asian setup.
    """
    if df_d is None or df_h1 is None: return None
    
    trend = check_strict_daily_trend(df_d)
    if trend == "NEUTRAL": return None

    # Calculate Asian Range
    df_h1 = calculate_emas(df_h1)
    
    paris_tz = pytz.timezone("Europe/Paris")
    df_h1_tz = df_h1.copy()
    df_h1_tz.index = df_h1_tz.index.tz_convert(paris_tz)
    
    last_dt = df_h1_tz.index[-1]
    target_date = last_dt.date()
    start_h, end_h = get_asian_hours(last_dt)
    
    # Session Mask
    session_mask = (df_h1_tz.index.date == target_date) & \
                   (df_h1_tz.index.hour >= start_h) & \
                   (df_h1_tz.index.hour < end_h)
                   
    session_candles = df_h1_tz[session_mask]
    
    # Fallback if session incomplete (try yesterday)
    if len(session_candles) < 3:
        target_date = target_date - datetime.timedelta(days=1)
        session_mask = (df_h1_tz.index.date == target_date) & \
                       (df_h1_tz.index.hour >= start_h) & \
                       (df_h1_tz.index.hour < end_h)
        session_candles = df_h1_tz[session_mask]
        
        if len(session_candles) < 3:
            return None 

    asian_high_val = session_candles['High'].max()
    asian_low_val = session_candles['Low'].min()
    
    idx_high = session_candles['High'].idxmax()
    idx_low = session_candles['Low'].idxmin()
    
    candle_high = session_candles.loc[idx_high]
    candle_low = session_candles.loc[idx_low]

    # --- SETUP 1: STANDARD (Deep Retracement) ---
    if trend == "BULLISH":
        emas_at_low = [candle_low[f'EMA_{l}'] for l in EMA_LENGTHS]
        ribbon_min = min(emas_at_low)
        if candle_low['Close'] < ribbon_min:
            return "STD"
            
    elif trend == "BEARISH":
        emas_at_high = [candle_high[f'EMA_{l}'] for l in EMA_LENGTHS]
        ribbon_max = max(emas_at_high)
        if candle_high['Close'] > ribbon_max:
             return "STD"

    # --- SETUP 2: ALREADY (Continuation) ---
    # Requires Perfect H1 Alignment as well
    current_candle = df_h1.iloc[-1]
    current_open = current_candle['Open']
    
    emas_h1 = [current_candle[f'EMA_{l}'] for l in EMA_LENGTHS]
    is_h1_aligned_bull = all(emas_h1[i] > emas_h1[i+1] for i in range(len(emas_h1)-1))
    is_h1_aligned_bear = all(emas_h1[i] < emas_h1[i+1] for i in range(len(emas_h1)-1))

    # Relaxed Threshold: Check against EMA55 (Bottom of Ribbon) instead of EMA35 (Mid)
    ema55_at_high = candle_high['EMA_55']
    ema55_at_low = candle_low['EMA_55']
    
    # Fibonacci 50% of Asian Range
    fibo_50 = asian_low_val + (asian_high_val - asian_low_val) * 0.5

    if trend == "BULLISH":
        # Asian Low > EMA55 + H1 Aligned + Open > Fibo 50%
        if asian_low_val > ema55_at_low and is_h1_aligned_bull and current_open > fibo_50:
            return "ALR"
            
    elif trend == "BEARISH":
        # Asian High < EMA55 + H1 Aligned + Open < Fibo 50%
        if asian_high_val < ema55_at_high and is_h1_aligned_bear and current_open < fibo_50:
             return "ALR"
             
    return None

# --- TELEGRAM ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram credentials not found.")
        return
    
    # Simple requests import locally if not global
    requests = install_and_import("requests")
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML" # Use HTML for bolding if needed, or Markdown
    }
    
    try:
        requests.post(url, json=payload, timeout=10)
        print("📤 Telegram message sent.")
    except Exception as e:
        print(f"⚠️ Error sending Telegram message: {e}")

# --- WORKER FUNCTION ---
def process_pair(pair):
    clean_pair = pair.replace("=X", "")
    
    # Fetch Data (Smart)
    df_1h,  src1 = fetch_data_smart(pair, "1h", "1mo")
    df_1d,  src2 = fetch_data_smart(pair, "1d", "1y")
    df_1w,  src3 = fetch_data_smart(pair, "1w", "2y")
    df_1m,  src4 = fetch_data_smart(pair, "1mo", "5y") # Monthly for MTF check

    # Calculate Signals
    s1, c1, m1 = calculate_signal(df_1h, LOOKBACK)
    s2, c2, m2 = calculate_signal(df_1d, LOOKBACK)
    s3, c3, m3 = calculate_signal(df_1w, LOOKBACK)
    
    # CANDLE TREND STATUS (PineScript Logic)
    ct_d = calculate_candle_trend_status(df_1d)
    ct_w = calculate_candle_trend_status(df_1w)
    ct_m = calculate_candle_trend_status(df_1m)

    # Daily % Change Calculation
    pct_change = 0.0
    if df_1d is not None and not df_1d.empty:
        daily_open = df_1d['Open'].iloc[-1]
        current_price = c1 if s1 != 0 else (c2 if s2 != 0 else 0) 
        
        if daily_open > 0 and current_price > 0:
            pct_change = ((current_price - daily_open) / daily_open) * 100.0
    
    # Daily EMA Alignment Check
    daily_ema_align = calculate_ema_aligned_status(df_1d)

    # Asian Setup Check
    asian_setup = check_asian_setup(df_1d, df_1h)

    # Scoring Logic
    # Majority Vote (Score > 0 means 2 or 3 Bulls)
    score = s1 + s2 + s3
    
    # Mandatory DAILY Confirmation Logic (s2 is Daily) AND Daily EMA Alignment AND Candle Trend (Pine Logic)
    is_buy = (score > 0) and (s2 > 0) and (daily_ema_align == "BULLISH") and (ct_d == "BULLISH")
    is_sell = (score < 0) and (s2 < 0) and (daily_ema_align == "BEARISH") and (ct_d == "BEARISH")
    
    return {
        "pair": pair,
        "clean_pair": clean_pair,
        "s1": s1, "s2": s2, "s3": s3,
        "ct_d": ct_d, "ct_w": ct_w, "ct_m": ct_m, # Candle Trends
        "pct_change": pct_change,
        "is_buy": is_buy,
        "is_sell": is_sell,
        "daily_ema_align": daily_ema_align,
        "asian_setup": asian_setup
    }

def main():
    global TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    # Refresh env vars in case they were set after module load
    if not TELEGRAM_BOT_TOKEN:
        TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    
    # console output
    print(f"{Style.BRIGHT}MTF RANGE CENTER SCANNER (Asian Edition + Candle Trend){Style.RESET_ALL}")
    print(f"Source: TradingView (OANDA) | Filter: Daily EMA Align + Asian Setup + Candle Logic")
    print(f"{'PAIR':<10} {'1H':<5} {'DAILY':<5} {'WEEKLY':<5} | {'GLOBAL SIGNAL':<15} | {'D-EMA':<10} | {'ASIAN':<8} | {'C-TRND (D)':<10} | {'VAR%':<8}")
    print("-" * 115)
    
    # Buffer for Telegram
    tg_header = [f"📡 <b>MTF SCANNER (EMA+ASIAN)</b>", ""]
    tg_signals = []  # List of (pct, formatted_line) for sorting
    
    count_buy = 0
    count_sell = 0
    
    start_time = time.time()
    
    # --- MULTI-THREADING ---
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pair = {executor.submit(process_pair, pair): pair for pair in PAIRS}
        for future in as_completed(future_to_pair):
            try:
                res = future.result()
                results.append(res)
            except Exception as e:
                pass
    
    results.sort(key=lambda x: x['clean_pair'])

    for res in results:
        clean_pair = res['clean_pair']
        s1, s2, s3 = res['s1'], res['s2'], res['s3']
        is_buy, is_sell = res['is_buy'], res['is_sell']
        pct_change = res['pct_change']
        daily_ema_align = res['daily_ema_align']
        asian_setup = res['asian_setup']
        ct_d = res['ct_d'] # Candle Trend Daily

        # Formatting Output
        def fmt_sig(s):
            return f"{Fore.GREEN}UP{Style.RESET_ALL}" if s > 0 else f"{Fore.RED}DN{Style.RESET_ALL}" if s < 0 else "NA"
        
        str_1 = fmt_sig(s1)
        str_2 = fmt_sig(s2)
        str_3 = fmt_sig(s3)
        
        global_str = f"{Fore.WHITE}NEUTRAL{Style.RESET_ALL}"
        
        if is_buy:
            global_str = f"{Fore.GREEN}{Style.BRIGHT}BUY{Style.RESET_ALL}"
            count_buy += 1
        elif is_sell:
            global_str = f"{Fore.RED}{Style.BRIGHT}SELL{Style.RESET_ALL}"
            count_sell += 1
            
        # D-EMA String
        d_ema_str = "NEUT"
        if daily_ema_align == "BULLISH": d_ema_str = f"{Fore.GREEN}ACCEL{Style.RESET_ALL}"
        elif daily_ema_align == "BEARISH": d_ema_str = f"{Fore.RED}DECEL{Style.RESET_ALL}"

        # Asian String
        asian_str = "-"
        if asian_setup == "STD": asian_str = f"{Fore.MAGENTA}STD{Style.RESET_ALL}"
        elif asian_setup == "ALR": asian_str = f"{Fore.CYAN}ALR{Style.RESET_ALL}"
            
        # Candle Trend String
        ct_str = "-"
        if ct_d == "BULLISH": ct_str = f"{Fore.GREEN}BULL{Style.RESET_ALL}"
        elif ct_d == "BEARISH": ct_str = f"{Fore.RED}BEAR{Style.RESET_ALL}"

        # Format PCT
        pct_color = Fore.GREEN if pct_change > 0 else Fore.RED
        pct_str = f"{pct_color}{pct_change:+.2f}%{Style.RESET_ALL}"
        pct_str_clean = f"{pct_change:+.2f}%"
            
        # Console Print
        print(f"{clean_pair:<10} {str_1:<14} {str_2:<14} {str_3:<14} | {global_str:<24} | {d_ema_str:<19} | {asian_str:<17} | {ct_str:<19} | {pct_str}")
        
        # Telegram Append (Filters)
        send_to_telegram = False
        if is_buy or is_sell:
            trend_agrees = (is_buy and pct_change > 0) or (is_sell and pct_change < 0)
            vol_ok = abs(pct_change) >= 0.15
            
            # Asian setup is not mandatory for global signal, but good to show
            if trend_agrees and vol_ok:
                send_to_telegram = True
        
        if send_to_telegram:
            # Format: 🔴 PAIR (-0.08%) ✅
            # Ball color based on Signal Direction
            ball = "🟢" if is_buy else "🔴"
            
            # Checkmark ONLY if Asian Setup is present (STD or ALR)
            checkmark = "✅" if asian_setup else ""
            
            tg_signals.append((abs(pct_change), f"{ball} <b>{clean_pair}</b> ({pct_str_clean}) {checkmark}"))
        
    print("-" * 115)
    print(f"SUMMARY: {Fore.GREEN}{count_buy} BUYS{Style.RESET_ALL} | {Fore.RED}{count_sell} SELLS{Style.RESET_ALL}")
    print(f"⏱️ Done in {time.time() - start_time:.2f}s")
    
    if tg_signals:
        tg_signals.sort(key=lambda x: x[0], reverse=True)
        tg_lines = tg_header + [line for _, line in tg_signals]
        
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        tg_lines.append("")
        tg_lines.append(f"⏰ {now_str} Paris")
        
        send_telegram_message("\n".join(tg_lines))
    else:
        print("No active signals passed filters to send to Telegram.")

if __name__ == "__main__":
    main()
