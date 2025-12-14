
import sys
import os
import time
import datetime
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
        print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name)

# Load libs
yf = install_and_import("yfinance")
pd = install_and_import("pandas")
colorama = install_and_import("colorama")
websocket = install_and_import("websocket-client", "websocket")
install_and_import("python-dotenv", "dotenv")
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
    df = fetch_data_tv(pair, interval, n_candles=100)
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

def main():
    global TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    # Refresh env vars in case they were set after module load
    if not TELEGRAM_BOT_TOKEN:
        TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    
    # console output
    print(f"{Style.BRIGHT}MTF RANGE CENTER SCANNER (Lookback: {LOOKBACK}){Style.RESET_ALL}")
    print(f"Source: TradingView (OANDA) with Yahoo Fallback")
    print(f"{'PAIR':<10} {'1H':<5} {'DAILY':<5} {'WEEKLY':<5} | {'GLOBAL SIGNAL':<15} | {'VAR%':<8}")
    print("-" * 75)
    
    # Buffer for Telegram
    tg_header = [f"📡 <b>MTF SCANNER</b>", ""]
    tg_signals = []  # List of (pct, formatted_line) for sorting
    
    count_buy = 0
    count_sell = 0
    
    for pair in PAIRS:
        clean_pair = pair.replace("=X", "")
        
        # Fetch Data (Smart)
        df_1h,  src1 = fetch_data_smart(pair, "1h", "1mo")
        df_1d,  src2 = fetch_data_smart(pair, "1d", "1y")
        df_1w,  src3 = fetch_data_smart(pair, "1w", "2y")
        
        # Calculate Signals
        s1, c1, m1 = calculate_signal(df_1h, LOOKBACK)
        s2, c2, m2 = calculate_signal(df_1d, LOOKBACK)
        s3, c3, m3 = calculate_signal(df_1w, LOOKBACK)
        
        # Daily % Change Calculation
        pct_change = 0.0
        if df_1d is not None and not df_1d.empty:
            daily_open = df_1d['Open'].iloc[-1]
            current_price = c1 if s1 != 0 else (c2 if s2 != 0 else 0) # Fallback to Daily Close if 1H missing
            
            if daily_open > 0 and current_price > 0:
                pct_change = ((current_price - daily_open) / daily_open) * 100.0
        
        # Scoring Logic
        # Majority Vote (Score > 0 means 2 or 3 Bulls)
        score = s1 + s2 + s3
        
        # Mandatory DAILY Confirmation Logic (s2 is Daily)
        is_buy = (score > 0) and (s2 > 0)
        is_sell = (score < 0) and (s2 < 0)
        
        # Formatting Output
        def fmt_sig(s):
            return f"{Fore.GREEN}UP{Style.RESET_ALL}" if s > 0 else f"{Fore.RED}DN{Style.RESET_ALL}" if s < 0 else "NA"
        
        # Emoji for Telegram
        def get_emoji(s):
            return "🟢" if s > 0 else "🔴" if s < 0 else "⚪"

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
            
        # Format PCT
        pct_color = Fore.GREEN if pct_change > 0 else Fore.RED
        pct_str = f"{pct_color}{pct_change:+.2f}%{Style.RESET_ALL}"
        pct_str_clean = f"{pct_change:+.2f}%"
            
        # Console Print
        print(f"{clean_pair:<10} {str_1:<14} {str_2:<14} {str_3:<14} | {global_str:<24} | {pct_str}")
        
        # Telegram Append (Filters)
        # 1. No Neutral (Implicit in is_buy/is_sell check)
        # 2. Trend Agreement (Buy > 0, Sell < 0)
        # 3. Min Volatility (|pct| >= 0.15%)
        
        send_to_telegram = False
        if is_buy or is_sell:
            trend_agrees = (is_buy and pct_change > 0) or (is_sell and pct_change < 0)
            vol_ok = abs(pct_change) >= 0.15
            
            if trend_agrees and vol_ok:
                send_to_telegram = True
        
        if send_to_telegram:
            # Format: 🔴 PAIR (-0.08%) ✅
            # Ball color based on Signal Direction
            ball = "🟢" if is_buy else "🔴"
            
            # Store with pct for sorting later
            tg_signals.append((abs(pct_change), f"{ball} <b>{clean_pair}</b> ({pct_str_clean}) ✅"))
        
    print("-" * 75)
    print(f"SUMMARY: {Fore.GREEN}{count_buy} BUYS{Style.RESET_ALL} | {Fore.RED}{count_sell} SELLS{Style.RESET_ALL}")
    
    # Check if we actually added any signals
    if tg_signals:
        # Sort by |pct| descending
        tg_signals.sort(key=lambda x: x[0], reverse=True)
        
        # Build final message
        tg_lines = tg_header + [line for _, line in tg_signals]
        
        # Add Date footer
        import datetime
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        tg_lines.append("")
        tg_lines.append(f"⏰ {now_str} Paris")
        
        send_telegram_message("\n".join(tg_lines))
    else:
        print("No active signals passed filters to send to Telegram.")

if __name__ == "__main__":
    main()
