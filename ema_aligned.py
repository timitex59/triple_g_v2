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
np = install_and_import("numpy")
colorama = install_and_import("colorama")
websocket = install_and_import("websocket-client", "websocket")
install_and_import("python-dotenv", "dotenv")

from websocket import create_connection
from colorama import Fore, Style, init
from dotenv import load_dotenv
init(autoreset=True)

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

EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]

# Optimal score range based on backtest (best performance: 10-15)
SCORE_MIN = 10
SCORE_MAX = 15

# --- TV HELPERS ---
def generate_session_id():
    return "cs_" + ''.join(random.choices(string.ascii_letters + string.digits, k=12))

def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"

def fetch_data_tv(pair_yahoo, interval_code, n_candles=300):
    """Fetches data from TradingView via WebSocket."""
    clean_pair = pair_yahoo.replace("=X", "")
    tv_pair = f"OANDA:{clean_pair}"
    
    tv_interval = "60"
    if interval_code == "1w": tv_interval = "1W"
    elif interval_code == "1d": tv_interval = "D"
    elif interval_code == "1h": tv_interval = "60"
    
    ws = None
    extracted_df = None
    
    try:
        headers = {
            "Origin": "https://www.tradingview.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
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
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = yf.Ticker(pair_yahoo).history(period=period, interval=interval_code)
            if df.empty: return None
            return df
    except Exception:
        return None

def fetch_data_smart(pair, interval, period_for_yahoo):
    df = fetch_data_tv(pair, interval, n_candles=200)
    if df is not None and not df.empty:
        return df, "TV"
    df = fetch_data_yahoo(pair, interval, period_for_yahoo)
    if df is not None:
        return df, "YF"
    return None, "NA"

# --- EMA RATIO CALCULATION ---
def calculate_ema_ratios(df):
    """
    Calculate ratios between successive EMAs.
    Returns list of ratios: [EMA20/EMA25, EMA25/EMA30, ...]
    """
    if df is None or len(df) < max(EMA_LENGTHS):
        return None
    
    close = df['Close']
    emas = [close.ewm(span=l, adjust=False).mean().iloc[-1] for l in EMA_LENGTHS]
    
    # Calculate ratios between successive EMAs
    ratios = []
    for i in range(len(emas) - 1):
        ratio = emas[i] / emas[i + 1]
        ratios.append(ratio)
    
    return ratios

def calculate_similarity_score(ratios_h1, ratios_daily):
    """
    Calculate similarity score between H1 and Daily EMA ratios.
    Lower score = more similar = better alignment.
    """
    if ratios_h1 is None or ratios_daily is None:
        return None, None
    
    # Calculate absolute differences for each ratio pair
    differences = []
    for r_h1, r_d in zip(ratios_h1, ratios_daily):
        diff = abs(r_h1 - r_d)
        differences.append(diff)
    
    # Average difference (similarity score)
    avg_diff = sum(differences) / len(differences)
    
    # Convert to basis points for readability (multiply by 10000)
    score_bps = avg_diff * 10000
    
    return score_bps, differences

def get_trend_direction(df):
    """Determine trend direction based on EMA alignment."""
    if df is None or len(df) < max(EMA_LENGTHS):
        return "NEUTRAL"
    
    close = df['Close']
    emas = [close.ewm(span=l, adjust=False).mean().iloc[-1] for l in EMA_LENGTHS]
    
    is_bull = all(emas[i] > emas[i+1] for i in range(len(emas)-1))
    if is_bull:
        return "BULL"
    
    is_bear = all(emas[i] < emas[i+1] for i in range(len(emas)-1))
    if is_bear:
        return "BEAR"
    
    return "NEUTRAL"

def is_price_above_emas(df):
    """Check if current price is above all EMAs (for BULL confirmation)."""
    if df is None or len(df) < max(EMA_LENGTHS):
        return False
    
    close = df['Close'].iloc[-1]
    emas = [df['Close'].ewm(span=l, adjust=False).mean().iloc[-1] for l in EMA_LENGTHS]
    
    return close > max(emas)

def is_price_below_emas(df):
    """Check if current price is below all EMAs (for BEAR confirmation)."""
    if df is None or len(df) < max(EMA_LENGTHS):
        return False
    
    close = df['Close'].iloc[-1]
    emas = [df['Close'].ewm(span=l, adjust=False).mean().iloc[-1] for l in EMA_LENGTHS]
    
    return close < min(emas)

def calculate_pct_runner(df):
    """Calculate %RUNNER (daily % change from Open to Close)."""
    if df is None or len(df) < 1:
        return 0.0
    
    open_price = df['Open'].iloc[-1]
    close_price = df['Close'].iloc[-1]
    
    if open_price > 0:
        return ((close_price - open_price) / open_price) * 100
    return 0.0

# --- WORKER FUNCTION ---
def process_pair(pair):
    clean_pair = pair.replace("=X", "")
    
    # Fetch Data
    df_1d, src_d = fetch_data_smart(pair, "1d", "1y")
    df_1h, src_h = fetch_data_smart(pair, "1h", "1mo")
    
    # Calculate EMA ratios
    ratios_daily = calculate_ema_ratios(df_1d)
    ratios_h1 = calculate_ema_ratios(df_1h)
    
    # Calculate similarity score
    score, differences = calculate_similarity_score(ratios_h1, ratios_daily)
    
    # Get trend direction (based on Daily)
    trend_daily = get_trend_direction(df_1d)
    trend_h1 = get_trend_direction(df_1h)
    
    # Check price position relative to EMAs
    price_above_emas_daily = is_price_above_emas(df_1d)
    price_below_emas_daily = is_price_below_emas(df_1d)
    
    # Calculate %RUNNER
    pct_runner = calculate_pct_runner(df_1d)
    
    return {
        "pair": pair,
        "clean_pair": clean_pair,
        "ratios_daily": ratios_daily,
        "ratios_h1": ratios_h1,
        "score": score,
        "differences": differences,
        "trend_daily": trend_daily,
        "trend_h1": trend_h1,
        "price_above_emas": price_above_emas_daily,
        "price_below_emas": price_below_emas_daily,
        "pct_runner": pct_runner
    }

# --- TELEGRAM ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram credentials not found.")
        return
    
    requests = install_and_import("requests")
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        requests.post(url, json=payload, timeout=10)
        print("📤 Telegram message sent.")
    except Exception as e:
        print(f"⚠️ Error sending Telegram message: {e}")

def main():
    print(f"\n{Style.BRIGHT}📊 DAILY/HOURLY EMA ALIGNMENT SCANNER V2{Style.RESET_ALL}")
    print(f"Optimal score range: {SCORE_MIN}-{SCORE_MAX} (based on backtest)")
    print(f"Filters: EMAs aligned + Price above/below EMAs")
    print("-" * 80)
    
    start_time = time.time()
    
    # Process all pairs
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pair = {executor.submit(process_pair, pair): pair for pair in PAIRS}
        for future in as_completed(future_to_pair):
            try:
                res = future.result()
                if res['score'] is not None:
                    results.append(res)
            except Exception:
                pass
    
    # Filter: only keep pairs where:
    # 1. EMAs are aligned on BOTH timeframes (same direction)
    # 2. Price is correctly positioned relative to EMAs
    aligned_results = []
    for res in results:
        trend_d = res['trend_daily']
        trend_h1 = res['trend_h1']
        
        # Both must be BULL or both must be BEAR
        if trend_d == "BULL" and trend_h1 == "BULL":
            # For BULL: price must be above all EMAs
            if res['price_above_emas']:
                aligned_results.append(res)
        elif trend_d == "BEAR" and trend_h1 == "BEAR":
            # For BEAR: price must be below all EMAs
            if res['price_below_emas']:
                aligned_results.append(res)
    
    # Sort by score (highest = strongest momentum)
    aligned_results.sort(key=lambda x: x['score'], reverse=True)
    
    # Separate into optimal and other signals
    optimal_signals = [r for r in aligned_results if SCORE_MIN <= r['score'] <= SCORE_MAX]
    other_signals = [r for r in aligned_results if r['score'] < SCORE_MIN or r['score'] > SCORE_MAX]
    
    print(f"\n{Style.BRIGHT}🎯 OPTIMAL SIGNALS (Score {SCORE_MIN}-{SCORE_MAX}){Style.RESET_ALL}")
    print(f"{'PAIR':<10} {'SCORE':<10} {'TREND':<8} {'%RUNNER':<12} {'QUALITY':<15}")
    print("-" * 60)
    
    tg_lines = ["📊 <b>EMA ALIGNMENT V2</b>", ""]
    
    if not optimal_signals:
        print("No optimal signals found.")
        tg_lines.append("No optimal signals (10-15).")
    
    for res in optimal_signals:
        clean_pair = res['clean_pair']
        score = res['score']
        trend_d = res['trend_daily']
        pct_runner = res['pct_runner']
        
        # Quality based on score within optimal range
        if 10 <= score <= 12:
            quality = "⭐⭐⭐ BEST"
            quality_color = Fore.GREEN + Style.BRIGHT
        else:
            quality = "⭐⭐ GOOD"
            quality_color = Fore.GREEN
        
        # Trend color
        trend_color = Fore.GREEN if trend_d == "BULL" else Fore.RED
        
        # %RUNNER color
        runner_color = Fore.GREEN if pct_runner > 0 else Fore.RED if pct_runner < 0 else Fore.WHITE
        
        print(f"{clean_pair:<10} {score:<10.1f} {trend_color}{trend_d:<8}{Style.RESET_ALL} {runner_color}{pct_runner:+.2f}%{Style.RESET_ALL}      {quality_color}{quality}{Style.RESET_ALL}")
        
        # Telegram
        emoji = "🟢" if trend_d == "BULL" else "🔴"
        tg_lines.append(f"{emoji} {clean_pair} ({pct_runner:+.2f}%)")
    
    # Show other signals if any
    if other_signals:
        print(f"\n{Style.BRIGHT}📋 OTHER SIGNALS{Style.RESET_ALL}")
        print(f"{'PAIR':<10} {'SCORE':<10} {'TREND':<8} {'%RUNNER':<12}")
        print("-" * 45)
        
        for res in other_signals[:10]:  # Limit to 10
            clean_pair = res['clean_pair']
            score = res['score']
            trend_d = res['trend_daily']
            pct_runner = res['pct_runner']
            
            trend_color = Fore.GREEN if trend_d == "BULL" else Fore.RED
            runner_color = Fore.GREEN if pct_runner > 0 else Fore.RED if pct_runner < 0 else Fore.WHITE
            
            print(f"{clean_pair:<10} {score:<10.1f} {trend_color}{trend_d:<8}{Style.RESET_ALL} {runner_color}{pct_runner:+.2f}%{Style.RESET_ALL}")
    
    print("-" * 60)
    print(f"Total aligned: {len(aligned_results)} | Optimal: {len(optimal_signals)} | Other: {len(other_signals)}")
    print(f"⏱️ Done in {time.time() - start_time:.2f}s")
    
    # Send Telegram only if optimal signals exist
    if optimal_signals:
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        tg_lines.append("")
        tg_lines.append(f"⏰ {now_str} Paris")
        send_telegram_message("\n".join(tg_lines))
    else:
        print("No Telegram sent (no optimal signals).")

if __name__ == "__main__":
    main()
