#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TRIPLE G INDICATORS (ICH + EMA GAP)
Basé sur le PineScript "triple_G"
Analyse H1 et Daily pour chaque paire via TradingView WebSocket (Données OANDA).
"""

import sys
import subprocess
import importlib

def install_and_import(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        return importlib.import_module(import_name)
    except ImportError:
        # print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name)

# Installer les dépendances critiques avant les imports globaux
# Auto-install dependencies fix for GitHub Actions
install_and_import("pandas")
install_and_import("numpy")
install_and_import("requests")
install_and_import("python-dotenv", "dotenv")
install_and_import("websocket-client", "websocket")
install_and_import("yfinance")

import pandas as pd
import numpy as np
import time
import json
import requests
import os
import random
import string
import yfinance as yf
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from websocket import create_connection

# Charger les variables d'environnement
load_dotenv()

# Force UTF-8 output for Windows console
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
# Liste des paires (Format Yahoo conservé pour compatibilité, converti en interne)
PAIRS = [
    "EURUSD=X", "EURGBP=X", "EURJPY=X", "EURCHF=X", "EURAUD=X", "EURCAD=X", "EURNZD=X",
    "GBPCHF=X", "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPUSD=X", "GBPJPY=X",
    "NZDCHF=X", "NZDCAD=X", "NZDUSD=X", "NZDJPY=X",
    "USDCHF=X", "USDJPY=X", "USDCAD=X",
    "AUDCHF=X", "AUDCAD=X", "AUDUSD=X", "AUDJPY=X", "AUDNZD=X",
    "CHFJPY=X", "CADJPY=X", "CADCHF=X"
]

# EMA Gap Settings
EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]
EMA_RSI_LEN = 7
EMA_EMA_LEN = 7

# Runner History Settings
RUNNER_HISTORY_FILE = Path(__file__).parent / "runner_history.json"
RUNNER_HISTORY_MAX = 21  # Nombre max de points conservés par paire
RUNNER_RSI_LEN = 7  # Période RSI pour le RUNNER

# Telegram Settings
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message):
    """Envoie un message via Telegram Bot API."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram credentials non configurés.")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, data=payload, timeout=10)
        return response.json().get("ok", False)
    except Exception as e:
        print(f"⚠️ Erreur Telegram: {e}")
        return False

def get_clean_pair_name(ticker):
    return ticker.replace("=X", "")

# --- DATA ENGINE (TV + YAHOO FALLBACK + CACHE) ---
CACHE_FILE = "market_cache.pkl"
GLOBAL_CACHE = None

def load_global_cache():
    global GLOBAL_CACHE
    if GLOBAL_CACHE is not None: return
    
    if os.path.exists(CACHE_FILE):
        try:
            import pickle
            with open(CACHE_FILE, "rb") as f:
                GLOBAL_CACHE = pickle.load(f)
            # print(f"📦 Cache chargé ({len(GLOBAL_CACHE)} paires)")
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
        # Mapping Intervalles Yahoo
        # "1h" -> "1h", "1d" -> "1d", "1wk" -> "1wk"
        # Période : on doit estimer une période suffisante pour n_candles
        period = "1y"
        if interval_code == "1h": period = "1mo"
        elif interval_code == "1wk": period = "2y"
        
        df = yf.Ticker(pair_yahoo).history(period=period, interval=interval_code)
        if df.empty: return None
        
        # Renommer index si nécessaire (yfinance met déjà Title Case pour colonnes)
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
    
    # Mapping Intervalles
    tv_interval = "60"
    if interval_code == "1h": tv_interval = "60"
    elif interval_code == "1d": tv_interval = "D"
    elif interval_code == "1wk": tv_interval = "W"
    
    ws = None
    extracted_df = None
    
    try:
        # Headers pour simuler un navigateur
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
        # Le cache stocke des dicts { "1h": df, "1d": df, ... }
        cache_data = GLOBAL_CACHE[pair_yahoo]
        if interval_code in cache_data:
            return cache_data[interval_code]
            
    # 1. TradingView WebSocket
    df = fetch_data_tv(pair_yahoo, interval_code, n_candles)
    if df is not None and not df.empty:
        return df
    
    # 2. Yahoo Finance (Fallback)
    return fetch_data_yahoo(pair_yahoo, interval_code, n_candles)

def get_current_price_smart(pair_yahoo):
    """
    Récupère juste le dernier prix (Close) via smart fetch.
    """
    df = fetch_data_smart(pair_yahoo, "1h", n_candles=10)
    if df is not None and not df.empty:
        return df['Close'].iloc[-1]
    return None

# --- RUNNER HISTORY FUNCTIONS ---

# Opportunity Stats Settings
STATS_FILE = Path(__file__).parent / "opportunity_stats.json"
INITIAL_CAPITAL = 10000.0  # Capital de départ simulé

# Spreads réalistes (Compte Standard moyen - Source Confirmée)
SPREADS = {
    "EURUSD": 1.1, "USDJPY": 1.5, "GBPUSD": 1.5, "AUDUSD": 1.3,
    "USDCHF": 1.9, "USDCAD": 1.7, "NZDUSD": 1.8,
    "EURGBP": 1.8, "EURJPY": 2.0, "EURCHF": 2.0, "EURAUD": 2.5,
    "EURCAD": 2.4, "EURNZD": 3.2,
    "GBPJPY": 2.6, "GBPCHF": 2.8, "GBPAUD": 3.1, "GBPCAD": 3.1,
    "GBPNZD": 4.0,
    "AUDJPY": 2.2, "CHFJPY": 2.4, "CADJPY": 2.2, "NZDJPY": 2.4,
    "AUDCAD": 2.4, "AUDCHF": 2.4, "AUDNZD": 3.1, "CADCHF": 2.5,
    "NZDCAD": 2.8, "NZDCHF": 2.8
}
DEFAULT_SPREAD = 2.5

def calculate_cost_percentage(pair, price, spread_pips):
    """Calcule le coût du spread en pourcentage."""
    if price == 0: return 0.0
    pip_val = 0.01 if "JPY" in pair else 0.0001
    cost_val = spread_pips * pip_val
    return (cost_val / price) * 100.0

def load_stats():
    """Charge les stats de trading."""
    if STATS_FILE.exists():
        try:
            with open(STATS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {
        "active_trades": {},
        "closed_trades": [],
        "performance": {"total_pnl": 0.0, "wins": 0, "losses": 0}
    }

def save_stats(stats):
    """Sauvegarde les stats."""
    try:
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
    except Exception as e:
        print(f"⚠️ Erreur sauvegarde stats: {e}")

def get_current_price_from_results(pair, results):
    """Récupère le prix live via TV/Yahoo."""
    # Note: 'pair' est ici le nom propre (sans =X) ex: EURUSD
    ticker = f"{pair}=X"
    return get_current_price_smart(ticker)

def get_paris_time():
    """Retourne l'heure actuelle à Paris (aware datetime)."""
    import pytz
    paris_tz = pytz.timezone('Europe/Paris')
    return datetime.now(paris_tz)

def update_performance_tracking(big3_pairs, confluence_pairs, top_5_short, top_5_large):
    """
    Gère le tracking, les horaires de trading et le calcul Portefeuille.
    """
    stats = load_stats()
    paris_now = get_paris_time()
    
    is_weekday = paris_now.weekday() < 5
    current_hour = paris_now.hour
    
    can_trade_new = is_weekday and (6 <= current_hour < 22)
    force_close_all = current_hour >= 22 or not is_weekday
    
    entry_pool = set(p['pair'] for p in big3_pairs + confluence_pairs)
    retention_pool = set(p['pair'] for p in big3_pairs + confluence_pairs + top_5_short + top_5_large)
    
    to_close = []
    
    for pair, trade in stats["active_trades"].items():
        should_close = False
        reason = ""
        
        if pair not in retention_pool:
            should_close = True
            reason = "Signal technique (Sortie screener global)"
            
        if force_close_all:
            should_close = True
            reason = "Clôture journalière (22h)"
            
        if should_close:
            current_price = get_current_price_from_results(pair, [])
            if current_price:
                to_close.append((pair, current_price, reason))
    
    for pair, exit_price, reason in to_close:
        trade = stats["active_trades"][pair]
        entry_price = trade["entry_price"]
        direction = trade["direction"]
        
        pct_change = ((exit_price - entry_price) / entry_price) * 100
        if direction == "SHORT":
            pct_change = -pct_change
            
        spread = SPREADS.get(pair, DEFAULT_SPREAD)
        cost_pct = calculate_cost_percentage(pair, entry_price, spread)
        
        net_pnl = pct_change - cost_pct
        
        stats["performance"]["total_pnl"] += net_pnl
        if net_pnl > 0:
            stats["performance"]["wins"] += 1
        else:
            stats["performance"]["losses"] += 1
            
        closed_trade = {
            "pair": pair,
            "entry_date": trade["entry_date"],
            "exit_date": paris_now.strftime("%Y-%m-%d %H:%M"),
            "entry_price": entry_price,
            "exit_price": exit_price,
            "direction": direction,
            "pnl_net": round(net_pnl, 2),
            "spread_cost_pct": round(cost_pct, 4),
            "reason": reason
        }
        stats["closed_trades"].append(closed_trade)
        del stats["active_trades"][pair]
        
        print(f"💰 Trade fermé ({reason}): {pair} ({direction}) PnL: {net_pnl:+.2f}%")

    if can_trade_new:
        for p_data in big3_pairs + confluence_pairs:
            pair = p_data['pair']
            if pair not in stats["active_trades"]:
                direction = "LONG" if p_data['pct'] >= 0 else "SHORT"
                price = get_current_price_from_results(pair, [])
                if price:
                    stats["active_trades"][pair] = {
                        "entry_date": paris_now.strftime("%Y-%m-%d %H:%M"),
                        "entry_price": price,
                        "direction": direction,
                        "initial_runner": p_data['pct']
                    }
                    print(f"💰 Nouveau trade: {pair} ({direction}) @ {price:.4f}")
    else:
        if force_close_all:
            print("🚫 Créneau fermé (After 22h / Weekend). Pas de nouvelles positions.")
        else:
            print(f"⏸️ Avant 06h (Il est {current_hour}h). Monitoring seulement.")
    
    save_stats(stats)
    
    total_pnl_pct = stats["performance"]["total_pnl"]
    current_capital = INITIAL_CAPITAL * (1 + total_pnl_pct / 100)
    total_accumulated_eur = current_capital - INITIAL_CAPITAL
    
    emoji_capital = "✅" if total_accumulated_eur >= 0 else "❌"
    
    active_pnl_eur = 0.0
    active_count = len(stats["active_trades"])
    
    for pair, trade in stats["active_trades"].items():
         current_price = get_current_price_from_results(pair, [])
         if current_price:
             pct = ((current_price - trade["entry_price"]) / trade["entry_price"]) * 100
             if trade["direction"] == "SHORT": pct = -pct
             
             spread = SPREADS.get(pair, DEFAULT_SPREAD)
             cost_pct = calculate_cost_percentage(pair, trade["entry_price"], spread)
             pct -= cost_pct
             
             active_pnl_eur += (pct / 100) * INITIAL_CAPITAL
    
    h24_pnl_eur = 0.0
    h24_count = 0
    now = datetime.now()
    for trade in stats["closed_trades"]:
        exit_dt = datetime.strptime(trade["exit_date"], "%Y-%m-%d %H:%M")
        if (now - exit_dt).total_seconds() < 24 * 3600:
            pnl_val = (trade["pnl_net"] / 100) * INITIAL_CAPITAL
            h24_pnl_eur += pnl_val
            h24_count += 1
            
    return {
        "current_capital": current_capital,
        "total_pnl_pct": total_pnl_pct,
        "capital_emoji": emoji_capital,
        "active_pnl_eur": active_pnl_eur,
        "active_count": active_count,
        "h24_pnl_eur": h24_pnl_eur,
        "h24_count": h24_count,
        "total_accumulated_eur": total_accumulated_eur
    }


def load_runner_history():
    """
    Charge l'historique RUNNER depuis le fichier JSON.
    Réinitialise automatiquement si c'est un nouveau jour (nouvelle bougie daily).
    """
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    
    if RUNNER_HISTORY_FILE.exists():
        try:
            with open(RUNNER_HISTORY_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            last_date = data.get("_last_date", "")
            if last_date != today:
                print(f"🔄 Nouvelle bougie daily détectée ({last_date} → {today}). Réinitialisation de l'historique RUNNER.")
                return {"_last_date": today}
            
            return data
        except:
            return {"_last_date": today}
    return {"_last_date": today}

def save_runner_history(history):
    """Sauvegarde l'historique RUNNER."""
    from datetime import datetime
    history["_last_date"] = datetime.now().strftime("%Y-%m-%d")
    
    for pair in history:
        if pair.startswith("_"): continue
        if len(history[pair]) > RUNNER_HISTORY_MAX:
            history[pair] = history[pair][-RUNNER_HISTORY_MAX:]
    
    with open(RUNNER_HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, indent=2)

def calculate_runner_rsi_from_list(values, period=7):
    """Calcule le RSI sur une liste de valeurs RUNNER."""
    if len(values) < period + 1:
        return None
    
    series = pd.Series(values)
    delta = series.diff()
    
    up = delta.copy()
    down = delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    down = abs(down)
    
    alpha = 1.0 / period
    roll_up = up.ewm(alpha=alpha, adjust=False).mean()
    roll_down = down.ewm(alpha=alpha, adjust=False).mean()
    
    rs = roll_up / roll_down
    rsi = 100.0 - (100.0 / (1.0 + rs))
    
    return rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else None

# --- INDICATEURS TECHNIQUES ---

def calculate_rsi_series(series, period=14):
    """Calcule le RSI sur une série arbitraire."""
    delta = series.diff()
    up = delta.copy()
    down = delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    down = abs(down)
    
    alpha = 1.0 / period
    roll_up = up.ewm(alpha=alpha, adjust=False).mean()
    roll_down = down.ewm(alpha=alpha, adjust=False).mean()
    
    rs = roll_up / roll_down
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def calculate_ema_gap_status(df):
    """Reproduit la logique EMA Gap du PineScript."""
    if df is None or len(df) < max(EMA_LENGTHS) + EMA_RSI_LEN + EMA_EMA_LEN:
        return "NEUTRAL"
        
    close = df['Close']
    emas = []
    for length in EMA_LENGTHS:
        emas.append(close.ewm(span=length, adjust=False).mean())
        
    diffs = []
    for i in range(len(emas) - 1):
        diffs.append(emas[i] - emas[i+1])
        
    sum_diffs = sum(diffs)
    avgGapSigned = sum_diffs / 7.0
    
    rsiAvgGap = calculate_rsi_series(avgGapSigned, EMA_RSI_LEN)
    emaRsi8 = rsiAvgGap.ewm(span=EMA_EMA_LEN, adjust=False).mean()
    
    last_rsi = rsiAvgGap.iloc[-1]
    last_ema = emaRsi8.iloc[-1]
    
    if last_ema < last_rsi:
        return "BULLISH 🟢"
    elif last_ema > last_rsi:
        return "BEARISH 🔴"
    else:
        return "NEUTRAL ⚪"

def calculate_ema_aligned_status(df):
    """Vérifie si les 8 EMAs sont parfaitement alignées."""
    if df is None or len(df) < max(EMA_LENGTHS):
        return "NEUTRAL ⚪"
    
    close = df['Close']
    emas = [close.ewm(span=l, adjust=False).mean().iloc[-1] for l in EMA_LENGTHS]
    
    is_bull = all(emas[i] > emas[i+1] for i in range(len(emas)-1))
    if is_bull: return "BULLISH 🟢"
        
    is_bear = all(emas[i] < emas[i+1] for i in range(len(emas)-1))
    if is_bear: return "BEARISH 🔴"
        
    return "NEUTRAL ⚪"

def analyze_pair(ticker):
    pair_name = get_clean_pair_name(ticker)
    
    # Fetch Data via WebSocket TV (Single Shot)
    # H1 sur 500 bougies (~1 mois)
    df_h1 = fetch_data_smart(ticker, "1h", n_candles=500)
    # Daily sur 300 bougies (~1 an)
    df_d = fetch_data_smart(ticker, "1d", n_candles=300)
    # Weekly sur 150 bougies (~3 ans)
    df_w = fetch_data_smart(ticker, "1wk", n_candles=150)
    
    # --- ANALYSE SHORT (H1 + D1) ---
    align_h1 = calculate_ema_aligned_status(df_h1)
    ema_d = calculate_ema_gap_status(df_d)
    
    # --- ANALYSE LARGE (D1 + W1) ---
    align_d = calculate_ema_aligned_status(df_d)
    ema_w = calculate_ema_gap_status(df_w)
    
    # Calcul Smart PCT (Variation Daily) - Méthode asian_scanner1.py
    pct = 0.0
    if df_d is not None and not df_d.empty:
        daily_open = df_d['Open'].iloc[-1]
        
        # Utiliser le dernier Close H1 comme prix actuel (comme asian_scanner1.py)
        if df_h1 is not None and not df_h1.empty:
            current_price = df_h1['Close'].iloc[-1]
        else:
            current_price = df_d['Close'].iloc[-1]
        
        if daily_open > 0:
            pct = ((current_price - daily_open) / daily_open) * 100.0

    return {
        "pair": pair_name,
        # Short
        "h1_align": align_h1,
        "d_ema": ema_d,
        # Large
        "d_align": align_d,
        "w_ema": ema_w,
        # Common
        "pct": pct
    }

def main():
    print(f"🚀 TRIPLE G INDICATORS SCAN (WebSocket TV Edition)")
    print(f"Lancement de l'analyse sur {len(PAIRS)} paires...\n")
    
    runner_history = load_runner_history()
    start_time = time.time()
    results = []
    
    # Multithreading (très efficace car I/O bound avec le socket)
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pair = {executor.submit(analyze_pair, pair): pair for pair in PAIRS}
        for future in as_completed(future_to_pair):
            try:
                res = future.result()
                results.append(res)
                print(f".", end="", flush=True)
            except Exception as e:
                # print(e)
                print(f"x", end="", flush=True)
    
    # Mise à jour et sauvegarde historique RUNNER
    for r in results:
        pair = r['pair']
        if pair not in runner_history:
            runner_history[pair] = []
        runner_history[pair].append(r['pct'])
    
    save_runner_history(runner_history)
    
    # Calcul RSI RUNNER
    rsi_runner_data = []
    for r in results:
        pair = r['pair']
        history_values = runner_history.get(pair, [])
        rsi_value = calculate_runner_rsi_from_list(history_values, RUNNER_RSI_LEN)
        rsi_runner_data.append({
            "pair": pair,
            "pct": r['pct'],
            "rsi_runner": rsi_value,
            "history_len": len(history_values)
        })
                
    results.sort(key=lambda x: abs(x['pct']), reverse=True)
    
    # --- FILTRAGE SHORT VIEW ---
    short_aligned_runners = []
    for r in results:
        if "NEUTRAL" in r['h1_align'] or "NEUTRAL" in r['d_ema']: continue
        if abs(r['pct']) <= 0.2: continue  # Seuil 0.2% comme asian_scanner1.py
        
        h1 = r['h1_align'].split()[0]
        d1 = r['d_ema'].split()[0]
        
        if h1 == "BULLISH" and r['pct'] < 0: continue
        if h1 == "BEARISH" and r['pct'] > 0: continue
        
        if h1 == d1:
            short_aligned_runners.append(r)
            
    top_5_short = short_aligned_runners[:5]
    
    # --- FILTRAGE LARGE VIEW ---
    large_aligned_runners = []
    for r in results:
        if "NEUTRAL" in r['d_align'] or "NEUTRAL" in r['w_ema']: continue
        if abs(r['pct']) <= 0.2: continue  # Seuil 0.2% comme asian_scanner1.py
        
        d1 = r['d_align'].split()[0]
        w1 = r['w_ema'].split()[0]
        
        if d1 == "BULLISH" and r['pct'] < 0: continue
        if d1 == "BEARISH" and r['pct'] > 0: continue
        
        if d1 == w1:
            large_aligned_runners.append(r)
            
    top_5_large = large_aligned_runners[:5]
    
    # --- RSI Runner Aligned ---
    aligned_pairs = set(r['pair'] for r in short_aligned_runners + large_aligned_runners)
    rsi_runner_aligned = [r for r in rsi_runner_data 
                          if r['pair'] in aligned_pairs 
                          and r['rsi_runner'] is not None 
                          and r['rsi_runner'] > 55]
    
    rsi_runner_aligned.sort(key=lambda x: -x['rsi_runner'])
    top_5_rsi_runner = rsi_runner_aligned[:5]
    
    print(f"\n\n⏱️ Terminé en {time.time() - start_time:.2f}s")
    
    # --- AFFICHAGE ---
    print(f"\n📊 TOP 5 RSI RUNNER")
    print(f"{'PAIRE':<10} | {'RUNNER':<10} | {'RSI_7':<8}")
    print("-" * 35)
    has_rsi = any(x['rsi_runner'] is not None for x in rsi_runner_data)
    if not top_5_rsi_runner:
        if not has_rsi:
            print(f"⏳ Pas assez de données (min {RUNNER_RSI_LEN + 1} exécutions requises)")
        else:
            print("Aucune paire alignée avec RSI > 55.")
    else:
        for r in top_5_rsi_runner:
            pct_str = f"{r['pct']:+.2f}%"
            rsi_str = f"{r['rsi_runner']:.1f}" if r['rsi_runner'] is not None else "N/A"
            print(f"{r['pair']:<10} | {pct_str:<10} | {rsi_str:<8}")
    
    print("\n")
    
    print(f"✅ TOP 5 SHORT")
    print(f"{'PAIRE':<12} | {'RUNNER':<10}")
    print("-" * 27)
    if not top_5_short:
        print("Aucune paire alignée.")
    else:
        for r in top_5_short:
            pct_str = f"{r['pct']:+.2f}%"
            emoji = "🟢" if r['pct'] > 0 else "🔴"
            print(f"{emoji} {r['pair']:<10} | {pct_str:<10}")

    print("\n")

    print(f"✅ TOP 5 LARGE")
    print(f"{'PAIRE':<12} | {'RUNNER':<10}")
    print("-" * 27)
    if not top_5_large:
        print("Aucune paire alignée.")
    else:
        for r in top_5_large:
            pct_str = f"{r['pct']:+.2f}%"
            emoji = "🟢" if r['pct'] > 0 else "🔴"
            print(f"{emoji} {r['pair']:<10} | {pct_str:<10}")

    print("\n")

    # --- CONFLUENCE ---
    short_pairs = set(r['pair'] for r in top_5_short)
    large_pairs = set(r['pair'] for r in top_5_large)
    confluence_pairs = short_pairs & large_pairs
    
    confluence_runners = [r for r in results if r['pair'] in confluence_pairs]
    confluence_runners.sort(key=lambda x: abs(x['pct']), reverse=True)
    
    print(f"⭐ CONFLUENCE (SHORT ∩ LARGE)")
    print(f"{'PAIRE':<12} | {'RUNNER':<10}")
    print("-" * 27)
    if not confluence_runners:
        print("Aucune paire en confluence.")
    else:
        for r in confluence_runners:
            pct_str = f"{r['pct']:+.2f}%"
            emoji = "🟢" if r['pct'] > 0 else "🔴"
            print(f"{emoji} {r['pair']:<10} | {pct_str:<10}")

    print("\n")

    # --- BIG3 ---
    all_aligned_pairs = {r['pair']: r for r in top_5_short}
    for r in top_5_large:
        if r['pair'] not in all_aligned_pairs:
            all_aligned_pairs[r['pair']] = r
    
    big3_runners = sorted(all_aligned_pairs.values(), key=lambda x: abs(x['pct']), reverse=True)[:3]
    
    print(f"✅ BIG 3")
    print(f"{'PAIRE':<12} | {'RUNNER':<10}")
    print("-" * 27)
    if not big3_runners:
        print("Aucune paire.")
    else:
        for r in big3_runners:
            pct_str = f"{r['pct']:+.2f}%"
            emoji = "🟢" if r['pct'] > 0 else "🔴"
            print(f"{emoji} {r['pair']:<10} | {pct_str:<10}")

    # --- ENVOI TELEGRAM & TRACKING ---
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    msg_lines = ["🚀 TRIPLE G SCAN 🚀", ""]
    
    big3_pairs_set = set(r['pair'] for r in big3_runners)
    confluence_pairs_set = set(r['pair'] for r in confluence_runners)
    common_pairs = big3_pairs_set.intersection(confluence_pairs_set)

    msg_lines.append("✅ BIG 3")
    for r in big3_runners:
        emoji = "🟢" if r['pct'] > 0 else "🔴"
        mark = "🔥" if r['pair'] in common_pairs else ""
        msg_lines.append(f"{emoji}{mark}{r['pair']} ({r['pct']:+.1f}%)")
    msg_lines.append("")
    
    msg_lines.append("⭐ CONFLUENCE")
    if confluence_runners:
        for r in confluence_runners:
            emoji = "🟢" if r['pct'] > 0 else "🔴"
            mark = "🔥" if r['pair'] in common_pairs else ""
            msg_lines.append(f"{emoji}{mark}{r['pair']} ({r['pct']:+.1f}%)")
    else:
        msg_lines.append("Aucune")
    
    msg_lines.append("")
    
    # Tracking Performance
    perf = update_performance_tracking(big3_runners, confluence_runners, top_5_short, top_5_large)
    
    msg_lines.append("")
    msg_lines.append("💰 PORTEFEUILLE")
    msg_lines.append("-------------------------")
    msg_lines.append(f"   Capital : {int(perf['current_capital']):,} EUR ({perf['total_pnl_pct']:+.1f}%) {perf['capital_emoji']}")
    msg_lines.append("")
    msg_lines.append(f"   En cours : {int(perf['active_pnl_eur']):+} EUR ({perf['active_count']} pos)")
    msg_lines.append(f"   Fermées 24h : {int(perf['h24_pnl_eur']):+} EUR ({perf['h24_count']} trades)")
    msg_lines.append(f"   Total accumulé : {int(perf['total_accumulated_eur']):+} EUR")
    
    msg_lines.append("")
    msg_lines.append(f"⏰ {now} Paris")
    
    telegram_msg = "\n".join(msg_lines)
    
    if send_telegram_message(telegram_msg):
        print("\n✅ Message Telegram envoyé!")
    else:
        print("\n⚠️ Échec envoi Telegram")

if __name__ == "__main__":
    main()

