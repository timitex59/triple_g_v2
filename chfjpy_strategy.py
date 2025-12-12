#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CHF/JPY RUNNER RANKING
======================
Affiche uniquement le classement des runners (variation journalière)
pour toutes les paires impliquant CHF ou JPY.
Source Données : TradingView WebSocket (OANDA)
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
install_and_import("pandas")
install_and_import("requests")
install_and_import("python-dotenv", "dotenv")
install_and_import("websocket-client", "websocket")
install_and_import("yfinance")
install_and_import("pytz")

import time
import json
import random
import string
import os
import requests
import pandas as pd
import yfinance as yf
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
from websocket import create_connection
from dotenv import load_dotenv

# Import EMA indicators from triple_g_indicators
from triple_g_indicators import calculate_ema_gap_status, calculate_ema_aligned_status, fetch_data_smart

# Charger les variables d'environnement
load_dotenv()

# Force UTF-8 output for Windows console
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# --- TELEGRAM CONFIGURATION ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Fallback manuel si .env non chargé ou inexistant (similaire à asian_scanner)
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if line.strip() and not line.startswith("#") and "=" in line:
                    key, value = line.strip().split("=", 1)
                    if key == "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN = value
                    if key == "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID = value

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

# Liste complète des paires (basée sur reference.py)
ALL_PAIRS = [
    "EURUSD=X","EURGBP=X","EURJPY=X","EURCHF=X","EURAUD=X","EURCAD=X","EURNZD=X",
    "GBPCHF=X","GBPAUD=X","GBPCAD=X","GBPNZD=X","GBPUSD=X","GBPJPY=X",
    "NZDCHF=X","NZDCAD=X","NZDUSD=X","NZDJPY=X",
    "USDCHF=X","USDJPY=X","USDCAD=X",
    "AUDCHF=X","AUDCAD=X","AUDUSD=X","AUDJPY=X","AUDNZD=X",
    "CHFJPY=X","CADJPY=X","CADCHF=X"
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

def fetch_pair_data_yahoo(pair_yahoo, debug=False):
    """
    Fallback sur Yahoo Finance via yfinance.
    """
    clean_pair = pair_yahoo.replace("=X", "")
    try:
        # On demande 5 jours pour être sûr d'avoir l'Open Daily
        ticker = yf.Ticker(pair_yahoo)
        df = ticker.history(period="5d", interval="1d")
        if df.empty: return None
        
        # Nettoyage week-end
        if df.index[-1].dayofweek >= 5:
            df = df.iloc[:-1]
            if df.empty: return None
            
        daily_open = df.iloc[-1]['Open']
        daily_close = df.iloc[-1]['Close'] # Close temporaire du jour
        
        # Pour être iso avec TV qui donne le live price :
        current_price = daily_close
        
        if daily_open > 0:
            pct_change = ((current_price - daily_open) / daily_open) * 100
            return {
                "pair": clean_pair,
                "pct": pct_change,
                "close": current_price,
                "open": daily_open,
                "method": "Yahoo_Fallback"
            }
        return None
    except Exception as e:
        if debug: print(f"Yahoo Err {clean_pair}: {e}")
        return None

def fetch_pair_data_tv(pair_yahoo, debug=False):
    """
    Récupère les données Daily via WebSocket TV pour calculer la variation.
    Utilise la dernière bougie Daily (en cours) :
    - Open = Open du jour (00:00 ou 23:00 selon broker, mais TV OANDA est calé standard)
    - Close = Prix actuel temps réel
    """
    clean_pair = pair_yahoo.replace("=X", "")
    tv_pair = f"OANDA:{clean_pair}"
    
    ws = None
    extracted_data = None
    
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
        # On demande juste 2 bougies Daily (Hier et Aujourd'hui)
        ws.send(create_message("create_series", [session_id, "sds_1", "s1", "sds_sym_1", "D", 2, ""]))
        
        start_t = time.time()
        while time.time() - start_t < 5: # Timeout court
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
                        # Dernier élément = bougie en cours
                        last_candle = data[-1]["v"]
                        # Format TV "v": [timestamp, open, high, low, close, volume]
                        extracted_data = {
                            "open": last_candle[1],
                            "close": last_candle[4]
                        }
                
                if "series_completed" in res:
                    break
            except: pass
        
        ws.close()
        
        if extracted_data:
            daily_open = extracted_data["open"]
            current_price = extracted_data["close"]
            
            if daily_open > 0:
                pct_change = ((current_price - daily_open) / daily_open) * 100
                return {
                    "pair": clean_pair,
                    "pct": pct_change,
                    "close": current_price,
                    "open": daily_open,
                    "method": "TV_Socket"
                }
        return None
        
    except Exception as e:
        if debug: print(f"Err {clean_pair}: {e}")
        if ws: 
            try: ws.close()
            except: pass
        return None

def fetch_pair_data_smart(pair_yahoo, debug=False):
    """
    Tente de récupérer les données via Cache > TV > Yahoo.
    """
    clean_pair = pair_yahoo.replace("=X", "")
    
    # 0. Cache Local
    load_global_cache()
    if GLOBAL_CACHE and pair_yahoo in GLOBAL_CACHE:
        cache_data = GLOBAL_CACHE[pair_yahoo]
        if "1d" in cache_data:
            df = cache_data["1d"]
            if df is not None and not df.empty:
                last = df.iloc[-1]
                daily_open = last['Open']
                current_price = last['Close']
                if daily_open > 0:
                    pct_change = ((current_price - daily_open) / daily_open) * 100
                    return {
                        "pair": clean_pair,
                        "pct": pct_change,
                        "close": current_price,
                        "open": daily_open,
                        "method": "Cache_Local"
                    }

    # 1. TradingView WebSocket
    res = fetch_pair_data_tv(pair_yahoo, debug)
    if res:
        return res
        
    # 2. Yahoo Finance
    return fetch_pair_data_yahoo(pair_yahoo, debug)

def get_target_pairs():
    """Filtre les paires pour ne garder que celles avec CHF ou JPY."""
    targets = []
    for p in ALL_PAIRS:
        clean_pair = p.replace("=X", "")
        if "CHF" in clean_pair or "JPY" in clean_pair:
            targets.append(p) # On garde le format yahoo pour la compatibilité fetch
    return targets

def main():
    target_pairs = get_target_pairs()
    clean_targets = [p.replace("=X","") for p in target_pairs]
    
    print(f"\n🚀 ANALYSE RUNNER CHF & JPY ({len(target_pairs)} paires)")
    print("=" * 40)
    print("Récupération des données TradingView (OANDA)...")

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor: # Plus de workers car socket rapide
        future_to_pair = {executor.submit(fetch_pair_data_smart, pair): pair for pair in target_pairs}
        for future in as_completed(future_to_pair):
            res = future.result()
            if res:
                results.append(res)
                print(".", end="", flush=True)
    print("\n")

    # Tri par performance (du plus fort au plus faible)
    results.sort(key=lambda x: x['pct'], reverse=True)

    print("\n🏆 CLASSEMENT RUNNER (Daily %)")
    print("-" * 40)

    # Identification de CHFJPY
    chfjpy_res = next((r for r in results if r['pair'] == "CHFJPY"), None)

    # --- HELPER: ANALYSE RELATIVE ---
    def get_relative_info(pair1, pair2):
        """
        Retourne soit une paire de duel, soit une devise unique.
        Returns: (type, value)
        - ("pair", "AUDCAD") si on peut former un duel
        - ("single", "USD") si une seule devise driver apparaît 2 fois (ex: USDJPY vs USDCHF)
        - ("intrus", "USD") si une seule devise driver apparaît 1 fois (ex: CHFJPY vs USDCHF)
        - (None, None) si impossible
        """
        # Décomposition
        # pair1 est déjà clean (ex: EURUSD)
        
        # Extraire la devise "driver" (celle qui n'est ni CHF ni JPY)
        def get_driver(p):
            b, q = p[:3], p[3:]
            if b not in ["CHF", "JPY"]: return b
            if q not in ["CHF", "JPY"]: return q
            return None
        
        d1 = get_driver(pair1)
        d2 = get_driver(pair2)
        
        # Cas spécial: si une seule devise driver apparaît 2 fois (ex: USDJPY vs USDCHF -> USD seul)
        if d1 and d2 and d1 == d2:
            return ("single", d1)
        
        # Cas intrus: une paire n'a pas de driver (ex: CHFJPY) et l'autre en a un
        if (d1 and not d2) or (d2 and not d1):
            intrus = d1 if d1 else d2
            return ("intrus", intrus)
        
        # Si aucun driver valide
        if not d1 and not d2:
            return (None, None)

        # Essayer de former la paire de duel
        candidate1 = f"{d1}{d2}"
        candidate2 = f"{d2}{d1}"

        # On vérifie si elle existe dans ALL_PAIRS (clean)
        clean_all = [p.replace("=X","") for p in ALL_PAIRS]

        final_pair = None
        if candidate1 in clean_all: final_pair = candidate1
        elif candidate2 in clean_all: final_pair = candidate2

        if final_pair:
            return ("pair", final_pair)
        return (None, None)

    # --- HELPER: GET EMA INDICATORS ---
    def get_ema_indicators_for_pair(pair_yahoo):
        """Fetch daily data and calculate EMA indicators."""
        df_d = fetch_data_smart(pair_yahoo, "1d", n_candles=300)
        if df_d is None or df_d.empty:
            return "⚪", "⚪"
        ema_status = calculate_ema_gap_status(df_d)
        ema_aligned = calculate_ema_aligned_status(df_d)
        ema_emoji = ema_status.split()[-1] if ema_status else "⚪"
        align_emoji = ema_aligned.split()[-1] if ema_aligned else "⚪"
        return ema_emoji, align_emoji
    
    # --- HELPER: CHECK EMA COHERENCE ---
    def is_ema_coherent(runner_emoji, ema_e, align_e):
        """Check if runner direction is coherent with EMA indicators.
        🟢 runner → EMA must be 🟢 or ⚪ (not 🔴)
        🔴 runner → EMA must be 🔴 or ⚪ (not 🟢)
        """
        if runner_emoji == "🟢":
            return ema_e != "🔴" and align_e != "🔴"
        elif runner_emoji == "🔴":
            return ema_e != "🟢" and align_e != "🟢"
        return True  # ⚪ is always coherent

    # --- CONSTRUCTION RAPPORT TELEGRAM ---
    msg_lines = ["🚀 <b>ANALYSE RUNNER</b>", ""]

    # 1. CHFJPY LINE (only if coherent)
    if chfjpy_res:
        pct = chfjpy_res['pct']
        emoji = "🟢" if pct > 0 else "🔴" if pct < 0 else "⚪"
        ema_e, align_e = get_ema_indicators_for_pair("CHFJPY=X")
        print(f"🟢 CHFJPY {pct:+.2f}%")
        if is_ema_coherent(emoji, ema_e, align_e):
            msg_lines.append(f"{emoji} <b>{chfjpy_res['pair']}</b> {pct:+.2f}% | {ema_e}{align_e}")

    # Variables pour stocker les infos unifiées
    final_top_strong = None
    final_last_weak = None

    # 2. TOP DUEL / INTRUE (keep logic for Duel calculation, but don't add to Telegram)
    top_list = results[:2]
    if len(top_list) >= 2:
        info_type, info_value = get_relative_info(top_list[0]['pair'], top_list[1]['pair'])
        if info_type == "pair":
            rel_data = fetch_pair_data_smart(f"{info_value}=X")
            if rel_data:
                rp = rel_data['pct']
                print(f"🚀 Top: {'🟢' if rp > 0 else '🔴'} {info_value} {rp:+.2f}%")
                if rp > 0: final_top_strong = info_value[:3]
                else: final_top_strong = info_value[3:]

        elif info_type in ["single", "intrus"]:
            print(f"🚀 Top: {info_value}")
            final_top_strong = info_value

    # 3. LAST DUEL / INTRUE (keep logic for Duel calculation, but don't add to Telegram)
    start_index = max(2, len(results) - 2)
    last_list = results[start_index:]
    
    if len(last_list) >= 2:
        p1 = last_list[-2]['pair']
        p2 = last_list[-1]['pair']
        info_type, info_value = get_relative_info(p1, p2)
        if info_type == "pair":
            rel_data = fetch_pair_data_smart(f"{info_value}=X")
            if rel_data:
                rp = rel_data['pct']
                print(f"📉 Last: {'🟢' if rp > 0 else '🔴'} {info_value} {rp:+.2f}%")
                if rp < 0: final_last_weak = info_value[:3] 
                else: final_last_weak = info_value[3:]

        elif info_type in ["single", "intrus"]:
            print(f"📉 Last: {info_value}")
            final_last_weak = info_value

    # 4. DUEL & DUEL (only add to Telegram if coherent)
    if final_top_strong and final_last_weak and final_top_strong != final_last_weak:
        candidate1 = f"{final_top_strong}{final_last_weak}"
        candidate2 = f"{final_last_weak}{final_top_strong}"
        clean_all = [p.replace("=X","") for p in ALL_PAIRS]
        
        final_pair = None
        if candidate1 in clean_all: final_pair = candidate1
        elif candidate2 in clean_all: final_pair = candidate2
        
        if final_pair:
            dd_data = fetch_pair_data_smart(f"{final_pair}=X")
            if dd_data:
                dd_pct = dd_data['pct']
                dd_emoji = "🟢" if dd_pct > 0 else "🔴" if dd_pct < 0 else "⚪"
                ema_e, align_e = get_ema_indicators_for_pair(f"{final_pair}=X")
                print(f"⚔️ Duel : {dd_emoji} {final_pair} {dd_pct:+.2f}%")
                if is_ema_coherent(dd_emoji, ema_e, align_e):
                    msg_lines.append("")
                    msg_lines.append("⚔️ Duel :")
                    msg_lines.append(f"{dd_emoji} {final_pair} {dd_pct:+.2f}% | {ema_e}{align_e}")
    
    # Time (handled by caller logic usually, but here it's inside main)
    # The existing code prints "Analyse terminée" then sends.
    from datetime import datetime
    paris_tz = pytz.timezone('Europe/Paris')
    now = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    msg_lines.append("")
    msg_lines.append(f"⏰ {now} Paris")
    
    print("-" * 40)
    print(f"✅ Analyse terminée ({len(results)}/{len(target_pairs)} paires)")
    
    # Envoi Telegram
    telegram_msg = "\n".join(msg_lines)
    if send_telegram_message(telegram_msg):
        print("📤 Message Telegram envoyé.")
    else:
        print("⚠️ Échec envoi Telegram.")


if __name__ == "__main__":
    main()
