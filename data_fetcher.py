#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MASTER DATA FETCHER
===================
Ce script s'exécute en premier dans le workflow.
Il récupère TOUTES les données de marché (H1, D1, W1) pour toutes les paires
via TradingView WebSocket et les stocke dans un cache local (Pickle).
Les autres scripts liront ce cache pour éviter de multiplier les connexions.
"""

import sys
import time
import json
import random
import string
import subprocess
import importlib
import os
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- DEPENDENCY MANAGEMENT ---
def install_and_import(package, import_name=None):
    if import_name is None: import_name = package
    try:
        return importlib.import_module(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name)

install_and_import("pandas")
install_and_import("numpy")
install_and_import("websocket-client", "websocket")

import pandas as pd
from websocket import create_connection

# --- CONFIGURATION ---
CACHE_FILE = "market_cache.pkl"

# Liste complète des paires (Union des besoins des 3 scripts)
PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
    "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
    "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPCHF=X",
    "AUDNZD=X", "AUDCAD=X", "AUDCHF=X",
    "NZDCAD=X", "NZDCHF=X",
    "CADCHF=X"
]

# --- TRADINGVIEW ENGINE ---
def generate_session_id():
    return "cs_" + ''.join(random.choices(string.ascii_letters + string.digits, k=12))

def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"

def fetch_single_tf(pair_yahoo, interval_code, n_candles, session_id=None):
    """
    Récupère un seul timeframe via WebSocket.
    """
    clean_pair = pair_yahoo.replace("=X", "")
    tv_pair = f"OANDA:{clean_pair}"
    
    # Mapping
    tv_interval = "60"
    if interval_code == "1h": tv_interval = "60"
    elif interval_code == "1d": tv_interval = "D"
    elif interval_code == "1wk": tv_interval = "W"
    
    ws = None
    extracted_df = None
    
    try:
        headers = {
            "Origin": "https://www.tradingview.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        ws = create_connection("wss://prodata.tradingview.com/socket.io/websocket", header=headers, timeout=10)
        if not session_id:
            session_id = generate_session_id()
        
        ws.send(create_message("chart_create_session", [session_id, ""]))
        ws.send(create_message("resolve_symbol", [session_id, "sds_sym_1", f"={{\"symbol\":\"{tv_pair}\",\"adjustment\":\"splits\",\"session\":\"regular\"}}"]))
        ws.send(create_message("create_series", [session_id, "sds_1", "s1", "sds_sym_1", tv_interval, n_candles]))
        
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
    except:
        if ws: 
            try: ws.close() 
            except: pass
        return None

def fetch_tv_batch(pair_yahoo):
    """
    Récupère H1, D1, W1 de manière séquentielle pour éviter les surcharges.
    """
    data_map = {}
    
    # H1
    df_h1 = fetch_single_tf(pair_yahoo, "1h", 500)
    if df_h1 is not None: data_map["1h"] = df_h1
    
    # D1
    df_d = fetch_single_tf(pair_yahoo, "1d", 300)
    if df_d is not None: data_map["1d"] = df_d
    
    # W1
    df_w = fetch_single_tf(pair_yahoo, "1wk", 150)
    if df_w is not None: data_map["1wk"] = df_w
    
    # Si on a au moins 1 TF, on considère le fetch réussi (partiellement)
    if len(data_map) > 0:
        return (pair_yahoo, data_map)
    return None

def main():
    print(f"🌍 MASTER DATA FETCHER")
    print(f"Récupération centralisée pour {len(PAIRS)} paires...")
    print(f"Cache File: {CACHE_FILE}")
    
    start_time = time.time()
    global_cache = {}
    
    # On utilise moins de workers pour être plus discret (mais une seule connexion fait 3 jobs)
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pair = {executor.submit(fetch_tv_batch, pair): pair for pair in PAIRS}
        for future in as_completed(future_to_pair):
            res = future.result()
            if res:
                pair, data = res
                global_cache[pair] = data
                print(f"✅ {pair} ({len(data)} TFs)")
            else:
                print(f"❌ {future_to_pair[future]}")

    # Sauvegarde Pickle
    print(f"\nSauvegarde du cache ({len(global_cache)} paires)...")
    try:
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(global_cache, f)
        print(f"✅ Cache sauvegardé avec succès ({os.path.getsize(CACHE_FILE) / 1024:.1f} KB)")
    except Exception as e:
        print(f"⚠️ Erreur sauvegarde cache: {e}")

    print(f"⏱️ Temps total: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    main()
