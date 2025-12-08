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

def fetch_tv_batch(pair_yahoo):
    """
    Récupère H1, D1, W1 en une seule session pour une paire donnée.
    """
    clean_pair = pair_yahoo.replace("=X", "")
    tv_pair = f"OANDA:{clean_pair}"
    
    # On demande large pour couvrir tous les besoins
    # H1: 500 (Triple G) vs 120 (Asian) -> 500
    # D1: 300 (Triple G) vs 200 (Asian) -> 300
    # W1: 150 (Triple G)
    reqs = [
        ("1h", "60", 500),
        ("1d", "D", 300),
        ("1wk", "W", 150)
    ]
    
    data_map = {}
    ws = None
    
    try:
        headers = {
            "Origin": "https://www.tradingview.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        ws = create_connection("wss://prodata.tradingview.com/socket.io/websocket", header=headers, timeout=10)
        session_id = generate_session_id()
        
        ws.send(create_message("chart_create_session", [session_id, ""]))
        ws.send(create_message("resolve_symbol", [session_id, "sds_sym_1", f"={{\"symbol\":\"{tv_pair}\",\"adjustment\":\"splits\",\"session\":\"regular\"}}"]))
        
        # On lance les 3 créations de séries (s1, s2, s3)
        # s1 = H1, s2 = D1, s3 = W1
        ws.send(create_message("create_series", [session_id, "sds_1", "s1", "sds_sym_1", "60", 500]))
        ws.send(create_message("create_series", [session_id, "sds_2", "s2", "sds_sym_1", "D", 300]))
        ws.send(create_message("create_series", [session_id, "sds_3", "s3", "sds_sym_1", "W", 150]))
        
        start_t = time.time()
        completed = set()
        
        while time.time() - start_t < 15: # Timeout global 15s
            try:
                res = ws.recv()
                
                # Parsing générique pour s1, s2, s3
                for s_id, label, k_name in [("sds_1", "s1", "1h"), ("sds_2", "s2", "1d"), ("sds_3", "s3", "1wk")]:
                    if f'"{s_id}":' in res and '"s":[' in res:
                        # Extraction brute (un peu simplifiée, on suppose que le message contient les données de la série)
                        # Attention: TV peut envoyer plusieurs updates dans un message ou séparés
                        # On cherche le bloc correspondant à la série
                        pass 
                
                # Méthode plus robuste : on cherche les patterns "sds_X"
                if '"s":[' in res:
                    # Identifier quelle série c'est
                    key = None
                    if '"sds_1":' in res: key = "1h"
                    elif '"sds_2":' in res: key = "1d"
                    elif '"sds_3":' in res: key = "1wk"
                    
                    if key and key not in data_map:
                        start = res.find('"s":[')
                        end = res.find('"ns":', start)
                        if start != -1 and end != -1:
                            extract_end = end - 1
                            while res[extract_end] not in [',', '}']: extract_end -= 1
                            
                            raw = res[start + 4:extract_end]
                            data = json.loads(raw)
                            fdata = [item["v"] for item in data]
                            df = pd.DataFrame(fdata, columns=["timestamp", "open", "high", "low", "close", "volume"])
                            
                            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
                            df.set_index('datetime', inplace=True)
                            df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}, inplace=True)
                            df.drop(columns=['timestamp'], inplace=True)
                            
                            data_map[key] = df
                            completed.add(key)
            
                if len(completed) >= 3:
                    break
                    
            except Exception:
                break
        
        ws.close()
        
        if len(data_map) > 0:
            return (pair_yahoo, data_map)
        return None
        
    except Exception as e:
        # print(f"Err {pair_yahoo}: {e}")
        if ws: 
            try: ws.close() 
            except: pass
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
