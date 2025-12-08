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

import time
import json
import random
import string
import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from websocket import create_connection
from dotenv import load_dotenv

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

# --- TRADINGVIEW WEBSOCKET ENGINE ---
def generate_session_id():
    return "cs_" + ''.join(random.choices(string.ascii_letters + string.digits, k=12))

def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"

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
        ws = create_connection("wss://prodata.tradingview.com/socket.io/websocket")
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
        future_to_pair = {executor.submit(fetch_pair_data_tv, pair): pair for pair in target_pairs}
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

    # --- CONSTRUCTION RAPPORT TELEGRAM ---
    msg_lines = ["🚀 <b>ANALYSE RUNNER CHF & JPY</b>", ""]

    # --- TOP SECTION ---
    print("🚀 TOP (Meilleure Performance)")
    msg_lines.append("🚀 <b>TOP</b>")

    top_list = results[:2]

    # Calcul du rang de CHFJPY
    chfjpy_rank = results.index(chfjpy_res) + 1 if chfjpy_res else None
    mid_rank = len(results) // 2  # Milieu du classement

    # Si CHFJPY n'est pas dans le Top 2 MAIS est dans la première moitié du classement, on l'ajoute
    if chfjpy_res and chfjpy_res not in top_list and chfjpy_res not in results[:2] and chfjpy_rank and chfjpy_rank <= mid_rank:
        top_list.append(chfjpy_res)

    for i, res in enumerate(top_list, 1):
        pair = res['pair']
        pct = res['pct']
        # On recalcule le rang réel si c'est CHFJPY ajouté manuellement
        rank = i
        if res == chfjpy_res and res not in results[:2]:
             rank = results.index(res) + 1

        emoji = "🟢" if pct > 0 else "🔴" if pct < 0 else "⚪"
        line = f"{rank}. {emoji} <b>{pair}</b> {pct:+.2f}%"
        print(f"   {rank}. {emoji} {pair:<7} {pct:+.2f}%")
        msg_lines.append(line)

    # Variables pour stocker les infos des duels et devises uniques
    top_duel_pair = None
    top_duel_pct = None
    last_duel_pair = None
    last_duel_pct = None
    top_single_currency = None  # Devise forte ou intrue du TOP
    last_single_currency = None  # Devise faible ou intrue du LAST

    # Analyse Relative TOP 2
    if len(top_list) >= 2:
        info_type, info_value = get_relative_info(top_list[0]['pair'], top_list[1]['pair'])
        if info_type == "pair":
            # Appel fetch_pair_data_tv avec format yahoo
            rel_data = fetch_pair_data_tv(f"{info_value}=X")
            if rel_data:
                rp = rel_data['pct']
                re = "🟢" if rp > 0 else "🔴" if rp < 0 else "⚪"
                print(f"   👉 Duel: {re} {info_value} {rp:+.2f}%")
                msg_lines.append(f"👉 Duel: {re} {info_value} {rp:+.2f}%")
                top_duel_pair = info_value
                top_duel_pct = rp
        elif info_type == "single":
            print(f"   💪 Devise forte : {info_value}")
            msg_lines.append(f"💪 Devise forte : <b>{info_value}</b>")
            top_single_currency = info_value
        elif info_type == "intrus":
            print(f"   🎯 Devise intrue : {info_value}")
            msg_lines.append(f"🎯 Devise intrue : <b>{info_value}</b>")
            top_single_currency = info_value

    print("-" * 20)
    msg_lines.append("")

    # --- BOTTOM SECTION ---
    print("📉 LAST (Moins bonne Performance)")
    msg_lines.append("📉 <b>LAST</b>")

    # On prend les 2 derniers
    start_index = max(2, len(results) - 2)
    last_list = results[start_index:]

    # Si CHFJPY n'est pas dans les Last 2 MAIS est dans la seconde moitié du classement, on l'ajoute
    if chfjpy_res and chfjpy_res not in last_list and chfjpy_rank and chfjpy_rank > mid_rank:
        last_list.insert(0, chfjpy_res)

    for res in last_list:
        pair = res['pair']
        pct = res['pct']
        # Rang réel
        rank = results.index(res) + 1
        emoji = "🟢" if pct > 0 else "🔴" if pct < 0 else "⚪"
        line = f"{rank}. {emoji} <b>{pair}</b> {pct:+.2f}%"
        print(f"   {rank}. {emoji} {pair:<7} {pct:+.2f}%")
        msg_lines.append(line)

    # Analyse Relative LAST 2 (Les 2 derniers de la liste affichée)
    if len(last_list) >= 2:
        # On compare les deux derniers éléments de la liste affichée (souvent les rangs 12 et 13)
        p1 = last_list[-2]['pair']
        p2 = last_list[-1]['pair']
        info_type, info_value = get_relative_info(p1, p2)
        if info_type == "pair":
            rel_data = fetch_pair_data_tv(f"{info_value}=X")
            if rel_data:
                rp = rel_data['pct']
                re = "🟢" if rp > 0 else "🔴" if rp < 0 else "⚪"
                print(f"   👉 Duel: {re} {info_value} {rp:+.2f}%")
                msg_lines.append(f"👉 Duel: {re} {info_value} {rp:+.2f}%")
                last_duel_pair = info_value
                last_duel_pct = rp
        elif info_type == "single":
            print(f"   📉 Devise faible : {info_value}")
            msg_lines.append(f"📉 Devise faible : <b>{info_value}</b>")
            last_single_currency = info_value
        elif info_type == "intrus":
            print(f"   🎯 Devise intrue : {info_value}")
            msg_lines.append(f"🎯 Devise intrue : <b>{info_value}</b>")
            last_single_currency = info_value

    # --- DUEL & DUEL SECTION ---
    if top_duel_pair and last_duel_pair and top_duel_pct is not None and last_duel_pct is not None:
        # Extraire devise forte du TOP duel
        if top_duel_pct < 0:
            strong_currency = top_duel_pair[3:]  # Quote
        else:
            strong_currency = top_duel_pair[:3]  # Base
        
        # Extraire devise faible du LAST duel
        if last_duel_pct < 0:
            weak_currency = last_duel_pair[:3]  # Base
        else:
            weak_currency = last_duel_pair[3:]  # Quote
        
        # Former la paire DUEL & DUEL (si les devises sont différentes)
        if strong_currency != weak_currency:
            # Essayer de former la paire
            candidate1 = f"{strong_currency}{weak_currency}"
            candidate2 = f"{weak_currency}{strong_currency}"
            clean_all = [p.replace("=X","") for p in ALL_PAIRS]
            
            final_pair = None
            if candidate1 in clean_all: final_pair = candidate1
            elif candidate2 in clean_all: final_pair = candidate2
            
            if final_pair:
                print("-" * 20)
                print("⚔️ DUEL & DUEL")
                msg_lines.append("")
                msg_lines.append("⚔️ <b>DUEL & DUEL</b>")
                dd_data = fetch_pair_data_tv(f"{final_pair}=X")
                if dd_data:
                    dd_pct = dd_data['pct']
                    dd_emoji = "🟢" if dd_pct > 0 else "🔴" if dd_pct < 0 else "⚪"
                    print(f"   {dd_emoji} {final_pair} {dd_pct:+.2f}%")
                    print(f"   (Fort: {strong_currency} vs Faible: {weak_currency})")
                    msg_lines.append(f"{dd_emoji} <b>{final_pair}</b> {dd_pct:+.2f}%")
                    msg_lines.append(f"(Fort: {strong_currency} vs Faible: {weak_currency})")

    # --- CROSS SECTION (pour devises uniques du TOP et LAST) ---
    elif top_single_currency and last_single_currency and top_single_currency != last_single_currency:
        # Former la paire entre les deux devises uniques
        candidate1 = f"{top_single_currency}{last_single_currency}"
        candidate2 = f"{last_single_currency}{top_single_currency}"
        clean_all = [p.replace("=X","") for p in ALL_PAIRS]
        
        final_pair = None
        if candidate1 in clean_all: final_pair = candidate1
        elif candidate2 in clean_all: final_pair = candidate2
        
        if final_pair:
            print("-" * 20)
            print("🔀 CROSS")
            msg_lines.append("")
            msg_lines.append("🔀 <b>CROSS</b>")
            cross_data = fetch_pair_data_tv(f"{final_pair}=X")
            if cross_data:
                cross_pct = cross_data['pct']
                cross_emoji = "🟢" if cross_pct > 0 else "🔴" if cross_pct < 0 else "⚪"
                print(f"   {cross_emoji} {final_pair} {cross_pct:+.2f}%")
                print(f"   ({top_single_currency} vs {last_single_currency})")
                msg_lines.append(f"{cross_emoji} <b>{final_pair}</b> {cross_pct:+.2f}%")
                msg_lines.append(f"({top_single_currency} vs {last_single_currency})")

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
