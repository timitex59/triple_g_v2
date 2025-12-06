#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TRIPLE G INDICATORS (ICH + EMA GAP)
Basé sur le PineScript "triple_G"
Analyse H1 et Daily pour chaque paire.
"""

import yfinance as yf
import pandas as pd
import numpy as np
import sys
import time
import json
import requests
import os
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Force UTF-8 output for Windows console
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
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

def fetch_data(ticker, period, interval):
    try:
        df = yf.Ticker(ticker).history(period=period, interval=interval)
        if df.empty or len(df) < 60:
            return None
        return df
    except:
        return None

# --- RUNNER HISTORY FUNCTIONS ---

# Opportunity Stats Settings
STATS_FILE = Path(__file__).parent / "opportunity_stats.json"

# Spreads standards (pour calcul PnL net)
SPREADS = {
    "EURUSD": 1.0, "GBPUSD": 1.5, "USDJPY": 1.0, "USDCHF": 1.5,
    "EURJPY": 2.0, "GBPJPY": 2.5, "AUDJPY": 2.0, "CHFJPY": 2.5,
    "CADJPY": 2.5, "NZDJPY": 2.5, "EURGBP": 1.5, "EURAUD": 2.5,
    "EURCAD": 2.5, "EURNZD": 3.0, "GBPCHF": 3.0, "GBPAUD": 3.0,
    "GBPCAD": 3.5, "GBPNZD": 4.0, "AUDCAD": 2.5, "AUDCHF": 2.5,
    "AUDNZD": 3.0, "CADCHF": 3.0, "NZDCAD": 3.5, "NZDCHF": 3.5
}
DEFAULT_SPREAD = 2.5

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
    """Récupère le prix (approximatif via Close daily) depuis les résultats d'analyse."""
    # Note: Dans ce script on n'a que le pct daily, pas le prix exact live sauf si on refetch.
    # Pour simplifier et être précis, on va refetch le dernier prix live ici.
    try:
        # Correspondance symbole (ajouter =X si besoin)
        ticker = pair if "=X" in pair else f"{pair}=X"
        df = yf.Ticker(ticker).history(period="1d", interval="1m")
        if not df.empty:
            return df['Close'].iloc[-1]
    except:
        pass
    return None

def update_performance_tracking(big3_pairs, confluence_pairs):
    """
    Gère l'ouverture et la fermeture des trades virtuels.
    - Entrée : Si dans BIG3 ou CONFLUENCE
    - Sortie : Si disparait des deux listes
    """
    stats = load_stats()
    valid_pairs = set(p['pair'] for p in big3_pairs + confluence_pairs)
    
    # 1. Vérifier les sorties (Clôtures)
    to_close = []
    current_prices = {} # Cache
    
    for pair, trade in stats["active_trades"].items():
        if pair not in valid_pairs:
            # Signal de sortie !
            current_price = get_current_price_from_results(pair, [])
            if current_price:
                to_close.append((pair, current_price))
    
    closed_summary = []
    
    for pair, exit_price in to_close:
        trade = stats["active_trades"][pair]
        entry_price = trade["entry_price"]
        direction = trade["direction"]
        
        # Calcul PnL Brut
        pct_change = ((exit_price - entry_price) / entry_price) * 100
        if direction == "SHORT":
            pct_change = -pct_change
            
        # Coûts (Spread)
        spread_pips = SPREADS.get(pair, DEFAULT_SPREAD)
        # Estim pip value % (approx 0.01% pour majors, 0.015% crosses)
        # On simplifie : coût standard 0.03% par trade (entrée+sortie)
        cost_pct = 0.03 
        
        net_pnl = pct_change - cost_pct
        
        # Mise à jour stats globales
        stats["performance"]["total_pnl"] += net_pnl
        if net_pnl > 0:
            stats["performance"]["wins"] += 1
        else:
            stats["performance"]["losses"] += 1
            
        closed_trade = {
            "pair": pair,
            "entry_date": trade["entry_date"],
            "exit_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "entry_price": entry_price,
            "exit_price": exit_price,
            "direction": direction,
            "pnl_net": round(net_pnl, 2)
        }
        stats["closed_trades"].append(closed_trade)
        del stats["active_trades"][pair]
        
        emoji = "✅" if net_pnl > 0 else "❌"
        closed_summary.append(f"{emoji} Clôture {pair}: {net_pnl:+.2f}%")
        print(f"💰 Trade fermé: {pair} ({direction}) PnL: {net_pnl:+.2f}%")

    # 2. Vérifier les entrées (Ouvertures)
    for p_data in big3_pairs + confluence_pairs:
        pair = p_data['pair']
        if pair not in stats["active_trades"]:
            # Nouveau trade !
            # Déterminer direction via le signe du runner actuel
            # (Si runner positif -> BULLISH/LONG, car tendance haussière forte du jour)
            # (Si runner négatif -> BEARISH/SHORT)
            direction = "LONG" if p_data['pct'] >= 0 else "SHORT"
            
            price = get_current_price_from_results(pair, [])
            if price:
                stats["active_trades"][pair] = {
                    "entry_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "entry_price": price,
                    "direction": direction,
                    "initial_runner": p_data['pct']
                }
                print(f"💰 Nouveau trade ouvert: {pair} ({direction}) @ {price:.4f}")
    
    save_stats(stats)
    return closed_summary, stats["performance"]

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
            
            # Vérifier si c'est un nouveau jour
            last_date = data.get("_last_date", "")
            if last_date != today:
                # Nouvelle bougie daily -> réinitialiser l'historique
                print(f"🔄 Nouvelle bougie daily détectée ({last_date} → {today}). Réinitialisation de l'historique RUNNER.")
                return {"_last_date": today}
            
            return data
        except:
            return {"_last_date": today}
    return {"_last_date": today}

def save_runner_history(history):
    """Sauvegarde l'historique RUNNER (limité à RUNNER_HISTORY_MAX points par paire)."""
    from datetime import datetime
    
    # Mettre à jour la date
    history["_last_date"] = datetime.now().strftime("%Y-%m-%d")
    
    # Limiter chaque paire à RUNNER_HISTORY_MAX valeurs
    for pair in history:
        if pair.startswith("_"):
            continue  # Skip metadata keys like _last_date
        if len(history[pair]) > RUNNER_HISTORY_MAX:
            history[pair] = history[pair][-RUNNER_HISTORY_MAX:]
    
    with open(RUNNER_HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, indent=2)

def calculate_runner_rsi_from_list(values, period=7):
    """
    Calcule le RSI sur une liste de valeurs RUNNER.
    Retourne None si pas assez de données.
    """
    if len(values) < period + 1:
        return None
    
    series = pd.Series(values)
    delta = series.diff()
    
    up = delta.copy()
    down = delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    down = abs(down)
    
    # Wilder's Smoothing
    alpha = 1.0 / period
    roll_up = up.ewm(alpha=alpha, adjust=False).mean()
    roll_down = down.ewm(alpha=alpha, adjust=False).mean()
    
    rs = roll_up / roll_down
    rsi = 100.0 - (100.0 / (1.0 + rs))
    
    return rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else None

# --- INDICATEURS TECHNIQUES ---

def calculate_rsi_series(series, period=14):
    """
    Calcule le RSI sur une série arbitraire (pas forcément des prix).
    Utilise la méthode de Wilder (Alpha = 1/N) pour correspondre à PineScript ta.rsi.
    """
    delta = series.diff()
    
    up = delta.copy()
    down = delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    down = abs(down)
    
    # Wilder's Smoothing
    alpha = 1.0 / period
    roll_up = up.ewm(alpha=alpha, adjust=False).mean()
    roll_down = down.ewm(alpha=alpha, adjust=False).mean()
    
    rs = roll_up / roll_down
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def calculate_ema_gap_status(df):
    """
    Reproduit la logique EMA Gap du PineScript.
    """
    if df is None or len(df) < max(EMA_LENGTHS) + EMA_RSI_LEN + EMA_EMA_LEN:
        return "NEUTRAL"
        
    close = df['Close']
    
    # Calcul des 8 EMAs
    emas = []
    for length in EMA_LENGTHS:
        emas.append(close.ewm(span=length, adjust=False).mean())
        
    # Calcul des 7 différences (d1..d7)
    # d1 = EMA1 - EMA2, etc.
    diffs = []
    for i in range(len(emas) - 1):
        diffs.append(emas[i] - emas[i+1])
        
    # Moyenne des écarts signés
    # avgGapSigned = sum(diffs) / 7.0
    sum_diffs = sum(diffs)
    avgGapSigned = sum_diffs / 7.0
    
    # RSI on Avg Gap
    rsiAvgGap = calculate_rsi_series(avgGapSigned, EMA_RSI_LEN)
    
    # EMA on RSI
    emaRsi8 = rsiAvgGap.ewm(span=EMA_EMA_LEN, adjust=False).mean()
    
    # Check Last Value
    last_rsi = rsiAvgGap.iloc[-1]
    last_ema = emaRsi8.iloc[-1]
    
    if last_ema < last_rsi:
        return "BULLISH 🟢"
    elif last_ema > last_rsi:
        return "BEARISH 🔴"
    else:
        return "NEUTRAL ⚪"

def calculate_ema_aligned_status(df):
    """
    Vérifie si les 8 EMAs sont parfaitement alignées.
    """
    if df is None or len(df) < max(EMA_LENGTHS):
        return "NEUTRAL ⚪"
    
    close = df['Close']
    # Calculer les dernières valeurs des 8 EMAs
    emas = [close.ewm(span=l, adjust=False).mean().iloc[-1] for l in EMA_LENGTHS]
    
    # Vérifier Alignement Bullish: EMA1 > EMA2 > ... > EMA8
    is_bull = all(emas[i] > emas[i+1] for i in range(len(emas)-1))
    if is_bull:
        return "BULLISH 🟢"
        
    # Vérifier Alignement Bearish: EMA1 < EMA2 < ... < EMA8
    is_bear = all(emas[i] < emas[i+1] for i in range(len(emas)-1))
    if is_bear:
        return "BEARISH 🔴"
        
    return "NEUTRAL ⚪"

def analyze_pair(ticker):
    pair_name = get_clean_pair_name(ticker)
    
    # Fetch Data
    df_h1 = fetch_data(ticker, "1mo", "1h")
    df_d = fetch_data(ticker, "1y", "1d")
    df_w = fetch_data(ticker, "2y", "1wk")
    
    # --- ANALYSE SHORT (H1 + D1) ---
    align_h1 = calculate_ema_aligned_status(df_h1)
    ema_d = calculate_ema_gap_status(df_d)
    
    # --- ANALYSE LARGE (D1 + W1) ---
    align_d = calculate_ema_aligned_status(df_d)
    ema_w = calculate_ema_gap_status(df_w)
    
    # Calcul Smart PCT (Variation Daily)
    pct = 0.0
    if df_d is not None and not df_d.empty:
        last_idx = df_d.index[-1]
        last_date = last_idx.strftime("%Y-%m-%d")
        daily_open = df_d['Open'].iloc[-1]
        daily_close = df_d['Close'].iloc[-1]
        
        # Smart Close (toujours utiliser la dernière H1 si dispo pour le jour même)
        if df_h1 is not None and not df_h1.empty:
            # Attention au timezone
            day_1h_candles = df_h1[df_h1.index.strftime("%Y-%m-%d") == last_date]
            if not day_1h_candles.empty:
                daily_close = day_1h_candles['Close'].iloc[-1]
        
        if daily_open > 0:
            pct = ((daily_close - daily_open) / daily_open) * 100.0

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
    print(f"🚀 TRIPLE G INDICATORS SCAN (Short & Large Views)")
    print(f"Lancement de l'analyse sur {len(PAIRS)} paires...\n")
    
    # Charger l'historique RUNNER
    runner_history = load_runner_history()
    
    start_time = time.time()
    results = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pair = {executor.submit(analyze_pair, pair): pair for pair in PAIRS}
        for future in as_completed(future_to_pair):
            try:
                res = future.result()
                results.append(res)
                print(f".", end="", flush=True)
            except Exception as e:
                print(f"x", end="", flush=True)
    
    # Mettre à jour l'historique RUNNER avec les nouveaux PCT
    for r in results:
        pair = r['pair']
        if pair not in runner_history:
            runner_history[pair] = []
        runner_history[pair].append(r['pct'])
    
    # Sauvegarder l'historique (limité à 21 points)
    save_runner_history(runner_history)
    
    # Calculer le RSI RUNNER pour chaque paire
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
                
    # Tri initial par pourcentage absolu décroissant (RUNNER)
    results.sort(key=lambda x: abs(x['pct']), reverse=True)
    
    # --- FILTRAGE SHORT VIEW ---
    short_aligned_runners = []
    for r in results:
        if "NEUTRAL" in r['h1_align'] or "NEUTRAL" in r['d_ema']: continue
        if abs(r['pct']) <= 0.2: continue
        
        h1 = r['h1_align'].split()[0]
        d1 = r['d_ema'].split()[0]
        
        # Vérification Cohérence Sens (Bullish doit être positif, Bearish négatif)
        # Si Bullish et pct < 0 -> Rejet
        # Si Bearish et pct > 0 -> Rejet
        if h1 == "BULLISH" and r['pct'] < 0: continue
        if h1 == "BEARISH" and r['pct'] > 0: continue
        
        if h1 == d1:
            short_aligned_runners.append(r)
            
    top_5_short = short_aligned_runners[:5]
    
    # --- FILTRAGE LARGE VIEW ---
    large_aligned_runners = []
    for r in results:
        if "NEUTRAL" in r['d_align'] or "NEUTRAL" in r['w_ema']: continue
        if abs(r['pct']) <= 0.2: continue
        
        d1 = r['d_align'].split()[0]
        w1 = r['w_ema'].split()[0]
        
        # Vérification Cohérence Sens Large
        if d1 == "BULLISH" and r['pct'] < 0: continue
        if d1 == "BEARISH" and r['pct'] > 0: continue
        
        if d1 == w1:
            large_aligned_runners.append(r)
            
    top_5_large = large_aligned_runners[:5]
    
    # --- Filtrer RSI RUNNER pour ne garder que les paires alignées avec RSI > 55 ---
    aligned_pairs = set(r['pair'] for r in short_aligned_runners + large_aligned_runners)
    rsi_runner_aligned = [r for r in rsi_runner_data 
                          if r['pair'] in aligned_pairs 
                          and r['rsi_runner'] is not None 
                          and r['rsi_runner'] > 55]
    
    # Trier par RSI RUNNER décroissant
    rsi_runner_aligned.sort(key=lambda x: -x['rsi_runner'])
    top_5_rsi_runner = rsi_runner_aligned[:5]
    
    print(f"\n\n⏱️ Terminé en {time.time() - start_time:.2f}s")
    
    # --- AFFICHAGE TOP 5 RSI RUNNER ---
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
    
    # --- AFFICHAGE SHORT ---
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

    # --- AFFICHAGE LARGE ---
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

    # --- AFFICHAGE CONFLUENCE (Croisement SHORT & LARGE) ---
    short_pairs = set(r['pair'] for r in top_5_short)
    large_pairs = set(r['pair'] for r in top_5_large)
    confluence_pairs = short_pairs & large_pairs  # Intersection
    
    # Récupérer les données complètes des paires en confluence
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

    # --- AFFICHAGE BIG3 (Top 3 de l'union SHORT + LARGE par % RUNNER) ---
    # Combiner SHORT et LARGE, dédupliquer, trier par % absolu
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

    # --- ENVOI TELEGRAM ---
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Construction du message simplifié (BIG3 + CONFLUENCE)
    msg_lines = ["🚀 TRIPLE G SCAN 🚀", ""]
    
    # Identifier les paires présentes dans les deux sections
    big3_pairs_set = set(r['pair'] for r in big3_runners)
    confluence_pairs_set = set(r['pair'] for r in confluence_runners)
    common_pairs = big3_pairs_set.intersection(confluence_pairs_set)

    # BIG3
    msg_lines.append("✅ BIG 3")
    for r in big3_runners:
        emoji = "🟢" if r['pct'] > 0 else "🔴"
        mark = "🔥" if r['pair'] in common_pairs else ""
        msg_lines.append(f"{emoji}{mark}{r['pair']} ({r['pct']:+.1f}%)")
    msg_lines.append("")
    
    # CONFLUENCE
    msg_lines.append("⭐ CONFLUENCE")
    if confluence_runners:
        for r in confluence_runners:
            emoji = "🟢" if r['pct'] > 0 else "🔴"
            mark = "🔥" if r['pair'] in common_pairs else ""
            msg_lines.append(f"{emoji}{mark}{r['pair']} ({r['pct']:+.1f}%)")
    else:
        msg_lines.append("Aucune")
    
    msg_lines.append("")
    msg_lines.append(f"⏰ {now} Paris")

    # --- TRACKING PERFORMANCE ---
    closed_summary, perf = update_performance_tracking(big3_runners, confluence_runners)
    
    msg_lines.append("")
    msg_lines.append("💰 PERFORMANCE")
    msg_lines.append(f"PnL Total: {perf['total_pnl']:+.2f}%")
    msg_lines.append(f"Win/Loss: {perf['wins']}W / {perf['losses']}L")
    
    if closed_summary:
        msg_lines.append("")
        msg_lines.append("Clôtures récentes:")
        msg_lines.extend(closed_summary)
    
    telegram_msg = "\n".join(msg_lines)
    
    if send_telegram_message(telegram_msg):
        print("\n✅ Message Telegram envoyé!")
    else:
        print("\n⚠️ Échec envoi Telegram")


if __name__ == "__main__":
    main()

