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
INITIAL_CAPITAL = 10000.0  # Capital de départ simulé

# Spreads réalistes (Compte Standard moyen)
SPREADS = {
    # Majors
    "EURUSD": 1.1, "USDJPY": 1.1, "GBPUSD": 1.4, "AUDUSD": 1.3,
    "USDCHF": 1.5, "USDCAD": 1.6, "NZDUSD": 1.6,
    # EUR Crosses
    "EURGBP": 1.5, "EURJPY": 1.9, "EURCHF": 2.0, "EURAUD": 2.4,
    "EURCAD": 2.4, "EURNZD": 3.8,
    # GBP Crosses
    "GBPJPY": 2.6, "GBPCHF": 3.2, "GBPAUD": 3.2, "GBPCAD": 3.5,
    "GBPNZD": 4.5,
    # JPY Crosses
    "AUDJPY": 2.1, "CHFJPY": 2.3, "CADJPY": 2.4, "NZDJPY": 2.5,
    # Other Crosses
    "AUDCAD": 2.2, "AUDCHF": 2.3, "AUDNZD": 2.8, "CADCHF": 2.6,
    "NZDCAD": 2.8, "NZDCHF": 2.8
}
DEFAULT_SPREAD = 2.5

def calculate_cost_percentage(pair, price, spread_pips):
    """Calcule le coût du spread en pourcentage."""
    if price == 0: return 0.0
    
    # Valeur d'un pip
    # JPY pairs (2 décimales) -> 0.01
    # Autres (4 décimales) -> 0.0001
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
    """Récupère le prix live."""
    try:
        ticker = pair if "=X" in pair else f"{pair}=X"
        df = yf.Ticker(ticker).history(period="1d", interval="1m")
        if not df.empty:
            return df['Close'].iloc[-1]
    except:
        pass
    return None

    # 3. Calculs Métriques Portefeuille
    total_pnl_pct = stats["performance"]["total_pnl"]
    current_capital = INITIAL_CAPITAL * (1 + total_pnl_pct / 100)
    total_accumulated_eur = current_capital - INITIAL_CAPITAL
    
    emoji_capital = "✅" if total_accumulated_eur >= 0 else "❌"
    
    # En cours (Active PnL)
    active_pnl_eur = 0.0
    active_count = len(stats["active_trades"])
    
    for pair, trade in stats["active_trades"].items():
         current_price = get_current_price_from_results(pair, [])
         if current_price:
             pct = ((current_price - trade["entry_price"]) / trade["entry_price"]) * 100
             if trade["direction"] == "SHORT": pct = -pct
             
             # Coût dynamique (Spread)
             spread = SPREADS.get(pair, DEFAULT_SPREAD)
             cost_pct = calculate_cost_percentage(pair, trade["entry_price"], spread)
             pct -= cost_pct
             
             active_pnl_eur += (pct / 100) * INITIAL_CAPITAL
    
    # PnL 24h
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

def get_paris_time():
    """Retourne l'heure actuelle à Paris (aware datetime)."""
    import pytz
    paris_tz = pytz.timezone('Europe/Paris')
    return datetime.now(paris_tz)

def update_performance_tracking(big3_pairs, confluence_pairs):
    """
    Gère le tracking, les horaires de trading et le calcul Portefeuille.
    Horaires: Lundi-Vendredi, 06h-22h Paris.
    A 22h: Clôture forcée de tout.
    """
    stats = load_stats()
    paris_now = get_paris_time()
    
    # Trading Window checks
    # Lundi=0, Dimanche=6. On trade Lundi-Vendredi (0-4)
    is_weekday = paris_now.weekday() < 5
    current_hour = paris_now.hour
    
    # Plage active: 06h00 inclus à 21h59 inclus.
    # A partir de 22h00 -> On ferme tout.
    # Avant 06h00 -> On ne fait rien (monitoring only)
    
    can_trade_new = is_weekday and (6 <= current_hour < 22)
    force_close_all = current_hour >= 22 or not is_weekday # APRÈS 22h ou Weekend -> CLOSE ALL
    
    valid_pairs = set(p['pair'] for p in big3_pairs + confluence_pairs)
    
    # 1. Gestion des Sorties (Normales + Forcées)
    to_close = []
    
    for pair, trade in stats["active_trades"].items():
        should_close = False
        reason = ""
        
        # Condition 1: Sortie technique (plus dans le screener)
        if pair not in valid_pairs:
            should_close = True
            reason = "Signal technique (Sortie screener)"
            
        # Condition 2: Clôture Forcée (22h ou Weekend)
        if force_close_all:
            should_close = True
            reason = "Clôture journalière (22h)"
            
        if should_close:
            current_price = get_current_price_from_results(pair, [])
            if current_price:
                to_close.append((pair, current_price, reason))
    
    # Exécution des clôtures
    for pair, exit_price, reason in to_close:
        trade = stats["active_trades"][pair]
        entry_price = trade["entry_price"]
        direction = trade["direction"]
        
        pct_change = ((exit_price - entry_price) / entry_price) * 100
        if direction == "SHORT":
            pct_change = -pct_change
            
        # Coût dynamique
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

    # 2. Gestion des Entrées (Uniquement dans la fenêtre de tir)
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
    
    # 3. Calculs Métriques Portefeuille (Code existant inchangé ci-dessous)

    total_pnl_pct = stats["performance"]["total_pnl"]
    current_capital = INITIAL_CAPITAL * (1 + total_pnl_pct / 100)
    total_accumulated_eur = current_capital - INITIAL_CAPITAL
    
    emoji_capital = "✅" if total_accumulated_eur >= 0 else "❌"
    
    # En cours (Active PnL)
    active_pnl_eur = 0.0
    active_count = len(stats["active_trades"])
    
    for pair, trade in stats["active_trades"].items():
         current_price = get_current_price_from_results(pair, [])
         if current_price:
             pct = ((current_price - trade["entry_price"]) / trade["entry_price"]) * 100
             if trade["direction"] == "SHORT": pct = -pct
             
             # Coût dynamique (Spread)
             spread = SPREADS.get(pair, DEFAULT_SPREAD)
             cost_pct = calculate_cost_percentage(pair, trade["entry_price"], spread)
             pct -= cost_pct
             
             active_pnl_eur += (pct / 100) * INITIAL_CAPITAL
    
    # PnL 24h
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

    
    # --- TRACKING PERFORMANCE ---
    perf = update_performance_tracking(big3_runners, confluence_runners)
    
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

