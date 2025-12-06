#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TRIPLE G SCANNER: ULTIMATE EDITION (Separated Views) - LOCAL GITHUB VERSION
====================================================
Analyse complète avec deux moteurs INDÉPENDANTS :

1. SHORT VIEW (Logique 'Simple.py')
   - Tendance Rapide : H1 + Daily
   - Tendance Lente  : Daily + Weekly
   - Calcul Force    : Priorité Rapide, Fallback Lente, Bonus Convergence.
   - Historique      : Stocké sous la clé "short"

2. LARGE VIEW (Transposition Daily/Weekly)
   - Tendance Rapide : Daily + Weekly
   - Tendance Lente  : Weekly (Seul)
   - Calcul Force    : Priorité Rapide, Fallback Lente.
   - Historique      : Stocké sous la clé "large"

Fichiers de données cloisonnés dans : 
- gap_history_large.json (Structure { "short": {...}, "large": {...} })
- last_states_large.json (Structure { "short": {...}, "large": {...} })

VERSION LOCALE (Sans dépendance Google Drive)
"""

from __future__ import annotations
import argparse, importlib, os, subprocess, sys, json, time, requests, random
# Force UTF-8 output for Windows console
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import stats_tracker

REQUIRED_PKGS = [
    ("yfinance","yfinance"),
    ("pandas","pandas"),
    ("numpy","numpy")
]

# Global variables
yf = None
pd = None
np = None

PAIR_CODES = []
PAIR_MAP = {}
EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]
_CACHE = {}
HISTORY_FILE = ""
LAST_STATES_FILE = ""
TELEGRAM_BOT_TOKEN = None
TELEGRAM_CHAT_ID = None
IS_GITHUB_ACTION = False
FORCE_SAVE = False

class DependencyError(Exception): pass

def _install_missing(pkgs: List[str]) -> bool:
    try:
        proc = subprocess.run([sys.executable,"-m","pip","install",*pkgs], capture_output=True, text=True)
        return proc.returncode == 0
    except: return False

def ensure_deps(auto):
    global yf, pd, np
    missing, modules = {}, {}
    for mod, pip in REQUIRED_PKGS:
        try: 
            modules[mod] = importlib.import_module(mod)
        except ModuleNotFoundError: 
            missing[mod] = pip
    
    if missing:
        if auto and not _install_missing(list(missing.values())):
            raise DependencyError(f"Install: pip install {' '.join(missing.values())}")
        for mod, pip in missing.items():
            try: modules[mod] = importlib.import_module(mod)
            except ModuleNotFoundError: raise DependencyError(f"Failed to import {mod} after install attempt.")
    
    yf = modules["yfinance"]
    pd = modules["pandas"]
    np = modules["numpy"]
    return yf, pd, np

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--debug-pair", type=str, help="Debug une paire spécifique")
    p.add_argument("--no-auto-install", action="store_true")
    p.add_argument("--reset-history", action="store_true", help="Efface l'historique des Gaps")
    # Arguments statistiques
    p.add_argument("--stats", type=str, help="Afficher stats d'une paire")
    p.add_argument("--stats-summary", action="store_true", help="Vue d'ensemble des stats")
    p.add_argument("--best-setups", action="store_true", help="Top configurations")
    p.add_argument("--active-trades", action="store_true", help="Opportunités en tracking")
    p.add_argument("--whipsaw-report", action="store_true", help="Paires instables")
    p.add_argument("--performance", action="store_true", help="Rapport de performance")
    p.add_argument("--reset-portfolio", action="store_true", help="Reset portefeuille")
    # Mode rapport
    p.add_argument("--full-report", action="store_true", help="Mode rapport complet (OFF=MIXT seul)")
    # Mode force-save (pour écraser le mode lecture seule en local)
    p.add_argument("--force-save", action="store_true", help="Force la sauvegarde en local (écrase mode lecture seule)")
    args, _ = p.parse_known_args()
    return args

def setup_environment():
    global PAIR_CODES, PAIR_MAP, HISTORY_FILE, LAST_STATES_FILE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, IS_GITHUB_ACTION, FORCE_SAVE
    
    ALL_PAIRS = [
        "EURUSD=X","EURGBP=X","EURJPY=X","EURCHF=X","EURAUD=X","EURCAD=X","EURNZD=X",
        "GBPCHF=X","GBPAUD=X","GBPCAD=X","GBPNZD=X","GBPUSD=X","GBPJPY=X",
        "NZDCHF=X","NZDCAD=X","NZDUSD=X","NZDJPY=X",
        "USDCHF=X","USDJPY=X","USDCAD=X",
        "AUDCHF=X","AUDCAD=X","AUDUSD=X","AUDJPY=X","AUDNZD=X",
        "CHFJPY=X","CADJPY=X","CADCHF=X"
    ]

    PAIR_CODES = [p.replace("=X","") for p in ALL_PAIRS]
    PAIR_MAP = {code: f"{code}=X" for code in PAIR_CODES}

    script_dir = os.path.dirname(os.path.abspath(__file__))
    BASE_DIR = script_dir
    print(f"Local: {BASE_DIR}")

    os.makedirs(BASE_DIR, exist_ok=True)
    HISTORY_FILE = os.path.join(BASE_DIR, "gap_history_large.json")
    LAST_STATES_FILE = os.path.join(BASE_DIR, "last_states_large.json")
    ENV_FILE = os.path.join(BASE_DIR, ".env")
    
    # Initialiser stats_tracker
    stats_tracker.init_stats_file(BASE_DIR)
    
    # Charger les variables d'environnement
    # Priorité 1: Variables système (GitHub Secrets)
    # Priorité 2: Fichier .env local
    ENV_VARS = {}
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    k, v = line.strip().split('=', 1)
                    ENV_VARS[k] = v
    
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", ENV_VARS.get("TELEGRAM_BOT_TOKEN"))
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", ENV_VARS.get("TELEGRAM_CHAT_ID"))
    
    # Détecter environnement et mode force-save
    IS_GITHUB_ACTION = os.environ.get("GITHUB_ACTIONS") == "true"
    # FORCE_SAVE sera lu depuis les args dans main_cli
    
    print("✅ Configuration chargée (Mode Cloisonné - Local).")
    
    if not IS_GITHUB_ACTION and not FORCE_SAVE:
        print("🔒 Mode Lecture Seule (Sauvegarde désactivée) - Utilisez --force-save pour écraser")
        stats_tracker.set_read_only(True)
    elif FORCE_SAVE:
        print("⚠️ Mode Force Save activé (Sauvegarde locale autorisée)")

# --- GESTION HISTORIQUE GAP (CLOISONNÉ) ---
def load_history():
    if not os.path.exists(HISTORY_FILE): return {"short": {}, "large": {}}
    try:
        with open(HISTORY_FILE, 'r') as f: 
            data = json.load(f)
            if data and "short" not in data and "large" not in data:
                return {"short": {}, "large": {}}
            return data
    except: return {"short": {}, "large": {}}

def save_history(history):
    if not IS_GITHUB_ACTION and not FORCE_SAVE:
        return
    try:
        with open(HISTORY_FILE, 'w') as f: json.dump(history, f, indent=2)
    except Exception as e: print(f"⚠️ Erreur sauvegarde historique: {e}")

def load_last_states():
    if not os.path.exists(LAST_STATES_FILE): return {"short": {}, "large": {}}
    try:
        with open(LAST_STATES_FILE, 'r') as f: 
            data = json.load(f)
            if data and "short" not in data:
                return {"short": {}, "large": {}}
            return data
    except: return {"short": {}, "large": {}}

def save_last_states(states):
    if not IS_GITHUB_ACTION and not FORCE_SAVE:
        return
    try:
        with open(LAST_STATES_FILE, 'w') as f: json.dump(states, f, indent=2)
    except Exception as e: print(f"⚠️ Erreur sauvegarde last_states: {e}")

def update_history_section(section_history, current_gaps):
    now = datetime.now()
    timestamp = now.isoformat()
    cutoff_date = now - timedelta(days=14)
    
    for pair, gap in current_gaps.items():
        if pair not in section_history: section_history[pair] = []
        section_history[pair].append({"t": timestamp, "v": gap})
        
        filtered = []
        for entry in section_history[pair]:
            try:
                t_str = entry['t']
                if "T" in t_str: entry_dt = datetime.fromisoformat(t_str)
                else: entry_dt = datetime.strptime(t_str, "%Y-%m-%d")
                if entry_dt >= cutoff_date: filtered.append(entry)
            except: pass
        section_history[pair] = filtered
    return section_history

def calculate_rsi(series, period=7):
    if len(series) < period + 1: return 50.0
    deltas = np.diff(series)
    seed = deltas[:period+1]
    up = seed[seed >= 0].sum()/period
    down = -seed[seed < 0].sum()/period

    if down == 0: rs = 0 if up == 0 else float('inf')
    else: rs = up/down

    rsi = np.zeros_like(deltas)
    if rs == float('inf'): rsi[:period] = 100.
    elif rs == 0 and up == 0: rsi[:period] = 50.
    else: rsi[:period] = 100. - 100./(1. + rs)

    for i in range(period, len(deltas)):
        delta = deltas[i]
        if delta > 0: upval, downval = delta, 0.
        else: upval, downval = 0., -delta
        up = (up*(period-1) + upval)/period
        down = (down*(period-1) + downval)/period
        if down == 0: rs = 0 if up == 0 else float('inf')
        else: rs = up/down
        if rs == float('inf'): rsi[i] = 100.
        elif rs == 0 and up == 0: rsi[i] = 50.
        else: rsi[i] = 100. - 100./(1. + rs)
    return rsi[-1]

def detect_transitions(old_states, new_states):
    """Détecte les changements d'état significatifs entre deux runs"""
    transitions = []
    all_pairs = set(old_states.keys()) | set(new_states.keys())
    
    for pair in sorted(all_pairs):
        old = old_states.get(pair, "NONE")
        new = new_states.get(pair, "NONE")
        
        if old == new: continue
        
        # On ne notifie que les changements "intéressants" pour éviter le bruit
        # Intéressant : Entrée ou Sortie de POWER/ACCEL/DECEL
        if old != "NONE" or new != "NONE":
            transitions.append(f"   {pair}: {old} -> {new}")
            
    return transitions

# --- ANALYSE ---
def fetch_data(pair, period, interval, retries=3):
    key = (pair, period, interval)
    if key in _CACHE: return _CACHE[key]
    time.sleep(random.uniform(0.1, 0.5))
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(PAIR_MAP[pair])
            df = ticker.history(period=period, interval=interval)
            if df.empty or len(df) < 60: 
                if attempt < retries - 1:
                    time.sleep(1) 
                    continue
                df = None
            _CACHE[key] = df
            return df
        except Exception:
            if attempt < retries - 1:
                time.sleep(1)
                continue
            _CACHE[key] = None
            return None
    return None

# ==================== PSAR SIGNAL HELPERS ====================

def calculate_psar(df, iaf=0.1, maxaf=0.2, stepaf=0.1):
    """Calcule le Parabolic SAR."""
    high = df['High'].values
    low = df['Low'].values
    close = df['Close'].values
    n = len(df)
    
    psar = [0.0] * n
    
    bull = True
    af = iaf
    ep = low[0]
    psar[0] = low[0]
    
    for i in range(1, n):
        prev_psar = psar[i-1]
        
        if bull:
            sar = prev_psar + af * (ep - prev_psar)
        else:
            sar = prev_psar + af * (ep - prev_psar)
            
        reverse = False
        
        if bull:
            if low[i] < sar:
                bull = False
                reverse = True
                sar = ep
                ep = low[i]
                af = iaf
            else:
                if high[i] > ep:
                    ep = high[i]
                    af = min(af + stepaf, maxaf)
                if i >= 1: sar = min(sar, low[i-1])
                if i >= 2: sar = min(sar, low[i-2])
        else:
            if high[i] > sar:
                bull = True
                reverse = True
                sar = ep
                ep = high[i]
                af = iaf
            else:
                if low[i] < ep:
                    ep = low[i]
                    af = min(af + stepaf, maxaf)
                if i >= 1: sar = max(sar, high[i-1])
                if i >= 2: sar = max(sar, high[i-2])
                
        psar[i] = sar
        
    return psar

def detect_8am_crossover(df, psar_values):
    """Détecte le DERNIER croisement Prix/PSAR après 08:00."""
    if df.empty or len(df) != len(psar_values):
        return None

    df_calc = df.copy()
    df_calc['PSAR'] = psar_values
    
    last_date = df_calc.index[-1].date()
    today_df = df_calc[df_calc.index.date == last_date]
    
    if today_df.empty:
        return None

    crossover_info = None

    for i in range(1, len(today_df)):
        current_candle = today_df.iloc[i]
        prev_candle = today_df.iloc[i-1]
        
        if current_candle.name.hour < 6:
            continue

        prev_bull = prev_candle['Close'] > prev_candle['PSAR']
        curr_bull = current_candle['Close'] > current_candle['PSAR']
        
        if prev_bull != curr_bull:
            # time_str = current_candle.name.strftime("%H:%M")
            emoji = "🟢" if curr_bull else "🔴"
            crossover_info = emoji # Juste l'emoji du signal
            
    return crossover_info

def get_pair_signal_8am(pair):
    """Récupère le signal 8am pour une paire."""
    # On fetch 1mo 1h pour avoir assez de données pour le PSAR
    df_1h = fetch_data(pair, "1mo", "1h")
    if df_1h is None or df_1h.empty:
        return ""
        
    try:
        psar_series = calculate_psar(df_1h, iaf=0.1, stepaf=0.1, maxaf=0.2)
        signal = detect_8am_crossover(df_1h, psar_series)
        return f" {signal}" if signal else ""
    except:
        return ""

def get_psar_position(pair):
    """
    Vérifie la position actuelle du prix par rapport au PSAR.
    Returns: 'ABOVE' si prix > PSAR, 'BELOW' si prix < PSAR, None si erreur
    """
    df_1h = fetch_data(pair, "1mo", "1h")
    if df_1h is None or df_1h.empty:
        return None
        
    try:
        psar_series = calculate_psar(df_1h, iaf=0.1, stepaf=0.1, maxaf=0.2)
        df_calc = df_1h.copy()
        df_calc['PSAR'] = psar_series
        
        # Dernière bougie
        last_candle = df_calc.iloc[-1]
        current_price = last_candle['Close']
        current_psar = last_candle['PSAR']
        
        if current_price > current_psar:
            return 'ABOVE'
        else:
            return 'BELOW'
    except:
        return None

def check_ema_alignment(df):
    if df is None: return "NEUTRAL"
    close = df['Close']
    emas = {l: close.ewm(span=l, adjust=False).mean().iloc[-1] for l in EMA_LENGTHS}
    vals = [emas[l] for l in EMA_LENGTHS]
    if all(vals[i] > vals[i+1] for i in range(len(vals)-1)): return "BULLISH"
    if all(vals[i] < vals[i+1] for i in range(len(vals)-1)): return "BEARISH"
    return "NEUTRAL"

def analyze_pair_full(pair):
    df_1h = fetch_data(pair, "1mo", "1h")
    df_d = fetch_data(pair, "1y", "1d")
    df_w = fetch_data(pair, "2y", "1wk")

    if df_d is not None and not df_d.empty:
        if df_d.index[-1].weekday() >= 5: df_d = df_d.iloc[:-1]

    st_1h = check_ema_alignment(df_1h)
    st_d = check_ema_alignment(df_d)
    st_w = check_ema_alignment(df_w)
    
    # --- LOGIQUE SHORT (Similaire à Simple.py) ---
    # Rapide = H1, Lent = Daily
    mode_short_fast = "NEUTRAL" # H1+D
    if st_1h == "BULLISH" and st_d == "BULLISH": mode_short_fast = "BULLISH"
    elif st_1h == "BEARISH" and st_d == "BEARISH": mode_short_fast = "BEARISH"
    
    mode_short_slow = "NEUTRAL" # D+W
    if st_d == "BULLISH" and st_w == "BULLISH": mode_short_slow = "BULLISH"
    elif st_d == "BEARISH" and st_w == "BEARISH": mode_short_slow = "BEARISH"

    convergence_short = "NON"
    if mode_short_fast == "BULLISH" and mode_short_slow == "BULLISH": convergence_short = "BULLISH 🔥"
    elif mode_short_fast == "BEARISH" and mode_short_slow == "BEARISH": convergence_short = "BEARISH 🔥"

    # --- LOGIQUE LARGE (Transposée) ---
    # Rapide = Daily+Weekly (Correspond au 'Slow' du short), Lent = Weekly (Seul)
    mode_large_fast = mode_short_slow # D+W
    
    mode_large_slow = st_w # Weekly seul comme "Lent"
    
    convergence_large = "NON"
    if mode_large_fast == "BULLISH" and mode_large_slow == "BULLISH": convergence_large = "BULLISH 🔥"
    elif mode_large_fast == "BEARISH" and mode_large_slow == "BEARISH": convergence_large = "BEARISH 🔥"

    # Données Bougie Daily (Commune aux deux, validation finale)
    last_date = None
    daily_open = 0.0
    daily_close = 0.0
    
    if df_d is not None and not df_d.empty:
        last_idx = df_d.index[-1]
        last_date = last_idx.strftime("%Y-%m-%d")
        daily_open = df_d['Open'].iloc[-1]
        daily_close = df_d['Close'].iloc[-1]
        
        # Smart Close (toujours utiliser la dernière H1)
        if df_1h is not None and not df_1h.empty:
            day_1h_candles = df_1h[df_1h.index.strftime("%Y-%m-%d") == last_date]
            if not day_1h_candles.empty:
                daily_close = day_1h_candles['Close'].iloc[-1]

    candle = "UNKNOWN"
    if daily_open > 0:
        candle = "GREEN" if daily_close >= daily_open else "RED"

    pct = 0.0
    if daily_open > 0:
        pct = ((daily_close - daily_open) / daily_open) * 100

    return {
        "pair": pair,
        # Short View Data
        "short_fast": mode_short_fast,
        "short_slow": mode_short_slow,
        "short_convergence": convergence_short,
        # Large View Data
        "large_fast": mode_large_fast,
        "large_slow": mode_large_slow,
        "large_convergence": convergence_large,
        # Common
        "candle": candle,
        "last_date": last_date,
        "pct": pct,
        "pct_real": pct
    }

def calculate_scores_separated(results, view_type):
    """
    Calcule les scores de force (GAP) selon la logique exacte de simple.py
    view_type: "short" ou "large"
    """
    scores = {c: 0 for c in ["EUR", "USD", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"]}
    
    for res in results:
        pair = res['pair']
        base, quote = pair[:3], pair[3:]
        
        if view_type == "short":
            fast = res['short_fast']
            slow = res['short_slow']
            conv = res['short_convergence']
        else:
            fast = res['large_fast']
            slow = res['large_slow']
            conv = res['large_convergence']
            
        # Logique Simple.py :
        # Poids double si convergence (🔥)
        weight = 2 if "🔥" in conv else 1
        
        # Direction prioritaire = Fast. Si Neutre, fallback sur Slow.
        status = fast
        if status == "NEUTRAL": status = slow
        
        if status == "BULLISH":
            scores[base] += weight
            scores[quote] -= weight
        elif status == "BEARISH":
            scores[base] -= weight
            scores[quote] += weight
            
    return scores

def process_view_results(all_results, scores, view_type, full_history):
    """
    Traite les résultats pour une vue spécifique (Short ou Large).
    Calcule les gaps, RSI, tries, etc.
    Retourne une structure complète pour l'affichage.
    """
    # Calcul des Gaps actuels
    current_gaps = {}
    for pair in PAIR_CODES:
        base_s = scores.get(pair[:3], 0)
        quote_s = scores.get(pair[3:], 0)
        current_gaps[pair] = abs(base_s - quote_s)
    
    # Mise à jour historique spécifique à la vue
    view_history = full_history.get(view_type, {})
    view_history = update_history_section(view_history, current_gaps)
    full_history[view_type] = view_history # Update global object
    
    # Enrichissement des résultats pour cette vue
    view_results = []
    for res in all_results:
        # On crée une copie légère enrichie pour cette vue
        r = res.copy()
        r['gap'] = current_gaps[r['pair']]
        
        # RSI sur l'historique de CETTE vue
        pair_hist = view_history.get(r['pair'], [])
        gap_series = [item['v'] for item in pair_hist]
        r['rsi_gap'] = calculate_rsi(np.array(gap_series)) if len(gap_series)>=8 else 50.0
        
        # Clés spécifiques pour le tri/affichage
        if view_type == "short":
            r['main_trend'] = r['short_fast'] if r['short_fast'] != "NEUTRAL" else r['short_slow']
            r['convergence'] = r['short_convergence']
        else:
            r['main_trend'] = r['large_fast'] if r['large_fast'] != "NEUTRAL" else r['large_slow']
            r['convergence'] = r['large_convergence']
            
        r['sort_score'] = r['gap'] + (100 if "🔥" in r['convergence'] else 0)
        
        # Filtre: on ne garde que si actif (comme simple.py : if mode != NEUTRAL)
        # Mais simple.py garde tout dans all_results et filtre pour le display.
        # On garde tout ici.
        view_results.append(r)
        
    return view_results, current_gaps

def generate_report(title, view_results, scores, transitions=None):
    # Filtrage pour l'affichage (ceux qui ont une tendance)
    active_res = [r for r in view_results if r['main_trend'] != "NEUTRAL"]
    active_res.sort(key=lambda x: x['sort_score'], reverse=True)
    
    top_gap = sorted(active_res, key=lambda x: x['gap'], reverse=True)[:5]
    runners = sorted(view_results, key=lambda x: abs(x.get('pct', 0)), reverse=True)[:5]
    
    accelerating = [r for r in view_results if r['rsi_gap'] > 55 and r['main_trend'] != "NEUTRAL"]
    top_rsi = sorted(accelerating, key=lambda x: x['rsi_gap'], reverse=True)[:5]
    
    decelerating = [r for r in view_results if r['rsi_gap'] < 45] # Decel ne regarde pas forcement la trend
    bottom_rsi = sorted(decelerating, key=lambda x: x['rsi_gap'])[:5]
    
    gap_pairs = {r['pair'] for r in top_gap}
    rsi_pairs = {r['pair'] for r in top_rsi}
    stars = gap_pairs.intersection(rsi_pairs)
    
    lines = []
    lines.append(f"🚀 {title}")
    lines.append("-" * 30)
    
    # TOP POWER
    lines.append("🏆 TOP 5 PUISSANCE")
    for i, r in enumerate(top_gap, 1):
        emoji = "🟢" if r['main_trend'] == "BULLISH" else "🔴"
        c_emoji = "🟢" if r['candle'] == "GREEN" else "🔴"
        lines.append(f"   {i}. {emoji} {c_emoji} {r['pair']:<7} (Gap: {r['gap']})")
    lines.append("-" * 20)
    
    # RUNNERS
    lines.append("🚀 TOP 5 RUNNER")
    for i, r in enumerate(runners, 1):
        val = r.get('pct', 0)
        t_emoji = "🟢" if r['main_trend'] == "BULLISH" else "🔴" if r['main_trend'] == "BEARISH" else "⚪"
        c_emoji = "🟢" if val > 0 else "🔴"
        lines.append(f"   {i}. {t_emoji} {c_emoji} {r['pair']:<7} ({val:+.2f}%)")
    lines.append("-" * 20)

    # ACCEL
    lines.append("🚀 ACCÉLÉRATION (RSI > 55)")
    if not top_rsi: lines.append("   (Aucune)")
    else:
        for i, r in enumerate(top_rsi, 1):
            emoji = "🟢" if r['main_trend'] == "BULLISH" else "🔴"
            c_emoji = "🟢" if r['candle'] == "GREEN" else "🔴"
            lines.append(f"   {i}. {emoji} {c_emoji} {r['pair']:<7} (RSI: {r['rsi_gap']:.1f})")
    lines.append("-" * 20)
    
    # DUEL
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top_2 = [c for c, s in sorted_scores[:2]]
    flop_2 = [c for c, s in sorted_scores[-2:]]
    
    duel_pair = None
    tc = "UNKNOWN"
    
    def get_duel(c1, c2):
        pair = f"{c1}{c2}"
        rev = False
        if pair not in PAIR_CODES:
            pair = f"{c2}{c1}"
            rev = True
        if pair not in PAIR_CODES: return None
        # On cherche dans view_results
        res = next((x for x in view_results if x['pair'] == pair), None)
        if not res: return None
        can = res['candle']
        win = None
        if rev: win = c2 if can=="GREEN" else c1 if can=="RED" else None
        else: win = c1 if can=="GREEN" else c2 if can=="RED" else None
        return win, pair, can

    true_king, weakest = None, None
    if len(top_2)==2:
        res = get_duel(top_2[0], top_2[1])
        if res: true_king = res[0]
    if len(flop_2)==2:
        res = get_duel(flop_2[0], flop_2[1])
        if res: weakest = res[0] if res[0]!=flop_2[0] else flop_2[1]
        
    if true_king and weakest:
        res = get_duel(true_king, weakest)
        if res:
            _, tp, trend_c = res
            base = tp[:3]
            exp = "GREEN" if true_king == base else "RED"
            if trend_c == exp: 
                duel_pair = tp
                tc = trend_c

    if duel_pair: lines.append(f"⚡ DUEL TREND: {duel_pair} ({tc})")
    else: lines.append("⚡ DUEL TREND: (Pas de duel validé)")
    lines.append("-" * 20)
    
    # CONFIRMED
    candidates = {}
    def is_conf(r):
        d = r['main_trend']
        c = r['candle']
        if d == "BULLISH" and c == "GREEN": return True
        if d == "BEARISH" and c == "RED": return True
        return False

    for r in top_gap:
        if is_conf(r): candidates.setdefault(r['pair'], []).append("Power")
    for r in runners:
        if is_conf(r): candidates.setdefault(r['pair'], []).append("Runner")
    for r in top_rsi:
        if is_conf(r): candidates.setdefault(r['pair'], []).append("Accel")
    if duel_pair:
        candidates.setdefault(duel_pair, []).append("Duel")
    
    # Exclusion des paires en décélération de la liste des opportunités
    decel_pairs = {r['pair'] for r in bottom_rsi}
    candidates = {pair: tags for pair, tags in candidates.items() if pair not in decel_pairs}
        
    lines.append("🎯 OPPORTUNITÉS CONFIRMÉES")
    if not candidates: lines.append("   (Aucune)")
    else:
        sorted_c = sorted(candidates.items(), key=lambda x: (-len(x[1]), x[0]))
        for i, (pair, tags) in enumerate(sorted_c, 1):
            res = next((x for x in view_results if x['pair'] == pair), None)
            d = res['main_trend']
            t_e = "🟢" if d == "BULLISH" else "🔴"
            c_e = "🟢" if res['candle'] == "GREEN" else "🔴"
            lines.append(f"   {i}. {t_e} {c_e} {pair:<7} ({', '.join(tags)})")
            
    # DECELERATION (Console)
    lines.append("-" * 20)
    lines.append("📉 DÉCÉLÉRATION")
    if not bottom_rsi: lines.append("   (Aucune)")
    else:
        for i, r in enumerate(bottom_rsi, 1):
            emoji = "🟢" if r['main_trend'] == "BULLISH" else "🔴" if r['main_trend'] == "BEARISH" else "⚪"
            c_emoji = "🟢" if r['candle'] == "GREEN" else "🔴"
            star = "⭐" if r['pair'] in stars else ""
            extra = "⚠️" if r['rsi_gap'] < 30 else ""
            lines.append(f"   {i}. {emoji} {c_emoji} {r['pair']:<7} (RSI: {r['rsi_gap']:.1f}) {extra} {star}")

    # --- TELEGRAM REPORT CONSTRUCTION ---
    short_title = title.split(' (')[0]
    tg_lines = []
    tg_lines.append(f"🚀 *{short_title}*")
    tg_lines.append("-" * 25)
    


    # CONFIRMED (TG)
    tg_lines.append("🎯 OPPORTUNITÉS")
    if not candidates: tg_lines.append("   (Aucune)")
    else:
        sorted_c = sorted(candidates.items(), key=lambda x: (-len(x[1]), x[0]))
        for i, (pair, tags) in enumerate(sorted_c, 1):
            res = next((x for x in view_results if x['pair'] == pair), None)
            d = res['main_trend']
            t_e = "🟢" if d == "BULLISH" else "🔴"
            c_e = "🟢" if res['candle'] == "GREEN" else "🔴"
            # Format compact TG
            tg_lines.append(f"   {t_e} {c_e} {pair:<7} ({', '.join(tags)})")
            
    tg_lines.append("-" * 20)
    
    # DECELERATION (TG)
    tg_lines.append("📉 DÉCÉLÉRATION")
    if not bottom_rsi: tg_lines.append("   (Aucune)")
    else:
        for i, r in enumerate(bottom_rsi, 1):
            emoji = "🟢" if r['main_trend'] == "BULLISH" else "🔴" if r['main_trend'] == "BEARISH" else "⚪"
            c_emoji = "🟢" if r['candle'] == "GREEN" else "🔴"
            star = "⭐" if r['pair'] in stars else ""
            extra = "⚠️" if r['rsi_gap'] < 30 else ""
            tg_lines.append(f"   {emoji} {c_emoji} {r['pair']:<7} (RSI: {r['rsi_gap']:.1f}) {extra} {star}")

    return "\n".join(lines), "\n".join(tg_lines), candidates

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram: Token ou Chat ID manquant")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            print("📤 Message Telegram envoyé avec succès")
        else:
            print(f"⚠️ Erreur Telegram ({response.status_code}): {response.text}")
    except Exception as e:
        print(f"⚠️ Exception Telegram: {e}")

def main_cli(debug_pair_code=None, reset_history=False, full_report=False, force_save=False):
    global FORCE_SAVE
    FORCE_SAVE = force_save
    
    # Mettre à jour le mode lecture seule de stats_tracker si force_save est activé
    if force_save:
        stats_tracker.set_read_only(False)
        print("⚠️ Mode Force Save activé (Sauvegarde locale autorisée)")
    
    print(f"\n🚀 TRIPLE G SCANNER (Ultimate Separated) - LOCAL")
    print("=" * 40)

    if reset_history and os.path.exists(HISTORY_FILE): os.remove(HISTORY_FILE)

    full_history = load_history()
    pairs_to_scan = PAIR_CODES if not debug_pair_code else [debug_pair_code]
    all_raw_results = []

    print("Analyse en cours (Multithread)...")
    start_time = time.time()
    max_w = 1 if debug_pair_code else 2 # 2 workers comme simple.py pour stabilité

    with ThreadPoolExecutor(max_workers=max_w) as executor:
        future_to_pair = {executor.submit(analyze_pair_full, pair): pair for pair in pairs_to_scan}
        for future in as_completed(future_to_pair):
            try:
                res = future.result()
                all_raw_results.append(res)
            except Exception: pass

    print(f"\n⏱️ Temps d'analyse: {time.time() - start_time:.2f}s\n")

    # --- PROCESSING SHORT VIEW ---
    scores_short = calculate_scores_separated(all_raw_results, "short")
    results_short, gaps_short = process_view_results(all_raw_results, scores_short, "short", full_history)
    
    # --- PROCESSING LARGE VIEW ---
    scores_large = calculate_scores_separated(all_raw_results, "large")
    results_large, gaps_large = process_view_results(all_raw_results, scores_large, "large", full_history)

    # --- SAUVEGARDE HISTORIQUE GLOBAL ---
    save_history(full_history)
    
    # --- SAUVEGARDE ÉTATS (LAST STATES) ---
    # On charge les ANCIENS états avant de les écraser
    old_states_full = load_last_states()
    
    # On reconstruit la structure d'états séparés
    last_states = {"short": {}, "large": {}}
    
    # Helper pour extraire les états
    def get_states_from_results(results):
        st = {}
        # Il faut réidentifier les groupes (Top Gap, RSI...)
        # Pour simplifier, on stocke l'état "main_trend" si actif, ou DECEL/ACCEL
        # La logique simple.py stocke: POWER, ACCEL, DECEL ou NONE
        # On doit refaire le tri sommaire ici
        active = [r for r in results if r['main_trend']!="NEUTRAL"]
        for r in results: st[r['pair']] = "NONE"
        
        # Recalcul rapide des listes pour tags
        # (Copie simplifiée de la logique display)
        active.sort(key=lambda x: x['sort_score'], reverse=True)
        top_gap = sorted(active, key=lambda x: x['gap'], reverse=True)[:5]
        
        accel = [r for r in active if r['rsi_gap']>55]
        top_rsi = sorted(accel, key=lambda x: x['rsi_gap'], reverse=True)[:5]
        
        decel = [r for r in results if r['rsi_gap']<45]
        bot_rsi = sorted(decel, key=lambda x: x['rsi_gap'])[:5]
        
        for r in bot_rsi: st[r['pair']] = "DECEL"
        for r in top_rsi: st[r['pair']] = "ACCEL"
        for r in top_gap: st[r['pair']] = "POWER"
        return st

    last_states["short"] = get_states_from_results(results_short)
    last_states["large"] = get_states_from_results(results_large)
    
    # --- DÉTECTION TRANSITIONS ---
    trans_short = detect_transitions(old_states_full.get("short", {}), last_states["short"])
    trans_large = detect_transitions(old_states_full.get("large", {}), last_states["large"])
    
    save_last_states(last_states)

    # --- RAPPORTS ---
    print("\n" + "="*40)
    rep_short_console, rep_short_tg, candidates_short = generate_report("SHORT VIEW (H1 + Daily)", results_short, scores_short, trans_short)
    print(rep_short_console)
    
    print("\n" + "="*40)
    rep_large_console, rep_large_tg, candidates_large = generate_report("LARGE VIEW (Daily + Weekly)", results_large, scores_large, trans_large)
    print(rep_large_console)
    
    # --- DÉTECTION OPPORTUNITÉS VALIDÉES (ASSOUPLI) ---
    # Règle : Short OU Large, si tags contiennent "Power" ET "Runner".
    # + Filtre Momentum > 0.1%
    
    eligible_pairs = set()
    
    # Vérification vue SHORT
    for pair, tags in candidates_short.items():
        if "Power" in tags and "Runner" in tags:
            eligible_pairs.add(pair)
            
    # Vérification vue LARGE
    for pair, tags in candidates_large.items():
        if "Power" in tags and "Runner" in tags:
            eligible_pairs.add(pair)
    
    # Liste finale après filtre Momentum
    confluence_pairs = set()
    
    if eligible_pairs:
        print(f"\n🔍 Vérification Power + Runner + Momentum (> 0.1%) sur {len(eligible_pairs)} paires:")
        
    for pair in eligible_pairs:
        # On récupère les données (priorité short pour les données temps réel)
        res = next((r for r in results_short if r['pair'] == pair), None)
        if not res:
             res = next((r for r in results_large if r['pair'] == pair), None)
             
        if res:
            pct = res.get('pct_real', 0.0)
            direction = res['main_trend']
            
            # 1. FILTRE MOMENTUM
            # > +0.1% pour Achat, < -0.1% pour Vente
            momentum_ok = False
            if (direction == "BULLISH" and pct > 0.1) or \
               (direction == "BEARISH" and pct < -0.1):
                momentum_ok = True
            
            if not momentum_ok:
                print(f"  ⚠️ {pair} ignoré (Variation trop faible: {pct:+.2f}%)")
                continue

            # Si tout est OK
            confluence_pairs.add(pair)
    
    # Si aucune paire ne passe le filtre, confluence_pairs sera vide
    
    # Construire section MIXT pour Telegram
    mixt_tg = []
    if confluence_pairs:
        mixt_tg.append(f"🔥 *OPPORTUNITÉS VALIDÉES ({len(confluence_pairs)})*")
        mixt_tg.append("-" * 25)
        for pair in sorted(confluence_pairs):
            # Récupérer les données de la paire depuis results_short
            res = next((r for r in results_short if r['pair'] == pair), None)
            if not res:
                continue
            data = {'main_trend': res['main_trend'], 'candle': res['candle'], 'pct_real': res.get('pct_real', 0.0)}
            t_e = "🟢" if data['main_trend'] == "BULLISH" else "🔴"
            c_e = "🟢" if data['candle'] == "GREEN" else "🔴"
            pct_str = f"{data['pct_real']:+.2f}%"
            # Ajouter une alerte visuelle si proche du seuil (optionnel, mais utile pour debug)
            mixt_tg.append(f"   {t_e} {c_e} {pair}  {pct_str}")
    
    # Marquer les paires en confluence dans SHORT et LARGE avec 🔥
    def add_confluence_marker(tg_report, confluence_pairs):
        lines = tg_report.split('\n')
        new_lines = []
        for line in lines:
            # Chercher si une paire de confluence est dans cette ligne
            marked = False
            for pair in confluence_pairs:
                if pair in line and "🎯 OPPORTUNITÉS" not in line:
                    # Ajouter 🔥 à la fin si pas déjà présent
                    if "🔥" not in line:
                        line = line.rstrip() + " 🔥"
                    marked = True
                    break
            new_lines.append(line)
        return '\n'.join(new_lines)
    
    rep_short_tg = add_confluence_marker(rep_short_tg, confluence_pairs)
    rep_large_tg = add_confluence_marker(rep_large_tg, confluence_pairs)
    
    print("\n" + "-" * 40)
    utc = datetime.now(timezone.utc)
    paris = utc + timedelta(hours=2 if 3<=utc.month<=10 else 1)
    print(f"⏰ {paris.strftime('%Y-%m-%d %H:%M')} Paris\n")

    # --- TRACKING STATISTIQUE ---
    print("\n📊 Mise à jour statistiques...")
    
    # Vérifier si on est à 22H (Market Close)
    is_market_close = (paris.hour == 22)
    
    # Vérifier opportunités actives (passer les paires MIXT actuelles pour détection OUT_OF_MIXT)
    # Si market_close=True, toutes les positions seront fermées
    stats_tracker.update_active_opportunities(PAIR_MAP, set(confluence_pairs), market_close=is_market_close)
    
    # Tracker UNIQUEMENT les paires en CONFLUENCE MIXT (et seulement entre 8H et 22H)
    if confluence_pairs and 6 <= paris.hour < 22:
        # Créer une liste de résultats enrichis pour les paires MIXT
        mixt_results = []
        for pair in confluence_pairs:
            # Prendre les données de SHORT (pourrait être LARGE, peu importe)
            res = next((r for r in results_short if r['pair'] == pair), None)
            if res:
                # Enrichir avec info de confluence
                res_copy = res.copy()
                res_copy['confluence'] = True
                mixt_results.append(res_copy)
        
        # Tracker ces opportunités comme "mixt"
        stats_tracker.track_new_opportunities(mixt_results, "mixt", PAIR_MAP)
        print(f"  ✅ {len(mixt_results)} opportunités MIXT trackées")
    else:
        print("  ℹ️ Aucune confluence MIXT à tracker")
    
    print("✅ Statistiques mises à jour\n")

    # --- GESTION OUT OF MIXT ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tracker_file = os.path.join(script_dir, "mixt_tracker.json")
    
    def load_mixt_tracker():
        if not os.path.exists(tracker_file):
            return {"current_mixt": [], "out_of_mixt": [], "last_reset": "", "last_telegram_time": ""}
        try:
            with open(tracker_file, 'r') as f:
                return json.load(f)
        except:
            return {"current_mixt": [], "out_of_mixt": [], "last_reset": "", "last_telegram_time": ""}
    
    def save_mixt_tracker(data):
        if not IS_GITHUB_ACTION and not FORCE_SAVE:
            return
        try:
            with open(tracker_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"⚠️ Erreur sauvegarde tracker MIXT: {e}")
    
    tracker = load_mixt_tracker()
    
    # Vérifier si reset nécessaire (nouveau jour de trading = après 8H)
    last_reset = datetime.fromisoformat(tracker["last_reset"]) if tracker["last_reset"] else datetime.min.replace(tzinfo=timezone.utc)
    
    # Convertir last_reset en heure de Paris pour comparaison
    if last_reset.tzinfo is None:
        last_reset = last_reset.replace(tzinfo=timezone.utc)
    last_reset_paris = last_reset + timedelta(hours=2 if 3 <= last_reset.month <= 10 else 1)
    
    # Reset si on est un nouveau jour ET qu'on a dépassé 8H
    current_day = paris.date()
    last_reset_day = last_reset_paris.date()
    
    if current_day > last_reset_day and paris.hour >= 6:
        # Reset quotidien (nouveau jour de trading)
        tracker["out_of_mixt"] = []
        tracker["last_reset"] = paris.isoformat()
        print("🔄 Reset OUT OF MIXT (Nouveau jour de trading)")
    
    # Market Close à 22H : vider OUT OF MIXT
    if paris.hour == 22:
        tracker["out_of_mixt"] = []
        print("🌙 MARKET CLOSE (22H) - Liste OUT OF MIXT vidée")
    
    # Période active (8H-22H) - Tracking des entrées/sorties de MIXT
    if 6 <= paris.hour < 22:
        previous_mixt = set(tracker["current_mixt"])
        current_mixt = set([p for p in confluence_pairs])
        
        # Détecter sorties
        exited = previous_mixt - current_mixt
        for pair in exited:
            # Vérifier si pas déjà dans OUT
            if not any(out["pair"] == pair for out in tracker["out_of_mixt"]):
                # Récupérer direction
                res = next((r for r in results_short if r['pair'] == pair), None)
                direction = res['main_trend'] if res else "NEUTRAL"
                candle = res['candle'] if res else "UNKNOWN"
                
                tracker["out_of_mixt"].append({
                    "pair": pair,
                    "exit_time": paris.isoformat(),
                    "direction": direction,
                    "candle": candle
                })
                print(f"⚠️ OUT: {pair} sorti de MIXT à {paris.strftime('%HH')}")
        
        # Détecter retours
        returned = current_mixt & set([out["pair"] for out in tracker["out_of_mixt"]])
        for pair in returned:
            tracker["out_of_mixt"] = [out for out in tracker["out_of_mixt"] if out["pair"] != pair]
            print(f"🔄 {pair} revenu dans MIXT")
        
        # Mettre à jour current_mixt
        tracker["current_mixt"] = list(current_mixt)
    
    save_mixt_tracker(tracker)

    # Fonction pour construire MIXT avec tags et tri par runner
    def build_mixt_section_with_tags(confluence_pairs, results_short, results_large):
        if not confluence_pairs:
            return None
        
        # Enrichir les paires avec leurs données
        mixt_data = []
        for pair in confluence_pairs:
            res_s = next((r for r in results_short if r['pair'] == pair), None)
            res_l = next((r for r in results_large if r['pair'] == pair), None)
            
            if not res_s:
                continue
            
            # Extraire tags SHORT
            tags_short = []
            if pair in candidates_short:
                tags_short = candidates_short[pair]
            
            # Extraire tags LARGE
            tags_large = []
            if pair in candidates_large:
                tags_large = candidates_large[pair]
            
            # Union des tags
            all_tags = list(set(tags_short + tags_large))
            
            # Récupérer le % runner (mouvement daily)
            pct_real = res_s.get('pct', 0)  # Valeur réelle (avec signe)
            runner_pct = abs(pct_real)       # Valeur absolue (pour tri)
            
            mixt_data.append({
                'pair': pair,
                'main_trend': res_s['main_trend'],
                'candle': res_s['candle'],
                'tags': all_tags,
                'runner_pct': runner_pct,
                'pct_real': pct_real
            })
        
        # Trier par runner (du plus fort au plus faible)
        mixt_data.sort(key=lambda x: x['runner_pct'], reverse=True)
        
        # Construire le texte
        lines = []
        lines.append(f"🔥 *CONFLUENCE MIXT ({len(mixt_data)})*")
        lines.append("-" * 25)
        
        for data in mixt_data:
            t_e = "🟢" if data['main_trend'] == "BULLISH" else "🔴"
            c_e = "🟢" if data['candle'] == "GREEN" else "🔴"
            pct_str = f"{data['pct_real']:+.2f}%"
            
            # Ajouter signal PSAR 8am
            signal = get_pair_signal_8am(data['pair'])
            
            lines.append(f"   {t_e} {c_e} {data['pair']:<7} {pct_str}{signal}")
        
        return '\n'.join(lines)
    
    # Construction message Telegram selon le mode
    if full_report:
        # MODE COMPLET : MIXT + DÉCÉLÉRATION + SHORT/LARGE (sans doublons)
        rep_short_tg = add_confluence_marker(rep_short_tg, confluence_pairs)
        rep_large_tg = add_confluence_marker(rep_large_tg, confluence_pairs)
        
        # Construire MIXT avec tags et trié par runner
        mixt_section_full = build_mixt_section_with_tags(confluence_pairs, results_short, results_large)
        
        if mixt_section_full:
            full_msg = f"🚀 *TRIPLE G ULTIMATE*\n\n{mixt_section_full}\n\n==============\n\n{rep_short_tg}\n\n==============\n\n{rep_large_tg}\n\n⏰ {paris.strftime('%Y-%m-%d %H:%M')} Paris"
        else:
            full_msg = f"🚀 *TRIPLE G ULTIMATE*\n\n{rep_short_tg}\n\n==============\n\n{rep_large_tg}\n\n⏰ {paris.strftime('%Y-%m-%d %H:%M')} Paris"
    else:
        # MODE MINIMALISTE : MIXT SEUL + OUT
        mixt_section_full = build_mixt_section_with_tags(confluence_pairs, results_short, results_large)
        
        # Construire section OUT OF MIXT
        out_section = ""
        if tracker["out_of_mixt"]:
            out_lines = []
            out_lines.append(f"⚠️ *OUT OF MIXT ({len(tracker['out_of_mixt'])})*")
            out_lines.append("-" * 25)
            for out in tracker["out_of_mixt"]:
                exit_dt = datetime.fromisoformat(out["exit_time"])
                
                # Récupérer les couleurs ACTUELLES (temps réel) au lieu de celles figées
                pair = out["pair"]
                res = next((r for r in results_short if r['pair'] == pair), None)
                
                if res:
                    current_direction = res['main_trend']
                    current_candle = res['candle']
                    t_e = "🟢" if current_direction == "BULLISH" else "🔴" if current_direction == "BEARISH" else "⚪"
                    c_e = "🟢" if current_candle == "GREEN" else "🔴"
                else:
                    # Fallback si la paire n'est plus dans les résultats
                    t_e = "⚪"
                    c_e = "⚪"
                
                out_lines.append(f"   {t_e} {c_e} {pair:<7} (Sorti à {exit_dt.strftime('%HH')})")
            out_section = "\n\n" + "\n".join(out_lines)
        
        # Construire section PORTEFEUILLE
        portfolio = stats_tracker.get_portfolio_summary(PAIR_MAP)
        pf_lines = []
        pf_lines.append(f"💰 *PORTEFEUILLE*")
        pf_lines.append("-" * 25)
        
        cap_cur = portfolio["current_capital"]
        cap_ini = portfolio["initial_capital"]
        
        # Calculer l'Equity (Capital Latent) = Capital Réalisé + PNL Latent
        equity = cap_cur + portfolio['active_pnl_eur']
        equity_pct = ((equity - cap_ini) / cap_ini) * 100
        equity_emoji = "💚" if equity_pct >= 0 else "❌"
        
        pf_lines.append(f"   Capital : {equity:,.0f} {portfolio['currency']} ({equity_pct:+.1f}%) {equity_emoji}")
        pf_lines.append("")
        pf_lines.append(f"   En cours : {portfolio['active_pnl_eur']:+.0f} {portfolio['currency']} ({portfolio['active_count']} pos)")
        pf_lines.append(f"   Fermées 24h : {portfolio['h24_pnl_eur']:+.0f} {portfolio['currency']} ({portfolio['h24_count']} trades)")
        pf_lines.append(f"   Total accumulé : {portfolio['total_pnl_eur']:+.0f} {portfolio['currency']}")
        
        portfolio_section = "\n\n" + "\n".join(pf_lines)
        
        if mixt_section_full:
            full_msg = f"🚀 *TRIPLE G ULTIMATE*\n\n{mixt_section_full}{out_section}{portfolio_section}\n\n⏰ {paris.strftime('%Y-%m-%d %H:%M')} Paris"
        else:
            full_msg = f"🚀 *TRIPLE G ULTIMATE*\n\n❌ Aucune confluence détectée{out_section}{portfolio_section}\n\n⏰ {paris.strftime('%Y-%m-%d %H:%M')} Paris"
    
    
    # --- TELEGRAM SENDING (EVERY RUN) ---
    # Envoi systématique à chaque exécution (demande utilisateur)
    send_telegram_message(full_msg)
    

if __name__ == "__main__":
    args = parse_args()
    
    # Gérer commandes statistiques
    if args.stats or args.stats_summary or args.best_setups or args.active_trades or args.whipsaw_report or args.performance or args.reset_portfolio:
        auto = (not args.no_auto_install) and os.getenv("AUTO_INSTALL_DEPS","1").strip() not in ("0","false","False")
        try:
            ensure_deps(auto)
            setup_environment()
            
            if args.reset_portfolio:
                stats_tracker.reset_portfolio()
            elif args.stats:
                stats_tracker.display_pair_stats(args.stats)
            elif args.stats_summary:
                stats_tracker.display_summary()
            elif args.best_setups:
                stats_tracker.display_best_setups()
            elif args.active_trades:
                stats_tracker.display_active_trades()
            elif args.whipsaw_report:
                stats_tracker.display_whipsaw_report()
            elif args.performance:
                stats_tracker.display_performance_report()
        except DependencyError as e:
            print(f"❌ {e}")
        sys.exit(0)
    
    # Exécution normale du scanner
    auto = (not args.no_auto_install) and os.getenv("AUTO_INSTALL_DEPS","1").strip() not in ("0","false","False")
    try:
        ensure_deps(auto)
        setup_environment()
        main_cli(debug_pair_code=args.debug_pair, reset_history=args.reset_history, full_report=args.full_report, force_save=args.force_save)
    except DependencyError as e:
        print(f"❌ {e}")
