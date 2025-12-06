#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FOREX OPPORTUNITY STATISTICS TRACKER
====================================
Système de suivi statistique des opportunités détectées par le scanner.
Accumule des données pour identifier patterns prédictifs et optimiser le timing.

Fonctionnalités:
- Tracking continu des opportunités jusqu'à résolution (TP/SL/Invalidation/Stale)
- Catégorisation par vitesse (Fast/Medium/Slow)
- Calcul de statistiques multi-dimensionnelles
- Rétro-analyse des historiques existants
- Interface CLI pour consultation
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import yfinance as yf

# Configuration globale
TP_PERCENT = 0.5
SL_PERCENT = 0.3
STALE_DAYS = 7
STATS_FILE = ""

# Sessions de trading (UTC)
SESSIONS = {
    "Asia": (0, 9),
    "London": (8, 17),
    "NY": (13, 22)
}

# Spreads standards du marché (en pips)
SPREADS_STANDARD = {
    # Majors (spreads faibles)
    "EURUSD": 1.0, "GBPUSD": 1.5, "USDJPY": 1.0, "USDCHF": 1.5,
    # JPY crosses
    "EURJPY": 2.0, "GBPJPY": 2.5, "AUDJPY": 2.0, "CHFJPY": 2.5,
    "CADJPY": 2.5, "NZDJPY": 2.5,
    # EUR crosses
    "EURGBP": 1.5, "EURCHF": 2.0, "EURAUD": 2.5, "EURCAD": 2.5, "EURNZD": 3.0,
    # GBP crosses
    "GBPCHF": 3.0, "GBPAUD": 3.0, "GBPCAD": 3.5, "GBPNZD": 4.0,
    # Autres crosses
    "AUDCAD": 2.5, "AUDCHF": 2.5, "AUDNZD": 3.0, "CADCHF": 3.0,
    "NZDCAD": 3.5, "NZDCHF": 3.5, "NZDUSD": 2.0, "AUDUSD": 1.5, "USDCAD": 1.5
}

SLIPPAGE_PIPS = 0.5  # Slippage moyen conservateur

# Fichiers
STATS_FILE = ""
PORTFOLIO_FILE = ""
READ_ONLY = False

def set_read_only(enabled: bool):
    """Active ou désactive le mode lecture seule"""
    global READ_ONLY
    READ_ONLY = enabled
    if enabled:
        print("🔒 Stats Tracker: Mode Lecture Seule activé")

def init_stats_file(base_dir: str):
    """Initialise les chemins des fichiers de stats et portfolio"""
    global STATS_FILE, PORTFOLIO_FILE
    STATS_FILE = os.path.join(base_dir, "opportunity_stats.json")
    PORTFOLIO_FILE = os.path.join(base_dir, "portfolio_config.json")

def load_stats() -> Dict:
    """Charge les données statistiques"""
    if not os.path.exists(STATS_FILE):
        return {
            "active_opportunities": {},
            "completed_opportunities": [],
            "pair_statistics": {},
            "global_stats": {
                "total_opportunities_tracked": 0,
                "overall_success_rate": 0,
                "best_performing_pairs": [],
                "worst_performing_pairs": [],
                "most_reliable_profile": "",
                "best_session": "",
                "whipsaw_pairs": []
            }
        }
    
    try:
        with open(STATS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Erreur chargement stats: {e}")
        return {
            "active_opportunities": {},
            "completed_opportunities": [],
            "pair_statistics": {},
            "global_stats": {
                "total_opportunities_tracked": 0,
                "overall_success_rate": 0,
                "best_performing_pairs": [],
                "worst_performing_pairs": [],
                "most_reliable_profile": "",
                "best_session": "",
                "whipsaw_pairs": []
            }
        }

def save_stats(stats: Dict):
    """Sauvegarde les données statistiques"""
    if READ_ONLY:
        return
    try:
        # Convertir les types numpy en types Python natifs
        def convert_numpy(obj):
            import numpy as np
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {k: convert_numpy(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy(item) for item in obj]
            return obj
        
        clean_stats = convert_numpy(stats)
        
        with open(STATS_FILE, 'w') as f:
            json.dump(clean_stats, f, indent=2)
    except Exception as e:
        print(f"⚠️ Erreur sauvegarde stats: {e}")

def load_portfolio_config() -> Dict:
    """Charge la configuration du portefeuille"""
    if not os.path.exists(PORTFOLIO_FILE):
        # Config par défaut
        default_config = {
            "initial_capital": 10000.0,
            "currency": "EUR",
            "reset_date": datetime.now(timezone.utc).isoformat()
        }
        save_portfolio_config(default_config)
        return default_config
    
    try:
        with open(PORTFOLIO_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Erreur chargement portfolio: {e}")
        return {"initial_capital": 10000.0, "currency": "EUR", "reset_date": ""}

def save_portfolio_config(config: Dict):
    """Sauvegarde la configuration du portefeuille"""
    if READ_ONLY:
        return
    try:
        with open(PORTFOLIO_FILE, 'w') as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        print(f"⚠️ Erreur sauvegarde portfolio: {e}")

def reset_portfolio():
    """Réinitialise le portefeuille à zéro"""
    config = load_portfolio_config()
    config["reset_date"] = datetime.now(timezone.utc).isoformat()
    save_portfolio_config(config)
    
    # Archiver les stats anciennes
    stats = load_stats()
    stats["completed_opportunities"] = []
    stats["active_opportunities"] = {}
    save_stats(stats)
    
    print("✅ Portefeuille réinitialisé !")
    print(f"   Capital initial : {config['initial_capital']} {config['currency']}")
    print(f"   Date reset : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}")

def calculate_active_pnl(pair_map: Dict[str, str]) -> Tuple[float, float]:
    """Calcule le PNL non réalisé des positions actives"""
    stats = load_stats()
    total_pnl_percent = 0.0
    total_pnl_eur = 0.0
    config = load_portfolio_config()
    capital_per_trade = config["initial_capital"]  # Simple: même capital par trade
    
    for track_id, track in stats["active_opportunities"].items():
        pair = track["pair"]
        current_price = get_current_price(pair, pair_map)
        
        if current_price is None:
            continue
        
        entry_price = track["entry_price"]
        direction = track["direction"]
        
        # Calculer PNL brut
        pnl_percent = ((current_price - entry_price) / entry_price) * 100
        if direction == "BEARISH":
            pnl_percent = -pnl_percent
        
        # Calculer PNL net (avec coûts)
        pnl_net, _ = calculate_net_profit(pair, pnl_percent, entry_price)
        
        # Convertir en EUR
        pnl_eur = (pnl_net / 100) * capital_per_trade
        
        total_pnl_percent += pnl_net
        total_pnl_eur += pnl_eur
    
    return total_pnl_percent, total_pnl_eur

def calculate_24h_pnl() -> Tuple[float, float]:
    """Calcule le PNL des trades fermés dans les dernières 24h"""
    stats = load_stats()
    config = load_portfolio_config()
    capital_per_trade = config["initial_capital"]
    
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)
    
    total_pnl_percent = 0.0
    total_pnl_eur = 0.0
    
    for opp in stats["completed_opportunities"]:
        exit_time = datetime.fromisoformat(opp["exit_time"])
        
        if exit_time >= cutoff:
            pnl_net = opp.get("profit_net_percent", opp.get("profit_percent", 0))
            pnl_eur = (pnl_net / 100) * capital_per_trade
            
            total_pnl_percent += pnl_net
            total_pnl_eur += pnl_eur
    
    return total_pnl_percent, total_pnl_eur

def calculate_total_accumulated() -> Tuple[float, float]:
    """Calcule le PNL total accumulé depuis le reset"""
    stats = load_stats()
    config = load_portfolio_config()
    capital_per_trade = config["initial_capital"]
    
    total_pnl_percent = 0.0
    total_pnl_eur = 0.0
    
    for opp in stats["completed_opportunities"]:
        pnl_net = opp.get("profit_net_percent", opp.get("profit_percent", 0))
        pnl_eur = (pnl_net / 100) * capital_per_trade
        
        total_pnl_percent += pnl_net
        total_pnl_eur += pnl_eur
    
    return total_pnl_percent, total_pnl_eur

def get_portfolio_summary(pair_map: Dict[str, str]) -> Dict:
    """Récupère un résumé complet du portefeuille"""
    config = load_portfolio_config()
    
    active_pnl_pct, active_pnl_eur = calculate_active_pnl(pair_map)
    h24_pnl_pct, h24_pnl_eur = calculate_24h_pnl()
    total_pnl_pct, total_pnl_eur = calculate_total_accumulated()
    
    initial_capital = config["initial_capital"]
    current_capital = initial_capital + total_pnl_eur
    
    stats = load_stats()
    active_count = len(stats["active_opportunities"])
    
    # Compter trades 24h
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)
    h24_count = sum(1 for opp in stats["completed_opportunities"] 
                    if datetime.fromisoformat(opp["exit_time"]) >= cutoff)
    
    return {
        "initial_capital": initial_capital,
        "current_capital": current_capital,
        "currency": config["currency"],
        "active_pnl_eur": active_pnl_eur,
        "active_pnl_pct": active_pnl_pct,
        "active_count": active_count,
        "h24_pnl_eur": h24_pnl_eur,
        "h24_pnl_pct": h24_pnl_pct,
        "h24_count": h24_count,
        "total_pnl_eur": total_pnl_eur,
        "total_pnl_pct": total_pnl_pct
    }


def pips_to_percent(pips: float, pair: str, entry_price: float) -> float:
    """Convertit des pips en pourcentage"""
    if "JPY" in pair:
        pip_value = 0.01  # 1 pip = 0.01 pour JPY
    else:
        pip_value = 0.0001  # 1 pip = 0.0001 pour autres
    
    return (pips * pip_value / entry_price) * 100

def percent_to_pips(percent: float, pair: str, entry_price: float) -> float:
    """Convertit un pourcentage en pips"""
    if "JPY" in pair:
        pip_value = 0.01
    else:
        pip_value = 0.0001
    
    return (percent / 100) * entry_price / pip_value

def calculate_trading_costs(pair: str, entry_price: float) -> Tuple[float, float]:
    """
    Calcule les coûts de trading (spread + slippage)
    
    Returns:
        (cost_percent, cost_pips)
    """
    # Récupérer spread configuré ou utiliser défaut
    spread_pips = SPREADS_STANDARD.get(pair, 2.5)
    
    # Ajouter slippage
    total_cost_pips = spread_pips + SLIPPAGE_PIPS
    
    # Convertir en pourcentage
    cost_percent = pips_to_percent(total_cost_pips, pair, entry_price)
    
    return cost_percent, total_cost_pips

def calculate_net_profit(pair: str, profit_gross: float, entry_price: float) -> Tuple[float, float]:
    """
    Calcule le profit net après déduction des coûts de trading
    
    Returns:
        (profit_net_percent, profit_net_pips)
    """
    cost_percent, cost_pips = calculate_trading_costs(pair, entry_price)
    
    # Soustraire les coûts
    profit_net = profit_gross - cost_percent
    
    # Convertir en pips pour info
    profit_net_pips = percent_to_pips(profit_net, pair, entry_price)
    
    return profit_net, profit_net_pips


def calculate_tp_sl(entry_price: float, direction: str) -> Tuple[float, float]:
    """
    Calcule les targets TP et SL
    
    Args:
        entry_price: Prix d'entrée
        direction: "BULLISH" ou "BEARISH"
    
    Returns:
        (tp_target, sl_target)
    """
    if direction == "BULLISH":
        tp = entry_price * (1 + TP_PERCENT / 100)
        sl = entry_price * (1 - SL_PERCENT / 100)
    else:  # BEARISH
        tp = entry_price * (1 - TP_PERCENT / 100)
        sl = entry_price * (1 + SL_PERCENT / 100)
    
    return tp, sl

def get_session(hour_utc: int) -> str:
    """Détermine la session de trading selon l'heure UTC"""
    for session, (start, end) in SESSIONS.items():
        if start <= hour_utc < end:
            return session
    return "Off-hours"

def get_current_price(pair: str, pair_map: Dict[str, str]) -> Optional[float]:
    """Récupère le prix actuel d'une paire"""
    try:
        ticker = yf.Ticker(pair_map[pair])
        data = ticker.history(period="1d", interval="1m")
        if data.empty:
            return None
        return data['Close'].iloc[-1]
    except Exception:
        return None

def detect_resolution(
    current_price: float,
    entry_price: float,
    tp_target: float,
    sl_target: float,
    direction: str,
    entry_time: datetime,
    new_opportunities: Dict,
    pair: str,
    view: str
) -> Tuple[Optional[str], Optional[float]]:
    """
    Détecte si l'opportunité a atteint une résolution
    
    Returns:
        (outcome, profit_percent) ou (None, None) si toujours en tracking
    """
    now = datetime.now(timezone.utc)
    duration = (now - entry_time).total_seconds() / 3600  # heures
    
    # Vérifier STALE (7 jours)
    if duration > STALE_DAYS * 24:
        profit = ((current_price - entry_price) / entry_price) * 100
        if direction == "BEARISH":
            profit = -profit
        return "STALE", profit
    
    # Vérifier TP/SL
    if direction == "BULLISH":
        if current_price >= tp_target:
            return "TP_HIT", TP_PERCENT
        elif current_price <= sl_target:
            return "SL_HIT", -SL_PERCENT
    else:  # BEARISH
        if current_price <= tp_target:
            return "TP_HIT", TP_PERCENT
        elif current_price >= sl_target:
            return "SL_HIT", -SL_PERCENT
    
    # Vérifier INVALIDATION (signal inversé)
    opp_key = f"{pair}_{view}"
    if opp_key in new_opportunities:
        new_dir = new_opportunities[opp_key]["direction"]
        if new_dir != direction:
            profit = ((current_price - entry_price) / entry_price) * 100
            if direction == "BEARISH":
                profit = -profit
            return "INVALIDATED", profit
    
    return None, None

def categorize_speed(duration_hours: float) -> str:
    """Catégorise la vitesse de résolution"""
    if duration_hours < 12:
        return "fast"
    elif duration_hours < 48:
        return "medium"
    else:
        return "slow"

def create_opportunity_id(pair: str, view: str, timestamp: str) -> str:
    """Crée un ID unique pour une opportunité"""
    clean_ts = timestamp.replace(":", "").replace("-", "").replace("T", "_")[:15]
    return f"{pair}_{view}_{clean_ts}"

def track_new_opportunities(
    view_results: List[Dict],
    view_type: str,
    pair_map: Dict[str, str]
):
    """
    Détecte et crée le tracking des nouvelles opportunités
    
    Args:
        view_results: Résultats enrichis de la vue (short ou large)
        view_type: "short" ou "large"
        pair_map: Map des codes paires vers symboles yfinance
    """
    stats = load_stats()
    now = datetime.now(timezone.utc)
    timestamp = now.isoformat()
    
    # Construire dict des opportunités actuelles
    current_opps = {}
    
    for res in view_results:
        # Vérifier si c'est une opportunité confirmée
        main_trend = res.get('main_trend', 'NEUTRAL')
        candle = res.get('candle', 'UNKNOWN')
        
        is_confirmed = False
        if main_trend == "BULLISH" and candle == "GREEN":
            is_confirmed = True
        elif main_trend == "BEARISH" and candle == "RED":
            is_confirmed = True
        
        # Exclure si en décélération
        if res.get('rsi_gap', 50) < 45:
            is_confirmed = False
        
        if not is_confirmed:
            continue
        
        pair = res['pair']
        direction = main_trend
        
        # Récupérer prix actuel
        current_price = get_current_price(pair, pair_map)
        if current_price is None:
            continue
        
        # Calculer TP/SL
        tp_target, sl_target = calculate_tp_sl(current_price, direction)
        
        # Extraire tags
        tags = []
        if res.get('gap', 0) >= 3:
            tags.append("Power")
        if abs(res.get('pct', 0)) >= 0.3:
            tags.append("Runner")
        if res.get('rsi_gap', 50) > 55:
            tags.append("Accel")
        
        # Session et timing
        paris_time = now + timedelta(hours=2 if 3 <= now.month <= 10 else 1)
        hour = paris_time.hour
        day_of_week = paris_time.strftime("%A")
        session = get_session(now.hour)
        
        opp_data = {
            "pair": pair,
            "view": view_type,
            "entry_time": timestamp,
            "entry_price": current_price,
            "direction": direction,
            "tags": tags,
            "gap": res.get('gap', 0),
            "rsi_gap": res.get('rsi_gap', 50),
            "candle": candle,
            "day_of_week": day_of_week,
            "session": session,
            "hour": hour,
            "tp_target": tp_target,
            "sl_target": sl_target,
            "status": "tracking",
            "first_seen": timestamp,
            "consecutive_appearances": 1
        }
        
        current_opps[f"{pair}_{view_type}"] = opp_data
    
    # Vérifier si opportunités déjà en tracking
    for opp_key, opp_data in current_opps.items():
        # Chercher si existe déjà
        existing_id = None
        for track_id, track_data in stats["active_opportunities"].items():
            if track_data["pair"] == opp_data["pair"] and track_data["view"] == opp_data["view"]:
                existing_id = track_id
                break
        
        if existing_id:
            # Incrémenter consecutive_appearances
            stats["active_opportunities"][existing_id]["consecutive_appearances"] += 1
        else:
            # Créer nouveau tracking
            new_id = create_opportunity_id(opp_data["pair"], opp_data["view"], timestamp)
            stats["active_opportunities"][new_id] = opp_data
            print(f"  📊 Nouveau tracking: {opp_data['pair']} ({opp_data['view']}) - {opp_data['direction']}")
    
    save_stats(stats)

def update_active_opportunities(pair_map: Dict[str, str], current_mixt_pairs: set = None, market_close: bool = False):
    """
    Met à jour toutes les opportunités actives et vérifie les résolutions
    
    Args:
        pair_map: Map des codes paires vers symboles yfinance
        current_mixt_pairs: Set des paires actuellement en MIXT (pour détection OUT_OF_MIXT)
        market_close: Si True, ferme toutes les positions actives (clôture journalière à 22H)
    """
    if current_mixt_pairs is None:
        current_mixt_pairs = set()
    
    stats = load_stats()
    now = datetime.now(timezone.utc)
    
    # Si market_close, fermer TOUTES les positions actives
    if market_close:
        print("  🌙 MARKET CLOSE (22H) - Fermeture de toutes les positions")
        to_remove = []
        
        for track_id, track_data in stats["active_opportunities"].items():
            pair = track_data["pair"]
            current_price = get_current_price(pair, pair_map)
            if current_price is None:
                continue
            
            entry_time = datetime.fromisoformat(track_data["entry_time"])
            duration_hours = (now - entry_time).total_seconds() / 3600
            
            # Calculer profit au prix actuel
            profit = ((current_price - track_data["entry_price"]) / track_data["entry_price"]) * 100
            if track_data["direction"] == "BEARISH":
                profit = -profit
            
            # Créer l'enregistrement de clôture
            speed_category = categorize_speed(duration_hours)
            entry_price = track_data["entry_price"]
            profit_gross = profit
            profit_net, profit_net_pips = calculate_net_profit(pair, profit_gross, entry_price)
            cost_percent, cost_pips = calculate_trading_costs(pair, entry_price)
            profit_gross_pips = percent_to_pips(profit_gross, pair, entry_price)
            
            completed = {
                "pair": pair,
                "view": track_data["view"],
                "entry_time": track_data["entry_time"],
                "entry_price": entry_price,
                "exit_time": now.isoformat(),
                "duration_hours": round(duration_hours, 2),
                "outcome": "MARKET_CLOSE",
                "speed_category": speed_category,
                "profit_gross_percent": round(profit_gross, 2),
                "profit_gross_pips": round(profit_gross_pips, 1),
                "profit_net_percent": round(profit_net, 2),
                "profit_net_pips": round(profit_net_pips, 1),
                "trading_cost_percent": round(cost_percent, 4),
                "trading_cost_pips": round(cost_pips, 1),
                "direction": track_data["direction"],
                "tags": track_data["tags"],
                "gap": track_data["gap"],
                "rsi_gap": track_data["rsi_gap"],
                "session": track_data["session"],
                "day_of_week": track_data["day_of_week"],
                "hour": track_data["hour"],
                "confluence": False,
                "consecutive_appearances": track_data["consecutive_appearances"],
                "milestones": {"final": round(profit, 2)}
            }
            
            stats["completed_opportunities"].append(completed)
            to_remove.append(track_id)
            print(f"    🌙 {pair} ({track_data['view']}): MARKET_CLOSE après {duration_hours:.1f}h (Brut: {profit_gross:+.2f}% | Net: {profit_net:+.2f}%)")
        
        # Supprimer toutes les positions
        for track_id in to_remove:
            del stats["active_opportunities"][track_id]
        
        calculate_pair_statistics(stats)
        save_stats(stats)
        return
    
    # Logique normale (pas de market close)
    
    # Construire dict des opportunités actuelles pour détection invalidation
    # (sera rempli par track_new_opportunities avant cet appel normalement)
    current_opps = {}
    
    to_remove = []
    
    for track_id, track_data in stats["active_opportunities"].items():
        pair = track_data["pair"]
        
        # Récupérer prix actuel
        current_price = get_current_price(pair, pair_map)
        if current_price is None:
            continue
        
        entry_time = datetime.fromisoformat(track_data["entry_time"])
        
        # Vérifier si la paire est sortie de MIXT (seulement pour les opportunités 'mixt')
        if track_data.get("view") == "mixt" and pair not in current_mixt_pairs:
            # La paire est sortie de MIXT, on clôture la position
            duration_hours = (now - entry_time).total_seconds() / 3600
            profit = ((current_price - track_data["entry_price"]) / track_data["entry_price"]) * 100
            if track_data["direction"] == "BEARISH":
                profit = -profit
            
            outcome = "OUT_OF_MIXT"
            # Continue vers la section de clôture ci-dessous
        else:
            # Vérifier résolution normale (TP/SL/INVALIDATION/STALE)
            outcome, profit = detect_resolution(
                current_price,
                track_data["entry_price"],
                track_data["tp_target"],
                track_data["sl_target"],
                track_data["direction"],
                entry_time,
                current_opps,
                pair,
                track_data["view"]
            )
        
        if outcome:
            # Résolu !
            duration_hours = (now - entry_time).total_seconds() / 3600
            speed_category = categorize_speed(duration_hours)
            
            # Calculer milestones (simplifiés pour l'instant)
            milestones = {
                "final": round(profit, 2)
            }
            
            # Vérifier confluence (même paire dans les deux vues)
            confluence = False
            for other_id, other_data in stats["active_opportunities"].items():
                if other_id != track_id and other_data["pair"] == pair and other_data["view"] != track_data["view"]:
                    confluence = True
                    break
            
            # Calculer profit net et coûts de trading
            entry_price = track_data["entry_price"]
            profit_gross = profit  # Profit théorique
            
            profit_net, profit_net_pips = calculate_net_profit(pair, profit_gross, entry_price)
            cost_percent, cost_pips = calculate_trading_costs(pair, entry_price)
            
            # Convertir profit brut en pips pour info
            profit_gross_pips = percent_to_pips(profit_gross, pair, entry_price)
            
            completed = {
                "pair": pair,
                "view": track_data["view"],
                "entry_time": track_data["entry_time"],
                "entry_price": entry_price,
                "exit_time": now.isoformat(),
                "duration_hours": round(duration_hours, 2),
                "outcome": outcome,
                "speed_category": speed_category,
                # Profit brut (théorique)
                "profit_gross_percent": round(profit_gross, 2),
                "profit_gross_pips": round(profit_gross_pips, 1),
                # Profit net (réaliste avec coûts)
                "profit_net_percent": round(profit_net, 2),
                "profit_net_pips": round(profit_net_pips, 1),
                # Coûts de trading
                "trading_cost_percent": round(cost_percent, 4),
                "trading_cost_pips": round(cost_pips, 1),
                # Autres données
                "direction": track_data["direction"],
                "tags": track_data["tags"],
                "gap": track_data["gap"],
                "rsi_gap": track_data["rsi_gap"],
                "session": track_data["session"],
                "day_of_week": track_data["day_of_week"],
                "hour": track_data["hour"],
                "confluence": confluence,
                "consecutive_appearances": track_data["consecutive_appearances"],
                "milestones": milestones
            }
            
            stats["completed_opportunities"].append(completed)
            to_remove.append(track_id)
            
            outcome_emoji = "✅" if outcome == "TP_HIT" else "❌" if outcome == "SL_HIT" else "🔄" if outcome == "INVALIDATED" else "🚪" if outcome == "OUT_OF_MIXT" else "😴"
            print(f"  {outcome_emoji} {pair} ({track_data['view']}): {outcome} après {duration_hours:.1f}h (Brut: {profit_gross:+.2f}% | Net: {profit_net:+.2f}%)")
    
    # Supprimer les opportunités résolues
    for track_id in to_remove:
        del stats["active_opportunities"][track_id]
    
    # Recalculer statistiques par paire
    calculate_pair_statistics(stats)
    
    save_stats(stats)

def calculate_pair_statistics(stats: Dict):
    """Recalcule toutes les statistiques agrégées par paire"""
    pair_stats = {}
    
    for completed in stats["completed_opportunities"]:
        pair = completed["pair"]
        
        if pair not in pair_stats:
            pair_stats[pair] = {
                "total_signals": 0,
                "outcomes": {"tp_hit": 0, "sl_hit": 0, "invalidated": 0, "stale": 0},
                "success_rate": 0,
                "avg_duration_to_tp_hours": 0,
                "speed_distribution": {"fast": 0, "medium": 0, "slow": 0},
                "timing": {
                    "best_day": "",
                    "best_hour": 0,
                    "best_session": "",
                    "day_distribution": {},
                    "hour_distribution": {},
                    "session_distribution": {}
                },
                "profile_performance": {},
                "confluence_boost": {"short_only": 0, "large_only": 0, "both_views": 0},
                "volatility": {
                    "avg_profit_on_tp": 0,
                    "avg_loss_on_sl": 0,
                    "max_excursion_avg": 0
                },
                "whipsaw_score": 0,
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
        
        ps = pair_stats[pair]
        ps["total_signals"] += 1
        
        # Outcomes
        outcome_key = completed["outcome"].lower()
        if outcome_key in ps["outcomes"]:
            ps["outcomes"][outcome_key] += 1
        
        # Speed distribution
        if completed["speed_category"] in ps["speed_distribution"]:
            ps["speed_distribution"][completed["speed_category"]] += 1
        
        # Timing
        day = completed["day_of_week"]
        hour = completed["hour"]
        session = completed["session"]
        
        ps["timing"]["day_distribution"][day] = ps["timing"]["day_distribution"].get(day, 0) + 1
        ps["timing"]["hour_distribution"][str(hour)] = ps["timing"]["hour_distribution"].get(str(hour), 0) + 1
        ps["timing"]["session_distribution"][session] = ps["timing"]["session_distribution"].get(session, 0) + 1
        
        # Profile performance
        profile_key = "+".join(sorted(completed["tags"])) if completed["tags"] else "NoTags"
        if profile_key not in ps["profile_performance"]:
            ps["profile_performance"][profile_key] = {"signals": 0, "tp_rate": 0, "tp_count": 0}
        
        ps["profile_performance"][profile_key]["signals"] += 1
        if completed["outcome"] == "TP_HIT":
            ps["profile_performance"][profile_key]["tp_count"] += 1
    
    # Calculs finaux
    for pair, ps in pair_stats.items():
        total = ps["total_signals"]
        if total == 0:
            continue
        
        tp_count = ps["outcomes"]["tp_hit"]
        ps["success_rate"] = round((tp_count / total) * 100, 1)
        
        # Avg duration to TP
        tp_durations = [c["duration_hours"] for c in stats["completed_opportunities"] 
                       if c["pair"] == pair and c["outcome"] == "TP_HIT"]
        if tp_durations:
            ps["avg_duration_to_tp_hours"] = round(sum(tp_durations) / len(tp_durations), 1)
        
        # Best timing
        if ps["timing"]["day_distribution"]:
            ps["timing"]["best_day"] = max(ps["timing"]["day_distribution"], key=ps["timing"]["day_distribution"].get)
        if ps["timing"]["hour_distribution"]:
            ps["timing"]["best_hour"] = int(max(ps["timing"]["hour_distribution"], key=ps["timing"]["hour_distribution"].get))
        if ps["timing"]["session_distribution"]:
            ps["timing"]["best_session"] = max(ps["timing"]["session_distribution"], key=ps["timing"]["session_distribution"].get)
        
        # Profile performance TP rates
        for profile, data in ps["profile_performance"].items():
            if data["signals"] > 0:
                data["tp_rate"] = round((data["tp_count"] / data["signals"]) * 100, 1)
        
        # Volatility
        tp_profits = [c.get("profit_percent", c.get("profit_gross_percent", 0)) for c in stats["completed_opportunities"]
                     if c["pair"] == pair and c["outcome"] == "TP_HIT"]
        sl_losses = [c.get("profit_percent", c.get("profit_gross_percent", 0)) for c in stats["completed_opportunities"]
                    if c["pair"] == pair and c["outcome"] == "SL_HIT"]
        
        if tp_profits:
            ps["volatility"]["avg_profit_on_tp"] = round(sum(tp_profits) / len(tp_profits), 2)
        if sl_losses:
            ps["volatility"]["avg_loss_on_sl"] = round(sum(sl_losses) / len(sl_losses), 2)
        
        # Whipsaw score (invalidated / total)
        ps["whipsaw_score"] = round(ps["outcomes"]["invalidated"] / total, 2)
    
    stats["pair_statistics"] = pair_stats
    
    # Global stats
    if stats["completed_opportunities"]:
        all_pairs = list(pair_stats.keys())
        sorted_by_success = sorted(all_pairs, key=lambda p: pair_stats[p]["success_rate"], reverse=True)
        
        stats["global_stats"]["total_opportunities_tracked"] = len(stats["completed_opportunities"])
        stats["global_stats"]["best_performing_pairs"] = sorted_by_success[:5]
        stats["global_stats"]["worst_performing_pairs"] = sorted_by_success[-5:]
        
        # Most reliable profile
        all_profiles = {}
        for ps in pair_stats.values():
            for profile, data in ps["profile_performance"].items():
                if profile not in all_profiles:
                    all_profiles[profile] = {"signals": 0, "tp_count": 0}
                all_profiles[profile]["signals"] += data["signals"]
                all_profiles[profile]["tp_count"] += data["tp_count"]
        
        best_profile = ""
        best_rate = 0
        for profile, data in all_profiles.items():
            if data["signals"] >= 5:  # Minimum 5 signaux
                rate = (data["tp_count"] / data["signals"]) * 100
                if rate > best_rate:
                    best_rate = rate
                    best_profile = profile
        
        stats["global_stats"]["most_reliable_profile"] = best_profile
        
        # Whipsaw pairs
        whipsaw_pairs = [p for p, ps in pair_stats.items() if ps["whipsaw_score"] > 0.2]
        stats["global_stats"]["whipsaw_pairs"] = whipsaw_pairs

def display_pair_stats(pair: str):
    """Affiche les statistiques détaillées d'une paire"""
    stats = load_stats()
    
    if pair not in stats["pair_statistics"]:
        print(f"\n❌ Aucune statistique disponible pour {pair}")
        print("   Cette paire n'a pas encore été trackée ou n'a jamais été opportunité.")
        return
    
    ps = stats["pair_statistics"][pair]
    
    print("\n" + "="*50)
    print(f"{pair} - Statistiques de Performance")
    print("="*50)
    
    print("\n📊 Vue d'ensemble:")
    print(f"  Total Signaux: {ps['total_signals']}")
    tp = ps['outcomes']['tp_hit']
    sl = ps['outcomes']['sl_hit']
    inv = ps['outcomes']['invalidated']
    stale = ps['outcomes']['stale']
    print(f"  Taux de Succès: {ps['success_rate']}% ({tp} TP / {sl} SL / {inv} Invalidés / {stale} Stale)")
    if ps['avg_duration_to_tp_hours'] > 0:
        print(f"  Durée Moyenne TP: {ps['avg_duration_to_tp_hours']} heures")
    
    if sum(ps['speed_distribution'].values()) > 0:
        print("\n⏱️ Distribution Vitesse:")
        for speed, count in ps['speed_distribution'].items():
            if count > 0:
                label = "Fast (<12h)" if speed == "fast" else "Medium (12-48h)" if speed == "medium" else "Slow (48h+)"
                print(f"  {label}: {count} signaux")
    
    if ps['profile_performance']:
        print("\n🎯 Meilleurs Profils:")
        sorted_profiles = sorted(ps['profile_performance'].items(), 
                                key=lambda x: x[1]['tp_rate'], reverse=True)
        for profile, data in sorted_profiles[:5]:
            if data['signals'] >= 2:
                print(f"  {profile}: {data['tp_rate']}% ({data['signals']} signaux)")
    
    if ps['timing']['best_day']:
        print("\n⏰ Timing Optimal:")
        print(f"  Meilleur Jour: {ps['timing']['best_day']}")
        print(f"  Meilleure Heure: {ps['timing']['best_hour']}h")
        print(f"  Meilleure Session: {ps['timing']['best_session']}")
    
    if ps['volatility']['avg_profit_on_tp'] > 0:
        print("\n💰 Volatilité:")
        print(f"  Profit Moyen (TP): +{ps['volatility']['avg_profit_on_tp']}%")
        if ps['volatility']['avg_loss_on_sl'] < 0:
            print(f"  Perte Moyenne (SL): {ps['volatility']['avg_loss_on_sl']}%")
            ratio = abs(ps['volatility']['avg_profit_on_tp'] / ps['volatility']['avg_loss_on_sl'])
            print(f"  Ratio Risk/Reward: {ratio:.2f}")
    
    print(f"\n⚠️ Whipsaw Score: {ps['whipsaw_score']} {'(Faible - Stable)' if ps['whipsaw_score'] < 0.15 else '(Élevé - Instable)' if ps['whipsaw_score'] > 0.25 else '(Modéré)'}")
    
    # Recommandation
    print("\n💡 Recommandation:")
    avg_dur = ps['avg_duration_to_tp_hours']
    if avg_dur < 12:
        print("  Type de Trading: SCALPING / DAY TRADING")
        print("  Horizon: < 12 heures")
    elif avg_dur < 48:
        print("  Type de Trading: SWING TRADING")
        print("  Horizon: 12-48 heures")
    else:
        print("  Type de Trading: POSITION TRADING")
        print("  Horizon: 48+ heures")
    
    best_profile = max(ps['profile_performance'].items(), 
                      key=lambda x: x[1]['tp_rate']) if ps['profile_performance'] else None
    if best_profile and best_profile[1]['signals'] >= 2:
        print(f"  Setup Optimal: {best_profile[0]} pendant {ps['timing']['best_session']}, {ps['timing']['best_day']}")
    
    print()

def display_summary():
    """Affiche un résumé des statistiques globales"""
    stats = load_stats()
    gs = stats["global_stats"]
    
    print("\n" + "="*50)
    print("Résumé Statistiques Globales")
    print("="*50)
    
    print(f"\n📊 Total Opportunités Trackées: {gs['total_opportunities_tracked']}")
    
    if gs['best_performing_pairs']:
        print("\n🏆 Top 5 Paires Performantes:")
        for i, pair in enumerate(gs['best_performing_pairs'], 1):
            ps = stats['pair_statistics'][pair]
            print(f"  {i}. {pair}: {ps['success_rate']}% ({ps['total_signals']} signaux)")
    
    if gs['most_reliable_profile']:
        print(f"\n⭐ Profil le Plus Fiable: {gs['most_reliable_profile']}")
    
    if gs['whipsaw_pairs']:
        print(f"\n⚠️ Paires Instables (Whipsaw): {', '.join(gs['whipsaw_pairs'])}")
    
    print(f"\n📈 Opportunités Actives en Tracking: {len(stats['active_opportunities'])}")
    print()

def display_active_trades():
    """Affiche les opportunités actuellement en tracking"""
    stats = load_stats()
    
    if not stats["active_opportunities"]:
        print("\n📭 Aucune opportunité active en tracking")
        return
    
    print("\n" + "="*50)
    print(f"Opportunités Actives ({len(stats['active_opportunities'])})")
    print("="*50)
    
    for track_id, track in stats["active_opportunities"].items():
        entry_time = datetime.fromisoformat(track["entry_time"])
        now = datetime.now(timezone.utc)
        duration_h = (now - entry_time).total_seconds() / 3600
        
        dir_emoji = "🟢" if track["direction"] == "BULLISH" else "🔴"
        tags_str = ", ".join(track["tags"]) if track["tags"] else "No tags"
        
        print(f"\n{dir_emoji} {track['pair']} ({track['view'].upper()})")
        print(f"  Direction: {track['direction']}")
        print(f"  Tags: {tags_str}")
        print(f"  Entrée: {entry_time.strftime('%Y-%m-%d %H:%M')} ({duration_h:.1f}h ago)")
        print(f"  Prix: {track['entry_price']:.5f}")
        print(f"  TP: {track['tp_target']:.5f} | SL: {track['sl_target']:.5f}")
        print(f"  Apparitions consécutives: {track['consecutive_appearances']}")
    
    print()

def display_best_setups():
    """Affiche les configurations avec le meilleur taux de succès"""
    stats = load_stats()
    
    # Agréger tous les profils
    all_setups = {}
    
    for pair, ps in stats["pair_statistics"].items():
        for profile, data in ps["profile_performance"].items():
            key = f"{profile}"
            if key not in all_setups:
                all_setups[key] = {"signals": 0, "tp_count": 0, "pairs": []}
            all_setups[key]["signals"] += data["signals"]
            all_setups[key]["tp_count"] += data["tp_count"]
            all_setups[key]["pairs"].append(pair)
    
    # Calculer taux et trier
    for setup, data in all_setups.items():
        if data["signals"] > 0:
            data["tp_rate"] = (data["tp_count"] / data["signals"]) * 100
    
    sorted_setups = sorted(all_setups.items(), 
                          key=lambda x: (x[1]["tp_rate"], x[1]["signals"]), 
                          reverse=True)
    
    print("\n" + "="*50)
    print("Top Configurations (Minimum 3 signaux)")
    print("="*50)
    
    count = 0
    for setup, data in sorted_setups:
        if data["signals"] >= 3 and count < 10:
            count += 1
            print(f"\n{count}. {setup}")
            print(f"   Taux de Succès: {data['tp_rate']:.1f}%")
            print(f"   Signaux: {data['signals']} ({data['tp_count']} TP)")
            print(f"   Paires: {', '.join(set(data['pairs'][:5]))}")
    
    if count == 0:
        print("\n📭 Pas encore assez de données (minimum 3 signaux par setup)")
    
    print()

def display_whipsaw_report():
    """Affiche les paires avec un score whipsaw élevé"""
    stats = load_stats()
    
    whipsaw_pairs = [(pair, ps) for pair, ps in stats["pair_statistics"].items() 
                     if ps["whipsaw_score"] > 0.15]
    
    if not whipsaw_pairs:
        print("\n✅ Aucune paire instable détectée")
        return
    
    sorted_pairs = sorted(whipsaw_pairs, key=lambda x: x[1]["whipsaw_score"], reverse=True)
    
    print("\n" + "="*50)
    print("⚠️ Paires Instables (Whipsaw)")
    print("="*50)
    print("\nCes paires changent fréquemment de direction (signaux invalidés).")
    print("À éviter ou trader avec prudence.\n")
    
    for pair, ps in sorted_pairs:
        inv = ps["outcomes"]["invalidated"]
        total = ps["total_signals"]
        print(f"{pair}: {ps['whipsaw_score']:.2f} ({inv}/{total} invalidés)")
    
    print()

def display_performance_report():
    """Affiche un rapport de performance complet du scanner"""
    stats = load_stats()
    
    if not stats["completed_opportunities"]:
        print("\n📭 Pas encore de données de performance")
        print("   Attendez que le scanner accumule des opportunités résolues.\n")
        return
    
    completed = stats["completed_opportunities"]
    
    print("\n" + "="*60)
    print("📊 RAPPORT DE PERFORMANCE - SIGNAUX MIXT")
    print("="*60)
    
    # Stats globales
    total = len(completed)
    tp_count = sum(1 for c in completed if c["outcome"] == "TP_HIT")
    sl_count = sum(1 for c in completed if c["outcome"] == "SL_HIT")
    precision = (tp_count / total * 100) if total > 0 else 0
    
    print(f"\n🎯 STATISTIQUES GLOBALES (CONFLUENCE UNIQUEMENT)")
    print(f"   Total signaux MIXT résolus : {total}")
    print(f"   TP atteints : {tp_count} ✅")
    print(f"   SL touchés : {sl_count} ❌")
    print(f"   Précision : {precision:.1f}%")
    
    # Performance financière
    profit_gross = sum(c.get("profit_gross_percent", c.get("profit_percent", 0)) for c in completed)
    profit_net = sum(c.get("profit_net_percent", c.get("profit_percent", 0)) for c in completed)
    total_costs = profit_gross - profit_net
    
    profit_gross_pips = sum(c.get("profit_gross_pips", 0) for c in completed)
    profit_net_pips = sum(c.get("profit_net_pips", 0) for c in completed)
    
    print(f"\n💰 PERFORMANCE FINANCIÈRE")
    print(f"   Profit brut : {profit_gross:+.2f}% ({profit_gross_pips:+.0f} pips)")
    print(f"   Coûts trading : {total_costs:.2f}% ({profit_gross_pips - profit_net_pips:.0f} pips)")
    print(f"   Profit net : {profit_net:+.2f}% ({profit_net_pips:+.0f} pips) {'💚' if profit_net > 0 else '❌'}")
    
    if profit_net > 0:
        print(f"\n   💡 Si vous aviez tradé tous les signaux MIXT :")
        print(f"      Capital 10,000 EUR → {10000 * (1 + profit_net/100):.0f} EUR")
        print(f"      Gain net : {10000 * profit_net/100:+.0f} EUR")
    
    # Top performers
    pair_performance = {}
    for c in completed:
        pair = c["pair"]
        if pair not in pair_performance:
            pair_performance[pair] = {"total": 0, "tp": 0}
        pair_performance[pair]["total"] += 1
        if c["outcome"] == "TP_HIT":
            pair_performance[pair]["tp"] += 1
    
    # Filtrer et trier
    valid_pairs = {p: v for p, v in pair_performance.items() if v["total"] >= 3}
    if valid_pairs:
        sorted_pairs = sorted(valid_pairs.items(), key=lambda x: x[1]["tp"]/x[1]["total"], reverse=True)
        
        print(f"\n🏆 TOP 5 PAIRES PERFORMANTES (min 3 signaux)")
        for i, (pair, perf) in enumerate(sorted_pairs[:5], 1):
            prec = perf["tp"] / perf["total"] * 100
            print(f"   {i}. {pair}: {prec:.0f}% ({perf['tp']}/{perf['total']} TP)")
    
    # Patterns temporels
    day_stats = {}
    for c in completed:
        day = c.get("day_of_week", "Unknown")
        if day not in day_stats:
            day_stats[day] = {"total": 0, "tp": 0}
        day_stats[day]["total"] += 1
        if c["outcome"] == "TP_HIT":
            day_stats[day]["tp"] += 1
    
    if day_stats:
        best_day = max(day_stats.items(), key=lambda x: x[1]["tp"]/x[1]["total"] if x[1]["total"] > 0 else 0)
        print(f"\n🕐 PATTERNS TEMPORELS")
        print(f"   Meilleur jour : {best_day[0]} ({best_day[1]['tp']}/{best_day[1]['total']} TP = {best_day[1]['tp']/best_day[1]['total']*100:.0f}%)")
    
    print("\n" + "="*60)
    print()

