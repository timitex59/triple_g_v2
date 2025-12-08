#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CHF/JPY RUNNER RANKING
======================
Affiche uniquement le classement des runners (variation journalière)
pour toutes les paires impliquant CHF ou JPY.
"""

import sys
import time
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

# Force UTF-8 output for Windows console
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# Liste complète des paires (basée sur reference.py)
ALL_PAIRS = [
    "EURUSD=X","EURGBP=X","EURJPY=X","EURCHF=X","EURAUD=X","EURCAD=X","EURNZD=X",
    "GBPCHF=X","GBPAUD=X","GBPCAD=X","GBPNZD=X","GBPUSD=X","GBPJPY=X",
    "NZDCHF=X","NZDCAD=X","NZDUSD=X","NZDJPY=X",
    "USDCHF=X","USDJPY=X","USDCAD=X",
    "AUDCHF=X","AUDCAD=X","AUDUSD=X","AUDJPY=X","AUDNZD=X",
    "CHFJPY=X","CADJPY=X","CADCHF=X"
]

def get_target_pairs():
    """Filtre les paires pour ne garder que celles avec CHF ou JPY."""
    targets = []
    for p in ALL_PAIRS:
        clean_pair = p.replace("=X", "")
        if "CHF" in clean_pair or "JPY" in clean_pair:
            targets.append(clean_pair)
    return targets

def fetch_pair_data(pair, debug=False):
    """
    Récupère les données et calcule la variation.
    Méthode hybride : Daily Open (officiel) + 5m Close (précis)
    """
    ticker_symbol = f"{pair}=X"
    try:
        ticker = yf.Ticker(ticker_symbol)

        # 1. Récupérer Daily pour avoir l'Open officiel
        df_daily = ticker.history(period="5d", interval="1d")
        if df_daily.empty:
            return None

        # Nettoyage week-end (supprimer samedi/dimanche)
        last_dt = df_daily.index[-1]
        if last_dt.weekday() >= 5:  # 5=Sam, 6=Dim
            df_daily = df_daily.iloc[:-1]
            if df_daily.empty:
                return None
            last_dt = df_daily.index[-1]

        target_date = last_dt.strftime("%Y-%m-%d")

        # Open officiel du Daily
        daily_open = df_daily.iloc[-1]['Open']
        daily_close = df_daily.iloc[-1]['Close']

        # 2. Smart Close avec 5m (plus précis que H1)
        final_close = daily_close
        method = "Daily"
        close_time = "-"

        try:
            df_5m = ticker.history(period="1d", interval="5m")
            if not df_5m.empty:
                # Filtrer les bougies 5m du jour
                day_5m_candles = df_5m[df_5m.index.strftime("%Y-%m-%d") == target_date]
                if not day_5m_candles.empty:
                    final_close = day_5m_candles['Close'].iloc[-1]
                    close_time = day_5m_candles.index[-1].strftime("%H:%M")
                    method = "Smart(5m)"
        except:
            pass

        if daily_open == 0:
            return None

        # Calcul: Daily Open + Smart Close 5m
        pct_change = ((final_close - daily_open) / daily_open) * 100

        if debug:
            print(f"  {pair}: DailyOpen={daily_open:.5f}, Close({close_time})={final_close:.5f}, Pct={pct_change:+.2f}% [{method}]")

        return {
            "pair": pair,
            "pct": pct_change,
            "close": final_close,
            "open": daily_open,
            "method": method
        }
    except Exception as e:
        if debug:
            print(f"  {pair}: ERREUR - {e}")
        return None

def main():
    target_pairs = get_target_pairs()
    print(f"\n🚀 ANALYSE RUNNER CHF & JPY ({len(target_pairs)} paires)")
    print("=" * 40)
    print("Récupération des données en cours...")

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pair = {executor.submit(fetch_pair_data, pair): pair for pair in target_pairs}
        for future in as_completed(future_to_pair):
            res = future.result()
            if res:
                results.append(res)

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
        p1_base, p1_quote = pair1[:3], pair1[3:]
        p2_base, p2_quote = pair2[:3], pair2[3:]
        
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

        # On vérifie si elle existe dans ALL_PAIRS
        clean_all = [p.replace("=X","") for p in ALL_PAIRS]

        final_pair = None
        if candidate1 in clean_all: final_pair = candidate1
        elif candidate2 in clean_all: final_pair = candidate2

        if final_pair:
            return ("pair", final_pair)
        return (None, None)

    # --- TOP SECTION ---
    print("🚀 TOP (Meilleure Performance)")
    top_list = results[:2]

    # Calcul du rang de CHFJPY
    chfjpy_rank = results.index(chfjpy_res) + 1 if chfjpy_res else None
    mid_rank = len(results) // 2  # Milieu du classement

    # Si CHFJPY n'est pas dans le Top 2 MAIS est dans la première moitié du classement, on l'ajoute
    if chfjpy_res and chfjpy_res not in top_list and chfjpy_rank and chfjpy_rank <= mid_rank:
        top_list.append(chfjpy_res)

    for i, res in enumerate(top_list, 1):
        pair = res['pair']
        pct = res['pct']
        # On recalcule le rang réel si c'est CHFJPY ajouté manuellement
        rank = i
        if res == chfjpy_res and res not in results[:2]:
             rank = results.index(res) + 1

        emoji = "🟢" if pct > 0 else "🔴" if pct < 0 else "⚪"
        print(f"   {rank}. {emoji} {pair:<7} {pct:+.2f}%")

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
            rel_data = fetch_pair_data(info_value)
            if rel_data:
                rp = rel_data['pct']
                re = "🟢" if rp > 0 else "🔴" if rp < 0 else "⚪"
                print(f"   👉 Duel: {re} {info_value} {rp:+.2f}%")
                top_duel_pair = info_value
                top_duel_pct = rp
        elif info_type == "single":
            print(f"   💪 Devise forte : {info_value}")
            top_single_currency = info_value
        elif info_type == "intrus":
            print(f"   🎯 Devise intrue : {info_value}")
            top_single_currency = info_value

    print("-" * 20)

    # --- BOTTOM SECTION ---
    print("📉 LAST (Moins bonne Performance)")

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
        print(f"   {rank}. {emoji} {pair:<7} {pct:+.2f}%")

    # Analyse Relative LAST 2 (Les 2 derniers de la liste affichée)
    if len(last_list) >= 2:
        # On compare les deux derniers éléments de la liste affichée (souvent les rangs 12 et 13)
        p1 = last_list[-2]['pair']
        p2 = last_list[-1]['pair']
        info_type, info_value = get_relative_info(p1, p2)
        if info_type == "pair":
            rel_data = fetch_pair_data(info_value)
            if rel_data:
                rp = rel_data['pct']
                re = "🟢" if rp > 0 else "🔴" if rp < 0 else "⚪"
                print(f"   👉 Duel: {re} {info_value} {rp:+.2f}%")
                last_duel_pair = info_value
                last_duel_pct = rp
        elif info_type == "single":
            print(f"   📉 Devise faible : {info_value}")
            last_single_currency = info_value
        elif info_type == "intrus":
            print(f"   🎯 Devise intrue : {info_value}")
            last_single_currency = info_value

    # --- DUEL & DUEL SECTION ---
    if top_duel_pair and last_duel_pair and top_duel_pct is not None and last_duel_pct is not None:
        # Extraire devise forte du TOP duel
        # Rouge (<0) = devise forte est la 2ème (quote)
        # Vert (>0) = devise forte est la 1ère (base)
        if top_duel_pct < 0:
            strong_currency = top_duel_pair[3:]  # Quote
        else:
            strong_currency = top_duel_pair[:3]  # Base
        
        # Extraire devise faible du LAST duel
        # Rouge (<0) = devise faible est la 1ère (base)
        # Vert (>0) = devise faible est la 2ème (quote)
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
                dd_data = fetch_pair_data(final_pair)
                if dd_data:
                    dd_pct = dd_data['pct']
                    dd_emoji = "🟢" if dd_pct > 0 else "🔴" if dd_pct < 0 else "⚪"
                    print(f"   {dd_emoji} {final_pair} {dd_pct:+.2f}%")
                    print(f"   (Fort: {strong_currency} vs Faible: {weak_currency})")

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
            cross_data = fetch_pair_data(final_pair)
            if cross_data:
                cross_pct = cross_data['pct']
                cross_emoji = "🟢" if cross_pct > 0 else "🔴" if cross_pct < 0 else "⚪"
                print(f"   {cross_emoji} {final_pair} {cross_pct:+.2f}%")
                print(f"   ({top_single_currency} vs {last_single_currency})")

    print("-" * 40)
    print(f"✅ Analyse terminée ({len(results)}/{len(target_pairs)} paires)")

if __name__ == "__main__":
    main()
