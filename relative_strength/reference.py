# -*- coding: utf-8 -*-
"""
RSB Reference Price Engine
==========================
Gestion du prix de référence 07:00 Europe/Paris.
Conforme à l'Amendement #1:
- Utilise la même source de prix que le VIVIER / screener existant.
- Enregistre le premier prix réellement observé au premier run >= 07:00 Paris.
- Stocke reference_time, reference_price, reference_source.
- Reset automatique chaque jour.
"""

from datetime import datetime, time
from typing import Dict, Any, Optional
from zoneinfo import ZoneInfo
from relative_strength.config import PARIS_TZ, REFERENCE_HOUR


def is_at_or_after_reference_time(dt_paris: datetime) -> bool:
    """Vérifie si l'horodatage Paris est >= 07:00."""
    target_hour, target_minute = map(int, REFERENCE_HOUR.split(":"))
    ref_time = time(target_hour, target_minute)
    return dt_paris.time() >= ref_time


def update_pair_reference(
    pair: str,
    current_dt_paris: datetime,
    current_price: float,
    current_source: str,
    references_store: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Met à jour ou restitue le prix de référence de 07h pour la paire donnée.
    
    Format de store:
    {
        "NZDUSD": {
            "date": "2026-08-14",
            "reference_price": 0.58666,
            "reference_time": "07:19:02",
            "reference_source": "TradingView_WebSocket"
        }
    }
    """
    today_str = current_dt_paris.strftime("%Y-%m-%d")
    existing_ref = references_store.get(pair)
    
    # 1. Si la référence existe déjà pour aujourd'hui, la réutiliser
    if existing_ref and existing_ref.get("date") == today_str:
        return existing_ref
    
    # 2. Si on est >= 07:00 Paris aujourd'hui et qu'aucune référence n'est définie pour aujourd'hui
    if is_at_or_after_reference_time(current_dt_paris):
        new_ref = {
            "date": today_str,
            "reference_price": current_price,
            "reference_time": current_dt_paris.strftime("%H:%M:%S"),
            "reference_source": current_source,
            "pair": pair
        }
        references_store[pair] = new_ref
        return new_ref
    
    # 3. Si on est avant 07:00 Paris (ex: 05:30), si une référence d'hier existe, la conserver temporairement
    if existing_ref:
        return existing_ref
    
    # 4. Fallback temporaire au premier prix observé si aucune référence n'existe du tout
    fallback_ref = {
        "date": today_str,
        "reference_price": current_price,
        "reference_time": current_dt_paris.strftime("%H:%M:%S"),
        "reference_source": f"{current_source}_pre07h_fallback",
        "pair": pair
    }
    references_store[pair] = fallback_ref
    return fallback_ref
