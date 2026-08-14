# -*- coding: utf-8 -*-
"""
RSB Reference Price Engine
==========================
Gestion du prix de référence 07:00 Europe/Paris.

Règles de Fixation du Niveau 0 (07h00 Paris) :
1. Avant 07h00 Paris (ex: 04h15 UTC = 06h15 Paris) : aucun niveau 0 officiel n'est fixé. Un fallback temporaire (is_fallback=True) est utilisé sans déclencher de signaux.
2. Au premier run >= 07h00 Paris (ex: 07h15 Paris) : le VRAI niveau 0 de 07h00 est figé pour toute la journée (is_fallback=False).
3. Pour tous les runs suivants de la journée (jusqu'à 22h00) : le niveau 0 figé à 07h00 reste strictement inchangé.
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
    Garantit que le VRAI prix de 07h00 est locked au premier run >= 07:00 Paris.
    """
    today_str = current_dt_paris.strftime("%Y-%m-%d")
    existing_ref = references_store.get(pair)
    
    # 1. Si la référence officielle existe déjà pour aujourd'hui et n'est pas un fallback pre-07h, la conserver
    if existing_ref and existing_ref.get("date") == today_str and not existing_ref.get("is_fallback", False):
        return existing_ref
    
    # 2. Si on est >= 07:00 Paris aujourd'hui : figer le VRAI niveau 0 de 07h00
    if is_at_or_after_reference_time(current_dt_paris):
        new_ref = {
            "date": today_str,
            "reference_price": current_price,
            "reference_time": current_dt_paris.strftime("%H:%M:%S"),
            "reference_source": current_source,
            "pair": pair,
            "is_fallback": False
        }
        references_store[pair] = new_ref
        return new_ref
    
    # 3. Si on est avant 07:00 Paris (ex: 06h15 Paris / 04h15 UTC) : enregistrer un fallback temporaire marqué is_fallback=True
    fallback_ref = {
        "date": today_str,
        "reference_price": current_price,
        "reference_time": current_dt_paris.strftime("%H:%M:%S"),
        "reference_source": f"{current_source}_pre07h_fallback",
        "pair": pair,
        "is_fallback": True
    }
    references_store[pair] = fallback_ref
    return fallback_ref
