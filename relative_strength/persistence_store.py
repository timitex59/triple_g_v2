# -*- coding: utf-8 -*-
"""
RSB Persistence & Audit Trail Store
====================================
Gestion de la persistance JSON et de l'Audit Trail append-only.
Conforme à l'Amendement #11:
- À chaque run, sauvegarde un snapshot déterministe complet de toutes les métriques et classements.
- Permet au backtest de rejouer et vérifier exactement les classements affichés en temps réel.
"""

import json
import os
from typing import Dict, List, Any, Optional
from relative_strength.models import RelativeStrengthState

STATE_FILE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rsb_state.json")
AUDIT_TRAIL_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rsb_audit_trail.jsonl")


def load_rsb_state(state_path: str = STATE_FILE_PATH) -> Dict[str, Any]:
    """Charge le dernier état sauvegardé depuis rsb_state.json."""
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Erreur chargement rsb_state.json: {e}")
        return {}


def save_rsb_state(
    timestamp_iso: str,
    references: Dict[str, Any],
    states_list: List[RelativeStrengthState],
    currency_strengths: Dict[str, float],
    state_path: str = STATE_FILE_PATH,
    audit_trail_path: str = AUDIT_TRAIL_PATH
) -> bool:
    """
    Sauvegarde le snapshot courant (rsb_state.json) et l'ajoute à l'Audit Trail (rsb_audit_trail.jsonl).
    Amendement #11: Audit trail déterministe complet.
    """
    pairs_data = {state.pair: state.to_dict() for state in states_list}
    
    snapshot = {
        "timestamp": timestamp_iso,
        "references": references,
        "currency_strengths": currency_strengths,
        "pairs": pairs_data
    }
    
    try:
        # 1. Sauvegarde rsb_state.json (Dernier snapshot)
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2, ensure_ascii=False)
            
        # 2. Append dans rsb_audit_trail.jsonl (Journal d'Audit)
        with open(audit_trail_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(snapshot, ensure_ascii=False) + "\n")
            
        return True
    except Exception as e:
        print(f"⚠️ Erreur sauvegarde state/audit RSB: {e}")
        return False
