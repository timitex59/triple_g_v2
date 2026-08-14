# -*- coding: utf-8 -*-
"""
RSB Exit Strategy Engine (Disabled in V1 by Default)
====================================================
Conforme à l'Amendement #8:
- Placée derrière le flag ENABLE_RSB_EXITS = False.
- La mission V1 du moteur est la détection, le ranking, la sélection et le timing d'entrée.
"""

from typing import Tuple
from relative_strength.config import ENABLE_RSB_EXITS, THRESHOLD_PIPS, TRAILING_DISTANCE
from relative_strength.models import RelativeStrengthState


def evaluate_exit_signal(state: RelativeStrengthState) -> Tuple[bool, str]:
    """
    Évalue si un signal de sortie doit être émis pour la position active.
    
    Returns:
    (should_exit: bool, exit_reason: str)
    """
    # Amendement #8 : Sorties désactivées en V1
    if not ENABLE_RSB_EXITS:
        return (False, "EXITS_DISABLED_IN_V1")
    
    # 1. EXIT HARD : ré-invalidation du mouvement sous -threshold
    if state.directional_pips < -THRESHOLD_PIPS:
        return (True, "EXIT_HARD_INVALIDATION")
    
    # 2. EXIT TRAILING : rétraction de TRAILING_DISTANCE pips depuis un MFE >= 15
    if state.mfe_pips >= 15.0 and (state.mfe_pips - state.directional_pips) >= TRAILING_DISTANCE:
        return (True, f"EXIT_TRAILING_STOP (MFE={state.mfe_pips:.1f}, Current={state.directional_pips:.1f})")
    
    # 3. EXIT MOMENTUM LOSS : vitesse et accélération durablement négatives
    if state.velocity_pips_per_hour < -5.0 and state.acceleration_pips_per_hour2 < -2.0:
        return (True, "EXIT_MOMENTUM_LOSS")
    
    # 4. EXIT ALIGNMENT LOSS : contradiction technique majeure
    if state.technical_alignment in ["↑↓", "↓↑"]:
        return (True, "EXIT_ALIGNMENT_LOSS")
        
    return (False, "NO_EXIT")
