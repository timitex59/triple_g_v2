# -*- coding: utf-8 -*-
"""
RSB Triple Ranking Engine
=========================
Génération des 3 classements indépendants à chaque run.
Conforme aux Amendements #9, #12 et au Trade Eligibility Gate:
1. PIP RANK (Performance) : Classement brut par pips parcourus depuis 07h (Toutes paires).
2. QUALITY RANK (Qualité)  : Classement par la robustesse et propreté du mouvement (Toutes paires).
3. OPPORTUNITY RANK (Opportunité) : Classement attribué UNIQUEMENT aux paires ÉLIGIBLES.
"""

from typing import List
from relative_strength.models import RelativeStrengthState


def apply_triple_ranking(states: List[RelativeStrengthState]) -> List[RelativeStrengthState]:
    """
    Assigne pip_rank, quality_rank à toutes les paires, et opportunity_rank UNIQUEMENT aux paires éligibles.
    """
    if not states:
        return states
    
    # 1. PIP RANK (Tri par directional_pips décroissant sur toutes les paires)
    states.sort(key=lambda s: s.directional_pips, reverse=True)
    for rank, state in enumerate(states, start=1):
        state.pip_rank = rank
        
    # 2. QUALITY RANK (Tri par quality_score décroissant sur toutes les paires)
    states.sort(key=lambda s: s.quality_score, reverse=True)
    for rank, state in enumerate(states, start=1):
        state.quality_rank = rank
        
    # 3. OPPORTUNITY RANK (Tri par opportunity_score décroissant UNIQUEMENT pour les paires éligibles)
    eligible_states = [s for s in states if s.is_eligible]
    non_eligible_states = [s for s in states if not s.is_eligible]
    
    eligible_states.sort(key=lambda s: s.opportunity_score, reverse=True)
    for rank, state in enumerate(eligible_states, start=1):
        state.opportunity_rank = rank
        
    for state in non_eligible_states:
        state.opportunity_rank = 0  # 0 indique "NON ÉLIGIBLE"
        
    return states
