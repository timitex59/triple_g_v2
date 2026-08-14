# -*- coding: utf-8 -*-
"""
RSB Threshold & State Machine Engine (V1 Simplified)
===================================================
Machine à états V1 simplifiée, flags indépendants et Trade Eligibility Gate.
Conforme aux amendements #3 et à la porte d'éligibilité de trade (Trade Eligibility Gate).
"""

from typing import Tuple, Dict, Any
from relative_strength.config import (
    THRESHOLD_PIPS,
    MIN_CONFIRMATION_RUNS,
    NOISE_MAX,
    SWEET_SPOT_MAX,
    EXTENSION_MAX,
)


def evaluate_threshold_state(
    directional_pips: float,
    confirmation_runs: int,
    threshold_pips: float = THRESHOLD_PIPS,
    min_confirmation_runs: int = MIN_CONFIRMATION_RUNS,
    extended_pips: float = EXTENSION_MAX
) -> str:
    """
    Détermine l'état exclusif V1 de la paire.
    États exclusifs: NEUTRAL, BREAKOUT_PENDING, BREAKOUT_CONFIRMED, EXTENDED, INVALIDATED.
    """
    if directional_pips < -threshold_pips:
        return "INVALIDATED"
    
    if abs(directional_pips) <= threshold_pips:
        return "NEUTRAL"
    
    if directional_pips > threshold_pips:
        if directional_pips >= extended_pips and confirmation_runs >= min_confirmation_runs:
            return "EXTENDED"
        elif confirmation_runs >= min_confirmation_runs:
            return "BREAKOUT_CONFIRMED"
        else:
            return "BREAKOUT_PENDING"
            
    return "NEUTRAL"


def evaluate_entry_zone(
    directional_pips: float,
    threshold_pips: float = THRESHOLD_PIPS,
    sweet_spot_max: float = SWEET_SPOT_MAX,
    extension_max: float = EXTENSION_MAX
) -> str:
    """
    Détermine la zone d'entrée de la paire selon son extension depuis 07h.
    NOISE, SWEET_SPOT, EXTENSION, CHASING.
    """
    if directional_pips <= threshold_pips:
        return "NOISE"
    elif directional_pips <= sweet_spot_max:
        return "SWEET_SPOT"
    elif directional_pips <= extension_max:
        return "EXTENSION"
    else:
        return "CHASING"


def evaluate_trade_eligibility(
    threshold_state: str,
    entry_zone: str
) -> Tuple[str, bool]:
    """
    TRADE ELIGIBILITY GATE (Filtre dur de décision) :
    Séépare strictement l'éligibilité d'un trade de son score d'attractivité.
    
    Règles :
    - NEUTRAL / INVALIDATED -> NOT_ELIGIBLE (is_eligible = False)
    - BREAKOUT_PENDING -> WATCHLIST (is_eligible = False)
    - BREAKOUT_CONFIRMED / EXTENDED :
      - SWEET_SPOT -> ELIGIBLE (is_eligible = True)
      - EXTENSION  -> ELIGIBLE_WITH_PENALTY (is_eligible = True)
      - CHASING    -> NOT_ELIGIBLE (is_eligible = False)
      
    Returns:
    (trade_eligibility_status, is_eligible)
    """
    if threshold_state in ["NEUTRAL", "INVALIDATED"]:
        return ("NOT_ELIGIBLE", False)
        
    if threshold_state == "BREAKOUT_PENDING":
        return ("WATCHLIST", False)
        
    if threshold_state in ["BREAKOUT_CONFIRMED", "EXTENDED"]:
        if entry_zone == "SWEET_SPOT":
            return ("ELIGIBLE", True)
        elif entry_zone == "EXTENSION":
            return ("ELIGIBLE_WITH_PENALTY", True)
        elif entry_zone == "CHASING":
            return ("NOT_ELIGIBLE", False)
            
    return ("NOT_ELIGIBLE", False)


def evaluate_independent_flags(
    directional_pips: float,
    previous_directional_pips: float,
    confirmation_runs: int,
    mfe_pips: float,
    drawdown_pct: float,
    velocity_pips_per_hour: float,
    acceleration_pips_per_hour2: float,
    has_technical_warning: bool = False
) -> Tuple[bool, bool, bool, bool, bool, bool]:
    """
    Évalue les 6 flags indépendants.
    
    Returns:
    (is_expanding, is_trending, is_exhausting, reversal_warning, has_warning, is_chasing)
    """
    is_chasing = (directional_pips > EXTENSION_MAX)
    is_expanding = (directional_pips > previous_directional_pips and directional_pips > THRESHOLD_PIPS)
    
    reversal_warning = (
        (mfe_pips >= 15.0 and (mfe_pips - directional_pips) >= 10.0) or
        (drawdown_pct >= 35.0 and mfe_pips >= 10.0)
    )
    
    has_warning = (has_technical_warning or reversal_warning)
    
    is_trending = (
        confirmation_runs >= MIN_CONFIRMATION_RUNS and
        directional_pips > THRESHOLD_PIPS and
        not has_warning
    )
    
    is_exhausting = (
        directional_pips > THRESHOLD_PIPS and (
            (velocity_pips_per_hour < 0 and acceleration_pips_per_hour2 < 0) or
            drawdown_pct >= 25.0 or
            has_warning
        )
    )
    
    return (is_expanding, is_trending, is_exhausting, reversal_warning, has_warning, is_chasing)
