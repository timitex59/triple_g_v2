# -*- coding: utf-8 -*-
"""
RSB Multi-run Persistence Engine
================================
Suivi des compteurs de persistance multi-runs (Section 5).
Permet de valoriser les mouvements établis et durables par rapport aux spikes éphémères.
"""

from typing import Dict, Any, Tuple
from relative_strength.config import THRESHOLD_PIPS


def update_persistence_counters(
    directional_pips: float,
    trade_direction: str,
    has_warning: bool,
    prev_persistence: Dict[str, int],
    threshold_pips: float = THRESHOLD_PIPS
) -> Tuple[int, int, int, int]:
    """
    Met à jour les 4 compteurs de persistance multi-runs.
    
    Returns:
    (confirmation_runs, above_threshold_runs, signal_direction_runs, warning_runs)
    """
    prev_conf = prev_persistence.get("confirmation_runs", 0)
    prev_above = prev_persistence.get("above_threshold_runs", 0)
    prev_dir = prev_persistence.get("signal_direction_runs", 0)
    prev_warn = prev_persistence.get("warning_runs", 0)
    prev_direction = prev_persistence.get("last_trade_direction", "NEUTRAL")
    
    # 1. above_threshold_runs : nombre de runs consécutifs où directional_pips > threshold
    if directional_pips > threshold_pips:
        above_threshold_runs = prev_above + 1
    else:
        above_threshold_runs = 0
        
    # 2. confirmation_runs : runs consécutifs au-dessus du threshold dans le sens du trade sans invalidation
    if directional_pips > threshold_pips and trade_direction != "NEUTRAL":
        confirmation_runs = prev_conf + 1
    elif directional_pips <= 0:
        confirmation_runs = 0
    else:
        confirmation_runs = prev_conf
        
    # 3. signal_direction_runs : runs consécutifs avec la même direction de trade
    if trade_direction != "NEUTRAL" and trade_direction == prev_direction:
        signal_direction_runs = prev_dir + 1
    elif trade_direction != "NEUTRAL":
        signal_direction_runs = 1
    else:
        signal_direction_runs = 0
        
    # 4. warning_runs : runs consécutifs avec warning actif
    if has_warning:
        warning_runs = prev_warn + 1
    else:
        warning_runs = 0
        
    return (confirmation_runs, above_threshold_runs, signal_direction_runs, warning_runs)
