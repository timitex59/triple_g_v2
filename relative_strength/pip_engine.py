# -*- coding: utf-8 -*-
"""
RSB Pip Calculation Engine
==========================
Calculs des mouvements en pips, MFE, MAE, Drawdown et Efficacité.
Conforme à l'Amendement #2:
- Séparation stricte de raw_market_pips, long_pips, short_pips, directional_pips.
- MFE/MAE/drawdown reproductibles indépendamment du sens de trade.
"""

from typing import Tuple, Dict, Any


def get_pip_size(pair: str) -> float:
    """Retourne 0.01 pour les paires JPY et 0.0001 pour les autres."""
    clean_pair = pair.upper().replace("=X", "")
    if "JPY" in clean_pair:
        return 0.01
    return 0.0001


def calculate_pips(
    current_price: float,
    reference_price: float,
    pip_size: float
) -> Tuple[float, float, float]:
    """
    Calcule la variation brute et les pips LONG / SHORT.
    Returns: (raw_market_pips, long_pips, short_pips)
    """
    if reference_price <= 0 or pip_size <= 0:
        return (0.0, 0.0, 0.0)
    
    raw_market_pips = (current_price - reference_price) / pip_size
    long_pips = raw_market_pips
    short_pips = -raw_market_pips
    return (raw_market_pips, long_pips, short_pips)


def determine_directional_pips(
    raw_market_pips: float,
    trade_direction: str
) -> float:
    """
    Assigne directional_pips selon la direction de trade retenue ("LONG", "SHORT", "NEUTRAL").
    """
    direction_upper = trade_direction.upper()
    if direction_upper == "LONG":
        return raw_market_pips
    elif direction_upper == "SHORT":
        return -raw_market_pips
    else:
        # En NEUTRAL, directional_pips représente la variation absolue du mouvement
        return abs(raw_market_pips)


def update_excursion_metrics(
    directional_pips: float,
    prev_mfe: float,
    prev_mae: float,
    cumulative_path_pips: float = 0.0
) -> Tuple[float, float, float, float, float]:
    """
    Met à jour MFE, MAE, Drawdown depuis MFE, % Drawdown et Ratio d'Efficacité.
    
    Returns:
    (mfe_pips, mae_pips, drawdown_from_mfe, drawdown_pct, efficiency)
    """
    # MFE (Maximum Favorable Excursion) : plus haut pic positif atteint
    mfe_pips = max(prev_mfe, directional_pips, 0.0)
    
    # MAE (Maximum Adverse Excursion) : pire creux négatif atteint (négatif ou 0)
    mae_pips = min(prev_mae, directional_pips, 0.0)
    
    # Drawdown depuis MFE
    drawdown_from_mfe = max(0.0, mfe_pips - directional_pips)
    
    # % Drawdown depuis MFE
    if mfe_pips > 0:
        drawdown_pct = (drawdown_from_mfe / mfe_pips) * 100.0
    else:
        drawdown_pct = 0.0
    
    # Efficacité (0.0 à 1.0)
    if cumulative_path_pips > 0:
        efficiency = max(0.0, min(1.0, directional_pips / cumulative_path_pips))
    else:
        efficiency = 1.0 if directional_pips >= 0 else 0.0
        
    return (mfe_pips, mae_pips, drawdown_from_mfe, drawdown_pct, efficiency)
