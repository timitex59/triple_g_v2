# -*- coding: utf-8 -*-
"""
RSB Scoring Engine (Quality Score & Opportunity Score)
======================================================
Calcul déterministe des deux scores principaux du moteur RSB.
Conforme aux Amendements #7, #9 et #12:
- #7: Poids EXPERIMENTAL_DEFAULTS (non présentés comme probabilités).
- #9: OPPORTUNITY SCORE focalisé sur la qualité d'entrée au run actuel sans saturation.
- #12: Séparation stricte de PERFORMANCE, QUALITY et OPPORTUNITY.
"""

from relative_strength.config import (
    EXPERIMENTAL_DEFAULTS,
    PENALTIES,
    THRESHOLD_PIPS,
    SWEET_SPOT_MAX,
    EXTENSION_MAX,
)
from relative_strength.models import RelativeStrengthState


def compute_quality_score(state: RelativeStrengthState) -> float:
    """
    Calcule le QUALITY SCORE (0-100) : mesure la propreté et la robustesse historique du mouvement.
    Amendement #7: Poids expérimentaux.
    """
    w = EXPERIMENTAL_DEFAULTS
    
    # 1. Component Strength Differential (0..100)
    diff_val = max(0.0, min(100.0, (state.strength_differential + 100.0) / 2.0))
    
    # 2. Currency Breadth (0..100)
    breadth_val = state.breadth_ratio * 100.0
    
    # 3. Persistence (0..100)
    persistence_val = min(100.0, state.confirmation_runs * 20.0)
    
    # 4. Pip Expansion (0..100)
    expansion_val = min(100.0, (state.directional_pips / 20.0) * 100.0) if state.directional_pips > 0 else 0.0
    
    # 5. Velocity (0..100) (proxy positif de vitesse brute)
    velocity_val = min(100.0, max(0.0, state.velocity_pips_per_hour * 10.0))
    
    # 6. Technical Alignment (0..100)
    align_val = 100.0 if state.technical_alignment in ["↑↑", "↓↓"] else (50.0 if state.technical_alignment == "⚪" else 0.0)
    
    # 7. Acceleration (0..100)
    accel_val = min(100.0, max(0.0, (state.acceleration_pips_per_hour2 + 5.0) * 10.0))
    
    # 8. Convergence (0..100)
    convergence_val = 100.0 if state.above_threshold_runs >= 2 else 50.0
    
    # 9. Efficiency (0..100)
    efficiency_val = state.efficiency * 100.0
    
    # 10. Entry Quality (0..100)
    if state.entry_zone == "SWEET_SPOT":
        entry_val = 100.0
    elif state.entry_zone == "NOISE":
        entry_val = 60.0
    elif state.entry_zone == "EXTENSION":
        entry_val = 40.0
    else:  # CHASING
        entry_val = 10.0
        
    raw_score = (
        w["strength_differential"] * diff_val +
        w["currency_breadth"] * breadth_val +
        w["persistence"] * persistence_val +
        w["pip_expansion"] * expansion_val +
        w["velocity"] * velocity_val +
        w["technical_alignment"] * align_val +
        w["acceleration"] * accel_val +
        w["convergence"] * convergence_val +
        w["efficiency"] * efficiency_val +
        w["entry_quality"] * entry_val
    )
    
    # Application des pénalités
    total_penalty = 0.0
    p = PENALTIES
    
    if state.has_warning:
        total_penalty += p["warning"]
    if state.entry_zone == "CHASING":
        total_penalty += p["chasing"]
    if abs(state.mae_pips) > p["high_mae_threshold"]:
        total_penalty += p["high_mae_penalty"]
    if state.drawdown_pct > 0:
        total_penalty += state.drawdown_pct * p["drawdown_pct_mult"]
        
    # Bonus Flamme 🔥 (max +5)
    if state.has_fire_signal:
        raw_score += 5.0
        
    final_score = max(0.0, min(100.0, raw_score - total_penalty))
    return round(final_score, 1)


def compute_opportunity_score(state: RelativeStrengthState) -> float:
    """
    Calcule l'OPPORTUNITY SCORE (0-100) sans saturation artificielle.
    Modèle de mise à échelle normalisé (Base = 0.0) :
    - Bonus Zone d'entrée : SWEET_SPOT (+25), NOISE (+10), EXTENSION (+5), CHASING (-35)
    - Breakout récent confirmé (2..4 runs) : +15
    - Persistance établie (>4 runs) : +10
    - Vitesse positive : +10 (si négative : -15)
    - Accélération positive : +5 (si négative : -10)
    - Faible MAE (abs(mae) <= 3) : +10
    - Breadth ratio (max +15)
    - Strength differential (max +10)
    - Pénalité drawdown : -0.5 * drawdown_pips
    - Pénalité warning : -20
    - Pénalité perte d'alignement : -25
    """
    score = 0.0
    
    # 1. Zone d'entrée
    if state.entry_zone == "SWEET_SPOT":
        score += 25.0
    elif state.entry_zone == "NOISE":
        score += 10.0
    elif state.entry_zone == "EXTENSION":
        score += 5.0
    elif state.entry_zone == "CHASING":
        score -= 35.0
        
    # 2. Persistance & Breakout récent
    if 2 <= state.confirmation_runs <= 4:
        score += 15.0
    elif state.confirmation_runs > 4:
        score += 10.0
        
    # 3. Cinétique
    if state.velocity_pips_per_hour > 0:
        score += 10.0
    elif state.velocity_pips_per_hour < 0:
        score -= 15.0
        
    if state.acceleration_pips_per_hour2 > 0:
        score += 5.0
    elif state.acceleration_pips_per_hour2 < 0:
        score -= 10.0
        
    # 4. MAE
    if abs(state.mae_pips) <= 3.0:
        score += 10.0
    elif abs(state.mae_pips) > 10.0:
        score -= 15.0
        
    # 5. Breadth et Force Differential
    score += state.breadth_ratio * 15.0
    
    diff_norm = max(0.0, min(1.0, abs(state.strength_differential) / 50.0))
    score += diff_norm * 10.0
    
    # 6. Pénalités de retournement et warning
    if state.drawdown_from_mfe > 0:
        score -= state.drawdown_from_mfe * 0.5
    if state.has_warning:
        score -= 20.0
    if state.technical_alignment in ["↑↓", "↓↑"]:
        score -= 25.0
        
    final_score = max(0.0, min(100.0, score))
    return round(final_score, 1)
