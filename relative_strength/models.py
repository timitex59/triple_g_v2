# -*- coding: utf-8 -*-
"""
RSB Data Models
===============
Dataclass et structures de données pour le moteur 07H Relative Strength Breakout.
Conforme aux amendements #2, #3, #4, #6, #7, #9, #10, #12 et au Trade Eligibility Gate.
"""

from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, Any


@dataclass
class RelativeStrengthState:
    pair: str
    timestamp: str
    reference_time: str
    reference_price: float
    reference_source: str
    current_price: float
    trade_direction: str = "NEUTRAL"  # "LONG", "SHORT", "NEUTRAL"
    
    # Mouvement marché brut et séries directionnelles séparées (#2)
    raw_market_pips: float = 0.0
    long_pips: float = 0.0
    short_pips: float = 0.0
    directional_pips: float = 0.0
    
    # Machine d'états V1 simplifiée (états exclusifs) (#3)
    # Exclusifs: NEUTRAL, BREAKOUT_PENDING, BREAKOUT_CONFIRMED, EXTENDED, INVALIDATED
    threshold_state: str = "NEUTRAL"
    
    # TRADE ELIGIBILITY GATE (Filtre dur de décision)
    # NOT_ELIGIBLE, WATCHLIST, ELIGIBLE, ELIGIBLE_WITH_PENALTY
    trade_eligibility: str = "NOT_ELIGIBLE"
    is_eligible: bool = False
    
    # Flags indépendants (#3)
    is_expanding: bool = False
    is_trending: bool = False
    is_exhausting: bool = False
    reversal_warning: bool = False
    has_warning: bool = False
    is_chasing: bool = False
    
    # Timing & Persistance
    threshold_break_time: Optional[str] = None
    above_threshold_runs: int = 0
    confirmation_runs: int = 0
    signal_direction_runs: int = 0
    warning_runs: int = 0
    
    # Excursion & Efficacité
    mfe_pips: float = 0.0
    mae_pips: float = 0.0
    drawdown_from_mfe: float = 0.0
    drawdown_pct: float = 0.0
    efficiency: float = 1.0  # 0.0 à 1.0
    
    # Cinétique en valeurs brutes (#4)
    velocity_pips_per_hour: float = 0.0
    acceleration_pips_per_hour2: float = 0.0
    
    # Zone d'entrée
    entry_zone: str = "NOISE"  # NOISE, SWEET_SPOT, EXTENSION, CHASING
    
    # Devises & Forces
    base_currency: str = ""
    quote_currency: str = ""
    base_strength: float = 50.0
    quote_strength: float = 50.0
    strength_differential: float = 0.0
    
    # Currency Breadth (#6)
    confirming_crosses: int = 0
    contradicting_crosses: int = 0
    available_crosses: int = 0
    breadth_ratio: float = 0.0
    
    # Alignements & Signaux annexes
    technical_alignment: str = "⚪"
    has_fire_signal: bool = False
    
    # Scores et triple ranking (#7, #9, #12)
    quality_score: float = 0.0       # QUALITY (0-100)
    opportunity_score: float = 0.0   # OPPORTUNITY (0-100)
    
    pip_rank: int = 0          # PERFORMANCE RANK (toutes paires)
    quality_rank: int = 0      # QUALITY RANK (toutes paires)
    opportunity_rank: int = 0  # OPPORTUNITY RANK (attribué UNIQUEMENT aux paires éligibles, 0 si non éligible)
    
    # Best expression tradable (#10)
    is_best_expression: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RelativeStrengthState':
        return cls(**data)
