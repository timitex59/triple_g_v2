# -*- coding: utf-8 -*-
"""
RSB Engine Configuration Module
===============================
Paramètres configurables du moteur 07H Relative Strength Breakout.
Tous les poids sont marqués EXPERIMENTAL_DEFAULTS et destinables à optimisation via backtest.
"""

from zoneinfo import ZoneInfo

# --- HEURE & TIMEZONE ---
REFERENCE_HOUR = "07:00"
TIMEZONE_STR = "Europe/Paris"
PARIS_TZ = ZoneInfo(TIMEZONE_STR)

# --- THRESHOLDS & CONFIRMATION ---
THRESHOLD_PIPS = 5.0
MIN_CONFIRMATION_RUNS = 2

# --- ZONES D'ENTRÉE (PIPS) ---
NOISE_MAX = 5.0
SWEET_SPOT_MAX = 15.0
EXTENSION_MAX = 25.0
# > EXTENSION_MAX (25.0 pips) = CHASING

# --- SORTIES (DISABLED IN V1 BY DEFAULT) ---
ENABLE_RSB_EXITS = False  # Amendement #8: Désactivé en V1
TRAILING_ENABLED = True
TRAILING_DISTANCE = 10.0

# --- BEST EXPRESSION ---
ALLOW_CHASING_BEST_EXPRESSION = False  # Amendement #10: Exclut la zone CHASING pour Best Expression

# --- PONDÉRATIONS EXPÉRIMENTALES (QUALITY SCORE) ---
# Amendement #7: Poids expérimentaux non statistiquement validés. Optimisables via backtest.
EXPERIMENTAL_DEFAULTS = {
    "strength_differential": 0.20,
    "currency_breadth": 0.15,
    "persistence": 0.15,
    "pip_expansion": 0.10,
    "velocity": 0.10,
    "technical_alignment": 0.10,
    "acceleration": 0.05,
    "convergence": 0.05,
    "efficiency": 0.05,
    "entry_quality": 0.05,
}

# --- PÉNALITÉS DU QUALITY SCORE ---
PENALTIES = {
    "warning": 15.0,
    "chasing": 20.0,
    "contradiction": 30.0,
    "high_mae_threshold": 10.0,
    "high_mae_penalty": 15.0,
    "drawdown_pct_mult": 0.3,
}

# --- PONDÉRATIONS ET PÉNALITÉS DU OPPORTUNITY SCORE ---
# Amendement #9: Priorise les opportunités d'entrée immédiates au run actuel
OPPORTUNITY_WEIGHTS = {
    "entry_zone_bonus_sweet_spot": 25.0,
    "recent_breakout_bonus": 15.0,
    "persistence_bonus": 15.0,
    "positive_velocity_bonus": 10.0,
    "low_mae_bonus": 10.0,
    "breadth_bonus": 15.0,
    "strength_diff_bonus": 10.0,
}

OPPORTUNITY_PENALTIES = {
    "chasing_penalty": 35.0,
    "drawdown_from_mfe_mult": 0.5,
    "negative_velocity_penalty": 15.0,
    "negative_acceleration_penalty": 10.0,
    "warning_penalty": 20.0,
    "alignment_loss_penalty": 25.0,
    "late_signal_penalty": 15.0,
}

# --- PAIRS & CURRENCIES DEFINITIONS ---
ALL_28_PAIRS = [
    "EURUSD", "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
    "GBPCHF", "GBPAUD", "GBPCAD", "GBPNZD", "GBPUSD", "GBPJPY",
    "NZDCHF", "NZDCAD", "NZDUSD", "NZDJPY",
    "USDCHF", "USDJPY", "USDCAD",
    "AUDCHF", "AUDCAD", "AUDUSD", "AUDJPY", "AUDNZD",
    "CHFJPY", "CADJPY", "CADCHF"
]

MAJOR_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"]
