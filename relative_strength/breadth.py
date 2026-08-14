# -*- coding: utf-8 -*-
"""
RSB Currency Strength, Breadth & Best Expression Engine
=======================================================
Conforme aux Amendements #5, #6 et #10:
- #5: Réutilisation directe du moteur de force devise du projet (renko_score_29pairs_v16.py: currency_strength).
- #6: Calcul rigoureux du Breadth sans double comptage (confirming_crosses, contradicting_crosses, available_crosses, breadth_ratio).
- #10: Best Expression tradable excluant strictement la zone CHASING.
"""

from typing import Dict, List, Tuple, Optional, Any
from relative_strength.config import MAJOR_CURRENCIES, ALL_28_PAIRS, ALLOW_CHASING_BEST_EXPRESSION


def normalize_existing_currency_strengths(
    raw_strengths: Any
) -> Dict[str, float]:
    """
    Provenance (#5): Convertit la sortie de currency_strength() de renko_score_29pairs_v16.py
    (ou un dictionnaire de force brute % / pips) en un score 0-100 standardisé.
    """
    if isinstance(raw_strengths, list):
        # Format tuple list [("NZD", +0.81), ("GBP", +0.51), ...]
        avg_map = {ccy: val for ccy, val in raw_strengths if ccy in MAJOR_CURRENCIES}
    elif isinstance(raw_strengths, dict):
        avg_map = {ccy: val for ccy, val in raw_strengths.items() if ccy in MAJOR_CURRENCIES}
    else:
        avg_map = {}
        
    for c in MAJOR_CURRENCIES:
        if c not in avg_map:
            avg_map[c] = 0.0

    min_val = min(avg_map.values())
    max_val = max(avg_map.values())
    val_range = max_val - min_val

    if val_range <= 0.0001:
        return {c: 50.0 for c in MAJOR_CURRENCIES}

    return {
        c: round(((avg_map[c] - min_val) / val_range) * 100.0, 2)
        for c in MAJOR_CURRENCIES
    }


def compute_currency_strengths_from_pips(
    pairs_raw_pips: Dict[str, float]
) -> Dict[str, float]:
    """
    Provenance (#5): Calcule la force par devise exactement selon la même logique que
    currency_strength() de renko_score_29pairs_v16.py (ligne 3422) :
    Pour chaque paire majeure (base, quote), on accumule +pips pour la base et -pips pour la quote,
    on fait la moyenne par devise, puis on normalise sur 0..100.
    """
    currency_totals: Dict[str, float] = {c: 0.0 for c in MAJOR_CURRENCIES}
    currency_counts: Dict[str, int] = {c: 0 for c in MAJOR_CURRENCIES}

    for pair, pips in pairs_raw_pips.items():
        clean_pair = pair.upper().replace("=X", "")
        if len(clean_pair) == 6:
            base = clean_pair[:3]
            quote = clean_pair[3:]

            if base in currency_totals:
                currency_totals[base] += pips
                currency_counts[base] += 1
            if quote in currency_totals:
                currency_totals[quote] -= pips
                currency_counts[quote] += 1

    averages = {
        c: (currency_totals[c] / currency_counts[c]) if currency_counts[c] > 0 else 0.0
        for c in MAJOR_CURRENCIES
    }

    return normalize_existing_currency_strengths(averages)


def compute_currency_breadth(
    target_currency: str,
    desired_direction: str,  # "BULL" (fort) ou "BEAR" (faible)
    pairs_raw_pips: Dict[str, float],
    threshold_pips: float = 5.0
) -> Tuple[int, int, int, float]:
    """
    Calcul formel du Breadth (#6) sans double comptage.
    
    Returns:
    (confirming_crosses, contradicting_crosses, available_crosses, breadth_ratio)
    """
    confirming = 0
    contradicting = 0
    available = 0

    curr = target_currency.upper()
    is_bull = (desired_direction.upper() in ["BULL", "LONG", "STRONG"])

    for pair, pips in pairs_raw_pips.items():
        clean_pair = pair.upper().replace("=X", "")
        if len(clean_pair) != 6:
            continue

        base = clean_pair[:3]
        quote = clean_pair[3:]

        if base != curr and quote != curr:
            continue

        available += 1

        if base == curr:
            if is_bull:
                if pips > 0: confirming += 1
                elif pips < 0: contradicting += 1
            else:
                if pips < 0: confirming += 1
                elif pips > 0: contradicting += 1
        elif quote == curr:
            if is_bull:
                if pips < 0: confirming += 1
                elif pips > 0: contradicting += 1
            else:
                if pips > 0: confirming += 1
                elif pips < 0: contradicting += 1

    ratio = (confirming / available) if available > 0 else 0.0
    return (confirming, contradicting, available, round(ratio, 4))


def select_best_currency_expression(
    states_list: List[Any],
    target_currency: str,
    trade_direction: str = "LONG",
    allow_chasing: bool = ALLOW_CHASING_BEST_EXPRESSION
) -> Optional[Any]:
    """
    Sélectionne la meilleure expression tradable (#10) pour une devise forte/faible.
    Exclut strictement les paires en zone CHASING sauf override config.
    """
    curr = target_currency.upper()
    dir_upper = trade_direction.upper()

    candidates = []
    for state in states_list:
        p = state.pair.upper()
        base = state.base_currency.upper()
        quote = state.quote_currency.upper()

        is_relevant = False
        if dir_upper == "LONG" and ((base == curr and state.trade_direction == "LONG") or (quote == curr and state.trade_direction == "SHORT")):
            is_relevant = True
        elif dir_upper == "SHORT" and ((base == curr and state.trade_direction == "SHORT") or (quote == curr and state.trade_direction == "LONG")):
            is_relevant = True

        if is_relevant:
            if not allow_chasing and state.entry_zone == "CHASING":
                continue
            candidates.append(state)

    if not candidates:
        return None

    candidates.sort(key=lambda s: (s.opportunity_score, s.quality_score), reverse=True)
    return candidates[0]
