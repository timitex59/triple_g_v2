#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface commune des adaptateurs.

Chaque famille de signaux (fondamental, technique, sentiment, correlation)
produit un FamilyResult : un score 0-100 par devise (50 = neutre) plus des
details libres pour l'explication. Un adaptateur indisponible (cle manquante,
reseau KO) renvoie available=False et son poids est redistribue en aval.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FamilyResult:
    name: str                                   # "fundamental" | "technical" | ...
    available: bool
    scores: dict[str, float] = field(default_factory=dict)   # devise -> 0..100
    details: dict = field(default_factory=dict)              # libre, pour l'explication
    note: str = ""                              # message court (ex. cause d'indispo)

    def score(self, currency: str) -> float | None:
        return self.scores.get(currency)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def to_0_100(directional: float) -> float:
    """Convertit un score directionnel dans [-1, 1] en force 0..100 (50 neutre)."""
    return 50.0 + 50.0 * clamp(directional, -1.0, 1.0)


def rank_to_0_100(value: float, values: list[float]) -> float:
    """Position percentile de value dans values -> 0..100 (robuste aux echelles)."""
    if not values:
        return 50.0
    below = sum(1 for v in values if v < value)
    equal = sum(1 for v in values if v == value)
    pct = (below + 0.5 * equal) / len(values)
    return 100.0 * pct
