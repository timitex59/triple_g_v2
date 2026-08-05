#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Composite par devise (Currency Power Ranking).

Fusionne les FamilyResult disponibles en un score 0-100 par devise, avec les
poids CONFLUENCE_WEIGHTS renormalises sur les seules familles qui ont un score
pour la devise consideree. Sert au classement des devises et de base au moteur
de confluence.
"""

from __future__ import annotations

from .. import config as cfg
from ..adapters.base import FamilyResult


def currency_composites(families: list[FamilyResult]) -> dict[str, dict]:
    """Retourne {devise: {composite, parts:{famille:score}}}."""
    out: dict[str, dict] = {}
    for cur in cfg.CURRENCIES:
        parts: dict[str, float] = {}
        wsum = 0.0
        acc = 0.0
        for fam in families:
            if not fam.available:
                continue
            s = fam.score(cur)
            if s is None:
                continue
            w = cfg.CONFLUENCE_WEIGHTS.get(fam.name, 0.0)
            if w <= 0:
                continue
            parts[fam.name] = s
            acc += w * s
            wsum += w
        if wsum > 0:
            out[cur] = {"composite": round(acc / wsum, 1), "parts": parts}
    return out


def ranking(composites: dict[str, dict]) -> list[tuple[str, float]]:
    rows = [(cur, d["composite"]) for cur, d in composites.items()]
    rows.sort(key=lambda x: x[1], reverse=True)
    return rows
