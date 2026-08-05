#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Adaptateur CORRELATIONS / FLUX INTER-MARCHES (donnees TradingView).

Plutot que d'empiler des coefficients de correlation bruts, on mesure le
"vent macro" qui pousse chaque devise, a partir du momentum d'instruments
independants (DXY, or, rendements US, actions, VIX, petrole) et d'une matrice
de relations inter-marches bien etablies :

  - DXY / rendements US     -> USD
  - rendements US, VIX, SPX -> JPY, CHF (valeurs refuges)
  - petrole                 -> CAD
  - actions + or            -> AUD, NZD (risque / matieres premieres)
  - -DXY                    -> EUR, GBP

Chaque momentum est ecrase par une tanh, combine, puis converti en score 0-100.
"""

from __future__ import annotations

import math

from .. import config as cfg
from .. import tv_feed
from .base import FamilyResult, clamp, to_0_100

# Echelle du momentum (retour sur la fenetre) par instrument, pour la tanh.
_SCALE = {
    "DXY": 0.05, "GOLD": 0.08, "US10Y": 0.15, "US02Y": 0.15,
    "SPX": 0.06, "VIX": 0.35, "OIL": 0.12, "BTC": 0.25,
}

# Contribution (devise -> {instrument: poids}). Un poids negatif = relation inverse.
_MATRIX: dict[str, dict[str, float]] = {
    "USD": {"DXY": 1.0, "US10Y": 0.4},
    "EUR": {"DXY": -0.7},
    "GBP": {"DXY": -0.5, "SPX": 0.2},
    "JPY": {"US10Y": -0.7, "VIX": 0.5, "SPX": -0.3},
    "CHF": {"VIX": 0.6, "SPX": -0.3, "DXY": -0.2},
    "CAD": {"OIL": 0.8, "DXY": -0.2},
    "AUD": {"SPX": 0.6, "GOLD": 0.4, "DXY": -0.2},
    "NZD": {"SPX": 0.6, "GOLD": 0.4, "DXY": -0.2},
}


def _momentum(symbol: str, lookback: int) -> float | None:
    df = tv_feed.fetch(symbol, "D", max(lookback + 20, 120))
    if df is None or len(df) < lookback + 2:
        return None
    close = df["Close"]
    past = float(close.iloc[-1 - lookback])
    now = float(close.iloc[-1])
    if past == 0:
        return None
    return now / past - 1.0


def run(verbose: bool = False) -> FamilyResult:
    lookback = cfg.CORR_LOOKBACK
    mom_raw: dict[str, float] = {}
    mom_tanh: dict[str, float] = {}
    for name, symbol in cfg.CORRELATION_INSTRUMENTS.items():
        m = _momentum(symbol, lookback)
        if m is None:
            continue
        mom_raw[name] = round(m * 100.0, 2)  # en %
        mom_tanh[name] = math.tanh(m / _SCALE.get(name, 0.1))
        if verbose:
            print(f"  [corr] {name}: {m * 100:+.1f}% -> {mom_tanh[name]:+.2f}")

    if not mom_tanh:
        return FamilyResult("correlation", False, {}, {}, note="instruments macro indisponibles")

    scores: dict[str, float] = {}
    contrib: dict[str, dict] = {}
    for cur, weights in _MATRIX.items():
        total = 0.0
        used = {}
        for inst, w in weights.items():
            if inst in mom_tanh:
                total += w * mom_tanh[inst]
                used[inst] = round(w * mom_tanh[inst], 3)
        if used:
            scores[cur] = round(to_0_100(clamp(total, -1.0, 1.0)), 1)
            contrib[cur] = used

    return FamilyResult(
        name="correlation",
        available=len(scores) > 0,
        scores=scores,
        details={"momentum_pct": mom_raw, "contrib": contrib},
        note=f"flux macro sur {lookback} bougies D",
    )
