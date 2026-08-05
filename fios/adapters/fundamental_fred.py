#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Adaptateur FONDAMENTAL — macro via FRED (St. Louis Fed).

Necessite une cle API FRED gratuite dans la variable d'environnement
FRED_API_KEY (https://fred.stlouisfed.org/docs/api/api_key.html). Sans cle,
l'adaptateur renvoie available=False et son poids est redistribue.

Pour chaque devise (voir cfg.FRED_SERIES), on recupere quelques series cle
(taux directeur, rendements, inflation, chomage) et on evalue leur DIRECTION
recente (momentum sur ~6 mois), orientee selon le sens attendu ("up" = une
hausse renforce la devise). Le score par devise agrege ces directions -> 0-100.

Phase 1 volontairement sobre : c'est la tendance directionnelle des drivers
macro, pas un modele econometrique. On enrichit en Phase 2 (surprises vs
consensus via un calendrier, ponderation par importance).
"""

from __future__ import annotations

import math
import os

import requests

from .. import config as cfg
from .base import FamilyResult, clamp, to_0_100

_FRED_URL = "https://api.stlouisfed.org/fred/series/observations"


def _series_momentum(series_id: str, api_key: str) -> float | None:
    """Retourne un momentum normalise dans [-1, 1] sur ~6 dernieres observations,
    ou None si indisponible."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": "14",
    }
    try:
        r = requests.get(_FRED_URL, params=params, timeout=20)
        r.raise_for_status()
        obs = r.json().get("observations", [])
    except Exception:
        return None

    vals: list[float] = []
    for o in obs:
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            vals.append(float(v))
        except ValueError:
            continue
    if len(vals) < 4:
        return None

    # vals[0] = plus recent (tri desc). Compare recent vs ~6 periodes avant.
    recent = vals[0]
    back_idx = min(6, len(vals) - 1)
    past = vals[back_idx]
    if past == 0:
        delta = recent - past
        scale = 1.0
    else:
        delta = (recent - past) / abs(past)
        scale = 0.05  # 5% de variation relative -> signal fort
    return math.tanh(delta / scale)


def run(verbose: bool = False) -> FamilyResult:
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        return FamilyResult(
            "fundamental", False, {}, {},
            note="FRED_API_KEY absente (.env) — famille fondamentale ignoree",
        )

    scores: dict[str, float] = {}
    detail: dict[str, dict] = {}
    for cur, series_list in cfg.FRED_SERIES.items():
        dirs: list[float] = []
        used: dict[str, float] = {}
        for s in series_list:
            m = _series_momentum(s["id"], api_key)
            if m is None:
                continue
            oriented = m if s["dir"] == "up" else -m
            dirs.append(oriented)
            used[s["label"]] = round(oriented, 3)
        if dirs:
            scores[cur] = round(to_0_100(clamp(sum(dirs) / len(dirs), -1.0, 1.0)), 1)
            detail[cur] = used
            if verbose:
                print(f"  [fund] {cur}: {scores[cur]} {used}")

    if not scores:
        return FamilyResult("fundamental", False, {}, {}, note="aucune serie FRED exploitable")
    return FamilyResult("fundamental", True, scores, {"drivers": detail}, note="FRED (momentum 6M)")
