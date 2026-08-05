#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Adaptateur SENTIMENT / POSITIONNEMENT — COT (CFTC, gratuit).

Lit le rapport "Legacy Futures-Only" de la CFTC via l'API Socrata et extrait,
pour chaque devise, le positionnement net des "non-commercials" (large specs,
generalement suiveurs de tendance) sur le future CME correspondant.

net = long - short ; net_pct = net / open_interest.
USD est derive du future "U.S. DOLLAR INDEX" (positionnement direct).

Le score par devise est le rang percentile du net_pct parmi les devises
disponibles (0 = le plus vendeur, 100 = le plus acheteur). Publie chaque
vendredi (donnees arretees au mardi), donc lent mais robuste et fiable.
"""

from __future__ import annotations

import requests

from .. import config as cfg
from .base import FamilyResult, rank_to_0_100


def _latest_for(pattern: str) -> dict | None:
    params = {
        "$where": f"market_and_exchange_names like '%{pattern}%'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": "1",
    }
    try:
        r = requests.get(cfg.COT_SOCRATA_URL, params=params, timeout=20)
        r.raise_for_status()
        rows = r.json()
        return rows[0] if rows else None
    except Exception:
        return None


def _f(row: dict, key: str) -> float:
    try:
        return float(row.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def run(verbose: bool = False) -> FamilyResult:
    raw: dict[str, dict] = {}
    for cur, pattern in cfg.COT_MARKET_PATTERNS.items():
        row = _latest_for(pattern)
        if not row:
            if verbose:
                print(f"  [cot] {cur}: introuvable ({pattern})")
            continue
        longs = _f(row, "noncomm_positions_long_all")
        shorts = _f(row, "noncomm_positions_short_all")
        oi = _f(row, "open_interest_all")
        net = longs - shorts
        net_pct = (net / oi * 100.0) if oi else 0.0
        chg = _f(row, "change_in_noncomm_long_all") - _f(row, "change_in_noncomm_short_all")
        raw[cur] = {
            "net": net,
            "net_pct": round(net_pct, 2),
            "change": chg,
            "report_date": row.get("report_date_as_yyyy_mm_dd", "")[:10],
        }
        if verbose:
            print(f"  [cot] {cur}: net%={net_pct:+.1f} chg={chg:+.0f} ({raw[cur]['report_date']})")

    if not raw:
        return FamilyResult("sentiment", False, {}, {}, note="COT indisponible (reseau/CFTC)")

    net_pcts = [v["net_pct"] for v in raw.values()]
    scores = {cur: round(rank_to_0_100(v["net_pct"], net_pcts), 1) for cur, v in raw.items()}
    return FamilyResult(
        name="sentiment",
        available=True,
        scores=scores,
        details={"cot": raw},
        note=f"COT au {next(iter(raw.values()))['report_date']}",
    )
