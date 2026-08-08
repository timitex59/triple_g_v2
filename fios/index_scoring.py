#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fios/index_scoring.py
---------------------
Système de Scoring des Indices Devises Constitutifs (-100 à +100 Pts).

Calcule le score objectif de chaque devise (AUD, NZD, EUR, GBP, CHF, CAD, JPY, USD)
à partir de 4 piliers (la STRUCTURE domine, le pop du jour ne fait que confirmer) :
1. Alignement Multi-TF M/W/D - Max ±30 Pts  (le socle)
2. Force Composite FIOS       - Max ±25 Pts
3. Variation Daily (CHG%D)    - Max ±15 Pts  (confirmation, pas domination)
4. Rang Relatif sur 8         - Max ±10 Pts  (départage)
Total borné à ±80.
"""

from __future__ import annotations

from dataclasses import dataclass

CURRENCIES_ALL = ["AUD", "NZD", "EUR", "GBP", "CHF", "CAD", "JPY", "USD"]


@dataclass
class CurrencyScore:
    currency: str
    total_score: int                   # -100 à +100
    label: str                         # "ULTRA-FORT", "FORT", "NEUTRE", "FAIBLE", "ULTRA-FAIBLE"
    icon: str                          # 🟢🟢, 🟢, ⚪, 🔴, 🔴🔴
    daily_chg: float
    tag_mwd: str
    rank: int                          # 1 à 8
    fios_composite: float


def _sign(v: int) -> str:
    return "+" if v == 1 else ("-" if v == -1 else "0")


def compute_currency_index_scores(
    composites: dict[str, dict],
    payload: dict | None,
) -> dict[str, CurrencyScore]:
    payload = payload or {}
    align_indexes = payload.get("indexes") or payload.get("currencies") or {}

    # Gather daily change and M/W/D states for each currency
    cur_data: dict[str, dict] = {}
    for cur in CURRENCIES_ALL:
        idx_info = align_indexes.get(cur) or {}
        chg = float(idx_info.get("daily_chg") or 0.0)
        px_m = int(idx_info.get("px_m") or 0)
        px_w = int(idx_info.get("px_w") or 0)
        px_d = int(idx_info.get("px_d") or 0)

        comp_info = composites.get(cur) or {}
        comp_val = float(comp_info.get("composite") or 50.0)

        cur_data[cur] = {
            "daily_chg": chg,
            "px_m": px_m,
            "px_w": px_w,
            "px_d": px_d,
            "composite": comp_val,
        }

    # Rank currencies by daily change (primary) and composite (secondary)
    ranked_cur = sorted(
        CURRENCIES_ALL,
        key=lambda c: (cur_data[c]["daily_chg"], cur_data[c]["composite"]),
        reverse=True,
    )
    ranks = {cur: i + 1 for i, cur in enumerate(ranked_cur)}

    rank_points_map = {1: 10, 2: 7, 3: 4, 4: 1, 5: -1, 6: -4, 7: -7, 8: -10}

    results: dict[str, CurrencyScore] = {}

    for cur in CURRENCIES_ALL:
        d = cur_data[cur]
        chg = d["daily_chg"]
        px_m, px_w, px_d = d["px_m"], d["px_w"], d["px_d"]
        comp = d["composite"]
        rank = ranks[cur]

        # ── Pilier 1: Alignement Multi-TF M/W/D (Max ±30) — le socle ──
        p1_struct = (px_m * 10) + (px_w * 10) + (px_d * 10)

        # ── Pilier 2: Force Composite FIOS (Max ±25) ──
        p2_comp = max(-25, min(25, round((comp - 50.0) * 0.5)))

        # ── Pilier 3: Variation Daily (Max ±15) — confirme, ne domine pas ──
        if chg >= 0.30:
            p3_daily = 15
        elif chg >= 0.10:
            p3_daily = 8
        elif chg > -0.10:
            p3_daily = 0
        elif chg > -0.30:
            p3_daily = -8
        else:
            p3_daily = -15

        # ── Pilier 4: Rang Relatif sur 8 (Max ±10) — départage ──
        p4_rank = rank_points_map.get(rank, 0)

        # Score Global borné à ±80.
        tot = max(-80, min(80, p1_struct + p2_comp + p3_daily + p4_rank))

        if tot >= 55:
            label, icon = "ULTRA-FORT", "🟢🟢"
        elif tot >= 25:
            label, icon = "FORT", "🟢"
        elif tot > -25:
            label, icon = "NEUTRE", "⚪"
        elif tot > -55:
            label, icon = "FAIBLE", "🔴"
        else:
            label, icon = "ULTRA-FAIBLE", "🔴🔴"

        tag_mwd = f"M{_sign(px_m)} W{_sign(px_w)} D{_sign(px_d)}"

        results[cur] = CurrencyScore(
            currency=cur,
            total_score=tot,
            label=label,
            icon=icon,
            daily_chg=chg,
            tag_mwd=tag_mwd,
            rank=rank,
            fios_composite=comp,
        )

    return results
