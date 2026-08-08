#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fios/multilayer.py
------------------
Moteur de Scoring Multicouche Ultime avec Agrégation des Indices Devises.

Calcule le score de chaque paire (0 à 100 Pts) à partir du différentiel d'indices
et de la structure technique pour établir le Podium des Meilleures Paires (🥇 🥈 🥉).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from .index_scoring import compute_currency_index_scores, CurrencyScore
from . import config as cfg


@dataclass
class MultiLayerScore:
    pair: str
    direction: int                     # +1 BULL, -1 BEAR
    total_score: int                   # 0 à 100 points
    grade: str                         # "A+", "A", "B", "NONE"
    layers_passed: list[str] = field(default_factory=list)
    index_diff: int = 0
    base_score: int = 0
    quote_score: int = 0
    base_cur: str = ""
    quote_cur: str = ""
    daily_chg: float | None = None
    tag_mwd: str = ""


def evaluate_pair_multilayer(
    pair: str,
    composites: dict[str, dict],
    align_pairs: dict[str, dict],
    fibo_50_results: dict[str, dict],
    vivier_state: dict,
    index_scores: dict[str, CurrencyScore],
) -> MultiLayerScore | None:
    if len(pair) != 6:
        return None

    base, quote = pair[:3], pair[3:]
    sb = index_scores.get(base)
    sq = index_scores.get(quote)
    if not sb or not sq:
        return None

    index_diff = sb.total_score - sq.total_score
    index_dir = 1 if index_diff > 0 else (-1 if index_diff < 0 else 0)

    # Determine potential trade direction from Renko alignment or Index differential
    a = align_pairs.get(pair) or {}
    px_m = a.get("px_m") or 0
    px_w = a.get("px_w") or 0
    px_d = a.get("px_d") or 0

    if px_m == px_w == px_d and px_m != 0:
        direction = px_m
    elif index_dir != 0:
        direction = index_dir
    else:
        return None

    score = 0
    layers_passed = []

    # ── Layer 1: Différentiel d'Indices Devises (Max 30 Pts) ──
    signed_diff = index_diff * direction
    if signed_diff >= 120:
        score += 30
        layers_passed.append("Diff Index Max")
    elif signed_diff >= 80:
        score += 20
        layers_passed.append("Diff Index Fort")
    elif signed_diff >= 40:
        score += 10
        layers_passed.append("Diff Index Modéré")

    # ── Layer 2: Structure Renko M/W/D (Max 30 Pts) ──
    l2_strict = (px_m == direction and px_w == direction and px_d == direction)
    l2_solid = (
        (sum(1 for v in (px_m, px_w, px_d) if v == direction) >= 2)
        and (sum(1 for v in (px_m, px_w, px_d) if v == -direction) == 0)
    )
    if l2_strict:
        score += 30
        layers_passed.append("Renko M/W/D")
    elif l2_solid:
        score += 20
        layers_passed.append("Renko 2-TF")

    # ── Layer 3: Breakout / Retracement Fibo 50% (Max 25 Pts) ──
    tf_fibo = fibo_50_results.get(pair) or {}
    d_fibo = tf_fibo.get("D") or tf_fibo.get("W")
    if d_fibo:
        fibo_dir = getattr(d_fibo, "direction", 0)
        fibo_conf = getattr(d_fibo, "three_brick_confirmed", False)
        px_fibo = getattr(d_fibo, "px_vs_fibo", 0)
        if fibo_dir == direction and fibo_conf and px_fibo == direction:
            score += 25
            layers_passed.append("Fibo 50%")

    # ── Layer 4: Timing Intraday Vivier H1 (Max 15 Pts) ──
    v_pairs = (vivier_state or {}).get("pairs") or {}
    v_entry = v_pairs.get(pair) or {}
    if v_entry and v_entry.get("direction") == direction:
        v_sar = v_entry.get("daily_sar_dir")
        if v_sar == direction:
            score += 15
            layers_passed.append("Vivier H1")

    # Grade Classification
    if score >= 80:
        grade = "A+"
    elif score >= 60:
        grade = "A"
    elif score >= 40:
        grade = "B"
    else:
        grade = "NONE"

    if grade == "NONE":
        return None

    def _sign(v):
        return "+" if v == 1 else ("-" if v == -1 else "0")

    tag_mwd = f"M{_sign(px_m)} W{_sign(px_w)} D{_sign(px_d)}"
    chg = a.get("daily_chg")

    return MultiLayerScore(
        pair=pair,
        direction=direction,
        total_score=score,
        grade=grade,
        layers_passed=layers_passed,
        index_diff=index_diff,
        base_score=sb.total_score,
        quote_score=sq.total_score,
        base_cur=base,
        quote_cur=quote,
        daily_chg=chg,
        tag_mwd=tag_mwd,
    )


def compute_multilayer_matrix(
    composites: dict[str, dict],
    payload: dict | None,
    fibo_50_results: dict[str, dict] | None = None,
    vivier_state: dict | None = None,
) -> list[MultiLayerScore]:
    payload = payload or {}
    align_pairs = payload.get("pairs") or {}
    fibo_50_results = fibo_50_results or {}
    vivier_state = vivier_state or {}

    index_scores = compute_currency_index_scores(composites, payload)
    all_pairs = sorted(list(set(list(align_pairs.keys()) + list(PAIRS_ALL))))
    scores: list[MultiLayerScore] = []

    for pair in all_pairs:
        s = evaluate_pair_multilayer(
            pair, composites, align_pairs, fibo_50_results, vivier_state, index_scores
        )
        if s is not None:
            scores.append(s)

    return sorted(scores, key=lambda x: (-x.total_score, x.pair))


def format_multilayer_section(scores: list[MultiLayerScore], top_n: int = 5) -> list[str]:
    retained = [s for s in scores if s.grade in ("A+", "A", "B")]
    if not retained:
        return []

    lines = ["🏆 CLASSEMENT DES MEILLEURES PAIRES (TOP CONFLUENCE)"]
    medals = ["🥇", "🥈", "🥉"]

    for i, s in enumerate(retained[:top_n], 1):
        medal = medals[i - 1] if i <= 3 else f"{i}."
        icon = "🟢" if s.direction == 1 else "🔴"
        chg_str = f" ({s.daily_chg:+.2f}%)" if isinstance(s.daily_chg, (int, float)) else ""
        layers_str = " + ".join(s.layers_passed)

        lines.append(f"{medal} {icon} {s.pair}{chg_str} ({s.tag_mwd}) · Score: {s.total_score}/100 (GRADE {s.grade})")
        lines.append(f"   ↳ Diff Index: {s.index_diff:+d} ({s.base_cur} {s.base_score:+d} vs {s.quote_cur} {s.quote_score:+d}) · {layers_str}")

    return lines


PAIRS_ALL = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    "EURGBP", "EURJPY", "EURCHF", "EURCAD", "EURAUD", "EURNZD",
    "GBPJPY", "GBPCHF", "GBPCAD", "GBPAUD", "GBPNZD",
    "CHFJPY", "CADJPY", "AUDJPY", "NZDJPY",
    "CADCHF", "AUDCHF", "AUDCAD", "AUDNZD", "NZDCHF", "NZDCAD", "BTCUSD"
]
