#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fios/multilayer.py
------------------
Moteur de Scoring Multicouche (Layer 1 à Layer 4) pour identifier les opportunités
à la plus haute conviction (Grade A+, Grade A, Grade B).

Couches d'analyse :
- Layer 1: Force Devises & Indices FIOS (25 Pts)
- Layer 2: Alignement Renko M/W/D (30 Pts)
- Layer 3: Retracement & Breakout Fibo 50% (25 Pts)
- Layer 4: Timing Intraday Vivier H1 (20 Pts)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime

from . import config as cfg


@dataclass
class MultiLayerScore:
    pair: str
    direction: int                     # +1 BULL, -1 BEAR
    total_score: int                   # 0 à 100 points
    grade: str                         # "A+", "A", "B", "NONE"
    layers_passed: list[str] = field(default_factory=list)
    fios_diff: float = 0.0
    daily_chg: float | None = None
    tag_mwd: str = ""


def evaluate_pair_multilayer(
    pair: str,
    composites: dict[str, dict],
    align_pairs: dict[str, dict],
    fibo_50_results: dict[str, dict],
    vivier_state: dict,
) -> MultiLayerScore | None:
    if len(pair) != 6:
        return None

    base, quote = pair[:3], pair[3:]
    cb, cq = composites.get(base), composites.get(quote)
    if not cb or not cq:
        return None

    fios_diff = float(cb["composite"]) - float(cq["composite"])
    thr = cfg.PAIR_MIN_FIOS_DIFF
    fios_dir = 1 if fios_diff >= thr else (-1 if fios_diff <= -thr else 0)

    # Determine potential trade direction from FIOS or Renko alignment
    a = align_pairs.get(pair) or {}
    px_m = a.get("px_m") or 0
    px_w = a.get("px_w") or 0
    px_d = a.get("px_d") or 0

    if px_m == px_w == px_d and px_m != 0:
        direction = px_m
    elif fios_dir != 0:
        direction = fios_dir
    else:
        return None

    score = 0
    layers_passed = []

    # ── Layer 1: Force Devises & Indices FIOS (25 Pts) ──
    l1_pass = (direction == 1 and fios_diff >= thr) or (direction == -1 and fios_diff <= -thr)
    if l1_pass:
        score += 25
        layers_passed.append("FIOS")

    # ── Layer 2: Alignement Renko M/W/D (30 Pts) ──
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

    # ── Layer 3: Retracement Fibo 50% (25 Pts) ──
    tf_fibo = fibo_50_results.get(pair) or {}
    d_fibo = tf_fibo.get("D") or tf_fibo.get("W")
    if d_fibo:
        fibo_dir = getattr(d_fibo, "direction", 0)
        fibo_conf = getattr(d_fibo, "three_brick_confirmed", False)
        px_fibo = getattr(d_fibo, "px_vs_fibo", 0)
        if fibo_dir == direction and fibo_conf and px_fibo == direction:
            score += 25
            layers_passed.append("Fibo 50%")

    # ── Layer 4: Timing Intraday Vivier (20 Pts) ──
    v_pairs = (vivier_state or {}).get("pairs") or {}
    v_entry = v_pairs.get(pair) or {}
    if v_entry and v_entry.get("direction") == direction:
        v_sar = v_entry.get("daily_sar_dir")
        if v_sar == direction:
            score += 20
            layers_passed.append("Vivier H1")

    # Grade Classification
    if score >= 85:
        grade = "A+"
    elif score >= 65:
        grade = "A"
    elif score >= 45:
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
        fios_diff=fios_diff,
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

    all_pairs = sorted(list(set(list(align_pairs.keys()) + list(PAIRS_ALL))))
    scores: list[MultiLayerScore] = []

    for pair in all_pairs:
        s = evaluate_pair_multilayer(pair, composites, align_pairs, fibo_50_results, vivier_state)
        if s is not None:
            scores.append(s)

    return sorted(scores, key=lambda x: (-x.total_score, x.pair))


def format_multilayer_section(scores: list[MultiLayerScore]) -> list[str]:
    grade_aplus = [s for s in scores if s.grade == "A+"]
    grade_a = [s for s in scores if s.grade == "A"]

    if not grade_aplus and not grade_a:
        return []

    lines = []

    if grade_aplus:
        lines.append("🌟🌟🌟 TRADES GRADE A+ (CONFLUENCE TOTALE)")
        for s in grade_aplus:
            icon = "🟢" if s.direction == 1 else "🔴"
            chg_str = f" ({s.daily_chg:+.2f}%)" if isinstance(s.daily_chg, (int, float)) else ""
            layers_str = " + ".join(s.layers_passed)
            lines.append(f"{icon} {s.pair}{chg_str} ({s.tag_mwd}) · Score: {s.total_score}/100")
            lines.append(f"   ↳ {layers_str}")

    if grade_a:
        if lines:
            lines.append("")
        lines.append("🌟🌟 TRADES GRADE A (CONFLUENCE FORTE)")
        for s in grade_a:
            icon = "🟢" if s.direction == 1 else "🔴"
            chg_str = f" ({s.daily_chg:+.2f}%)" if isinstance(s.daily_chg, (int, float)) else ""
            layers_str = " + ".join(s.layers_passed)
            lines.append(f"{icon} {s.pair}{chg_str} ({s.tag_mwd}) · Score: {s.total_score}/100")
            lines.append(f"   ↳ {layers_str}")

    return lines


PAIRS_ALL = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    "EURGBP", "EURJPY", "EURCHF", "EURCAD", "EURAUD", "EURNZD",
    "GBPJPY", "GBPCHF", "GBPCAD", "GBPAUD", "GBPNZD",
    "CHFJPY", "CADJPY", "AUDJPY", "NZDJPY",
    "CADCHF", "AUDCHF", "AUDCAD", "AUDNZD", "NZDCHF", "NZDCAD", "BTCUSD"
]
