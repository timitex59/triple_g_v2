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

import json
import os
from datetime import datetime
from dataclasses import dataclass, field

import pytz

from .index_scoring import compute_currency_index_scores, CurrencyScore
from . import config as cfg

# ── Memoire de podium (hysteresis) ─────────────────────────────────────────
# Le podium etait recalcule de zero a chaque cycle, sans inertie : le moindre
# bruit sur le score faisait tourner le champion. On donne au podium une
# MEMOIRE (comme le VIVIER garde ses positions ouvertes) : une paire qui reste
# qualifiee accumule de l'anciennete (streak) et garde sa place tant qu'elle ne
# perd pas de palier de grade. Persiste entre les runs CI via STATE_FILES.
_PODIUM_STATE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "fios_podium_state.json"
)
_PARIS = pytz.timezone("Europe/Paris")
_STREAK_CAP = 6          # au-dela, l'anciennete sature (~6 cycles = ~1h)
_PODIUM_GRACE = 1        # tolere 1 cycle d'absence (anti-clignotement du seuil)
_GRADE_TIER = {"A+": 3, "A": 2, "B": 1, "NONE": 0}


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
    streak: int = 0                    # cycles consecutifs qualifie (hysteresis)


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

    # Confirmation du jour : la variation du jour doit aller dans le sens du
    # trade et depasser le seuil. Garantit la coherence avec la section
    # CONFLUENCE (plus de #1 dont le jour contredit la direction).
    chg = a.get("daily_chg")
    if not isinstance(chg, (int, float)) or abs(chg) <= cfg.PAIR_MIN_DAILY_CHG:
        return None
    if (1 if chg > 0 else -1) != direction:
        return None

    score = 0
    layers_passed = []

    # ── Layer 1: Différentiel d'Indices Devises (Max 30 Pts) ──
    # Seuils cales sur la nouvelle echelle d'indice (±80 -> diff jusqu'a ±160).
    signed_diff = index_diff * direction
    if signed_diff >= 90:
        score += 30
        layers_passed.append("Diff Index Max")
    elif signed_diff >= 55:
        score += 20
        layers_passed.append("Diff Index Fort")
    elif signed_diff >= 25:
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


def _paris_date() -> str:
    return datetime.now(_PARIS).strftime("%Y-%m-%d")


def _load_podium_state(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_podium_state(path: str, state: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass  # non-fatal : la persistance ne doit jamais casser le run FIOS


def _apply_hysteresis(scores: list[MultiLayerScore], path: str) -> None:
    """Renseigne .streak (nb de cycles consecutifs qualifie) et persiste l'etat.
    Une paire qui reste qualifiee gagne de l'anciennete, ce qui la maintient au
    podium malgre le bruit du score. Tolere _PODIUM_GRACE cycle(s) d'absence
    pour ne pas remettre l'anciennete a zero sur un simple clignotement du seuil.
    L'etat est reinitialise a chaque nouveau jour (heure de Paris)."""
    today = _paris_date()
    prev = _load_podium_state(path)
    prev_pairs = prev.get("pairs", {}) if prev.get("date") == today else {}

    cur_names: set[str] = set()
    new_pairs: dict[str, dict] = {}
    for s in scores:
        cur_names.add(s.pair)
        p = prev_pairs.get(s.pair)
        if p and int(p.get("direction", 0)) == s.direction:
            s.streak = int(p.get("streak", 0)) + 1
        else:
            s.streak = 1
        new_pairs[s.pair] = {"streak": s.streak, "direction": s.direction, "misses": 0}

    # Grace : garde en memoire (sans afficher) une paire absente ce cycle, pour
    # que son anciennete reprenne si elle repasse le seuil au cycle suivant.
    for name, p in prev_pairs.items():
        if name in cur_names:
            continue
        misses = int(p.get("misses", 0)) + 1
        if misses <= _PODIUM_GRACE:
            new_pairs[name] = {
                "streak": int(p.get("streak", 0)),
                "direction": int(p.get("direction", 0)),
                "misses": misses,
            }

    _save_podium_state(path, {"date": today, "pairs": new_pairs})


def compute_multilayer_matrix(
    composites: dict[str, dict],
    payload: dict | None,
    fibo_50_results: dict[str, dict] | None = None,
    vivier_state: dict | None = None,
    state_path: str | None = None,
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

    # Hysteresis : renseigne .streak et persiste l'etat du podium.
    _apply_hysteresis(scores, state_path or _PODIUM_STATE)

    # Tri stable : palier de grade d'abord (A+ > A > B), puis anciennete (une
    # paire deja installee garde sa place), puis score fin, puis ecart d'indice
    # continu (departage reel, plus l'ordre alphabetique arbitraire), puis nom.
    return sorted(
        scores,
        key=lambda x: (
            -_GRADE_TIER.get(x.grade, 0),
            -min(x.streak, _STREAK_CAP),
            -x.total_score,
            -(x.index_diff * x.direction),
            x.pair,
        ),
    )


def format_multilayer_section(scores: list[MultiLayerScore], top_n: int = 5) -> list[str]:
    """Classement : icône + paire + CHG%D + tag M/W/D, même format que CONFLUENCE.

    La SELECTION des paires (top_n) reste pilotée par le score + l'hystérésis
    (membres stables) ; seul l'AFFICHAGE est trié par |CHG%D| décroissant."""
    retained = [s for s in scores if s.grade in ("A+", "A", "B")]
    if not retained:
        return []

    def _abs_chg(s: MultiLayerScore) -> float:
        return abs(s.daily_chg) if isinstance(s.daily_chg, (int, float)) else 0.0

    top = sorted(retained[:top_n], key=_abs_chg, reverse=True)

    lines = ["🏆 CLASSEMENT", ""]
    for s in top:
        icon = "🟢" if s.direction == 1 else "🔴"
        chg_txt = f"{s.daily_chg:+.2f}%" if isinstance(s.daily_chg, (int, float)) else "---"
        lines.append(f"{icon} {s.pair} ({chg_txt}) ({s.tag_mwd})")

    return lines


PAIRS_ALL = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    "EURGBP", "EURJPY", "EURCHF", "EURCAD", "EURAUD", "EURNZD",
    "GBPJPY", "GBPCHF", "GBPCAD", "GBPAUD", "GBPNZD",
    "CHFJPY", "CADJPY", "AUDJPY", "NZDJPY",
    "CADCHF", "AUDCHF", "AUDCAD", "AUDNZD", "NZDCHF", "NZDCAD"
]
