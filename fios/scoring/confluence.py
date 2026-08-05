#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Moteur de CONFLUENCE par paire.

Pour chaque paire, chaque famille produit une contribution directionnelle dans
[-100, +100] (positive = favorable a la base) :

  - fondamental : fund[base] - fund[quote]
  - sentiment   : cot[base]  - cot[quote]
  - correlation : corr[base] - corr[quote]
  - technique   : score multi-TF PROPRE a la paire (x100) — confirmation directe

Le score net est la moyenne ponderee (CONFLUENCE_WEIGHTS renormalises sur les
familles presentes). Un signal BUY/SELL n'est emis que si le net depasse le
seuil ET qu'au moins MIN_FAMILIES_AGREE familles independantes vont dans le
meme sens. Sinon WAIT. La confiance combine l'amplitude du net et le taux
d'accord entre familles.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .. import config as cfg
from ..adapters.base import FamilyResult, clamp

_DEADZONE = 5.0  # |contribution| en-dessous duquel une famille est "neutre"


@dataclass
class PairSignal:
    pair: str
    base: str
    quote: str
    decision: str                       # BUY | SELL | WAIT
    net: float                          # -100..100 (positif = base forte)
    confidence: float                   # 0..100
    quality: int                        # 1..5 etoiles
    contributions: dict[str, float] = field(default_factory=dict)  # famille -> -100..100
    agree: int = 0
    families: int = 0
    coherence: float = 0.0              # 0..100 : les familles racontent-elles la meme histoire ?
    premium: bool = False               # confluence forte ET coherente


def _pair_contributions(
    pair: dict, families: dict[str, FamilyResult],
    pair_score_maps: dict[str, dict[str, float]],
) -> dict[str, float]:
    """Contribution directionnelle [-100,100] par famille pour une paire.

    Familles a score PAR PAIRE (technique, retail) : on utilise leur score
    propre a la paire (x100), plus direct. Les autres (fondamental, sentiment,
    correlation) : difference de force base - quote.
    """
    b, q = pair["base"], pair["quote"]
    contribs: dict[str, float] = {}
    for fam_name, fam in families.items():
        if not fam or not fam.available:
            continue
        if cfg.CONFLUENCE_WEIGHTS.get(fam_name, 0.0) <= 0:
            continue
        pmap = pair_score_maps.get(fam_name)
        if pmap is not None and pair["name"] in pmap:
            contribs[fam_name] = clamp(pmap[pair["name"]] * 100.0, -100.0, 100.0)
            continue
        sb, sq = fam.score(b), fam.score(q)
        if sb is None or sq is None:
            continue
        contribs[fam_name] = clamp(sb - sq, -100.0, 100.0)
    return contribs


def evaluate_pair(
    pair: dict, families: dict[str, FamilyResult],
    pair_score_maps: dict[str, dict[str, float]],
) -> PairSignal:
    contribs = _pair_contributions(pair, families, pair_score_maps)

    # Moyenne ponderee sur les familles presentes.
    wsum = 0.0
    acc = 0.0
    wabs = 0.0
    for fam_name, c in contribs.items():
        w = cfg.CONFLUENCE_WEIGHTS.get(fam_name, 0.0)
        acc += w * c
        wabs += w * abs(c)
        wsum += w
    net = (acc / wsum) if wsum > 0 else 0.0

    # Coherence : |somme ponderee signee| / somme ponderee absolue -> 0..100.
    # 100 = toutes les familles pointent dans le meme sens ; bas = elles se
    # contredisent. Mesure directement la qualite de la convergence (distinct de
    # la confiance, qui melange amplitude et accord).
    coherence = (abs(acc) / wabs * 100.0) if wabs > 0 else 0.0

    direction = 1 if net > 0 else -1
    agree = sum(
        1 for c in contribs.values()
        if abs(c) >= _DEADZONE and (1 if c > 0 else -1) == direction
    )
    families_present = len(contribs)

    decision = "WAIT"
    if agree >= cfg.MIN_FAMILIES_AGREE:
        if net >= cfg.BUY_THRESHOLD:
            decision = "BUY"
        elif net <= cfg.SELL_THRESHOLD:
            decision = "SELL"

    agree_ratio = (agree / families_present) if families_present else 0.0
    confidence = clamp(abs(net) / 60.0 * 70.0 + agree_ratio * 30.0, 0.0, 99.0)
    if decision == "WAIT":
        confidence = min(confidence, 45.0)
    quality = 1 + int(clamp(confidence / 20.0, 0.0, 4.0))
    premium = (decision != "WAIT" and coherence >= 70.0
               and agree >= cfg.MIN_FAMILIES_AGREE)

    return PairSignal(
        pair=pair["name"], base=pair["base"], quote=pair["quote"],
        decision=decision, net=round(net, 1), confidence=round(confidence, 1),
        quality=quality,
        contributions={k: round(v, 1) for k, v in contribs.items()},
        agree=agree, families=families_present,
        coherence=round(coherence, 1), premium=premium,
    )


def evaluate_all(
    families: dict[str, FamilyResult],
    pair_score_maps: dict[str, dict[str, float]],
    pairs: list[dict] | None = None,
) -> list[PairSignal]:
    pairs = pairs or cfg.PAIRS
    signals = [evaluate_pair(p, families, pair_score_maps) for p in pairs]
    # Tri : signaux actionnables d'abord, puis par confiance.
    signals.sort(key=lambda s: (s.decision == "WAIT", -s.confidence))
    return signals
