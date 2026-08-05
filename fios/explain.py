#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Explication d'un signal (Phase 1 : gabarit deterministe).

Traduit un PairSignal + les details des familles en raisons lisibles. En
Phase 1.5, cette couche sera remplacee/augmentee par un LLM qui redige le
"pourquoi" en langage naturel a partir des memes elements structures.
"""

from __future__ import annotations

from .adapters.base import FamilyResult
from .scoring.confluence import PairSignal

_FR = {"BUY": "ACHAT", "SELL": "VENTE", "WAIT": "ATTENTE"}


def explain(sig: PairSignal, families: dict[str, FamilyResult]) -> list[str]:
    b, q = sig.base, sig.quote
    reasons: list[str] = []
    fav = b if sig.net >= 0 else q  # devise favorisee par le net

    fund = families.get("fundamental")
    if fund and fund.available and "fundamental" in sig.contributions:
        c = sig.contributions["fundamental"]
        strong, weak = (b, q) if c >= 0 else (q, b)
        reasons.append(f"Fondamental : {strong} mieux oriente que {weak} (ecart {c:+.0f}).")

    tech = families.get("technical")
    if tech and tech.available and "technical" in sig.contributions:
        c = sig.contributions["technical"]
        tfd = tech.details.get("tf_detail", {}).get(sig.pair, {}).get("tfs", {})
        tf_txt = ", ".join(f"{k}:{'haussier' if v > 0 else 'baissier'}" for k, v in tfd.items())
        sens = "haussiere" if c >= 0 else "baissiere"
        reasons.append(f"Technique {sens} sur {sig.pair} ({tf_txt}).")

    sent = families.get("sentiment")
    if sent and sent.available and "sentiment" in sig.contributions:
        cot = sent.details.get("cot", {})
        cb = cot.get(b, {}).get("net_pct")
        cq = cot.get(q, {}).get("net_pct")
        if cb is not None and cq is not None:
            reasons.append(
                f"Positionnement COT : {b} net {cb:+.0f}% vs {q} net {cq:+.0f}% "
                "(large specs)."
            )

    retail = families.get("retail")
    if retail and retail.available and "retail" in sig.contributions:
        lp = retail.details.get("long_pct", {}).get(sig.pair, {})
        pct = lp.get("long_pct")
        if pct is not None:
            reasons.append(
                f"Retail contrarien : {pct:.0f}% des particuliers longs {sig.pair} "
                f"-> biais {'baissier' if pct >= 50 else 'haussier'}."
            )

    corr = families.get("correlation")
    if corr and corr.available and "correlation" in sig.contributions:
        c = sig.contributions["correlation"]
        reasons.append(
            f"Flux inter-marches {'favorables' if (c >= 0) == (sig.net >= 0) else 'defavorables'} "
            f"a {fav} (DXY / rendements / risque)."
        )

    reasons.append(
        f"Confluence : {sig.agree}/{sig.families} familles convergent, "
        f"net {sig.net:+.0f}, confiance {sig.confidence:.0f}%."
    )
    return reasons


def headline(sig: PairSignal) -> str:
    stars = "★" * sig.quality + "☆" * (5 - sig.quality)
    return f"{_FR.get(sig.decision, sig.decision)} {sig.pair}  {stars}  ({sig.confidence:.0f}%)"
