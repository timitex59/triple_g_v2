#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Journal des recommandations (memoire statistique).

Chaque run actionnable est ajoute a fios_journal.json : horodatage, signal,
scores et contributions. C'est la matiere premiere du backtest (Phase 2) et de
l'apprentissage adaptatif (Phase 3) — le ML ne peut ajuster les poids que
lorsqu'un historique (signaux -> resultat) existe. On l'accumule donc des
maintenant, meme si l'exploitation vient plus tard.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from .scoring.confluence import PairSignal

_SCHEMA = 1


def _empty() -> dict:
    return {"schema_version": _SCHEMA, "entries": [], "updated_at": None}


def load(path: str) -> dict:
    if not os.path.exists(path):
        return _empty()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "entries" not in data:
            return _empty()
        return data
    except Exception:
        return _empty()


def append_run(path: str, paris_date: str, signals: list[PairSignal],
               only_actionable: bool = True,
               context: dict[str, dict] | None = None) -> dict:
    """Journalise chaque signal avec TOUT son contexte des le premier jour, pour
    que la Phase 2 puisse analyser/calibrer sans regretter des variables
    manquantes. context[pair] apporte forces devises, parts par famille, regime,
    plan (ATR/entree/SL/TP/RR) et etat du marche."""
    data = load(path)
    context = context or {}
    now = datetime.now(timezone.utc).isoformat()
    for s in signals:
        if only_actionable and s.decision == "WAIT":
            continue
        ctx = context.get(s.pair, {})
        data["entries"].append({
            "ts": now,
            "date": paris_date,
            "pair": s.pair,
            "base": s.base,
            "quote": s.quote,
            "decision": s.decision,
            "net": s.net,
            "confidence": s.confidence,
            "coherence": s.coherence,
            "premium": s.premium,
            "quality": s.quality,
            "contributions": s.contributions,
            "agree": s.agree,
            "families": s.families,
            # --- Contexte (pour l'analyse Phase 2) ---
            "force_base": ctx.get("force_base"),
            "force_quote": ctx.get("force_quote"),
            "parts_base": ctx.get("parts_base"),
            "parts_quote": ctx.get("parts_quote"),
            "regime": ctx.get("regime"),
            "market_risk": ctx.get("market_risk"),
            "market_vol": ctx.get("market_vol"),
            "atr": ctx.get("atr"),
            "entry": ctx.get("entry"),
            "sl": ctx.get("sl"),
            "tp1": ctx.get("tp1"),
            "tp2": ctx.get("tp2"),
            "rr": ctx.get("rr"),
            # Rempli au denouement : outcome, exit_reason, r, bars_held,
            # mae_r (excursion adverse max), mfe_r (excursion favorable max).
            "result": None,
        })
    data["updated_at"] = now
    return data


def save(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
