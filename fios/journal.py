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
               only_actionable: bool = True) -> dict:
    data = load(path)
    now = datetime.now(timezone.utc).isoformat()
    for s in signals:
        if only_actionable and s.decision == "WAIT":
            continue
        data["entries"].append({
            "ts": now,
            "date": paris_date,
            "pair": s.pair,
            "decision": s.decision,
            "net": s.net,
            "confidence": s.confidence,
            "quality": s.quality,
            "contributions": s.contributions,
            "agree": s.agree,
            "families": s.families,
            "result": None,   # a renseigner au denouement (Phase 2)
        })
    data["updated_at"] = now
    return data


def save(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
