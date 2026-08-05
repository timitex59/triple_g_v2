#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agregation statistique d'une liste de trades denoues.

Chaque trade est un dict portant au moins "r" (multiple de risque realise) et
"outcome". On calcule win rate, R moyen (expectancy), somme des R, profit
factor, R moyen gagnant/perdant et drawdown max (sur la courbe cumulative des R,
c.-a-d. en supposant un risque constant par trade).
"""

from __future__ import annotations

from typing import Callable


def summarize(trades: list[dict]) -> dict:
    n = len(trades)
    if n == 0:
        return {"trades": 0}
    rs = [float(t["r"]) for t in trades]
    wins = [r for r in rs if r > 0]
    losses = [r for r in rs if r <= 0]
    sum_win = sum(wins)
    sum_loss = sum(losses)  # <= 0
    pf = (sum_win / abs(sum_loss)) if sum_loss < 0 else None

    eq = 0.0
    peak = 0.0
    maxdd = 0.0
    for r in rs:
        eq += r
        peak = max(peak, eq)
        maxdd = min(maxdd, eq - peak)

    return {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(100.0 * len(wins) / n, 1),
        "avg_r": round(sum(rs) / n, 3),
        "sum_r": round(sum(rs), 2),
        "profit_factor": round(pf, 2) if pf is not None else None,
        "avg_win_r": round(sum_win / len(wins), 3) if wins else None,
        "avg_loss_r": round(sum_loss / len(losses), 3) if losses else None,
        "max_drawdown_r": round(maxdd, 2),
    }


def grouped(trades: list[dict], keyfn: Callable[[dict], str],
            min_trades: int = 1) -> dict[str, dict]:
    groups: dict[str, list[dict]] = {}
    for t in trades:
        groups.setdefault(keyfn(t), []).append(t)
    out = {k: summarize(v) for k, v in groups.items() if len(v) >= min_trades}
    # Tri par expectancy decroissante pour la lisibilite.
    return dict(sorted(out.items(), key=lambda kv: kv[1].get("avg_r", 0), reverse=True))


def confidence_bucket(conf: float) -> str:
    if conf >= 70:
        return "70-100"
    if conf >= 55:
        return "55-70"
    if conf >= 40:
        return "40-55"
    return "0-40"


def family_combo(contributions: dict) -> str:
    """Cle lisible de la combinaison de familles presentes dans un signal."""
    abbr = {"fundamental": "Fond", "technical": "Tech", "sentiment": "COT",
            "retail": "Retail", "correlation": "Corr"}
    return "+".join(abbr.get(k, k) for k in sorted(contributions.keys())) or "aucune"
