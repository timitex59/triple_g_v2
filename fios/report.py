#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mise en forme des sorties : message Telegram + rapport console.
"""

from __future__ import annotations

from datetime import datetime

import pytz

from . import config as cfg
from .adapters.base import FamilyResult
from .explain import explain, headline
from .scoring.confluence import PairSignal

PARIS = pytz.timezone("Europe/Paris")

_FAM_ABBR = {"fundamental": "Fond", "technical": "Tech", "sentiment": "COT",
             "retail": "Retail", "correlation": "Corr"}
_FAM_LETTER = {"fundamental": "F", "technical": "T", "sentiment": "C",
               "retail": "R", "correlation": "K"}
_FAM_ORDER = ("fundamental", "technical", "sentiment", "retail", "correlation")


def _ranking_line(ranking_rows: list[tuple[str, float]]) -> str:
    return " · ".join(f"{cur} {score:.0f}" for cur, score in ranking_rows)


def _decompo_lines(ranking_rows: list[tuple[str, float]],
                   composites: dict[str, dict]) -> list[str]:
    """Decomposition auditable de la force de chaque devise par famille."""
    if not composites:
        return []
    weights = " ".join(
        f"{_FAM_LETTER[f]}={cfg.CONFLUENCE_WEIGHTS.get(f, 0):.2f}" for f in _FAM_ORDER
    )
    lines = [f"🔍 Décompo force (F=Fond T=Tech C=COT R=Ret K=Corr · poids {weights})"]
    for cur, score in ranking_rows:
        parts = (composites.get(cur) or {}).get("parts", {})
        detail = " ".join(
            f"{_FAM_LETTER[f]}{parts[f]:.0f}" for f in _FAM_ORDER if f in parts
        )
        lines.append(f"{cur} {score:.0f} · {detail}")
    return lines


def _weighted_line(sig: PairSignal) -> str:
    """Ligne pondere auditable : contribution x poids par famille (tri par impact)."""
    items = sorted(
        sig.contributions.items(),
        key=lambda kv: abs(kv[1] * cfg.CONFLUENCE_WEIGHTS.get(kv[0], 0.0)),
        reverse=True,
    )
    parts = [f"{_FAM_ABBR.get(k, k)} {c:+.0f}×{cfg.CONFLUENCE_WEIGHTS.get(k, 0):.2f}"
             for k, c in items]
    return "⚖️ " + " · ".join(parts) + f"  → net {sig.net:+.0f}"


def _families_line(families: dict[str, FamilyResult]) -> str:
    labels = {"fundamental": "Fond", "technical": "Tech", "sentiment": "COT",
              "retail": "Retail", "correlation": "Corr"}
    order = ("fundamental", "technical", "sentiment", "retail", "correlation")
    on = [labels[n] for n in order if families.get(n) and families[n].available]
    off = [labels[n] for n in order if not (families.get(n) and families[n].available)]
    txt = "Familles: " + ("+".join(on) if on else "aucune")
    if off:
        txt += f"  (inactives: {', '.join(off)})"
    return txt


def build_message(
    ranking_rows: list[tuple[str, float]],
    signals: list[PairSignal],
    families: dict[str, FamilyResult],
    top_n: int = 5,
    desk_note: str | None = None,
    cross_section: list[str] | None = None,
    composites: dict[str, dict] | None = None,
) -> str:
    now = datetime.now(PARIS).strftime("%d/%m/%Y %H:%M")
    lines: list[str] = []
    lines.append("🧭 FIOS — Confluence Forex")
    lines.append(_families_line(families))
    lines.append("")
    if desk_note:
        lines.append("🧠 Note du desk")
        lines.append(desk_note.strip())
        lines.append("")
    lines.append("💪 Force devises (0-100)")
    lines.append(_ranking_line(ranking_rows))
    lines.append("")
    if composites:
        lines.extend(_decompo_lines(ranking_rows, composites))
        lines.append("")
    if cross_section:
        lines.extend(cross_section)
        lines.append("")

    actionable = [s for s in signals if s.decision != "WAIT"][:top_n]
    if actionable:
        lines.append("🎯 Signaux (confluence)")
        for s in actionable:
            emoji = "🟢" if s.decision == "BUY" else "🔴"
            badge = " 🔷 Premium" if s.premium else ""
            lines.append(f"{emoji} {headline(s)} · cohérence {s.coherence:.0f}%{badge}")
            lines.append(f"   {_weighted_line(s)}")
            for r in explain(s, families)[:2]:
                lines.append(f"   • {r}")
            lines.append("")
    else:
        lines.append("😴 Aucun signal en confluence aujourd'hui (tout en ATTENTE).")
        lines.append("")

    lines.append(f"⏰ {now} Paris")
    return "\n".join(lines)


def build_console(
    ranking_rows: list[tuple[str, float]],
    signals: list[PairSignal],
    families: dict[str, FamilyResult],
) -> str:
    lines = ["=" * 60, "FIOS — Confluence Forex", "=" * 60]
    lines.append(_families_line(families))
    lines.append("")
    lines.append("Force par devise:")
    for cur, score in ranking_rows:
        bar = "█" * int(score / 5)
        lines.append(f"  {cur}  {score:5.1f}  {bar}")
    lines.append("")
    lines.append("Signaux par paire (tries):")
    for s in signals:
        tag = {"BUY": "BUY ", "SELL": "SELL", "WAIT": "wait"}[s.decision]
        contribs = " ".join(f"{k[:4]}={v:+.0f}" for k, v in s.contributions.items())
        lines.append(
            f"  [{tag}] {s.pair:7s} net={s.net:+6.1f} conf={s.confidence:4.0f}% "
            f"agree={s.agree}/{s.families}  {contribs}"
        )
    return "\n".join(lines)
