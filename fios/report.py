#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mise en forme des sorties : message Telegram + rapport console.
"""

from __future__ import annotations

from datetime import datetime

import pytz

from .adapters.base import FamilyResult
from .explain import explain, headline
from .scoring.confluence import PairSignal

PARIS = pytz.timezone("Europe/Paris")


def _ranking_line(ranking_rows: list[tuple[str, float]]) -> str:
    return " · ".join(f"{cur} {score:.0f}" for cur, score in ranking_rows)


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
    if cross_section:
        lines.extend(cross_section)
        lines.append("")

    actionable = [s for s in signals if s.decision != "WAIT"][:top_n]
    if actionable:
        lines.append("🎯 Signaux (confluence)")
        for s in actionable:
            emoji = "🟢" if s.decision == "BUY" else "🔴"
            lines.append(f"{emoji} {headline(s)}")
            for r in explain(s, families)[:3]:
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
