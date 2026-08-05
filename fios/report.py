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


_FR = {"BUY": "ACHAT", "SELL": "VENTE", "WAIT": "ATTENTE"}


def _vote_line(sig: PairSignal) -> str:
    """Vote de chaque famille : ✔ si elle va dans le sens de la decision, ✗ sinon
    (avec sa contribution signee). Rend la cohérence intuitive."""
    ddir = 1 if sig.decision == "BUY" else -1
    items = sorted(sig.contributions.items(), key=lambda kv: abs(kv[1]), reverse=True)
    parts = []
    for k, c in items:
        agree = (1 if c > 0 else -1) == ddir and abs(c) >= 5
        parts.append(f"{'✔' if agree else '✗'}{_FAM_ABBR.get(k, k)} {c:+.0f}")
    return " · ".join(parts)


def _freshness_line(families: dict[str, FamilyResult], freshness: dict | None) -> str:
    if not freshness:
        return ""
    order = ("fundamental", "technical", "sentiment", "retail", "correlation")
    parts = [f"{_FAM_ABBR[n]} {freshness[n]}"
             for n in order
             if families.get(n) and families[n].available and freshness.get(n)]
    return "🕐 Fraîcheur : " + " · ".join(parts) if parts else ""


def _plan_lines(plan: dict | None) -> list[str]:
    if not plan:
        return []
    return [
        (f"   📍 Entrée {plan['entry_s']} · SL {plan['sl_s']} · "
         f"TP1 {plan['tp1_s']} · TP2 {plan['tp2_s']} · RR {plan['rr']:.1f}"),
        f"   ❌ Invalidation : clôture D au-delà de {plan['sl_s']}",
    ]


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
    freshness: dict | None = None,
    trade_plans: dict[str, dict] | None = None,
) -> str:
    now = datetime.now(PARIS).strftime("%d/%m/%Y %H:%M")
    trade_plans = trade_plans or {}
    lines: list[str] = []
    lines.append("🧭 FIOS — Confluence Forex")
    lines.append(_families_line(families))
    fr_line = _freshness_line(families, freshness)
    if fr_line:
        lines.append(fr_line)
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
            stars = "★" * s.quality + "☆" * (5 - s.quality)
            badge = " 🔷 Premium" if s.premium else ""
            lines.append(
                f"{emoji} {_FR.get(s.decision, s.decision)} {s.pair}  {stars}  "
                f"{s.agree}/{s.families} fam · cohérence {s.coherence:.0f}% · "
                f"conf {s.confidence:.0f}%{badge}"
            )
            lines.append(f"   {_vote_line(s)}")
            lines.extend(_plan_lines(trade_plans.get(s.pair)))
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
