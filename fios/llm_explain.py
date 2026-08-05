#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Couche d'explication LLM (Phase 1.2) — "note du desk".

A partir des donnees FIOS deja calculees (classement des devises, signaux de
confluence, contributions par famille), un LLM redige une note de marche courte
et professionnelle en francais. Le LLM n'invente rien : il ne fait que mettre en
mots des chiffres qu'on lui fournit.

Fournisseur : Anthropic (Claude) par defaut, OpenAI en secours. Toute erreur
(cle absente, SDK non installe, API indisponible, refus) renvoie None, et
l'appelant retombe sur le gabarit deterministe : le message quotidien ne casse
jamais a cause du LLM.
"""

from __future__ import annotations

import os
from typing import Any

from . import config as cfg
from .adapters.base import FamilyResult
from .scoring.confluence import PairSignal

_FR = {"BUY": "ACHAT", "SELL": "VENTE", "WAIT": "ATTENTE"}

_SYSTEM = (
    "Tu es analyste sur un desk de trading Forex. Tu rediges une note de marche "
    "quotidienne, courte et professionnelle, en francais. "
    "Regles strictes : n'utilise QUE les chiffres fournis, n'invente aucune "
    "donnee ni aucun niveau de prix. Sois concis (5 a 8 phrases maximum). "
    "Structure : 1) le biais general des devises du jour, 2) les 2-3 meilleures "
    "idees de trade en expliquant la confluence (quelles familles convergent et "
    "pourquoi), 3) une phrase de prudence. Pas de titres, pas de listes a puces, "
    "du texte fluide. Ne promets aucun resultat."
)


def _data_block(
    ranking_rows: list[tuple[str, float]],
    signals: list[PairSignal],
    families: dict[str, FamilyResult],
    top_n: int,
) -> str:
    lines: list[str] = []
    fam_on = [n for n in ("fundamental", "technical", "sentiment", "retail", "correlation")
              if families.get(n) and families[n].available]
    lines.append("Familles actives : " + ", ".join(fam_on))
    lines.append("")
    lines.append("Force des devises (0-100, 50=neutre) :")
    lines.append("  " + " ; ".join(f"{c}={s:.0f}" for c, s in ranking_rows))
    lines.append("")
    lines.append("Signaux de confluence (les plus forts) :")
    actionable = [s for s in signals if s.decision != "WAIT"][:top_n]
    rows = actionable or signals[:top_n]
    cot = families.get("sentiment")
    ret = families.get("retail")
    for s in rows:
        contribs = ", ".join(f"{k}={v:+.0f}" for k, v in s.contributions.items())
        extra = []
        if cot and cot.available:
            c = cot.details.get("cot", {})
            b = c.get(s.base, {}).get("net_pct")
            q = c.get(s.quote, {}).get("net_pct")
            if b is not None and q is not None:
                extra.append(f"COT {s.base} net {b:+.0f}% vs {s.quote} net {q:+.0f}%")
        if ret and ret.available:
            lp = ret.details.get("long_pct", {}).get(s.pair, {}).get("long_pct")
            if lp is not None:
                extra.append(f"{lp:.0f}% des particuliers longs {s.pair} (contrarien)")
        extra_txt = (" | " + " ; ".join(extra)) if extra else ""
        lines.append(
            f"  {_FR.get(s.decision, s.decision)} {s.pair} : net {s.net:+.0f}, "
            f"confiance {s.confidence:.0f}%, {s.agree}/{s.families} familles ; "
            f"contributions [{contribs}]{extra_txt}"
        )
    return "\n".join(lines)


def _anthropic_note(prompt: str, model: str) -> str | None:
    try:
        import anthropic  # lazy import
    except ImportError:
        return None
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=model,
            max_tokens=cfg.LLM_MAX_TOKENS,
            system=_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        if getattr(resp, "stop_reason", None) == "refusal":
            return None
        parts = [getattr(b, "text", "") for b in resp.content
                 if getattr(b, "type", None) == "text"]
        text = "".join(parts).strip()
        return text or None
    except Exception as e:
        print(f"  [llm] Anthropic indisponible: {e}")
        return None


def _openai_note(prompt: str, model: str) -> str | None:
    try:
        from openai import OpenAI  # lazy import
    except ImportError:
        return None
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    messages: Any = [
        {"role": "system", "content": _SYSTEM},
        {"role": "user", "content": prompt},
    ]

    def _call(token_kwarg: str):
        client = OpenAI(api_key=api_key)
        return client.chat.completions.create(  # type: ignore[call-overload]
            model=model, messages=messages,
            **{token_kwarg: cfg.LLM_MAX_TOKENS},
        )

    try:
        try:
            resp = _call("max_tokens")
        except Exception as e:
            # Les modeles recents (gpt-5, o-series) refusent max_tokens et
            # exigent max_completion_tokens : on retente avec ce parametre.
            if "max_tokens" in str(e) or "max_completion_tokens" in str(e):
                resp = _call("max_completion_tokens")
            else:
                raise
        text = (resp.choices[0].message.content or "").strip()
        return text or None
    except Exception as e:
        print(f"  [llm] OpenAI indisponible: {e}")
        return None


def desk_note(
    ranking_rows: list[tuple[str, float]],
    signals: list[PairSignal],
    families: dict[str, FamilyResult],
) -> str | None:
    """Retourne une note du desk redigee par le LLM, ou None (repli gabarit)."""
    provider = os.getenv("FIOS_LLM_PROVIDER", cfg.LLM_PROVIDER_DEFAULT).lower()
    if provider == "off":
        return None
    prompt = _data_block(ranking_rows, signals, families, cfg.LLM_TOP_SIGNALS)
    override_model = os.getenv("FIOS_LLM_MODEL")

    if provider == "openai":
        return _openai_note(prompt, override_model or cfg.LLM_OPENAI_MODEL)

    # Defaut : Anthropic, avec bascule OpenAI si indisponible.
    note = _anthropic_note(prompt, override_model or cfg.LLM_ANTHROPIC_MODEL)
    if note is None:
        note = _openai_note(prompt, cfg.LLM_OPENAI_MODEL)
    return note
