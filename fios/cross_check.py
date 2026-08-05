#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Croisement FULL ALIGNMENT x FIOS (confluence inter-systemes).

Lit le sidecar `full_alignment_index.json` produit par
renko_full_alignment_29pairs.py (force devises via index DXY/EXY/... +
alignement Renko M/W/D) et le confronte a la force composite de FIOS.

Deux methodologies independantes : quand elles pointent la meme devise dans le
meme sens, c'est une confluence a plus haute conviction. Quand elles divergent,
c'est un signal de prudence.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pytz

from . import config as cfg

SIDECAR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "full_alignment_index.json")
PARIS = pytz.timezone("Europe/Paris")


def _fresh(payload: dict, today: str) -> bool:
    if payload.get("paris_date") == today:
        return True
    gen = payload.get("generated_at")
    if not gen:
        return False
    try:
        dt = datetime.fromisoformat(gen)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(PARIS).strftime("%Y-%m-%d") == today
    except Exception:
        return False


def load_align() -> dict | None:
    """Retourne le payload du sidecar (clés 'currencies' et 'pairs') s'il existe
    et date d'aujourd'hui (heure de Paris), sinon None."""
    try:
        with open(SIDECAR, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return None
    today = datetime.now(PARIS).strftime("%Y-%m-%d")
    if not _fresh(payload, today):
        return None
    if not isinstance(payload.get("currencies"), dict) or not payload["currencies"]:
        return None
    return payload


def _px_tag(a: dict) -> str:
    def s(v):
        return "+" if v == 1 else ("-" if v == -1 else "0")
    return f"M{s(a.get('px_m'))} W{s(a.get('px_w'))} D{s(a.get('px_d'))}"


def _align_score(a: dict) -> float:
    """Score pondere M/W/D dans [-1, 1] (poids 3/2/1 comme le scanner)."""
    m = a.get("px_m") or 0
    w = a.get("px_w") or 0
    d = a.get("px_d") or 0
    return (m * 3 + w * 2 + d * 1) / 6.0


def confluence(composites: dict[str, dict], align: dict) -> list[dict]:
    recs: list[dict] = []
    for cur in cfg.CURRENCIES:
        comp = composites.get(cur)
        a = align.get(cur)
        if not comp or not a:
            continue
        force = float(comp["composite"])
        ascore = _align_score(a)
        fios_dir = 1 if force > 52 else (-1 if force < 48 else 0)
        align_dir = 1 if ascore > 0 else (-1 if ascore < 0 else 0)
        if fios_dir != 0 and fios_dir == align_dir:
            verdict = "bull" if fios_dir > 0 else "bear"
        elif fios_dir != 0 and align_dir != 0 and fios_dir != align_dir:
            verdict = "divergence"
        else:
            verdict = "neutre"
        # Divergence intraday : la structure M/W/D et la variation du jour vont
        # en sens opposes. "repli" = structure haussiere mais jour en baisse
        # (souvent un pullback) ; "rebond" = structure baissiere mais jour en
        # hausse (souvent un rebond a vendre).
        chg = a.get("daily_chg")
        intra = None
        if isinstance(chg, (int, float)) and align_dir != 0:
            if align_dir > 0 and chg < 0:
                intra = "repli"
            elif align_dir < 0 and chg > 0:
                intra = "rebond"
        recs.append({
            "currency": cur,
            "force": round(force, 0),
            "align_score": round(ascore, 2),
            "tag": _px_tag(a),
            "daily_chg": chg,
            "verdict": verdict,
            "intra": intra,
            "strength": round(abs(force - 50) + abs(ascore) * 50, 1),
        })
    return recs


def build_section(composites: dict[str, dict], payload: dict | None) -> list[str]:
    if not payload:
        return []
    recs = confluence(composites, payload.get("currencies", {}))

    def _num(r) -> bool:
        return isinstance(r["daily_chg"], (int, float))

    # STRICT : une devise n'est retenue que si TOUT est aligne — force FIOS,
    # alignement RENKO M/W/D et variation du jour dans le meme sens.
    bulls = sorted([r for r in recs if r["verdict"] == "bull" and _num(r) and r["daily_chg"] > 0],
                   key=lambda r: r["strength"], reverse=True)
    bears = sorted([r for r in recs if r["verdict"] == "bear" and _num(r) and r["daily_chg"] < 0],
                   key=lambda r: r["strength"], reverse=True)
    if not bulls and not bears:
        return []

    # Exclus : contradictions (retires de la liste).
    div_fa = [r["currency"] for r in recs if r["verdict"] == "divergence"]
    div_intra = [r["currency"] for r in recs
                 if r.get("intra") and r["verdict"] != "divergence"]

    lines = ["🔀 Confluence FULL ALIGN × FIOS (stricte)"]

    def _chg(r):
        c = r["daily_chg"]
        return f" ({c:+.2f}%)" if isinstance(c, (int, float)) else ""

    for r in bulls:
        lines.append(f"🟢 {r['currency']}  FIOS {r['force']:.0f} · Align {r['tag']}{_chg(r)}")
    for r in bears:
        lines.append(f"🔴 {r['currency']}  FIOS {r['force']:.0f} · Align {r['tag']}{_chg(r)}")

    excl = []
    if div_fa:
        excl.append(f"{', '.join(div_fa)} (FIOS×Align)")
    if div_intra:
        excl.append(f"{', '.join(div_intra)} (jour≠structure)")
    if excl:
        lines.append("🚫 Exclus : " + " · ".join(excl))
    if bulls and bears:
        lines.append(f"→ Paire confluente : ACHAT {bulls[0]['currency']}{bears[0]['currency']}")
    return lines


def pair_confluence(composites: dict[str, dict], pairs: dict) -> list[dict]:
    """Meme analyse au niveau paire : direction FIOS (force base - force quote),
    alignement RENKO M/W/D de la paire, variation du jour de la paire."""
    recs: list[dict] = []
    for name, a in pairs.items():
        if len(name) != 6:
            continue
        base, quote = name[:3], name[3:]
        cb, cq = composites.get(base), composites.get(quote)
        if not cb or not cq:
            continue
        fios_diff = float(cb["composite"]) - float(cq["composite"])
        ascore = _align_score(a)
        chg = a.get("daily_chg")
        fios_dir = 1 if fios_diff > 2 else (-1 if fios_diff < -2 else 0)
        align_dir = 1 if ascore > 0 else (-1 if ascore < 0 else 0)
        daily_dir = (1 if isinstance(chg, (int, float)) and chg > 0
                     else (-1 if isinstance(chg, (int, float)) and chg < 0 else 0))
        recs.append({
            "pair": name, "base": base, "quote": quote,
            "fios_diff": round(fios_diff, 0), "tag": _px_tag(a), "daily_chg": chg,
            "fios_dir": fios_dir, "align_dir": align_dir, "daily_dir": daily_dir,
            "strength": round(abs(fios_diff) + abs(ascore) * 50, 1),
        })
    return recs


def build_pairs_section(composites: dict[str, dict], payload: dict | None) -> list[str]:
    if not payload:
        return []
    pairs = payload.get("pairs") or {}
    if not pairs:
        return []
    recs = pair_confluence(composites, pairs)
    # STRICT : FIOS, alignement RENKO et variation du jour tous dans le meme sens.
    buys = sorted([r for r in recs if r["align_dir"] > 0 and r["fios_dir"] > 0 and r["daily_dir"] > 0],
                  key=lambda r: r["strength"], reverse=True)
    sells = sorted([r for r in recs if r["align_dir"] < 0 and r["fios_dir"] < 0 and r["daily_dir"] < 0],
                   key=lambda r: r["strength"], reverse=True)

    lines = ["🔀 Confluence PAIRES (stricte)"]

    def _chg(r):
        c = r["daily_chg"]
        return f" ({c:+.2f}%)" if isinstance(c, (int, float)) else ""

    if not buys and not sells:
        lines.append("Aucune paire strictement alignée aujourd'hui.")
        return lines
    for r in buys:
        lines.append(f"🟢 ACHAT {r['pair']} · Align {r['tag']} · FIOS {r['fios_diff']:+.0f}{_chg(r)}")
    for r in sells:
        lines.append(f"🔴 VENTE {r['pair']} · Align {r['tag']} · FIOS {r['fios_diff']:+.0f}{_chg(r)}")
    return lines
