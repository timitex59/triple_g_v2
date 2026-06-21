#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
combo_pipeline.py

Fusionne en UN SEUL message Telegram les resultats de:
  - nasdaq_sector_pipeline.py  -> TOP 3 STOCK (Nasdaq-100) + TOP 3 ETF UCITS
  - middle_pipeline.py         -> TOP 3 MID-CAP TECH (vs IWR)

Il NE recalcule rien: il lit les rapports JSON deja produits par les deux
scripts dans le meme run CI (lus seulement s'ils datent d'aujourd'hui), en
reconstruit les objets et reutilise les classements d'origine -> meme TOP que
les messages separes. Envoi une seule fois par jour (anti-doublon).

Les deux pipelines tournent donc avec --no-telegram (calcul + rapports + sidecar
UCITS pour ETF_V3 inchanges); seul combo envoie le message fusionne.

Usage:
    python combo_pipeline.py                # construit + envoie (si >=20h non requis)
    python combo_pipeline.py --no-telegram  # affichage seul
    python combo_pipeline.py --force        # ignore l'anti-doublon
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import pytz

import middle_pipeline as mid
import nasdaq_sector_pipeline as nsp
from nasdaq_sector_pipeline import (
    AssetMetrics, RenkoConfirmation, format_pct, retained_assets_ranked, send_telegram,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NAS_JSON = nsp.REPORT_JSON
MID_JSON = mid.REPORT_JSON
SEND_STATE = os.path.join(SCRIPT_DIR, "combo_pipeline_send.json")
PARIS = pytz.timezone("Europe/Paris")


def _load(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _fresh(report: dict, today: str) -> bool:
    """Le rapport est-il du jour ? Compare en DATE PARIS (generated_at est en UTC;
    une simple troncature [:10] casse autour de minuit Paris)."""
    gen = report.get("generated_at", "")
    if not gen:
        return False
    try:
        dt = datetime.fromisoformat(gen)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(PARIS).strftime("%Y-%m-%d") == today
    except Exception:
        return gen[:10] == today


def _stocks(rep: dict) -> list[AssetMetrics]:
    return [AssetMetrics(**m) for m in rep.get("stocks", [])]


def _renko(rep: dict) -> dict:
    return {k: RenkoConfirmation(**v) for k, v in rep.get("renko", {}).items()}


# Noms de secteurs abreges pour la ligne 🔥.
SECTOR_ABBR = {
    "Semiconductors / AI infrastructure": "Semis",
    "Cybersecurity": "Cyber",
    "Cloud / Data software": "Cloud",
    "AI platforms / Mega-cap tech": "IA",
    "Consumer internet / E-commerce": "Internet",
    "Healthcare / Biotech": "Santé",
    "Industrials / Automation": "Indus",
    "Utilities / Power": "Énergie",
    # Sous-themes mid-cap (deja courts; on abrege les plus longs).
    "Distributors": "Distrib",
    "IT / Internet": "IT/Net",
}


def _top_themes(rep: dict, n: int = 2) -> list[str]:
    th = sorted(rep.get("themes", []), key=lambda s: s.get("final_score", 0), reverse=True)
    return [SECTOR_ABBR.get(t["theme"], str(t["theme"]).split(" / ")[0]) for t in th[:n]]


def nasdaq_top_stocks(rep: dict, n: int = 3) -> list[tuple[str, float]]:
    """Meme classement que nasdaq build_telegram_summary: validees Renko d'abord,
    puis repli par force relative."""
    stocks = _stocks(rep)
    etfs = [AssetMetrics(**m) for m in rep.get("etfs", [])]
    renko = _renko(rep)
    ranked = [(r, c, a) for r, c, a in retained_assets_ranked(renko, stocks, etfs)
              if c.asset_type == "STOCK"]
    fallback = sorted([m for m in stocks if m.rel_63d_vs_qqq is not None],
                      key=lambda m: (m.rel_63d_vs_qqq or 0.0, m.score), reverse=True)
    out: list[tuple[str, float]] = []
    used: set[str] = set()
    for _, c, a in ranked:
        if len(out) >= n:
            break
        if a is None or c.ticker in used:
            continue
        out.append((c.ticker, a.rel_63d_vs_qqq)); used.add(c.ticker)
    for a in fallback:
        if len(out) >= n:
            break
        if a.ticker in used:
            continue
        out.append((a.ticker, a.rel_63d_vs_qqq)); used.add(a.ticker)
    return out


def nasdaq_ucits(rep: dict, n: int = 3) -> list[tuple[str, float]]:
    eu = [AssetMetrics(**m) for m in rep.get("europe_etfs", [])]
    ranked = sorted([m for m in eu if m.rel_63d_vs_qqq is not None],
                    key=lambda m: (m.rel_63d_vs_qqq or 0.0, m.score), reverse=True)
    return [(m.ticker, m.rel_63d_vs_qqq) for m in ranked[:n]]


def mid_top_stocks(rep: dict, n: int = 3) -> list[tuple[str, float]]:
    """Meme classement unifie que middle_pipeline (validees Renko d'abord)."""
    ranking = mid.unified_ranking(_stocks(rep), _renko(rep))
    return [(e["ticker"], e["rel"]) for e in ranking[:n]]


def build_message(nas: dict, midr: dict) -> str | None:
    lines: list[str] = []

    ns = nasdaq_top_stocks(nas) if nas else []
    if ns:
        lines.append("📈 LARGE CAP")
        themes = _top_themes(nas)
        if themes:
            lines.append("🔥 " + ", ".join(themes))
        lines += [f"{i}. {t} ({format_pct(rel)})" for i, (t, rel) in enumerate(ns, 1)]

    ms = mid_top_stocks(midr) if midr else []
    if ms:
        if lines:
            lines.append("")
        lines.append("📈 MID-CAP TECH")
        themes = _top_themes(midr)
        if themes:
            lines.append("🔥 " + ", ".join(themes))
        lines.append("")
        lines += [f"{i}. {t} ({format_pct(rel)})" for i, (t, rel) in enumerate(ms, 1)]

    uc = nasdaq_ucits(nas) if nas else []
    if uc:
        if lines:
            lines.append("")
        lines.append("🇪🇺 TOP 3 ETF UCITS")
        lines += [f"{i}. {t} ({format_pct(rel)})" for i, (t, rel) in enumerate(uc, 1)]

    if not lines:
        return None
    stamp = datetime.now(PARIS).strftime("%Y-%m-%d %H:%M")
    lines += ["", f"⏰ {stamp} Paris"]
    return "\n".join(lines)


def main() -> int:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Message Telegram fusionne Nasdaq + Mid-cap tech.")
    ap.add_argument("--no-telegram", action="store_true", help="Affichage seul, pas d'envoi.")
    ap.add_argument("--force", action="store_true", help="Ignore l'anti-doublon journalier.")
    args = ap.parse_args()

    today = datetime.now(PARIS).strftime("%Y-%m-%d")
    nas = _load(NAS_JSON)
    midr = _load(MID_JSON)
    if not _fresh(nas, today):
        print("Rapport Nasdaq absent ou perime — section ignoree.")
        nas = {}
    if not _fresh(midr, today):
        print("Rapport Mid-cap absent ou perime — section ignoree.")
        midr = {}

    message = build_message(nas, midr)
    if message is None:
        print("Aucune donnee fraiche — aucun message.")
        return 0
    print("\n" + message + "\n")

    already = _load(SEND_STATE).get("last_sent_date") == today
    if args.no_telegram:
        print("Telegram desactive (--no-telegram).")
    elif already and not args.force:
        print(f"Deja envoye aujourd'hui ({today}) — 1x/jour, pas de renvoi.")
    else:
        send_telegram(message)
        try:
            with open(SEND_STATE, "w", encoding="utf-8") as f:
                json.dump({"last_sent_date": today}, f)
        except Exception:
            pass
        print("Telegram: message fusionne envoye.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
