#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Adaptateur SENTIMENT RETAIL — OANDA + Myfxbook (signal CONTRARIEN).

Les particuliers sont statistiquement du mauvais cote aux extremes : quand une
ecrasante majorite est acheteuse d'une paire, c'est souvent un signal de vente.
On agrege donc le % long retail par paire depuis deux sources independantes :

  - OANDA position book (API v20, compte demo gratuit, token OANDA_API_TOKEN)
  - Myfxbook Community Outlook (compte gratuit, MYFXBOOK_EMAIL/PASSWORD)

Pour chaque paire : avg_long = moyenne des sources disponibles.
Score contrarien de la paire = -((avg_long/100 - 0.5) * 2)  dans [-1, 1]
(positif = favorable a la BASE, car retail vendeur de la base).

Puis decomposition en force par devise (0-100), comme la technique. Les deux
sources sont optionnelles : sans identifiants, la famille est simplement
indisponible et son poids est redistribue.
"""

from __future__ import annotations

import os

import requests

from .. import config as cfg
from .base import FamilyResult, clamp, to_0_100


# --- OANDA -----------------------------------------------------------------

def _oanda_long_pct(pair: dict, token: str, host: str) -> float | None:
    instrument = f"{pair['base']}_{pair['quote']}"
    url = f"{host}/v3/instruments/{instrument}/positionBook"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if r.status_code != 200:
            return None
        buckets = r.json().get("positionBook", {}).get("buckets", [])
        long_pct = sum(float(b.get("longCountPercent", 0) or 0) for b in buckets)
        short_pct = sum(float(b.get("shortCountPercent", 0) or 0) for b in buckets)
        total = long_pct + short_pct
        if total <= 0:
            return None
        return 100.0 * long_pct / total
    except Exception:
        return None


def _oanda_all(pairs: list[dict], verbose: bool) -> dict[str, float]:
    token = os.getenv("OANDA_API_TOKEN")
    if not token:
        return {}
    host = cfg.OANDA_HOSTS.get(os.getenv("OANDA_ENV", "practice"), cfg.OANDA_HOSTS["practice"])
    out: dict[str, float] = {}
    for p in pairs:
        lp = _oanda_long_pct(p, token, host)
        if lp is not None:
            out[p["name"]] = lp
    if verbose and out:
        print(f"  [retail/oanda] {len(out)} paires")
    return out


# --- Myfxbook --------------------------------------------------------------

def _myfxbook_all(verbose: bool) -> dict[str, float]:
    email = os.getenv("MYFXBOOK_EMAIL")
    password = os.getenv("MYFXBOOK_PASSWORD")
    if not email or not password:
        return {}
    session = None
    out: dict[str, float] = {}
    try:
        r = requests.get(cfg.MYFXBOOK_LOGIN_URL,
                         params={"email": email, "password": password}, timeout=15)
        data = r.json()
        if data.get("error") or not data.get("session"):
            return {}
        session = data["session"]
        # Le token de session est DEJA url-encode : le repasser via params le
        # double-encoderait ("Invalid session"). On l'injecte brut dans l'URL.
        r2 = requests.get(f"{cfg.MYFXBOOK_OUTLOOK_URL}?session={session}", timeout=20)
        outlook = r2.json()
        for sym in outlook.get("symbols", []):
            name = str(sym.get("name", "")).upper()
            lp = sym.get("longPercentage")
            if name and lp is not None:
                try:
                    out[name] = float(lp)
                except (TypeError, ValueError):
                    continue
        if verbose and out:
            print(f"  [retail/myfxbook] {len(out)} paires")
    except Exception:
        return out
    finally:
        if session:
            try:
                requests.get(f"{cfg.MYFXBOOK_LOGOUT_URL}?session={session}", timeout=10)
            except Exception:
                pass
    return out


# --- Agregation ------------------------------------------------------------

def run(pairs: list[dict] | None = None, verbose: bool = False) -> FamilyResult:
    pairs = pairs or cfg.PAIRS
    oanda = _oanda_all(pairs, verbose)
    myfx = _myfxbook_all(verbose)

    if not oanda and not myfx:
        return FamilyResult(
            "retail", False, {}, {},
            note="OANDA_API_TOKEN et MYFXBOOK_* absents (.env) — retail ignore",
        )

    # Score contrarien par paire.
    pair_scores: dict[str, float] = {}
    detail: dict[str, dict] = {}
    for p in pairs:
        name = p["name"]
        longs = [v for v in (oanda.get(name), myfx.get(name)) if v is not None]
        if not longs:
            continue
        avg_long = sum(longs) / len(longs)
        contrarian = clamp(-((avg_long / 100.0 - 0.5) * 2.0), -1.0, 1.0)
        pair_scores[name] = round(contrarian, 4)
        detail[name] = {
            "long_pct": round(avg_long, 1),
            "oanda": round(oanda[name], 1) if name in oanda else None,
            "myfxbook": round(myfx[name], 1) if name in myfx else None,
        }

    if not pair_scores:
        return FamilyResult("retail", False, {}, {}, note="aucune paire retail exploitable")

    # Decomposition paire -> devise.
    acc: dict[str, list[float]] = {c: [] for c in cfg.CURRENCIES}
    for p in pairs:
        s = pair_scores.get(p["name"])
        if s is None:
            continue
        acc[p["base"]].append(s)
        acc[p["quote"]].append(-s)
    scores = {c: round(to_0_100(sum(v) / len(v)), 1) for c, v in acc.items() if v}

    sources = []
    if oanda:
        sources.append("OANDA")
    if myfx:
        sources.append("Myfxbook")
    return FamilyResult(
        name="retail",
        available=True,
        scores=scores,
        details={"pairs": pair_scores, "long_pct": detail},
        note=f"contrarien ({'+'.join(sources)})",
    )
