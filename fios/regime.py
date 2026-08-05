#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Contexte de marche : etat global (risk-on/off + volatilite) et regime par paire
(tendance / range). Aucune donnee nouvelle inventee — on reutilise le momentum
des instruments deja calcule par l'adaptateur correlations et l'ADX(D) des
bougies deja telechargees par l'adaptateur technique.

Un signal n'a pas la meme valeur selon le contexte : un SELL en range n'est pas
un SELL en tendance installee.
"""

from __future__ import annotations

from . import config as cfg
from . import tv_feed
from .adapters.base import FamilyResult
from .indicators import adx as adx_fn


def market_state(corr: FamilyResult | None) -> dict:
    """Etat global (risk-on/off) depuis le momentum des actifs de risque deja
    calcule par correlations : SPX + BTC (risque), or (refuge, en sens inverse)."""
    out = {"risk": "n/d", "vol": "n/d"}
    if not corr or not corr.available:
        return out
    mom = corr.details.get("momentum_pct", {})
    votes = []
    for name, sign in (("SPX", 1), ("BTC", 1), ("GOLD", -1)):
        v = mom.get(name)
        if v is not None and v != 0:
            votes.append(sign * (1 if v > 0 else -1))
    if votes:
        s = sum(votes)
        out["risk"] = "RISK-ON" if s > 0 else ("RISK-OFF" if s < 0 else "MIXTE")
    return out


def pair_regime(pair_tv: str, tf: str = "D") -> str:
    """TENDANCE / RANGE / TRANSITION selon l'ADX(D) de la paire."""
    interval, _ = cfg.TIMEFRAMES.get(tf, cfg.TIMEFRAMES["D"])
    df = tv_feed.fetch(pair_tv, interval, 120)
    if df is None or len(df) < 30:
        return "n/d"
    try:
        a = float(adx_fn(df["High"], df["Low"], df["Close"], cfg.ADX_LEN).iloc[-1])
    except Exception:
        return "n/d"
    if a >= 25:
        return "TENDANCE"
    if a < 18:
        return "RANGE"
    return "TRANSITION"


def regimes_for(signals, pairs_by_name: dict[str, dict], top_n: int) -> dict[str, str]:
    out: dict[str, str] = {}
    for s in [s for s in signals if s.decision != "WAIT"][:top_n]:
        p = pairs_by_name.get(s.pair)
        if p:
            out[s.pair] = pair_regime(p["tv"])
    return out
