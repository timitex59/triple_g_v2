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
    """Etat global depuis le momentum SPX / VIX (deja calcule par correlations)."""
    out = {"risk": "n/d", "vol": "n/d"}
    if not corr or not corr.available:
        return out
    mom = corr.details.get("momentum_pct", {})
    spx = mom.get("SPX")
    vix = mom.get("VIX")
    if spx is not None and vix is not None:
        if spx > 0 and vix < 0:
            out["risk"] = "RISK-ON"
        elif spx < 0 and vix > 0:
            out["risk"] = "RISK-OFF"
        else:
            out["risk"] = "MIXTE"
    if vix is not None:
        out["vol"] = "en hausse" if vix > 5 else ("en baisse" if vix < -5 else "normale")
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
