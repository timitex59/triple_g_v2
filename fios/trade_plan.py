#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plan de trade par signal (entree / SL / TP / RR) via ATR.

Transforme une reco directionnelle en plan executable, avec le meme moteur ATR
que le backtest (coherence entre ce qui est propose et ce qui est teste) :
  - risque = SL_ATR x ATR(D)  (stop a 1R)
  - TP1 = 1R, TP2 = 2R  -> RR 2.0
Le stop sert aussi de niveau d'invalidation.

Reutilise le cache TradingView de l'adaptateur technique (memes bougies D deja
telechargees) : cout reseau quasi nul.
"""

from __future__ import annotations

from . import config as cfg
from . import tv_feed
from .indicators import atr as atr_fn


def _fmt(price: float) -> str:
    # Paires en JPY (~100+) : 3 decimales ; autres (~1) : 5 decimales.
    return f"{price:.3f}" if abs(price) >= 20 else f"{price:.5f}"


def compute(pair: dict, direction: int, tf: str = "D") -> dict | None:
    """direction : +1 (ACHAT) / -1 (VENTE). Retourne le plan ou None."""
    interval, _ = cfg.TIMEFRAMES.get(tf, cfg.TIMEFRAMES["D"])
    df = tv_feed.fetch(pair["tv"], interval, 120)
    if df is None or len(df) < 20:
        return None
    a = float(atr_fn(df["High"], df["Low"], df["Close"], cfg.BT_ATR_LEN).iloc[-1])
    entry = float(df["Close"].iloc[-1])
    if a <= 0:
        return None
    risk = cfg.BT_SL_ATR * a               # distance du stop = 1R
    sl = entry - direction * risk
    tp1 = entry + direction * risk * 1.0   # 1R
    tp2 = entry + direction * risk * 2.0   # 2R
    return {
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "rr": 2.0, "atr": round(a, 6),
        "entry_s": _fmt(entry), "sl_s": _fmt(sl),
        "tp1_s": _fmt(tp1), "tp2_s": _fmt(tp2),
    }


def plans_for(signals, pairs_by_name: dict[str, dict], top_n: int) -> dict[str, dict]:
    """Calcule les plans pour les signaux actionnables (BUY/SELL) du top."""
    out: dict[str, dict] = {}
    actionable = [s for s in signals if s.decision != "WAIT"][:top_n]
    for s in actionable:
        p = pairs_by_name.get(s.pair)
        if not p:
            continue
        direction = 1 if s.decision == "BUY" else -1
        plan = compute(p, direction)
        if plan:
            out[s.pair] = plan
    return out
