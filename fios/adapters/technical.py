#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Adaptateur TECHNIQUE (donnees TradingView).

Pour chaque paire, on calcule un score directionnel multi-timeframe dans
[-1, +1] (positif = base plus forte que quote), a partir de :
  - l'alignement des EMA (20/50/200) et la position du prix,
  - le biais RSI,
  - module par l'ADX (une tendance faible reduit la magnitude -> pousse au WAIT).

Les scores de paires sont ensuite decomposes en force par devise (0-100), et
les scores de paires bruts sont exposes pour la confirmation technique directe
dans le moteur de confluence.
"""

from __future__ import annotations

import pandas as pd

from .. import config as cfg
from .. import tv_feed
from ..indicators import adx, ema, rsi
from .base import FamilyResult, clamp, to_0_100


def _tf_score(df: pd.DataFrame | None) -> float | None:
    if df is None or len(df) < 60:
        return None
    close = df["Close"]
    high = df["High"]
    low = df["Low"]

    e_fast = ema(close, cfg.EMA_FAST)
    e_mid = ema(close, cfg.EMA_MID)
    have_slow = len(df) >= cfg.EMA_SLOW + 10
    e_slow = ema(close, cfg.EMA_SLOW) if have_slow else None

    price = float(close.iloc[-1])
    align = 0.0
    n = 0
    align += 1.0 if price > float(e_fast.iloc[-1]) else -1.0
    n += 1
    align += 1.0 if float(e_fast.iloc[-1]) > float(e_mid.iloc[-1]) else -1.0
    n += 1
    if e_slow is not None:
        align += 1.0 if float(e_mid.iloc[-1]) > float(e_slow.iloc[-1]) else -1.0
        n += 1
        align += 1.0 if price > float(e_slow.iloc[-1]) else -1.0
        n += 1
    trend = align / n  # -1..1

    r = float(rsi(close, cfg.RSI_LEN).iloc[-1])
    rsi_bias = clamp((r - 50.0) / 30.0, -1.0, 1.0)

    a = float(adx(high, low, close, cfg.ADX_LEN).iloc[-1])
    adx_w = clamp(a / 30.0, 0.25, 1.0)

    raw = 0.7 * trend + 0.3 * rsi_bias
    return clamp(raw * adx_w, -1.0, 1.0)


def _pair_score(tv_symbol: str) -> tuple[float | None, dict[str, float]]:
    tfs: dict[str, float] = {}
    for code, (interval, n) in cfg.TIMEFRAMES.items():
        df = tv_feed.fetch(tv_symbol, interval, n)
        s = _tf_score(df)
        if s is not None:
            tfs[code] = round(s, 4)
    if not tfs:
        return None, {}
    wsum = sum(cfg.TF_WEIGHTS[c] for c in tfs)
    comp = sum(cfg.TF_WEIGHTS[c] * tfs[c] for c in tfs) / wsum
    return comp, tfs


def run(pairs: list[dict] | None = None, verbose: bool = False) -> FamilyResult:
    pairs = pairs or cfg.PAIRS
    pair_scores: dict[str, dict] = {}
    for p in pairs:
        comp, tfs = _pair_score(p["tv"])
        if comp is None:
            if verbose:
                print(f"  [tech] {p['name']}: pas de donnees")
            continue
        pair_scores[p["name"]] = {"score": round(comp, 4), "tfs": tfs}
        if verbose:
            print(f"  [tech] {p['name']}: {comp:+.3f} {tfs}")

    # Decomposition paire -> force par devise.
    acc: dict[str, list[float]] = {c: [] for c in cfg.CURRENCIES}
    for p in pairs:
        row = pair_scores.get(p["name"])
        if not row:
            continue
        s = row["score"]
        acc[p["base"]].append(s)
        acc[p["quote"]].append(-s)

    scores: dict[str, float] = {}
    for c, vals in acc.items():
        if vals:
            scores[c] = round(to_0_100(sum(vals) / len(vals)), 1)

    available = len(scores) > 0
    note = "" if available else "aucune donnee TradingView recuperee"
    return FamilyResult(
        name="technical",
        available=available,
        scores=scores,
        details={"pairs": {k: v["score"] for k, v in pair_scores.items()},
                 "tf_detail": pair_scores},
        note=note,
    )
