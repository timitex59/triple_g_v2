#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backtest historique de la famille TECHNIQUE (donnees TradingView).

La technique est la seule famille avec un historique complet et rejouable : COT
est hebdomadaire et non archive ici, Myfxbook n'a pas d'historique, FRED est
mensuel. On isole donc l'edge technique — la question "le score technique
a-t-il un pouvoir predictif ?" — paire par paire.

Pour chaque paire, on parcourt les bougies (BT_TF) : a chaque barre on recalcule
le score technique AVEC les seules donnees jusqu'a cette barre (pas de fuite du
futur). Quand |score| depasse le seuil, on ouvre dans le sens du score et on
denoue via le moteur TP/SL en ATR. Pas de chevauchement : la barre suivante
consideree est celle qui suit la sortie.
"""

from __future__ import annotations

from .. import config as cfg
from .. import tv_feed
from ..adapters.technical import _tf_score
from .engine import resolve_forward


def backtest_pair(pair: dict, verbose: bool = False) -> list[dict]:
    interval, _ = cfg.TIMEFRAMES[cfg.BT_TF]
    df = tv_feed.fetch(pair["tv"], interval, cfg.BT_HISTORY_BARS)
    if df is None or len(df) < 80:
        if verbose:
            print(f"  [bt] {pair['name']}: pas assez de donnees")
        return []

    trades: list[dict] = []
    n = len(df)
    i = 60  # warmup minimal pour les EMA/RSI/ADX
    while i < n - 1:
        score = _tf_score(df.iloc[: i + 1])
        if score is None or abs(score) < cfg.BT_ENTRY_THRESHOLD:
            i += 1
            continue
        direction = 1 if score > 0 else -1
        res = resolve_forward(df, i, direction)
        if res is None:
            i += 1
            continue
        res.update({
            "pair": pair["name"],
            "base": pair["base"],
            "quote": pair["quote"],
            "direction": "BUY" if direction > 0 else "SELL",
            "score": round(score, 3),
        })
        trades.append(res)
        i = res["exit_idx"] + 1  # pas de chevauchement

    if verbose:
        print(f"  [bt] {pair['name']}: {len(trades)} trades")
    return trades


def backtest_all(pairs: list[dict] | None = None, verbose: bool = False) -> list[dict]:
    pairs = pairs or cfg.PAIRS
    all_trades: list[dict] = []
    for p in pairs:
        all_trades.extend(backtest_pair(p, verbose=verbose))
    return all_trades
