#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Indicateurs techniques en pandas pur (aucune dependance TA-Lib).

Toutes les fonctions prennent des Series/DataFrame OHLC et renvoient des Series
alignees sur l'index d'entree.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def ema(series: pd.Series, length: int) -> pd.Series:
    return pd.Series(series.ewm(span=length, adjust=False).mean())


def rsi(close: pd.Series, length: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    # Lissage de Wilder.
    avg_gain = gain.ewm(alpha=1.0 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    out = pd.Series(100.0 - (100.0 / (1.0 + rs)))
    return out.fillna(50.0)


def atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return pd.Series(tr.ewm(alpha=1.0 / length, adjust=False).mean())


def adx(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    up = high.diff()
    down = -low.diff()
    plus_dm = ((up > down) & (up > 0)) * up
    minus_dm = ((down > up) & (down > 0)) * down

    tr = atr(high, low, close, length)
    tr = tr.replace(0.0, np.nan)
    plus_di = 100.0 * plus_dm.ewm(alpha=1.0 / length, adjust=False).mean() / tr
    minus_di = 100.0 * minus_dm.ewm(alpha=1.0 / length, adjust=False).mean() / tr

    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    return dx.ewm(alpha=1.0 / length, adjust=False).mean().fillna(0.0)
