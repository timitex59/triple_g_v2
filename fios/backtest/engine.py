#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Moteur de denouement (walk-forward TP/SL en ATR).

Coeur commun au backtest historique et au denouement du journal live. A partir
d'une entree (bougie d'entree, direction), on pose SL et TP en multiples d'ATR
et on deroule les bougies suivantes :
  - direction longue : SL touche si low <= SL ; TP touche si high >= TP
  - direction courte : SL touche si high >= SL ; TP touche si low <= TP
SL prioritaire si SL et TP sont dans la meme bougie (hypothese conservatrice).
Si ni l'un ni l'autre avant l'horizon, sortie a la cloture -> R realise.

R (multiple de risque) : +TP_mult si gagnant sur TP, -SL_mult si perdant sur SL,
sinon (timeout) le mouvement realise divise par l'ATR d'entree.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from .. import config as cfg
from .. import tv_feed
from ..indicators import atr as atr_fn


def _atr_at(df: pd.DataFrame, idx: int, length: int) -> float:
    a = atr_fn(df["High"].iloc[: idx + 1], df["Low"].iloc[: idx + 1],
              df["Close"].iloc[: idx + 1], length)
    try:
        return float(a.iloc[-1])
    except Exception:
        return 0.0


def _iso(ts) -> str:
    try:
        return pd.Timestamp(ts).tz_convert("UTC").isoformat()
    except Exception:
        return str(ts)


def _mk(outcome: str, reason: str, entry_price: float, exit_price: float,
        df: pd.DataFrame, entry_idx: int, exit_idx: int, r: float) -> dict:
    return {
        "outcome": outcome,               # WIN | LOSS
        "exit_reason": reason,            # TP | SL | TIMEOUT
        "entry_price": round(entry_price, 6),
        "exit_price": round(exit_price, 6),
        "entry_time": _iso(df.index[entry_idx]),
        "exit_time": _iso(df.index[exit_idx]),
        "entry_idx": entry_idx,
        "exit_idx": exit_idx,
        "bars_held": exit_idx - entry_idx,
        "r": round(float(r), 3),
    }


def resolve_forward(
    df: pd.DataFrame, entry_idx: int, direction: int,
    sl_mult: float | None = None, tp_mult: float | None = None,
    max_bars: int | None = None, atr_len: int | None = None,
) -> dict | None:
    """Denoue une entree a partir de la bougie entry_idx. direction: +1 long,
    -1 short. Les parametres None sont lus dans config AU MOMENT DE L'APPEL
    (permet les overrides CLI). Retourne None si l'ATR est nul ou s'il n'y a
    aucune bougie apres l'entree (signal trop recent -> OPEN cote appelant)."""
    sl_mult = cfg.BT_SL_ATR if sl_mult is None else sl_mult
    tp_mult = cfg.BT_TP_ATR if tp_mult is None else tp_mult
    max_bars = cfg.BT_MAX_BARS if max_bars is None else max_bars
    atr_len = cfg.BT_ATR_LEN if atr_len is None else atr_len
    n = len(df)
    if entry_idx < 0 or entry_idx >= n - 1:
        return None
    a = _atr_at(df, entry_idx, atr_len)
    if a <= 0:
        return None
    entry_price = float(df["Close"].iloc[entry_idx])
    risk = a * sl_mult
    sl = entry_price - direction * risk
    tp = entry_price + direction * a * tp_mult
    last_j = min(entry_idx + max_bars, n - 1)

    # Excursions max favorable / adverse (en R) avant la sortie.
    max_fav = 0.0
    max_adv = 0.0

    def _final(res: dict) -> dict:
        res["mfe_r"] = round(max_fav / risk, 2) if risk > 0 else 0.0
        res["mae_r"] = round(-max_adv / risk, 2) if risk > 0 else 0.0
        return res

    for j in range(entry_idx + 1, last_j + 1):
        high = float(df["High"].iloc[j])
        low = float(df["Low"].iloc[j])
        if direction > 0:
            max_fav = max(max_fav, high - entry_price)
            max_adv = max(max_adv, entry_price - low)
            if low <= sl:
                return _final(_mk("LOSS", "SL", entry_price, sl, df, entry_idx, j, -sl_mult))
            if high >= tp:
                return _final(_mk("WIN", "TP", entry_price, tp, df, entry_idx, j, tp_mult))
        else:
            max_fav = max(max_fav, entry_price - low)
            max_adv = max(max_adv, high - entry_price)
            if high >= sl:
                return _final(_mk("LOSS", "SL", entry_price, sl, df, entry_idx, j, -sl_mult))
            if low <= tp:
                return _final(_mk("WIN", "TP", entry_price, tp, df, entry_idx, j, tp_mult))

    exit_price = float(df["Close"].iloc[last_j])
    r = (exit_price - entry_price) * direction / a
    outcome = "WIN" if r > 0 else "LOSS"
    return _final(_mk(outcome, "TIMEOUT", entry_price, exit_price, df, entry_idx, last_j, r))


def _entry_index(df: pd.DataFrame, entry_dt: datetime) -> int | None:
    """Premiere bougie dont l'horodatage est >= entry_dt (l'entree se fait a la
    cloture de cette bougie)."""
    target = pd.Timestamp(entry_dt)
    if target.tzinfo is None:
        target = target.tz_localize("UTC")
    for i in range(len(df)):
        if df.index[i] >= target:
            return i
    return None


def resolve_signal(
    pair_tv: str, direction: int, entry_dt: datetime, tf: str = cfg.BT_TF,
) -> dict | None:
    """Denoue un signal (paire, direction, date d'entree) via TradingView.
    Retourne {"outcome": "OPEN"} si le signal est trop recent pour etre denoue,
    None si pas de donnees."""
    interval, _ = cfg.TIMEFRAMES[tf]
    df = tv_feed.fetch(pair_tv, interval, cfg.BT_HISTORY_BARS)
    if df is None or len(df) < 30:
        return None
    idx = _entry_index(df, entry_dt)
    if idx is None or idx >= len(df) - 1:
        return {"outcome": "OPEN"}
    res = resolve_forward(df, idx, direction)
    return res or {"outcome": "OPEN"}
