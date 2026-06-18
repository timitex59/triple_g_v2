#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Real-time SIGNAL + weighted Bull/Bear SCORE for the 29 instruments, mirroring
the logic of renko_forex_V16.pine:

- For each pair, build Renko bricks (ATR box size) on Monthly, Weekly and
  Daily timeframes.
- For each timeframe, compare the live price to the latest Renko brick's
  open/close (above = +1, inside = 0, below = -1), refined by the brick's
  own green/red streak ("effective bias") exactly like f_effective_bias().
- SIGNAL = BULL only if M/W/D effective biases are all +1, BEAR only if all
  -1, otherwise MIXED.
- BASE SCORE = pxM*3 + pxW*2 + pxD*1, normalized to a -100%..+100% reading.
- FINAL SCORE = base_score × chg_coeff, where chg_coeff (0..1) maps the
  magnitude of the daily CHG% to a confidence multiplier. CHG%D in the wrong
  direction or below 0.1% → coeff=0 → score=0 → pair never selected.
"""

import argparse
import bisect
import json
import os
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from ichimoku_v4 import PAIRS_29, fetch_tv_ohlc, send_telegram_message


PARIS_TZ = ZoneInfo("Europe/Paris")
SCORE_THRESHOLD = 60.0
CHG_THRESHOLD = 0.1

# Suivi historique de l'intensite de tendance (dispersion des devises majeures).
# Sert a juger le jour en RELATIF (percentile sur une fenetre glissante) plutot
# qu'avec un seuil fixe arbitraire.
STRENGTH_HISTORY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "renko_strength_history.json")
HISTORY_WINDOW = 60        # jours glissants de reference
HISTORY_CALIB_MIN = 20     # jours mini avant d'utiliser le percentile
HISTORY_MAX_DAYS = 500     # borne la taille du fichier

# Profil intraday (statique, construit par seed_intraday_profile.py sur 1 an):
# pour chaque heure depuis l'ouverture de session NY, la distribution historique du spread.
# Permet un percentile JUSTE a chaque heure (au lieu de comparer un mouvement
# partiel a des journees completes).
INTRADAY_PROFILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "renko_intraday_profile.json")
# Log vivant (auto-enrichissement): le spread de chaque heure est accumule a
# chaque run. Des qu'une heure a >= MIN_LIVE echantillons, le percentile bascule
# sur ces donnees RECENTES (le seed reste le repli, notamment pour les heures de
# nuit hors fenetre cron).
INTRADAY_LIVE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "renko_intraday_live.json")
USE_INTRADAY_LIVE_PROFILE = False  # garde l'intensite calibree sur le profil 1 an
INTRADAY_LIVE_MIN = 60     # echantillons mini pour preferer le live au seed
INTRADAY_LIVE_CAP = 300    # garde au plus N echantillons recents par heure
NY_TZ = ZoneInfo("America/New_York")
SESSION_OPEN_HOUR = 17     # 17:00 New York = ouverture journaliere OANDA

# Reference de regime recente (statique, construite par seed_regime_reference.py):
# distribution du spread quotidien sur 3 ans. Situe la PERIODE actuelle (calme /
# agitee) vs le regime recent du forex.
REGIME_REFERENCE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "renko_regime_reference.json")
REGIME_WINDOW = 30         # jours recents pour estimer le regime courant


TIMEFRAME_LABELS = {"M": "Monthly", "W": "Weekly", "D": "Daily"}
WEIGHTS = {"M": 3.0, "W": 2.0, "D": 1.0}


@dataclass
class TFState:
    px_state: int          # +1 above brick, 0 inside, -1 below
    bias: int              # effective bias (px_state refined by brick streak)
    direction: int         # latest brick direction: 1 green, -1 red, 0 none
    green_streak: int
    red_streak: int
    renko_open: float
    renko_close: float
    streak_count: int
    streak_low: float
    streak_high: float


def parse_args():
    parser = argparse.ArgumentParser(description="Real-time SIGNAL + weighted Bull/Bear SCORE for the 29 pairs (mirrors renko_forex_V15.pine).")
    parser.add_argument("--length", type=int, default=14, help="ATR length.")
    parser.add_argument("--candles", type=int, default=300, help="Number of candles fetched per symbol and timeframe.")
    parser.add_argument("--max-streak", type=int, default=50, help="Cap on the consecutive green/red brick streak count.")
    return parser.parse_args()


def atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()


def build_renko_bricks(df: pd.DataFrame, length: int) -> list[tuple[float, float, int]]:
    """Replays the close series into Renko bricks. Returns a list of
    (brick_open, brick_close, direction) in chronological order — direction
    is +1 for a green (up) brick, -1 for a red (down) brick."""
    atr_series = atr(df, length)
    box_size = float(atr_series.iloc[-1])
    if pd.isna(box_size) or box_size <= 0:
        return []

    close = df["close"].astype(float)
    anchor = float(close.iloc[0])
    direction = 0
    bricks: list[tuple[float, float, int]] = []

    for price in close.iloc[1:]:
        price = float(price)
        formed = 0
        new_direction = direction
        move = 0

        if direction == 0:
            if price >= anchor + box_size:
                move = int((price - anchor) // box_size)
                new_direction = 1
            elif price <= anchor - box_size:
                move = int((anchor - price) // box_size)
                new_direction = -1
        elif direction == 1:
            if price >= anchor + box_size:
                move = int((price - anchor) // box_size)
                new_direction = 1
            elif price <= anchor - (2.0 * box_size):
                move = int((anchor - price) // box_size) - 1
                new_direction = -1
        else:
            if price <= anchor - box_size:
                move = int((anchor - price) // box_size)
                new_direction = -1
            elif price >= anchor + (2.0 * box_size):
                move = int((price - anchor) // box_size) - 1
                new_direction = 1

        if move > 0:
            for _ in range(move):
                brick_open = anchor
                anchor = anchor + box_size if new_direction == 1 else anchor - box_size
                bricks.append((brick_open, anchor, new_direction))
            formed = move
            direction = new_direction

        if formed == 0:
            continue

    return bricks


def closed_renko_source(df: pd.DataFrame) -> pd.DataFrame:
    """Retire la derniere bougie du timeframe source: en live elle peut encore
    former une brique Renko non cloturee. Le prix H1 live reste compare ensuite
    a ces niveaux valides."""
    if len(df) <= 1:
        return df.iloc[0:0]
    return df.iloc[:-1]


def f_px_state(renko_open: float, renko_close: float, price: float) -> int:
    hi = max(renko_open, renko_close)
    lo = min(renko_open, renko_close)
    if price > hi:
        return 1
    if price < lo:
        return -1
    return 0


def f_effective_bias(px_state: int, green_streak: int, red_streak: int) -> int:
    if px_state == 1:
        return 1
    if px_state == -1:
        return -1
    if green_streak > 0 and red_streak == 0:
        return 1
    if green_streak == 0 and red_streak > 0:
        return -1
    return 0


def streaks_from_bricks(bricks: list[tuple[float, float, int]], max_bars: int) -> tuple[int, int]:
    """Counts the trailing run of consecutive same-direction bricks, capped at
    max_bars — mirrors f_green_streak_live() / f_red_streak_live() applied to
    the Renko chart's own bars (close>open = green, close<open = red)."""
    green = 0
    red = 0
    for _, _, direction in reversed(bricks):
        if direction == 1:
            if red > 0:
                break
            green += 1
        elif direction == -1:
            if green > 0:
                break
            red += 1
        else:
            break
        if green >= max_bars or red >= max_bars:
            break
    return green, red


def streak_range_from_bricks(bricks: list[tuple[float, float, int]]) -> tuple[int, float, float]:
    """Range complet du dernier streak Renko valide: toutes les briques finales
    consecutives dans le meme sens que la derniere brique."""
    direction = bricks[-1][2]
    run: list[tuple[float, float, int]] = []
    for brick in reversed(bricks):
        if brick[2] != direction:
            break
        run.append(brick)

    lows = [min(o, c) for o, c, _ in run]
    highs = [max(o, c) for o, c, _ in run]
    return len(run), min(lows), max(highs)


def h1_vs_streak_position(states: dict[str, TFState], h1_price: float | None) -> dict | None:
    """Compare le close H1 actuel au range du dernier streak Renko D/W/M.
    +1 = au-dessus du streak, -1 = en-dessous, 0 = dans le range du streak."""
    if h1_price is None:
        return None

    pos: dict[str, int] = {}
    counts = {1: 0, -1: 0, 0: 0}
    for tf in ("D", "W", "M"):
        state = states.get(tf)
        if state is None:
            continue
        if h1_price > state.streak_high:
            v = 1
        elif h1_price < state.streak_low:
            v = -1
        else:
            v = 0
        pos[tf] = v
        counts[v] += 1

    parts = []
    if counts[1]:
        parts.append(f"🟢{counts[1]}")
    if counts[-1]:
        parts.append(f"🔴{counts[-1]}")
    if counts[0]:
        parts.append(f"⚪{counts[0]}")
    return {"tag": " ".join(parts), "pos": pos, "counts": counts}


# Parabolic SAR (rapide) pour le timing 1H: start AF 0.1, increment 0.1, max 0.2.
SAR_AF_START = 0.1
SAR_AF_STEP = 0.1
SAR_AF_MAX = 0.2


def parabolic_sar(df: pd.DataFrame,
                  af_start: float = SAR_AF_START,
                  af_step: float = SAR_AF_STEP,
                  af_max: float = SAR_AF_MAX) -> dict | None:
    """Parabolic SAR classique. Renvoie directions et niveaux SAR par barre.
    trend: +1 = haussier (SAR sous le prix), -1 = baissier (SAR au-dessus).
    None si pas assez de barres."""
    open_ = [float(x) for x in df["open"].tolist()]
    high = [float(x) for x in df["high"].tolist()]
    low = [float(x) for x in df["low"].tolist()]
    n = len(high)
    if n < 3 or len(open_) != n:
        return None

    trend = [1] * n
    sar_values = [0.0] * n
    trend[0] = 1 if high[1] >= high[0] else -1
    if trend[0] == 1:
        ep, sar = high[0], low[0]
    else:
        ep, sar = low[0], high[0]
    sar_values[0] = sar
    af = af_start

    for i in range(1, n):
        sar = sar + af * (ep - sar)
        if trend[i - 1] == 1:
            sar = min(sar, low[i - 1], low[i - 2] if i >= 2 else low[i - 1])
            if low[i] < sar:                      # retournement haussier -> baissier
                trend[i] = -1
                sar = ep
                ep = low[i]
                af = af_start
            else:
                trend[i] = 1
                if high[i] > ep:
                    ep = high[i]
                    af = min(af + af_step, af_max)
        else:
            sar = max(sar, high[i - 1], high[i - 2] if i >= 2 else high[i - 1])
            if high[i] > sar:                     # retournement baissier -> haussier
                trend[i] = 1
                sar = ep
                ep = high[i]
                af = af_start
            else:
                trend[i] = -1
                if low[i] < ep:
                    ep = low[i]
                    af = min(af + af_step, af_max)
        sar_values[i] = sar
    return {"trend": trend, "sar": sar_values}


def sar_flip_event(df: pd.DataFrame, sar_state: dict | None) -> tuple[bool, int]:
    """Retournement SAR strict sur les deux dernieres bougies H1.
    BULL: trend -1 -> +1, open precedent sous son SAR, close courant au-dessus du SAR.
    BEAR: trend +1 -> -1, open precedent au-dessus de son SAR, close courant sous le SAR."""
    if not sar_state:
        return False, 0
    trend = sar_state.get("trend") or []
    sar_values = sar_state.get("sar") or []
    if len(trend) < 2 or len(sar_values) < 2 or len(df) < 2:
        return False, 0

    prev_dir, cur_dir = trend[-2], trend[-1]
    prev_open = float(df["open"].iloc[-2])
    cur_close = float(df["close"].iloc[-1])
    prev_sar = float(sar_values[-2])
    cur_sar = float(sar_values[-1])

    bull = prev_dir == -1 and cur_dir == 1 and prev_open < prev_sar and cur_close > cur_sar
    bear = prev_dir == 1 and cur_dir == -1 and prev_open > prev_sar and cur_close < cur_sar
    return bool(bull or bear), cur_dir


def compute_h1_month_fib(pair: str, h1_candles: int = 800) -> dict | None:
    """Same Fibo as the V15 Pine "Monthly Reset" panel, but built from H1
    bars: range = high/low accumulated since the start of the current
    calendar month (UTC), 0.5 level = midpoint of that range. Returns where
    the live H1 price sits relative to that 0.5 level."""
    df = fetch_tv_ohlc(f"OANDA:{pair}", "60", h1_candles)
    if df is None or df.empty:
        return None

    last_ts = pd.Timestamp(df.index[-1])
    month_start = last_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_df = df[df.index >= month_start]
    if month_df.empty:
        return None

    month_high = float(month_df["high"].max())
    month_low = float(month_df["low"].min())
    fib_range = month_high - month_low
    if fib_range <= 0:
        return None

    fib50 = month_low + fib_range * 0.5
    live_price = float(df["close"].iloc[-1])
    position = "ABOVE" if live_price > fib50 else ("BELOW" if live_price < fib50 else "AT")

    # Parabolic SAR 1H: retournement strict sur les deux dernieres barres.
    # BULL: open precedent sous SAR precedent, close courant au-dessus du SAR.
    # BEAR: open precedent au-dessus SAR precedent, close courant sous le SAR.
    sar_state = parabolic_sar(df)
    sar_flipped, sar_dir = sar_flip_event(df, sar_state)
    sar_values = sar_state.get("sar") if sar_state else None

    return {
        "fib50": fib50,
        "position": position,
        "live_price": live_price,
        "month_high": month_high,
        "month_low": month_low,
        "pct_of_range": (live_price - month_low) / fib_range * 100.0,
        "sar_dir": sar_dir,
        "sar_flipped": sar_flipped,
        "sar_value": sar_values[-1] if sar_values else None,
        "sar_prev_value": sar_values[-2] if sar_values and len(sar_values) >= 2 else None,
    }


def compute_tf_state(pair: str, interval: str, length: int, candles: int, max_streak: int, live_price: float) -> TFState | None:
    df = fetch_tv_ohlc(f"OANDA:{pair}", interval, candles)
    if df is None or df.empty:
        return None

    # Le dernier candle D/W/M est le candle en cours dans les runs live.
    # Les streaks Renko doivent rester bases sur des briques cloturees.
    bricks = build_renko_bricks(closed_renko_source(df), length)
    if not bricks:
        return None

    renko_open, renko_close, direction = bricks[-1]
    green_streak, red_streak = streaks_from_bricks(bricks, max_streak)
    streak_count, streak_low, streak_high = streak_range_from_bricks(bricks)

    px_state = f_px_state(renko_open, renko_close, live_price)
    bias = f_effective_bias(px_state, green_streak, red_streak)

    return TFState(
        px_state=px_state,
        bias=bias,
        direction=direction,
        green_streak=green_streak,
        red_streak=red_streak,
        renko_open=renko_open,
        renko_close=renko_close,
        streak_count=streak_count,
        streak_low=streak_low,
        streak_high=streak_high,
    )


def compute_pair_score(pair: str, length: int, candles: int, max_streak: int) -> dict | None:
    # "Daily Live Close": the latest (possibly still-forming) daily close,
    # used as the chart reference price compared against each TF's Renko brick.
    df_d_live = fetch_tv_ohlc(f"OANDA:{pair}", "D", 2)
    if df_d_live is None or df_d_live.empty:
        return None
    live_price = float(df_d_live["close"].iloc[-1])
    prev_close = float(df_d_live["close"].iloc[-2]) if len(df_d_live) >= 2 else None
    daily_chg = ((live_price - prev_close) / prev_close * 100.0) if prev_close and prev_close != 0 else None

    states: dict[str, TFState] = {}
    for interval in ("M", "W", "D"):
        state = compute_tf_state(pair, interval, length, candles, max_streak, live_price)
        if state is None:
            return None
        states[interval] = state

    px = {tf: states[tf].px_state for tf in ("M", "W", "D")}
    bias = {tf: states[tf].bias for tf in ("M", "W", "D")}

    aligned_bull = bias["M"] == 1 and bias["W"] == 1 and bias["D"] == 1
    aligned_bear = bias["M"] == -1 and bias["W"] == -1 and bias["D"] == -1
    signal_state = 1 if aligned_bull else (-1 if aligned_bear else 0)

    max_score = sum(WEIGHTS.values())
    weighted_score = sum(px[tf] * WEIGHTS[tf] for tf in ("M", "W", "D"))
    base_pct = (weighted_score / max_score) * 100.0

    # CHG%D multiplier: 0..1 based on magnitude and direction alignment.
    # Wrong direction or below 0.1% → coeff=0 → final score=0 → never selected.
    coeff = chg_coeff(daily_chg, signal_state if signal_state != 0 else (1 if base_pct >= 0 else -1))
    weighted_pct = base_pct * coeff
    rounded_pct = round(weighted_pct)

    # SCORE state: positive final score = BULL, negative = BEAR, 0 = MIXED.
    score_state = 1 if weighted_pct > 0 else (-1 if weighted_pct < 0 else 0)

    # CONFIRMED: signal aligned + final score crosses SCORE_THRESHOLD.
    confirmed_bull = signal_state == 1 and weighted_pct >= SCORE_THRESHOLD
    confirmed_bear = signal_state == -1 and weighted_pct <= -SCORE_THRESHOLD
    confirmed = 1 if confirmed_bull else (-1 if confirmed_bear else 0)

    h1_fib = compute_h1_month_fib(pair)
    h1_price = h1_fib["live_price"] if h1_fib is not None else None
    streak_position = h1_vs_streak_position(states, h1_price)

    return {
        "pair": pair,
        "live_price": live_price,
        "h1_price": h1_price,
        "daily_chg": daily_chg,
        "states": states,
        "px": px,
        "bias": bias,
        "signal_state": signal_state,
        "base_pct": base_pct,
        "weighted_pct": weighted_pct,
        "rounded_pct": rounded_pct,
        "score_state": score_state,
        "confirmed": confirmed,
        "h1_fib": h1_fib,
        "streak_position": streak_position,
        "streak_tag": streak_position["tag"] if streak_position else "",
    }


def chg_coeff(daily_chg: float | None, signal_direction: int) -> float:
    """Maps CHG%D magnitude to a 0..1 confidence multiplier.
    Returns 0.0 if CHG%D is in the wrong direction or below the minimum threshold."""
    if daily_chg is None:
        return 0.0
    # Wrong direction → coeff = 0 → score killed
    if signal_direction == 1 and daily_chg <= 0.0:
        return 0.0
    if signal_direction == -1 and daily_chg >= 0.0:
        return 0.0
    abs_chg = abs(daily_chg)
    if abs_chg <= 0.1:
        return 0.0
    elif abs_chg <= 0.15:
        return 0.5
    elif abs_chg <= 0.2:
        return 0.6
    elif abs_chg <= 0.3:
        return 0.8
    elif abs_chg <= 0.4:
        return 0.9
    elif abs_chg <= 0.5:
        return 0.95
    else:
        return 1.0


def state_text(state: int) -> str:
    return {1: "BULL", -1: "BEAR", 0: "MIXED"}[state]


def confirmed_text(confirmed: int) -> str:
    return {1: "✓ BULL", -1: "✓ BEAR", 0: "—"}[confirmed]


def print_table(rows: list[dict]) -> None:
    print(
        f"{'PAIR':<8} {'SIGNAL':<7} {'SCORE_ST':<9} {'BASE%':<8} {'FINAL%':<8} {'CHG%D':<8} {'CONFIRMED':<10} "
        f"{'M(px/bias)':<12} {'W(px/bias)':<12} {'D(px/bias)':<12} {'PRICE':<12} {'H1/STREAK':<12} "
        f"{'1H vs FIB 0.5 (month)':<28}"
    )
    print("-" * 160)
    for row in sorted(rows, key=lambda r: r["weighted_pct"], reverse=True):
        pair = row["pair"]
        signal = state_text(row["signal_state"])
        score_st = state_text(row["score_state"])
        base_txt = f"{row['base_pct']:+.0f}%"
        score_txt = f"{row['weighted_pct']:+.0f}%"
        chg = row["daily_chg"]
        chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
        conf_txt = confirmed_text(row["confirmed"])
        m_txt = f"{row['px']['M']:+d}/{row['bias']['M']:+d}"
        w_txt = f"{row['px']['W']:+d}/{row['bias']['W']:+d}"
        d_txt = f"{row['px']['D']:+d}/{row['bias']['D']:+d}"
        streak_txt = row.get("streak_tag") or "N/A"

        h1_fib = row["h1_fib"]
        if h1_fib is None:
            fib_txt = "N/A"
        else:
            fib_txt = (
                f"{h1_fib['position']:<6} "
                f"(0.5={h1_fib['fib50']:.5f}, "
                f"{h1_fib['pct_of_range']:.0f}% of range)"
            )

        print(
            f"{pair:<8} {signal:<7} {score_st:<9} {base_txt:<8} {score_txt:<8} {chg_txt:<8} {conf_txt:<10} "
            f"{m_txt:<12} {w_txt:<12} {d_txt:<12} {row['live_price']:<12.5f} {streak_txt:<12} "
            f"{fib_txt:<28}"
        )


def filter_strong_signals(rows: list[dict]) -> list[dict]:
    """Keeps only CONFIRMED pairs (confirmed == ±1), mirroring Pine V16's
    confirmedBull / confirmedBear: signal aligned + score >= threshold + CHG%D >= threshold."""
    return [row for row in rows if row["confirmed"] != 0]


MAJORS = {"AUD", "CAD", "CHF", "EUR", "GBP", "JPY", "NZD", "USD"}


def currency_strength(rated: list[dict], min_pairs: int = 2) -> list[tuple[str, float]]:
    """Force par devise (currency strength meter): pour chaque devise majeure,
    moyenne du CHG%D qui lui est attribuable sur toutes les paires ou elle
    apparait (signe + si devise de base, - si contre-devise). On n'inclut que
    les paires 100% devises majeures: les paires metal (XAU) sont ecartees car
    la volatilite de l'or polluerait la force de sa contre-devise (USD).
    Retour trie du plus fort au plus faible."""
    agg: dict[str, list[float]] = {}
    for r in rated:
        p = r["pair"]
        if len(p) != 6:
            continue
        base, quote = p[:3], p[3:]
        if base not in MAJORS or quote not in MAJORS:
            continue
        c = r["daily_chg"]
        agg.setdefault(base, []).append(c)
        agg.setdefault(quote, []).append(-c)
    strength = {ccy: sum(v) / len(v) for ccy, v in agg.items() if len(v) >= min_pairs}
    return sorted(strength.items(), key=lambda kv: kv[1], reverse=True)


def tf_streak(r: dict, tf: str, direction: int) -> int:
    """Streak Renko (vert si direction>0, rouge sinon) pour un timeframe (M/W/D).
    0 = pas de run dans ce sens."""
    s = r.get("states", {}).get(tf)
    if s is None:
        return 0
    return s.green_streak if direction > 0 else s.red_streak


def top_daily_ok(r: dict, direction: int, min_abs: float = 0.15) -> bool:
    """Qualification qui renforce RENKO FIBO: mouvement du jour dans le sens
    `direction` et > min_abs, ET streaks Renko MENSUEL (M), HEBDO (W) ET
    JOURNALIER (D) pleins (>= 1) et alignes avec ce sens (biais + D/W/M alignes)."""
    chg = r.get("daily_chg")
    if chg is None:
        return False
    if direction > 0 and chg <= min_abs:
        return False
    if direction < 0 and chg >= -min_abs:
        return False
    return (tf_streak(r, "M", direction) >= 1
            and tf_streak(r, "W", direction) >= 1
            and tf_streak(r, "D", direction) >= 1)


def _load_strength_history() -> dict:
    try:
        with open(STRENGTH_HISTORY_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_strength_history(hist: dict) -> None:
    try:
        with open(STRENGTH_HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(hist, f, ensure_ascii=False)
    except Exception:
        pass


def update_strength_history(day_key: str, spread: float, avg: float, up: int, dn: int) -> dict:
    """Met a jour l'entree du JOUR (ecrasee a chaque run -> finit a la valeur de
    cloture), borne l'historique, et renvoie tout l'historique."""
    hist = _load_strength_history()
    hist[day_key] = {
        "spread": round(spread, 4),
        "force": round(avg, 4),
        "up": up,
        "dn": dn,
        "ts": datetime.now(PARIS_TZ).strftime("%Y-%m-%d %H:%M"),
    }
    keys = sorted(hist.keys())
    for k in keys[:-HISTORY_MAX_DAYS]:
        del hist[k]
    _save_strength_history(hist)
    return hist


def rank_spread(hist: dict, day_key: str, value: float, window: int = HISTORY_WINDOW) -> dict:
    """Classe la dispersion du jour vs les `window` derniers jours COMPLETS
    (aujourd'hui exclu). Renvoie n, percentile, pic, mediane, label."""
    past = [v["spread"] for k, v in sorted(hist.items())
            if k != day_key and isinstance(v, dict) and "spread" in v][-window:]
    n = len(past)
    if n == 0:
        return {"n": 0, "pct": 0.0, "peak": value, "median": value, "label": "?"}
    pct = 100.0 * sum(1 for x in past if x <= value) / n
    label = ("EXTREME" if pct >= 90 else "FORTE" if pct >= 66
             else "NORMALE" if pct >= 33 else "FAIBLE")
    return {"n": n, "pct": pct, "peak": max(past), "median": statistics.median(past), "label": label}


def _load_intraday_profile() -> dict | None:
    try:
        with open(INTRADAY_PROFILE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) and data.get("by_hour") else None
    except Exception:
        return None


def session_hour_idx(now: datetime | None = None) -> int:
    """Nombre d'heures ecoulees depuis l'ouverture de session NY (17:00)."""
    now = (now or datetime.now(NY_TZ)).astimezone(NY_TZ)
    op = now.replace(hour=SESSION_OPEN_HOUR, minute=0, second=0, microsecond=0)
    if now < op:
        op -= timedelta(days=1)
    return int((now - op).total_seconds() // 3600)


def _load_intraday_live() -> dict:
    try:
        with open(INTRADAY_LIVE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def update_intraday_live(live_log: dict, hour_idx: int, spread: float) -> None:
    """Ajoute le spread de l'heure courante au log vivant (borne a INTRADAY_LIVE_CAP
    echantillons recents par heure) et sauvegarde."""
    key = str(hour_idx)
    arr = live_log.get(key, [])
    arr.append(round(spread, 4))
    if len(arr) > INTRADAY_LIVE_CAP:
        arr = arr[-INTRADAY_LIVE_CAP:]
    live_log[key] = arr
    try:
        with open(INTRADAY_LIVE_PATH, "w", encoding="utf-8") as f:
            json.dump(live_log, f, ensure_ascii=False)
    except Exception:
        pass


def _intensity_label(pct: float) -> str:
    return ("EXTREME" if pct >= 90 else "FORTE" if pct >= 66
            else "NORMALE" if pct >= 33 else "FAIBLE")


def intraday_rank(profile: dict | None, live_log: dict | None, spread: float,
                  h_idx: int | None = None) -> dict | None:
    """Classe le spread courant vs la MEME heure: en priorite sur les donnees
    LIVE accumulees (si >= INTRADAY_LIVE_MIN echantillons), sinon sur le profil
    seed. Renvoie pct, heure, mediane, source, label."""
    h = h_idx if h_idx is not None else session_hour_idx()
    samples = (live_log or {}).get(str(h))
    if USE_INTRADAY_LIVE_PROFILE and samples and len(samples) >= INTRADAY_LIVE_MIN:
        srt = sorted(samples)
        pct = max(0.0, min(100.0, 100.0 * bisect.bisect_right(srt, spread) / len(srt)))
        return {"pct": pct, "hour": h, "n": len(srt), "median": srt[len(srt) // 2],
                "src": "live", "label": _intensity_label(pct)}
    bh = (profile or {}).get("by_hour", {}).get(str(h)) if profile else None
    if bh:
        q = bh["q"]                       # 101 quantiles tries P0..P100
        pct = max(0.0, min(100.0, float(bisect.bisect_right(q, spread) - 1)))
        return {"pct": pct, "hour": h, "n": bh.get("n", 0), "median": q[50],
                "src": "seed", "label": _intensity_label(pct)}
    return None


def _load_regime_reference() -> dict | None:
    try:
        with open(REGIME_REFERENCE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) and data.get("q") else None
    except Exception:
        return None


def regime_block(hist: dict) -> list[str]:
    """Situe la PERIODE recente (mediane du spread sur REGIME_WINDOW jours) vs la
    distribution recente (3 ans). Renvoie 2 lignes (label + valeurs) ou []."""
    ref = _load_regime_reference()
    if not ref:
        return []
    recent = [v["spread"] for k, v in sorted(hist.items())
              if isinstance(v, dict) and "spread" in v][-REGIME_WINDOW:]
    if len(recent) < 10:
        return []
    med = statistics.median(recent)
    q = ref["q"]
    pct = max(0, min(100, bisect.bisect_right(q, med) - 1))
    label = ("TRÈS AGITÉ" if pct >= 85 else "AGITÉ" if pct >= 66
             else "NORMAL" if pct >= 33 else "CALME" if pct >= 15 else "TRÈS CALME")
    return [f"🌐 RÉGIME: {label}",
            f"    ({med:.2f} / {ref['median']:.2f})"]


def daily_chg_section(all_rows: list[dict]) -> list[str]:
    """Biais journalier credible du marche, sur le CHG%D:
      1) verdict 🐂/🐻/⚖️ (breadth ET force concordent ; NEUTRE si marche calme)
      2) breadth chiffree + force moyenne
      3) currency strength meter (devise forte / faible)."""
    rated = [r for r in all_rows if r.get("daily_chg") is not None]
    lines: list[str] = []
    if not rated:
        return lines

    up = sum(1 for r in rated if r["daily_chg"] > 0)
    dn = sum(1 for r in rated if r["daily_chg"] < 0)
    avg = sum(r["daily_chg"] for r in rated) / len(rated)
    if up > dn and avg > 0:
        emoji, direction = "🐂", "BULL"
    elif dn > up and avg < 0:
        emoji, direction = "🐻", "BEAR"
    else:
        emoji, direction = "⚖️", "Partage"

    strength = currency_strength(rated)
    spread = (strength[0][1] - strength[-1][1]) if len(strength) >= 2 else 0.0
    day_key = datetime.now(PARIS_TZ).date().isoformat()
    hist = update_strength_history(day_key, spread, avg, up, dn)

    # 1) DIRECTION (le marche penche-t-il ?) + breadth + devises fortes/faibles.
    if direction == "Partage":
        lines.append("⚖️ Partagé")
    else:
        lines.append(f"{emoji} {direction} domine")
    # % de paires haussieres vs la norme 22 ans (mediane ~52%).
    _ref = _load_regime_reference()
    breadth_txt = ""
    if _ref and _ref.get("breadth_median") is not None and (up + dn) > 0:
        breadth_txt = f" · ({100.0 * up / (up + dn):.0f}% / {_ref['breadth_median']:.0f}%)"
    lines.append(f"▲{up} ▼{dn} ({avg:+.2f}%){breadth_txt}")
    if len(strength) >= 2:
        lines.append(f"💪 Fortes: {strength[0][0]} {strength[0][1]:+.2f}")
        lines.append(f"🥀 Faibles: {strength[-1][0]} {strength[-1][1]:+.2f}")

    # 2) INTENSITE (force du mouvement, relative a l'HEURE).
    h_idx = session_hour_idx()
    live_log = _load_intraday_live()
    intra = intraday_rank(_load_intraday_profile(), live_log, spread, h_idx)
    update_intraday_live(live_log, h_idx, spread)   # accumule (today exclu du classement)
    lines.append("")
    if intra is not None:
        lines.append(f"⚡️ INTENSITÉ (h+{intra['hour']}): {intra['label']}")
        lines.append(f"    ({spread:.2f} / {intra['median']:.2f})")
    else:
        rk = rank_spread(hist, day_key, spread)
        if rk["n"] >= HISTORY_CALIB_MIN:
            lines.append(f"⚡️ INTENSITÉ: {_intensity_label(rk['pct'])}")
            lines.append(f"    ({spread:.2f} / {rk['peak']:.2f})")
        else:
            lines.append(f"⚡️ INTENSITÉ: calibrage ({rk['n']}/{HISTORY_CALIB_MIN}j)")
            lines.append(f"    ({spread:.2f})")

    # 3) REGIME (contexte de la periode vs reference 3 ans).
    lines.extend(regime_block(hist))

    return lines


def sar_streak_turn_section(all_rows: list[dict]) -> list[str]:
    """Paires dont le prix H1 est au-dessus/en-dessous des 3 streaks D/W/M et
    dont le SAR 1H vient de se retourner dans le meme sens."""
    matches: list[tuple[int, dict]] = []
    for row in all_rows:
        h1_fib = row.get("h1_fib") or {}
        if not h1_fib.get("sar_flipped"):
            continue

        sar_dir = h1_fib.get("sar_dir")
        counts = (row.get("streak_position") or {}).get("counts") or {}
        if sar_dir == 1 and counts.get(1, 0) == 3:
            matches.append((1, row))
        elif sar_dir == -1 and counts.get(-1, 0) == 3:
            matches.append((-1, row))

    if not matches:
        return []

    lines = ["🔥 SAR H1 + STREAK"]
    for direction, row in sorted(matches, key=lambda x: (-x[0], x[1]["pair"])):
        icon = "🟢" if direction == 1 else "🔴"
        tag = "🟢3" if direction == 1 else "🔴3"
        sar_txt = "SAR↑" if direction == 1 else "SAR↓"
        lines.append(f"{icon} {row['pair']} {tag} {sar_txt}")
    return lines


def build_telegram_message(rows: list[dict], all_rows: list[dict] | None = None) -> str:
    # Group BULL together and BEAR together (strongest signal_state first),
    # and within each group rank by conviction — strongest |score| first —
    # instead of a flat descending sort that buries the strongest BEAR
    # (-100%) at the bottom, after the weaker BULL signals.
    ordered = sorted(rows, key=lambda r: (-r["signal_state"], -abs(r["weighted_pct"])))
    # Renforcement: une paire confirmee n'apparait dans RENKO FIBO que si elle
    # qualifie aussi pour le TOP DAILY de son sens (mouvement du jour > seuil +
    # streaks Renko W et D pleins et alignes).
    ordered = [r for r in ordered if top_daily_ok(r, r["signal_state"])]
    lines = ["📊 RENKO FIBO", ""]
    for row in ordered:
        icon = "🟢" if row["signal_state"] == 1 else "🔴"
        h1_fib = row["h1_fib"]
        fib_letter = "?"
        if h1_fib is not None:
            fib_letter = "A" if h1_fib["position"] == "ABOVE" else ("B" if h1_fib["position"] == "BELOW" else "=")
        # 🔥 si le Parabolic SAR 1H vient de se retourner dans le sens du signal
        # (sortie de retracement en faveur de la tendance).
        flame = ""
        if (h1_fib is not None and h1_fib.get("sar_flipped")
                and h1_fib.get("sar_dir") == row["signal_state"]):
            flame = " 🔥"
        streak_tag = row.get("streak_tag", "")
        streak_txt = f" {streak_tag}" if streak_tag else ""
        lines.append(f"{icon} {row['pair']} ({fib_letter} {row['weighted_pct']:+.0f}%){streak_txt}{flame}")

    sar_lines = sar_streak_turn_section(all_rows if all_rows is not None else rows)
    if sar_lines:
        lines.append("")
        lines.extend(sar_lines)

    # Section CHG%D journalier, sur l'ensemble des paires (pas seulement les
    # signaux confirmes RENKO FIBO).
    daily_lines = daily_chg_section(all_rows if all_rows is not None else rows)
    if daily_lines:
        lines.append("")
        lines.extend(daily_lines)

    lines.append("")
    lines.append(f"⏰ {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M Paris')}")
    return "\n".join(lines)


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    args = parse_args()
    rows = []
    for pair in PAIRS_29:
        try:
            row = compute_pair_score(pair, args.length, args.candles, args.max_streak)
        except Exception as exc:
            print(f"{pair}: error — {exc}")
            continue
        if row is None:
            print(f"{pair}: no data")
            continue
        rows.append(row)

    print_table(rows)

    strong_rows = filter_strong_signals(rows)
    message = build_telegram_message(strong_rows, rows)
    print("")
    print(message)
    send_telegram_message(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
