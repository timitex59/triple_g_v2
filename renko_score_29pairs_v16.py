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
import hashlib
import json
import os
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from ichimoku_v4 import PAIRS_29, fetch_tv_ohlc, send_telegram_message
from renko_forex_V2 import fetch_tv_renko_ohlc as fetch_tv_native_renko_ohlc


PARIS_TZ = ZoneInfo("Europe/Paris")
SCORE_THRESHOLD = 60.0
CHG_THRESHOLD = 0.1
CONFIRMED_MIN_STREAKS = {"M": 1, "W": 1, "D": 2}
VIVIER_MIN_ABS_BASE_SCORE = 33.0
VIVIER_FIB_MIDPOINT_PCT = 50.0
VIVIER_STATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "renko_score_29pairs_vivier_state.json",
)

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
FIB_LEVELS = (
    (0.0, "0"),
    (0.236, "0.236"),
    (0.382, "0.382"),
    (0.500, "0.500"),
    (0.618, "0.618"),
    (0.786, "0.786"),
    (1.0, "1"),
)
VIVIER_EVENTS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "renko_vivier_events.jsonl"
)
VIVIER_PERFORMANCE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "renko_vivier_performance.json"
)
VIVIER_PERFORMANCE_VERSION = 2
VIVIER_PERFORMANCE_HOURS = (1, 4, 12, 24, 72)


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
    parser.add_argument("--no-telegram", action="store_true", help="Print the analysis without sending it to Telegram.")
    parser.add_argument("--vivier-state", default=VIVIER_STATE_PATH, help="Path to the persistent VIVIER state file.")
    parser.add_argument("--vivier-events", default=VIVIER_EVENTS_PATH, help="Append-only VIVIER event journal.")
    parser.add_argument("--vivier-performance", default=VIVIER_PERFORMANCE_PATH, help="VIVIER performance tracker.")
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


def streak_in_direction(state: TFState, direction: int) -> int:
    return state.green_streak if direction > 0 else state.red_streak


def strict_confirmed_alignment(states: dict[str, TFState], direction: int) -> bool:
    """True only when CONFIRMED is backed by real Renko streaks.

    Monthly may be inside or broken in the signal direction, but Weekly and
    Daily must have actually broken in that direction. Daily requires 2 bricks
    to avoid 1-brick whipsaws.
    """
    if direction not in (1, -1):
        return False
    for tf, min_streak in CONFIRMED_MIN_STREAKS.items():
        state = states.get(tf)
        if state is None or streak_in_direction(state, direction) < min_streak:
            return False
    if states["M"].px_state not in (0, direction):
        return False
    return states["W"].px_state == direction and states["D"].px_state == direction


def score_passes_threshold(weighted_pct: float | None, direction: int) -> bool:
    if weighted_pct is None:
        return False
    if direction > 0:
        return weighted_pct >= SCORE_THRESHOLD
    if direction < 0:
        return weighted_pct <= -SCORE_THRESHOLD
    return False


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


def closed_h1_source(df: pd.DataFrame, now: pd.Timestamp | None = None) -> pd.DataFrame:
    """Exclude the last H1 candle only while its one-hour window is open."""
    if df.empty:
        return df
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    now = pd.Timestamp(now)
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    else:
        now = now.tz_convert("UTC")
    last_start = pd.Timestamp(df.index[-1])
    if last_start.tzinfo is None:
        last_start = last_start.tz_localize("UTC")
    else:
        last_start = last_start.tz_convert("UTC")
    return df.iloc[:-1] if now < last_start + pd.Timedelta(hours=1) else df


def sar_cross_event(df: pd.DataFrame, sar_state: dict | None) -> dict | None:
    """Standard close/SAR crossover or crossunder on the last two closed H1 bars."""
    if not sar_state or len(df) < 2:
        return None
    trend = sar_state.get("trend") or []
    sar_values = sar_state.get("sar") or []
    if len(trend) < 2 or len(sar_values) < 2:
        return None

    prev_close = float(df["close"].iloc[-2])
    cur_close = float(df["close"].iloc[-1])
    prev_sar = float(sar_values[-2])
    cur_sar = float(sar_values[-1])
    bull = trend[-2] == -1 and trend[-1] == 1 and prev_close <= prev_sar and cur_close > cur_sar
    bear = trend[-2] == 1 and trend[-1] == -1 and prev_close >= prev_sar and cur_close < cur_sar
    if not bull and not bear:
        return None
    bar_time = pd.Timestamp(df.index[-1])
    event_time = bar_time + pd.Timedelta(hours=1)
    return {
        "direction": 1 if bull else -1,
        "sar_value": cur_sar,
        "time_utc": event_time.isoformat(),
        "bar_time_utc": bar_time.isoformat(),
    }


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

    # VIVIER SAR records use only completed H1 bars and the standard
    # close/SAR crossover definition, independently from the live SAR fields.
    closed_df = closed_h1_source(df)
    closed_sar_state = parabolic_sar(closed_df)
    cross_event = sar_cross_event(closed_df, closed_sar_state)
    closed_extreme = None
    if not closed_df.empty:
        closed_ts = pd.Timestamp(closed_df.index[-1])
        closed_month_start = closed_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        before_closed = closed_df[
            (closed_df.index >= closed_month_start) & (closed_df.index < closed_ts)
        ]
        if not before_closed.empty:
            closed_extreme = {
                "time_utc": (closed_ts + pd.Timedelta(hours=1)).isoformat(),
                "bar_time_utc": closed_ts.isoformat(),
                "high": float(closed_df["high"].iloc[-1]),
                "low": float(closed_df["low"].iloc[-1]),
                "fib1_before": float(before_closed["high"].max()),
                "fib0_before": float(before_closed["low"].min()),
            }
    if cross_event is not None:
        event_ts = pd.Timestamp(closed_df.index[-1])
        event_month_start = event_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        event_month_df = closed_df[closed_df.index >= event_month_start]
        event_high = float(event_month_df["high"].max())
        event_low = float(event_month_df["low"].min())
        cross_event["fib50"] = event_low + (event_high - event_low) * 0.5

    closed_bars = [
        {
            "time_utc": (pd.Timestamp(ts) + pd.Timedelta(hours=1)).isoformat(),
            "high": float(bar["high"]),
            "low": float(bar["low"]),
            "close": float(bar["close"]),
        }
        for ts, bar in closed_df.iterrows()
    ]
    closed_time = closed_bars[-1]["time_utc"] if closed_bars else None
    closed_price = closed_bars[-1]["close"] if closed_bars else None

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
        "sar_cross_event": cross_event,
        "closed_extreme": closed_extreme,
        "h1_closed_time_utc": closed_time,
        "h1_closed_price": closed_price,
        "_closed_h1_bars": closed_bars,
    }


def fib_ceiling_label(h1_fib: dict | None) -> str:
    """Fibonacci level immediately above the current H1 price."""
    if not h1_fib:
        return "Fibo ?"
    pct = h1_fib.get("pct_of_range")
    if not isinstance(pct, (int, float)):
        return "Fibo ?"
    ratio = float(pct) / 100.0
    if ratio < 0.0:
        return "Fibo <0"
    for level, text in FIB_LEVELS[1:]:
        if ratio < level:
            return f"Fibo <{text}"
    return "Fibo >1"


def fib_directional_label(h1_fib: dict | None, direction: int) -> str:
    """Relevant Fibonacci boundary: ceiling for BULL, floor for BEAR."""
    if direction >= 0:
        return fib_ceiling_label(h1_fib)
    if not h1_fib:
        return "Fibo ?"
    pct = h1_fib.get("pct_of_range")
    if not isinstance(pct, (int, float)):
        return "Fibo ?"
    ratio = float(pct) / 100.0
    if ratio > 1.0:
        return "Fibo >1"
    for level, text in reversed(FIB_LEVELS[:-1]):
        if ratio > level:
            return f"Fibo >{text}"
    return "Fibo <0"


def compute_tf_state(pair: str, interval: str, length: int, candles: int, max_streak: int, live_price: float) -> TFState | None:
    # Use TradingView's native ATR Renko series. Rebuilding Renko locally from
    # standard OHLC candles produces different ATR box sizes (especially on M)
    # and can classify an actual TradingView "Inside" state as Above/Below.
    native_bricks = fetch_tv_native_renko_ohlc(
        f"OANDA:{pair}",
        interval,
        atr_length=length,
        n_bricks=max(candles, max_streak + 1),
    )
    if not native_bricks:
        return None

    bricks: list[tuple[float, float, int]] = []
    for brick in native_bricks:
        renko_open = float(brick["open"])
        renko_close = float(brick["close"])
        direction = 1 if renko_close > renko_open else (-1 if renko_close < renko_open else 0)
        if direction:
            bricks.append((renko_open, renko_close, direction))
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

    # CONFIRMED: signal aligned + final score crosses SCORE_THRESHOLD + real
    # M/W/D Renko streaks in the signal direction.
    strict_confirmed = strict_confirmed_alignment(states, signal_state)
    confirmed_bull = signal_state == 1 and strict_confirmed and score_passes_threshold(weighted_pct, 1)
    confirmed_bear = signal_state == -1 and strict_confirmed and score_passes_threshold(weighted_pct, -1)
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
        "strict_confirmed": strict_confirmed,
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
    """Keeps only strict CONFIRMED pairs:
    signal aligned + score threshold + CHG%D threshold + M/W/D Renko streaks."""
    return [row for row in rows if row["confirmed"] != 0]


def _vivier_px(row: dict) -> dict[str, int] | None:
    """Return the strict price/Renko states used by VIVIER (never the bias)."""
    px = row.get("px") or {}
    if any(px.get(tf) not in (-1, 0, 1) for tf in ("M", "W", "D")):
        return None
    return {tf: int(px[tf]) for tf in ("M", "W", "D")}


def _base_score_from_px(px: dict) -> float:
    weighted_score = sum(float(px[tf]) * WEIGHTS[tf] for tf in ("M", "W", "D"))
    return weighted_score / sum(WEIGHTS.values()) * 100.0


def vivier_entry_direction(row: dict) -> int:
    """Direction of a new VIVIER entry, or 0.

    Monthly must be strictly outside its Renko brick. At least one lower
    timeframe must be strictly opposite; an Inside state never qualifies as
    the opposition that creates an entry. BULL entries must be in the lower
    half of the monthly H1 Fibonacci range, BEAR entries in the upper half.
    """
    px = _vivier_px(row)
    if px is None or px["M"] not in (-1, 1):
        return 0
    direction = px["M"]
    has_strict_opposition = px["W"] == -direction or px["D"] == -direction
    score_ok = abs(_base_score_from_px(px)) >= VIVIER_MIN_ABS_BASE_SCORE
    fib_pct = (row.get("h1_fib") or {}).get("pct_of_range")
    fib_ok = isinstance(fib_pct, (int, float)) and (
        (direction == 1 and fib_pct <= VIVIER_FIB_MIDPOINT_PCT)
        or (direction == -1 and fib_pct >= VIVIER_FIB_MIDPOINT_PCT)
    )
    return direction if has_strict_opposition and score_ok and fib_ok else 0


def vivier_full_alignment(row: dict, direction: int) -> bool:
    """True only for strict M/W/D price-Renko alignment (raw score +/-100%)."""
    px = _vivier_px(row)
    return bool(px is not None and direction in (-1, 1)
                and all(px[tf] == direction for tf in ("M", "W", "D")))


def load_vivier_state(path: str = VIVIER_STATE_PATH) -> dict:
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        return {"version": 1, "pairs": {}}
    except Exception as exc:
        print(f"VIVIER: impossible de lire l'etat ({exc}); demarrage avec un etat vide.")
        return {"version": 1, "pairs": {}}

    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    if not isinstance(pairs, dict):
        return {"version": 1, "pairs": {}}
    valid_pairs = {
        str(pair): dict(entry)
        for pair, entry in pairs.items()
        if isinstance(entry, dict) and entry.get("direction") in (-1, 1)
    }
    return {"version": 1, "pairs": valid_pairs}


def save_vivier_state(state: dict, path: str = VIVIER_STATE_PATH) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp.{os.getpid()}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def apply_vivier_sar_record(entry: dict, row: dict, direction: int) -> None:
    """Set a one-run flame when a valid H1 SAR event makes a new record."""
    entry["sar_flame"] = False
    event = (row.get("h1_fib") or {}).get("sar_cross_event") or {}
    if event.get("direction") != direction:
        return
    event_time = event.get("time_utc")
    if not event_time or event_time == entry.get("sar_last_processed_time_utc"):
        return
    entry["sar_last_processed_time_utc"] = event_time

    sar_value = event.get("sar_value")
    fib50 = event.get("fib50")
    if not isinstance(sar_value, (int, float)) or not isinstance(fib50, (int, float)):
        return
    # BEAR: crossunder SAR above Fibo 0.500 and new running maximum.
    # BULL: crossover SAR below Fibo 0.500 and new running minimum.
    fib_ok = sar_value > fib50 if direction == -1 else sar_value < fib50
    if not fib_ok:
        return
    previous = entry.get("sar_record_value")
    is_record = previous is None or (
        sar_value > previous if direction == -1 else sar_value < previous
    )
    if not is_record:
        return
    entry["sar_record_value"] = float(sar_value)
    entry["sar_record_time_utc"] = event_time
    entry["sar_flame"] = True


def apply_vivier_fib_extreme_reset(entry: dict, row: dict, direction: int) -> bool:
    """Clear a SAR record after BULL touches prior Fibo 1 or BEAR prior Fibo 0."""
    if entry.get("sar_record_value") is None:
        return False
    extreme = (row.get("h1_fib") or {}).get("closed_extreme") or {}
    bar_time = extreme.get("time_utc")
    if not bar_time or bar_time == entry.get("sar_last_fib_reset_time_utc"):
        return False
    if direction == 1:
        high, fib1 = extreme.get("high"), extreme.get("fib1_before")
        touched = (isinstance(high, (int, float)) and isinstance(fib1, (int, float))
                   and high >= fib1)
        reason = "FIB1_TOUCH"
    else:
        low, fib0 = extreme.get("low"), extreme.get("fib0_before")
        touched = (isinstance(low, (int, float)) and isinstance(fib0, (int, float))
                   and low <= fib0)
        reason = "FIB0_TOUCH"
    if not touched:
        return False
    entry.pop("sar_record_value", None)
    entry.pop("sar_record_time_utc", None)
    entry["sar_last_fib_reset_time_utc"] = bar_time
    entry["sar_last_fib_reset_reason"] = reason
    return True


def update_vivier(rows: list[dict], previous_state: dict | None = None,
                   now: datetime | None = None) -> tuple[dict, list[dict]]:
    """Advance the persistent VIVIER state and return one-shot alignments.

    Entry is strict opposition to Monthly. Once tracked, a pair remains in the
    pool while W/D pass through Inside. It leaves when M is no longer in its
    original strict direction, or emits a one-shot signal when M/W/D become
    strictly aligned in that direction. Missing market rows are kept unchanged
    so a temporary data error cannot erase the watchlist.
    """
    now = now or datetime.now(PARIS_TZ)
    stamp = now.astimezone(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
    old_pairs = (previous_state or {}).get("pairs") or {}
    tracked = {
        str(pair): dict(entry)
        for pair, entry in old_pairs.items()
        if isinstance(entry, dict) and entry.get("direction") in (-1, 1)
    }
    for entry in tracked.values():
        entry["sar_flame"] = False
    signals: list[dict] = []

    for row in rows:
        pair = str(row.get("pair") or "")
        px = _vivier_px(row)
        if not pair or px is None:
            continue

        existing = tracked.get(pair)
        if existing is not None:
            direction = int(existing["direction"])
            if vivier_full_alignment(row, direction):
                signals.append({
                    "pair": pair,
                    "direction": direction,
                    "px": px,
                    "entered_at_paris": existing.get("entered_at_paris"),
                    "signaled_at_paris": stamp,
                    "base_pct": row.get("base_pct"),
                    "weighted_pct": row.get("weighted_pct"),
                    "fib_position": fib_directional_label(row.get("h1_fib"), direction),
                })
                del tracked[pair]
                continue
            if px["M"] == direction:
                if abs(_base_score_from_px(px)) < VIVIER_MIN_ABS_BASE_SCORE:
                    del tracked[pair]
                    continue
                existing["last_seen_at_paris"] = stamp
                existing["last_px"] = px
                existing["base_pct"] = row.get("base_pct")
                existing["weighted_pct"] = row.get("weighted_pct")
                existing["fib_position"] = fib_directional_label(row.get("h1_fib"), direction)
                existing["fib_pct_of_range"] = (row.get("h1_fib") or {}).get("pct_of_range")
                reset_now = apply_vivier_fib_extreme_reset(existing, row, direction)
                event_time = ((row.get("h1_fib") or {}).get("sar_cross_event") or {}).get("time_utc")
                reset_time = existing.get("sar_last_fib_reset_time_utc")
                if not reset_now or event_time != reset_time:
                    apply_vivier_sar_record(existing, row, direction)
                continue

            # Monthly became Inside or reversed: invalidate the old pool.
            del tracked[pair]

        direction = vivier_entry_direction(row)
        if direction:
            tracked[pair] = {
                "direction": direction,
                "entered_at_paris": stamp,
                "last_seen_at_paris": stamp,
                "entry_px": px,
                "last_px": px,
                "base_pct": row.get("base_pct"),
                "weighted_pct": row.get("weighted_pct"),
                "fib_position": fib_directional_label(row.get("h1_fib"), direction),
                "fib_pct_of_range": (row.get("h1_fib") or {}).get("pct_of_range"),
            }
            apply_vivier_sar_record(tracked[pair], row, direction)

    return {
        "version": 1,
        "updated_at_paris": stamp,
        "pairs": dict(sorted(tracked.items())),
    }, sorted(signals, key=lambda item: (-item["direction"], item["pair"]))


def _px_compact(px: dict | None) -> str:
    symbols = {1: "+", 0: "0", -1: "-"}
    px = px or {}
    return " ".join(f"{tf}{symbols.get(px.get(tf), '?')}" for tf in ("M", "W", "D"))


def vivier_base_score(entry: dict) -> float:
    """Raw M/W/D Renko score used to rank progress toward +/-100%."""
    px = entry.get("last_px") or {}
    if any(px.get(tf) not in (-1, 0, 1) for tf in ("M", "W", "D")):
        return 0.0
    return _base_score_from_px(px)


def vivier_groups(state: dict) -> tuple[list[tuple[str, dict]], list[tuple[str, dict]]]:
    pairs = state.get("pairs") or {}
    rank = lambda item: (-abs(vivier_base_score(item[1])), item[0])
    bull = sorted(
        ((pair, entry) for pair, entry in pairs.items() if entry.get("direction") == 1),
        key=rank,
    )
    bear = sorted(
        ((pair, entry) for pair, entry in pairs.items() if entry.get("direction") == -1),
        key=rank,
    )
    return bull, bear


def print_vivier_report(state: dict, signals: list[dict]) -> None:
    bull, bear = vivier_groups(state)
    print("\nVIVIER RENKO")
    print(f"BULL ({len(bull)}): " + (", ".join(pair for pair, _ in bull) or "--"))
    for pair, entry in bull:
        fib = entry.get("fib_position", "Fibo ?")
        flame = " 🔥" if entry.get("sar_flame") else ""
        print(f"  {pair:<8} {vivier_base_score(entry):+4.0f}%  {fib:<12}  {_px_compact(entry.get('last_px'))}{flame}")
    print(f"BEAR ({len(bear)}): " + (", ".join(pair for pair, _ in bear) or "--"))
    for pair, entry in bear:
        fib = entry.get("fib_position", "Fibo ?")
        flame = " 🔥" if entry.get("sar_flame") else ""
        print(f"  {pair:<8} {vivier_base_score(entry):+4.0f}%  {fib:<12}  {_px_compact(entry.get('last_px'))}{flame}")
    if signals:
        print("SIGNAUX VIVIER:")
        for signal in signals:
            label = "BULL" if signal["direction"] == 1 else "BEAR"
            print(f"  {signal['pair']:<8} {label} {_px_compact(signal.get('px'))}")


def _event_price_at(row: dict | None, time_utc: str | None) -> float | None:
    if row is None:
        return None
    h1 = row.get("h1_fib") or {}
    if time_utc:
        for bar in h1.get("_closed_h1_bars") or []:
            if bar.get("time_utc") == time_utc:
                return float(bar["close"])
    value = h1.get("h1_closed_price", row.get("h1_price", row.get("live_price")))
    return float(value) if isinstance(value, (int, float)) else None


def _build_vivier_event(event_type: str, pair: str, direction: int,
                         row: dict | None, time_utc: str | None = None,
                         **extra) -> dict:
    h1 = (row or {}).get("h1_fib") or {}
    time_utc = time_utc or h1.get("h1_closed_time_utc")
    if not time_utc:
        time_utc = datetime.now(ZoneInfo("UTC")).isoformat()
    ts = pd.Timestamp(time_utc)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    canonical_time = ts.isoformat()
    identity = f"{event_type}|{pair}|{direction}|{canonical_time}"
    payload = {
        "event_id": hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20],
        "event_type": event_type,
        "pair": pair,
        "direction": direction,
        "time_utc": canonical_time,
        "time_paris": ts.tz_convert(PARIS_TZ).isoformat(),
        "price": _event_price_at(row, canonical_time),
        "base_pct": (row or {}).get("base_pct"),
        "final_pct": (row or {}).get("weighted_pct"),
        "fib_position": fib_directional_label(h1, direction),
        "fib_pct_of_range": h1.get("pct_of_range"),
        "px": (row or {}).get("px"),
    }
    payload.update(extra)
    return payload


def collect_vivier_run_events(previous_state: dict, current_state: dict,
                               vivier_signals: list[dict], strong_rows: list[dict],
                               all_rows: list[dict]) -> list[dict]:
    """Build one-shot lifecycle events by comparing the previous/current pools."""
    previous = (previous_state or {}).get("pairs") or {}
    current = (current_state or {}).get("pairs") or {}
    rows_by_pair = {row["pair"]: row for row in all_rows}
    signal_pairs = {signal["pair"] for signal in vivier_signals}
    renko_fibo_pairs = {
        row["pair"] for row in strong_rows if sar_streak_full(row)
    }
    events: list[dict] = []

    # Invalid exits first, including a same-run direction switch.
    for pair, old in sorted(previous.items()):
        new = current.get(pair)
        direction_changed = new is not None and new.get("direction") != old.get("direction")
        if (new is None and pair not in signal_pairs) or direction_changed:
            events.append(_build_vivier_event(
                "VIVIER_EXIT_INVALID", pair, int(old["direction"]), rows_by_pair.get(pair),
                reason="DIRECTION_CHANGE" if direction_changed else "MONTHLY_OR_SCORE_INVALID",
            ))

    for pair, entry in sorted(current.items()):
        old = previous.get(pair)
        if old is None or old.get("direction") != entry.get("direction"):
            events.append(_build_vivier_event(
                "VIVIER_ENTRY", pair, int(entry["direction"]), rows_by_pair.get(pair),
                entered_at_paris=entry.get("entered_at_paris"),
            ))
        if entry.get("sar_flame"):
            events.append(_build_vivier_event(
                "SAR_FLAME", pair, int(entry["direction"]), rows_by_pair.get(pair),
                time_utc=entry.get("sar_record_time_utc"),
                sar_value=entry.get("sar_record_value"),
            ))
        reset_time = entry.get("sar_last_fib_reset_time_utc")
        if reset_time and reset_time != (old or {}).get("sar_last_fib_reset_time_utc"):
            events.append(_build_vivier_event(
                "SAR_RECORD_RESET", pair, int(entry["direction"]), rows_by_pair.get(pair),
                time_utc=reset_time, reason=entry.get("sar_last_fib_reset_reason"),
            ))

    for signal in vivier_signals:
        pair = signal["pair"]
        event_type = ("VIVIER_TO_RENKO_FIBO" if pair in renko_fibo_pairs
                      else "SIGNAL_VIVIER")
        events.append(_build_vivier_event(
            event_type, pair, int(signal["direction"]), rows_by_pair.get(pair),
            entered_at_paris=signal.get("entered_at_paris"),
        ))

    return sorted(events, key=lambda event: (event["time_utc"], event["pair"], event["event_type"]))


def append_vivier_event_journal(events: list[dict], path: str = VIVIER_EVENTS_PATH) -> int:
    """Append unseen event IDs and return the number of newly written records."""
    if not events:
        return 0
    existing_ids: set[str] = set()
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                try:
                    existing_ids.add(json.loads(line)["event_id"])
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue
    except FileNotFoundError:
        pass
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    unseen = []
    for event in events:
        if event["event_id"] in existing_ids:
            continue
        unseen.append(event)
        existing_ids.add(event["event_id"])
    if unseen:
        with open(path, "a", encoding="utf-8") as handle:
            for event in unseen:
                handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    return len(unseen)


def load_vivier_performance(path: str = VIVIER_PERFORMANCE_PATH) -> dict:
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {
            "version": VIVIER_PERFORMANCE_VERSION, "events": []
        }
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {"version": VIVIER_PERFORMANCE_VERSION, "events": []}


def _performance_bucket(events: list[dict]) -> dict:
    horizons = {}
    for hours in VIVIER_PERFORMANCE_HOURS:
        key = f"{hours}h"
        values = [
            float(event["horizons"][key]["directional_pct"])
            for event in events
            if key in (event.get("horizons") or {})
        ]
        horizons[key] = {
            "samples": len(values),
            "wins": sum(value > 0 for value in values),
            "win_rate_pct": (sum(value > 0 for value in values) / len(values) * 100.0
                             if values else None),
            "avg_directional_pct": statistics.fmean(values) if values else None,
        }
    mfe_values = [float(event["mfe_72h_pct"]) for event in events if "mfe_72h_pct" in event]
    mae_values = [float(event["mae_72h_pct"]) for event in events if "mae_72h_pct" in event]
    return {
        "count": len(events),
        "complete": sum(bool(event.get("complete")) for event in events),
        "horizons": horizons,
        "avg_mfe_72h_pct": statistics.fmean(mfe_values) if mfe_values else None,
        "avg_mae_72h_pct": statistics.fmean(mae_values) if mae_values else None,
    }


def _performance_summary(events: list[dict]) -> dict:
    event_types = sorted({event["event_type"] for event in events})
    overall = _performance_bucket(events)
    return {
        "total": overall["count"],
        "complete": overall["complete"],
        "overall": overall,
        "by_type": {
            event_type: _performance_bucket([
                event for event in events if event["event_type"] == event_type
            ])
            for event_type in event_types
        },
    }


def update_vivier_performance(previous: dict, new_events: list[dict],
                               all_rows: list[dict], now: datetime | None = None) -> dict:
    """Evaluate events after N subsequently closed H1 bars, excluding market gaps."""
    tracked = {
        event["event_id"]: dict(event)
        for event in (previous or {}).get("events", [])
        if isinstance(event, dict) and event.get("event_id")
    }
    for event in new_events:
        if event.get("price") is not None:
            tracked.setdefault(event["event_id"], {**event, "horizons": {}})

    bars_by_pair = {
        row["pair"]: (row.get("h1_fib") or {}).get("_closed_h1_bars") or []
        for row in all_rows
    }
    for event in tracked.values():
        price = event.get("price")
        bars = bars_by_pair.get(event.get("pair")) or []
        if not isinstance(price, (int, float)) or price == 0 or not bars:
            continue
        start = pd.Timestamp(event["time_utc"])
        if start.tzinfo is None:
            start = start.tz_localize("UTC")
        direction = int(event["direction"])
        parsed = sorted(
            [(pd.Timestamp(bar["time_utc"]), bar) for bar in bars],
            key=lambda item: item[0],
        )
        future_bars = [(ts, bar) for ts, bar in parsed if ts > start]
        if event.get("horizon_basis") != "closed_h1_bars":
            # Version 1 used elapsed calendar time and collapsed weekend gaps.
            event["horizons"] = {}
            event.pop("mfe_72h_pct", None)
            event.pop("mae_72h_pct", None)
            event.pop("last_evaluated_time_utc", None)
            event["horizon_basis"] = "closed_h1_bars"
        horizons = event.setdefault("horizons", {})
        for hours in VIVIER_PERFORMANCE_HOURS:
            key = f"{hours}h"
            if key in horizons:
                continue
            if len(future_bars) < hours:
                continue
            ts, bar = future_bars[hours - 1]
            raw_pct = (float(bar["close"]) - price) / price * 100.0
            horizons[key] = {
                "time_utc": ts.isoformat(),
                "price": float(bar["close"]),
                "raw_pct": raw_pct,
                "directional_pct": raw_pct * direction,
            }

        window = [bar for _, bar in future_bars[:max(VIVIER_PERFORMANCE_HOURS)]]
        if window:
            if direction == 1:
                mfe = (max(float(bar["high"]) for bar in window) - price) / price * 100.0
                mae = (min(float(bar["low"]) for bar in window) - price) / price * 100.0
            else:
                mfe = (price - min(float(bar["low"]) for bar in window)) / price * 100.0
                mae = (price - max(float(bar["high"]) for bar in window)) / price * 100.0
            event["mfe_72h_pct"] = mfe
            event["mae_72h_pct"] = mae
            event["last_evaluated_time_utc"] = future_bars[-1][0].isoformat()
        event["complete"] = all(f"{hours}h" in horizons for hours in VIVIER_PERFORMANCE_HOURS)

    events = sorted(tracked.values(), key=lambda event: (event["time_utc"], event["event_id"]))
    stamp = (now or datetime.now(ZoneInfo("UTC"))).isoformat()
    return {"version": VIVIER_PERFORMANCE_VERSION,
            "updated_at_utc": stamp, "summary": _performance_summary(events),
            "events": events}


def save_vivier_performance(payload: dict, path: str = VIVIER_PERFORMANCE_PATH) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp.{os.getpid()}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


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
    JOURNALIER (D) pleins et alignes avec ce sens. Daily exige 2 briques."""
    chg = r.get("daily_chg")
    if chg is None:
        return False
    if direction > 0 and chg <= min_abs:
        return False
    if direction < 0 and chg >= -min_abs:
        return False
    return (tf_streak(r, "M", direction) >= CONFIRMED_MIN_STREAKS["M"]
            and tf_streak(r, "W", direction) >= CONFIRMED_MIN_STREAKS["W"]
            and tf_streak(r, "D", direction) >= CONFIRMED_MIN_STREAKS["D"])


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


def sar_streak_full(r: dict) -> bool:
    """Confluence maximale = LE SEUL profil retenu dans RENKO FIBO:
      - prix H1 au-dela des plages des 3 streaks Renko D/W/M (compteur == 3,
        soit le tag 🟢3 / 🔴3), ET
      - SAR H1 aligne avec le sens du biais (sar_dir == signal_state).
    Toutes les unites de temps + le timing H1 pointent dans le meme sens."""
    d = r.get("signal_state")
    if d not in (1, -1):
        return False
    h1 = r.get("h1_fib") or {}
    if h1.get("sar_dir") != d:
        return False
    counts = (r.get("streak_position") or {}).get("counts") or {}
    return counts.get(d, 0) == 3


TURN_TIERS = {"anticipe": "🌱 anticipe", "amorce": "⏳ amorce", "confirme": "✅ confirmé"}
TURN_ORDER = {"anticipe": 0, "amorce": 1, "confirme": 2}
TELEGRAM_TURN_TIERS = {"confirme"}


def turn_tier(r: dict, direction: int) -> str | None:
    """Niveau de retournement dans le sens `direction` (BULL=1 / BEAR=-1), ou None.

    Base: tendance forte M+W = prix au-dessus OU dans la plage du streak
    (pos in {sens, 0}) ET streak plein (>= 1) sur Monthly ET Weekly.
    Puis classement selon le Daily, avec confirmation SAR H1:
      🌱 anticipe : prix replie (<= plage, pos in {0,-sens}) + SAR H1 vient de
                    flipper dans le sens -> on prend le creux qui tourne (le + tot).
      ⏳ amorce   : prix > plus-haut (pos==sens), Renko D pas encore aligne
                    (streak D == 0), SAR H1 aligne -> cassure naissante.
      ✅ confirme : conditions strictes CONFIRMED + score >= seuil, avec >= 2
                    briques Renko D dans le sens.
    """
    pos = (r.get("streak_position") or {}).get("pos") or {}
    states = r.get("states") or {}
    if any(tf not in pos or states.get(tf) is None for tf in ("M", "W", "D")):
        return None
    up = 1 if direction > 0 else -1

    def strk(tf: str) -> int:
        s = states[tf]
        return s.green_streak if direction > 0 else s.red_streak

    # Tendance forte M + W (au-dessus ou dans la plage, streak plein).
    if not (pos["M"] in (up, 0) and strk("M") >= 1
            and pos["W"] in (up, 0) and strk("W") >= 1):
        return None

    h1 = r.get("h1_fib") or {}
    sar_dir, sar_flip = h1.get("sar_dir"), h1.get("sar_flipped")
    gd = strk("D")
    if pos["D"] == up and gd >= CONFIRMED_MIN_STREAKS["D"]:
        if strict_confirmed_alignment(states, direction) and score_passes_threshold(r.get("weighted_pct"), direction):
            return "confirme"
        return None
    if pos["D"] == up and gd == 0 and sar_dir == up:
        return "amorce"
    if pos["D"] in (0, -up) and gd == 0 and sar_flip and sar_dir == up:
        return "anticipe"
    return None


def build_telegram_message(rows: list[dict], all_rows: list[dict] | None = None,
                           vivier_state: dict | None = None,
                           vivier_signals: list[dict] | None = None) -> str | None:
    # Group BULL together and BEAR together (strongest signal_state first),
    # and within each group rank by conviction — strongest |score| first —
    # instead of a flat descending sort that buries the strongest BEAR
    # (-100%) at the bottom, after the weaker BULL signals.
    all_ordered = sorted(rows, key=lambda r: (-r["signal_state"], -abs(r["weighted_pct"])))
    # RENKO FIBO ne retient QUE le profil de confluence maximale: prix H1
    # au-dela des 3 streaks D/W/M (🟢3 / 🔴3) ET SAR H1 aligne avec le biais.
    all_ordered = [r for r in all_ordered if sar_streak_full(r)]
    all_ordered_pairs = {r["pair"] for r in all_ordered}
    vivier_signals = vivier_signals or []
    transitions = [s for s in vivier_signals if s["pair"] in all_ordered_pairs]
    transition_pairs = {s["pair"] for s in transitions}
    vivier_signals = [s for s in vivier_signals if s["pair"] not in transition_pairs]
    # A direct VIVIER -> RENKO FIBO transition is rendered only in its dedicated
    # section, never duplicated in the regular RENKO FIBO or SIGNAL VIVIER list.
    ordered = [r for r in all_ordered if r["pair"] not in transition_pairs]

    # Section "RETOURNEMENTS": Telegram ne doit envoyer que les confirmes.
    # Les niveaux anticipe/amorce restent calculables, mais silencieux.
    shown = all_ordered_pairs
    turns: list[tuple[int, str, str]] = []   # (direction, tier, pair)
    for r in (all_rows if all_rows is not None else rows):
        if r["pair"] in shown:
            continue
        for d in (1, -1):
            tier = turn_tier(r, d)
            if tier in TELEGRAM_TURN_TIERS:
                turns.append((d, tier, r["pair"]))
                break

    bull_vivier, bear_vivier = vivier_groups(vivier_state or {})

    # Aucun signal, retournement ou suivi VIVIER -> aucun message.
    if (not ordered and not transitions and not turns and not bull_vivier
            and not bear_vivier and not vivier_signals):
        return None
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

    has_content = bool(ordered)
    if transitions:
        if has_content:
            lines.append("")
        lines.append("🚀 VIVIER → RENKO FIBO")
        for signal in transitions:
            direction = signal["direction"]
            icon = "🟢" if direction == 1 else "🔴"
            score = signal.get("weighted_pct")
            score_txt = f"{score:+.0f}%" if isinstance(score, (int, float)) else "?"
            fib = signal.get("fib_position", "Fibo ?").removeprefix("Fibo ")
            lines.append(f"{icon} {signal['pair']} ({score_txt} | {fib})")
        has_content = True

    if turns:
        if has_content:
            lines.append("")
        lines.append("🔄 RETOURNEMENTS")
        for d, tier, pair in sorted(turns, key=lambda x: (TURN_ORDER[x[1]], -x[0], x[2])):
            icon = "🟢" if d == 1 else "🔴"
            lines.append(f"{icon} {pair} · {TURN_TIERS[tier]}")
        has_content = True

    if vivier_signals:
        if has_content:
            lines.append("")
        lines.append("🚨 SIGNAL VIVIER")
        for signal in vivier_signals:
            direction = signal["direction"]
            icon = "🟢" if direction == 1 else "🔴"
            score = signal.get("weighted_pct")
            score_txt = f" · {score:+.0f}%" if isinstance(score, (int, float)) else ""
            lines.append(f"{icon} {signal['pair']} · M/W/D alignés{score_txt}")
        has_content = True

    for direction, title, entries in (
        (1, "🌱 VIVIER BULL", bull_vivier),
        (-1, "🌱 VIVIER BEAR", bear_vivier),
    ):
        if not entries:
            continue
        if has_content:
            lines.append("")
        lines.append(title)
        icon = "🟢" if direction == 1 else "🔴"
        for pair, entry in entries:
            score = vivier_base_score(entry)
            fib = entry.get("fib_position", "Fibo ?").removeprefix("Fibo ")
            flame = " 🔥" if entry.get("sar_flame") else ""
            lines.append(f"{icon} {pair} ({score:+.0f}% | {fib}){flame}")
        has_content = True

    # Message: RENKO FIBO, retournements, suivi VIVIER et horodatage.
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

    previous_vivier = load_vivier_state(args.vivier_state)
    vivier_state, vivier_signals = update_vivier(rows, previous_vivier)
    strong_rows = filter_strong_signals(rows)
    run_events = collect_vivier_run_events(
        previous_vivier,
        vivier_state,
        vivier_signals,
        strong_rows,
        rows,
    )
    new_event_count = append_vivier_event_journal(run_events, args.vivier_events)
    performance = update_vivier_performance(
        load_vivier_performance(args.vivier_performance),
        run_events,
        rows,
    )
    save_vivier_performance(performance, args.vivier_performance)
    save_vivier_state(vivier_state, args.vivier_state)
    print_vivier_report(vivier_state, vivier_signals)
    tracker_summary = performance["summary"]
    print(
        "VIVIER TRACKER: "
        f"{new_event_count} événement(s) ajouté(s), "
        f"{tracker_summary['total']} suivi(s), "
        f"{tracker_summary['complete']} complet(s) à 72h"
    )

    message = build_telegram_message(
        strong_rows,
        rows,
        vivier_state=vivier_state,
        vivier_signals=vivier_signals,
    )
    if message is None:
        print("\nRENKO FIBO vide — aucun message Telegram envoyé.")
        return 0
    print("")
    print(message)
    if args.no_telegram:
        print("Telegram: envoi désactivé (--no-telegram).")
    else:
        send_telegram_message(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
