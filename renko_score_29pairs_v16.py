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
VIVIER_STATE_VERSION = 2
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
FIB_TRANSITION_MIN_TRADING_DAYS = 5
FIB_TRANSITION_MIN_RANGE_RATIO = 0.60
FIB_TRANSITION_FORCE_CURRENT_DAY = 11
VIVIER_EVENTS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "renko_vivier_events.jsonl"
)
VIVIER_PERFORMANCE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "renko_vivier_performance.json"
)
VIVIER_PIPS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "renko_vivier_pips.json"
)
VIVIER_PIPS_VERSION = 1
VIVIER_PIPS_START_HOUR_PARIS = 7
VIVIER_PIPS_END_HOUR_PARIS = 23
VIVIER_PIPS_DAY_RESULT_EPSILON = 0.05
VIVIER_PERFORMANCE_VERSION = 3
VIVIER_PERFORMANCE_HOURS = (1, 4, 12, 24, 72)
VIVIER_PERFORMANCE_WINDOW_H1 = max(VIVIER_PERFORMANCE_HOURS)
# A position is considered net profitable only after this configurable buffer.
# The default 0.02% is a conservative proxy for spread/slippage when bid/ask
# history is unavailable from the standard TradingView candles.
VIVIER_BREAK_EVEN_PCT = float(os.getenv("VIVIER_BREAK_EVEN_PCT", "0.02"))
VIVIER_PAIR_PROFILE_MIN_COMPLETE = 20
VIVIER_ACTIONABLE_EVENT_TYPES = {
    "VIVIER_ENTRY",
    "SAR_FLAME",
    "SIGNAL_VIVIER",
    "VIVIER_TO_RENKO_FIBO",
}
FIBO_CURRENCY_MIN_PAIRS = 2
FIBO_CURRENCY_TOP_N = 2
FIBO_SAR_CONTRADICTION_FACTOR = 0.5
SAR_FLAME_LABELS = {
    "FIRST": "🔥1",
    "RECORD": "🔥R",
    "RESET": "🔥↻",
}


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
    parser.add_argument("--force-telegram", action="store_true", help="Send the Telegram message even if the body is unchanged.")
    parser.add_argument("--vivier-state", default=VIVIER_STATE_PATH, help="Path to the persistent VIVIER state file.")
    parser.add_argument("--vivier-events", default=VIVIER_EVENTS_PATH, help="Append-only VIVIER event journal.")
    parser.add_argument("--vivier-performance", default=VIVIER_PERFORMANCE_PATH, help="VIVIER performance tracker.")
    parser.add_argument("--vivier-pips", default=VIVIER_PIPS_PATH, help="Persistent daily/weekly/monthly VIVIER pip tracker.")
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


def monthly_fib_transition_context(df: pd.DataFrame) -> dict | None:
    """Select the stable Fibo range used around an UTC month reset.

    The completed previous month remains authoritative until the current
    month has at least five trading days, 60% of the previous range and both
    ranges agree on the side of 0.5. The current month is forced from day 11.
    """
    if df is None or df.empty:
        return None
    last_ts = pd.Timestamp(df.index[-1])
    month_start = last_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    previous_start = month_start - pd.offsets.MonthBegin(1)
    current_df = df[df.index >= month_start]
    previous_df = df[(df.index >= previous_start) & (df.index < month_start)]
    if current_df.empty:
        return None

    current_high = float(current_df["high"].max())
    current_low = float(current_df["low"].min())
    current_range = current_high - current_low
    if current_range <= 0:
        return None
    live_price = float(df["close"].iloc[-1])
    current_pct = (live_price - current_low) / current_range * 100.0
    current_month_utc = month_start.strftime("%Y-%m")

    previous_high = previous_low = previous_pct = previous_range = None
    if not previous_df.empty:
        previous_high = float(previous_df["high"].max())
        previous_low = float(previous_df["low"].min())
        previous_range = previous_high - previous_low
        if previous_range > 0:
            previous_pct = (live_price - previous_low) / previous_range * 100.0

    trading_dates = {
        pd.Timestamp(ts).date()
        for ts in current_df.index
        if pd.Timestamp(ts).weekday() < 5
    }
    trading_days = len(trading_dates)
    range_ratio = (
        current_range / previous_range
        if isinstance(previous_range, (int, float)) and previous_range > 0
        else None
    )
    same_side = (
        (current_pct <= 50.0) == (previous_pct <= 50.0)
        if isinstance(previous_pct, (int, float)) else None
    )
    early_relay = bool(
        trading_days >= FIB_TRANSITION_MIN_TRADING_DAYS
        and isinstance(range_ratio, (int, float))
        and range_ratio >= FIB_TRANSITION_MIN_RANGE_RATIO
        and same_side is True
    )
    forced_relay = last_ts.day >= FIB_TRANSITION_FORCE_CURRENT_DAY
    previous_available = (
        isinstance(previous_low, (int, float))
        and isinstance(previous_high, (int, float))
        and isinstance(previous_pct, (int, float))
        and isinstance(previous_range, (int, float))
        and previous_range > 0
    )
    use_current = not previous_available or early_relay or forced_relay
    if use_current:
        month_high = current_high
        month_low = current_low
        month_utc = current_month_utc
        pct_of_range = current_pct
        source = "CURRENT"
        relay_reason = (
            "NO_PREVIOUS" if not previous_available
            else "FORCED_DAY_11" if forced_relay
            else "MATURE_AGREEMENT"
        )
    else:
        month_high = float(previous_high)
        month_low = float(previous_low)
        month_utc = previous_start.strftime("%Y-%m")
        pct_of_range = float(previous_pct)
        source = "PREVIOUS"
        relay_reason = "PREVIOUS_TRANSITION"

    fib50 = month_low + (month_high - month_low) * 0.5
    position = "ABOVE" if live_price > fib50 else ("BELOW" if live_price < fib50 else "AT")
    return {
        "fib50": fib50,
        "position": position,
        "live_price": live_price,
        "month_high": month_high,
        "month_low": month_low,
        "month_utc": month_utc,
        "pct_of_range": pct_of_range,
        "effective_fib_source": source,
        "transition_active": source == "PREVIOUS",
        "transition_relay_reason": relay_reason,
        "transition_trading_days": trading_days,
        "transition_range_ratio": range_ratio,
        "transition_same_side": same_side,
        "current_month_high": current_high,
        "current_month_low": current_low,
        "current_month_utc": current_month_utc,
        "current_pct_of_range": current_pct,
        "previous_month_high": previous_high,
        "previous_month_low": previous_low,
        "previous_month_utc": previous_start.strftime("%Y-%m"),
        "previous_pct_of_range": previous_pct,
    }


def compute_h1_month_fib(pair: str, h1_candles: int = 800) -> dict | None:
    """Build the effective monthly Fibo plus H1 SAR timing for one pair."""
    df = fetch_tv_ohlc(f"OANDA:{pair}", "60", h1_candles)
    context = monthly_fib_transition_context(df)
    if context is None:
        return None
    fib50 = float(context["fib50"])

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
        closed_extreme = {
            "time_utc": (closed_ts + pd.Timedelta(hours=1)).isoformat(),
            "bar_time_utc": closed_ts.isoformat(),
            "month_utc": closed_month_start.strftime("%Y-%m"),
            "high": float(closed_df["high"].iloc[-1]),
            "low": float(closed_df["low"].iloc[-1]),
        }
        if not before_closed.empty:
            closed_extreme["fib1_before"] = float(before_closed["high"].max())
            closed_extreme["fib0_before"] = float(before_closed["low"].min())
    if cross_event is not None:
        cross_event["fib50"] = fib50

    closed_sar_values = (closed_sar_state or {}).get("sar") or []
    closed_sar_trend = (closed_sar_state or {}).get("trend") or []
    closed_bars = []
    for index, (ts, bar) in enumerate(closed_df.iterrows()):
        closed_bars.append({
            "time_utc": (pd.Timestamp(ts) + pd.Timedelta(hours=1)).isoformat(),
            "high": float(bar["high"]),
            "low": float(bar["low"]),
            "close": float(bar["close"]),
            "sar_value": (
                float(closed_sar_values[index])
                if index < len(closed_sar_values) else None
            ),
            "sar_dir": (
                int(closed_sar_trend[index])
                if index < len(closed_sar_trend) else 0
            ),
        })
    closed_time = closed_bars[-1]["time_utc"] if closed_bars else None
    closed_price = closed_bars[-1]["close"] if closed_bars else None
    closed_month_utc = (
        pd.Timestamp(closed_df.index[-1]).strftime("%Y-%m")
        if not closed_df.empty else None
    )

    return {
        **context,
        "sar_dir": sar_dir,
        "sar_flipped": sar_flipped,
        "sar_value": sar_values[-1] if sar_values else None,
        "sar_prev_value": sar_values[-2] if sar_values and len(sar_values) >= 2 else None,
        "sar_cross_event": cross_event,
        "closed_extreme": closed_extreme,
        "h1_closed_time_utc": closed_time,
        "h1_closed_price": closed_price,
        "h1_closed_month_utc": closed_month_utc,
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

    Monthly must be strictly outside its Renko brick. Entry is allowed after
    either a strict W/D opposition, or when Weekly is already aligned with
    Monthly while Daily is Inside. BULL entries must be in the lower half of
    the monthly H1 Fibonacci range, BEAR entries in the upper half.
    """
    px = _vivier_px(row)
    if px is None or px["M"] not in (-1, 1):
        return 0
    direction = px["M"]
    has_strict_opposition = px["W"] == -direction or px["D"] == -direction
    has_aligned_weekly_inside_daily = px["W"] == direction and px["D"] == 0
    entry_profile_ok = has_strict_opposition or has_aligned_weekly_inside_daily
    score_ok = abs(_base_score_from_px(px)) >= VIVIER_MIN_ABS_BASE_SCORE
    fib_pct = (row.get("h1_fib") or {}).get("pct_of_range")
    fib_ok = isinstance(fib_pct, (int, float)) and (
        (direction == 1 and fib_pct <= VIVIER_FIB_MIDPOINT_PCT)
        or (direction == -1 and fib_pct >= VIVIER_FIB_MIDPOINT_PCT)
    )
    return direction if entry_profile_ok and score_ok and fib_ok else 0


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
        return {"version": VIVIER_STATE_VERSION, "pairs": {},
                "pending_objectives": {}}
    except Exception as exc:
        print(f"VIVIER: impossible de lire l'etat ({exc}); demarrage avec un etat vide.")
        return {"version": VIVIER_STATE_VERSION, "pairs": {},
                "pending_objectives": {}}

    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    if not isinstance(pairs, dict):
        return {"version": VIVIER_STATE_VERSION, "pairs": {},
                "pending_objectives": {}}
    valid_pairs = {
        str(pair): dict(entry)
        for pair, entry in pairs.items()
        if isinstance(entry, dict) and entry.get("direction") in (-1, 1)
    }
    pending = payload.get("pending_objectives")
    valid_pending = {
        str(pair): dict(entry)
        for pair, entry in (pending.items() if isinstance(pending, dict) else [])
        if (isinstance(entry, dict)
            and entry.get("direction") in (-1, 1)
            and isinstance(entry.get("value"), (int, float)))
    }
    state = {"version": VIVIER_STATE_VERSION, "pairs": valid_pairs,
             "pending_objectives": valid_pending}
    for key in ("telegram_last_body_hash", "telegram_last_sent_at_paris"):
        value = payload.get(key)
        if isinstance(value, str):
            state[key] = value
    return state


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


def _set_vivier_fib_objective(entry: dict, row: dict, direction: int) -> bool:
    """Start one fixed directional Fibonacci objective for the active cycle."""
    h1 = row.get("h1_fib") or {}
    value = h1.get("month_high" if direction == 1 else "month_low")
    month_utc = h1.get("month_utc")
    entry["fib_context_month_utc"] = month_utc
    entry["fib_objective_active"] = isinstance(value, (int, float))
    entry["fib_objective_carried"] = False
    for key in (
        "fib_objective_carried_at_time_utc",
        "fib_objective_carried_from_month_utc",
        "fib_objective_carried_since_month_utc",
    ):
        entry.pop(key, None)
    if not entry["fib_objective_active"]:
        entry.pop("fib_objective_value", None)
        entry.pop("fib_objective_origin_month_utc", None)
        return False
    entry["fib_objective_value"] = float(value)
    entry["fib_objective_origin_month_utc"] = month_utc
    entry["fib_objective_started_time_utc"] = h1.get("h1_closed_time_utc")
    return True


def update_vivier_fib_objective_month(entry: dict, row: dict,
                                       direction: int) -> None:
    """Carry one untouched objective across UTC month resets."""
    h1 = row.get("h1_fib") or {}
    current_month = h1.get("month_utc") or h1.get("h1_closed_month_utc")
    if "fib_objective_active" not in entry:
        _set_vivier_fib_objective(entry, row, direction)
        return
    previous_month = entry.get("fib_context_month_utc")
    if not current_month:
        return
    if not previous_month:
        entry["fib_context_month_utc"] = current_month
        return
    if current_month == previous_month:
        return
    if (entry.get("fib_objective_active")
            and isinstance(entry.get("fib_objective_value"), (int, float))
            and not entry.get("fib_objective_carried")):
        entry["fib_objective_carried"] = True
        entry["fib_objective_carried_from_month_utc"] = (
            entry.get("fib_objective_origin_month_utc") or previous_month
        )
        entry["fib_objective_carried_since_month_utc"] = current_month
        entry["fib_objective_carried_at_time_utc"] = h1.get("h1_closed_time_utc")
    entry["fib_context_month_utc"] = current_month


def _pending_objective_from_entry(entry: dict, row: dict,
                                  direction: int) -> dict | None:
    """Detach an untouched active objective after full M/W/D alignment."""
    value = entry.get("fib_objective_value")
    if not entry.get("fib_objective_active") or not isinstance(value, (int, float)):
        return None
    h1 = row.get("h1_fib") or {}
    signal_price = h1.get("h1_closed_price", row.get("h1_price", row.get("live_price")))
    return {
        "direction": direction,
        "value": float(value),
        "origin_month_utc": entry.get("fib_objective_origin_month_utc"),
        "started_time_utc": entry.get("fib_objective_started_time_utc"),
        "signal_time_utc": h1.get("h1_closed_time_utc"),
        "signal_price": float(signal_price) if isinstance(signal_price, (int, float)) else None,
        "signal_base_pct": row.get("base_pct"),
        "signal_weighted_pct": row.get("weighted_pct"),
        "signal_fib_position": fib_directional_label(h1, direction),
        "signal_px": row.get("px"),
        "post_alignment_time_utc": h1.get("h1_closed_time_utc"),
        "post_alignment_month_utc": h1.get("month_utc"),
        "was_carried_before_alignment": bool(entry.get("fib_objective_carried")),
        "source_entered_at_paris": entry.get("entered_at_paris"),
    }


def _pending_objective_touched(pending: dict, row: dict) -> tuple[bool, str | None]:
    extreme = (row.get("h1_fib") or {}).get("closed_extreme") or {}
    value = pending.get("value")
    direction = pending.get("direction")
    if not isinstance(value, (int, float)) or direction not in (-1, 1):
        return False, None
    price = extreme.get("high" if direction == 1 else "low")
    touched = isinstance(price, (int, float)) and (
        price >= value if direction == 1 else price <= value
    )
    return touched, extreme.get("time_utc") if touched else None


def _attach_pending_objective(entry: dict, pending: dict, row: dict) -> None:
    """Reuse the same post-alignment target on a same-direction re-entry."""
    h1 = row.get("h1_fib") or {}
    entry["fib_objective_active"] = True
    entry["fib_objective_value"] = float(pending["value"])
    entry["fib_objective_origin_month_utc"] = pending.get("origin_month_utc")
    entry["fib_objective_started_time_utc"] = pending.get("started_time_utc")
    entry["fib_objective_carried"] = True
    entry["fib_context_month_utc"] = h1.get("month_utc")
    entry["fib_objective_reattached_from_post_alignment"] = True
    entry["fib_objective_post_alignment_time_utc"] = pending.get(
        "post_alignment_time_utc"
    )


def apply_vivier_sar_record(entry: dict, row: dict, direction: int) -> None:
    """Set a one-run flame when a valid H1 SAR event makes a new record."""
    entry["sar_flame"] = False
    entry.pop("sar_flame_kind", None)
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
    if entry.get("sar_rearmed_after_fib_reset"):
        entry["sar_flame_kind"] = "RESET"
        entry["sar_rearmed_after_fib_reset"] = False
    elif previous is None:
        entry["sar_flame_kind"] = "FIRST"
    else:
        entry["sar_flame_kind"] = "RECORD"
    if not entry.get("fib_objective_active"):
        _set_vivier_fib_objective(entry, row, direction)


def apply_vivier_fib_extreme_reset(entry: dict, row: dict, direction: int) -> bool:
    """Complete the fixed objective and rearm SAR, including after month reset."""
    if not entry.get("fib_objective_active"):
        return False
    extreme = (row.get("h1_fib") or {}).get("closed_extreme") or {}
    bar_time = extreme.get("time_utc")
    if not bar_time or bar_time == entry.get("fib_last_objective_touch_time_utc"):
        return False
    objective = entry.get("fib_objective_value")
    if not isinstance(objective, (int, float)):
        return False
    if direction == 1:
        high = extreme.get("high")
        touched = isinstance(high, (int, float)) and high >= objective
        reason = "FIB1_TOUCH"
    else:
        low = extreme.get("low")
        touched = isinstance(low, (int, float)) and low <= objective
        reason = "FIB0_TOUCH"
    if not touched:
        return False
    was_carried = bool(entry.get("fib_objective_carried"))
    origin_month = entry.get("fib_objective_origin_month_utc")
    entry["fib_last_objective_touch_time_utc"] = bar_time
    entry["fib_last_objective_touch_value"] = float(objective)
    entry["fib_last_objective_touch_reason"] = reason
    entry["fib_last_objective_touch_was_carried"] = was_carried
    entry["fib_last_objective_origin_month_utc"] = origin_month
    entry["fib_objective_active"] = False
    entry["fib_objective_carried"] = False
    entry.pop("fib_objective_value", None)
    entry.pop("fib_objective_origin_month_utc", None)
    cross_event = (row.get("h1_fib") or {}).get("sar_cross_event") or {}
    if cross_event.get("time_utc") == bar_time:
        entry["sar_last_processed_time_utc"] = bar_time
    had_sar_record = entry.get("sar_record_value") is not None
    if had_sar_record:
        entry.pop("sar_record_value", None)
        entry.pop("sar_record_time_utc", None)
        entry["sar_rearmed_after_fib_reset"] = True
        entry["sar_last_fib_reset_time_utc"] = bar_time
        entry["sar_last_fib_reset_reason"] = reason
    return True


def transition_entry_fib_invalid(entry: dict, row: dict, direction: int) -> bool:
    """Revalidate entries created during an unstable month-reset window."""
    # Entries created after the transition logic was deployed were already
    # validated with their effective Fibo. Fibo remains an entry filter only;
    # this migration guard is exclusively for legacy entries with no source.
    if entry.get("entry_fib_source") in ("PREVIOUS", "CURRENT"):
        return False
    h1 = row.get("h1_fib") or {}
    if not h1.get("transition_active"):
        return False
    current_month = h1.get("current_month_utc")
    entered_at = str(entry.get("entered_at_paris") or "")
    if not current_month or entered_at[:7] != current_month:
        return False
    pct = h1.get("pct_of_range")
    if not isinstance(pct, (int, float)):
        return False
    return (direction == 1 and pct > VIVIER_FIB_MIDPOINT_PCT) or (
        direction == -1 and pct < VIVIER_FIB_MIDPOINT_PCT
    )


def update_vivier(rows: list[dict], previous_state: dict | None = None,
                   now: datetime | None = None) -> tuple[dict, list[dict]]:
    """Advance the persistent VIVIER state and return one-shot alignments.

    Entry accepts strict opposition to Monthly, plus an aligned M/W with Daily
    Inside. Once tracked, a pair remains in the pool while W/D pass through
    Inside. It leaves when M is no longer in its original strict direction, or
    emits a one-shot signal when M/W/D become strictly aligned in that
    direction. One untouched Fibo 1/0 objective is carried across UTC month
    resets until reached. Missing rows are kept so a data error cannot erase
    the pool.
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
        entry.pop("sar_flame_kind", None)
    old_pending = (previous_state or {}).get("pending_objectives") or {}
    pending_objectives = {
        str(pair): dict(entry)
        for pair, entry in old_pending.items()
        if (isinstance(entry, dict) and entry.get("direction") in (-1, 1)
            and isinstance(entry.get("value"), (int, float)))
    }
    objective_events: list[dict] = []
    signals: list[dict] = []

    for row in rows:
        pair = str(row.get("pair") or "")
        px = _vivier_px(row)
        if not pair or px is None:
            continue

        existing = tracked.get(pair)
        if existing is not None:
            direction = int(existing["direction"])
            if transition_entry_fib_invalid(existing, row, direction):
                del tracked[pair]
                continue
            if vivier_full_alignment(row, direction):
                pending = _pending_objective_from_entry(existing, row, direction)
                if pending is not None:
                    pending_objectives[pair] = pending
                    objective_events.append({
                        "event_type": "POST_ALIGNMENT_OBJECTIVE_CREATED",
                        "pair": pair,
                        "direction": direction,
                        "time_utc": pending.get("post_alignment_time_utc"),
                        "objective_value": pending["value"],
                        "origin_month_utc": pending.get("origin_month_utc"),
                    })
                    reached, reached_time = _pending_objective_touched(pending, row)
                    if reached:
                        objective_events.append({
                            "event_type": "POST_ALIGNMENT_OBJECTIVE_REACHED",
                            "pair": pair,
                            "direction": direction,
                            "time_utc": reached_time,
                            "objective_value": pending["value"],
                            "origin_month_utc": pending.get("origin_month_utc"),
                        })
                        del pending_objectives[pair]
                signals.append({
                    "pair": pair,
                    "direction": direction,
                    "px": px,
                    "entered_at_paris": existing.get("entered_at_paris"),
                    "signaled_at_paris": stamp,
                    "base_pct": row.get("base_pct"),
                    "weighted_pct": row.get("weighted_pct"),
                    "fib_position": fib_directional_label(row.get("h1_fib"), direction),
                    "objective_value": (
                        pending.get("value") if pending is not None
                        else existing.get("fib_objective_value")
                    ),
                })
                del tracked[pair]
                continue
            if px["M"] == direction:
                if abs(_base_score_from_px(px)) < VIVIER_MIN_ABS_BASE_SCORE:
                    del tracked[pair]
                    continue
                existing.setdefault("entry_fib_position", existing.get("fib_position"))
                existing.setdefault("entry_fib_pct_of_range", existing.get("fib_pct_of_range"))
                existing["last_seen_at_paris"] = stamp
                existing["last_px"] = px
                existing["base_pct"] = row.get("base_pct")
                existing["weighted_pct"] = row.get("weighted_pct")
                existing["fib_position"] = fib_directional_label(row.get("h1_fib"), direction)
                existing["fib_pct_of_range"] = (row.get("h1_fib") or {}).get("pct_of_range")
                existing["fib_source"] = (row.get("h1_fib") or {}).get(
                    "effective_fib_source"
                )
                sar_dir = (row.get("h1_fib") or {}).get("sar_dir")
                if sar_dir in (-1, 1):
                    existing["sar_dir"] = int(sar_dir)
                update_vivier_fib_objective_month(existing, row, direction)
                objective_touched_now = apply_vivier_fib_extreme_reset(
                    existing, row, direction
                )
                if not objective_touched_now:
                    apply_vivier_sar_record(existing, row, direction)
                continue

            # Monthly became Inside or reversed: invalidate the old pool.
            del tracked[pair]

        pending = pending_objectives.get(pair)
        if pending is not None:
            pending_direction = int(pending["direction"])
            event_time = (row.get("h1_fib") or {}).get("h1_closed_time_utc")
            if px["M"] != pending_direction:
                objective_events.append({
                    "event_type": "POST_ALIGNMENT_OBJECTIVE_CANCELED",
                    "pair": pair,
                    "direction": pending_direction,
                    "time_utc": event_time,
                    "objective_value": pending["value"],
                    "reason": "MONTHLY_INVALID",
                })
                del pending_objectives[pair]
                pending = None
            else:
                touched, touch_time = _pending_objective_touched(pending, row)
                if touched:
                    objective_events.append({
                        "event_type": "POST_ALIGNMENT_OBJECTIVE_REACHED",
                        "pair": pair,
                        "direction": pending_direction,
                        "time_utc": touch_time,
                        "objective_value": pending["value"],
                        "origin_month_utc": pending.get("origin_month_utc"),
                    })
                    del pending_objectives[pair]
                    pending = None

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
                "fib_source": (row.get("h1_fib") or {}).get("effective_fib_source"),
                "entry_fib_position": fib_directional_label(row.get("h1_fib"), direction),
                "entry_fib_pct_of_range": (row.get("h1_fib") or {}).get("pct_of_range"),
                "entry_fib_source": (row.get("h1_fib") or {}).get(
                    "effective_fib_source"
                ),
                "sar_dir": (row.get("h1_fib") or {}).get("sar_dir"),
            }
            reusable = pending_objectives.get(pair)
            if reusable is not None and reusable.get("direction") == direction:
                _attach_pending_objective(tracked[pair], reusable, row)
                objective_events.append({
                    "event_type": "POST_ALIGNMENT_OBJECTIVE_REATTACHED",
                    "pair": pair,
                    "direction": direction,
                    "time_utc": (row.get("h1_fib") or {}).get("h1_closed_time_utc"),
                    "objective_value": reusable["value"],
                    "origin_month_utc": reusable.get("origin_month_utc"),
                })
                del pending_objectives[pair]
            else:
                _set_vivier_fib_objective(tracked[pair], row, direction)
            apply_vivier_sar_record(tracked[pair], row, direction)

    return {
        "version": VIVIER_STATE_VERSION,
        "updated_at_paris": stamp,
        "pairs": dict(sorted(tracked.items())),
        "pending_objectives": dict(sorted(pending_objectives.items())),
        "objective_events": objective_events,
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


def _strip_fibo_prefix(value: str | None) -> str:
    return (value or "Fibo ?").removeprefix("Fibo ")


def vivier_fib_path(entry: dict) -> str:
    """Entry Fibo -> current Fibo, compact for Telegram."""
    current = _strip_fibo_prefix(entry.get("fib_position"))
    initial = _strip_fibo_prefix(
        entry.get("entry_fib_position") or entry.get("fib_position")
    )
    return f"{initial}→{current}" if initial != current else current


def _parse_paris_minute(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=PARIS_TZ)
    except (TypeError, ValueError):
        return None


def vivier_age_label(entry: dict, now: datetime | None = None) -> str:
    entered = _parse_paris_minute(entry.get("entered_at_paris"))
    if entered is None:
        return ""
    ref = (now or datetime.now(PARIS_TZ)).astimezone(PARIS_TZ)
    days = max(0, (ref.date() - entered.date()).days)
    return f"J+{days}"


def vivier_flame_label(entry: dict) -> str:
    if not entry.get("sar_flame"):
        return ""
    return SAR_FLAME_LABELS.get(entry.get("sar_flame_kind"), "🔥")


def _format_vivier_entry_line(pair: str, entry: dict, now: datetime | None = None) -> str:
    direction = int(entry.get("direction", 1))
    icon = "🟢" if direction == 1 else "🔴"
    score = vivier_base_score(entry)
    parts = [f"{score:+.0f}%", vivier_fib_path(entry)]
    if entry.get("fib_source") == "PREVIOUS":
        parts.append("M-1")
    age = vivier_age_label(entry, now)
    if age:
        parts.append(age)
    line = f"{icon} {pair} ({' | '.join(parts)})"
    flame = vivier_flame_label(entry)
    return f"{line} {flame}" if flame else line


def _format_telegram_vivier_entry_line(pair: str, entry: dict) -> str:
    """Compact Telegram line: current score, current Fibo and active flame."""
    direction = int(entry.get("direction", 1))
    icon = "🟢" if direction == 1 else "🔴"
    sar_icon = {1: "🟢", -1: "🔴"}.get(entry.get("sar_dir"), "⚪")
    score = vivier_base_score(entry)
    current_fib = _strip_fibo_prefix(entry.get("fib_position"))
    line = f"{icon}{sar_icon} {pair} ({score:+.0f}% | {current_fib})"
    flame = vivier_flame_label(entry)
    return f"{line} {flame}" if flame else line


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


def near_alignment_entries(state: dict) -> list[tuple[str, dict]]:
    """Active VIVIER pairs where M/W already match the pool direction."""
    pairs = state.get("pairs") or {}
    entries = []
    for pair, entry in pairs.items():
        direction = entry.get("direction")
        px = entry.get("last_px") or {}
        if direction not in (-1, 1):
            continue
        if px.get("M") == direction and px.get("W") == direction and px.get("D") != direction:
            entries.append((pair, entry))
    return sorted(entries, key=lambda item: (-abs(vivier_base_score(item[1])), item[0]))


def _compact_price(value: float | int | None) -> str:
    if not isinstance(value, (int, float)):
        return "?"
    return f"{float(value):.5f}".rstrip("0").rstrip(".")


def _row_current_price(row: dict | None) -> float | None:
    if row is None:
        return None
    h1 = row.get("h1_fib") or {}
    value = h1.get("h1_closed_price", row.get("h1_price", row.get("live_price")))
    return float(value) if isinstance(value, (int, float)) else None


def post_signal_tracking_entries(state: dict, all_rows: list[dict] | None = None) -> list[dict]:
    """Pending post-alignment Fibo objectives, enriched with current PnL."""
    pending = state.get("pending_objectives") or {}
    rows_by_pair = {row.get("pair"): row for row in (all_rows or [])}
    result: list[dict] = []
    for pair, objective in pending.items():
        direction = objective.get("direction")
        if direction not in (-1, 1):
            continue
        row = rows_by_pair.get(pair)
        current_price = _row_current_price(row)
        signal_price = objective.get("signal_price")
        directional_pct = None
        if (isinstance(current_price, (int, float))
                and isinstance(signal_price, (int, float))
                and signal_price != 0):
            directional_pct = ((current_price - signal_price) / signal_price
                               * 100.0 * int(direction))
        result.append({
            "pair": pair,
            "direction": int(direction),
            "objective_value": objective.get("value"),
            "objective_label": "F1" if direction == 1 else "F0",
            "directional_pct": directional_pct,
            "signal_weighted_pct": objective.get("signal_weighted_pct"),
            "sar_dir": ((row or {}).get("h1_fib") or {}).get("sar_dir"),
        })
    return sorted(result, key=lambda item: (
        -abs(item["directional_pct"]) if isinstance(item["directional_pct"], (int, float)) else 0,
        item["pair"],
    ))


def print_vivier_report(state: dict, signals: list[dict]) -> None:
    bull, bear = vivier_groups(state)
    print("\nVIVIER RENKO")
    print(f"BULL ({len(bull)}): " + (", ".join(pair for pair, _ in bull) or "--"))
    for pair, entry in bull:
        print(f"  {_format_vivier_entry_line(pair, entry)}  {_px_compact(entry.get('last_px'))}")
    print(f"BEAR ({len(bear)}): " + (", ".join(pair for pair, _ in bear) or "--"))
    for pair, entry in bear:
        print(f"  {_format_vivier_entry_line(pair, entry)}  {_px_compact(entry.get('last_px'))}")
    near = near_alignment_entries(state)
    if near:
        print("PROCHE ALIGNEMENT:")
        for pair, entry in near:
            print(f"  {_format_vivier_entry_line(pair, entry)}  D restant")
    pending = state.get("pending_objectives") or {}
    if pending:
        print(f"OBJECTIFS POST-ALIGNEMENT ({len(pending)}):")
        for pair, objective in sorted(pending.items()):
            label = "BULL F1" if objective.get("direction") == 1 else "BEAR F0"
            print(f"  {pair:<8} {label:<8} {objective.get('value')}")
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
                objective_value=entry.get("fib_objective_value"),
            ))
        if entry.get("sar_flame"):
            events.append(_build_vivier_event(
                "SAR_FLAME", pair, int(entry["direction"]), rows_by_pair.get(pair),
                time_utc=entry.get("sar_record_time_utc"),
                sar_value=entry.get("sar_record_value"),
                flame_kind=entry.get("sar_flame_kind"),
                objective_value=entry.get("fib_objective_value"),
            ))
        carried_time = entry.get("fib_objective_carried_at_time_utc")
        if carried_time and carried_time != (old or {}).get("fib_objective_carried_at_time_utc"):
            carried_value = entry.get("fib_objective_value")
            if not isinstance(carried_value, (int, float)):
                carried_value = entry.get("fib_last_objective_touch_value")
            events.append(_build_vivier_event(
                "FIB_OBJECTIVE_CARRIED", pair, int(entry["direction"]),
                rows_by_pair.get(pair), time_utc=carried_time,
                objective_value=carried_value,
                origin_month_utc=entry.get("fib_objective_carried_from_month_utc"),
                current_month_utc=entry.get("fib_objective_carried_since_month_utc"),
            ))
        touch_time = entry.get("fib_last_objective_touch_time_utc")
        if touch_time and touch_time != (old or {}).get("fib_last_objective_touch_time_utc"):
            events.append(_build_vivier_event(
                "FIB_OBJECTIVE_TOUCH", pair, int(entry["direction"]),
                rows_by_pair.get(pair), time_utc=touch_time,
                objective_value=entry.get("fib_last_objective_touch_value"),
                objective_was_carried=entry.get("fib_last_objective_touch_was_carried"),
                origin_month_utc=entry.get("fib_last_objective_origin_month_utc"),
                reason=entry.get("fib_last_objective_touch_reason"),
            ))
        reset_time = entry.get("sar_last_fib_reset_time_utc")
        if reset_time and reset_time != (old or {}).get("sar_last_fib_reset_time_utc"):
            events.append(_build_vivier_event(
                "SAR_RECORD_RESET", pair, int(entry["direction"]), rows_by_pair.get(pair),
                time_utc=reset_time, reason=entry.get("sar_last_fib_reset_reason"),
            ))

    for objective_event in current_state.get("objective_events") or []:
        pair = objective_event.get("pair")
        direction = objective_event.get("direction")
        event_type = objective_event.get("event_type")
        if not pair or direction not in (-1, 1) or not event_type:
            continue
        extra = {
            key: value for key, value in objective_event.items()
            if key not in {"event_type", "pair", "direction", "time_utc"}
        }
        events.append(_build_vivier_event(
            event_type, pair, int(direction), rows_by_pair.get(pair),
            time_utc=objective_event.get("time_utc"), **extra,
        ))

    for signal in vivier_signals:
        pair = signal["pair"]
        event_type = ("VIVIER_TO_RENKO_FIBO" if pair in renko_fibo_pairs
                      else "SIGNAL_VIVIER")
        events.append(_build_vivier_event(
            event_type, pair, int(signal["direction"]), rows_by_pair.get(pair),
            entered_at_paris=signal.get("entered_at_paris"),
            objective_value=signal.get("objective_value"),
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


def _percentile(values: list[float], ratio: float) -> float | None:
    """Small dependency-free linear percentile for persistent JSON profiles."""
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = max(0.0, min(1.0, ratio)) * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _directional_return_pct(price: float, close: float, direction: int) -> float:
    return (float(close) - float(price)) / float(price) * 100.0 * int(direction)


def _update_event_path_metrics(event: dict,
                               future_bars: list[tuple[pd.Timestamp, dict]]) -> None:
    """Record the H1 path used to learn profitability and exit timing."""
    price = event.get("price")
    direction = event.get("direction")
    if not isinstance(price, (int, float)) or price == 0 or direction not in (-1, 1):
        return

    # F0/F1 may be reached after the 72-H1 learning window. Keep looking for
    # the first touch on every run until it is found, then persist it forever.
    objective = event.get("objective_value")
    if (isinstance(objective, (int, float))
            and "objective_reached_h1_bars" not in event):
        for ordinal, (ts, bar) in enumerate(future_bars, start=1):
            extreme = bar.get("high" if direction == 1 else "low")
            if not isinstance(extreme, (int, float)):
                continue
            reached = extreme >= objective if direction == 1 else extreme <= objective
            if reached:
                event["objective_reached_h1_bars"] = ordinal
                event["objective_reached_time_utc"] = ts.isoformat()
                break

    # Once the first 72 traded H1 bars are frozen, never recalculate them from
    # a shorter rolling market-data history.
    if event.get("path_complete"):
        return

    window = future_bars[:VIVIER_PERFORMANCE_WINDOW_H1]
    if not window:
        return
    returns = [
        _directional_return_pct(float(price), float(bar["close"]), int(direction))
        for _, bar in window
    ]
    event["observed_h1_bars"] = len(window)
    event["break_even_pct"] = VIVIER_BREAK_EVEN_PCT
    event["latest_directional_pct"] = returns[-1]

    first_positive = next((i for i, value in enumerate(returns, 1) if value > 0.0), None)
    first_profitable = next((
        i for i, value in enumerate(returns, 1)
        if value > VIVIER_BREAK_EVEN_PCT
    ), None)
    first_stable = next((
        i for i in range(2, len(returns) + 1)
        if returns[i - 2] > VIVIER_BREAK_EVEN_PCT
        and returns[i - 1] > VIVIER_BREAK_EVEN_PCT
    ), None)
    if first_positive is not None:
        event["first_positive_h1_bars"] = first_positive
    if first_profitable is not None:
        event["first_profitable_h1_bars"] = first_profitable
    if first_stable is not None:
        event["first_stable_profitable_h1_bars"] = first_stable

    profitable_flags = [value > VIVIER_BREAK_EVEN_PCT for value in returns]
    event["profitable_closes_pct"] = (
        sum(profitable_flags) / len(profitable_flags) * 100.0
    )
    longest = current = 0
    for profitable in profitable_flags:
        current = current + 1 if profitable else 0
        longest = max(longest, current)
    event["longest_profitable_streak_h1"] = longest

    peak_index = max(range(len(returns)), key=returns.__getitem__)
    event["peak_directional_pct"] = returns[peak_index]
    event["peak_h1_bars"] = peak_index + 1
    event["peak_time_utc"] = window[peak_index][0].isoformat()
    event["giveback_from_peak_pct"] = returns[peak_index] - returns[-1]

    first_favorable_sar = None
    first_adverse_after_favorable = None
    for ordinal, (_, bar) in enumerate(window, start=1):
        sar_dir = bar.get("sar_dir")
        if first_favorable_sar is None and sar_dir == direction:
            first_favorable_sar = ordinal
        elif first_favorable_sar is not None and sar_dir == -direction:
            first_adverse_after_favorable = ordinal
            break
    if first_favorable_sar is not None:
        event["first_favorable_sar_h1_bars"] = first_favorable_sar
    if first_adverse_after_favorable is not None:
        event["first_adverse_sar_h1_bars"] = first_adverse_after_favorable

    if len(window) >= VIVIER_PERFORMANCE_WINDOW_H1:
        event["path_complete"] = True


def _pair_profile_bucket(events: list[dict]) -> dict:
    complete = [event for event in events if event.get("path_complete")]
    first_profit = [
        float(event["first_profitable_h1_bars"])
        for event in complete if "first_profitable_h1_bars" in event
    ]
    first_stable = [
        float(event["first_stable_profitable_h1_bars"])
        for event in complete if "first_stable_profitable_h1_bars" in event
    ]
    profitable_peak_times = [
        float(event["peak_h1_bars"])
        for event in complete
        if event.get("peak_directional_pct", 0.0) > VIVIER_BREAK_EVEN_PCT
    ]
    adverse_sar_times = [
        float(event["first_adverse_sar_h1_bars"])
        for event in complete if "first_adverse_sar_h1_bars" in event
    ]
    objective_samples = [
        event for event in complete if isinstance(event.get("objective_value"), (int, float))
    ]
    objective_times = [
        float(event["objective_reached_h1_bars"])
        for event in objective_samples if "objective_reached_h1_bars" in event
    ]
    ready = len(complete) >= VIVIER_PAIR_PROFILE_MIN_COMPLETE
    exit_window = None
    if ready and profitable_peak_times:
        exit_window = {
            "start_h1": _percentile(profitable_peak_times, 0.25),
            "end_h1": _percentile(profitable_peak_times, 0.75),
        }
    return {
        "status": "READY" if ready else "LEARNING",
        "signals": len(events),
        "complete_72h": len(complete),
        "minimum_complete_required": VIVIER_PAIR_PROFILE_MIN_COMPLETE,
        "break_even_pct": VIVIER_BREAK_EVEN_PCT,
        "profitable_within_4h_pct": (
            sum(value <= 4 for value in first_profit) / len(complete) * 100.0
            if complete else None
        ),
        "profitable_within_12h_pct": (
            sum(value <= 12 for value in first_profit) / len(complete) * 100.0
            if complete else None
        ),
        "profitable_within_24h_pct": (
            sum(value <= 24 for value in first_profit) / len(complete) * 100.0
            if complete else None
        ),
        "profitable_within_72h_pct": (
            len(first_profit) / len(complete) * 100.0 if complete else None
        ),
        "median_first_profitable_h1": (
            statistics.median(first_profit) if first_profit else None
        ),
        "median_first_stable_profitable_h1": (
            statistics.median(first_stable) if first_stable else None
        ),
        "median_peak_h1": (
            statistics.median(profitable_peak_times) if profitable_peak_times else None
        ),
        "exit_window_h1": exit_window,
        "median_adverse_sar_h1": (
            statistics.median(adverse_sar_times) if adverse_sar_times else None
        ),
        "objective_samples": len(objective_samples),
        "objective_hit_rate_pct": (
            len(objective_times) / len(objective_samples) * 100.0
            if objective_samples else None
        ),
        "median_objective_h1": (
            statistics.median(objective_times) if objective_times else None
        ),
    }


def _pair_performance_profiles(events: list[dict]) -> dict:
    grouped: dict[str, list[dict]] = {}
    for event in events:
        if event.get("event_type") not in VIVIER_ACTIONABLE_EVENT_TYPES:
            continue
        pair = event.get("pair")
        if pair:
            grouped.setdefault(str(pair), []).append(event)
    profiles = {}
    for pair, pair_events in sorted(grouped.items()):
        event_types = sorted({event["event_type"] for event in pair_events})
        profiles[pair] = {
            "overall": _pair_profile_bucket(pair_events),
            "by_direction": {
                label: _pair_profile_bucket([
                    event for event in pair_events if event.get("direction") == direction
                ])
                for direction, label in ((1, "BULL"), (-1, "BEAR"))
                if any(event.get("direction") == direction for event in pair_events)
            },
            "by_signal": {
                event_type: _pair_profile_bucket([
                    event for event in pair_events if event["event_type"] == event_type
                ])
                for event_type in event_types
            },
        }
    return profiles


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

        _update_event_path_metrics(event, future_bars)
        window = [bar for _, bar in future_bars[:VIVIER_PERFORMANCE_WINDOW_H1]]
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
    return {
        "version": VIVIER_PERFORMANCE_VERSION,
        "updated_at_utc": stamp,
        "settings": {
            "break_even_pct": VIVIER_BREAK_EVEN_PCT,
            "learning_window_h1": VIVIER_PERFORMANCE_WINDOW_H1,
            "pair_profile_min_complete": VIVIER_PAIR_PROFILE_MIN_COMPLETE,
        },
        "summary": _performance_summary(events),
        "pair_profiles": _pair_performance_profiles(events),
        "events": events,
    }


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


def vivier_pip_size(pair: str) -> float:
    """Standard pip size for the 29-instrument universe."""
    return 0.01 if pair.endswith("JPY") or pair == "XAUUSD" else 0.0001


def load_vivier_pips(path: str = VIVIER_PIPS_PATH) -> dict:
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            return payload
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    return {
        "version": VIVIER_PIPS_VERSION,
        "open_segments": {},
        "days": {},
        "reports_sent": {"weekly": [], "monthly": []},
    }


def save_vivier_pips(payload: dict, path: str = VIVIER_PIPS_PATH) -> None:
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


def _pip_timestamp(value: str | datetime | pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _pip_row_snapshot(row: dict | None) -> tuple[pd.Timestamp, float] | None:
    if not row:
        return None
    h1 = row.get("h1_fib") or {}
    price = h1.get("h1_closed_price", row.get("h1_price", row.get("live_price")))
    ts = _pip_timestamp(h1.get("h1_closed_time_utc"))
    if ts is None or not isinstance(price, (int, float)):
        return None
    return ts, float(price)


def _pip_historical_snapshot(row: dict | None, cutoff: pd.Timestamp,
                             start: pd.Timestamp | None = None
                             ) -> tuple[pd.Timestamp, float] | None:
    bars = ((row or {}).get("h1_fib") or {}).get("_closed_h1_bars") or []
    selected = None
    for bar in bars:
        ts = _pip_timestamp(bar.get("time_utc"))
        price = bar.get("close")
        if ts is None or not isinstance(price, (int, float)) or ts > cutoff:
            continue
        if start is not None and ts < start:
            continue
        if selected is None or ts > selected[0]:
            selected = (ts, float(price))
    return selected


def _pip_historical_start_snapshot(row: dict | None, start: pd.Timestamp,
                                   cutoff: pd.Timestamp
                                   ) -> tuple[pd.Timestamp, float] | None:
    bars = ((row or {}).get("h1_fib") or {}).get("_closed_h1_bars") or []
    selected = None
    for bar in bars:
        ts = _pip_timestamp(bar.get("time_utc"))
        price = bar.get("close")
        if (ts is None or not isinstance(price, (int, float))
                or ts < start or ts > cutoff):
            continue
        if selected is None or ts < selected[0]:
            selected = (ts, float(price))
    return selected


def _pip_segment_value(segment: dict, price: float) -> float:
    start_price = segment.get("start_price")
    direction = segment.get("direction")
    pair = str(segment.get("pair") or "")
    if (not isinstance(start_price, (int, float)) or start_price == 0
            or direction not in (-1, 1) or not pair):
        return 0.0
    return ((float(price) - float(start_price)) / vivier_pip_size(pair)
            * int(direction))


def _pip_day(state: dict, day_key: str) -> dict:
    return state.setdefault("days", {}).setdefault(day_key, {
        "date": day_key,
        "segments": [],
        "finalized": False,
    })


def _close_pip_segment(state: dict, pair: str, segment: dict,
                       end_ts: pd.Timestamp, end_price: float,
                       reason: str) -> None:
    day = _pip_day(state, str(segment["date"]))
    closed = dict(segment)
    closed.update({
        "end_time_utc": end_ts.isoformat(),
        "end_time_paris": end_ts.tz_convert(PARIS_TZ).isoformat(),
        "end_price": float(end_price),
        "pips": _pip_segment_value(segment, float(end_price)),
        "close_reason": reason,
    })
    closed.pop("last_price", None)
    closed.pop("last_time_utc", None)
    closed.pop("current_pips", None)
    day.setdefault("segments", []).append(closed)
    state.setdefault("open_segments", {}).pop(pair, None)


def _pip_day_totals(state: dict, day_key: str, include_open: bool = True) -> dict:
    bull = bear = 0.0
    day = (state.get("days") or {}).get(day_key) or {}
    for segment in day.get("segments") or []:
        pips = float(segment.get("pips") or 0.0)
        if segment.get("direction") == 1:
            bull += pips
        elif segment.get("direction") == -1:
            bear += pips
    if include_open:
        for segment in (state.get("open_segments") or {}).values():
            if segment.get("date") != day_key:
                continue
            pips = float(segment.get("current_pips") or 0.0)
            if segment.get("direction") == 1:
                bull += pips
            elif segment.get("direction") == -1:
                bear += pips
    return {"bull_pips": bull, "bear_pips": bear, "total_pips": bull + bear}


def _pip_day_result(total_pips: float) -> str:
    if total_pips > VIVIER_PIPS_DAY_RESULT_EPSILON:
        return "WIN"
    if total_pips < -VIVIER_PIPS_DAY_RESULT_EPSILON:
        return "LOSS"
    return "FLAT"


def _pip_day_result_counts(state: dict, day_keys: list[str]) -> dict:
    wins = losses = flats = followed = 0
    for day_key in day_keys:
        day = (state.get("days") or {}).get(day_key) or {}
        if not day.get("finalized"):
            continue
        followed += 1
        result = _pip_day_result(
            _pip_day_totals(state, day_key, include_open=False)["total_pips"]
        )
        if result == "WIN":
            wins += 1
        elif result == "LOSS":
            losses += 1
        else:
            flats += 1
    return {
        "followed_days": followed,
        "winning_days": wins,
        "losing_days": losses,
        "flat_days": flats,
    }


def _last_friday_of_month(year: int, month: int):
    if month == 12:
        next_month = datetime(year + 1, 1, 1).date()
    else:
        next_month = datetime(year, month + 1, 1).date()
    last_day = next_month - timedelta(days=1)
    return last_day - timedelta(days=(last_day.weekday() - 4) % 7)


def _date_keys_between(start, end) -> list[str]:
    keys = []
    current = start
    while current <= end:
        keys.append(current.isoformat())
        current += timedelta(days=1)
    return keys


def _period_pip_totals(
    state: dict, day_keys: list[str], live_day_key: str | None = None
) -> dict:
    bull = bear = 0.0
    for day_key in day_keys:
        day = (state.get("days") or {}).get(day_key) or {}
        is_live_day = day_key == live_day_key
        if not day.get("finalized") and not is_live_day:
            continue
        totals = _pip_day_totals(
            state, day_key, include_open=is_live_day and not day.get("finalized")
        )
        bull += totals["bull_pips"]
        bear += totals["bear_pips"]
    return {"bull_pips": bull, "bear_pips": bear, "total_pips": bull + bear}


def _monthly_cycle_start(day):
    month_close = _last_friday_of_month(day.year, day.month)
    if day > month_close:
        previous_close = month_close
    else:
        first = day.replace(day=1)
        previous_last = first - timedelta(days=1)
        previous_close = _last_friday_of_month(
            previous_last.year, previous_last.month
        )
    return previous_close + timedelta(days=1)


def _weekly_pip_report(state: dict, clock: datetime) -> dict:
    monday = clock.date() - timedelta(days=clock.weekday())
    labels = ("Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi")
    daily = []
    bull = bear = 0.0
    for offset, label in enumerate(labels):
        day_key = (monday + timedelta(days=offset)).isoformat()
        totals = _pip_day_totals(state, day_key, include_open=False)
        bull += totals["bull_pips"]
        bear += totals["bear_pips"]
        daily.append({"label": label, "date": day_key, **totals})
    iso = clock.isocalendar()
    day_keys = [(monday + timedelta(days=offset)).isoformat() for offset in range(5)]
    return {
        "key": f"{iso.year}-W{iso.week:02d}",
        "daily": daily,
        "bull_pips": bull,
        "bear_pips": bear,
        "total_pips": bull + bear,
        **_pip_day_result_counts(state, day_keys),
    }


def _monthly_pip_report(state: dict, clock: datetime) -> dict | None:
    month_close = _last_friday_of_month(clock.year, clock.month)
    month_start = _monthly_cycle_start(month_close)
    month_key = f"{month_close.year:04d}-{month_close.month:02d}"
    selected = [
        (day_key, day) for day_key, day in (state.get("days") or {}).items()
        if month_start.isoformat() <= day_key <= month_close.isoformat()
        and day.get("finalized")
    ]
    if not selected:
        return None
    bull = bear = 0.0
    for day_key, _ in selected:
        totals = _pip_day_totals(state, day_key, include_open=False)
        bull += totals["bull_pips"]
        bear += totals["bear_pips"]
    month_names = (
        "JANVIER", "FEVRIER", "MARS", "AVRIL", "MAI", "JUIN",
        "JUILLET", "AOUT", "SEPTEMBRE", "OCTOBRE", "NOVEMBRE", "DECEMBRE",
    )
    day_keys = [day_key for day_key, _ in selected]
    return {
        "key": month_key,
        "label": month_names[month_close.month - 1],
        "days": len(selected),
        "bull_pips": bull,
        "bear_pips": bear,
        "total_pips": bull + bear,
        **_pip_day_result_counts(state, day_keys),
    }


def _period_pip_summary_report(state: dict, clock: datetime, day_key: str) -> dict:
    day = (state.get("days") or {}).get(day_key) or {}
    day_date = clock.date()
    week_start = day_date - timedelta(days=day_date.weekday())
    month_start = _monthly_cycle_start(day_date)
    finalized = bool(day.get("finalized"))
    return {
        "date": day_key,
        "finalized": finalized,
        "daily": _pip_day_totals(state, day_key, include_open=not finalized),
        "weekly": _period_pip_totals(
            state, _date_keys_between(week_start, day_date), live_day_key=day_key
        ),
        "monthly": _period_pip_totals(
            state, _date_keys_between(month_start, day_date), live_day_key=day_key
        ),
    }


def update_vivier_pip_tracker(previous: dict | None, vivier_state: dict,
                              rows: list[dict], now: datetime | None = None
                              ) -> tuple[dict, dict]:
    """Track every active VIVIER segment between 07:00 and 23:00 Paris."""
    state = json.loads(json.dumps(previous)) if isinstance(previous, dict) else {}
    state["version"] = VIVIER_PIPS_VERSION
    state.setdefault("open_segments", {})
    state.setdefault("days", {})
    reports_sent = state.setdefault("reports_sent", {})
    reports_sent.setdefault("weekly", [])
    reports_sent.setdefault("monthly", [])

    clock = now or datetime.now(PARIS_TZ)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=PARIS_TZ)
    clock = clock.astimezone(PARIS_TZ)
    today = clock.date().isoformat()
    rows_by_pair = {str(row.get("pair")): row for row in rows if row.get("pair")}
    snapshots = {
        pair: snapshot for pair, row in rows_by_pair.items()
        if (snapshot := _pip_row_snapshot(row)) is not None
    }

    # Close segments left open by a missed 23:00 run using the latest H1 close
    # available at or before the original day's Paris cutoff.
    for pair, segment in list(state["open_segments"].items()):
        if segment.get("date") == today:
            continue
        segment_date = datetime.fromisoformat(str(segment["date"]))
        cutoff_local = datetime(
            segment_date.year, segment_date.month, segment_date.day,
            VIVIER_PIPS_END_HOUR_PARIS, tzinfo=PARIS_TZ,
        )
        cutoff = pd.Timestamp(cutoff_local).tz_convert("UTC")
        start = _pip_timestamp(segment.get("start_time_utc"))
        historical = _pip_historical_snapshot(rows_by_pair.get(pair), cutoff, start)
        if historical is None:
            historical = (
                _pip_timestamp(segment.get("last_time_utc")) or cutoff,
                float(segment.get("last_price", segment.get("start_price", 0.0))),
            )
        _close_pip_segment(state, pair, segment, historical[0], historical[1], "DAY_END")
        old_day = _pip_day(state, str(segment["date"]))
        old_day["finalized"] = True
        old_day["finalized_at_paris"] = cutoff_local.isoformat()

    in_window = VIVIER_PIPS_START_HOUR_PARIS <= clock.hour <= VIVIER_PIPS_END_HOUR_PARIS
    fresh_pairs = {
        pair for pair, (ts, _) in snapshots.items()
        if ts.tz_convert(PARIS_TZ).date().isoformat() == today
        and ts.tz_convert(PARIS_TZ).hour >= VIVIER_PIPS_START_HOUR_PARIS
    }
    fresh_market = bool(fresh_pairs)
    active_entries = (vivier_state or {}).get("pairs") or {}
    day_already_finalized = bool(
        ((state.get("days") or {}).get(today) or {}).get("finalized")
    )

    if in_window and fresh_market and not day_already_finalized:
        day = _pip_day(state, today)
        day["finalized"] = False

        # Freeze pairs that left the pool or switched direction.
        for pair, segment in list(state["open_segments"].items()):
            if segment.get("date") != today:
                continue
            entry = active_entries.get(pair)
            same_direction = entry and entry.get("direction") == segment.get("direction")
            if same_direction:
                continue
            ts, price = snapshots.get(pair, (
                _pip_timestamp(segment.get("last_time_utc")),
                segment.get("last_price", segment.get("start_price")),
            ))
            if ts is not None and isinstance(price, (int, float)):
                _close_pip_segment(state, pair, segment, ts, float(price), "VIVIER_EXIT")

        # Open new segments (including a same-run opposite re-entry) and mark
        # all active segments to the latest confirmed H1 close.
        for pair, entry in active_entries.items():
            direction = entry.get("direction")
            snapshot = snapshots.get(pair)
            if direction not in (-1, 1) or snapshot is None or pair not in fresh_pairs:
                continue
            ts, price = snapshot
            segment = state["open_segments"].get(pair)
            if segment is None:
                window_start_local = datetime(
                    clock.year, clock.month, clock.day,
                    VIVIER_PIPS_START_HOUR_PARIS, tzinfo=PARIS_TZ,
                )
                entry_time = _parse_paris_minute(entry.get("entered_at_paris"))
                if entry_time is not None and entry_time.date() == clock.date():
                    entry_time = entry_time.replace(minute=0, second=0, microsecond=0)
                    window_start_local = max(window_start_local, entry_time)
                desired_start = pd.Timestamp(window_start_local).tz_convert("UTC")
                historical_start = _pip_historical_start_snapshot(
                    rows_by_pair.get(pair), desired_start, ts
                )
                start_ts, start_price = historical_start or (ts, price)
                segment = {
                    "pair": pair,
                    "direction": int(direction),
                    "date": today,
                    "start_time_utc": start_ts.isoformat(),
                    "start_time_paris": start_ts.tz_convert(PARIS_TZ).isoformat(),
                    "start_price": float(start_price),
                }
                state["open_segments"][pair] = segment
            segment["last_time_utc"] = ts.isoformat()
            segment["last_price"] = float(price)
            segment["current_pips"] = _pip_segment_value(segment, float(price))

        if clock.hour >= VIVIER_PIPS_END_HOUR_PARIS:
            for pair, segment in list(state["open_segments"].items()):
                if segment.get("date") != today:
                    continue
                ts, price = snapshots.get(pair, (
                    _pip_timestamp(segment.get("last_time_utc")),
                    segment.get("last_price", segment.get("start_price")),
                ))
                if ts is not None and isinstance(price, (int, float)):
                    _close_pip_segment(state, pair, segment, ts, float(price), "DAY_END")
            day["finalized"] = True
            day["finalized_at_paris"] = clock.isoformat()

    report: dict = {
        "intraday": None,
        "eod_summary": None,
        "period_summary": None,
        "weekly": None,
        "monthly": None,
    }
    if in_window and fresh_market:
        today_totals = _pip_day_totals(state, today)
        today_finalized = bool(
            ((state.get("days") or {}).get(today) or {}).get("finalized")
        )
        report["intraday"] = {
            "date": today,
            **today_totals,
            "finalized": today_finalized,
            "day_result": _pip_day_result(today_totals["total_pips"]),
        }
        summary = _period_pip_summary_report(state, clock, today)
        report["period_summary"] = summary
        if today_finalized:
            report["eod_summary"] = summary

    if clock.weekday() == 4 and clock.hour >= VIVIER_PIPS_END_HOUR_PARIS:
        weekly = _weekly_pip_report(state, clock)
        if weekly["key"] not in reports_sent["weekly"]:
            report["weekly"] = weekly

    month_close = _last_friday_of_month(clock.year, clock.month)
    if (clock.date() == month_close
            and clock.hour >= VIVIER_PIPS_END_HOUR_PARIS):
        monthly = _monthly_pip_report(state, clock)
        if monthly and monthly["key"] not in reports_sent["monthly"]:
            report["monthly"] = monthly

    # Keep slightly more than one year of daily detail.
    day_keys = sorted(state["days"])
    for obsolete in day_keys[:-400]:
        state["days"].pop(obsolete, None)
    state["updated_at_paris"] = clock.isoformat()
    return state, report


def mark_vivier_pip_reports_sent(state: dict, report: dict | None) -> None:
    sent = state.setdefault("reports_sent", {"weekly": [], "monthly": []})
    for kind in ("weekly", "monthly"):
        item = (report or {}).get(kind)
        if not item:
            continue
        keys = sent.setdefault(kind, [])
        if item["key"] not in keys:
            keys.append(item["key"])
            del keys[:-100]


def _format_pips(value: float) -> str:
    value = float(value)
    if abs(value) < 0.05:
        value = 0.0
    return f"{value:+.1f}"


def vivier_pip_intraday_lines(report: dict | None) -> list[str]:
    item = (report or {}).get("intraday")
    if not item:
        return []
    lines = [
        "📈 PIPS GLOBAL DEPUIS 07H",
        f"🟢 BULL : {_format_pips(item['bull_pips'])} pips",
        f"🔴 BEAR : {_format_pips(item['bear_pips'])} pips",
        f"Σ TOTAL : {_format_pips(item['total_pips'])} pips",
    ]
    if item.get("finalized"):
        label = {
            "WIN": "🟢 JOUR GAGNANT",
            "LOSS": "🔴 JOUR PERDANT",
            "FLAT": "⚪ JOUR NEUTRE",
        }.get(item.get("day_result"), "⚪ JOUR NEUTRE")
        lines.append(label)
    summary = (report or {}).get("period_summary") or (report or {}).get("eod_summary")
    if summary:
        title = (
            "📊 CUMULS FIN DE JOURNÉE"
            if item.get("finalized")
            else "📊 CUMULS EN COURS"
        )
        lines.extend([
            "",
            title,
            f"Σ Daily : {_format_pips(summary['daily']['total_pips'])} pips",
            f"Σ Weekly : {_format_pips(summary['weekly']['total_pips'])} pips",
            f"Σ Monthly : {_format_pips(summary['monthly']['total_pips'])} pips",
        ])
    return lines


def vivier_pip_period_lines(report: dict | None) -> list[str]:
    lines: list[str] = []
    weekly = (report or {}).get("weekly")
    if weekly:
        lines.extend(["📅 BILAN HEBDOMADAIRE"])
        lines.extend(
            f"{day['label']} : {_format_pips(day['total_pips'])} pips"
            for day in weekly["daily"]
        )
        lines.append(
            "JOURS : "
            f"🟢 {weekly.get('winning_days', 0)} gagnants / "
            f"🔴 {weekly.get('losing_days', 0)} perdants / "
            f"⚪ {weekly.get('flat_days', 0)} neutres"
        )
        lines.append(f"TOTAL : {_format_pips(weekly['total_pips'])} pips")
    monthly = (report or {}).get("monthly")
    if monthly:
        if lines:
            lines.append("")
        lines.extend([
            f"🗓 BILAN MENSUEL — {monthly['label']}",
            f"{monthly['days']} journées suivies",
            "JOURS : "
            f"🟢 {monthly.get('winning_days', 0)} gagnants / "
            f"🔴 {monthly.get('losing_days', 0)} perdants / "
            f"⚪ {monthly.get('flat_days', 0)} neutres",
            f"🟢 BULL : {_format_pips(monthly['bull_pips'])} pips",
            f"🔴 BEAR : {_format_pips(monthly['bear_pips'])} pips",
            f"Σ TOTAL : {_format_pips(monthly['total_pips'])} pips",
        ])
    return lines


MAJORS = {"AUD", "CAD", "CHF", "EUR", "GBP", "JPY", "NZD", "USD"}


def fibo_currency_coefficient(h1_fib: dict | None) -> int | None:
    """Coefficient directionnel de la paire selon sa position vs Fibo 0.5.

    Positive = devise de base forte / devise de cotation faible.
    Negative = devise de base faible / devise de cotation forte.
    Intensite par zone:
      <0=-4, <0.236=-3, <0.382=-2, <0.5=-1,
      >0.5=+1, >0.618=+2, >0.786=+3, >1=+4.
    """
    pct = (h1_fib or {}).get("pct_of_range")
    if not isinstance(pct, (int, float)):
        return None
    ratio = float(pct) / 100.0
    if ratio < 0.0:
        return -4
    if ratio < 0.236:
        return -3
    if ratio < 0.382:
        return -2
    if ratio < 0.5:
        return -1
    if ratio == 0.5:
        return 0
    if ratio < 0.618:
        return 1
    if ratio < 0.786:
        return 2
    if ratio <= 1.0:
        return 3
    return 4


def fibo_currency_sar_adjusted_coefficient(h1_fib: dict | None) -> float | None:
    """Same Fibo coefficient, halved when SAR contradicts pair direction."""
    coeff = fibo_currency_coefficient(h1_fib)
    if coeff is None or coeff == 0:
        return coeff
    sar_dir = (h1_fib or {}).get("sar_dir")
    if sar_dir not in (-1, 1):
        return float(coeff)
    if (coeff > 0 and sar_dir < 0) or (coeff < 0 and sar_dir > 0):
        return float(coeff) * FIBO_SAR_CONTRADICTION_FACTOR
    return float(coeff)


def fibo_currency_strength(rows: list[dict],
                           min_pairs: int = FIBO_CURRENCY_MIN_PAIRS,
                           strict_sar: bool = False) -> list[dict]:
    """Classe les devises par force Fibo mensuelle H1 sur toutes les paires.

    Pour ABCXYZ:
      - coefficient positif: ABC fort, XYZ faible;
      - coefficient negatif: ABC faible, XYZ fort.
    Le score final est la moyenne des contributions disponibles par devise.
    """
    agg: dict[str, list[float]] = {}
    for row in rows:
        pair = str(row.get("pair") or "")
        if len(pair) != 6:
            continue
        base, quote = pair[:3], pair[3:]
        if base not in MAJORS or quote not in MAJORS:
            continue
        h1_fib = row.get("h1_fib")
        coeff = (
            fibo_currency_sar_adjusted_coefficient(h1_fib)
            if strict_sar else fibo_currency_coefficient(h1_fib)
        )
        if coeff is None or coeff == 0:
            continue
        agg.setdefault(base, []).append(float(coeff))
        agg.setdefault(quote, []).append(float(-coeff))

    ranked = []
    for currency, values in agg.items():
        if len(values) < min_pairs:
            continue
        ranked.append({
            "currency": currency,
            "score": statistics.fmean(values),
            "samples": len(values),
            "raw": sum(values),
        })
    return sorted(
        ranked,
        key=lambda item: (-float(item["score"]), -int(item["samples"]), item["currency"]),
    )


def _fibo_currency_strength_block(title: str, ranked: list[dict]) -> list[str]:
    if len(ranked) < 2:
        return []
    strongest = ranked[0]
    weakest = ranked[-1]
    top = ", ".join(
        f"{item['currency']} {item['score']:+.2f}"
        for item in ranked[:FIBO_CURRENCY_TOP_N]
    )
    bottom_ranked = sorted(ranked[-FIBO_CURRENCY_TOP_N:],
                           key=lambda item: float(item["score"]))
    bottom = ", ".join(
        f"{item['currency']} {item['score']:+.2f}"
        for item in bottom_ranked
    )
    return [
        title,
        f"💪 Forte: {strongest['currency']} {strongest['score']:+.2f}",
        f"🥶 Faible: {weakest['currency']} {weakest['score']:+.2f}",
        f"Top: {top}",
        f"Bas: {bottom}",
    ]


def fibo_currency_strength_lines(rows: list[dict] | None) -> list[str]:
    ranked = fibo_currency_strength(rows or [])
    return _fibo_currency_strength_block("💱 FORCE FIBO 0.5", ranked)


def fibo_currency_strict_strength_lines(rows: list[dict] | None) -> list[str]:
    ranked = fibo_currency_strength(rows or [], strict_sar=True)
    return _fibo_currency_strength_block("🎚️ FORCE FIBO+SAR", ranked)


def vivier_currency_roles(vivier_entries: dict[str, dict] | None) -> dict[str, int]:
    """Return each currency's unanimous role in the active vivier.

    A BULL pair makes its base strong and quote weak; a BEAR pair does the
    opposite. A currency observed in both roles is neutral (0) and therefore
    cannot support a strict strong/weak theoretical pair.
    """
    votes: dict[str, set[int]] = {}
    for pair, entry in (vivier_entries or {}).items():
        if len(pair) != 6:
            continue
        direction = entry.get("direction")
        if direction not in (-1, 1):
            continue
        base, quote = pair[:3], pair[3:]
        votes.setdefault(base, set()).add(int(direction))
        votes.setdefault(quote, set()).add(-int(direction))
    return {
        currency: next(iter(roles)) if len(roles) == 1 else 0
        for currency, roles in votes.items()
    }


def fibo_theoretical_pairs(rows: list[dict] | None,
                           available_pairs: list[str] | set[str] | None = None,
                           vivier_entries: dict[str, dict] | None = None) -> list[dict]:
    """Cross strict Top currencies against strict Bottom currencies.

    If the strong/weak pair exists, it is a theoretical long. If only the
    inverse exists, it is a theoretical short on the listed pair.
    """
    ranked = fibo_currency_strength(rows or [], strict_sar=True)
    strong = [
        item for item in ranked
        if isinstance(item.get("score"), (int, float)) and item["score"] > 0
    ][:FIBO_CURRENCY_TOP_N]
    weak = sorted(
        [
            item for item in ranked
            if isinstance(item.get("score"), (int, float)) and item["score"] < 0
        ],
        key=lambda item: float(item["score"]),
    )[:FIBO_CURRENCY_TOP_N]
    if not strong or not weak:
        return []

    universe = set(available_pairs or PAIRS_29)
    vivier_roles = vivier_currency_roles(vivier_entries)
    require_vivier_link = vivier_entries is not None
    ideas: list[dict] = []
    seen: set[str] = set()
    for strong_item in strong:
        strong_ccy = str(strong_item["currency"])
        for weak_item in weak:
            weak_ccy = str(weak_item["currency"])
            if strong_ccy == weak_ccy:
                continue
            # Both currencies must be present in the active vivier and their
            # roles must be unanimous: strong only versus weak only.
            if require_vivier_link and (
                vivier_roles.get(strong_ccy) != 1
                or vivier_roles.get(weak_ccy) != -1
            ):
                continue
            direct = f"{strong_ccy}{weak_ccy}"
            inverse = f"{weak_ccy}{strong_ccy}"
            if direct in universe:
                pair, direction = direct, 1
            elif inverse in universe:
                pair, direction = inverse, -1
            else:
                continue
            if pair in seen:
                continue
            seen.add(pair)
            edge = float(strong_item["score"]) - float(weak_item["score"])
            ideas.append({
                "pair": pair,
                "direction": direction,
                "strong": strong_ccy,
                "weak": weak_ccy,
                "edge": edge,
            })
    return sorted(ideas, key=lambda item: (-float(item["edge"]), item["pair"]))


def fibo_theoretical_pairs_lines(rows: list[dict] | None,
                                 vivier_entries: dict[str, dict] | None = None) -> list[str]:
    ideas = fibo_theoretical_pairs(rows, vivier_entries=vivier_entries)
    if not ideas:
        return []
    vivier_currencies = set(vivier_currency_roles(vivier_entries))
    lines = ["🧭 PAIRES FORT/FAIBLE"]
    for idea in ideas:
        icon = "🟢" if idea["direction"] == 1 else "🔴"
        pair = idea["pair"]
        shared = [currency for currency in (pair[:3], pair[3:])
                  if currency in vivier_currencies]
        vivier_tag = f" 🌱{'/'.join(shared)}" if shared else ""
        lines.append(f"{icon} {pair}{vivier_tag}")
    return lines


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


def build_telegram_message(rows: list[dict], all_rows: list[dict] | None = None,
                           vivier_state: dict | None = None,
                           vivier_signals: list[dict] | None = None,
                           pip_report: dict | None = None) -> str | None:
    vivier_signals = vivier_signals or []
    vivier_state = vivier_state or {}
    bull_vivier, bear_vivier = vivier_groups(vivier_state)
    near_entries = near_alignment_entries(vivier_state)
    post_signal_entries = post_signal_tracking_entries(vivier_state, all_rows)
    strength_rows = all_rows if all_rows is not None else rows
    active_vivier_entries = dict(bull_vivier + bear_vivier)
    theoretical_pairs = fibo_theoretical_pairs_lines(
        strength_rows,
        vivier_entries=active_vivier_entries,
    )
    intraday_pip_lines = vivier_pip_intraday_lines(pip_report)
    period_pip_lines = vivier_pip_period_lines(pip_report)

    # Telegram is dedicated exclusively to the VIVIER ecosystem. Standalone
    # RENKO FIBO and reversal signals remain internal and silent.
    if (not bull_vivier and not bear_vivier and not vivier_signals
            and not near_entries and not post_signal_entries
            and not theoretical_pairs and not intraday_pip_lines
            and not period_pip_lines):
        return None
    lines = ["📊 VIVIER", ""]
    has_content = False

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

    if post_signal_entries:
        if has_content:
            lines.append("")
        lines.append("🎯 SUIVI SIGNAL")
        for item in post_signal_entries:
            icon = "🟢" if item["direction"] == 1 else "🔴"
            sar_icon = {1: "🟢", -1: "🔴"}.get(item.get("sar_dir"), "⚪")
            pct = item.get("directional_pct")
            pct_txt = f" ({pct:+.2f}%)" if isinstance(pct, (int, float)) else ""
            lines.append(f"{icon}{sar_icon} {item['pair']}{pct_txt}")
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
        for pair, entry in entries:
            lines.append(_format_telegram_vivier_entry_line(pair, entry))
        has_content = True

    if near_entries:
        if has_content:
            lines.append("")
        lines.append("⏳ PROCHE ALIGNEMENT")
        for pair, entry in near_entries:
            direction = int(entry["direction"])
            icon = "🟢" if direction == 1 else "🔴"
            sar_icon = {1: "🟢", -1: "🔴"}.get(entry.get("sar_dir"), "⚪")
            score = vivier_base_score(entry)
            missing = "D restant"
            lines.append(f"{icon}{sar_icon} {pair} · {missing} · {score:+.0f}%")
        has_content = True

    if intraday_pip_lines:
        if has_content:
            lines.append("")
        lines.extend(intraday_pip_lines)
        has_content = True

    if theoretical_pairs:
        if has_content:
            lines.append("")
        lines.extend(theoretical_pairs)
        has_content = True

    if period_pip_lines:
        if has_content:
            lines.append("")
        lines.extend(period_pip_lines)
        has_content = True

    # Message: ecosysteme VIVIER et horodatage.
    lines.append("")
    lines.append(f"⏰ {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M Paris')}")
    return "\n".join(lines)


def telegram_body_hash(message: str) -> str:
    """Hash the actionable Telegram body, ignoring only the run timestamp."""
    body = "\n".join(
        line for line in message.splitlines()
        if not line.startswith("⏰ ")
    ).strip()
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def carry_telegram_metadata(previous_state: dict, current_state: dict) -> None:
    for key in ("telegram_last_body_hash", "telegram_last_sent_at_paris"):
        if key in previous_state and key not in current_state:
            current_state[key] = previous_state[key]


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    args = parse_args()
    run_now = datetime.now(PARIS_TZ)
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
    pip_state, pip_report = update_vivier_pip_tracker(
        load_vivier_pips(args.vivier_pips),
        vivier_state,
        rows,
        now=run_now,
    )
    save_vivier_pips(pip_state, args.vivier_pips)
    carry_telegram_metadata(previous_vivier, vivier_state)
    save_vivier_state(vivier_state, args.vivier_state)
    print_vivier_report(vivier_state, vivier_signals)
    tracker_summary = performance["summary"]
    pair_profiles = performance.get("pair_profiles") or {}
    ready_profiles = sum(
        profile.get("overall", {}).get("status") == "READY"
        for profile in pair_profiles.values()
    )
    print(
        "VIVIER TRACKER: "
        f"{new_event_count} événement(s) ajouté(s), "
        f"{tracker_summary['total']} suivi(s), "
        f"{tracker_summary['complete']} complet(s) à 72h, "
        f"{ready_profiles}/{len(pair_profiles)} profil(s) paire prêt(s)"
    )

    message = build_telegram_message(
        strong_rows,
        rows,
        vivier_state=vivier_state,
        vivier_signals=vivier_signals,
        pip_report=pip_report,
    )
    if message is None:
        print("\nVIVIER vide — aucun message Telegram envoyé.")
        return 0
    print("")
    print(message)
    message_hash = telegram_body_hash(message)
    if args.no_telegram:
        print("Telegram: envoi désactivé (--no-telegram).")
    elif not args.force_telegram and message_hash == previous_vivier.get("telegram_last_body_hash"):
        print("Telegram: message inchangé, envoi ignoré.")
        mark_vivier_pip_reports_sent(pip_state, pip_report)
        save_vivier_pips(pip_state, args.vivier_pips)
    else:
        send_telegram_message(message)
        mark_vivier_pip_reports_sent(pip_state, pip_report)
        save_vivier_pips(pip_state, args.vivier_pips)
        vivier_state["telegram_last_body_hash"] = message_hash
        vivier_state["telegram_last_sent_at_paris"] = run_now.strftime("%Y-%m-%d %H:%M")
        save_vivier_state(vivier_state, args.vivier_state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
