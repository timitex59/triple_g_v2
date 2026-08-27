#!/usr/bin/env python3
"""Extract the IMP Trend table for the 29 OANDA instruments.

The calculations mirror imp_trend.pine:
* TradingView native ATR Renko states on M/W/D.
* PSAR(0.1, 0.1, 0.2) IMP signals.
* IMP 21 ratio and the last 21 completed IMP-to-IMP price moves.
* Separate IMP statistics for the H1 and Daily charts.

TradingView's private websocket protocol is unofficial and can change.

2026-08-26: added CURRENCY_INDEX, a 12th vote (threshold VOTE_THRESHOLD_V1,
9/12) derived from the 8 currency indices already used by PAIRE_CHECK /
renko_full_alignment_29pairs.py (cf. `fetch_currency_index_rows` and
`currency_index_vote`). It is NOT a same-color check: two red currencies are
not equivalent (GBP +1.20 red is far weaker than CAD +0.00 red, so GBPCAD
still votes BEAR) -- it reuses `currency_spread`/`signed_strength`
(intensity-weighted, sign-oriented) and requires a minimum spread
(CURRENCY_INDEX_MIN_SPREAD) between the two currencies to vote at all, else
NEUTRAL (e.g. AUD +0.28 vs USD +0.26, both green, is too close to call).
`imp_trend_backtest.py` still calls `select_aligned_pairs`/
`invalidation_reason` without index_by_currency, so CURRENCY_INDEX always
reads NEUTRAL there -- its walk-forward history is effectively
9-of-11-real under the new threshold, not the 8-of-11 it was tuned against.

2026-08-26 (later same day): CURRENCY_INDEX also acts as a veto (cf.
`currency_index_diverges`), on the same principle as
`renko_full_alignment_29pairs.is_engine_divergent`. An active contradiction
-- CURRENCY_INDEX votes BEAR while the other votes already carry the pair to
BULL, not just NEUTRAL/abstaining -- excludes the pair outright, even if the
other votes alone would have cleared VOTE_THRESHOLD_V1. Example that
prompted this: GBPJPY BULL on Renko/IMP21 with GBP far weaker than JPY on
the currency indices (spread well past CURRENCY_INDEX_MIN_SPREAD) is now
dropped, where before it would have stayed BULL with one wasted vote.

2026-08-26 (later still): CURRENCY_INDEX's spread-based BULL/BEAR is now
also quality-gated by each currency's OWN trend (cf. `compute_currency_imp`,
`fetch_currency_imp_rows`, `currency_trend_confirms`) -- the same
Renko/PSAR/IMP21 replay used for pairs, run against the 8 currency indices'
own TradingView symbols (TVC:DXY etc.) instead of just their Renko-streak
CHG%D score. The currency CURRENCY_INDEX calls "strong" should itself be
trending BULL on its own D1/H1 IMP21, the "weak" one BEAR; if neither
currency's own trend supports the role the spread assigned it (0 confirmed
out of the applicable checks -- same 0-of-N convention as V2's D1/H1 average
quality gate), the vote is downgraded to NEUTRAL instead of counting on a
spread nothing else corroborates. `pip_size` treats the 8 index codes like
JPY pairs (0.01) for the IMP move averages -- an arbitrary but readable
convention, these aren't real pip-quoted instruments.

2026-08-27: added a `tradable` flag (new for V1, mirroring V2's existing
quality-gate mechanism) driven by `pair_touches_a_divergent_currency` --
excludes a pair from position/session tracking (not from the message: it
still shows with a "(non tradée)" suffix, cf. `build_telegram_message`) when
either of its two currencies has a same-day move opposite its own
structural consensus (cf. `currency_consensus_status`,
`currency_diverges_from_its_own_day`) -- e.g. GBP down -0.24% today (BEAR)
while its Renko M/W/D + D1/H1 IMP21 majority still reads BULL. Deliberately
a downgrade, not the outright veto `currency_index_diverges` already is:
`daily_chg` is a single, noisy, midnight-resetting data point, and this
contradiction usually just means an ordinary pullback inside an established
trend -- too weak a signal to throw the pair out of the message entirely,
but real enough that the user chose to keep it out of position tracking.

2026-08-27 (later): V1, V2 and paire_check.py each fetched their own
`index_by_currency`/`imp_by_currency` independently, a few minutes apart --
.github/workflows/triple_g_workflow.yml runs the three of them sequentially
in the SAME CI job, but each is a live TradingView snapshot, so a currency
sitting right at a threshold (daily_chg near CURRENCY_DAILY_MOVE_MIN_MAGNITUDE,
or a consensus vote near CURRENCY_CONSENSUS_THRESHOLD) could genuinely read
differently in each script's own message. `save_currency_snapshot`/
`load_currency_snapshot`/`fetch_or_load_currency_data` fix this: since all
three scripts already share the same CI job's filesystem, the first one to
run fetches live and writes a snapshot file; the next ones find it (still
fresh, cf. `max_age_seconds`) and reuse it verbatim instead of re-fetching --
guarantees byte-identical currency data across the three for that run, and
cuts the currency-related TradingView calls roughly 3x (fetched once, not
three times). Falls back to its own live fetch when the snapshot is
missing/stale/corrupt, so each script stays fully usable standalone (e.g.
for a manual one-off run) -- this is a reused-if-fresh optimization, not a
new dependency between the scripts."""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import string
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException, create_connection

from renko_full_alignment_29pairs import (
    FOREX_INDEX_ASSETS,
    SYNC_MIN_EXPECTED,
    compute_asset_score,
    currency_spread,
)
from renko_score_29pairs_v16 import TFState


PAIRS_29 = [
    "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "AUDUSD",
    "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD",
    "EURCHF", "EURGBP", "EURJPY", "EURNZD", "EURUSD",
    "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD",
    "GBPUSD", "NZDCAD", "NZDCHF", "NZDJPY", "NZDUSD",
    "USDCAD", "USDCHF", "USDJPY", "XAUUSD",
]

WS_URL = "wss://prodata.tradingview.com/socket.io/websocket"
WS_HEADERS = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}
PARIS = ZoneInfo("Europe/Paris")
SESSION_DEFINITIONS = {
    "Sydney": (22 * 60, 7 * 60),
    "Tokyo": (0, 9 * 60),
    "Londres": (8 * 60, 17 * 60),
    "New York": (13 * 60, 22 * 60),
}

try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, OSError):
    pass


@dataclass(frozen=True)
class RenkoPoint:
    time: pd.Timestamp
    open: float
    close: float
    direction: int
    green_streak: int
    red_streak: int


def _session_id() -> str:
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


def _message(method: str, params: list) -> str:
    payload = json.dumps({"m": method, "p": params}, separators=(",", ":"))
    return f"~m~{len(payload)}~m~{payload}"


def _frames(raw: str) -> list[str]:
    if raw in ("~h", "h"):
        return [raw]
    frames: list[str] = []
    offset = 0
    while raw.startswith("~m~", offset):
        offset += 3
        boundary = raw.find("~m~", offset)
        if boundary < 0:
            break
        size = int(raw[offset:boundary])
        offset = boundary + 3
        frames.append(raw[offset:offset + size])
        offset += size
    return frames or [raw]


def _fetch_series(
    symbol: str,
    interval: str,
    count: int,
    renko_atr_length: int | None = None,
    timeout: int = 25,
    retries: int = 2,
) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        ws = None
        try:
            ws = create_connection(WS_URL, header=WS_HEADERS, timeout=timeout)
            ws.settimeout(timeout)
            sid = _session_id()
            ws.send(_message("chart_create_session", [sid, ""]))
            base = {"symbol": symbol, "adjustment": "splits", "session": "regular"}
            if renko_atr_length is None:
                descriptor = base
            else:
                descriptor = {
                    "symbol": base,
                    "type": "BarSetRenko@tv-prostudies-40!",
                    "inputs": {
                        "source": "close",
                        "sources": "Close",
                        "boxSize": renko_atr_length,
                        "style": "ATR",
                        "atrLength": renko_atr_length,
                        "wicks": True,
                    },
                }
            ws.send(_message("resolve_symbol", [sid, "sym_1", "=" + json.dumps(descriptor)]))
            ws.send(_message("create_series", [sid, "series_1", "s1", "sym_1", interval, count, ""]))

            points: list[dict] = []
            started = time.monotonic()
            while time.monotonic() - started < 18:
                try:
                    raw = ws.recv()
                except WebSocketTimeoutException:
                    continue
                except WebSocketConnectionClosedException:
                    break
                for frame in _frames(raw):
                    if frame in ("~h", "h"):
                        ws.send("~h")
                        continue
                    if '"m":"symbol_error"' in frame or '"m":"series_error"' in frame:
                        raise RuntimeError(f"TradingView rejected {symbol} {interval}")
                    if '"m":"timescale_update"' in frame:
                        payload = json.loads(frame)
                        series = payload.get("p", [None, {}])[1]
                        if isinstance(series, dict) and "series_1" in series:
                            points = series["series_1"].get("s", []) or points
                if "series_completed" in raw:
                    break

            rows = []
            for item in points:
                values = item.get("v", [])
                if len(values) >= 5 and all(value is not None for value in values[:5]):
                    rows.append(values[:5])
            if not rows:
                raise RuntimeError(f"No TradingView data for {symbol} {interval}")
            df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close"])
            df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
            for column in ("open", "high", "low", "close"):
                df[column] = pd.to_numeric(df[column], errors="coerce")
            df = df.dropna().sort_values("time", kind="stable")
            if renko_atr_length is None:
                df = df.drop_duplicates("time", keep="last")
            return df.reset_index(drop=True)
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
    raise RuntimeError(str(last_error) if last_error else "TradingView request failed")


def fetch_ohlc(pair: str, interval: str, count: int, tv_symbol: str | None = None) -> pd.DataFrame:
    return _fetch_series(tv_symbol or f"OANDA:{pair}", interval, count)


def fetch_renko(
    pair: str, interval: str, count: int, atr_length: int, max_streak: int, tv_symbol: str | None = None,
) -> list[RenkoPoint]:
    df = _fetch_series(tv_symbol or f"OANDA:{pair}", interval, count, renko_atr_length=atr_length)
    points: list[RenkoPoint] = []
    green = red = 0
    for row in df.itertuples(index=False):
        direction = 1 if row.close > row.open else -1 if row.close < row.open else 0
        if direction == 1:
            green, red = min(green + 1, max_streak), 0
        elif direction == -1:
            green, red = 0, min(red + 1, max_streak)
        else:
            green = red = 0
        points.append(RenkoPoint(row.time, float(row.open), float(row.close), direction, green, red))
    if not points:
        raise RuntimeError(f"No Renko bricks for {pair} {interval}")
    return points


def parabolic_sar(df: pd.DataFrame, start: float = 0.1, increment: float = 0.1, maximum: float = 0.2) -> list[float]:
    """TradingView-style ta.sar initialization and recurrence."""
    high = df["high"].astype(float).tolist()
    low = df["low"].astype(float).tolist()
    close = df["close"].astype(float).tolist()
    result = [math.nan] * len(df)
    if len(df) < 2:
        return result

    uptrend = close[1] > close[0]
    extreme = high[1] if uptrend else low[1]
    sar = low[0] if uptrend else high[0]
    acceleration = start
    result[1] = sar

    for i in range(2, len(df)):
        sar += acceleration * (extreme - sar)
        if uptrend:
            if sar > low[i]:
                uptrend = False
                sar = max(high[i], extreme)
                extreme = low[i]
                acceleration = start
            else:
                if high[i] > extreme:
                    extreme = high[i]
                    acceleration = min(acceleration + increment, maximum)
                sar = min(sar, low[i - 1], low[i - 2])
        else:
            if sar < high[i]:
                uptrend = True
                sar = min(low[i], extreme)
                extreme = high[i]
                acceleration = start
            else:
                if low[i] < extreme:
                    extreme = low[i]
                    acceleration = min(acceleration + increment, maximum)
                sar = max(sar, high[i - 1], high[i - 2])
        result[i] = sar
    return result


def px_state(point: RenkoPoint, price: float) -> int:
    high = max(point.open, point.close)
    low = min(point.open, point.close)
    return 1 if price > high else -1 if price < low else 0


def effective_bias(state: int, point: RenkoPoint) -> int:
    if state:
        return state
    if point.green_streak > 0 and point.red_streak == 0:
        return 1
    if point.red_streak > 0 and point.green_streak == 0:
        return -1
    return 0


def point_at(points: list[RenkoPoint], timestamp: pd.Timestamp, cursor: int) -> tuple[RenkoPoint | None, int]:
    while cursor + 1 < len(points) and points[cursor + 1].time <= timestamp:
        cursor += 1
    if cursor >= 0 and points[cursor].time <= timestamp:
        return points[cursor], cursor
    return None, cursor


INDEX_CODES = {str(asset["pair"]) for asset in FOREX_INDEX_ASSETS}  # {"DXY", "EXY", ...}


def pip_size(pair: str) -> float:
    if pair == "XAUUSD" or pair in INDEX_CODES:
        return 0.01  # convention JPY: indices devises TVC cotent ~100 avec 2 decimales
    return 0.01 if pair.endswith("JPY") else 0.0001


def ratio_fields(bulls: int, bears: int) -> tuple[float | None, str]:
    ratio = bulls / bears if bears else None
    if bulls == 0 and bears == 0:
        status = "NEUTRAL"
    elif bears == 0 or (ratio is not None and ratio >= 2.0):
        status = "BULL"
    elif ratio is not None and ratio <= 0.5:
        status = "BEAR"
    else:
        status = "NEUTRAL"
    return ratio, status


def replay_imp(pair: str, df: pd.DataFrame, renko: dict[str, list[RenkoPoint]]) -> dict:
    sar = parabolic_sar(df)
    cursors = {tf: -1 for tf in ("M", "W", "D")}
    last_bull_level = last_bear_level = None
    tracked_bull_level = tracked_bear_level = None
    tracked_bull_important = tracked_bear_important = False
    previous_bull_reversal = previous_bear_reversal = None
    signals: list[tuple[int, float, pd.Timestamp]] = []

    previous_close = previous_sar = None
    for i, row in enumerate(df.itertuples(index=False)):
        current_sar = sar[i]
        close = float(row.close)
        if math.isnan(current_sar):
            previous_close, previous_sar = close, current_sar
            continue

        bull_cross = previous_close is not None and not math.isnan(previous_sar) and previous_close <= previous_sar and close > current_sar
        bear_cross = previous_close is not None and not math.isnan(previous_sar) and previous_close >= previous_sar and close < current_sar
        if bull_cross:
            last_bull_level = tracked_bull_level = current_sar
            tracked_bull_important = False
        if bear_cross:
            last_bear_level = tracked_bear_level = current_sar
            tracked_bear_important = False

        points: dict[str, RenkoPoint] = {}
        ready = True
        for tf in ("M", "W", "D"):
            point, cursors[tf] = point_at(renko[tf], row.time, cursors[tf])
            if point is None:
                ready = False
                break
            points[tf] = point

        bull_imp = bear_imp = False
        if ready:
            states = {tf: px_state(points[tf], close) for tf in points}
            biases = {tf: effective_bias(states[tf], points[tf]) for tf in points}
            bull_count = sum(value == 1 for value in biases.values())
            bear_count = sum(value == -1 for value in biases.values())
            bull_conf = sum(biases[tf] == 1 and states[tf] == 1 for tf in points)
            bear_conf = sum(biases[tf] == -1 and states[tf] == -1 for tf in points)
            global_bull = bull_count >= 2
            global_bear = bear_count >= 2
            bull_reversal_ok = global_bear and bull_count >= 1
            bear_reversal_ok = global_bull and bear_count >= 1
            bull_renko_ok = bull_conf >= 2 or bull_reversal_ok
            bear_renko_ok = bear_conf >= 2 or bear_reversal_ok

            if not global_bear:
                previous_bull_reversal = None
            if not global_bull:
                previous_bear_reversal = None

            bear_raw = (
                not tracked_bear_important
                and tracked_bear_level is not None
                and last_bull_level is not None
                and close < last_bull_level
                and bear_renko_ok
            )
            if bear_raw:
                tracked_bear_important = True
                bear_imp = True
                if bear_reversal_ok:
                    bear_imp = previous_bear_reversal is None or tracked_bear_level < previous_bear_reversal
                    previous_bear_reversal = tracked_bear_level

            bull_raw = (
                not tracked_bull_important
                and tracked_bull_level is not None
                and last_bear_level is not None
                and close > last_bear_level
                and bull_renko_ok
            )
            if bull_raw:
                tracked_bull_important = True
                bull_imp = True
                if bull_reversal_ok:
                    bull_imp = previous_bull_reversal is None or tracked_bull_level > previous_bull_reversal
                    previous_bull_reversal = tracked_bull_level

        if bull_imp:
            signals.append((1, close, row.time))
        if bear_imp:
            signals.append((-1, close, row.time))
        previous_close, previous_sar = close, current_sar

    recent_signals = signals[-21:]
    bull_count = sum(direction == 1 for direction, _, _ in recent_signals)
    bear_count = sum(direction == -1 for direction, _, _ in recent_signals)
    ratio, status = ratio_fields(bull_count, bear_count)

    completed = []
    size = pip_size(pair)
    for previous, current in zip(signals, signals[1:]):
        completed.append((previous[0], (current[1] - previous[1]) / size))
    completed = completed[-21:]
    bull_moves = [move for direction, move in completed if direction == 1]
    bear_moves = [move for direction, move in completed if direction == -1]
    all_moves = [move for _, move in completed]

    def average(values: list[float]) -> float | None:
        return sum(values) / len(values) if values else None

    return {
        "imp_status": status,
        "imp_ratio": ratio,
        "bull_count": bull_count,
        "bear_count": bear_count,
        "bull_avg_pips": average(bull_moves),
        "bull_avg_n": len(bull_moves),
        "bear_avg_pips": average(bear_moves),
        "bear_avg_n": len(bear_moves),
        "all_avg_pips": average(all_moves),
        "all_avg_n": len(all_moves),
        "last_imp": "BULL" if signals and signals[-1][0] == 1 else "BEAR" if signals else None,
        "last_imp_time": signals[-1][2].isoformat() if signals else None,
    }


def current_renko_status(renko: dict[str, list[RenkoPoint]], reference_price: float) -> dict:
    result = {}
    for tf in ("M", "W", "D"):
        point = renko[tf][-1]
        state = px_state(point, reference_price)
        bias = effective_bias(state, point)
        streak = point.green_streak if bias == 1 else point.red_streak if bias == -1 else 0
        result[tf] = {
            "status": "BULL" if bias == 1 else "BEAR" if bias == -1 else "MIXED",
            "px": state,
            "streak": streak,
            "open": point.open,
            "close": point.close,
        }
    return result


def compute_pair(pair: str, candles_h1: int, candles_d1: int, renko_bricks: int, atr_length: int, max_streak: int) -> dict:
    h1 = fetch_ohlc(pair, "60", candles_h1)
    d1 = fetch_ohlc(pair, "D", candles_d1)
    renko = {
        tf: fetch_renko(pair, tf, renko_bricks, atr_length, max_streak)
        for tf in ("M", "W", "D")
    }
    reference_price = float(h1["close"].iloc[-1])
    return {
        "pair": pair,
        "reference_price": reference_price,
        "renko": current_renko_status(renko, reference_price),
        "H1": replay_imp(pair, h1, renko),
        "D1": replay_imp(pair, d1, renko),
    }


def compute_currency_imp(
    asset: dict, candles_h1: int, candles_d1: int, renko_bricks: int, atr_length: int, max_streak: int,
) -> dict:
    """Meme calcul que `compute_pair`, mais sur le symbole TradingView complet
    d'un indice devise (`asset["tv_symbol"]`, ex. TVC:DXY) plutot que sur
    OANDA:{pair} -- Renko M/W/D + IMP21 D1/H1 (imp_status + moyennes bull/
    bear/toutes, cf. `replay_imp`) pour la devise elle-meme, pas pour une
    paire. `pip_size(asset["pair"])` renvoie 0.01 pour les codes d'indice
    (cf. INDEX_CODES): convention JPY, les indices TVC cotent ~100 avec 2
    decimales. Sert de filtre de qualite au vote CURRENCY_INDEX (cf.
    `currency_trend_confirms`), pas de source de son signe -- celui-ci reste
    port par `currency_spread`/`signed_strength` (Renko + CHG%D, cf.
    `fetch_currency_index_rows`)."""
    tv_symbol = str(asset["tv_symbol"])
    code = str(asset["pair"])
    h1 = fetch_ohlc(code, "60", candles_h1, tv_symbol=tv_symbol)
    d1 = fetch_ohlc(code, "D", candles_d1, tv_symbol=tv_symbol)
    renko = {
        tf: fetch_renko(code, tf, renko_bricks, atr_length, max_streak, tv_symbol=tv_symbol)
        for tf in ("M", "W", "D")
    }
    reference_price = float(h1["close"].iloc[-1])
    return {
        "pair": code,
        "currency": asset.get("currency"),
        "reference_price": reference_price,
        "renko": current_renko_status(renko, reference_price),
        "H1": replay_imp(code, h1, renko),
        "D1": replay_imp(code, d1, renko),
    }


def fetch_currency_imp_rows(
    candles_h1: int = 5000,
    candles_d1: int = 2500,
    renko_bricks: int = 2500,
    atr_length: int = 14,
    max_streak: int = 50,
    workers: int = 4,
) -> dict[str, dict]:
    """`compute_currency_imp` pour les 8 `FOREX_INDEX_ASSETS`, en parallele --
    dict indexe par devise. Devise absente si son fetch a echoue -- traitee
    alors comme non applicable par `currency_trend_confirms` (aucun filtre,
    pas un rejet).

    `workers` a 4 (pas 8, un par devise) car TradingView renvoie un handshake
    429 Too Many Requests des que ~6-8 connexions websocket s'ouvrent en
    meme temps sur cet endpoint prive -- verifie en direct (8/8 devises OK a
    4 workers, echecs systematiques a 8). Chaque devise fait 5 fetches
    sequentiels (H1, D1, Renko M/W/D), donc ceci ralentit le run global sans
    le rendre sequentiel."""
    rows: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(
                compute_currency_imp, asset, candles_h1, candles_d1, renko_bricks, atr_length, max_streak,
            ): asset
            for asset in FOREX_INDEX_ASSETS
        }
        for future in as_completed(futures):
            asset = futures[future]
            try:
                row = future.result()
            except Exception:
                row = None
            if row is not None:
                rows[str(asset["currency"])] = row
    return rows


def flatten(result: dict) -> dict:
    row = {"pair": result["pair"], "reference_price": result["reference_price"]}
    for tf in ("M", "W", "D"):
        item = result["renko"][tf]
        row[f"renko_{tf}_status"] = item["status"]
        row[f"renko_{tf}_px"] = item["px"]
        row[f"renko_{tf}_streak"] = item["streak"]
    for chart in ("H1", "D1"):
        for key, value in result[chart].items():
            row[f"{chart}_{key}"] = value
    return row


def average_direction(value: float | None) -> str:
    if value is None or value == 0:
        return "NEUTRAL"
    return "BULL" if value > 0 else "BEAR"


def directional_average_confirms(result: dict, chart: str, direction: str) -> bool:
    value = (
        result[chart]["bull_avg_pips"]
        if direction == "BULL"
        else result[chart]["bear_avg_pips"]
    )
    if value is None:
        return False
    return value >= 0 if direction == "BULL" else value <= 0


def fetch_currency_index_rows(length: int = 14, candles: int = 300, max_streak: int = 50, workers: int = 4) -> dict[str, dict]:
    """Ligne complete `compute_asset_score` par devise (les 8 indices
    `FOREX_INDEX_ASSETS`) -- meme calcul que la section INDEX CHG%D de
    PAIRE_CHECK/FULL MOMENTUM. Reutilisee telle quelle par `currency_spread`/
    `signed_strength` (renko_full_alignment_29pairs.py) pour le vote
    CURRENCY_INDEX (cf. `currency_index_vote`): il faut la ligne complete, pas
    seulement l'icone, car `signed_strength` a besoin du score d'intensite
    (streak M/W/D pondere x CHG%D), pas seulement de son signe -- cf.
    `currency_index_vote` pour pourquoi deux devises de la meme couleur ne
    sont pas equivalentes. Devise absente du dict si son fetch a echoue --
    traitee alors comme neutre par `currency_index_vote`.

    `workers` a 4, pas 8: meme rate-limit 429 que `fetch_currency_imp_rows`
    (cf. son docstring) des que trop de connexions websocket s'ouvrent en
    meme temps sur cet endpoint TradingView prive."""
    rows: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(compute_asset_score, asset, length, candles, max_streak): asset
            for asset in FOREX_INDEX_ASSETS
        }
        for future in as_completed(futures):
            asset = futures[future]
            try:
                row = future.result()
            except Exception:
                row = None
            if row is not None:
                rows[str(asset["currency"])] = row
    return rows


CURRENCY_SNAPSHOT_MAX_AGE = 600  # secondes -- large marge pour tout le pipeline CI, court devant l'heure entre deux runs


def _serialize_currency_index_row(row: dict) -> dict:
    """`index_by_currency` entry -> dict JSON-safe: remplace les `TFState`
    (dataclasses) de `states` par des dicts plats via `dataclasses.asdict`.
    Symetrique de `_deserialize_currency_index_row`. `imp_by_currency` n'a
    pas besoin de cet aller-retour: `compute_currency_imp` ne renvoie deja
    que des types JSON natifs (str/float/int/None/dict)."""
    serialized = dict(row)
    states = row.get("states") or {}
    serialized["states"] = {tf: asdict(state) for tf, state in states.items()}
    return serialized


def _deserialize_currency_index_row(data: dict) -> dict:
    """Inverse de `_serialize_currency_index_row`: reconstruit les `TFState`
    a partir des dicts plats stockes dans le snapshot JSON."""
    row = dict(data)
    states = data.get("states") or {}
    row["states"] = {tf: TFState(**fields) for tf, fields in states.items()}
    return row


def save_currency_snapshot(
    path: Path, index_by_currency: dict[str, dict], imp_by_currency: dict[str, dict],
) -> None:
    """Persiste `index_by_currency`/`imp_by_currency` dans un fichier JSON
    partage entre les 3 scripts qui les consomment (imp_trend_29pairs.py,
    imp_trend_29pairs_v2.py, paire_check.py) -- tous tournent dans le meme
    job CI (cf. .github/workflows/triple_g_workflow.yml), le premier a
    fetcher laisse un instantane que les suivants reutilisent au lieu de
    refaire leur propre fetch live a quelques minutes d'ecart (cf.
    `load_currency_snapshot`, `fetch_or_load_currency_data`)."""
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "index_by_currency": {
            currency: _serialize_currency_index_row(row) for currency, row in index_by_currency.items()
        },
        "imp_by_currency": imp_by_currency,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, default=str), encoding="utf-8")


def load_currency_snapshot(
    path: Path, max_age_seconds: int = CURRENCY_SNAPSHOT_MAX_AGE,
) -> tuple[dict[str, dict], dict[str, dict]] | None:
    """Relit un instantane ecrit par `save_currency_snapshot`, si present et
    assez recent. None si absent, illisible ou perime (cf. `max_age_seconds`)
    -- l'appelant doit alors fetcher lui-meme en direct, cf.
    `fetch_or_load_currency_data`, ce qui garde chaque script utilisable seul
    (ex. un run manuel isole, sans les deux autres avant lui)."""
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        generated_at = datetime.fromisoformat(payload["generated_at"])
    except (OSError, json.JSONDecodeError, KeyError, ValueError):
        return None
    age = (datetime.now(timezone.utc) - generated_at).total_seconds()
    if age < 0 or age > max_age_seconds:
        return None
    try:
        index_by_currency = {
            currency: _deserialize_currency_index_row(row)
            for currency, row in payload["index_by_currency"].items()
        }
        imp_by_currency = payload["imp_by_currency"]
    except (KeyError, TypeError):
        return None
    return index_by_currency, imp_by_currency


def fetch_or_load_currency_data(
    snapshot_path: Path | None,
    snapshot_max_age: int,
    index_length: int,
    index_candles: int,
    index_max_streak: int,
    imp_h1_candles: int,
    imp_d1_candles: int,
    imp_renko_bricks: int,
    imp_atr_length: int,
    imp_max_streak: int,
) -> tuple[dict[str, dict], dict[str, dict]]:
    """Point d'entree unique pour `index_by_currency`/`imp_by_currency`,
    utilise par les 3 scripts (V1, V2, paire_check.py). Reutilise un
    instantane partage recent si `snapshot_path` en a un (cf.
    `load_currency_snapshot`) -- sinon fetch en direct (cf.
    `fetch_currency_index_rows`/`fetch_currency_imp_rows`) puis ecrit le
    resultat dans `snapshot_path` pour que les scripts suivants du meme run
    CI le reutilisent (cf. `save_currency_snapshot`). `snapshot_path=None`
    desactive le mecanisme (toujours fetcher en direct, jamais rien lire ni
    ecrire) -- utile pour un test isole qui ne doit pas dependre d'un
    instantane laisse par un run precedent."""
    if snapshot_path is not None:
        cached = load_currency_snapshot(snapshot_path, snapshot_max_age)
        if cached is not None:
            print(f"Instantane devise reutilise: {snapshot_path.resolve()}")
            return cached
    index_by_currency = fetch_currency_index_rows(index_length, index_candles, index_max_streak)
    imp_by_currency = fetch_currency_imp_rows(
        imp_h1_candles, imp_d1_candles, imp_renko_bricks, imp_atr_length, imp_max_streak,
    )
    if snapshot_path is not None:
        save_currency_snapshot(snapshot_path, index_by_currency, imp_by_currency)
    return index_by_currency, imp_by_currency


CURRENCY_INDEX_MIN_SPREAD = SYNC_MIN_EXPECTED  # 0.30 -- meme seuil que sync_marker (renko_full_alignment_29pairs.py)


def currency_trend_confirms(pair: str, direction: str, imp_by_currency: dict[str, dict]) -> tuple[int, int]:
    """Filtre de qualite du vote CURRENCY_INDEX: la devise forte (cf.
    `direction`) devrait elle-meme trender BULL sur son propre IMP21 (D1 et
    H1, cf. `compute_currency_imp`/`fetch_currency_imp_rows`), la devise
    faible BEAR -- jusqu'a 4 verifications (D1/H1 x base/cotee), ignorees
    quand la devise est absente de `imp_by_currency` ou que son IMP21 est
    NEUTRAL sur ce graphique (ni pour ni contre -- meme principe que
    `directional_average_confirms` cote paires). Renvoie
    (applicable, confirmations): `currency_index_vote` retrograde son vote en
    NEUTRAL quand applicable > 0 et confirmations == 0 (echec total), meme
    convention que le filtre qualite D1/H1 de V2 (cf.
    imp_trend_29pairs_v2.select_aligned_pairs_v2)."""
    if len(pair) != 6 or direction not in ("BULL", "BEAR"):
        return 0, 0
    base, quote = pair[:3], pair[3:]
    strong, weak = (base, quote) if direction == "BULL" else (quote, base)
    expected_status = {strong: "BULL", weak: "BEAR"}
    applicable = confirmations = 0
    for currency, expected in expected_status.items():
        row = imp_by_currency.get(currency)
        if row is None:
            continue
        for chart in ("D1", "H1"):
            status = row[chart]["imp_status"]
            if status == "NEUTRAL":
                continue
            applicable += 1
            if status == expected:
                confirmations += 1
    return applicable, confirmations


def currency_index_vote(
    pair: str, index_by_currency: dict[str, dict], imp_by_currency: dict[str, dict] | None = None,
) -> str:
    """Vote derive de l'ecart de force entre les deux devises de `pair`, via
    `currency_spread` (renko_full_alignment_29pairs.py): force signee de la
    base moins force signee de la cotee, ou la force signee (`signed_strength`)
    est deja orientee par le sens du CHG%D ET ponderee par son intensite.

    Deux devises rouges ne sont donc PAS equivalentes: GBP (+1.20) rouge est
    beaucoup plus faible que CAD (+0.00) rouge (quasi neutre), donc GBPCAD
    vote BEAR malgre la meme couleur des deux cotes -- de meme GBPJPY avec
    JPY (+0.29) rouge. A l'inverse, AUD (+0.28) et USD (+0.26), verts tous
    les deux, sont en pratique interchangeables (ecart 0.02): AUDUSD ne
    recoit aucun point, cf. CURRENCY_INDEX_MIN_SPREAD.

    NEUTRAL si l'ecart est indisponible (devise manquante -- XAUUSD: XAU n'a
    pas d'indice) ou trop faible (< CURRENCY_INDEX_MIN_SPREAD) pour trancher.
    Sinon BULL/BEAR selon le signe de l'ecart -- sauf si `imp_by_currency` est
    fourni et que `currency_trend_confirms` echoue totalement (les deux
    devises trendent elles-memes a l'inverse de leur role suppose): le vote
    est alors retrograde en NEUTRAL plutot que de compter sur un simple ecart
    de force que rien d'autre ne soutient."""
    spread = currency_spread({"pair": pair, "asset_type": "PAIR"}, index_by_currency)
    if spread is None or abs(spread) < CURRENCY_INDEX_MIN_SPREAD:
        return "NEUTRAL"
    direction = "BULL" if spread > 0 else "BEAR"
    if imp_by_currency:
        applicable, confirmations = currency_trend_confirms(pair, direction, imp_by_currency)
        if applicable > 0 and confirmations == 0:
            return "NEUTRAL"
    return direction


def currency_index_diverges(votes: dict[str, str], direction: str) -> bool:
    """True si CURRENCY_INDEX contredit activement `direction` (BEAR quand la
    paire est BULL, ou l'inverse) -- pas seulement NEUTRAL/abstention.

    Sert de veto (cf. select_aligned_pairs / select_aligned_pairs_v2), sur le
    meme principe que `is_engine_divergent` dans renko_full_alignment_29pairs.py:
    une contradiction active exclut la paire plutot que de se diluer comme un
    simple vote de moins parmi les autres -- ex. GBPJPY BULL (Renko/IMP21)
    avec GBP tres nettement plus faible que JPY d'apres les indices devises
    (ecart >> CURRENCY_INDEX_MIN_SPREAD) est exclue, meme si les autres votes
    suffisaient a eux seuls a atteindre le seuil."""
    opposite = "BEAR" if direction == "BULL" else "BULL"
    return votes["CURRENCY_INDEX"] == opposite


def screening_votes(
    result: dict,
    index_by_currency: dict[str, dict] | None = None,
    imp_by_currency: dict[str, dict] | None = None,
) -> dict[str, str]:
    return {
        "RENKO_M": result["renko"]["M"]["status"],
        "RENKO_W": result["renko"]["W"]["status"],
        "RENKO_D": result["renko"]["D"]["status"],
        "D1_IMP21": result["D1"]["imp_status"],
        "D1_ALL_AVG": average_direction(result["D1"]["all_avg_pips"]),
        "D1_BULL_AVG": average_direction(result["D1"]["bull_avg_pips"]),
        "D1_BEAR_AVG": average_direction(result["D1"]["bear_avg_pips"]),
        "H1_IMP21": result["H1"]["imp_status"],
        "H1_ALL_AVG": average_direction(result["H1"]["all_avg_pips"]),
        "H1_BULL_AVG": average_direction(result["H1"]["bull_avg_pips"]),
        "H1_BEAR_AVG": average_direction(result["H1"]["bear_avg_pips"]),
        "CURRENCY_INDEX": currency_index_vote(result["pair"], index_by_currency or {}, imp_by_currency),
    }


CURRENCY_CONSENSUS_THRESHOLD = 4  # sur 5 votes reels -- cf. currency_consensus_status


def currency_consensus_status(currency_row: dict) -> str:
    """BULL/BEAR/NEUTRAL resumant une devise sur les 5 votes reellement
    distincts de `screening_votes` calcules pour cette devise seule (RENKO_M,
    RENKO_W, RENKO_D, D1_IMP21, H1_IMP21) -- CURRENCY_INDEX exclu, il n'a pas
    de sens pour une devise isolee (il compare deux devises entre elles).

    Les 6 moyennes D1/H1 ne comptent pas: memes principe et seuil que la
    correction deja appliquee aux paires en V2 (cf. imp_trend_29pairs_v2.py
    module docstring point 1) -- elles derivent toutes de la meme serie de
    signaux que leur IMP21 respectif, 4 votes qui basculent ensemble ne sont
    pas 4 confirmations independantes.

    BULL/BEAR si >= CURRENCY_CONSENSUS_THRESHOLD des 5 votes s'accordent,
    NEUTRAL sinon (pas de consensus net). Cf. paire_check.currency_consensus_ball
    pour le rendu en boule 🟢/🔴/⚪ -- ce module ne s'occupe que du calcul."""
    votes = screening_votes(currency_row)
    keys = ("RENKO_M", "RENKO_W", "RENKO_D", "D1_IMP21", "H1_IMP21")
    bull = sum(votes[key] == "BULL" for key in keys)
    bear = sum(votes[key] == "BEAR" for key in keys)
    if bull >= CURRENCY_CONSENSUS_THRESHOLD:
        return "BULL"
    if bear >= CURRENCY_CONSENSUS_THRESHOLD:
        return "BEAR"
    return "NEUTRAL"


CURRENCY_DAILY_MOVE_MIN_MAGNITUDE = 0.10  # % -- en dessous, daily_chg reste NEUTRAL


def currency_daily_status(daily_chg: float | None) -> str:
    """BULL/BEAR/NEUTRAL a partir du `daily_chg` d'une devise (meme valeur
    que l'icone 🟢/🔴 en tete de sa ligne INDEX CHG%D), avec un seuil minimum
    (CURRENCY_DAILY_MOVE_MIN_MAGNITUDE) en dessous duquel le mouvement est
    trop proche de zero pour compter comme un vrai signal directionnel --
    meme principe que CURRENCY_INDEX_MIN_SPREAD, applique ici a une seule
    devise plutot qu'a un ecart entre deux. Sans ce seuil, un CAD +0.00%
    ou un USD +0.01% comptaient comme BULL au meme titre qu'un GBP -1.20%,
    ce qui rendait `currency_diverges_from_its_own_day` sensible au bruit
    proche de zero -- cf. son docstring pour l'usage."""
    if not isinstance(daily_chg, (int, float)) or abs(daily_chg) < CURRENCY_DAILY_MOVE_MIN_MAGNITUDE:
        return "NEUTRAL"
    return "BULL" if daily_chg > 0 else "BEAR"


def currency_diverges_from_its_own_day(
    currency: str,
    index_by_currency: dict[str, dict] | None,
    imp_by_currency: dict[str, dict] | None,
) -> bool:
    """True si le mouvement du jour d'une devise (cf. `currency_daily_status`)
    contredit ouvertement son propre consensus structurel (cf.
    `currency_consensus_status`) -- BULL le jour contre BEAR de fond, ou
    l'inverse. Ne se declenche PAS quand l'un des deux est simplement
    NEUTRAL/indecis (doublement indecise, uni-indecise ou doublement
    validee: pas une contradiction) -- ni quand le mouvement du jour est
    trop faible pour compter (CURRENCY_DAILY_MOVE_MIN_MAGNITUDE). Devise
    absente de l'un des deux dicts -> False, pas assez d'information pour
    conclure a une contradiction.

    Ex. GBP -0.24% aujourd'hui (BEAR) alors que son consensus structurel est
    BULL (Renko M/W/D + D1/H1 IMP21 majoritairement haussiers): probablement
    un pullback dans une tendance etablie, pas un retournement confirme --
    mais assez incertain pour justifier d'exclure les paires concernees du
    suivi de position plutot que de trancher a la place de l'utilisateur."""
    index_row = (index_by_currency or {}).get(currency)
    imp_row = (imp_by_currency or {}).get(currency)
    if index_row is None or imp_row is None:
        return False
    daily_status = currency_daily_status(index_row.get("daily_chg"))
    consensus_status = currency_consensus_status(imp_row)
    if daily_status == "NEUTRAL" or consensus_status == "NEUTRAL":
        return False
    return daily_status != consensus_status


def pair_touches_a_divergent_currency(
    pair: str,
    index_by_currency: dict[str, dict] | None,
    imp_by_currency: dict[str, dict] | None,
) -> bool:
    """True si au moins une des deux devises de `pair` a une contradiction
    jour/consensus (cf. `currency_diverges_from_its_own_day`)."""
    if len(pair) != 6:
        return False
    base, quote = pair[:3], pair[3:]
    return (
        currency_diverges_from_its_own_day(base, index_by_currency, imp_by_currency)
        or currency_diverges_from_its_own_day(quote, index_by_currency, imp_by_currency)
    )


VOTE_THRESHOLD_V1 = 9  # sur 12 votes (les 11 d'origine + CURRENCY_INDEX, cf. currency_index_vote)


def select_aligned_pairs(
    results: list[dict],
    index_by_currency: dict[str, dict] | None = None,
    imp_by_currency: dict[str, dict] | None = None,
) -> list[dict]:
    selected = []
    for result in results:
        votes = screening_votes(result, index_by_currency, imp_by_currency)
        if votes["D1_IMP21"] == "NEUTRAL" or votes["H1_IMP21"] == "NEUTRAL":
            continue
        bull_votes = sum(vote == "BULL" for vote in votes.values())
        bear_votes = sum(vote == "BEAR" for vote in votes.values())
        direction = (
            "BULL" if bull_votes >= VOTE_THRESHOLD_V1
            else "BEAR" if bear_votes >= VOTE_THRESHOLD_V1
            else None
        )
        if direction is None:
            continue
        if currency_index_diverges(votes, direction):
            continue
        confirmations = bull_votes if direction == "BULL" else bear_votes
        renko_confirmations = sum(votes[f"RENKO_{tf}"] == direction for tf in ("M", "W", "D"))
        if votes["RENKO_D"] != direction or renko_confirmations < 2:
            continue
        directional_avg_confirmations = sum(
            directional_average_confirms(result, chart, direction)
            for chart in ("D1", "H1")
        )
        avg_vote_confirmations = sum(
            votes[key] == direction
            for key in ("D1_BULL_AVG", "D1_BEAR_AVG", "H1_BULL_AVG", "H1_BEAR_AVG")
        )
        if directional_avg_confirmations == 0:
            rank_tier, rank_reason = 4, f"{confirmations}/12 MAIS AVG {direction} 0/2"
        elif confirmations == 12:
            rank_tier, rank_reason = 1, "12/12"
        elif confirmations == 11:
            rank_tier, rank_reason = 2, "11/12"
        elif confirmations == 10:
            rank_tier, rank_reason = 3, "10/12"
        else:
            rank_tier, rank_reason = 4, "9/12"

        # Devise en interne contradictoire (mouvement du jour vs consensus
        # structurel, cf. `pair_touches_a_divergent_currency`) -- non
        # tradable plutot qu'exclue: reste affichee/loggee, mais retrogradee
        # au pire rang et retiree du suivi de position (cf. `main`).
        tradable = not pair_touches_a_divergent_currency(result["pair"], index_by_currency, imp_by_currency)
        if not tradable:
            rank_tier = 4
            rank_reason += " MAIS DEVISE JOUR/CONSENSUS CONTRADICTOIRE"

        selected.append({
            "pair": result["pair"],
            "direction": direction,
            "confirmations": confirmations,
            "renko_confirmations": renko_confirmations,
            "avg_vote_confirmations": avg_vote_confirmations,
            "directional_avg_confirmations": directional_avg_confirmations,
            "tradable": tradable,
            "rank_tier": rank_tier,
            "rank_reason": rank_reason,
            "RENKO_M": votes["RENKO_M"],
            "RENKO_W": votes["RENKO_W"],
            "RENKO_D": votes["RENKO_D"],
            "D1_IMP21": votes["D1_IMP21"],
            "D1_ALL_AVG": votes["D1_ALL_AVG"],
            "D1_ALL_AVG_PIPS": result["D1"]["all_avg_pips"],
            "D1_BULL_AVG": votes["D1_BULL_AVG"],
            "D1_BEAR_AVG": votes["D1_BEAR_AVG"],
            "D1_DIRECTIONAL_AVG_PIPS": result["D1"]["bull_avg_pips"] if direction == "BULL" else result["D1"]["bear_avg_pips"],
            "H1_IMP21": votes["H1_IMP21"],
            "H1_ALL_AVG": votes["H1_ALL_AVG"],
            "H1_ALL_AVG_PIPS": result["H1"]["all_avg_pips"],
            "H1_BULL_AVG": votes["H1_BULL_AVG"],
            "H1_BEAR_AVG": votes["H1_BEAR_AVG"],
            "H1_DIRECTIONAL_AVG_PIPS": result["H1"]["bull_avg_pips"] if direction == "BULL" else result["H1"]["bear_avg_pips"],
            "CURRENCY_INDEX": votes["CURRENCY_INDEX"],
        })
    return sorted(
        selected,
        key=lambda item: (
            0 if item["direction"] == "BULL" else 1,
            item["rank_tier"],
            item["pair"],
        ),
    )


def load_eligible_state(path: Path) -> dict:
    if not path.exists():
        return {"version": 1, "active": {}, "events": []}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(state, dict):
            state.setdefault("version", 1)
            state.setdefault("active", {})
            state.setdefault("events", [])
            return state
    except (OSError, json.JSONDecodeError):
        pass
    return {"version": 1, "active": {}, "events": []}


def invalidation_reason(
    result: dict | None,
    index_by_currency: dict[str, dict] | None = None,
    imp_by_currency: dict[str, dict] | None = None,
) -> str:
    if result is None:
        return "DATA_UNAVAILABLE"
    if result["D1"]["imp_status"] == "NEUTRAL":
        return "D1_IMP21_NEUTRAL"
    if result["H1"]["imp_status"] == "NEUTRAL":
        return "H1_IMP21_NEUTRAL"
    votes = screening_votes(result, index_by_currency, imp_by_currency)
    values = list(votes.values())
    direction = (
        "BULL" if values.count("BULL") >= VOTE_THRESHOLD_V1
        else "BEAR" if values.count("BEAR") >= VOTE_THRESHOLD_V1
        else None
    )
    if direction is None:
        return "LESS_THAN_9_OF_12"
    if currency_index_diverges(votes, direction):
        return "CURRENCY_INDEX_DIVERGENT"
    if result["renko"]["D"]["status"] != direction:
        return "RENKO_D_NOT_ALIGNED"
    renko_aligned = sum(result["renko"][tf]["status"] == direction for tf in ("M", "W", "D"))
    if renko_aligned < 2:
        return "LESS_THAN_2_RENKO_ALIGNED"
    return "DIRECTION_CHANGED"


def update_eligible_state(
    path: Path,
    selected: list[dict],
    results: list[dict],
    index_by_currency: dict[str, dict] | None = None,
    imp_by_currency: dict[str, dict] | None = None,
) -> tuple[dict, list[dict]]:
    state = load_eligible_state(path)
    previous = state.get("active", {})
    current_selection = {item["pair"]: item for item in selected}
    results_by_pair = {item["pair"]: item for item in results}
    now = datetime.now(timezone.utc).isoformat()
    active: dict[str, dict] = {}
    run_events: list[dict] = []

    for pair, item in current_selection.items():
        old = previous.get(pair)
        changed = old is not None and old.get("direction") != item["direction"]
        if old is None or changed:
            event = {
                "time_utc": now,
                "event": "ELIGIBLE" if old is None else "DIRECTION_CHANGED",
                "pair": pair,
                "direction": item["direction"],
                "previous_direction": old.get("direction") if changed else None,
                "confirmations": item["confirmations"],
            }
            run_events.append(event)
            entered_at = now
        else:
            entered_at = old.get("entered_at", now)
        active[pair] = {
            "direction": item["direction"],
            "confirmations": item["confirmations"],
            "entered_at": entered_at,
            "last_seen_at": now,
            "snapshot": item,
        }

    for pair, old in previous.items():
        if pair in current_selection:
            continue
        run_events.append({
            "time_utc": now,
            "event": "INVALIDATED",
            "pair": pair,
            "direction": old.get("direction"),
            "reason": invalidation_reason(results_by_pair.get(pair), index_by_currency, imp_by_currency),
            "entered_at": old.get("entered_at"),
        })

    history = state.get("events", []) + run_events
    state.update({
        "version": 1,
        "updated_at": now,
        "active": active,
        "events": history[-2000:],
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return state, run_events


def print_eligibility_tracking(state: dict, events: list[dict]) -> None:
    print("\nSUIVI ELIGIBILITE")
    if not events:
        print(f'Aucun changement. {len(state.get("active", {}))} paire(s) toujours suivie(s).')
        return
    for event in events:
        if event["event"] == "ELIGIBLE":
            print(f'+ ENTREE {event["pair"]} {event["direction"]} ({event["confirmations"]}/12)')
        elif event["event"] == "DIRECTION_CHANGED":
            print(f'! RETOURNEMENT {event["pair"]}: {event["previous_direction"]} -> {event["direction"]}')
        else:
            print(f'- SORTIE {event["pair"]} {event.get("direction", "")} ({event["reason"]})')


def fmt(value: float | None) -> str:
    return "N/A" if value is None else f"{value:.2f}"


def print_table(results: list[dict]) -> None:
    rows = []
    for result in results:
        renko = result["renko"]
        rows.append({
            "PAIR": result["pair"],
            "M": renko["M"]["status"],
            "W": renko["W"]["status"],
            "D": renko["D"]["status"],
            "H1 IMP21": f'{result["H1"]["imp_status"]} {fmt(result["H1"]["imp_ratio"])} ({result["H1"]["bull_count"]}/{result["H1"]["bear_count"]})',
            "H1 BULL": fmt(result["H1"]["bull_avg_pips"]),
            "H1 BEAR": fmt(result["H1"]["bear_avg_pips"]),
            "H1 ALL": fmt(result["H1"]["all_avg_pips"]),
            "D1 IMP21": f'{result["D1"]["imp_status"]} {fmt(result["D1"]["imp_ratio"])} ({result["D1"]["bull_count"]}/{result["D1"]["bear_count"]})',
            "D1 BULL": fmt(result["D1"]["bull_avg_pips"]),
            "D1 BEAR": fmt(result["D1"]["bear_avg_pips"]),
            "D1 ALL": fmt(result["D1"]["all_avg_pips"]),
        })
    print(pd.DataFrame(rows).to_string(index=False))


def print_selection(selected: list[dict]) -> None:
    print("\nSELECTION 9 CRITERES SUR 12")
    if not selected:
        print("Aucune paire retenue.")
        return
    for direction in ("BULL", "BEAR"):
        rows = [item for item in selected if item["direction"] == direction]
        if not rows:
            continue
        print(f"\n{direction}")
        display = [{
            "RANG": item["rank_tier"],
            "PAIR": item["pair"],
            "SCORE": f'{item["confirmations"]}/12',
            "TRADABLE": "oui" if item["tradable"] else "NON",
            "AVG4": f'{item["avg_vote_confirmations"]}/4',
            "AVG DIR": f'{item["directional_avg_confirmations"]}/2',
            "CRITERE": item["rank_reason"],
            "M": item["RENKO_M"],
            "W": item["RENKO_W"],
            "D": item["RENKO_D"],
            "D1 IMP21": item["D1_IMP21"],
            "D1 ALL": f'{item["D1_ALL_AVG"]} ({fmt(item["D1_ALL_AVG_PIPS"])})',
            "D1 AVG DIR": fmt(item["D1_DIRECTIONAL_AVG_PIPS"]),
            "H1 IMP21": item["H1_IMP21"],
            "H1 ALL": f'{item["H1_ALL_AVG"]} ({fmt(item["H1_ALL_AVG_PIPS"])})',
            "H1 AVG DIR": fmt(item["H1_DIRECTIONAL_AVG_PIPS"]),
            "IDX": item["CURRENCY_INDEX"],
        } for item in rows]
        print(pd.DataFrame(display).to_string(index=False))


def _trading_session_id(now_paris: datetime, start_minute: int, end_minute: int) -> str | None:
    minute = now_paris.hour * 60 + now_paris.minute
    if start_minute < end_minute:
        return now_paris.date().isoformat() if start_minute <= minute < end_minute else None
    if minute >= start_minute:
        return now_paris.date().isoformat()
    if minute < end_minute:
        return (now_paris.date() - timedelta(days=1)).isoformat()
    return None


def load_session_state(path: Path) -> dict:
    if path.exists():
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(state, dict):
                return state
        except (OSError, json.JSONDecodeError):
            pass
    return {"version": 1, "sessions": {}}


def update_session_tracking(
    path: Path,
    selected: list[dict],
    results: list[dict],
    now: datetime | None = None,
) -> dict:
    now_utc = now or datetime.now(timezone.utc)
    now_paris = now_utc.astimezone(PARIS)
    state = load_session_state(path)
    sessions = state.setdefault("sessions", {})
    selected_by_pair = {item["pair"]: item for item in selected}
    prices = {item["pair"]: item["reference_price"] for item in results}

    for name, (start_minute, end_minute) in SESSION_DEFINITIONS.items():
        session = sessions.setdefault(name, {
            "realized_pips": 0.0,
            "current_session_realized_pips": 0.0,
            "last_session_pips": 0.0,
            "active_session_id": None,
            "positions": {},
            "events": [],
        })
        positions = session.setdefault("positions", {})
        events = session.setdefault("events", [])
        current_id = _trading_session_id(now_paris, start_minute, end_minute)
        just_opened = False  # nouveau cycle qui commence ce run -> vider la trajectoire
        just_closed = False  # cycle qui vient de se clore ce run -> dernier point a garder

        def close_position(pair: str, reason: str) -> bool:
            position = positions.get(pair)
            price = prices.get(pair)
            if position is None or price is None:
                return False
            sign = 1 if position["direction"] == "BULL" else -1
            trade_pips = sign * (price - position["entry_price"]) / pip_size(pair)
            session["realized_pips"] = float(session.get("realized_pips", 0.0)) + trade_pips
            session["current_session_realized_pips"] = float(
                session.get("current_session_realized_pips", 0.0)
            ) + trade_pips
            events.append({
                "time_utc": now_utc.isoformat(),
                "event": "EXIT",
                "pair": pair,
                "direction": position["direction"],
                "entry_price": position["entry_price"],
                "exit_price": price,
                "pips": trade_pips,
                "reason": reason,
            })
            del positions[pair]
            return True

        if current_id is None:
            for pair in list(positions):
                close_position(pair, "SESSION_END")
            if not positions:
                if session.get("active_session_id") is not None:
                    session["last_session_pips"] = float(
                        session.get("current_session_realized_pips", 0.0)
                    )
                    just_closed = True
                session["active_session_id"] = None
        else:
            previous_id = session.get("active_session_id")
            if previous_id not in (None, current_id):
                for pair in list(positions):
                    close_position(pair, "SESSION_ROLLOVER")
                session["last_session_pips"] = float(
                    session.get("current_session_realized_pips", 0.0)
                )
                just_closed = True
            if previous_id != current_id:
                session["current_session_realized_pips"] = 0.0
                just_opened = True
            session["active_session_id"] = current_id

            for pair in list(positions):
                item = selected_by_pair.get(pair)
                if item is None:
                    close_position(pair, "INVALIDATED")
                elif item["direction"] != positions[pair]["direction"]:
                    close_position(pair, "DIRECTION_CHANGED")

            for pair, item in selected_by_pair.items():
                if pair in positions or pair not in prices:
                    continue
                positions[pair] = {
                    "direction": item["direction"],
                    "entry_price": prices[pair],
                    "entry_time_utc": now_utc.isoformat(),
                }
                events.append({
                    "time_utc": now_utc.isoformat(),
                    "event": "ENTRY",
                    "pair": pair,
                    "direction": item["direction"],
                    "entry_price": prices[pair],
                })

        unrealized = 0.0
        for pair, position in positions.items():
            price = prices.get(pair)
            if price is None:
                continue
            sign = 1 if position["direction"] == "BULL" else -1
            unrealized += sign * (price - position["entry_price"]) / pip_size(pair)
        session["unrealized_pips"] = unrealized
        # "cumul" (total_pips): cumul a vie de `realized_pips` (chaque cloture
        # de CETTE session s'y ajoute, jamais remis a zero) + le flottant en
        # cours -- mesure l'efficacite de la strategie sur cette session dans
        # la duree. "jour" (daily_pips): resultat du cycle courant/le plus
        # recent -- retombe a 0 exactement a l'ouverture (cf.
        # `current_session_realized_pips`, reset a chaque nouveau
        # `active_session_id`), fluctue avec le flottant pendant que la
        # session est ouverte, se fige a la cloture (`last_session_pips`)
        # jusqu'a la PROCHAINE ouverture de cette meme session -- pas jusqu'a
        # minuit: un solde fige d'hier soir reste donc affiche tel quel avant
        # la reouverture du jour, ce n'est pas un bug (cf. exemple valide
        # avec l'utilisateur: Sydney 22h->7h->22h, +45 fige puis +0/+45 a la
        # reouverture, +12/+57 une fois +12 flottant).
        session["total_pips"] = float(session.get("realized_pips", 0.0)) + unrealized
        # "cmp": cumul repartant a 0 pour V1 ET V2 au meme instant (le premier
        # run de chacune apres l'ajout de ce champ), pour comparer les deux
        # equitablement sans que le passif de V1 (qui tournait deja avant la
        # naissance de V2) ne fausse la comparaison. `comparison_baseline`
        # s'ancre une seule fois, au niveau de cumul ("total_pips") de ce tout
        # premier run -- cmp = 0 pour les deux ce jour-la, puis evolue en
        # parallele ensuite. Champ generique, sans lien avec V1/V2
        # specifiquement: n'importe quel script qui reutilise
        # update_session_tracking en beneficie de la meme facon.
        if "comparison_baseline" not in session:
            session["comparison_baseline"] = session["total_pips"]
        session["comparison_pips"] = session["total_pips"] - session["comparison_baseline"]
        session["is_active"] = current_id is not None
        session["daily_pips"] = (
            float(session.get("current_session_realized_pips", 0.0)) + unrealized
            if current_id is not None
            else float(session.get("last_session_pips", 0.0))
        )
        session["events"] = events[-2000:]

        # Trajectoire "jour" du cycle courant/le plus recent (cf. `session_lines`):
        # un point par run pendant que la session est ouverte, + le point de
        # cloture, videe a chaque nouvelle ouverture -- ne recommence pas a
        # accumuler des points identiques une fois figee (pas de point ajoute
        # run apres run pendant que la session reste fermee).
        if just_opened:
            session["daily_pips_history"] = []
        history = session.setdefault("daily_pips_history", [])
        if current_id is not None or just_closed:
            history.append({"time_utc": now_utc.isoformat(), "daily_pips": session["daily_pips"]})
        session["daily_pips_history"] = history[-200:]

    state["version"] = 1
    state["updated_at"] = now_utc.isoformat()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return state


def session_trajectory_line(session: dict) -> str | None:
    """Ligne `↳ depuis HHhMM: +0.0 → HHhMM +x.x → ... (clôturée)` retracant
    l'evolution de "jour" du cycle courant/le plus recent d'une session,
    point par run (cf. `daily_pips_history` dans `update_session_tracking`).
    None si l'historique est vide (session jamais encore ouverte sous ce
    mecanisme)."""
    history = session.get("daily_pips_history") or []
    if not history:
        return None
    first_time = datetime.fromisoformat(history[0]["time_utc"]).astimezone(PARIS)
    steps = [f"{history[0]['daily_pips']:+.1f}"]
    for point in history[1:]:
        point_time = datetime.fromisoformat(point["time_utc"]).astimezone(PARIS)
        steps.append(f"{point_time:%Hh%M} {point['daily_pips']:+.1f}")
    suffix = "" if session.get("is_active") else " (clôturée)"
    return f"  ↳ depuis {first_time:%Hh%M}: " + " → ".join(steps) + suffix


def session_lines(session_state: dict | None) -> list[str]:
    """Bloc `Sessions de trading (jour/cumul/cmp)` + une ligne par session
    (cf. `update_session_tracking`), suivie de sa trajectoire heure par
    heure (cf. `session_trajectory_line`) -- factorise hors de
    `build_telegram_message` pour etre reutilise tel quel par d'autres
    variantes du message (ex. imp_trend_29pairs_v2.py). "cmp" (cf.
    `comparison_pips`) repart de 0 au meme instant pour toute variante qui
    partage ce mecanisme -- comparable entre V1 et V2 sans que l'anciennete
    de V1 ne fausse la lecture, contrairement a "cumul" (a vie, cf.
    docstring de `update_session_tracking`). Vide si `session_state` est
    None."""
    if session_state is None:
        return []
    lines = ["Sessions de trading (jour/cumul/cmp)", ""]
    sessions = session_state.get("sessions", {})
    for name in SESSION_DEFINITIONS:
        session = sessions.get(name, {})
        daily = float(session.get("daily_pips", 0.0))
        total = float(session.get("total_pips", 0.0))
        comparison = float(session.get("comparison_pips", 0.0))
        suffix = " • EN COURS" if session.get("is_active") else ""
        lines.append(f"{name} : ({daily:+.1f}/{total:+.1f}/{comparison:+.1f}){suffix}")
        trajectory = session_trajectory_line(session)
        if trajectory:
            lines.append(trajectory)
    return lines


def build_telegram_message(
    selected: list[dict],
    session_state: dict | None = None,
    now: datetime | None = None,
) -> str:
    timestamp = (now or datetime.now(timezone.utc)).astimezone(PARIS)
    ordered = sorted(
        selected,
        key=lambda item: (
            item["rank_tier"],
            -item["confirmations"],
            item["pair"],
        ),
    )
    lines = ["📊 IMP TREND", ""]
    if ordered:
        for item in ordered:
            marker = "🟢" if item["direction"] == "BULL" else "🔴"
            suffix = "" if item["tradable"] else " (non tradée)"
            lines.append(f'{marker}{item["pair"]}{suffix}')
    else:
        lines.append("Aucune paire filtrée")
    extra = session_lines(session_state)
    if extra:
        lines.append("")
        lines.extend(extra)
    lines.extend(["", f"⏰ {timestamp:%Y-%m-%d %H:%M} Paris"])
    return "\n".join(lines)


def send_telegram_message(message: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("Telegram: identifiants manquants, envoi ignore.")
        return False
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message},
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            print(f"Telegram: echec ({payload.get('description', 'reponse inconnue')}).")
            return False
        print("Telegram: message IMP TREND envoye.")
        return True
    except Exception as exc:
        print(f"Telegram: echec non fatal ({exc}).")
        return False


def telegram_window_is_open(now: datetime | None = None) -> bool:
    timestamp = (now or datetime.now(timezone.utc)).astimezone(PARIS)
    return 6 <= timestamp.hour <= 23


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TradingView IMP Trend extractor for 29 OANDA instruments")
    parser.add_argument("--pairs", nargs="+", default=PAIRS_29, help="Pairs to process, e.g. EURUSD USDJPY")
    parser.add_argument("--h1-candles", type=int, default=5000)
    parser.add_argument("--d1-candles", type=int, default=2500)
    parser.add_argument("--renko-bricks", type=int, default=2500)
    parser.add_argument("--atr-length", type=int, default=14)
    parser.add_argument("--max-streak", type=int, default=50)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument(
        "--index-candles", type=int, default=300,
        help="Bougies/briques M/W/D par indice devise pour le vote CURRENCY_INDEX (cf. fetch_currency_index_rows).",
    )
    parser.add_argument(
        "--currency-snapshot", type=str, default="currency_snapshot.json",
        help="Fichier partage entre V1/V2/paire_check.py pour reutiliser un fetch devise recent au lieu de "
             "refetcher (cf. fetch_or_load_currency_data). Vide pour desactiver (toujours fetcher en direct).",
    )
    parser.add_argument(
        "--currency-snapshot-max-age", type=int, default=CURRENCY_SNAPSHOT_MAX_AGE,
        help="Age maximum en secondes d'un instantane devise reutilisable.",
    )
    parser.add_argument("--csv", type=Path, default=Path("imp_trend_29pairs.csv"))
    parser.add_argument("--json", type=Path, default=Path("imp_trend_29pairs.json"))
    parser.add_argument("--selection-csv", type=Path, default=Path("imp_trend_selection.csv"))
    parser.add_argument("--selection-json", type=Path, default=Path("imp_trend_selection.json"))
    parser.add_argument("--eligible-state", type=Path, default=Path("imp_trend_eligible_state.json"))
    parser.add_argument("--sessions-state", type=Path, default=Path("imp_trend_sessions_state.json"))
    parser.add_argument("--telegram", action="store_true", help="Envoyer les paires filtrees sur Telegram")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pairs = [pair.upper() for pair in args.pairs]
    snapshot_path = Path(args.currency_snapshot) if args.currency_snapshot else None
    index_by_currency, imp_by_currency = fetch_or_load_currency_data(
        snapshot_path, args.currency_snapshot_max_age,
        args.atr_length, args.index_candles, args.max_streak,
        args.h1_candles, args.d1_candles, args.renko_bricks, args.atr_length, args.max_streak,
    )
    results: list[dict] = []
    errors: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(
                compute_pair,
                pair,
                args.h1_candles,
                args.d1_candles,
                args.renko_bricks,
                args.atr_length,
                args.max_streak,
            ): pair
            for pair in pairs
        }
        for future in as_completed(futures):
            pair = futures[future]
            try:
                results.append(future.result())
                print(f"{pair}: OK")
            except Exception as exc:
                errors.append((pair, str(exc)))
                print(f"{pair}: ERROR - {exc}")

    order = {pair: index for index, pair in enumerate(pairs)}
    results.sort(key=lambda item: order[item["pair"]])
    if results:
        print()
        print_table(results)
        selected = select_aligned_pairs(results, index_by_currency, imp_by_currency)
        print_selection(selected)
        eligible_state, eligibility_events = update_eligible_state(
            args.eligible_state, selected, results, index_by_currency, imp_by_currency,
        )
        tradable_selected = [item for item in selected if item["tradable"]]
        session_state = update_session_tracking(args.sessions_state, tradable_selected, results)
        print_eligibility_tracking(eligible_state, eligibility_events)
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.selection_csv.parent.mkdir(parents=True, exist_ok=True)
        args.selection_json.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([flatten(result) for result in results]).to_csv(args.csv, index=False)
        args.json.write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        pd.DataFrame(selected).to_csv(args.selection_csv, index=False)
        args.selection_json.write_text(json.dumps(selected, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nCSV:  {args.csv.resolve()}")
        print(f"JSON: {args.json.resolve()}")
        print(f"Selection CSV:  {args.selection_csv.resolve()}")
        print(f"Selection JSON: {args.selection_json.resolve()}")
        print(f"Suivi eligibilite: {args.eligible_state.resolve()}")
        print(f"Suivi sessions: {args.sessions_state.resolve()}")
        if args.telegram and telegram_window_is_open():
            telegram_message = build_telegram_message(selected, session_state)
            print("\n" + telegram_message)
            send_telegram_message(telegram_message)
        elif args.telegram:
            print("Telegram: plage silencieuse 00:00-05:59 Europe/Paris, envoi ignore.")
    if errors:
        print("\nErrors:")
        for pair, error in errors:
            print(f"  {pair}: {error}")
    return 1 if errors and not results else 0


if __name__ == "__main__":
    raise SystemExit(main())
