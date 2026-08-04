#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
renko_fibo_50_strategy.py
-------------------------
Renko ATR(14) Fibonacci 50% Retracement Strategy across 29 Forex pairs.
Analyzes Monthly (1M), Weekly (1W), and Daily (1D) Renko charts.

Core Strategy Rules:
1. Run a Parabolic SAR (0.1 / 0.1 / 0.2) ON THE RENKO BRICK SERIES and locate
   the most recent SAR flip.
2. Require at least 3 consecutive bricks in the SAR direction after that flip
   (three_brick_confirmed).
3. Anchors are FROZEN on the swing being retraced — the last run of bricks
   opposite to the SAR, plus the brick just before it (whose extreme is the
   actual swing top/bottom). They never move with the current brick:
     - Bull flip: Anchor High = top of the down swing, Anchor Low = its bottom.
     - Bear flip: Anchor Low = bottom of the up swing, Anchor High = its top.
   Fibo 50% = (Anchor High + Anchor Low) / 2.
4. A signal fires when the current leg recovers more than 50% of that swing:
   a brick must CROSS the Fibo 50% in the SAR direction, within the current leg
   and no more than --max-age-bricks ago, and price must still be on that side.
     - BULL: SAR bullish, a brick closed above the 50% of the prior down swing.
     - BEAR: SAR bearish, a brick closed below the 50% of the prior up swing.
5. Multi-timeframe (M/W/D) alignments and Telegram notifications on new signals.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from ichimoku_v4 import PAIRS_29, send_telegram_message
from renko_score_29pairs_v16 import (
    atr,
    closed_renko_source,
    fetch_tv_native_renko_ohlc,
    fetch_tv_ohlc,
    parabolic_sar,
)

PARIS_TZ = ZoneInfo("Europe/Paris")
SCRIPT_DIR = Path(__file__).resolve().parent
STATE_FILE = SCRIPT_DIR / "renko_fibo_50_state.json"

# Parabolic SAR applied to the Renko brick series.
SAR_AF_START = 0.1
SAR_AF_STEP = 0.1
SAR_AF_MAX = 0.2

# TradingView's create_series rejects the legacy "MN"/"W1"/"D1" codes with
# "invalid parameters" — it wants "1M"/"1W"/"1D". A wrong code costs the full
# retry budget (~126 s per symbol/TF) before silently yielding nothing.
TIMEFRAMES = [("M", "1M"), ("W", "1W"), ("D", "1D")]


@dataclass
class Fibo50AnchorState:
    pair: str
    tf: str
    direction: int                # +1 Bullish SAR, -1 Bearish SAR, 0 undetermined
    anchor_high: float
    anchor_low: float
    fibo_50: float
    last_brick_open: float
    last_brick_close: float
    px_vs_fibo: int               # +1 close above fibo, -1 below, 0 exactly on it
    signal: str                   # "BULL", "BEAR", or "NONE"
    three_brick_confirmed: bool
    crossed_50: bool              # a brick crossed the 50% in the SAR direction after confirmation
    bricks_since_flip: int
    live_price: float | None = None   # latest daily close, the chart reference price
    daily_chg: float | None = None    # % change of that close vs the previous one


def compute_renko_bricks(df: pd.DataFrame, length: int = 14) -> list[tuple[float, float, int]]:
    """Build ATR(length) Renko bricks from an OHLC dataframe.

    The box size is taken from the ATR value *at the candle being replayed*, so
    historical bricks are not rebuilt with today's ATR.
    """
    if len(df) < length + 5:
        return []

    atr_series = atr(df, length)
    closes = df["close"].astype(float).tolist()
    boxes = atr_series.astype(float).tolist()
    if not closes:
        return []

    anchor = 0.0
    started = False
    direction = 0
    bricks: list[tuple[float, float, int]] = []

    for i in range(len(closes)):
        box_size = float(boxes[i])
        if pd.isna(box_size) or box_size <= 0:
            continue
        price = float(closes[i])
        if not started:
            anchor = price
            started = True
            continue

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
        else:  # direction == -1
            if price <= anchor - box_size:
                move = int((anchor - price) // box_size)
                new_direction = -1
            elif price >= anchor + (2.0 * box_size):
                move = int((price - anchor) // box_size) - 1
                new_direction = 1

        # A reversal that just clears the 2-box threshold rounds down to 0 bricks;
        # it must still print (and flip the direction) instead of being dropped.
        if new_direction != direction and new_direction != 0 and move < 1:
            move = 1

        if move > 0:
            for _ in range(move):
                brick_open = anchor
                anchor = anchor + box_size if new_direction == 1 else anchor - box_size
                bricks.append((brick_open, anchor, new_direction))
            direction = new_direction

    return bricks


def _bricks_to_frame(bricks: list[tuple[float, float, int]]) -> pd.DataFrame:
    """Renko bricks -> OHLC frame usable by parabolic_sar()."""
    return pd.DataFrame(
        {
            "open": [o for o, _c, _d in bricks],
            "close": [c for _o, c, _d in bricks],
            "high": [max(o, c) for o, c, _d in bricks],
            "low": [min(o, c) for o, c, _d in bricks],
        }
    )


def detect_fibo_50_level(
    bricks: list[tuple[float, float, int]]
) -> tuple[float, float, float, bool, int, int, int]:
    """Locate the last Parabolic SAR flip on the brick series and derive anchors.

    Returns:
        (anchor_high, anchor_low, fibo_50, three_brick_confirmed,
         sar_direction, flip_index, confirm_index)
        confirm_index is -1 while the 3-brick confirmation has not happened.
    """
    if len(bricks) < 3:
        if bricks:
            o, c, d = bricks[-1]
            high, low = max(o, c), min(o, c)
            return high, low, (high + low) / 2.0, False, d, 0, -1
        return 0.0, 0.0, 0.0, False, 0, 0, -1

    frame = _bricks_to_frame(bricks)
    sar_state = parabolic_sar(frame, SAR_AF_START, SAR_AF_STEP, SAR_AF_MAX)
    if not sar_state:
        return 0.0, 0.0, 0.0, False, 0, 0, -1

    trend = sar_state["trend"]
    sar_dir = trend[-1]

    # Index of the most recent flip, and of the flip before it.
    flip_idx = 0
    for i in range(len(trend) - 1, 0, -1):
        if trend[i] != trend[i - 1]:
            flip_idx = i
            break

    highs = [float(x) for x in frame["high"]]
    lows = [float(x) for x in frame["low"]]

    # 3 consecutive bricks in the SAR direction, counted from the flip.
    run = 0
    confirm_idx = -1
    for i in range(flip_idx, len(bricks)):
        if bricks[i][2] == sar_dir:
            run += 1
            if run >= 3 and confirm_idx < 0:
                confirm_idx = i
        else:
            run = 0
    three_confirmed = confirm_idx >= 0

    # Anchors = the swing that is being retraced: the last run of bricks opposite
    # to the SAR direction, plus the brick just before it (that brick's extreme
    # IS the swing top/bottom — the first opposite brick only opens there).
    # This window is closed and frozen, so fibo_50 never drifts with the current
    # brick. SAR legs alone are too short here (af 0.1/0.1/0.2 flips in 2-3 bricks).
    end_opp = flip_idx
    while end_opp >= 0 and bricks[end_opp][2] == sar_dir:
        end_opp -= 1

    if end_opp < 0:                       # no opposite brick yet: whole history
        swing = slice(0, len(bricks))
    else:
        start_opp = end_opp
        while start_opp - 1 >= 0 and bricks[start_opp - 1][2] != sar_dir:
            start_opp -= 1
        swing = slice(max(0, start_opp - 1), end_opp + 1)

    anchor_high = max(highs[swing])
    anchor_low = min(lows[swing])
    fibo_50 = (anchor_high + anchor_low) / 2.0
    return anchor_high, anchor_low, fibo_50, three_confirmed, sar_dir, flip_idx, confirm_idx


def evaluate_bricks(pair: str, tf: str, bricks: list[tuple[float, float, int]],
                    max_age_bricks: int = 3) -> Fibo50AnchorState | None:
    if not bricks:
        return None

    (anchor_high, anchor_low, fibo_50, three_confirmed,
     sar_dir, flip_idx, confirm_idx) = detect_fibo_50_level(bricks)
    last_open, last_close, _ = bricks[-1]

    if last_close > fibo_50:
        px_vs_fibo = 1
    elif last_close < fibo_50:
        px_vs_fibo = -1
    else:
        px_vs_fibo = 0

    # Rule 4: the new leg must have recovered more than 50% of the swing it
    # retraces. anchor_high/anchor_low come from a closed swing, so this is a
    # real condition — not the tautology it was when anchors tracked the current
    # brick. crossed_50 flags where the 50% was actually taken out.
    crossed = False
    signal = "NONE"
    if three_confirmed:
        leg = bricks[flip_idx:]
        # The 50% must be taken out BY THE CURRENT LEG, and recently: on monthly
        # Renko a leg runs for years, so without a freshness window a level
        # cleared in 2021 would still be reported as a signal today.
        cross_idx = -1
        for i, (o, c, _d) in enumerate(leg):
            if (sar_dir == 1 and o <= fibo_50 < c) or (sar_dir == -1 and o >= fibo_50 > c):
                cross_idx = i
        crossed = cross_idx >= 0
        fresh = crossed and (len(leg) - 1 - cross_idx) <= max_age_bricks

        if fresh and sar_dir == 1 and last_close > fibo_50:
            signal = "BULL"
        elif fresh and sar_dir == -1 and last_close < fibo_50:
            signal = "BEAR"

    return Fibo50AnchorState(
        pair=pair,
        tf=tf,
        direction=sar_dir,
        anchor_high=anchor_high,
        anchor_low=anchor_low,
        fibo_50=fibo_50,
        last_brick_open=last_open,
        last_brick_close=last_close,
        px_vs_fibo=px_vs_fibo,
        signal=signal,
        three_brick_confirmed=three_confirmed,
        crossed_50=crossed,
        bricks_since_flip=len(bricks) - flip_idx,
    )


def evaluate_pair_tf_state(pair: str, tf: str, tv_symbol: str, length: int = 14,
                           candles: int = 80, max_age_bricks: int = 3) -> Fibo50AnchorState | None:
    """Evaluates Renko Fibo 50% state for a specific pair and timeframe."""
    try:
        native = fetch_tv_native_renko_ohlc(tv_symbol, tf, atr_length=length, n_bricks=candles)
        if native:
            bricks = []
            for r in native:
                o, c = float(r["open"]), float(r["close"])
                d = 1 if c > o else (-1 if c < o else 0)
                if d:
                    bricks.append((o, c, d))
        else:
            df_raw = fetch_tv_ohlc(tv_symbol, tf, n_candles=max(candles, length * 8))
            if df_raw is None or df_raw.empty:
                return None
            df_src = closed_renko_source(df_raw)
            bricks = compute_renko_bricks(df_src, length=length)

        return evaluate_bricks(pair, tf, bricks, max_age_bricks=max_age_bricks)
    except Exception as e:
        # One bad symbol must not abort a 10-minute scan.
        print(f"  [warn] {pair} {tf}: {type(e).__name__}: {e}")
        return None


def fetch_daily_quote(pair: str) -> tuple[float | None, float | None]:
    """(live price, daily change %) from a single daily fetch.

    The price is the latest — possibly still-forming — daily close, same
    convention as compute_pair_score() in the v16 screener. The change is that
    close against the previous completed one.
    """
    try:
        df = fetch_tv_ohlc(f"OANDA:{pair}", "D", n_candles=50)
        if df is None or df.empty:
            return None, None
        closes = df["close"].astype(float)
        live = float(closes.iloc[-1])
        if len(closes) < 2:
            return live, None
        prev = float(closes.iloc[-2])
        chg = ((live - prev) / prev * 100.0) if prev else None
        return live, chg
    except Exception as e:
        print(f"  [warn] daily quote {pair}: {type(e).__name__}: {e}")
        return None, None


def scan_all_pairs(length: int = 14, candles: int = 80, workers: int = 8,
                   max_age_bricks: int = 3) -> dict[str, dict[str, Fibo50AnchorState]]:
    """Scans all 29 pairs across M, W, D timeframes (network-bound -> threaded)."""
    tasks = [
        (pair, tf_code, tf_name)
        for pair in PAIRS_29
        for tf_code, tf_name in TIMEFRAMES
    ]

    def run(task):
        pair, tf_code, tf_name = task
        state = evaluate_pair_tf_state(pair, tf_name, f"OANDA:{pair}",
                                       length=length, candles=candles,
                                       max_age_bricks=max_age_bricks)
        return pair, tf_code, state

    results: dict[str, dict[str, Fibo50AnchorState]] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        quotes = dict(zip(PAIRS_29, pool.map(fetch_daily_quote, PAIRS_29)))
        for pair, tf_code, state in pool.map(run, tasks):
            if state:
                state.live_price, state.daily_chg = quotes.get(pair, (None, None))
                results.setdefault(pair, {})[tf_code] = state
    return results


_ICONS = {"BULL": "🟢", "BEAR": "🔴", "NEUTRE": "⚪"}


def _fmt_chg(chg: float | None) -> str:
    """Daily change, always signed so direction reads at a glance."""
    return "CHG n/a" if chg is None else f"{chg:+.2f}%"


DEFAULT_MIN_CHG = 0.05


def chg_confirms(label: str, chg: float | None, min_chg: float = DEFAULT_MIN_CHG) -> bool:
    """The day's move must back the structural bias, and be big enough to mean
    something: |chg| strictly above min_chg, and its sign matching the bias
    (positive for BULL, negative for BEAR). An unknown change never confirms."""
    if chg is None or abs(chg) <= min_chg:
        return False
    return chg > 0 if label == "BULL" else chg < 0 if label == "BEAR" else False


def currency_strengths(rows: list[tuple]) -> dict[str, set[int]]:
    """Strength verdicts each retained pair casts on its two currencies.

    A BULL on BASE/QUOTE means the base strengthens and the quote weakens;
    a BEAR means the opposite. A currency collecting both +1 and -1 is telling
    two contradictory stories at once.
    """
    votes: dict[str, set[int]] = {}
    for pair, label, *_ in rows:
        base, quote = pair[:3], pair[3:]
        base_vote = 1 if label == "BULL" else -1
        votes.setdefault(base, set()).add(base_vote)
        votes.setdefault(quote, set()).add(-base_vote)
    return votes


def drop_currency_conflicts(*sections: list[tuple]) -> tuple[list[list[tuple]], set[str]]:
    """Remove every pair touching a currency whose verdicts contradict.

    Conflicts are resolved across ALL sections at once: a currency is a single
    market view, so it cannot be strong in one section and weak in another.
    Single pass — a pair is dropped on the conflict set computed from the full
    original selection, so the outcome never depends on evaluation order.
    """
    all_rows = [row for section in sections for row in section]
    conflicted = {cur for cur, votes in currency_strengths(all_rows).items() if len(votes) > 1}
    if not conflicted:
        return list(sections), conflicted

    cleaned = [
        [row for row in section
         if row[0][:3] not in conflicted and row[0][3:] not in conflicted]
        for section in sections
    ]
    return cleaned, conflicted


def bias_label(bias: str, live_close: float | None,
               tf_levels: list[tuple[float, float]]) -> str:
    """A directional bias only holds when the full chain lines up on EVERY
    timeframe checked:

        BULL:  Close (live price) > PX (Renko brick close) > Fibo 50%
        BEAR:  Close (live price) < PX (Renko brick close) < Fibo 50%

    tf_levels holds one (brick_close, fibo_50) pair per timeframe. Anything that
    breaks the chain is downgraded to NEUTRE rather than reported as actionable.
    """
    if live_close is None or not tf_levels:
        return "NEUTRE"
    if bias == "BULL":
        ok = all(live_close > px > fibo for px, fibo in tf_levels)
        return "BULL" if ok else "NEUTRE"
    if bias == "BEAR":
        ok = all(live_close < px < fibo for px, fibo in tf_levels)
        return "BEAR" if ok else "NEUTRE"
    return "NEUTRE"


PIPS_FILE = SCRIPT_DIR / "renko_fibo_50_pips.json"
PIPS_VERSION = 1


def pip_size(pair: str) -> float:
    """Standard pip size for the 29-instrument universe (same as the VIVIER)."""
    return 0.01 if pair.endswith("JPY") or pair == "XAUUSD" else 0.0001


def load_pips_state(path: Path = PIPS_FILE) -> dict:
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
    return {"version": PIPS_VERSION, "year": None, "open": {}, "closed": []}


def save_pips_state(state: dict, path: Path = PIPS_FILE) -> None:
    try:
        tmp = path.with_suffix(f".tmp{os.getpid()}")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception as e:
        print(f"Warning: Failed to save pips file {path}: {e}")


def structure_holds(tf_map: dict[str, Fibo50AnchorState]) -> int:
    """Direction of the M/W/D SAR structure, 0 when it no longer holds.

    This is what keeps a virtual position alive. The display filters (daily
    change, currency coherence) can hide a pair from the report without closing
    its position — only the structure breaking does that.
    """
    m, w, d = tf_map.get("M"), tf_map.get("W"), tf_map.get("D")
    if not (m and w and d):
        return 0
    if not (m.three_brick_confirmed and w.three_brick_confirmed and d.three_brick_confirmed):
        return 0
    if m.direction == w.direction == d.direction and m.direction != 0:
        return m.direction
    return 0


def update_pips_tracker(results: dict[str, dict[str, Fibo50AnchorState]],
                        full_alignment: list[tuple], state: dict,
                        now: datetime | None = None) -> dict:
    """Open a virtual position when a pair reaches FULL ALIGNMENT, hold it while
    the M/W/D structure survives, close it when that structure breaks."""
    now = now or datetime.now(PARIS_TZ)

    # Yearly reset: cumulative totals restart on 1 January. Open positions carry
    # over — it is the accounting that restarts, not the market view.
    if state.get("year") != now.year:
        state["year"] = now.year
        state["closed"] = []

    open_pos: dict = state.setdefault("open", {})
    closed: list = state.setdefault("closed", [])

    # 1. Close positions whose structure broke (or reversed).
    for pair in list(open_pos):
        entry = open_pos[pair]
        tf_map = results.get(pair) or {}
        direction = structure_holds(tf_map)
        d_state = tf_map.get("D")
        price = d_state.live_price if d_state else None

        if direction == entry.get("direction") and direction != 0:
            continue                                  # still valid, hold
        if price is None:
            continue                                  # no price: cannot close fairly

        pips = (price - entry["entry_price"]) / pip_size(pair) * entry["direction"]
        closed.append({
            "pair": pair,
            "direction": entry["direction"],
            "entry_price": entry["entry_price"],
            "entry_date": entry["entry_date"],
            "exit_price": float(price),
            "exit_date": now.strftime("%Y-%m-%d"),
            "exit_time": now.isoformat(),
            "pips": round(pips, 1),
            "reason": "reversed" if direction else "structure_broken",
        })
        open_pos.pop(pair)

    # 2. Open a position for every newly listed FULL ALIGNMENT pair.
    for pair, label, _chg in full_alignment:
        if pair in open_pos:
            continue
        d_state = (results.get(pair) or {}).get("D")
        price = d_state.live_price if d_state else None
        if price is None:
            continue
        open_pos[pair] = {
            "direction": 1 if label == "BULL" else -1,
            "entry_price": float(price),
            "entry_date": now.strftime("%Y-%m-%d"),
            "entry_time": now.isoformat(),
        }

    # 3. Refresh the unrealised value of every open position.
    for pair, entry in open_pos.items():
        d_state = (results.get(pair) or {}).get("D")
        price = d_state.live_price if d_state else None
        if price is not None:
            entry["last_price"] = float(price)
            entry["open_pips"] = round(
                (price - entry["entry_price"]) / pip_size(pair) * entry["direction"], 1)
    return state


def _sum_pips(rows: list[dict]) -> tuple[float, float, float]:
    """(bull, bear, total) over a set of closed positions."""
    bull = sum(float(r.get("pips") or 0.0) for r in rows if r.get("direction") == 1)
    bear = sum(float(r.get("pips") or 0.0) for r in rows if r.get("direction") == -1)
    return bull, bear, bull + bear


def _fmt_pips(value: float) -> str:
    return f"{0.0 if abs(value) < 0.05 else value:+.1f}"


def pips_report_lines(state: dict, now: datetime | None = None) -> list[str]:
    """💰 PIPS section: realised day / week / month / year-to-date, plus the
    unrealised total of the positions still open."""
    now = now or datetime.now(PARIS_TZ)
    closed = state.get("closed") or []

    today = now.strftime("%Y-%m-%d")
    week = now.isocalendar()[:2]
    month = now.strftime("%Y-%m")

    def on_day(r):
        return r.get("exit_date") == today

    def on_week(r):
        try:
            return datetime.strptime(r.get("exit_date", ""), "%Y-%m-%d").isocalendar()[:2] == week
        except ValueError:
            return False

    def on_month(r):
        return str(r.get("exit_date", "")).startswith(month)

    d_bull, d_bear, d_total = _sum_pips([r for r in closed if on_day(r)])
    _, _, w_total = _sum_pips([r for r in closed if on_week(r)])
    _, _, m_total = _sum_pips([r for r in closed if on_month(r)])
    _, _, y_total = _sum_pips(closed)          # closed is already year-scoped

    open_pos = state.get("open") or {}
    open_pips = sum(float(e.get("open_pips") or 0.0) for e in open_pos.values())

    lines = [
        "💰 PIPS",
        f"📈 Daily : {_fmt_pips(d_total)} · 🟢 {_fmt_pips(d_bull)} 🔴 {_fmt_pips(d_bear)}",
        f"📊 Weekly : {_fmt_pips(w_total)} · Monthly : {_fmt_pips(m_total)}",
        f"🗓 YTD {state.get('year', now.year)} : {_fmt_pips(y_total)}",
    ]
    if open_pos:
        detail = " ".join(
            f"{p} {_fmt_pips(float(e.get('open_pips') or 0.0))}"
            for p, e in sorted(open_pos.items())
        )
        lines.append(f"🔓 Ouvert {_fmt_pips(open_pips)} · {detail}")
    return lines


def build_sections(results: dict[str, dict[str, Fibo50AnchorState]],
                   min_chg: float = DEFAULT_MIN_CHG) -> tuple[list, list, list]:
    """The three report sections, after every filter. Shared by the report and
    the pip tracker so both always agree on what qualifies."""
    daily_alignments = []
    strict_alignments = []
    mw_alignments = []

    for pair in sorted(results):
        tf_map = results[pair]
        m_state = tf_map.get("M")
        w_state = tf_map.get("W")
        d_state = tf_map.get("D")

        # Daily standing on its own: the full chain must line up on D1, whatever
        # the higher timeframes are doing.
        if d_state and d_state.three_brick_confirmed and d_state.direction != 0:
            d_bias = "BULL" if d_state.direction == 1 else "BEAR"
            d_lv_solo = (d_state.last_brick_close, d_state.fibo_50)
            d_label_solo = bias_label(d_bias, d_state.live_price, [d_lv_solo])
            if d_label_solo != "NEUTRE" and chg_confirms(d_label_solo, d_state.daily_chg, min_chg):
                daily_alignments.append((pair, d_label_solo, d_state.daily_chg))

        # Monthly + Weekly agreeing is the base structural bias. Daily is then
        # either a confirmation (full alignment) or a counter-trend pullback.
        if not (m_state and w_state):
            continue
        if not (m_state.three_brick_confirmed and w_state.three_brick_confirmed):
            continue
        if m_state.direction != w_state.direction or m_state.direction == 0:
            continue

        bias = "BULL" if m_state.direction == 1 else "BEAR"
        live = w_state.live_price          # market price, identical across TFs
        m_lv = (m_state.last_brick_close, m_state.fibo_50)
        w_lv = (w_state.last_brick_close, w_state.fibo_50)

        if d_state and d_state.three_brick_confirmed and d_state.direction == m_state.direction:
            d_lv = (d_state.last_brick_close, d_state.fibo_50)
            label = bias_label(bias, live, [m_lv, w_lv, d_lv])
            if label != "NEUTRE" and chg_confirms(label, d_state.daily_chg, min_chg):
                strict_alignments.append((pair, label, d_state.daily_chg))
        else:
            label = bias_label(bias, live, [m_lv, w_lv])
            if label != "NEUTRE" and chg_confirms(label, w_state.daily_chg, min_chg):
                mw_alignments.append((pair, label, w_state.daily_chg))

    (daily_alignments, strict_alignments, mw_alignments), conflicted = \
        drop_currency_conflicts(daily_alignments, strict_alignments, mw_alignments)
    if conflicted:
        print(f"  Devises contradictoires, paires exclues: {', '.join(sorted(conflicted))}")

    return daily_alignments, strict_alignments, mw_alignments


def format_telegram_fibo_50_report(results: dict[str, dict[str, Fibo50AnchorState]],
                                   min_chg: float = DEFAULT_MIN_CHG,
                                   pips_state: dict | None = None) -> str | None:
    """Formats the Telegram report: alignment sections plus the pip tracker."""
    daily_alignments, strict_alignments, mw_alignments = build_sections(results, min_chg)

    if not (daily_alignments or strict_alignments or mw_alignments):
        return None

    now_paris = datetime.now(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
    lines = ["📊 RENKO FIBO 50%", ""]

    for header, rows in (
        ("☀️ DAILY", daily_alignments),
        ("📊 FULL ALIGNMENT", strict_alignments),
        ("🧭 BIAIS M/W", mw_alignments),
    ):
        if not rows:
            continue
        lines.append(header)
        for pair, label, chg in sorted(rows, key=lambda r: r[0]):
            lines.append(f"{_ICONS[label]} {pair} · {_fmt_chg(chg)}")
        lines.append("")

    if pips_state is not None:
        lines.extend(pips_report_lines(pips_state))
        lines.append("")

    while lines and not lines[-1]:
        lines.pop()
    lines.append(f"⏰ {now_paris} Paris")
    return "\n".join(lines)


TELEGRAM_MAX_CHARS = 4096


def signal_hash(report_text: str | None) -> str:
    """Fingerprint of the report CONTENT, timestamp excluded.

    The report ends with a '⏰ <date> <heure> Paris' line that changes every
    minute. Hashing the whole text made every scan look like new content, so the
    dedup check never matched and Telegram fired on each run.
    """
    if not report_text:
        return ""
    kept = []
    for line in report_text.splitlines():
        if line.startswith("💰"):
            break                     # pip totals move on every tick
        if not line.startswith("⏰"):
            kept.append(line)
    body = "\n".join(kept)
    # The daily change drifts with every tick, so hashing it would make each
    # scan look new and re-send the same selection ~19 times a day. What matters
    # is WHICH pairs are listed under WHICH bias, not the exact percentage.
    body = re.sub(r"[+-]\d+\.\d+%", "%", body)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def split_for_telegram(text: str, limit: int = TELEGRAM_MAX_CHARS) -> list[str]:
    """Split on line boundaries so no chunk exceeds Telegram's message limit.
    A single oversized message is rejected by the API and silently lost."""
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current: list[str] = []
    size = 0
    for line in text.splitlines():
        line = line[:limit]                      # pathological single line
        extra = len(line) + (1 if current else 0)
        if size + extra > limit and current:
            chunks.append("\n".join(current))
            current, size = [line], len(line)
        else:
            current.append(line)
            size += extra
    if current:
        chunks.append("\n".join(current))
    return chunks


def send_telegram_report(report_text: str) -> bool:
    """Send the report, split across several messages when needed.
    Returns True only if every chunk went through."""
    chunks = split_for_telegram(report_text)
    if len(chunks) > 1:
        print(f"Rapport découpé en {len(chunks)} messages ({len(report_text)} caractères).")
    ok = True
    for i, chunk in enumerate(chunks, 1):
        suffix = f"\n\n({i}/{len(chunks)})" if len(chunks) > 1 else ""
        if not send_telegram_message(chunk + suffix):
            ok = False
    return ok


def load_previous_state(file_path: Path = STATE_FILE) -> dict:
    if file_path.exists():
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_current_state(state: dict, file_path: Path = STATE_FILE) -> None:
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"Warning: Failed to save state file {file_path}: {e}")


def parse_args():
    parser = argparse.ArgumentParser(description="Scan 29 pairs for Renko ATR(14) Fibonacci 50% retracement signals.")
    parser.add_argument("--length", type=int, default=14, help="ATR length.")
    parser.add_argument("--candles", type=int, default=80, help="Number of Renko bricks / candles fetched per symbol and timeframe.")
    parser.add_argument("--workers", type=int, default=8, help="Parallel fetch workers.")
    parser.add_argument("--min-chg", type=float, default=DEFAULT_MIN_CHG, help="Minimum absolute daily change %% required, exclusive.")
    parser.add_argument("--max-age-bricks", type=int, default=3, help="Max bricks since the Fibo 50%% cross for a signal to still count as fresh.")
    parser.add_argument("--telegram", action="store_true", help="Send scanner result to Telegram.")
    parser.add_argument("--force-telegram", action="store_true", help="Force Telegram send even if body unchanged.")
    return parser.parse_args()


def main():
    args = parse_args()
    print(f"[{datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M:%S Paris')}] Scanning 29 pairs for Renko Fibo 50% retracements...")

    results = scan_all_pairs(length=args.length, candles=args.candles, workers=args.workers,
                             max_age_bricks=args.max_age_bricks)
    pips_state = load_pips_state()
    _daily, full_alignment, _mw = build_sections(results, min_chg=args.min_chg)
    pips_state = update_pips_tracker(results, full_alignment, pips_state)
    save_pips_state(pips_state)

    report_text = format_telegram_fibo_50_report(results, min_chg=args.min_chg,
                                                 pips_state=pips_state)

    if report_text:
        print("\n" + report_text + "\n")
    else:
        print("Aucun signal ou alignement Fibo 50% à signaler.")

    prev_state = load_previous_state()
    last_hash = prev_state.get("last_body_hash", "")
    body_hash = signal_hash(report_text)

    telegram_enabled = args.telegram or os.getenv("ENABLE_TELEGRAM_FIBO_50", "").lower() == "true"

    if not report_text:
        # Reset the dedup hash so an identical report later is not swallowed.
        if last_hash:
            prev_state["last_body_hash"] = ""
            save_current_state(prev_state)
        if telegram_enabled:
            print("Telegram send skipped (aucun signal).")
        return

    if telegram_enabled:
        if args.force_telegram or body_hash != last_hash:
            print("Sending Telegram notification...")
            ok = send_telegram_report(report_text)
            print(f"Telegram status: {ok}")
            # Only remember the hash on success — otherwise a failed send would
            # mark the report as "already delivered" and it would never be
            # retried on the next scan.
            if ok:
                prev_state["last_body_hash"] = body_hash
                prev_state["last_sent_at"] = datetime.now(PARIS_TZ).isoformat()
                save_current_state(prev_state)
            else:
                print("Envoi échoué: hash non enregistré, nouvelle tentative au prochain scan.")
        else:
            print("Telegram send skipped (message unchanged).")


if __name__ == "__main__":
    main()
