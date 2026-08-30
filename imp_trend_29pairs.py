#!/usr/bin/env python3
# =============================================================================
# imp_trend_29pairs.py — live scanner for the IMP TREND setup over the 29 pairs
#
# Python port of the signal half of imp_trend_indicateur.pine /
# imp_trend_strategy.pine. It answers one question per pair: "is there a
# tradeable IMP TREND signal right now, and if not, is this pair armed and
# waiting for one?" — then sends the selection to Telegram, in the same
# spirit as renko_score_29pairs_v16.py.
#
# Signal chain (identical to the Pine scripts):
#   1. Renko bias    — Monthly / Weekly / Daily ATR-Renko bricks vs the live
#                      price give a BULL/BEAR/MIXED bias per timeframe. Two or
#                      more agreeing timeframes set the global bias, which
#                      classifies a signal as TREND (with the bias) or REV
#                      (against it).
#   2. Daily Early IMP — the first daily candle of the matching colour after a
#                      daily Parabolic SAR crossover. It ARMS a direction.
#   3. 1H Entry Trigger — the first hourly SAR crossover in the armed
#                      direction. It FIRES the entry and disarms.
#   Stop = the hourly SAR value on the trigger bar; target = entry +/- RR x risk.
#
# What this script does NOT do: it is a scanner, not a backtester. It reports
# signals; it does not simulate entries, re-entries, stops or equity. The
# Strategy Tester on imp_trend_strategy.pine remains the reference for P/L.
#
# IMPORTANT — one deliberate difference from the Pine version. The Pine scripts
# read the daily Early IMP with barmerge.lookahead_on, so on a chart they know
# the daily outcome before the daily bar closes (it repaints intraday). A live
# scanner cannot do that honestly, so here a daily Early IMP only arms a
# direction once its daily candle has CLOSED. Consequence: this script can arm
# a pair up to one day later than the Pine chart shows, and it will never
# announce a signal that later vanishes. Trigger detection on the hourly side
# uses closed bars only, for the same reason.
#
# Usage:
#   python imp_trend_29pairs.py                  # scan + Telegram if anything found
#   python imp_trend_29pairs.py --no-telegram    # console only
#   python imp_trend_29pairs.py --armed-only     # only the watchlist section
#   python imp_trend_29pairs.py --pairs EURUSD,GBPJPY
#   python imp_trend_29pairs.py --trigger-age 3  # accept triggers up to 3 H1 bars old
# =============================================================================

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from ichimoku_v4 import PAIRS_29, fetch_tv_ohlc, send_telegram_message

# The Renko/SAR primitives are shared with the V16 screener rather than
# duplicated, so both scripts always agree on what a brick or a SAR is.
from renko_score_29pairs_v16 import (
    atr,
    build_renko_bricks,
    closed_renko_source,
    f_effective_bias,
    f_px_state,
    parabolic_sar,
    streaks_from_bricks,
)

PARIS_TZ = ZoneInfo("Europe/Paris")

# Set by --fixed-box: replays Renko with the V16 single-box model instead of
# the rolling-ATR one, to compare the two against the Pine status table.
FIXED_BOX = False

# Defaults mirror the Pine inputs: PSAR 0.1 / 0.1 / 0.2, Renko ATR 14,
# max streak 50, R:R 3.0.
SAR_START = 0.1
SAR_STEP = 0.1
SAR_MAX = 0.2
ATR_LENGTH = 14
MAX_STREAK = 50
RR_RATIO = 3.0


def build_renko_bricks_rolling(df: pd.DataFrame, length: int) -> list[tuple[float, float, int]]:
    """Renko bricks with a ROLLING ATR box, closer to what TradingView's
    ticker.renko(sym, "ATR", n) produces than the fixed-box version.

    renko_score_29pairs_v16.build_renko_bricks() sizes every brick in history
    with the SAME box -- the ATR of the very last bar. TradingView instead
    re-reads the ATR as it walks the series, so an old brick is as wide as
    volatility was back then. The two brick sequences drift apart, and with
    them the streak counts and the price-vs-brick state.

    Geometry (reversal needs two boxes) is identical to the fixed-box version;
    only the box size varies per bar. Pass --fixed-box to compare."""
    atr_series = atr(df, length)
    close = df["close"].astype(float)
    if len(close) < 2:
        return []

    anchor = float(close.iloc[0])
    direction = 0
    bricks: list[tuple[float, float, int]] = []

    for i in range(1, len(close)):
        box_size = float(atr_series.iloc[i])
        if pd.isna(box_size) or box_size <= 0:
            continue

        price = float(close.iloc[i])
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
            direction = new_direction

    return bricks


@dataclass
class PairResult:
    pair: str
    direction: int          # +1 bull, -1 bear, 0 none
    kind: str               # "TRIGGER" | "ARMED" | "NONE"
    bars_ago: int | None    # H1 bars since the trigger fired
    is_reversal: bool       # REV (against the Renko bias) vs TREND (with it)
    entry: float | None
    stop: float | None
    target: float | None
    bias_txt: str           # e.g. "M+ W+ D-"
    when: pd.Timestamp | None


def parse_args():
    p = argparse.ArgumentParser(
        description="Live IMP TREND scanner over the 29 pairs (port of imp_trend_indicateur.pine)."
    )
    p.add_argument("--pairs", type=str, default="",
                   help="Comma-separated subset, e.g. EURUSD,GBPJPY. Default: the 29 pairs.")
    p.add_argument("--trigger-age", type=int, default=1,
                   help="A trigger is reported if it fired within the last N closed H1 bars (default 1).")
    p.add_argument("--h1-candles", type=int, default=600, help="H1 bars fetched per pair.")
    p.add_argument("--d-candles", type=int, default=400, help="Daily bars fetched per pair.")
    p.add_argument("--length", type=int, default=ATR_LENGTH, help="Renko ATR length.")
    p.add_argument("--max-streak", type=int, default=MAX_STREAK, help="Cap on the brick streak count.")
    p.add_argument("--rr", type=float, default=RR_RATIO, help="Risk:reward ratio used for the target.")
    p.add_argument("--armed-only", action="store_true", help="Report only armed pairs, no triggers.")
    p.add_argument("--no-telegram", action="store_true", help="Print the message but do not send it.")
    p.add_argument("--dry-run", action="store_true",
                   help="Do not write the tracker file (safe for testing).")
    p.add_argument("--fixed-box", action="store_true",
                   help="Use the V16 fixed-ATR Renko box instead of the rolling one.")
    return p.parse_args()


# --------------------------------------------------------------------------- #
# Signal building blocks
# --------------------------------------------------------------------------- #
def closed_bars(df: pd.DataFrame) -> pd.DataFrame:
    """Drops the last, still-forming candle. Every signal in this script is
    built from closed bars only -- see the lookahead note in the header."""
    return df.iloc[:-1] if len(df) > 1 else df.iloc[0:0]


def sar_crossovers(df: pd.DataFrame) -> tuple[list[int], list[float]]:
    """Bar indices where close crosses the Parabolic SAR, and the SAR series.
    Returns (events, sar) with events as (index, direction) flattened into a
    list of ints per bar: +1 bull cross, -1 bear cross, 0 nothing."""
    state = parabolic_sar(df, SAR_START, SAR_STEP, SAR_MAX)
    if not state:
        return [], []
    sar = state["sar"]
    close = [float(x) for x in df["close"].tolist()]
    events = [0] * len(close)
    for i in range(1, len(close)):
        above_now = close[i] > sar[i]
        above_prev = close[i - 1] > sar[i - 1]
        if above_now and not above_prev:
            events[i] = 1
        elif not above_now and above_prev:
            events[i] = -1
    return events, sar


def daily_early_imp(df_d: pd.DataFrame) -> list[tuple[pd.Timestamp, int]]:
    """Daily Early IMP events: the first daily candle of the matching colour
    following a daily SAR crossover. Verbatim port of f_daily_early_imp().

    Each event is timestamped at the moment it becomes KNOWN, i.e. when its
    daily candle closes -- not when that candle opened. OANDA stamps a daily
    bar at its open (21:00 UTC), so using the raw index would arm a direction
    a full day early and leak information the market had not published yet."""
    events, _ = sar_crossovers(df_d)
    if not events:
        return []

    open_ = [float(x) for x in df_d["open"].tolist()]
    close = [float(x) for x in df_d["close"].tolist()]

    index = [pd.Timestamp(t) for t in df_d.index]

    def close_time(i: int) -> pd.Timestamp:
        """When bar i actually closed: the next bar's open, or one span later
        for the final bar."""
        if i + 1 < len(index):
            return index[i + 1]
        span = index[-1] - index[-2] if len(index) >= 2 else pd.Timedelta(days=1)
        return index[i] + span

    out: list[tuple[pd.Timestamp, int]] = []
    wait_bull = False
    wait_bear = False

    for i in range(len(close)):
        green = close[i] > open_[i]
        red = close[i] < open_[i]

        # The wait flags are tested BEFORE the crossover is applied, exactly
        # like the Pine version: a crossover never fires on its own bar.
        if wait_bull and green:
            out.append((close_time(i), 1))
            wait_bull = False
        if wait_bear and red:
            out.append((close_time(i), -1))
            wait_bear = False

        if events[i] == 1:
            wait_bull, wait_bear = True, False
        elif events[i] == -1:
            wait_bear, wait_bull = True, False

    return out


def renko_bias(pair: str, live_price: float, length: int, max_streak: int) -> dict[str, int] | None:
    """Monthly / Weekly / Daily Renko bias, as the Pine status table shows it."""
    bias: dict[str, int] = {}
    for interval in ("M", "W", "D"):
        df = fetch_tv_ohlc(f"OANDA:{pair}", interval, 300)
        if df is None or df.empty:
            return None
        builder = build_renko_bricks if FIXED_BOX else build_renko_bricks_rolling
        bricks = builder(closed_renko_source(df), length)
        if not bricks:
            return None
        brick_open, brick_close, _ = bricks[-1]
        green, red = streaks_from_bricks(bricks, max_streak)
        bias[interval] = f_effective_bias(f_px_state(brick_open, brick_close, live_price), green, red)
    return bias


def classify(bias: dict[str, int], direction: int) -> bool:
    """True when the signal is a REVERSAL (against the global Renko bias).

    Mirrors impBullReversalOk / impBearReversalOk: the global bias needs two
    agreeing timeframes, and the signal direction needs at least one."""
    bull_count = sum(1 for v in bias.values() if v == 1)
    bear_count = sum(1 for v in bias.values() if v == -1)
    if direction == 1:
        return bear_count >= 2 and bull_count >= 1
    return bull_count >= 2 and bear_count >= 1


def bias_text(bias: dict[str, int]) -> str:
    sym = {1: "+", -1: "-", 0: "="}
    return " ".join(f"{tf}{sym[bias[tf]]}" for tf in ("M", "W", "D"))


# --------------------------------------------------------------------------- #
# Trade engine — replays the same rules as imp_trend_indicateur.pine
# --------------------------------------------------------------------------- #
# The engine is a PURE REPLAY: every run rebuilds the whole trade history from
# the H1 series, so the tracker file only has to remember which events were
# already announced. A missed run, a double run or a restart can therefore
# never corrupt the state -- unlike an incremental "what changed since last
# time?" design.
#
# Rules mirrored from the Pine version:
#   entry  = next bar's open after a 1H Entry Trigger
#   stop   = hourly SAR on the trigger bar, exit on a CONFIRMED close beyond it
#            (fills at the following open)
#   target = entry +/- RR x risk, exact wick touch
#   EXIT   = opposite 1H trigger on a confirmed close, fills at the next open,
#            re-arms that direction and cancels any resting re-entry
#   re-entry = resting stop order at the same entry level after a stop, same
#            stop and RR, cancelled by a fresh trigger or an opposite Early IMP


def replay(pair: str, df_h1: pd.DataFrame, h1_events: list[int], h1_sar: list[float],
           imp_events: list[tuple[pd.Timestamp, int]], rr: float) -> tuple[dict | None, list[dict]]:
    """Returns (open_position_or_None, closed_and_open_events_in_order)."""
    imp_queue = list(imp_events)
    qi = 0
    armed = {1: False, -1: False}

    pos: dict | None = None
    pending_entry: dict | None = None     # fills at the next bar's open
    pending_exit: str | None = None       # "SL" or "EXIT", fills at the next open
    reentry: dict | None = None           # resting stop order
    events: list[dict] = []

    index = [pd.Timestamp(t) for t in df_h1.index]
    op = [float(x) for x in df_h1["open"].tolist()]
    hi = [float(x) for x in df_h1["high"].tolist()]
    lo = [float(x) for x in df_h1["low"].tolist()]
    cl = [float(x) for x in df_h1["close"].tolist()]

    def open_pos(ts, direction, price, stop, is_reentry):
        nonlocal pos
        risk = max(abs(price - stop), 1e-9)
        pos = {"dir": direction, "entry": price, "stop": stop,
               "target": price + rr * risk if direction == 1 else price - rr * risk,
               "opened_at": ts, "is_reentry": is_reentry}
        events.append({"ts": ts, "type": "OPEN", "dir": direction, "price": price,
                       "stop": pos["stop"], "target": pos["target"], "is_reentry": is_reentry})

    def close_pos(ts, kind, price):
        nonlocal pos, reentry
        r_mult = ((price - pos["entry"]) * pos["dir"]) / max(abs(pos["entry"] - pos["stop"]), 1e-9)
        events.append({"ts": ts, "type": kind, "dir": pos["dir"], "price": price, "r": r_mult})
        if kind == "SL":
            # A stopped-out trade arms the re-entry at the same level and stop.
            reentry = {"level": pos["entry"], "stop": pos["stop"], "dir": pos["dir"]}
        pos = None

    for i in range(len(index)):
        ts = index[i]

        while qi < len(imp_queue) and imp_queue[qi][0] <= ts:
            d = imp_queue[qi][1]
            armed[d] = True
            # An opposite Early IMP invalidates a resting re-entry.
            if reentry is not None and reentry["dir"] == -d:
                reentry = None
            qi += 1

        # 1) deferred exits fill at this bar's open
        if pending_exit and pos is not None:
            close_pos(ts, pending_exit, op[i])
            pending_exit = None

        # 2) queued entry fills at this bar's open
        if pending_entry is not None:
            if pos is None:
                open_pos(ts, pending_entry["dir"], op[i], pending_entry["stop"], False)
            pending_entry = None

        # 3) resting re-entry stop order
        if reentry is not None and pos is None:
            lvl = reentry["level"]
            fill = None
            if reentry["dir"] == 1:
                fill = op[i] if op[i] >= lvl else (lvl if hi[i] >= lvl else None)
            else:
                fill = op[i] if op[i] <= lvl else (lvl if lo[i] <= lvl else None)
            if fill is not None:
                open_pos(ts, reentry["dir"], fill, reentry["stop"], True)
                reentry = None

        # 4) exits on the live trade (target first, stop needs a confirmed close)
        if pos is not None:
            d = pos["dir"]
            target_hit = hi[i] >= pos["target"] if d == 1 else lo[i] <= pos["target"]
            stop_close = cl[i] <= pos["stop"] if d == 1 else cl[i] >= pos["stop"]
            if target_hit:
                close_pos(ts, "TP", pos["target"])
            elif stop_close:
                pending_exit = "SL"

        # 5) opposite 1H trigger closes and re-arms that direction
        ev = h1_events[i]
        if pos is not None and ev != 0 and armed[ev] and ev == -pos["dir"] and pending_exit is None:
            pending_exit = "EXIT"
            reentry = None
            armed[ev] = True          # re-armed: the next crossover may open it
            continue

        # 6) fresh trigger while flat
        if ev != 0 and armed[ev]:
            armed[ev] = False
            if pos is None and pending_entry is None and pending_exit is None:
                reentry = None
                pending_entry = {"dir": ev, "stop": float(h1_sar[i])}

    return pos, events


@dataclass
class Scan:
    pair: str
    events: list[dict]
    position: dict | None
    armed_dir: int
    bias: dict[str, int] | None
    bias_txt: str
    new: list[dict] = field(default_factory=list)   # events not announced yet


def scan_pair(pair: str, args) -> Scan | None:
    df_d_raw = fetch_tv_ohlc(f"OANDA:{pair}", "D", args.d_candles)
    df_h1_raw = fetch_tv_ohlc(f"OANDA:{pair}", "60", args.h1_candles)
    if df_d_raw is None or df_d_raw.empty or df_h1_raw is None or df_h1_raw.empty:
        return None

    live_price = float(df_h1_raw["close"].iloc[-1])
    df_d = closed_bars(df_d_raw)
    df_h1 = closed_bars(df_h1_raw)
    if len(df_d) < 30 or len(df_h1) < 30:
        return None

    imp_events = daily_early_imp(df_d)
    h1_events, h1_sar = sar_crossovers(df_h1)
    if not h1_events:
        return None

    position, events = replay(pair, df_h1, h1_events, h1_sar, imp_events, args.rr)

    # Armed state at the end of the replay, for the watchlist section.
    armed_dir = 0
    last_ts = pd.Timestamp(df_h1.index[-1])
    consumed: set[int] = set()
    for i, ev in enumerate(h1_events):
        if ev != 0:
            consumed.add(ev)
    pend = {1: False, -1: False}
    qi = 0
    for i, ts in enumerate(pd.Timestamp(t) for t in df_h1.index):
        while qi < len(imp_events) and imp_events[qi][0] <= ts:
            pend[imp_events[qi][1]] = True
            qi += 1
        if h1_events[i] != 0 and pend[h1_events[i]]:
            pend[h1_events[i]] = False
    while qi < len(imp_events):
        pend[imp_events[qi][1]] = True
        qi += 1
    if position is None:
        armed_dir = 1 if pend[1] else (-1 if pend[-1] else 0)

    bias = None
    bias_txt = ""
    if position is not None or armed_dir != 0 or events:
        bias = renko_bias(pair, live_price, args.length, args.max_streak)
        if bias is not None:
            bias_txt = bias_text(bias)

    return Scan(pair, events, position, armed_dir, bias, bias_txt)


# --------------------------------------------------------------------------- #
# Tracker — remembers which events were already announced
# --------------------------------------------------------------------------- #
TRACKER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "imp_trend_tracker.json")


def load_tracker() -> dict:
    if not os.path.exists(TRACKER_PATH):
        return {}
    try:
        with open(TRACKER_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        print(f"Tracker: illisible ({exc}), repart de zero.")
        return {}


def save_tracker(tracker: dict) -> None:
    tracker["_meta"] = {"updated_at": datetime.now(PARIS_TZ).isoformat()}
    try:
        with open(TRACKER_PATH, "w", encoding="utf-8") as fh:
            json.dump(tracker, fh, indent=1, ensure_ascii=False)
    except Exception as exc:
        print(f"Tracker: ecriture impossible ({exc})")


def new_events(scan, tracker: dict) -> list[dict]:
    """Events this pair produced that have not been announced yet.

    On the very first sighting of a pair nothing historical is announced --
    the replay covers weeks of trades and dumping them all into Telegram
    would be noise. The state is simply adopted."""
    key = f"OANDA:{scan.pair}"
    seen = tracker.get(key, {}).get("last_event_ts")
    if seen is None:
        return []
    seen_ts = pd.Timestamp(seen)
    return [e for e in scan.events if pd.Timestamp(e["ts"]) > seen_ts]


def remember(scan, tracker: dict) -> None:
    key = f"OANDA:{scan.pair}"
    entry = {
        "name": scan.pair,
        "last_event_ts": (pd.Timestamp(scan.events[-1]["ts"]).isoformat()
                          if scan.events else None),
        "open": None,
        "updated_at": datetime.now(PARIS_TZ).isoformat(),
    }
    if scan.position is not None:
        p = scan.position
        entry["open"] = {
            "side": "LONG" if p["dir"] == 1 else "SHORT",
            "entry": p["entry"], "stop": p["stop"], "target": p["target"],
            "opened_at": pd.Timestamp(p["opened_at"]).isoformat(),
            "is_reentry": p["is_reentry"],
        }
    tracker[key] = entry


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
ICON = {1: "\U0001F7E2", -1: "\U0001F534"}
EVENT_ICON = {"OPEN": "", "TP": "\u2705", "SL": "\u274C", "EXIT": "\u26A0\ufe0f"}


def fmt(v) -> str:
    return f"{v:.5f}" if v is not None else "-"


def print_table(scans: list) -> None:
    print(f"{'PAIR':<9}{'STATE':<11}{'DIR':<7}{'BIAS':<12}{'ENTRY':<12}{'STOP':<12}{'TARGET':<12}{'NEW':<5}")
    print("-" * 80)

    def rank(s) -> tuple:
        return (0 if s.position else (1 if s.armed_dir else 2), s.pair)

    for s in sorted(scans, key=rank):
        if s.position:
            p = s.position
            state = "OPEN*" if p["is_reentry"] else "OPEN"
            d = "LONG" if p["dir"] == 1 else "SHORT"
            row = (fmt(p["entry"]), fmt(p["stop"]), fmt(p["target"]))
        elif s.armed_dir:
            state, d, row = "ARMED", ("BULL" if s.armed_dir == 1 else "BEAR"), ("-", "-", "-")
        else:
            state, d, row = "NONE", "-", ("-", "-", "-")
        print(f"{s.pair:<9}{state:<11}{d:<7}{s.bias_txt:<12}"
              f"{row[0]:<12}{row[1]:<12}{row[2]:<12}{len(s.new):<5}")


def build_message(scans: list, armed_only: bool):
    opened, closed = [], []
    for s in scans:
        for e in s.new:
            (opened if e["type"] == "OPEN" else closed).append((s, e))

    open_positions = [s for s in scans if s.position]
    armed = [s for s in scans if s.position is None and s.armed_dir]

    if armed_only:
        opened, closed, open_positions = [], [], []
    if not (opened or closed or open_positions or armed):
        return None

    lines = ["\U0001F4CA IMP TREND", ""]

    if opened:
        lines.append("\U0001F195 NOUVEAUX TRADES")
        for s, e in sorted(opened, key=lambda x: (-x[1]["dir"], x[0].pair)):
            tag = "re-entree" if e["is_reentry"] else "fresh"
            lines.append(f"{ICON[e['dir']]} {s.pair} ({tag})")
            lines.append(f"   E {e['price']:.5f} \u00b7 SL {e['stop']:.5f} \u00b7 TP {e['target']:.5f}")
        lines.append("")

    if closed:
        lines.append("\U0001F3C1 CLOTURES")
        for s, e in sorted(closed, key=lambda x: pd.Timestamp(x[1]["ts"])):
            lines.append(f"{EVENT_ICON[e['type']]}{ICON[e['dir']]} {s.pair} "
                         f"({e['type']}) {e['r']:+.1f}R")
        lines.append("")

    if open_positions:
        lines.append("\U0001F4CC POSITIONS OUVERTES")
        for s in sorted(open_positions, key=lambda x: (-x.position["dir"], x.pair)):
            p = s.position
            star = "*" if p["is_reentry"] else ""
            lines.append(f"{ICON[p['dir']]} {s.pair}{star} \u00b7 E {p['entry']:.5f} "
                         f"\u00b7 SL {p['stop']:.5f} \u00b7 TP {p['target']:.5f}")
        lines.append("")

    if armed:
        lines.append("\u23f3 ARMEES (attente croisement SAR 1H)")
        for s in sorted(armed, key=lambda x: (-x.armed_dir, x.pair)):
            tag = "REV" if (s.bias and classify(s.bias, s.armed_dir)) else "TREND"
            lines.append(f"{ICON[s.armed_dir]} {s.pair} \u00b7 {tag} \u00b7 {s.bias_txt}")
        lines.append("")

    lines.append(f"\u23f0 {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M Paris')}")
    return "\n".join(lines)


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    args = parse_args()
    global FIXED_BOX
    FIXED_BOX = args.fixed_box
    pairs = [p.strip().upper() for p in args.pairs.split(",") if p.strip()] or PAIRS_29

    tracker = load_tracker()
    scans = []
    for pair in pairs:
        try:
            scan = scan_pair(pair, args)
        except Exception as exc:
            print(f"{pair}: error - {exc}")
            continue
        if scan is None:
            print(f"{pair}: no data")
            continue
        scan.new = new_events(scan, tracker)
        scans.append(scan)

    if not scans:
        print("Aucune donnee recuperee.")
        return 1

    print_table(scans)

    message = build_message(scans, args.armed_only)
    if message is not None:
        print("")
        print(message)
        if not args.no_telegram:
            send_telegram_message(message)
    else:
        print("\nRien a signaler - pas de message Telegram.")

    if args.dry_run:
        print("(--dry-run : tracker non mis a jour)")
    else:
        for scan in scans:
            remember(scan, tracker)
        save_tracker(tracker)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
