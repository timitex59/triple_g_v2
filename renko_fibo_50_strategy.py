#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
renko_fibo_50_strategy.py
-------------------------
Renko ATR(14) Fibonacci 50% Retracement Strategy across 29 Forex pairs.
Analyzes Monthly (MN), Weekly (W1), and Daily (D1) Renko charts.

Core Strategy Rules:
1. Identify 3 consecutive Renko bricks following a SAR flip.
2. Determine Anchor Low (swing low below SAR on Bull flip) and Anchor High (swing high above SAR on Bear flip).
3. Compute Fibo 50% level = (Anchor High + Anchor Low) / 2.
4. Trigger BULL signal when a Renko brick closes ABOVE Fibo 50%, or BEAR signal when a Renko brick closes BELOW Fibo 50%.
5. Monitor multi-timeframe (M/W/D) alignments and send Telegram notifications when new signals occur.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

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


@dataclass
class Fibo50AnchorState:
    pair: str
    tf: str
    direction: int                # +1 for Bullish, -1 for Bearish, 0 for None
    anchor_high: float
    anchor_low: float
    fibo_50: float
    last_brick_open: float
    last_brick_close: float
    px_vs_fibo: int               # +1 if close > fibo_50, -1 if close < fibo_50, 0 inside
    signal: str                   # "BULL", "BEAR", or "NONE"
    three_brick_confirmed: bool


def compute_renko_bricks(df: pd.DataFrame, length: int = 14) -> list[tuple[float, float, int]]:
    """Build ATR(14) Renko bricks from OHLC dataframe."""
    if len(df) < length + 5:
        return []

    atr_series = atr(df, length)
    box_size = float(atr_series.iloc[-1])
    if not box_size or box_size <= 0:
        return []

    closes = [float(x) for x in df["close"].tolist()]
    if not closes:
        return []

    anchor = closes[0]
    direction = 0
    bricks: list[tuple[float, float, int]] = []

    for price in closes[1:]:
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
        else: # direction == -1
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


def detect_fibo_50_level(bricks: list[tuple[float, float, int]]) -> tuple[float, float, float, bool, int]:
    """Detects 3-brick SAR flip sequence and computes Anchor High, Anchor Low, Fibo 50%.

    Returns:
        (anchor_high, anchor_low, fibo_50, three_brick_confirmed, current_direction)
    """
    if len(bricks) < 3:
        if bricks:
            o, c, d = bricks[-1]
            high, low = max(o, c), min(o, c)
            return high, low, (high + low) / 2.0, False, d
        return 0.0, 0.0, 0.0, False, 0

    # Build Highs/Lows history from bricks
    brick_data = []
    for o, c, d in bricks:
        brick_data.append({
            "open": o, "close": c, "direction": d,
            "high": max(o, c), "low": min(o, c)
        })

    # Track runs of consecutive bricks
    runs = []
    curr_dir = brick_data[0]["direction"]
    curr_run = [brick_data[0]]

    for b in brick_data[1:]:
        if b["direction"] == curr_dir:
            curr_run.append(b)
        else:
            runs.append((curr_dir, curr_run))
            curr_dir = b["direction"]
            curr_run = [b]
    runs.append((curr_dir, curr_run))

    if not runs:
        return 0.0, 0.0, 0.0, False, 0

    latest_dir, latest_run = runs[-1]
    three_confirmed = len(latest_run) >= 3

    # Determine Anchor High and Anchor Low across recent swings
    all_highs = [b["high"] for b in brick_data]
    all_lows = [b["low"] for b in brick_data]

    if len(runs) >= 2:
        prev_dir, prev_run = runs[-2]
        if latest_dir == 1: # Bullish flip (from Red to Green)
            anchor_low = min([b["low"] for b in prev_run] + [b["low"] for b in latest_run[:1]])
            anchor_high = max([b["high"] for b in latest_run])
        else: # Bearish flip (from Green to Red)
            anchor_high = max([b["high"] for b in prev_run] + [b["high"] for b in latest_run[:1]])
            anchor_low = min([b["low"] for b in latest_run])
    else:
        anchor_high = max(all_highs)
        anchor_low = min(all_lows)

    fibo_50 = (anchor_high + anchor_low) / 2.0
    return anchor_high, anchor_low, fibo_50, three_confirmed, latest_dir


def evaluate_pair_tf_state(pair: str, tf: str, tv_symbol: str, length: int = 14, candles: int = 300) -> Fibo50AnchorState | None:
    """Evaluates Renko Fibo 50% state for a specific pair and timeframe."""
    native = fetch_tv_native_renko_ohlc(tv_symbol, tf, atr_length=length, n_bricks=candles)
    if native:
        bricks = []
        for r in native:
            o, c = float(r["open"]), float(r["close"])
            d = 1 if c > o else (-1 if c < o else 0)
            if d:
                bricks.append((o, c, d))
    else:
        df_raw = fetch_tv_ohlc(tv_symbol, tf, n_candles=candles)
        if df_raw is None or df_raw.empty:
            return None
        df_src = closed_renko_source(df_raw)
        bricks = compute_renko_bricks(df_src, length=length)

    if not bricks:
        return None

    anchor_high, anchor_low, fibo_50, three_confirmed, curr_dir = detect_fibo_50_level(bricks)
    last_open, last_close, _ = bricks[-1]

    # Evaluate signal trigger: close relative to Fibo 50%
    if last_close > fibo_50 and last_open <= fibo_50:
        signal = "BULL"
        px_vs_fibo = 1
    elif last_close < fibo_50 and last_open >= fibo_50:
        signal = "BEAR"
        px_vs_fibo = -1
    elif last_close > fibo_50:
        signal = "BULL" if curr_dir == 1 and three_confirmed else "NONE"
        px_vs_fibo = 1
    elif last_close < fibo_50:
        signal = "BEAR" if curr_dir == -1 and three_confirmed else "NONE"
        px_vs_fibo = -1
    else:
        signal = "NONE"
        px_vs_fibo = 0

    return Fibo50AnchorState(
        pair=pair,
        tf=tf,
        direction=curr_dir,
        anchor_high=anchor_high,
        anchor_low=anchor_low,
        fibo_50=fibo_50,
        last_brick_open=last_open,
        last_brick_close=last_close,
        px_vs_fibo=px_vs_fibo,
        signal=signal,
        three_brick_confirmed=three_confirmed,
    )


def scan_all_pairs(length: int = 14, candles: int = 300) -> dict[str, dict[str, Fibo50AnchorState]]:
    """Scans all 29 pairs across M, W, D timeframes."""
    results: dict[str, dict[str, Fibo50AnchorState]] = {}
    timeframes = [("M", "MN"), ("W", "W1"), ("D", "D1")]

    for pair in PAIRS_29:
        tv_symbol = f"OANDA:{pair}"
        pair_tf_map = {}
        for tf_code, tf_name in timeframes:
            state = evaluate_pair_tf_state(pair, tf_name, tv_symbol, length=length, candles=candles)
            if state:
                pair_tf_map[tf_code] = state
        if pair_tf_map:
            results[pair] = pair_tf_map
    return results


def format_telegram_fibo_50_report(results: dict[str, dict[str, Fibo50AnchorState]]) -> str | None:
    """Formats a clean Telegram report summarizing Fibo 50% signals and alignments."""
    bull_signals = []
    bear_signals = []
    strict_alignments = []

    for pair, tf_map in results.items():
        m_state = tf_map.get("M")
        w_state = tf_map.get("W")
        d_state = tf_map.get("D")

        # Check signals
        for tf_code, state in tf_map.items():
            if state.three_brick_confirmed:
                if state.signal == "BULL" and state.px_vs_fibo == 1:
                    bull_signals.append((pair, tf_code, state))
                elif state.signal == "BEAR" and state.px_vs_fibo == -1:
                    bear_signals.append((pair, tf_code, state))

        # Check M/W/D full alignment
        if m_state and w_state and d_state:
            if (m_state.direction == 1 and w_state.direction == 1 and d_state.direction == 1 and
                m_state.three_brick_confirmed and w_state.three_brick_confirmed and d_state.three_brick_confirmed):
                strict_alignments.append((pair, "BULL", d_state.fibo_50, d_state.last_brick_close))
            elif (m_state.direction == -1 and w_state.direction == -1 and d_state.direction == -1 and
                  m_state.three_brick_confirmed and w_state.three_brick_confirmed and d_state.three_brick_confirmed):
                strict_alignments.append((pair, "BEAR", d_state.fibo_50, d_state.last_brick_close))

    # If no actionable signals or strict alignments, suppress empty telegram notification
    if not bull_signals and not bear_signals and not strict_alignments:
        return None

    now_paris = datetime.now(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
    lines = ["📊 RENKO FIBO 50% RETRACEMENT", ""]

    if bull_signals:
        lines.append("🌱 SIGNAUX BULL (> FIBO 50%)")
        for pair, tf, s in bull_signals:
            lines.append(f"🟢 {pair} ({tf}) · Fibo 50%: {s.fibo_50:.5f} | PX: {s.last_brick_close:.5f}")
        lines.append("")

    if bear_signals:
        lines.append("🔻 SIGNAUX BEAR (< FIBO 50%)")
        for pair, tf, s in bear_signals:
            lines.append(f"🔴 {pair} ({tf}) · Fibo 50%: {s.fibo_50:.5f} | PX: {s.last_brick_close:.5f}")
        lines.append("")

    if strict_alignments:
        lines.append("📊 FULL ALIGNMENT M/W/D (3 BRICKS SAR)")
        for pair, direction, fibo, px in strict_alignments:
            icon = "🟢" if direction == "BULL" else "🔴"
            lines.append(f"{icon} {pair} · Fibo 50%: {fibo:.5f}")
        lines.append("")

    lines.append(f"⏰ {now_paris} Paris")
    return "\n".join(lines)


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
    parser.add_argument("--candles", type=int, default=300, help="Number of candles fetched per symbol and timeframe.")
    parser.add_argument("--telegram", action="store_true", help="Send scanner result to Telegram.")
    parser.add_argument("--force-telegram", action="store_true", help="Force Telegram send even if body unchanged.")
    return parser.parse_args()


def main():
    args = parse_args()
    print(f"[{datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M:%S Paris')}] Scanning 29 pairs for Renko Fibo 50% retracements...")

    results = scan_all_pairs(length=args.length, candles=args.candles)
    report_text = format_telegram_fibo_50_report(results)

    if report_text:
        print("\n" + report_text + "\n")
    else:
        print("Aucun signal ou alignement Fibo 50% à signaler.")

    prev_state = load_previous_state()
    body_hash = hashlib.sha256(report_text.encode("utf-8")).hexdigest() if report_text else ""
    last_hash = prev_state.get("last_body_hash", "")

    if args.telegram or os.getenv("ENABLE_TELEGRAM_FIBO_50", "").lower() == "true":
        if report_text and (args.force_telegram or body_hash != last_hash):
            print("Sending Telegram notification...")
            res = send_telegram_message(report_text)
            print(f"Telegram status: {res}")
            prev_state["last_body_hash"] = body_hash
            prev_state["last_sent_at"] = datetime.now(PARIS_TZ).isoformat()
            save_current_state(prev_state)
        else:
            print("Telegram send skipped (message empty or unchanged).")


if __name__ == "__main__":
    main()
