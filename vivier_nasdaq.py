#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Persistent Renko/Fibonacci vivier for Nasdaq-100 stocks.

The universe comes from ``nasdaq_sector_pipeline_report.json``. Renko states
use the existing Nasdaq horizons 3M / M / W and are mapped to the tested
VIVIER engine's three adjacent horizons. This script owns its state and its
Telegram message; it does not modify the Forex vivier.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import sys
from typing import Callable
from zoneinfo import ZoneInfo

import pandas as pd

from renko_nasdaq import (
    NASDAQ_100,
    NasdaqSnapshot,
    fetch_tv_ohlc,
    scan_symbol,
    send_telegram,
)
from renko_score_29pairs_v16 import (
    PARIS_TZ,
    carry_telegram_metadata,
    closed_h1_source,
    load_vivier_state,
    near_alignment_entries,
    parabolic_sar,
    post_signal_tracking_entries,
    sar_cross_event,
    sar_flip_event,
    save_vivier_state,
    telegram_body_hash,
    update_vivier,
    vivier_age_label,
    vivier_base_score,
    vivier_fib_path,
    vivier_flame_label,
    vivier_groups,
)


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_REPORT_PATH = SCRIPT_DIR / "nasdaq_sector_pipeline_report.json"
DEFAULT_STATE_PATH = SCRIPT_DIR / "vivier_nasdaq_state.json"
NEW_YORK_TZ = ZoneInfo("America/New_York")
VIVIER_MIN_ABS_SCORE = 33.0


@dataclass(frozen=True)
class NasdaqAsset:
    ticker: str
    tv_symbol: str
    name: str
    theme: str
    pipeline_score: float | None


def _numeric(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) else None


def load_pipeline_universe(
    report_path: str | Path = DEFAULT_REPORT_PATH,
    *,
    limit: int = 0,
    tickers: set[str] | None = None,
    symbol_lookup: dict[str, str] | None = None,
) -> list[NasdaqAsset]:
    """Load the stock universe ranked by the Nasdaq sector pipeline."""
    path = Path(report_path)
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    stocks = payload.get("stocks")
    if not isinstance(stocks, list):
        raise ValueError(f"Rapport Nasdaq invalide: clé 'stocks' absente dans {path}")

    lookup = symbol_lookup or {ticker: tv for tv, ticker in NASDAQ_100}
    wanted = {ticker.upper() for ticker in tickers} if tickers else None
    assets: list[NasdaqAsset] = []
    seen: set[str] = set()
    for item in stocks:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").upper().strip()
        if not ticker or ticker in seen or (wanted is not None and ticker not in wanted):
            continue
        seen.add(ticker)
        assets.append(NasdaqAsset(
            ticker=ticker,
            tv_symbol=lookup.get(ticker, f"NASDAQ:{ticker}"),
            name=str(item.get("name") or ticker),
            theme=str(item.get("theme") or "Non classé"),
            pipeline_score=_numeric(item.get("score")),
        ))
        if limit > 0 and len(assets) >= limit:
            break
    return assets


def bars_to_frame(bars: list[dict] | None) -> pd.DataFrame:
    """Convert TradingView OHLC rows to a clean UTC-indexed frame."""
    if not bars:
        return pd.DataFrame(columns=["open", "high", "low", "close"])
    frame = pd.DataFrame(bars)
    required = {"time", "open", "high", "low", "close"}
    if not required.issubset(frame.columns):
        return pd.DataFrame(columns=["open", "high", "low", "close"])
    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["time"] = pd.to_numeric(frame["time"], errors="coerce")
    frame = frame.dropna(subset=list(required))
    if frame.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close"])
    frame.index = pd.to_datetime(frame.pop("time"), unit="s", utc=True)
    return frame[["open", "high", "low", "close"]].sort_index()


def analyze_h1_frame(frame: pd.DataFrame) -> dict | None:
    """Compute the monthly Fibo range and H1 SAR event for one stock."""
    if frame.empty or len(frame) < 3:
        return None
    df = frame[~frame.index.duplicated(keep="last")].sort_index()
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

    live_price = float(df["close"].iloc[-1])
    fib50 = month_low + fib_range * 0.5
    position = "ABOVE" if live_price > fib50 else ("BELOW" if live_price < fib50 else "AT")
    sar_state = parabolic_sar(df)
    sar_flipped, sar_dir = sar_flip_event(df, sar_state)
    sar_values = sar_state.get("sar") if sar_state else None

    closed_df = closed_h1_source(df)
    closed_sar_state = parabolic_sar(closed_df)
    cross_event = sar_cross_event(closed_df, closed_sar_state)
    closed_extreme = None
    if not closed_df.empty:
        closed_ts = pd.Timestamp(closed_df.index[-1])
        closed_month_start = closed_ts.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
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
    if cross_event is not None and not closed_df.empty:
        event_ts = pd.Timestamp(closed_df.index[-1])
        event_month_start = event_ts.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        event_month = closed_df[closed_df.index >= event_month_start]
        event_high = float(event_month["high"].max())
        event_low = float(event_month["low"].min())
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
    closed_month = (
        pd.Timestamp(closed_df.index[-1]).strftime("%Y-%m")
        if not closed_df.empty else None
    )
    return {
        "fib50": fib50,
        "position": position,
        "live_price": live_price,
        "month_high": month_high,
        "month_low": month_low,
        "month_utc": month_start.strftime("%Y-%m"),
        "pct_of_range": (live_price - month_low) / fib_range * 100.0,
        "sar_dir": sar_dir,
        "sar_flipped": sar_flipped,
        "sar_value": sar_values[-1] if sar_values else None,
        "sar_prev_value": sar_values[-2] if sar_values and len(sar_values) >= 2 else None,
        "sar_cross_event": cross_event,
        "closed_extreme": closed_extreme,
        "h1_closed_time_utc": closed_time,
        "h1_closed_price": closed_price,
        "h1_closed_month_utc": closed_month,
        "_closed_h1_bars": closed_bars,
    }


def compute_stock_h1_fib(tv_symbol: str, h1_candles: int = 800) -> dict | None:
    return analyze_h1_frame(bars_to_frame(fetch_tv_ohlc(tv_symbol, "60", h1_candles)))


def snapshot_px(snapshot: NasdaqSnapshot) -> dict[str, int]:
    """Map Nasdaq 3M/M/W states to the three slots used by the vivier engine."""
    return {
        "M": int(snapshot.px_mn),
        "W": int(snapshot.px_w1),
        "D": int(snapshot.px_d1),
    }


def pre_entry_candidate(px: dict[str, int]) -> bool:
    """Cheap Renko-only gate, before downloading H1 Fibo/SAR data."""
    if any(px.get(tf) not in (-1, 0, 1) for tf in ("M", "W", "D")):
        return False
    direction = px["M"]
    if direction not in (-1, 1):
        return False
    profile = (
        px["W"] == -direction
        or px["D"] == -direction
        or (px["W"] == direction and px["D"] == 0)
    )
    score = vivier_base_score({"last_px": px})
    return profile and abs(score) >= VIVIER_MIN_ABS_SCORE


def snapshot_to_row(
    asset: NasdaqAsset,
    snapshot: NasdaqSnapshot,
    h1_fib: dict | None,
) -> dict:
    px = snapshot_px(snapshot)
    score = vivier_base_score({"last_px": px})
    return {
        "pair": asset.ticker,
        "name": asset.name,
        "theme": asset.theme,
        "tv_symbol": asset.tv_symbol,
        "pipeline_score": asset.pipeline_score,
        "px": px,
        "base_pct": score,
        "weighted_pct": score,
        "live_price": snapshot.close,
        "h1_price": (h1_fib or {}).get("live_price"),
        "h1_fib": h1_fib,
    }


def scan_assets(
    assets: list[NasdaqAsset],
    *,
    atr_length: int,
    workers: int,
    debug: bool,
    scan_fn: Callable[..., NasdaqSnapshot | None] = scan_symbol,
) -> dict[str, NasdaqSnapshot]:
    """Scan Renko states concurrently without sharing mutable bias state."""
    snapshots: dict[str, NasdaqSnapshot] = {}

    def one(asset: NasdaqAsset) -> tuple[str, NasdaqSnapshot | None]:
        snapshot = scan_fn(asset.tv_symbol, asset.ticker, atr_length, None, {}, debug)
        return asset.ticker, snapshot

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(one, asset): asset.ticker for asset in assets}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                key, snapshot = future.result()
            except Exception as exc:
                if debug:
                    print(f"  {ticker}: erreur Renko: {exc}")
                continue
            if snapshot is not None:
                snapshots[key] = snapshot
    return snapshots


def fetch_h1_for_candidates(
    candidates: list[NasdaqAsset],
    *,
    h1_candles: int,
    workers: int,
    debug: bool,
) -> dict[str, dict | None]:
    results: dict[str, dict | None] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(compute_stock_h1_fib, asset.tv_symbol, h1_candles): asset.ticker
            for asset in candidates
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                results[ticker] = future.result()
            except Exception as exc:
                results[ticker] = None
                if debug:
                    print(f"  {ticker}: erreur H1/Fibo/SAR: {exc}")
    return results


def _compact_price(value: object) -> str:
    return f"{float(value):.5f}".rstrip("0").rstrip(".") if _numeric(value) is not None else "?"


def _format_vivier_line(ticker: str, entry: dict) -> str:
    direction = int(entry.get("direction", 1))
    icon = "🟢" if direction == 1 else "🔴"
    parts = [f"{vivier_base_score(entry):+.0f}%", vivier_fib_path(entry)]
    age = vivier_age_label(entry)
    if age:
        parts.append(age)
    line = f"{icon} {ticker} ({' | '.join(parts)})"
    flame = vivier_flame_label(entry)
    return f"{line} {flame}" if flame else line


def build_nasdaq_message(
    state: dict,
    signals: list[dict],
    rows: list[dict],
    *,
    now: datetime | None = None,
) -> str | None:
    bull, bear = vivier_groups(state)
    near = near_alignment_entries(state)
    post_signal = post_signal_tracking_entries(state, rows)
    if not signals and not bull and not bear and not near and not post_signal:
        return None

    lines = ["📊 VIVIER NASDAQ"]
    has_content = False

    if signals:
        lines.extend(["", "🚨 SIGNAL VIVIER NASDAQ"])
        for signal in signals:
            direction = int(signal["direction"])
            icon = "🟢" if direction == 1 else "🔴"
            score = signal.get("weighted_pct")
            score_text = f" · {score:+.0f}%" if _numeric(score) is not None else ""
            lines.append(f"{icon} {signal['pair']} · 3M/M/W alignés{score_text}")
        has_content = True

    if post_signal:
        lines.extend(["", "🎯 SUIVI SIGNAL"])
        for item in post_signal:
            icon = "🟢" if item["direction"] == 1 else "🔴"
            pct = item.get("directional_pct")
            pct_text = f"{pct:+.2f}% depuis signal" if _numeric(pct) is not None else "depuis signal"
            objective = f"{item['objective_label']} {_compact_price(item.get('objective_value'))}"
            lines.append(f"{icon} {item['pair']} ({pct_text} | {objective})")
        has_content = True

    for title, entries in (
        ("🌱 VIVIER NASDAQ BULL", bull),
        ("🌱 VIVIER NASDAQ BEAR", bear),
    ):
        if not entries:
            continue
        lines.extend(["", title])
        lines.extend(_format_vivier_line(ticker, entry) for ticker, entry in entries)
        has_content = True

    if near:
        lines.extend(["", "⏳ PROCHE ALIGNEMENT"])
        for ticker, entry in near:
            direction = int(entry["direction"])
            icon = "🟢" if direction == 1 else "🔴"
            lines.append(
                f"{icon} {ticker} · W restant · {vivier_base_score(entry):+.0f}%"
            )
        has_content = True

    if not has_content:
        return None
    stamp = (now or datetime.now(PARIS_TZ)).astimezone(PARIS_TZ)
    lines.extend(["", f"⏰ {stamp.strftime('%Y-%m-%d %H:%M Paris')}"])
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Vivier Renko/Fibo/SAR indépendant pour le Nasdaq-100"
    )
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--state", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--limit", type=int, default=0, help="0 = toutes les actions du rapport")
    parser.add_argument("--ticker", action="append", dest="tickers", help="Limiter à un ticker; répétable")
    parser.add_argument("--length", type=int, default=14, help="Longueur ATR Renko")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--h1-candles", type=int, default=800)
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--force-telegram", action="store_true")
    parser.add_argument("--no-save", action="store_true", help="Ne pas modifier l'état JSON")
    parser.add_argument("--allow-weekend", action="store_true")
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()
    now_ny = datetime.now(NEW_YORK_TZ)
    if now_ny.weekday() >= 5 and not args.allow_weekend:
        print("Marché US fermé le week-end; utilisez --allow-weekend pour forcer le scan.")
        return 0

    try:
        assets = load_pipeline_universe(
            args.report,
            limit=max(0, args.limit),
            tickers=set(args.tickers) if args.tickers else None,
        )
    except Exception as exc:
        print(f"Impossible de charger l'univers Nasdaq: {exc}", file=sys.stderr)
        return 2
    if not assets:
        print("Aucune action Nasdaq à analyser.", file=sys.stderr)
        return 2

    previous_state = load_vivier_state(args.state)
    print(f"Scan Renko 3M/M/W: {len(assets)} actions...")
    snapshots = scan_assets(
        assets,
        atr_length=args.length,
        workers=args.workers,
        debug=args.debug,
    )
    if not snapshots:
        print("Aucun snapshot Renko disponible; état conservé.", file=sys.stderr)
        return 1

    active = set((previous_state.get("pairs") or {}).keys())
    pending = set((previous_state.get("pending_objectives") or {}).keys())
    asset_by_ticker = {asset.ticker: asset for asset in assets}
    h1_candidates = [
        asset_by_ticker[ticker]
        for ticker, snapshot in snapshots.items()
        if ticker in asset_by_ticker
        and (ticker in active or ticker in pending or pre_entry_candidate(snapshot_px(snapshot)))
    ]
    print(
        f"Snapshots valides: {len(snapshots)}/{len(assets)} | "
        f"Fibo/SAR H1 nécessaires: {len(h1_candidates)}"
    )
    h1_by_ticker = fetch_h1_for_candidates(
        h1_candidates,
        h1_candles=args.h1_candles,
        workers=args.workers,
        debug=args.debug,
    )

    rows = [
        snapshot_to_row(asset, snapshots[asset.ticker], h1_by_ticker.get(asset.ticker))
        for asset in assets
        if asset.ticker in snapshots
    ]
    now_paris = datetime.now(PARIS_TZ)
    current_state, signals = update_vivier(rows, previous_state, now=now_paris)
    carry_telegram_metadata(previous_state, current_state)
    message = build_nasdaq_message(current_state, signals, rows, now=now_paris)

    bull, bear = vivier_groups(current_state)
    print(f"Vivier Nasdaq: {len(bull)} BULL | {len(bear)} BEAR | {len(signals)} signal(s)")
    if message:
        print()
        print(message)
        body_hash = telegram_body_hash(message)
        if args.no_telegram:
            print("Telegram: envoi désactivé (--no-telegram).")
        elif not args.force_telegram and body_hash == previous_state.get("telegram_last_body_hash"):
            print("Telegram: contenu inchangé, envoi ignoré.")
        elif send_telegram(message):
            current_state["telegram_last_body_hash"] = body_hash
            current_state["telegram_last_sent_at_paris"] = now_paris.strftime("%Y-%m-%d %H:%M")
            print("Telegram: message envoyé.")
        else:
            print("Telegram: échec de l'envoi.", file=sys.stderr)
    else:
        print("Aucun titre dans le vivier et aucun nouveau signal.")

    if not args.no_save:
        save_vivier_state(current_state, args.state)
        print(f"État sauvegardé: {args.state}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
