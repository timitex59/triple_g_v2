#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
seed_intraday_profile.py

Construit un PROFIL INTRADAY de l'intensite de tendance a partir de la derniere
annee des CSV 1H des 29 paires (forex_1h_data/). Objectif: pouvoir juger l'intensite du jour de
maniere JUSTE a n'importe quelle heure, en comparant la dispersion accumulee
"a l'heure H" a la distribution historique de la meme heure H (et non a des
journees completes, ce qui sous-estime en cours de journee).

Definitions (alignees sur la logique de production):
  - Journee FX = ouverture a 17:00 New York (gere le DST via America/New_York).
  - Pour chaque bougie 1H: mouvement cumule depuis l'ouverture de session =
    (close - close_de_reference) / close_de_reference, ou close_de_reference est
    le dernier close de la session precedente (~ cloture journaliere precedente).
  - hour_idx = nombre d'heures ecoulees depuis l'ouverture de session (0..~23).
  - Pour chaque (jour, hour_idx): dispersion des devises (spread = devise la plus
    forte - la plus faible) via la MEME fonction currency_strength() que la prod.

Sortie: renko_intraday_profile.json
  { "by_hour": { "<h>": {"n": .., "q": [101 quantiles tries]} , ... },
    "meta": {...} }
Les 101 quantiles (P0..P100) permettent au live de situer le spread courant en
percentile pour l'heure courante, de façon compacte.

Usage:
    python seed_intraday_profile.py
    python seed_intraday_profile.py --dry-run
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

import renko_score_29pairs_v16 as rk

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "forex_1h_data")
OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "renko_intraday_profile.json")
NY = ZoneInfo("America/New_York")
SESSION_OPEN_HOUR = 17           # 17:00 New York = ouverture journaliere OANDA
MIN_PAIRS_PER_CELL = 10          # nb mini de paires pour calculer une dispersion
PROFILE_LOOKBACK_DAYS = 365      # reference recente pour l'intensite intraday


def session_open_dt(ts: int) -> datetime:
    """Ouverture de session (17:00 NY) la plus recente <= ts."""
    dt = datetime.fromtimestamp(ts, NY)
    op = dt.replace(hour=SESSION_OPEN_HOUR, minute=0, second=0, microsecond=0)
    if dt < op:
        op -= timedelta(days=1)
    return op


def load_1h() -> dict[str, pd.DataFrame]:
    files = glob.glob(os.path.join(DATA_DIR, "OANDA_*.csv"))
    by_pair: dict[str, pd.DataFrame] = {}
    for f in files:
        m = re.search(r"OANDA_([A-Z]{6})", os.path.basename(f))
        if not m:
            continue
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        pair = m.group(1)
        if pair not in by_pair or len(df) > len(by_pair[pair]):   # garde le + fourni
            by_pair[pair] = df
    return by_pair


def trim_to_recent_window(by_pair: dict[str, pd.DataFrame], days: int) -> tuple[dict[str, pd.DataFrame], int, int]:
    max_ts = max(int(df["time"].max()) for df in by_pair.values() if len(df))
    cutoff_ts = max_ts - days * 24 * 3600
    trimmed = {}
    for pair, df in by_pair.items():
        df2 = df[df["time"].astype(int) >= cutoff_ts].copy()
        if len(df2):
            trimmed[pair] = df2
    return trimmed, cutoff_ts, max_ts


def pair_chg_cells(df: pd.DataFrame) -> dict[tuple[str, int], float]:
    """Pour une paire: {(jour_iso, hour_idx): chg% cumule depuis l'ouverture}."""
    df = df.sort_values("time")
    ts = [int(t) for t in df["time"].tolist()]
    close = [float(c) for c in df["close"].tolist()]

    # Decoupe en sessions; ref d'une session = dernier close de la session precedente.
    cells: dict[tuple[str, int], float] = {}
    cur_key = None
    cur_open_ts = None
    prev_session_last_close = None
    this_session_last_close = None

    for t, c in zip(ts, close):
        op = session_open_dt(t)
        key = op.date().isoformat()
        if key != cur_key:
            # nouvelle session: la ref devient le dernier close de la precedente
            prev_session_last_close = this_session_last_close
            cur_key = key
            cur_open_ts = int(op.timestamp())
            this_session_last_close = None
        if prev_session_last_close:
            hour_idx = int(round((t - cur_open_ts) / 3600.0))
            if 0 <= hour_idx <= 26:
                cells[(cur_key, hour_idx)] = (c - prev_session_last_close) / prev_session_last_close * 100.0
        this_session_last_close = c
    return cells


def build_profile(by_pair: dict[str, pd.DataFrame], cutoff_ts: int, max_ts: int) -> dict:
    # chg par paire par cellule
    chg = {p: pair_chg_cells(df) for p, df in by_pair.items()}
    # toutes les cellules (jour, heure)
    all_cells: set[tuple[str, int]] = set()
    for d in chg.values():
        all_cells.update(d.keys())

    spreads_by_hour: dict[int, list[float]] = {}
    for (day, hour) in all_cells:
        rows = [{"pair": p, "daily_chg": chg[p][(day, hour)]} for p in chg if (day, hour) in chg[p]]
        if len(rows) < MIN_PAIRS_PER_CELL:
            continue
        strength = rk.currency_strength(rows)
        if len(strength) < 2:
            continue
        spread = strength[0][1] - strength[-1][1]
        spreads_by_hour.setdefault(hour, []).append(spread)

    by_hour = {}
    for hour, vals in spreads_by_hour.items():
        vals.sort()
        n = len(vals)
        if n < 30:                                   # heure trop peu peuplee (bords)
            continue
        q = [round(vals[round(p / 100 * (n - 1))], 4) for p in range(101)]  # P0..P100
        by_hour[str(hour)] = {"n": n, "q": q}

    return {
        "by_hour": by_hour,
        "meta": {
            "pairs": sorted(by_pair),
            "session_open_ny": SESSION_OPEN_HOUR,
            "lookback_days": PROFILE_LOOKBACK_DAYS,
            "reference_start": datetime.fromtimestamp(cutoff_ts, timezone.utc).date().isoformat(),
            "reference_end": datetime.fromtimestamp(max_ts, timezone.utc).date().isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    }


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Construit renko_intraday_profile.json depuis forex_1h_data/.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    by_pair = load_1h()
    print(f"Paires 1H chargees: {len(by_pair)}")
    if len(by_pair) < 10:
        print("Pas assez de paires.")
        return 1

    by_pair, cutoff_ts, max_ts = trim_to_recent_window(by_pair, PROFILE_LOOKBACK_DAYS)
    print(f"Fenetre reference: {datetime.fromtimestamp(cutoff_ts, timezone.utc).date()} -> "
          f"{datetime.fromtimestamp(max_ts, timezone.utc).date()} ({PROFILE_LOOKBACK_DAYS}j)")
    if len(by_pair) < 10:
        print("Pas assez de paires apres filtrage.")
        return 1

    profile = build_profile(by_pair, cutoff_ts, max_ts)
    hours = sorted(int(h) for h in profile["by_hour"])
    print(f"Heures profilees: {len(hours)}  ({hours[0]}..{hours[-1]} depuis l'ouverture NY)")
    print("Courbe du spread MEDIAN par heure (montee typique de la tendance dans la journee):")
    for h in hours:
        d = profile["by_hour"][str(h)]
        med = d["q"][50]
        bar = "█" * int(med / max(0.01, profile["by_hour"][str(hours[-1])]["q"][50]) * 30)
        print(f"  h+{h:>2} (n={d['n']:>3}) med {med:5.2f}  P90 {d['q'][90]:5.2f}  {bar}")

    if args.dry_run:
        print("(dry-run: rien ecrit)")
        return 0
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(profile, f, ensure_ascii=False)
    print(f"Ecrit -> {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
