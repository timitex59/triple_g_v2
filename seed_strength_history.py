#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
seed_strength_history.py

Pre-remplit renko_strength_history.json a partir des CSV journaliers OANDA
deposes dans forex_data/, pour que l'intensite de tendance (percentile dans
renko_score_29pairs_v16.py) soit calibree immediatement, sans attendre ~20-60
jours de runs en production.

Pour chaque jour, on reconstruit la dispersion des devises (spread = devise la
plus forte - la plus faible) en REUTILISANT la meme fonction currency_strength()
que le script de production, donc la logique est strictement identique.

Usage:
    python seed_strength_history.py            # ecrit renko_strength_history.json
    python seed_strength_history.py --dry-run  # affiche seulement les stats

Les CSV attendus: forex_data/OANDA_XXXYYY*.csv avec colonnes time,open,high,low,close
(time = timestamp Unix en secondes). Les doublons (meme paire) sont dedupliques
en gardant le fichier le plus fourni.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import statistics
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd

import renko_score_29pairs_v16 as rk

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "forex_data")
PARIS = ZoneInfo("Europe/Paris")


def load_daily_changes() -> dict[str, dict[str, float]]:
    """Retourne {pair: {date_iso: chg%_journalier}}."""
    files = glob.glob(os.path.join(DATA_DIR, "OANDA_*.csv"))
    by_pair: dict[str, pd.DataFrame] = {}
    for f in files:
        m = re.search(r"OANDA_([A-Z]{6})", os.path.basename(f))
        if not m:
            continue
        pair = m.group(1)
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        # doublon -> garde le plus fourni
        if pair not in by_pair or len(df) > len(by_pair[pair]):
            by_pair[pair] = df

    chg_by_pair: dict[str, dict[str, float]] = {}
    for pair, df in by_pair.items():
        df = df.sort_values("time")
        ts = [int(t) for t in df["time"].tolist()]
        closes = [float(c) for c in df["close"].tolist()]
        dates = [datetime.fromtimestamp(t, timezone.utc).astimezone(PARIS).date().isoformat() for t in ts]
        d: dict[str, float] = {}
        for i in range(1, len(closes)):
            if closes[i - 1]:
                d[dates[i]] = (closes[i] - closes[i - 1]) / closes[i - 1] * 100.0
        chg_by_pair[pair] = d
    return chg_by_pair


def build_history(chg_by_pair: dict[str, dict[str, float]]) -> dict[str, dict]:
    all_dates = sorted(set().union(*[set(d) for d in chg_by_pair.values()]))
    hist: dict[str, dict] = {}
    for date in all_dates:
        rows = [{"pair": p, "daily_chg": d[date]} for p, d in chg_by_pair.items() if date in d]
        if len(rows) < 10:                       # jour trop peu couvert (debut de serie)
            continue
        strength = rk.currency_strength(rows)    # meme logique que la production
        if len(strength) < 2:
            continue
        spread = strength[0][1] - strength[-1][1]
        vals = [r["daily_chg"] for r in rows]
        hist[date] = {
            "spread": round(spread, 4),
            "force": round(sum(vals) / len(vals), 4),
            "up": sum(1 for v in vals if v > 0),
            "dn": sum(1 for v in vals if v < 0),
            "ts": "seed",
        }
    # borne a la meme taille que la production
    keys = sorted(hist)
    for k in keys[:-rk.HISTORY_MAX_DAYS]:
        del hist[k]
    return hist


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed renko_strength_history.json depuis forex_data/.")
    parser.add_argument("--dry-run", action="store_true", help="N'ecrit pas, affiche seulement les stats.")
    args = parser.parse_args()

    chg_by_pair = load_daily_changes()
    print(f"Paires chargees: {len(chg_by_pair)}  ({', '.join(sorted(chg_by_pair))})")

    hist = build_history(chg_by_pair)
    if not hist:
        print("Aucun jour exploitable.")
        return 1

    spreads = sorted(v["spread"] for v in hist.values())
    print(f"Jours d'historique: {len(hist)}  ({min(hist)} -> {max(hist)})")
    print(f"Spread: min {spreads[0]:.3f} | mediane {statistics.median(spreads):.3f} "
          f"| P66 {spreads[int(.66*len(spreads))]:.3f} | P90 {spreads[int(.90*len(spreads))]:.3f} "
          f"| pic {spreads[-1]:.3f}")

    if args.dry_run:
        print("(dry-run: rien ecrit)")
        return 0

    with open(rk.STRENGTH_HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False)
    print(f"Ecrit -> {rk.STRENGTH_HISTORY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
