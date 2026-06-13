#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
seed_regime_reference.py

Construit la REFERENCE DE REGIME long terme: la distribution du spread quotidien
(dispersion des devises) sur tout l'historique daily disponible (forex_data/,
jusqu'a ~22 ans). Sert a situer la PERIODE actuelle (calme / agitee) par rapport
au long terme, ce que ni la fenetre 60j ni le profil intraday (3,4 ans) ne
peuvent donner.

Sortie: renko_regime_reference.json
  { "q": [101 quantiles tries P0..P100], "median": .., "n": .., "meta": {...} }

Usage:
    python seed_regime_reference.py
    python seed_regime_reference.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import re
import statistics
from datetime import datetime, timezone

import renko_score_29pairs_v16 as rk

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "forex_data")
OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "renko_regime_reference.json")
MIN_PAIRS = 10


def daily_spreads() -> tuple[list[float], list[float]]:
    by_pair: dict[str, list[dict]] = {}
    for f in glob.glob(os.path.join(DATA_DIR, "OANDA_*.csv")):
        m = re.search(r"OANDA_([A-Z]{6})", os.path.basename(f))
        if not m:
            continue
        rows = list(csv.DictReader(open(f)))
        if m.group(1) not in by_pair or len(rows) > len(by_pair[m.group(1)]):
            by_pair[m.group(1)] = rows

    chg: dict[str, dict[str, float]] = {}
    for p, rows in by_pair.items():
        rows = sorted(rows, key=lambda r: int(r["time"]))
        d, prev = {}, None
        for r in rows:
            c = float(r["close"])
            dt = datetime.fromtimestamp(int(r["time"]), timezone.utc).date().isoformat()
            if prev:
                d[dt] = (c - prev) / prev * 100.0
            prev = c
        chg[p] = d

    dates = sorted(set().union(*[set(d) for d in chg.values()]))
    spreads, breadth = [], []
    for dt in dates:
        rws = [{"pair": p, "daily_chg": chg[p][dt]} for p in chg if dt in chg[p]]
        if len(rws) < MIN_PAIRS:
            continue
        s = rk.currency_strength(rws)
        if len(s) < 2:
            continue
        spreads.append(s[0][1] - s[-1][1])
        up = sum(1 for r in rws if r["daily_chg"] > 0)
        dn = sum(1 for r in rws if r["daily_chg"] < 0)
        if up + dn > 0:
            breadth.append(100.0 * up / (up + dn))   # % de paires haussieres
    return spreads, breadth


def main() -> int:
    parser = argparse.ArgumentParser(description="Construit renko_regime_reference.json (distribution 22 ans).")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    vals, breadth = daily_spreads()
    if len(vals) < 200:
        print(f"Pas assez de jours ({len(vals)}).")
        return 1
    vals.sort()
    n = len(vals)
    q = [round(vals[round(p / 100 * (n - 1))], 4) for p in range(101)]
    breadth.sort()
    nb = len(breadth)
    bq = [round(breadth[round(p / 100 * (nb - 1))], 1) for p in range(101)] if nb else []
    ref = {
        "q": q,
        "median": round(statistics.median(vals), 4),
        "n": n,
        "breadth_q": bq,                                   # % paires haussieres, P0..P100
        "breadth_median": round(statistics.median(breadth), 1) if breadth else 50.0,
        "meta": {"generated_at": datetime.now(timezone.utc).isoformat()},
    }
    print(f"Jours: {n}")
    print(f"Spread  mediane {ref['median']} | P25 {q[25]} | P75 {q[75]} | P90 {q[90]} | pic {q[100]}")
    print(f"Breadth mediane {ref['breadth_median']}% | P25 {bq[25]}% | P75 {bq[75]}% | P90 {bq[90]}%")

    if args.dry_run:
        print("(dry-run: rien ecrit)")
        return 0
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(ref, f, ensure_ascii=False)
    print(f"Ecrit -> {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
