#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cycle_analyzer.py

Analyse de cyclicite et de proportionnalite des grandes jambes d'un actif
(par defaut BTCUSD mensuel). Trois sorties en une:

  1. MESURE AUTO des jambes (ZigZag sur seuil de retournement %): pour chaque
     jambe haussiere/baissiere -> variation %, multiple, duree (mois/jours),
     pente log et angle (convention reproductible: 1 decade/an = 45 deg).
  2. RATIOS successifs (gains/gains, drawdowns/drawdowns, durees, intervalles
     sommet-a-sommet) + proximite des ratios aux niveaux de Fibonacci.
  3. PROJECTION naive du prochain creux et du prochain sommet (duree + ampleur
     extrapolees de la decroissance observee), avec prix/date estimes.

⚠️ Echantillon de quelques cycles seulement -> tendance observee, PAS une loi.
Aucune valeur predictive garantie. Outil d'analyse, pas de conseil financier.

Usage:
    python cycle_analyzer.py
    python cycle_analyzer.py --symbol INDEX:BTCUSD --interval M --threshold 0.4
    python cycle_analyzer.py --symbol NASDAQ:AAPL --interval W --threshold 0.3
    python cycle_analyzer.py --json cycle_btc.json
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import pandas as pd
from tabulate import tabulate

# Niveaux de Fibonacci usuels pour juger la decroissance d'un ratio.
FIB_LEVELS = {
    "0.236": 0.236, "0.382": 0.382, "0.5": 0.5, "0.618": 0.618,
    "0.786": 0.786, "1.0": 1.0, "1.272": 1.272, "1.618": 1.618,
}


# --------------------------------------------------------------------------- #
# Donnees
# --------------------------------------------------------------------------- #
def fetch_prices(symbol: str, interval: str, candles: int) -> pd.DataFrame:
    """OHLC via fetch_tv_ohlc (projet) avec repli yfinance si echec/insuffisant."""
    df = None
    try:
        from ichimoku_v4 import fetch_tv_ohlc
        df = fetch_tv_ohlc(symbol, interval, candles)
    except Exception as exc:
        print(f"fetch_tv_ohlc indisponible ({exc}); repli yfinance.", file=sys.stderr)

    if df is None or getattr(df, "empty", True) or len(df) < 10:
        try:
            import yfinance as yf
            yf_int = {"M": "1mo", "W": "1wk", "D": "1d"}.get(interval, "1mo")
            yf_sym = symbol.split(":")[-1]
            yf_sym = {"BTCUSD": "BTC-USD"}.get(yf_sym, yf_sym)
            data = yf.download(yf_sym, period="max", interval=yf_int,
                               auto_adjust=True, progress=False)
            if data is not None and not data.empty:
                data = data.rename(columns=str.lower)
                df = data[["open", "high", "low", "close"]].dropna()
        except Exception as exc:
            print(f"Repli yfinance echoue: {exc}", file=sys.stderr)
    if df is None or df.empty:
        raise RuntimeError(f"Aucune donnee pour {symbol} ({interval}).")
    df = df.copy()
    df.index = pd.DatetimeIndex(df.index).tz_localize(None)
    return df


# --------------------------------------------------------------------------- #
# Detection des pivots majeurs (ZigZag par seuil de retournement %)
# --------------------------------------------------------------------------- #
@dataclass
class Pivot:
    idx: int
    date: str
    price: float
    kind: str   # 'H' (sommet) / 'L' (creux)


def zigzag(prices: list[float], dates: list[str], pct: float) -> list[Pivot]:
    """Pivots majeurs: un sommet/creux est confirme quand le prix se retourne
    de >= pct depuis l'extreme courant."""
    if len(prices) < 3:
        return []
    direction = 0
    ext_i, ext_p = 0, prices[0]
    pivots: list[Pivot] = []
    for i in range(1, len(prices)):
        p = prices[i]
        if direction == 0:
            if p >= prices[0] * (1 + pct):
                pivots.append(Pivot(0, dates[0], prices[0], "L"))
                direction, ext_i, ext_p = 1, i, p
            elif p <= prices[0] * (1 - pct):
                pivots.append(Pivot(0, dates[0], prices[0], "H"))
                direction, ext_i, ext_p = -1, i, p
            continue
        if direction == 1:
            if p >= ext_p:
                ext_i, ext_p = i, p
            elif p <= ext_p * (1 - pct):
                pivots.append(Pivot(ext_i, dates[ext_i], ext_p, "H"))
                direction, ext_i, ext_p = -1, i, p
        else:
            if p <= ext_p:
                ext_i, ext_p = i, p
            elif p >= ext_p * (1 + pct):
                pivots.append(Pivot(ext_i, dates[ext_i], ext_p, "L"))
                direction, ext_i, ext_p = 1, i, p
    pivots.append(Pivot(ext_i, dates[ext_i], ext_p, "H" if direction == 1 else "L"))
    return pivots


# --------------------------------------------------------------------------- #
# Mesure des jambes
# --------------------------------------------------------------------------- #
@dataclass
class Leg:
    kind: str            # 'up' / 'down'
    date0: str
    date1: str
    price0: float
    price1: float
    pct: float           # variation %
    mult: float          # multiple (price1/price0)
    months: int          # nb de barres
    days: int
    slope: float         # log10(mult) / mois
    angle: float         # convention: 1 decade/an = 45 deg


def measure_legs(pivots: list[Pivot]) -> list[Leg]:
    legs: list[Leg] = []
    for a, b in zip(pivots, pivots[1:]):
        if a.price <= 0 or b.price <= 0:
            continue
        mult = b.price / a.price
        pct = (mult - 1.0) * 100.0
        months = b.idx - a.idx
        days = (datetime.fromisoformat(b.date) - datetime.fromisoformat(a.date)).days
        slope = math.log10(mult) / months if months else 0.0
        # angle reproductible: pente en decades/an, 1 decade/an -> 45 deg.
        angle = math.degrees(math.atan(slope * 12.0))
        legs.append(Leg(
            kind="up" if b.price > a.price else "down",
            date0=a.date, date1=b.date, price0=a.price, price1=b.price,
            pct=round(pct, 2), mult=round(mult, 3), months=months, days=days,
            slope=round(slope, 4), angle=round(angle, 1),
        ))
    return legs


def nearest_fib(ratio: float) -> str:
    name, lvl = min(FIB_LEVELS.items(), key=lambda kv: abs(kv[1] - ratio))
    return f"~{name} (ecart {abs(lvl - ratio):.2f})"


def successive_ratios(values: list[float]) -> list[float]:
    return [values[i + 1] / values[i] for i in range(len(values) - 1) if values[i]]


# --------------------------------------------------------------------------- #
# Projection
# --------------------------------------------------------------------------- #
def geo_mean(vals: list[float]) -> float:
    vals = [v for v in vals if v > 0]
    if not vals:
        return 1.0
    return math.exp(sum(math.log(v) for v in vals) / len(vals))


def project(pivots: list[Pivot], legs: list[Leg]) -> dict:
    ups = [l for l in legs if l.kind == "up"]
    downs = [l for l in legs if l.kind == "down"]
    peaks = [p for p in pivots if p.kind == "H"]
    if len(downs) < 2 or len(ups) < 2 or not pivots:
        return {"note": "Pas assez de cycles pour projeter."}

    # Ratios moyens de decroissance (geometriques) sur les magnitudes.
    drop_ratio = geo_mean(successive_ratios([abs(d.pct) for d in downs]))
    gain_ratio = geo_mean(successive_ratios([abs(u.pct) for u in ups]))
    bear_months = round(sum(d.months for d in downs[-2:]) / 2)
    bull_months = round(sum(u.months for u in ups[-2:]) / 2)

    last = pivots[-1]
    out: dict = {
        "etat_actuel": f"dernier pivot = {last.kind} {last.price:.2f} ({last.date[:10]})",
        "decroissance_drawdown_par_cycle": round(drop_ratio, 3),
        "decroissance_gain_par_cycle": round(gain_ratio, 3),
    }

    last_date = datetime.fromisoformat(last.date)

    if last.kind == "H":
        proj_drop = downs[-1].pct * drop_ratio          # ex: -75% * 0.97
        trough_price = last.price * (1 + proj_drop / 100.0)
        trough_date = last_date + pd.DateOffset(months=bear_months)
        out["prochain_creux"] = {
            "drawdown_estime_%": round(proj_drop, 1),
            "duree_estimee_mois": bear_months,
            "prix_estime": round(trough_price, 2),
            "date_estimee": trough_date.strftime("%Y-%m"),
        }
        next_gain = ups[-1].pct * gain_ratio
        peak_price = trough_price * (1 + next_gain / 100.0)
        peak_date = trough_date + pd.DateOffset(months=bull_months)
        out["sommet_suivant"] = {
            "gain_estime_%": round(next_gain, 1),
            "prix_estime": round(peak_price, 2),
            "date_estimee": peak_date.strftime("%Y-%m"),
        }
    else:
        next_gain = ups[-1].pct * gain_ratio
        peak_price = last.price * (1 + next_gain / 100.0)
        peak_date = last_date + pd.DateOffset(months=bull_months)
        out["prochain_sommet"] = {
            "gain_estime_%": round(next_gain, 1),
            "duree_estimee_mois": bull_months,
            "prix_estime": round(peak_price, 2),
            "date_estimee": peak_date.strftime("%Y-%m"),
        }

    if len(peaks) >= 2:
        intervals = [peaks[i + 1].idx - peaks[i].idx for i in range(len(peaks) - 1)]
        out["intervalles_sommet_a_sommet_mois"] = intervals
        out["intervalle_sommet_moyen_mois"] = round(sum(intervals) / len(intervals), 1)
    return out


# --------------------------------------------------------------------------- #
# Rapport
# --------------------------------------------------------------------------- #
def build_report(symbol, interval, pivots, legs, proj) -> str:
    lines = [f"ANALYSE DE CYCLE — {symbol} ({interval}) — {len(pivots)} pivots, {len(legs)} jambes", ""]

    lines.append("JAMBES (auto, ZigZag)")
    rows = []
    for l in legs:
        rows.append([
            "↑" if l.kind == "up" else "↓",
            f"{l.date0[:7]}→{l.date1[:7]}",
            f"{l.pct:+.1f}%", f"x{l.mult:.2f}",
            f"{l.months}m / {l.days}j",
            f"{l.angle:+.1f}°",
        ])
    lines.append(tabulate(rows, headers=["", "Periode", "Var", "Mult", "Duree", "Angle"], tablefmt="github"))

    ups = [l for l in legs if l.kind == "up"]
    downs = [l for l in legs if l.kind == "down"]

    lines += ["", "RATIOS SUCCESSIFS (proportionnalite)"]
    if len(ups) >= 2:
        gr = successive_ratios([abs(u.pct) for u in ups])
        lines.append("Gains haussiers   : " + " · ".join(f"{r:.2f} [{nearest_fib(r)}]" for r in gr))
    if len(downs) >= 2:
        dr = successive_ratios([abs(d.pct) for d in downs])
        lines.append("Drawdowns baissiers: " + " · ".join(f"{r:.2f} [{nearest_fib(r)}]" for r in dr))
        durr = successive_ratios([d.months for d in downs])
        if durr:
            lines.append("Durees baissieres : " + " · ".join(f"{r:.2f}" for r in durr))

    lines += ["", "PROJECTION (extrapolation naive — faible echantillon)"]
    for k, v in proj.items():
        if isinstance(v, dict):
            lines.append(f"• {k}:")
            for kk, vv in v.items():
                lines.append(f"    {kk}: {vv}")
        else:
            lines.append(f"• {k}: {v}")

    lines += ["", "⚠️ Quelques cycles seulement -> tendance observee, pas une loi. "
              "Pas un conseil financier."]
    return "\n".join(lines)


def main() -> int:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")
    p = argparse.ArgumentParser(description="Analyse cyclicite + ratios + projection.")
    p.add_argument("--symbol", default="INDEX:BTCUSD")
    p.add_argument("--interval", default="M", help="M / W / D")
    p.add_argument("--candles", type=int, default=240)
    p.add_argument("--threshold", type=float, default=0.5,
                   help="Seuil de retournement ZigZag (0.5 = 50%). Monte-le pour ne garder que les grands cycles.")
    p.add_argument("--start", default=None, help="Date de debut YYYY-MM-DD (ignore le bruit ancien).")
    p.add_argument("--json", default=None)
    args = p.parse_args()

    print(f"Telechargement {args.symbol} ({args.interval})...")
    df = fetch_prices(args.symbol, args.interval, args.candles)
    if args.start:
        df = df[df.index >= pd.Timestamp(args.start)]
    prices = [float(x) for x in df["close"].tolist()]
    dates = [pd.Timestamp(d).isoformat() for d in df.index]
    print(f"{len(prices)} barres ({dates[0][:7]} -> {dates[-1][:7]}).")

    pivots = zigzag(prices, dates, args.threshold)
    if len(pivots) < 3:
        print("Pas assez de pivots — baisse --threshold.", file=sys.stderr)
        return 0
    legs = measure_legs(pivots)
    proj = project(pivots, legs)

    report = build_report(args.symbol, args.interval, pivots, legs, proj)
    print()
    print(report)

    if args.json:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "symbol": args.symbol, "interval": args.interval, "threshold": args.threshold,
            "pivots": [asdict(p) for p in pivots],
            "legs": [asdict(l) for l in legs],
            "projection": proj,
        }
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"\nSaved: {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
