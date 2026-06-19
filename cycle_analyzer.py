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
  3. PROJECTION par regression log-lineaire (duree + ampleur) avec bandes
     basse/centrale/haute autour de la tendance observee.

⚠️ Echantillon de quelques cycles seulement -> tendance observee, PAS une loi.
Aucune valeur predictive garantie. Outil d'analyse, pas de conseil financier.

Usage:
    python cycle_analyzer.py
    python cycle_analyzer.py --symbol INDEX:BTCUSD --interval M --threshold 0.4
    python cycle_analyzer.py --symbol NASDAQ:AAPL --interval W --threshold 0.3
    python cycle_analyzer.py --csv "INDEX_BTCUSD, 1M_d64fb.csv" --pivot-mode ohlc
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


def load_csv_prices(path: str) -> pd.DataFrame:
    """Load local OHLC CSV. Supports TradingView-style `time,open,high,low,close`."""
    df = pd.read_csv(path)
    df.columns = [str(c).strip().lower() for c in df.columns]
    if "time" in df.columns:
        time_col = df["time"]
        if pd.api.types.is_numeric_dtype(time_col):
            idx = pd.to_datetime(time_col, unit="s", utc=True)
        else:
            idx = pd.to_datetime(time_col, utc=True)
    elif "date" in df.columns:
        idx = pd.to_datetime(df["date"], utc=True)
    elif "datetime" in df.columns:
        idx = pd.to_datetime(df["datetime"], utc=True)
    else:
        idx = pd.to_datetime(df.iloc[:, 0], utc=True)
    df.index = pd.DatetimeIndex(idx).tz_localize(None)
    needed = {"open", "high", "low", "close"}
    missing = sorted(needed - set(df.columns))
    if missing:
        raise RuntimeError(f"CSV OHLC incomplet, colonnes manquantes: {', '.join(missing)}")
    return df[["open", "high", "low", "close"]].astype(float).dropna()


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


def zigzag_ohlc(highs: list[float], lows: list[float], dates: list[str], pct: float) -> list[Pivot]:
    """ZigZag on OHLC extremes: H pivots use highs, L pivots use lows."""
    if len(highs) < 3 or len(lows) != len(highs):
        return []

    direction = 0
    high_i, high_p = 0, highs[0]
    low_i, low_p = 0, lows[0]
    pivots: list[Pivot] = []

    for i in range(1, len(highs)):
        h = highs[i]
        l = lows[i]
        if direction == 0:
            if h > high_p:
                high_i, high_p = i, h
            if l < low_p:
                low_i, low_p = i, l
            if h >= low_p * (1 + pct):
                pivots.append(Pivot(low_i, dates[low_i], low_p, "L"))
                direction = 1
                high_i, high_p = i, h
            elif l <= high_p * (1 - pct):
                pivots.append(Pivot(high_i, dates[high_i], high_p, "H"))
                direction = -1
                low_i, low_p = i, l
            continue

        if direction == 1:
            if h >= high_p:
                high_i, high_p = i, h
            elif l <= high_p * (1 - pct):
                pivots.append(Pivot(high_i, dates[high_i], high_p, "H"))
                direction = -1
                low_i, low_p = i, l
        else:
            if l <= low_p:
                low_i, low_p = i, l
            elif h >= low_p * (1 + pct):
                pivots.append(Pivot(low_i, dates[low_i], low_p, "L"))
                direction = 1
                high_i, high_p = i, h

    if direction == 1:
        pivots.append(Pivot(high_i, dates[high_i], high_p, "H"))
    elif direction == -1:
        pivots.append(Pivot(low_i, dates[low_i], low_p, "L"))
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
def loglin_forecast(values: list[float]) -> tuple[float, float, float]:
    """Regression log-lineaire de `values` sur l'index de cycle (0,1,2,...) et
    prevision de l'observation SUIVANTE. Renvoie (central, bas, haut) ou la
    bande = +/- 1 erreur-type de PREDICTION (residus). Le log capte la nature
    multiplicative (decroissance/croissance %)."""
    pos = [v for v in values if v > 0]
    if len(pos) < 2:
        v = pos[-1] if pos else 0.0
        return v, v, v
    ys = [math.log(v) for v in pos]
    n = len(ys)
    xs = list(range(n))
    xbar = sum(xs) / n
    ybar = sum(ys) / n
    sxx = sum((x - xbar) ** 2 for x in xs) or 1e-9
    b1 = sum((xs[i] - xbar) * (ys[i] - ybar) for i in range(n)) / sxx
    b0 = ybar - b1 * xbar
    x_new = n                                   # prochain cycle
    yhat = b0 + b1 * x_new
    sse = sum((ys[i] - (b0 + b1 * xs[i])) ** 2 for i in range(n))
    if n >= 3:
        s = math.sqrt(sse / (n - 2))
        se = s * math.sqrt(1 + 1 / n + (x_new - xbar) ** 2 / sxx)
    else:                                       # 2 points: pas de residu -> proxy
        se = abs(ys[1] - ys[0]) * 0.5
    return math.exp(yhat), math.exp(yhat - se), math.exp(yhat + se)


def _band(bas, central, haut) -> dict:
    return {"bas": bas, "central": central, "haut": haut}


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def pct_change(a: float, b: float) -> float:
    return (b / a - 1.0) * 100.0 if a else 0.0


def project(pivots: list[Pivot], legs: list[Leg]) -> dict:
    # On ecarte la 1re jambe (amorce partielle bornee par --start, pas un vrai pivot).
    stat_legs = legs[1:] if len(legs) > 1 else legs
    ups = [l for l in stat_legs if l.kind == "up"]
    downs = [l for l in stat_legs if l.kind == "down"]
    peaks = [p for p in pivots if p.kind == "H"]
    if len(downs) < 2 or len(ups) < 2 or not pivots:
        return {"note": "Pas assez de cycles complets (baisse --threshold ou allonge l'historique)."}

    last = pivots[-1]
    last_date = datetime.fromisoformat(last.date)
    out: dict = {
        "etat_actuel": f"dernier pivot = {last.kind} {last.price:.0f} ({last.date[:7]})",
        "modele": "regression log-lineaire par cycle, bande ~1 erreur-type",
    }

    dp, dlo, dhi = loglin_forecast([abs(d.pct) for d in downs])       # drawdown %
    dp, dlo, dhi = tuple(clamp(x, 0.0, 99.0) for x in (dp, dlo, dhi))
    gp, glo, ghi = loglin_forecast([abs(u.pct) for u in ups])         # gain %
    bp, blo, bhi = loglin_forecast([float(d.months) for d in downs])  # duree bear
    up_, ulo, uhi = loglin_forecast([float(u.months) for u in ups])   # duree bull
    peak_mults = [peaks[i + 1].price / peaks[i].price
                  for i in range(len(peaks) - 1) if peaks[i].price > 0]
    peak_model = loglin_forecast(peak_mults) if len(peak_mults) >= 2 else None
    peak_intervals = [peaks[i + 1].idx - peaks[i].idx for i in range(len(peaks) - 1)]
    peak_interval_model = loglin_forecast([float(x) for x in peak_intervals]) if len(peak_intervals) >= 2 else None
    last_peak = peaks[-1] if peaks else last
    last_peak_date = datetime.fromisoformat(last_peak.date)

    def date_off(d0, m):
        return (d0 + pd.DateOffset(months=int(round(m)))).strftime("%Y-%m")

    def add_peak_to_peak_projection(target: dict, trough_price: float) -> None:
        if peak_model is None:
            return
        mp, mlo, mhi = peak_model
        p_lo = last_peak.price * mlo
        p_mid = last_peak.price * mp
        p_hi = last_peak.price * mhi
        target["multiple_sommet_a_sommet"] = _band(round(mlo, 2), round(mp, 2), round(mhi, 2))
        target["prix_sommet_a_sommet"] = _band(round(p_lo), round(p_mid), round(p_hi))
        target["gain_sommet_a_sommet_%"] = _band(round(pct_change(last_peak.price, p_lo)),
                                                 round(pct_change(last_peak.price, p_mid)),
                                                 round(pct_change(last_peak.price, p_hi)))
        target["gain_requis_new_ath_depuis_creux_%"] = round(pct_change(trough_price, last_peak.price))
        target["gain_central_sommet_a_sommet_depuis_creux_%"] = round(pct_change(trough_price, p_mid))
        if peak_interval_model is not None:
            ip, ilo, ihi = peak_interval_model
            target["date_sommet_a_sommet"] = _band(date_off(last_peak_date, ilo),
                                                   date_off(last_peak_date, ip),
                                                   date_off(last_peak_date, ihi))
        if p_mid < last_peak.price:
            target["diagnostic_sommet"] = (
                "lower high central: la regression sommet-a-sommet passe sous le dernier sommet"
            )
        else:
            target["diagnostic_sommet"] = "new ATH central: la regression sommet-a-sommet reste au-dessus"

    if last.kind == "H":
        trough_central = last.price * (1 - dp / 100)
        out["prochain_creux"] = {
            "drawdown_%": _band(round(-dhi, 1), round(-dp, 1), round(-dlo, 1)),
            "prix": _band(round(last.price * (1 - dhi / 100)),       # drop profond -> prix bas
                          round(last.price * (1 - dp / 100)),
                          round(last.price * (1 - dlo / 100))),
            "duree_mois": _band(round(blo), round(bp), round(bhi)),
            "date": _band(date_off(last_date, blo), date_off(last_date, bp), date_off(last_date, bhi)),
        }
        peak_m = round(bp) + round(up_)
        trough_gain_price = trough_central * (1 + gp / 100)
        out["sommet_suivant"] = {
            "gain_depuis_creux_%": _band(round(glo), round(gp), round(ghi)),
            "prix_depuis_creux": _band(round(trough_central * (1 + glo / 100)),
                                       round(trough_gain_price),
                                       round(trough_central * (1 + ghi / 100))),
            "date_depuis_creux": date_off(last_date, peak_m),
            "vs_dernier_sommet_depuis_creux": f"{pct_change(last_peak.price, trough_gain_price):+.0f}%",
        }
        add_peak_to_peak_projection(out["sommet_suivant"], trough_central)
    else:
        trough_central = last.price
        trough_gain_price = last.price * (1 + gp / 100)
        out["prochain_sommet"] = {
            "gain_depuis_creux_%": _band(round(glo), round(gp), round(ghi)),
            "prix_depuis_creux": _band(round(last.price * (1 + glo / 100)),
                                       round(trough_gain_price),
                                       round(last.price * (1 + ghi / 100))),
            "duree_mois": _band(round(ulo), round(up_), round(uhi)),
            "date": _band(date_off(last_date, ulo), date_off(last_date, up_), date_off(last_date, uhi)),
            "vs_dernier_sommet_depuis_creux": f"{pct_change(last_peak.price, trough_gain_price):+.0f}%",
        }
        add_peak_to_peak_projection(out["prochain_sommet"], trough_central)

    if peak_interval_model is not None:
        pp, plo, phi = peak_interval_model
        out["intervalle_sommet_a_sommet_mois"] = {
            "observes": peak_intervals,
            "prochain": _band(round(plo), round(pp), round(phi)),
        }
    if peak_model is not None:
        mp, mlo, mhi = peak_model
        out["multiples_sommet_a_sommet"] = {
            "observes": [round(x, 2) for x in peak_mults],
            "prochain": _band(round(mlo, 2), round(mp, 2), round(mhi, 2)),
        }
    return out


# --------------------------------------------------------------------------- #
# Rapport
# --------------------------------------------------------------------------- #
def format_projection_value(value) -> str:
    if isinstance(value, dict) and set(value.keys()) == {"bas", "central", "haut"}:
        return f"{value['bas']} / {value['central']} / {value['haut']}"
    return str(value)


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

    lines += ["", "PROJECTION (regression log-lineaire + bandes bas/central/haut)"]
    for k, v in proj.items():
        if isinstance(v, dict):
            lines.append(f"• {k}:")
            for kk, vv in v.items():
                lines.append(f"    {kk}: {format_projection_value(vv)}")
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
    p.add_argument("--csv", default=None, help="CSV OHLC local a utiliser au lieu du fetch TradingView/yfinance.")
    p.add_argument("--price-column", default="close", choices=["open", "high", "low", "close"],
                   help="Colonne OHLC utilisee en mode pivot=single.")
    p.add_argument("--pivot-mode", default="single", choices=["single", "ohlc"],
                   help="single=une colonne; ohlc=sommets sur high et creux sur low.")
    p.add_argument("--json", default=None)
    args = p.parse_args()

    if args.csv:
        print(f"Chargement CSV {args.csv}...")
        df = load_csv_prices(args.csv)
        source = args.csv
    else:
        print(f"Telechargement {args.symbol} ({args.interval})...")
        df = fetch_prices(args.symbol, args.interval, args.candles)
        source = args.symbol
    if args.start:
        df = df[df.index >= pd.Timestamp(args.start)]
    dates = [pd.Timestamp(d).isoformat() for d in df.index]
    if args.pivot_mode == "ohlc":
        pivots = zigzag_ohlc([float(x) for x in df["high"].tolist()],
                             [float(x) for x in df["low"].tolist()],
                             dates, args.threshold)
        mode_label = "ohlc high/low"
    else:
        prices = [float(x) for x in df[args.price_column].tolist()]
        pivots = zigzag(prices, dates, args.threshold)
        mode_label = args.price_column
    print(f"{len(df)} barres ({dates[0][:7]} -> {dates[-1][:7]}), mode={mode_label}.")

    if len(pivots) < 3:
        print("Pas assez de pivots — baisse --threshold.", file=sys.stderr)
        return 0
    legs = measure_legs(pivots)
    proj = project(pivots, legs)

    report = build_report(source, f"{args.interval}, {mode_label}", pivots, legs, proj)
    print()
    print(report)

    if args.json:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "symbol": args.symbol, "source": source, "interval": args.interval,
            "threshold": args.threshold, "price_column": args.price_column,
            "pivot_mode": args.pivot_mode,
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
