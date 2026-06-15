#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
middle_backtest.py

Backtest WALK-FORWARD de la rotation mid-cap TECH (sous-themes Semis /
Logiciels / Hardware / Distributors / IT-Internet).

Principe (sans biais de look-ahead): a chaque rebalancement (debut de mois),
on ne regarde QUE les donnees connues a cette date, on classe les sous-themes
par momentum, on detient le(s) plus fort(s) pendant le mois, puis on enchaine.

Strategies comparees:
  - Rotation Top-1  : on detient le sous-theme le plus fort
  - Rotation Top-2  : on detient les 2 sous-themes les plus forts
  - Equal-Weight    : on detient TOUTE la tech mid-cap (baseline investie)
  - IWR Buy & Hold  : reference mid-cap (achat-conservation)

Metriques: rendement total, CAGR, volatilite annualisee, Sharpe (rf=0),
max drawdown, % de mois gagnants, alpha de CAGR vs IWR. Frais modelises par
le turnover a chaque rebalancement.

LIMITE ASSUMEE — biais du survivant: l'univers = membres ACTUELS du S&P
MidCap 400 (secteur Information Technology). Les societes ejectees/delistees
dans le passe sont absentes, ce qui EMBELLIT les resultats. A lire comme un
ordre de grandeur d'edge, pas une performance realisable telle quelle.

Usage:
    python middle_backtest.py                 # 3 ans, rebal mensuel
    python middle_backtest.py --years 5 --cost-bps 10
    python middle_backtest.py --holdings-csv IWR.csv   # univers iShares exact
    python middle_backtest.py --telegram      # envoie un resume compact
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytz
from tabulate import tabulate

import middle_pipeline as mid
from nasdaq_sector_pipeline import download_history, send_telegram

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BENCHMARK = "IWR"
OUT_JSON = os.path.join(SCRIPT_DIR, "middle_backtest_report.json")
OUT_TXT = os.path.join(SCRIPT_DIR, "middle_backtest_report.txt")

# Blend de momentum (rendements glissants) — point-in-time, sans look-ahead.
LOOKBACKS = (21, 63, 126)            # ~1, 3, 6 mois de bourse
LB_WEIGHTS = (0.30, 0.45, 0.25)
MIN_HISTORY = max(LOOKBACKS) + 5     # barres minimales avant d'inclure une action
MIN_NAMES_PER_THEME = 2              # un sous-theme doit avoir >=2 actions notees


def build_price_matrix(tickers: list[str], period: str) -> pd.DataFrame:
    hist = download_history(tickers + [BENCHMARK], period=period)
    cols: dict[str, pd.Series] = {}
    for t, df in hist.items():
        if df is not None and not df.empty and "Close" in df:
            s = df["Close"].dropna()
            if not s.empty:
                cols[t] = s
    if BENCHMARK not in cols:
        raise RuntimeError(f"Pas de donnees pour le benchmark {BENCHMARK}.")
    px = pd.DataFrame(cols).sort_index()
    px.index = pd.DatetimeIndex(px.index).tz_localize(None)
    return px


def momentum_score(series: pd.Series) -> float | None:
    """Blend de rendements glissants a la derniere date de la serie fournie."""
    s = series.dropna()
    if len(s) <= MIN_HISTORY:
        return None
    score = 0.0
    last = float(s.iloc[-1])
    for lb, w in zip(LOOKBACKS, LB_WEIGHTS):
        past = float(s.iloc[-1 - lb])
        if past <= 0:
            return None
        score += w * (last / past - 1.0)
    return score


def rebalance_dates(px: pd.DataFrame) -> list[pd.Timestamp]:
    """Premier jour de bourse de chaque mois present dans l'historique."""
    s = pd.Series(px.index, index=px.index)
    firsts = s.resample("MS").first().dropna()
    return [pd.Timestamp(d) for d in firsts.tolist()]


def theme_ranking(px: pd.DataFrame, subtheme_of: dict[str, str], t: pd.Timestamp) -> list[tuple[str, float]]:
    """Classement des sous-themes a la date t (donnees <= t uniquement)."""
    upto = px.loc[:t]
    by_theme: dict[str, list[float]] = {}
    for ticker in px.columns:
        if ticker == BENCHMARK:
            continue
        sc = momentum_score(upto[ticker])
        if sc is None:
            continue
        by_theme.setdefault(subtheme_of.get(ticker, "Autre"), []).append(sc)
    ranked = [(th, float(np.mean(v))) for th, v in by_theme.items() if len(v) >= MIN_NAMES_PER_THEME]
    return sorted(ranked, key=lambda x: x[1], reverse=True)


def holdings_for(px: pd.DataFrame, subtheme_of: dict[str, str], t: pd.Timestamp,
                 top_k: int | None) -> list[str]:
    """Tickers detenus a partir de t. top_k=None -> toute la tech (baseline)."""
    have_data = [c for c in px.columns if c != BENCHMARK
                 and momentum_score(px.loc[:t, c]) is not None]
    if top_k is None:
        return have_data
    ranked = theme_ranking(px, subtheme_of, t)
    chosen = {th for th, _ in ranked[:top_k]}
    return [c for c in have_data if subtheme_of.get(c, "Autre") in chosen]


def period_return(px: pd.DataFrame, held: list[str], t0: pd.Timestamp, t1: pd.Timestamp) -> float:
    """Rendement equipondere du panier entre t0 et t1 (buy-and-hold du mois)."""
    if not held:
        return 0.0
    rets = []
    for c in held:
        p0 = px.at[t0, c] if t0 in px.index else np.nan
        p1 = px.at[t1, c] if t1 in px.index else np.nan
        if pd.notna(p0) and pd.notna(p1) and p0 > 0:
            rets.append(float(p1) / float(p0) - 1.0)
    return float(np.mean(rets)) if rets else 0.0


def turnover(prev: list[str], cur: list[str]) -> float:
    """Fraction du portefeuille modifiee (poids egaux) entre 2 rebalancements."""
    if not prev and not cur:
        return 0.0
    pw = {c: 1.0 / len(prev) for c in prev} if prev else {}
    cw = {c: 1.0 / len(cur) for c in cur} if cur else {}
    names = set(pw) | set(cw)
    return 0.5 * sum(abs(cw.get(n, 0.0) - pw.get(n, 0.0)) for n in names)


def run_strategy(px: pd.DataFrame, subtheme_of: dict[str, str], rebals: list[pd.Timestamp],
                 top_k: int | None, cost_bps: float) -> dict:
    """Renvoie equity, rendements mensuels nets et historique des choix."""
    monthly, choices, prev = [], [], []
    for i in range(len(rebals) - 1):
        t0, t1 = rebals[i], rebals[i + 1]
        held = holdings_for(px, subtheme_of, t0, top_k)
        gross = period_return(px, held, t0, t1)
        cost = (cost_bps / 10000.0) * turnover(prev, held)
        monthly.append(gross - cost)
        if top_k is not None:
            top = theme_ranking(px, subtheme_of, t0)[:top_k]
            choices.append((t0.strftime("%Y-%m"), [th for th, _ in top]))
        prev = held
    m = pd.Series(monthly, index=[r.strftime("%Y-%m") for r in rebals[1:]])
    return {"monthly": m, "equity": (1.0 + m).cumprod(), "choices": choices}


def benchmark_monthly(px: pd.DataFrame, rebals: list[pd.Timestamp]) -> pd.Series:
    vals = []
    for i in range(len(rebals) - 1):
        p0, p1 = px.at[rebals[i], BENCHMARK], px.at[rebals[i + 1], BENCHMARK]
        vals.append(float(p1) / float(p0) - 1.0 if p0 > 0 else 0.0)
    return pd.Series(vals, index=[r.strftime("%Y-%m") for r in rebals[1:]])


def metrics(monthly: pd.Series) -> dict:
    m = monthly.dropna()
    if m.empty:
        return {"total": 0, "cagr": 0, "vol": 0, "sharpe": 0, "maxdd": 0, "win": 0, "months": 0}
    eq = (1.0 + m).cumprod()
    years = len(m) / 12.0
    final = float(eq.iloc[-1])
    cagr = final ** (1.0 / years) - 1.0 if years > 0 and final > 0 else 0.0
    vol = float(m.std() * np.sqrt(12))
    sharpe = (float(m.mean()) * 12) / vol if vol > 0 else 0.0
    maxdd = float((eq / eq.cummax() - 1.0).min())
    win = float((m > 0).mean() * 100)
    return {"total": final - 1.0, "cagr": cagr, "vol": vol, "sharpe": sharpe,
            "maxdd": maxdd, "win": win, "months": int(len(m))}


def pct(x: float) -> str:
    return f"{x * 100:+.1f}%"


def build_report(results: dict, bench_m: pd.Series, choices: list, args, n_universe: int) -> str:
    paris = pytz.timezone("Europe/Paris")
    now = datetime.now(paris).strftime("%Y-%m-%d %H:%M %Z")
    bench_cagr = metrics(bench_m)["cagr"]

    order = ["Rotation Top-1", "Rotation Top-2", "Equal-Weight Tech", "IWR Buy & Hold"]
    rows = []
    for name in order:
        mo = results[name]
        mt = metrics(mo)
        rows.append([
            name, pct(mt["total"]), pct(mt["cagr"]), pct(mt["vol"]),
            f"{mt['sharpe']:.2f}", pct(mt["maxdd"]), f"{mt['win']:.0f}%",
            pct(mt["cagr"] - bench_cagr),
        ])
    table = tabulate(rows, headers=[
        "Strategie", "Total", "CAGR", "Vol", "Sharpe", "MaxDD", "Win%", "Alpha CAGR",
    ], tablefmt="github")

    months = metrics(bench_m)["months"]
    lines = [
        "MID-CAP TECH — BACKTEST ROTATION (walk-forward)",
        f"Generated: {now}",
        f"Univers: {n_universe} actions | Benchmark: {BENCHMARK} | "
        f"Periode: ~{args.years} an(s) ({months} mois) | Rebal: mensuel | Frais: {args.cost_bps:.0f} bps",
        "",
        "PERFORMANCE COMPAREE",
        table,
        "",
        "ROTATION TOP-1 — derniers choix mensuels",
        ", ".join(f"{ym}:{th[0]}" for ym, th in choices[-12:]) if choices else "—",
        "",
        "METHODE",
        "Walk-forward: a chaque debut de mois, classement des sous-themes par",
        "momentum (rendements 21/63/126j, ponderes), detention du/des plus forts",
        "le mois suivant. Aucune donnee future utilisee. Frais appliques au",
        "turnover de chaque rebalancement.",
        "",
        "⚠️ BIAIS DU SURVIVANT: univers = membres ACTUELS du S&P MidCap 400 tech.",
        "Les valeurs sorties de l'indice (souvent perdantes) sont absentes -> les",
        "performances sont surestimees. A lire comme un ORDRE DE GRANDEUR d'edge,",
        "pas une performance realisable telle quelle.",
    ]
    return "\n".join(lines)


def build_telegram(results: dict, bench_m: pd.Series, args) -> str:
    bench_cagr = metrics(bench_m)["cagr"]
    lines = ["🧪 BACKTEST MID-CAP TECH", f"(~{args.years}an, rebal mensuel)"]
    for name, short in [("Rotation Top-1", "Rot.Top1"), ("Rotation Top-2", "Rot.Top2"),
                        ("Equal-Weight Tech", "EqW Tech"), ("IWR Buy & Hold", "IWR")]:
        mt = metrics(results[name])
        a = "" if name == "IWR Buy & Hold" else f" · α {pct(mt['cagr'] - bench_cagr)}"
        lines.append(f"• {short}: CAGR {pct(mt['cagr'])} · DD {pct(mt['maxdd'])} · Sh {mt['sharpe']:.2f}{a}")
    lines += ["", "⚠️ Biais survivant: perf surestimee (ordre de grandeur)."]
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backtest walk-forward rotation mid-cap tech")
    p.add_argument("--holdings-csv", default=None, help="CSV iShares IWR (sinon S&P MidCap 400)")
    p.add_argument("--years", type=int, default=3, help="Fenetre de backtest en annees")
    p.add_argument("--cost-bps", type=float, default=10.0, help="Frais par rebalancement (bps de turnover)")
    p.add_argument("--telegram", action="store_true", help="Envoie un resume compact Telegram")
    return p.parse_args()


def main() -> int:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()

    if args.holdings_csv:
        components, _ = mid.fetch_from_ishares_csv(args.holdings_csv)
        subtheme_of = {}
        src = "iShares IWR CSV"
    else:
        print("Fetching S&P MidCap 400 (Information Technology)...")
        components = mid.fetch_sp400_tech()
        subtheme_of = {r["ticker"]: mid.classify_subtheme(r.get("sub_industry", ""))
                       for _, r in components.iterrows()}
        src = "S&P MidCap 400 (Wikipedia)"
    tickers = components["ticker"].tolist()
    print(f"Univers: {len(tickers)} actions ({src}).")

    fetch_period = "5y" if args.years > 2 else ("2y" if args.years == 2 else "1y")
    print(f"Telechargement historique ({fetch_period})...")
    px = build_price_matrix(tickers, fetch_period)

    # Trim a la fenetre demandee + classification (si Yahoo industry necessaire).
    cutoff = px.index.max() - pd.Timedelta(days=int(args.years * 365.25) + 10)
    px = px.loc[px.index >= cutoff]
    if not subtheme_of:  # univers iShares: pas de sous-industrie -> via Yahoo
        print("Profils Yahoo pour la classification...")
        profiles = mid.fetch_profiles_ex(tickers)
        subtheme_of = {t: mid.classify_subtheme("", profiles.get(t, {}).get("industry", ""))
                       for t in tickers}

    rebals = rebalance_dates(px)
    if len(rebals) < 6:
        print("ERROR: pas assez d'historique pour un backtest.", file=sys.stderr)
        return 0
    print(f"Rebalancements: {len(rebals)} mois ({rebals[0].date()} -> {rebals[-1].date()}).")

    results = {
        "Rotation Top-1": run_strategy(px, subtheme_of, rebals, 1, args.cost_bps)["monthly"],
        "Rotation Top-2": run_strategy(px, subtheme_of, rebals, 2, args.cost_bps)["monthly"],
        "Equal-Weight Tech": run_strategy(px, subtheme_of, rebals, None, args.cost_bps)["monthly"],
    }
    bench_m = benchmark_monthly(px, rebals)
    results["IWR Buy & Hold"] = bench_m
    choices = run_strategy(px, subtheme_of, rebals, 1, args.cost_bps)["choices"]

    report = build_report(results, bench_m, choices, args, len(tickers))
    print()
    print(report)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": src, "benchmark": BENCHMARK, "years": args.years,
        "cost_bps": args.cost_bps, "months": metrics(bench_m)["months"],
        "metrics": {name: metrics(mo) for name, mo in results.items()},
        "rotation_choices": choices,
        "monthly_returns": {name: {k: round(float(v), 5) for k, v in mo.items()}
                            for name, mo in results.items()},
    }
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(OUT_TXT, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\nSaved: {OUT_TXT}")
    print(f"Saved: {OUT_JSON}")

    if args.telegram:
        send_telegram(build_telegram(results, bench_m, args))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
