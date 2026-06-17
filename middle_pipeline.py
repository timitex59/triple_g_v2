#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
middle_pipeline.py

Variante MID-CAP / secteur TECHNOLOGIE du nasdaq_sector_pipeline.

Au lieu du Nasdaq-100 (mega-cap), l'univers est constitue d'actions MID-CAP
appartenant uniquement au secteur "Information Technology", puis passe dans la
meme machinerie d'analyse (momentum, force relative, sous-themes, news, filtre
Renko 3M/M/W).

Source de l'univers (par ordre de priorite):
  1. --holdings-csv PATH : un CSV de holdings iShares telecharge manuellement
     depuis https://www.ishares.com/us/products/239763/ (Russell Mid-Cap ETF,
     IWR). On filtre Asset Class = Equity et Sector = Information Technology.
  2. Par defaut : la liste du S&P MidCap 400 (Wikipedia), filtree sur
     GICS Sector = Information Technology. C'est l'univers mid-cap US standard,
     accessible programmatiquement (iShares bloque le telechargement direct).

Le script ne donne pas de conseil financier: il construit un tableau de bord de
force relative mid-cap tech.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import pytz
import requests
import yfinance as yf
from bs4 import BeautifulSoup

import nasdaq_sector_pipeline as nsp
from nasdaq_sector_pipeline import (
    USER_AGENT,
    AssetMetrics,
    build_asset_metrics,
    build_theme_scores,
    download_history,
    emerging_shift_themes,
    fetch_news_for_theme,
    format_pct,
    retained_assets_ranked,
    run_renko_filter,
    send_telegram,
    table_analytic_classification,
    table_assets,
    table_global_retained_ranking,
    table_renko,
    table_theme_scores,
    top_stocks_by_theme,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SP400_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
BENCHMARK = "IWR"  # iShares Russell Mid-Cap ETF — reference mid-cap
REPORT_JSON = os.path.join(SCRIPT_DIR, "middle_pipeline_report.json")
REPORT_TXT = os.path.join(SCRIPT_DIR, "middle_pipeline_report.txt")
TRACKER_PATH = os.path.join(SCRIPT_DIR, "middle_pipeline_tracker.json")
TRACK_TOP_N = 5       # vivier suivi = EXACTEMENT le TOP STOCKS affiche (il faut
                      # avoir atteint le TOP pour etre suivi / devenir durable)
SWITCH_CONFIRM_DAYS = 3  # jours ou un nouveau #1 doit tenir pour confirmer une bascule

# Codes d'echange Yahoo -> exchange TradingView (pour le scan Renko).
YF_EXCH_TV = {
    "NMS": "NASDAQ", "NGM": "NASDAQ", "NCM": "NASDAQ", "NSI": "NASDAQ",
    "NYQ": "NYSE", "NYS": "NYSE", "NGS": "NASDAQ",
    "ASE": "AMEX", "PCX": "AMEX", "BTS": "AMEX",
}

# Rempli a l'execution: {ticker: "NASDAQ"/"NYSE"/...} pour stock_tv_symbol.
TICKER_EXCHANGE: dict[str, str] = {}


def _ishares_exchange_to_tv(raw: str) -> str:
    blob = (raw or "").lower()
    if "nasdaq" in blob:
        return "NASDAQ"
    if "new york stock exchange" in blob or blob.strip() == "nyse":
        return "NYSE"
    if "arca" in blob or "cboe" in blob or "bats" in blob or "mkt" in blob:
        return "AMEX"
    return "NASDAQ"


def midcap_tv_symbol(ticker: str) -> str:
    """Symbole TradingView mid-cap, exchange resolu via TICKER_EXCHANGE."""
    return f"{TICKER_EXCHANGE.get(ticker, 'NASDAQ')}:{ticker}"


# Sous-industrie GICS de chaque action (depuis le S&P 400), pour la classification.
TICKER_SUBIND: dict[str, str] = {}


def classify_subtheme(sub_industry: str, yahoo_industry: str = "") -> str:
    """Mappe la sous-industrie GICS (12 sous-secteurs Info Tech) sur 5 themes
    parlants: Semis / Logiciels / Hardware / Distributors / IT / Internet."""
    blob = f"{sub_industry} {yahoo_industry}".lower()
    if "semiconductor" in blob:
        return "Semis"
    if "software" in blob:
        return "Logiciels"
    if "distributor" in blob:
        return "Distributors"
    if ("it consulting" in blob or "it services" in blob or "internet" in blob
            or "information technology services" in blob
            or "internet services" in blob):
        return "IT / Internet"
    # Communications Equipment, Hardware/Storage/Peripherals, Electronic
    # Equipment/Components, Manufacturing Services, Instruments...
    return "Hardware"


def midcap_infer_theme(ticker: str, sector: str, industry: str) -> str:
    """Remplace nsp.infer_theme: classe par sous-industrie GICS (sinon Yahoo)."""
    return classify_subtheme(TICKER_SUBIND.get(ticker, ""), industry)


# Mots-cles d'actualite par sous-theme (pour des requetes news pertinentes).
MIDCAP_THEME_KEYWORDS = {
    "Semis": ["semiconductor", "chip", "foundry", "wafer", "semiconductor equipment"],
    "Logiciels": ["software", "saas", "application software", "cloud software"],
    "Hardware": ["electronic components", "networking", "communications equipment", "hardware"],
    "Distributors": ["technology distributor", "it distribution", "electronics distribution"],
    "IT / Internet": ["it services", "internet infrastructure", "it consulting", "cloud infrastructure"],
}


def fetch_from_ishares_csv(path: str) -> tuple[pd.DataFrame, dict[str, str]]:
    """Lit un CSV de holdings iShares (IWR) telecharge manuellement.
    Filtre Equity + Information Technology. Renvoie (DataFrame, exchange_map)."""
    with open(path, encoding="utf-8-sig") as f:
        raw = f.read().splitlines()
    header = None
    for i, line in enumerate(raw):
        if line.replace('"', "").startswith("Ticker,Name"):
            header = i
            break
    if header is None:
        raise RuntimeError(f"En-tete 'Ticker,Name' introuvable dans {path}")
    df = pd.read_csv(StringIO("\n".join(raw[header:])))
    df.columns = [str(c).strip() for c in df.columns]
    if "Asset Class" in df.columns:
        df = df[df["Asset Class"].astype(str).str.contains("Equity", case=False, na=False)]
    sector_col = next((c for c in df.columns if c.lower() == "sector"), None)
    if sector_col:
        df = df[df[sector_col].astype(str).str.strip().str.lower() == "information technology"]
    rows, ex_map = [], {}
    for _, r in df.iterrows():
        ticker = str(r.get("Ticker", "")).strip().replace(".", "-")
        if not re.match(r"^[A-Z][A-Z0-9-]{0,6}$", ticker):
            continue
        rows.append({"ticker": ticker, "name": str(r.get("Name", ticker)).strip()})
        ex_map[ticker] = _ishares_exchange_to_tv(str(r.get("Exchange", "")))
    out = pd.DataFrame(rows).drop_duplicates("ticker").reset_index(drop=True)
    return out, ex_map


def fetch_sp400_tech() -> pd.DataFrame:
    """Composants S&P MidCap 400 (Wikipedia), filtres GICS Sector = Info Tech."""
    resp = requests.get(SP400_URL, timeout=30, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for table in soup.select("table.wikitable"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [c.get_text(" ", strip=True).lower() for c in rows[0].find_all(["th", "td"])]
        if "symbol" not in headers or not any("sector" in h for h in headers):
            continue
        sym_col = headers.index("symbol")
        sec_col = next(i for i, h in enumerate(headers) if "sector" in h)
        sub_col = next((i for i, h in enumerate(headers) if "sub-industry" in h), None)
        name_col = next((i for i, h in enumerate(headers) if "security" in h or "company" in h), 1)
        parsed = []
        for row in rows[1:]:
            cells = [c.get_text(" ", strip=True) for c in row.find_all(["th", "td"])]
            if len(cells) <= max(sym_col, sec_col, name_col):
                continue
            if cells[sec_col].strip().lower() != "information technology":
                continue
            ticker = cells[sym_col].replace(".", "-").strip()
            if not re.match(r"^[A-Z][A-Z0-9-]{0,6}$", ticker):
                continue
            parsed.append({
                "ticker": ticker,
                "name": cells[name_col].strip(),
                "sub_industry": cells[sub_col].strip() if sub_col is not None and len(cells) > sub_col else "",
            })
        if parsed:
            return pd.DataFrame(parsed).drop_duplicates("ticker").reset_index(drop=True)
    raise RuntimeError("Table S&P MidCap 400 introuvable sur Wikipedia")


def fetch_profiles_ex(tickers: list[str], max_workers: int = 12) -> dict[str, dict[str, str]]:
    """Profils Yahoo + code d'echange -> exchange TradingView (pour Renko)."""
    sess = nsp._YF_SESSION

    def one(ticker: str) -> tuple[str, dict[str, str]]:
        try:
            tk = yf.Ticker(ticker, session=sess) if sess is not None else yf.Ticker(ticker)
            info = tk.get_info()
            return ticker, {
                "sector": str(info.get("sector") or ""),
                "industry": str(info.get("industry") or ""),
                "shortName": str(info.get("shortName") or info.get("longName") or ""),
                "market_cap": str(info.get("marketCap") or ""),
                "exchange_tv": YF_EXCH_TV.get(str(info.get("exchange") or ""), "NASDAQ"),
            }
        except Exception:
            return ticker, {"sector": "", "industry": "", "shortName": "", "market_cap": "", "exchange_tv": "NASDAQ"}

    profiles: dict[str, dict[str, str]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(one, t) for t in tickers]
        for fut in as_completed(futures):
            ticker, profile = fut.result()
            profiles[ticker] = profile
    return profiles


def _load_tracker() -> dict:
    try:
        with open(TRACKER_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_tracker(state: dict) -> None:
    try:
        with open(TRACKER_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def update_tracker(ranking: list[dict], theme_scores=None,
                   top_n: int = TRACK_TOP_N) -> dict:
    """Suivi de persistance du vivier TOP (1x/jour, idempotent intra-journee).

    Le vivier suit EXACTEMENT le meme classement que le TOP STOCKS (validees
    Renko d'abord, voir unified_ranking) -> ce qui est affiche = ce qui est
    suivi. Maintient par ticker: streak, jours cumules, rang, force relative.
    Detecte: entrants, valeurs qui faiblissent, sorties. Calcule aussi le
    score de PERSISTANCE PONDERE (streak x force) et l'ALERTE DE BASCULE.
    """
    today = datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%d")
    state = _load_tracker()
    tickers_state: dict[str, dict] = state.get("tickers", {})

    current = ranking[:top_n]
    cur_set = {e["ticker"] for e in current}

    # Idempotent: si deja calcule aujourd'hui (CI tourne plusieurs fois/soir),
    # on renvoie les evenements du jour sans muter l'historique.
    if state.get("updated") == today:
        return state.get("last_events", {})

    prev_update = state.get("updated")  # date de la derniere mise a jour
    prev_active = {t for t, i in tickers_state.items() if i.get("active")}
    events: dict = {"persistent": [], "new": [], "weakening": [], "dropped": []}

    for rank, e in enumerate(current, 1):
        ticker = e["ticker"]
        info = tickers_state.get(ticker, {})
        was_in = bool(info.get("active")) and info.get("last_seen") == prev_update
        rel = round(float(e["rel"] or 0.0), 2)
        prev_rel = info.get("last_rel")
        prev_rank = info.get("last_rank")
        streak = (info.get("streak", 0) + 1) if was_in else 1
        if not was_in:
            events["new"].append(ticker)
        elif prev_rel is not None and (rel < prev_rel - 1.0 or (prev_rank and rank > prev_rank)):
            events["weakening"].append({
                "ticker": ticker, "rank": rank, "prev_rank": prev_rank,
                "rel": rel, "prev_rel": prev_rel,
            })
        tickers_state[ticker] = {
            "streak": streak,
            "days": info.get("days", 0) + 1,
            "first_seen": info.get("first_seen", today),
            "last_seen": today,
            "last_rank": rank,
            "peak_rank": min(rank, info.get("peak_rank", rank)),
            "last_rel": rel,
            "active": True,
        }

    for t in prev_active - cur_set:
        tickers_state[t]["active"] = False
        tickers_state[t]["dropped_on"] = today
        events["dropped"].append(t)

    events["persistent"] = sorted(
        ([e["ticker"], tickers_state[e["ticker"]]["streak"]] for e in current),
        key=lambda x: -x[1],
    )[:5]

    # Score de persistance PONDERE = streak x force relative (longevite ET force).
    events["durable"] = sorted(
        ([t, tickers_state[t]["streak"], tickers_state[t]["last_rel"],
          round(tickers_state[t]["streak"] * tickers_state[t]["last_rel"], 1)]
         for t in cur_set),
        key=lambda x: -x[3],
    )[:5]

    # ALERTE DE BASCULE: le sous-theme #1 change et tient >= SWITCH_CONFIRM_DAYS.
    top_theme = theme_scores[0].theme if theme_scores else None
    leader_raw = state.get("leader_raw")
    leader_days = int(state.get("leader_raw_days", 0))
    confirmed = state.get("leader_confirmed")
    switch = None
    if top_theme:
        if top_theme == leader_raw:
            leader_days += 1
        else:
            leader_raw, leader_days = top_theme, 1
        if top_theme != confirmed and leader_days >= SWITCH_CONFIRM_DAYS:
            if confirmed is not None:                       # 1re etablie en silence
                switch = {"from": confirmed, "to": top_theme, "days": leader_days}
            confirmed = top_theme
    events["switch"] = switch

    state.update({"updated": today, "prev_update": prev_update,
                  "tickers": tickers_state, "last_events": events,
                  "leader_raw": leader_raw, "leader_raw_days": leader_days,
                  "leader_confirmed": confirmed})
    _save_tracker(state)
    return events


def tracker_lines(ev: dict) -> list[str]:
    """Lignes Telegram du suivi de persistance (compactes)."""
    if not ev:
        return []
    out: list[str] = []
    if ev.get("durable"):
        out.append("💎 Durables")
        for t, s, rel, _ in ev["durable"][:3]:
            out.append(f"{t} {s}j · {rel:+.0f}%")
    if ev.get("new"):
        out.append("🆕 Entrants: " + ", ".join(ev["new"][:3]))
    if ev.get("weakening"):
        out.append("📉 Faiblissent: " + ", ".join(w["ticker"] for w in ev["weakening"][:3]))
    if ev.get("dropped"):
        out.append("❌ Sortis: " + ", ".join(ev["dropped"][:3]))
    return out


def unified_ranking(stock_metrics: list[AssetMetrics], renko: dict) -> list[dict]:
    """Classement UNIQUE servant a la fois au TOP STOCKS et au vivier suivi.

    Ordre: actions validees Renko d'abord (par score combine), puis le reste
    par force relative. Garantit la coherence: ce qui est affiche = ce qui est
    suivi (une valeur du TOP est forcement dans le vivier)."""
    order: list[dict] = []
    seen: set[str] = set()
    for _, c, a in retained_assets_ranked(renko, stock_metrics, []):
        if c.asset_type != "STOCK" or a is None or c.ticker in seen:
            continue
        order.append({"ticker": c.ticker, "rel": a.rel_63d_vs_qqq, "validated": True})
        seen.add(c.ticker)
    rest = sorted(
        [m for m in stock_metrics if m.ticker not in seen and m.rel_63d_vs_qqq is not None],
        key=lambda m: (m.rel_63d_vs_qqq or 0.0, m.score), reverse=True,
    )
    for m in rest:
        order.append({"ticker": m.ticker, "rel": m.rel_63d_vs_qqq, "validated": False})
    return order


def build_telegram_summary(
    theme_scores,
    ranking: list[dict],
    events: dict | None = None,
) -> str:
    lines = ["🇺🇸 MID-CAP TECH"]
    if theme_scores:
        top = ", ".join(s.theme for s in theme_scores[:2])
        lines.append(f"🔥 {top}")
    if events and events.get("switch"):
        sw = events["switch"]
        lines.append(f"🔄 BASCULE: {sw['from']} → {sw['to']} ({sw['days']}j)")

    lines.append("")
    lines.append("📈 TOP STOCKS (vs IWR)")
    if ranking:
        for i, e in enumerate(ranking[:TRACK_TOP_N], 1):
            tag = "" if e["validated"] else " ⏳"
            lines.append(f"{i}. {e['ticker']} ({format_pct(e['rel'])}){tag}")
    else:
        lines.append("---")

    tl = tracker_lines(events or {})
    if tl:
        lines.append("")
        lines.append("🛰️ SUIVI")
        lines.extend(tl)

    stamp = datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
    lines += ["", f"⏰ {stamp} Paris"]
    return "\n".join(lines)


def build_report(theme_scores, stock_metrics, news, renko, args, events=None) -> str:
    paris = pytz.timezone("Europe/Paris")
    now = datetime.now(paris).strftime("%Y-%m-%d %H:%M %Z")
    top_themes = [s.theme for s in theme_scores[:args.top_themes]]
    leaders = top_stocks_by_theme(stock_metrics, top_themes, args.top_stocks)

    lines = [
        "MID-CAP TECH PIPELINE (S&P MidCap 400 — Information Technology)",
        f"Generated: {now}",
        f"Universe: {len(stock_metrics)} mid-cap tech stocks | Benchmark: {BENCHMARK} | "
        f"News window: {args.news_days} days",
        "",
        "DOMINANT SUB-THEMES",
        table_theme_scores(theme_scores, args.top_themes),
    ]
    if events:
        lines += ["", "SUIVI DE PERSISTANCE (vivier TOP)"]
        if events.get("switch"):
            sw = events["switch"]
            lines.append(f"🔄 BASCULE confirmee: {sw['from']} -> {sw['to']} ({sw['days']}j de #1)")
        if events.get("durable"):
            lines.append("Durables (streak x force): "
                         + ", ".join(f"{t} [{s}j, Rel {rel:+.0f}%, score {sc:.0f}]"
                                     for t, s, rel, sc in events["durable"]))
        if events.get("new"):
            lines.append("Entrants: " + ", ".join(events["new"]))
        if events.get("weakening"):
            lines.append("Faiblissent: " + ", ".join(
                f"{w['ticker']} (rang {w['prev_rank']}→{w['rank']}, Rel {w['prev_rel']:+.0f}→{w['rel']:+.0f})"
                for w in events["weakening"]))
        if events.get("dropped"):
            lines.append("Sortis du TOP: " + ", ".join(events["dropped"]))
    lines += [
        "",
        "TOP STOCKS BY SUB-THEME",
    ]
    for theme in top_themes:
        lines += ["", theme, table_assets(leaders.get(theme, []), args.top_stocks)]

    if renko:
        validated = sorted(
            [c for c in renko.values() if c.final_action == "VALIDATED"],
            key=lambda c: (c.theme, c.ticker),
        )
        watchlist = sorted(
            [c for c in renko.values() if c.final_action == "WATCHLIST"],
            key=lambda c: (c.theme, c.ticker),
        )
        rejected = sorted(
            [c for c in renko.values() if c.final_action == "REJECTED"],
            key=lambda c: (c.theme, c.ticker),
        )
        lines += [
            "",
            "FINAL SELECTION - RENKO VALIDATED",
            table_renko(validated) if validated else "No candidate fully validated by Renko.",
            "",
            "GLOBAL RANKING - RETAINED STOCKS",
            table_global_retained_ranking(renko, stock_metrics, [], None) if validated else "No retained asset.",
            "",
            "ANALYTIC CLASSIFICATION",
            table_analytic_classification(renko, stock_metrics, []) if validated else "No retained asset.",
            "",
            "WATCHLIST - GOOD THEME, WAITING FOR RENKO",
            table_renko(watchlist, args.renko_watchlist_limit) if watchlist else "No watchlist candidate.",
            "",
            "REJECTED BY RENKO",
            table_renko(rejected, args.renko_watchlist_limit) if rejected else "None.",
        ]
    else:
        lines += ["", "RENKO FILTER", "Skipped (--no-renko)."]

    lines += ["", "NEWS VALIDATION"]
    for theme in top_themes:
        n = news.get(theme, {})
        lines.append("")
        lines.append(f"{theme}: score={n.get('score', 0)} hits={n.get('hits', 0)} sources={n.get('sources', 0)}")
        for item in n.get("entries", [])[:args.news_items]:
            date = item.get("published", "")[:10]
            lines.append(f"- {date} | {item.get('source', 'Unknown')} | {item.get('title', '')}")

    lines += [
        "",
        "METHOD",
        "Univers = actions MID-CAP (S&P 400) du seul secteur Information Technology.",
        "Score action = momentum 21D/63D/126D + force relative vs IWR + volume + SMA50/200 + high 20D.",
        "Sous-themes = semis / logiciels / cybersecurite / etc. au sein de la tech mid-cap.",
        "Selection finale = sous-theme fort + action forte + Renko 3M/M/W bullish (ROC14/ROC21 > 0).",
        "Radar de force relative mid-cap tech, pas un signal d'achat automatique.",
    ]
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mid-cap technology sector rotation pipeline")
    p.add_argument("--holdings-csv", default=None, help="CSV iShares IWR (sinon: S&P MidCap 400 Wikipedia)")
    p.add_argument("--max-stocks", type=int, default=0, help="Limite l'univers (0 = tout)")
    p.add_argument("--top-themes", type=int, default=3)
    p.add_argument("--top-stocks", type=int, default=3)
    p.add_argument("--news-days", type=int, default=14)
    p.add_argument("--news-items", type=int, default=2)
    p.add_argument("--period", default="9mo")
    p.add_argument("--no-news", action="store_true")
    p.add_argument("--no-renko", action="store_true")
    p.add_argument("--renko-length", type=int, default=14)
    p.add_argument("--renko-workers", type=int, default=1)
    p.add_argument("--renko-candidates-per-theme", type=int, default=3)
    p.add_argument("--renko-watchlist-limit", type=int, default=20)
    p.add_argument("--renko-debug", action="store_true")
    p.add_argument("--json", default=REPORT_JSON)
    p.add_argument("--txt", default=REPORT_TXT)
    p.add_argument("--telegram", action="store_true", default=True)
    p.add_argument("--no-telegram", action="store_false", dest="telegram")
    p.add_argument("--telegram-full", action="store_true")
    return p.parse_args()


def main() -> int:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()
    t0 = time.time()

    ishares_ex_map: dict[str, str] = {}
    if args.holdings_csv:
        print(f"Lecture holdings iShares: {args.holdings_csv}")
        components, ishares_ex_map = fetch_from_ishares_csv(args.holdings_csv)
        src = "iShares IWR CSV"
    else:
        print("Fetching S&P MidCap 400 (Information Technology)...")
        components = fetch_sp400_tech()
        src = "S&P MidCap 400 (Wikipedia)"

    if args.max_stocks and len(components) > args.max_stocks:
        components = components.head(args.max_stocks).reset_index(drop=True)
    tickers = components["ticker"].tolist()
    print(f"Loaded {len(tickers)} mid-cap tech stocks from {src}.")
    if not tickers:
        print("ERROR: univers vide.", file=sys.stderr)
        return 0

    print(f"Downloading price history (benchmark {BENCHMARK})...")
    histories = download_history(tickers + [BENCHMARK], period=args.period)
    bench_close = histories.get(BENCHMARK, pd.DataFrame()).get("Close", pd.Series(dtype=float))

    covered = sum(1 for t in tickers if t in histories)
    min_required = max(10, len(tickers) // 2)
    if bench_close.empty or covered < min_required:
        print(
            f"ERROR: donnees insuffisantes ({BENCHMARK} vide={bench_close.empty}, "
            f"couverts={covered}/{len(tickers)}, min={min_required}). "
            f"Cause probable: rate limit Yahoo. Abandon sans ecrire ni envoyer.",
            file=sys.stderr,
        )
        return 0

    print("Fetching Yahoo profiles (sector/industry/exchange)...")
    profiles = fetch_profiles_ex(tickers)

    # Resolution exchange pour le scan Renko (iShares CSV prioritaire, sinon Yahoo).
    global TICKER_EXCHANGE, TICKER_SUBIND
    TICKER_EXCHANGE = {t: ishares_ex_map.get(t) or profiles.get(t, {}).get("exchange_tv", "NASDAQ")
                       for t in tickers}
    nsp.stock_tv_symbol = midcap_tv_symbol  # monkeypatch: run_renko_filter l'utilise

    # Classification par sous-industrie GICS (Semis/Logiciels/Hardware/...).
    if "sub_industry" in components.columns:
        TICKER_SUBIND = dict(zip(components["ticker"], components["sub_industry"]))
    nsp.infer_theme = midcap_infer_theme            # monkeypatch: build_asset_metrics l'utilise
    nsp.THEME_KEYWORDS.update(MIDCAP_THEME_KEYWORDS)  # requetes news pertinentes

    print("Computing stock and sub-theme metrics...")
    stock_histories = {k: v for k, v in histories.items() if k != BENCHMARK}
    stock_metrics = build_asset_metrics(components, profiles, stock_histories, bench_close)

    preliminary = pd.DataFrame([asdict(m) for m in stock_metrics])
    candidate_themes = []
    if not preliminary.empty:
        candidate_themes = (
            preliminary.groupby("theme")["score"].mean()
            .sort_values(ascending=False).head(max(args.top_themes + 3, 6)).index.tolist()
        )

    news: dict = {}
    if args.no_news:
        news = {t: {"theme": t, "score": 0, "hits": 0, "sources": 0, "entries": []} for t in candidate_themes}
    else:
        print("Validating sub-themes with recent news...")
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(fetch_news_for_theme, t, args.news_days): t for t in candidate_themes}
            for fut in as_completed(futures):
                item = fut.result()
                news[item["theme"]] = item

    theme_scores = build_theme_scores(stock_metrics, news)
    top_themes = [s.theme for s in theme_scores[:args.top_themes]]
    renko_theme_pool = list(dict.fromkeys(top_themes + emerging_shift_themes(theme_scores)))

    renko: dict = {}
    if args.no_renko:
        print("Skipping Renko validation.")
    else:
        print("Applying Renko filter to top mid-cap tech stocks...")
        candidates: list[AssetMetrics] = []
        for _, rows in top_stocks_by_theme(stock_metrics, renko_theme_pool, args.renko_candidates_per_theme).items():
            candidates.extend(rows)
        renko = run_renko_filter(candidates, [], atr_length=args.renko_length,
                                 max_workers=args.renko_workers, debug=args.renko_debug)

    ranking = unified_ranking(stock_metrics, renko)
    events = update_tracker(ranking, theme_scores)

    report = build_report(theme_scores, stock_metrics, news, renko, args, events)
    print()
    print(report)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_seconds": round(time.time() - t0, 2),
        "source": src,
        "benchmark": BENCHMARK,
        "themes": [asdict(s) for s in theme_scores],
        "stocks": [asdict(m) for m in sorted(stock_metrics, key=lambda x: x.score, reverse=True)],
        "renko": {k: asdict(v) for k, v in renko.items()},
        "news": news,
    }
    with open(args.json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(args.txt, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\nSaved: {args.txt}")
    print(f"Saved: {args.json}")

    text = report if args.telegram_full else build_telegram_summary(theme_scores, ranking, events)
    print("\nTELEGRAM SUMMARY:\n" + text)
    if args.telegram:
        send_telegram(text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
