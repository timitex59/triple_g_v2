#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orchestrateur FIOS (Phase 1).

Lance chaque adaptateur (technique + COT + correlations + fondamental FRED),
fusionne les familles disponibles en un composite par devise, calcule les
signaux de confluence par paire, affiche/journalise le tout et envoie le
message Telegram.

Usage:
    python -m fios.run                 # calcul + envoi Telegram
    python -m fios.run --no-telegram   # console seulement
    python -m fios.run --verbose       # detail par adaptateur
    python -m fios.run --limit 6       # limite le nb de paires (tests rapides)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

from . import config as cfg
from . import cross_check
from . import journal as journal_mod
from . import llm_explain
from . import regime as regime_mod
from . import report as report_mod
from . import trade_plan
from . import tv_feed
from .adapters import correlations as corr_adapter
from .adapters import cot as cot_adapter
from .adapters import fundamental_fred as fund_adapter
from .adapters import retail as retail_adapter
from .adapters import technical as tech_adapter
from .adapters.base import FamilyResult
from .backtest import journal as bt_journal
from .scoring import confluence as conf_mod
from .scoring import currency as cur_mod

load_dotenv()

# Windows: console cp1252 par defaut -> les emojis/barres plantent l'affichage.
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
except Exception:
    pass

PARIS = report_mod.PARIS


def send_telegram(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram env manquant (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for i in range(0, len(text), 3800):
        try:
            requests.post(url, json={"chat_id": chat_id, "text": text[i:i + 3800]}, timeout=15)
        except Exception as e:
            print(f"Echec envoi Telegram: {e}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="FIOS — moteur de confluence Forex (Phase 1)")
    p.add_argument("--no-telegram", action="store_true", help="Ne pas envoyer sur Telegram")
    p.add_argument("--no-cot", action="store_true", help="Ignorer l'adaptateur COT")
    p.add_argument("--no-retail", action="store_true", help="Ignorer le sentiment retail (OANDA/Myfxbook)")
    p.add_argument("--no-fund", action="store_true", help="Ignorer le fondamental FRED")
    p.add_argument("--no-corr", action="store_true", help="Ignorer les correlations")
    p.add_argument("--no-llm", action="store_true", help="Ignorer la note du desk (LLM)")
    p.add_argument("--no-cross", action="store_true", help="Ignorer la confluence FULL ALIGNMENT")
    p.add_argument("--no-backtest", action="store_true", help="Ne pas denouer le journal (stats)")
    p.add_argument("--limit", type=int, default=0, help="Limiter le nb de paires (0 = toutes)")
    p.add_argument("--top", type=int, default=5, help="Nb de signaux dans le message")
    p.add_argument("--verbose", action="store_true", help="Detail par adaptateur")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    pairs = cfg.PAIRS[: args.limit] if args.limit else cfg.PAIRS

    print("FIOS — collecte des familles de signaux...")

    # --- Technique (TradingView) — toujours actif, socle du systeme. ---
    print("• Technique (TradingView)...")
    tech = tech_adapter.run(pairs=pairs, verbose=args.verbose)

    # --- Sentiment / COT ---
    if args.no_cot:
        cot = FamilyResult("sentiment", False, note="desactive (--no-cot)")
    else:
        print("• Sentiment (COT CFTC)...")
        cot = cot_adapter.run(verbose=args.verbose)

    # --- Sentiment retail (OANDA + Myfxbook), contrarien ---
    if args.no_retail:
        retail = FamilyResult("retail", False, note="desactive (--no-retail)")
    else:
        print("• Sentiment retail (OANDA + Myfxbook)...")
        retail = retail_adapter.run(pairs=pairs, verbose=args.verbose)

    # --- Correlations (TradingView) ---
    if args.no_corr:
        corr = FamilyResult("correlation", False, note="desactive (--no-corr)")
    else:
        print("• Correlations inter-marches...")
        corr = corr_adapter.run(verbose=args.verbose)

    # --- Fondamental (FRED) ---
    if args.no_fund:
        fund = FamilyResult("fundamental", False, note="desactive (--no-fund)")
    else:
        print("• Fondamental (FRED)...")
        fund = fund_adapter.run(verbose=args.verbose)

    families: dict[str, FamilyResult] = {
        "fundamental": fund, "technical": tech, "sentiment": cot,
        "retail": retail, "correlation": corr,
    }
    for name, fam in families.items():
        state = "OK" if fam.available else f"inactif — {fam.note}"
        print(f"   [{name}] {state}")

    # --- Fusion + confluence ---
    composites = cur_mod.currency_composites(list(families.values()))
    ranking_rows = cur_mod.ranking(composites)
    # Familles a score par paire (confirmation directe sur la paire).
    pair_score_maps: dict[str, dict[str, float]] = {}
    if tech.available:
        pair_score_maps["technical"] = tech.details.get("pairs", {})
    if retail.available:
        pair_score_maps["retail"] = retail.details.get("pairs", {})
    signals = conf_mod.evaluate_all(families, pair_score_maps, pairs=pairs)

    # --- Sorties console ---
    print()
    print(report_mod.build_console(ranking_rows, signals, families))
    print()

    # --- Journal ---
    paris_date = datetime.now(PARIS).strftime("%Y-%m-%d")
    data = journal_mod.append_run(cfg.JOURNAL_FILE, paris_date, signals)
    journal_mod.save(cfg.JOURNAL_FILE, data)

    # --- Denouement des signaux murus + snapshot stats (Phase 2) ---
    if not args.no_backtest:
        stats = bt_journal.resolve_and_snapshot(cfg.JOURNAL_FILE, cfg.STATS_FILE)
        print(f"Journal: {stats['resolved_trades']} trades denoues, "
              f"{stats['open_signals']} ouverts")

    # --- Snapshot JSON auditable (transparence complete) ---
    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "paris_date": paris_date,
        "families_available": {n: f.available for n, f in families.items()},
        "confluence_weights": cfg.CONFLUENCE_WEIGHTS,
        "currency_ranking": [{"currency": c, "composite": s} for c, s in ranking_rows],
        # Decomposition de chaque force devise : score composite + parts par famille.
        "composites": composites,
        # Detail fondamental : drivers FRED orientes par devise (pourquoi ce score).
        "fundamental_drivers": (fund.details.get("drivers", {}) if fund.available else {}),
        # Detail retail : % long par source (OANDA / Myfxbook) par paire.
        "retail_detail": (retail.details.get("long_pct", {}) if retail.available else {}),
        "signals": [
            {
                "pair": s.pair, "decision": s.decision, "net": s.net,
                "confidence": s.confidence, "coherence": s.coherence,
                "premium": s.premium, "quality": s.quality,
                "contributions": s.contributions, "agree": s.agree, "families": s.families,
            }
            for s in signals
        ],
    }
    with open(cfg.REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    # --- Confluence FULL ALIGNMENT x FIOS ---
    cross_section = None
    if not args.no_cross:
        align = cross_check.load_align()
        if align:
            cross_section = cross_check.build_section(composites, align)
            print(f"• Confluence FULL ALIGNMENT: {len(cross_section)} lignes")
        else:
            print("• Confluence FULL ALIGNMENT: sidecar absent/perime — ignore")

    # --- Note du desk (LLM) ---
    desk_note = None
    if not args.no_llm:
        print("• Note du desk (LLM)...")
        desk_note = llm_explain.desk_note(ranking_rows, signals, families)
        print("   " + ("note generee" if desk_note else "indisponible — repli gabarit"))

    # --- Fraîcheur des donnees par famille ---
    freshness = {"technical": "live", "correlation": "live", "retail": "live",
                 "fundamental": "macro"}
    cot = families.get("sentiment")
    if cot and cot.available:
        det = cot.details.get("cot", {})
        rd = next((v.get("report_date") for v in det.values() if v.get("report_date")), None)
        try:
            days = (datetime.now(timezone.utc).date()
                    - datetime.fromisoformat(rd).date()).days if rd else None
            freshness["sentiment"] = f"{days}j" if days is not None else "COT"
        except Exception:
            freshness["sentiment"] = "COT"

    # --- Plans de trade + contexte (etat marche, regime par paire) ---
    pairs_by_name = {p["name"]: p for p in pairs}
    trade_plans = trade_plan.plans_for(signals, pairs_by_name, args.top)
    market = regime_mod.market_state(corr)
    regimes = regime_mod.regimes_for(signals, pairs_by_name, args.top)
    fund_drivers = fund.details.get("drivers", {}) if fund.available else {}

    # --- Telegram ---
    message = report_mod.build_message(
        ranking_rows, signals, families, top_n=args.top,
        desk_note=desk_note, cross_section=cross_section, composites=composites,
        freshness=freshness, trade_plans=trade_plans,
        market=market, regimes=regimes, fundamental_drivers=fund_drivers,
    )
    print(message)
    if not args.no_telegram:
        send_telegram(message)
        print("\n✅ Message Telegram envoye.")

    tv_feed.clear_cache()


if __name__ == "__main__":
    main()
