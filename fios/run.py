#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FIOS — message unique : confluence de la force des devises (FIOS) avec
l'alignement RENKO M/W/D des index de devises (FULL ALIGNMENT).

FIOS calcule une force composite par devise (fondamental FRED + technique
TradingView + COT + sentiment retail + correlations). Le script FULL ALIGNMENT
(renko_full_alignment_29pairs.py) ecrit un sidecar avec l'alignement Renko
M/W/D des index (DXY/EXY/...). Ce script croise les deux et envoie la seule
section confluence sur Telegram.

Usage:
    python -m fios.run                 # calcul + envoi Telegram
    python -m fios.run --no-telegram   # affichage seul
    python -m fios.run --verbose       # detail par famille
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import pytz
import requests
from dotenv import load_dotenv

from . import config as cfg
from . import cross_check
from . import tv_feed
from .adapters import correlations as corr_adapter
from .adapters import cot as cot_adapter
from .adapters import fundamental_fred as fund_adapter
from .adapters import retail as retail_adapter
from .adapters import technical as tech_adapter
from .adapters.base import FamilyResult
from .scoring import currency as cur_mod

load_dotenv()
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
except Exception:
    pass

PARIS = pytz.timezone("Europe/Paris")


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
    p = argparse.ArgumentParser(description="FIOS — confluence force devises x RENKO M/W/D")
    p.add_argument("--no-telegram", action="store_true", help="Ne pas envoyer sur Telegram")
    p.add_argument("--no-fund", action="store_true", help="Ignorer le fondamental FRED")
    p.add_argument("--no-cot", action="store_true", help="Ignorer le COT")
    p.add_argument("--no-retail", action="store_true", help="Ignorer le sentiment retail")
    p.add_argument("--no-corr", action="store_true", help="Ignorer les correlations")
    p.add_argument("--verbose", action="store_true", help="Detail par famille")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    print("FIOS — collecte des forces devises...")

    tech = tech_adapter.run(verbose=args.verbose)
    cot = FamilyResult("sentiment", False) if args.no_cot else cot_adapter.run(verbose=args.verbose)
    corr = FamilyResult("correlation", False) if args.no_corr else corr_adapter.run(verbose=args.verbose)
    fund = FamilyResult("fundamental", False) if args.no_fund else fund_adapter.run(verbose=args.verbose)
    retail = FamilyResult("retail", False) if args.no_retail else retail_adapter.run(verbose=args.verbose)

    families = [fund, tech, cot, retail, corr]
    for f in families:
        state = "OK" if f.available else f"inactif — {f.note}"
        print(f"   [{f.name}] {state}")

    composites = cur_mod.currency_composites(families)

    # --- Confluence avec le sidecar RENKO M/W/D (FULL ALIGNMENT) ---
    align = cross_check.load_align()
    if not align:
        print("Sidecar FULL ALIGNMENT absent/perime — aucun message "
              "(le script renko_full_alignment doit tourner avant FIOS).")
        tv_feed.clear_cache()
        return
    section = cross_check.build_section(composites, align)
    if not section:
        print("Aucune confluence a afficher aujourd'hui.")
        tv_feed.clear_cache()
        return

    now = datetime.now(PARIS).strftime("%d/%m/%Y %H:%M")
    message = "\n".join(section) + f"\n\n⏰ {now} Paris"
    print("\n" + message)

    if not args.no_telegram:
        send_telegram(message)
        print("\n✅ Message Telegram envoye.")

    tv_feed.clear_cache()


if __name__ == "__main__":
    main()
