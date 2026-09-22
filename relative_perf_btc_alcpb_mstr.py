#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
relative_perf_btc_alcpb_mstr.py

Suivi de performance relative (base 100) entre BTC, ALCPB (Capital B,
Euronext Paris) et MSTR (Strategy Inc, Nasdaq) -- les deux entreprises
"Bitcoin Treasury" comparees a la performance du sous-jacent qu'elles
accumulent.

Base par defaut: 2024-11-05, date de lancement de la strategie "Bitcoin
Treasury Company" par The Blockchain Group (devenue Capital B / ALCPB).

Donnees: fetch_tv_ohlc (TradingView), journalier.
Symboles: INDEX:BTCUSD, EURONEXT:ALCPB, NASDAQ:MSTR.

Usage:
  python relative_perf_btc_alcpb_mstr.py                    # calcule + envoie le graphique Telegram
  python relative_perf_btc_alcpb_mstr.py --no-send           # calcule + sauvegarde le PNG local, sans envoi
  python relative_perf_btc_alcpb_mstr.py --rebase-date 2025-01-01

Le script ne donne pas de conseil financier.
"""

from __future__ import annotations

import argparse
import io
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import requests

from ichimoku_v4 import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, fetch_tv_ohlc

PARIS_TZ = ZoneInfo("Europe/Paris")

# label -> (symbole TradingView, couleur du trace)
ASSETS = [
    ("BTC", "INDEX:BTCUSD", "#f7931a"),
    ("ALCPB", "EURONEXT:ALCPB", "#1f77b4"),
    ("MSTR", "NASDAQ:MSTR", "#9467bd"),
]
ICONS = {"BTC": "\U0001f7e0", "ALCPB": "\U0001f535", "MSTR": "\U0001f7e3"}

DEFAULT_REBASE_DATE = "2024-11-05"  # lancement strategie Bitcoin Treasury (The Blockchain Group -> Capital B)
CANDLES = 1200
CHART_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "relative_perf_btc_alcpb_mstr.png")


def compute_indexed_series(rebase_date: str) -> dict[str, pd.Series]:
    rebase_ts = pd.Timestamp(rebase_date, tz="UTC")
    series: dict[str, pd.Series] = {}
    for label, symbol, _ in ASSETS:
        df = fetch_tv_ohlc(symbol, "D", CANDLES)
        if df is None or df.empty:
            print(f"{label} ({symbol}): pas de donnees, ignore")
            continue
        closes = df["close"]
        closes = closes[closes.index >= rebase_ts]
        if closes.empty:
            print(f"{label} ({symbol}): pas de donnees depuis {rebase_date}, ignore")
            continue
        base = closes.iloc[0]
        series[label] = (closes / base) * 100.0
    return series


def build_chart(series: dict[str, pd.Series], rebase_date: str) -> bytes:
    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    for label, _, color in ASSETS:
        if label not in series:
            continue
        s = series[label]
        ax.plot(s.index, s.values, label=f"{label} ({s.iloc[-1] - 100:+.1f}%)", color=color, linewidth=1.8)

    ax.axhline(100, color="grey", linewidth=0.8, linestyle="--")
    ax.set_title(f"Performance relative BTC / ALCPB / MSTR -- base 100 le {rebase_date}")
    ax.set_ylabel("Indice (base 100)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def build_caption(series: dict[str, pd.Series], rebase_date: str) -> str:
    lines = [f"\U0001f4ca BTC / ALCPB / MSTR -- base 100 le {rebase_date}"]
    ranked = sorted(series.items(), key=lambda kv: kv[1].iloc[-1], reverse=True)
    for label, s in ranked:
        icon = ICONS.get(label, "⚪")
        lines.append(f"{icon} {label}\t{s.iloc[-1] - 100:+.1f}%")
    lines.append(f"⏰ {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M')} Paris")
    return "\n".join(lines)


def send_telegram_photo(photo_bytes: bytes, caption: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram: credentials missing, skip send.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
        files = {"photo": ("relative_perf.png", photo_bytes, "image/png")}
        data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption}
        response = requests.post(url, data=data, files=files, timeout=20)
        ok = bool(response.json().get("ok", False))
        print(f"Telegram: {'sent' if ok else 'failed'}")
        return ok
    except Exception as exc:
        print(f"Telegram: send failed ({exc})")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rebase-date", default=DEFAULT_REBASE_DATE, help="date de base (YYYY-MM-DD), indice 100")
    parser.add_argument("--no-send", action="store_true", help="ne pas envoyer sur Telegram, juste sauvegarder le PNG")
    args = parser.parse_args()

    series = compute_indexed_series(args.rebase_date)
    if not series:
        print("Aucune donnee disponible, arret.")
        return

    photo_bytes = build_chart(series, args.rebase_date)
    with open(CHART_PATH, "wb") as handle:
        handle.write(photo_bytes)
    print(f"Graphique sauvegarde: {CHART_PATH}")
    for label, s in series.items():
        print(f"{label}: {s.iloc[-1] - 100:+.2f}% depuis {args.rebase_date}")

    if not args.no_send:
        send_telegram_photo(photo_bytes, build_caption(series, args.rebase_date))


if __name__ == "__main__":
    main()
