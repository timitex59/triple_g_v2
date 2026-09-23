#!/usr/bin/env python3
"""Position des 8 indices devises (TVC) par rapport a leur SAR DAILY.

- BULL : prix de l'indice AU-DESSUS du SAR daily ;
- BEAR : prix de l'indice EN DESSOUS du SAR daily.

Bougie daily en cours incluse (prix live), comme le graphique TradingView :
le SAR compare est celui de la derniere bougie. SAR = pine_sar (ta.sar),
parametres par defaut 0.1 / 0.1 / 0.2 comme les autres scripts du repo.

Exemples :
    python index_sar_daily.py               # tableau console + apercu Telegram
    python index_sar_daily.py --telegram    # envoi Telegram
"""
from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime

import imp_trend_29pairs as base
from imp_trend5_29pairs import pine_sar, send_telegram_message

INDICES = ["DXY", "EXY", "BXY", "JXY", "SXY", "CXY", "AXY", "ZXY"]
ICON = {"BULL": "\U0001f7e2", "BEAR": "\U0001f534", "NEUTRE": "⚪"}


def sar_position(df, start: float, increment: float, maximum: float) -> dict:
    sar = pine_sar(df, start, increment, maximum)[-1]
    price = float(df["close"].iloc[-1])
    if math.isnan(sar) or price == sar:
        verdict = "NEUTRE"
    else:
        verdict = "BULL" if price > sar else "BEAR"
    return dict(price=price, sar=sar, verdict=verdict,
                dist=(price - sar) / sar * 100 if sar and not math.isnan(sar) else math.nan)


def build_telegram_message(rows: list[dict], now: datetime | None = None) -> str:
    lines = [f"{r['index']}\t{ICON[r['verdict']]}" for r in rows]
    footer = f"⏰ {(now or datetime.now(base.PARIS)).strftime('%Y-%m-%d %H:%M')} Paris"
    return "\n".join(["\U0001f9ed INDEX SAR D", ""] + lines + ["", footer])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--telegram", action="store_true", help="Envoie le message Telegram.")
    parser.add_argument("--d1-candles", type=int, default=500)
    parser.add_argument("--sar-start", type=float, default=0.1)
    parser.add_argument("--sar-increment", type=float, default=0.1)
    parser.add_argument("--sar-maximum", type=float, default=0.2)
    args = parser.parse_args()

    rows, errors = [], []
    for index in INDICES:
        try:
            df = base.fetch_ohlc(index, "D", args.d1_candles, tv_symbol=f"TVC:{index}")
            rows.append(dict(index=index, **sar_position(df, args.sar_start, args.sar_increment, args.sar_maximum)))
        except Exception as exc:
            errors.append((index, str(exc)))

    print(f"Indices devises vs SAR daily au {datetime.now(base.PARIS):%Y-%m-%d %H:%M} Paris\n")
    print(f"{'INDEX':<6} {'':2}  {'prix':>10}  {'SAR D':>10}  {'dist':>7}")
    for r in rows:
        print(f"{r['index']:<6} {ICON[r['verdict']]}  {r['price']:>10.3f}  {r['sar']:>10.3f}  {r['dist']:>+6.2f}%")
    for verdict in ("BULL", "BEAR"):
        names = [r["index"] for r in rows if r["verdict"] == verdict]
        print(f"\n{ICON[verdict]} {verdict} : {', '.join(names) or 'aucun'}")

    if rows:
        message = build_telegram_message(rows)
        if args.telegram:
            print("\nTelegram :\n" + message)
            if send_telegram_message(message):
                print("  Message envoye.")
        else:
            print("\nApercu Telegram (non envoye, ajouter --telegram) :\n" + message)
    if errors:
        print("\nErreurs :")
        for index, error in errors:
            print(f"  {index}: {error}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
