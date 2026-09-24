#!/usr/bin/env python3
"""Position des 8 indices devises (TVC) par rapport a leur SAR DAILY.

- BULL : prix de l'indice AU-DESSUS du SAR daily ;
- BEAR : prix de l'indice EN DESSOUS du SAR daily.
+ CHG% daily : (prix live - close de la veille) / close de la veille.
+ SCORE : |dist x CHG%D|, positif si dist et CHG%D sont tous deux positifs, negatif sinon.
+ boule grise (⚪) quand dist et CHG%D sont de signes opposes.

Bougie daily en cours incluse (prix live), comme le graphique TradingView :
le SAR compare est celui de la derniere bougie. SAR = pine_sar (ta.sar),
parametres par defaut 0.1 / 0.1 / 0.2 comme les autres scripts du repo.

ELIGIBLE : chaque combinaison devise forte (boule verte) x devise faible (boule
rouge) donne une paire des 29 ; les devises a boule grise sont exclues. La paire
est ELIGIBLE si son prix a casse son SAR H1 dans le sens de la combinaison sur une
bougie H1 CLOTUREE depuis le run precedent : crossover si la devise forte est la
devise de base (USDJPY avec USD fort), crossunder si c'est la devise de cotation
(AUDUSD avec USD fort). S'il y a plusieurs crosses dans la fenetre, le dernier
compte. Fenetre = bougies H1 cloturees apres le run precedent (`--state-file`, mis
a jour uniquement avec `--telegram`) ; sans etat : la derniere bougie cloturee.

Exemples :
    python index_sar_daily.py               # tableau console + apercu Telegram
    python index_sar_daily.py --telegram    # envoi Telegram
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

import imp_trend_29pairs as base
from imp_early_imp_triangles import drop_unconfirmed, find_crosses
from imp_trend5_29pairs import pine_sar, send_telegram_message

# Indice TVC -> devise affichee (plus lisible que le sigle de l'indice).
INDICES = {"DXY": "USD", "EXY": "EUR", "BXY": "GBP", "JXY": "JPY",
           "SXY": "CHF", "CXY": "CAD", "AXY": "AUD", "ZXY": "NZD"}
ICON = {"BULL": "\U0001f7e2", "BEAR": "\U0001f534", "NEUTRE": "⚪"}


def sar_position(df, start: float, increment: float, maximum: float) -> dict:
    sar = pine_sar(df, start, increment, maximum)[-1]
    price = float(df["close"].iloc[-1])
    if math.isnan(sar) or price == sar:
        verdict = "NEUTRE"
    else:
        verdict = "BULL" if price > sar else "BEAR"
    prev_close = float(df["close"].iloc[-2]) if len(df) > 1 else math.nan
    chg = (price - prev_close) / prev_close * 100 if prev_close else math.nan
    dist = (price - sar) / sar * 100 if sar and not math.isnan(sar) else math.nan
    return dict(price=price, sar=sar, verdict=verdict, chg=chg, dist=dist, score=score(dist, chg))


def score(dist: float, chg: float) -> float:
    """|dist x CHG%D|, positif seulement si les deux sont positifs ; negatif s'ils sont
    de signe contraire ou tous les deux negatifs."""
    magnitude = abs(dist * chg)
    return magnitude if dist > 0 and chg > 0 else -magnitude


def ball(row: dict) -> str:
    """Boule grise quand dist et CHG%D sont de signes opposes, sinon celle du verdict SAR."""
    return ICON["NEUTRE"] if row["dist"] * row["chg"] < 0 else ICON[row["verdict"]]


def format_chg(chg: float) -> str:
    return "n/a" if math.isnan(chg) else f"{chg:+.2f}%"


def format_score(value: float) -> str:
    return "n/a" if math.isnan(value) else f"{value:+.2f}"


def combinations(rows: list[dict]) -> list[tuple[str, str]]:
    """[(paire, sens attendu)] pour chaque devise forte (verte) x devise faible (rouge) :
    BULL (crossover) si la forte est la devise de base, BEAR (crossunder) sinon."""
    strong = [r["index"] for r in rows if ball(r) == ICON["BULL"]]
    weak = [r["index"] for r in rows if ball(r) == ICON["BEAR"]]
    combos = []
    for s in strong:
        for w in weak:
            if s + w in base.PAIRS_29:
                combos.append((s + w, "BULL"))
            elif w + s in base.PAIRS_29:
                combos.append((w + s, "BEAR"))
    return sorted(combos)


def h1_cross_since(times: list, closes: list[float], sar: list[float], since: pd.Timestamp | None) -> str | None:
    """Sens ("BULL"/"BEAR") du dernier cross prix/SAR H1 parmi les bougies (cloturees)
    dont la cloture tombe apres `since` ; sans `since`, la derniere bougie seulement.
    None s'il n'y en a pas."""
    bull, bear = find_crosses(closes, sar)
    if since is None:
        first = len(times) - 1
    else:
        first = next((i for i, t in enumerate(times) if t + pd.Timedelta(hours=1) > since), len(times))
    event = None
    for i in range(max(first, 1), len(times)):
        if bull[i]:
            event = "BULL"
        elif bear[i]:
            event = "BEAR"
    return event


def build_telegram_message(rows: list[dict], eligible: list[tuple[str, str]] | None = None,
                           now: datetime | None = None) -> str:
    lines = [f"{ball(r)}{r['index']} ({format_score(r['score'])})" for r in rows]
    if eligible:
        lines += ["", "ELIGIBLE"] + [f"{ICON[direction]}{pair}" for pair, direction in eligible]
    footer = f"⏰ {(now or datetime.now(base.PARIS)).strftime('%Y-%m-%d %H:%M')} Paris"
    return "\n".join(["\U0001f9ed INDEX SAR D", ""] + lines + ["", footer])


def load_last_run(path: Path) -> pd.Timestamp | None:
    try:
        return pd.Timestamp(json.loads(path.read_text(encoding="utf-8"))["last_run"])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--telegram", action="store_true", help="Envoie le message Telegram et met a jour l'etat.")
    parser.add_argument("--d1-candles", type=int, default=500)
    parser.add_argument("--h1-candles", type=int, default=300)
    parser.add_argument("--state-file", type=Path, default=Path("index_sar_daily_state.json"),
                        help="Heure du run precedent (debut de la fenetre des crosses H1).")
    parser.add_argument("--sar-start", type=float, default=0.1)
    parser.add_argument("--sar-increment", type=float, default=0.1)
    parser.add_argument("--sar-maximum", type=float, default=0.2)
    args = parser.parse_args()

    run_time = pd.Timestamp.now(tz="UTC")
    rows, errors = [], []
    for index, currency in INDICES.items():
        try:
            df = base.fetch_ohlc(index, "D", args.d1_candles, tv_symbol=f"TVC:{index}")
            rows.append(dict(index=currency, **sar_position(df, args.sar_start, args.sar_increment, args.sar_maximum)))
        except Exception as exc:
            errors.append((f"{currency} ({index})", str(exc)))
    # Du plus grand SCORE au plus faible ; SCORE inconnu en dernier.
    rows.sort(key=lambda r: (math.isnan(r["score"]), -r["score"] if not math.isnan(r["score"]) else 0))

    print(f"Indices devises vs SAR daily au {datetime.now(base.PARIS):%Y-%m-%d %H:%M} Paris\n")
    print(f"{'INDEX':<6} {'':2}  {'prix':>10}  {'SAR D':>10}  {'dist':>7}  {'CHG%D':>7}  {'SCORE':>6}")
    for r in rows:
        print(f"{r['index']:<6} {ball(r)}  {r['price']:>10.3f}  {r['sar']:>10.3f}  {r['dist']:>+6.2f}%"
              f"  {format_chg(r['chg']):>7}  {format_score(r['score']):>6}")
    for verdict in ("BULL", "BEAR"):
        names = [r["index"] for r in rows if r["verdict"] == verdict]
        print(f"\n{ICON[verdict]} {verdict} : {', '.join(names) or 'aucun'}")

    since = load_last_run(args.state_file)
    print(f"\nCombinaisons forte x faible, cross SAR H1 depuis "
          f"{since.tz_convert(base.PARIS):%Y-%m-%d %H:%M} Paris :" if since is not None
          else "\nCombinaisons forte x faible, cross SAR H1 sur la derniere bougie cloturee :")
    eligible = []
    for pair, direction in combinations(rows):
        try:
            h1 = drop_unconfirmed(base.fetch_ohlc(pair, "60", args.h1_candles), timeframe="H")
            sar = pine_sar(h1, args.sar_start, args.sar_increment, args.sar_maximum)
            event = h1_cross_since(h1["time"].tolist(), h1["close"].astype(float).tolist(), sar, since)
        except Exception as exc:
            errors.append((pair, str(exc)))
            continue
        expected = "crossover " if direction == "BULL" else "crossunder"
        print(f"  {pair:<7} attendu {expected}  cross H1 : {event or '-'}")
        if event == direction:
            eligible.append((pair, direction))
    print(f"\nELIGIBLE : {', '.join(p for p, _ in eligible) or 'aucune'}")

    if rows:
        message = build_telegram_message(rows, eligible)
        if args.telegram:
            print("\nTelegram :\n" + message)
            if send_telegram_message(message):
                print("  Message envoye.")
        else:
            print("\nApercu Telegram (non envoye, ajouter --telegram) :\n" + message)
    if args.telegram:
        args.state_file.write_text(json.dumps(dict(last_run=run_time.isoformat()), indent=2), encoding="utf-8")
    if errors:
        print("\nErreurs :")
        for index, error in errors:
            print(f"  {index}: {error}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
