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
rouge) donne une paire des 29 ; les devises a boule grise sont exclues.
- Entree : le prix de la paire a casse son SAR H1 dans le sens de la combinaison
  sur une bougie H1 CLOTUREE depuis le run precedent : crossover si la devise forte
  est la devise de base (USDJPY avec USD fort), crossunder si c'est la devise de
  cotation (AUDUSD avec USD fort). S'il y a plusieurs bougies depuis le run
  precedent, elles sont rejouees dans l'ordre : le premier cross dans le bon sens
  dont le niveau tient encore fait entrer la paire (un cross inverse ensuite ne
  l'annule pas). Sans etat : la derniere bougie cloturee seulement.
- Niveau : la valeur du SAR H1 sur la bougie du cross est memorisee.
- Sortie : une CLOTURE H1 franche au-dela de ce niveau dans le sens inverse
  (au-dessus pour un crossunder, en dessous pour un crossover) ; une meche ne
  suffit pas. Un nouveau cross dans le meme sens ne change ni l'heure ni le niveau.
- Warning : la paire reste eligible si la combinaison forte x faible qui l'a fait
  entrer n'existe plus, mais elle porte un warning.
- Heure : entre parentheses (heure de cloture de la bougie du cross, Paris ; date
  en plus si ce n'est pas aujourd'hui) pour les paires deja eligibles avant ce
  run ; pas d'heure pour celles qui entrent a ce run.
CROSS DEPUIS 01:00 : au premier run a partir de 07:00 Paris (run precedent avant
07:00, ou pas d'etat), pour chaque combinaison forte x faible, heure de cloture Paris
du DERNIER cross H1 dans le bon sens entre 01:00 et 07:00 : les messages commencent a 7h.
Etat (`--state-file`) mis a jour uniquement avec `--telegram`.

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
WARNING_ICON = "⚠️"
H1 = pd.Timedelta(hours=1)
DAY_START_HOUR = 1   # recap du premier message du jour : crosses depuis 01:00 Paris
RECAP_HOUR = 7       # ... envoye au premier run a partir de 07:00 Paris (debut des messages)


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


def first_standing_cross(times: list, closes: list[float], sar: list[float],
                         since: pd.Timestamp | None, direction: str) -> int | None:
    """Index du premier cross prix/SAR H1 dans le sens `direction` parmi les bougies
    (cloturees) dont la cloture tombe apres `since` (sans `since` : la derniere bougie
    seulement) et dont le niveau n'a pas ete recasse depuis. Rejoue ce qu'aurait fait
    un run a chaque bougie : un cross inverse ne fait pas sortir, seul le niveau
    compte. None s'il n'y en a pas."""
    bull, bear = find_crosses(closes, sar)
    crosses = bull if direction == "BULL" else bear
    if since is None:
        first = len(times) - 1
    else:
        first = next((i for i, t in enumerate(times) if t + H1 > since), len(times))
    for i in range(max(first, 1), len(times)):
        if crosses[i] and not level_broken(times, closes, times[i], float(sar[i]), direction):
            return i
    return None


def level_broken(times: list, closes: list[float], cross_open: pd.Timestamp, level: float, direction: str) -> bool:
    """Cloture H1 franche au-dela du niveau du SAR du cross, dans le sens inverse, sur
    une bougie posterieure a celle du cross."""
    for t, close in zip(times, closes):
        if t > cross_open and (close > level if direction == "BEAR" else close < level):
            return True
    return False


def update_eligible(previous: dict[str, dict], combos: list[tuple[str, str]], h1: dict[str, tuple],
                    since: pd.Timestamp | None) -> tuple[list[dict], dict[str, dict]]:
    """Liste ELIGIBLE, avec persistance d'un run a l'autre.

    `previous` = {paire: {direction, level, cross_open}} ; `h1` = {paire: (times,
    closes, sar)} des bougies H1 cloturees (une paire absente = fetch en erreur : elle
    garde son etat). Renvoie (entrees, nouvel_etat) ; entree = {pair, direction,
    cross_open, fresh (entree a ce run), warning (combinaison disparue)}.
    """
    state: dict[str, dict] = {}
    fresh: set[str] = set()
    for pair, entry in previous.items():
        data = h1.get(pair)
        if data is not None and level_broken(data[0], data[1], pd.Timestamp(entry["cross_open"]),
                                             entry["level"], entry["direction"]):
            continue
        state[pair] = dict(entry)
    for pair, direction in combos:
        if state.get(pair, {}).get("direction") == direction or pair not in h1:
            continue
        times, closes, sar = h1[pair]
        index = first_standing_cross(times, closes, sar, since, direction)
        if index is None:
            continue
        state[pair] = dict(direction=direction, level=float(sar[index]), cross_open=times[index].isoformat())
        fresh.add(pair)
    active = set(combos)
    entries = [dict(pair=pair, direction=e["direction"], cross_open=pd.Timestamp(e["cross_open"]),
                    fresh=pair in fresh, warning=(pair, e["direction"]) not in active)
               for pair, e in sorted(state.items())]
    return entries, state


def format_cross_time(cross_open: pd.Timestamp, now: datetime) -> str:
    closed = (cross_open + H1).tz_convert(base.PARIS)
    return f"{closed:%H:%M}" if closed.date() == now.date() else f"{closed:%d/%m %H:%M}"


def day_start(now: pd.Timestamp) -> pd.Timestamp:
    """Debut de la journee de recap : 01:00 Paris du jour (de la veille avant 01:00)."""
    paris = now.tz_convert(base.PARIS)
    start = paris.normalize() + pd.Timedelta(hours=DAY_START_HOUR)
    return start if paris >= start else start - pd.Timedelta(days=1)


def first_run_of_day(since: pd.Timestamp | None, now: pd.Timestamp) -> bool:
    """Premier run a partir de 07:00 Paris : il est 07:00 passe et le run precedent date
    d'avant 07:00 (ou pas d'etat). Un run de nuit (cron GitHub retarde) ne prend pas
    le recap, qui reste pour le premier message du matin."""
    paris = now.tz_convert(base.PARIS)
    recap_from = paris.normalize() + pd.Timedelta(hours=RECAP_HOUR)
    return paris >= recap_from and (since is None or since < recap_from)


def last_night_cross(times: list, closes: list[float], sar: list[float], direction: str,
                     start: pd.Timestamp) -> pd.Timestamp | None:
    """Heure de cloture du DERNIER cross H1 dans le sens `direction` dont la bougie a
    cloture entre `start` (01:00 Paris) et 07:00 Paris le meme jour, bornes incluses.
    None s'il n'y en a pas."""
    end = start.normalize() + pd.Timedelta(hours=RECAP_HOUR)
    bull, bear = find_crosses(closes, sar)
    crosses = bull if direction == "BULL" else bear
    hits = [t + H1 for i, t in enumerate(times) if crosses[i] and start <= t + H1 <= end]
    return hits[-1] if hits else None


def build_telegram_message(rows: list[dict], eligible: list[dict] | None = None,
                           now: datetime | None = None,
                           recap: list[tuple[str, str, pd.Timestamp]] | None = None) -> str:
    now = now or datetime.now(base.PARIS)
    lines = [f"{ball(r)}{r['index']} ({format_score(r['score'])})" for r in rows]
    if eligible:
        lines += ["", "ELIGIBLE"]
        for e in eligible:
            when = "" if e["fresh"] else f" ({format_cross_time(e['cross_open'], now)})"
            lines.append(f"{ICON[e['direction']]}{e['pair']}{when}{' ' + WARNING_ICON if e['warning'] else ''}")
    if recap:
        lines += ["", f"CROSS DEPUIS {DAY_START_HOUR:02d}:00"]
        for pair, direction, closed_at in recap:
            lines.append(f"{ICON[direction]}{pair} {closed_at.tz_convert(base.PARIS):%H:%M}")
    footer = f"⏰ {now.strftime('%Y-%m-%d %H:%M')} Paris"
    return "\n".join(["\U0001f9ed INDEX SAR D", ""] + lines + ["", footer])


def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--telegram", action="store_true", help="Envoie le message Telegram et met a jour l'etat.")
    parser.add_argument("--d1-candles", type=int, default=500)
    parser.add_argument("--h1-candles", type=int, default=300)
    parser.add_argument("--state-file", type=Path, default=Path("index_sar_daily_state.json"),
                        help="Heure du run precedent et paires eligibles (niveau du SAR du cross).")
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

    saved = load_state(args.state_file)
    since = pd.Timestamp(saved["last_run"]) if saved.get("last_run") else None
    previous = saved.get("eligible", {})
    combos = combinations(rows)
    h1: dict[str, tuple] = {}
    for pair in sorted({p for p, _ in combos} | set(previous)):
        try:
            df = drop_unconfirmed(base.fetch_ohlc(pair, "60", args.h1_candles), timeframe="H")
            h1[pair] = (df["time"].tolist(), df["close"].astype(float).tolist(),
                        pine_sar(df, args.sar_start, args.sar_increment, args.sar_maximum))
        except Exception as exc:
            errors.append((pair, str(exc)))
    eligible, new_state = update_eligible(previous, combos, h1, since)

    print("\nCombinaisons forte x faible : "
          + (", ".join(f"{p} ({'crossover' if d == 'BULL' else 'crossunder'})" for p, d in combos) or "aucune"))
    print("ELIGIBLE : " + (", ".join(
        f"{e['pair']} [{'nouvelle' if e['fresh'] else format_cross_time(e['cross_open'], datetime.now(base.PARIS))}"
        f", niveau {new_state[e['pair']]['level']:.5f}]{' ' + WARNING_ICON if e['warning'] else ''}"
        for e in eligible) or "aucune"))
    dropped = sorted(set(previous) - set(new_state))
    if dropped:
        print("Sorties (niveau du SAR du cross recasse) : " + ", ".join(dropped))

    recap = None
    if first_run_of_day(since, run_time):
        start = day_start(run_time)
        recap = [(pair, direction, closed_at) for pair, direction in combos if pair in h1
                 for closed_at in [last_night_cross(*h1[pair], direction, start)] if closed_at is not None]
        print(f"Premier run du jour : dernier cross favorable entre {start:%d/%m %H:%M} et "
              f"{RECAP_HOUR:02d}:00 Paris : "
              + (", ".join(f"{p} {t.tz_convert(base.PARIS):%H:%M}" for p, _, t in recap) or "aucun"))

    if rows:
        message = build_telegram_message(rows, eligible, recap=recap)
        if args.telegram:
            print("\nTelegram :\n" + message)
            if send_telegram_message(message):
                print("  Message envoye.")
        else:
            print("\nApercu Telegram (non envoye, ajouter --telegram) :\n" + message)
    if args.telegram:
        payload = dict(last_run=run_time.isoformat(), eligible=new_state)
        args.state_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    if errors:
        print("\nErreurs :")
        for index, error in errors:
            print(f"  {index}: {error}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
