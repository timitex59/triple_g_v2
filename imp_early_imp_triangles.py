#!/usr/bin/env python3
"""Early IMP triangles: port Python de imp_early_imp_triangles.pine.

Signal "Daily Early IMP" : la 1ere bougie Daily de la bonne couleur qui suit
un cross SAR Daily.
- triangle vert (BULL) : 1ere bougie verte (close > open) apres un cross bull
  du SAR (close passe au-dessus du SAR) ;
- triangle rouge (BEAR) : 1ere bougie rouge (close < open) apres un cross bear.

Fidele au Pine, y compris l'ordre d'evaluation par bougie :
1. le declenchement est teste AVANT que la bougie ne (ré)arme l'attente -- la
   bougie de cross elle-meme ne declenche donc jamais, c'est la suivante ;
2. un cross de sens oppose annule l'attente en cours ;
3. le signal est le front montant de l'etat (`early and not early[1]`).

No-repaint : seules les bougies Daily cloturees comptent (equivalent du
`barstate.isconfirmed` du Pine) -- la bougie en cours est ecartee.

Dates : elles suivent l'etiquette TradingView d'une bougie D1 forex, c'est-a-dire
le jour de CLOTURE (la bougie "lundi" ouvre le dimanche soir), en heure de Paris.

Exemples :
    python imp_early_imp_triangles.py CHFJPY
    python imp_early_imp_triangles.py CHFJPY EURUSD --last 10
    python imp_early_imp_triangles.py --days 3        # 29 paires, signaux des 3 dernieres bougies
"""
from __future__ import annotations

import argparse
import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

import pandas as pd

import imp_trend_29pairs as base
from imp_trend5_29pairs import pine_sar

KIND_LABEL = {"BULL": "\U0001f7e2 BULL ▲", "BEAR": "\U0001f534 BEAR ▼"}


def drop_unconfirmed(df: pd.DataFrame, now: pd.Timestamp | None = None) -> pd.DataFrame:
    """Ecarte la derniere bougie si elle est encore en formation.

    Une bougie D1 forex ouvre a `time` et dure 24h : elle n'est confirmee que
    quand `time + 24h <= now`. Contrairement a un `iloc[:-1]` systematique,
    ca garde la bougie du vendredi quand on regarde le week-end.
    """
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    if len(df) and df["time"].iloc[-1] + pd.Timedelta(days=1) > now:
        df = df.iloc[:-1]
    return df.reset_index(drop=True)


def find_crosses(closes: list[float], sar: list[float]) -> tuple[list[bool], list[bool]]:
    """ta.crossover / ta.crossunder(close, sar), bougie par bougie."""
    n = len(closes)
    bull, bear = [False] * n, [False] * n
    for i in range(1, n):
        if math.isnan(sar[i]) or math.isnan(sar[i - 1]):
            continue
        if closes[i - 1] <= sar[i - 1] and closes[i] > sar[i]:
            bull[i] = True
        if closes[i - 1] >= sar[i - 1] and closes[i] < sar[i]:
            bear[i] = True
    return bull, bear


def early_imp_signals(
    opens: list[float], closes: list[float], bull_cross: list[bool], bear_cross: list[bool],
) -> tuple[list[dict], dict | None]:
    """Machine a etats de f_daily_early_imp().

    Renvoie (signaux, arme) : `signaux` = [{kind, index, cross_index}] (index de
    la bougie du triangle et de la bougie de cross qui l'a arme) ; `arme` = etat
    d'attente courant {kind, cross_index} ou None.
    """
    wait_bull = wait_bear = False
    bull_cross_idx = bear_cross_idx = None
    prev_early_bull = prev_early_bear = False
    signals: list[dict] = []
    for i in range(len(closes)):
        green, red = closes[i] > opens[i], closes[i] < opens[i]
        early_bull = early_bear = False
        if wait_bull and green:
            early_bull, wait_bull = True, False
        if wait_bear and red:
            early_bear, wait_bear = True, False
        if early_bull and not prev_early_bull:
            signals.append(dict(kind="BULL", index=i, cross_index=bull_cross_idx))
        if early_bear and not prev_early_bear:
            signals.append(dict(kind="BEAR", index=i, cross_index=bear_cross_idx))
        prev_early_bull, prev_early_bear = early_bull, early_bear
        if bull_cross[i]:
            wait_bull, wait_bear, bull_cross_idx = True, False, i
        if bear_cross[i]:
            wait_bear, wait_bull, bear_cross_idx = True, False, i
    armed = None
    if wait_bull:
        armed = dict(kind="BULL", cross_index=bull_cross_idx)
    elif wait_bear:
        armed = dict(kind="BEAR", cross_index=bear_cross_idx)
    return signals, armed


def day_label(timestamp: pd.Timestamp) -> str:
    """Jour de cloture Paris d'une bougie D1 forex (= etiquette TradingView)."""
    return (timestamp.tz_convert(base.PARIS) + timedelta(hours=12)).strftime("%Y-%m-%d")


def analyze_pair(pair: str, args) -> dict:
    df = drop_unconfirmed(base.fetch_ohlc(pair, "D", args.d1_candles))
    if len(df) < 3:
        raise ValueError("Historique D1 insuffisant")
    sar = pine_sar(df, args.sar_start, args.sar_increment, args.sar_maximum)
    opens = df["open"].astype(float).tolist()
    closes = df["close"].astype(float).tolist()
    times = df["time"].tolist()
    bull, bear = find_crosses(closes, sar)
    signals, armed = early_imp_signals(opens, closes, bull, bear)

    rows = []
    for signal in signals:
        i, ci = signal["index"], signal["cross_index"]
        rows.append(dict(
            kind=signal["kind"], date=day_label(times[i]), index=i,
            cross_date=day_label(times[ci]), bars_after_cross=i - ci,
            open=opens[i], close=closes[i], cross_sar=sar[ci],
        ))
    if args.days:
        rows = [r for r in rows if r["index"] >= len(df) - args.days]
    rows = rows[-args.last:]
    armed_info = None
    if armed is not None:
        armed_info = dict(kind=armed["kind"], cross_date=day_label(times[armed["cross_index"]]))
    return dict(pair=pair, last_candle=day_label(times[-1]), signals=rows, armed=armed_info)


def decimals(pair: str) -> int:
    return 3 if pair.endswith("JPY") else 2 if pair == "XAUUSD" else 5


def print_result(result: dict) -> None:
    pair, d = result["pair"], decimals(result["pair"])
    print(f"\n{pair}  (derniere bougie D1 cloturee : {result['last_candle']})")
    armed = result["armed"]
    if armed:
        color = "verte" if armed["kind"] == "BULL" else "rouge"
        print(f"  En attente : {KIND_LABEL[armed['kind']]} arme depuis le cross du {armed['cross_date']} "
              f"(1ere bougie {color} attendue)")
    else:
        print("  En attente : rien")
    if not result["signals"]:
        print("  Aucun triangle sur la periode demandee.")
        return
    for r in result["signals"]:
        print(f"  {r['date']}  {KIND_LABEL[r['kind']]}   cross {r['cross_date']} (+{r['bars_after_cross']} bougie(s))   "
              f"open {r['open']:.{d}f}  close {r['close']:.{d}f}   SAR au cross {r['cross_sar']:.{d}f}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pairs", nargs="*", type=str.upper, help="Paires (defaut : les 29).")
    parser.add_argument("--last", type=int, default=5, help="Nombre de triangles les plus recents affiches par paire.")
    parser.add_argument("--days", type=int, default=0,
                        help="Ne garder que les triangles des N dernieres bougies D1 cloturees (0 = pas de filtre).")
    parser.add_argument("--d1-candles", type=int, default=2500)
    parser.add_argument("--sar-start", type=float, default=0.1)
    parser.add_argument("--sar-increment", type=float, default=0.1)
    parser.add_argument("--sar-maximum", type=float, default=0.2)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--stagger", type=float, default=0.4,
                        help="Delai (s) entre deux soumissions au pool, pour eviter les 429 TradingView.")
    args = parser.parse_args()
    unknown = [p for p in args.pairs if p not in base.PAIRS_29]
    if unknown:
        parser.error(f"Paire(s) inconnue(s) : {', '.join(unknown)}")
    if args.last < 1 or args.days < 0 or args.d1_candles < 5 or args.workers < 1 or args.stagger < 0 \
            or min(args.sar_start, args.sar_increment, args.sar_maximum) <= 0:
        parser.error("Parametres invalides (last >= 1, days >= 0, d1-candles >= 5, workers >= 1, stagger >= 0, SAR > 0)")
    return args


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()
    pairs = list(dict.fromkeys(args.pairs or base.PAIRS_29))

    results, errors = {}, []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {}
        for i, pair in enumerate(pairs):
            if i:
                time.sleep(args.stagger)
            futures[pool.submit(analyze_pair, pair, args)] = pair
        for future in as_completed(futures):
            pair = futures[future]
            try:
                results[pair] = future.result()
            except Exception as exc:
                errors.append((pair, str(exc)))

    for pair in pairs:
        if pair in results and (results[pair]["signals"] or not args.days):
            print_result(results[pair])
    if args.days and not any(results[p]["signals"] for p in results):
        print(f"\nAucun triangle sur les {args.days} derniere(s) bougie(s) D1 cloturee(s).")
    if errors:
        print("\nErreurs :")
        for pair, error in errors:
            print(f"  {pair}: {error}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
