#!/usr/bin/env python3
"""Early IMP triangles: port Python de imp_early_imp_triangles.pine, multi-UT.

Signal "Early IMP" : la 1ere bougie de la bonne couleur qui suit un cross SAR.
- triangle vert (BULL) : 1ere bougie verte (close > open) apres un cross bull
  du SAR (close passe au-dessus du SAR) ;
- triangle rouge (BEAR) : 1ere bougie rouge (close < open) apres un cross bear.

Fidele au Pine, y compris l'ordre d'evaluation par bougie :
1. le declenchement est teste AVANT que la bougie ne (ré)arme l'attente -- la
   bougie de cross elle-meme ne declenche donc jamais, c'est la suivante ;
2. un cross de sens oppose annule l'attente en cours ;
3. le signal est le front montant de l'etat (`early and not early[1]`).

Multi-UT (Daily, Weekly, Monthly) : chaque UT est calculee sur SES PROPRES
bougies (SAR de l'UT, couleur de la bougie de l'UT). Ce n'est pas ce que fait
le Pine tel qu'il est ecrit : son `request.security(..., "D", ...)` est code en
dur sur Daily, donc applique sur un graphique W ou M il ne fait que reporter les
signaux DAILY sur la bougie W/M qui les contient (et seulement s'ils tombent sur
le dernier jour de la bougie) -- avec, sur la bougie en cours, un triangle qui
peut disparaitre a la bougie Daily suivante.

No-repaint : seules les bougies CLOTUREES comptent (equivalent du
`barstate.isconfirmed`) -- une bougie D est confirmee 24h apres son ouverture,
une W apres 5 jours (vendredi 17h New York), une M apres la derniere session du
mois. Un triangle W/M n'apparait donc qu'a la cloture de la semaine/du mois et ne
disparait jamais ensuite.

Dates (heure de Paris, etiquette TradingView d'une bougie forex = jour de
CLOTURE de sa 1ere session, la bougie "lundi" ouvre le dimanche soir) :
D = jour, W = lundi de la semaine, M = mois.

Exemples :
    python imp_early_imp_triangles.py CHFJPY
    python imp_early_imp_triangles.py XAUUSD --timeframes W M --last 5
    python imp_early_imp_triangles.py --recent 1     # 29 paires : triangles de la derniere bougie de chaque UT
"""
from __future__ import annotations

import argparse
import calendar
import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

import imp_trend_29pairs as base
from imp_trend5_29pairs import pine_sar

NEW_YORK = ZoneInfo("America/New_York")
TIMEFRAMES = ("D", "W", "M")
TIMEFRAME_NAME = {"D": "Daily", "W": "Weekly", "M": "Monthly"}
KIND_LABEL = {"BULL": "\U0001f7e2 BULL ▲", "BEAR": "\U0001f534 BEAR ▼"}


def period_label(timestamp: pd.Timestamp, timeframe: str = "D") -> str:
    """Etiquette d'une bougie : jour de cloture de sa 1ere session (heure de Paris)."""
    closing_day = timestamp.tz_convert(base.PARIS) + timedelta(hours=12)
    return closing_day.strftime("%Y-%m" if timeframe == "M" else "%Y-%m-%d")


def period_end(open_time: pd.Timestamp, timeframe: str = "D") -> pd.Timestamp:
    """Instant de cloture d'une bougie forex ouverte a `open_time`.

    H : 1h. D : 24h. W : 5 jours (dimanche 17h New York -> vendredi 17h New York).
    M : 17h New York du dernier jour ouvre du mois de la bougie.
    """
    if timeframe == "H":
        return open_time + pd.Timedelta(hours=1)
    if timeframe == "D":
        return open_time + pd.Timedelta(days=1)
    if timeframe == "W":
        return open_time + pd.Timedelta(days=5)
    if timeframe == "M":
        closing_day = open_time.tz_convert(base.PARIS) + timedelta(hours=12)
        last = date(closing_day.year, closing_day.month, calendar.monthrange(closing_day.year, closing_day.month)[1])
        while last.weekday() >= 5:
            last -= timedelta(days=1)
        return pd.Timestamp(datetime(last.year, last.month, last.day, 17, tzinfo=NEW_YORK)).tz_convert("UTC")
    raise ValueError(f"UT inconnue : {timeframe}")


def drop_unconfirmed(df: pd.DataFrame, now: pd.Timestamp | None = None, timeframe: str = "D") -> pd.DataFrame:
    """Ecarte la derniere bougie si elle est encore en formation.

    Contrairement a un `iloc[:-1]` systematique, ca garde la derniere bougie
    quand elle vient de se cloturer (ex. la semaine du vendredi, le week-end).
    """
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    if len(df) and period_end(df["time"].iloc[-1], timeframe) > now:
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


def compute_signals(pair: str, timeframe: str, args) -> dict:
    """Bougies confirmees d'une UT + tous ses triangles (non tronques).

    `live_price` = dernier close brut du fetch, bougie en cours incluse : c'est
    le prix actuel, alors que signaux/SAR ne portent que sur les bougies cloturees.
    """
    candles = {"D": args.d1_candles, "W": args.w1_candles, "M": args.m1_candles}[timeframe]
    raw = base.fetch_ohlc(pair, timeframe, candles)
    live_price = float(raw["close"].iloc[-1])
    prev_close = float(raw["close"].iloc[-2]) if len(raw) >= 2 else None
    df = drop_unconfirmed(raw, timeframe=timeframe)
    if len(df) < 3:
        raise ValueError(f"Historique {timeframe} insuffisant")
    sar = pine_sar(df, args.sar_start, args.sar_increment, args.sar_maximum)
    opens = df["open"].astype(float).tolist()
    closes = df["close"].astype(float).tolist()
    bull, bear = find_crosses(closes, sar)
    signals, armed = early_imp_signals(opens, closes, bull, bear)
    return dict(times=df["time"].tolist(), opens=opens, closes=closes, sar=sar,
                signals=signals, armed=armed, live_price=live_price, prev_close=prev_close)


def analyze_timeframe(pair: str, timeframe: str, args) -> dict:
    computed = compute_signals(pair, timeframe, args)
    times, opens, closes, sar = computed["times"], computed["opens"], computed["closes"], computed["sar"]
    signals, armed = computed["signals"], computed["armed"]

    rows = []
    for signal in signals:
        i, ci = signal["index"], signal["cross_index"]
        rows.append(dict(
            kind=signal["kind"], date=period_label(times[i], timeframe), index=i,
            cross_date=period_label(times[ci], timeframe), bars_after_cross=i - ci,
            open=opens[i], close=closes[i], cross_sar=sar[ci],
        ))
    if args.recent:
        rows = [r for r in rows if r["index"] >= len(times) - args.recent]
    rows = rows[-args.last:]
    armed_info = None
    if armed is not None:
        armed_info = dict(kind=armed["kind"], cross_date=period_label(times[armed["cross_index"]], timeframe))
    return dict(last_candle=period_label(times[-1], timeframe), signals=rows, armed=armed_info)


def analyze_pair(pair: str, args) -> dict:
    return dict(pair=pair, timeframes={tf: analyze_timeframe(pair, tf, args) for tf in args.timeframes})


def decimals(pair: str) -> int:
    return 3 if pair.endswith("JPY") else 2 if pair == "XAUUSD" else 5


def print_result(result: dict, show_empty: bool) -> None:
    pair, d = result["pair"], decimals(result["pair"])
    print(f"\n{pair}")
    for tf, reading in result["timeframes"].items():
        if not show_empty and not reading["signals"]:
            continue
        armed = reading["armed"]
        if armed:
            color = "verte" if armed["kind"] == "BULL" else "rouge"
            waiting = f"{KIND_LABEL[armed['kind']]} arme (cross {armed['cross_date']}, 1ere bougie {color} attendue)"
        else:
            waiting = "rien"
        print(f"  {TIMEFRAME_NAME[tf]:<7} (derniere bougie cloturee : {reading['last_candle']})  en attente : {waiting}")
        if not reading["signals"]:
            print("    Aucun triangle sur la periode demandee.")
        for r in reading["signals"]:
            print(f"    {r['date']}  {KIND_LABEL[r['kind']]}   cross {r['cross_date']} (+{r['bars_after_cross']} bougie(s))   "
                  f"open {r['open']:.{d}f}  close {r['close']:.{d}f}   SAR au cross {r['cross_sar']:.{d}f}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pairs", nargs="*", type=str.upper, help="Paires (defaut : les 29).")
    parser.add_argument("--timeframes", nargs="+", type=str.upper, choices=TIMEFRAMES, default=list(TIMEFRAMES),
                        help="UT a analyser (defaut : D W M).")
    parser.add_argument("--last", type=int, default=3, help="Nombre de triangles les plus recents affiches par paire et par UT.")
    parser.add_argument("--recent", type=int, default=0,
                        help="Ne garder que les triangles des N dernieres bougies cloturees de chaque UT (0 = pas de filtre).")
    parser.add_argument("--d1-candles", type=int, default=2500)
    parser.add_argument("--w1-candles", type=int, default=1500)
    parser.add_argument("--m1-candles", type=int, default=500)
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
    args.timeframes = list(dict.fromkeys(args.timeframes))
    if args.last < 1 or args.recent < 0 or min(args.d1_candles, args.w1_candles, args.m1_candles) < 5 \
            or args.workers < 1 or args.stagger < 0 or min(args.sar_start, args.sar_increment, args.sar_maximum) <= 0:
        parser.error("Parametres invalides (last >= 1, recent >= 0, candles >= 5, workers >= 1, stagger >= 0, SAR > 0)")
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

    printed = False
    for pair in pairs:
        if pair not in results:
            continue
        has_signal = any(r["signals"] for r in results[pair]["timeframes"].values())
        if has_signal or not args.recent:
            print_result(results[pair], show_empty=not args.recent)
            printed = True
    if args.recent and not printed:
        print(f"\nAucun triangle sur la/les {args.recent} derniere(s) bougie(s) cloturee(s) de chaque UT.")
    if errors:
        print("\nErreurs :")
        for pair, error in errors:
            print(f"  {pair}: {error}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
