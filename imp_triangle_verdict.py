#!/usr/bin/env python3
"""Verdict BULL / BEAR / NEUTRE par UT a partir des triangles Early IMP.

Regle (par UT : Daily, Weekly, Monthly) :
- BULL : le prix actuel est AU-DESSUS de l'open de la bougie du dernier triangle
  ROUGE cloture (le triangle du camp oppose : son niveau a ete repris) ;
- BEAR : le prix actuel est EN DESSOUS de l'open de la bougie du dernier triangle
  VERT cloture.

La bougie de reference est celle ou le triangle s'affiche (celle qui declenche),
pas la bougie de cross du SAR. Seuls les triangles de bougies cloturees comptent
(cf. imp_early_imp_triangles.py, pas de repaint) ; le prix actuel est le prix live.

Si les deux conditions sont vraies en meme temps (prix entre les deux opens), la
couleur du triangle le plus recent departage (vert = BULL, rouge = BEAR). Si
aucune n'est vraie, ou s'il manque un triangle de reference : NEUTRE.

Exemples :
    python imp_triangle_verdict.py                    # 29 paires, tableau croise D/W/M
    python imp_triangle_verdict.py CHFJPY             # une paire : detail des niveaux de reference
    python imp_triangle_verdict.py EURUSD --timeframes D W --details
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import imp_trend_29pairs as base
from imp_early_imp_triangles import (
    TIMEFRAME_NAME,
    TIMEFRAMES,
    compute_signals,
    decimals,
    period_label,
)

VERDICT_ICON = {"BULL": "\U0001f7e2", "BEAR": "\U0001f534", "NEUTRE": "⚪"}


def classify(price: float, green_open: float | None, red_open: float | None, latest_kind: str | None) -> str:
    """Verdict d'une UT (cf. docstring du module).

    `green_open` / `red_open` : open de la bougie du dernier triangle vert / rouge
    cloture (None s'il n'y en a pas). `latest_kind` : couleur du triangle le plus
    recent des deux ("BULL" = vert, "BEAR" = rouge), pour departager.
    """
    cond_bull = red_open is not None and price > red_open
    cond_bear = green_open is not None and price < green_open
    if cond_bull and cond_bear:
        return latest_kind if latest_kind in ("BULL", "BEAR") else "NEUTRE"
    if cond_bull:
        return "BULL"
    if cond_bear:
        return "BEAR"
    return "NEUTRE"


def timeframe_verdict(computed: dict, timeframe: str, price: float) -> dict:
    last = {"BULL": None, "BEAR": None}
    for signal in computed["signals"]:
        last[signal["kind"]] = signal
    times, opens = computed["times"], computed["opens"]

    def reference(kind: str) -> dict | None:
        signal = last[kind]
        if signal is None:
            return None
        return dict(date=period_label(times[signal["index"]], timeframe), open=opens[signal["index"]])

    green, red = reference("BULL"), reference("BEAR")
    latest = computed["signals"][-1]["kind"] if computed["signals"] else None
    return dict(
        verdict=classify(price, green["open"] if green else None, red["open"] if red else None, latest),
        green=green, red=red, latest_kind=latest,
        last_candle=period_label(times[-1], timeframe),
    )


def analyze_pair(pair: str, args) -> dict:
    computed = {tf: compute_signals(pair, tf, args) for tf in args.timeframes}
    # Un seul prix par paire pour toutes les UT : celui du 1er fetch.
    price = computed[args.timeframes[0]]["live_price"]
    return dict(pair=pair, price=price,
                timeframes={tf: timeframe_verdict(computed[tf], tf, price) for tf in args.timeframes})


def aligned_verdict(result: dict) -> str | None:
    """BULL/BEAR si toutes les UT s'accordent, sinon None."""
    verdicts = {reading["verdict"] for reading in result["timeframes"].values()}
    if len(verdicts) == 1:
        only = next(iter(verdicts))
        return only if only != "NEUTRE" else None
    return None


def print_details(result: dict) -> None:
    pair, d = result["pair"], decimals(result["pair"])
    print(f"\n{pair}  prix actuel {result['price']:.{d}f}")
    for tf, reading in result["timeframes"].items():
        print(f"  {TIMEFRAME_NAME[tf]:<7} {VERDICT_ICON[reading['verdict']]} {reading['verdict']}"
              f"   (derniere bougie cloturee : {reading['last_candle']})")
        for label, key, condition in (("dernier vert ", "green", "BEAR si prix <"), ("dernier rouge", "red", "BULL si prix >")):
            ref = reading[key]
            if ref is None:
                print(f"    {label} : aucun")
                continue
            gap = (result["price"] - ref["open"]) / ref["open"] * 100
            print(f"    {label} : {ref['date']}  open {ref['open']:.{d}f}   [{condition} open]   prix {gap:+.2f}% vs open")


def print_table(results: list[dict], timeframes: list[str]) -> None:
    header = "  ".join(f"{tf:^2}" for tf in timeframes)
    print(f"\n{'PAIRE':<8} {header}   prix")
    for result in results:
        icons = "  ".join(VERDICT_ICON[result["timeframes"][tf]["verdict"]] for tf in timeframes)
        print(f"{result['pair']:<8} {icons}   {result['price']:.{decimals(result['pair'])}f}")
    for verdict, title in (("BULL", "Alignees BULL"), ("BEAR", "Alignees BEAR")):
        pairs = [r["pair"] for r in results if aligned_verdict(r) == verdict]
        if len(timeframes) > 1:
            print(f"\n{VERDICT_ICON[verdict]} {title} ({'+'.join(timeframes)}) : {', '.join(pairs) or 'aucune'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pairs", nargs="*", type=str.upper, help="Paires (defaut : les 29).")
    parser.add_argument("--timeframes", nargs="+", type=str.upper, choices=TIMEFRAMES, default=list(TIMEFRAMES),
                        help="UT a analyser (defaut : D W M).")
    parser.add_argument("--details", action="store_true",
                        help="Affiche les niveaux de reference (auto pour 1 a 3 paires).")
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
    if min(args.d1_candles, args.w1_candles, args.m1_candles) < 5 or args.workers < 1 or args.stagger < 0 \
            or min(args.sar_start, args.sar_increment, args.sar_maximum) <= 0:
        parser.error("Parametres invalides (candles >= 5, workers >= 1, stagger >= 0, SAR > 0)")
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

    ordered = [results[p] for p in pairs if p in results]
    print(f"Verdict Early IMP au {datetime.now(base.PARIS):%Y-%m-%d %H:%M} Paris "
          f"(prix live, triangles de bougies cloturees)")
    if args.details or len(ordered) <= 3:
        for result in ordered:
            print_details(result)
    if len(ordered) > 1:
        print_table(ordered, args.timeframes)
    if errors:
        print("\nErreurs :")
        for pair, error in errors:
            print(f"  {pair}: {error}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
