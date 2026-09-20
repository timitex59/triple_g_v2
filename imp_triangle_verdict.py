#!/usr/bin/env python3
"""Verdict BULL / BEAR / NEUTRE par UT a partir des triangles Early IMP.

Regle de base (par UT : Daily, Weekly, Monthly) :
- BULL : le prix actuel est AU-DESSUS de l'open de la bougie du dernier triangle
  ROUGE cloture (le triangle du camp oppose : son niveau a ete repris) ;
- BEAR : le prix actuel est EN DESSOUS de l'open de la bougie du dernier triangle
  VERT cloture.

Reference deplacee quand le triangle oppose est "trop loin" : si au moins
`--far-count` triangles de la couleur du verdict se sont formes depuis le dernier
triangle oppose (defaut 2 = "plusieurs"), on compare au CLOSE du dernier triangle
de la couleur du verdict au lieu de l'open du dernier triangle oppose :
- BULL : au moins N verts depuis le dernier rouge -> prix > close du dernier vert ;
- BEAR : au moins N rouges depuis le dernier vert -> prix < close du dernier rouge.
Ca evite de comparer a un niveau vieux de plusieurs signaux (un rouge de 2024
alors que 3 verts se sont formes depuis).

La bougie de reference est celle ou le triangle s'affiche (celle qui declenche),
pas la bougie de cross du SAR. Seuls les triangles de bougies cloturees comptent
(cf. imp_early_imp_triangles.py, pas de repaint) ; le prix actuel est le prix live.

Si les deux conditions sont vraies en meme temps, la couleur du triangle le plus
recent departage (vert = BULL, rouge = BEAR). Si aucune n'est vraie, ou s'il manque
une reference : NEUTRE.

Exemples :
    python imp_triangle_verdict.py                    # 29 paires, tableau croise D/W/M
    python imp_triangle_verdict.py CHFJPY             # une paire : detail des niveaux de reference
    python imp_triangle_verdict.py EURUSD --timeframes D W --far-count 3
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
from imp_trend5_29pairs import send_telegram_message

VERDICT_ICON = {"BULL": "\U0001f7e2", "BEAR": "\U0001f534", "NEUTRE": "⚪"}
BASIS_TEXT = {
    "OPEN_RED": "open du dernier rouge",
    "CLOSE_GREEN": "close du dernier vert",
    "OPEN_GREEN": "open du dernier vert",
    "CLOSE_RED": "close du dernier rouge",
}


def classify(price: float, bull_above: float | None, bear_below: float | None, latest_kind: str | None) -> str:
    """Verdict d'une UT (cf. docstring du module).

    `bull_above` : niveau que le prix doit depasser par le haut pour etre BULL ;
    `bear_below` : niveau sous lequel il doit passer pour etre BEAR (None = pas de
    reference). `latest_kind` : couleur du triangle le plus recent ("BULL" = vert,
    "BEAR" = rouge), pour departager quand les deux conditions sont vraies.
    """
    cond_bull = bull_above is not None and price > bull_above
    cond_bear = bear_below is not None and price < bear_below
    if cond_bull and cond_bear:
        return latest_kind if latest_kind in ("BULL", "BEAR") else "NEUTRE"
    if cond_bull:
        return "BULL"
    if cond_bear:
        return "BEAR"
    return "NEUTRE"


def count_since(signals: list[dict], opposite: str, same: str) -> int:
    """Nombre de triangles `same` formes depuis le dernier triangle `opposite`
    (tous les `same` s'il n'y a jamais eu d'`opposite`)."""
    last_opposite = max((i for i, s in enumerate(signals) if s["kind"] == opposite), default=-1)
    return sum(1 for s in signals[last_opposite + 1:] if s["kind"] == same)


def timeframe_verdict(computed: dict, timeframe: str, price: float, far_count: int = 2) -> dict:
    signals = computed["signals"]
    times, opens, closes = computed["times"], computed["opens"], computed["closes"]
    last = {"BULL": None, "BEAR": None}
    for signal in signals:
        last[signal["kind"]] = signal
    green, red = last["BULL"], last["BEAR"]

    def candle(signal: dict | None) -> dict | None:
        if signal is None:
            return None
        i = signal["index"]
        return dict(date=period_label(times[i], timeframe), open=opens[i], close=closes[i])

    green_candle, red_candle = candle(green), candle(red)
    greens_since_red = count_since(signals, "BEAR", "BULL")
    reds_since_green = count_since(signals, "BULL", "BEAR")

    if green_candle is not None and greens_since_red >= far_count:
        bull_ref = dict(basis="CLOSE_GREEN", level=green_candle["close"], date=green_candle["date"])
    elif red_candle is not None:
        bull_ref = dict(basis="OPEN_RED", level=red_candle["open"], date=red_candle["date"])
    else:
        bull_ref = None
    if red_candle is not None and reds_since_green >= far_count:
        bear_ref = dict(basis="CLOSE_RED", level=red_candle["close"], date=red_candle["date"])
    elif green_candle is not None:
        bear_ref = dict(basis="OPEN_GREEN", level=green_candle["open"], date=green_candle["date"])
    else:
        bear_ref = None

    latest = signals[-1]["kind"] if signals else None
    return dict(
        verdict=classify(price, bull_ref["level"] if bull_ref else None,
                         bear_ref["level"] if bear_ref else None, latest),
        green=green_candle, red=red_candle, latest_kind=latest,
        bull_ref=bull_ref, bear_ref=bear_ref,
        greens_since_red=greens_since_red, reds_since_green=reds_since_green,
        last_candle=period_label(times[-1], timeframe),
    )


def analyze_pair(pair: str, args) -> dict:
    computed = {tf: compute_signals(pair, tf, args) for tf in args.timeframes}
    # Un seul prix par paire pour toutes les UT : celui du 1er fetch.
    price = computed[args.timeframes[0]]["live_price"]
    return dict(pair=pair, price=price,
                timeframes={tf: timeframe_verdict(computed[tf], tf, price, args.far_count)
                            for tf in args.timeframes})


def aligned_verdict(result: dict) -> str | None:
    """BULL/BEAR si toutes les UT s'accordent, sinon None."""
    verdicts = {reading["verdict"] for reading in result["timeframes"].values()}
    if len(verdicts) == 1:
        only = next(iter(verdicts))
        return only if only != "NEUTRE" else None
    return None


def print_details(result: dict) -> None:
    pair, d = result["pair"], decimals(result["pair"])
    price = result["price"]
    print(f"\n{pair}  prix actuel {price:.{d}f}")
    for tf, reading in result["timeframes"].items():
        print(f"  {TIMEFRAME_NAME[tf]:<7} {VERDICT_ICON[reading['verdict']]} {reading['verdict']}"
              f"   (derniere bougie cloturee : {reading['last_candle']})")
        for label, ref_key, comparator, since_key, since_name, opposite in (
            ("BULL si prix >", "bull_ref", ">", "greens_since_red", "verts", "rouge"),
            ("BEAR si prix <", "bear_ref", "<", "reds_since_green", "rouges", "vert"),
        ):
            ref = reading[ref_key]
            if ref is None:
                print(f"    {label} : aucune reference")
                continue
            gap = (price - ref["level"]) / ref["level"] * 100
            note = ""
            if ref["basis"] in ("CLOSE_GREEN", "CLOSE_RED"):
                note = f"  [{reading[since_key]} {since_name} depuis le dernier {opposite}]"
            print(f"    {label} {ref['level']:.{d}f}   {BASIS_TEXT[ref['basis']]} ({ref['date']}){note}   prix {gap:+.2f}%")


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


def build_telegram_message(results: list[dict], timeframes: list[str], now: datetime | None = None) -> str | None:
    """Message au format des autres alertes : titre, sections, `PAIRE<tab>icones`
    (une icone par UT, dans l'ordre de `timeframes`), horodatage Paris en pied.

    Seules les paires ALIGNEES sur toutes les UT analysees sont annoncees (BULL puis
    BEAR, ordre alphabetique) ; None si aucune -- silence plutot qu'un message vide,
    comme VIVIER / SAR BREAK / MTF SAR STRUCTURE.
    """
    lines = ["\U0001f53a EARLY IMP", ""]
    has_content = False
    for verdict, title in (("BULL", "BULL"), ("BEAR", "BEAR")):
        aligned = sorted((r for r in results if aligned_verdict(r) == verdict), key=lambda r: r["pair"])
        if not aligned:
            continue
        lines.append(f"{VERDICT_ICON[verdict]} {title} ({'+'.join(timeframes)})")
        for result in aligned:
            icons = "".join(VERDICT_ICON[result["timeframes"][tf]["verdict"]] for tf in timeframes)
            lines.append(f"{result['pair']}\t{icons}")
        lines.append("")
        has_content = True
    if not has_content:
        return None
    lines.append(f"⏰ {(now or datetime.now(base.PARIS)).strftime('%Y-%m-%d %H:%M')} Paris")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pairs", nargs="*", type=str.upper, help="Paires (defaut : les 29).")
    parser.add_argument("--timeframes", nargs="+", type=str.upper, choices=TIMEFRAMES, default=list(TIMEFRAMES),
                        help="UT a analyser (defaut : D W M).")
    parser.add_argument("--far-count", type=int, default=2,
                        help="Nombre de triangles de la couleur du verdict formes depuis le dernier triangle oppose "
                             "a partir duquel celui-ci est juge trop loin (defaut 2 = plusieurs).")
    parser.add_argument("--details", action="store_true",
                        help="Affiche les niveaux de reference (auto pour 1 a 3 paires).")
    parser.add_argument("--telegram", action="store_true",
                        help="Envoie les paires alignees sur Telegram (sans ce flag : apercu du message seulement).")
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
    if args.far_count < 1 or min(args.d1_candles, args.w1_candles, args.m1_candles) < 5 or args.workers < 1 \
            or args.stagger < 0 or min(args.sar_start, args.sar_increment, args.sar_maximum) <= 0:
        parser.error("Parametres invalides (far-count >= 1, candles >= 5, workers >= 1, stagger >= 0, SAR > 0)")
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
          f"(prix live, triangles de bougies cloturees, far-count {args.far_count})")
    if args.details or len(ordered) <= 3:
        for result in ordered:
            print_details(result)
    if len(ordered) > 1:
        print_table(ordered, args.timeframes)

    message = build_telegram_message(ordered, args.timeframes)
    if message is None:
        print("\nTelegram : rien a annoncer (aucune paire alignee sur toutes les UT).")
    elif args.telegram:
        print("\nTelegram :")
        print(message)
        if send_telegram_message(message):
            print("  Message envoye.")
    else:
        print("\nApercu Telegram (non envoye, ajouter --telegram) :")
        print(message)
    if errors:
        print("\nErreurs :")
        for pair, error in errors:
            print(f"  {pair}: {error}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
