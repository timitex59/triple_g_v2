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

Selection finale (liste Telegram EARLY IMP) : une paire alignee sur toutes les UT
n'est retenue que si |CHG% daily| > `--chg-threshold` (defaut 0.1). Une paire deja
retenue au run precedent qui repasse sous le seuil reste avec un warning ; le warning
disparait quand elle repasse au-dessus. Une paire retenue qui n'est plus alignee reste
pour le reste du jour de trading avec un double warning (la boule de couleur est
remplacee par un warning), puis sort. Cet etat est persiste dans `--state-file` (mis
a jour uniquement avec `--telegram`, donc un apercu local ne le modifie pas).

Exemples :
    python imp_triangle_verdict.py                    # 29 paires, tableau croise D/W/M
    python imp_triangle_verdict.py CHFJPY             # une paire : detail des niveaux de reference
    python imp_triangle_verdict.py EURUSD --timeframes D W --far-count 3
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import imp_trend_29pairs as base
from imp_early_imp_triangles import (
    NEW_YORK,
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


def daily_chg_pct(live_price: float, prev_close: float | None) -> float | None:
    """CHG% daily : (prix live - close de la veille) / close de la veille, comme
    `daily_chg` dans paire_check.py. None si la veille est inconnue ou nulle."""
    if prev_close is None or prev_close == 0:
        return None
    return (live_price - prev_close) / prev_close * 100.0


def analyze_pair(pair: str, args) -> dict:
    computed = {tf: compute_signals(pair, tf, args) for tf in args.timeframes}
    # Un seul prix par paire pour toutes les UT : celui du 1er fetch.
    price = computed[args.timeframes[0]]["live_price"]
    if "D" in computed:
        chg = daily_chg_pct(computed["D"]["live_price"], computed["D"]["prev_close"])
    else:
        daily = base.fetch_ohlc(pair, "D", 5)
        chg = daily_chg_pct(float(daily["close"].iloc[-1]), float(daily["close"].iloc[-2]))
    return dict(pair=pair, price=price, chg=chg,
                timeframes={tf: timeframe_verdict(computed[tf], tf, price, args.far_count)
                            for tf in args.timeframes})


def trading_day(now: datetime) -> str:
    """Identifiant du jour de trading forex : bascule a 17h New York (= la bougie D1)."""
    return (now.astimezone(NEW_YORK) + timedelta(hours=7)).strftime("%Y-%m-%d")


def update_selection(
    results: list[dict], previous: dict[str, dict], threshold: float, today: str,
) -> tuple[list[dict], dict[str, dict]]:
    """Selection finale de la liste EARLY IMP, avec persistance d'un run a l'autre.

    - Une paire ALIGNEE entre dans la liste si |CHG% daily| > `threshold`.
    - Une paire deja retenue au run precedent (meme sens) qui repasse sous le seuil
      reste avec `warning` (1 warning) ; il disparait quand elle repasse au-dessus.
    - Une paire deja retenue qui n'est plus alignee reste pour le reste du jour de
      trading `today` avec `lost` (double warning : la boule de couleur est remplacee
      par un warning) ; elle sort ensuite. Si elle se realigne dans le meme sens elle
      reprend le traitement normal ; dans le sens oppose elle doit repasser le seuil
      comme une nouvelle.
    - Une paire absente de `results` (fetch en erreur ce run-la) garde son etat
      precedent, jour d'expiration compris : une erreur reseau ne doit pas la faire
      sortir.

    Renvoie (selection, nouvel_etat) : `selection` = [{pair, verdict, warning, lost,
    chg}] (`verdict` = sens d'origine), `nouvel_etat` = {pair: {verdict, warning[,
    lost_day]}}.
    """
    selection: list[dict] = []
    state: dict[str, dict] = {}
    seen = {r["pair"] for r in results}
    for result in results:
        pair, chg = result["pair"], result["chg"]
        prev = previous.get(pair)
        verdict = aligned_verdict(result)
        if verdict is None:
            if prev is None:
                continue
            lost_day = prev.get("lost_day") or today
            if lost_day != today:
                continue
            selection.append(dict(pair=pair, verdict=prev["verdict"], warning=True, lost=True, chg=chg))
            state[pair] = dict(verdict=prev["verdict"], warning=True, lost_day=lost_day)
            continue
        passes = chg is not None and abs(chg) > threshold
        was_selected = prev is not None and prev["verdict"] == verdict
        if not passes and not was_selected:
            continue
        selection.append(dict(pair=pair, verdict=verdict, warning=not passes, lost=False, chg=chg))
        state[pair] = dict(verdict=verdict, warning=not passes)
    for pair, entry in previous.items():
        if pair in seen:
            continue
        if entry.get("lost_day") not in (None, today):
            continue
        selection.append(dict(pair=pair, verdict=entry["verdict"], warning=entry.get("warning", False),
                              lost=entry.get("lost_day") is not None, chg=None))
        state[pair] = dict(entry)
    selection.sort(key=lambda s: (s["verdict"] != "BULL", s["pair"]))
    return selection, state


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_state(path: Path, pairs: dict[str, dict], now: datetime | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = (now or datetime.now(base.PARIS)).isoformat()
    path.write_text(json.dumps(dict(updated_paris=stamp, pairs=pairs), indent=2, sort_keys=True), encoding="utf-8")


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
    print(f"\n{'PAIRE':<8} {header}   {'CHG%D':>7}   prix")
    for result in results:
        icons = "  ".join(VERDICT_ICON[result["timeframes"][tf]["verdict"]] for tf in timeframes)
        chg = f"{result['chg']:+.2f}%" if result["chg"] is not None else "n/a"
        print(f"{result['pair']:<8} {icons}   {chg:>7}   {result['price']:.{decimals(result['pair'])}f}")
    for verdict, title in (("BULL", "Alignees BULL"), ("BEAR", "Alignees BEAR")):
        pairs = [r["pair"] for r in results if aligned_verdict(r) == verdict]
        if len(timeframes) > 1:
            print(f"\n{VERDICT_ICON[verdict]} {title} ({'+'.join(timeframes)}) : {', '.join(pairs) or 'aucune'}")


WARNING_ICON = "⚠️"


def build_telegram_message(selection: list[dict], now: datetime | None = None) -> str | None:
    """Message au format des autres alertes : titre, une ligne `PAIRE<tab>icone` par
    paire retenue (cf. `update_selection`), horodatage Paris en pied.

    BULL (vert) d'abord puis BEAR (rouge), par ordre alphabetique dans chaque groupe ;
    une paire retenue mais repassee sous le seuil de CHG% porte un warning collé a la
    boule ; une paire qui n'est plus alignee porte un double warning (la boule est
    remplacee par un warning). None si la selection est vide -- silence plutot qu'un
    message vide, comme VIVIER / SAR BREAK / MTF SAR STRUCTURE.
    """
    ordered = sorted(selection, key=lambda s: (s["verdict"] != "BULL", s["pair"]))
    lines = [
        f"{s['pair']}\t{WARNING_ICON if s['lost'] else VERDICT_ICON[s['verdict']]}{WARNING_ICON if s['warning'] else ''}"
        for s in ordered
    ]
    if not lines:
        return None
    header = ["\U0001f53a EARLY IMP", ""]
    footer = ["", f"⏰ {(now or datetime.now(base.PARIS)).strftime('%Y-%m-%d %H:%M')} Paris"]
    return "\n".join(header + lines + footer)


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
                        help="Envoie la selection sur Telegram et met a jour le fichier d'etat "
                             "(sans ce flag : apercu du message seulement, etat non modifie).")
    parser.add_argument("--chg-threshold", type=float, default=0.1,
                        help="|CHG%% daily| minimum (en %%) pour entrer dans la liste (defaut 0.1).")
    parser.add_argument("--state-file", type=Path, default=Path("imp_triangle_verdict_state.json"),
                        help="Etat de la selection d'un run a l'autre (paires retenues, warning).")
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
    if args.far_count < 1 or args.chg_threshold < 0 or min(args.d1_candles, args.w1_candles, args.m1_candles) < 5 \
            or args.workers < 1 or args.stagger < 0 or min(args.sar_start, args.sar_increment, args.sar_maximum) <= 0:
        parser.error("Parametres invalides (far-count >= 1, chg-threshold >= 0, candles >= 5, workers >= 1, "
                     "stagger >= 0, SAR > 0)")
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

    previous = load_state(args.state_file).get("pairs", {})
    selection, new_state = update_selection(
        ordered, previous, args.chg_threshold, trading_day(datetime.now(base.PARIS)))
    print(f"\nSelection (alignee {'+'.join(args.timeframes)} et |CHG%D| > {args.chg_threshold:g}%) : "
          + (", ".join(f"{s['pair']}{' ' + WARNING_ICON * (2 if s['lost'] else 1) if s['warning'] else ''}"
                       for s in selection) or "aucune"))

    message = build_telegram_message(selection)
    if message is None:
        print("\nTelegram : rien a annoncer (aucune paire retenue).")
    elif args.telegram:
        print("\nTelegram :")
        print(message)
        if send_telegram_message(message):
            print("  Message envoye.")
    else:
        print("\nApercu Telegram (non envoye, ajouter --telegram) :")
        print(message)
    if args.telegram:
        save_state(args.state_file, new_state)
    if errors:
        print("\nErreurs :")
        for pair, error in errors:
            print(f"  {pair}: {error}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
