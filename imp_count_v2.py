#!/usr/bin/env python3
"""Version V2 : au lieu de suivre des IMP confirmés par le Renko multi-UT,
le signal est directement le cross prix/SAR (Parabolic SAR), sur H1 et D.

La logique de momentum, de "dernier signal valide" et de vote/trend reprend
celle d'``imp_count.py`` (version D:\\tradingview, réutilisée telle quelle
mais inlinée ici pour rester autonome) : seule la façon dont un signal est
généré change. Ici plus de Renko, plus de cassure de structure préalable :
chaque cross SAR est un signal.

Exemple :
    python imp_count_v2.py AUDNZD
    python imp_count_v2.py --json
"""

from __future__ import annotations

import argparse
import json
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

import break_line as bl
import imp_trend_29pairs as tv


@dataclass(frozen=True)
class ImpSignal:
    time: pd.Timestamp
    direction: int
    open: float
    close: float
    level: float


def _direction_text(value: int) -> str:
    return "BULL" if value == 1 else "BEAR" if value == -1 else "NEUTRE"


def _momentum(signals: list[ImpSignal], confirmed_close: float) -> int:
    if not signals:
        return 0
    last = signals[-1]
    if last.direction == 1 and confirmed_close > last.close:
        return 1
    if last.direction == -1 and confirmed_close < last.close:
        return -1
    return 0


def _last_valid(signals: list[ImpSignal], confirmed_close: float) -> tuple[int, int]:
    previous_close: dict[int, float | None] = {1: None, -1: None}
    direction = 0
    imp_open: float | None = None
    for signal in signals:
        prior = previous_close[signal.direction]
        valid = prior is not None and (
            signal.close > prior if signal.direction == 1 else signal.close < prior
        )
        if valid:
            direction = signal.direction
            imp_open = signal.open
        previous_close[signal.direction] = signal.close

    active = int(
        direction != 0
        and imp_open is not None
        and (
            (direction == 1 and confirmed_close >= imp_open)
            or (direction == -1 and confirmed_close <= imp_open)
        )
    )
    return direction, active


def calculate_cross_signals(
    candles: pd.DataFrame,
    sar_start: float = 0.1,
    sar_increment: float = 0.1,
    sar_maximum: float = 0.2,
) -> list[ImpSignal]:
    """Un signal par cross prix/SAR, sans confirmation Renko ni cassure de structure."""
    sar = tv.parabolic_sar(candles, sar_start, sar_increment, sar_maximum)
    previous_close = previous_sar = None
    signals: list[ImpSignal] = []

    for i, row in enumerate(candles.itertuples(index=False)):
        current_sar = sar[i]
        current_close = float(row.close)
        if math.isnan(current_sar):
            previous_close, previous_sar = current_close, current_sar
            continue

        bull_cross = (
            previous_close is not None
            and previous_sar is not None
            and not math.isnan(previous_sar)
            and previous_close <= previous_sar
            and current_close > current_sar
        )
        bear_cross = (
            previous_close is not None
            and previous_sar is not None
            and not math.isnan(previous_sar)
            and previous_close >= previous_sar
            and current_close < current_sar
        )
        if bull_cross:
            signals.append(
                ImpSignal(row.time, 1, float(row.open), current_close, float(current_sar))
            )
        if bear_cross:
            signals.append(
                ImpSignal(row.time, -1, float(row.open), current_close, float(current_sar))
            )

        previous_close, previous_sar = current_close, current_sar
    return signals


def calculate(pair: str, args: argparse.Namespace) -> dict[str, Any]:
    h1_raw = tv.fetch_ohlc(pair, "60", args.h1_bars)
    daily_raw = tv.fetch_ohlc(pair, "D", args.daily_bars)
    if len(h1_raw) < 3 or len(daily_raw) < 3:
        raise RuntimeError("Historique insuffisant")

    # La dernière ligne peut être la période courante : exclusion systématique.
    h1 = h1_raw.iloc[:-1].copy().reset_index(drop=True)
    daily = daily_raw.iloc[:-1].copy().reset_index(drop=True)

    h1_signals = calculate_cross_signals(h1, args.sar_start, args.sar_increment, args.sar_maximum)
    daily_signals = calculate_cross_signals(daily, args.sar_start, args.sar_increment, args.sar_maximum)

    h1_close = float(h1["close"].iloc[-1])
    daily_close = float(daily["close"].iloc[-1])
    momentum_d = _momentum(daily_signals, daily_close)
    momentum_h1 = _momentum(h1_signals, h1_close)
    last_d, active_d = _last_valid(daily_signals, daily_close)
    last_h1, active_h1 = _last_valid(h1_signals, h1_close)

    enabled = active_d == 1
    bull_votes = int(enabled) * sum((
        momentum_d == 1,
        momentum_h1 == 1,
        last_d == 1,
        active_h1 == 1 and last_h1 == 1,
    ))
    bear_votes = int(enabled) * sum((
        momentum_d == -1,
        momentum_h1 == -1,
        last_d == -1,
        active_h1 == 1 and last_h1 == -1,
    ))
    trend = 1 if bull_votes > bear_votes else -1 if bear_votes > bull_votes else 0
    percent = max(bull_votes, bear_votes) * 25 if trend else 0

    # Cassure de ligne H1 (break_line.py) : ligne verte = support ascendant
    # (cross bull SAR), ligne rouge = résistance descendante (cross bear SAR).
    # GREEN = close H1 au-dessus des deux lignes (cassure haussière de la
    # rouge) ; RED = close H1 sous les deux lignes (cassure baissière de la
    # verte). On ne retient la cassure que si elle confirme le sens du trend.
    h1_line = bl.compute_break_line_state(h1)
    line_break_confirmed = (trend == 1 and h1_line.state == 1) or (
        trend == -1 and h1_line.state == -1
    )

    return {
        "pair": pair,
        "as_of_h1": h1["time"].iloc[-1].isoformat(),
        "as_of_daily": daily["time"].iloc[-1].isoformat(),
        "momentum_d": _direction_text(momentum_d),
        "momentum_h1": _direction_text(momentum_h1),
        "last_cross_d": _direction_text(last_d),
        "last_cross_d_active": bool(active_d),
        "last_cross_h1": _direction_text(last_h1),
        "last_cross_h1_active": bool(active_h1),
        "trend": _direction_text(trend),
        "trend_percent": percent,
        "h1_line_state": bl.state_name(h1_line.state),
        "line_break_confirmed": bool(line_break_confirmed),
    }


def _print_table(result: dict[str, Any]) -> None:
    active = lambda value: "ACTIF" if value else "INACTIF"
    rows = (
        ("MOMENTUM D", result["momentum_d"], "CLOSE CONF."),
        ("MOMENTUM H1", result["momentum_h1"], "CLOSE CONF."),
        ("LAST CROSS D", result["last_cross_d"], active(result["last_cross_d_active"])),
        ("LAST CROSS H1", result["last_cross_h1"], active(result["last_cross_h1_active"])),
        ("TREND", result["trend"], f'{result["trend_percent"]}%'),
        ("LIGNE H1", result["h1_line_state"], "CONFIRME" if result["line_break_confirmed"] else "-"),
    )
    print(f'\n{result["pair"]}  H1={result["as_of_h1"]}  D={result["as_of_daily"]}')
    print("+---------------+---------+-------------+")
    for name, status, detail in rows:
        print(f"| {name:<13} | {status:^7} | {detail:^11} |")
    print("+---------------+---------+-------------+")


def _print_summary(results: list[dict[str, Any]], errors: list[dict[str, str]]) -> None:
    print("\n+---------+-------+-------+---------+--------+---------+--------+---------+")
    print("| PAIRE   | MOM D | MOM H1| LAST D  | ETAT D | TREND   | %      | LIGNE H1|")
    print("+---------+-------+-------+---------+--------+---------+--------+---------+")
    for result in sorted(results, key=lambda item: item["pair"]):
        state_d = "ACTIF" if result["last_cross_d_active"] else "INACTIF"
        print(
            f'| {result["pair"]:<7} | {result["momentum_d"]:^5} '
            f'| {result["momentum_h1"]:^5} | {result["last_cross_d"]:^7} '
            f'| {state_d:^6} | {result["trend"]:^7} '
            f'| {str(result["trend_percent"]) + "%":^6} '
            f'| {result["h1_line_state"]:^7} |'
        )
    print("+---------+-------+-------+---------+--------+---------+--------+---------+")
    print(f"Analysés: {len(results)}/{len(results) + len(errors)}")
    if errors:
        print("\nErreurs :")
        for error in sorted(errors, key=lambda item: item["pair"]):
            print(f'- {error["pair"]}: {error["error"]}')


def _currency_strength(results: list[dict[str, Any]]) -> dict[str, str]:
    """Force ('fort'/'faible') de chaque devise déduite des paires à 100%.

    Une paire XY à 100% fige les deux devises : BULL => X fort, Y faible ;
    BEAR => X faible, Y fort. En cas de contradiction entre deux 100% pour
    une même devise, celle-ci reste indéterminée (ni fort ni faible).
    """
    implications: dict[str, set[str]] = {}
    for result in results:
        if result["trend_percent"] != 100 or result["trend"] == "NEUTRE":
            continue
        base, quote = result["pair"][:3], result["pair"][3:]
        base_state, quote_state = (
            ("fort", "faible") if result["trend"] == "BULL" else ("faible", "fort")
        )
        implications.setdefault(base, set()).add(base_state)
        implications.setdefault(quote, set()).add(quote_state)
    return {currency: states.pop() for currency, states in implications.items() if len(states) == 1}


def currency_exposure(filtered: list[dict[str, Any]]) -> dict[str, dict[str, float | int]]:
    """Compte les apparitions de chaque devise dans les paires retenues et
    calcule un score cumulé pondéré par occurrence, séparément pour le sens
    fort (BULL) et faible (BEAR).

    Une devise est en position "fort" dans une paire XY si elle est la base
    d'une paire BULL, ou la quote d'une paire BEAR ; "faible" sinon. Chaque
    paire compte pour ses deux devises (ex. EURAUD compte à la fois pour EUR
    et pour AUD).

    Le score n'est pas une simple moyenne des trend_percent (qui ignorerait
    le nombre d'apparitions et pourrait classer une devise vue 2 fois devant
    une devise vue 3 fois) : c'est la somme des confiances divisée par le
    nombre d'apparitions maximum observé dans ce lot, de sorte qu'une devise
    présente plus souvent, avec de bonnes confiances, monte plus haut qu'une
    devise moins présente même si sa moyenne individuelle est plus faible.
    """
    pcts: dict[str, dict[str, list[int]]] = {}
    for result in filtered:
        base, quote = result["pair"][:3], result["pair"][3:]
        pct = result["trend_percent"]
        sides = (
            ((base, "bull"), (quote, "bear"))
            if result["trend"] == "BULL"
            else ((base, "bear"), (quote, "bull"))
        )
        for currency, side in sides:
            pcts.setdefault(currency, {"bull": [], "bear": []})[side].append(pct)

    if not pcts:
        return {}
    max_count = max(len(sides["bull"]) + len(sides["bear"]) for sides in pcts.values())

    report: dict[str, dict[str, float | int]] = {}
    for currency, sides in pcts.items():
        bull_pcts, bear_pcts = sides["bull"], sides["bear"]
        report[currency] = {
            "count": len(bull_pcts) + len(bear_pcts),
            "bull_count": len(bull_pcts),
            "bull_pct": sum(bull_pcts) / max_count if bull_pcts else 0.0,
            "bear_count": len(bear_pcts),
            "bear_pct": sum(bear_pcts) / max_count if bear_pcts else 0.0,
        }
    return report


def _currency_exposure_lines(filtered: list[dict[str, Any]]) -> list[str]:
    """Lignes `icone DEVISE (Nx) XX% SENS`, triées du plus BULL au plus BEAR
    (score net = score BULL - score BEAR, décroissant). Une devise dont les
    apparitions sont mélangées (fort ET faible selon les paires) affiche les
    deux scores et se classe selon son solde net."""
    report = currency_exposure(filtered)
    ordered = sorted(
        report.items(),
        key=lambda kv: (-(kv[1]["bull_pct"] - kv[1]["bear_pct"]), -kv[1]["count"], kv[0]),
    )
    lines = []
    for currency, data in ordered:
        count = data["count"]
        if data["bull_pct"] and data["bear_pct"]:
            lines.append(
                f"⚪{currency} ({count}x) 🟢{data['bull_pct']:.0f}% / 🔴{data['bear_pct']:.0f}%"
            )
        elif data["bull_pct"]:
            lines.append(f"🟢{currency} ({count}x) {data['bull_pct']:.0f}% BULL")
        else:
            lines.append(f"🔴{currency} ({count}x) {data['bear_pct']:.0f}% BEAR")
    return lines


def _print_currency_exposure(filtered: list[dict[str, Any]]) -> None:
    print("\nForce par devise (paires retenues) :")
    lines = _currency_exposure_lines(filtered)
    if not lines:
        print("(aucune)")
        return
    for line in lines:
        print(f"  {line}")


# --- Suivi TENDANCE (run après run) --------------------------------------
# Même technique que `paire_check.py` (`update_index_trend_state` /
# `index_trend_lines`), mais appliquée au classement BULL->BEAR de
# `currency_exposure` (net = bull_pct - bear_pct) plutôt qu'au score Renko
# INDEX : à chaque run, on compare les scores nets des 2 devises d'une paire
# suivie, on cumule un poids "monté" (🟢, base plus forte que quote) / "baissé"
# (🔴) pondéré par récence (demi-vie 4h -- un run vieux de 4h pèse moitié
# moins que le dernier), et on remet le compteur à zéro chaque jour Paris.
#
# Le rapport passé à `update_currency_trend_state` doit venir de `results`
# complets (les 29 instruments, sans le filtrage de `filter_active` --
# min_percent, cassure de ligne H1 confirmée, contradiction avec une devise
# déjà verrouillée à 100%), pas de `filtered` : sinon un jour sans paire
# retenue pour SAR BREAK viderait aussi TENDANCE, alors que `results` fournit
# un trend/trend_percent par paire dès que sa D1 est active (cf. `calculate`,
# qui remet déjà `trend` à NEUTRE si `last_cross_d_active` est faux -- il
# suffit donc d'exclure les NEUTRE, cf. `main`).
CURRENCY_TREND_HALF_LIFE_HOURS = 4.0
CURRENCY_TREND_STATE_FILE = Path("imp_count_v2_currency_trend_state.json")


def signed_currency_net(currency: str, report: dict[str, dict[str, float | int]]) -> float | None:
    """bull_pct - bear_pct pour `currency` dans `report` (cf. currency_exposure).
    None si la devise n'apparaît dans aucune paire retenue de ce run."""
    data = report.get(currency)
    if data is None:
        return None
    return data["bull_pct"] - data["bear_pct"]


def pair_trend_icon(pair: str, report: dict[str, dict[str, float | int]]) -> str | None:
    """Bille 🟢/🔴/⚪ comparant les scores nets des 2 devises de `pair` (base
    plus fort que quote => 🟢, l'inverse => 🔴, égalité => ⚪) -- même principe
    que `pair_index_icon` dans paire_check.py, mais basé sur notre classement
    BULL->BEAR. None si une des deux devises est absente de `report` (aucune
    paire retenue ne l'impliquait ce run-là)."""
    base, quote = pair[:3], pair[3:]
    base_net = signed_currency_net(base, report)
    quote_net = signed_currency_net(quote, report)
    if base_net is None or quote_net is None:
        return None
    if base_net > quote_net:
        return "🟢"
    if base_net < quote_net:
        return "🔴"
    return "⚪"


def _parse_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def load_currency_trend_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_currency_trend_state(path: Path, state: dict) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def update_currency_trend_state(
    state: dict, pairs: list[str], report: dict[str, dict[str, float | int]], now: datetime,
) -> dict:
    """Ajoute le run courant au poids monté(🟢)/baissé(🔴) de chaque paire de
    `pairs`, décoté par récence puis incrémenté par `pair_trend_icon` -- une
    bille ⚪ ou absente ne compte ni pour l'un ni pour l'autre mais ne fait
    pas perdre l'historique. Remis à zéro à chaque nouvelle date Paris."""
    today = now.astimezone(tv.PARIS).date().isoformat()
    old = state if isinstance(state, dict) else {}
    old_counts = old.get("pairs", {}) if old.get("date") == today else {}
    new_counts: dict[str, dict] = {}
    for pair in pairs:
        prior = old_counts.get(pair) or {}
        weighted_up = float(prior.get("weighted_up", 0.0))
        weighted_down = float(prior.get("weighted_down", 0.0))
        last_update = _parse_iso_datetime(prior.get("last_update"))
        if last_update is not None:
            elapsed_hours = max((now - last_update).total_seconds(), 0.0) / 3600.0
            decay = 0.5 ** (elapsed_hours / CURRENCY_TREND_HALF_LIFE_HOURS)
            weighted_up *= decay
            weighted_down *= decay
        icon = pair_trend_icon(pair, report)
        if icon == "🟢":
            weighted_up += 1.0
        elif icon == "🔴":
            weighted_down += 1.0
        new_counts[pair] = {
            "weighted_up": weighted_up, "weighted_down": weighted_down, "last_update": now.isoformat(),
        }
    return {"date": today, "pairs": new_counts}


def currency_trend_lines(pairs: list[str], trend_state: dict) -> list[str]:
    """Section `📈 TENDANCE` : pour chaque paire de `pairs` ayant un poids
    accumulé aujourd'hui, 2 lignes `🟢 {PAIR} ({pct_up}%)` puis
    `🔴 {PAIR} ({pct_down}%)` -- part pondérée par récence des runs montés
    puis baissés, sur le poids total. Vide si aucune paire n'a de poids
    accumulé (ex. tout premier run du jour)."""
    counts_by_pair = trend_state.get("pairs", {}) if isinstance(trend_state, dict) else {}
    lines = []
    for pair in pairs:
        counts = counts_by_pair.get(pair) or {}
        up = float(counts.get("weighted_up", 0.0))
        down = float(counts.get("weighted_down", 0.0))
        total = up + down
        if total <= 0.0:
            continue
        lines.append(f"🟢 {pair} ({up / total * 100.0:.2f}%)")
        lines.append(f"🔴 {pair} ({down / total * 100.0:.2f}%)")
    if not lines:
        return []
    return ["📈 TENDANCE", *lines]


def filter_active(results: list[dict[str, Any]], min_percent: int) -> list[dict[str, Any]]:
    """Paires avec LAST CROSS D actif et trend_percent >= min_percent, triées par % décroissant.

    Exclut aussi toute paire dont le sens contredit une force de devise déjà
    établie par un signal à 100% (ex. si CAD est faible d'après AUDCAD BULL
    100%, CADCHF BULL est écartée car elle prétendrait CAD fort), ainsi que
    toute paire dont la cassure de ligne H1 (break_line.py) ne confirme pas
    le sens du trend (BULL => ligne rouge cassée à la hausse / GREEN,
    BEAR => ligne verte cassée à la baisse / RED).
    """
    strength = _currency_strength(results)
    kept = []
    for result in results:
        if not result["last_cross_d_active"] or result["trend_percent"] < min_percent:
            continue
        if result["trend"] == "NEUTRE":
            continue
        if not result["line_break_confirmed"]:
            continue
        base, quote = result["pair"][:3], result["pair"][3:]
        base_state, quote_state = (
            ("fort", "faible") if result["trend"] == "BULL" else ("faible", "fort")
        )
        if strength.get(base, base_state) != base_state:
            continue
        if strength.get(quote, quote_state) != quote_state:
            continue
        kept.append(result)
    return sorted(kept, key=lambda item: (-item["trend_percent"], item["pair"]))


def _print_filtered(results: list[dict[str, Any]], min_percent: int) -> None:
    print(f"\nPaires ETAT D=ACTIF, TREND >= {min_percent}% et ligne H1 cassée dans le sens du trend :")
    if not results:
        print("(aucune)")
        return
    print("+---------+---------+--------+---------+")
    print("| PAIRE   | TREND   | %      | LIGNE H1|")
    print("+---------+---------+--------+---------+")
    for result in results:
        print(
            f'| {result["pair"]:<7} | {result["trend"]:^7} '
            f'| {str(result["trend_percent"]) + "%":^6} '
            f'| {result["h1_line_state"]:^7} |'
        )
    print("+---------+---------+--------+---------+")


def build_telegram_message(
    filtered: list[dict[str, Any]], trend_lines: list[str] | None = None,
) -> str | None:
    """Message Telegram au même format que VIVIER (renko_score_29pairs_v16.py) :
    icônes 🟢/🔴 collées au nom de paire, groupé BULL puis BEAR, horodatage
    Paris en pied de message. Retourne None si rien à annoncer : ni paire
    confirmée, ni TENDANCE accumulée (cf. `trend_lines`) -- comme VIVIER.

    `trend_lines` (cf. `currency_trend_lines`) ajoute la section `📈 TENDANCE`
    juste avant l'horodatage, sur le même principe que `paire_check.py` :
    disponible même les jours où `filtered` est vide, tant qu'un historique
    existe déjà pour au moins une paire suivie.
    """
    if not filtered and not trend_lines:
        return None

    lines = ["📊 SAR BREAK", ""]
    for icon, direction in (("🟢", "BULL"), ("🔴", "BEAR")):
        for result in filtered:
            if result["trend"] == direction:
                lines.append(f"{icon}{result['pair']} ({result['trend_percent']}%)")

    currency_lines = _currency_exposure_lines(filtered)
    if currency_lines:
        lines.append("")
        lines.append("💱 FORCE DEVISES")
        lines.extend(currency_lines)

    if trend_lines:
        lines.append("")
        lines.extend(trend_lines)

    lines.append("")
    lines.append(f"⏰ {datetime.now(tv.PARIS).strftime('%Y-%m-%d %H:%M')} Paris")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "pair",
        nargs="?",
        default=None,
        help="Symbole OANDA; sans symbole, analyse les 29 instruments",
    )
    parser.add_argument("--sar-start", type=float, default=0.1)
    parser.add_argument("--sar-increment", type=float, default=0.1)
    parser.add_argument("--sar-maximum", type=float, default=0.2)
    parser.add_argument("--h1-bars", type=int, default=5000)
    parser.add_argument("--daily-bars", type=int, default=2500)
    parser.add_argument("--workers", type=int, default=4,
                        help="Nombre d'analyses parallèles (4 par défaut)")
    parser.add_argument("--min-percent", type=int, default=50,
                        help="Seuil de trend_percent pour le tri des paires actives (50 par défaut)")
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--telegram", action="store_true",
                        help="Envoie les paires filtrées sur Telegram (format VIVIER)")
    parser.add_argument(
        "--trend-pairs", nargs="+", default=["EURUSD"],
        help="Paires suivies run après run par la section TENDANCE (EURUSD par défaut).",
    )
    parser.add_argument(
        "--trend-state-file", default=str(CURRENCY_TREND_STATE_FILE),
        help="Fichier d'état JSON du compteur monté/baissé de TENDANCE, remis à zéro chaque jour Paris.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.pair:
        result = calculate(args.pair.upper(), args)
        if args.as_json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            _print_table(result)
        return 0

    results: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    workers = max(1, min(args.workers, len(tv.PAIRS_29)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(calculate, pair, args): pair for pair in tv.PAIRS_29}
        for future in as_completed(futures):
            pair = futures[future]
            try:
                results.append(future.result())
                if not args.as_json:
                    print(f"[{len(results) + len(errors):02d}/29] {pair} OK")
            except Exception as exc:
                errors.append({"pair": pair, "error": str(exc)})
                if not args.as_json:
                    print(f"[{len(results) + len(errors):02d}/29] {pair} ERREUR")

    filtered = filter_active(results, args.min_percent)

    # TENDANCE se base sur `results` complets (hors NEUTRE), pas `filtered` :
    # elle reste alimentée même les jours sans paire retenue pour SAR BREAK
    # (cf. commentaire au-dessus de `signed_currency_net`).
    trend_candidates = [r for r in results if r["trend"] != "NEUTRE"]
    trend_report = currency_exposure(trend_candidates)
    now = datetime.now(tv.PARIS)
    trend_state_path = Path(args.trend_state_file)
    trend_state = load_currency_trend_state(trend_state_path)
    new_trend_state = update_currency_trend_state(trend_state, args.trend_pairs, trend_report, now)
    save_currency_trend_state(trend_state_path, new_trend_state)
    trend_lines = currency_trend_lines(args.trend_pairs, new_trend_state)

    if args.telegram:
        message = build_telegram_message(filtered, trend_lines)
        if message is None:
            print("Telegram: rien à envoyer (aucune paire confirmée ni tendance accumulée).")
        else:
            bl.send_telegram_message(message)

    if args.as_json:
        print(json.dumps({"results": sorted(results, key=lambda item: item["pair"]),
                          "errors": errors, "filtered": filtered},
                          ensure_ascii=False, indent=2))
    else:
        _print_summary(results, errors)
        _print_filtered(filtered, args.min_percent)
        _print_currency_exposure(filtered)
        if trend_lines:
            print()
            for line in trend_lines:
                print(line)
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
