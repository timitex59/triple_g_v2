#!/usr/bin/env python3
"""IMP TREND V2: meme scan des 29 paires OANDA que imp_trend_29pairs.py (V1),
mais un systeme de score revu pour corriger 3 angles morts identifies dans
V1 en audit avec l'utilisateur:

1. INFLATION DES VOTES -- V1 compte 11 "votes" mais 8 d'entre eux derivent
   de seulement 2 series de donnees (les 21 derniers signaux D1, les 21
   derniers signaux H1): un historique qui penche bull fait basculer 4 votes
   ensemble, ce n'est pas 4 confirmations independantes. V2 ne garde que 5
   votes de direction reellement distincts (Renko M/W/D + IMP21 D1 + IMP21
   H1, cf. VOTE_THRESHOLD), et retrograde les 4 votes de moyennes D1/H1 au
   rang de filtre de qualite (cf. point 3) plutot que de votes.

2. RANG CALCULE MAIS PAS EXPLOITE -- en V1, une paire "rang 4" (score brut
   correct mais aucune moyenne directionnelle ne confirme) ouvre quand meme
   une position, exactement comme une paire "rang 1". V2 exclut ces paires
   du suivi de positions (`tradable=False`): elles restent affichees dans
   le message pour information, mais update_session_tracking ne les recoit
   plus.

3. MOYENNES SUR PETIT ECHANTILLON -- une moyenne basee sur 2 signaux pesait
   pareil qu'une moyenne sur 19 signaux. V2 ignore (ni confirme ni infirme)
   toute moyenne dont l'echantillon est sous MIN_AVG_SAMPLE.

Le 4e point souleve (concentration de devises cachee derriere un format
diversifie) n'est pas corrige en filtrant les paires ici -- l'utilisateur a
choisi de le rendre visible plutot que de plafonner: cf. `currency_exposure_lines`.

5. VOTE DE DIRECTION EN PLUS, PAS DE FILTRE -- ajoute apres coup (2026-08-26,
   a la demande de l'utilisateur): CURRENCY_INDEX compare les 8 indices
   devises de PAIRE_CHECK (cf. imp_trend_29pairs.currency_index_vote) pour
   confirmer -- ou non -- qu'une paire va dans le sens ou sa devise de base
   est plus forte que sa devise cotee. Ce n'est PAS une comparaison de
   couleur: deux devises rouges ne sont pas equivalentes (GBP +1.20 rouge est
   bien plus faible que CAD +0.00 rouge, GBPCAD vote quand meme BEAR) --
   c'est l'ecart de force signee (cf. CURRENCY_INDEX_MIN_SPREAD dans
   imp_trend_29pairs.py) qui tranche, avec un minimum requis pour eviter de
   trancher sur du bruit (AUD +0.28 vs USD +0.26, verts tous les deux, reste
   NEUTRAL). Compte comme un 6e vote de direction a part entiere
   (VOTE_THRESHOLD passe de 4/5 a 5/6, meme tolerance qu'avant: au plus 1
   vote dissident/neutre), pas comme un filtre de qualite -- une paire dont
   les 5 votes d'origine etaient 4/5 (1 dissident) a desormais besoin que
   CURRENCY_INDEX confirme pour rester retenue.

   2026-08-26 (plus tard le meme jour): CURRENCY_INDEX sert aussi de veto
   (cf. imp_trend_29pairs.currency_index_diverges, meme principe que
   is_engine_divergent dans renko_full_alignment_29pairs.py) -- une
   contradiction active (BEAR alors que la paire est BULL, pas juste
   NEUTRAL/abstention) exclut la paire, meme si les 5 votes d'origine
   suffisaient a eux seuls. Cas qui a motive l'ajout: GBPJPY BULL sur
   Renko/IMP21 mais GBP nettement plus faible que JPY sur les indices
   devises -- desormais exclue plutot que retenue avec un vote perdu.

   2026-08-26 (encore plus tard): le BULL/BEAR de CURRENCY_INDEX est
   desormais lui-meme filtre par le trend propre de chaque devise (cf.
   imp_trend_29pairs.currency_trend_confirms / fetch_currency_imp_rows) --
   Renko/PSAR/IMP21 rejoue sur le symbole TradingView de chaque indice
   (TVC:DXY etc.), pas juste son score Renko+CHG%D. Si aucune des deux
   devises ne trend elle-meme dans le sens attendu par son role (forte =
   BULL, faible = BEAR), le vote est retrograde en NEUTRAL avant meme
   d'atteindre le veto ci-dessus -- meme convention 0-sur-N que le filtre
   qualite D1/H1 du point 3.

6. DEVISE INTERNEMENT CONTRADICTOIRE -> NON TRADABLE, PAS EXCLUE (2026-08-27,
   a la demande de l'utilisateur): quand le mouvement du jour d'une devise
   (signe de son CHG%D) contredit son propre consensus structurel (cf.
   imp_trend_29pairs.currency_consensus_status/pair_touches_a_divergent_currency
   -- ex. GBP -0.24% aujourd'hui alors que son Renko M/W/D + D1/H1 IMP21
   restent majoritairement BULL), toute paire touchant cette devise passe
   tradable=False, meme traitement que le filtre qualite du point 3 -- pas
   un veto comme le point 5 (`daily_chg` est une donnee sur un seul jour,
   trop bruitee/volatile pour justifier d'exclure la paire du message: il
   s'agit le plus souvent d'un simple pullback dans une tendance etablie).

Tout le reste (fetch TradingView, replay Renko/PSAR/IMP, suivi de session
jour/cumul, plomberie Telegram) est repris tel quel depuis imp_trend_29pairs.py
(V1) -- aucune duplication, pour que les deux versions restent comparables
sur la meme mecanique de fond et ne divergent que sur le scoring.

Fichiers d'etat entierement separes de V1 (imp_trend_*_v2.json) pour que les
deux tournent en parallele sans interference, en vue de comparer leurs
performances sur la duree."""

from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from imp_trend_29pairs import (
    PAIRS_29,
    PARIS,
    compute_pair,
    CURRENCY_SNAPSHOT_MAX_AGE,
    currency_index_diverges,
    directional_average_confirms,
    fetch_or_load_currency_data,
    pair_touches_a_divergent_currency,
    screening_votes,
    send_telegram_message,
    session_lines,
    telegram_window_is_open,
    update_session_tracking,
)

MIN_AVG_SAMPLE = 5  # en dessous, une moyenne D1/H1 n'est ni confirmee ni infirmee
VOTE_THRESHOLD = 5  # sur 6 votes de direction (les 5 d'origine + CURRENCY_INDEX)


def _pair_currencies(pair: str) -> tuple[str, str]:
    """Devise de base / devise cotee -- XAUUSD suit la meme convention
    (base="XAU", quote="USD")."""
    return pair[:3], pair[3:]


def select_aligned_pairs_v2(
    results: list[dict],
    index_by_currency: dict[str, dict] | None = None,
    imp_by_currency: dict[str, dict] | None = None,
) -> list[dict]:
    """6 votes de direction reellement distincts (au lieu des 11 de V1, cf.
    module docstring point 1): RENKO_M, RENKO_W, RENKO_D, D1_IMP21, H1_IMP21,
    CURRENCY_INDEX (cf. imp_trend_29pairs.currency_index_vote -- confirme la
    direction quand la devise de base est plus forte que la devise cotee,
    d'apres les 8 indices devises de PAIRE_CHECK). Retenue si >= VOTE_THRESHOLD
    votes s'accordent, avec les memes garde-fous Renko que V1 (RENKO_D doit
    matcher, >= 2/3 Renko alignes).

    Chaque paire retenue porte un flag `tradable`: False si le filtre de
    qualite (moyennes D1/H1 dans le sens de la direction, cf.
    `directional_average_confirms`, uniquement quand l'echantillon est
    suffisant -- MIN_AVG_SAMPLE) echoue totalement alors qu'il etait
    applicable au moins une fois. Le suivi de position (cf. `main`) ignore
    les paires non tradables; elles restent listees dans le message."""
    selected = []
    for result in results:
        votes = screening_votes(result, index_by_currency, imp_by_currency)
        if votes["D1_IMP21"] == "NEUTRAL" or votes["H1_IMP21"] == "NEUTRAL":
            continue

        direction_votes = {
            "RENKO_M": votes["RENKO_M"],
            "RENKO_W": votes["RENKO_W"],
            "RENKO_D": votes["RENKO_D"],
            "D1_IMP21": votes["D1_IMP21"],
            "H1_IMP21": votes["H1_IMP21"],
            "CURRENCY_INDEX": votes["CURRENCY_INDEX"],
        }
        bull_votes = sum(v == "BULL" for v in direction_votes.values())
        bear_votes = sum(v == "BEAR" for v in direction_votes.values())
        direction = (
            "BULL" if bull_votes >= VOTE_THRESHOLD
            else "BEAR" if bear_votes >= VOTE_THRESHOLD
            else None
        )
        if direction is None:
            continue
        if currency_index_diverges(direction_votes, direction):
            continue
        confirmations = bull_votes if direction == "BULL" else bear_votes

        renko_confirmations = sum(
            direction_votes[f"RENKO_{tf}"] == direction for tf in ("M", "W", "D")
        )
        if direction_votes["RENKO_D"] != direction or renko_confirmations < 2:
            continue

        quality_checks = []
        for chart in ("D1", "H1"):
            sample_n = (
                result[chart]["bull_avg_n"] if direction == "BULL"
                else result[chart]["bear_avg_n"]
            )
            if sample_n is None or sample_n < MIN_AVG_SAMPLE:
                continue  # echantillon trop faible: exclu du filtre, ni pour ni contre
            quality_checks.append(directional_average_confirms(result, chart, direction))
        quality_applicable = len(quality_checks)
        quality_confirmations = sum(quality_checks)
        tradable = not (quality_applicable > 0 and quality_confirmations == 0)

        rank_tier = 1 if confirmations == 6 else 2
        rank_reason = f"{confirmations}/6"
        if quality_applicable == 0:
            rank_reason += " (qualite n/a)"
        elif not tradable:
            rank_reason += f" MAIS QUALITE {direction} 0/{quality_applicable}"

        # Devise en interne contradictoire (mouvement du jour vs consensus
        # structurel, cf. `pair_touches_a_divergent_currency`) -- meme
        # traitement que le filtre qualite ci-dessus: non tradable plutot
        # qu'exclue, reste affichee/loggee mais retiree du suivi de position.
        if pair_touches_a_divergent_currency(result["pair"], index_by_currency, imp_by_currency):
            tradable = False
            rank_reason += " MAIS DEVISE JOUR/CONSENSUS CONTRADICTOIRE"

        selected.append({
            "pair": result["pair"],
            "direction": direction,
            "confirmations": confirmations,
            "renko_confirmations": renko_confirmations,
            "quality_applicable": quality_applicable,
            "quality_confirmations": quality_confirmations,
            "tradable": tradable,
            "rank_tier": rank_tier,
            "rank_reason": rank_reason,
            **direction_votes,
        })
    return sorted(
        selected,
        key=lambda item: (
            0 if item["direction"] == "BULL" else 1,
            item["rank_tier"],
            0 if item["tradable"] else 1,
            item["pair"],
        ),
    )


def currency_exposure_lines(tradable_selected: list[dict]) -> list[str]:
    """Section `EXPOSITION DEVISES`: pour chaque devise touchee par au moins
    une paire *tradable*, combien de positions l'achetent vs. la vendent --
    rend visible la concentration qu'un format une-ligne-par-paire masque
    (cf. module docstring point 4: 6 paires JPY en quote sur 8 retenues, ce
    n'est pas 8 paris independants). Ne filtre rien, juste informatif."""
    counts: dict[str, dict[str, int]] = {}
    for item in tradable_selected:
        base, quote = _pair_currencies(item["pair"])
        bought, sold = (base, quote) if item["direction"] == "BULL" else (quote, base)
        counts.setdefault(bought, {"achetee": 0, "vendue": 0})["achetee"] += 1
        counts.setdefault(sold, {"achetee": 0, "vendue": 0})["vendue"] += 1
    if not counts:
        return []
    lines = ["📊 EXPOSITION DEVISES"]
    for currency, c in sorted(counts.items(), key=lambda kv: -(kv[1]["achetee"] + kv[1]["vendue"])):
        if c["achetee"] and c["vendue"]:
            detail = f"achetée: {c['achetee']} / vendue: {c['vendue']}"
        elif c["achetee"]:
            detail = f"{c['achetee']} position{'s' if c['achetee'] > 1 else ''} (achetée)"
        else:
            detail = f"{c['vendue']} position{'s' if c['vendue'] > 1 else ''} (vendue)"
        lines.append(f"{currency}: {detail}")
    return lines


def print_selection_v2(selected: list[dict]) -> None:
    print("\nSELECTION V2 -- 5/6 CRITERES DE DIRECTION")
    if not selected:
        print("Aucune paire retenue.")
        return
    for direction in ("BULL", "BEAR"):
        rows = [item for item in selected if item["direction"] == direction]
        if not rows:
            continue
        print(f"\n{direction}")
        display = [{
            "RANG": item["rank_tier"],
            "PAIR": item["pair"],
            "SCORE": f'{item["confirmations"]}/6',
            "TRADABLE": "oui" if item["tradable"] else "NON",
            "QUALITE": f'{item["quality_confirmations"]}/{item["quality_applicable"]}'
                       if item["quality_applicable"] else "n/a",
            "CRITERE": item["rank_reason"],
            "M": item["RENKO_M"],
            "W": item["RENKO_W"],
            "D": item["RENKO_D"],
            "D1 IMP21": item["D1_IMP21"],
            "H1 IMP21": item["H1_IMP21"],
            "IDX": item["CURRENCY_INDEX"],
        } for item in rows]
        print(pd.DataFrame(display).to_string(index=False))


def build_telegram_message_v2(
    selected: list[dict],
    session_state: dict | None = None,
    now: datetime | None = None,
) -> str:
    """Meme charpente que `build_telegram_message` (V1): titre, liste de
    paires, sessions, horodatage -- avec une difference: les paires non
    tradables (cf. `select_aligned_pairs_v2`) sont marquees "(non tradée)".

    L'exposition devises (cf. `currency_exposure_lines`) n'est PAS incluse
    ici sur demande explicite de l'utilisateur (pas d'interet pour lui sur
    Telegram) -- la fonction reste disponible pour un usage console/log."""
    timestamp = (now or datetime.now(timezone.utc)).astimezone(PARIS)
    ordered = sorted(
        selected,
        key=lambda item: (
            item["rank_tier"],
            -item["confirmations"],
            item["pair"],
        ),
    )
    lines = ["📊 IMP TREND V2", ""]
    if ordered:
        for item in ordered:
            marker = "🟢" if item["direction"] == "BULL" else "🔴"
            suffix = "" if item["tradable"] else " (non tradée)"
            lines.append(f'{marker}{item["pair"]}{suffix}')
    else:
        lines.append("Aucune paire filtrée")

    extra = session_lines(session_state)
    if extra:
        lines.append("")
        lines.extend(extra)
    lines.extend(["", f"⏰ {timestamp:%Y-%m-%d %H:%M} Paris"])
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="IMP TREND V2 -- scoring revu, 29 paires OANDA")
    parser.add_argument("--pairs", nargs="+", default=PAIRS_29, help="Paires a traiter")
    parser.add_argument("--h1-candles", type=int, default=5000)
    parser.add_argument("--d1-candles", type=int, default=2500)
    parser.add_argument("--renko-bricks", type=int, default=2500)
    parser.add_argument("--atr-length", type=int, default=14)
    parser.add_argument("--max-streak", type=int, default=50)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument(
        "--index-candles", type=int, default=300,
        help="Bougies/briques M/W/D par indice devise pour le vote CURRENCY_INDEX.",
    )
    parser.add_argument(
        "--currency-snapshot", type=str, default="currency_snapshot.json",
        help="Fichier partage entre V1/V2/paire_check.py pour reutiliser un fetch devise recent au lieu de "
             "refetcher (cf. imp_trend_29pairs.fetch_or_load_currency_data). Vide pour desactiver.",
    )
    parser.add_argument(
        "--currency-snapshot-max-age", type=int, default=CURRENCY_SNAPSHOT_MAX_AGE,
        help="Age maximum en secondes d'un instantane devise reutilisable.",
    )
    parser.add_argument("--json", type=Path, default=Path("imp_trend_29pairs_v2.json"))
    parser.add_argument("--selection-json", type=Path, default=Path("imp_trend_selection_v2.json"))
    parser.add_argument("--sessions-state", type=Path, default=Path("imp_trend_sessions_state_v2.json"))
    parser.add_argument("--telegram", action="store_true", help="Envoyer les paires filtrees sur Telegram")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pairs = [pair.upper() for pair in args.pairs]
    snapshot_path = Path(args.currency_snapshot) if args.currency_snapshot else None
    index_by_currency, imp_by_currency = fetch_or_load_currency_data(
        snapshot_path, args.currency_snapshot_max_age,
        args.atr_length, args.index_candles, args.max_streak,
        args.h1_candles, args.d1_candles, args.renko_bricks, args.atr_length, args.max_streak,
    )
    results: list[dict] = []
    errors: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(
                compute_pair, pair, args.h1_candles, args.d1_candles,
                args.renko_bricks, args.atr_length, args.max_streak,
            ): pair
            for pair in pairs
        }
        for future in as_completed(futures):
            pair = futures[future]
            try:
                results.append(future.result())
                print(f"{pair}: OK")
            except Exception as exc:
                errors.append((pair, str(exc)))
                print(f"{pair}: ERROR - {exc}")

    order = {pair: index for index, pair in enumerate(pairs)}
    results.sort(key=lambda item: order[item["pair"]])
    if results:
        selected = select_aligned_pairs_v2(results, index_by_currency, imp_by_currency)
        print_selection_v2(selected)
        tradable_selected = [item for item in selected if item["tradable"]]
        for line in currency_exposure_lines(tradable_selected):
            print(line)  # console/log seulement, pas dans le message Telegram
        session_state = update_session_tracking(args.sessions_state, tradable_selected, results)

        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.selection_json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        args.selection_json.write_text(json.dumps(selected, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nJSON: {args.json.resolve()}")
        print(f"Selection JSON: {args.selection_json.resolve()}")
        print(f"Suivi sessions: {args.sessions_state.resolve()}")

        if args.telegram and telegram_window_is_open():
            telegram_message = build_telegram_message_v2(selected, session_state)
            print("\n" + telegram_message)
            send_telegram_message(telegram_message)
        elif args.telegram:
            print("Telegram: plage silencieuse 00:00-05:59 Europe/Paris, envoi ignore.")
    if errors:
        print("\nErrors:")
        for pair, error in errors:
            print(f"  {pair}: {error}")
    return 1 if errors and not results else 0


if __name__ == "__main__":
    raise SystemExit(main())
