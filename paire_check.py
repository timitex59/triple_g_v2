"""PAIRE_CHECK: une section INDEX CHG%D identique a celle de FULL MOMENTUM
(les 8 devises), suivie d'une section BEST PAIRE (devise verte la plus
forte + devise rouge la plus forte du classement INDEX CHG%D, cf.
`best_pair_lines`), puis d'une ligne par paire, billes seules
(`EURUSD 🟢🟢🟢`), a partir des memes signaux INDEX/06h/RUN que
`pair_check_lines` dans renko_full_alignment_29pairs.py (cf.
`pair_check_signals`), pour un jeu de paires choisies -- sans lancer le
scan complet 29 paires de ce dernier.

- INDEX: renko M/W/D complet (cf. `compute_asset_score`) pour les 8 devises,
  comme FULL MOMENTUM -- alimente a la fois la section INDEX CHG%D affichee
  et le segment INDEX de chaque ligne PAIRES.
- 06h / RUN: close Daily (prix + CHG%D) pour le sous-ensemble des 28 paires
  OANDA du scanner qui impliquent au moins une devise des paires suivies --
  assez pour que `currency_pip_sum` retrouve ses 7 paires par devise, sans
  fetcher les 28.

L'etat de reference 06h/run precedent (`update_price_trends`) est persiste
dans son propre fichier, independant de celui de renko_full_alignment_29pairs
(meme mecanisme, cf. ce module, mais un cycle de run distinct).

2026-08-26: chaque ligne INDEX CHG%D porte en plus une boule de consensus
🟢/🔴/⚪ (cf. `currency_consensus_ball`) resumant les 5 votes reellement
distincts de CETTE devise (Renko M/W/D + D1 IMP21 + H1 IMP21, calcules par
`imp_trend_29pairs.compute_currency_imp`/`fetch_currency_imp_rows` -- le
meme moteur Renko+PSAR+IMP21 que pour les 29 paires, rejoue sur le symbole
TradingView de l'indice, TVC:DXY etc., pas juste son score Renko-streak
utilise pour le classement de la section). Les 6 moyennes D1/H1 ne comptent
pas dans ce consensus, meme raisonnement que celui deja applique aux paires
en V2 (cf. imp_trend_29pairs_v2.py): elles derivent toutes de la meme serie
de signaux que leur IMP21, 4 votes qui basculent ensemble ne sont pas 4
confirmations independantes. Une premiere version affichait les 11 boules
brutes avant cette correction (cf. historique git). Ajoute ~40 appels
TradingView par run (5 par devise x 8) en plus des ~32 deja utilises pour
INDEX."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from ichimoku_v4 import fetch_tv_ohlc, send_telegram_message
from imp_trend_29pairs import fetch_currency_imp_rows, screening_votes
from renko_full_alignment_29pairs import (
    FOREX_INDEX_ASSETS,
    FOREX_PAIR_ASSETS,
    PARIS_TZ,
    _pair_currencies,
    _strength_status_lines,
    all_index_status_rows,
    compute_asset_score,
    load_price_trend_state,
    pair_check_signals,
    save_price_trend_state,
    update_price_trends,
)

VOTE_BALL = {"BULL": "🟢", "BEAR": "🔴", "NEUTRAL": "⚪"}

# Modifier cette liste (ou passer --pairs) pour suivre d'autres paires.
DEFAULT_PAIRS = ["EURUSD", "EURJPY", "USDJPY", "CHFJPY"]
STATE_FILE = Path("paire_check_price_trend_state.json")


def needed_currencies(pairs: list[str]) -> set[str]:
    """Devises impliquees par `pairs` (base + quote de chacune)."""
    currencies: set[str] = set()
    for pair in pairs:
        parsed = _pair_currencies(pair)
        if parsed:
            currencies.update(parsed)
    return currencies


def needed_helper_pairs(currencies: set[str]) -> list[str]:
    """Sous-ensemble des 28 paires OANDA du scanner qui impliquent au moins
    une devise de `currencies` -- assez pour que `currency_pip_sum` retrouve
    ses 7 paires par devise, sans fetcher les 28."""
    helper_pairs = []
    for asset in FOREX_PAIR_ASSETS:
        pair = str(asset["pair"])
        parsed = _pair_currencies(pair)
        if parsed and currencies & set(parsed):
            helper_pairs.append(pair)
    return helper_pairs


def fetch_pair_row(pair: str) -> dict | None:
    """Close Daily live + CHG%D pour `pair`: assez pour `update_price_trends`
    (pips depuis 06h / depuis le run precedent), sans le renko M/W/D complet
    -- inutile ici, seul INDEX en a besoin, pour les devises seulement."""
    tv_symbol = f"OANDA:{pair}"
    df = fetch_tv_ohlc(tv_symbol, "D", 50)
    if df is None or df.empty:
        print(f"{pair}: pas de donnees")
        return None
    live_price = float(df["close"].iloc[-1])
    prev_close = float(df["close"].iloc[-2]) if len(df) >= 2 else None
    daily_chg = (
        ((live_price - prev_close) / prev_close * 100.0)
        if prev_close is not None and prev_close != 0 else None
    )
    return {
        "pair": pair,
        "tv_symbol": tv_symbol,
        "asset_type": "PAIR",
        "live_price": live_price,
        "daily_chg": daily_chg,
    }


def fetch_index_rows(length: int, candles: int, max_streak: int) -> list[dict]:
    """Renko M/W/D complet (cf. `compute_asset_score`) pour les 8 devises --
    alimente a la fois la section INDEX CHG%D et le segment INDEX de chaque
    ligne PAIRES (via `index_by_currency`, construit par l'appelant)."""
    rows: list[dict] = []
    for asset in FOREX_INDEX_ASSETS:
        row = compute_asset_score(asset, length, candles, max_streak)
        if row is not None:
            rows.append(row)
        else:
            print(f"{asset.get('currency')} ({asset['pair']}): pas de donnees indice")
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PAIRE_CHECK: verdict INDEX/06h/RUN pour un jeu de paires choisies.",
    )
    parser.add_argument(
        "--pairs", nargs="+", default=DEFAULT_PAIRS,
        help=f"Paires a verifier (par defaut: {' '.join(DEFAULT_PAIRS)}).",
    )
    parser.add_argument("--length", type=int, default=14, help="ATR Renko length.")
    parser.add_argument(
        "--candles", type=int, default=300,
        help="Nombre de bougies/briques recuperees par symbole/timeframe pour INDEX.",
    )
    parser.add_argument(
        "--max-streak", type=int, default=50,
        help="Plafond du streak de briques Renko consecutives pour INDEX.",
    )
    parser.add_argument(
        "--imp-h1-candles", type=int, default=5000,
        help="Bougies H1 par devise pour la boule de consensus de INDEX CHG%%D (cf. currency_consensus_ball).",
    )
    parser.add_argument(
        "--imp-d1-candles", type=int, default=2500,
        help="Bougies D1 par devise pour les boules Renko/IMP21 de INDEX CHG%%D.",
    )
    parser.add_argument(
        "--imp-renko-bricks", type=int, default=2500,
        help="Briques Renko M/W/D par devise pour les boules Renko/IMP21 de INDEX CHG%%D.",
    )
    parser.add_argument(
        "--state-file", default=str(STATE_FILE),
        help="Fichier d'etat JSON (reference 06h + run precedent), propre a ce script.",
    )
    parser.add_argument(
        "--telegram", action="store_true",
        help="Envoie le resultat sur Telegram. Par defaut, affichage seul.",
    )
    return parser.parse_args()


def pair_check_compact_line(
    pair: str, rows_by_pair: dict[str, dict], price_trends: dict[str, dict],
    index_by_currency: dict[str, dict],
) -> str | None:
    """`"{PAIR} {billes}"` (ex. `EURUSD 🟢🟢🟢`), billes seules dans l'ordre
    INDEX/06h/RUN, sans label -- cf. `pair_check_signals` pour leur sens.
    None si aucun signal n'est exploitable pour `pair`."""
    signals = pair_check_signals(pair, rows_by_pair, price_trends, index_by_currency)
    if not signals:
        return None
    return f"{pair} " + "".join(icon for _label, icon in signals)


CURRENCY_CONSENSUS_THRESHOLD = 4  # sur 5 votes reels -- cf. currency_consensus_ball


def currency_consensus_ball(currency_row: dict) -> str:
    """Boule unique 🟢/🔴/⚪ resumant une devise, sur les 5 votes reellement
    distincts de `imp_trend_29pairs.screening_votes` calcules pour cette
    devise seule (RENKO_M, RENKO_W, RENKO_D, D1_IMP21, H1_IMP21) --
    CURRENCY_INDEX exclu (n'a pas de sens pour une devise isolee, il compare
    deux devises entre elles).

    Les 6 moyennes D1/H1 (toutes/bull/bear) ne comptent PAS: elles derivent
    toutes de la meme serie de signaux que leur IMP21 respectif -- un
    historique qui penche BULL fait basculer les 4 votes D1 ensemble, ce
    n'est pas 4 confirmations independantes. Meme constat, meme correction
    que celle deja appliquee aux paires en V2 (cf. module docstring point 1
    de imp_trend_29pairs_v2.py) -- juste que la premiere version de cette
    fonction affichait encore les 11 boules brutes avant cette correction.

    BULL/BEAR si >= CURRENCY_CONSENSUS_THRESHOLD des 5 votes s'accordent,
    NEUTRAL sinon (pas de consensus net)."""
    votes = screening_votes(currency_row)
    keys = ("RENKO_M", "RENKO_W", "RENKO_D", "D1_IMP21", "H1_IMP21")
    bull = sum(votes[key] == "BULL" for key in keys)
    bear = sum(votes[key] == "BEAR" for key in keys)
    if bull >= CURRENCY_CONSENSUS_THRESHOLD:
        return VOTE_BALL["BULL"]
    if bear >= CURRENCY_CONSENSUS_THRESHOLD:
        return VOTE_BALL["BEAR"]
    return VOTE_BALL["NEUTRAL"]


def index_chg_lines(
    index_rows: list[dict], imp_by_currency: dict[str, dict] | None = None,
) -> list[str]:
    """Section `💱 INDEX CHG%D`, identique a celle de FULL MOMENTUM (les 8
    devises, memes icones/valeurs, cf. `all_index_status_rows` et
    `_strength_status_lines`). Vide si `index_rows` l'est.

    Avec `imp_by_currency` (cf. `fetch_currency_imp_rows`), chaque ligne
    reçoit en plus la boule de consensus de sa devise (cf.
    `currency_consensus_ball`) -- absent pour une devise dont le fetch a
    echoue, la ligne reste alors sans boule plutot que d'echouer."""
    sorted_rows = all_index_status_rows(index_rows)
    if not sorted_rows:
        return []
    base_lines = _strength_status_lines(sorted_rows)
    if not imp_by_currency:
        return ["💱 INDEX CHG%D", *base_lines]
    lines = ["💱 INDEX CHG%D"]
    for row, line in zip(sorted_rows, base_lines):
        currency_row = imp_by_currency.get(str(row.get("currency")))
        lines.append(line if currency_row is None else f"{line} {currency_consensus_ball(currency_row)}")
    return lines


KNOWN_PAIR_CODES = {str(asset["pair"]) for asset in FOREX_PAIR_ASSETS}  # les 28 paires OANDA reelles


def best_pair_name(index_rows: list[dict]) -> str | None:
    """Associe la devise 🔴 au score le plus eleve a la devise 🟢 au score le
    plus eleve, dans l'ordre deja fourni par `all_index_status_rows` (score
    d'intensite decroissant, cf. `_strength_sort_key`) -- donc simplement le
    premier 🔴 et le premier 🟢 rencontres. La verte (forte) sert de base, la
    rouge (faible) de quote, convention "acheter le fort, vendre le faible"
    deja utilisee par `currency_spread`. None si aucune devise rouge ou
    aucune verte n'est exploitable (CHG%D manquant pour l'une des deux).

    Le nom retourne est toujours l'une des 28 paires OANDA reelles (cf.
    `KNOWN_PAIR_CODES`): {forte}{faible} si elle existe, sinon {faible}{forte}
    -- les 8 devises couvrent chaque combinaison de 2 devises exactement une
    fois parmi les 28 paires, jamais dans les deux sens a la fois, donc l'un
    des deux existe forcement (None si aucun des deux, en pratique jamais
    atteint). Inverser l'ordre ne change pas le sens fort/faible affiche
    ensuite: `pair_check_signals` compare directement base et cotee, quel
    que soit lequel des deux est la devise forte -- pas besoin d'inverser une
    direction, juste de ne pas inventer un symbole qui n'existe pas."""
    sorted_rows = all_index_status_rows(index_rows)
    strong = next(
        (row for row in sorted_rows if isinstance(row.get("daily_chg"), (int, float))
         and row["daily_chg"] > 0),
        None,
    )
    weak = next(
        (row for row in sorted_rows if isinstance(row.get("daily_chg"), (int, float))
         and row["daily_chg"] < 0),
        None,
    )
    if strong is None or weak is None:
        return None
    forward = f"{strong['currency']}{weak['currency']}"
    if forward in KNOWN_PAIR_CODES:
        return forward
    reverse = f"{weak['currency']}{strong['currency']}"
    return reverse if reverse in KNOWN_PAIR_CODES else None


def best_pair_lines(
    pair: str | None, rows_by_pair: dict[str, dict], price_trends: dict[str, dict],
    index_by_currency: dict[str, dict],
) -> list[str]:
    """Section `🏆 BEST PAIRE`: `pair` (cf. `best_pair_name`) avec ses billes
    INDEX/06h/RUN, meme rendu que les lignes PAIRES (cf.
    `pair_check_compact_line`). Vide si pas de `pair`, ou si ses billes ne
    sont pas exploitables (ex. devise absente des paires-support fetchees --
    voir `main`, qui inclut les devises de `best_pair_name` dans le fetch)."""
    if pair is None:
        return []
    line = pair_check_compact_line(pair, rows_by_pair, price_trends, index_by_currency)
    if line is None:
        return []
    return ["🏆 BEST PAIRE", line]


def build_message(pairs: list[str], rows_by_pair: dict[str, dict],
                  price_trends: dict[str, dict], index_by_currency: dict[str, dict],
                  now: datetime, imp_by_currency: dict[str, dict] | None = None) -> tuple[str, bool]:
    """Assemble le message PAIRE_CHECK. Renvoie (message, has_content): le 2e
    indique si au moins une paire a produit une ligne exploitable, pour
    conditionner l'envoi Telegram cote `main` (la section INDEX CHG%D seule
    ne suffit pas a envoyer -- c'est un rappel, pas le coeur du message)."""
    pair_lines = [
        line for line in (
            pair_check_compact_line(pair, rows_by_pair, price_trends, index_by_currency)
            for pair in pairs
        )
        if line is not None
    ]

    lines = ["📐 PAIRE_CHECK", ""]
    index_rows = list(index_by_currency.values())
    index_lines = index_chg_lines(index_rows, imp_by_currency)
    if index_lines:
        lines.extend(index_lines)
        lines.append("")
    best_lines = best_pair_lines(
        best_pair_name(index_rows), rows_by_pair, price_trends, index_by_currency,
    )
    if best_lines:
        lines.extend(best_lines)
        lines.append("")
    if pair_lines:
        lines.extend(["PAIRES", *pair_lines])
        lines.append("")
    lines.append(f"⏰ {now:%Y-%m-%d %H:%M} Paris")
    return "\n".join(lines), bool(pair_lines)


def main() -> int:
    args = parse_args()
    now = datetime.now(PARIS_TZ)

    # INDEX d'abord: le nom du BEST PAIRE (devises dynamiques, cf.
    # `best_pair_name`) doit etre connu avant de choisir les paires-support a
    # fetcher, pour que ses billes 06h/RUN soient exploitables comme celles
    # de PAIRES -- pas seulement son segment INDEX.
    index_rows = fetch_index_rows(args.length, args.candles, args.max_streak)
    index_by_currency = {str(row["currency"]): row for row in index_rows}
    best_pair = best_pair_name(index_rows)
    imp_by_currency = fetch_currency_imp_rows(
        args.imp_h1_candles, args.imp_d1_candles, args.imp_renko_bricks, args.length, args.max_streak,
    )

    currencies = needed_currencies(args.pairs)
    best_pair_currencies = _pair_currencies(best_pair) if best_pair else None
    if best_pair_currencies:
        currencies |= set(best_pair_currencies)
    helper_pairs = needed_helper_pairs(currencies)
    rows = [row for row in (fetch_pair_row(pair) for pair in helper_pairs) if row is not None]
    rows_by_pair = {str(row["pair"]): row for row in rows}

    state = load_price_trend_state(args.state_file)
    new_state, price_trends = update_price_trends(state, rows, now)
    save_price_trend_state(args.state_file, new_state)

    message, has_content = build_message(
        args.pairs, rows_by_pair, price_trends, index_by_currency, now, imp_by_currency,
    )
    print(message)
    if args.telegram:
        if not has_content:
            print("Aucune paire exploitable: message Telegram ignoré.")
        else:
            send_telegram_message(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
