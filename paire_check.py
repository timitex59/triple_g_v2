"""PAIRE_CHECK: une section INDEX CHG%D identique a celle de FULL MOMENTUM
(les 8 devises), suivie d'une section BEST PAIRE (une ligne par devise
doublement validee BULL associee a une devise doublement validee BEAR, cf.
`best_pair_names`), puis d'une ligne par paire, billes seules
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
INDEX.

2026-08-27: BEST PAIRE simplifie a la demande de l'utilisateur --
`best_pair_names` (qui remplace l'ancien `best_pair_name`) n'associe plus la
devise au CHG%D le plus fort et celle au CHG%D le plus faible quel que soit
leur consensus, mais uniquement des devises "doublement validees": jour ET
consensus d'accord entre eux (cf. `aligned_currency_names`), en sens inverse
l'une de l'autre (une confirmee forte, l'autre confirmee faible) -- une
devise contradictoire (comme GBP jour rouge/consensus vert) ou indecise
(un ⚪) n'est plus jamais retenue comme "forte" ou "faible" juste parce que
son CHG%D etait le plus extreme. Peut desormais produire 0, 1 ou plusieurs
BEST PAIRE selon le nombre de devises doublement validees de chaque cote --
une ligne par combinaison.

2026-08-27 (plus tard): section(s) `🏆 BEST PAIRE {devise}` dediee(s) (cf.
`--focus-currency`, `DEFAULT_FOCUS_CURRENCIES = ["JPY"]`), ajoutee(s) apres
la section BEST PAIRE generale. `best_pair_names(..., only_currency=devise)`
restreint aux combinaisons impliquant CETTE devise -- elle doublement BULL
associee a chaque devise doublement BEAR, ou l'inverse -- meme logique de
double confirmation que la section generale, juste filtree sur une devise
precise plutot que sur les 8."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from ichimoku_v4 import fetch_tv_ohlc, send_telegram_message
from imp_trend_29pairs import (
    CURRENCY_SNAPSHOT_MAX_AGE,
    currency_consensus_status,
    currency_daily_status,
    fetch_or_load_currency_data,
)
from renko_full_alignment_29pairs import (
    FOREX_PAIR_ASSETS,
    PARIS_TZ,
    _pair_currencies,
    _strength_status_lines,
    all_index_status_rows,
    load_price_trend_state,
    pair_check_signals,
    save_price_trend_state,
    update_price_trends,
)

VOTE_BALL = {"BULL": "🟢", "BEAR": "🔴", "NEUTRAL": "⚪"}

# Modifier cette liste (ou passer --pairs) pour suivre d'autres paires.
DEFAULT_PAIRS = ["EURUSD", "EURJPY", "USDJPY", "CHFJPY"]
# Devises avec leur propre section "🏆 BEST PAIRE {devise}" (ou --focus-currency).
DEFAULT_FOCUS_CURRENCIES = ["JPY"]
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PAIRE_CHECK: verdict INDEX/06h/RUN pour un jeu de paires choisies.",
    )
    parser.add_argument(
        "--pairs", nargs="+", default=DEFAULT_PAIRS,
        help=f"Paires a verifier (par defaut: {' '.join(DEFAULT_PAIRS)}).",
    )
    parser.add_argument(
        "--focus-currency", nargs="*", default=DEFAULT_FOCUS_CURRENCIES,
        help=f"Devises avec leur propre section BEST PAIRE dediee (par defaut: "
             f"{' '.join(DEFAULT_FOCUS_CURRENCIES)}). Vide (--focus-currency sans argument) pour desactiver.",
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
        "--currency-snapshot", type=str, default="currency_snapshot.json",
        help="Fichier partage entre V1/V2/paire_check.py pour reutiliser un fetch devise recent au lieu de "
             "refetcher (cf. imp_trend_29pairs.fetch_or_load_currency_data). Vide pour desactiver.",
    )
    parser.add_argument(
        "--currency-snapshot-max-age", type=int, default=CURRENCY_SNAPSHOT_MAX_AGE,
        help="Age maximum en secondes d'un instantane devise reutilisable.",
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


def currency_consensus_ball(currency_row: dict) -> str:
    """Boule unique 🟢/🔴/⚪ resumant une devise -- rendu de
    `imp_trend_29pairs.currency_consensus_status` (le calcul lui-meme, cf. ce
    module pour son detail: 5 votes reellement distincts, seuil
    CURRENCY_CONSENSUS_THRESHOLD). Deplace dans imp_trend_29pairs.py le
    2026-08-27 pour etre reutilisable par `pair_touches_a_divergent_currency`
    (V1/V2) sans creer d'import circulaire (paire_check.py importe deja
    imp_trend_29pairs.py, pas l'inverse)."""
    return VOTE_BALL[currency_consensus_status(currency_row)]


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


def aligned_currency_names(
    index_rows: list[dict], imp_by_currency: dict[str, dict],
) -> tuple[list[str], list[str]]:
    """Devises "doublement validees" parmi les 8: jour ET consensus d'accord
    (cf. `imp_trend_29pairs.currency_daily_status`/`currency_consensus_status`
    -- memes calculs que la boule de consensus et le mouvement du jour deja
    affiches sur chaque ligne INDEX CHG%D), dans l'ordre d'intensite deja
    utilise pour cette section (cf. `all_index_status_rows`). Exclut les
    devises contradictoires (jour/consensus opposes, cf.
    `imp_trend_29pairs.currency_diverges_from_its_own_day`) et indecises (au
    moins un ⚪) -- celles-ci ne servent plus a designer un BEST PAIRE.
    Renvoie (bull, bear): les doublement BULL, les doublement BEAR."""
    bull, bear = [], []
    for row in all_index_status_rows(index_rows):
        currency = str(row.get("currency"))
        imp_row = imp_by_currency.get(currency)
        if imp_row is None:
            continue
        daily = currency_daily_status(row.get("daily_chg"))
        consensus = currency_consensus_status(imp_row)
        if daily == "BULL" and consensus == "BULL":
            bull.append(currency)
        elif daily == "BEAR" and consensus == "BEAR":
            bear.append(currency)
    return bull, bear


def best_pair_names(
    index_rows: list[dict], imp_by_currency: dict[str, dict], only_currency: str | None = None,
) -> list[str]:
    """Un BEST PAIRE par combinaison (devise doublement BULL, devise
    doublement BEAR) -- cf. `aligned_currency_names` -- "alignees sur leurs 2
    boules, mais en sens inverse l'une de l'autre" (l'une confirmee forte,
    l'autre confirmee faible), plutot que l'ancien classement par simple
    magnitude de CHG%D qui pouvait retenir une devise contradictoire (comme
    GBP jour rouge/consensus vert) comme si elle etait une "forte" ou une
    "faible" fiable. Peut renvoyer 0, 1 ou plusieurs paires -- une par
    combinaison dont le symbole existe reellement parmi les 28 paires OANDA
    (cf. `KNOWN_PAIR_CODES`; {forte}{faible} si cet ordre existe, sinon
    {faible}{forte} -- meme raisonnement que l'ancien `best_pair_name`,
    cf. historique git: les 8 devises couvrent chaque combinaison de 2
    exactement une fois parmi les 28 paires, jamais les deux sens a la fois,
    et l'inverser ne change pas le sens fort/faible lu ensuite par
    `pair_check_signals`, qui compare directement base et cotee).

    `only_currency` (ex. "JPY") restreint aux combinaisons impliquant cette
    devise -- elle-meme doublement BULL associee a chaque devise doublement
    BEAR, ou l'inverse. Liste vide si `only_currency` n'est ni doublement
    BULL ni doublement BEAR (contradictoire, indecise, ou donnees absentes) --
    cf. `best_pair_lines` pour la section dediee `BEST PAIRE {devise}`."""
    bull, bear = aligned_currency_names(index_rows, imp_by_currency)
    if only_currency is not None:
        if only_currency in bull:
            bull, bear = [only_currency], bear
        elif only_currency in bear:
            bull, bear = bull, [only_currency]
        else:
            return []
    pairs = []
    for strong in bull:
        for weak in bear:
            forward = f"{strong}{weak}"
            if forward in KNOWN_PAIR_CODES:
                pairs.append(forward)
                continue
            reverse = f"{weak}{strong}"
            if reverse in KNOWN_PAIR_CODES:
                pairs.append(reverse)
    return pairs


def best_pair_lines(
    pairs: list[str], rows_by_pair: dict[str, dict], price_trends: dict[str, dict],
    index_by_currency: dict[str, dict], label: str = "🏆 BEST PAIRE",
) -> list[str]:
    """Section `{label}`: une ligne par paire de `pairs` (cf.
    `best_pair_names`), memes billes que les lignes PAIRES (cf.
    `pair_check_compact_line`). Vide si `pairs` l'est, ou si aucune de ses
    billes n'est exploitable (ex. devise absente des paires-support fetchees
    -- voir `main`, qui inclut les devises de `best_pair_names` dans le
    fetch). `label` permet une section dediee par devise (ex. "🏆 BEST PAIRE
    JPY", cf. `--focus-currency`) distincte de la section generale."""
    lines = [
        line for line in (
            pair_check_compact_line(pair, rows_by_pair, price_trends, index_by_currency)
            for pair in pairs
        )
        if line is not None
    ]
    if not lines:
        return []
    return [label, *lines]


def build_message(pairs: list[str], rows_by_pair: dict[str, dict],
                  price_trends: dict[str, dict], index_by_currency: dict[str, dict],
                  now: datetime, imp_by_currency: dict[str, dict] | None = None,
                  focus_currencies: list[str] | None = None) -> tuple[str, bool]:
    """Assemble le message PAIRE_CHECK. Renvoie (message, has_content): le 2e
    indique si au moins une paire a produit une ligne exploitable, pour
    conditionner l'envoi Telegram cote `main` (la section INDEX CHG%D seule
    ne suffit pas a envoyer -- c'est un rappel, pas le coeur du message).

    `focus_currencies` (ex. `["JPY"]`) ajoute une section `🏆 BEST PAIRE
    {devise}` par devise, apres la section BEST PAIRE generale -- cf.
    `best_pair_names(..., only_currency=devise)`."""
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
        best_pair_names(index_rows, imp_by_currency or {}), rows_by_pair, price_trends, index_by_currency,
    )
    if best_lines:
        lines.extend(best_lines)
        lines.append("")
    for currency in (focus_currencies or []):
        focus_lines = best_pair_lines(
            best_pair_names(index_rows, imp_by_currency or {}, only_currency=currency),
            rows_by_pair, price_trends, index_by_currency, label=f"🏆 BEST PAIRE {currency}",
        )
        if focus_lines:
            lines.extend(focus_lines)
            lines.append("")
    if pair_lines:
        lines.extend(["PAIRES", *pair_lines])
        lines.append("")
    lines.append(f"⏰ {now:%Y-%m-%d %H:%M} Paris")
    return "\n".join(lines), bool(pair_lines)


def main() -> int:
    args = parse_args()
    now = datetime.now(PARIS_TZ)

    # INDEX d'abord: les noms des BEST PAIRE (devises dynamiques, cf.
    # `best_pair_names`) doivent etre connus avant de choisir les
    # paires-support a fetcher, pour que leurs billes 06h/RUN soient
    # exploitables comme celles de PAIRES -- pas seulement leur segment INDEX.
    snapshot_path = Path(args.currency_snapshot) if args.currency_snapshot else None
    index_by_currency, imp_by_currency = fetch_or_load_currency_data(
        snapshot_path, args.currency_snapshot_max_age,
        args.length, args.candles, args.max_streak,
        args.imp_h1_candles, args.imp_d1_candles, args.imp_renko_bricks, args.length, args.max_streak,
    )
    index_rows = list(index_by_currency.values())
    best_pairs = best_pair_names(index_rows, imp_by_currency)
    focus_best_pairs = [
        pair
        for currency in args.focus_currency
        for pair in best_pair_names(index_rows, imp_by_currency, only_currency=currency)
    ]

    currencies = needed_currencies(args.pairs)
    for pair in [*best_pairs, *focus_best_pairs]:
        pair_currencies = _pair_currencies(pair)
        if pair_currencies:
            currencies |= set(pair_currencies)
    helper_pairs = needed_helper_pairs(currencies)
    rows = [row for row in (fetch_pair_row(pair) for pair in helper_pairs) if row is not None]
    rows_by_pair = {str(row["pair"]): row for row in rows}

    state = load_price_trend_state(args.state_file)
    new_state, price_trends = update_price_trends(state, rows, now)
    save_price_trend_state(args.state_file, new_state)

    message, has_content = build_message(
        args.pairs, rows_by_pair, price_trends, index_by_currency, now, imp_by_currency, args.focus_currency,
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
