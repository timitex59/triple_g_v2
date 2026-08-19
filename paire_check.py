"""PAIRE_CHECK: une section INDEX CHG%D identique a celle de FULL MOMENTUM
(les 8 devises) suivie d'une ligne par paire, billes seules (`EURUSD 🟢🟢🟢`),
a partir des memes signaux INDEX/06h/RUN que `pair_check_lines` dans
renko_full_alignment_29pairs.py (cf. `pair_check_signals`), pour un jeu de
paires choisies -- sans lancer le scan complet 29 paires de ce dernier.

- INDEX: renko M/W/D complet (cf. `compute_asset_score`) pour les 8 devises,
  comme FULL MOMENTUM -- alimente a la fois la section INDEX CHG%D affichee
  et le segment INDEX de chaque ligne PAIRES.
- 06h / RUN: close Daily (prix + CHG%D) pour le sous-ensemble des 28 paires
  OANDA du scanner qui impliquent au moins une devise des paires suivies --
  assez pour que `currency_pip_sum` retrouve ses 7 paires par devise, sans
  fetcher les 28.

L'etat de reference 06h/run precedent (`update_price_trends`) est persiste
dans son propre fichier, independant de celui de renko_full_alignment_29pairs
(meme mecanisme, cf. ce module, mais un cycle de run distinct)."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from ichimoku_v4 import fetch_tv_ohlc, send_telegram_message
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

# Modifier cette liste (ou passer --pairs) pour suivre d'autres paires.
DEFAULT_PAIRS = ["EURUSD", "CHFJPY", "USDJPY"]
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


def index_chg_lines(index_rows: list[dict]) -> list[str]:
    """Section `💱 INDEX CHG%D`, identique a celle de FULL MOMENTUM (les 8
    devises, memes icones/valeurs, cf. `all_index_status_rows` et
    `_strength_status_lines`). Vide si `index_rows` l'est."""
    sorted_rows = all_index_status_rows(index_rows)
    if not sorted_rows:
        return []
    return ["💱 INDEX CHG%D", *_strength_status_lines(sorted_rows)]


def build_message(pairs: list[str], rows_by_pair: dict[str, dict],
                  price_trends: dict[str, dict], index_by_currency: dict[str, dict],
                  now: datetime) -> tuple[str, bool]:
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
    index_lines = index_chg_lines(list(index_by_currency.values()))
    if index_lines:
        lines.extend(index_lines)
        lines.append("")
    if pair_lines:
        lines.extend(["PAIRES", *pair_lines])
        lines.append("")
    lines.append(f"⏰ {now:%Y-%m-%d %H:%M} Paris")
    return "\n".join(lines), bool(pair_lines)


def main() -> int:
    args = parse_args()
    now = datetime.now(PARIS_TZ)

    currencies = needed_currencies(args.pairs)
    helper_pairs = needed_helper_pairs(currencies)
    rows = [row for row in (fetch_pair_row(pair) for pair in helper_pairs) if row is not None]
    rows_by_pair = {str(row["pair"]): row for row in rows}

    state = load_price_trend_state(args.state_file)
    new_state, price_trends = update_price_trends(state, rows, now)
    save_price_trend_state(args.state_file, new_state)

    index_rows = fetch_index_rows(args.length, args.candles, args.max_streak)
    index_by_currency = {str(row["currency"]): row for row in index_rows}

    message, has_content = build_message(
        args.pairs, rows_by_pair, price_trends, index_by_currency, now,
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
