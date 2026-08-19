"""PAIRE_CHECK: le verdict compact INDEX/06h/RUN (cf. `pair_check_lines` dans
renko_full_alignment_29pairs.py) pour un jeu de paires choisies, sans lancer
le scan complet 29 paires + 8 indices de ce dernier.

Seules les devises impliquees par les paires suivies sont recuperees:
- INDEX: renko M/W/D complet (cf. `compute_asset_score`) pour chacune de ces
  devises seulement, pas les 8.
- 06h / RUN: close Daily (prix + CHG%D) pour toutes les paires OANDA du
  scanner qui impliquent au moins une de ces devises -- assez pour que
  `currency_pip_sum` retrouve ses 7 paires par devise, sans le reste.

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
    compute_asset_score,
    load_price_trend_state,
    pair_check_lines,
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


def fetch_index_by_currency(
    currencies: set[str], length: int, candles: int, max_streak: int,
) -> dict[str, dict]:
    """Renko M/W/D complet (cf. `compute_asset_score`) pour chaque devise de
    `currencies`, indexe par devise -- alimente le segment INDEX."""
    index_by_currency: dict[str, dict] = {}
    for asset in FOREX_INDEX_ASSETS:
        currency = str(asset.get("currency") or "")
        if currency not in currencies:
            continue
        row = compute_asset_score(asset, length, candles, max_streak)
        if row is not None:
            index_by_currency[currency] = row
        else:
            print(f"{currency} ({asset['pair']}): pas de donnees indice")
    return index_by_currency


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


def build_message(pairs: list[str], rows_by_pair: dict[str, dict],
                  price_trends: dict[str, dict], index_by_currency: dict[str, dict],
                  now: datetime) -> tuple[str, bool]:
    """Assemble le message PAIRE_CHECK. Renvoie (message, has_content): le 2e
    indique si au moins une paire a produit une ligne exploitable, pour
    conditionner l'envoi Telegram cote `main`."""
    lines = ["📐 PAIRE_CHECK"]
    has_content = False
    for pair in pairs:
        check = pair_check_lines(pair, rows_by_pair, price_trends, index_by_currency)
        if not check:
            continue
        has_content = True
        lines.extend(["", *check])
    lines.extend(["", f"⏰ {now:%Y-%m-%d %H:%M} Paris"])
    return "\n".join(lines), has_content


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

    index_by_currency = fetch_index_by_currency(
        currencies, args.length, args.candles, args.max_streak,
    )

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
