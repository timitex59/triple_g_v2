#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Calcule la quantité à utiliser pour obtenir un notionnel USD identique
sur les 29 instruments du screener.

Exemple:
    python position_size_29pairs.py
    python position_size_29pairs.py --notional 1000
"""

import argparse
from typing import Iterable

from ichimoku_v4 import PAIRS_29, fetch_tv_ohlc


USD_DIRECT_SYMBOLS = {
    "AUD": "AUDUSD",
    "EUR": "EURUSD",
    "GBP": "GBPUSD",
    "NZD": "NZDUSD",
    "XAU": "XAUUSD",
}

USD_INVERSE_SYMBOLS = {
    "CAD": "USDCAD",
    "CHF": "USDCHF",
    "JPY": "USDJPY",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Compute equal-USD quantities for the 29 TradingView instruments.")
    parser.add_argument("--notional", type=float, default=50000.0, help="Target notional in USD per instrument.")
    parser.add_argument("--interval", default="D", help="TradingView interval used for the reference close price.")
    parser.add_argument("--candles", type=int, default=10, help="Number of candles fetched for each symbol.")
    return parser.parse_args()


def pair_base_currency(pair: str) -> str:
    if len(pair) != 6:
        raise ValueError(f"Unsupported symbol format: {pair}")
    return pair[:3]


def fetch_last_close(symbol: str, interval: str, candles: int) -> float:
    df = fetch_tv_ohlc(f"OANDA:{symbol}", interval, candles)
    if df is None or df.empty:
        raise RuntimeError(f"Unable to fetch price for {symbol}")
    return float(df["close"].iloc[-1])


def get_currency_usd_rate(currency: str, interval: str, candles: int, cache: dict[str, float]) -> float:
    if currency == "USD":
        return 1.0
    if currency in cache:
        return cache[currency]

    if currency in USD_DIRECT_SYMBOLS:
        rate = fetch_last_close(USD_DIRECT_SYMBOLS[currency], interval, candles)
    elif currency in USD_INVERSE_SYMBOLS:
        inverse_price = fetch_last_close(USD_INVERSE_SYMBOLS[currency], interval, candles)
        if inverse_price == 0:
            raise RuntimeError(f"Invalid zero price for {currency}")
        rate = 1.0 / inverse_price
    else:
        raise ValueError(f"No USD conversion configured for currency {currency}")

    cache[currency] = rate
    return rate


def compute_quantity(pair: str, notional_usd: float, interval: str, candles: int, cache: dict[str, float]) -> dict:
    last_price = fetch_last_close(pair, interval, candles)
    base_currency = pair_base_currency(pair)
    base_usd_rate = get_currency_usd_rate(base_currency, interval, candles, cache)
    if base_usd_rate == 0:
        raise RuntimeError(f"Invalid USD rate for {base_currency}")

    quantity = notional_usd / base_usd_rate
    rounded_quantity = round(quantity)
    standard_lots = quantity / 100000.0 if pair != "XAUUSD" else None
    mini_lots = quantity / 10000.0 if pair != "XAUUSD" else None
    micro_lots = quantity / 1000.0 if pair != "XAUUSD" else None
    return {
        "pair": pair,
        "price": last_price,
        "base_currency": base_currency,
        "base_usd_rate": base_usd_rate,
        "quantity": quantity,
        "rounded_quantity": rounded_quantity,
        "standard_lots": standard_lots,
        "mini_lots": mini_lots,
        "micro_lots": micro_lots,
    }


def build_rows(pairs: Iterable[str], notional_usd: float, interval: str, candles: int) -> list[dict]:
    cache: dict[str, float] = {}
    rows = []
    for pair in pairs:
        rows.append(compute_quantity(pair, notional_usd, interval, candles, cache))
    return rows


def print_report(rows: list[dict], notional_usd: float) -> None:
    print(f"Equal USD notional target: {notional_usd:,.2f} USD")
    print("")
    print(
        f"{'PAIR':<8} {'PRICE':>12} {'BASE->USD':>12} {'QUANTITY':>14} "
        f"{'ROUNDED':>12} {'STD LOT':>10} {'MINI':>10} {'MICRO':>10}"
    )
    print("-" * 92)
    for row in rows:
        std_lot = "N/A" if row["standard_lots"] is None else f"{row['standard_lots']:.3f}"
        mini_lot = "N/A" if row["mini_lots"] is None else f"{row['mini_lots']:.3f}"
        micro_lot = "N/A" if row["micro_lots"] is None else f"{row['micro_lots']:.3f}"
        print(
            f"{row['pair']:<8} "
            f"{row['price']:>12.5f} "
            f"{row['base_usd_rate']:>12.5f} "
            f"{row['quantity']:>14,.2f} "
            f"{row['rounded_quantity']:>12,} "
            f"{std_lot:>10} "
            f"{mini_lot:>10} "
            f"{micro_lot:>10}"
        )


def print_base_currency_summary(rows: list[dict], notional_usd: float) -> None:
    grouped: dict[str, dict] = {}
    for row in rows:
        grouped.setdefault(
            row["base_currency"],
            {
                "base_currency": row["base_currency"],
                "base_usd_rate": row["base_usd_rate"],
                "rounded_quantity": row["rounded_quantity"],
                "pairs": [],
            },
        )["pairs"].append(row["pair"])

    print("")
    print("Base currency summary")
    print(f"{'BASE':<6} {'BASE->USD':>12} {'ROUNDED':>12} {'PAIRS':<}")
    print("-" * 72)
    for base_currency in sorted(grouped):
        item = grouped[base_currency]
        pairs = ", ".join(sorted(item["pairs"]))
        print(
            f"{item['base_currency']:<6} "
            f"{item['base_usd_rate']:>12.5f} "
            f"{item['rounded_quantity']:>12,} "
            f"{pairs}"
        )
    print("")
    print(f"Notional per line: {notional_usd:,.2f} USD")


def main() -> int:
    args = parse_args()
    rows = build_rows(PAIRS_29, args.notional, args.interval, args.candles)
    print_report(rows, args.notional)
    print_base_currency_summary(rows, args.notional)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
