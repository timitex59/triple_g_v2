#!/usr/bin/env python3
"""MTF SAR STRUCTURE: multi-timeframe (H1 + D1) trend read from the SAR flip
levels themselves, not from momentum/Renko votes.

For each pair/currency and each timeframe (H1, D1):
  1. Detect every Parabolic SAR bull/bear cross (same recurrence as
     imp_trend5_29pairs.pine_sar, i.e. TradingView's ta.sar()).
  2. Drop any level that reversed after exactly one candle (a 1-bar
     whipsaw) -- it never was a real structural turning point.
  3. Keep the last `--levels` (default 3) valid bull levels and the last
     `--levels` valid bear levels, and count how many consecutive pairs of
     levels rose vs fell (Dow-theory-style structure) -- combined across
     both colors, that gives a single up/down score per timeframe.

The dominant direction on D1 is only trusted if the *live* price is
actually on the matching side of the *live* D1 SAR (a pair can't count as
bullish on D1 if price currently sits below the D1 SAR, however bullish
its recent level structure looks) -- this is `d1_valid` below, and a
failing pair is reported as D1_REJECTED, not silently defaulted to bull
or bear.

A pair/currency is CONVERGENT when D1 (validated) and H1 agree,
DIVERGENT when they disagree, D1_REJECTED when the D1 validation fails,
and NEUTRAL when either side has no clear majority. Only CONVERGENT and
DIVERGENT entries are worth alerting on -- D1_REJECTED/NEUTRAL are noise,
not a tradeable read, so the Telegram message stays silent on them.

Run: python sar_mtf_structure.py [--telegram]
"""
from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

import imp_trend_29pairs as base
from imp_trend5_29pairs import pine_sar

# Devise -> (code d'indice, symbole TradingView), cf. renko_full_alignment_29pairs.FOREX_INDEX_ASSETS.
CURRENCY_INDEX = {
    "USD": ("DXY", "TVC:DXY"),
    "EUR": ("EXY", "TVC:EXY"),
    "GBP": ("BXY", "TVC:BXY"),
    "JPY": ("JXY", "TVC:JXY"),
    "CHF": ("SXY", "TVC:SXY"),
    "CAD": ("CXY", "TVC:CXY"),
    "AUD": ("AXY", "TVC:AXY"),
    "NZD": ("ZXY", "TVC:ZXY"),
}

DEFAULT_PAIRS = [p for p in base.PAIRS_29 if p != "XAUUSD"]
DEFAULT_CURRENCIES = list(CURRENCY_INDEX)

DIRECTION_ICON = {"MONTANT": "\U0001f7e2", "DESCENDANT": "\U0001f534", "NEUTRE": "⚪", None: "⚪"}


def crossings(df, sar) -> list[tuple[str, Any, float, float]]:
    """(kind, time, level, open) for every bull/bear SAR cross, chronological."""
    closes = df["close"].astype(float).tolist()
    opens = df["open"].astype(float).tolist()
    times = df["time"].tolist()
    events: list[tuple[str, Any, float, float]] = []
    for i in range(1, len(df)):
        if closes[i - 1] <= sar[i - 1] and closes[i] > sar[i]:
            events.append(("BULL", times[i], sar[i], opens[i]))
        if closes[i - 1] >= sar[i - 1] and closes[i] < sar[i]:
            events.append(("BEAR", times[i], sar[i], opens[i]))
    return events


def filter_whipsaws(events, all_times) -> list[tuple[str, Any, float, float]]:
    """Drop a level that reversed after exactly 1 candle -- pure noise, not structure."""
    idx_of = {t: i for i, t in enumerate(all_times)}
    kept = []
    for idx, ev in enumerate(events):
        kind, t, _level, _open = ev
        nxt = next((e for e in events[idx + 1:] if e[0] != kind), None)
        if nxt is not None and idx_of[nxt[1]] - idx_of[t] == 1:
            continue
        kept.append(ev)
    return kept


def last_n_levels(events, kind: str, n: int) -> list[float]:
    return [v for k, _t, v, _o in events if k == kind][-n:]


def up_down(levels: list[float]) -> tuple[int, int]:
    diffs = [b - a for a, b in zip(levels, levels[1:])]
    return sum(1 for d in diffs if d > 0), sum(1 for d in diffs if d < 0)


def timeframe_reading(df, sar_start: float, sar_increment: float, sar_maximum: float, n_levels: int) -> dict:
    """Up/down structure score for one timeframe, plus the live price/SAR for validation."""
    sar = pine_sar(df, sar_start, sar_increment, sar_maximum)
    events = filter_whipsaws(crossings(df, sar), df["time"].tolist())
    up = down = 0
    for kind in ("BULL", "BEAR"):
        levels = last_n_levels(events, kind, n_levels)
        if len(levels) < n_levels:
            continue
        u, d = up_down(levels)
        up += u
        down += d
    total = up + down
    return dict(
        pct_up=up / total * 100 if total else None,
        pct_down=down / total * 100 if total else None,
        up=up, down=down, total=total,
        price=float(df["close"].iloc[-1]), sar=sar[-1],
    )


def dominant(reading: dict) -> tuple[str | None, float | None]:
    if not reading["total"]:
        return None, None
    if reading["pct_up"] == reading["pct_down"]:
        return "NEUTRE", reading["pct_up"]
    return ("MONTANT", reading["pct_up"]) if reading["pct_up"] > reading["pct_down"] else ("DESCENDANT", reading["pct_down"])


def analyze_instrument(label: str, code: str, tv_symbol: str, args) -> dict:
    h1_df = base.fetch_ohlc(code, "60", args.h1_candles, tv_symbol=tv_symbol)
    d1_df = base.fetch_ohlc(code, "D", args.d1_candles, tv_symbol=tv_symbol)
    if len(h1_df) < 3 or len(d1_df) < 3:
        raise ValueError("Historique insuffisant")
    h1 = timeframe_reading(h1_df, args.sar_start, args.sar_increment, args.sar_maximum, args.levels)
    d1 = timeframe_reading(d1_df, args.sar_start, args.sar_increment, args.sar_maximum, args.levels)

    d1_dir, d1_pct = dominant(d1)
    h1_dir, h1_pct = dominant(h1)

    price, sar_val = d1["price"], d1["sar"]
    side = "ABOVE" if price > sar_val else "BELOW" if price < sar_val else "ON"

    # Une paire ne peut compter comme montante en D1 que si le prix est
    # reellement au-dessus du SAR D1 live, et inversement pour descendante --
    # la structure des niveaux seule (potentiellement vieille de semaines)
    # ne suffit pas.
    d1_valid = not ((d1_dir == "MONTANT" and side != "ABOVE") or (d1_dir == "DESCENDANT" and side != "BELOW"))

    if not d1_valid:
        status = "D1_REJECTED"
    elif d1_dir in (None, "NEUTRE") or h1_dir in (None, "NEUTRE"):
        status = "NEUTRAL"
    elif d1_dir == h1_dir:
        status = "CONVERGENT"
    else:
        status = "DIVERGENT"

    return dict(
        label=label, code=code,
        d1_direction=d1_dir, d1_percent=d1_pct, d1_side=side, d1_valid=d1_valid,
        d1_price=price, d1_sar=sar_val,
        h1_direction=h1_dir, h1_percent=h1_pct,
        status=status,
    )


def run_batch(items: list[tuple[str, str, str]], args) -> tuple[list[dict], list[dict]]:
    """items: list of (label, code, tv_symbol). Returns (results, errors)."""
    results, errors = [], []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {}
        for i, (label, code, tv_symbol) in enumerate(items):
            if i:
                time.sleep(args.stagger)
            futures[pool.submit(analyze_instrument, label, code, tv_symbol, args)] = label
        for future in as_completed(futures):
            label = futures[future]
            try:
                results.append(future.result())
                print(f"{label}: OK", flush=True)
            except Exception as exc:
                errors.append(dict(label=label, error=str(exc)))
                print(f"{label}: ERREUR - {exc}", flush=True)
    return results, errors


def print_table(title: str, results: list[dict]) -> None:
    print(f"\n{title}")
    if not results:
        print("  (aucune)")
        return
    for r in sorted(results, key=lambda r: (r["status"], -(r["d1_percent"] or -1))):
        d1_pct = f"{r['d1_percent']:.1f}%" if r["d1_percent"] is not None else "N/A"
        h1_pct = f"{r['h1_percent']:.1f}%" if r["h1_percent"] is not None else "N/A"
        valid = "OK" if r["d1_valid"] else "REJET"
        print(f"  {r['label']:<8} D1={r['d1_direction'] or '-':<11}{d1_pct:>6} [{r['d1_side']:<5} {valid}]  "
              f"H1={r['h1_direction'] or '-':<11}{h1_pct:>6}  -> {r['status']}")


def format_compact_line(result: dict) -> str:
    valid_icon = "✅" if result["d1_valid"] else "❌"
    return f"{result['label']}\t{DIRECTION_ICON[result['d1_direction']]}{valid_icon}{DIRECTION_ICON[result['h1_direction']]}"


def build_telegram_message(pair_results: list[dict], currency_results: list[dict]) -> str | None:
    """Only CONVERGENT/DIVERGENT entries are shown -- D1_REJECTED/NEUTRAL carry
    no tradeable read (cf. module docstring). None if nothing qualifies on
    either side, same convention as the rest of the pipeline (VIVIER, SAR
    BREAK, TREND): silence rather than an empty shell."""
    lines = ["\U0001f9ed MTF SAR STRUCTURE", ""]
    has_content = False
    for section_title, results in (("PAIRES", pair_results), ("DEVISES", currency_results)):
        signal = [r for r in results if r["status"] in ("CONVERGENT", "DIVERGENT")]
        if not signal:
            continue
        signal.sort(key=lambda r: (r["status"] != "CONVERGENT", -(r["d1_percent"] or 0)))
        lines.append(section_title)
        lines.extend(format_compact_line(r) for r in signal)
        lines.append("")
        has_content = True
    if not has_content:
        return None
    lines.append(f"⏰ {datetime.now(base.PARIS).strftime('%Y-%m-%d %H:%M')} Paris")
    return "\n".join(lines)


def send_telegram_message(text: str) -> bool:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("  Telegram non configure (.env manquant ou incomplet) - message non envoye.")
        return False
    try:
        response = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                                  data={"chat_id": chat_id, "text": text}, timeout=15)
        response.raise_for_status()
        return True
    except Exception as exc:
        print(f"  Envoi Telegram echoue : {exc}")
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pairs", nargs="+", default=DEFAULT_PAIRS, choices=base.PAIRS_29,
                         help="Paires a analyser (par defaut les 28, XAUUSD exclu).")
    parser.add_argument("--currencies", nargs="+", default=DEFAULT_CURRENCIES, choices=list(CURRENCY_INDEX),
                         help="Devises (index TVC) a analyser.")
    parser.add_argument("--h1-candles", type=int, default=500)
    parser.add_argument("--d1-candles", type=int, default=2500)
    parser.add_argument("--sar-start", type=float, default=0.1)
    parser.add_argument("--sar-increment", type=float, default=0.1)
    parser.add_argument("--sar-maximum", type=float, default=0.2)
    parser.add_argument("--levels", type=int, default=3,
                         help="Nombre de derniers niveaux valides (par couleur/UT) compares pour la structure.")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--stagger", type=float, default=0.4,
                         help="Delai (s) entre deux soumissions au pool, pour eviter les 429 TradingView.")
    parser.add_argument("--json", type=Path, default=Path("sar_mtf_structure.json"))
    parser.add_argument("--telegram", action="store_true", help="Envoyer le resultat sur Telegram.")
    args = parser.parse_args()
    if (min(args.workers, args.sar_start, args.sar_increment, args.sar_maximum) <= 0
            or args.levels < 3 or args.h1_candles < 5 or args.d1_candles < 5 or args.stagger < 0):
        parser.error("Parametres positifs requis, au moins 3 niveaux et 5 bougies par UT, stagger >= 0")
    return args


def main() -> int:
    args = parse_args()

    pair_items = [(pair, pair, f"OANDA:{pair}") for pair in args.pairs]
    # CURRENCY_INDEX[c] = (index_code, tv_symbol) ; analyze_instrument veut (label, code, tv_symbol).
    currency_items = [(currency, CURRENCY_INDEX[currency][0], CURRENCY_INDEX[currency][1])
                       for currency in args.currencies]

    print(f"Scan de {len(pair_items)} paires...", flush=True)
    pair_results, pair_errors = run_batch(pair_items, args)
    print(f"Scan de {len(currency_items)} devises...", flush=True)
    currency_results, currency_errors = run_batch(currency_items, args)

    print_table("PAIRES", pair_results)
    print_table("DEVISES", currency_results)

    telegram_sent = False
    if args.telegram:
        message = build_telegram_message(pair_results, currency_results)
        if message is None:
            print("\nTelegram: rien a envoyer (aucun CONVERGENT/DIVERGENT).")
        else:
            print("\nTelegram :")
            print(message)
            telegram_sent = send_telegram_message(message)
            if telegram_sent:
                print("  Message envoye.")

    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(dict(
        time_utc=datetime.now(timezone.utc).isoformat(),
        settings=vars(args) | {"json": str(args.json)},
        pairs=pair_results, pair_errors=pair_errors,
        currencies=currency_results, currency_errors=currency_errors,
        telegram_sent=telegram_sent,
    ), indent=2, allow_nan=False, default=str), encoding="utf-8")
    print(f"\nJSON: {args.json.resolve()}")
    return 1 if (pair_errors or currency_errors) else 0


if __name__ == "__main__":
    raise SystemExit(main())
