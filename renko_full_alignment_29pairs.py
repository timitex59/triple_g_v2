"""Scanner dedicated to strict M/W/D Renko full alignment on the 29 FX pairs.

This is intentionally separate from the VIVIER state machine. It only detects
pairs where price is strictly outside Monthly, Weekly and Daily Renko bricks in
the same direction:

- BULL: M+/W+/D+ => raw score +100%
- BEAR: M-/W-/D- => raw score -100%

Optional default fibo filter keeps only the extreme continuation area:

- BULL: price at or above 0.786 of the effective monthly H1 range
- BEAR: price at or below 0.214 of the effective monthly H1 range
"""

from __future__ import annotations

import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

from ichimoku_v4 import PAIRS_29, send_telegram_message
from renko_score_29pairs_v16 import compute_pair_score, fib_directional_label


PARIS_TZ = ZoneInfo("Europe/Paris")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan the 29 OANDA FX pairs for strict Monthly/Weekly/Daily "
            "Renko full alignment."
        )
    )
    parser.add_argument("--length", type=int, default=14, help="ATR Renko length.")
    parser.add_argument(
        "--candles",
        type=int,
        default=300,
        help="Number of candles/bricks fetched per symbol and timeframe.",
    )
    parser.add_argument(
        "--max-streak",
        type=int,
        default=50,
        help="Cap on consecutive green/red Renko brick streak count.",
    )
    parser.add_argument(
        "--no-fib-extreme-filter",
        action="store_true",
        help="Keep every +/-100% M/W/D alignment, regardless of fibo position.",
    )
    parser.add_argument(
        "--fib-extreme",
        type=float,
        default=78.6,
        help=(
            "Extreme fibo threshold in percent. Default 78.6 means BULL >= "
            "78.6%% and BEAR <= 21.4%%."
        ),
    )
    parser.add_argument(
        "--telegram",
        action="store_true",
        help="Send the scanner result to Telegram. By default it only prints.",
    )
    return parser.parse_args()


def _px(row: dict) -> dict[str, int] | None:
    px = row.get("px") or {}
    if any(px.get(tf) not in (-1, 0, 1) for tf in ("M", "W", "D")):
        return None
    return {tf: int(px[tf]) for tf in ("M", "W", "D")}


def full_alignment_direction(row: dict) -> int:
    """Return +1/-1 for strict M/W/D alignment, else 0."""
    px = _px(row)
    if px is None:
        return 0
    if px["M"] == 1 and px["W"] == 1 and px["D"] == 1:
        return 1
    if px["M"] == -1 and px["W"] == -1 and px["D"] == -1:
        return -1
    return 0


def raw_alignment_score(row: dict) -> float:
    """Raw M/W/D score using the same 3/2/1 weights as the VIVIER scanner."""
    px = _px(row)
    if px is None:
        return 0.0
    return (px["M"] * 3.0 + px["W"] * 2.0 + px["D"] * 1.0) / 6.0 * 100.0


def fib_extreme_ok(row: dict, direction: int, extreme_pct: float = 78.6) -> bool:
    """Keep BULL near F1 and BEAR near F0 of the effective monthly H1 fibo."""
    fib_pct = (row.get("h1_fib") or {}).get("pct_of_range")
    if not isinstance(fib_pct, (int, float)):
        return False
    lower_extreme = 100.0 - float(extreme_pct)
    if direction == 1:
        return float(fib_pct) >= float(extreme_pct)
    if direction == -1:
        return float(fib_pct) <= lower_extreme
    return False


def select_full_alignment_rows(
    rows: list[dict],
    *,
    require_fib_extreme: bool = True,
    fib_extreme_pct: float = 78.6,
) -> list[dict]:
    selected: list[dict] = []
    for row in rows:
        direction = full_alignment_direction(row)
        if direction == 0:
            continue
        if require_fib_extreme and not fib_extreme_ok(row, direction, fib_extreme_pct):
            continue
        enriched = dict(row)
        enriched["full_alignment_direction"] = direction
        enriched["raw_alignment_score"] = raw_alignment_score(row)
        enriched["fib_directional_label"] = fib_directional_label(row.get("h1_fib"), direction)
        selected.append(enriched)

    def sort_key(row: dict) -> tuple[int, float, str]:
        direction = int(row["full_alignment_direction"])
        fib_pct = float((row.get("h1_fib") or {}).get("pct_of_range") or 0.0)
        # BULL: highest fib first. BEAR: lowest fib first.
        return (0 if direction == 1 else 1, -fib_pct if direction == 1 else fib_pct, row["pair"])

    return sorted(selected, key=sort_key)


def _format_px(row: dict) -> str:
    px = _px(row) or {}
    symbol = {1: "+", 0: "0", -1: "-"}
    return "/".join(f"{tf}{symbol.get(px.get(tf), '?')}" for tf in ("M", "W", "D"))


def format_full_alignment_message(rows: list[dict], now: datetime | None = None) -> str:
    now = (now or datetime.now(PARIS_TZ)).astimezone(PARIS_TZ)
    lines = ["📊 FULL ALIGNMENT M/W/D", ""]
    if not rows:
        lines.append("Aucune paire en alignement strict.")
    else:
        for row in rows:
            direction = int(row["full_alignment_direction"])
            icon = "🟢" if direction == 1 else "🔴"
            score = float(row["raw_alignment_score"])
            fib = str(row.get("fib_directional_label") or "Fibo ?").removeprefix("Fibo ")
            price = row.get("h1_price") or row.get("live_price")
            price_txt = f" | {float(price):.5f}" if isinstance(price, (int, float)) else ""
            lines.append(
                f"{icon} {row['pair']} ({score:+.0f}% | {fib} | {_format_px(row)}{price_txt})"
            )
    lines.extend(["", f"⏰ {now:%Y-%m-%d %H:%M} Paris"])
    return "\n".join(lines)


def scan_pairs(length: int, candles: int, max_streak: int) -> list[dict]:
    rows: list[dict] = []
    for pair in PAIRS_29:
        try:
            row = compute_pair_score(pair, length, candles, max_streak)
        except Exception as exc:  # keep the scanner usable if one pair fails
            print(f"{pair}: erreur {exc}")
            continue
        if row is not None:
            rows.append(row)
    return rows


def main() -> int:
    args = parse_args()
    rows = scan_pairs(args.length, args.candles, args.max_streak)
    selected = select_full_alignment_rows(
        rows,
        require_fib_extreme=not args.no_fib_extreme_filter,
        fib_extreme_pct=args.fib_extreme,
    )
    message = format_full_alignment_message(selected)
    print(message)
    if args.telegram:
        send_telegram_message(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
