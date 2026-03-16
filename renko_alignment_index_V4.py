#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Merge V2 and V3 outputs into one Telegram message.

FILTERED:
- pairs confirmed by the index-filtered logic from V2

NO FILTER:
- pairs confirmed by the direct 29-pair logic from V3
- excluding pairs already present in FILTERED
"""

import argparse
from datetime import datetime

from ichimoku_v4 import PAIRS_29, send_telegram_message
from renko_alignment_index_V2 import INDICES, PAIRS, PARIS_TZ, aligned_currencies, build_states
from renko_alignment_index_V2 import compute_pair_checks as compute_filtered_pair_checks
from renko_alignment_index_V3 import compute_pair_checks as compute_direct_pair_checks


MIN_ABS_CHG_CC_DAILY = 0.1


def parse_args():
    parser = argparse.ArgumentParser(description="Merge V2 filtered signals and V3 direct signals into one Telegram message.")
    parser.add_argument("--length", type=int, default=14, help="ATR length.")
    parser.add_argument("--candles", type=int, default=260, help="Number of H1 candles fetched per symbol.")
    return parser.parse_args()


def confirmed_rows(checks):
    return [
        row
        for row in checks
        if row.confirms is True
        and row.chg_cc_daily is not None
        and abs(float(row.chg_cc_daily)) > MIN_ABS_CHG_CC_DAILY
    ]


def sort_rows(rows):
    return sorted(
        rows,
        key=lambda row: (
            abs(float(row.chg_cc_daily)) if row.chg_cc_daily is not None else -1.0,
            row.pair,
        ),
        reverse=True,
    )


def format_row(row) -> str:
    icon = "\U0001F7E2" if row.expected == "BULLISH" else "\U0001F534"
    chg_txt = "N/A" if row.chg_cc_daily is None else f"{row.chg_cc_daily:+.2f}%"
    return f"{icon} {row.pair} ({chg_txt})"


def build_telegram_message(filtered_checks, direct_checks) -> str:
    filtered_rows = sort_rows(confirmed_rows(filtered_checks))
    filtered_pairs = {row.pair for row in filtered_rows}

    no_filter_rows = sort_rows([row for row in confirmed_rows(direct_checks) if row.pair not in filtered_pairs])

    lines = ["RENKO_HEIKEN_V2", ""]

    lines.append("FILTERED")
    if filtered_rows:
        lines.extend(format_row(row) for row in filtered_rows)
    else:
        lines.append("(none)")

    lines.append("NO FILTER")
    if no_filter_rows:
        lines.extend(format_row(row) for row in no_filter_rows)
    else:
        lines.append("(none)")

    lines.append(f"\u23F0 {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M Paris')}")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()

    all_states = build_states(INDICES, args.length, args.candles)
    green_ccy, red_ccy = aligned_currencies(all_states)
    filtered_checks = compute_filtered_pair_checks(PAIRS, green_ccy, red_ccy)

    direct_checks = compute_direct_pair_checks(PAIRS_29, args.length, args.candles)

    message = build_telegram_message(filtered_checks, direct_checks)
    print(message)
    send_telegram_message(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
