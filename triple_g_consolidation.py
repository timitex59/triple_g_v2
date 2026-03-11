#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Consolide les screeners break_line et ichimoku avec une logique de conflit devise.
"""

import json
import os
import time
from collections import defaultdict

BREAK_LINE_PATH = os.path.join(os.path.dirname(__file__), "break_line_scan.json")
ICHIMOKU_PATH = os.path.join(os.path.dirname(__file__), "ichimoku_scan.json")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "triple_g_consolidation.json")


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid JSON payload in {path}")
    return data


def pair_currencies(pair: str) -> tuple[str, str] | None:
    if not pair or len(pair) != 6:
        return None
    return pair[:3], pair[3:]


def row_direction(row: dict) -> str | None:
    direction = row.get("direction")
    if direction in {"BULL", "BEAR"}:
        return direction
    direction = row.get("w1d1_direction")
    if direction in {"BULL", "BEAR"}:
        return direction
    return None


def build_entry(source: str, section: str, row: dict) -> dict | None:
    pair = row.get("pair")
    direction = row_direction(row)
    currencies = pair_currencies(pair)
    if currencies is None or direction is None:
        return None

    base, quote = currencies
    if direction == "BULL":
        strong, weak = base, quote
    else:
        strong, weak = quote, base

    return {
        "pair": pair,
        "direction": direction,
        "strong_currency": strong,
        "weak_currency": weak,
        "source": source,
        "section": section,
        "chg_cc_d1": row.get("chg_cc_d1"),
    }


def collect_entries(break_line_data: dict, ichimoku_data: dict) -> list[dict]:
    entries = []

    mapping = [
        ("break_line", "aligned", break_line_data.get("aligned_rows", [])),
        ("break_line", "retracing", break_line_data.get("retracing_rows", [])),
        ("break_line", "exited", break_line_data.get("exited_rows", [])),
        ("ichimoku", "aligned", ichimoku_data.get("aligned_rows", [])),
        ("ichimoku", "mid_aligned", ichimoku_data.get("mid_aligned_rows", [])),
    ]

    for source, section, rows in mapping:
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            entry = build_entry(source, section, row)
            if entry is not None:
                entries.append(entry)

    return entries


def collect_conflicted_currencies(entries: list[dict]) -> set[str]:
    strong = {entry["strong_currency"] for entry in entries}
    weak = {entry["weak_currency"] for entry in entries}
    return strong & weak


def aggregate_tradable_pairs(entries: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}

    for entry in entries:
        pair = entry["pair"]
        item = grouped.setdefault(
            pair,
            {
                "pair": pair,
                "direction": entry["direction"],
                "strong_currency": entry["strong_currency"],
                "weak_currency": entry["weak_currency"],
                "sources": set(),
                "sections": set(),
                "chg_cc_d1_values": [],
            },
        )
        item["sources"].add(entry["source"])
        item["sections"].add(f"{entry['source']}:{entry['section']}")
        if entry.get("chg_cc_d1") is not None:
            item["chg_cc_d1_values"].append(float(entry["chg_cc_d1"]))

    tradable_pairs = []
    for item in grouped.values():
        section_count = len(item["sections"])
        best_chg = None
        if item["chg_cc_d1_values"]:
            best_chg = max(item["chg_cc_d1_values"], key=abs)
        tradable_pairs.append(
            {
                "pair": item["pair"],
                "direction": item["direction"],
                "strong_currency": item["strong_currency"],
                "weak_currency": item["weak_currency"],
                "sources": sorted(item["sources"]),
                "sections": sorted(item["sections"]),
                "confirmation_count": section_count,
                "stars": min(section_count, 3),
                "best_chg_cc_d1": best_chg,
            }
        )

    tradable_pairs.sort(
        key=lambda item: (
            -item["confirmation_count"],
            -(abs(item["best_chg_cc_d1"]) if item["best_chg_cc_d1"] is not None else -1.0),
            item["pair"],
        )
    )
    return tradable_pairs


def save_output(path: str, payload: dict) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def print_report(payload: dict) -> None:
    conflicts = payload["conflicted_currencies"]
    invalidated = payload["invalidated_pairs"]
    tradable = payload["tradable_pairs"]

    print("TRIPLE G CONSOLIDATION")
    print("")

    print("CONFLICTED CURRENCIES")
    if not conflicts:
        print("None")
    else:
        print(", ".join(conflicts))

    print("")
    print("INVALIDATED PAIRS")
    if not invalidated:
        print("None")
    else:
        for row in invalidated:
            blocked = "/".join(row["blocked_by"])
            print(f"  {row['pair']:<8} {row['direction']:<4} blocked by {blocked}")

    print("")
    print("TRADABLE PAIRS")
    if not tradable:
        print("None")
    else:
        for row in tradable:
            chg = row["best_chg_cc_d1"]
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            stars = "*" * row["stars"]
            sections = ", ".join(row["sections"])
            print(f"  {row['pair']:<8} {row['direction']:<4} {stars:<3} ({chg_txt}) [{sections}]")


def main() -> int:
    missing = [path for path in (BREAK_LINE_PATH, ICHIMOKU_PATH) if not os.path.exists(path)]
    if missing:
        for path in missing:
            print(f"Missing input snapshot: {path}")
        return 1

    break_line_data = load_json(BREAK_LINE_PATH)
    ichimoku_data = load_json(ICHIMOKU_PATH)
    entries = collect_entries(break_line_data, ichimoku_data)

    conflicted_currencies = sorted(collect_conflicted_currencies(entries))
    tradable_entries = [
        entry
        for entry in entries
        if entry["strong_currency"] not in conflicted_currencies
        and entry["weak_currency"] not in conflicted_currencies
    ]
    invalidated_entries = [
        entry
        for entry in entries
        if entry["strong_currency"] in conflicted_currencies
        or entry["weak_currency"] in conflicted_currencies
    ]

    invalidated_pairs_map: dict[tuple[str, str], dict] = {}
    for entry in invalidated_entries:
        key = (entry["pair"], entry["direction"])
        item = invalidated_pairs_map.setdefault(
            key,
            {
                "pair": entry["pair"],
                "direction": entry["direction"],
                "blocked_by": set(),
                "sections": set(),
            },
        )
        if entry["strong_currency"] in conflicted_currencies:
            item["blocked_by"].add(entry["strong_currency"])
        if entry["weak_currency"] in conflicted_currencies:
            item["blocked_by"].add(entry["weak_currency"])
        item["sections"].add(f"{entry['source']}:{entry['section']}")

    invalidated_pairs = []
    for item in invalidated_pairs_map.values():
        invalidated_pairs.append(
            {
                "pair": item["pair"],
                "direction": item["direction"],
                "blocked_by": sorted(item["blocked_by"]),
                "sections": sorted(item["sections"]),
            }
        )
    invalidated_pairs.sort(key=lambda item: (item["pair"], item["direction"]))

    tradable_pairs = aggregate_tradable_pairs(tradable_entries)

    payload = {
        "updated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "input_files": {
            "break_line": BREAK_LINE_PATH,
            "ichimoku": ICHIMOKU_PATH,
        },
        "entry_count": len(entries),
        "conflicted_currencies": conflicted_currencies,
        "invalidated_pairs": invalidated_pairs,
        "tradable_pairs": tradable_pairs,
    }

    save_output(OUTPUT_PATH, payload)
    print_report(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
