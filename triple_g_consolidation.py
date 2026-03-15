#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Consolide les screeners break_line et ichimoku avec une logique de conflit devise.
"""

import json
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

BREAK_LINE_PATH = os.path.join(os.path.dirname(__file__), "break_line_scan.json")
ICHIMOKU_PATH = os.path.join(os.path.dirname(__file__), "ichimoku_scan.json")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "triple_g_consolidation.json")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SECTION_ORDER = [
    ("break_line", "aligned"),
    ("break_line", "retracing"),
    ("break_line", "exited"),
    ("ichimoku", "aligned"),
    ("ichimoku", "mid_aligned"),
]

SECTION_LABELS = {
    ("break_line", "aligned"): "ALIGNED PAIRS",
    ("break_line", "retracing"): "RETRACING",
    ("break_line", "exited"): "EXITED PAIRS",
    ("ichimoku", "aligned"): "ICHIMOKU",
    ("ichimoku", "mid_aligned"): "MID ALIGNED",
}
SECTION_LABELS_BY_KEY = {
    format_key: SECTION_LABELS[key]
    for key, format_key in [((source, section), f"{source}:{section}") for source, section in SECTION_ORDER]
}


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
        ("break_line", "aligned", break_line_data.get("telegram_rows", [])),
        ("break_line", "retracing", break_line_data.get("retracing_rows", [])),
        ("break_line", "exited", break_line_data.get("exited_rows", [])),
        ("ichimoku", "aligned", ichimoku_data.get("telegram_aligned_rows", [])),
        ("ichimoku", "mid_aligned", ichimoku_data.get("telegram_mid_aligned_rows", [])),
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


def format_section_key(source: str, section: str) -> str:
    return f"{source}:{section}"


def direction_icon(direction: str) -> str:
    return "\U0001F7E2" if direction == "BULL" else "\U0001F534"


def format_pair_line(row: dict, include_stars: bool = False) -> str:
    icon = direction_icon(row["direction"])
    chg = row.get("best_chg_cc_d1", row.get("chg_cc_d1"))
    chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
    suffix = ""
    if include_stars and row.get("stars"):
        suffix = " " + ("\u2B50" * row["stars"])
    return f"{icon} {row['pair']} ({chg_txt}){suffix}"


def group_entries_by_section(entries: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, dict[str, dict]] = {}
    for entry in entries:
        section_key = format_section_key(entry["source"], entry["section"])
        section_group = grouped.setdefault(section_key, {})
        current = section_group.get(entry["pair"])
        if current is None:
            section_group[entry["pair"]] = dict(entry)
            continue
        current_chg = current.get("chg_cc_d1")
        new_chg = entry.get("chg_cc_d1")
        if new_chg is not None and (current_chg is None or abs(new_chg) > abs(current_chg)):
            section_group[entry["pair"]] = dict(entry)

    ordered: dict[str, list[dict]] = {}
    for source, section in SECTION_ORDER:
        section_key = format_section_key(source, section)
        rows = list(grouped.get(section_key, {}).values())
        rows.sort(
            key=lambda row: (
                -(abs(row["chg_cc_d1"]) if row.get("chg_cc_d1") is not None else -1.0),
                row["pair"],
            )
        )
        ordered[section_key] = rows
    return ordered


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
        item["sections"].add(format_section_key(entry["source"], entry["section"]))
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


def aggregate_best_opportunities(tradable_pairs: list[dict]) -> list[dict]:
    best = [row for row in tradable_pairs if row["confirmation_count"] >= 2]
    best.sort(
        key=lambda row: (
            -row["confirmation_count"],
            -(abs(row["best_chg_cc_d1"]) if row["best_chg_cc_d1"] is not None else -1.0),
            row["pair"],
        )
    )
    return best


def save_output(path: str, payload: dict) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def build_telegram_message(payload: dict) -> str:
    tradable_pairs = payload["tradable_pairs"]

    no_entry_icon = "\u26D4"
    clock_icon = "\u23F0"
    paris_now = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M")

    lines = ["CONSOLIDATION", "", ""]
    if not tradable_pairs:
        lines.append(f"{no_entry_icon} Aucune paire tradable")
    else:
        for row in tradable_pairs:
            lines.append(format_pair_line(row))

    lines.extend(["", f"{clock_icon} {paris_now} Paris"])
    return "\n".join(lines)


def send_telegram_message(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram: credentials missing, skip send.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        data = response.json()
        ok = bool(data.get("ok", False))
        print(f"Telegram: {'sent' if ok else 'failed'}")
        return ok
    except Exception as exc:
        print(f"Telegram: send failed ({exc})")
        return False


def print_report(payload: dict) -> None:
    conflicts = payload["conflicted_currencies"]
    invalidated = payload["invalidated_pairs"]
    tradable = payload["tradable_pairs"]
    tradable_by_section = payload["tradable_by_section"]
    best_opportunities = payload["best_opportunities"]

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

    print("")
    print("TRADABLE BY SECTION")
    for source, section in SECTION_ORDER:
        section_key = format_section_key(source, section)
        rows = tradable_by_section.get(section_key, [])
        print(SECTION_LABELS[(source, section)])
        if not rows:
            print("  None")
            continue
        for row in rows:
            print(f"  {format_pair_line(row)}")

    print("")
    print("BEST OPPORTUNITIES")
    if not best_opportunities:
        print("None")
    else:
        for row in best_opportunities:
            sections = " + ".join(SECTION_LABELS_BY_KEY[key] for key in row["sections"])
            print(f"  {row['pair']} {'*' * row['stars']} [{sections}]")


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
    tradable_by_section = group_entries_by_section(tradable_entries)
    best_opportunities = aggregate_best_opportunities(tradable_pairs)

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
        "tradable_by_section": tradable_by_section,
        "best_opportunities": best_opportunities,
    }

    save_output(OUTPUT_PATH, payload)
    print_report(payload)
    send_telegram_message(build_telegram_message(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
