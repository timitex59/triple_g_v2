#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Track Renko RETRACE -> ALIGNED shifts within the same Paris day.

Rules:
- keep a daily memory of pairs seen in LIST RETRACE
- when one of those pairs later appears in LIST ALIGNED on the same Paris day,
  mark it with a flame
"""

import argparse
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from break_line import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, send_telegram_message
from renko_lists_29pairs import build_states, fmt_chg, list_aligned, list_retrace, parse_args as parse_lists_args


PARIS_TZ = ZoneInfo("Europe/Paris")
TRACKING_PATH = os.path.join(os.path.dirname(__file__), "renko_lists_tracker_state.json")


def parse_args():
    parser = argparse.ArgumentParser(description="Track Renko RETRACE to ALIGNED shifts within the same Paris day.")
    parser.add_argument("--renko-length", type=int, default=14, help="ATR length for Renko box size")
    parser.add_argument("--w-candles", type=int, default=260, help="Weekly candles fetched")
    parser.add_argument("--d-candles", type=int, default=320, help="Daily candles fetched")
    parser.add_argument("--h-candles", type=int, default=900, help="Hourly candles fetched")
    parser.add_argument("--telegram", action="store_true", help="Send the RENKO SCREEN message to Telegram")
    return parser.parse_args()


def load_state(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return data
    except Exception:
        return {}
    return {}


def save_state(path: str, payload: dict) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def state_for_today(previous: dict, today_str: str) -> dict:
    if previous.get("date_paris") == today_str:
        return previous
    return {
        "date_paris": today_str,
        "seen_retrace": [],
        "shifted_today": [],
    }


def print_group(title: str, rows, flame_pairs: set[str] | None = None) -> None:
    flame_pairs = flame_pairs or set()
    print(title)
    if not rows:
        print("(none)")
        print("")
        return

    for state in rows:
        flame = " 🔥" if state.pair in flame_pairs else ""
        print(
            f"{state.pair:<8} "
            f"RENKO={state.renko_w1}/{state.renko_d1}/{state.renko_h1} "
            f"CHG={fmt_chg(state.chg_cc_d1)} "
            f"FRESH={state.weekly_fresh}/{state.daily_fresh}/{state.hourly_fresh}{flame}"
        )
    print("")


def telegram_lines_for_rows(rows, flame_pairs: set[str] | None = None) -> list[str]:
    flame_pairs = flame_pairs or set()
    lines = []
    for state in rows:
        if state.renko_w1 == state.renko_d1 == "BULL":
            icon = "\U0001F7E2"
        elif state.renko_w1 == state.renko_d1 == "BEAR":
            icon = "\U0001F534"
        else:
            icon = "\u26AA"
        flame = " \U0001F525" if state.pair in flame_pairs else ""
        lines.append(f"{icon} {state.pair} ({fmt_chg(state.chg_cc_d1)}){flame}")
    return lines


def build_telegram_message(aligned_green, aligned_red, retrace_green, retrace_red, flame_pairs: set[str]) -> str:
    lines = ["RENKO SCREEN", ""]

    lines.append("LIST ALIGNED")
    aligned_rows = aligned_green + aligned_red
    if aligned_rows:
        lines.extend(telegram_lines_for_rows(aligned_rows, flame_pairs))
    else:
        lines.append("NO DEAL \U0001F61E")

    lines.extend(["", "LIST RETRACE"])
    retrace_rows = retrace_green + retrace_red
    if retrace_rows:
        lines.extend(telegram_lines_for_rows(retrace_rows))
    else:
        lines.append("NO DEAL \U0001F61E")

    now_txt = datetime.now(PARIS_TZ).strftime("%Y-%m-%d %H:%M")
    lines.extend(["", f"\u23F0 {now_txt} Paris"])
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    states = build_states(args)
    aligned_green, aligned_red = list_aligned(states)
    retrace_green, retrace_red = list_retrace(states)

    now_paris = datetime.now(PARIS_TZ)
    today_str = now_paris.strftime("%Y-%m-%d")

    previous = load_state(TRACKING_PATH)
    state = state_for_today(previous, today_str)

    seen_retrace = set(state.get("seen_retrace", []))
    shifted_today = set(state.get("shifted_today", []))

    current_retrace_pairs = {item.pair for item in retrace_green + retrace_red}
    aligned_pairs = {item.pair for item in aligned_green + aligned_red}

    seen_retrace.update(current_retrace_pairs)
    new_shifts = aligned_pairs & seen_retrace
    shifted_today.update(new_shifts)

    payload = {
        "date_paris": today_str,
        "updated_at_paris": now_paris.strftime("%Y-%m-%d %H:%M:%S"),
        "seen_retrace": sorted(seen_retrace),
        "shifted_today": sorted(shifted_today),
    }
    save_state(TRACKING_PATH, payload)

    print(f"TRACK DATE : {today_str}")
    print(f"UPDATED AT : {payload['updated_at_paris']} Paris")
    print("")
    print_group("LIST ALIGNED | GREEN/GREEN/GREEN", aligned_green, shifted_today)
    print_group("LIST ALIGNED | RED/RED/RED", aligned_red, shifted_today)
    print_group("LIST RETRACE | D1 + W1 GREEN | H1 RED", retrace_green)
    print_group("LIST RETRACE | D1 + W1 RED | H1 GREEN", retrace_red)

    if shifted_today:
        print("SHIFTED RETRACE -> ALIGNED TODAY")
        print(", ".join(sorted(shifted_today)))
    else:
        print("SHIFTED RETRACE -> ALIGNED TODAY")
        print("(none)")

    if args.telegram and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_telegram_message(build_telegram_message(aligned_green, aligned_red, retrace_green, retrace_red, shifted_today))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
