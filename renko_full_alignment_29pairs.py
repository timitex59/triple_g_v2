"""Scanner dedicated to strict M/W/D Renko full alignment on FX assets.

This is intentionally separate from the VIVIER state machine. It only detects
pairs where price is strictly outside Monthly, Weekly and Daily Renko bricks in
the same direction:

- BULL: M+/W+/D+ => raw score +100%
- BEAR: M-/W-/D- => raw score -100%

It also flags H1 SAR breaks in the same direction as the full alignment.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ichimoku_v4 import PAIRS_29, send_telegram_message
from renko_score_29pairs_v16 import (
    TFState,
    closed_h1_source,
    f_effective_bias,
    f_px_state,
    fetch_tv_native_renko_ohlc,
    fetch_tv_ohlc,
    parabolic_sar,
    streak_range_from_bricks,
    streaks_from_bricks,
)


PARIS_TZ = ZoneInfo("Europe/Paris")
FULL_SAR_STATE_FILE = Path("renko_full_alignment_full_sar_state.json")
FULL_SAR_WINDOW_START_HOUR = 7
FULL_SAR_WINDOW_END_HOUR = 23

FOREX_INDEX_ASSETS: list[dict] = [
    {"pair": "DXY", "tv_symbol": "TVC:DXY", "asset_type": "INDEX", "currency": "USD"},
    {"pair": "EXY", "tv_symbol": "TVC:EXY", "asset_type": "INDEX", "currency": "EUR"},
    {"pair": "BXY", "tv_symbol": "TVC:BXY", "asset_type": "INDEX", "currency": "GBP"},
    {"pair": "JXY", "tv_symbol": "TVC:JXY", "asset_type": "INDEX", "currency": "JPY"},
    {"pair": "SXY", "tv_symbol": "TVC:SXY", "asset_type": "INDEX", "currency": "CHF"},
    {"pair": "CXY", "tv_symbol": "TVC:CXY", "asset_type": "INDEX", "currency": "CAD"},
    {"pair": "AXY", "tv_symbol": "TVC:AXY", "asset_type": "INDEX", "currency": "AUD"},
    {"pair": "ZXY", "tv_symbol": "TVC:ZXY", "asset_type": "INDEX", "currency": "NZD"},
]

FOREX_PAIR_ASSETS: list[dict] = [
    {"pair": pair, "tv_symbol": f"OANDA:{pair}", "asset_type": "PAIR"}
    for pair in PAIRS_29
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan the 29 OANDA FX pairs and/or currency indices for strict Monthly/Weekly/Daily "
            "Renko full alignment."
        )
    )
    parser.add_argument(
        "--assets",
        choices=("all", "pairs", "indices"),
        default="all",
        help="Asset universe to scan. Default: all = 29 pairs + 8 forex indices.",
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
        "--sar-candles",
        type=int,
        default=400,
        help="Number of H1 candles used to detect SAR breaks.",
    )
    parser.add_argument(
        "--telegram",
        action="store_true",
        help="Send the scanner result to Telegram. By default it only prints.",
    )
    parser.add_argument(
        "--full-sar-state-file",
        default=str(FULL_SAR_STATE_FILE),
        help="JSON state file used to persist FULL SAR detections between 07:00 and 23:00 Paris.",
    )
    return parser.parse_args()


def assets_for_scope(scope: str) -> list[dict]:
    if scope == "pairs":
        return list(FOREX_PAIR_ASSETS)
    if scope == "indices":
        return list(FOREX_INDEX_ASSETS)
    return [*FOREX_PAIR_ASSETS, *FOREX_INDEX_ASSETS]


def compute_tf_state_for_symbol(
    tv_symbol: str,
    interval: str,
    length: int,
    candles: int,
    max_streak: int,
    live_price: float,
) -> TFState | None:
    native_bricks = fetch_tv_native_renko_ohlc(
        tv_symbol,
        interval,
        atr_length=length,
        n_bricks=max(candles, max_streak + 1),
    )
    if not native_bricks:
        return None

    bricks: list[tuple[float, float, int]] = []
    for brick in native_bricks:
        renko_open = float(brick["open"])
        renko_close = float(brick["close"])
        direction = 1 if renko_close > renko_open else (-1 if renko_close < renko_open else 0)
        if direction:
            bricks.append((renko_open, renko_close, direction))
    if not bricks:
        return None

    renko_open, renko_close, direction = bricks[-1]
    green_streak, red_streak = streaks_from_bricks(bricks, max_streak)
    streak_count, streak_low, streak_high = streak_range_from_bricks(bricks)
    px_state = f_px_state(renko_open, renko_close, live_price)
    bias = f_effective_bias(px_state, green_streak, red_streak)

    return TFState(
        px_state=px_state,
        bias=bias,
        direction=direction,
        green_streak=green_streak,
        red_streak=red_streak,
        renko_open=renko_open,
        renko_close=renko_close,
        streak_count=streak_count,
        streak_low=streak_low,
        streak_high=streak_high,
    )


def compute_asset_score(asset: dict, length: int, candles: int, max_streak: int) -> dict | None:
    tv_symbol = str(asset["tv_symbol"])
    df_d_live = fetch_tv_ohlc(tv_symbol, "D", max(candles, 50))
    if df_d_live is None or df_d_live.empty:
        return None
    live_price = float(df_d_live["close"].iloc[-1])

    states: dict[str, TFState] = {}
    for interval in ("M", "W", "D"):
        state = compute_tf_state_for_symbol(
            tv_symbol,
            interval,
            length,
            candles,
            max_streak,
            live_price,
        )
        if state is None:
            return None
        states[interval] = state

    return {
        "pair": asset["pair"],
        "tv_symbol": tv_symbol,
        "asset_type": asset.get("asset_type", "PAIR"),
        "currency": asset.get("currency"),
        "live_price": live_price,
        "states": states,
        "px": {tf: states[tf].px_state for tf in ("M", "W", "D")},
        "bias": {tf: states[tf].bias for tf in ("M", "W", "D")},
    }


def sar_break_state_from_close_sar(closes: list[float], sar_values: list[float]) -> dict:
    """Detect SAR crosses on closed H1 candles.

    BULL SAR break: previous close <= previous SAR and current close > current SAR.
    BEAR SAR break: previous close >= previous SAR and current close < current SAR.
    """
    events: list[dict] = []
    count = min(len(closes), len(sar_values))
    for index in range(1, count):
        close = float(closes[index])
        prev_close = float(closes[index - 1])
        sar = float(sar_values[index])
        prev_sar = float(sar_values[index - 1])

        if prev_close <= prev_sar and close > sar:
            events.append({
                "index": index,
                "direction": 1,
                "level": sar,
                "kind": "SAR BULL",
            })
        elif prev_close >= prev_sar and close < sar:
            events.append({
                "index": index,
                "direction": -1,
                "level": sar,
                "kind": "SAR BEAR",
            })

    last_event = events[-1] if events else None
    last_bar_index = count - 1
    last_bar_direction = (
        int(last_event["direction"])
        if last_event is not None and int(last_event["index"]) == last_bar_index
        else 0
    )
    return {
        "last_bar_sar_break_direction": last_bar_direction,
        "last_bar_sar_break_kind": (
            str(last_event["kind"])
            if last_event is not None and int(last_event["index"]) == last_bar_index
            else ""
        ),
        "last_sar_break_direction": int(last_event["direction"]) if last_event else 0,
        "last_sar_break_kind": str(last_event["kind"]) if last_event else "",
        "last_sar_break_level": float(last_event["level"]) if last_event else None,
        "events": events,
    }


def compute_sar_break_state_for_symbol(tv_symbol: str, h1_candles: int = 400) -> dict:
    df = fetch_tv_ohlc(tv_symbol, "60", h1_candles)
    if df is None or df.empty:
        return {"last_bar_sar_break_direction": 0, "last_sar_break_direction": 0}
    closed_df = closed_h1_source(df)
    sar_state = parabolic_sar(closed_df)
    if not sar_state:
        return {"last_bar_sar_break_direction": 0, "last_sar_break_direction": 0}
    sar_values = sar_state.get("sar") or []
    closes = [float(value) for value in closed_df["close"].tolist()]
    return sar_break_state_from_close_sar(closes, sar_values)


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


def select_full_alignment_rows(rows: list[dict]) -> list[dict]:
    selected: list[dict] = []
    for row in rows:
        direction = full_alignment_direction(row)
        if direction == 0:
            continue
        enriched = dict(row)
        enriched["full_alignment_direction"] = direction
        enriched["raw_alignment_score"] = raw_alignment_score(row)
        selected.append(enriched)

    def sort_key(row: dict) -> tuple[int, int, str]:
        asset_rank = 1 if row.get("asset_type") == "INDEX" else 0
        direction = int(row["full_alignment_direction"])
        return (asset_rank, 0 if direction == 1 else 1, row["pair"])

    return sorted(selected, key=sort_key)


def is_directional_sar_break(row: dict, direction: int) -> bool:
    sar_break = row.get("sar_break") or {}
    return int(sar_break.get("last_bar_sar_break_direction") or 0) == direction


def select_full_sar_rows(rows: list[dict]) -> list[dict]:
    return [
        row for row in rows
        if is_directional_sar_break(row, int(row.get("full_alignment_direction") or 0))
    ]


def default_full_sar_history_state() -> dict:
    return {"version": 1, "days": {}}


def load_full_sar_history_state(path: str | Path) -> dict:
    state_path = Path(path)
    if not state_path.exists():
        return default_full_sar_history_state()
    try:
        loaded = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return default_full_sar_history_state()
    if not isinstance(loaded, dict):
        return default_full_sar_history_state()
    loaded.setdefault("version", 1)
    if not isinstance(loaded.get("days"), dict):
        loaded["days"] = {}
    return loaded


def save_full_sar_history_state(path: str | Path, state: dict) -> None:
    state_path = Path(path)
    if state_path.parent != Path("."):
        state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _is_full_sar_tracking_window(now: datetime) -> bool:
    paris_now = now.astimezone(PARIS_TZ)
    return FULL_SAR_WINDOW_START_HOUR <= paris_now.hour <= FULL_SAR_WINDOW_END_HOUR


def _prune_full_sar_history_state(state: dict, keep_days: int = 45) -> None:
    days = state.setdefault("days", {})
    if not isinstance(days, dict):
        state["days"] = {}
        return
    for day_key in sorted(days)[:-keep_days]:
        days.pop(day_key, None)


def update_full_sar_history(
    state: dict,
    full_sar_rows: list[dict],
    now: datetime,
) -> tuple[dict, dict]:
    paris_now = now.astimezone(PARIS_TZ)
    day_key = f"{paris_now:%Y-%m-%d}"
    state.setdefault("version", 1)
    days = state.setdefault("days", {})
    if not isinstance(days, dict):
        days = {}
        state["days"] = days
    day_state = days.setdefault(day_key, {"events": []})
    events = day_state.setdefault("events", [])
    if not isinstance(events, list):
        events = []
        day_state["events"] = events

    if not _is_full_sar_tracking_window(paris_now):
        _prune_full_sar_history_state(state)
        return state, day_state

    by_key = {
        str(event.get("key")): event
        for event in events
        if isinstance(event, dict) and event.get("key")
    }
    timestamp = paris_now.isoformat(timespec="minutes")
    for row in full_sar_rows:
        pair = str(row["pair"])
        direction = int(row.get("full_alignment_direction") or 0)
        if direction == 0:
            continue
        key = f"{pair}|{direction}"
        event = by_key.get(key)
        if event is None:
            event = {
                "key": key,
                "pair": pair,
                "asset_type": row.get("asset_type", "PAIR"),
                "currency": row.get("currency"),
                "direction": direction,
                "first_seen": timestamp,
                "last_seen": timestamp,
                "count": 0,
            }
            events.append(event)
            by_key[key] = event

        event["last_seen"] = timestamp
        event["count"] = int(event.get("count") or 0) + 1

    day_state["events"] = sorted(
        [event for event in events if isinstance(event, dict)],
        key=lambda event: (
            1 if event.get("asset_type") == "INDEX" else 0,
            0 if int(event.get("direction") or 0) == 1 else 1,
            str(event.get("pair") or ""),
        ),
    )
    _prune_full_sar_history_state(state)
    return state, day_state


def attach_sar_break_states(rows: list[dict], h1_candles: int = 400) -> list[dict]:
    for row in rows:
        tv_symbol = row.get("tv_symbol")
        if isinstance(tv_symbol, str) and tv_symbol:
            row["sar_break"] = compute_sar_break_state_for_symbol(tv_symbol, h1_candles)
    return rows


def _format_px(row: dict) -> str:
    px = _px(row) or {}
    symbol = {1: "+", 0: "0", -1: "-"}
    return "/".join(f"{tf}{symbol.get(px.get(tf), '?')}" for tf in ("M", "W", "D"))


def _sar_suffix(row: dict) -> str:
    alignment_direction = int(row.get("full_alignment_direction") or 0)
    if is_directional_sar_break(row, alignment_direction):
        return " 🔥"
    return ""


def _asset_display_name(row: dict) -> str:
    if row.get("asset_type") == "INDEX":
        return str(row.get("currency") or row["pair"])
    return str(row["pair"])


def _history_asset_display_name(event: dict) -> str:
    if event.get("asset_type") == "INDEX":
        return str(event.get("currency") or event.get("pair") or "")
    return str(event.get("pair") or "")


def _format_history_time_range(event: dict) -> str:
    first_seen = str(event.get("first_seen") or "")
    last_seen = str(event.get("last_seen") or "")
    first_hm = first_seen[11:16] if len(first_seen) >= 16 else ""
    last_hm = last_seen[11:16] if len(last_seen) >= 16 else ""
    if first_hm and last_hm and first_hm != last_hm:
        return f"{first_hm}→{last_hm}"
    return first_hm or last_hm


def _format_full_sar_history_event(event: dict) -> str:
    direction = int(event.get("direction") or 0)
    icon = "🟢" if direction == 1 else "🔴"
    name = _history_asset_display_name(event)
    time_label = _format_history_time_range(event)
    suffix = f" {time_label}" if time_label else ""
    return f"{icon} {name} 🔥{suffix}"


def format_full_alignment_message(
    rows: list[dict],
    full_sar_rows: list[dict] | None = None,
    full_sar_history: dict | None = None,
    now: datetime | None = None,
) -> str:
    now = (now or datetime.now(PARIS_TZ)).astimezone(PARIS_TZ)
    lines = ["📊 FULL ALIGNMENT M/W/D", ""]
    if not rows:
        lines.append("Aucune paire en alignement strict.")
    else:
        pair_rows = [row for row in rows if row.get("asset_type") != "INDEX"]
        index_rows = [row for row in rows if row.get("asset_type") == "INDEX"]

        for row in pair_rows:
            direction = int(row["full_alignment_direction"])
            icon = "🟢" if direction == 1 else "🔴"
            name = _asset_display_name(row)
            lines.append(f"{icon} {name}{_sar_suffix(row)}")
        if pair_rows and index_rows:
            lines.append("")
        for row in index_rows:
            direction = int(row["full_alignment_direction"])
            icon = "🟢" if direction == 1 else "🔴"
            name = _asset_display_name(row)
            lines.append(f"{icon} {name}{_sar_suffix(row)}")

    if full_sar_rows:
        lines.extend(["", "⚡ FULL SAR"])
        pair_rows = [row for row in full_sar_rows if row.get("asset_type") != "INDEX"]
        index_rows = [row for row in full_sar_rows if row.get("asset_type") == "INDEX"]
        for row in pair_rows:
            direction = int(row["full_alignment_direction"])
            icon = "🟢" if direction == 1 else "🔴"
            name = _asset_display_name(row)
            lines.append(f"{icon} {name} 🔥")
        if pair_rows and index_rows:
            lines.append("")
        for row in index_rows:
            direction = int(row["full_alignment_direction"])
            icon = "🟢" if direction == 1 else "🔴"
            name = _asset_display_name(row)
            lines.append(f"{icon} {name} 🔥")
    history_events = []
    if isinstance(full_sar_history, dict):
        raw_events = full_sar_history.get("events") or []
        if isinstance(raw_events, list):
            history_events = [event for event in raw_events if isinstance(event, dict)]
    if history_events:
        lines.extend(["", "📋 FULL SAR 07H-23H"])
        for event in history_events:
            lines.append(_format_full_sar_history_event(event))
    lines.extend(["", f"⏰ {now:%Y-%m-%d %H:%M} Paris"])
    return "\n".join(lines)


def scan_assets(assets: list[dict], length: int, candles: int, max_streak: int) -> list[dict]:
    rows: list[dict] = []
    for asset in assets:
        label = str(asset["pair"])
        try:
            row = compute_asset_score(asset, length, candles, max_streak)
        except Exception as exc:  # keep the scanner usable if one pair fails
            print(f"{label}: erreur {exc}")
            continue
        if row is not None:
            rows.append(row)
    return rows


def scan_pairs(length: int, candles: int, max_streak: int) -> list[dict]:
    return scan_assets(FOREX_PAIR_ASSETS, length, candles, max_streak)


def main() -> int:
    args = parse_args()
    now = datetime.now(PARIS_TZ)
    rows = scan_assets(assets_for_scope(args.assets), args.length, args.candles, args.max_streak)
    selected = select_full_alignment_rows(rows)
    attach_sar_break_states(selected, args.sar_candles)
    full_sar_rows = select_full_sar_rows(selected)
    history_state = load_full_sar_history_state(args.full_sar_state_file)
    history_state, today_history = update_full_sar_history(history_state, full_sar_rows, now)
    save_full_sar_history_state(args.full_sar_state_file, history_state)
    message = format_full_alignment_message(selected, full_sar_rows, today_history, now=now)
    print(message)
    if args.telegram:
        send_telegram_message(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
