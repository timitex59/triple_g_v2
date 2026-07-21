"""Scanner dedicated to strict M/W/D Renko full alignment on FX assets.

This is intentionally separate from the VIVIER state machine. It only detects
pairs where price is strictly outside Monthly, Weekly and Daily Renko bricks in
the same direction:

- BULL: M+/W+/D+ => raw score +100%
- BEAR: M-/W-/D- => raw score -100%
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
FULL_CASSURE_STATE_FILE = Path("renko_full_alignment_full_cassure_state.json")
FULL_CASSURE_WINDOW_START_HOUR = 7
FULL_CASSURE_WINDOW_END_HOUR = 23

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
        "--imp-candles",
        type=int,
        default=400,
        help="Number of H1 candles used to detect Bull/Bear IMP events.",
    )
    parser.add_argument(
        "--telegram",
        action="store_true",
        help="Send the scanner result to Telegram. By default it only prints.",
    )
    parser.add_argument(
        "--full-cassure-state-file",
        default=str(FULL_CASSURE_STATE_FILE),
        help="JSON state file used to persist FULL-CASSURE detections between 07:00 and 23:00 Paris.",
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


def imp_state_from_close_sar(closes: list[float], sar_values: list[float]) -> dict:
    """Mirror Pine IMP levels and detect their later breaks.

    Formation:
    - Bear IMP: price closes below the last Bull SAR level.
    - Bull IMP: price closes above the last Bear SAR level.

    Break:
    - Bull IMP broken down: price crosses below the active Bull IMP level.
    - Bear IMP broken up: price crosses above the active Bear IMP level.
    """
    last_bull_level = None
    last_bear_level = None
    tracked_bull_level = None
    tracked_bear_level = None
    tracked_bull_important = False
    tracked_bear_important = False
    events: list[dict] = []
    break_events: list[dict] = []
    active_bull_imp_level = None
    active_bear_imp_level = None
    active_bull_imp_broken = False
    active_bear_imp_broken = False

    count = min(len(closes), len(sar_values))
    for index in range(1, count):
        close = float(closes[index])
        prev_close = float(closes[index - 1])
        sar = float(sar_values[index])
        prev_sar = float(sar_values[index - 1])

        bull = close > sar and prev_close <= prev_sar
        bear = close < sar and prev_close >= prev_sar

        if bull:
            last_bull_level = sar
            tracked_bull_level = sar
            tracked_bull_important = False
        if bear:
            last_bear_level = sar
            tracked_bear_level = sar
            tracked_bear_important = False

        bear_imp = (
            not tracked_bear_important
            and tracked_bear_level is not None
            and last_bull_level is not None
            and close < last_bull_level
        )
        bull_imp = (
            not tracked_bull_important
            and tracked_bull_level is not None
            and last_bear_level is not None
            and close > last_bear_level
        )

        if bear_imp:
            tracked_bear_important = True
            active_bear_imp_level = tracked_bear_level
            active_bear_imp_broken = False
            events.append({
                "index": index,
                "direction": -1,
                "level": tracked_bear_level,
                "kind": "IMP BEAR",
            })
        if bull_imp:
            tracked_bull_important = True
            active_bull_imp_level = tracked_bull_level
            active_bull_imp_broken = False
            events.append({
                "index": index,
                "direction": 1,
                "level": tracked_bull_level,
                "kind": "IMP BULL",
            })

        bear_imp_break_up = (
            active_bear_imp_level is not None
            and not active_bear_imp_broken
            and prev_close <= active_bear_imp_level
            and close > active_bear_imp_level
        )
        bull_imp_break_down = (
            active_bull_imp_level is not None
            and not active_bull_imp_broken
            and prev_close >= active_bull_imp_level
            and close < active_bull_imp_level
        )

        if bear_imp_break_up:
            active_bear_imp_broken = True
            break_events.append({
                "index": index,
                "direction": 1,
                "level": active_bear_imp_level,
                "kind": "IMP BEAR CASSÉ HAUSSE",
            })
        if bull_imp_break_down:
            active_bull_imp_broken = True
            break_events.append({
                "index": index,
                "direction": -1,
                "level": active_bull_imp_level,
                "kind": "IMP BULL CASSÉ BAISSE",
            })

    last_event = events[-1] if events else None
    last_break_event = break_events[-1] if break_events else None
    last_bar_index = count - 1
    last_bar_imp_direction = (
        int(last_event["direction"])
        if last_event is not None and int(last_event["index"]) == last_bar_index
        else 0
    )
    last_bar_break_direction = (
        int(last_break_event["direction"])
        if last_break_event is not None and int(last_break_event["index"]) == last_bar_index
        else 0
    )
    return {
        "last_bar_imp_direction": last_bar_imp_direction,
        "last_imp_direction": int(last_event["direction"]) if last_event else 0,
        "last_imp_level": float(last_event["level"]) if last_event else None,
        "last_bar_break_direction": last_bar_break_direction,
        "last_bar_break_kind": (
            str(last_break_event["kind"])
            if last_break_event is not None and int(last_break_event["index"]) == last_bar_index
            else ""
        ),
        "last_break_direction": int(last_break_event["direction"]) if last_break_event else 0,
        "last_break_level": float(last_break_event["level"]) if last_break_event else None,
        "events": events,
        "break_events": break_events,
    }


def compute_imp_state_for_symbol(tv_symbol: str, h1_candles: int = 400) -> dict:
    df = fetch_tv_ohlc(tv_symbol, "60", h1_candles)
    if df is None or df.empty:
        return {"last_bar_imp_direction": 0, "last_imp_direction": 0,
                "last_bar_break_direction": 0, "last_break_direction": 0}
    closed_df = closed_h1_source(df)
    sar_state = parabolic_sar(closed_df)
    if not sar_state:
        return {"last_bar_imp_direction": 0, "last_imp_direction": 0,
                "last_bar_break_direction": 0, "last_break_direction": 0}
    sar_values = sar_state.get("sar") or []
    closes = [float(value) for value in closed_df["close"].tolist()]
    return imp_state_from_close_sar(closes, sar_values)


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


def is_directional_imp_break(row: dict, direction: int) -> bool:
    imp = row.get("imp") or {}
    break_kind = str(imp.get("last_bar_break_kind") or "")
    if direction == 1:
        return break_kind == "IMP BEAR CASSÉ HAUSSE"
    if direction == -1:
        return break_kind == "IMP BULL CASSÉ BAISSE"
    return False


def select_full_cassure_rows(rows: list[dict]) -> list[dict]:
    return [
        row for row in rows
        if is_directional_imp_break(row, int(row.get("full_alignment_direction") or 0))
    ]


def default_full_cassure_history_state() -> dict:
    return {"version": 1, "days": {}}


def load_full_cassure_history_state(path: str | Path) -> dict:
    state_path = Path(path)
    if not state_path.exists():
        return default_full_cassure_history_state()
    try:
        loaded = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return default_full_cassure_history_state()
    if not isinstance(loaded, dict):
        return default_full_cassure_history_state()
    loaded.setdefault("version", 1)
    if not isinstance(loaded.get("days"), dict):
        loaded["days"] = {}
    return loaded


def save_full_cassure_history_state(path: str | Path, state: dict) -> None:
    state_path = Path(path)
    if state_path.parent != Path("."):
        state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _is_full_cassure_tracking_window(now: datetime) -> bool:
    paris_now = now.astimezone(PARIS_TZ)
    return FULL_CASSURE_WINDOW_START_HOUR <= paris_now.hour <= FULL_CASSURE_WINDOW_END_HOUR


def _prune_full_cassure_history_state(state: dict, keep_days: int = 45) -> None:
    days = state.setdefault("days", {})
    if not isinstance(days, dict):
        state["days"] = {}
        return
    for day_key in sorted(days)[:-keep_days]:
        days.pop(day_key, None)


def update_full_cassure_history(
    state: dict,
    full_cassure_rows: list[dict],
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

    if not _is_full_cassure_tracking_window(paris_now):
        _prune_full_cassure_history_state(state)
        return state, day_state

    by_key = {
        str(event.get("key")): event
        for event in events
        if isinstance(event, dict) and event.get("key")
    }
    timestamp = paris_now.isoformat(timespec="minutes")
    for row in full_cassure_rows:
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
    _prune_full_cassure_history_state(state)
    return state, day_state


def attach_imp_states(rows: list[dict], h1_candles: int = 400) -> list[dict]:
    for row in rows:
        tv_symbol = row.get("tv_symbol")
        if isinstance(tv_symbol, str) and tv_symbol:
            row["imp"] = compute_imp_state_for_symbol(tv_symbol, h1_candles)
    return rows


def _format_px(row: dict) -> str:
    px = _px(row) or {}
    symbol = {1: "+", 0: "0", -1: "-"}
    return "/".join(f"{tf}{symbol.get(px.get(tf), '?')}" for tf in ("M", "W", "D"))


def _imp_suffix(row: dict) -> str:
    alignment_direction = int(row.get("full_alignment_direction") or 0)
    if is_directional_imp_break(row, alignment_direction):
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


def _format_full_history_event(event: dict) -> str:
    direction = int(event.get("direction") or 0)
    icon = "🟢" if direction == 1 else "🔴"
    name = _history_asset_display_name(event)
    time_label = _format_history_time_range(event)
    suffix = f" {time_label}" if time_label else ""
    return f"{icon} {name} 🔥{suffix}"


def format_full_alignment_message(
    rows: list[dict],
    full_cassure_rows: list[dict] | None = None,
    full_cassure_history: dict | None = None,
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
            lines.append(f"{icon} {name}{_imp_suffix(row)}")
        if pair_rows and index_rows:
            lines.append("")
        for row in index_rows:
            direction = int(row["full_alignment_direction"])
            icon = "🟢" if direction == 1 else "🔴"
            name = _asset_display_name(row)
            lines.append(f"{icon} {name}{_imp_suffix(row)}")

    if full_cassure_rows:
        lines.extend(["", "⚡ FULL-CASSURE"])
        pair_rows = [row for row in full_cassure_rows if row.get("asset_type") != "INDEX"]
        index_rows = [row for row in full_cassure_rows if row.get("asset_type") == "INDEX"]
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
    if isinstance(full_cassure_history, dict):
        raw_events = full_cassure_history.get("events") or []
        if isinstance(raw_events, list):
            history_events = [event for event in raw_events if isinstance(event, dict)]
    if history_events:
        lines.extend(["", "📋 FULL-CASSURE 07H-23H"])
        for event in history_events:
            lines.append(_format_full_history_event(event))
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
    attach_imp_states(selected, args.imp_candles)
    full_cassures = select_full_cassure_rows(selected)
    history_state = load_full_cassure_history_state(args.full_cassure_state_file)
    history_state, today_history = update_full_cassure_history(history_state, full_cassures, now)
    save_full_cassure_history_state(args.full_cassure_state_file, history_state)
    message = format_full_alignment_message(selected, full_cassures, today_history, now=now)
    print(message)
    if args.telegram:
        send_telegram_message(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
