"""Scanner dedicated to strict M/W/D Renko full alignment on FX assets.

This is intentionally separate from the VIVIER state machine. It only detects
pairs where price is strictly outside Monthly, Weekly and Daily Renko bricks in
the same direction:

- BULL: M+/W+/D+ => raw score +100%
- BEAR: M-/W-/D- => raw score -100%
"""

from __future__ import annotations

import argparse
from datetime import datetime
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
    imp = row.get("imp") or {}
    alignment_direction = int(row.get("full_alignment_direction") or 0)
    break_kind = str(imp.get("last_bar_break_kind") or "")
    if alignment_direction == 1 and break_kind == "IMP BEAR CASSÉ HAUSSE":
        return " 🔥"
    if alignment_direction == -1 and break_kind == "IMP BULL CASSÉ BAISSE":
        return " 🔥"
    return ""


def format_full_alignment_message(rows: list[dict], now: datetime | None = None) -> str:
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
            name = str(row["pair"])
            lines.append(f"{icon} {name}{_imp_suffix(row)}")
        if pair_rows and index_rows:
            lines.append("")
        for row in index_rows:
            direction = int(row["full_alignment_direction"])
            icon = "🟢" if direction == 1 else "🔴"
            name = str(row.get("currency") or row["pair"])
            lines.append(f"{icon} {name}{_imp_suffix(row)}")
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
    rows = scan_assets(assets_for_scope(args.assets), args.length, args.candles, args.max_streak)
    selected = select_full_alignment_rows(rows)
    attach_imp_states(selected, args.imp_candles)
    message = format_full_alignment_message(selected)
    print(message)
    if args.telegram:
        send_telegram_message(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
