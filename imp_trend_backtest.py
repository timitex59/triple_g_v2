#!/usr/bin/env python3
"""Walk-forward history for the 11-criterion IMP Trend screener.

The test uses only H1 candles whose close is known at each snapshot. The live
Daily candle is rebuilt from those H1 candles. Native TradingView Renko bricks
are time-filtered before every calculation so later bricks are not consumed.
"""

from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

import imp_trend_29pairs as imp


PARIS = ZoneInfo("Europe/Paris")
UTC = ZoneInfo("UTC")


@dataclass
class PairHistory:
    pair: str
    h1: pd.DataFrame
    d1: pd.DataFrame
    renko: dict[str, list[imp.RenkoPoint]]


def fetch_history(pair: str, args: argparse.Namespace) -> PairHistory:
    return PairHistory(
        pair=pair,
        h1=imp.fetch_ohlc(pair, "60", args.h1_candles),
        d1=imp.fetch_ohlc(pair, "D", args.d1_candles),
        renko={
            tf: imp.fetch_renko(pair, tf, args.renko_bricks, args.atr_length, args.max_streak)
            for tf in ("M", "W", "D")
        },
    )


def daily_as_of(history: PairHistory, timestamp: pd.Timestamp, closed_h1: pd.DataFrame) -> pd.DataFrame:
    starts = history.d1.loc[history.d1["time"] < timestamp, "time"]
    if starts.empty:
        return history.d1.iloc[0:0].copy()
    current_start = starts.iloc[-1]
    previous = history.d1[history.d1["time"] < current_start].copy()
    partial = closed_h1[(closed_h1["time"] >= current_start) & (closed_h1["time"] < timestamp)]
    if partial.empty:
        return previous
    current = pd.DataFrame([{
        "time": current_start,
        "open": float(partial["open"].iloc[0]),
        "high": float(partial["high"].max()),
        "low": float(partial["low"].min()),
        "close": float(partial["close"].iloc[-1]),
    }])
    return pd.concat([previous, current], ignore_index=True)


def snapshot_pair(history: PairHistory, timestamp: pd.Timestamp) -> dict | None:
    # A TradingView H1 timestamp is the opening time. At 14:30, for example,
    # the 14:00 candle is still open and the latest usable candle is 13:00.
    last_closed_open = timestamp - pd.Timedelta(hours=1)
    h1 = history.h1[history.h1["time"] <= last_closed_open].copy()
    if len(h1) < 50:
        return None
    d1 = daily_as_of(history, timestamp, h1)
    if len(d1) < 50:
        return None
    renko = {
        tf: [point for point in history.renko[tf] if point.time < timestamp]
        for tf in ("M", "W", "D")
    }
    if any(not points for points in renko.values()):
        return None
    reference_price = float(h1["close"].iloc[-1])
    return {
        "pair": history.pair,
        "reference_price": reference_price,
        "renko": imp.current_renko_status(renko, reference_price),
        "H1": imp.replay_imp(history.pair, h1, renko),
        "D1": imp.replay_imp(history.pair, d1, renko),
    }


def parse_paris(value: str) -> pd.Timestamp:
    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=PARIS)
    return pd.Timestamp(parsed.astimezone(UTC))


def fmt_paris(timestamp: pd.Timestamp) -> str:
    return timestamp.tz_convert(PARIS).strftime("%Y-%m-%d %H:%M")


def run_backtest(
    histories: dict[str, PairHistory],
    snapshots: pd.DatetimeIndex,
    force_exit_time: tuple[int, int] | None,
) -> tuple[list[dict], list[dict], list[dict]]:
    progression: list[dict] = []
    selected_history: list[dict] = []
    events: list[dict] = []
    active: dict[str, dict] = {}
    blocked_entry_date = None

    for snapshot_index, timestamp in enumerate(snapshots):
        results = []
        for pair in imp.PAIRS_29:
            history = histories.get(pair)
            if history is None:
                continue
            result = snapshot_pair(history, timestamp)
            if result is not None:
                results.append(result)
        selected = imp.select_aligned_pairs(results)
        current = {item["pair"]: item for item in selected}
        prices = {item["pair"]: item["reference_price"] for item in results}
        local_timestamp = timestamp.tz_convert(PARIS)
        force_exit_now = (
            force_exit_time is not None
            and (local_timestamp.hour, local_timestamp.minute) == force_exit_time
        )

        if force_exit_now:
            for pair in list(active):
                exit_price = prices.get(pair)
                if exit_price is not None:
                    reason = f"FORCED_EXIT_{force_exit_time[0]:02d}H{force_exit_time[1]:02d}"
                    events.append(close_event(pair, active[pair], timestamp, exit_price, reason))
                del active[pair]
            blocked_entry_date = local_timestamp.date()
        else:
            entries_allowed = local_timestamp.date() != blocked_entry_date
            if entries_allowed:
                for pair, item in current.items():
                    old = active.get(pair)
                    if old is None or old["direction"] != item["direction"]:
                        if old is not None:
                            exit_price = prices[pair]
                            events.append(close_event(pair, old, timestamp, exit_price, "DIRECTION_CHANGED"))
                        active[pair] = {
                            "direction": item["direction"],
                            "entry_time": timestamp,
                            "entry_price": prices[pair],
                        }
                        events.append({
                            "time_paris": fmt_paris(timestamp),
                            "event": "INITIAL" if snapshot_index == 0 else "ENTRY",
                            "pair": pair,
                            "direction": item["direction"],
                            "score": item["confirmations"],
                            "rank": item["rank_tier"],
                            "price": prices[pair],
                            "pips": 0.0,
                            "reason": item["rank_reason"],
                            "entry_time_paris": fmt_paris(timestamp),
                            "entry_price": prices[pair],
                        })

            for pair in list(active):
                if pair in current:
                    continue
                result = next((item for item in results if item["pair"] == pair), None)
                exit_price = prices.get(pair)
                if exit_price is not None:
                    reason = imp.invalidation_reason(result)
                    events.append(close_event(pair, active[pair], timestamp, exit_price, reason))
                del active[pair]

        bull = [item["pair"] for item in selected if item["direction"] == "BULL"]
        bear = [item["pair"] for item in selected if item["direction"] == "BEAR"]
        active_bull = [pair for pair, position in active.items() if position["direction"] == "BULL"]
        active_bear = [pair for pair, position in active.items() if position["direction"] == "BEAR"]
        progression.append({
            "time_paris": fmt_paris(timestamp),
            "eligible_count": len(selected),
            "bull_count": len(bull),
            "bear_count": len(bear),
            "bull_pairs": ",".join(bull),
            "bear_pairs": ",".join(bear),
            "position_count": len(active),
            "bull_positions": ",".join(active_bull),
            "bear_positions": ",".join(active_bear),
        })
        for item in selected:
            selected_history.append({
                "time_paris": fmt_paris(timestamp),
                **item,
                "price": prices[item["pair"]],
            })
    return progression, selected_history, events


def close_event(pair: str, position: dict, timestamp: pd.Timestamp, exit_price: float, reason: str) -> dict:
    direction_value = 1 if position["direction"] == "BULL" else -1
    pips = direction_value * (exit_price - position["entry_price"]) / imp.pip_size(pair)
    return {
        "time_paris": fmt_paris(timestamp),
        "event": "EXIT",
        "pair": pair,
        "direction": position["direction"],
        "score": None,
        "rank": None,
        "price": exit_price,
        "pips": pips,
        "reason": reason,
        "entry_time_paris": fmt_paris(position["entry_time"]),
        "entry_price": position["entry_price"],
    }


def build_summary(events: list[dict], progression: list[dict]) -> dict:
    exits = [event for event in events if event["event"] == "EXIT"]
    winning = [event for event in exits if event["pips"] > 0]
    losing = [event for event in exits if event["pips"] < 0]
    flat = [event for event in exits if event["pips"] == 0]
    gross_profit = sum(event["pips"] for event in winning)
    gross_loss = sum(event["pips"] for event in losing)
    net_pips = gross_profit + gross_loss

    by_pair = []
    for pair in sorted({event["pair"] for event in exits}):
        pair_exits = [event for event in exits if event["pair"] == pair]
        pair_pips = sum(event["pips"] for event in pair_exits)
        by_pair.append({
            "pair": pair,
            "closed_trades": len(pair_exits),
            "winning_trades": sum(event["pips"] > 0 for event in pair_exits),
            "losing_trades": sum(event["pips"] < 0 for event in pair_exits),
            "net_pips": pair_pips,
        })
    by_pair.sort(key=lambda item: item["net_pips"], reverse=True)

    return {
        "closed_trades": len(exits),
        "winning_trades": len(winning),
        "losing_trades": len(losing),
        "flat_trades": len(flat),
        "win_rate_percent": (len(winning) / len(exits) * 100.0) if exits else 0.0,
        "gross_profit_pips": gross_profit,
        "gross_loss_pips": gross_loss,
        "net_pips": net_pips,
        "average_pips_per_trade": (net_pips / len(exits)) if exits else 0.0,
        "open_positions_at_end": progression[-1]["position_count"] if progression else 0,
        "by_pair": by_pair,
    }


def parse_force_exit_time(value: str) -> tuple[int, int] | None:
    if value == "-1":
        return None
    parts = value.split(":")
    if len(parts) not in (1, 2):
        raise argparse.ArgumentTypeError("use HH or HH:MM, for example 23 or 14:30")
    try:
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) == 2 else 0
    except ValueError as exc:
        raise argparse.ArgumentTypeError("use HH or HH:MM, for example 23 or 14:30") from exc
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise argparse.ArgumentTypeError("hour must be 00-23 and minute must be 00-59")
    return hour, minute


def build_snapshots(
    start: pd.Timestamp,
    end: pd.Timestamp,
    frequency: str,
    force_exit_time: tuple[int, int] | None,
) -> pd.DatetimeIndex:
    snapshots = pd.date_range(start=start, end=end, freq=frequency, tz="UTC")
    if force_exit_time is None:
        return snapshots

    start_date = start.tz_convert(PARIS).date()
    end_date = end.tz_convert(PARIS).date()
    forced = []
    for day in pd.date_range(start=start_date, end=end_date, freq="D"):
        local = pd.Timestamp(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=force_exit_time[0],
            minute=force_exit_time[1],
            tz=PARIS,
        ).tz_convert(UTC)
        if start <= local <= end:
            forced.append(local)
    return snapshots.union(pd.DatetimeIndex(forced)).sort_values()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hourly walk-forward backtest of the IMP Trend screener")
    parser.add_argument("--start", default="2026-08-21 00:00", help="Paris time, YYYY-MM-DD HH:MM")
    parser.add_argument("--end", help="Paris time; defaults to the latest complete hour")
    parser.add_argument("--frequency", default="1h", help="Pandas frequency, e.g. 1h or 4h")
    parser.add_argument(
        "--force-exit-hour",
        "--force-exit-time",
        type=parse_force_exit_time,
        default=parse_force_exit_time("23:00"),
        metavar="HH[:MM]",
        help="Paris time for mandatory daily exit; use -1 to disable (default: 23:00)",
    )
    parser.add_argument("--h1-candles", type=int, default=5000)
    parser.add_argument("--d1-candles", type=int, default=2500)
    parser.add_argument("--renko-bricks", type=int, default=2500)
    parser.add_argument("--atr-length", type=int, default=14)
    parser.add_argument("--max-streak", type=int, default=50)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--output-prefix", type=Path, default=Path("imp_trend_backtest"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    histories: dict[str, PairHistory] = {}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(fetch_history, pair, args): pair for pair in imp.PAIRS_29}
        for future in as_completed(futures):
            pair = futures[future]
            try:
                histories[pair] = future.result()
                print(f"{pair}: OK")
            except Exception as exc:
                print(f"{pair}: ERROR - {exc}")

    if not histories:
        return 1
    start = parse_paris(args.start)
    end = parse_paris(args.end) if args.end else pd.Timestamp.now(tz="UTC").floor("h")
    force_exit_time = args.force_exit_hour
    snapshots = build_snapshots(start, end, args.frequency, force_exit_time)
    progression, selected, events = run_backtest(histories, snapshots, force_exit_time)
    summary = build_summary(events, progression)

    prefix = args.output_prefix
    prefix.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(progression).to_csv(f"{prefix}_progression.csv", index=False)
    pd.DataFrame(selected).to_csv(f"{prefix}_eligible.csv", index=False)
    pd.DataFrame(events).to_csv(f"{prefix}_events.csv", index=False)
    Path(f"{prefix}.json").write_text(json.dumps({
        "method": "walk_forward_hourly",
        "start_paris": args.start,
        "end_paris": fmt_paris(end),
        "force_exit_time_paris": (
            f"{force_exit_time[0]:02d}:{force_exit_time[1]:02d}" if force_exit_time else None
        ),
        "progression": progression,
        "eligible": selected,
        "events": events,
        "summary": summary,
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    print("\nPROGRESSION")
    print(pd.DataFrame(progression).to_string(index=False))
    print("\nEVENEMENTS")
    print(pd.DataFrame(events).fillna("").to_string(index=False) if events else "Aucun evenement")
    print("\nBILAN REALISE")
    print(f"Trades clotures : {summary['closed_trades']}")
    print(f"Gagnants / perdants / nuls : {summary['winning_trades']} / {summary['losing_trades']} / {summary['flat_trades']}")
    print(f"Taux de reussite : {summary['win_rate_percent']:.1f}%")
    print(f"Pips gagnes : +{summary['gross_profit_pips']:.1f}")
    print(f"Pips perdus : {summary['gross_loss_pips']:.1f}")
    print(f"TOTAL NET : {summary['net_pips']:+.1f} pips")
    print(f"Moyenne par trade : {summary['average_pips_per_trade']:+.1f} pips")
    print(f"Positions encore ouvertes : {summary['open_positions_at_end']} (non incluses dans le total)")
    if summary["by_pair"]:
        print("\nRESULTAT PAR PAIRE")
        print(pd.DataFrame(summary["by_pair"]).to_string(index=False, formatters={"net_pips": "{:+.1f}".format}))
    print(f"\nSnapshots: {len(progression)}")
    print(f"Exports: {prefix}_progression.csv, {prefix}_eligible.csv, {prefix}_events.csv, {prefix}.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
