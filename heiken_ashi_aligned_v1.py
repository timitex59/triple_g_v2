#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
heiken_ashi_aligned_v1.py

Scan des 29 instruments avec la logique du script Pine `heiken_ashi_alignment.pine`:
- Heikin Ashi Daily
- Heikin Ashi Weekly
- Prix H1 au-dessus/en-dessous du nuage Ichimoku
- CHG% CC daily

Alignement bull:
- HA Daily = BULL
- HA Weekly = BULL
- H1 Cloud = BULL
- CHG% CC daily > 0 et abs(CHG% CC daily) > 0.1

Alignement bear:
- HA Daily = BEAR
- HA Weekly = BEAR
- H1 Cloud = BEAR
- CHG% CC daily < 0 et abs(CHG% CC daily) > 0.1
"""

import argparse
import json
import os
import random
import string
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from websocket import WebSocketConnectionClosedException, create_connection

TIMEFRAME_LABELS = {
    "60": "H1",
    "D": "D1",
    "W": "W1",
}

PAIRS_29 = [
    "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "AUDUSD",
    "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD",
    "EURCHF", "EURGBP", "EURJPY", "EURNZD", "EURUSD",
    "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD",
    "GBPUSD", "NZDCAD", "NZDCHF", "NZDJPY", "NZDUSD",
    "USDCAD", "USDCHF", "USDJPY", "XAUUSD",
]

PARIS_TZ = ZoneInfo("Europe/Paris")
MIN_ABS_DAILY_CHG_CC = 0.1
ENV_PATH = Path(__file__).with_name(".env")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


@dataclass
class HaState:
    pair: str
    interval: str
    label: str
    state: int
    color: str
    close: float | None
    open: float | None


@dataclass
class PairAlignment:
    pair: str
    daily_state: int
    weekly_state: int
    h1_cloud_state: int
    daily_chg_cc: float | None
    align_state: int


@dataclass
class ScanResult:
    rows: list[PairAlignment]
    errors: list[str]


def _gen_session_id() -> str:
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


def _create_msg(func, args) -> str:
    payload = json.dumps({"m": func, "p": args})
    return f"~m~{len(payload)}~m~{payload}"


def _parse_frames(raw: str):
    if raw in ("~h", "h"):
        return [raw]
    out = []
    i = 0
    while raw.startswith("~m~", i):
        i += 3
        j = raw.find("~m~", i)
        if j == -1:
            break
        size = int(raw[i:j])
        i = j + 3
        out.append(raw[i : i + size])
        i += size
    return out if out else [raw]


def fetch_tv_ohlc(symbol: str, interval: str, n_candles: int, timeout_s: int = 20, retries: int = 2, debug: bool = False) -> pd.DataFrame | None:
    for attempt in range(retries + 1):
        ws = None
        try:
            if debug:
                print(f"[fetch] {symbol} {interval} attempt {attempt + 1}/{retries + 1}")
            ws = create_connection(
                "wss://prodata.tradingview.com/socket.io/websocket",
                header={"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"},
                timeout=timeout_s,
            )
            sid = _gen_session_id()
            ws.send(_create_msg("chart_create_session", [sid, ""]))
            ws.send(
                _create_msg(
                    "resolve_symbol",
                    [sid, "sds_sym_1", f'={{"symbol":"{symbol}","adjustment":"splits","session":"regular"}}'],
                )
            )
            ws.send(_create_msg("create_series", [sid, "sds_1", "s1", "sds_sym_1", interval, n_candles, ""]))

            points = []
            t0 = time.time()
            while time.time() - t0 < 14:
                try:
                    raw = ws.recv()
                except WebSocketConnectionClosedException:
                    break

                for frame in _parse_frames(raw):
                    if frame in ("~h", "h"):
                        ws.send("~h")
                        continue

                    if '"m":"timescale_update"' in frame:
                        payload = json.loads(frame)
                        series = payload.get("p", [None, {}])[1]
                        if isinstance(series, dict) and "sds_1" in series:
                            points = series["sds_1"].get("s", []) or points
                            if debug and points:
                                print(f"[fetch] {symbol} {interval} received {len(points)} points")

                    if "series_completed" in frame:
                        break

                if "series_completed" in raw:
                    break

            if not points:
                if debug:
                    print(f"[fetch] {symbol} {interval} no points returned")
                continue

            rows = []
            for item in points:
                values = item.get("v", [])
                if len(values) < 5:
                    continue
                ts, o, h, l, c = values[:5]
                rows.append({"date": ts, "open": o, "high": h, "low": l, "close": c})

            if not rows:
                if debug:
                    print(f"[fetch] {symbol} {interval} points had no OHLC rows")
                continue

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"], unit="s", utc=True)
            if debug:
                print(f"[fetch] {symbol} {interval} OK rows={len(df)} first={df['date'].iloc[0]} last={df['date'].iloc[-1]}")
            return df.set_index("date").sort_index()
        except Exception as exc:
            if debug:
                print(f"[fetch] {symbol} {interval} ERROR: {type(exc).__name__}: {exc}")
            if attempt >= retries:
                return None
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
    return None


def load_local_env(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def parse_args():
    parser = argparse.ArgumentParser(description="Scan HA D1/W1 alignment with H1 Ichimoku cloud and daily CHG% CC.")
    parser.add_argument("--pair", type=str, help="Single pair to analyze, e.g. EURUSD.")
    parser.add_argument("--candles-h1", type=int, default=300, help="Number of H1 candles to fetch.")
    parser.add_argument("--candles-d", type=int, default=200, help="Number of D1 candles to fetch.")
    parser.add_argument("--candles-w", type=int, default=200, help="Number of W1 candles to fetch.")
    parser.add_argument("--show-all", action="store_true", help="Show all pairs, not only aligned ones.")
    parser.add_argument("--no-telegram", action="store_true", help="Do not send aligned pairs to Telegram.")
    parser.add_argument("--telegram-title", type=str, default="HEIKEN_ICHI", help="Telegram message title.")
    parser.add_argument("--debug-fetch", action="store_true", help="Print websocket fetch diagnostics.")
    return parser.parse_args()


def state_name(value: int) -> str:
    return "BULL" if value == 1 else "BEAR" if value == -1 else "MIXED"


def heikin_ashi_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out["ha_close"] = (out["open"] + out["high"] + out["low"] + out["close"]) / 4.0

    ha_open = []
    for i, (_, row) in enumerate(out.iterrows()):
        if i == 0:
            ha_open.append((float(row["open"]) + float(row["close"])) / 2.0)
        else:
            ha_open.append((ha_open[-1] + float(out["ha_close"].iloc[i - 1])) / 2.0)
    out["ha_open"] = ha_open
    out["ha_high"] = out[["high", "ha_open", "ha_close"]].max(axis=1)
    out["ha_low"] = out[["low", "ha_open", "ha_close"]].min(axis=1)
    return out


def compute_ha_state(pair: str, interval: str, candles: int, debug_fetch: bool = False) -> HaState:
    df = fetch_tv_ohlc(f"OANDA:{pair}", interval, candles, debug=debug_fetch)
    if df is None or df.empty:
        raise RuntimeError(f"Unable to fetch OHLC for {pair} on {interval}")
    return compute_ha_state_from_df(pair, interval, df)


def compute_ha_state_from_df(pair: str, interval: str, df: pd.DataFrame) -> HaState:
    if df is None or df.empty:
        raise RuntimeError(f"Unable to compute HA state for {pair} on {interval}: empty dataframe")

    ha = heikin_ashi_df(df)
    last_ha_open = float(ha["ha_open"].iloc[-1])
    last_ha_close = float(ha["ha_close"].iloc[-1])
    state = 1 if last_ha_close > last_ha_open else -1 if last_ha_close < last_ha_open else 0
    return HaState(
        pair=pair,
        interval=interval,
        label=TIMEFRAME_LABELS.get(interval, interval),
        state=state,
        color=state_name(state),
        close=last_ha_close,
        open=last_ha_open,
    )


def donchian_avg(high: pd.Series, low: pd.Series, length: int) -> pd.Series:
    return (high.rolling(length).max() + low.rolling(length).min()) / 2.0


def ichimoku_price_state(
    df: pd.DataFrame,
    conversion_periods: int = 9,
    base_periods: int = 26,
    span_b_periods: int = 52,
    displacement: int = 26,
) -> int:
    if df is None or df.empty:
        return 0

    min_bars = max(span_b_periods + displacement + 5, base_periods + displacement + 5)
    if len(df) < min_bars:
        return 0

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    conversion_line = donchian_avg(high, low, conversion_periods)
    base_line = donchian_avg(high, low, base_periods)
    lead_line1 = (conversion_line + base_line) / 2.0
    lead_line2 = donchian_avg(high, low, span_b_periods)

    shift = max(displacement - 1, 0)
    visible_span_a = lead_line1.shift(shift)
    visible_span_b = lead_line2.shift(shift)
    cloud_top = pd.concat([visible_span_a, visible_span_b], axis=1).max(axis=1)
    cloud_bottom = pd.concat([visible_span_a, visible_span_b], axis=1).min(axis=1)

    last_close = close.iloc[-1]
    last_top = cloud_top.iloc[-1]
    last_bottom = cloud_bottom.iloc[-1]

    if pd.isna(last_top) or pd.isna(last_bottom):
        return 0
    if last_close > last_top:
        return 1
    if last_close < last_bottom:
        return -1
    return 0


def daily_chg_cc(df_d1: pd.DataFrame) -> float | None:
    if df_d1 is None or df_d1.empty or len(df_d1) < 2:
        return None
    prev_close = float(df_d1["close"].iloc[-2])
    last_close = float(df_d1["close"].iloc[-1])
    if prev_close == 0:
        return None
    return ((last_close - prev_close) / prev_close) * 100.0


def compute_pair_alignment(pair: str, candles_h1: int, candles_d: int, candles_w: int, debug_fetch: bool = False) -> PairAlignment:
    df_h1 = fetch_tv_ohlc(f"OANDA:{pair}", "60", candles_h1, debug=debug_fetch)
    df_d = fetch_tv_ohlc(f"OANDA:{pair}", "D", candles_d, debug=debug_fetch)
    df_w = fetch_tv_ohlc(f"OANDA:{pair}", "W", candles_w, debug=debug_fetch)

    if df_h1 is None or df_h1.empty:
        raise RuntimeError(f"Unable to fetch H1 OHLC for {pair}")
    if df_d is None or df_d.empty:
        raise RuntimeError(f"Unable to fetch D1 OHLC for {pair}")
    if df_w is None or df_w.empty:
        raise RuntimeError(f"Unable to fetch W1 OHLC for {pair}")

    ha_daily = compute_ha_state_from_df(pair, "D", df_d)
    ha_weekly = compute_ha_state_from_df(pair, "W", df_w)
    h1_cloud_state = ichimoku_price_state(df_h1)
    chg = daily_chg_cc(df_d)

    chg_abs_ok = chg is not None and abs(chg) > MIN_ABS_DAILY_CHG_CC
    bull = ha_daily.state == 1 and ha_weekly.state == 1 and h1_cloud_state == 1 and chg_abs_ok and chg > 0
    bear = ha_daily.state == -1 and ha_weekly.state == -1 and h1_cloud_state == -1 and chg_abs_ok and chg < 0
    align_state = 1 if bull else -1 if bear else 0

    return PairAlignment(
        pair=pair,
        daily_state=ha_daily.state,
        weekly_state=ha_weekly.state,
        h1_cloud_state=h1_cloud_state,
        daily_chg_cc=chg,
        align_state=align_state,
    )


def build_alignments(pairs: Iterable[str], candles_h1: int, candles_d: int, candles_w: int, debug_fetch: bool = False) -> ScanResult:
    rows: list[PairAlignment] = []
    errors: list[str] = []
    for pair in pairs:
        try:
            rows.append(compute_pair_alignment(pair, candles_h1, candles_d, candles_w, debug_fetch=debug_fetch))
        except Exception as exc:
            err = f"{pair:<8} ERROR: {exc}"
            errors.append(err)
            print(err)
            time.sleep(0.5)
    return ScanResult(rows=rows, errors=errors)


def print_table(rows: list[PairAlignment], show_all: bool) -> None:
    ordered_rows = sorted(
        rows,
        key=lambda row: abs(row.daily_chg_cc) if row.daily_chg_cc is not None else -1.0,
        reverse=True,
    )
    print(f"{'PAIR':<8} {'D1':<8} {'W1':<8} {'H1_CLOUD':<10} {'CHG%CC':<10} {'ALIGN':<8}")
    print("-" * 60)
    for row in ordered_rows:
        if not show_all and row.align_state == 0:
            continue
        chg_text = "na" if row.daily_chg_cc is None else f"{row.daily_chg_cc:.2f}%"
        print(
            f"{row.pair:<8} "
            f"{state_name(row.daily_state):<8} "
            f"{state_name(row.weekly_state):<8} "
            f"{state_name(row.h1_cloud_state):<10} "
            f"{chg_text:<10} "
            f"{state_name(row.align_state):<8}"
        )


def print_summary(rows: list[PairAlignment]) -> None:
    bull = [row.pair for row in rows if row.align_state == 1]
    bear = [row.pair for row in rows if row.align_state == -1]

    print()
    print(f"BULL aligned ({len(bull)}): {', '.join(bull) if bull else 'none'}")
    print(f"BEAR aligned ({len(bear)}): {', '.join(bear) if bear else 'none'}")


def build_telegram_message(rows: list[PairAlignment], title: str) -> str:
    selected = sorted(
        [row for row in rows if row.align_state != 0 and row.daily_chg_cc is not None],
        key=lambda row: abs(row.daily_chg_cc),
        reverse=True,
    )

    lines = [title, ""]
    for row in selected:
        icon = "🟢" if row.align_state == 1 else "🔴"
        lines.append(f"{icon} {row.pair} ({row.daily_chg_cc:+.2f}%)")

    if not selected:
        lines.append("NO DEAL 😞")

    lines.append("")
    lines.append(f"⏰ {datetime.now(PARIS_TZ):%Y-%m-%d %H:%M} Paris")
    return "\n".join(lines)


def send_telegram_message(message: str) -> None:
    bot_token = TELEGRAM_BOT_TOKEN or os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = TELEGRAM_CHAT_ID or os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")

    response = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": message,
        },
        timeout=20,
    )
    response.raise_for_status()


def main():
    load_local_env(ENV_PATH)
    args = parse_args()
    pairs = [args.pair.upper()] if args.pair else list(PAIRS_29)
    result = build_alignments(pairs, args.candles_h1, args.candles_d, args.candles_w, debug_fetch=args.debug_fetch)
    rows = result.rows
    print_table(rows, show_all=args.show_all)
    print_summary(rows)
    if result.errors:
        print()
        print(f"Fetch errors: {len(result.errors)}")
    if not rows:
        print("No data fetched. Telegram message skipped.")
        return
    if not args.no_telegram:
        message = build_telegram_message(rows, args.telegram_title)
        send_telegram_message(message)
        print()
        print("Telegram message sent.")


if __name__ == "__main__":
    main()
