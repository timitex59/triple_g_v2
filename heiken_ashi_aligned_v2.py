#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
heiken_ashi_aligned_v2.py

Scan des 29 paires + 8 indices devises avec la logique du script Pine `heiken_ashi_alignment.pine`:
- Heikin Ashi Daily
- Heikin Ashi Weekly
- Prix H1 au-dessus/en-dessous du nuage Ichimoku
- CHG% CC daily
- Renko Daily (dernière brique clôturée ATR 14)
- Px/Renko (position du prix actuel par rapport à la brique Renko)

Alignement bull:
- HA Daily = BULL
- HA Weekly = BULL
- H1 Cloud = BULL
- CHG% CC daily > 0 et abs(CHG% CC daily) > 0.1
- Px/Renko = BULL (prix au-dessus du close Renko)

Alignement bear:
- HA Daily = BEAR
- HA Weekly = BEAR
- H1 Cloud = BEAR
- CHG% CC daily < 0 et abs(CHG% CC daily) > 0.1
- Px/Renko = BEAR (prix en-dessous de l'open Renko)
"""

import argparse
import json
import math
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

INDICES = {
    "DXY": "TVC:DXY",
    "EXY": "TVC:EXY",
    "BXY": "TVC:BXY",
    "JXY": "TVC:JXY",
    "SXY": "TVC:SXY",
    "CXY": "TVC:CXY",
    "AXY": "TVC:AXY",
    "ZXY": "TVC:ZXY",
}

INDEX_TO_CCY = {
    "DXY": "USD",
    "EXY": "EUR",
    "BXY": "GBP",
    "JXY": "JPY",
    "SXY": "CHF",
    "CXY": "CAD",
    "AXY": "AUD",
    "ZXY": "NZD",
}

CCY_TO_INDEX = {v: k for k, v in INDEX_TO_CCY.items()}

KNOWN_CCY = {"USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"}


def split_pair_ccy(pair: str) -> tuple[str, str] | None:
    if len(pair) == 6:
        base, quote = pair[:3], pair[3:]
        if base in KNOWN_CCY or base == "XAU":
            return base, quote
    return None

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
class RenkoBrick:
    open: float
    close: float
    state: int      # 1 = bull, -1 = bear
    timestamp: float | None


@dataclass
class PairAlignment:
    pair: str
    daily_state: int
    weekly_state: int
    h1_cloud_state: int
    daily_chg_cc: float | None
    renko_state: int
    px_vs_renko: int        # 1 = above close, -1 = below open, 0 = inside
    renko_open: float | None
    renko_close: float | None
    current_price: float | None
    align_state: int
    idx_confirm: int = 0    # 1 = indices confirm bull, -1 = confirm bear, 0 = no confirmation


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
    parser = argparse.ArgumentParser(description="Scan HA D1/W1 alignment with H1 Ichimoku cloud, daily CHG% CC, and Renko D Px/Renko.")
    parser.add_argument("--pair", type=str, help="Single pair to analyze, e.g. EURUSD.")
    parser.add_argument("--candles-h1", type=int, default=300, help="Number of H1 candles to fetch.")
    parser.add_argument("--candles-d", type=int, default=200, help="Number of D1 candles to fetch.")
    parser.add_argument("--candles-w", type=int, default=200, help="Number of W1 candles to fetch.")
    parser.add_argument("--show-all", action="store_true", help="Show all pairs, not only aligned ones.")
    parser.add_argument("--no-telegram", action="store_true", help="Do not send aligned pairs to Telegram.")
    parser.add_argument("--telegram-title", type=str, default="HEIKEN_ICHI_V2", help="Telegram message title.")
    parser.add_argument("--debug-fetch", action="store_true", help="Print websocket fetch diagnostics.")
    return parser.parse_args()


def state_name(value: int) -> str:
    return "BULL" if value == 1 else "BEAR" if value == -1 else "MIXED"


def px_vs_renko_label(value: int) -> str:
    return "> Close" if value == 1 else "< Open" if value == -1 else "Inside"


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


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def compute_renko_bricks(df_d: pd.DataFrame, atr_period: int = 14) -> list[RenkoBrick]:
    if df_d is None or df_d.empty or len(df_d) < atr_period + 1:
        return []

    atr_series = compute_atr(df_d, atr_period)
    closes = df_d["close"].astype(float).values
    timestamps = df_d.index

    bricks: list[RenkoBrick] = []
    brick_top = float(closes[atr_period])
    brick_bot = float(closes[atr_period])
    brick_state = 0
    box_size = float(atr_series.iloc[atr_period])

    for i in range(atr_period + 1, len(closes)):
        c = float(closes[i])
        atr_val = float(atr_series.iloc[i])
        if math.isnan(atr_val) or atr_val <= 0:
            continue

        ts = timestamps[i].timestamp() if hasattr(timestamps[i], "timestamp") else float(timestamps[i])

        made_brick = True
        while made_brick:
            made_brick = False
            if brick_state >= 0:  # Bull or initial
                if c >= brick_top + box_size:
                    brick_bot = brick_top
                    brick_top = brick_top + box_size
                    brick_state = 1
                    box_size = atr_val
                    bricks.append(RenkoBrick(open=brick_bot, close=brick_top, state=1, timestamp=ts))
                    made_brick = True
                elif c <= brick_bot - box_size:
                    brick_top = brick_bot
                    brick_bot = brick_bot - box_size
                    brick_state = -1
                    box_size = atr_val
                    bricks.append(RenkoBrick(open=brick_top, close=brick_bot, state=-1, timestamp=ts))
                    made_brick = True
            else:  # Bear
                if c <= brick_bot - box_size:
                    brick_top = brick_bot
                    brick_bot = brick_bot - box_size
                    brick_state = -1
                    box_size = atr_val
                    bricks.append(RenkoBrick(open=brick_top, close=brick_bot, state=-1, timestamp=ts))
                    made_brick = True
                elif c >= brick_top + box_size:
                    brick_bot = brick_top
                    brick_top = brick_top + box_size
                    brick_state = 1
                    box_size = atr_val
                    bricks.append(RenkoBrick(open=brick_bot, close=brick_top, state=1, timestamp=ts))
                    made_brick = True

    return bricks


def compute_px_vs_renko(current_price: float, renko_open: float, renko_close: float) -> int:
    renko_high = max(renko_open, renko_close)
    renko_low = min(renko_open, renko_close)
    if current_price > renko_high:
        return 1
    elif current_price < renko_low:
        return -1
    return 0


def compute_pair_alignment(pair: str, candles_h1: int, candles_d: int, candles_w: int, debug_fetch: bool = False, tv_symbol: str | None = None) -> PairAlignment:
    symbol = tv_symbol or f"OANDA:{pair}"
    df_h1 = fetch_tv_ohlc(symbol, "60", candles_h1, debug=debug_fetch)
    df_d = fetch_tv_ohlc(symbol, "D", candles_d, debug=debug_fetch)
    df_w = fetch_tv_ohlc(symbol, "W", candles_w, debug=debug_fetch)

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

    # Renko Daily
    renko_bricks = compute_renko_bricks(df_d, atr_period=14)
    if renko_bricks:
        last_brick = renko_bricks[-1]
        renko_state = last_brick.state
        renko_open = last_brick.open
        renko_close = last_brick.close
    else:
        renko_state = 0
        renko_open = None
        renko_close = None

    # Px vs Renko: compare current price to last Renko brick
    current_price = float(df_d["close"].iloc[-1])
    if renko_open is not None and renko_close is not None:
        px_vs_renko = compute_px_vs_renko(current_price, renko_open, renko_close)
    else:
        px_vs_renko = 0

    # Alignment: all must agree
    chg_abs_ok = chg is not None and abs(chg) > MIN_ABS_DAILY_CHG_CC
    bull = (
        ha_daily.state == 1
        and ha_weekly.state == 1
        and h1_cloud_state == 1
        and chg_abs_ok and chg > 0
        and px_vs_renko == 1
    )
    bear = (
        ha_daily.state == -1
        and ha_weekly.state == -1
        and h1_cloud_state == -1
        and chg_abs_ok and chg < 0
        and px_vs_renko == -1
    )
    align_state = 1 if bull else -1 if bear else 0

    return PairAlignment(
        pair=pair,
        daily_state=ha_daily.state,
        weekly_state=ha_weekly.state,
        h1_cloud_state=h1_cloud_state,
        daily_chg_cc=chg,
        renko_state=renko_state,
        px_vs_renko=px_vs_renko,
        renko_open=renko_open,
        renko_close=renko_close,
        current_price=current_price,
        align_state=align_state,
    )


def compute_idx_confirm(rows: list[PairAlignment]) -> None:
    index_names = set(INDICES.keys())
    idx_states: dict[str, int] = {}
    for row in rows:
        if row.pair in index_names:
            ccy = INDEX_TO_CCY.get(row.pair)
            if ccy:
                idx_states[ccy] = row.align_state

    for row in rows:
        if row.pair in index_names:
            continue
        parts = split_pair_ccy(row.pair)
        if not parts:
            continue
        base, quote = parts
        base_idx = idx_states.get(base, 0)
        quote_idx = idx_states.get(quote, 0)
        if base_idx == 1 and quote_idx == -1:
            row.idx_confirm = 1
        elif base_idx == -1 and quote_idx == 1:
            row.idx_confirm = -1
        else:
            row.idx_confirm = 0


def build_alignments(pairs: Iterable[str], candles_h1: int, candles_d: int, candles_w: int, debug_fetch: bool = False, indices: dict[str, str] | None = None) -> ScanResult:
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
    if indices:
        for name, tv_symbol in indices.items():
            try:
                rows.append(compute_pair_alignment(name, candles_h1, candles_d, candles_w, debug_fetch=debug_fetch, tv_symbol=tv_symbol))
            except Exception as exc:
                err = f"{name:<8} ERROR: {exc}"
                errors.append(err)
                print(err)
                time.sleep(0.5)
    compute_idx_confirm(rows)
    return ScanResult(rows=rows, errors=errors)


def idx_confirm_label(value: int) -> str:
    return "✓" if value != 0 else "-"


def _print_rows(rows: list[PairAlignment], show_all: bool, show_idx: bool = True) -> None:
    for row in rows:
        if not show_all and row.align_state == 0:
            continue
        chg_text = "na" if row.daily_chg_cc is None else f"{row.daily_chg_cc:.2f}%"
        idx_text = f"{idx_confirm_label(row.idx_confirm):<5}" if show_idx else ""
        print(
            f"{row.pair:<8} "
            f"{state_name(row.daily_state):<8} "
            f"{state_name(row.weekly_state):<8} "
            f"{state_name(row.h1_cloud_state):<10} "
            f"{chg_text:<10} "
            f"{state_name(row.renko_state):<8} "
            f"{px_vs_renko_label(row.px_vs_renko):<10} "
            f"{idx_text}"
            f"{state_name(row.align_state):<8}"
        )


def print_table(rows: list[PairAlignment], show_all: bool) -> None:
    index_names = set(INDICES.keys())
    pair_rows = [r for r in rows if r.pair not in index_names]
    index_rows = [r for r in rows if r.pair in index_names]

    sort_key = lambda row: abs(row.daily_chg_cc) if row.daily_chg_cc is not None else -1.0
    pair_rows = sorted(pair_rows, key=sort_key, reverse=True)
    index_rows = sorted(index_rows, key=sort_key, reverse=True)

    header_pair = f"{'PAIR':<8} {'D1':<8} {'W1':<8} {'H1_CLOUD':<10} {'CHG%CC':<10} {'RENKO_D':<8} {'PX/RENKO':<10} {'IDX':<5}{'ALIGN':<8}"
    header_idx = f"{'PAIR':<8} {'D1':<8} {'W1':<8} {'H1_CLOUD':<10} {'CHG%CC':<10} {'RENKO_D':<8} {'PX/RENKO':<10} {'ALIGN':<8}"
    sep = "-" * 83

    print(header_pair)
    print(sep)
    _print_rows(pair_rows, show_all, show_idx=True)

    if index_rows:
        print()
        print("--- INDICES ---")
        print(header_idx)
        print("-" * 78)
        _print_rows(index_rows, show_all, show_idx=False)


def print_summary(rows: list[PairAlignment]) -> None:
    bull = [row.pair for row in rows if row.align_state == 1]
    bear = [row.pair for row in rows if row.align_state == -1]

    print()
    print(f"BULL aligned ({len(bull)}): {', '.join(bull) if bull else 'none'}")
    print(f"BEAR aligned ({len(bear)}): {', '.join(bear) if bear else 'none'}")


def build_telegram_message(rows: list[PairAlignment], title: str) -> str:
    index_names = set(INDICES.keys())
    sort_key = lambda row: abs(row.daily_chg_cc) if row.daily_chg_cc is not None else 0.0

    pair_selected = sorted(
        [row for row in rows if row.align_state != 0 and row.daily_chg_cc is not None and row.pair not in index_names],
        key=sort_key, reverse=True,
    )
    index_selected = sorted(
        [row for row in rows if row.align_state != 0 and row.daily_chg_cc is not None and row.pair in index_names],
        key=sort_key, reverse=True,
    )
    all_pairs = [row for row in rows if row.pair not in index_names and row.daily_chg_cc is not None]
    idx_confirmed = sorted(
        [r for r in all_pairs if r.idx_confirm != 0 and abs(r.daily_chg_cc) > MIN_ABS_DAILY_CHG_CC],
        key=sort_key, reverse=True,
    )

    lines = [title]

    # Section 1: INDICES
    if index_selected:
        lines.append("")
        lines.append("📊 INDICES")
        for row in index_selected:
            icon = "🟢" if row.align_state == 1 else "🔴"
            ccy = INDEX_TO_CCY.get(row.pair, row.pair)
            lines.append(f"{icon} {ccy} ({row.daily_chg_cc:+.2f}%)")

    # Section 2: TOP 5 (pure, sans effet indice)
    top5 = pair_selected[:5]
    lines.append("")
    lines.append("🏆 TOP 5")
    if top5:
        for row in top5:
            icon = "🟢" if row.align_state == 1 else "🔴"
            lines.append(f"{icon} {row.pair} ({row.daily_chg_cc:+.2f}%)")
        if len(pair_selected) > 5:
            lines.append(f"... +{len(pair_selected) - 5} autres")
    else:
        lines.append("Aucune paire alignée")

    # Section 3: IDX CONFIRMED (paires où indices base/quote divergent)
    if idx_confirmed:
        lines.append("")
        lines.append("⭐ IDX CONFIRMED")
        for row in idx_confirmed:
            icon = "🟢" if row.idx_confirm == 1 else "🔴"
            lines.append(f"{icon} {row.pair} ({row.daily_chg_cc:+.2f}%)")

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
    indices_to_scan = None if args.pair else dict(INDICES)
    result = build_alignments(pairs, args.candles_h1, args.candles_d, args.candles_w, debug_fetch=args.debug_fetch, indices=indices_to_scan)
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
