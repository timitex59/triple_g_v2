#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ichimoku_v4.py

Version Python du biais Ichimoku de ichimoku_V4.pine.

- Analyse le biais D1 + W1 d'une paire.
- Option --scan29: screener des 29 instruments.
- N'affiche que les paires alignees BULL/BEAR sur Daily + Weekly.
"""

import argparse
import json
import os
import random
import string
import time
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from websocket import WebSocketConnectionClosedException, create_connection

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

PAIRS_29 = [
    "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "AUDUSD",
    "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD",
    "EURCHF", "EURGBP", "EURJPY", "EURNZD", "EURUSD",
    "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD",
    "GBPUSD", "NZDCAD", "NZDCHF", "NZDJPY", "NZDUSD",
    "USDCAD", "USDCHF", "USDJPY", "XAUUSD",
]

if load_dotenv:
    load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


@dataclass
class IchimokuBiasState:
    state: int  # 1=BULL, -1=BEAR, 0=NEUTRAL
    close: float | None
    visible_cloud_top: float | None
    visible_cloud_bottom: float | None
    visible_cloud_rsi: float | None
    live_cloud_rsi: float | None
    visible_color: str
    live_color: str


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


def fetch_tv_ohlc(symbol: str, interval: str, n_candles: int, timeout_s: int = 20, retries: int = 2) -> pd.DataFrame | None:
    for attempt in range(retries + 1):
        ws = None
        try:
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

                    if "series_completed" in frame:
                        break

                if "series_completed" in raw:
                    break

            if not points:
                continue

            rows = []
            for item in points:
                values = item.get("v", [])
                if len(values) < 5:
                    continue
                ts, o, h, l, c = values[:5]
                rows.append({"date": ts, "open": o, "high": h, "low": l, "close": c})

            if not rows:
                continue

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"], unit="s", utc=True)
            return df.set_index("date").sort_index()

        except Exception:
            if attempt >= retries:
                return None
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
    return None


def rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0.0, pd.NA)
    out = 100.0 - (100.0 / (1.0 + rs))
    out = out.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    out = out.mask((avg_gain == 0) & (avg_loss > 0), 0.0)
    return out.astype(float)


def donchian_avg(high: pd.Series, low: pd.Series, length: int) -> pd.Series:
    return (high.rolling(length).max() + low.rolling(length).min()) / 2.0


def compute_ichimoku_bias_state(
    df: pd.DataFrame,
    conversion_periods: int = 9,
    base_periods: int = 26,
    span_b_periods: int = 52,
    displacement: int = 26,
    cloud_trend_rsi_length: int = 5,
) -> IchimokuBiasState:
    min_bars = max(span_b_periods + displacement + 5, cloud_trend_rsi_length + displacement + 5)
    if df is None or df.empty or len(df) < min_bars:
        return IchimokuBiasState(0, None, None, None, None, None, "NEUTRAL", "NEUTRAL")

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

    visible_top = pd.concat([visible_span_a, visible_span_b], axis=1).max(axis=1)
    visible_bottom = pd.concat([visible_span_a, visible_span_b], axis=1).min(axis=1)
    visible_mid = (visible_top + visible_bottom) / 2.0
    visible_rsi = rsi(visible_mid, cloud_trend_rsi_length)

    live_top = pd.concat([lead_line1, lead_line2], axis=1).max(axis=1)
    live_bottom = pd.concat([lead_line1, lead_line2], axis=1).min(axis=1)
    live_mid = (live_top + live_bottom) / 2.0
    live_rsi = rsi(live_mid, cloud_trend_rsi_length)

    last_visible_a = float(visible_span_a.iloc[-1]) if pd.notna(visible_span_a.iloc[-1]) else None
    last_visible_b = float(visible_span_b.iloc[-1]) if pd.notna(visible_span_b.iloc[-1]) else None
    last_visible_top = float(visible_top.iloc[-1]) if pd.notna(visible_top.iloc[-1]) else None
    last_visible_bottom = float(visible_bottom.iloc[-1]) if pd.notna(visible_bottom.iloc[-1]) else None
    last_visible_rsi = float(visible_rsi.iloc[-1]) if pd.notna(visible_rsi.iloc[-1]) else None
    last_live_rsi = float(live_rsi.iloc[-1]) if pd.notna(live_rsi.iloc[-1]) else None

    visible_color = "BULL" if last_visible_a is not None and last_visible_b is not None and last_visible_a > last_visible_b else "BEAR" if last_visible_a is not None and last_visible_b is not None and last_visible_a < last_visible_b else "NEUTRAL"
    live_color = "BULL" if pd.notna(lead_line1.iloc[-1]) and pd.notna(lead_line2.iloc[-1]) and float(lead_line1.iloc[-1]) > float(lead_line2.iloc[-1]) else "BEAR" if pd.notna(lead_line1.iloc[-1]) and pd.notna(lead_line2.iloc[-1]) and float(lead_line1.iloc[-1]) < float(lead_line2.iloc[-1]) else "NEUTRAL"

    visible_trend_bull = last_visible_rsi is not None and last_visible_rsi > 50.0
    visible_trend_bear = last_visible_rsi is not None and last_visible_rsi < 50.0
    live_trend_bull = last_live_rsi is not None and last_live_rsi > 50.0
    live_trend_bear = last_live_rsi is not None and last_live_rsi < 50.0

    if visible_color == "BULL" and visible_trend_bull and live_color == "BULL" and live_trend_bull:
        state = 1
    elif visible_color == "BEAR" and visible_trend_bear and live_color == "BEAR" and live_trend_bear:
        state = -1
    else:
        state = 0

    return IchimokuBiasState(
        state=state,
        close=float(close.iloc[-1]),
        visible_cloud_top=last_visible_top,
        visible_cloud_bottom=last_visible_bottom,
        visible_cloud_rsi=last_visible_rsi,
        live_cloud_rsi=last_live_rsi,
        visible_color=visible_color,
        live_color=live_color,
    )


def state_name(value: int) -> str:
    return "BULL" if value == 1 else "BEAR" if value == -1 else "NEUTRAL"


def daily_chg_cc(df_d1: pd.DataFrame) -> float | None:
    if df_d1 is None or df_d1.empty or len(df_d1) < 2:
        return None
    prev_close = float(df_d1["close"].iloc[-2])
    last_close = float(df_d1["close"].iloc[-1])
    if prev_close == 0:
        return None
    return ((last_close - prev_close) / prev_close) * 100.0


def build_telegram_aligned_message(aligned_rows: list[dict]) -> str:
    lines = ["ICHIMOKU", ""]
    if not aligned_rows:
        lines.append("Aucune paire alignee")
        lines.append("")
        lines.append(f"⏰ {datetime.now(ZoneInfo('Europe/Paris')).strftime('%Y-%m-%d %H:%M Paris')}")
        return "\n".join(lines)

    for row in aligned_rows:
        icon = "🟢" if row["direction"] == "BULL" else "🔴"
        chg = row.get("chg_cc_d1")
        chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
        flame = " 🔥" if row.get("hourly_aligned") else ""
        lines.append(f"{icon} {row['pair']} ({chg_txt}){flame}")
    lines.append("")
    lines.append(f"⏰ {datetime.now(ZoneInfo('Europe/Paris')).strftime('%Y-%m-%d %H:%M Paris')}")
    return "\n".join(lines)


def passes_chg_filter(direction: str, chg_cc_d1: float | None) -> bool:
    if chg_cc_d1 is None:
        return False
    if direction == "BULL":
        return chg_cc_d1 > 0
    if direction == "BEAR":
        return chg_cc_d1 < 0
    return False


def build_telegram_mid_aligned_message(aligned_rows: list[dict], mid_aligned_rows: list[dict]) -> str:
    lines = ["ICHIMOKU", ""]
    if not aligned_rows:
        lines.append("Aucune paire alignee")
    else:
        for row in aligned_rows:
            icon = "ðŸŸ¢" if row["direction"] == "BULL" else "ðŸ”´"
            chg = row.get("chg_cc_d1")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            flame = " ðŸ”¥" if row.get("hourly_aligned") else ""
            lines.append(f"{icon} {row['pair']} ({chg_txt}){flame}")

    lines.extend(["", "MID ALIGNED", ""])
    if not mid_aligned_rows:
        lines.append("Aucune paire mid aligned")
    else:
        for row in mid_aligned_rows:
            icon = "ðŸŸ¢" if row["direction"] == "BULL" else "ðŸ”´"
            chg = row.get("chg_cc_d1")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            lines.append(f"{icon} {row['pair']} ({chg_txt})")

    lines.append("")
    lines.append(f"â° {datetime.now(ZoneInfo('Europe/Paris')).strftime('%Y-%m-%d %H:%M Paris')}")
    return "\n".join(lines)


def build_telegram_report_message(aligned_rows: list[dict], mid_aligned_rows: list[dict]) -> str:
    clock = "\u23F0"
    bull_icon = "\U0001F7E2"
    bear_icon = "\U0001F534"
    fire_icon = "\U0001F525"
    neutral_icon = "\u26AA\uFE0F"
    mixed_icon = "\u2194\uFE0F"
    lines = ["ICHIMOKU", ""]

    if not aligned_rows:
        lines.append("Aucune paire alignee")
    else:
        for row in aligned_rows:
            icon = bull_icon if row["direction"] == "BULL" else bear_icon
            chg = row.get("chg_cc_d1")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            if row.get("hourly_aligned"):
                suffix = fire_icon
            elif passes_chg_filter(row.get("direction"), chg):
                suffix = neutral_icon
            else:
                suffix = mixed_icon
            lines.append(f"{icon} {row['pair']} ({chg_txt}) {suffix}")

    lines.extend(["", "MID ALIGNED", ""])
    if not mid_aligned_rows:
        lines.append("Aucune paire mid aligned")
    else:
        for row in mid_aligned_rows:
            icon = bull_icon if row["direction"] == "BULL" else bear_icon
            chg = row.get("chg_cc_d1")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            lines.append(f"{icon} {row['pair']} ({chg_txt})")

    lines.extend(["", f"{clock} {datetime.now(ZoneInfo('Europe/Paris')).strftime('%Y-%m-%d %H:%M Paris')}"])
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


def analyze_pair_alignment(pair: str) -> int:
    symbol = f"OANDA:{pair}"
    daily_df = fetch_tv_ohlc(symbol, "D", 500)
    weekly_df = fetch_tv_ohlc(symbol, "W", 500)
    if daily_df is None or weekly_df is None:
        print(f"{pair}: data unavailable on D1 or W1")
        return 1

    daily_state = compute_ichimoku_bias_state(daily_df)
    weekly_state = compute_ichimoku_bias_state(weekly_df)
    chg_cc_d1 = daily_chg_cc(daily_df)
    aligned = daily_state.state != 0 and daily_state.state == weekly_state.state
    direction = state_name(daily_state.state) if aligned else "NONE"

    print(f"PAIR           : {pair}")
    print(f"DAILY STATE    : {state_name(daily_state.state)}")
    print(f"WEEKLY STATE   : {state_name(weekly_state.state)}")
    print(f"ALIGNMENT      : {direction}")
    print(f"DAILY COLOR    : {daily_state.visible_color} | LIVE: {daily_state.live_color}")
    print(f"WEEKLY COLOR   : {weekly_state.visible_color} | LIVE: {weekly_state.live_color}")
    print(f"DAILY RSI      : vis={daily_state.visible_cloud_rsi} live={daily_state.live_cloud_rsi}")
    print(f"WEEKLY RSI     : vis={weekly_state.visible_cloud_rsi} live={weekly_state.live_cloud_rsi}")
    print(f"CHG%CC D1      : {'N/A' if chg_cc_d1 is None else f'{chg_cc_d1:+.2f}%'}")
    print(f"DAILY CLOUD    : top={daily_state.visible_cloud_top} bottom={daily_state.visible_cloud_bottom}")
    print(f"WEEKLY CLOUD   : top={weekly_state.visible_cloud_top} bottom={weekly_state.visible_cloud_bottom}")
    return 0


def scan_alignment(pairs: list[str]) -> int:
    rows = []
    t0 = time.time()
    print(f"Scan Ichimoku D1 + W1 on {len(pairs)} instruments")

    for i, pair in enumerate(pairs, 1):
        symbol = f"OANDA:{pair}"
        print(f"[{i:>2}/{len(pairs)}] {pair} ...", end=" ", flush=True)
        daily_df = fetch_tv_ohlc(symbol, "D", 500)
        weekly_df = fetch_tv_ohlc(symbol, "W", 500)
        hourly_df = fetch_tv_ohlc(symbol, "60", 1400)

        if daily_df is None or weekly_df is None or hourly_df is None:
            print("ERROR")
            rows.append({"pair": pair, "error": True})
            continue

        daily_state = compute_ichimoku_bias_state(daily_df)
        weekly_state = compute_ichimoku_bias_state(weekly_df)
        hourly_state = compute_ichimoku_bias_state(hourly_df)
        chg_cc_d1 = daily_chg_cc(daily_df)
        aligned = daily_state.state != 0 and daily_state.state == weekly_state.state
        direction = state_name(daily_state.state) if daily_state.state != 0 else "NONE"
        hourly_aligned = daily_state.state != 0 and hourly_state.state == daily_state.state
        mid_aligned = hourly_aligned and weekly_state.state == 0 and passes_chg_filter(direction, chg_cc_d1)

        rows.append(
            {
                "pair": pair,
                "error": False,
                "daily_state": daily_state.state,
                "weekly_state": weekly_state.state,
                "hourly_state": hourly_state.state,
                "chg_cc_d1": chg_cc_d1,
                "aligned": aligned,
                "direction": direction,
                "hourly_aligned": hourly_aligned,
                "mid_aligned": mid_aligned,
            }
        )

        print(f"H1={state_name(hourly_state.state)} D={state_name(daily_state.state)} W={state_name(weekly_state.state)} ALIGN={direction}{' 🔥' if hourly_aligned else ''}")

    aligned_rows = [row for row in rows if not row.get("error") and row.get("aligned")]
    aligned_rows.sort(
        key=lambda row: (
            -(abs(row["chg_cc_d1"]) if row.get("chg_cc_d1") is not None else -1.0),
            row["pair"],
        )
    )

    mid_aligned_rows = [row for row in rows if not row.get("error") and row.get("mid_aligned")]
    mid_aligned_rows.sort(
        key=lambda row: (
            -(abs(row["chg_cc_d1"]) if row.get("chg_cc_d1") is not None else -1.0),
            row["pair"],
        )
    )

    print("\nALIGNED PAIRS")
    if not aligned_rows:
        print("None")
    else:
        for row in aligned_rows:
            chg = row.get("chg_cc_d1")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            flame = " 🔥" if row.get("hourly_aligned") else ""
            print(f"  {row['pair']:<8} {row['direction']:<4} ({chg_txt}){flame}")

    print("\nMID ALIGNED")
    if not mid_aligned_rows:
        print("None")
    else:
        for row in mid_aligned_rows:
            chg = row.get("chg_cc_d1")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            print(f"  {row['pair']:<8} {row['direction']:<4} ({chg_txt})")

    send_telegram_message(build_telegram_report_message(aligned_rows, mid_aligned_rows))
    print(f"Elapsed: {time.time() - t0:.2f}s")
    return 0


def parse_args():
    parser = argparse.ArgumentParser(description="ichimoku_v4.py - Python version of ichimoku_V4 bias")
    parser.add_argument("--pair", default=None, help="Analyze only this pair on D1+W1, ex: EURUSD")
    parser.add_argument("--scan29", action="store_true", help="Scan 29 instruments for D1/W1 alignment")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.scan29 or not args.pair:
        raise SystemExit(scan_alignment(PAIRS_29))
    raise SystemExit(analyze_pair_alignment(args.pair.upper()))


if __name__ == "__main__":
    main()
