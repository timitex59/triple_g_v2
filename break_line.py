#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
break_line.py

Version Python du script TradingView break_line.pine.

- Mode par defaut: analyse D1 + H1 d'une paire et indique l'alignement BULL/BEAR/NONE.
- Option --scan29: screener D1 + H1 sur 29 instruments.
- Option --single-tf: diagnostic detaille d'un seul timeframe.
"""

import argparse
import json
import os
import random
import string
import time
from datetime import datetime
from dataclasses import dataclass
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from websocket import WebSocketConnectionClosedException, create_connection
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]

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
TELEGRAM_MIN_ABS_CHG_D1 = 0.15
TRACKING_PATH = os.path.join(os.path.dirname(__file__), "break_line_state.json")


@dataclass
class BreakLineState:
    state: int  # 1=GREEN, -1=RED, 0=NEUTRAL
    close: float | None
    bull_vw0: float | None
    bull_vw1: float | None
    bear_vw0: float | None
    bear_vw1: float | None
    bull_line_now: float | None
    bear_line_now: float | None
    bull_valid_last_event: bool
    bear_valid_last_event: bool


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
                v = item.get("v", [])
                if len(v) < 5:
                    continue
                ts, o, h, l, c = v[:5]
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


def calculate_psar(df: pd.DataFrame, start: float = 0.1, increment: float = 0.1, maximum: float = 0.2) -> pd.Series | None:
    if df is None or len(df) < 3:
        return None

    highs = df["high"].values.astype(float)
    lows = df["low"].values.astype(float)
    closes = df["close"].values.astype(float)

    psar = np.zeros(len(df), dtype=float)
    bull = closes[1] >= closes[0]
    af = start
    ep = highs[0] if bull else lows[0]
    psar[0] = lows[0] if bull else highs[0]

    for i in range(1, len(df)):
        psar[i] = psar[i - 1] + af * (ep - psar[i - 1])
        if bull:
            if lows[i] < psar[i]:
                bull = False
                psar[i] = ep
                ep = lows[i]
                af = start
            else:
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + increment, maximum)
                psar[i] = min(psar[i], lows[i - 1], lows[i - 2]) if i >= 2 else min(psar[i], lows[i - 1])
        else:
            if highs[i] > psar[i]:
                bull = True
                psar[i] = ep
                ep = highs[i]
                af = start
            else:
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + increment, maximum)
                psar[i] = max(psar[i], highs[i - 1], highs[i - 2]) if i >= 2 else max(psar[i], highs[i - 1])

    return pd.Series(psar, index=df.index)


def _line_value_now(x1: int, y1: float, x2: int, y2: float, now_x: int) -> float | None:
    if x2 == x1:
        return None
    return y1 + (y2 - y1) * (now_x - x1) / (x2 - x1)


def compute_break_line_state(df: pd.DataFrame) -> BreakLineState:
    if df is None or df.empty or len(df) < 5:
        return BreakLineState(0, None, None, None, None, None, None, None, False, False)

    psar = calculate_psar(df)
    if psar is None or psar.empty:
        return BreakLineState(0, float(df["close"].iloc[-1]), None, None, None, None, None, None, False, False)

    close = df["close"]
    bull = ((close > psar) & (close.shift(1) <= psar.shift(1))).fillna(False)
    bear = ((close < psar) & (close.shift(1) >= psar.shift(1))).fillna(False)

    bull_events: list[tuple[int, float]] = []
    bear_events: list[tuple[int, float]] = []

    bull_vw0 = bull_vw1 = None
    bear_vw0 = bear_vw1 = None
    last_bull_line = None
    last_bear_line = None

    # Persistance Pine: mise a jour uniquement sur nouvel evenement valide.
    for i in range(len(df)):
        if bool(bull.iloc[i]):
            bull_events.append((i, float(psar.iloc[i])))
            bull_vw0 = bull_events[-1][1]
            bull_vw1 = bull_events[-2][1] if len(bull_events) >= 2 else None
            if len(bull_events) >= 2:
                (x1, y1), (x2, y2) = bull_events[-2], bull_events[-1]
                if y2 > y1:  # bull_vw0 > bull_vw1
                    last_bull_line = (x1, y1, x2, y2)

        if bool(bear.iloc[i]):
            bear_events.append((i, float(psar.iloc[i])))
            bear_vw0 = bear_events[-1][1]
            bear_vw1 = bear_events[-2][1] if len(bear_events) >= 2 else None
            if len(bear_events) >= 2:
                (x1, y1), (x2, y2) = bear_events[-2], bear_events[-1]
                if y2 < y1:  # bear_vw0 < bear_vw1
                    last_bear_line = (x1, y1, x2, y2)

    now_i = len(df) - 1
    bull_line_now = None if last_bull_line is None else _line_value_now(*last_bull_line, now_i)
    bear_line_now = None if last_bear_line is None else _line_value_now(*last_bear_line, now_i)

    last_close = float(close.iloc[-1])
    has_both = (bull_line_now is not None) and (bear_line_now is not None)

    if has_both and last_close > bull_line_now and last_close > bear_line_now:
        state = 1
    elif has_both and last_close < bull_line_now and last_close < bear_line_now:
        state = -1
    else:
        state = 0

    return BreakLineState(
        state=state,
        close=last_close,
        bull_vw0=bull_vw0,
        bull_vw1=bull_vw1,
        bear_vw0=bear_vw0,
        bear_vw1=bear_vw1,
        bull_line_now=bull_line_now,
        bear_line_now=bear_line_now,
        bull_valid_last_event=(last_bull_line is not None),
        bear_valid_last_event=(last_bear_line is not None),
    )


def state_name(v: int) -> str:
    return "GREEN" if v == 1 else "RED" if v == -1 else "NEUTRAL"


def build_telegram_aligned_message(aligned_rows: list[dict], retrace_rows: list[dict]) -> str:
    lines = ["ALIGNED PAIRS", ""]
    if not aligned_rows:
        lines.append("Aucune paire alignee")
    else:
        for r in aligned_rows:
            direction = r.get("direction")
            bull_bear_icon = "\U0001F7E2" if direction == "BULL" else "\U0001F534" if direction == "BEAR" else "\u26AA"
            w1d1_icon = bull_bear_icon if r.get("aligned_w1_d1") else "\u26AA"
            d1h1_icon = bull_bear_icon if r.get("aligned") else "\u26AA"
            chg = r.get("chg_cc_d1")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            flame = " \U0001F525" if r.get("flame") else ""
            lines.append(f"{w1d1_icon}{d1h1_icon} {r['pair']} ({chg_txt}){flame}")

    lines.extend(["", "", "RETRACING PAIRS", ""])
    if not retrace_rows:
        lines.append("Aucune paire retrace")
    else:
        for r in retrace_rows:
            direction = r.get("w1d1_direction")
            bull_bear_icon = "\U0001F7E2" if direction == "BULL" else "\U0001F534" if direction == "BEAR" else "\u26AA"
            w1d1_icon = bull_bear_icon if r.get("aligned_w1_d1") else "\u26AA"
            d1h1_icon = "\u26AA"
            chg = r.get("chg_cc_d1")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            flame = " \U0001F525" if r.get("retrace_flame") else ""
            lines.append(f"{w1d1_icon}{d1h1_icon} {r['pair']} ({chg_txt}){flame}")
    paris_now = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
    lines.extend(["", f"⏰ {paris_now} Paris"])
    return "\n".join(lines)


def _direction_icon(direction: str | None) -> str:
    if direction == "BULL":
        return "\U0001F7E2"
    if direction == "BEAR":
        return "\U0001F534"
    return "\u26AA"


def _format_tracked_row(row: dict) -> str:
    section = row.get("section")
    chg = row.get("chg_cc_d1")
    chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"

    if section == "RETRACING":
        cloud = " \u2601\uFE0F" if row.get("daily_cloud_ok") or row.get("retrace_daily_cloud_ok") else ""
        w1d1_icon = _direction_icon(row.get("w1d1_direction"))
        d1h1_icon = "\u26AA\uFE0F"
        flame = " \U0001F525" if row.get("retrace_flame") else ""
        return f"{w1d1_icon}{d1h1_icon} {row['pair']} ({chg_txt}){cloud}{flame}"

    cloud = " \u2601\uFE0F" if row.get("daily_cloud_ok") else ""
    w1d1_icon = _direction_icon(row.get("direction"))
    d1h1_icon = _direction_icon(row.get("direction"))
    flame = " \U0001F525" if row.get("flame") else ""
    return f"{w1d1_icon}{d1h1_icon} {row['pair']} ({chg_txt}){cloud}{flame}"


def build_telegram_tracking_message(aligned_rows: list[dict], retrace_rows: list[dict], exited_rows: list[dict]) -> str:
    lines = ["ALIGNED PAIRS", ""]
    if not aligned_rows:
        lines.append("Aucune paire alignee")
    else:
        for row in aligned_rows:
            lines.append(_format_tracked_row({**row, "section": "ALIGNED"}))

    lines.extend(["", "", "RETRACING PAIRS", ""])
    if not retrace_rows:
        lines.append("Aucune paire retrace")
    else:
        for row in retrace_rows:
            lines.append(_format_tracked_row({**row, "section": "RETRACING"}))

    lines.extend(["", "", "EXITED PAIRS", ""])
    if not exited_rows:
        lines.append("Aucune paire sortie")
    else:
        for row in exited_rows:
            lines.append(_format_tracked_row(row))

    paris_now = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
    lines.extend(["", f"\u23F0 {paris_now} Paris"])
    return "\n".join(lines)


def load_tracking_state(path: str) -> dict:
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


def save_tracking_state(path: str, aligned_rows: list[dict], retrace_rows: list[dict]) -> None:
    aligned_map = {
        row["pair"]: {
            "pair": row["pair"],
            "direction": row.get("direction"),
            "chg_cc_d1": row.get("chg_cc_d1"),
            "flame": bool(row.get("flame")),
            "daily_cloud_ok": bool(row.get("daily_cloud_ok")),
            "section": "ALIGNED",
        }
        for row in aligned_rows
    }
    retrace_map = {
        row["pair"]: {
            "pair": row["pair"],
            "w1d1_direction": row.get("w1d1_direction"),
            "chg_cc_d1": row.get("chg_cc_d1"),
            "retrace_flame": bool(row.get("retrace_flame")),
            "daily_cloud_ok": bool(row.get("daily_cloud_ok")),
            "retrace_daily_cloud_ok": bool(row.get("retrace_daily_cloud_ok")),
            "section": "RETRACING",
        }
        for row in retrace_rows
    }
    payload = {
        "updated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "telegram_aligned": aligned_map,
        "telegram_retracing": retrace_map,
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def compute_exited_rows(previous: dict, aligned_rows: list[dict], retrace_rows: list[dict]) -> list[dict]:
    prev_aligned = previous.get("telegram_aligned", {}) if isinstance(previous, dict) else {}
    prev_retracing = previous.get("telegram_retracing", {}) if isinstance(previous, dict) else {}
    current_pairs = {row["pair"] for row in aligned_rows} | {row["pair"] for row in retrace_rows}

    exited_rows = []
    for pair, row in prev_aligned.items():
        if pair not in current_pairs:
            exited_rows.append(
                {
                    "pair": pair,
                    "direction": row.get("direction"),
                    "chg_cc_d1": row.get("chg_cc_d1"),
                    "flame": False,
                    "daily_cloud_ok": bool(row.get("daily_cloud_ok")),
                    "section": "ALIGNED",
                }
            )

    for pair, row in prev_retracing.items():
        if pair not in current_pairs:
            exited_rows.append(
                {
                    "pair": pair,
                    "w1d1_direction": row.get("w1d1_direction"),
                    "chg_cc_d1": row.get("chg_cc_d1"),
                    "retrace_flame": False,
                    "daily_cloud_ok": bool(row.get("daily_cloud_ok")),
                    "retrace_daily_cloud_ok": bool(row.get("retrace_daily_cloud_ok")),
                    "section": "RETRACING",
                }
            )

    exited_rows.sort(key=lambda row: abs(row.get("chg_cc_d1") or 0.0), reverse=True)
    return exited_rows


def send_telegram_message(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram: credentials missing, skip send.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=10,
        )
        data = response.json()
        ok = bool(data.get("ok", False))
        print(f"Telegram: {'sent' if ok else 'failed'}")
        return ok
    except Exception as exc:
        print(f"Telegram: send failed ({exc})")
        return False


def passes_chg_filter(direction: str, chg_cc_d1: float | None) -> bool:
    if chg_cc_d1 is None:
        return False
    if direction == "BULL":
        return chg_cc_d1 > 0
    if direction == "BEAR":
        return chg_cc_d1 < 0
    return False


def passes_telegram_abs_chg_filter(chg_cc_d1: float | None, min_abs_chg: float = TELEGRAM_MIN_ABS_CHG_D1) -> bool:
    if chg_cc_d1 is None:
        return False
    return abs(chg_cc_d1) > min_abs_chg


def _pair_currencies(pair: str) -> tuple[str, str] | None:
    if not pair or len(pair) != 6:
        return None
    return pair[:3], pair[3:]


def _row_strength_roles(row: dict) -> tuple[str, str] | None:
    pair = row.get("pair")
    direction = row.get("direction")
    if direction is None:
        direction = row.get("w1d1_direction")

    currencies = _pair_currencies(pair)
    if currencies is None or direction not in {"BULL", "BEAR"}:
        return None

    base, quote = currencies
    if direction == "BULL":
        return base, quote
    return quote, base


def collect_ambiguous_currencies(*row_groups: list[dict]) -> set[str]:
    strong: set[str] = set()
    weak: set[str] = set()

    for rows in row_groups:
        for row in rows:
            roles = _row_strength_roles(row)
            if roles is None:
                continue
            strong_ccy, weak_ccy = roles
            strong.add(strong_ccy)
            weak.add(weak_ccy)

    return strong & weak


def filter_ambiguous_rows(rows: list[dict], ambiguous_currencies: set[str]) -> list[dict]:
    if not ambiguous_currencies:
        return rows

    filtered_rows = []
    for row in rows:
        currencies = _pair_currencies(row.get("pair"))
        if currencies is None:
            filtered_rows.append(row)
            continue
        if currencies[0] in ambiguous_currencies or currencies[1] in ambiguous_currencies:
            continue
        filtered_rows.append(row)
    return filtered_rows


def daily_ichimoku_cloud_direction(
    df_d1: pd.DataFrame,
    conversion_periods: int = 9,
    base_periods: int = 26,
    span_b_periods: int = 52,
) -> int:
    if df_d1 is None or df_d1.empty or len(df_d1) < span_b_periods:
        return 0

    high = df_d1["high"].astype(float)
    low = df_d1["low"].astype(float)
    close = df_d1["close"].astype(float)

    conversion_line = (high.rolling(conversion_periods).max() + low.rolling(conversion_periods).min()) / 2.0
    base_line = (high.rolling(base_periods).max() + low.rolling(base_periods).min()) / 2.0
    span_a = (conversion_line + base_line) / 2.0
    span_b = (high.rolling(span_b_periods).max() + low.rolling(span_b_periods).min()) / 2.0

    last_span_a = span_a.iloc[-1]
    last_span_b = span_b.iloc[-1]
    last_close = close.iloc[-1]
    if pd.isna(last_span_a) or pd.isna(last_span_b) or pd.isna(last_close):
        return 0

    cloud_top = max(float(last_span_a), float(last_span_b))
    cloud_bottom = min(float(last_span_a), float(last_span_b))
    if float(last_close) > cloud_top:
        return 1
    if float(last_close) < cloud_bottom:
        return -1
    return 0


def daily_ema_ribbon_direction(df_d1: pd.DataFrame) -> int:
    """
    Return:
      1  -> bullish ribbon (EMA20 > EMA25 > ... > EMA55) and close above all
     -1  -> bearish ribbon (EMA20 < EMA25 < ... < EMA55) and close below all
      0  -> neutral / insufficient data
    """
    if df_d1 is None or df_d1.empty or len(df_d1) < max(EMA_LENGTHS) + 2:
        return 0

    close = df_d1["close"].astype(float)
    ema_vals = [float(close.ewm(span=l, adjust=False).mean().iloc[-1]) for l in EMA_LENGTHS]
    last_close = float(close.iloc[-1])

    bull_stack = all(ema_vals[i] > ema_vals[i + 1] for i in range(len(ema_vals) - 1))
    bear_stack = all(ema_vals[i] < ema_vals[i + 1] for i in range(len(ema_vals) - 1))
    close_above_all = all(last_close > v for v in ema_vals)
    close_below_all = all(last_close < v for v in ema_vals)

    if bull_stack and close_above_all:
        return 1
    if bear_stack and close_below_all:
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


def retrace_flame_active(df: pd.DataFrame, direction: str) -> bool:
    """
    RETRACING flame persistence rule on H1:
    - W1/D1 BULL: flame stays ON while close is above SAR.
    - W1/D1 BEAR: flame stays ON while close is below SAR.
    """
    if df is None or df.empty or len(df) < 2:
        return False

    psar = calculate_psar(df)
    if psar is None or psar.empty:
        return False

    last_close = float(df["close"].iloc[-1])
    last_psar = float(psar.iloc[-1])

    if direction == "BULL":
        return last_close > last_psar
    if direction == "BEAR":
        return last_close < last_psar
    return False


def analyze_single_tf(pair: str, interval: str, candles: int) -> int:
    symbol = f"OANDA:{pair}"
    df = fetch_tv_ohlc(symbol, interval, candles)
    if df is None:
        print(f"{pair} {interval}: data unavailable")
        return 1

    st = compute_break_line_state(df)
    print(f"PAIR      : {pair}")
    print(f"TF        : {interval}")
    print(f"CLOSE     : {st.close}")
    print(f"STATE     : {state_name(st.state)}")
    print(f"bull_vw0  : {st.bull_vw0}")
    print(f"bull_vw1  : {st.bull_vw1}")
    print(f"bear_vw0  : {st.bear_vw0}")
    print(f"bear_vw1  : {st.bear_vw1}")
    print(f"bull_line : {st.bull_line_now}")
    print(f"bear_line : {st.bear_line_now}")
    print(f"bull_last_valid_event: {st.bull_valid_last_event}")
    print(f"bear_last_valid_event: {st.bear_valid_last_event}")
    return 0


def analyze_pair_alignment(pair: str) -> int:
    symbol = f"OANDA:{pair}"
    w1 = fetch_tv_ohlc(symbol, "W", 350)
    d1 = fetch_tv_ohlc(symbol, "D", 500)
    h1 = fetch_tv_ohlc(symbol, "60", 1400)
    if w1 is None or d1 is None or h1 is None:
        print(f"{pair}: data unavailable on W1, D1 or H1")
        return 1

    s_w1 = compute_break_line_state(w1)
    s_d1 = compute_break_line_state(d1)
    s_h1 = compute_break_line_state(h1)
    ribbon_d1 = daily_ema_ribbon_direction(d1)
    chg_cc_d1 = daily_chg_cc(d1)
    aligned_d1_h1 = (s_d1.state != 0) and (s_d1.state == s_h1.state)
    aligned_w1_d1 = (s_w1.state != 0) and (s_w1.state == s_d1.state)
    double_aligned = aligned_d1_h1 and aligned_w1_d1
    direction = "BULL" if aligned_d1_h1 and s_d1.state == 1 else "BEAR" if aligned_d1_h1 and s_d1.state == -1 else "NONE"
    flame = "\U0001F525" if aligned_d1_h1 and ribbon_d1 == s_d1.state else ""

    print(f"PAIR      : {pair}")
    print(f"W1 STATE  : {state_name(s_w1.state)}")
    print(f"D1 STATE  : {state_name(s_d1.state)}")
    print(f"H1 STATE  : {state_name(s_h1.state)}")
    print(f"D1/H1 ALIGNMENT : {direction} {flame}".rstrip())
    print(f"W1/D1 ALIGNMENT : {'YES' if aligned_w1_d1 else 'NO'}")
    print(f"DOUBLE ALIGNMENT: {'YES' if double_aligned else 'NO'}")
    print(f"D1 RIBBON : {'BULL' if ribbon_d1 == 1 else 'BEAR' if ribbon_d1 == -1 else 'NEUTRAL'}")
    print(f"CHG%CC D1 : {'N/A' if chg_cc_d1 is None else f'{chg_cc_d1:+.2f}%'}")
    print(f"W1 close  : {s_w1.close}")
    print(f"D1 close  : {s_d1.close}")
    print(f"H1 close  : {s_h1.close}")
    print(f"W1 bull_line: {s_w1.bull_line_now} | W1 bear_line: {s_w1.bear_line_now}")
    print(f"D1 bull_line: {s_d1.bull_line_now} | D1 bear_line: {s_d1.bear_line_now}")
    print(f"H1 bull_line: {s_h1.bull_line_now} | H1 bear_line: {s_h1.bear_line_now}")
    print(f"W1 bull_last_valid_event: {s_w1.bull_valid_last_event} | W1 bear_last_valid_event: {s_w1.bear_valid_last_event}")
    print(f"D1 bull_last_valid_event: {s_d1.bull_valid_last_event} | D1 bear_last_valid_event: {s_d1.bear_valid_last_event}")
    print(f"H1 bull_last_valid_event: {s_h1.bull_valid_last_event} | H1 bear_last_valid_event: {s_h1.bear_valid_last_event}")
    return 0

def scan_alignment(pairs: list[str]) -> int:
    rows = []
    t0 = time.time()

    print(f"Scan alignment D1/H1 + W1/D1 on {len(pairs)} instruments")
    for i, pair in enumerate(pairs, 1):
        symbol = f"OANDA:{pair}"
        print(f"[{i:>2}/{len(pairs)}] {pair} ...", end=" ", flush=True)

        w1 = fetch_tv_ohlc(symbol, "W", 350)
        d1 = fetch_tv_ohlc(symbol, "D", 500)
        h1 = fetch_tv_ohlc(symbol, "60", 1400)
        if w1 is None or d1 is None or h1 is None:
            print("ERROR")
            rows.append({"pair": pair, "error": True})
            continue

        s_w1 = compute_break_line_state(w1)
        s_d1 = compute_break_line_state(d1)
        s_h1 = compute_break_line_state(h1)
        ribbon_d1 = daily_ema_ribbon_direction(d1)
        ichimoku_d1 = daily_ichimoku_cloud_direction(d1)
        chg_cc_d1 = daily_chg_cc(d1)
        aligned_d1_h1 = (s_d1.state != 0) and (s_d1.state == s_h1.state)
        aligned_w1_d1 = (s_w1.state != 0) and (s_w1.state == s_d1.state)
        double_aligned = aligned_d1_h1 and aligned_w1_d1
        direction = "BULL" if aligned_d1_h1 and s_d1.state == 1 else "BEAR" if aligned_d1_h1 and s_d1.state == -1 else "NONE"
        w1d1_direction = "BULL" if aligned_w1_d1 and s_d1.state == 1 else "BEAR" if aligned_w1_d1 and s_d1.state == -1 else "NONE"
        flame = aligned_d1_h1 and (ribbon_d1 == s_d1.state)
        aligned_cloud_ok = (direction == "BULL" and ichimoku_d1 == 1) or (direction == "BEAR" and ichimoku_d1 == -1)
        retrace_cloud_ok = (w1d1_direction == "BULL" and ichimoku_d1 == 1) or (w1d1_direction == "BEAR" and ichimoku_d1 == -1)

        rows.append(
            {
                "pair": pair,
                "error": False,
                "w1_state": s_w1.state,
                "d1_state": s_d1.state,
                "h1_state": s_h1.state,
                "d1_ribbon_state": ribbon_d1,
                "chg_cc_d1": chg_cc_d1,
                "aligned": aligned_d1_h1,
                "aligned_w1_d1": aligned_w1_d1,
                "double_aligned": double_aligned,
                "direction": direction,
                "w1d1_direction": w1d1_direction,
                "flame": flame,
                "retrace_flame": retrace_flame_active(h1, w1d1_direction),
                "daily_cloud_ok": aligned_cloud_ok,
                "retrace_daily_cloud_ok": retrace_cloud_ok,
            }
        )

        print(
            f"W1={state_name(s_w1.state)} D1={state_name(s_d1.state)} H1={state_name(s_h1.state)} "
            f"D1H1={direction} W1D1={'YES' if aligned_w1_d1 else 'NO'}{' 🔥' if flame else ''}"
        )

    aligned_rows = [r for r in rows if not r.get("error") and r.get("aligned")]
    aligned_rows = [r for r in aligned_rows if passes_chg_filter(r.get("direction"), r.get("chg_cc_d1"))]
    aligned_rows.sort(key=lambda r: (0 if r["direction"] == "BULL" else 1, r["pair"]))

    print("\nALIGNED PAIRS")
    if not aligned_rows:
        print("None")
    else:
        for r in aligned_rows:
            chg_txt = "N/A" if r.get("chg_cc_d1") is None else f"{r['chg_cc_d1']:+.2f}%"
            print(f"  {r['pair']:<8} {r['direction']}{' 🔥' if r.get('flame') else ''}  ({chg_txt})")

    telegram_rows = [
        r for r in aligned_rows
        if r.get("double_aligned")
        and passes_telegram_abs_chg_filter(r.get("chg_cc_d1"))
        and passes_chg_filter(r.get("direction"), r.get("chg_cc_d1"))
    ]
    telegram_rows.sort(key=lambda r: abs(r.get("chg_cc_d1") or 0.0), reverse=True)
    retrace_rows = [
        r for r in rows
        if not r.get("error")
        and r.get("aligned_w1_d1")
        and r.get("direction") == "NONE"
        and passes_telegram_abs_chg_filter(r.get("chg_cc_d1"))
        and passes_chg_filter(r.get("w1d1_direction"), r.get("chg_cc_d1"))
    ]
    retrace_rows.sort(key=lambda r: abs(r.get("chg_cc_d1") or 0.0), reverse=True)
    ambiguous_currencies = collect_ambiguous_currencies(telegram_rows, retrace_rows)
    telegram_rows = filter_ambiguous_rows(telegram_rows, ambiguous_currencies)
    retrace_rows = filter_ambiguous_rows(retrace_rows, ambiguous_currencies)
    previous_state = load_tracking_state(TRACKING_PATH)
    exited_rows = compute_exited_rows(previous_state, telegram_rows, retrace_rows)
    exited_rows = filter_ambiguous_rows(exited_rows, ambiguous_currencies)
    tg_text = build_telegram_tracking_message(telegram_rows, retrace_rows, exited_rows)
    send_telegram_message(tg_text)
    save_tracking_state(TRACKING_PATH, telegram_rows, retrace_rows)

    print(f"Elapsed: {time.time() - t0:.2f}s")
    return 0

def parse_args():
    parser = argparse.ArgumentParser(description="break_line.py - Python version of break_line.pine")
    parser.add_argument("--pair", default=None, help="Analyze only this pair on D1+H1 (ex: EURUSD)")
    parser.add_argument("--single-tf", action="store_true", help="Analyze only one timeframe instead of D1+H1")
    parser.add_argument("--tf", default="60", help="Timeframe for --single-tf, ex: 60, D, W")
    parser.add_argument("--candles", type=int, default=1400, help="Number of candles for --single-tf")
    parser.add_argument("--scan29", action="store_true", help="Scan 29 instruments for D1/H1 alignment")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.single_tf:
        if not args.pair:
            raise SystemExit("--single-tf requires --pair")
        raise SystemExit(analyze_single_tf(args.pair.upper(), args.tf, args.candles))
    if args.scan29 or not args.pair:
        raise SystemExit(scan_alignment(PAIRS_29))
    raise SystemExit(analyze_pair_alignment(args.pair.upper()))
if __name__ == "__main__":
    main()


