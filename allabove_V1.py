#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
allabove_V1.py

Forex scanner (28 pairs):
- Keep pairs where close > EMA 8 on all three timeframes: W1, D1, H1.
"""

import json
import os
import random
import string
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from websocket import WebSocketConnectionClosedException, create_connection

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURNZD", "EURCHF",
    "GBPAUD", "GBPCAD", "GBPNZD", "GBPCHF",
    "AUDNZD", "AUDCAD", "AUDCHF",
    "NZDCAD", "NZDCHF",
    "CADCHF",
]

EMA_LEN = 8
EMA_STACK = [20, 25, 30, 35, 40, 45, 50, 55]

TF_CONFIG = {
    "W1": {"interval": "W", "candles": 220},
    "D1": {"interval": "D", "candles": 400},
    "H1": {"interval": "60", "candles": 1000},
}

if load_dotenv:
    load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def _gen_session_id():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


def _create_msg(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"


def _parse_frames(raw):
    if raw in ("~h", "h"):
        return [raw]
    frames = []
    i = 0
    while raw.startswith("~m~", i):
        i += 3
        j = raw.find("~m~", i)
        if j == -1:
            break
        size = int(raw[i:j])
        i = j + 3
        frames.append(raw[i : i + size])
        i += size
    return frames if frames else [raw]


def fetch_tv_ohlc(symbol, interval, n_candles, timeout_s=20, retries=2):
    for attempt in range(retries + 1):
        ws = None
        try:
            ws = create_connection(
                "wss://prodata.tradingview.com/socket.io/websocket",
                header={"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"},
                timeout=timeout_s,
            )
            session_id = _gen_session_id()
            ws.send(_create_msg("chart_create_session", [session_id, ""]))
            ws.send(
                _create_msg(
                    "resolve_symbol",
                    [session_id, "sds_sym_1", f'={{"symbol":"{symbol}","adjustment":"splits","session":"regular"}}'],
                )
            )
            ws.send(_create_msg("create_series", [session_id, "sds_1", "s1", "sds_sym_1", interval, n_candles, ""]))

            points = []
            start_t = time.time()
            while time.time() - start_t < 12:
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
            df.set_index("date", inplace=True)
            return df.sort_index()
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


def close_above_ema8(df):
    if df is None or df.empty or len(df) < EMA_LEN + 2:
        return False, None, None
    ema8 = df["close"].ewm(span=EMA_LEN, adjust=False).mean().iloc[-1]
    close = float(df["close"].iloc[-1])
    return close > float(ema8), close, float(ema8)


def bullish_ema_stack_aligned(df):
    if df is None or df.empty or len(df) < max(EMA_STACK) + 2:
        return False, None
    values = [float(df["close"].ewm(span=l, adjust=False).mean().iloc[-1]) for l in EMA_STACK]
    is_aligned = all(values[i] > values[i + 1] for i in range(len(values) - 1))
    return is_aligned, values


def bearish_ema_stack_aligned(df):
    if df is None or df.empty or len(df) < max(EMA_STACK) + 2:
        return False, None
    values = [float(df["close"].ewm(span=l, adjust=False).mean().iloc[-1]) for l in EMA_STACK]
    is_aligned = all(values[i] < values[i + 1] for i in range(len(values) - 1))
    return is_aligned, values


def close_below_ema8(df):
    if df is None or df.empty or len(df) < EMA_LEN + 2:
        return False, None, None
    ema8 = df["close"].ewm(span=EMA_LEN, adjust=False).mean().iloc[-1]
    close = float(df["close"].iloc[-1])
    return close < float(ema8), close, float(ema8)


def calculate_psar(df, start=0.1, increment=0.1, maximum=0.2):
    if df is None or len(df) < 3:
        return None
    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values
    psar = np.zeros(len(df))
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


def psar_valuewhen_levels(df_h1):
    out = {"bull_vw0": None, "bull_vw1": None, "bull_vw2": None, "bear_vw0": None, "bear_vw1": None, "bear_vw2": None}
    if df_h1 is None or df_h1.empty or len(df_h1) < 4:
        return out

    psar = calculate_psar(df_h1)
    if psar is None or psar.empty:
        return out

    close = df_h1["close"]
    bull = ((close > psar) & (close.shift(1) < psar.shift(1))).fillna(False)
    bear = ((close < psar) & (close.shift(1) > psar.shift(1))).fillna(False)

    bull_vals = [float(psar.iloc[i]) for i, flag in enumerate(bull.tolist()) if flag]
    bear_vals = [float(psar.iloc[i]) for i, flag in enumerate(bear.tolist()) if flag]

    if len(bull_vals) >= 1:
        out["bull_vw0"] = bull_vals[-1]
    if len(bull_vals) >= 2:
        out["bull_vw1"] = bull_vals[-2]
    if len(bull_vals) >= 3:
        out["bull_vw2"] = bull_vals[-3]
    if len(bear_vals) >= 1:
        out["bear_vw0"] = bear_vals[-1]
    if len(bear_vals) >= 2:
        out["bear_vw1"] = bear_vals[-2]
    if len(bear_vals) >= 3:
        out["bear_vw2"] = bear_vals[-3]
    return out


def analyze_pair(pair):
    symbol = f"OANDA:{pair}"
    checks = {}
    details = {}

    tf_data = {}
    for tf_name, cfg in TF_CONFIG.items():
        df = fetch_tv_ohlc(symbol, cfg["interval"], cfg["candles"])
        tf_data[tf_name] = df
        ok, close, ema8 = close_above_ema8(df)
        checks[tf_name] = ok
        details[tf_name] = {"close": close, "ema8": ema8}

        if tf_name in ("W1", "D1"):
            aligned, stack_vals = bullish_ema_stack_aligned(df)
            checks[f"{tf_name}_ALIGN8"] = aligned
            details[tf_name]["aligned8"] = aligned
            details[tf_name]["ema_stack"] = stack_vals

    # Build BEAR checks in parallel to support both bias conditions.
    w1_below, _, _ = close_below_ema8(tf_data.get("W1"))
    d1_below, _, _ = close_below_ema8(tf_data.get("D1"))
    h1_below, _, _ = close_below_ema8(tf_data.get("H1"))
    w1_align_bear, _ = bearish_ema_stack_aligned(tf_data.get("W1"))
    d1_align_bear, _ = bearish_ema_stack_aligned(tf_data.get("D1"))
    checks["W1_BEAR"] = w1_below
    checks["D1_BEAR"] = d1_below
    checks["H1_BEAR"] = h1_below
    checks["W1_ALIGN8_BEAR"] = w1_align_bear
    checks["D1_ALIGN8_BEAR"] = d1_align_bear
    details["W1"]["aligned8_bear"] = w1_align_bear
    details["D1"]["aligned8_bear"] = d1_align_bear

    # H1 PSAR valuewhen filter requested.
    vw = psar_valuewhen_levels(tf_data.get("H1"))
    bull_vw0 = vw.get("bull_vw0")
    bull_vw1 = vw.get("bull_vw1")
    bull_vw2 = vw.get("bull_vw2")
    bear_vw0 = vw.get("bear_vw0")
    bear_vw1 = vw.get("bear_vw1")
    bear_vw2 = vw.get("bear_vw2")
    h1_bull_vw_ok = (bull_vw0 is not None) and (bull_vw1 is not None) and (bull_vw0 > bull_vw1)
    h1_bear_vw_ok = (bear_vw0 is not None) and (bear_vw1 is not None) and (bear_vw0 < bear_vw1)
    h1_bull_vw_flame = (
        (bull_vw0 is not None)
        and (bull_vw1 is not None)
        and (bull_vw2 is not None)
        and (bull_vw0 > bull_vw1)
        and (bull_vw2 > bull_vw1)
    )
    h1_bear_vw_flame = (
        (bear_vw0 is not None)
        and (bear_vw1 is not None)
        and (bear_vw2 is not None)
        and (bear_vw0 < bear_vw1)
        and (bear_vw2 < bear_vw1)
    )
    checks["H1_BULL_VW"] = h1_bull_vw_ok
    checks["H1_BEAR_VW"] = h1_bear_vw_ok
    checks["H1_BULL_VW_FLAME"] = h1_bull_vw_flame
    checks["H1_BEAR_VW_FLAME"] = h1_bear_vw_flame
    details["H1"]["bull_vw0"] = bull_vw0
    details["H1"]["bull_vw1"] = bull_vw1
    details["H1"]["bull_vw2"] = bull_vw2
    details["H1"]["bear_vw0"] = bear_vw0
    details["H1"]["bear_vw1"] = bear_vw1
    details["H1"]["bear_vw2"] = bear_vw2

    chg_cc_daily = daily_chg_cc(tf_data.get("D1"))
    chg_cc_weekly = weekly_chg_cc(tf_data.get("W1"))

    bull_ok = (
        checks["W1"]
        and checks["D1"]
        and checks["H1"]
        and checks.get("W1_ALIGN8", False)
        and checks.get("D1_ALIGN8", False)
        and checks["H1_BULL_VW"]
        and (chg_cc_daily is not None)
        and (chg_cc_daily > 0)
        and (chg_cc_weekly is not None)
        and (chg_cc_weekly > 0)
    )
    bear_ok = (
        checks["W1_BEAR"]
        and checks["D1_BEAR"]
        and checks["H1_BEAR"]
        and checks.get("W1_ALIGN8_BEAR", False)
        and checks.get("D1_ALIGN8_BEAR", False)
        and checks["H1_BEAR_VW"]
        and (chg_cc_daily is not None)
        and (chg_cc_daily < 0)
        and (chg_cc_weekly is not None)
        and (chg_cc_weekly < 0)
    )
    bull_wd_ok = (
        checks["W1"]
        and checks["D1"]
        and checks.get("W1_ALIGN8", False)
        and checks.get("D1_ALIGN8", False)
        and (chg_cc_daily is not None)
        and (chg_cc_daily > 0)
        and (chg_cc_weekly is not None)
        and (chg_cc_weekly > 0)
    )
    bear_wd_ok = (
        checks["W1_BEAR"]
        and checks["D1_BEAR"]
        and checks.get("W1_ALIGN8_BEAR", False)
        and checks.get("D1_ALIGN8_BEAR", False)
        and (chg_cc_daily is not None)
        and (chg_cc_daily < 0)
        and (chg_cc_weekly is not None)
        and (chg_cc_weekly < 0)
    )
    bias = "BULL" if bull_ok else "BEAR" if bear_ok else "NONE"
    wd_bias = "BULL" if bull_wd_ok else "BEAR" if bear_wd_ok else "NONE"
    all_ok = bull_ok or bear_ok
    h1_flame = (bias == "BULL" and h1_bull_vw_flame) or (bias == "BEAR" and h1_bear_vw_flame)
    return {
        "pair": pair,
        "bias": bias,
        "wd_bias": wd_bias,
        "wd_ok": bull_wd_ok or bear_wd_ok,
        "h1_flame": h1_flame,
        "chg_cc_daily": chg_cc_daily,
        "chg_cc_weekly": chg_cc_weekly,
        "all_ok": all_ok,
        "checks": checks,
        "details": details,
    }


def fmt_pct_dist(close, ema):
    if close is None or ema is None or ema == 0:
        return "N/A"
    return f"{((close - ema) / ema) * 100:+.2f}%"


def daily_chg_cc(df_d1):
    if df_d1 is None or df_d1.empty or len(df_d1) < 2:
        return None
    prev_close = float(df_d1["close"].iloc[-2])
    last_close = float(df_d1["close"].iloc[-1])
    if prev_close == 0:
        return None
    return ((last_close - prev_close) / prev_close) * 100.0


def weekly_chg_cc(df_w1):
    if df_w1 is None or df_w1.empty or len(df_w1) < 2:
        return None
    prev_close = float(df_w1["close"].iloc[-2])
    last_close = float(df_w1["close"].iloc[-1])
    if prev_close == 0:
        return None
    return ((last_close - prev_close) / prev_close) * 100.0


def now_paris_str():
    return datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M")


def build_telegram_allabove_lines(matches):
    lines = ["ALLABOVE"]
    if not matches:
        lines.append("⚪ NO DEAL")
    else:
        for r in matches:
            icon = "🟢" if r.get("bias") == "BULL" else "🔴"
            chg = r.get("chg_cc_daily")
            chg_txt = "N/A" if chg is None else f"{chg:+.2f}%"
            flame = "🔥" if r.get("h1_flame") else ""
            lines.append(f"{icon} {r['pair']} ({chg_txt}){flame}")
    lines.append("")
    lines.append(f"⏰ {now_paris_str()} Paris")
    return lines


def send_telegram_message(text):
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


def main():
    print("ALL ABOVE V1")
    print("BULL: close > EMA8 (W1/D1/H1) + 8EMA aligned up (W1/D1) + bull_vw0 > bull_vw1 (H1) + CHG% CC DAILY > 0 + CHG% CC WEEKLY > 0")
    print("BEAR: close < EMA8 (W1/D1/H1) + 8EMA aligned down (W1/D1) + bear_vw0 < bear_vw1 (H1) + CHG% CC DAILY < 0 + CHG% CC WEEKLY < 0")

    t0 = time.time()
    matches = []
    wd_matches = []
    failed = []

    for pair in PAIRS:
        result = analyze_pair(pair)
        if result["wd_ok"]:
            wd_matches.append(result)
        if result["all_ok"]:
            matches.append(result)
        else:
            # Keep explicit failures where at least one TF failed or data missing.
            if not all(result["checks"].values()):
                failed.append(result)

    print(f"\nPairs scanned: {len(PAIRS)}")
    print(f"Weekly+Daily matches: {len(wd_matches)}")
    print(f"Matches: {len(matches)}")
    print(f"Non-matches: {len(failed)}")

    if wd_matches:
        print("\nWEEKLY + DAILY ONLY")
        print("  Conditions: W1/D1 only (no H1 filter)")
        for r in sorted(wd_matches, key=lambda x: x["pair"]):
            w = r["details"]["W1"]
            d = r["details"]["D1"]
            chg_d_txt = "N/A" if r.get("chg_cc_daily") is None else f"{r['chg_cc_daily']:+.2f}%"
            chg_w_txt = "N/A" if r.get("chg_cc_weekly") is None else f"{r['chg_cc_weekly']:+.2f}%"
            print(
                f"  {r['pair']} [{r['wd_bias']}] | "
                f"W1:{fmt_pct_dist(w['close'], w['ema8'])} "
                f"D1:{fmt_pct_dist(d['close'], d['ema8'])} "
                f"| CHG%CC D1:{chg_d_txt} W1:{chg_w_txt}"
            )
    else:
        print("\nWEEKLY + DAILY ONLY")
        print("  No pair matches weekly+daily conditions (W1/D1 only).")

    if matches:
        # Rank by average percentage distance to EMA8 across the 3 TFs.
        def avg_dist(x):
            vals = []
            for tf in ("W1", "D1", "H1"):
                d = x["details"][tf]
                c, e = d["close"], d["ema8"]
                if c is not None and e not in (None, 0):
                    vals.append((c - e) / e)
            return sum(vals) / len(vals) if vals else -999.0

        matches.sort(key=avg_dist, reverse=True)

        print("\nSECTION INITIALE (INCLUANT H1)")
        print("  🔥 = H1 strong VW: BULL(bull_vw0 > bull_vw1 and bull_vw2 > bull_vw1) | BEAR(bear_vw0 < bear_vw1 and bear_vw2 < bear_vw1)")
        print("  H1_BASE:OK = ancienne regle validee (BULL: bull_vw0 > bull_vw1 | BEAR: bear_vw0 < bear_vw1)")
        for r in matches:
            p = r["pair"]
            b = r["bias"]
            flame = "🔥 " if r.get("h1_flame") else ""
            w = r["details"]["W1"]
            d = r["details"]["D1"]
            h = r["details"]["H1"]
            if b == "BULL":
                vw_txt = f"bull_vw0:{h.get('bull_vw0'):.5f} > bull_vw1:{h.get('bull_vw1'):.5f}"
            else:
                vw_txt = f"bear_vw0:{h.get('bear_vw0'):.5f} < bear_vw1:{h.get('bear_vw1'):.5f}"
            chg_d_txt = "N/A" if r.get("chg_cc_daily") is None else f"{r['chg_cc_daily']:+.2f}%"
            chg_w_txt = "N/A" if r.get("chg_cc_weekly") is None else f"{r['chg_cc_weekly']:+.2f}%"
            print(
                f"  {flame}{p} [{b}] | W1:{fmt_pct_dist(w['close'], w['ema8'])} "
                f"D1:{fmt_pct_dist(d['close'], d['ema8'])} "
                f"H1:{fmt_pct_dist(h['close'], h['ema8'])} | H1_BASE:OK | CHG%CC D1:{chg_d_txt} W1:{chg_w_txt} | {vw_txt}"
            )
    else:
        print("\nNo pair matches the BULL/BEAR bias filters.")

    # Telegram: send only the initial section (including H1 filter).
    tg_text = "\n".join(build_telegram_allabove_lines(matches))
    send_telegram_message(tg_text)

    print(f"\nElapsed: {time.time() - t0:.2f}s")


if __name__ == "__main__":
    main()
