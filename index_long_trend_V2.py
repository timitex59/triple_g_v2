#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
INDEX_LONG_TREND_V
- Websocket TradingView source.
- For each FX pair, compute the percentage distance between:
  EMA20-EMA25, EMA25-EMA30, EMA30-EMA35, EMA35-EMA40,
  EMA40-EMA45, EMA45-EMA50, EMA50-EMA55.
- Compute the average of these 7 distances.
- Rank pairs by descending average.
"""

import json
import random
import string
import time
import os

import pandas as pd
import requests
from websocket import create_connection, WebSocketConnectionClosedException

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]
TIMEFRAME = "D"
N_CANDLES = 350
TIMEFRAME_H1 = "60"
N_CANDLES_H1 = 900
MOMENTUM_LOOKBACK = 5
CONSISTENCY_BARS = 20
PSAR_START = 0.1
PSAR_INCREMENT = 0.1
PSAR_MAXIMUM = 0.2
W_PSAR = 0.15
W_CHG = 0.15
W_WCHG = 0.15

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURNZD", "EURCHF",
    "GBPAUD", "GBPCAD", "GBPNZD", "GBPCHF",
    "AUDNZD", "AUDCAD", "AUDCHF",
    "NZDCAD", "NZDCHF",
    "CADCHF",
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


def fetch_tv_ohlc(symbol, interval, n_candles, sleep_s=0.05, timeout_s=20, retries=2):
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
            if sleep_s:
                time.sleep(sleep_s)
    return None


def ema_series(close_series, length):
    return close_series.ewm(span=length, adjust=False).mean()


def calculate_psar(df, start, increment, maximum):
    if df is None or len(df) < 3:
        return None
    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values
    psar = [0.0] * len(df)
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


def pct_distance(a, b):
    if b == 0 or pd.isna(a) or pd.isna(b):
        return None
    return (a - b) / b * 100.0


def fmt_num(v):
    return "na" if v is None or pd.isna(v) else f"{v:+.3f}%"


def clamp(v, lo=-1.0, hi=1.0):
    if pd.isna(v):
        return 0.0
    return max(lo, min(hi, float(v)))


def structure_state(ema_vals):
    ordered = [ema_vals[l] for l in EMA_LENGTHS]
    if all(ordered[i] > ordered[i + 1] for i in range(len(ordered) - 1)):
        return "BULLISH"
    if all(ordered[i] < ordered[i + 1] for i in range(len(ordered) - 1)):
        return "BEARISH"
    return "NEUTRAL"


def classify_state(score, structure):
    if structure == "BULLISH" and score >= 0.20:
        return "BULLISH"
    if structure == "BEARISH" and score <= -0.20:
        return "BEARISH"
    return "NEUTRAL"


def combine_with_negative_rule(a, b):
    if pd.isna(a) or pd.isna(b):
        return pd.NA
    s = float(a) * float(b)
    # Special rule: if both inputs are negative, keep final result negative.
    if float(a) < 0 and float(b) < 0:
        return -abs(s)
    return s


def analyze_symbol(name, tv_symbol, out_key, extra_fields=None, timeframe=TIMEFRAME, n_candles=N_CANDLES, include_weekly=True):
    df = fetch_tv_ohlc(tv_symbol, timeframe, n_candles)
    if df is None or df.empty:
        return None
    df_w = fetch_tv_ohlc(tv_symbol, "W", 220) if include_weekly else None
    psar = calculate_psar(df, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
    psar_now = psar.iloc[-1] if psar is not None and not psar.empty else None

    ema_vals = {}
    for length in EMA_LENGTHS:
        ema_vals[length] = ema_series(df["close"], length).iloc[-1]

    distances = []
    labels = []
    for i in range(len(EMA_LENGTHS) - 1):
        a = EMA_LENGTHS[i]
        b = EMA_LENGTHS[i + 1]
        d = pct_distance(ema_vals[a], ema_vals[b])
        distances.append(d)
        labels.append(f"EMA{a}-EMA{b}")

    valid = [d for d in distances if d is not None and not pd.isna(d)]
    avg = sum(valid) / len(valid) if valid else None

    ema20 = ema_series(df["close"], 20)
    ema20_now = ema20.iloc[-1]
    ema20_prev = ema20.iloc[-1 - MOMENTUM_LOOKBACK] if len(ema20) > MOMENTUM_LOOKBACK else pd.NA
    slope_ema20 = pct_distance(ema20_now, ema20_prev) if pd.notna(ema20_prev) else None
    price_vs_ema20 = pct_distance(df["close"].iloc[-1], ema20_now)

    if len(df) > MOMENTUM_LOOKBACK:
        ema_prev_vals = {}
        for length in EMA_LENGTHS:
            series = ema_series(df["close"], length)
            ema_prev_vals[length] = series.iloc[-1 - MOMENTUM_LOOKBACK]
        prev_distances = []
        for i in range(len(EMA_LENGTHS) - 1):
            a = EMA_LENGTHS[i]
            b = EMA_LENGTHS[i + 1]
            prev_distances.append(pct_distance(ema_prev_vals[a], ema_prev_vals[b]))
        prev_valid = [d for d in prev_distances if d is not None and not pd.isna(d)]
        avg_prev = sum(prev_valid) / len(prev_valid) if prev_valid else None
    else:
        avg_prev = None
    accel_gap = (avg - avg_prev) if avg is not None and avg_prev is not None else None

    recent = df["close"].tail(CONSISTENCY_BARS)
    bull_count = 0
    bear_count = 0
    if len(recent) > 0:
        for i in range(len(recent)):
            chunk = df["close"].iloc[: len(df) - len(recent) + i + 1]
            tmp = {l: ema_series(chunk, l).iloc[-1] for l in EMA_LENGTHS}
            st = structure_state(tmp)
            if st == "BULLISH":
                bull_count += 1
            elif st == "BEARISH":
                bear_count += 1
    consistency = (bull_count - bear_count) / max(1, len(recent))

    structure = structure_state(ema_vals)
    structure_sign = 1.0 if structure == "BULLISH" else -1.0 if structure == "BEARISH" else 0.0
    close_now = df["close"].iloc[-1]
    if psar_now is None or pd.isna(psar_now):
        psar_side = "NA"
        psar_component = 0.0
    elif close_now > psar_now:
        psar_side = "ABOVE"
        psar_component = 1.0 if structure == "BULLISH" else 0.0
    elif close_now < psar_now:
        psar_side = "BELOW"
        psar_component = -1.0 if structure == "BEARISH" else 0.0
    else:
        psar_side = "ON"
        psar_component = 0.0

    if len(df) < 2:
        chg_cc = None
    else:
        prev_close = df["close"].iloc[-2]
        curr_close = df["close"].iloc[-1]
        chg_cc = None if prev_close == 0 else ((curr_close - prev_close) / prev_close) * 100.0

    if chg_cc is None or pd.isna(chg_cc):
        chg_side = "NA"
        chg_component = 0.0
    elif chg_cc > 0:
        chg_side = "POS"
        chg_component = 1.0 if structure == "BULLISH" else 0.0
    elif chg_cc < 0:
        chg_side = "NEG"
        chg_component = -1.0 if structure == "BEARISH" else 0.0
    else:
        chg_side = "ZERO"
        chg_component = 0.0

    if df_w is None or df_w.empty or len(df_w) < 2:
        w_chg_cc = None
    else:
        w_prev_close = df_w["close"].iloc[-2]
        w_curr_close = df_w["close"].iloc[-1]
        w_chg_cc = None if w_prev_close == 0 else ((w_curr_close - w_prev_close) / w_prev_close) * 100.0

    if w_chg_cc is None or pd.isna(w_chg_cc):
        w_chg_side = "NA"
        w_chg_component = 0.0
    elif w_chg_cc > 0:
        w_chg_side = "POS"
        w_chg_component = 1.0 if structure == "BULLISH" else 0.0
    elif w_chg_cc < 0:
        w_chg_side = "NEG"
        w_chg_component = -1.0 if structure == "BEARISH" else 0.0
    else:
        w_chg_side = "ZERO"
        w_chg_component = 0.0

    score = (
        0.35 * structure_sign
        + 0.25 * clamp((avg or 0.0) / 0.30)
        + 0.20 * clamp((slope_ema20 or 0.0) / 1.00)
        + 0.10 * clamp((accel_gap or 0.0) / 0.20)
        + 0.10 * clamp((price_vs_ema20 or 0.0) / 1.00)
        + W_PSAR * psar_component
        + W_CHG * chg_component
        + (W_WCHG * w_chg_component if include_weekly else 0.0)
    )
    state = classify_state(score, structure)

    out = {
        out_key: name,
        "STATE": state,
        "SCORE": score,
        "STRUCTURE": structure,
        "PSAR_SIDE": psar_side,
        "PSAR_OK": "YES" if psar_component != 0.0 else "NO",
        "CHG_CC_DAILY_%": chg_cc,
        "CHG_SIDE": chg_side,
        "CHG_OK": "YES" if chg_component != 0.0 else "NO",
        "CHG_CC_WEEKLY_%": w_chg_cc,
        "W_CHG_SIDE": w_chg_side,
        "W_CHG_OK": "YES" if w_chg_component != 0.0 else "NO",
        "AVG_EMA_GAP_%": avg,
        "SLOPE_EMA20_%": slope_ema20,
        "ACCEL_GAP_%": accel_gap,
        "PRICE_VS_EMA20_%": price_vs_ema20,
        "CONSISTENCY": consistency,
    }
    if extra_fields:
        out.update(extra_fields)
    for lbl, val in zip(labels, distances):
        out[lbl] = val
    return out


def main():
    if load_dotenv:
        load_dotenv()

    pair_rows = []
    for pair in PAIRS:
        r = analyze_symbol(
            pair,
            f"OANDA:{pair}",
            "PAIR",
            {"BASE": pair[:3], "QUOTE": pair[3:]},
        )
        if r:
            pair_rows.append(r)

    pair_rows_h1 = []
    for pair in PAIRS:
        r = analyze_symbol(
            pair,
            f"OANDA:{pair}",
            "PAIR",
            {"BASE": pair[:3], "QUOTE": pair[3:]},
            timeframe=TIMEFRAME_H1,
            n_candles=N_CANDLES_H1,
            include_weekly=False,
        )
        if r:
            pair_rows_h1.append(r)

    index_rows = []
    for idx, sym in INDICES.items():
        ccy = INDEX_TO_CCY.get(idx, idx)
        r = analyze_symbol(
            idx,
            sym,
            "INDEX",
            {"INDEX_CODE": idx, "CCY": ccy},
        )
        if r:
            index_rows.append(r)

    if not pair_rows and not index_rows:
        print("INDEX LONG TREND V: no data")
        return

    cols_tail = ["CHG_CC_DAILY_%", "CHG_CC_WEEKLY_%", "AVG_EMA_GAP_%", "SLOPE_EMA20_%", "ACCEL_GAP_%", "PRICE_VS_EMA20_%", "CONSISTENCY",
                 "EMA20-EMA25", "EMA25-EMA30", "EMA30-EMA35",
                 "EMA35-EMA40", "EMA40-EMA45", "EMA45-EMA50", "EMA50-EMA55"]

    pair_df = pd.DataFrame(pair_rows) if pair_rows else pd.DataFrame(columns=["PAIR", "STATE", "SCORE", "STRUCTURE", "PSAR_SIDE", "PSAR_OK", "CHG_SIDE", "CHG_OK", "W_CHG_SIDE", "W_CHG_OK"] + cols_tail)
    index_df = pd.DataFrame(index_rows) if index_rows else pd.DataFrame(columns=["INDEX", "STATE", "SCORE", "STRUCTURE", "PSAR_SIDE", "PSAR_OK", "CHG_SIDE", "CHG_OK", "W_CHG_SIDE", "W_CHG_OK"] + cols_tail)

    if not pair_df.empty:
        pair_df = pair_df.sort_values(by="SCORE", ascending=False, na_position="last")
        pair_display = pair_df[["PAIR", "STATE", "SCORE", "STRUCTURE", "PSAR_SIDE", "PSAR_OK", "CHG_SIDE", "CHG_OK", "W_CHG_SIDE", "W_CHG_OK"] + cols_tail].copy()
        pair_display["SCORE"] = pair_display["SCORE"].map(lambda v: "na" if pd.isna(v) else f"{v:+.3f}")
        for c in cols_tail:
            pair_display[c] = pair_display[c].map(fmt_num)
        print("INDEX LONG TREND V2 (Pairs ranked by SCORE)")
        print(pair_display.to_string(index=False))
    else:
        print("INDEX LONG TREND V2 (Pairs ranked by SCORE)")
        print("No pair data")

    cols_tail_h1 = ["CHG_CC_DAILY_%", "AVG_EMA_GAP_%", "SLOPE_EMA20_%", "ACCEL_GAP_%", "PRICE_VS_EMA20_%", "CONSISTENCY",
                    "EMA20-EMA25", "EMA25-EMA30", "EMA30-EMA35",
                    "EMA35-EMA40", "EMA40-EMA45", "EMA45-EMA50", "EMA50-EMA55"]
    pair_h1_df = pd.DataFrame(pair_rows_h1) if pair_rows_h1 else pd.DataFrame(columns=["PAIR", "STATE", "SCORE", "STRUCTURE", "PSAR_SIDE", "PSAR_OK", "CHG_SIDE", "CHG_OK"] + cols_tail_h1)
    if not pair_h1_df.empty:
        pair_h1_df = pair_h1_df.sort_values(by="SCORE", ascending=False, na_position="last")
        pair_h1_display = pair_h1_df[["PAIR", "STATE", "SCORE", "STRUCTURE", "PSAR_SIDE", "PSAR_OK", "CHG_SIDE", "CHG_OK"] + cols_tail_h1].copy()
        pair_h1_display["SCORE"] = pair_h1_display["SCORE"].map(lambda v: "na" if pd.isna(v) else f"{v:+.3f}")
        for c in cols_tail_h1:
            pair_h1_display[c] = pair_h1_display[c].map(fmt_num)
        print("\nINDEX LONG TREND V2 (Pairs ranked by SCORE - H1, no weekly)")
        print(pair_h1_display.to_string(index=False))
    else:
        print("\nINDEX LONG TREND V2 (Pairs ranked by SCORE - H1, no weekly)")
        print("No pair H1 data")

    pair_summary_df = pd.DataFrame()
    if not pair_df.empty and not pair_h1_df.empty:
        idx_score_by_ccy = {}
        if not index_df.empty:
            for _, irow in index_df.iterrows():
                ccy = irow.get("CCY")
                sc = irow.get("SCORE")
                if isinstance(ccy, str) and pd.notna(sc):
                    idx_score_by_ccy[ccy] = float(sc)

        pair_daily_core = pair_df[["PAIR", "SCORE", "STATE", "CHG_CC_DAILY_%"]].rename(
            columns={"SCORE": "SCORE_DAILY", "STATE": "STATE_DAILY"}
        )
        pair_h1_core = pair_h1_df[["PAIR", "SCORE", "STATE"]].rename(
            columns={"SCORE": "SCORE_H1", "STATE": "STATE_H1"}
        )
        pair_summary_df = pair_daily_core.merge(pair_h1_core, on="PAIR", how="inner")
        pair_summary_df["BASE"] = pair_summary_df["PAIR"].str.slice(0, 3)
        pair_summary_df["QUOTE"] = pair_summary_df["PAIR"].str.slice(3, 6)
        pair_summary_df["TREND_COEF"] = pair_summary_df.apply(
            lambda r: (
                idx_score_by_ccy.get(r["BASE"]) - idx_score_by_ccy.get(r["QUOTE"])
                if r["BASE"] in idx_score_by_ccy and r["QUOTE"] in idx_score_by_ccy
                else pd.NA
            ),
            axis=1,
        )
        pair_summary_df["SCORE_DxH1"] = pair_summary_df.apply(
            lambda r: combine_with_negative_rule(r["SCORE_DAILY"], r["SCORE_H1"]), axis=1
        )
        pair_summary_df["FINAL_SCORE"] = pair_summary_df.apply(
            lambda r: combine_with_negative_rule(r["SCORE_DxH1"], r["TREND_COEF"]), axis=1
        )
        pair_summary_df["ABS_FINAL_SCORE"] = pair_summary_df["FINAL_SCORE"].abs()
        pair_summary_df = pair_summary_df.sort_values(by="ABS_FINAL_SCORE", ascending=False, na_position="last")
        pair_summary_display = pair_summary_df.copy()
        pair_summary_display["SCORE_DAILY"] = pair_summary_display["SCORE_DAILY"].map(
            lambda v: "na" if pd.isna(v) else f"{v:+.3f}"
        )
        pair_summary_display["SCORE_H1"] = pair_summary_display["SCORE_H1"].map(
            lambda v: "na" if pd.isna(v) else f"{v:+.3f}"
        )
        pair_summary_display["SCORE_DxH1"] = pair_summary_display["SCORE_DxH1"].map(
            lambda v: "na" if pd.isna(v) else f"{v:+.4f}"
        )
        pair_summary_display["TREND_COEF"] = pair_summary_display["TREND_COEF"].map(
            lambda v: "na" if pd.isna(v) else f"{v:+.3f}"
        )
        pair_summary_display["FINAL_SCORE"] = pair_summary_display["FINAL_SCORE"].map(
            lambda v: "na" if pd.isna(v) else f"{v:+.4f}"
        )
        print("\nINDEX LONG TREND V2 (Pairs score summary: DAILY x H1)")
        print(
            pair_summary_display[
                ["PAIR", "STATE_DAILY", "STATE_H1", "SCORE_DAILY", "SCORE_H1", "SCORE_DxH1", "TREND_COEF", "FINAL_SCORE"]
            ].to_string(index=False)
        )
    else:
        print("\nINDEX LONG TREND V2 (Pairs score summary: DAILY x H1)")
        print("No summary data")

    if not index_df.empty:
        index_df = index_df.sort_values(by="SCORE", ascending=False, na_position="last")
        index_display = index_df[["INDEX", "CCY", "STATE", "SCORE", "STRUCTURE", "PSAR_SIDE", "PSAR_OK", "CHG_SIDE", "CHG_OK", "W_CHG_SIDE", "W_CHG_OK"] + cols_tail].copy()
        index_display["SCORE"] = index_display["SCORE"].map(lambda v: "na" if pd.isna(v) else f"{v:+.3f}")
        for c in cols_tail:
            index_display[c] = index_display[c].map(fmt_num)
        print("\nINDEX LONG TREND V2 (Indices ranked by SCORE)")
        print(index_display.to_string(index=False))
    else:
        print("\nINDEX LONG TREND V2 (Indices ranked by SCORE)")
        print("No index data")

    combo_df = pd.DataFrame()
    if not pair_df.empty and not index_df.empty:
        idx_by_ccy = {}
        for _, row in index_df.iterrows():
            ccy = row.get("CCY")
            if isinstance(ccy, str):
                idx_by_ccy[ccy] = row

        combo_rows = []
        for _, prow in pair_df.iterrows():
            pair = prow.get("PAIR")
            base = prow.get("BASE")
            quote = prow.get("QUOTE")
            if base not in idx_by_ccy or quote not in idx_by_ccy:
                continue

            bidx = idx_by_ccy[base]
            qidx = idx_by_ccy[quote]
            b_state = bidx.get("STATE", "NEUTRAL")
            q_state = qidx.get("STATE", "NEUTRAL")
            pair_state = prow.get("STATE", "NEUTRAL")

            if b_state == "BULLISH" and q_state == "BEARISH":
                bias = "BULL"
            elif b_state == "BEARISH" and q_state == "BULLISH":
                bias = "BEAR"
            else:
                bias = "NEUTRAL"

            confirmed = (bias == "BULL" and pair_state == "BULLISH") or (bias == "BEAR" and pair_state == "BEARISH")
            b_score = abs(float(bidx.get("SCORE", 0.0))) if pd.notna(bidx.get("SCORE")) else 0.0
            q_score = abs(float(qidx.get("SCORE", 0.0))) if pd.notna(qidx.get("SCORE")) else 0.0
            p_score = abs(float(prow.get("SCORE", 0.0))) if pd.notna(prow.get("SCORE")) else 0.0
            confidence = (b_score + q_score + p_score) / 3.0
            b_score_signed = float(bidx.get("SCORE", 0.0)) if pd.notna(bidx.get("SCORE")) else 0.0
            q_score_signed = float(qidx.get("SCORE", 0.0)) if pd.notna(qidx.get("SCORE")) else 0.0
            p_score_signed = float(prow.get("SCORE", 0.0)) if pd.notna(prow.get("SCORE")) else 0.0
            direction_idx = (b_score_signed + q_score_signed) / 2.0
            direction_all = (b_score_signed + q_score_signed + p_score_signed) / 3.0

            combo_rows.append(
                {
                    "PAIR": pair,
                    "BASE_IDX": bidx.get("INDEX"),
                    "QUOTE_IDX": qidx.get("INDEX"),
                    "INDEX_BIAS": bias,
                    "PAIR_STATE": pair_state,
                    "CONFIRMED": "YES" if confirmed else "NO",
                    "CONFIDENCE": confidence,
                    "DIRECTION_IDX": direction_idx,
                    "DIRECTION_ALL": direction_all,
                }
            )

        if combo_rows:
            combo_df = pd.DataFrame(combo_rows).sort_values(
                by=["CONFIRMED", "CONFIDENCE"],
                ascending=[False, False],
                na_position="last",
            )
            combo_display = combo_df.copy()
            combo_display["CONFIDENCE"] = combo_display["CONFIDENCE"].map(lambda v: "na" if pd.isna(v) else f"{v:+.3f}")
            combo_display["DIRECTION_IDX"] = combo_display["DIRECTION_IDX"].map(lambda v: "na" if pd.isna(v) else f"{v:+.3f}")
            combo_display["DIRECTION_ALL"] = combo_display["DIRECTION_ALL"].map(lambda v: "na" if pd.isna(v) else f"{v:+.3f}")
            print("\nINDEX LONG TREND V2 (Index Combinations Confirmation)")
            print(combo_display.to_string(index=False))
        else:
            print("\nINDEX LONG TREND V2 (Index Combinations Confirmation)")
            print("No combination data")

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        lines = ["SCREENER"]
        if not pair_summary_df.empty:
            top5 = pair_summary_df.head(5)
            for _, row in top5.iterrows():
                final_score = row.get("FINAL_SCORE")
                chg_daily = row.get("CHG_CC_DAILY_%")
                if pd.isna(final_score) or pd.isna(chg_daily):
                    continue
                dot = "🟢" if float(final_score) >= 0 else "🔴"
                chg_txt = f"{float(chg_daily):+.2f}%"
                lines.append(f"{dot} {row['PAIR']} ({chg_txt})")
        if len(lines) == 2:
            lines.append("NO DEAL")
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            resp = requests.post(url, json={"chat_id": chat_id, "text": "\n".join(lines)}, timeout=10)
            ok = False
            try:
                ok = bool(resp.json().get("ok", False))
            except Exception:
                ok = False
            print(f"Telegram: {'sent' if ok else 'not sent'}")
        except Exception as exc:
            print(f"Telegram: not sent ({exc})")
    else:
        print("Telegram: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")


if __name__ == "__main__":
    main()
