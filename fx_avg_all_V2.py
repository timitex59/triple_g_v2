#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FX_Avg_All_V2
- Same screener as fx_avg_all.py
- Crosses with rubbeon_MTF_IMP_V2 (ALIGN + GLOBAL)
- Adds 🔥 next to pairs present in both results (Telegram only)
"""

import json
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from websocket import create_connection, WebSocketConnectionClosedException
import os
import requests
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import rubbeon_MTF_IMP_V2 as mtf


EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"]

INDEX_MAP = {
    "USD": "DXY",
    "EUR": "EXY",
    "GBP": "BXY",
    "JPY": "JXY",
    "CHF": "SXY",
    "CAD": "CXY",
    "AUD": "AXY",
    "NZD": "ZXY",
}

INDICES = {k: f"TVC:{v}" for k, v in INDEX_MAP.items()}

PAIR_LIST = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURNZD", "EURCHF",
    "GBPAUD", "GBPCAD", "GBPNZD", "GBPCHF",
    "AUDNZD", "AUDCAD", "AUDCHF",
    "NZDCAD", "NZDCHF",
    "CADCHF",
]
PAIR_SET = set(PAIR_LIST)


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


def daily_chg_pct(df_h1, df_d):
    if df_h1 is None or df_d is None or df_h1.empty or df_d.empty:
        return None
    h1_close = df_h1["close"].iloc[-1]
    d_open = df_d["open"].iloc[-1]
    if d_open == 0:
        return None
    return (h1_close - d_open) / d_open * 100.0


def dist_from_ema20(df_h1, df_d):
    if df_h1 is None or df_d is None or df_h1.empty or df_d.empty:
        return None
    h1_close = df_h1["close"].iloc[-1]
    e20 = ema_series(df_d["close"], EMA_LENGTHS[0]).iloc[-1]
    if e20 == 0:
        return None
    return (h1_close - e20) / e20 * 100.0


def is_below_8_emas(df_h1, df_d):
    if df_h1 is None or df_d is None or df_h1.empty or df_d.empty:
        return None
    h1_close = df_h1["close"].iloc[-1]
    for length in EMA_LENGTHS:
        if h1_close >= ema_series(df_d["close"], length).iloc[-1]:
            return False
    return True


def safe_add(v):
    return 0.0 if v is None or pd.isna(v) else float(v)


def fmt_num(v):
    return "na" if v is None or pd.isna(v) else f"{v:.2f}"


def find_pair(cur, other):
    if cur + other in PAIR_SET:
        return cur + other, True
    if other + cur in PAIR_SET:
        return other + cur, False
    return None, None


def build_pairs_for_currency(cur):
    pairs = []
    for other in CURRENCIES:
        if other == cur:
            continue
        pair_code, cur_is_base = find_pair(cur, other)
        if not pair_code:
            continue
        pairs.append(
            {
                "pair": pair_code,
                "symbol": f"OANDA:{pair_code}",
                "other": other,
                "invert": not cur_is_base,
                "index": INDEX_MAP[other],
            }
        )
    return pairs


def normalize_pair_code(pair):
    return pair.replace("=X", "")


def mtf_aligned_pairs():
    """
    Uses rubbeon_MTF_IMP_V2 to get pairs where ALIGN + GLOBAL are both BULL/BEAR.
    Returns a set of pair codes without '=X' (e.g., 'EURUSD').
    """
    aligned = set()
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_map = {executor.submit(mtf.analyze_pair_mtf, pair): pair for pair in mtf.PAIRS}
        for future in as_completed(future_map):
            pair = future_map[future]
            try:
                res = future.result()
            except Exception:
                continue
            if not res:
                continue
            align = res.get("alignment_status")
            global_status = res.get("global_status")
            if align in ("BULL", "BEAR") and global_status == align:
                aligned.add(normalize_pair_code(pair))
    return aligned


def main():
    if load_dotenv:
        load_dotenv()
    h1_candles = 80
    d1_candles = 200

    # Pre-fetch all symbols (pairs + indices) once
    all_pair_symbols = {f"OANDA:{p}" for p in PAIR_LIST}
    all_index_symbols = {v for v in INDICES.values()}

    data_cache = {}
    for sym in sorted(all_pair_symbols | all_index_symbols):
        data_cache[(sym, "60")] = fetch_tv_ohlc(sym, "60", h1_candles)
        data_cache[(sym, "D")] = fetch_tv_ohlc(sym, "D", d1_candles)

    # Precompute index states
    idx_bear = {}
    for cur, idx_sym in INDICES.items():
        h1 = data_cache.get((idx_sym, "60"))
        d1 = data_cache.get((idx_sym, "D"))
        idx_bear[INDEX_MAP[cur]] = is_below_8_emas(h1, d1)

    # Minority index status
    bull_indices = [k for k, v in idx_bear.items() if v is False]
    bear_indices = [k for k, v in idx_bear.items() if v is True]
    index_to_ccy = {v: k for k, v in INDEX_MAP.items()}
    if len(bull_indices) == 0 and len(bear_indices) == 0:
        index_status_msg = "INDEX STATUS: NO DATA"
    elif len(bull_indices) == 0:
        index_status_msg = "INDEX STATUS: ALL INDEX BEAR"
    elif len(bear_indices) == 0:
        index_status_msg = "INDEX STATUS: ALL INDEX BULL"
    else:
        if len(bull_indices) < len(bear_indices):
            ccy = ", ".join(sorted(index_to_ccy.get(i, i) for i in bull_indices))
            index_status_msg = "MINOR 🟢 : " + ccy
        elif len(bear_indices) < len(bull_indices):
            ccy = ", ".join(sorted(index_to_ccy.get(i, i) for i in bear_indices))
            index_status_msg = "MINOR 🔴 : " + ccy
        else:
            index_status_msg = "INDEX STATUS: NO MINORITY (MIXED)"

    print(index_status_msg)

    screener_rows = []

    for cur in CURRENCIES:
        pairs = build_pairs_for_currency(cur)
        pair_chg = {}
        pair_dist = {}
        pair_best = {}
        pair_index = {}
        for info in pairs:
            p = info["pair"]
            sym = info["symbol"]
            h1 = data_cache.get((sym, "60"))
            d1 = data_cache.get((sym, "D"))
            chg = daily_chg_pct(h1, d1)
            dist = dist_from_ema20(h1, d1)
            best = safe_add(chg) * abs(safe_add(dist))
            if info["invert"]:
                best = -best
            pair_chg[p] = chg
            pair_dist[p] = dist
            pair_best[p] = best
            pair_index[p] = info["index"]

        avg_chg = sum(safe_add(v) for v in pair_chg.values())
        cnt_chg = sum(1 for v in pair_chg.values() if v is not None and not pd.isna(v))
        avg_chg = avg_chg / cnt_chg if cnt_chg else None

        avg_dist = sum(safe_add(v) for v in pair_dist.values())
        cnt_dist = sum(1 for v in pair_dist.values() if v is not None and not pd.isna(v))
        avg_dist = avg_dist / cnt_dist if cnt_dist else None

        base_index = INDEX_MAP[cur]
        base_bear = idx_bear.get(base_index)

        require_avgdist_buy = True
        require_avgdist_sell = True
        decision_buy = (avg_chg is not None and avg_chg > 0) and (base_bear is True) and (
            (not require_avgdist_buy) or (avg_dist is not None and avg_dist > 0)
        )
        decision_sell = (avg_chg is not None and avg_chg < 0) and (base_bear is False) and (
            (not require_avgdist_sell) or (avg_dist is not None and avg_dist < 0)
        )

        sort_asc = decision_sell

        rows = []
        for p in pair_chg.keys():
            idx_name = pair_index[p]
            rows.append(
                {
                    "pair": p,
                    "chg_pct": pair_chg[p],
                    "index": idx_name,
                    "index_bear": idx_bear.get(idx_name),
                    "dist_ema20": pair_dist[p],
                    "best": pair_best[p],
                }
            )

        def sort_key(r):
            if r["best"] is None or pd.isna(r["best"]):
                return 1e9 if sort_asc else -1e9
            return r["best"]

        rows.sort(key=sort_key, reverse=not sort_asc)

        # Screener: only BUY/SELL
        decision_state = "BUY" if decision_buy else "SELL" if decision_sell else "NEUTRE"
        if decision_state in ("BUY", "SELL"):
            desired_bear = False if decision_state == "BUY" else True

            # Pair whose index matches desired state (BULL for BUY, BEAR for SELL)
            idx_match_pair = None
            idx_match_abs = -1.0
            for r in rows:
                if r["index_bear"] is None:
                    continue
                if r["index_bear"] == desired_bear:
                    chg = r["chg_pct"]
                    if chg is None or pd.isna(chg):
                        continue
                    if abs(chg) > idx_match_abs:
                        idx_match_abs = abs(chg)
                        idx_match_pair = r

            # Pair with max ABS(BEST)
            best_abs_pair = None
            best_abs_val = -1.0
            for r in rows:
                v = r["best"]
                if v is None or pd.isna(v):
                    continue
                if abs(v) > best_abs_val:
                    best_abs_val = abs(v)
                    best_abs_pair = r

            # Collect screener rows
            if idx_match_pair:
                screener_rows.append(
                    {
                        "currency": cur,
                        "decision": decision_state,
                        "reason": "INDEX",
                        "pair": idx_match_pair["pair"],
                        "chg_pct": idx_match_pair["chg_pct"],
                        "best": idx_match_pair["best"],
                        "index": idx_match_pair["index"],
                        "index_bear": idx_match_pair["index_bear"],
                    }
                )
            if best_abs_pair:
                screener_rows.append(
                    {
                        "currency": cur,
                        "decision": decision_state,
                        "reason": "BEST_ABS",
                        "pair": best_abs_pair["pair"],
                        "chg_pct": best_abs_pair["chg_pct"],
                        "best": best_abs_pair["best"],
                        "index": best_abs_pair["index"],
                        "index_bear": best_abs_pair["index_bear"],
                    }
                )

    if screener_rows:
        # Deduplicate by pair+reason+currency
        seen = set()
        unique_rows = []
        for r in screener_rows:
            key = (r["currency"], r["pair"], r["reason"])
            if key in seen:
                continue
            seen.add(key)
            unique_rows.append(r)

        unique_rows.sort(
            key=lambda r: 0.0 if r["chg_pct"] is None or pd.isna(r["chg_pct"]) else abs(r["chg_pct"]),
            reverse=True,
        )

        # Filter: only |CHG% (D)| > 0.2
        filtered_rows = [
            r
            for r in unique_rows
            if r["chg_pct"] is not None and not pd.isna(r["chg_pct"]) and abs(r["chg_pct"]) > 0.01
        ]

        if not filtered_rows:
            print("No pairs with |CHG% (D)| > 0.2")
            token = os.getenv("TELEGRAM_BOT_TOKEN")
            chat_id = os.getenv("TELEGRAM_CHAT_ID")
            if token and chat_id:
                try:
                    url = f"https://api.telegram.org/bot{token}/sendMessage"
                    requests.post(
                        url,
                        json={"chat_id": chat_id, "text": "INDEX_FX SCREENER\nNO DEAL 😞"},
                        timeout=10,
                    )
                except Exception:
                    pass
            return

        # Telegram send (dedup by pair)
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat_id and filtered_rows:
            mtf_pairs = mtf_aligned_pairs()
            pair_seen = set()
            lines = ["SCREENER FX", index_status_msg]
            for r in filtered_rows:
                pair = r["pair"]
                if pair in pair_seen:
                    continue
                pair_seen.add(pair)
                chg = r["chg_pct"]
                if chg is None or pd.isna(chg):
                    dot = "⚪"
                    chg_txt = "na"
                else:
                    dot = "🟢" if chg > 0 else "🔴" if chg < 0 else "⚪"
                    chg_txt = f"{chg:+.2f}%"
                fire = " 🔥" if pair in mtf_pairs else ""
                lines.append(f"{dot} {pair} ({chg_txt}){fire}")
            msg = "\n".join(lines)
            try:
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)
            except Exception:
                pass
        else:
            print("Telegram not sent (missing credentials or no rows).")
    else:
        print("No screener rows (no BUY/SELL decisions).")


if __name__ == "__main__":
    main()
