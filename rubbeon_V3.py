#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rubbeon_V3 logic in Python using TradingView socket data only (no cache).
"""

import argparse
import importlib
import os
import sys
import time
import json
import random
import string
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed


def install_and_import(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        return importlib.import_module(import_name)
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name)


pd = install_and_import("pandas")
np = install_and_import("numpy")
websocket = install_and_import("websocket-client", "websocket")
from websocket import create_connection
requests = install_and_import("requests")
dotenv = install_and_import("python-dotenv", "dotenv")

EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]
PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM = 0.1, 0.1, 0.2
ICON_BULL = "🟢"
ICON_BEAR = "🔴"
ICON_ALERT = "⚠️"
H1_CANDLES_DEFAULT = 2000
D1_CANDLES_DEFAULT = 500
H1_CANDLES_MAX = 5000
D1_CANDLES_MAX = 900
PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
    "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
    "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPCHF=X",
    "AUDNZD=X", "AUDCAD=X", "AUDCHF=X",
    "NZDCAD=X", "NZDCHF=X",
    "CADCHF=X",
]



# PAIRS = [
#     "EURUSD=X",  "USDJPY=X", "EURGBP=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", 
#     "CADJPY=X", "CHFJPY=X","EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
    
# ]





dotenv.load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
ASIAN_TZ = "Asia/Tokyo"
ASIAN_SESSION_START_HOUR = 9
ASIAN_SESSION_END_HOUR = 15
REVERSAL_RESET_HOUR = int(os.getenv("REVERSAL_RESET_HOUR", "22"))
REVERSAL_TZ = os.getenv("REVERSAL_TZ", "Europe/Paris")
PERSISTENCE_MIN_RUNNER = float(os.getenv("PERSISTENCE_MIN_RUNNER", "0.2"))
RUNNER_MIN_ABS = float(os.getenv("RUNNER_MIN_ABS", "0.3"))


def generate_session_id():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"


def fetch_data_tv(pair_yahoo, interval_code, n_candles=200):
    clean_pair = pair_yahoo.replace("=X", "")
    tv_pair = f"OANDA:{clean_pair}"
    tv_interval = {"1d": "D", "1h": "60"}.get(interval_code, "D")
    ws, extracted_df = None, None
    try:
        headers = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}
        ws = create_connection("wss://prodata.tradingview.com/socket.io/websocket", header=headers, timeout=10)
        session_id = generate_session_id()
        ws.send(create_message("chart_create_session", [session_id, ""]))
        ws.send(create_message("resolve_symbol", [session_id, "sds_sym_1", f"={{\"symbol\":\"{tv_pair}\",\"adjustment\":\"splits\",\"session\":\"regular\"}}"]))
        ws.send(create_message("create_series", [session_id, "sds_1", "s1", "sds_sym_1", tv_interval, n_candles, ""]))
        start_t = time.time()
        while time.time() - start_t < 8:
            try:
                res = ws.recv()
                if '"s":[' in res:
                    start = res.find('"s":[')
                    end = res.find('"ns":', res.find('"s":['))
                    if start != -1 and end != -1:
                        extract_end = end - 1
                        while res[extract_end] not in [",", "}"]:
                            extract_end -= 1
                        data = json.loads(res[start + 4 : extract_end])
                        fdata = [item["v"] for item in data]
                        extracted_df = pd.DataFrame(fdata, columns=["timestamp", "open", "high", "low", "close", "volume"])
                        extracted_df["datetime"] = pd.to_datetime(extracted_df["timestamp"], unit="s", utc=True)
                        extracted_df.set_index("datetime", inplace=True)
                        extracted_df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}, inplace=True)
                        extracted_df.drop(columns=["timestamp"], inplace=True)
                if "series_completed" in res:
                    break
            except Exception:
                pass
        ws.close()
        return extracted_df
    except Exception:
        if ws:
            try:
                ws.close()
            except Exception:
                pass
        return None



TRACKING_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rubbeon_state.json")


def load_tracking_state():
    if not os.path.exists(TRACKING_FILE):
        return {}
    try:
        with open(TRACKING_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_tracking_state(
    top5_items,
    reversal_pairs=None,
    reversal_day=None,
    top5_counts=None,
    run_count=None,
    persistence_day=None,
    exited_top5_pairs=None,
    top5_last=None,
):
    """
    Saves the list of dictionaries (the TOP 5 items) to JSON.
    We only need 'pair', 'aligned_state', 'daily_change_pct'.
    """
    state = {}
    timestamp = time.time()
    state["timestamp"] = timestamp
    
    # Store minimal info needed for comparison
    pairs_data = {}
    for item in top5_items:
        pairs_data[item["pair"]] = {
            "aligned_state": item["aligned_state"],
            "daily_change_pct": item.get("daily_change_pct", 0.0)
        }
    state["pairs"] = pairs_data
    state["reversal_pairs"] = sorted(reversal_pairs) if reversal_pairs else []
    state["reversal_day"] = reversal_day
    state["top5_counts"] = top5_counts or {}
    state["run_count"] = run_count or 0
    state["persistence_day"] = persistence_day
    state["exited_top5_pairs"] = sorted(exited_top5_pairs) if exited_top5_pairs else []
    state["top5_last"] = sorted(top5_last) if top5_last else []
    
    try:
        with open(TRACKING_FILE, "w") as f:
            json.dump(state, f, indent=4)
        print(f"Tracking state saved to {TRACKING_FILE}")
    except Exception as e:
        print(f"Error saving tracking state: {e}")


def check_runner_changes(previous_state, current_results):
    """
    Compare current results with previous state.
    Returns (alerts, new_reversal_pairs).
    """
    alerts = []
    new_reversals = set()
    if not previous_state or "pairs" not in previous_state:
        return alerts, new_reversals

    watched_pairs = previous_state["pairs"]
    
    # Map current results for easy lookup
    current_map = {res["pair"]: res for res in current_results}

    for pair, old_data in watched_pairs.items():
        if pair not in current_map:
            continue
            
        new_res = current_map[pair]
        old_runner = old_data.get("daily_change_pct", 0.0)
        new_runner = new_res.get("daily_change_pct", 0.0)
        
        # Check for sign change
        # We only care if it was significantly positive/negative and flipped
        if not np.isfinite(old_runner) or not np.isfinite(new_runner):
            continue
            
        # Logic: Sign flip detection
        # From + to -
        if old_runner > 0 and new_runner < 0:
            alerts.append(f"{ICON_ALERT} REVERSAL {format_pair_name(pair)}: {old_runner:+.2f}% -> {new_runner:+.2f}%")
            new_reversals.add(pair)
        # From - to +
        elif old_runner < 0 and new_runner > 0:
            alerts.append(f"{ICON_ALERT} REVERSAL {format_pair_name(pair)}: {old_runner:+.2f}% -> {new_runner:+.2f}%")
            new_reversals.add(pair)
            
    return alerts, new_reversals


def get_reversal_day(now_ts):
    try:
        tz = ZoneInfo(REVERSAL_TZ)
    except Exception:
        tz = ZoneInfo("UTC")
    local_dt = datetime.fromtimestamp(now_ts, tz=tz)
    if local_dt.hour < REVERSAL_RESET_HOUR:
        local_dt = local_dt - timedelta(days=1)
    return local_dt.date().isoformat()


def get_top5_persistence_pairs(top5_counts, results, allowed_pairs=None, min_abs_runner=PERSISTENCE_MIN_RUNNER):
    if not top5_counts:
        return set()
    current_map = {res["pair"]: res for res in results}
    pairs = set()
    for pair, count in top5_counts.items():
        if count <= 0:
            continue
        if allowed_pairs is not None and pair not in allowed_pairs:
            continue
        res = current_map.get(pair, {})
        runner = res.get("daily_change_pct")
        abs_runner = abs(runner) if runner is not None and np.isfinite(runner) else -1.0
        if abs_runner < min_abs_runner:
            continue
        pairs.add(pair)
    return pairs


def build_top5_persistence_section(top5_counts, run_count, results, allowed_pairs=None, best_pair=None, min_abs_runner=PERSISTENCE_MIN_RUNNER):
    if not top5_counts or run_count <= 0:
        return ""
    current_map = {res["pair"]: res for res in results}
    items = []
    for pair, count in top5_counts.items():
        if count <= 0:
            continue
        if allowed_pairs is not None and pair not in allowed_pairs:
            continue
        res = current_map.get(pair, {})
        runner = res.get("daily_change_pct")
        abs_runner = abs(runner) if runner is not None and np.isfinite(runner) else -1.0
        if abs_runner < min_abs_runner:
            continue
        items.append((abs_runner, runner, count, pair))
    if not items:
        return ""
    items.sort(key=lambda x: (-x[0], x[3]))
    lines = ["TOP5 PERSISTENCE"]
    for abs_runner, runner, count, pair in items:
        if runner is None or not np.isfinite(runner):
            icon = "⚪"
            runner_text = "NA"
        else:
            icon = ICON_BULL if runner > 0 else ICON_BEAR
            runner_text = f"{runner:+.2f}%"
        best_marker = " 🔥" if best_pair and pair == best_pair else ""
        lines.append(f"{icon} {format_pair_name(pair)} ({runner_text}){best_marker}")
    return "\n".join(lines)


def build_reversal_section(reversal_pairs, results):
    if not reversal_pairs:
        return ""
    current_map = {res["pair"]: res for res in results}
    lines = ["REVERSAL (persisted)"]
    for pair in sorted(reversal_pairs):
        item = current_map.get(pair)
        pct = item.get("daily_change_pct") if item else None
        pct_text = "NA" if pct is None or not np.isfinite(pct) else f"{pct:+.2f}%"
        lines.append(f"{ICON_ALERT} {format_pair_name(pair)} ({pct_text})")
    return "\n".join(lines)


def build_exited_top5_section(exited_pairs, results):
    if not exited_pairs:
        return ""
    current_map = {res["pair"]: res for res in results}
    lines = ["TOP5 EXIT (no reversal)"]
    for pair in sorted(exited_pairs):
        item = current_map.get(pair)
        pct = item.get("daily_change_pct") if item else None
        pct_text = "NA" if pct is None or not np.isfinite(pct) else f"{pct:+.2f}%"
        lines.append(f"{format_pair_name(pair)} ({pct_text})")
    return "\n".join(lines)


def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
        return response.json().get("ok", False)
    except Exception:
        return False


def format_pair_name(pair):
    return pair.replace("=X", "")


def build_telegram_message(aligned_items):
    lines = ["RUBBEON"]
    for item in aligned_items:
        icon = ICON_BULL if item["aligned_state"] == "BULL" else ICON_BEAR
        runner = item.get("daily_change_pct")
        runner_text = "NA" if runner is None or not np.isfinite(runner) else f"{runner:+.2f}%"
        lines.append(f"{icon} {format_pair_name(item['pair'])} ({runner_text})")
    return "\n".join(lines)


def compute_currency_strength(items):
    strength = {}
    for item in items:
        pair = format_pair_name(item.get("pair", ""))
        if len(pair) != 6:
            continue
        pct = item.get("daily_change_pct")
        if not np.isfinite(pct):
            continue
        base = pair[:3]
        quote = pair[3:]
        strength[base] = strength.get(base, 0.0) + pct
        strength[quote] = strength.get(quote, 0.0) - pct
    return strength


def build_strength_message(strength, top_n=3):
    if not strength:
        return ""
    ordered = sorted(strength.items(), key=lambda x: x[1], reverse=True)
    top = ordered[:top_n]
    bottom = ordered[-top_n:][::-1]
    lines = ["STRENGTH"]
    lines.append("Top:")
    for ccy, score in top:
        lines.append(f"{ccy}: {score:+.2f}")
    lines.append("Weak:")
    for ccy, score in bottom:
        lines.append(f"{ccy}: {score:+.2f}")
    return "\n".join(lines)


def find_best_trade_pair(strength, pairs):
    if not strength:
        return None, None
    ordered = sorted(strength.items(), key=lambda x: x[1], reverse=True)
    top_ccy = ordered[0][0]
    weak_ccy = ordered[-1][0]
    candidate_weak_top = f"{weak_ccy}{top_ccy}=X"
    if candidate_weak_top in pairs:
        return candidate_weak_top, "BEAR"
    candidate_top_weak = f"{top_ccy}{weak_ccy}=X"
    if candidate_top_weak in pairs:
        return candidate_top_weak, "BULL"
    return None, None


def find_pair_daily_pct(results, pair):
    for item in results:
        if item.get("pair") == pair:
            pct = item.get("daily_change_pct")
            return pct if np.isfinite(pct) else None
    return None


def normalize_index(df):
    if df is None or df.empty:
        return df
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df


def calculate_psar_series(df, start, increment, maximum):
    if df is None or len(df) < 3:
        return None
    highs = df["High"].values
    lows = df["Low"].values
    closes = df["Close"].values
    psar = np.zeros(len(df))
    bull = closes[1] >= closes[0]
    af = start
    ep = highs[0] if bull else lows[0]
    psar[0] = lows[0] if bull else highs[0]
    for i in range(1, len(df)):
        psar[i] = psar[i - 1] + af * (ep - psar[i - 1])
        if bull:
            if lows[i] < psar[i]:
                bull, psar[i], ep, af = False, ep, lows[i], start
            else:
                if highs[i] > ep:
                    ep, af = highs[i], min(af + increment, maximum)
                psar[i] = min(psar[i], lows[i - 1], lows[i - 2]) if i >= 2 else min(psar[i], lows[i - 1])
        else:
            if highs[i] > psar[i]:
                bull, psar[i], ep, af = True, ep, highs[i], start
            else:
                if lows[i] < ep:
                    ep, af = lows[i], min(af + increment, maximum)
                psar[i] = max(psar[i], highs[i - 1], highs[i - 2]) if i >= 2 else max(psar[i], highs[i - 1])
    return psar


def compute_ema_series(close_series, lengths):
    return {length: close_series.ewm(span=length, adjust=False).mean() for length in lengths}


def compute_valuewhen_series(closes, psar):
    bull_vw = np.full(len(closes), np.nan)
    bear_vw = np.full(len(closes), np.nan)
    last_bull = np.nan
    last_bear = np.nan
    for i in range(1, len(closes)):
        bull_cross = closes[i] > psar[i] and closes[i - 1] <= psar[i - 1]
        bear_cross = closes[i] < psar[i] and closes[i - 1] >= psar[i - 1]
        if bull_cross:
            last_bull = psar[i]
        if bear_cross:
            last_bear = psar[i]
        bull_vw[i] = last_bull
        bear_vw[i] = last_bear
    return bull_vw, bear_vw


def compute_asian_session_state(df_h1):
    if df_h1 is None or df_h1.empty:
        return "NONE", np.nan, np.nan, np.nan, None
    try:
        local_index = df_h1.index.tz_convert(ASIAN_TZ)
    except Exception:
        local_index = df_h1.index.tz_localize("UTC").tz_convert(ASIAN_TZ)
    hours = local_index.hour
    session_mask = (hours >= ASIAN_SESSION_START_HOUR) & (hours < ASIAN_SESSION_END_HOUR)
    session_df = df_h1.loc[session_mask].copy()
    if session_df.empty:
        return "NONE", np.nan, np.nan, np.nan, None
    session_df["local_date"] = local_index[session_mask].date
    last_date = session_df["local_date"].iloc[-1]
    last_session = session_df[session_df["local_date"] == last_date]
    asian_high = last_session["High"].max()
    asian_low = last_session["Low"].min()
    asian_mid = (asian_high + asian_low) / 2.0
    close_now = df_h1["Close"].iloc[-1]
    if close_now > asian_mid:
        asia_state = "BULL"
    elif close_now < asian_mid:
        asia_state = "BEAR"
    else:
        asia_state = "NEUTRE"
    return asia_state, asian_mid, asian_high, asian_low, last_date


def analyze_pair(pair, h1_candles=H1_CANDLES_DEFAULT, d1_candles=D1_CANDLES_DEFAULT):
    current_h1 = h1_candles
    current_d1 = d1_candles
    while True:
        df_h1 = fetch_data_tv(pair, "1h", n_candles=current_h1)
        df_d = fetch_data_tv(pair, "1d", n_candles=current_d1)

        if df_h1 is None or df_d is None:
            return None

        df_h1 = normalize_index(df_h1)
        df_d = normalize_index(df_d)

        if len(df_h1) < max(EMA_LENGTHS) + 3 or len(df_d) < max(EMA_LENGTHS) + 2:
            h1_retry = min(current_h1 * 2, H1_CANDLES_MAX)
            d1_retry = min(current_d1 * 2, D1_CANDLES_MAX)
            if h1_retry == current_h1 and d1_retry == current_d1:
                return None
            current_h1 = h1_retry
            current_d1 = d1_retry
            continue

        psar_h1 = calculate_psar_series(df_h1, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        if psar_h1 is None:
            return None

        ema_h1 = compute_ema_series(df_h1["Close"], EMA_LENGTHS)
        ema_d = compute_ema_series(df_d["Close"], EMA_LENGTHS)

        ema_max_h1 = pd.concat([ema_h1[l] for l in EMA_LENGTHS], axis=1).max(axis=1)
        ema_min_h1 = pd.concat([ema_h1[l] for l in EMA_LENGTHS], axis=1).min(axis=1)

        ema_max_d = pd.concat([ema_d[l] for l in EMA_LENGTHS], axis=1).max(axis=1)
        ema_min_d = pd.concat([ema_d[l] for l in EMA_LENGTHS], axis=1).min(axis=1)

        daily_df = pd.DataFrame({
            "close_d": df_d["Close"],
        })
        daily_df["close_prev_d"] = daily_df["close_d"].shift(1)
        for length in EMA_LENGTHS:
            daily_df[f"ema_prev_{length}"] = ema_d[length].shift(1)

        daily_on_h1 = daily_df.reindex(df_h1.index, method="ffill")

        ema_rt = []
        for length in EMA_LENGTHS:
            alpha = 2.0 / (length + 1)
            ema_prev = daily_on_h1[f"ema_prev_{length}"]
            ema_rt.append(ema_prev + alpha * (df_h1["Close"] - ema_prev))
        ema_max_d_rt = pd.concat(ema_rt, axis=1).max(axis=1)
        ema_min_d_rt = pd.concat(ema_rt, axis=1).min(axis=1)

        daily_above_all = df_h1["Close"] > ema_max_d_rt
        daily_below_all = df_h1["Close"] < ema_min_d_rt

        daily_change_pct = (df_h1["Close"] - daily_on_h1["close_prev_d"]) / daily_on_h1["close_prev_d"] * 100.0

        bull_vw0_h1, bear_vw0_h1 = compute_valuewhen_series(df_h1["Close"].values, psar_h1)

        if (np.isnan(bull_vw0_h1[-1]) or np.isnan(bear_vw0_h1[-1])) and current_h1 < H1_CANDLES_MAX:
            current_h1 = min(current_h1 * 2, H1_CANDLES_MAX)
            continue
        break

    hourly_bear_above_all = bear_vw0_h1 > ema_max_h1.values
    hourly_bull_below_all = bull_vw0_h1 < ema_min_h1.values

    bull_bg = (
        daily_above_all.values
        & hourly_bear_above_all
        & hourly_bull_below_all
        & (daily_change_pct.values > 0)
    )
    bear_bg = (
        daily_below_all.values
        & hourly_bear_above_all
        & hourly_bull_below_all
        & (daily_change_pct.values < 0)
    )

    last_bg_state = 0
    bear_min_since = np.nan
    bull_max_since = np.nan
    for i in range(len(df_h1)):
        if bull_bg[i]:
            last_bg_state = 1
            bull_max_since = bull_vw0_h1[i]
        elif bear_bg[i]:
            last_bg_state = -1
            bear_min_since = bear_vw0_h1[i]

        if last_bg_state == -1 and not np.isnan(bear_vw0_h1[i]):
            bear_min_since = bear_vw0_h1[i] if np.isnan(bear_min_since) else min(bear_min_since, bear_vw0_h1[i])
        if last_bg_state == 1 and not np.isnan(bull_vw0_h1[i]):
            bull_max_since = bull_vw0_h1[i] if np.isnan(bull_max_since) else max(bull_max_since, bull_vw0_h1[i])

    close_now = df_h1["Close"].iloc[-1]
    mom_state = "NONE"
    if last_bg_state == -1 and not np.isnan(bear_min_since):
        mom_state = "BEAR" if close_now < bear_min_since else "BULL"
    elif last_bg_state == 1 and not np.isnan(bull_max_since):
        mom_state = "BULL" if close_now > bull_max_since else "BEAR"

    asia_state, asian_mid, asian_high, asian_low, asian_date = compute_asian_session_state(df_h1)

    aligned_state = None
    daily_change_last = daily_change_pct.iloc[-1]
    runner_ok = np.isfinite(daily_change_last) and abs(daily_change_last) > RUNNER_MIN_ABS
    if last_bg_state == 1:
        state_text = "BULL"
    elif last_bg_state == -1:
        state_text = "BEAR"
    else:
        state_text = "NONE"

    if not np.isfinite(daily_change_last) or not runner_ok:
        chg_state = "NEUTRE"
    else:
        chg_state = "BULL" if daily_change_last >= 0 else "BEAR"

    if state_text == "BULL" and mom_state == "BULL" and chg_state == "BULL" and asia_state == "BULL":
        aligned_state = "BULL"
    elif state_text == "BEAR" and mom_state == "BEAR" and chg_state == "BEAR" and asia_state == "BEAR":
        aligned_state = "BEAR"

    return {
        "pair": pair,
        "source": "TV",
        "close_now": close_now,
        "daily_change_pct": daily_change_pct.iloc[-1],
        "daily_above_all": bool(daily_above_all.iloc[-1]),
        "daily_below_all": bool(daily_below_all.iloc[-1]),
        "bear_vw0_h1": bear_vw0_h1[-1],
        "bull_vw0_h1": bull_vw0_h1[-1],
        "ema_max_h1": ema_max_h1.iloc[-1],
        "ema_min_h1": ema_min_h1.iloc[-1],
        "bull_bg": bool(bull_bg[-1]),
        "bear_bg": bool(bear_bg[-1]),
        "last_bg_state": last_bg_state,
        "bear_min_since": bear_min_since,
        "bull_max_since": bull_max_since,
        "mom_state": mom_state,
        "asia_state": asia_state,
        "asian_mid": asian_mid,
        "asian_high": asian_high,
        "asian_low": asian_low,
        "asian_date": asian_date,
        "aligned_state": aligned_state,
    }


def print_result(result):
    print("=" * 60)
    print(f"Pair: {result['pair']}")
    print(f"Source: {result['source']}")
    print(f"Close (H1): {result['close_now']:.5f}")
    print(f"Daily % change: {result['daily_change_pct']:.2f}%")
    print(f"Daily above all EMA: {result['daily_above_all']}")
    print(f"Daily below all EMA: {result['daily_below_all']}")
    print(f"bear_vw0_H1: {result['bear_vw0_h1']}")
    print(f"bull_vw0_H1: {result['bull_vw0_h1']}")
    print(f"EMA max H1: {result['ema_max_h1']:.5f}")
    print(f"EMA min H1: {result['ema_min_h1']:.5f}")
    print(f"BG bullish: {result['bull_bg']}")
    print(f"BG bearish: {result['bear_bg']}")
    print(f"Last BG state: {result['last_bg_state']}")
    print(f"bear_min_since: {result['bear_min_since']}")
    print(f"bull_max_since: {result['bull_max_since']}")
    print(f"MOM: {result['mom_state']}")
    print(f"ASIA: {result['asia_state']} (mid={result['asian_mid']}, high={result['asian_high']}, low={result['asian_low']})")
    print("=" * 60)


def main():
    global RUNNER_MIN_ABS
    parser = argparse.ArgumentParser(description="Rubbeon_V3 analysis (H1/D1) using TradingView socket only.")
    parser.add_argument("--pair", help="Yahoo pair symbol, e.g. EURUSD=X")
    parser.add_argument("--h1-candles", type=int, default=H1_CANDLES_DEFAULT, help="Number of H1 candles to fetch")
    parser.add_argument("--d1-candles", type=int, default=D1_CANDLES_DEFAULT, help="Number of D1 candles to fetch")
    parser.add_argument("--workers", type=int, default=6, help="Max workers for multi-pair mode")
    parser.add_argument("--runner-min-abs", type=float, default=RUNNER_MIN_ABS, help="Min abs daily % change for alignment")
    args = parser.parse_args()

    if os.getenv("GITHUB_ACTIONS", "").lower() == "true":
        args.workers = min(args.workers, 2)
    RUNNER_MIN_ABS = args.runner_min_abs

    pairs = [args.pair] if args.pair else PAIRS
    results = []
    errors = []
    
    # Load previous tracking state
    previous_state = load_tracking_state()
    
    start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(
                analyze_pair,
                pair,
                h1_candles=args.h1_candles,
                d1_candles=args.d1_candles,
            ): pair
            for pair in pairs
        }
        for future in as_completed(future_map):
            pair = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                errors.append((pair, str(exc)))
                continue
            if result:
                results.append(result)
            else:
                errors.append((pair, "No data or insufficient history"))

    results.sort(key=lambda x: x["pair"])
    for result in results:
        print_result(result)

    print(f"Pairs analyzed: {len(pairs)} | OK: {len(results)} | Errors: {len(errors)}")
    print(f"Elapsed: {time.time() - start:.2f}s")
    aligned = [r for r in results if r["aligned_state"]]
    aligned.sort(
        key=lambda x: abs(x["daily_change_pct"]) if np.isfinite(x.get("daily_change_pct")) else -1,
        reverse=True,
    )
    aligned_pairs = {item["pair"] for item in aligned}
    now_ts = time.time()
    reversal_day = get_reversal_day(now_ts)
    persistence_day = reversal_day
    previous_reversal_day = previous_state.get("reversal_day")
    persisted_reversals = set(previous_state.get("reversal_pairs", []))
    if previous_reversal_day != reversal_day:
        persisted_reversals = set()

    previous_persistence_day = previous_state.get("persistence_day")
    top5_counts = dict(previous_state.get("top5_counts", {}))
    run_count = int(previous_state.get("run_count", 0) or 0)
    exited_top5_pairs = set(previous_state.get("exited_top5_pairs", []))
    previous_top5 = set(previous_state.get("top5_last", []))
    if previous_persistence_day != persistence_day:
        top5_counts = {}
        run_count = 0
        exited_top5_pairs = set()
        previous_top5 = set()
    run_count += 1

    tracking_alerts, new_reversals = check_runner_changes(previous_state, results)
    persisted_reversals |= new_reversals

    strength_all = compute_currency_strength(results)
    best_pair = None
    if strength_all:
        print("Strength (all pairs):")
        for ccy, score in sorted(strength_all.items(), key=lambda x: x[1], reverse=True):
            print(f"- {ccy}: {score:+.2f}")
        best_pair, best_dir = find_best_trade_pair(strength_all, PAIRS)
        if best_pair:
            best_icon = ICON_BEAR if best_dir == "BEAR" else ICON_BULL
            print(f"BEST TRADE: {best_icon} {format_pair_name(best_pair)}")
    tg_message = None
    top5_pairs = set()
    if aligned:
        print("Aligned (BG + MOM + Daily % + ASIA):")
        for item in aligned:
            print(f"- {item['pair']}: {item['aligned_state']}")
            
        # 1. Identify TOP 5
        top_5_items = aligned[:5]
        top5_pairs = {item["pair"] for item in top_5_items}
        for pair in top5_pairs:
            top5_counts[pair] = top5_counts.get(pair, 0) + 1
        persisted_reversals -= top5_pairs
        exited_top5_pairs -= top5_pairs
        exited_top5_pairs -= persisted_reversals
        exited_top5_pairs -= new_reversals
        newly_exited = previous_top5 - top5_pairs
        newly_exited -= persisted_reversals
        newly_exited -= new_reversals
        exited_top5_pairs |= newly_exited
        
        # 2. Build Standard Message (TOP 5)
        tg_message = build_telegram_message(top_5_items)
        
        # 3. Add Best Trade Section
        if strength_all:
            best_pair, best_dir = find_best_trade_pair(strength_all, PAIRS)
            if best_pair:
                best_icon = ICON_BEAR if best_dir == "BEAR" else ICON_BULL
                best_pct = find_pair_daily_pct(results, best_pair)
                best_pct_text = ""
                if best_pct is not None:
                    best_pct_text = f" ({best_pct:+.2f}%)"
                tg_message = f"{tg_message}\n\nBEST TRADE\n{best_icon} {format_pair_name(best_pair)}{best_pct_text}"
        
        if tracking_alerts:
            alert_section = "\n\nTRACKING ALERTS\n" + "\n".join(tracking_alerts)
            tg_message += alert_section
            print("Tracking Alerts generated.")

        persistence_pairs = get_top5_persistence_pairs(
            top5_counts,
            results,
            aligned_pairs,
        )
        reversal_section = build_reversal_section(persisted_reversals - persistence_pairs, results)
        if reversal_section:
            tg_message = f"{tg_message}\n\n{reversal_section}"

        exited_top5_section = build_exited_top5_section(exited_top5_pairs, results)
        if exited_top5_section:
            tg_message = f"{tg_message}\n\n{exited_top5_section}"

        top5_persistence_section = build_top5_persistence_section(
            top5_counts,
            run_count,
            results,
            aligned_pairs,
            best_pair,
        )
        if top5_persistence_section:
            tg_message = f"{top5_persistence_section}\n\n{tg_message}"

        # 5. Send Telegram
        if tg_message:
            sent = send_telegram_message(tg_message)
            if sent:
                print("Telegram: message sent.")
            else:
                print("Telegram: not sent (missing token/chat id or error).")
        
        # 6. Save NEW Top 5 + Best Trade for next time
        items_to_track = list(top_5_items)
        # Check if Best Trade exists and add it if not already in Top 5
        if strength_all:
            best_pair_name, _ = find_best_trade_pair(strength_all, PAIRS)
            if best_pair_name:
                # Find the result object in the full results list
                best_trade_item = next((r for r in results if r["pair"] == best_pair_name), None)
                if best_trade_item:
                    # Avoid duplicates
                    already_tracked = any(item["pair"] == best_pair_name for item in items_to_track)
                    if not already_tracked:
                        items_to_track.append(best_trade_item)

        save_tracking_state(
            items_to_track,
            persisted_reversals,
            reversal_day,
            top5_counts,
            run_count,
            persistence_day,
            exited_top5_pairs,
            top5_pairs,
        )
            
    else:
        # RUBBEON is empty: reset all tracking state and hide other sections.
        persisted_reversals = set()
        top5_counts = {}
        run_count = 0
        exited_top5_pairs = set()
        previous_top5 = set()
        top5_pairs = set()

        tg_message = "NO DEAL \U0001F61E"
        sent = send_telegram_message(tg_message)
        if sent:
            print("Telegram: message sent.")
        else:
            print("Telegram: not sent (missing token/chat id or error).")
        save_tracking_state(
            [],
            persisted_reversals,
            reversal_day,
            top5_counts,
            run_count,
            persistence_day,
            exited_top5_pairs,
            top5_pairs,
        )
    if errors:
        print("Errors:")
        for pair, message in errors:
            print(f"- {pair}: {message}")


if __name__ == "__main__":
    main()
