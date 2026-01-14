#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Annual_V2 - Combinaison de RUBBEON_V3 et ANNUAL_V1
Filtre RUBBEON (BG + MOM + Daily % + ASIA) + Validation ANNUAL (ratio >= 75%)
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

# ============================================================================
# CONFIGURATION
# ============================================================================
EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]
PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM = 0.1, 0.1, 0.2
ICON_BULL = "🟢"
ICON_BEAR = "🔴"
ICON_ALERT = "⚠️"
H1_CANDLES_DEFAULT = 2000
D1_CANDLES_DEFAULT = 500
H1_CANDLES_MAX = 5000
D1_CANDLES_MAX = 900
WARNING_RATIO_THRESHOLD = 75  # Validation ANNUAL: ratio >= 75%

PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
    "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
    "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPCHF=X",
    "AUDNZD=X", "AUDCAD=X", "AUDCHF=X",
    "NZDCAD=X", "NZDCHF=X",
    "CADCHF=X",
]

dotenv.load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# RUBBEON settings
ASIAN_TZ = "Asia/Tokyo"
ASIAN_SESSION_START_HOUR = 9
ASIAN_SESSION_END_HOUR = 15
REVERSAL_RESET_HOUR = int(os.getenv("REVERSAL_RESET_HOUR", "22"))
REVERSAL_TZ = os.getenv("REVERSAL_TZ", "Europe/Paris")
PERSISTENCE_MIN_RUNNER = float(os.getenv("PERSISTENCE_MIN_RUNNER", "0.2"))
RUNNER_MIN_ABS = float(os.getenv("RUNNER_MIN_ABS", "0.3"))

# File paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ANNUAL_DATA_FILE = os.path.join(SCRIPT_DIR, "annual_v2_data.json")
TRACKING_FILE = os.path.join(SCRIPT_DIR, "annual_v2_state.json")


# ============================================================================
# ANNUAL DATA PERSISTENCE
# ============================================================================
def load_annual_data():
    if not os.path.exists(ANNUAL_DATA_FILE):
        return {"year": datetime.now().year, "pairs": {}}
    try:
        with open(ANNUAL_DATA_FILE, "r") as f:
            data = json.load(f)
        if data.get("year") != datetime.now().year:
            return {"year": datetime.now().year, "pairs": {}}
        return data
    except Exception:
        return {"year": datetime.now().year, "pairs": {}}


def save_annual_data(data):
    try:
        with open(ANNUAL_DATA_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Erreur sauvegarde annual data: {e}")


def update_highest_pct(saved_data, pair, current_highest_pct):
    pair_key = pair.replace("=X", "")
    pair_data = saved_data["pairs"].get(pair_key, {})
    saved_highest = pair_data.get("highest_pct_ytd", float("-inf"))
    if current_highest_pct > saved_highest:
        pair_data["highest_pct_ytd"] = current_highest_pct
        saved_data["pairs"][pair_key] = pair_data
        return current_highest_pct
    return saved_highest


def update_lowest_pct(saved_data, pair, current_lowest_pct):
    pair_key = pair.replace("=X", "")
    pair_data = saved_data["pairs"].get(pair_key, {})
    saved_lowest = pair_data.get("lowest_pct_ytd", float("inf"))
    if current_lowest_pct < saved_lowest:
        pair_data["lowest_pct_ytd"] = current_lowest_pct
        saved_data["pairs"][pair_key] = pair_data
        return current_lowest_pct
    return saved_lowest


def update_direction(saved_data, pair, is_bullish):
    pair_key = pair.replace("=X", "")
    pair_data = saved_data["pairs"].get(pair_key, {})
    was_bullish = pair_data.get("was_bullish", None)
    direction_changed = False
    if was_bullish is not None and was_bullish != is_bullish:
        direction_changed = True
    pair_data["was_bullish"] = is_bullish
    saved_data["pairs"][pair_key] = pair_data
    return direction_changed


# ============================================================================
# RUBBEON TRACKING STATE
# ============================================================================
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
    state = {}
    timestamp = time.time()
    state["timestamp"] = timestamp
    pairs_data = {}
    for item in top5_items:
        pairs_data[item["pair"]] = {
            "aligned_state": item.get("aligned_state"),
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
    except Exception as e:
        print(f"Error saving tracking state: {e}")


def check_runner_changes(previous_state, current_results):
    alerts = []
    new_reversals = set()
    if not previous_state or "pairs" not in previous_state:
        return alerts, new_reversals
    watched_pairs = previous_state["pairs"]
    current_map = {res["pair"]: res for res in current_results}
    for pair, old_data in watched_pairs.items():
        if pair not in current_map:
            continue
        new_res = current_map[pair]
        old_runner = old_data.get("daily_change_pct", 0.0)
        new_runner = new_res.get("daily_change_pct", 0.0)
        if not np.isfinite(old_runner) or not np.isfinite(new_runner):
            continue
        if old_runner > 0 and new_runner < 0:
            alerts.append(f"{ICON_ALERT} REVERSAL {format_pair_name(pair)}: {old_runner:+.2f}% -> {new_runner:+.2f}%")
            new_reversals.add(pair)
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


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================
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


def normalize_index(df):
    if df is None or df.empty:
        return df
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df


# ============================================================================
# TECHNICAL INDICATORS
# ============================================================================
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


# ============================================================================
# ANNUAL CALCULATIONS
# ============================================================================
def get_annual_open(df_d):
    if df_d is None or df_d.empty:
        return np.nan
    current_year = datetime.now().year
    year_data = df_d[df_d.index.year == current_year]
    if year_data.empty:
        return np.nan
    return year_data["Open"].iloc[0]


def compute_highest_pct_ytd(df_d, annual_open):
    if df_d is None or df_d.empty or np.isnan(annual_open):
        return np.nan
    current_year = datetime.now().year
    year_data = df_d[df_d.index.year == current_year]
    if year_data.empty:
        return np.nan
    highest_close = year_data["High"].max()
    highest_pct = ((highest_close - annual_open) / annual_open) * 100
    return highest_pct


def compute_lowest_pct_ytd(df_d, annual_open):
    if df_d is None or df_d.empty or np.isnan(annual_open):
        return np.nan
    current_year = datetime.now().year
    year_data = df_d[df_d.index.year == current_year]
    if year_data.empty:
        return np.nan
    lowest_close = year_data["Low"].min()
    lowest_pct = ((lowest_close - annual_open) / annual_open) * 100
    return lowest_pct


# ============================================================================
# RUBBEON: ASIAN SESSION
# ============================================================================
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


# ============================================================================
# CURRENCY STRENGTH
# ============================================================================
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


def find_best_trade_pair_with_bg(strength, pairs, results):
    pair, direction = find_best_trade_pair(strength, pairs)
    if not pair:
        return None, None
    result_map = {res["pair"]: res for res in results}
    res = result_map.get(pair)
    if not res:
        return None, None
    if direction == "BULL" and res.get("bull_bg"):
        return pair, direction
    if direction == "BEAR" and res.get("bear_bg"):
        return pair, direction
    return None, None


def find_pair_daily_pct(results, pair):
    for item in results:
        if item.get("pair") == pair:
            pct = item.get("daily_change_pct")
            return pct if np.isfinite(pct) else None
    return None


# ============================================================================
# MAIN ANALYSIS (RUBBEON + ANNUAL COMBINED)
# ============================================================================
def analyze_pair(pair, h1_candles=H1_CANDLES_DEFAULT, d1_candles=D1_CANDLES_DEFAULT, annual_data=None):
    global RUNNER_MIN_ABS
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

        daily_df = pd.DataFrame({"close_d": df_d["Close"]})
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

    # RUBBEON logic
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

    # RUBBEON alignment
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

    # ANNUAL calculations
    annual_open = get_annual_open(df_d)
    highest_pct_ytd = compute_highest_pct_ytd(df_d, annual_open)
    lowest_pct_ytd = compute_lowest_pct_ytd(df_d, annual_open)
    
    if annual_data is not None and not np.isnan(highest_pct_ytd):
        highest_pct_ytd = update_highest_pct(annual_data, pair, highest_pct_ytd)
    if annual_data is not None and not np.isnan(lowest_pct_ytd):
        lowest_pct_ytd = update_lowest_pct(annual_data, pair, lowest_pct_ytd)

    annual_change_pct = ((close_now - annual_open) / annual_open * 100) if not np.isnan(annual_open) else np.nan
    
    # ANNUAL ratio calculation
    annual_ratio = np.nan
    is_annual_bullish = annual_change_pct >= 0 if not np.isnan(annual_change_pct) else True
    direction_changed = False
    if annual_data is not None:
        direction_changed = update_direction(annual_data, pair, is_annual_bullish)
    
    if is_annual_bullish:
        if not np.isnan(highest_pct_ytd) and not np.isnan(annual_change_pct) and highest_pct_ytd > 0:
            annual_ratio = (annual_change_pct / highest_pct_ytd) * 100
        extreme_pct = highest_pct_ytd
    else:
        if not np.isnan(lowest_pct_ytd) and not np.isnan(annual_change_pct) and lowest_pct_ytd < 0:
            annual_ratio = (annual_change_pct / lowest_pct_ytd) * 100
        extreme_pct = lowest_pct_ytd

    # ANNUAL validation: ratio >= WARNING_RATIO_THRESHOLD
    annual_valid = not np.isnan(annual_ratio) and annual_ratio >= WARNING_RATIO_THRESHOLD

    return {
        "pair": pair,
        "close_now": close_now,
        "daily_change_pct": daily_change_last,
        # RUBBEON fields
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
        "aligned_state": aligned_state,
        # ANNUAL fields
        "annual_open": annual_open,
        "annual_change_pct": annual_change_pct,
        "highest_pct_ytd": highest_pct_ytd,
        "lowest_pct_ytd": lowest_pct_ytd,
        "extreme_pct": extreme_pct,
        "annual_ratio": annual_ratio,
        "annual_valid": annual_valid,
        "direction_changed": direction_changed,
    }


# ============================================================================
# OUTPUT FUNCTIONS
# ============================================================================
def print_result(result):
    print("=" * 60)
    pair_name = format_pair_name(result['pair'])
    rubbeon_ok = "✓" if result.get("aligned_state") else "✗"
    annual_ok = "✓" if result.get("annual_valid") else "✗"
    print(f"Pair: {pair_name} | RUBBEON: {rubbeon_ok} | ANNUAL: {annual_ok}")
    print(f"Close: {result['close_now']:.5f}")
    print(f"Daily % change: {result['daily_change_pct']:.2f}%")
    print(f"Aligned State: {result.get('aligned_state', 'NONE')}")
    print(f"MOM: {result['mom_state']} | ASIA: {result['asia_state']}")
    ao = result.get('annual_open', np.nan)
    print(f"Annual Open: {ao:.5f}" if not np.isnan(ao) else "Annual Open: N/A")
    acp = result.get('annual_change_pct', np.nan)
    print(f"Annual Change: {acp:+.2f}%" if not np.isnan(acp) else "Annual Change: N/A")
    ratio = result.get('annual_ratio', np.nan)
    print(f"Annual Ratio: {ratio:.0f}% (threshold: {WARNING_RATIO_THRESHOLD}%)" if not np.isnan(ratio) else "Annual Ratio: N/A")
    print("=" * 60)


def build_telegram_message(validated_items):
    """Build Telegram message for pairs that pass BOTH RUBBEON and ANNUAL filters."""
    lines = ["ANNUAL V2"]
    if not validated_items:
        lines.append("NO DEAL 😞")
        return "\n".join(lines)
    
    # Sort by annual_ratio descending
    sorted_items = sorted(validated_items, key=lambda x: x.get("annual_ratio", 0), reverse=True)
    
    for item in sorted_items:
        aligned = item.get("aligned_state")
        icon = ICON_BULL if aligned == "BULL" else ICON_BEAR
        ratio = item.get("annual_ratio", np.nan)
        ratio_str = f"{ratio:.0f}%" if not np.isnan(ratio) else "N/A"
        direction_icon = " 🔄" if item.get("direction_changed", False) else ""
        lines.append(f"{icon} {format_pair_name(item['pair'])} ({ratio_str}){direction_icon}")
    
    return "\n".join(lines)


# ============================================================================
# MAIN
# ============================================================================
def main():
    global RUNNER_MIN_ABS
    parser = argparse.ArgumentParser(description="Annual_V2: RUBBEON + ANNUAL combined analysis.")
    parser.add_argument("--pair", help="Yahoo pair symbol, e.g. EURUSD=X")
    parser.add_argument("--h1-candles", type=int, default=H1_CANDLES_DEFAULT)
    parser.add_argument("--d1-candles", type=int, default=D1_CANDLES_DEFAULT)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--runner-min-abs", type=float, default=RUNNER_MIN_ABS)
    args = parser.parse_args()

    if os.getenv("GITHUB_ACTIONS", "").lower() == "true":
        args.workers = min(args.workers, 2)
    RUNNER_MIN_ABS = args.runner_min_abs

    pairs = [args.pair] if args.pair else PAIRS
    annual_data = load_annual_data()
    previous_state = load_tracking_state()
    
    results = []
    errors = []
    start = time.time()
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(
                analyze_pair,
                pair,
                h1_candles=args.h1_candles,
                d1_candles=args.d1_candles,
                annual_data=annual_data,
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

    save_annual_data(annual_data)
    
    results.sort(key=lambda x: x["pair"])
    for result in results:
        print_result(result)

    print(f"\nPairs analyzed: {len(pairs)} | OK: {len(results)} | Errors: {len(errors)}")
    print(f"Elapsed: {time.time() - start:.2f}s")

    # Filter: RUBBEON aligned + ANNUAL valid
    aligned = [r for r in results if r.get("aligned_state")]
    validated = [r for r in aligned if r.get("annual_valid")]
    
    print(f"\nRUBBEON aligned: {len(aligned)}")
    print(f"ANNUAL validated (ratio >= {WARNING_RATIO_THRESHOLD}%): {len(validated)}")

    # Tracking state management
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

    # Build and send Telegram message
    tg_message = build_telegram_message(validated)
    
    # Add tracking alerts if any
    if tracking_alerts and validated:
        alert_section = "\n\nTRACKING ALERTS\n" + "\n".join(tracking_alerts)
        tg_message += alert_section

    sent = send_telegram_message(tg_message)
    print(f"Telegram: {'sent' if sent else 'not sent'}")

    # Save tracking state
    top5_pairs = {item["pair"] for item in validated[:5]} if validated else set()
    for pair in top5_pairs:
        top5_counts[pair] = top5_counts.get(pair, 0) + 1

    save_tracking_state(
        validated[:5] if validated else [],
        persisted_reversals,
        reversal_day,
        top5_counts,
        run_count,
        persistence_day,
        exited_top5_pairs,
        top5_pairs,
    )

    if errors:
        print("\nErrors:")
        for pair, message in errors:
            print(f"  - {pair}: {message}")


if __name__ == "__main__":
    main()
