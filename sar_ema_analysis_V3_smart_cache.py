#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily SAR + EMA Scanner - SMART CACHE VERSION
Check which forex pairs are above all 8 EMAs and above PSAR.
PSAR parameters: start=0.1, increment=0.1, maximum=0.2
"""

import os
import sys
import time
import json
import random
import string
import importlib
import subprocess
import pickle
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

def install_and_import(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        return importlib.import_module(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return importlib.import_module(import_name)

yf = install_and_import("yfinance")
pd = install_and_import("pandas")
np = install_and_import("numpy")
websocket = install_and_import("websocket-client", "websocket")
from websocket import create_connection
requests = install_and_import("requests")
dotenv = install_and_import("python-dotenv", "dotenv")

# PAIRS = ["EURUSD=X", "EURGBP=X", "EURJPY=X", "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X","USDJPY=X", 
#          "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X"]





PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
    "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
    "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPCHF=X",
    "AUDNZD=X", "AUDCAD=X", "AUDCHF=X",
    "NZDCAD=X", "NZDCHF=X",
    "CADCHF=X",
]



EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]
PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM = 0.1, 0.1, 0.2
TV_CANDLES, MAX_WORKERS, FORCE_TV_ONLY = 200, 8, True
CACHE_FILE = "market_cache.pkl"
USE_CACHE = True
CACHE_H1_CANDLES, CACHE_D1_CANDLES, CACHE_W1_CANDLES, CACHE_MAX_WORKERS = 500, 300, 150, 5
H1_REFRESH_CANDLES, D1_REFRESH_CANDLES, W1_REFRESH_CANDLES = 24, 5, 4
REFRESH_MIN_SECONDS = 600
CACHE_VALIDITY_TRADING_HOURS, CACHE_VALIDITY_OFF_HOURS, CACHE_VALIDITY_WEEKEND = 3600, 14400, 86400

dotenv.load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

_CACHE_DATA, _CACHE_TIMESTAMP = None, None

def is_forex_trading_hours():
    now = datetime.now(timezone.utc)
    weekday, hour = now.weekday(), now.hour
    if weekday == 5 or (weekday == 6 and hour < 22) or (weekday == 4 and hour >= 22):
        return False
    return True

def get_market_status():
    now = datetime.now(timezone.utc)
    weekday, hour = now.weekday(), now.hour
    if weekday == 5 or (weekday == 6 and hour < 22):
        return {"status": "WEEKEND", "cache_validity": CACHE_VALIDITY_WEEKEND, "is_trading": False, "message": "🔴 Marché FERMÉ (Weekend)"}
    if weekday == 4 and hour >= 22:
        return {"status": "WEEKEND_START", "cache_validity": CACHE_VALIDITY_WEEKEND, "is_trading": False, "message": "🔴 Marché FERMÉ (Clôture vendredi)"}
    if is_forex_trading_hours():
        if 7 <= hour < 22:
            return {"status": "ACTIVE_HOURS", "cache_validity": CACHE_VALIDITY_TRADING_HOURS, "is_trading": True, "message": "🟢 Marché OUVERT (Heures actives)"}
        return {"status": "QUIET_HOURS", "cache_validity": CACHE_VALIDITY_OFF_HOURS, "is_trading": True, "message": "🟡 Marché OUVERT (Heures calmes)"}
    return {"status": "OFF_HOURS", "cache_validity": CACHE_VALIDITY_OFF_HOURS, "is_trading": False, "message": "🔴 Marché FERMÉ"}

def check_h1_candle_available(cache_obj):
    """
    Vérifie si une nouvelle bougie H1 devrait être disponible
    Important pour scripts qui tournent toutes les heures !
    """
    try:
        cache_data = cache_obj.get("data", {})
        if not cache_data:
            return True, "Cache vide"
        
        # Vérifier quelques paires pour voir la dernière bougie H1
        sample_pairs = list(cache_data.keys())[:3]  # Échantillon de 3 paires
        
        for pair in sample_pairs:
            pair_data = cache_data.get(pair, {})
            h1_df = pair_data.get("1h")
            
            if h1_df is not None and not h1_df.empty:
                # Dernière bougie H1 dans le cache
                last_h1_time = h1_df.index[-1]
                if last_h1_time.tzinfo is None:
                    last_h1_time = last_h1_time.tz_localize("UTC")
                else:
                    last_h1_time = last_h1_time.tz_convert("UTC")
                
                # Heure actuelle
                now = pd.Timestamp.now(tz="UTC")
                
                # Calculer combien d'heures complètes se sont écoulées
                hours_elapsed = (now - last_h1_time).total_seconds() / 3600
                
                # Si plus d'1h15 s'est écoulée, nouvelle bougie disponible
                # (marge de 15min pour être sûr que la bougie est clôturée)
                if hours_elapsed >= 1.25:
                    return True, f"Nouvelle bougie H1 disponible (dernière: {last_h1_time.strftime('%H:%M')}, elapsed: {hours_elapsed:.1f}h)"
        
        return False, "Bougies H1 à jour"
        
    except Exception as e:
        return False, f"Erreur vérification H1: {e}"

def should_refresh_cache(force=False):
    if force:
        return True, "Refresh forcé"
    if not os.path.exists(cache_file_path()):
        return True, "Cache inexistant"
    try:
        with open(cache_file_path(), "rb") as f:
            cache_obj = pickle.load(f)
        cache_timestamp = cache_obj.get("timestamp")
        if not cache_timestamp:
            return True, "Cache sans timestamp"
        
        market_status = get_market_status()
        cache_age = time.time() - cache_timestamp
        validity = market_status["cache_validity"]
        
        # Vérification 1 : Expiration temporelle classique
        if cache_age >= validity:
            return True, f"Cache expiré ({format_duration(cache_age)}) - {market_status['message']}"
        
        # Vérification 2 : Nouvelle bougie H1 disponible ?
        # Important pour scripts qui tournent toutes les heures !
        if market_status["is_trading"]:  # Seulement si marché ouvert
            h1_check, h1_reason = check_h1_candle_available(cache_obj)
            if h1_check:
                return True, f"🕐 {h1_reason}"
        
        # Cache valide sur tous les plans
        return False, f"Cache valide ({format_duration(cache_age)}/{format_duration(validity)}) - {market_status['message']}"
        
    except Exception as e:
        return True, f"Erreur: {e}"

def format_duration(seconds):
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds/60)}m"
    return f"{seconds/3600:.1f}h"

def generate_session_id():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))

def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"

def fetch_data_tv(pair_yahoo, interval_code, n_candles=200):
    clean_pair, tv_pair = pair_yahoo.replace("=X", ""), f"OANDA:{pair_yahoo.replace('=X', '')}"
    tv_interval = {"1wk": "W", "1d": "D", "1h": "60"}.get(interval_code, "W")
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
                    start, end = res.find('"s":['), res.find('"ns":', res.find('"s":['))
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
            except:
                pass
        ws.close()
        return extracted_df
    except:
        if ws:
            try:
                ws.close()
            except:
                pass
        return None

def fetch_data_yahoo(pair_yahoo, interval_code, period):
    try:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = yf.Ticker(pair_yahoo).history(period=period, interval=interval_code)
            return df if not df.empty else None
    except:
        return None

def cache_file_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), CACHE_FILE)

def load_market_cache():
    global _CACHE_DATA, _CACHE_TIMESTAMP
    if _CACHE_DATA is not None:
        return _CACHE_DATA
    path = cache_file_path()
    if not os.path.exists(path):
        _CACHE_DATA = {}
        return _CACHE_DATA
    try:
        with open(path, "rb") as handle:
            payload = pickle.load(handle)
        _CACHE_TIMESTAMP = payload.get("timestamp") if isinstance(payload, dict) else None
        _CACHE_DATA = payload.get("data", {}) if isinstance(payload, dict) else {}
    except:
        _CACHE_DATA = {}
    return _CACHE_DATA

def get_cached_df(pair, interval):
    cache = load_market_cache()
    if not cache:
        return None
    pair_data = cache.get(pair)
    if not pair_data:
        return None
    df = pair_data.get(interval)
    if df is None or df.empty:
        return None
    return df

def fetch_data_smart(pair, interval, n_candles, yahoo_period):
    df = get_cached_df(pair, interval)
    if df is not None and not df.empty:
        return df, "CACHE"
    df = fetch_data_tv(pair, interval, n_candles=n_candles)
    if df is not None and not df.empty:
        return df, "TV"
    if FORCE_TV_ONLY:
        return None, "NA"
    df = fetch_data_yahoo(pair, interval, yahoo_period)
    return (df, "YF") if df is not None and not df.empty else (None, "NA")

def normalize_timestamp(ts):
    if ts is None:
        return None
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")

def should_refresh(last_index, interval_code):
    if last_index is None:
        return True
    last_index = normalize_timestamp(last_index)
    now = pd.Timestamp.now(tz="UTC")
    if interval_code == "1h":
        return (now - last_index) >= pd.Timedelta(hours=1)
    if interval_code == "1d":
        return now.date() > last_index.date()
    if interval_code == "1wk":
        now_iso, last_iso = now.isocalendar(), last_index.isocalendar()
        return (now_iso.year, now_iso.week) != (last_iso.year, last_iso.week)
    return True

def merge_frames(existing, new_df, max_candles):
    if new_df is None or new_df.empty:
        return existing
    if existing is None or existing.empty:
        merged = new_df
    else:
        merged = pd.concat([existing, new_df])
        merged = merged[~merged.index.duplicated(keep="last")]
        merged.sort_index(inplace=True)
    if max_candles and len(merged) > max_candles:
        merged = merged.iloc[-max_candles:]
    return merged

def fetch_tv_batch(pair_yahoo, h1_candles, d1_candles, w1_candles):
    data_map = {}
    for interval, n_candles, key in [("1h", h1_candles, "1h"), ("1d", d1_candles, "1d"), ("1wk", w1_candles, "1wk")]:
        if n_candles and n_candles > 0:
            df = fetch_data_tv(pair_yahoo, interval, n_candles=n_candles)
            if df is not None:
                data_map[key] = df
    return (pair_yahoo, data_map) if data_map else None

def refresh_market_cache():
    start_time = time.time()
    existing_cache = load_market_cache() or {}
    if _CACHE_TIMESTAMP and (time.time() - _CACHE_TIMESTAMP) < REFRESH_MIN_SECONDS:
        return 0.0, True
    cache_data, fetch_specs = {}, {}
    for pair in PAIRS:
        if pair not in existing_cache:
            fetch_specs[pair] = (CACHE_H1_CANDLES, CACHE_D1_CANDLES, CACHE_W1_CANDLES)
            continue
        pair_cache = existing_cache.get(pair, {})
        h1_last, d1_last, w1_last = pair_cache.get("1h"), pair_cache.get("1d"), pair_cache.get("1wk")
        h1_last_idx = h1_last.index[-1] if h1_last is not None and not h1_last.empty else None
        d1_last_idx = d1_last.index[-1] if d1_last is not None and not d1_last.empty else None
        w1_last_idx = w1_last.index[-1] if w1_last is not None and not w1_last.empty else None
        h1_need = H1_REFRESH_CANDLES if should_refresh(h1_last_idx, "1h") else 0
        d1_need = D1_REFRESH_CANDLES if should_refresh(d1_last_idx, "1d") else 0
        w1_need = W1_REFRESH_CANDLES if should_refresh(w1_last_idx, "1wk") else 0
        fetch_specs[pair] = (h1_need, d1_need, w1_need)
    with ThreadPoolExecutor(max_workers=CACHE_MAX_WORKERS) as executor:
        future_to_pair = {}
        for pair, (h1_candles, d1_candles, w1_candles) in fetch_specs.items():
            if h1_candles == 0 and d1_candles == 0 and w1_candles == 0:
                if pair in existing_cache:
                    cache_data[pair] = existing_cache[pair]
                continue
            future_to_pair[executor.submit(fetch_tv_batch, pair, h1_candles, d1_candles, w1_candles)] = pair
        for future in as_completed(future_to_pair):
            pair = future_to_pair[future]
            result = future.result()
            if result:
                _, data = result
                if pair in existing_cache:
                    merged = {}
                    merged["1h"] = merge_frames(existing_cache[pair].get("1h"), data.get("1h"), CACHE_H1_CANDLES)
                    merged["1d"] = merge_frames(existing_cache[pair].get("1d"), data.get("1d"), CACHE_D1_CANDLES)
                    merged["1wk"] = merge_frames(existing_cache[pair].get("1wk"), data.get("1wk"), CACHE_W1_CANDLES)
                    cache_data[pair] = merged
                else:
                    cache_data[pair] = data
            elif pair in existing_cache:
                cache_data[pair] = existing_cache[pair]
    cache_with_meta = {"timestamp": time.time(), "data": cache_data, "market_status": get_market_status()}
    with open(cache_file_path(), "wb") as handle:
        pickle.dump(cache_with_meta, handle)
    return time.time() - start_time, False

def calculate_psar_series(df, start, increment, maximum):
    if df is None or len(df) < 3:
        return None
    highs, lows, closes = df["High"].values, df["Low"].values, df["Close"].values
    psar, bull, af = np.zeros(len(df)), closes[1] >= closes[0], start
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

def calculate_pct_runner_daily(df):
    """
    Calcule le % runner daily basé sur la dernière bougie Daily CLÔTURÉE
    (cohérent avec TradingView)
    """
    if df is None or len(df) < 1:
        return None
    open_price = df["Open"].iloc[-1]
    close_price = df["Close"].iloc[-1]
    if open_price > 0:
        return ((close_price - open_price) / open_price) * 100.0
    return None

def get_ema_band_series(close):
    ema_frames = [close.ewm(span=length, adjust=False).mean() for length in EMA_LENGTHS]
    all_emas = pd.concat(ema_frames, axis=1)
    ema_low = all_emas.min(axis=1)
    ema_high = all_emas.max(axis=1)
    return ema_low, ema_high

def get_last_psar_cross_series(psar, closes):
    last_bull, last_bear = [None] * len(psar), [None] * len(psar)
    current_bull, current_bear = None, None
    for i in range(len(psar)):
        if i > 0:
            if closes[i - 1] <= psar[i - 1] and closes[i] > psar[i]:
                current_bull = psar[i]
            if closes[i - 1] >= psar[i - 1] and closes[i] < psar[i]:
                current_bear = psar[i]
        last_bull[i], last_bear[i] = current_bull, current_bear
    return last_bull, last_bear

def compute_setup_state_1h(df, trend_signal):
    if df is None or df.empty:
        return None
    if "Close" not in df.columns:
        return None
    psar = calculate_psar_series(df, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
    if psar is None:
        return None
    close = df["Close"]
    ema_low, ema_high = get_ema_band_series(close)
    last_bull, last_bear = get_last_psar_cross_series(psar, close.values)
    setup_valid = []
    for i in range(len(df)):
        bull_vw0, bear_vw0 = last_bull[i], last_bear[i]
        if bull_vw0 is None or bear_vw0 is None:
            setup_valid.append(False)
            continue
        if trend_signal == "BULL":
            setup_valid.append(bull_vw0 < ema_low.iat[i] and bear_vw0 > ema_high.iat[i])
        else:
            setup_valid.append(bull_vw0 > ema_high.iat[i] and bear_vw0 < ema_low.iat[i])
    setup_valid = np.array(setup_valid, dtype=bool)
    last_close = float(close.iloc[-1])
    if setup_valid[-1]:
        bull_vw0, bear_vw0 = last_bull[-1], last_bear[-1]
        if trend_signal == "BULL":
            is_break = last_close > bear_vw0
        else:
            is_break = last_close < bull_vw0
        return {"status": "ACTIVE", "bull_vw0": bull_vw0, "bear_vw0": bear_vw0, "is_break": is_break, "last_close_1h": last_close}
    valid_indices = np.flatnonzero(setup_valid)
    if len(valid_indices) == 0:
        return None
    last_valid_index = int(valid_indices[-1])
    last_bull_now, last_bear_now = last_bull[-1], last_bear[-1]
    if last_bull_now is None or last_bear_now is None:
        return None
    ema_low_last, ema_high_last = ema_low.iat[-1], ema_high.iat[-1]
    if trend_signal == "BULL":
        lost = last_bull_now < ema_low_last and last_bear_now < ema_low_last
    else:
        lost = last_bull_now > ema_high_last and last_bear_now > ema_high_last
    if not lost:
        return None
    return {"status": "LOST", "bull_vw0": last_bull[last_valid_index], "bear_vw0": last_bear[last_valid_index], "is_break": None, "last_close_1h": last_close}

def format_runner(value):
    return "NA" if value is None else f"{value:+.2f}%"

def format_level(value):
    return "NA" if value is None else f"{value:.5f}"

def format_price(value):
    return "NA" if value is None else f"{value:.5f}"

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials not configured.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
        return response.json().get("ok", False)
    except Exception as exc:
        print(f"Telegram error: {exc}")
        return False

def format_alignment_ball(item, main_signal):
    """
    Première boule = Signal Daily (🟢 BULL / 🔴 BEAR)
    Deuxième boule = % Runner Daily (🟢 positif / 🔴 négatif / ⚪ nul)
    """
    main_ball = "🟢" if main_signal == "BULL" else "🔴"
    
    # Deuxième boule basée sur le % runner daily
    runner = item.get("daily_runner")
    if runner is None or runner == 0:
        runner_ball = "⚪"
    elif runner > 0:
        runner_ball = "🟢"
    else:
        runner_ball = "🔴"
    
    return main_ball + runner_ball

def build_telegram_message(bull_results, bear_results):
    lines = []
    if bull_results:
        lines.append("BULLISH D")
        for item in sorted(bull_results, key=lambda x: x["pair"]):
            balls = format_alignment_ball(item, "BULL")
            lines.append(f"{balls} {item['pair']}")
    if bear_results:
        if lines:
            lines.append("")
        lines.append("BEARISH D")
        for item in sorted(bear_results, key=lambda x: x["pair"]):
            balls = format_alignment_ball(item, "BEAR")
            lines.append(f"{balls} {item['pair']}")
    return "\n".join(lines)

def evaluate_conditions(df, use_psar=True):
    if df is None or df.empty:
        return None
    required = {"High", "Low", "Close"}
    if not required.issubset(df.columns):
        return None
    df = df.dropna(subset=["High", "Low", "Close"])
    if len(df) < max(EMA_LENGTHS) + 2:
        return None
    close_last = df["Close"].iloc[-1]
    ema_values = [df["Close"].ewm(span=length, adjust=False).mean().iloc[-1] for length in EMA_LENGTHS]
    above_emas = all(close_last > ema for ema in ema_values)
    below_emas = all(close_last < ema for ema in ema_values)
    psar = None
    above_psar, below_psar = True, True
    if use_psar:
        psar = calculate_psar_series(df, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        if psar is None:
            return None
        above_psar = close_last > psar[-1]
        below_psar = close_last < psar[-1]
    if above_emas and above_psar:
        return {"signal": "BULL", "close": close_last, "psar": psar[-1] if psar is not None else None}
    if below_emas and below_psar:
        return {"signal": "BEAR", "close": close_last, "psar": psar[-1] if psar is not None else None}
    return {"signal": "NEUTRAL", "close": close_last, "psar": psar[-1] if psar is not None else None}

def analyze_pair(pair):
    df_w = get_cached_df(pair, "1wk")
    df_d = get_cached_df(pair, "1d")
    df_h = get_cached_df(pair, "1h")
    source_w = "CACHE" if df_w is not None else None
    source_d = "CACHE" if df_d is not None else None
    source_h = "CACHE" if df_h is not None else None
    if df_w is None:
        df_w, source_w = fetch_data_smart(pair, "1wk", n_candles=CACHE_W1_CANDLES, yahoo_period="3y")
    if df_d is None:
        df_d, source_d = fetch_data_smart(pair, "1d", n_candles=TV_CANDLES, yahoo_period="2y")
    if df_h is None:
        df_h, source_h = fetch_data_smart(pair, "1h", n_candles=TV_CANDLES, yahoo_period="2mo")
    weekly = evaluate_conditions(df_w, use_psar=False)
    daily = evaluate_conditions(df_d, use_psar=False)
    hourly = evaluate_conditions(df_h, use_psar=True)
    daily_runner = calculate_pct_runner_daily(df_d)
    if weekly is None or daily is None or hourly is None:
        return None
    if weekly["signal"] not in ("BULL", "BEAR"):
        return None
    if daily["signal"] not in ("BULL", "BEAR"):
        return None
    if weekly["signal"] != daily["signal"]:
        return None
    setup_state = compute_setup_state_1h(df_h, daily["signal"])
    result = {"pair": pair.replace("=X", ""), "source": f"{source_w}/{source_d}/{source_h}", "close": daily["close"], "psar": daily["psar"], "signal": daily["signal"], "hourly_signal": hourly["signal"], "daily_runner": daily_runner, "status": None}
    if setup_state is None:
        return result
    result.update({"bull_vw0": setup_state["bull_vw0"], "bear_vw0": setup_state["bear_vw0"], "is_break": setup_state["is_break"], "last_close_1h": setup_state["last_close_1h"], "status": setup_state["status"]})
    return result

def main():
    # Fix Windows encoding for emojis
    if sys.platform == "win32":
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    
    print("=" * 80)
    print("Daily SAR + EMA (8) Scanner - SMART CACHE VERSION")
    print("=" * 80)
    market_status = get_market_status()
    print(f"\n{market_status['message']}")
    need_refresh, refresh_reason = should_refresh_cache(force=False)
    print(f"\nStatut du cache: {refresh_reason}")
    if need_refresh:
        print()
        elapsed, skipped = refresh_market_cache()
        if skipped:
            print(f"Cache refresh skipped (recent cache < {REFRESH_MIN_SECONDS}s)")
        else:
            print(f"✅ Cache rafraîchi en {elapsed:.2f}s")
    else:
        print("📦 Utilisation du cache existant")
    print()
    print("-" * 80)
    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_pair, pair): pair for pair in PAIRS}
        for future in as_completed(futures):
            result = future.result()
            if result:
                if result["status"] == "ACTIVE":
                    all_results.append(result)
    all_results.sort(key=lambda x: x["pair"])
    print(f"PSAR: start={PSAR_START}, increment={PSAR_INCREMENT}, maximum={PSAR_MAXIMUM}")
    if _CACHE_TIMESTAMP:
        print(f"Cache timestamp: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(_CACHE_TIMESTAMP))}")
    print(f"Pairs scanned: {len(PAIRS)} | Matches: {len(all_results)}")
    print("-" * 80)
    if all_results:
        print("Pairs with BULL_VW0 below EMA and BEAR_VW0 above EMA (1H)")
        print(f"{'PAIR':<10} {'SOURCE':<18} {'DAILY':>8} {'CLOSE':>12} {'PSAR':>12} {'H1_CLOSE':>12} {'BULL_VW0':>12} {'BEAR_VW0':>12} {'%RUNNER D':>11}")
        for item in all_results:
            print(f"{item['pair']:<10} {item['source']:<18} {item['signal']:>8} {format_price(item['close']):>12} {format_price(item['psar']):>12} {format_price(item['last_close_1h']):>12} {format_level(item['bull_vw0']):>12} {format_level(item['bear_vw0']):>12} {format_runner(item['daily_runner']):>11}")
    else:
        print("No pairs match the 1H EMA band setup.")
    bull_active = [item for item in all_results if item["signal"] == "BULL"]
    bear_active = [item for item in all_results if item["signal"] == "BEAR"]
    tg_message = build_telegram_message(bull_active, bear_active)
    if tg_message:
        send_telegram_message(tg_message)

if __name__ == "__main__":
    main()
