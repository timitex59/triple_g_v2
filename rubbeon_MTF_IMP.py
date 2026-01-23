#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Currency Indices SAR + EMA Scanner - SMART CACHE VERSION
Analyse les 8 indices de devises majeurs (DXY, EXY, BXY, JXY, AXY, CXY, SXY, ZXY)
avec PSAR et 8 EMAs pour détecter les tendances fortes.
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

# Indices de devises avec leurs symboles TradingView
CURRENCY_INDICES = {
    "DXY": "TVC:DXY",           # U.S. Dollar Index
    "EXY": "TVC:EXY",           # Euro Index
    "BXY": "TVC:BXY",           # British Pound Index
    "JXY": "TVC:JXY",           # Japanese Yen Index
    "AXY": "CAPITALCOM:AXY",    # Australian Dollar Index
    "CXY": "CAPITALCOM:CXY",    # Canadian Dollar Index
    "SXY": "TVC:SXY",           # Swiss Franc Index
    "ZXY": "OANDA:NZD_IDX",     # New Zealand Dollar Index
}

# Liste des indices à analyser
INDICES = list(CURRENCY_INDICES.keys())

EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]
PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM = 0.1, 0.1, 0.2
TV_CANDLES, MAX_WORKERS = 200, 4
CACHE_FILE = "indices_cache.pkl"
TELEGRAM_MIN_ABS_RUNNER = 0.15
USE_CACHE = True
CACHE_H1_CANDLES, CACHE_D1_CANDLES, CACHE_W1_CANDLES, CACHE_MAX_WORKERS = 500, 300, 150, 2
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
        
        sample_indices = list(cache_data.keys())[:3]
        
        for index in sample_indices:
            index_data = cache_data.get(index, {})
            h1_df = index_data.get("1h")
            
            if h1_df is not None and not h1_df.empty:
                last_h1_time = h1_df.index[-1]
                if last_h1_time.tzinfo is None:
                    last_h1_time = last_h1_time.tz_localize("UTC")
                else:
                    last_h1_time = last_h1_time.tz_convert("UTC")
                
                now = pd.Timestamp.now(tz="UTC")
                hours_elapsed = (now - last_h1_time).total_seconds() / 3600
                
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
        
        if cache_age >= validity:
            return True, f"Cache expiré ({format_duration(cache_age)}) - {market_status['message']}"
        
        if market_status["is_trading"]:
            h1_check, h1_reason = check_h1_candle_available(cache_obj)
            if h1_check:
                return True, f"🕐 {h1_reason}"
        
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

def fetch_data_tv(index_name_or_symbol, interval_code, n_candles=200):
    """
    Fetch data from TradingView for an index
    index_name_or_symbol: soit le code (DXY), soit le symbole complet (TVC:DXY)
    """
    # Si c'est un code (DXY, EXY, etc.), r??cup??rer le symbole complet
    if ":" not in index_name_or_symbol:
        tv_symbol = CURRENCY_INDICES.get(index_name_or_symbol, index_name_or_symbol)
    else:
        tv_symbol = index_name_or_symbol
    
    tv_interval = {"1wk": "W", "1d": "D", "1h": "60"}.get(interval_code, "W")
    extracted_df = None
    tv_ws_timeout = int(os.getenv("TV_WS_TIMEOUT", "10"))
    tv_recv_seconds = float(os.getenv("TV_RECV_TIMEOUT", "8"))
    tv_min_delay = float(os.getenv("TV_MIN_DELAY", "0.25"))
    tv_max_delay = float(os.getenv("TV_MAX_DELAY", "0.75"))
    tv_max_retries = int(os.getenv("TV_MAX_RETRIES", "2"))
    for attempt in range(tv_max_retries + 1):
        ws = None
        try:
            time.sleep(random.uniform(tv_min_delay, tv_max_delay))
            if os.getenv("TV_DEBUG", "0") == "1":
                print(f"[TV] Connecting {index_name_or_symbol} {interval_code} candles={n_candles} attempt={attempt+1}")
            headers = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}
            ws = create_connection("wss://prodata.tradingview.com/socket.io/websocket", header=headers, timeout=tv_ws_timeout)
            session_id = generate_session_id()
            if os.getenv("TV_DEBUG", "0") == "1":
                print(f"[TV] Session {session_id}")
            ws.send(create_message("chart_create_session", [session_id, ""]))
            ws.send(create_message("resolve_symbol", [session_id, "sds_sym_1", f"={{\"symbol\":\"{tv_symbol}\",\"adjustment\":\"splits\",\"session\":\"regular\"}}"]))
            ws.send(create_message("create_series", [session_id, "sds_1", "s1", "sds_sym_1", tv_interval, n_candles, ""]))
            start_t = time.time()
            while time.time() - start_t < tv_recv_seconds:
                try:
                    res = ws.recv()
                    if os.getenv("TV_DEBUG", "0") == "1":
                        print(f"[TV] recv {len(res)} bytes")
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
            if os.getenv("TV_DEBUG", "0") == "1":
                if extracted_df is None or extracted_df.empty:
                    print("[TV] No data received")
                else:
                    print(f"[TV] Data rows: {len(extracted_df)}")
            return extracted_df
        except Exception as exc:
            if os.getenv("TV_DEBUG", "0") == "1":
                print(f"TV fetch failed for {index_name_or_symbol} {interval_code}: {exc}")
            if ws:
                try:
                    ws.close()
                except:
                    pass
            if "429" in str(exc) and attempt < tv_max_retries:
                time.sleep(2.0 * (attempt + 1))
                continue
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

def tracking_file_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), TRACKING_FILE)

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

def fetch_data_cache(index_name, interval_code, n_candles):
    cache = load_market_cache()
    index_data = cache.get(index_name)
    if not index_data:
        return None
    df = index_data.get(interval_code)
    if df is None or df.empty:
        return None
    if n_candles and len(df) > n_candles:
        return df.tail(n_candles)
    return df

def fetch_data_smart(index_name, interval, n_candles):
    """Fetch data for an index using TradingView only"""
    df = fetch_data_cache(index_name, interval, n_candles)
    if df is not None and not df.empty:
        return df, "CACHE"
    df = fetch_data_tv(index_name, interval, n_candles=n_candles)
    if df is not None and not df.empty:
        return df, "TV"
    return None, "NA"

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

def fetch_tv_batch(index_name, h1_candles, d1_candles, w1_candles):
    """Fetch all timeframes for an index"""
    data_map = {}
    for interval, n_candles, key in [("1h", h1_candles, "1h"), ("1d", d1_candles, "1d"), ("1wk", w1_candles, "1wk")]:
        if n_candles and n_candles > 0:
            df = fetch_data_tv(index_name, interval, n_candles=n_candles)
            if df is not None:
                data_map[key] = df
    return (index_name, data_map) if data_map else None

def refresh_market_cache():
    global _CACHE_DATA, _CACHE_TIMESTAMP
    
    start_time = time.time()
    existing_cache = load_market_cache() or {}
    if _CACHE_TIMESTAMP and (time.time() - _CACHE_TIMESTAMP) < REFRESH_MIN_SECONDS:
        # Avoid skipping refresh when cache is empty
        if existing_cache:
            return 0.0, True
        _CACHE_TIMESTAMP = None
    cache_data, fetch_specs = {}, {}
    for index in INDICES:
        if index not in existing_cache:
            fetch_specs[index] = (CACHE_H1_CANDLES, CACHE_D1_CANDLES, CACHE_W1_CANDLES)
            continue
        index_cache = existing_cache.get(index, {})
        h1_last, d1_last, w1_last = index_cache.get("1h"), index_cache.get("1d"), index_cache.get("1wk")
        h1_last_idx = h1_last.index[-1] if h1_last is not None and not h1_last.empty else None
        d1_last_idx = d1_last.index[-1] if d1_last is not None and not d1_last.empty else None
        w1_last_idx = w1_last.index[-1] if w1_last is not None and not w1_last.empty else None
        h1_need = H1_REFRESH_CANDLES if should_refresh(h1_last_idx, "1h") else 0
        d1_need = D1_REFRESH_CANDLES if should_refresh(d1_last_idx, "1d") else 0
        w1_need = W1_REFRESH_CANDLES if should_refresh(w1_last_idx, "1wk") else 0
        fetch_specs[index] = (h1_need, d1_need, w1_need)
    with ThreadPoolExecutor(max_workers=CACHE_MAX_WORKERS) as executor:
        future_to_index = {}
        for index, (h1_candles, d1_candles, w1_candles) in fetch_specs.items():
            if h1_candles == 0 and d1_candles == 0 and w1_candles == 0:
                if index in existing_cache:
                    cache_data[index] = existing_cache[index]
                continue
            future_to_index[executor.submit(fetch_tv_batch, index, h1_candles, d1_candles, w1_candles)] = index
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            result = future.result()
            if result:
                _, data = result
                if index in existing_cache:
                    merged = {}
                    merged["1h"] = merge_frames(existing_cache[index].get("1h"), data.get("1h"), CACHE_H1_CANDLES)
                    merged["1d"] = merge_frames(existing_cache[index].get("1d"), data.get("1d"), CACHE_D1_CANDLES)
                    merged["1wk"] = merge_frames(existing_cache[index].get("1wk"), data.get("1wk"), CACHE_W1_CANDLES)
                    cache_data[index] = merged
                else:
                    cache_data[index] = data
            elif index in existing_cache:
                cache_data[index] = existing_cache[index]
    
    cache_with_meta = {"timestamp": time.time(), "data": cache_data, "market_status": get_market_status()}
    with open(cache_file_path(), "wb") as handle:
        pickle.dump(cache_with_meta, handle)
    
    _CACHE_DATA = cache_data
    _CACHE_TIMESTAMP = cache_with_meta["timestamp"]
    
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

def format_runner(value):
    return "NA" if value is None else f"{value:+.2f}%"

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

def trend_ball(signal):
    return "🟢" if signal == "BULL" else "🔴" if signal == "BEAR" else "⚪"

def runner_ball(value):
    if value is None or value == 0:
        return "⚪"
    return "🟢" if value > 0 else "🔴"

def get_strength_classification(ema_score, signal):
    """
    Retourne l'icône et le label de force basé sur le score EMA
    8/8 = Force maximale
    6-7/8 = Tendance forte
    4-5/8 = Tendance modérée
    2-3/8 = Tendance faible
    0-1/8 = Neutre/opposé
    """
    if signal == "BULL" or signal == "NEUTRAL":
        if ema_score == 8:
            return "🟢🟢", "BULLISH FORT"
        elif ema_score >= 6:
            return "🟢🟡", "Tendance haussière forte"
        elif ema_score >= 4:
            return "🟡🟢", "Tendance haussière modérée"
        elif ema_score >= 2:
            return "🟡⚪", "Tendance haussière faible"
        else:
            return "⚪⚪", "Neutre"
    else:  # BEAR
        if ema_score == 8:
            return "🔴🔴", "BEARISH FORT"
        elif ema_score >= 6:
            return "🔴🟡", "Tendance baissière forte"
        elif ema_score >= 4:
            return "🟡🔴", "Tendance baissière modérée"
        elif ema_score >= 2:
            return "🟡⚪", "Tendance baissière faible"
        else:
            return "⚪⚪", "Neutre"

def format_alignment_ball(item, main_signal):
    """
    Première boule = Signal Daily (🟢 BULL / 🔴 BEAR)
    Deuxième boule = % Runner Daily (🟢 positif / 🔴 négatif / ⚪ nul)
    """
    main_ball = trend_ball(main_signal)
    
    # Deuxième boule basée sur le % runner daily
    runner = item.get("daily_runner")
    if runner is None or runner == 0:
        runner_ball = "⚪"
    elif runner > 0:
        runner_ball = "🟢"
    else:
        runner_ball = "🔴"
    
    return main_ball + runner_ball

def filter_for_telegram(items):
    # Suppression du filtre - on envoie TOUTES les paires détectées
    return items

def build_telegram_message(bull_results, bear_results):
    """Construit le message Telegram pour les indices"""
    lines = []
    if bull_results:
        lines.append("📈 INDICES BULLISH")
        for item in sorted(bull_results, key=lambda x: x["index"]):
            balls = format_alignment_ball(item, "BULL")
            runner = format_runner(item["daily_runner"])
            lines.append(f"{balls} {item['index']:<4} {runner}")
    if bear_results:
        if lines:
            lines.append("")
        lines.append("📉 INDICES BEARISH")
        for item in sorted(bear_results, key=lambda x: x["index"]):
            balls = format_alignment_ball(item, "BEAR")
            runner = format_runner(item["daily_runner"])
            lines.append(f"{balls} {item['index']:<4} {runner}")
    return "\n".join(lines)

def build_telegram_message_with_score(strong_indices):
    """Construit le message Telegram avec les scores EMA (indices forts seulement)"""
    lines = ["📊 INDICES FORTS (Score EMA ≥ 6/8)"]
    for item in sorted(strong_indices, key=lambda x: x["ema_score"], reverse=True):
        icon, label = get_strength_classification(item["ema_score"], item["signal"])
        runner = format_runner(item["daily_runner"])
        lines.append(f"{icon} {item['index']} {item['ema_score']}/8 {runner}")
    return "\n".join(lines)


def load_tracking_state(path):
    if not os.path.exists(path):
        return {"current": {"bullish": [], "bearish": []}}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if "current" not in data:
            data["current"] = {"bullish": [], "bearish": []}
        return data
    except:
        return {"current": {"bullish": [], "bearish": []}}

def save_tracking_state(path, bull_results, bear_results):
    previous = load_tracking_state(path).get("current", {})
    prev_bull, prev_bear = set(previous.get("bullish", [])), set(previous.get("bearish", []))
    curr_bull, curr_bear = [item["pair"] for item in bull_results], [item["pair"] for item in bear_results]
    curr_bull_set, curr_bear_set = set(curr_bull), set(curr_bear)
    new_bull, new_bear = sorted(curr_bull_set - prev_bull), sorted(curr_bear_set - prev_bear)
    exit_bull, exit_bear = sorted(prev_bull - curr_bull_set), sorted(prev_bear - curr_bear_set)
    def with_new_flag(pairs, new_set):
        return [{"pair": pair, "new": pair in new_set} for pair in pairs]
    data = {
        "updated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "current": {"bullish": curr_bull, "bearish": curr_bear},
        "current_with_flags": {"bullish": with_new_flag(curr_bull, set(new_bull)), "bearish": with_new_flag(curr_bear, set(new_bear))},
        "new_entries": {"bullish": new_bull, "bearish": new_bear},
        "exits": {"bullish": exit_bull, "bearish": exit_bear},
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    return data

def get_strength_sets(bull_results, bear_results):
    total_pairs = len(bull_results) + len(bear_results)
    if total_pairs == 0:
        return set(), set(), 0
    strong_counts, weak_counts = {}, {}
    for item in bull_results:
        pair = item["pair"]
        if len(pair) >= 6:
            base, quote = pair[:3], pair[3:]
            strong_counts[base] = strong_counts.get(base, 0) + 1
            weak_counts[quote] = weak_counts.get(quote, 0) + 1
    for item in bear_results:
        pair = item["pair"]
        if len(pair) >= 6:
            base, quote = pair[:3], pair[3:]
            strong_counts[quote] = strong_counts.get(quote, 0) + 1
            weak_counts[base] = weak_counts.get(base, 0) + 1
    max_strong = max(strong_counts.values()) if strong_counts else 0
    max_weak = max(weak_counts.values()) if weak_counts else 0
    strongest = {k for k, v in strong_counts.items() if v == max_strong} if max_strong else set()
    weakest = {k for k, v in weak_counts.items() if v == max_weak} if max_weak else set()
    return strongest, weakest, total_pairs

def build_best_trade_lines(bull_results, bear_results):
    strongest, weakest, _ = get_strength_sets(bull_results, bear_results)
    if not strongest or not weakest:
        return []
    results_by_pair = {item["pair"]: item for item in (bull_results + bear_results)}
    available_pairs = [p.replace("=X", "") for p in PAIRS]
    best_pairs = []
    for pair in available_pairs:
        if len(pair) >= 6:
            base, quote = pair[:3], pair[3:]
            if (base in strongest and quote in weakest) or (base in weakest and quote in strongest):
                best_pairs.append(pair)
    if not best_pairs:
        return []
    lines = ["BEST TRADE"]
    for pair in sorted(best_pairs):
        item = results_by_pair.get(pair)
        balls = format_alignment_ball(item, item["signal"]) if item else "⚪⚪"
        lines.append(f"{balls} {pair}")
    return lines

def evaluate_conditions(df, use_psar=True):
    """Évalue si l'indice est au-dessus/en-dessous de toutes les EMAs et du PSAR"""
    if df is None or df.empty or not {"High", "Low", "Close"}.issubset(df.columns):
        return None
    df = df.dropna(subset=["High", "Low", "Close"])
    if len(df) < max(EMA_LENGTHS) + 2:
        return None
    close_last = df["Close"].iloc[-1]
    ema_values = [df["Close"].ewm(span=length, adjust=False).mean().iloc[-1] for length in EMA_LENGTHS]
    
    # Calcul du score EMA (combien d'EMAs sont au-dessus/en-dessous du prix)
    ema_score_above = sum(1 for ema in ema_values if close_last > ema)
    ema_score_below = sum(1 for ema in ema_values if close_last < ema)
    
    above_emas, below_emas = all(close_last > ema for ema in ema_values), all(close_last < ema for ema in ema_values)
    
    psar = None
    above_psar = True
    below_psar = True
    if use_psar:
        psar = calculate_psar_series(df, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        if psar is None:
            return None
        above_psar, below_psar = close_last > psar[-1], close_last < psar[-1]
    
    if above_emas and above_psar:
        return {"signal": "BULL", "close": close_last, "psar": psar[-1] if psar is not None else None, "ema_score": ema_score_above}
    if below_emas and below_psar:
        return {"signal": "BEAR", "close": close_last, "psar": psar[-1] if psar is not None else None, "ema_score": ema_score_below}
    return {"signal": "NEUTRAL", "close": close_last, "psar": psar[-1] if psar is not None else None, "ema_score": ema_score_above}

def analyze_index(index_name):
    """Analyse un indice de devise - Daily + Hourly seulement - Retourne TOUS les indices avec score"""
    df_d, source_d = fetch_data_smart(index_name, "1d", n_candles=TV_CANDLES)
    df_h, source_h = fetch_data_smart(index_name, "1h", n_candles=TV_CANDLES)
    
    daily = evaluate_conditions(df_d, use_psar=False)
    hourly = evaluate_conditions(df_h, use_psar=True)
    daily_runner = calculate_pct_runner_daily(df_d)
    
    if daily is None or hourly is None:
        return None
    
    # Retourne TOUS les indices au lieu de filtrer
    return {
        "index": index_name,
        "source": f"{source_d}/{source_h}",
        "close": daily["close"],
        "psar_h1": hourly["psar"],
        "signal": daily["signal"],
        "ema_score": daily["ema_score"],
        "hourly_signal": hourly["signal"],
        "daily_runner": daily_runner
    }

def main():
    # Fix Windows encoding for emojis
    if sys.platform == "win32":
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    
    print("=" * 80)
    print("Currency Indices SAR + EMA Scanner - SMART CACHE VERSION")
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
    if not load_market_cache() and os.getenv("ALLOW_EMPTY_CACHE_ANALYSIS", "0") != "1":
        print("Cache empty after refresh, analysis skipped to avoid rate limit.")
        print("Tip: set ALLOW_EMPTY_CACHE_ANALYSIS=1 to force analysis.")
        print("-" * 80)
        return
    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_index, index): index for index in INDICES}
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_results.append(result)
    
    # Trier par score EMA décroissant (du plus fort au plus faible)
    all_results.sort(key=lambda x: x["ema_score"], reverse=True)
    
    print(f"PSAR: start={PSAR_START}, increment={PSAR_INCREMENT}, maximum={PSAR_MAXIMUM}")
    if _CACHE_TIMESTAMP:
        print(f"Cache timestamp: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(_CACHE_TIMESTAMP))}")
    print(f"Indices analysés: {len(INDICES)} | Résultats: {len(all_results)}")
    print("-" * 80)
    
    if all_results:
        print()
        print("📊 CLASSEMENT DES INDICES PAR FORCE (Score EMA)")
        print(f"{'INDEX':<6} {'SCORE':<8} {'FORCE':<28} {'CLOSE':>12} {'%RUNNER D':>11} {'H1':>6}")
        for item in all_results:
            icon, label = get_strength_classification(item["ema_score"], item["signal"])
            h1_status = "🟢" if item["hourly_signal"] == "BULL" else "🔴" if item["hourly_signal"] == "BEAR" else "⚪"
            print(
                f"{item['index']:<6} "
                f"{icon} {item['ema_score']}/8  "
                f"{label:<28} "
                f"{item['close']:>12.5f} "
                f"{format_runner(item['daily_runner']):>11} "
                f"{h1_status:>6}"
            )
    else:
        print("\nAucun résultat disponible")
    
    print("-" * 80)
    
    # Telegram - envoyer les indices forts seulement (score >= 6)
    strong_indices = [item for item in all_results if item["ema_score"] >= 6]
    if strong_indices:
        tg_message = build_telegram_message_with_score(strong_indices)
        send_telegram_message(tg_message)

if __name__ == "__main__":
    main()
