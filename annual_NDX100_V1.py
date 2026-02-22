#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
annual_NDX100_V1 - Nasdaq 100 EMA20 Distance Ranking (Strict EMA Alignment)

Simplified logic:
- Universe: Nasdaq-100 constituents
- Indicator: EMA20 distance only
- Horizons: 6M, 3M, 1M, 1W (lookback windows on daily candles)
- Filter: strict 8-EMA bullish alignment on all horizons
- Ranking: symbols sorted by average positive % distance across horizons
"""

import argparse
import importlib
import json
import os
import random
import string
import sys
import time
from datetime import datetime, time as dt_time, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo


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
HORIZONS = {
    "6M": 126,  # ~ 6 months of US trading sessions
    "3M": 63,   # ~ 3 months
    "1M": 21,   # ~ 1 month
}

TV_STOCK_PREFIX = os.getenv("TV_STOCK_PREFIX", "NASDAQ")
LQQ_SYMBOL = os.getenv("LQQ_SYMBOL", "EURONEXT:LQQ")
ISOE_SYMBOL = os.getenv("ISOE_SYMBOL", "EURONEXT:ISOE")
USDTUSDC_SYMBOL = os.getenv("USDTUSDC_SYMBOL", "COINBASE:USDTUSDC")
NDX100_CONSTITUENTS_URL = os.getenv(
    "NDX100_CONSTITUENTS_URL",
    "https://financialmodelingprep.com/stable/nasdaq-constituent",
)
D1_CANDLES_DEFAULT = 320
D1_CANDLES_MAX = 900
W1_CANDLES_DEFAULT = 120
TOTAL_CAPITAL_USD = float(os.getenv("TOTAL_CAPITAL_USD", "10000"))
ALLOC_PER_SURVIVOR_USD = float(os.getenv("ALLOC_PER_SURVIVOR_USD", "1000"))
FEE_MODEL = os.getenv("TRADE_FEE_MODEL", "IBKR").strip().upper()  # IBKR | SIMPLE
# SIMPLE model (legacy): fixed + % notional
FEE_PCT = float(os.getenv("TRADE_FEE_PCT", "0.001"))  # 0.1%
FEE_FIXED_USD = float(os.getenv("TRADE_FEE_FIXED_USD", "1.0"))
# IBKR-like defaults
IBKR_US_PER_SHARE_USD = float(os.getenv("IBKR_US_PER_SHARE_USD", "0.005"))
IBKR_US_MIN_ORDER_USD = float(os.getenv("IBKR_US_MIN_ORDER_USD", "1.0"))
IBKR_US_MAX_PCT_NOTIONAL = float(os.getenv("IBKR_US_MAX_PCT_NOTIONAL", "0.01"))  # 1%
IBKR_US_TAF_PER_SHARE_USD = float(os.getenv("IBKR_US_TAF_PER_SHARE_USD", "0.000195"))
IBKR_US_TAF_MAX_USD = float(os.getenv("IBKR_US_TAF_MAX_USD", "5.95"))
IBKR_US_SEC_SELL_PCT = float(os.getenv("IBKR_US_SEC_SELL_PCT", "0.0"))  # SEC fee often 0 currently
IBKR_EU_PCT_NOTIONAL = float(os.getenv("IBKR_EU_PCT_NOTIONAL", "0.0005"))  # 0.05%
IBKR_EU_MIN_ORDER_EUR = float(os.getenv("IBKR_EU_MIN_ORDER_EUR", "3.0"))
IBKR_EURUSD = float(os.getenv("IBKR_EURUSD", "1.08"))
FEE_RESERVE_TRADE_COUNT = int(os.getenv("FEE_RESERVE_TRADE_COUNT", "10"))  # keep fees for 10 buys + 10 sells
FEE_RESERVE_NOTIONAL_REF = float(os.getenv("FEE_RESERVE_NOTIONAL_REF", str(ALLOC_PER_SURVIVOR_USD)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PORTFOLIO_STATE_FILE = os.path.join(SCRIPT_DIR, "annual_ndx100_portfolio_state.json")
SCAN_STATE_FILE = os.path.join(SCRIPT_DIR, "annual_ndx100_scan_state.json")

dotenv.load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Static fallback to keep script usable if API is unavailable.
NDX100_FALLBACK_SYMBOLS = [
    "AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "ALGN", "AMAT", "AMD",
    "AMGN", "AMZN", "ANSS", "APP", "ARM", "ASML", "AVGO", "AXON", "AZN", "BIIB",
    "BKNG", "CDNS", "CCEP", "CEG", "CHTR", "CMCSA", "COST", "CPRT", "CRWD", "CSCO",
    "CSGP", "CSX", "CTAS", "CTSH", "DASH", "DDOG", "DXCM", "EA", "EXC", "FANG",
    "FAST", "FTNT", "GEHC", "GFS", "GILD", "GOOG", "GOOGL", "HON", "IDXX", "INTC",
    "INTU", "ISRG", "KDP", "KHC", "KLAC", "LIN", "LRCX", "LULU", "MAR", "MCHP",
    "MDLZ", "MELI", "META", "MNST", "MRNA", "MRVL", "MSFT", "MU", "NFLX", "NVDA",
    "NXPI", "ODFL", "ON", "ORLY", "PANW", "PAYX", "PCAR", "PDD", "PEP", "PLTR",
    "PYPL", "QCOM", "REGN", "ROP", "ROST", "SBUX", "SNPS", "TEAM", "TMUS", "TSCO",
    "TSLA", "TTD", "TTWO", "TXN", "VRSK", "VRTX", "WBD", "WDAY", "XEL", "ZS",
]


def generate_session_id():
    return "cs_" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


def create_message(func, args):
    content = json.dumps({"m": func, "p": args})
    return f"~m~{len(content)}~m~{content}"


def fetch_data_tv(symbol, interval_code="1d", n_candles=200):
    tv_pair = symbol.strip().upper()
    if ":" not in tv_pair:
        tv_pair = f"{TV_STOCK_PREFIX}:{tv_pair}"
    tv_interval = {"1d": "D", "1h": "60", "1w": "W"}.get(interval_code, "D")
    ws, extracted_df = None, None
    try:
        headers = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}
        ws = create_connection("wss://prodata.tradingview.com/socket.io/websocket", header=headers, timeout=12)
        session_id = generate_session_id()
        ws.send(create_message("chart_create_session", [session_id, ""]))
        ws.send(create_message("resolve_symbol", [session_id, "sds_sym_1", f"={{\"symbol\":\"{tv_pair}\",\"adjustment\":\"splits\",\"session\":\"regular\"}}"]))
        ws.send(create_message("create_series", [session_id, "sds_1", "s1", "sds_sym_1", tv_interval, n_candles, ""]))
        start_t = time.time()
        while time.time() - start_t < 10:
            try:
                res = ws.recv()
                if '"s":[' in res:
                    start = res.find('"s":[')
                    end = res.find('"ns":', start)
                    if start != -1 and end != -1:
                        extract_end = end - 1
                        while extract_end > start and res[extract_end] not in [",", "}"]:
                            extract_end -= 1
                        data = json.loads(res[start + 4:extract_end])
                        fdata = [item["v"] for item in data]
                        extracted_df = pd.DataFrame(
                            fdata, columns=["timestamp", "open", "high", "low", "close", "volume"]
                        )
                        extracted_df["datetime"] = pd.to_datetime(extracted_df["timestamp"], unit="s", utc=True)
                        extracted_df.set_index("datetime", inplace=True)
                        extracted_df.rename(
                            columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"},
                            inplace=True,
                        )
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


def normalize_index(df):
    if df is None or df.empty:
        return df
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df


def compute_ema_series(close_series, lengths):
    return {length: close_series.ewm(span=length, adjust=False).mean() for length in lengths}


def compute_inter_ema_pct(ema_map, lengths):
    inter = {}
    for i in range(len(lengths) - 1):
        fast = lengths[i]
        slow = lengths[i + 1]
        fast_series = ema_map.get(fast)
        slow_series = ema_map.get(slow)
        key = f"{fast}-{slow}"
        if (
            fast_series is None
            or slow_series is None
            or fast_series.empty
            or slow_series.empty
        ):
            inter[key] = np.nan
            continue
        fast_v = fast_series.iloc[-1]
        slow_v = slow_series.iloc[-1]
        if not np.isfinite(fast_v) or not np.isfinite(slow_v) or slow_v == 0:
            inter[key] = np.nan
            continue
        inter[key] = (fast_v - slow_v) * 100.0 / slow_v
    valid = [v for v in inter.values() if np.isfinite(v)]
    inter_mean = float(np.mean(valid)) if valid else np.nan
    return inter, inter_mean


def ratio_above_ema_ribbon(ratio_close_series, lengths):
    if ratio_close_series is None or ratio_close_series.empty or len(ratio_close_series) < max(lengths):
        return {"pass": False, "above_all": False, "above_any": False, "ratio_last": np.nan}
    ema_map = compute_ema_series(ratio_close_series, lengths)
    ratio_last = ratio_close_series.iloc[-1]
    ema_last = []
    for l in lengths:
        s = ema_map.get(l)
        if s is None or s.empty:
            return {"pass": False, "above_all": False, "above_any": False, "ratio_last": ratio_last}
        v = s.iloc[-1]
        if not np.isfinite(v):
            return {"pass": False, "above_all": False, "above_any": False, "ratio_last": ratio_last}
        ema_last.append(v)
    above_all = all(ratio_last > v for v in ema_last)
    above_any = any(ratio_last > v for v in ema_last)
    return {
        "pass": above_all,
        "above_all": above_all,
        "above_any": above_any,
        "ratio_last": ratio_last,
    }


def ratio_below_ema_ribbon(ratio_close_series, lengths):
    if ratio_close_series is None or ratio_close_series.empty or len(ratio_close_series) < max(lengths):
        return {"pass": False, "below_all": False, "below_any": False, "ratio_last": np.nan}
    ema_map = compute_ema_series(ratio_close_series, lengths)
    ratio_last = ratio_close_series.iloc[-1]
    ema_last = []
    for l in lengths:
        s = ema_map.get(l)
        if s is None or s.empty:
            return {"pass": False, "below_all": False, "below_any": False, "ratio_last": ratio_last}
        v = s.iloc[-1]
        if not np.isfinite(v):
            return {"pass": False, "below_all": False, "below_any": False, "ratio_last": ratio_last}
        ema_last.append(v)
    below_all = all(ratio_last < v for v in ema_last)
    below_any = any(ratio_last < v for v in ema_last)
    return {
        "pass": below_all,
        "below_all": below_all,
        "below_any": below_any,
        "ratio_last": ratio_last,
    }


def build_ratio_series_by_date(asset_df, benchmark_df):
    if (
        asset_df is None
        or benchmark_df is None
        or asset_df.empty
        or benchmark_df.empty
        or "Close" not in asset_df.columns
        or "Close" not in benchmark_df.columns
    ):
        return pd.Series(dtype=float)
    a = asset_df[["Close"]].copy()
    b = benchmark_df[["Close"]].copy()
    a["date"] = a.index.date
    b["date"] = b.index.date
    a = a.groupby("date", as_index=False)["Close"].last().rename(columns={"Close": "asset_close"})
    b = b.groupby("date", as_index=False)["Close"].last().rename(columns={"Close": "bench_close"})
    merged = a.merge(b, on="date", how="inner")
    if merged.empty:
        return pd.Series(dtype=float)
    ratio = (merged["asset_close"] / merged["bench_close"]).replace([np.inf, -np.inf], np.nan).dropna()
    ratio.index = pd.to_datetime(merged.loc[ratio.index, "date"])
    return ratio


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


def get_ema_alignment(ema_map, lengths):
    values = []
    for length in lengths:
        series = ema_map.get(length)
        if series is None or series.empty:
            return "NONE"
        value = series.iloc[-1]
        if not np.isfinite(value):
            return "NONE"
        values.append(value)
    if all(values[i] > values[i + 1] for i in range(len(values) - 1)):
        return "BULL"
    if all(values[i] < values[i + 1] for i in range(len(values) - 1)):
        return "BEAR"
    return "NONE"


def fetch_nasdaq100_symbols():
    try:
        response = requests.get(NDX100_CONSTITUENTS_URL, timeout=15)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            symbols = []
            seen = set()
            for item in payload:
                if not isinstance(item, dict):
                    continue
                sym = str(item.get("symbol", "")).strip().upper()
                if not sym or sym in seen:
                    continue
                seen.add(sym)
                symbols.append(sym)
            if len(symbols) >= 90:
                return symbols, "api"
    except Exception:
        pass
    return NDX100_FALLBACK_SYMBOLS[:], "fallback"


def analyze_symbol(
    symbol,
    lqq_daily=None,
    lqq_weekly=None,
    isoe_daily=None,
    isoe_weekly=None,
    usdtusdc_daily=None,
    usdtusdc_weekly=None,
    d1_candles=D1_CANDLES_DEFAULT,
):
    current_d1 = d1_candles
    while True:
        df_d = fetch_data_tv(symbol, "1d", n_candles=current_d1)
        if df_d is None:
            return None
        df_d = normalize_index(df_d)
        if len(df_d) < 80 and current_d1 < D1_CANDLES_MAX:
            next_d1 = min(current_d1 * 2, D1_CANDLES_MAX)
            if next_d1 == current_d1:
                break
            current_d1 = next_d1
            continue
        break
    if df_d is None or df_d.empty:
        return None

    close_now = df_d["Close"].iloc[-1]
    horizon_pct = {}
    alignment_by_tf = {}
    psar_ok_by_tf = {}
    inter_ema_by_tf = {}
    inter_ema_mean_by_tf = {}
    psar_all_ok = True
    for name, window in HORIZONS.items():
        sample = df_d.tail(window).copy()
        if sample.empty or len(sample) < 20:
            horizon_pct[name] = np.nan
            alignment_by_tf[name] = "NONE"
            psar_ok_by_tf[name] = False
            inter_ema_by_tf[name] = {}
            inter_ema_mean_by_tf[name] = np.nan
            psar_all_ok = False
            continue

        psar_series = calculate_psar_series(sample, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
        if psar_series is None or not np.isfinite(psar_series[-1]):
            psar_ok_by_tf[name] = False
            psar_all_ok = False
        else:
            psar_ok = sample["Close"].iloc[-1] > psar_series[-1]
            psar_ok_by_tf[name] = bool(psar_ok)
            if not psar_ok:
                psar_all_ok = False

        ema_map = compute_ema_series(sample["Close"], EMA_LENGTHS)
        alignment_by_tf[name] = get_ema_alignment(ema_map, EMA_LENGTHS)
        inter_detail, inter_mean = compute_inter_ema_pct(ema_map, EMA_LENGTHS)
        inter_ema_by_tf[name] = inter_detail
        inter_ema_mean_by_tf[name] = inter_mean
        ema20 = sample["Close"].ewm(span=20, adjust=False).mean().iloc[-1]
        close_tf = sample["Close"].iloc[-1]
        if np.isfinite(ema20) and ema20 != 0 and np.isfinite(close_tf):
            horizon_pct[name] = (close_tf - ema20) * 100.0 / ema20
        else:
            horizon_pct[name] = np.nan

    # 1W: compute on true weekly candles (not 5 daily bars)
    df_w = fetch_data_tv(symbol, "1w", n_candles=W1_CANDLES_DEFAULT)
    if df_w is not None and not df_w.empty:
        df_w = normalize_index(df_w)
        if len(df_w) >= 20:
            psar_series_w = calculate_psar_series(df_w, PSAR_START, PSAR_INCREMENT, PSAR_MAXIMUM)
            if psar_series_w is None or not np.isfinite(psar_series_w[-1]):
                psar_ok_by_tf["1W"] = False
                psar_all_ok = False
            else:
                psar_ok_w = df_w["Close"].iloc[-1] > psar_series_w[-1]
                psar_ok_by_tf["1W"] = bool(psar_ok_w)
                if not psar_ok_w:
                    psar_all_ok = False

            ema_map_w = compute_ema_series(df_w["Close"], EMA_LENGTHS)
            alignment_by_tf["1W"] = get_ema_alignment(ema_map_w, EMA_LENGTHS)
            inter_detail_w, inter_mean_w = compute_inter_ema_pct(ema_map_w, EMA_LENGTHS)
            inter_ema_by_tf["1W"] = inter_detail_w
            inter_ema_mean_by_tf["1W"] = inter_mean_w
            ema20_w = df_w["Close"].ewm(span=20, adjust=False).mean().iloc[-1]
            close_w = df_w["Close"].iloc[-1]
            if np.isfinite(ema20_w) and ema20_w != 0 and np.isfinite(close_w):
                horizon_pct["1W"] = (close_w - ema20_w) * 100.0 / ema20_w
            else:
                horizon_pct["1W"] = np.nan
        else:
            alignment_by_tf["1W"] = "NONE"
            horizon_pct["1W"] = np.nan
            psar_ok_by_tf["1W"] = False
            inter_ema_by_tf["1W"] = {}
            inter_ema_mean_by_tf["1W"] = np.nan
            psar_all_ok = False
    else:
        alignment_by_tf["1W"] = "NONE"
        horizon_pct["1W"] = np.nan
        psar_ok_by_tf["1W"] = False
        inter_ema_by_tf["1W"] = {}
        inter_ema_mean_by_tf["1W"] = np.nan
        psar_all_ok = False

    valid_pcts = [v for v in horizon_pct.values() if np.isfinite(v)]
    mean_pct = float(np.mean(valid_pcts)) if valid_pcts else np.nan
    valid_inter_means = [v for v in inter_ema_mean_by_tf.values() if np.isfinite(v)]
    inter_ema_global_mean = float(np.mean(valid_inter_means)) if valid_inter_means else np.nan
    strict_bull_alignment = all(alignment_by_tf.get(tf) == "BULL" for tf in ["6M", "3M", "1M", "1W"])

    # Pre-filter: ACTION / LQQ ratio must be above EMA ribbon on Daily OR Weekly.
    ratio_pass_daily = {"pass": False, "above_all": False, "above_any": False, "ratio_last": np.nan}
    ratio_pass_weekly = {"pass": False, "above_all": False, "above_any": False, "ratio_last": np.nan}
    if lqq_daily is not None and not lqq_daily.empty:
        ratio_d = build_ratio_series_by_date(df_d, lqq_daily)
        if len(ratio_d) >= max(EMA_LENGTHS):
            ratio_pass_daily = ratio_above_ema_ribbon(ratio_d, EMA_LENGTHS)
    if lqq_weekly is not None and not lqq_weekly.empty and df_w is not None and not df_w.empty:
        ratio_w = build_ratio_series_by_date(df_w, lqq_weekly)
        if len(ratio_w) >= max(EMA_LENGTHS):
            ratio_pass_weekly = ratio_above_ema_ribbon(ratio_w, EMA_LENGTHS)
    ratio_lqq_filter_pass = bool(ratio_pass_daily["pass"] or ratio_pass_weekly["pass"])

    # Second ratio filter: ACTION / ISOE must also be above EMA ribbon on Daily OR Weekly.
    ratio_isoe_daily = {"pass": False, "above_all": False, "above_any": False, "ratio_last": np.nan}
    ratio_isoe_weekly = {"pass": False, "above_all": False, "above_any": False, "ratio_last": np.nan}
    if isoe_daily is not None and not isoe_daily.empty:
        ratio_d_isoe = build_ratio_series_by_date(df_d, isoe_daily)
        if len(ratio_d_isoe) >= max(EMA_LENGTHS):
            ratio_isoe_daily = ratio_above_ema_ribbon(ratio_d_isoe, EMA_LENGTHS)
    if isoe_weekly is not None and not isoe_weekly.empty and df_w is not None and not df_w.empty:
        ratio_w_isoe = build_ratio_series_by_date(df_w, isoe_weekly)
        if len(ratio_w_isoe) >= max(EMA_LENGTHS):
            ratio_isoe_weekly = ratio_above_ema_ribbon(ratio_w_isoe, EMA_LENGTHS)
    ratio_isoe_filter_pass = bool(ratio_isoe_daily["pass"] or ratio_isoe_weekly["pass"])

    # Final validation: USDTUSDC / ACTION must be below EMA ribbon on Daily AND Weekly.
    # No EMA alignment requirement: only the ratio level vs each EMA matters.
    ratio_usdt_daily = {"pass": False, "below_all": False, "below_any": False, "ratio_last": np.nan}
    ratio_usdt_weekly = {"pass": False, "below_all": False, "below_any": False, "ratio_last": np.nan}
    if usdtusdc_daily is not None and not usdtusdc_daily.empty:
        ratio_d_usdt = build_ratio_series_by_date(usdtusdc_daily, df_d)
        if len(ratio_d_usdt) >= max(EMA_LENGTHS):
            ratio_usdt_daily = ratio_below_ema_ribbon(ratio_d_usdt, EMA_LENGTHS)
    if usdtusdc_weekly is not None and not usdtusdc_weekly.empty and df_w is not None and not df_w.empty:
        ratio_w_usdt = build_ratio_series_by_date(usdtusdc_weekly, df_w)
        if len(ratio_w_usdt) >= max(EMA_LENGTHS):
            ratio_usdt_weekly = ratio_below_ema_ribbon(ratio_w_usdt, EMA_LENGTHS)
    ratio_usdt_filter_pass = bool(ratio_usdt_daily["pass"] and ratio_usdt_weekly["pass"])

    return {
        "symbol": symbol,
        "close": close_now,
        "pct_by_tf": horizon_pct,
        "alignment_by_tf": alignment_by_tf,
        "psar_ok_by_tf": psar_ok_by_tf,
        "inter_ema_by_tf": inter_ema_by_tf,
        "inter_ema_mean_by_tf": inter_ema_mean_by_tf,
        "inter_ema_global_mean": inter_ema_global_mean,
        "psar_all_ok": psar_all_ok,
        "strict_bull_alignment": strict_bull_alignment,
        "ratio_lqq_filter_pass": ratio_lqq_filter_pass,
        "ratio_lqq_daily": ratio_pass_daily,
        "ratio_lqq_weekly": ratio_pass_weekly,
        "ratio_isoe_filter_pass": ratio_isoe_filter_pass,
        "ratio_isoe_daily": ratio_isoe_daily,
        "ratio_isoe_weekly": ratio_isoe_weekly,
        "ratio_usdt_filter_pass": ratio_usdt_filter_pass,
        "ratio_usdt_daily": ratio_usdt_daily,
        "ratio_usdt_weekly": ratio_usdt_weekly,
        "avg_pct": mean_pct,
    }


def rank_key_cross(item):
    return (
        item.get("rank_cross", 10**9),
        item.get("rank_close", 10**9),
        item.get("rank_inter", 10**9),
        item["symbol"],
    )


def print_result(item):
    p = item["pct_by_tf"]
    a = item["alignment_by_tf"]
    s = item["psar_ok_by_tf"]
    im = item["inter_ema_mean_by_tf"]
    def fmt_pct(value):
        return f"{value:+.2f}%" if np.isfinite(value) else "N/A"
    def fmt_psar(ok):
        return "PSAR_OK" if ok else "PSAR_NO"
    print(
        f"{item['symbol']:>6} | RANK X:{item.get('rank_cross','-')} C:{item.get('rank_close','-')} I:{item.get('rank_inter','-')} | "
        f"AVG%: {item['avg_pct']:+.2f}% | INT_AVG: {fmt_pct(item.get('inter_ema_global_mean', np.nan))} | "
        f"6M:{fmt_pct(p['6M'])}/{a['6M']}/{fmt_psar(s.get('6M', False))} "
        f"3M:{fmt_pct(p['3M'])}/{a['3M']}/{fmt_psar(s.get('3M', False))} "
        f"1M:{fmt_pct(p['1M'])}/{a['1M']}/{fmt_psar(s.get('1M', False))} "
        f"1W:{fmt_pct(p['1W'])}/{a['1W']}/{fmt_psar(s.get('1W', False))} | "
        f"INT_TF 6M:{fmt_pct(im.get('6M', np.nan))} 3M:{fmt_pct(im.get('3M', np.nan))} "
        f"1M:{fmt_pct(im.get('1M', np.nan))} 1W:{fmt_pct(im.get('1W', np.nan))} | "
        f"R/LQQ D:{'OK' if item.get('ratio_lqq_daily', {}).get('pass') else 'NO'} "
        f"W:{'OK' if item.get('ratio_lqq_weekly', {}).get('pass') else 'NO'} | "
        f"R/ISOE D:{'OK' if item.get('ratio_isoe_daily', {}).get('pass') else 'NO'} "
        f"W:{'OK' if item.get('ratio_isoe_weekly', {}).get('pass') else 'NO'} | "
        f"USDT/A D:{'OK' if item.get('ratio_usdt_daily', {}).get('pass') else 'NO'} "
        f"W:{'OK' if item.get('ratio_usdt_weekly', {}).get('pass') else 'NO'} | "
        f"Close: {item['close']:.4f}"
    )


def now_iso():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def get_market_session_status(now_utc=None):
    # Trading sessions (cash market), holidays not modeled:
    # - US (NYSE/NASDAQ): 09:30-16:00 America/New_York
    # - Euronext Paris: 09:00-17:30 Europe/Paris
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    ny_tz = ZoneInfo("America/New_York")
    paris_tz = ZoneInfo("Europe/Paris")
    now_ny = now_utc.astimezone(ny_tz)
    now_paris = now_utc.astimezone(paris_tz)

    us_open = (
        now_ny.weekday() < 5
        and dt_time(9, 30) <= now_ny.time() < dt_time(16, 0)
    )
    eu_open = (
        now_paris.weekday() < 5
        and dt_time(9, 0) <= now_paris.time() < dt_time(17, 30)
    )
    return {
        "us_open": us_open,
        "eu_open": eu_open,
        "any_open": bool(us_open or eu_open),
        "ny_time": now_ny.strftime("%Y-%m-%d %H:%M:%S"),
        "paris_time": now_paris.strftime("%Y-%m-%d %H:%M:%S"),
    }


def default_portfolio_state():
    return {
        "version": 1,
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "run_count": 0,
        "cash": TOTAL_CAPITAL_USD,
        "positions": {},  # symbol -> {"shares": int, "avg_cost": float}
        "realized_pnl": 0.0,
        "fees_paid": 0.0,
        "turnover_notional": 0.0,
        "last_equity": TOTAL_CAPITAL_USD,
        "history": [],
        "benchmarks": {
            "initial_capital": TOTAL_CAPITAL_USD,
            "isoe_100": {
                "units": None,
                "equity": TOTAL_CAPITAL_USD,
                "fees_paid": 0.0,
                "entry_fee": 0.0,
            },
            "lqq_100": {
                "units": None,
                "equity": TOTAL_CAPITAL_USD,
                "fees_paid": 0.0,
                "entry_fee": 0.0,
            },
            "regime_switch": {
                "active": ISOE_SYMBOL,
                "units": None,
                "equity": TOTAL_CAPITAL_USD,
                "fees_paid": 0.0,
                "entry_fee": 0.0,
                "switch_count": 0,
                "last_switch_fee": 0.0,
            },
        },
    }


def load_portfolio_state():
    if not os.path.exists(PORTFOLIO_STATE_FILE):
        return default_portfolio_state()
    try:
        with open(PORTFOLIO_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return default_portfolio_state()
        base = default_portfolio_state()
        base.update(data)
        if "positions" not in base or not isinstance(base["positions"], dict):
            base["positions"] = {}
        if "history" not in base or not isinstance(base["history"], list):
            base["history"] = []
        if "benchmarks" not in base or not isinstance(base["benchmarks"], dict):
            base["benchmarks"] = default_portfolio_state()["benchmarks"]
        return base
    except Exception:
        return default_portfolio_state()


def save_portfolio_state(state):
    state["updated_at"] = now_iso()
    try:
        with open(PORTFOLIO_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as exc:
        print(f"Warning: failed to save portfolio state: {exc}")


def load_scan_state():
    if not os.path.exists(SCAN_STATE_FILE):
        return {"last_failed_symbols": [], "updated_at": None}
    try:
        with open(SCAN_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"last_failed_symbols": [], "updated_at": None}
        if "last_failed_symbols" not in data or not isinstance(data["last_failed_symbols"], list):
            data["last_failed_symbols"] = []
        return data
    except Exception:
        return {"last_failed_symbols": [], "updated_at": None}


def save_scan_state(failed_symbols):
    payload = {
        "updated_at": now_iso(),
        "last_failed_symbols": sorted(set(failed_symbols)),
    }
    try:
        with open(SCAN_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception as exc:
        print(f"Warning: failed to save scan state: {exc}")


def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        data = response.json()
        return bool(data.get("ok", False))
    except Exception:
        return False


def build_telegram_portfolio_report(
    state,
    equity_before,
    equity_after,
    unrealized,
    regime,
    n_lqq,
    n_isoe,
    price_map,
    benchmark_summary=None,
):
    baseline = TOTAL_CAPITAL_USD
    delta = equity_after - baseline
    equity_icon = "🟢" if delta > 0 else ("🔴" if delta < 0 else "🟡")
    lines = ["<b>NDX100 Portfolio</b>", "", "<b>Positions:</b>"]
    positions = state.get("positions", {})
    if not positions:
        lines.append("(none)")
    else:
        for sym in sorted(positions.keys()):
            p = positions[sym]
            shares = int(p.get("shares", 0) or 0)
            avg_cost = float(p.get("avg_cost", 0.0) or 0.0)
            last_px = price_map.get(sym, np.nan)
            status_icon = "⚪"
            if np.isfinite(last_px) and last_px > avg_cost:
                status_icon = "🟢"
            elif np.isfinite(last_px) and last_px < avg_cost:
                status_icon = "🔴"
            short_sym = "ISOE" if sym == ISOE_SYMBOL else sym
            if np.isfinite(last_px):
                pnl_pos = (last_px - avg_cost) * shares
                lines.append(f"{status_icon} {short_sym} ({shares}) {pnl_pos:+.2f}")
            else:
                lines.append(f"{status_icon} {short_sym} ({shares}) N/A")
    lines.append(f"Cash: {float(state.get('cash', 0.0)):.2f}")
    lines.append(f"{equity_icon} Equity: {equity_after:.2f}")
    lines.append("")
    lines.append(f"Realized: {float(state.get('realized_pnl', 0.0)):+.2f}")
    lines.append(f"Unrealized: {unrealized:+.2f}")
    lines.append(f"Fees: {float(state.get('fees_paid', 0.0)):.2f}")
    if isinstance(benchmark_summary, dict) and benchmark_summary:
        lines.append("")
        lines.append("<b>BENCHMARKS</b>")
        p1 = benchmark_summary.get("isoe_100", {})
        p2 = benchmark_summary.get("lqq_100", {})
        p3 = benchmark_summary.get("regime_switch", {})
        def icon_for_ret(ret_pct):
            if not np.isfinite(ret_pct):
                return "⚪"
            if ret_pct > 0:
                return "🟢"
            if ret_pct < 0:
                return "🔴"
            return "⚪"
        if p1:
            ret = float(p1.get("ret_pct", 0.0))
            lines.append(f"{icon_for_ret(ret)} ISOE ({ret:+.2f}%)")
        if p2:
            ret = float(p2.get("ret_pct", 0.0))
            lines.append(f"{icon_for_ret(ret)} LQQ ({ret:+.2f}%)")
        if p3:
            ret = float(p3.get("ret_pct", 0.0))
            lines.append(f"{icon_for_ret(ret)} Switch ({ret:+.2f}%)")
    return "\n".join(lines)


def _to_float(value, default=np.nan):
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if np.isfinite(v) else default


def _bench_perf(equity, initial):
    eq = _to_float(equity, np.nan)
    ini = _to_float(initial, np.nan)
    if not np.isfinite(eq) or not np.isfinite(ini) or ini <= 0:
        return {"equity": np.nan, "pnl": np.nan, "ret_pct": np.nan}
    pnl = eq - ini
    return {"equity": eq, "pnl": pnl, "ret_pct": (pnl * 100.0 / ini)}


def update_benchmark_trackers(state, price_map, regime, allow_switch=True):
    benchmarks = state.get("benchmarks", {})
    initial = _to_float(benchmarks.get("initial_capital", TOTAL_CAPITAL_USD), TOTAL_CAPITAL_USD)
    if not np.isfinite(initial) or initial <= 0:
        initial = TOTAL_CAPITAL_USD
    benchmarks["initial_capital"] = initial

    isoe_px = _to_float(price_map.get(ISOE_SYMBOL, np.nan), np.nan)
    lqq_px = _to_float(price_map.get(LQQ_SYMBOL, np.nan), np.nan)

    isoe_100 = benchmarks.get(
        "isoe_100",
        {"units": None, "equity": initial, "fees_paid": 0.0, "entry_fee": 0.0},
    )
    if np.isfinite(isoe_px) and isoe_px > 0:
        units_isoe = _to_float(isoe_100.get("units", np.nan), np.nan)
        if not np.isfinite(units_isoe):
            fee_entry = estimate_fee_for_notional(initial, symbol=ISOE_SYMBOL, side="BUY")
            investable = max(0.0, initial - fee_entry)
            isoe_100["units"] = investable / isoe_px
            isoe_100["fees_paid"] = _to_float(isoe_100.get("fees_paid", 0.0), 0.0) + fee_entry
            isoe_100["entry_fee"] = fee_entry
        isoe_100["equity"] = _to_float(isoe_100["units"], 0.0) * isoe_px
    benchmarks["isoe_100"] = isoe_100

    lqq_100 = benchmarks.get(
        "lqq_100",
        {"units": None, "equity": initial, "fees_paid": 0.0, "entry_fee": 0.0},
    )
    if np.isfinite(lqq_px) and lqq_px > 0:
        units_lqq = _to_float(lqq_100.get("units", np.nan), np.nan)
        if not np.isfinite(units_lqq):
            fee_entry = estimate_fee_for_notional(initial, symbol=LQQ_SYMBOL, side="BUY")
            investable = max(0.0, initial - fee_entry)
            lqq_100["units"] = investable / lqq_px
            lqq_100["fees_paid"] = _to_float(lqq_100.get("fees_paid", 0.0), 0.0) + fee_entry
            lqq_100["entry_fee"] = fee_entry
        lqq_100["equity"] = _to_float(lqq_100["units"], 0.0) * lqq_px
    benchmarks["lqq_100"] = lqq_100

    sw = benchmarks.get(
        "regime_switch",
        {
            "active": ISOE_SYMBOL,
            "units": None,
            "equity": initial,
            "fees_paid": 0.0,
            "entry_fee": 0.0,
            "switch_count": 0,
            "last_switch_fee": 0.0,
        },
    )
    active = sw.get("active", ISOE_SYMBOL)
    units = _to_float(sw.get("units", np.nan), np.nan)
    equity = _to_float(sw.get("equity", initial), initial)
    switch_fees_paid = _to_float(sw.get("fees_paid", 0.0), 0.0)
    switch_entry_fee = _to_float(sw.get("entry_fee", 0.0), 0.0)
    switch_count = int(_to_float(sw.get("switch_count", 0), 0))
    last_switch_fee = 0.0

    if regime == "LQQ_DOMINANT":
        desired_active = LQQ_SYMBOL
    elif regime == "ISOE_DOMINANT":
        desired_active = ISOE_SYMBOL
    else:
        desired_active = active if active in {ISOE_SYMBOL, LQQ_SYMBOL} else ISOE_SYMBOL

    if allow_switch:
        target_active = desired_active
    else:
        target_active = active if active in {ISOE_SYMBOL, LQQ_SYMBOL} else ISOE_SYMBOL

    px_by_symbol = {ISOE_SYMBOL: isoe_px, LQQ_SYMBOL: lqq_px}
    if not np.isfinite(units):
        # First initialization is treated as a real entry (with fees),
        # using current dominant regime to set the initial side.
        init_symbol = desired_active if desired_active in px_by_symbol else ISOE_SYMBOL
        start_px = px_by_symbol.get(init_symbol, np.nan)
        if np.isfinite(start_px) and start_px > 0:
            fee_entry = estimate_fee_for_notional(initial, symbol=init_symbol, side="BUY")
            investable = max(0.0, initial - fee_entry)
            units = investable / start_px
            active = init_symbol
            equity = units * start_px
            switch_fees_paid += fee_entry
            switch_entry_fee += fee_entry

    if np.isfinite(units) and active in px_by_symbol:
        active_px = px_by_symbol.get(active, np.nan)
        if np.isfinite(active_px) and active_px > 0:
            equity = units * active_px
            if target_active != active:
                target_px = px_by_symbol.get(target_active, np.nan)
                if np.isfinite(target_px) and target_px > 0:
                    fee_sell = estimate_fee_for_notional(equity, symbol=active, side="SELL")
                    after_sell = max(0.0, equity - fee_sell)
                    fee_buy = estimate_fee_for_notional(after_sell, symbol=target_active, side="BUY")
                    investable = max(0.0, after_sell - fee_buy)
                    switch_fee = (equity - investable)
                    switch_fees_paid += switch_fee
                    switch_count += 1
                    last_switch_fee = switch_fee
                    units = investable / target_px if target_px > 0 else 0.0
                    active = target_active
                    equity = units * target_px

    sw["active"] = active
    sw["units"] = units
    sw["equity"] = equity
    sw["fees_paid"] = switch_fees_paid
    sw["entry_fee"] = switch_entry_fee
    sw["switch_count"] = switch_count
    sw["last_switch_fee"] = last_switch_fee
    benchmarks["regime_switch"] = sw
    state["benchmarks"] = benchmarks

    summary = {
        "initial": initial,
        "isoe_100": _bench_perf(benchmarks.get("isoe_100", {}).get("equity", np.nan), initial),
        "lqq_100": _bench_perf(benchmarks.get("lqq_100", {}).get("equity", np.nan), initial),
        "regime_switch": _bench_perf(sw.get("equity", np.nan), initial),
    }
    summary["isoe_100"]["fees_paid"] = benchmarks.get("isoe_100", {}).get("fees_paid", 0.0)
    summary["isoe_100"]["entry_fee"] = benchmarks.get("isoe_100", {}).get("entry_fee", 0.0)
    summary["lqq_100"]["fees_paid"] = benchmarks.get("lqq_100", {}).get("fees_paid", 0.0)
    summary["lqq_100"]["entry_fee"] = benchmarks.get("lqq_100", {}).get("entry_fee", 0.0)
    summary["regime_switch"]["active"] = sw.get("active", ISOE_SYMBOL)
    summary["regime_switch"]["fees_paid"] = sw.get("fees_paid", 0.0)
    summary["regime_switch"]["entry_fee"] = sw.get("entry_fee", 0.0)
    summary["regime_switch"]["switch_count"] = sw.get("switch_count", 0)
    summary["regime_switch"]["last_switch_fee"] = sw.get("last_switch_fee", 0.0)
    return summary

def fee_for_notional(notional):
    if not np.isfinite(notional) or notional <= 0:
        return 0.0
    return FEE_FIXED_USD + (FEE_PCT * notional)


def get_symbol_exchange(symbol):
    s = str(symbol or "").strip().upper()
    if ":" in s:
        return s.split(":", 1)[0]
    return str(TV_STOCK_PREFIX).strip().upper()


def fee_for_trade(symbol, qty, price, side):
    if (
        not np.isfinite(qty)
        or not np.isfinite(price)
        or qty <= 0
        or price <= 0
    ):
        return 0.0
    qty_i = int(qty)
    notional = qty_i * float(price)
    side_u = str(side or "").strip().upper()

    if FEE_MODEL == "SIMPLE":
        return fee_for_notional(notional)

    ex = get_symbol_exchange(symbol)
    us_exchanges = {"NASDAQ", "NYSE", "AMEX", "ARCA", "BATS", "IEX"}
    if ex in us_exchanges:
        commission = max(IBKR_US_MIN_ORDER_USD, IBKR_US_PER_SHARE_USD * qty_i)
        commission = min(commission, IBKR_US_MAX_PCT_NOTIONAL * notional)
        regulatory = 0.0
        if side_u == "SELL":
            taf = min(IBKR_US_TAF_MAX_USD, IBKR_US_TAF_PER_SHARE_USD * qty_i)
            sec = max(0.0, IBKR_US_SEC_SELL_PCT) * notional
            regulatory = taf + sec
        return commission + regulatory

    # Euronext approximation in USD (min in EUR converted to USD).
    if ex in {"EURONEXT"}:
        min_usd = max(0.0, IBKR_EU_MIN_ORDER_EUR) * max(0.0, IBKR_EURUSD)
        return max(min_usd, max(0.0, IBKR_EU_PCT_NOTIONAL) * notional)

    # Fallback to legacy if exchange is unknown.
    return fee_for_notional(notional)


def estimate_fee_for_notional(notional, symbol=None, side="BUY"):
    if not np.isfinite(notional) or notional <= 0:
        return 0.0
    if FEE_MODEL == "SIMPLE":
        return fee_for_notional(notional)
    ex = get_symbol_exchange(symbol)
    if ex == "EURONEXT":
        min_usd = max(0.0, IBKR_EU_MIN_ORDER_EUR) * max(0.0, IBKR_EURUSD)
        return max(min_usd, max(0.0, IBKR_EU_PCT_NOTIONAL) * notional)
    # US approximation for reserve sizing when qty/price are not known.
    base = max(IBKR_US_MIN_ORDER_USD, 0.001 * notional)
    if str(side or "").strip().upper() == "SELL":
        base += min(IBKR_US_TAF_MAX_USD, IBKR_US_TAF_PER_SHARE_USD * max(1, int(notional / 100.0)))
    return base


def min_fee_reserve_cash():
    # Reserve enough cash for future operations:
    # 10 buys + 10 sells (20 trades) at a reference notional.
    ref_notional = max(0.0, FEE_RESERVE_NOTIONAL_REF)
    fee_buy = estimate_fee_for_notional(ref_notional, side="BUY")
    fee_sell = estimate_fee_for_notional(ref_notional, side="SELL")
    return max(0.0, FEE_RESERVE_TRADE_COUNT * (fee_buy + fee_sell))


def max_affordable_shares(cash, price, symbol, reserve_cash=0.0):
    if not np.isfinite(cash) or not np.isfinite(price) or cash <= 0 or price <= 0:
        return 0
    available = cash - max(0.0, reserve_cash)
    if available <= 0:
        return 0
    qty = int(available // price)
    while qty > 0:
        fee = fee_for_trade(symbol, qty, price, "BUY")
        if (qty * price) + fee <= available:
            return qty
        qty -= 1
    return 0


def get_latest_close(symbol):
    df = fetch_data_tv(symbol, "1d", n_candles=10)
    if df is None or df.empty:
        return np.nan
    df = normalize_index(df)
    return float(df["Close"].iloc[-1]) if not df.empty else np.nan


def compute_equity(state, price_map):
    equity = float(state.get("cash", 0.0))
    for sym, pos in state.get("positions", {}).items():
        shares = int(pos.get("shares", 0) or 0)
        px = price_map.get(sym, np.nan)
        if shares > 0 and np.isfinite(px) and px > 0:
            equity += shares * px
    return equity


def compute_unrealized_pnl(state, price_map):
    unrealized = 0.0
    for sym, pos in state.get("positions", {}).items():
        shares = int(pos.get("shares", 0) or 0)
        avg_cost = float(pos.get("avg_cost", 0.0) or 0.0)
        px = price_map.get(sym, np.nan)
        if shares > 0 and np.isfinite(px) and px > 0:
            unrealized += (px - avg_cost) * shares
    return unrealized


def build_target_values(equity, regime, isoe_candidates, lqq_candidates, price_map):
    target_values = {}
    benchmark_symbol = None
    if regime == "ISOE_DOMINANT":
        benchmark_symbol = ISOE_SYMBOL
        selected = isoe_candidates
    elif regime == "LQQ_DOMINANT":
        benchmark_symbol = LQQ_SYMBOL
        selected = lqq_candidates
    else:
        selected = []
    # Per-survivor allocation rule:
    # - default allocation per name = ALLOC_PER_SURVIVOR_USD
    # - if price > ALLOC_PER_SURVIVOR_USD, target at least 1 share (notional ~= price)
    raw_basket_budget = 0.0
    for item in selected:
        sym = item["symbol"]
        px = price_map.get(sym, np.nan)
        if np.isfinite(px) and px > ALLOC_PER_SURVIVOR_USD:
            alloc = float(px)
        else:
            alloc = ALLOC_PER_SURVIVOR_USD
        target_values[sym] = max(0.0, alloc)
        raw_basket_budget += target_values[sym]

    basket_budget = min(raw_basket_budget, max(0.0, equity))
    bench_budget = max(0.0, equity - basket_budget) if benchmark_symbol else 0.0
    if benchmark_symbol and bench_budget > 0:
        target_values[benchmark_symbol] = target_values.get(benchmark_symbol, 0.0) + bench_budget
    return target_values, benchmark_symbol, basket_budget, bench_budget


def rebalance_portfolio(state, price_map, target_values):
    trades = []
    positions = state.get("positions", {})
    cash = float(state.get("cash", 0.0))
    reserve_cash = min_fee_reserve_cash()
    realized = float(state.get("realized_pnl", 0.0))
    fees_paid = float(state.get("fees_paid", 0.0))
    turnover = float(state.get("turnover_notional", 0.0))

    # Build integer target shares with available market prices.
    target_shares = {}
    for sym, target_usd in target_values.items():
        px = price_map.get(sym, np.nan)
        if not np.isfinite(px) or px <= 0 or target_usd <= 0:
            target_shares[sym] = 0
        else:
            target_shares[sym] = int(target_usd // px)

    # Existing positions that are not in target should go to zero.
    for sym in list(positions.keys()):
        if sym not in target_shares:
            target_shares[sym] = 0

    # Sells first to free cash.
    for sym in sorted(target_shares.keys()):
        pos = positions.get(sym, {"shares": 0, "avg_cost": 0.0})
        current_shares = int(pos.get("shares", 0) or 0)
        tgt_shares = int(target_shares.get(sym, 0) or 0)
        if current_shares <= tgt_shares:
            continue
        px = price_map.get(sym, np.nan)
        if not np.isfinite(px) or px <= 0:
            continue
        qty = current_shares - tgt_shares
        notional = qty * px
        fee = fee_for_trade(sym, qty, px, "SELL")
        cash += (notional - fee)
        avg_cost = float(pos.get("avg_cost", 0.0) or 0.0)
        realized += (px - avg_cost) * qty - fee
        fees_paid += fee
        turnover += notional
        new_shares = current_shares - qty
        if new_shares > 0:
            positions[sym]["shares"] = new_shares
        else:
            positions.pop(sym, None)
        trades.append(
            {"side": "SELL", "symbol": sym, "qty": qty, "price": px, "notional": notional, "fee": fee}
        )

    # Buys second (whole shares only).
    for sym in sorted(target_shares.keys()):
        tgt_shares = int(target_shares.get(sym, 0) or 0)
        if tgt_shares <= 0:
            continue
        pos = positions.get(sym, {"shares": 0, "avg_cost": 0.0})
        current_shares = int(pos.get("shares", 0) or 0)
        need = tgt_shares - current_shares
        if need <= 0:
            continue
        px = price_map.get(sym, np.nan)
        if not np.isfinite(px) or px <= 0:
            continue
        max_qty = max_affordable_shares(cash, px, sym, reserve_cash=reserve_cash)
        qty = min(need, max_qty)
        if qty <= 0:
            continue
        notional = qty * px
        fee = fee_for_trade(sym, qty, px, "BUY")
        total_cost = notional + fee
        if cash - total_cost < reserve_cash:
            continue
        cash -= total_cost
        fees_paid += fee
        turnover += notional

        old_shares = current_shares
        old_avg = float(pos.get("avg_cost", 0.0) or 0.0)
        new_shares = old_shares + qty
        new_avg = ((old_avg * old_shares) + total_cost) / new_shares
        positions[sym] = {"shares": new_shares, "avg_cost": new_avg}
        trades.append(
            {"side": "BUY", "symbol": sym, "qty": qty, "price": px, "notional": notional, "fee": fee}
        )

    state["positions"] = positions
    state["cash"] = cash
    state["realized_pnl"] = realized
    state["fees_paid"] = fees_paid
    state["turnover_notional"] = turnover
    return trades


def scan_symbols_once(
    symbols,
    workers,
    lqq_daily,
    lqq_weekly,
    isoe_daily,
    isoe_weekly,
    usdtusdc_daily,
    usdtusdc_weekly,
    d1_candles,
):
    results = {}
    errors = {}
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
        future_map = {
            executor.submit(
                analyze_symbol,
                sym,
                lqq_daily,
                lqq_weekly,
                isoe_daily,
                isoe_weekly,
                usdtusdc_daily,
                usdtusdc_weekly,
                d1_candles,
            ): sym
            for sym in symbols
        }
        for future in as_completed(future_map):
            sym = future_map[future]
            try:
                res = future.result()
            except Exception as exc:
                errors[sym] = str(exc)
                continue
            if res:
                results[sym] = res
            else:
                errors[sym] = "No data or insufficient history"
    return results, errors


def main():
    parser = argparse.ArgumentParser(description="Cross-rank Nasdaq-100 by AVG% Close-EMA20 and AVG% inter-EMA.")
    parser.add_argument("--symbol", help="Single ticker, e.g. AAPL")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--d1-candles", type=int, default=D1_CANDLES_DEFAULT)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--retry-attempts", type=int, default=2, help="Additional retries on failed symbols.")
    parser.add_argument("--rerun-failed-only", action="store_true", help="Scan only symbols that failed in previous run.")
    args = parser.parse_args()

    if os.getenv("GITHUB_ACTIONS", "").lower() == "true":
        args.workers = min(args.workers, 2)

    if args.symbol:
        symbols = [args.symbol.strip().upper()]
        source = "manual"
    elif args.rerun_failed_only:
        scan_state = load_scan_state()
        failed_only = [str(s).strip().upper() for s in scan_state.get("last_failed_symbols", []) if str(s).strip()]
        if not failed_only:
            print("No failed symbols found from previous run. Nothing to scan.")
            return
        symbols = sorted(set(failed_only))
        source = "failed_only"
    else:
        symbols, source = fetch_nasdaq100_symbols()

    print(f"Universe source: {source} | Symbols loaded: {len(symbols)}")
    print("Analyzing EMA alignments...")

    # Fetch LQQ benchmark once for ratio pre-filter.
    lqq_daily = fetch_data_tv(LQQ_SYMBOL, "1d", n_candles=D1_CANDLES_MAX)
    lqq_weekly = fetch_data_tv(LQQ_SYMBOL, "1w", n_candles=W1_CANDLES_DEFAULT)
    isoe_daily = fetch_data_tv(ISOE_SYMBOL, "1d", n_candles=D1_CANDLES_MAX)
    isoe_weekly = fetch_data_tv(ISOE_SYMBOL, "1w", n_candles=W1_CANDLES_DEFAULT)
    usdtusdc_daily = fetch_data_tv(USDTUSDC_SYMBOL, "1d", n_candles=D1_CANDLES_MAX)
    usdtusdc_weekly = fetch_data_tv(USDTUSDC_SYMBOL, "1w", n_candles=W1_CANDLES_DEFAULT)
    lqq_daily = normalize_index(lqq_daily) if lqq_daily is not None else None
    lqq_weekly = normalize_index(lqq_weekly) if lqq_weekly is not None else None
    isoe_daily = normalize_index(isoe_daily) if isoe_daily is not None else None
    isoe_weekly = normalize_index(isoe_weekly) if isoe_weekly is not None else None
    usdtusdc_daily = normalize_index(usdtusdc_daily) if usdtusdc_daily is not None else None
    usdtusdc_weekly = normalize_index(usdtusdc_weekly) if usdtusdc_weekly is not None else None
    lqq_d_n = len(lqq_daily) if lqq_daily is not None else 0
    lqq_w_n = len(lqq_weekly) if lqq_weekly is not None else 0
    isoe_d_n = len(isoe_daily) if isoe_daily is not None else 0
    isoe_w_n = len(isoe_weekly) if isoe_weekly is not None else 0
    usdt_d_n = len(usdtusdc_daily) if usdtusdc_daily is not None else 0
    usdt_w_n = len(usdtusdc_weekly) if usdtusdc_weekly is not None else 0
    print(f"LQQ source: {LQQ_SYMBOL} | D bars: {lqq_d_n} | W bars: {lqq_w_n}")
    print(f"ISOE source: {ISOE_SYMBOL} | D bars: {isoe_d_n} | W bars: {isoe_w_n}")
    print(f"USDTUSDC source: {USDTUSDC_SYMBOL} | D bars: {usdt_d_n} | W bars: {usdt_w_n}")

    results = []
    errors = []
    start = time.time()
    results_map, errors_map = scan_symbols_once(
        symbols,
        args.workers,
        lqq_daily,
        lqq_weekly,
        isoe_daily,
        isoe_weekly,
        usdtusdc_daily,
        usdtusdc_weekly,
        args.d1_candles,
    )

    # Retry failed symbols with reduced workers at each attempt.
    retry_attempts = max(0, int(args.retry_attempts))
    retry_workers = max(1, int(args.workers))
    for attempt in range(1, retry_attempts + 1):
        failed_symbols = sorted(errors_map.keys())
        if not failed_symbols:
            break
        retry_workers = max(1, retry_workers // 2)
        print(
            f"Retry {attempt}/{retry_attempts}: "
            f"{len(failed_symbols)} failed symbols with workers={retry_workers}"
        )
        retry_results, retry_errors = scan_symbols_once(
            failed_symbols,
            retry_workers,
            lqq_daily,
            lqq_weekly,
            isoe_daily,
            isoe_weekly,
            usdtusdc_daily,
            usdtusdc_weekly,
            args.d1_candles,
        )
        for sym, res in retry_results.items():
            results_map[sym] = res
            errors_map.pop(sym, None)
        for sym, err in retry_errors.items():
            errors_map[sym] = err

    results = list(results_map.values())
    errors = sorted(errors_map.items(), key=lambda x: x[0])
    save_scan_state([sym for sym, _ in errors])

    psar_pass = [r for r in results if r.get("psar_all_ok")]
    bull_align_pass = [r for r in psar_pass if r.get("strict_bull_alignment")]
    avg_pct_pass = [r for r in bull_align_pass if np.isfinite(r.get("avg_pct", np.nan)) and r.get("avg_pct", 0) > 0]
    inter_pass = [
        r
        for r in avg_pct_pass
        if np.isfinite(r.get("inter_ema_global_mean", np.nan)) and r.get("inter_ema_global_mean", 0) > 0
    ]
    lqq_pass = [r for r in inter_pass if r.get("ratio_lqq_filter_pass")]
    aligned_and_positive = [r for r in lqq_pass if r.get("ratio_isoe_filter_pass")]
    final_validated = [r for r in aligned_and_positive if r.get("ratio_usdt_filter_pass")]

    print("\nFilter funnel:")
    print(f"  PSAR all TF OK: {len(psar_pass)}")
    print(f"  + Strict 8EMA bullish 4/4: {len(bull_align_pass)}")
    print(f"  + AVG% Close/EMA20 > 0: {len(avg_pct_pass)}")
    print(f"  + AVG% inter-EMA > 0: {len(inter_pass)}")
    print(f"  + Ratio ACTION/LQQ above EMA ribbon (D or W): {len(lqq_pass)}")
    print(f"  + Ratio ACTION/ISOE above EMA ribbon (D or W): {len(aligned_and_positive)}")
    print(f"  + Ratio USDTUSDC/ACTION below EMA ribbon (D and W): {len(final_validated)}")
    ranked_close = sorted(final_validated, key=lambda x: (-x["avg_pct"], x["symbol"]))
    ranked_inter = sorted(final_validated, key=lambda x: (-x["inter_ema_global_mean"], x["symbol"]))
    close_pos = {item["symbol"]: i + 1 for i, item in enumerate(ranked_close)}
    inter_pos = {item["symbol"]: i + 1 for i, item in enumerate(ranked_inter)}

    for item in final_validated:
        item["rank_close"] = close_pos[item["symbol"]]
        item["rank_inter"] = inter_pos[item["symbol"]]
        item["rank_cross"] = item["rank_close"] + item["rank_inter"]

    ranked = sorted(final_validated, key=rank_key_cross)
    top_n = ranked[: max(1, min(args.top, len(ranked)))] if ranked else []

    print(f"\nRanked results (top {len(top_n)}):")
    for item in top_n:
        print_result(item)

    # Dynamic portfolio engine (persistent state + real-world constraints):
    # - integer shares only
    # - buy/sell fees
    # - rolling entries/exits driven by screener results
    n_lqq = len(lqq_pass)
    n_isoe = len(final_validated)
    regime = "NONE"

    if n_isoe < n_lqq:
        regime = "ISOE_DOMINANT"
    elif n_lqq < n_isoe:
        regime = "LQQ_DOMINANT"
    # Portfolio state load
    state = load_portfolio_state()
    state["run_count"] = int(state.get("run_count", 0) or 0) + 1
    market_status = get_market_session_status()
    rebalance_allowed = bool(market_status.get("any_open", False))

    # Candidate sets for the two regimes.
    isoe_candidates = sorted(final_validated, key=rank_key_cross)
    lqq_candidates = sorted(lqq_pass, key=lambda x: (-x["avg_pct"], -x["inter_ema_global_mean"], x["symbol"]))

    # Price map from current scan + benchmark closes + current holdings fallback.
    price_map = {}
    for item in results:
        sym = item.get("symbol")
        px = item.get("close", np.nan)
        if sym and np.isfinite(px) and px > 0:
            price_map[sym] = float(px)
    if lqq_daily is not None and not lqq_daily.empty:
        price_map[LQQ_SYMBOL] = float(lqq_daily["Close"].iloc[-1])
    if isoe_daily is not None and not isoe_daily.empty:
        price_map[ISOE_SYMBOL] = float(isoe_daily["Close"].iloc[-1])
    for held_sym in state.get("positions", {}).keys():
        if held_sym not in price_map:
            px = get_latest_close(held_sym)
            if np.isfinite(px) and px > 0:
                price_map[held_sym] = float(px)

    equity_before = compute_equity(state, price_map)
    if regime == "NONE":
        target_values = {}
        benchmark_symbol = None
        basket_budget = 0.0
        bench_budget = 0.0
        trades = []
    else:
        target_values, benchmark_symbol, basket_budget, bench_budget = build_target_values(
            equity_before, regime, isoe_candidates, lqq_candidates, price_map
        )
        if rebalance_allowed:
            trades = rebalance_portfolio(state, price_map, target_values)
        else:
            trades = []
    equity_after = compute_equity(state, price_map)
    unrealized = compute_unrealized_pnl(state, price_map)
    benchmark_summary = update_benchmark_trackers(
        state=state,
        price_map=price_map,
        regime=regime,
        allow_switch=rebalance_allowed,
    )
    state["last_equity"] = equity_after
    state["history"].append(
        {
            "ts": now_iso(),
            "run": state["run_count"],
            "regime": regime,
            "n_lqq": n_lqq,
            "n_isoe": n_isoe,
            "equity_before": equity_before,
            "equity_after": equity_after,
            "cash": state.get("cash", 0.0),
            "trades": len(trades),
            "rebalance_allowed": rebalance_allowed,
            "us_open": bool(market_status.get("us_open", False)),
            "eu_open": bool(market_status.get("eu_open", False)),
            "bench_isoe_100": benchmark_summary.get("isoe_100", {}).get("equity", np.nan),
            "bench_lqq_100": benchmark_summary.get("lqq_100", {}).get("equity", np.nan),
            "bench_switch": benchmark_summary.get("regime_switch", {}).get("equity", np.nan),
        }
    )
    state["history"] = state["history"][-400:]
    save_portfolio_state(state)

    print("\nPortfolio simulation:")
    print(f"  State file: {PORTFOLIO_STATE_FILE}")
    print(f"  n_lqq={n_lqq} | n_isoe={n_isoe} | regime={regime}")
    print(
        f"  Equity before=${equity_before:.2f} | after=${equity_after:.2f} | "
        f"cash=${state.get('cash', 0.0):.2f}"
    )
    print(f"  Fee reserve (10 buys + 10 sells): ${min_fee_reserve_cash():.2f}")
    print(
        f"  RealizedPnL=${state.get('realized_pnl', 0.0):.2f} | "
        f"Unrealizedâ‰ˆ${unrealized:.2f} | FeesPaid=${state.get('fees_paid', 0.0):.2f}"
    )
    print(
        f"  Target basket=${basket_budget:.2f} ({ALLOC_PER_SURVIVOR_USD:.2f} per survivor, >=1 share if price>{ALLOC_PER_SURVIVOR_USD:.2f}) | "
        f"target benchmark={benchmark_symbol if benchmark_symbol else 'N/A'} ${bench_budget:.2f}"
    )
    print(
        f"  Market status: US={'OPEN' if market_status.get('us_open') else 'CLOSED'} "
        f"(NY {market_status.get('ny_time', 'N/A')}) | "
        f"Euronext={'OPEN' if market_status.get('eu_open') else 'CLOSED'} "
        f"(Paris {market_status.get('paris_time', 'N/A')})"
    )
    if not rebalance_allowed and regime != "NONE":
        print("  Rebalance blocked: both US and Euronext cash sessions are closed.")
    print("  Benchmark trackers (initial $10000):")
    b_isoe = benchmark_summary.get("isoe_100", {})
    b_lqq = benchmark_summary.get("lqq_100", {})
    b_sw = benchmark_summary.get("regime_switch", {})
    print(
        f"    - ISOE 100%: ${b_isoe.get('equity', np.nan):.2f} "
        f"({b_isoe.get('pnl', np.nan):+.2f} | {b_isoe.get('ret_pct', np.nan):+.2f}%)"
    )
    print(
        f"      fees=${b_isoe.get('fees_paid', 0.0):.2f} "
        f"| entry_fee=${b_isoe.get('entry_fee', 0.0):.2f}"
    )
    print(
        f"    - LQQ 100%: ${b_lqq.get('equity', np.nan):.2f} "
        f"({b_lqq.get('pnl', np.nan):+.2f} | {b_lqq.get('ret_pct', np.nan):+.2f}%)"
    )
    print(
        f"      fees=${b_lqq.get('fees_paid', 0.0):.2f} "
        f"| entry_fee=${b_lqq.get('entry_fee', 0.0):.2f}"
    )
    switch_active = str(b_sw.get("active", ISOE_SYMBOL)).replace("EURONEXT:", "")
    print(
        f"    - Switch ({switch_active}): ${b_sw.get('equity', np.nan):.2f} "
        f"({b_sw.get('pnl', np.nan):+.2f} | {b_sw.get('ret_pct', np.nan):+.2f}%)"
    )
    print(
        f"      fees=${b_sw.get('fees_paid', 0.0):.2f} "
        f"| switches={int(b_sw.get('switch_count', 0) or 0)} "
        f"| last_switch_fee=${b_sw.get('last_switch_fee', 0.0):.2f}"
    )
    if not trades:
        if regime != "NONE" and not rebalance_allowed:
            print("  No trades executed this run (blocked by market hours).")
        else:
            print("  No trades executed this run.")
    else:
        print(f"  Trades executed: {len(trades)}")
        for t in trades:
            print(
                f"    - {t['side']} {t['symbol']} x{t['qty']} @ {t['price']:.4f} "
                f"| notional=${t['notional']:.2f} fee=${t['fee']:.2f}"
            )

    print("  Open positions:")
    positions = state.get("positions", {})
    if not positions:
        print("    (none)")
    else:
        for sym in sorted(positions.keys()):
            shares = int(positions[sym].get("shares", 0) or 0)
            avg_cost = float(positions[sym].get("avg_cost", 0.0) or 0.0)
            px = price_map.get(sym, np.nan)
            mkt = shares * px if np.isfinite(px) else np.nan
            print(
                f"    - {sym}: shares={shares} avg_cost={avg_cost:.4f} "
                f"last={px:.4f} mkt=${mkt:.2f}" if np.isfinite(px) else
                f"    - {sym}: shares={shares} avg_cost={avg_cost:.4f} last=N/A mkt=N/A"
            )

    tg_message = build_telegram_portfolio_report(
        state=state,
        equity_before=equity_before,
        equity_after=equity_after,
        unrealized=unrealized,
        regime=regime,
        n_lqq=n_lqq,
        n_isoe=n_isoe,
        price_map=price_map,
        benchmark_summary=benchmark_summary,
    )
    tg_sent = send_telegram_message(tg_message)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        print(f"Telegram portfolio update: {'sent' if tg_sent else 'failed'}")
    else:
        print("Telegram portfolio update: skipped (missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID)")

    print(f"\nStrictly aligned bullish + positive-average symbols: {len(ranked)}")
    print(f"Symbols analyzed: {len(symbols)} | OK: {len(results)} | Errors: {len(errors)}")
    print(f"Elapsed: {time.time() - start:.2f}s")

    if errors:
        print("\nErrors:")
        for sym, msg in errors:
            print(f"  - {sym}: {msg}")


if __name__ == "__main__":
    main()
