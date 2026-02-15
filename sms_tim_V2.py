#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SMS Tim V1 (Python)
- Fetches OHLC from TradingView WebSocket
- Reproduces key SMS_Tim logic:
  - 8 EMA regime on 1H and 1D
  - Daily support/resistance slope angles (degrees from horizontal)
  - TRADE decision from: Regime 1H + Regime 1D + Slope Sup + Slope Res

Note:
Slope Sup and Slope Res are explicitly computed from DAILY candles.
"""

import argparse
import json
import math
import os
import random
import string
import time
from typing import Dict, List, Optional, Tuple


def install_and_import(package: str, import_name: Optional[str] = None):
    if import_name is None:
        import_name = package
    try:
        return __import__(import_name)
    except ImportError:
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return __import__(import_name)


pd = install_and_import("pandas")
websocket = install_and_import("websocket-client", "websocket")
requests = install_and_import("requests")
dotenv = install_and_import("python-dotenv", "dotenv")
from websocket import create_connection


TV_WS_URL = "wss://prodata.tradingview.com/socket.io/websocket"
EMA_LENGTHS = [20, 25, 30, 35, 40, 45, 50, 55]
DEFAULT_BASE_TF = "5M"
TF_TO_TV = {"1M": "1", "5M": "5", "15M": "15", "30M": "30", "1H": "60", "4H": "240", "1D": "1D"}
CCY_TO_INDEX = {
    "USD": "TVC:DXY",
    "EUR": "TVC:EXY",
    "GBP": "TVC:BXY",
    "JPY": "TVC:JXY",
    "CHF": "TVC:SXY",
    "CAD": "TVC:CXY",
    "AUD": "TVC:AXY",
    "NZD": "TVC:ZXY",
}

# Original signal defaults from Pine inputs.
MOMENTUM_THRESHOLD_BASE = 0.01
MIN_SIGNAL_DISTANCE = 5
VOLUME_LONG_PERIOD = 50
VOLUME_SHORT_PERIOD = 5
BREAKOUT_PERIOD = 5
USE_MOMENTUM_FILTER = True
USE_TREND_FILTER = True
USE_LOWER_TF_FILTER = True
USE_VOLUME_FILTER = True
USE_BREAKOUT_FILTER = True
RESTRICT_REPEATED_SIGNALS = True
PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "NZDJPY=X", "CADJPY=X", "CHFJPY=X",
    "EURAUD=X", "EURCAD=X", "EURNZD=X", "EURCHF=X",
    "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPCHF=X",
    "AUDNZD=X", "AUDCAD=X", "AUDCHF=X",
    "NZDCAD=X", "NZDCHF=X",
    "CADCHF=X",
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, ".env")
try:
    dotenv.load_dotenv(ENV_PATH)
except Exception:
    pass


def new_chart_session_id() -> str:
    suffix = "".join(random.choices(string.ascii_letters + string.digits, k=12))
    return f"cs_{suffix}"


def create_msg(func: str, args: List) -> str:
    payload = json.dumps({"m": func, "p": args})
    return f"~m~{len(payload)}~m~{payload}"


def parse_tv_messages(buffer: str) -> Tuple[List[str], str]:
    messages = []
    while True:
        if not buffer:
            break
        if buffer.startswith("~h"):
            if buffer == "~h":
                messages.append("~h")
                return messages, ""
            next_idx = buffer.find("~m~")
            if next_idx == -1:
                return messages, ""
            messages.append("~h")
            buffer = buffer[next_idx:]
            continue

        if not buffer.startswith("~m~"):
            next_idx = buffer.find("~m~")
            if next_idx == -1:
                return messages, ""
            buffer = buffer[next_idx:]

        len_end = buffer.find("~m~", 3)
        if len_end == -1:
            break

        length_str = buffer[3:len_end]
        if not length_str.isdigit():
            buffer = buffer[len_end + 3 :]
            continue

        msg_len = int(length_str)
        start = len_end + 3
        end = start + msg_len
        if len(buffer) < end:
            break

        messages.append(buffer[start:end])
        buffer = buffer[end:]

    return messages, buffer


def extract_bars_from_message(message: Dict) -> List[List[float]]:
    bars: List[List[float]] = []

    if not isinstance(message, dict):
        return bars

    if message.get("m") not in ("timescale_update", "du"):
        return bars

    def walk(node):
        if isinstance(node, dict):
            if "s" in node and isinstance(node["s"], list):
                for entry in node["s"]:
                    if isinstance(entry, dict) and "v" in entry and isinstance(entry["v"], list):
                        bars.append(entry["v"])
                    elif isinstance(entry, list):
                        bars.append(entry)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(message.get("p", []))
    return bars


def bars_to_df(bars: List[List[float]]):
    cleaned = []
    for b in bars:
        if not isinstance(b, (list, tuple)) or len(b) < 5:
            continue
        if len(b) < 6:
            b = list(b) + [0.0]
        cleaned.append(list(b[:6]))

    if not cleaned:
        return None

    df = pd.DataFrame(cleaned, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    df = df.set_index("datetime")
    df = df.rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )
    return df[["Open", "High", "Low", "Close", "Volume"]]


def fetch_tv_ohlc(symbol: str, interval: str, bars: int = 300, timeout_s: int = 12):
    ws = None
    buffer = ""
    bars_by_ts: Dict[int, List[float]] = {}
    series_done = False

    headers = {
        "Origin": "https://www.tradingview.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    try:
        ws = create_connection(TV_WS_URL, header=headers, timeout=10)
        session_id = new_chart_session_id()

        ws.send(create_msg("chart_create_session", [session_id, ""]))
        ws.send(
            create_msg(
                "resolve_symbol",
                [
                    session_id,
                    "sds_sym_1",
                    f'={{"adjustment":"splits","metric":"price","settlement-as-close":false,"symbol":"{symbol}"}}',
                ],
            )
        )
        ws.send(create_msg("create_series", [session_id, "sds_1", "s1", "sds_sym_1", interval, bars, ""]))

        started = time.time()
        while time.time() - started < timeout_s:
            try:
                raw = ws.recv()
            except Exception:
                continue

            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="ignore")

            buffer += raw
            payloads, buffer = parse_tv_messages(buffer)

            for payload in payloads:
                if payload == "~h":
                    try:
                        ws.send("~h")
                    except Exception:
                        pass
                    continue

                try:
                    msg = json.loads(payload)
                except Exception:
                    continue

                if msg.get("m") == "series_completed":
                    series_done = True

                for bar in extract_bars_from_message(msg):
                    if isinstance(bar, (list, tuple)) and len(bar) >= 5 and bar[0] is not None:
                        bars_by_ts[int(bar[0])] = list(bar)

            if series_done and bars_by_ts:
                break

        if not bars_by_ts:
            return None

        ordered = [bars_by_ts[k] for k in sorted(bars_by_ts.keys())]
        return bars_to_df(ordered)

    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass


def ema_regime(df) -> str:
    if df is None or df.empty or len(df) < max(EMA_LENGTHS):
        return "NEUTRAL"

    close = df["Close"]
    ema_last = [close.ewm(span=l, adjust=False).mean().iloc[-1] for l in EMA_LENGTHS]

    is_bull = all(ema_last[i] > ema_last[i + 1] for i in range(len(ema_last) - 1))
    is_bear = all(ema_last[i] < ema_last[i + 1] for i in range(len(ema_last) - 1))

    if is_bull:
        return "BULL"
    if is_bear:
        return "BEAR"
    return "NEUTRAL"


def _atr_rma(df, period: int = 14):
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / period, adjust=False).mean()


def _vwap_series(df):
    tp = (df["High"] + df["Low"] + df["Close"]) / 3.0
    vol = df["Volume"].replace(0, pd.NA)
    cum_pv = (tp * vol).cumsum()
    cum_v = vol.cumsum()
    vwap = cum_pv / cum_v
    return vwap.fillna(df["Close"])


def _trend_series(df):
    ema20 = df["Close"].ewm(span=20, adjust=False).mean()
    vwap = _vwap_series(df)
    trend = pd.Series(0, index=df.index, dtype="int64")
    trend = trend.mask((df["Close"] > ema20) & (df["Close"] > vwap), 1)
    trend = trend.mask((df["Close"] < ema20) & (df["Close"] < vwap), -1)
    return trend


def _trend_last_from_tf(symbol: str, tf_label: str, bars: int = 300) -> int:
    tf = TF_TO_TV.get(tf_label, "5")
    df = fetch_tv_ohlc(symbol=symbol, interval=tf, bars=bars)
    if df is None or df.empty:
        return 0
    return int(_trend_series(df).iloc[-1])


def _last_original_signal(symbol: str, base_bars: int = 500, higher_tf_choice: str = "5M", lower_tf_choice: str = "5M", restrict_tf_choice: str = "5M") -> str:
    base_tf = TF_TO_TV[DEFAULT_BASE_TF]
    df = fetch_tv_ohlc(symbol=symbol, interval=base_tf, bars=base_bars)
    if df is None or df.empty or len(df) < 30:
        return "NEUTRAL"

    df = df.copy()
    df["atr"] = _atr_rma(df, 14)
    df["volatility_factor"] = df["atr"] / df["Close"]
    df["momentum_threshold"] = MOMENTUM_THRESHOLD_BASE * (1.0 + df["volatility_factor"] * 2.0)
    df["price_change"] = ((df["Close"] - df["Close"].shift(1)) / df["Close"].shift(1)) * 100.0
    df["volAvg50"] = df["Volume"].rolling(VOLUME_LONG_PERIOD).mean()
    df["volShort"] = df["Volume"].rolling(VOLUME_SHORT_PERIOD).mean()
    df["volCondition"] = (df["Volume"] > df["volAvg50"]) & (df["volShort"].diff() > 0)
    df["highestBreakout"] = df["High"].rolling(BREAKOUT_PERIOD).max()
    df["lowestBreakout"] = df["Low"].rolling(BREAKOUT_PERIOD).min()

    trend_base = _trend_series(df)
    higher_const = _trend_last_from_tf(symbol, higher_tf_choice) if higher_tf_choice != DEFAULT_BASE_TF else None
    lower_const = _trend_last_from_tf(symbol, lower_tf_choice) if lower_tf_choice != DEFAULT_BASE_TF else None
    restrict_const = _trend_last_from_tf(symbol, restrict_tf_choice) if restrict_tf_choice != DEFAULT_BASE_TF else None

    last_signal_bar = -MIN_SIGNAL_DISTANCE - 1
    last_signal = "Neutral"
    last_trend = 0
    last_original_signal = "NEUTRAL"

    for i in range(2, len(df)):
        close_i = float(df["Close"].iloc[i])
        price_change_i = df["price_change"].iloc[i]
        momentum_threshold_i = df["momentum_threshold"].iloc[i]
        if pd.isna(price_change_i) or pd.isna(momentum_threshold_i):
            continue

        higher_tf_trend = int(trend_base.iloc[i]) if higher_const is None else int(higher_const)
        lower_tf_trend = int(trend_base.iloc[i]) if lower_const is None else int(lower_const)
        restrict_tf_trend = int(trend_base.iloc[i]) if restrict_const is None else int(restrict_const)

        bearish_trend_ok = higher_tf_trend == -1
        bullish_trend_ok = higher_tf_trend == 1
        lower_tf_bullish = lower_tf_trend == 1
        lower_tf_bearish = lower_tf_trend == -1
        lower_tf_not_neutral = lower_tf_trend != 0

        early_sell_signal = price_change_i < -float(momentum_threshold_i) if USE_MOMENTUM_FILTER else True
        early_buy_signal = price_change_i > float(momentum_threshold_i) if USE_MOMENTUM_FILTER else True

        sell_trend_ok = bearish_trend_ok if USE_TREND_FILTER else True
        buy_trend_ok = bullish_trend_ok if USE_TREND_FILTER else True
        sell_lower_tf_ok = (not lower_tf_bullish and lower_tf_not_neutral) if USE_LOWER_TF_FILTER else True
        buy_lower_tf_ok = (not lower_tf_bearish and lower_tf_not_neutral) if USE_LOWER_TF_FILTER else True
        sell_volume_ok = bool(df["volCondition"].iloc[i]) if USE_VOLUME_FILTER else True
        buy_volume_ok = bool(df["volCondition"].iloc[i]) if USE_VOLUME_FILTER else True

        prev_low_break = df["lowestBreakout"].shift(1).iloc[i]
        prev_high_break = df["highestBreakout"].shift(1).iloc[i]
        sell_breakout_ok = close_i < float(prev_low_break) if (USE_BREAKOUT_FILTER and pd.notna(prev_low_break)) else (not USE_BREAKOUT_FILTER)
        buy_breakout_ok = close_i > float(prev_high_break) if (USE_BREAKOUT_FILTER and pd.notna(prev_high_break)) else (not USE_BREAKOUT_FILTER)

        sell_allowed = (
            not RESTRICT_REPEATED_SIGNALS
            or (last_signal != "Sell" or (last_signal == "Sell" and restrict_tf_trend != last_trend and restrict_tf_trend != -1))
        )
        buy_allowed = (
            not RESTRICT_REPEATED_SIGNALS
            or (last_signal != "Buy" or (last_signal == "Buy" and restrict_tf_trend != last_trend and restrict_tf_trend != 1))
        )

        sell_condition = (
            early_sell_signal
            and (i - last_signal_bar >= MIN_SIGNAL_DISTANCE)
            and sell_trend_ok
            and sell_lower_tf_ok
            and sell_volume_ok
            and sell_breakout_ok
            and sell_allowed
        )
        buy_condition = (
            early_buy_signal
            and (i - last_signal_bar >= MIN_SIGNAL_DISTANCE)
            and buy_trend_ok
            and buy_lower_tf_ok
            and buy_volume_ok
            and buy_breakout_ok
            and buy_allowed
        )

        if sell_condition:
            last_signal = "Sell"
            last_signal_bar = i
            last_trend = restrict_tf_trend
            last_original_signal = "SELL"

        if buy_condition:
            last_signal = "Buy"
            last_signal_bar = i
            last_trend = restrict_tf_trend
            last_original_signal = "BUY"

    return last_original_signal


def daily_chg_cc(df_daily) -> float:
    if df_daily is None or df_daily.empty or len(df_daily) < 2:
        return math.nan
    d_close = float(df_daily["Close"].iloc[-1])
    d_prev = float(df_daily["Close"].iloc[-2])
    if d_prev == 0:
        return math.nan
    return (d_close - d_prev) / d_prev * 100.0


def _pair_ccy_from_symbol(symbol: str) -> Tuple[Optional[str], Optional[str]]:
    # e.g. OANDA:GBPUSD -> ("GBP", "USD")
    raw = symbol.split(":")[-1]
    if len(raw) < 6:
        return None, None
    return raw[:3], raw[3:6]


def fetch_index_chg_map(bars_d1: int = 300) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    for ccy, tv_symbol in CCY_TO_INDEX.items():
        try:
            df = fetch_tv_ohlc(symbol=tv_symbol, interval="1D", bars=bars_d1)
            if df is None or df.empty:
                out[ccy] = None
                continue
            chg = daily_chg_cc(df)
            out[ccy] = None if math.isnan(chg) else float(chg)
        except Exception:
            out[ccy] = None
    return out


def index_cc_confirms_trade(symbol: str, trade: str, index_chg_map: Dict[str, Optional[float]]) -> Tuple[Optional[bool], Optional[float], Optional[float]]:
    base_ccy, quote_ccy = _pair_ccy_from_symbol(symbol)
    if not base_ccy or not quote_ccy:
        return None, None, None
    base_chg = index_chg_map.get(base_ccy)
    quote_chg = index_chg_map.get(quote_ccy)
    if base_chg is None or quote_chg is None:
        return None, base_chg, quote_chg

    if trade == "BUY":
        return base_chg > quote_chg, base_chg, quote_chg
    if trade == "SELL":
        return base_chg < quote_chg, base_chg, quote_chg
    return None, base_chg, quote_chg


def compute_daily_slopes_deg(df_daily, short_period: int = 30, long_period: int = 100) -> Tuple[float, float]:
    """
    Reproduces Pine support/resistance anchor scan and slope angle from horizontal.

    IMPORTANT: This function must receive DAILY candles.
    """
    if df_daily is None or df_daily.empty:
        return math.nan, math.nan

    n = len(df_daily)
    if n < 3:
        return math.nan, math.nan

    lows = df_daily["Low"].reset_index(drop=True)
    highs = df_daily["High"].reset_index(drop=True)

    max_short = min(short_period, n - 1)
    max_long = min(long_period, n - 1)

    lowest_y2 = float("inf")
    lowest_x2 = 0
    highest_y2 = float("-inf")
    highest_x2 = 0

    for i in range(1, max_short + 1):
        li = float(lows.iloc[-1 - i])
        hi = float(highs.iloc[-1 - i])
        if li < lowest_y2:
            lowest_y2 = li
            lowest_x2 = i
        if hi > highest_y2:
            highest_y2 = hi
            highest_x2 = i

    lowest_y1 = float("inf")
    lowest_x1 = 0
    highest_y1 = float("-inf")
    highest_x1 = 0

    start_j = short_period + 1
    if start_j <= max_long:
        for j in range(start_j, max_long + 1):
            lj = float(lows.iloc[-1 - j])
            hj = float(highs.iloc[-1 - j])
            if lj < lowest_y1:
                lowest_y1 = lj
                lowest_x1 = j
            if hj > highest_y1:
                highest_y1 = hj
                highest_x1 = j

    sup_angle_deg = math.nan
    res_angle_deg = math.nan

    if lowest_x1 > 0 and lowest_x2 > 0:
        dx_sup = float(lowest_x1 - lowest_x2)
        dy_sup = float(lowest_y2 - lowest_y1)
        if dx_sup != 0:
            sup_angle_deg = math.degrees(math.atan(dy_sup / dx_sup))

    if highest_x1 > 0 and highest_x2 > 0:
        dx_res = float(highest_x1 - highest_x2)
        dy_res = float(highest_y2 - highest_y1)
        if dx_res != 0:
            res_angle_deg = math.degrees(math.atan(dy_res / dx_res))

    return sup_angle_deg, res_angle_deg


def trade_decision(regime_h1: str, regime_d1: str, sup_slope_deg: float, res_slope_deg: float) -> str:
    buy = regime_h1 in ("BULL", "NEUTRAL") and regime_d1 == "BULL" and sup_slope_deg > 0 and res_slope_deg > 0
    sell = regime_h1 in ("BEAR", "NEUTRAL") and regime_d1 == "BEAR" and sup_slope_deg < 0 and res_slope_deg < 0

    if buy:
        return "BUY"
    if sell:
        return "SELL"
    return "NEUTRAL"


def alignment_metrics(
    regime_h1: str,
    regime_d1: str,
    sup_slope_deg: float,
    res_slope_deg: float,
    chg_cc_daily: float,
    last_original_signal: str,
    index_base_chg: Optional[float],
    index_quote_chg: Optional[float],
) -> Tuple[str, float, int, int]:
    # 7-key alignment blocks for BULL/BEAR side.
    bull_count = 0
    bear_count = 0

    bull_count += 1 if regime_h1 == "BULL" else 0
    bear_count += 1 if regime_h1 == "BEAR" else 0

    bull_count += 1 if regime_d1 == "BULL" else 0
    bear_count += 1 if regime_d1 == "BEAR" else 0

    bull_count += 1 if sup_slope_deg > 0 else 0
    bear_count += 1 if sup_slope_deg < 0 else 0

    bull_count += 1 if res_slope_deg > 0 else 0
    bear_count += 1 if res_slope_deg < 0 else 0

    bull_count += 1 if chg_cc_daily > 0 else 0
    bear_count += 1 if chg_cc_daily < 0 else 0

    bull_count += 1 if last_original_signal == "BUY" else 0
    bear_count += 1 if last_original_signal == "SELL" else 0

    if index_base_chg is not None and index_quote_chg is not None:
        bull_count += 1 if index_base_chg > index_quote_chg else 0
        bear_count += 1 if index_base_chg < index_quote_chg else 0

    if bull_count > bear_count:
        global_trend = "BULL"
        alignment_count = bull_count
    elif bear_count > bull_count:
        global_trend = "BEAR"
        alignment_count = bear_count
    else:
        global_trend = "NEUTRAL"
        alignment_count = bull_count

    alignment_pct = round((alignment_count * 100.0) / 7.0, 2)
    return global_trend, alignment_pct, bull_count, bear_count


def compute_for_symbol(
    symbol: str,
    short_period: int,
    long_period: int,
    bars_h1: int,
    bars_d1: int,
    index_chg_map: Optional[Dict[str, Optional[float]]] = None,
) -> Dict:
    # 1H regime
    df_h1 = fetch_tv_ohlc(symbol=symbol, interval="60", bars=bars_h1)
    if df_h1 is None or df_h1.empty:
        raise RuntimeError(f"No H1 data from TradingView for {symbol}")

    # DAILY regime + DAILY slopes (explicitly daily)
    df_d1 = fetch_tv_ohlc(symbol=symbol, interval="1D", bars=bars_d1)
    if df_d1 is None or df_d1.empty:
        raise RuntimeError(f"No D1 data from TradingView for {symbol}")

    regime_h1 = ema_regime(df_h1)
    regime_d1 = ema_regime(df_d1)

    sup_slope_deg, res_slope_deg = compute_daily_slopes_deg(
        df_daily=df_d1,
        short_period=short_period,
        long_period=long_period,
    )

    trade = trade_decision(regime_h1, regime_d1, sup_slope_deg, res_slope_deg)
    chg_cc_daily = daily_chg_cc(df_d1)
    last_original_signal = _last_original_signal(
        symbol=symbol,
        base_bars=500,
        higher_tf_choice="5M",
        lower_tf_choice="5M",
        restrict_tf_choice="5M",
    )
    original_confirmed = (trade == "BUY" and last_original_signal == "BUY") or (trade == "SELL" and last_original_signal == "SELL")
    if index_chg_map is None:
        index_chg_map = fetch_index_chg_map(bars_d1=bars_d1)
    index_cc_confirmed, index_base_chg, index_quote_chg = index_cc_confirms_trade(symbol, trade, index_chg_map)
    global_trend, alignment_pct, bull_count, bear_count = alignment_metrics(
        regime_h1=regime_h1,
        regime_d1=regime_d1,
        sup_slope_deg=sup_slope_deg,
        res_slope_deg=res_slope_deg,
        chg_cc_daily=chg_cc_daily,
        last_original_signal=last_original_signal,
        index_base_chg=index_base_chg,
        index_quote_chg=index_quote_chg,
    )

    result = {
        "symbol": symbol,
        "regime_1H": regime_h1,
        "regime_1D": regime_d1,
        "slope_sup_deg_daily": None if math.isnan(sup_slope_deg) else round(sup_slope_deg, 4),
        "slope_res_deg_daily": None if math.isnan(res_slope_deg) else round(res_slope_deg, 4),
        "chg_cc_daily": None if math.isnan(chg_cc_daily) else round(chg_cc_daily, 4),
        "trade": trade,
        "last_original_signal": last_original_signal,
        "original_confirmed": original_confirmed,
        "index_base_chg_daily": None if index_base_chg is None else round(index_base_chg, 4),
        "index_quote_chg_daily": None if index_quote_chg is None else round(index_quote_chg, 4),
        "index_cc_confirmed": index_cc_confirmed,
        "global_trend": global_trend,
        "alignment_pct": alignment_pct,
        "alignment_count": max(bull_count, bear_count),
        "bull_confirmations": bull_count,
        "bear_confirmations": bear_count,
        "daily_bars_used": int(len(df_d1)),
        "h1_bars_used": int(len(df_h1)),
    }

    return result


def run(symbol: str, short_period: int, long_period: int, bars_h1: int, bars_d1: int):
    index_chg_map = fetch_index_chg_map(bars_d1=bars_d1)
    result = compute_for_symbol(symbol, short_period, long_period, bars_h1, bars_d1, index_chg_map=index_chg_map)
    print(json.dumps(result, indent=2))


def _trade_dot(trade: str) -> str:
    return "🟢" if trade == "BUY" else "🔴" if trade == "SELL" else "⚪"


def _chg_confirms_trade(row: Dict) -> bool:
    trade = row.get("trade")
    chg = row.get("chg_cc_daily")
    if chg is None:
        return False
    if trade == "BUY":
        return chg > 0
    if trade == "SELL":
        return chg < 0
    return False


def _flame_for_row(row: Dict) -> bool:
    trade = row.get("trade")
    h1 = row.get("regime_1H")
    chg = row.get("chg_cc_daily")

    # Flame when H1 is not neutral in the direction of the trade.
    h1_confirms = (trade == "BUY" and h1 == "BULL") or (trade == "SELL" and h1 == "BEAR")

    # Flame when CHG% CC DAILY confirms the trade direction.
    chg_confirms = _chg_confirms_trade(row)

    return h1_confirms or chg_confirms


def build_telegram_lines(filtered_rows: List[Dict]) -> List[str]:
    lines = ["SMS SCREENER"]
    for row in filtered_rows:
        pair = row.get("pair", row.get("symbol", "")).replace("=X", "").replace("OANDA:", "")
        align = row.get("alignment_pct")
        align_txt = "N/A" if align is None else f"{int(round(float(align)))}%"
        flame = " 🔥" if _flame_for_row(row) else ""
        lines.append(f"{_trade_dot(row.get('trade'))} {pair} ({align_txt}){flame}")
    return lines


def send_telegram_message(lines: List[str]) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": "\n".join(lines)}, timeout=10)
        return True
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description="SMS Tim V2 - Python/WebSocket TradingView")
    parser.add_argument("--symbol", default=None, help="TradingView symbol, e.g. OANDA:EURUSD or TVC:DXY")
    parser.add_argument("--short-trend-period", type=int, default=30)
    parser.add_argument("--long-trend-period", type=int, default=100)
    parser.add_argument("--bars-h1", type=int, default=300)
    parser.add_argument("--bars-d1", type=int, default=300)
    parser.add_argument("--debug-all", action="store_true", help="Show pair-by-pair diagnostics for all 28 pairs")
    args = parser.parse_args()

    if args.symbol:
        run(
            symbol=args.symbol,
            short_period=args.short_trend_period,
            long_period=args.long_trend_period,
            bars_h1=args.bars_h1,
            bars_d1=args.bars_d1,
        )
        return

    results = []
    errors = []
    index_chg_map = fetch_index_chg_map(bars_d1=args.bars_d1)
    for pair in PAIRS:
        symbol = f"OANDA:{pair.replace('=X', '')}"
        try:
            result = compute_for_symbol(
                symbol=symbol,
                short_period=args.short_trend_period,
                long_period=args.long_trend_period,
                bars_h1=args.bars_h1,
                bars_d1=args.bars_d1,
                index_chg_map=index_chg_map,
            )
            result["pair"] = pair
            results.append(result)
        except Exception as exc:
            errors.append({"pair": pair, "symbol": symbol, "error": str(exc)})

    # Keep only BUY/SELL confirmed by CHG% CC DAILY + last original signal.
    filtered = [
        r
        for r in results
        if r["trade"] in ("BUY", "SELL")
        and _chg_confirms_trade(r)
        and bool(r.get("original_confirmed"))
    ]
    buy_count = sum(1 for r in filtered if r["trade"] == "BUY")
    sell_count = sum(1 for r in filtered if r["trade"] == "SELL")

    print("=== FILTERED BUY/SELL ===")
    print(json.dumps(filtered, indent=2))
    print("=== SUMMARY ===")
    print(
        json.dumps(
            {
                "total_pairs": len(PAIRS),
                "computed": len(results),
                "filtered_buy_sell": len(filtered),
                "buy_count": buy_count,
                "sell_count": sell_count,
                "errors": len(errors),
            },
            indent=2,
        )
    )
    if errors:
        print("=== ERRORS ===")
        print(json.dumps(errors, indent=2))

    if args.debug_all:
        debug_rows = []
        for r in results:
            chg_ok = _chg_confirms_trade(r)
            orig_ok = bool(r.get("original_confirmed"))
            final_ok = r["trade"] in ("BUY", "SELL") and chg_ok and orig_ok
            reason = None
            if not final_ok:
                if r["trade"] not in ("BUY", "SELL"):
                    reason = "trade_neutral"
                elif not chg_ok:
                    reason = "chg_fail"
                elif not orig_ok:
                    reason = "original_fail"
            debug_rows.append(
                {
                    "pair": r.get("pair", r.get("symbol")),
                    "trade": r.get("trade"),
                    "regime_1H": r.get("regime_1H"),
                    "regime_1D": r.get("regime_1D"),
                    "slope_sup_deg_daily": r.get("slope_sup_deg_daily"),
                    "slope_res_deg_daily": r.get("slope_res_deg_daily"),
                    "chg_cc_daily": r.get("chg_cc_daily"),
                    "last_original_signal": r.get("last_original_signal"),
                    "index_cc_confirmed": r.get("index_cc_confirmed"),
                    "alignment_pct": r.get("alignment_pct"),
                    "pass_chg": chg_ok,
                    "pass_original": orig_ok,
                    "pass_final": final_ok,
                    "reject_reason": reason,
                }
            )
        debug_rows.sort(key=lambda x: x["pair"] if x["pair"] is not None else "")
        print("=== DEBUG ALL PAIRS ===")
        print(json.dumps(debug_rows, indent=2))

    # Telegram summary in requested format.
    if filtered:
        telegram_lines = build_telegram_lines(filtered)
    else:
        telegram_lines = ["SMS SCREENER", "AUCUN SIGNAL"]
    sent = send_telegram_message(telegram_lines)
    print("=== TELEGRAM ===")
    print("sent" if sent else "not_sent (missing token/chat_id or request failed)")


if __name__ == "__main__":
    main()
