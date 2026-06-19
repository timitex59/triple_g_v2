#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
binance_cmc_top100_pipeline.py

Crypto rotation pipeline:
  1. fetch CoinMarketCap Top 100 as the market-cap reference universe,
  2. intersect it with active Binance spot pairs quoted in USDT,
  3. rank tradable assets by momentum, relative strength vs BTC, volume and trend,
  4. validate top candidates with a local ATR Renko 3M/M/W filter,
  5. write a report and optionally send a compact Telegram summary.

The CMC API key is optional. If COINMARKETCAP_API_KEY is absent, the script
uses CoinMarketCap's keyless trial endpoint.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import ccxt
import numpy as np
import pandas as pd
import pytz
import requests
from dotenv import load_dotenv
from tabulate import tabulate

from nasdaq_sector_pipeline import (
    annualized_vol,
    clamp,
    format_pct,
    pct_change,
    pct_change_from_date,
    rsi,
    safe_float,
    send_telegram,
    zscore,
)

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_JSON = os.path.join(SCRIPT_DIR, "binance_cmc_top100_report.json")
REPORT_TXT = os.path.join(SCRIPT_DIR, "binance_cmc_top100_report.txt")
TRACKER_PATH = os.path.join(SCRIPT_DIR, "binance_cmc_top100_tracker.json")

CMC_PRO_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
CMC_TRIAL_URL = "https://pro-api.coinmarketcap.com/trial-pro-api/v1/cryptocurrency/listings/latest"
USER_AGENT = "chfjpy-strategy/1.0"
TRACK_TOP_N = 5

STABLE_BASES = {
    "USDT", "USDC", "FDUSD", "BUSD", "DAI", "TUSD", "USDP", "USDD", "PYUSD",
    "USDE", "SUSDE", "USDS", "FRAX", "LUSD", "USD1", "EURC", "EURS", "GUSD",
}

TOKEN_ALIASES = {
    "RENDER": ["RENDER", "RNDR"],
    "RNDR": ["RNDR", "RENDER"],
    "POL": ["POL", "MATIC"],
    "MATIC": ["MATIC", "POL"],
    "BTT": ["BTT", "BTTC"],
    "BTTC": ["BTTC", "BTT"],
    "LUNA": ["LUNA", "LUNC"],
    "LUNC": ["LUNC", "LUNA"],
    "RON": ["RON", "RONIN"],
}

THEME_MAP = {
    "BTC": "Store of value / PoW",
    "BCH": "Store of value / PoW",
    "LTC": "Store of value / PoW",
    "XMR": "Store of value / PoW",
    "ZEC": "Store of value / PoW",
    "DASH": "Store of value / PoW",
    "KAS": "Store of value / PoW",
    "ETH": "L1 / Smart contracts",
    "BNB": "Exchange tokens",
    "SOL": "L1 / Smart contracts",
    "ADA": "L1 / Smart contracts",
    "AVAX": "L1 / Smart contracts",
    "DOT": "L1 / Smart contracts",
    "NEAR": "AI / Compute",
    "TON": "L1 / Smart contracts",
    "SUI": "L1 / Smart contracts",
    "APT": "L1 / Smart contracts",
    "ATOM": "L1 / Smart contracts",
    "ICP": "AI / Compute",
    "HBAR": "L1 / Smart contracts",
    "ALGO": "L1 / Smart contracts",
    "EGLD": "L1 / Smart contracts",
    "TRX": "Payments / Settlement",
    "XRP": "Payments / Settlement",
    "XLM": "Payments / Settlement",
    "ARB": "L2 / Scaling",
    "OP": "L2 / Scaling",
    "POL": "L2 / Scaling",
    "MATIC": "L2 / Scaling",
    "STRK": "L2 / Scaling",
    "ZK": "L2 / Scaling",
    "MNT": "L2 / Scaling",
    "LINK": "Oracles / Data",
    "PYTH": "Oracles / Data",
    "API3": "Oracles / Data",
    "BAND": "Oracles / Data",
    "UNI": "DeFi / DEX",
    "AAVE": "DeFi / DEX",
    "MKR": "DeFi / DEX",
    "SKY": "DeFi / DEX",
    "CRV": "DeFi / DEX",
    "COMP": "DeFi / DEX",
    "SNX": "DeFi / DEX",
    "LDO": "DeFi / DEX",
    "RUNE": "DeFi / DEX",
    "CAKE": "DeFi / DEX",
    "PENDLE": "DeFi / DEX",
    "ENA": "DeFi / DEX",
    "JUP": "DeFi / DEX",
    "FET": "AI / Compute",
    "ASI": "AI / Compute",
    "TAO": "AI / Compute",
    "RENDER": "AI / Compute",
    "RNDR": "AI / Compute",
    "FIL": "Storage / Infra",
    "AR": "Storage / Infra",
    "STORJ": "Storage / Infra",
    "DOGE": "Meme",
    "SHIB": "Meme",
    "PEPE": "Meme",
    "BONK": "Meme",
    "WIF": "Meme",
    "FLOKI": "Meme",
    "AXS": "Gaming / Metaverse",
    "SAND": "Gaming / Metaverse",
    "MANA": "Gaming / Metaverse",
    "GALA": "Gaming / Metaverse",
    "IMX": "Gaming / Metaverse",
    "ENJ": "Gaming / Metaverse",
    "OKB": "Exchange tokens",
    "CRO": "Exchange tokens",
    "LEO": "Exchange tokens",
    "KCS": "Exchange tokens",
}


@dataclass
class CmcAsset:
    rank: int
    symbol: str
    name: str
    slug: str
    price: float | None
    market_cap: float | None
    volume_24h: float | None


@dataclass
class CryptoMetrics:
    ticker: str
    exchange_symbol: str
    base: str
    name: str
    theme: str
    cmc_rank: int
    cmc_market_cap: float | None
    cmc_volume_24h: float | None
    close: float | None
    perf_5d: float | None
    perf_21d: float | None
    perf_63d: float | None
    perf_126d: float | None
    perf_ytd: float | None
    rel_63d_vs_btc: float | None
    rsi14: float | None
    vol_21d: float | None
    volume_ratio: float | None
    above_sma50: bool
    above_sma200: bool
    near_20d_high: bool
    score: float


@dataclass
class CryptoThemeScore:
    theme: str
    assets: int
    avg_score: float
    median_21d: float | None
    median_63d: float | None
    median_rel_63d: float | None
    pct_above_sma50: float
    pct_above_sma200: float
    final_score: float


@dataclass
class CryptoRenkoConfirmation:
    ticker: str
    name: str
    theme: str
    exchange_symbol: str
    renko_status: str
    final_action: str
    aligned: int | None
    bias: str
    px_3m: int | None
    px_m: int | None
    px_w: int | None
    streak_3m: int | None
    streak_m: int | None
    streak_w: int | None
    roc14: float | None
    roc21: float | None
    close: float | None
    reason: str


def fetch_cmc_top(limit: int) -> tuple[list[CmcAsset], str]:
    api_key = os.getenv("COINMARKETCAP_API_KEY", "").strip()
    url = CMC_PRO_URL if api_key else CMC_TRIAL_URL
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    if api_key:
        headers["X-CMC_PRO_API_KEY"] = api_key
    params = {
        "start": 1,
        "limit": int(limit),
        "convert": "USD",
        "sort": "market_cap",
        "sort_dir": "desc",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    rows = []
    for item in payload.get("data", []):
        quote = (item.get("quote") or {}).get("USD") or {}
        rows.append(CmcAsset(
            rank=int(item.get("cmc_rank") or 0),
            symbol=str(item.get("symbol") or "").upper(),
            name=str(item.get("name") or ""),
            slug=str(item.get("slug") or ""),
            price=safe_float(quote.get("price")),
            market_cap=safe_float(quote.get("market_cap")),
            volume_24h=safe_float(quote.get("volume_24h")),
        ))
    return rows, ("CoinMarketCap Pro API" if api_key else "CoinMarketCap trial API")


def load_exchange(exchange_id: str) -> tuple[Any, str, list[str]]:
    errors: list[str] = []
    candidates = ["binance", "binanceus"] if exchange_id == "auto" else [exchange_id]
    for candidate in candidates:
        try:
            cls = getattr(ccxt, candidate)
        except AttributeError:
            errors.append(f"{candidate}: unknown ccxt exchange")
            continue
        exchange = cls({"enableRateLimit": True})
        try:
            exchange.load_markets()
            return exchange, candidate, errors
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{candidate}: {type(exc).__name__}: {str(exc)[:180]}")
    raise RuntimeError("No usable exchange. " + " | ".join(errors))


def classify_theme(base: str) -> str:
    return THEME_MAP.get(base.upper(), "Other crypto")


def find_market_symbol(markets: dict[str, Any], base: str, quote: str) -> tuple[str | None, str | None]:
    bases = TOKEN_ALIASES.get(base, [base])
    for candidate_base in bases:
        symbol = f"{candidate_base}/{quote}"
        market = markets.get(symbol)
        if market and market.get("spot") and market.get("active", True):
            return symbol, candidate_base
    return None, None


def build_universe(
    cmc_assets: list[CmcAsset],
    markets: dict[str, Any],
    quote: str,
    benchmark_symbol: str,
    include_stables: bool,
    include_benchmark: bool,
) -> tuple[pd.DataFrame, list[CmcAsset], list[CmcAsset]]:
    quote = quote.upper()
    benchmark_base = benchmark_symbol.split("/")[0].upper()
    tradable = []
    unavailable = []
    excluded = []
    seen_symbols: set[str] = set()
    for asset in cmc_assets:
        base = asset.symbol.upper()
        if not base or base in seen_symbols:
            continue
        seen_symbols.add(base)
        if base == quote:
            excluded.append(asset)
            continue
        if not include_stables and base in STABLE_BASES:
            excluded.append(asset)
            continue
        if not include_benchmark and base == benchmark_base:
            excluded.append(asset)
            continue
        market_symbol, resolved_base = find_market_symbol(markets, base, quote)
        if not market_symbol or not resolved_base:
            unavailable.append(asset)
            continue
        tradable.append({
            "ticker": market_symbol.replace("/", ""),
            "exchange_symbol": market_symbol,
            "base": resolved_base,
            "cmc_symbol": base,
            "name": asset.name,
            "theme": classify_theme(resolved_base),
            "cmc_rank": asset.rank,
            "cmc_market_cap": asset.market_cap,
            "cmc_volume_24h": asset.volume_24h,
        })
    return pd.DataFrame(tradable), unavailable, excluded


def ohlcv_to_frame(rows: list[list[float]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["timestamp", "Open", "High", "Low", "Close", "Volume"])
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.set_index("timestamp")
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["Open", "High", "Low", "Close"])


def fetch_ohlcv_history(
    exchange: Any,
    symbol: str,
    timeframe: str,
    candles: int,
    per_call: int = 1000,
) -> pd.DataFrame:
    tf_ms = int(exchange.parse_timeframe(timeframe) * 1000)
    now_ms = int(exchange.milliseconds())
    since = now_ms - candles * tf_ms
    rows: list[list[float]] = []
    while len(rows) < candles:
        limit = min(per_call, candles - len(rows))
        batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        if not batch:
            break
        if rows:
            last_ts = rows[-1][0]
            batch = [r for r in batch if r[0] > last_ts]
            if not batch:
                break
        rows.extend(batch)
        since = int(batch[-1][0]) + tf_ms
        if len(batch) < limit or since >= now_ms:
            break
    return ohlcv_to_frame(rows[-candles:])


def download_histories(
    exchange: Any,
    symbols: list[str],
    timeframe: str,
    candles: int,
    pause: float,
) -> dict[str, pd.DataFrame]:
    histories: dict[str, pd.DataFrame] = {}
    for i, symbol in enumerate(symbols, 1):
        try:
            frame = fetch_ohlcv_history(exchange, symbol, timeframe, candles)
            if not frame.empty:
                histories[symbol] = frame
        except Exception as exc:  # noqa: BLE001
            print(f"  history failed for {symbol}: {type(exc).__name__}: {str(exc)[:120]}", file=sys.stderr)
        if i % 10 == 0 or i == len(symbols):
            print(f"  histories: {i}/{len(symbols)}")
        if pause > 0 and i < len(symbols):
            time.sleep(pause)
    return histories


def build_crypto_metrics(
    components: pd.DataFrame,
    histories: dict[str, pd.DataFrame],
    benchmark_close: pd.Series,
) -> list[CryptoMetrics]:
    raw_rows: list[dict[str, Any]] = []
    year_start = datetime(datetime.now(timezone.utc).year, 1, 1, tzinfo=timezone.utc)
    bench_63d = pct_change(benchmark_close, 63)

    for row in components.itertuples(index=False):
        hist = histories.get(row.exchange_symbol)
        if hist is None or hist.empty or "Close" not in hist:
            continue
        close = hist["Close"].dropna()
        if len(close) < 65:
            continue
        volume = hist["Volume"].dropna() if "Volume" in hist else pd.Series(dtype=float)
        quote_volume = (hist["Close"] * hist["Volume"]).dropna() if "Volume" in hist else pd.Series(dtype=float)

        perf_5d = pct_change(close, 5)
        perf_21d = pct_change(close, 21)
        perf_63d = pct_change(close, 63)
        perf_126d = pct_change(close, 126)
        perf_ytd = pct_change_from_date(close, year_start)
        rel_63d = None if perf_63d is None or bench_63d is None else perf_63d - bench_63d
        sma50 = close.rolling(50).mean().iloc[-1]
        sma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else np.nan
        high20 = close.rolling(20).max().iloc[-1]
        vol_ratio = None
        vol_series = quote_volume if not quote_volume.empty else volume
        if len(vol_series) >= 22:
            avg_vol = float(vol_series.iloc[-22:-1].mean())
            vol_ratio = safe_float(float(vol_series.iloc[-1]) / avg_vol) if avg_vol else None

        raw_rows.append({
            "ticker": row.ticker,
            "exchange_symbol": row.exchange_symbol,
            "base": row.base,
            "name": row.name,
            "theme": row.theme,
            "cmc_rank": int(row.cmc_rank),
            "cmc_market_cap": safe_float(row.cmc_market_cap),
            "cmc_volume_24h": safe_float(row.cmc_volume_24h),
            "close": safe_float(close.iloc[-1]),
            "perf_5d": perf_5d,
            "perf_21d": perf_21d,
            "perf_63d": perf_63d,
            "perf_126d": perf_126d,
            "perf_ytd": perf_ytd,
            "rel_63d_vs_btc": rel_63d,
            "rsi14": rsi(close),
            "vol_21d": annualized_vol(close),
            "volume_ratio": vol_ratio,
            "above_sma50": bool(close.iloc[-1] > sma50) if not pd.isna(sma50) else False,
            "above_sma200": bool(close.iloc[-1] > sma200) if not pd.isna(sma200) else False,
            "near_20d_high": bool(close.iloc[-1] >= high20 * 0.97) if not pd.isna(high20) else False,
        })

    score_fields = ["perf_21d", "perf_63d", "perf_126d", "rel_63d_vs_btc", "volume_ratio"]
    distributions = {
        field: [float(r[field]) for r in raw_rows if r.get(field) is not None]
        for field in score_fields
    }
    metrics: list[CryptoMetrics] = []
    for row in raw_rows:
        score = 50.0
        score += 9.0 * zscore(row["perf_21d"], distributions["perf_21d"])
        score += 13.0 * zscore(row["perf_63d"], distributions["perf_63d"])
        score += 7.0 * zscore(row["perf_126d"], distributions["perf_126d"])
        score += 10.0 * zscore(row["rel_63d_vs_btc"], distributions["rel_63d_vs_btc"])
        score += 4.0 * zscore(row["volume_ratio"], distributions["volume_ratio"])
        score += 5.0 if row["above_sma50"] else -5.0
        score += 4.0 if row["above_sma200"] else -3.0
        score += 3.0 if row["near_20d_high"] else 0.0
        # Small liquidity/rank sanity tilt without letting CMC rank dominate momentum.
        if row["cmc_rank"] <= 20:
            score += 1.0
        elif row["cmc_rank"] > 75:
            score -= 1.0
        row["score"] = round(clamp(score, 0.0, 100.0), 2)
        metrics.append(CryptoMetrics(**row))
    return sorted(metrics, key=lambda item: item.score, reverse=True)


def build_theme_scores(metrics: list[CryptoMetrics]) -> list[CryptoThemeScore]:
    df = pd.DataFrame([asdict(m) for m in metrics])
    if df.empty:
        return []
    rows = []
    for theme, group in df.groupby("theme"):
        if len(group) < 2:
            continue
        avg_score = float(group["score"].mean())
        median_21d = safe_float(group["perf_21d"].median())
        median_63d = safe_float(group["perf_63d"].median())
        median_rel = safe_float(group["rel_63d_vs_btc"].median())
        pct_sma50 = float(group["above_sma50"].mean() * 100)
        pct_sma200 = float(group["above_sma200"].mean() * 100)
        final = (
            avg_score * 0.62
            + clamp((median_63d or 0.0) + 25.0, 0.0, 100.0) * 0.16
            + pct_sma50 * 0.12
            + pct_sma200 * 0.06
            + clamp((median_rel or 0.0) + 25.0, 0.0, 100.0) * 0.04
        )
        rows.append(CryptoThemeScore(
            theme=theme,
            assets=int(len(group)),
            avg_score=round(avg_score, 2),
            median_21d=round(median_21d, 2) if median_21d is not None else None,
            median_63d=round(median_63d, 2) if median_63d is not None else None,
            median_rel_63d=round(median_rel, 2) if median_rel is not None else None,
            pct_above_sma50=round(pct_sma50, 1),
            pct_above_sma200=round(pct_sma200, 1),
            final_score=round(final, 2),
        ))
    return sorted(rows, key=lambda item: item.final_score, reverse=True)


def resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "Open": df["Open"].resample(rule).first(),
        "High": df["High"].resample(rule).max(),
        "Low": df["Low"].resample(rule).min(),
        "Close": df["Close"].resample(rule).last(),
        "Volume": df["Volume"].resample(rule).sum(),
    })
    return out.dropna(subset=["Open", "High", "Low", "Close"])


def atr_from_ohlc(frame: pd.DataFrame, length: int) -> float | None:
    if len(frame) < length + 1:
        return None
    high = frame["High"].astype(float)
    low = frame["Low"].astype(float)
    close = frame["Close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    value = tr.dropna().tail(length).mean()
    return safe_float(value)


def renko_brick_series(frame: pd.DataFrame, box_size: float) -> list[dict[str, float]]:
    if frame.empty or box_size <= 0:
        return []
    result: list[dict[str, float]] = []
    ref = float(frame["Close"].iloc[0])
    for timestamp, row in frame.iloc[1:].iterrows():
        price = float(row["Close"])
        ts = int(pd.Timestamp(timestamp).timestamp())
        while abs(price - ref) >= box_size:
            direction = 1.0 if price > ref else -1.0
            b_open = ref
            b_close = ref + direction * box_size
            result.append({"time": ts, "open": b_open, "close": b_close})
            ref = b_close
    return result


def green_streak(bricks: list[dict[str, float]]) -> int:
    count = 0
    for brick in reversed(bricks):
        if float(brick["close"]) > float(brick["open"]):
            count += 1
        else:
            break
    return count


def px_state(renko_open: float, renko_close: float, price: float) -> int:
    high = max(renko_open, renko_close)
    low = min(renko_open, renko_close)
    return 1 if price > high else (-1 if price < low else 0)


def renko_for_frame(df: pd.DataFrame, rule: str, atr_length: int) -> tuple[int | None, int | None, str]:
    frame = resample_ohlcv(df, rule)
    if len(frame) < atr_length + 2:
        return None, None, f"not enough {rule} bars"
    box = atr_from_ohlc(frame, atr_length)
    if not box or box <= 0:
        return None, None, f"invalid {rule} ATR"
    bricks = renko_brick_series(frame, box)
    if not bricks:
        return 0, 0, f"no {rule} Renko brick"
    last = bricks[-1]
    state = px_state(float(last["open"]), float(last["close"]), float(df["Close"].iloc[-1]))
    return state, green_streak(bricks), "ok"


def run_renko_filter(
    candidates: list[CryptoMetrics],
    histories: dict[str, pd.DataFrame],
    atr_length: int,
) -> dict[str, CryptoRenkoConfirmation]:
    out: dict[str, CryptoRenkoConfirmation] = {}
    for asset in candidates:
        hist = histories.get(asset.exchange_symbol)
        if hist is None or hist.empty:
            out[asset.ticker] = CryptoRenkoConfirmation(
                ticker=asset.ticker, name=asset.name, theme=asset.theme,
                exchange_symbol=asset.exchange_symbol, renko_status="ERROR",
                final_action="WATCHLIST", aligned=None, bias="MIXED",
                px_3m=None, px_m=None, px_w=None, streak_3m=None, streak_m=None,
                streak_w=None, roc14=None, roc21=None, close=None,
                reason="history unavailable",
            )
            continue

        px_3m, streak_3m, reason_3m = renko_for_frame(hist, "QE", atr_length)
        px_m, streak_m, reason_m = renko_for_frame(hist, "ME", atr_length)
        px_w, streak_w, reason_w = renko_for_frame(hist, "W-SUN", atr_length)
        reasons = [r for r in [reason_3m, reason_m, reason_w] if r != "ok"]

        aligned = None
        if px_3m is not None and px_m is not None and px_w is not None:
            aligned = 1 if px_3m == px_m == px_w == 1 else (-1 if px_3m == px_m == px_w == -1 else 0)

        weekly = resample_ohlcv(hist, "W-SUN")["Close"].dropna()
        roc14 = pct_change(weekly, 14)
        roc21 = pct_change(weekly, 21)
        bias = "BULL" if aligned == 1 else "BEAR" if aligned == -1 else "MIXED"

        if reasons:
            status, action, reason = "ERROR", "WATCHLIST", "; ".join(reasons)[:140]
        elif aligned == 1 and (roc14 or 0.0) > 0.0 and (roc21 or 0.0) > 0.0:
            status, action, reason = "BULL CONFIRMED", "VALIDATED", "3M/M/W bullish + ROC14/ROC21 positive"
        elif aligned == 1:
            status, action, reason = "BULL WEAK ROC", "WATCHLIST", "Renko bullish but ROC confirmation incomplete"
        elif aligned == -1:
            status, action, reason = "BEAR", "REJECTED", "Renko bearish"
        else:
            status, action, reason = "MIXED", "WATCHLIST", "No bullish Renko alignment yet"

        out[asset.ticker] = CryptoRenkoConfirmation(
            ticker=asset.ticker,
            name=asset.name,
            theme=asset.theme,
            exchange_symbol=asset.exchange_symbol,
            renko_status=status,
            final_action=action,
            aligned=aligned,
            bias=bias,
            px_3m=px_3m,
            px_m=px_m,
            px_w=px_w,
            streak_3m=streak_3m,
            streak_m=streak_m,
            streak_w=streak_w,
            roc14=round(roc14, 2) if roc14 is not None else None,
            roc21=round(roc21, 2) if roc21 is not None else None,
            close=safe_float(hist["Close"].iloc[-1]),
            reason=reason,
        )
    return out


def table_theme_scores(scores: list[CryptoThemeScore], limit: int) -> str:
    rows = []
    for score in scores[:limit]:
        rows.append([
            score.theme, score.final_score, score.assets,
            format_pct(score.median_21d), format_pct(score.median_63d),
            format_pct(score.median_rel_63d),
            f"{score.pct_above_sma50:.0f}%", f"{score.pct_above_sma200:.0f}%",
        ])
    return tabulate(rows, headers=[
        "Group", "Score", "N", "21D", "63D", "Rel63 vs BTC", ">SMA50", ">SMA200",
    ], tablefmt="github")


def table_assets(metrics: list[CryptoMetrics], limit: int) -> str:
    rows = []
    for asset in metrics[:limit]:
        rows.append([
            asset.cmc_rank,
            asset.exchange_symbol,
            asset.name[:24],
            asset.theme[:24],
            asset.score,
            format_pct(asset.perf_21d),
            format_pct(asset.perf_63d),
            format_pct(asset.rel_63d_vs_btc),
            f"{asset.rsi14:.1f}" if asset.rsi14 else "---",
            "Y" if asset.above_sma50 else "N",
        ])
    return tabulate(rows, headers=[
        "CMC", "Pair", "Name", "Group", "Score", "21D", "63D", "Rel63", "RSI", ">SMA50",
    ], tablefmt="github")


def table_renko(confirmations: list[CryptoRenkoConfirmation], limit: int | None = None) -> str:
    items = confirmations[:limit] if limit else confirmations
    rows = []
    for conf in items:
        rows.append([
            conf.final_action,
            conf.exchange_symbol,
            conf.theme[:24],
            conf.renko_status,
            conf.bias,
            f"{conf.px_3m}/{conf.px_m}/{conf.px_w}",
            format_pct(conf.roc14),
            format_pct(conf.roc21),
            conf.reason[:44],
        ])
    return tabulate(rows, headers=[
        "Final", "Pair", "Group", "Renko", "Bias", "3M/M/W", "ROC14", "ROC21", "Reason",
    ], tablefmt="github")


def combined_rank_score(asset: CryptoMetrics, conf: CryptoRenkoConfirmation) -> float:
    perf63 = asset.perf_63d or 0.0
    rel63 = asset.rel_63d_vs_btc or 0.0
    roc14 = conf.roc14 or 0.0
    roc21 = conf.roc21 or 0.0
    return asset.score * 0.34 + perf63 * 0.14 + rel63 * 0.24 + roc14 * 0.14 + roc21 * 0.14


def unified_ranking(metrics: list[CryptoMetrics], renko: dict[str, CryptoRenkoConfirmation]) -> list[dict[str, Any]]:
    asset_by_ticker = {asset.ticker: asset for asset in metrics}
    order: list[dict[str, Any]] = []
    seen: set[str] = set()
    validated = []
    for ticker, conf in renko.items():
        if conf.final_action != "VALIDATED" or ticker not in asset_by_ticker:
            continue
        asset = asset_by_ticker[ticker]
        validated.append((combined_rank_score(asset, conf), asset, conf))
    for _, asset, _ in sorted(validated, key=lambda item: item[0], reverse=True):
        order.append({
            "ticker": asset.ticker,
            "exchange_symbol": asset.exchange_symbol,
            "rel": asset.rel_63d_vs_btc,
            "score": asset.score,
            "validated": True,
        })
        seen.add(asset.ticker)
    rest = sorted(
        [asset for asset in metrics if asset.ticker not in seen and asset.rel_63d_vs_btc is not None],
        key=lambda item: (item.rel_63d_vs_btc or 0.0, item.score),
        reverse=True,
    )
    for asset in rest:
        order.append({
            "ticker": asset.ticker,
            "exchange_symbol": asset.exchange_symbol,
            "rel": asset.rel_63d_vs_btc,
            "score": asset.score,
            "validated": False,
        })
    return order


def _load_tracker() -> dict[str, Any]:
    try:
        with open(TRACKER_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_tracker(state: dict[str, Any]) -> None:
    try:
        with open(TRACKER_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def update_tracker(ranking: list[dict[str, Any]], top_n: int = TRACK_TOP_N) -> dict[str, Any]:
    today = datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%d")
    state = _load_tracker()
    tickers_state: dict[str, dict[str, Any]] = state.get("tickers", {})
    current = ranking[:top_n]
    cur_set = {item["ticker"] for item in current}
    if state.get("updated") == today:
        return state.get("last_events", {})

    prev_update = state.get("updated")
    prev_active = {ticker for ticker, info in tickers_state.items() if info.get("active")}
    events: dict[str, Any] = {"persistent": [], "new": [], "weakening": [], "dropped": []}

    for rank, item in enumerate(current, 1):
        ticker = item["ticker"]
        info = tickers_state.get(ticker, {})
        was_in = bool(info.get("active")) and info.get("last_seen") == prev_update
        rel = round(float(item.get("rel") or 0.0), 2)
        prev_rel = info.get("last_rel")
        prev_rank = info.get("last_rank")
        streak = (info.get("streak", 0) + 1) if was_in else 1
        if not was_in:
            events["new"].append(item["exchange_symbol"])
        elif prev_rel is not None and (rel < prev_rel - 1.0 or (prev_rank and rank > prev_rank)):
            events["weakening"].append(item["exchange_symbol"])
        tickers_state[ticker] = {
            "streak": streak,
            "days": info.get("days", 0) + 1,
            "first_seen": info.get("first_seen", today),
            "last_seen": today,
            "last_rank": rank,
            "peak_rank": min(rank, info.get("peak_rank", rank)),
            "last_rel": rel,
            "active": True,
            "exchange_symbol": item["exchange_symbol"],
        }

    for ticker in prev_active - cur_set:
        tickers_state[ticker]["active"] = False
        tickers_state[ticker]["dropped_on"] = today
        events["dropped"].append(tickers_state[ticker].get("exchange_symbol", ticker))

    events["persistent"] = sorted(
        ([tickers_state[item["ticker"]].get("exchange_symbol", item["ticker"]),
          tickers_state[item["ticker"]]["streak"]] for item in current),
        key=lambda row: -row[1],
    )[:5]
    events["durable"] = sorted(
        ([tickers_state[ticker].get("exchange_symbol", ticker),
          tickers_state[ticker]["streak"], tickers_state[ticker]["last_rel"],
          round(tickers_state[ticker]["streak"] * tickers_state[ticker]["last_rel"], 1)]
         for ticker in cur_set),
        key=lambda row: -row[3],
    )[:5]

    state.update({"updated": today, "prev_update": prev_update,
                  "tickers": tickers_state, "last_events": events})
    _save_tracker(state)
    return events


def tracker_lines(events: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    if not events:
        return lines
    if events.get("durable"):
        lines.append("Durables")
        for ticker, streak, rel, _ in events["durable"][:TRACK_TOP_N]:
            lines.append(f"{ticker} {streak}j - {rel:+.0f}%")
    if events.get("new"):
        lines.append("Entrants: " + ", ".join(events["new"][:TRACK_TOP_N]))
    if events.get("weakening"):
        lines.append("Faiblissent: " + ", ".join(events["weakening"][:TRACK_TOP_N]))
    if events.get("dropped"):
        lines.append("Sortis: " + ", ".join(events["dropped"][:TRACK_TOP_N]))
    return lines


def build_telegram_summary(
    theme_scores: list[CryptoThemeScore],
    ranking: list[dict[str, Any]],
    exchange_id: str,
    coverage: str,
    events: dict[str, Any] | None = None,
) -> str:
    lines = ["BINANCE + CMC TOP100", f"Exchange: {exchange_id} | {coverage}"]
    if theme_scores:
        lines.append("Groups: " + ", ".join(score.theme for score in theme_scores[:2]))

    lines.append("")
    lines.append("TOP CRYPTO (vs BTC)")
    if ranking:
        for i, item in enumerate(ranking[:TRACK_TOP_N], 1):
            tag = "" if item["validated"] else " attente"
            lines.append(f"{i}. {item['exchange_symbol']} ({format_pct(item['rel'])}){tag}")
    else:
        lines.append("---")

    tl = tracker_lines(events or {})
    if tl:
        lines.append("")
        lines.append("SUIVI")
        lines.extend(tl)

    stamp = datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
    lines += ["", f"{stamp} Paris"]
    return "\n".join(lines)


def build_report(
    theme_scores: list[CryptoThemeScore],
    metrics: list[CryptoMetrics],
    renko: dict[str, CryptoRenkoConfirmation],
    unavailable: list[CmcAsset],
    excluded: list[CmcAsset],
    cmc_source: str,
    exchange_id: str,
    quote: str,
    benchmark: str,
    args: argparse.Namespace,
    events: dict[str, Any],
) -> str:
    paris = pytz.timezone("Europe/Paris")
    now = datetime.now(paris).strftime("%Y-%m-%d %H:%M %Z")
    coverage = f"{len(metrics)} tradable assets from CMC Top {args.cmc_limit}"
    lines = [
        "BINANCE + CMC TOP100 PIPELINE",
        f"Generated: {now}",
        f"CMC source: {cmc_source}",
        f"Exchange: {exchange_id} | Quote: {quote} | Benchmark: {benchmark}",
        f"Universe: {coverage} | excluded={len(excluded)} | unavailable={len(unavailable)}",
        "",
        "DOMINANT CRYPTO GROUPS",
        table_theme_scores(theme_scores, args.top_groups),
        "",
        "TOP CRYPTO BY SCORE",
        table_assets(metrics, args.top_assets),
    ]

    if events:
        lines += ["", "SUIVI DE PERSISTANCE"]
        if events.get("durable"):
            lines.append("Durables: " + ", ".join(
                f"{ticker} [{streak}j, Rel {rel:+.0f}%, score {score:.0f}]"
                for ticker, streak, rel, score in events["durable"]
            ))
        if events.get("new"):
            lines.append("Entrants: " + ", ".join(events["new"]))
        if events.get("weakening"):
            lines.append("Faiblissent: " + ", ".join(events["weakening"]))
        if events.get("dropped"):
            lines.append("Sortis du TOP: " + ", ".join(events["dropped"]))

    if renko:
        confirmations = list(renko.values())
        validated = sorted(
            [item for item in confirmations if item.final_action == "VALIDATED"],
            key=lambda item: (item.theme, item.ticker),
        )
        watchlist = sorted(
            [item for item in confirmations if item.final_action == "WATCHLIST"],
            key=lambda item: (item.theme, item.ticker),
        )
        rejected = sorted(
            [item for item in confirmations if item.final_action == "REJECTED"],
            key=lambda item: (item.theme, item.ticker),
        )
        lines += [
            "",
            "FINAL SELECTION - RENKO VALIDATED",
            table_renko(validated) if validated else "No candidate fully validated by Renko.",
            "",
            "WATCHLIST - WAITING FOR RENKO",
            table_renko(watchlist, args.renko_watchlist_limit) if watchlist else "No watchlist candidate.",
            "",
            "REJECTED BY RENKO",
            table_renko(rejected, args.renko_watchlist_limit) if rejected else "None.",
        ]
    else:
        lines += ["", "RENKO FILTER", "Skipped (--no-renko)."]

    if unavailable:
        lines += [
            "",
            "CMC TOP100 NOT TRADABLE ON SELECTED EXCHANGE/QUOTE",
            ", ".join(f"#{asset.rank} {asset.symbol}" for asset in unavailable[:30]),
        ]
    if excluded:
        lines += [
            "",
            "EXCLUDED FROM MOMENTUM UNIVERSE",
            ", ".join(f"#{asset.rank} {asset.symbol}" for asset in excluded[:30]),
        ]

    lines += [
        "",
        "METHOD",
        "Universe = CoinMarketCap Top 100, intersected with active Binance spot pairs in the selected quote.",
        "Stablecoins and the benchmark base are excluded from momentum ranking by default.",
        "Score = momentum 21D/63D/126D + relative strength vs BTC + volume ratio + SMA50/SMA200 + 20D high proximity.",
        "Final validation = local ATR Renko alignment 3M/M/W bullish with weekly ROC14/ROC21 positive.",
        "CMC rank is used as the market-cap overlay/universe filter, not as a buy signal.",
    ]
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Binance crypto rotation over CoinMarketCap Top 100")
    parser.add_argument("--exchange", default="auto", help="ccxt exchange id: auto, binance, binanceus...")
    parser.add_argument("--quote", default="USDT")
    parser.add_argument("--benchmark", default="BTC/USDT")
    parser.add_argument("--cmc-limit", type=int, default=100)
    parser.add_argument("--max-assets", type=int, default=0, help="Limit tradable universe after CMC/Binance intersection")
    parser.add_argument("--timeframe", default="1d")
    parser.add_argument("--history-candles", type=int, default=1500)
    parser.add_argument("--history-pause", type=float, default=0.05)
    parser.add_argument("--top-groups", type=int, default=4)
    parser.add_argument("--top-assets", type=int, default=12)
    parser.add_argument("--include-stables", action="store_true")
    parser.add_argument("--include-benchmark", action="store_true")
    parser.add_argument("--no-renko", action="store_true")
    parser.add_argument("--renko-length", type=int, default=14)
    parser.add_argument("--renko-candidates", type=int, default=30)
    parser.add_argument("--renko-watchlist-limit", type=int, default=20)
    parser.add_argument("--json", default=REPORT_JSON)
    parser.add_argument("--txt", default=REPORT_TXT)
    parser.add_argument("--telegram", action="store_true", default=True)
    parser.add_argument("--no-telegram", action="store_false", dest="telegram")
    parser.add_argument("--telegram-full", action="store_true")
    return parser.parse_args()


def main() -> int:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()
    t0 = time.time()

    try:
        print(f"Fetching CoinMarketCap Top {args.cmc_limit}...")
        cmc_assets, cmc_source = fetch_cmc_top(args.cmc_limit)
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: CoinMarketCap fetch failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 0

    try:
        print(f"Loading ccxt exchange: {args.exchange}...")
        exchange, exchange_id, exchange_errors = load_exchange(args.exchange)
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: exchange unavailable: {exc}", file=sys.stderr)
        return 0
    for error in exchange_errors:
        print(f"  exchange fallback: {error}")

    components, unavailable, excluded = build_universe(
        cmc_assets=cmc_assets,
        markets=exchange.markets,
        quote=args.quote,
        benchmark_symbol=args.benchmark,
        include_stables=args.include_stables,
        include_benchmark=args.include_benchmark,
    )
    if args.max_assets and len(components) > args.max_assets:
        components = components.head(args.max_assets).reset_index(drop=True)

    if components.empty:
        print("ERROR: no tradable CMC Top 100 asset found on selected exchange/quote.", file=sys.stderr)
        return 0

    symbols = list(dict.fromkeys([args.benchmark] + components["exchange_symbol"].tolist()))
    if args.benchmark not in exchange.markets:
        print(f"ERROR: benchmark {args.benchmark} unavailable on {exchange_id}.", file=sys.stderr)
        return 0

    print(f"Tradable universe: {len(components)}/{args.cmc_limit} CMC assets on {exchange_id}.")
    print(f"Downloading OHLCV ({args.timeframe}, {args.history_candles} candles)...")
    histories = download_histories(
        exchange=exchange,
        symbols=symbols,
        timeframe=args.timeframe,
        candles=args.history_candles,
        pause=args.history_pause,
    )
    benchmark_close = histories.get(args.benchmark, pd.DataFrame()).get("Close", pd.Series(dtype=float))
    if benchmark_close.empty:
        print(f"ERROR: benchmark history unavailable: {args.benchmark}", file=sys.stderr)
        return 0

    print("Computing crypto metrics...")
    metrics = build_crypto_metrics(components, histories, benchmark_close)
    if not metrics:
        print("ERROR: insufficient histories for metrics.", file=sys.stderr)
        return 0

    theme_scores = build_theme_scores(metrics)
    renko: dict[str, CryptoRenkoConfirmation] = {}
    if args.no_renko:
        print("Skipping Renko validation.")
    else:
        print("Applying local Renko filter to top crypto candidates...")
        candidates = sorted(
            metrics,
            key=lambda item: (item.score, item.rel_63d_vs_btc or -999.0),
            reverse=True,
        )[:args.renko_candidates]
        renko = run_renko_filter(candidates, histories, args.renko_length)

    today_paris = datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%d")
    already_sent_today = _load_tracker().get("updated") == today_paris

    ranking = unified_ranking(metrics, renko)
    events = update_tracker(ranking) if args.telegram else {}
    report = build_report(
        theme_scores=theme_scores,
        metrics=metrics,
        renko=renko,
        unavailable=unavailable,
        excluded=excluded,
        cmc_source=cmc_source,
        exchange_id=exchange_id,
        quote=args.quote,
        benchmark=args.benchmark,
        args=args,
        events=events,
    )

    print()
    print(report)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_seconds": round(time.time() - t0, 2),
        "cmc_source": cmc_source,
        "exchange": exchange_id,
        "quote": args.quote,
        "benchmark": args.benchmark,
        "coverage": {
            "cmc_limit": args.cmc_limit,
            "tradable": len(metrics),
            "unavailable": [asdict(asset) for asset in unavailable],
            "excluded": [asdict(asset) for asset in excluded],
        },
        "themes": [asdict(score) for score in theme_scores],
        "assets": [asdict(item) for item in metrics],
        "renko": {ticker: asdict(conf) for ticker, conf in renko.items()},
    }
    with open(args.json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(args.txt, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\nSaved: {args.txt}")
    print(f"Saved: {args.json}")

    coverage = f"{len(metrics)}/{args.cmc_limit} tradable"
    text = report if args.telegram_full else build_telegram_summary(
        theme_scores=theme_scores,
        ranking=ranking,
        exchange_id=exchange_id,
        coverage=coverage,
        events=events,
    )
    print("\nTELEGRAM SUMMARY:\n" + text)
    if args.telegram and not already_sent_today:
        send_telegram(text)
    elif args.telegram and already_sent_today:
        print(f"Already sent today ({today_paris}) - 1x/day, no resend.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
