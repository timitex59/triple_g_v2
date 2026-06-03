#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nasdaq_sector_pipeline.py

Pipeline complet pour isoler les themes dominants du Nasdaq-100:
- composants Nasdaq-100 depuis Wikipedia
- classification secteur / industrie via Yahoo Finance + overrides locaux
- momentum actions et secteurs
- validation par actualite recente via Google News RSS
- ETF proxies par theme et classement des ETF les plus forts
- top actions par theme dominant

Le script ne donne pas de conseil financier. Il construit un tableau de bord de
force relative et de confirmation narrative.
"""

from __future__ import annotations

import argparse
from io import StringIO
import json
import math
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus

import feedparser
import numpy as np
import pandas as pd
import pytz
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"
REPORT_JSON = os.path.join(SCRIPT_DIR, "nasdaq_sector_pipeline_report.json")
REPORT_TXT = os.path.join(SCRIPT_DIR, "nasdaq_sector_pipeline_report.txt")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


THEME_KEYWORDS: dict[str, list[str]] = {
    "Semiconductors / AI infrastructure": [
        "semiconductor", "semiconductors", "chip", "chips", "ai chip",
        "data center", "gpu", "memory", "wafer", "foundry", "storage",
    ],
    "Cybersecurity": [
        "cybersecurity", "security software", "zero trust", "firewall",
        "endpoint security", "cloud security", "ransomware",
    ],
    "Cloud / Data software": [
        "cloud", "saas", "software", "observability", "data platform",
        "enterprise software", "ai software",
    ],
    "AI platforms / Mega-cap tech": [
        "artificial intelligence", "ai", "hyperscaler", "cloud capex",
        "large language model", "agentic ai",
    ],
    "Consumer internet / E-commerce": [
        "e-commerce", "streaming", "online advertising", "marketplace",
        "consumer internet",
    ],
    "Healthcare / Biotech": [
        "biotech", "drug", "clinical", "genomics", "medical devices",
        "healthcare",
    ],
    "Industrials / Automation": [
        "automation", "industrial", "aerospace", "defense", "rail",
        "manufacturing",
    ],
    "Utilities / Power": [
        "power demand", "electricity", "utilities", "grid", "nuclear",
        "data center power",
    ],
}


ETF_UNIVERSE: list[dict[str, str]] = [
    {"ticker": "QQQ", "tv_symbol": "NASDAQ:QQQ", "theme": "Nasdaq-100 benchmark", "name": "Invesco QQQ Trust"},
    {"ticker": "QQQM", "tv_symbol": "NASDAQ:QQQM", "theme": "Nasdaq-100 benchmark", "name": "Invesco Nasdaq 100 ETF"},
    {"ticker": "SMH", "tv_symbol": "NASDAQ:SMH", "theme": "Semiconductors / AI infrastructure", "name": "VanEck Semiconductor ETF"},
    {"ticker": "SOXX", "tv_symbol": "NASDAQ:SOXX", "theme": "Semiconductors / AI infrastructure", "name": "iShares Semiconductor ETF"},
    {"ticker": "XSD", "tv_symbol": "AMEX:XSD", "theme": "Semiconductors / AI infrastructure", "name": "SPDR S&P Semiconductor ETF"},
    {"ticker": "SOXQ", "tv_symbol": "NASDAQ:SOXQ", "theme": "Semiconductors / AI infrastructure", "name": "Invesco PHLX Semiconductor ETF"},
    {"ticker": "PSI", "tv_symbol": "AMEX:PSI", "theme": "Semiconductors / AI infrastructure", "name": "Invesco Semiconductors ETF"},
    {"ticker": "CIBR", "tv_symbol": "NASDAQ:CIBR", "theme": "Cybersecurity", "name": "First Trust Nasdaq Cybersecurity ETF"},
    {"ticker": "HACK", "tv_symbol": "AMEX:HACK", "theme": "Cybersecurity", "name": "Amplify Cybersecurity ETF"},
    {"ticker": "BUG", "tv_symbol": "NASDAQ:BUG", "theme": "Cybersecurity", "name": "Global X Cybersecurity ETF"},
    {"ticker": "IHAK", "tv_symbol": "AMEX:IHAK", "theme": "Cybersecurity", "name": "iShares Cybersecurity and Tech ETF"},
    {"ticker": "WCLD", "tv_symbol": "NASDAQ:WCLD", "theme": "Cloud / Data software", "name": "WisdomTree Cloud Computing Fund"},
    {"ticker": "SKYY", "tv_symbol": "NASDAQ:SKYY", "theme": "Cloud / Data software", "name": "First Trust Cloud Computing ETF"},
    {"ticker": "CLOU", "tv_symbol": "NASDAQ:CLOU", "theme": "Cloud / Data software", "name": "Global X Cloud Computing ETF"},
    {"ticker": "IGV", "tv_symbol": "AMEX:IGV", "theme": "Cloud / Data software", "name": "iShares Expanded Tech-Software ETF"},
    {"ticker": "FDN", "tv_symbol": "AMEX:FDN", "theme": "Consumer internet / E-commerce", "name": "First Trust Dow Jones Internet ETF"},
    {"ticker": "IBB", "tv_symbol": "NASDAQ:IBB", "theme": "Healthcare / Biotech", "name": "iShares Biotechnology ETF"},
    {"ticker": "XBI", "tv_symbol": "AMEX:XBI", "theme": "Healthcare / Biotech", "name": "SPDR S&P Biotech ETF"},
    {"ticker": "BOTZ", "tv_symbol": "NASDAQ:BOTZ", "theme": "AI platforms / Mega-cap tech", "name": "Global X Robotics & AI ETF"},
    {"ticker": "ROBO", "tv_symbol": "AMEX:ROBO", "theme": "AI platforms / Mega-cap tech", "name": "ROBO Global Robotics and Automation ETF"},
    {"ticker": "IRBO", "tv_symbol": "AMEX:IRBO", "theme": "AI platforms / Mega-cap tech", "name": "iShares Robotics and AI ETF"},
    {"ticker": "THNQ", "tv_symbol": "AMEX:THNQ", "theme": "AI platforms / Mega-cap tech", "name": "ROBO Global Artificial Intelligence ETF"},
    {"ticker": "XLU", "tv_symbol": "AMEX:XLU", "theme": "Utilities / Power", "name": "Utilities Select Sector SPDR Fund"},
    {"ticker": "GRID", "tv_symbol": "NASDAQ:GRID", "theme": "Utilities / Power", "name": "First Trust Nasdaq Clean Edge Smart Grid ETF"},
    {"ticker": "ITA", "tv_symbol": "AMEX:ITA", "theme": "Industrials / Automation", "name": "iShares US Aerospace & Defense ETF"},
    {"ticker": "ROKT", "tv_symbol": "AMEX:ROKT", "theme": "Industrials / Automation", "name": "SPDR S&P Kensho Final Frontiers ETF"},
]

NYSE_TICKERS = {"LIN", "WMT", "SHOP", "TRI"}

EUROPE_ETF_ALTERNATIVES: dict[str, list[dict[str, str]]] = {
    "Semiconductors / AI infrastructure": [
        {
            "ticker": "SMH",
            "yahoo": "SMH.PA",
            "isin": "IE00BMC38736",
            "name": "VanEck Semiconductor UCITS ETF",
            "listing": "Euronext Paris",
            "currency": "EUR",
        },
        {
            "ticker": "SEME",
            "yahoo": "SEME.PA",
            "isin": "IE000I8KRLL9",
            "name": "iShares MSCI Global Semiconductors UCITS ETF",
            "listing": "Euronext Paris",
            "currency": "EUR",
        },
    ],
    "Cybersecurity": [
        {
            "ticker": "WCBR",
            "yahoo": "WCBR.PA",
            "isin": "IE00BLPK3577",
            "name": "WisdomTree Cybersecurity UCITS ETF",
            "listing": "Euronext Paris",
            "currency": "EUR",
        },
        {
            "ticker": "ISPY",
            "yahoo": "ISPY.AS",
            "isin": "IE00BYPLS672",
            "name": "L&G Cyber Security UCITS ETF",
            "listing": "Euronext Amsterdam",
            "currency": "EUR",
        },
    ],
    "AI platforms / Mega-cap tech": [
        {
            "ticker": "WTAI",
            "yahoo": "WTAI.PA",
            "isin": "IE00BDVPNG13",
            "name": "WisdomTree Artificial Intelligence UCITS ETF",
            "listing": "Euronext Paris",
            "currency": "EUR",
        },
        {
            "ticker": "BOTZ",
            "yahoo": "BOTZ.L",
            "isin": "IE00BLCHJB90",
            "name": "Global X Robotics & Artificial Intelligence UCITS ETF",
            "listing": "Europe UCITS",
            "currency": "EUR/USD listings",
        },
    ],
}


TICKER_THEME_OVERRIDES: dict[str, str] = {
    "AMD": "Semiconductors / AI infrastructure",
    "NVDA": "Semiconductors / AI infrastructure",
    "AVGO": "Semiconductors / AI infrastructure",
    "ASML": "Semiconductors / AI infrastructure",
    "AMAT": "Semiconductors / AI infrastructure",
    "LRCX": "Semiconductors / AI infrastructure",
    "KLAC": "Semiconductors / AI infrastructure",
    "MU": "Semiconductors / AI infrastructure",
    "MRVL": "Semiconductors / AI infrastructure",
    "MCHP": "Semiconductors / AI infrastructure",
    "MPWR": "Semiconductors / AI infrastructure",
    "ADI": "Semiconductors / AI infrastructure",
    "TXN": "Semiconductors / AI infrastructure",
    "QCOM": "Semiconductors / AI infrastructure",
    "NXPI": "Semiconductors / AI infrastructure",
    "ARM": "Semiconductors / AI infrastructure",
    "WDC": "Semiconductors / AI infrastructure",
    "STX": "Semiconductors / AI infrastructure",
    "INTC": "Semiconductors / AI infrastructure",
    "LITE": "Semiconductors / AI infrastructure",
    "CSCO": "Cybersecurity",
    "PANW": "Cybersecurity",
    "CRWD": "Cybersecurity",
    "FTNT": "Cybersecurity",
    "ZS": "Cybersecurity",
    "DDOG": "Cloud / Data software",
    "WDAY": "Cloud / Data software",
    "TEAM": "Cloud / Data software",
    "ADBE": "Cloud / Data software",
    "ADSK": "Cloud / Data software",
    "SNPS": "Cloud / Data software",
    "CDNS": "Cloud / Data software",
    "PLTR": "AI platforms / Mega-cap tech",
    "MSFT": "AI platforms / Mega-cap tech",
    "GOOG": "AI platforms / Mega-cap tech",
    "GOOGL": "AI platforms / Mega-cap tech",
    "META": "AI platforms / Mega-cap tech",
    "AAPL": "AI platforms / Mega-cap tech",
    "AMZN": "Consumer internet / E-commerce",
    "MELI": "Consumer internet / E-commerce",
    "PDD": "Consumer internet / E-commerce",
    "DASH": "Consumer internet / E-commerce",
    "NFLX": "Consumer internet / E-commerce",
    "REGN": "Healthcare / Biotech",
    "VRTX": "Healthcare / Biotech",
    "GILD": "Healthcare / Biotech",
    "AMGN": "Healthcare / Biotech",
    "ALNY": "Healthcare / Biotech",
    "DXCM": "Healthcare / Biotech",
    "ISRG": "Healthcare / Biotech",
    "HON": "Industrials / Automation",
    "AXON": "Industrials / Automation",
    "CSX": "Industrials / Automation",
    "PCAR": "Industrials / Automation",
    "XEL": "Utilities / Power",
    "AEP": "Utilities / Power",
    "EXC": "Utilities / Power",
    "CEG": "Utilities / Power",
}


@dataclass
class AssetMetrics:
    ticker: str
    name: str
    theme: str
    sector: str
    industry: str
    close: float | None
    perf_5d: float | None
    perf_21d: float | None
    perf_63d: float | None
    perf_126d: float | None
    perf_ytd: float | None
    rel_63d_vs_qqq: float | None
    rsi14: float | None
    vol_21d: float | None
    volume_ratio: float | None
    above_sma50: bool
    above_sma200: bool
    near_20d_high: bool
    score: float


@dataclass
class ThemeScore:
    theme: str
    stocks: int
    avg_score: float
    median_21d: float | None
    median_63d: float | None
    median_rel_63d: float | None
    pct_above_sma50: float
    pct_above_sma200: float
    news_score: float
    news_hits: int
    news_sources: int
    final_score: float


@dataclass
class RenkoConfirmation:
    ticker: str
    name: str
    asset_type: str
    theme: str
    tv_symbol: str
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
    pnl_pct: float | None
    roc14: float | None
    roc21: float | None
    close: float | None
    reason: str


def fetch_nasdaq_100() -> pd.DataFrame:
    resp = requests.get(WIKI_URL, timeout=20, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for table in soup.select("table.wikitable"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [cell.get_text(" ", strip=True) for cell in rows[0].find_all(["th", "td"])]
        lowered = [h.lower() for h in headers]
        if "ticker" not in lowered:
            continue
        ticker_col = lowered.index("ticker")
        name_col = lowered.index("company") if "company" in lowered else 0
        parsed = []
        for row in rows[1:]:
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
            if len(cells) <= max(ticker_col, name_col):
                continue
            ticker = cells[ticker_col].replace(".", "-").strip()
            name = cells[name_col].strip()
            if re.match(r"^[A-Z][A-Z0-9-]{0,6}$", ticker):
                parsed.append({"name": name, "ticker": ticker})
        if len(parsed) >= 90:
            return pd.DataFrame(parsed).drop_duplicates("ticker").reset_index(drop=True)
    raise RuntimeError("Could not find Nasdaq-100 components table on Wikipedia")


def stock_tv_symbol(ticker: str) -> str:
    exchange = "NYSE" if ticker in NYSE_TICKERS else "NASDAQ"
    return f"{exchange}:{ticker}"


def etf_tv_symbol(ticker: str) -> str:
    for item in ETF_UNIVERSE:
        if item["ticker"] == ticker:
            return item["tv_symbol"]
    return f"NASDAQ:{ticker}"


def safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    except Exception:
        return None


def pct_change(series: pd.Series, periods: int) -> float | None:
    clean = series.dropna()
    if len(clean) <= periods:
        return None
    start = float(clean.iloc[-periods - 1])
    end = float(clean.iloc[-1])
    if not start:
        return None
    return (end / start - 1.0) * 100.0


def pct_change_from_date(series: pd.Series, dt: datetime) -> float | None:
    clean = series.dropna()
    if clean.empty:
        return None
    target = pd.Timestamp(dt).tz_localize(None)
    index = pd.DatetimeIndex(clean.index).tz_localize(None)
    subset = clean[index >= target]
    if subset.empty:
        return None
    start = float(subset.iloc[0])
    end = float(clean.iloc[-1])
    if not start:
        return None
    return (end / start - 1.0) * 100.0


def rsi(series: pd.Series, length: int = 14) -> float | None:
    clean = series.dropna()
    if len(clean) < length + 2:
        return None
    delta = clean.diff()
    gain = delta.clip(lower=0).rolling(length).mean()
    loss = (-delta.clip(upper=0)).rolling(length).mean()
    rs = gain / loss.replace(0, np.nan)
    val = 100 - (100 / (1 + rs.iloc[-1]))
    return safe_float(val)


def annualized_vol(series: pd.Series, periods: int = 21) -> float | None:
    clean = series.dropna()
    if len(clean) < periods + 2:
        return None
    returns = clean.pct_change().dropna().iloc[-periods:]
    return safe_float(returns.std() * math.sqrt(252) * 100)


def zscore(value: float | None, values: list[float]) -> float:
    if value is None or not values:
        return 0.0
    arr = np.array(values, dtype=float)
    std = float(np.nanstd(arr))
    if std == 0:
        return 0.0
    return float((value - float(np.nanmean(arr))) / std)


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def fetch_profiles(tickers: list[str], max_workers: int = 12) -> dict[str, dict[str, str]]:
    def one(ticker: str) -> tuple[str, dict[str, str]]:
        try:
            tk = yf.Ticker(ticker, session=_YF_SESSION) if _YF_SESSION is not None else yf.Ticker(ticker)
            info = tk.get_info()
            return ticker, {
                "sector": str(info.get("sector") or ""),
                "industry": str(info.get("industry") or ""),
                "shortName": str(info.get("shortName") or info.get("longName") or ""),
            }
        except Exception:
            return ticker, {"sector": "", "industry": "", "shortName": ""}

    profiles: dict[str, dict[str, str]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(one, t) for t in tickers]
        for fut in as_completed(futures):
            ticker, profile = fut.result()
            profiles[ticker] = profile
    return profiles


def infer_theme(ticker: str, sector: str, industry: str) -> str:
    if ticker in TICKER_THEME_OVERRIDES:
        return TICKER_THEME_OVERRIDES[ticker]
    blob = f"{sector} {industry}".lower()
    if any(k in blob for k in ["semiconductor", "computer hardware", "electronic components"]):
        return "Semiconductors / AI infrastructure"
    if "security" in blob:
        return "Cybersecurity"
    if any(k in blob for k in ["software", "information technology services"]):
        return "Cloud / Data software"
    if any(k in blob for k in ["internet", "interactive media", "entertainment", "retail"]):
        return "Consumer internet / E-commerce"
    if any(k in blob for k in ["biotechnology", "healthcare", "medical"]):
        return "Healthcare / Biotech"
    if any(k in blob for k in ["aerospace", "industrial", "railroads", "farm"]):
        return "Industrials / Automation"
    if "utilities" in blob:
        return "Utilities / Power"
    return sector or "Other"


def _build_yf_session():
    """Session curl_cffi (impersonation navigateur) pour limiter les blocages
    'Too Many Requests' de Yahoo, surtout sur les runners CI. Optionnel: si
    curl_cffi n'est pas installe, on laisse yfinance utiliser sa session par defaut."""
    try:
        from curl_cffi import requests as cffi_requests

        return cffi_requests.Session(impersonate="chrome")
    except Exception:
        return None


_YF_SESSION = _build_yf_session()


def _download_batch(
    tickers: list[str], period: str, retries: int = 4, base_delay: float = 2.0
) -> pd.DataFrame | None:
    """Telecharge un lot avec retry + backoff exponentiel. Yahoo peut soit lever
    YFRateLimitError, soit retourner un DataFrame vide en cas de rate limit:
    on reessaie dans les deux cas."""
    delay = base_delay
    for attempt in range(1, retries + 1):
        try:
            kwargs: dict[str, Any] = dict(
                period=period,
                interval="1d",
                auto_adjust=True,
                group_by="ticker",
                progress=False,
                threads=False,
            )
            if _YF_SESSION is not None:
                kwargs["session"] = _YF_SESSION
            data = yf.download(tickers, **kwargs)
            if data is not None and not data.empty:
                return data
        except Exception as exc:  # noqa: BLE001 - inclut YFRateLimitError
            if attempt == retries:
                print(f"  download failed for {tickers}: {exc}", file=sys.stderr)
        if attempt < retries:
            time.sleep(delay)
            delay = min(delay * 2, 30.0)
    return None


def download_history(
    tickers: list[str], period: str = "9mo", batch_size: int = 8, pause: float = 1.5
) -> dict[str, pd.DataFrame]:
    """Telecharge l'historique par petits lots avec retry/backoff, pour resister
    au rate limit Yahoo (CI). Renvoie {ticker: DataFrame} pour les tickers obtenus."""
    result: dict[str, pd.DataFrame] = {}
    unique = list(dict.fromkeys(tickers))
    for i in range(0, len(unique), batch_size):
        batch = unique[i : i + batch_size]
        data = _download_batch(batch, period)
        if data is None or data.empty:
            continue
        if isinstance(data.columns, pd.MultiIndex):
            available = set(data.columns.get_level_values(0))
            for ticker in batch:
                if ticker in available:
                    frame = data[ticker].dropna(how="all").copy()
                    if not frame.empty:
                        result[ticker] = frame
        else:
            # Lot a un seul ticker: colonnes a plat
            frame = data.dropna(how="all").copy()
            if not frame.empty:
                result[batch[0]] = frame
        if i + batch_size < len(unique):
            time.sleep(pause)
    return result


def build_asset_metrics(
    components: pd.DataFrame,
    profiles: dict[str, dict[str, str]],
    histories: dict[str, pd.DataFrame],
    qqq_close: pd.Series,
) -> list[AssetMetrics]:
    raw_rows: list[dict[str, Any]] = []
    year_start = datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc)
    qqq_63d = pct_change(qqq_close, 63)

    for row in components.itertuples(index=False):
        ticker = str(row.ticker)
        hist = histories.get(ticker)
        if hist is None or hist.empty or "Close" not in hist:
            continue
        close = hist["Close"].dropna()
        if len(close) < 65:
            continue
        volume = hist["Volume"].dropna() if "Volume" in hist else pd.Series(dtype=float)
        profile = profiles.get(ticker, {})
        sector = profile.get("sector", "")
        industry = profile.get("industry", "")
        theme = infer_theme(ticker, sector, industry)

        perf_5d = pct_change(close, 5)
        perf_21d = pct_change(close, 21)
        perf_63d = pct_change(close, 63)
        perf_126d = pct_change(close, 126)
        perf_ytd = pct_change_from_date(close, year_start)
        rel_63d = None if perf_63d is None or qqq_63d is None else perf_63d - qqq_63d
        sma50 = close.rolling(50).mean().iloc[-1]
        sma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else np.nan
        high20 = close.rolling(20).max().iloc[-1]
        vol_ratio = None
        if len(volume) >= 22:
            avg_vol = float(volume.iloc[-22:-1].mean())
            vol_ratio = safe_float(float(volume.iloc[-1]) / avg_vol) if avg_vol else None

        raw_rows.append({
            "ticker": ticker,
            "name": str(row.name),
            "theme": theme,
            "sector": sector,
            "industry": industry,
            "close": safe_float(close.iloc[-1]),
            "perf_5d": perf_5d,
            "perf_21d": perf_21d,
            "perf_63d": perf_63d,
            "perf_126d": perf_126d,
            "perf_ytd": perf_ytd,
            "rel_63d_vs_qqq": rel_63d,
            "rsi14": rsi(close),
            "vol_21d": annualized_vol(close),
            "volume_ratio": vol_ratio,
            "above_sma50": bool(close.iloc[-1] > sma50) if not pd.isna(sma50) else False,
            "above_sma200": bool(close.iloc[-1] > sma200) if not pd.isna(sma200) else False,
            "near_20d_high": bool(close.iloc[-1] >= high20 * 0.97) if not pd.isna(high20) else False,
        })

    score_fields = ["perf_21d", "perf_63d", "perf_126d", "rel_63d_vs_qqq", "volume_ratio"]
    distributions = {
        field: [float(r[field]) for r in raw_rows if r.get(field) is not None]
        for field in score_fields
    }
    metrics: list[AssetMetrics] = []
    for r in raw_rows:
        score = 50.0
        score += 9.0 * zscore(r["perf_21d"], distributions["perf_21d"])
        score += 13.0 * zscore(r["perf_63d"], distributions["perf_63d"])
        score += 7.0 * zscore(r["perf_126d"], distributions["perf_126d"])
        score += 10.0 * zscore(r["rel_63d_vs_qqq"], distributions["rel_63d_vs_qqq"])
        score += 4.0 * zscore(r["volume_ratio"], distributions["volume_ratio"])
        score += 5.0 if r["above_sma50"] else -5.0
        score += 4.0 if r["above_sma200"] else -3.0
        score += 3.0 if r["near_20d_high"] else 0.0
        r["score"] = round(clamp(score, 0.0, 100.0), 2)
        metrics.append(AssetMetrics(**r))
    return metrics


def parse_entry_date(entry: Any) -> datetime | None:
    parsed = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not parsed:
        return None
    return datetime(*parsed[:6], tzinfo=timezone.utc)


def fetch_news_for_theme(theme: str, days: int, max_items: int = 12) -> dict[str, Any]:
    terms = [theme] + THEME_KEYWORDS.get(theme, [])[:5]
    query = " OR ".join(f'"{t}"' if " " in t else t for t in terms)
    url = f"https://news.google.com/rss/search?q={quote_plus(query + ' Nasdaq OR stocks')}&hl=en-US&gl=US&ceid=US:en"
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    feed = feedparser.parse(url)
    entries = []
    sources: set[str] = set()
    keyword_hits = 0
    for entry in feed.entries[:max_items * 3]:
        dt = parse_entry_date(entry)
        if dt and dt < cutoff:
            continue
        title = re.sub(r"\s+", " ", getattr(entry, "title", "")).strip()
        link = getattr(entry, "link", "")
        source = ""
        if hasattr(entry, "source"):
            source = getattr(entry.source, "title", "") or ""
        sources.add(source or "Unknown")
        blob = title.lower()
        keyword_hits += sum(1 for k in THEME_KEYWORDS.get(theme, []) if k.lower() in blob)
        entries.append({
            "title": title,
            "source": source or "Unknown",
            "published": dt.isoformat() if dt else "",
            "link": link,
        })
        if len(entries) >= max_items:
            break
    score = min(100.0, len(entries) * 5.0 + len(sources) * 6.0 + keyword_hits * 2.0)
    return {
        "theme": theme,
        "query_url": url,
        "hits": len(entries),
        "sources": len(sources),
        "keyword_hits": keyword_hits,
        "score": round(score, 2),
        "entries": entries,
    }


def build_theme_scores(metrics: list[AssetMetrics], news: dict[str, dict[str, Any]]) -> list[ThemeScore]:
    rows = []
    df = pd.DataFrame([asdict(m) for m in metrics])
    if df.empty:
        return rows
    for theme, g in df.groupby("theme"):
        if len(g) < 2:
            continue
        news_blob = news.get(theme, {})
        avg_score = float(g["score"].mean())
        median_21d = safe_float(g["perf_21d"].median())
        median_63d = safe_float(g["perf_63d"].median())
        median_rel = safe_float(g["rel_63d_vs_qqq"].median())
        pct_sma50 = float(g["above_sma50"].mean() * 100)
        pct_sma200 = float(g["above_sma200"].mean() * 100)
        news_score = float(news_blob.get("score", 0.0))
        final = (
            avg_score * 0.58
            + clamp((median_63d or 0.0) + 25.0, 0.0, 100.0) * 0.12
            + pct_sma50 * 0.10
            + pct_sma200 * 0.06
            + news_score * 0.14
        )
        rows.append(ThemeScore(
            theme=theme,
            stocks=int(len(g)),
            avg_score=round(avg_score, 2),
            median_21d=round(median_21d, 2) if median_21d is not None else None,
            median_63d=round(median_63d, 2) if median_63d is not None else None,
            median_rel_63d=round(median_rel, 2) if median_rel is not None else None,
            pct_above_sma50=round(pct_sma50, 1),
            pct_above_sma200=round(pct_sma200, 1),
            news_score=round(news_score, 2),
            news_hits=int(news_blob.get("hits", 0)),
            news_sources=int(news_blob.get("sources", 0)),
            final_score=round(final, 2),
        ))
    return sorted(rows, key=lambda x: x.final_score, reverse=True)


def build_etf_metrics(top_themes: list[str]) -> list[AssetMetrics]:
    wanted = [e for e in ETF_UNIVERSE if e["theme"] in set(top_themes) or e["theme"] == "Nasdaq-100 benchmark"]
    tickers = [e["ticker"] for e in wanted]
    histories = download_history(tickers, period="9mo")
    qqq_close = histories.get("QQQ", pd.DataFrame()).get("Close", pd.Series(dtype=float))
    components = pd.DataFrame([{"ticker": e["ticker"], "name": e["name"]} for e in wanted])
    profiles = {
        e["ticker"]: {"sector": "ETF", "industry": e["theme"], "shortName": e["name"]}
        for e in wanted
    }
    metrics = build_asset_metrics(components, profiles, histories, qqq_close)
    theme_by_ticker = {e["ticker"]: e["theme"] for e in wanted}
    for m in metrics:
        m.theme = theme_by_ticker.get(m.ticker, m.theme)
        m.sector = "ETF"
        m.industry = m.theme
    return sorted(metrics, key=lambda x: x.score, reverse=True)


def build_europe_etf_metrics(top_themes: list[str]) -> list[AssetMetrics]:
    wanted = europe_etf_alternatives(top_themes, limit=20)
    if not wanted:
        return []

    yahoo_to_meta = {item["yahoo"]: item for item in wanted if item.get("yahoo")}
    tickers = list(yahoo_to_meta)
    benchmark = "EQQQ.PA"
    histories = download_history(tickers + [benchmark], period="9mo")
    benchmark_close = histories.get(benchmark, pd.DataFrame()).get("Close", pd.Series(dtype=float))
    if benchmark_close.empty:
        benchmark_close = download_history(["QQQ"], period="9mo").get("QQQ", pd.DataFrame()).get("Close", pd.Series(dtype=float))

    components = pd.DataFrame([
        {"ticker": yahoo, "name": yahoo_to_meta[yahoo]["name"]}
        for yahoo in tickers
    ])
    profiles = {
        yahoo: {"sector": "ETF UCITS", "industry": yahoo_to_meta[yahoo]["theme"], "shortName": yahoo_to_meta[yahoo]["name"]}
        for yahoo in tickers
    }
    source_histories = {k: v for k, v in histories.items() if k in yahoo_to_meta}
    metrics = build_asset_metrics(components, profiles, source_histories, benchmark_close)
    for m in metrics:
        meta = yahoo_to_meta.get(m.ticker)
        if not meta:
            continue
        m.ticker = meta["ticker"]
        m.theme = meta["theme"]
        m.sector = "ETF UCITS"
        m.industry = f"{meta['listing']} | {meta['isin']}"
    return sorted(metrics, key=lambda x: x.score, reverse=True)


def format_pct(value: float | None) -> str:
    if value is None:
        return "---"
    return f"{value:+.2f}%"


def table_theme_scores(scores: list[ThemeScore], limit: int) -> str:
    rows = []
    for s in scores[:limit]:
        rows.append([
            s.theme, s.final_score, s.stocks, format_pct(s.median_21d),
            format_pct(s.median_63d), format_pct(s.median_rel_63d),
            f"{s.pct_above_sma50:.0f}%", f"{s.news_hits}/{s.news_sources}",
        ])
    return tabulate(rows, headers=[
        "Theme", "Score", "N", "21D", "63D", "Rel63 vs QQQ", ">SMA50", "News src",
    ], tablefmt="github")


def table_assets(metrics: list[AssetMetrics], limit: int) -> str:
    rows = []
    for m in metrics[:limit]:
        rows.append([
            m.ticker, m.name[:28], m.theme[:30], m.score,
            format_pct(m.perf_21d), format_pct(m.perf_63d),
            format_pct(m.rel_63d_vs_qqq), f"{m.rsi14:.1f}" if m.rsi14 else "---",
            "Y" if m.above_sma50 else "N",
        ])
    return tabulate(rows, headers=[
        "Ticker", "Name", "Theme", "Score", "21D", "63D", "Rel63", "RSI", ">SMA50",
    ], tablefmt="github")


def table_etfs(metrics: list[AssetMetrics], limit: int) -> str:
    rows = []
    for m in metrics[:limit]:
        rows.append([
            m.ticker, m.name[:32], m.theme[:34], m.score,
            format_pct(m.perf_21d), format_pct(m.perf_63d),
            format_pct(m.perf_126d), format_pct(m.rel_63d_vs_qqq),
        ])
    return tabulate(rows, headers=[
        "ETF", "Name", "Theme", "Score", "21D", "63D", "126D", "Rel63",
    ], tablefmt="github")


def top_stocks_by_theme(metrics: list[AssetMetrics], themes: list[str], per_theme: int) -> dict[str, list[AssetMetrics]]:
    out: dict[str, list[AssetMetrics]] = {}
    for theme in themes:
        selected = [m for m in metrics if m.theme == theme]
        out[theme] = sorted(selected, key=lambda x: x.score, reverse=True)[:per_theme]
    return out


def renko_final_action(snap: Any | None) -> tuple[str, str, str]:
    if snap is None:
        return "ERROR", "WATCHLIST", "Renko data unavailable"
    if snap.aligned == 1 and snap.bias_state_after == 1 and (snap.price_roc14 or 0) > 0 and (snap.price_roc21 or 0) > 0:
        return "BULL CONFIRMED", "VALIDATED", "3M/M/W bullish + ROC14/ROC21 positive"
    if snap.aligned == 1 and snap.bias_state_after == 1:
        return "BULL WEAK ROC", "WATCHLIST", "Renko bullish but ROC confirmation incomplete"
    if snap.bias_state_after == 1:
        return "BULL MIXED", "WATCHLIST", "Bias bullish but 3M/M/W not fully aligned"
    if snap.bias_state_after == -1 or snap.aligned == -1:
        return "BEAR", "REJECTED", "Renko bearish"
    return "MIXED", "WATCHLIST", "No bullish Renko alignment yet"


def run_renko_filter(
    stocks: list[AssetMetrics],
    etfs: list[AssetMetrics],
    atr_length: int,
    max_workers: int,
    debug: bool = False,
) -> dict[str, RenkoConfirmation]:
    from renko_nasdaq import STATE_PATH, _load_json, scan_symbol

    stored_state = _load_json(STATE_PATH)

    candidates: list[tuple[str, str, str, str, str]] = []
    seen: set[str] = set()
    for m in stocks:
        key = f"STOCK:{m.ticker}"
        if key in seen:
            continue
        seen.add(key)
        candidates.append((m.ticker, m.name, "STOCK", m.theme, stock_tv_symbol(m.ticker)))
    for m in etfs:
        if m.ticker in {"QQQ", "QQQM"}:
            continue
        key = f"ETF:{m.ticker}"
        if key in seen:
            continue
        seen.add(key)
        candidates.append((m.ticker, m.name, "ETF", m.theme, etf_tv_symbol(m.ticker)))

    def one(item: tuple[str, str, str, str, str]) -> tuple[str, RenkoConfirmation]:
        ticker, name, asset_type, theme, tv_symbol = item
        try:
            state = {}
            if asset_type == "STOCK" and tv_symbol in stored_state:
                state[tv_symbol] = dict(stored_state[tv_symbol])
            snap = scan_symbol(tv_symbol, ticker, atr_length, None, state, debug=debug)
            status, final_action, reason = renko_final_action(snap)
            bias = "BULL" if snap and snap.bias_state_after == 1 else "BEAR" if snap and snap.bias_state_after == -1 else "MIXED"
            return f"{asset_type}:{ticker}", RenkoConfirmation(
                ticker=ticker,
                name=name,
                asset_type=asset_type,
                theme=theme,
                tv_symbol=tv_symbol,
                renko_status=status,
                final_action=final_action,
                aligned=snap.aligned if snap else None,
                bias=bias,
                px_3m=snap.px_mn if snap else None,
                px_m=snap.px_w1 if snap else None,
                px_w=snap.px_d1 if snap else None,
                streak_3m=snap.streak_3m if snap else None,
                streak_m=snap.streak_m if snap else None,
                streak_w=snap.streak_w if snap else None,
                pnl_pct=snap.pnl_pct if snap else None,
                roc14=snap.price_roc14 if snap else None,
                roc21=snap.price_roc21 if snap else None,
                close=snap.close if snap else None,
                reason=reason,
            )
        except Exception as exc:
            ticker, name, asset_type, theme, tv_symbol = item
            return f"{asset_type}:{ticker}", RenkoConfirmation(
                ticker=ticker,
                name=name,
                asset_type=asset_type,
                theme=theme,
                tv_symbol=tv_symbol,
                renko_status="ERROR",
                final_action="WATCHLIST",
                aligned=None,
                bias="MIXED",
                px_3m=None,
                px_m=None,
                px_w=None,
                streak_3m=None,
                streak_m=None,
                streak_w=None,
                pnl_pct=None,
                roc14=None,
                roc21=None,
                close=None,
                reason=str(exc)[:120],
            )

    out: dict[str, RenkoConfirmation] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(one, c) for c in candidates]
        for fut in as_completed(futures):
            key, confirmation = fut.result()
            out[key] = confirmation
    return out


def table_renko(confirmations: list[RenkoConfirmation], limit: int | None = None) -> str:
    rows = []
    items = confirmations[:limit] if limit else confirmations
    for c in items:
        rows.append([
            c.final_action,
            c.asset_type,
            c.ticker,
            c.theme[:30],
            c.renko_status,
            c.bias,
            f"{c.px_3m}/{c.px_m}/{c.px_w}",
            format_pct(c.roc14),
            format_pct(c.roc21),
            format_pct(c.pnl_pct),
        ])
    return tabulate(rows, headers=[
        "Final", "Type", "Ticker", "Theme", "Renko", "Bias", "3M/M/W", "ROC14", "ROC21", "PnL",
    ], tablefmt="github")


def analytic_role(asset: AssetMetrics | None, confirmation: RenkoConfirmation) -> tuple[str, str, str]:
    score = asset.score if asset else 0.0
    rel63 = asset.rel_63d_vs_qqq if asset else None
    roc14 = confirmation.roc14 or 0.0
    roc21 = confirmation.roc21 or 0.0

    if confirmation.asset_type == "ETF":
        if score >= 85:
            return "CORE ETF", "Diversified sector vehicle", "Use first for broad exposure to the theme"
        if score >= 60:
            return "TACTICAL ETF", "Secondary sector vehicle", "Useful for exposure, but not the strongest ETF proxy"
        return "SATELLITE ETF", "Narrow/lagging proxy", "Keep smaller than core ETF candidates"

    if score >= 90 and (rel63 or 0.0) > 30 and roc14 > 50 and roc21 > 50:
        return "LEADER STOCK", "Primary momentum leader", "Best single-name expression of the theme"
    if score >= 80 and (rel63 or 0.0) > 10:
        return "TACTICAL STOCK", "Strong single-name momentum", "Actionable, but monitor extension and volatility"
    if score >= 60:
        return "SATELLITE STOCK", "Confirmed but less dominant", "Use after stronger leaders or for diversification"
    return "LOW PRIORITY", "Renko confirmed but weak relative score", "Confirmed trend, but not a top allocation candidate"


def table_analytic_classification(
    renko: dict[str, RenkoConfirmation],
    stock_metrics: list[AssetMetrics],
    etf_metrics: list[AssetMetrics],
) -> str:
    asset_map: dict[str, AssetMetrics] = {}
    for m in stock_metrics:
        asset_map[f"STOCK:{m.ticker}"] = m
    for m in etf_metrics:
        asset_map[f"ETF:{m.ticker}"] = m

    rows = []
    validated = [c for c in renko.values() if c.final_action == "VALIDATED"]
    validated.sort(key=lambda c: (c.theme, c.asset_type != "ETF", c.ticker))
    for c in validated:
        asset = asset_map.get(f"{c.asset_type}:{c.ticker}")
        bucket, role, note = analytic_role(asset, c)
        rows.append([
            c.theme[:30],
            bucket,
            c.asset_type,
            c.ticker,
            asset.score if asset else "---",
            format_pct(asset.perf_63d if asset else None),
            format_pct(asset.rel_63d_vs_qqq if asset else None),
            format_pct(c.roc14),
            format_pct(c.roc21),
            role,
            note,
        ])
    return tabulate(rows, headers=[
        "Theme", "Bucket", "Type", "Ticker", "Score", "63D", "Rel63", "R14", "R21", "Role", "Use",
    ], tablefmt="github")


def combined_rank_score(asset: AssetMetrics | None, confirmation: RenkoConfirmation) -> float:
    score = asset.score if asset else 0.0
    perf63 = asset.perf_63d or 0.0 if asset else 0.0
    rel63 = asset.rel_63d_vs_qqq or 0.0 if asset else 0.0
    r14 = confirmation.roc14 or 0.0
    r21 = confirmation.roc21 or 0.0
    pnl = confirmation.pnl_pct or 0.0
    return score * 0.30 + perf63 * 0.12 + rel63 * 0.22 + r14 * 0.14 + r21 * 0.14 + pnl * 0.08


def retained_assets_ranked(
    renko: dict[str, RenkoConfirmation],
    stock_metrics: list[AssetMetrics],
    etf_metrics: list[AssetMetrics],
) -> list[tuple[float, RenkoConfirmation, AssetMetrics | None]]:
    asset_map: dict[str, AssetMetrics] = {}
    for m in stock_metrics:
        asset_map[f"STOCK:{m.ticker}"] = m
    for m in etf_metrics:
        asset_map[f"ETF:{m.ticker}"] = m

    ranked = []
    for c in renko.values():
        if c.final_action != "VALIDATED":
            continue
        asset = asset_map.get(f"{c.asset_type}:{c.ticker}")
        ranked.append((combined_rank_score(asset, c), c, asset))
    return sorted(ranked, key=lambda x: x[0], reverse=True)


def table_global_retained_ranking(
    renko: dict[str, RenkoConfirmation],
    stock_metrics: list[AssetMetrics],
    etf_metrics: list[AssetMetrics],
    limit: int | None = None,
) -> str:
    ranked = retained_assets_ranked(renko, stock_metrics, etf_metrics)
    if limit:
        ranked = ranked[:limit]
    rows = []
    for i, (rank_score, c, asset) in enumerate(ranked, 1):
        rows.append([
            i,
            c.ticker,
            c.asset_type,
            c.theme[:30],
            round(rank_score, 2),
            asset.score if asset else "---",
            format_pct(asset.perf_63d if asset else None),
            format_pct(asset.rel_63d_vs_qqq if asset else None),
            format_pct(c.roc14),
            format_pct(c.roc21),
            format_pct(c.pnl_pct),
        ])
    return tabulate(rows, headers=[
        "#", "Ticker", "Type", "Theme", "Rank", "Score", "63D", "Rel63", "R14", "R21", "PnL Renko",
    ], tablefmt="github")


def europe_etf_alternatives(themes: list[str], limit: int = 6) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for theme in themes:
        for item in EUROPE_ETF_ALTERNATIVES.get(theme, []):
            key = item["isin"]
            if key in seen:
                continue
            seen.add(key)
            row = dict(item)
            row["theme"] = theme
            rows.append(row)
            if len(rows) >= limit:
                return rows
    return rows


def table_europe_etf_alternatives(themes: list[str]) -> str:
    rows = []
    for item in europe_etf_alternatives(themes):
        rows.append([
            item["theme"][:30],
            item["ticker"],
            item["isin"],
            item["listing"],
            item["currency"],
            item["name"][:42],
        ])
    return tabulate(rows, headers=[
        "Theme", "Ticker", "ISIN", "Listing", "Ccy", "UCITS ETF",
    ], tablefmt="github")


def table_europe_etf_metrics(metrics: list[AssetMetrics], limit: int = 10) -> str:
    rows = []
    for m in metrics[:limit]:
        rows.append([
            m.ticker,
            m.theme[:30],
            m.score,
            format_pct(m.perf_63d),
            format_pct(m.rel_63d_vs_qqq),
            m.industry[:34],
        ])
    return tabulate(rows, headers=[
        "Ticker", "Theme", "Score", "63D", "Rel63 vs EQQQ", "Listing / ISIN",
    ], tablefmt="github")


def theme_daily_rate(value: float | None, days: int) -> float:
    return (value or 0.0) / days


def slowing_dominant_themes(theme_scores: list[ThemeScore], top_count: int = 2) -> list[ThemeScore]:
    slowing = []
    for theme in theme_scores[:top_count]:
        rate_21d = theme_daily_rate(theme.median_21d, 21)
        rate_63d = theme_daily_rate(theme.median_63d, 63)
        if (theme.median_63d or 0.0) > 0 and (rate_21d < rate_63d * 0.75 or (theme.median_21d or 0.0) < 0):
            slowing.append(theme)
    return slowing


def emerging_shift_themes(theme_scores: list[ThemeScore], limit: int = 3) -> list[str]:
    emerging = []
    for theme in theme_scores[3:9]:
        rate_21d = theme_daily_rate(theme.median_21d, 21)
        rate_63d = theme_daily_rate(theme.median_63d, 63)
        accelerating = rate_21d > max(rate_63d * 1.15, 0.05)
        breadth_ok = theme.pct_above_sma50 >= 50
        rel_ok = (theme.median_rel_63d or -999.0) > -25
        if accelerating and breadth_ok and rel_ok:
            emerging.append(theme.theme)
        if len(emerging) >= limit:
            break
    return emerging


def early_shift_candidates(
    theme_scores: list[ThemeScore],
    renko: dict[str, RenkoConfirmation],
    stock_metrics: list[AssetMetrics],
    etf_metrics: list[AssetMetrics],
    limit: int = 3,
) -> tuple[list[ThemeScore], list[tuple[float, RenkoConfirmation, AssetMetrics | None]]]:
    slowing = slowing_dominant_themes(theme_scores)
    if not slowing:
        return [], []

    slowing_names = {s.theme for s in slowing}
    asset_map: dict[str, AssetMetrics] = {}
    for m in stock_metrics:
        asset_map[f"STOCK:{m.ticker}"] = m
    for m in etf_metrics:
        asset_map[f"ETF:{m.ticker}"] = m

    candidates = []
    for c in renko.values():
        if c.final_action != "WATCHLIST" or c.renko_status == "ERROR":
            continue
        if c.theme in slowing_names:
            continue
        if c.px_3m != 1:
            continue
        asset = asset_map.get(f"{c.asset_type}:{c.ticker}")
        rel63 = asset.rel_63d_vs_qqq or 0.0 if asset else 0.0
        perf21 = asset.perf_21d or 0.0 if asset else 0.0
        if rel63 <= 0 and perf21 <= 0:
            continue
        if (c.roc14 or 0.0) <= 0:
            continue
        score = rel63 * 0.45 + perf21 * 0.20 + (c.roc14 or 0.0) * 0.20 + (c.roc21 or 0.0) * 0.15
        candidates.append((score, c, asset))
    return slowing, sorted(candidates, key=lambda x: x[0], reverse=True)[:limit]


def build_telegram_summary(
    theme_scores: list[ThemeScore],
    stock_metrics: list[AssetMetrics],
    etf_metrics: list[AssetMetrics],
    europe_etf_metrics: list[AssetMetrics],
    renko: dict[str, RenkoConfirmation],
    top_n: int = 5,
) -> str:
    ranked = retained_assets_ranked(renko, stock_metrics, etf_metrics)

    stock_ranked = [(r, c, a) for r, c, a in ranked if c.asset_type == "STOCK"]
    etf_ranked = [(r, c, a) for r, c, a in ranked if c.asset_type == "ETF"]

    def _fallback(metrics: list[AssetMetrics]) -> list[AssetMetrics]:
        return sorted(
            [m for m in metrics if m.rel_63d_vs_qqq is not None],
            key=lambda m: ((m.rel_63d_vs_qqq or 0.0), m.score),
            reverse=True,
        )

    def _append_ranked_asset_lines(
        title: str,
        validated_rows: list[tuple[float, RenkoConfirmation, AssetMetrics | None]],
        fallback_rows: list[AssetMetrics],
    ) -> None:
        lines.extend(["", title] if lines else [title])
        used: set[str] = set()
        idx = 1
        for _, c, asset in validated_rows:
            if idx > 3:
                break
            if asset is None or c.ticker in used:
                continue
            lines.append(f"{idx}. {c.ticker} ({format_pct(asset.rel_63d_vs_qqq)})")
            used.add(c.ticker)
            idx += 1
        for asset in fallback_rows:
            if idx > 3:
                break
            if asset.ticker in used:
                continue
            lines.append(f"{idx}. {asset.ticker} ({format_pct(asset.rel_63d_vs_qqq)})")
            used.add(asset.ticker)
            idx += 1
        if idx == 1:
            lines.append("---")

    lines: list[str] = []
    _append_ranked_asset_lines("📈 TOP 3 STOCK", stock_ranked, _fallback(stock_metrics))
    _append_ranked_asset_lines("📊 TOP 3 ETF", etf_ranked, _fallback(etf_metrics))
    _append_ranked_asset_lines("🇪🇺 TOP 3 ETF UCITS", [], _fallback(europe_etf_metrics))

    slowing, early = early_shift_candidates(theme_scores, renko, stock_metrics, etf_metrics)
    if slowing and early:
        slowing_txt = ", ".join(s.theme for s in slowing)
        lines += ["", f"⚠️ EARLY SHIFT ({slowing_txt} ralentit)"]
        for i, (score, c, asset) in enumerate(early, 1):
            lines.append(f"{i}. {c.ticker} ({format_pct(asset.rel_63d_vs_qqq if asset else None)})")

    return "\n".join(lines)


def build_report(
    theme_scores: list[ThemeScore],
    stock_metrics: list[AssetMetrics],
    etf_metrics: list[AssetMetrics],
    europe_etf_metrics: list[AssetMetrics],
    news: dict[str, dict[str, Any]],
    renko: dict[str, RenkoConfirmation],
    args: argparse.Namespace,
) -> str:
    paris = pytz.timezone("Europe/Paris")
    now = datetime.now(paris).strftime("%Y-%m-%d %H:%M %Z")
    top_themes = [s.theme for s in theme_scores[:args.top_themes]]
    leaders = top_stocks_by_theme(stock_metrics, top_themes, args.top_stocks)

    lines = [
        "NASDAQ SECTOR PIPELINE",
        f"Generated: {now}",
        f"Universe: Nasdaq-100 | News window: {args.news_days} days",
        "",
        "DOMINANT THEMES",
        table_theme_scores(theme_scores, args.top_themes),
        "",
        "TOP STOCKS BY DOMINANT THEME",
    ]
    for theme in top_themes:
        lines += ["", theme, table_assets(leaders.get(theme, []), args.top_stocks)]

    lines += [
        "",
        "BEST ETF PROXIES",
        table_etfs(etf_metrics, args.top_etfs),
        "",
        "ETF UCITS EUROPE / FRANCE",
        table_europe_etf_alternatives(top_themes),
        "",
        "ETF UCITS SCORED",
        table_europe_etf_metrics(europe_etf_metrics, args.top_etfs) if europe_etf_metrics else "No UCITS ETF metrics available.",
    ]

    if renko:
        validated = sorted(
            [c for c in renko.values() if c.final_action == "VALIDATED"],
            key=lambda c: (c.asset_type != "ETF", c.theme, c.ticker),
        )
        watchlist = sorted(
            [c for c in renko.values() if c.final_action == "WATCHLIST"],
            key=lambda c: (c.asset_type, c.theme, c.ticker),
        )
        rejected = sorted(
            [c for c in renko.values() if c.final_action == "REJECTED"],
            key=lambda c: (c.asset_type, c.theme, c.ticker),
        )
        lines += [
            "",
            "FINAL SELECTION - RENKO VALIDATED",
            table_renko(validated) if validated else "No candidate fully validated by Renko.",
            "",
            "TOP 5 GLOBAL - ETF AND STOCKS COMBINED",
            table_global_retained_ranking(renko, stock_metrics, etf_metrics, 5) if validated else "No retained asset to rank.",
            "",
            "GLOBAL RANKING - RETAINED ASSETS",
            table_global_retained_ranking(renko, stock_metrics, etf_metrics) if validated else "No retained asset to rank.",
            "",
            "ANALYTIC CLASSIFICATION - RETAINED ASSETS",
            table_analytic_classification(renko, stock_metrics, etf_metrics) if validated else "No retained asset to classify.",
            "",
            "WATCHLIST - GOOD THEME, WAITING FOR RENKO",
            table_renko(watchlist, args.renko_watchlist_limit) if watchlist else "No watchlist candidate.",
        ]
        slowing, early = early_shift_candidates(theme_scores, renko, stock_metrics, etf_metrics)
        if slowing:
            lines += [
                "",
                "EARLY SHIFT - ONLY IF DOMINANT THEMES SLOW",
                "Slowing dominant themes: " + ", ".join(s.theme for s in slowing),
            ]
            if early:
                rows = []
                for score, c, asset in early:
                    rows.append([
                        c.ticker,
                        c.asset_type,
                        c.theme[:30],
                        round(score, 2),
                        format_pct(asset.perf_21d if asset else None),
                        format_pct(asset.rel_63d_vs_qqq if asset else None),
                        f"{c.px_3m}/{c.px_m}/{c.px_w}",
                        format_pct(c.roc14),
                        format_pct(c.roc21),
                    ])
                lines.append(tabulate(rows, headers=[
                    "Ticker", "Type", "Theme", "Early", "21D", "Rel63", "Renko", "R14", "R21",
                ], tablefmt="github"))
            else:
                lines.append("No early-shift candidate passed the partial Renko and momentum filters.")
        if rejected:
            lines += [
                "",
                "REJECTED BY RENKO",
                table_renko(rejected, args.renko_watchlist_limit),
            ]
    else:
        lines += [
            "",
            "RENKO FILTER",
            "Skipped. Run without --no-renko to require Renko validation.",
        ]

    lines += [
        "",
        "NEWS VALIDATION",
    ]
    for theme in top_themes:
        n = news.get(theme, {})
        lines.append("")
        lines.append(f"{theme}: score={n.get('score', 0)} hits={n.get('hits', 0)} sources={n.get('sources', 0)}")
        for item in n.get("entries", [])[:args.news_items]:
            date = item.get("published", "")[:10]
            lines.append(f"- {date} | {item.get('source', 'Unknown')} | {item.get('title', '')}")
            lines.append(f"  {item.get('link', '')}")

    lines += [
        "",
        "METHOD",
        "Score action = momentum 21D/63D/126D + force relative vs QQQ + volume + tendance SMA50/SMA200 + proximite high 20D.",
        "Score theme = score moyen actions + momentum median + breadth SMA + confirmation actualite.",
        "Selection finale = theme dominant + action/ETF fort + validation Renko 3M/M/W bullish avec ROC14/ROC21 positifs.",
        "A utiliser comme radar de rotation sectorielle, pas comme signal d'achat automatique.",
    ]
    return "\n".join(lines)


def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram env missing: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks = [text[i:i + 3800] for i in range(0, len(text), 3800)]
    for chunk in chunks:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk}, timeout=15)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Nasdaq-100 sector/theme rotation pipeline")
    p.add_argument("--top-themes", type=int, default=3, help="Number of dominant themes to display")
    p.add_argument("--top-stocks", type=int, default=3, help="Top stocks per dominant theme")
    p.add_argument("--top-etfs", type=int, default=10, help="Top ETF proxies to display")
    p.add_argument("--news-days", type=int, default=14, help="Recent news window in days")
    p.add_argument("--news-items", type=int, default=2, help="News links shown per theme")
    p.add_argument("--period", default="9mo", help="Yahoo Finance history period")
    p.add_argument("--no-news", action="store_true", help="Skip news validation")
    p.add_argument("--no-renko", action="store_true", help="Skip final Renko validation")
    p.add_argument("--renko-length", type=int, default=14, help="Renko ATR length")
    p.add_argument("--renko-workers", type=int, default=1, help="Parallel Renko scans")
    p.add_argument("--renko-candidates-per-theme", type=int, default=3, help="Stocks per dominant theme sent to Renko")
    p.add_argument("--renko-etf-candidates", type=int, default=10, help="ETF candidates sent to Renko")
    p.add_argument("--renko-watchlist-limit", type=int, default=20, help="Rows shown for Renko watchlist/rejections")
    p.add_argument("--renko-debug", action="store_true", help="Verbose Renko scan diagnostics")
    p.add_argument("--json", default=REPORT_JSON, help="JSON report path")
    p.add_argument("--txt", default=REPORT_TXT, help="Text report path")
    p.add_argument("--telegram", action="store_true", default=True, help="Send compact summary to Telegram")
    p.add_argument("--no-telegram", action="store_false", dest="telegram", help="Do not send Telegram summary")
    p.add_argument("--telegram-full", action="store_true", help="Send full text report to Telegram instead of compact summary")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    t0 = time.time()

    print("Fetching Nasdaq-100 components...")
    components = fetch_nasdaq_100()
    tickers = components["ticker"].tolist()
    print(f"Loaded {len(tickers)} components.")

    print("Downloading price history...")
    histories = download_history(tickers + ["QQQ"], period=args.period)
    qqq_close = histories.get("QQQ", pd.DataFrame()).get("Close", pd.Series(dtype=float))

    # Garde-fou anti-rapport-vide: si Yahoo a rate-limite (donnees insuffisantes),
    # on n'ecrase PAS le rapport existant et on n'envoie PAS de Telegram vide.
    # On retourne 0 pour ne pas casser la suite du workflow CI (set -e).
    covered = sum(1 for t in tickers if t in histories)
    min_required = max(20, len(tickers) // 2)
    if qqq_close.empty or covered < min_required:
        print(
            f"ERROR: donnees de prix insuffisantes "
            f"(QQQ vide={qqq_close.empty}, couverts={covered}/{len(tickers)}, "
            f"minimum={min_required}). Cause probable: rate limit Yahoo Finance. "
            f"Abandon SANS ecrire le rapport ni envoyer Telegram.",
            file=sys.stderr,
        )
        return 0

    print("Fetching Yahoo profiles...")
    profiles = fetch_profiles(tickers)

    print("Computing stock and theme metrics...")
    stock_histories = {k: v for k, v in histories.items() if k != "QQQ"}
    stock_metrics = build_asset_metrics(components, profiles, stock_histories, qqq_close)

    preliminary_df = pd.DataFrame([asdict(m) for m in stock_metrics])
    candidate_themes = []
    if not preliminary_df.empty:
        candidate_themes = (
            preliminary_df.groupby("theme")["score"]
            .mean()
            .sort_values(ascending=False)
            .head(max(args.top_themes + 3, 6))
            .index.tolist()
        )

    news: dict[str, dict[str, Any]] = {}
    if args.no_news:
        news = {theme: {"theme": theme, "score": 0, "hits": 0, "sources": 0, "entries": []} for theme in candidate_themes}
    else:
        print("Validating themes with recent news...")
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(fetch_news_for_theme, theme, args.news_days): theme for theme in candidate_themes}
            for fut in as_completed(futures):
                item = fut.result()
                news[item["theme"]] = item

    theme_scores = build_theme_scores(stock_metrics, news)
    top_themes = [s.theme for s in theme_scores[:args.top_themes]]
    renko_theme_pool = list(dict.fromkeys(top_themes + emerging_shift_themes(theme_scores)))

    print("Ranking ETF proxies...")
    etf_metrics = build_etf_metrics(renko_theme_pool)
    europe_etf_metrics = build_europe_etf_metrics(renko_theme_pool)

    renko: dict[str, RenkoConfirmation] = {}
    if args.no_renko:
        print("Skipping Renko validation.")
    else:
        print("Applying final Renko filter to top stocks and ETFs...")
        renko_stock_candidates: list[AssetMetrics] = []
        for theme, rows in top_stocks_by_theme(stock_metrics, renko_theme_pool, args.renko_candidates_per_theme).items():
            renko_stock_candidates.extend(rows)
        renko_etf_candidates = etf_metrics[:args.renko_etf_candidates]
        renko = run_renko_filter(
            renko_stock_candidates,
            renko_etf_candidates,
            atr_length=args.renko_length,
            max_workers=args.renko_workers,
            debug=args.renko_debug,
        )

    report = build_report(theme_scores, stock_metrics, etf_metrics, europe_etf_metrics, news, renko, args)
    print()
    print(report)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_seconds": round(time.time() - t0, 2),
        "themes": [asdict(s) for s in theme_scores],
        "stocks": [asdict(m) for m in sorted(stock_metrics, key=lambda x: x.score, reverse=True)],
        "etfs": [asdict(m) for m in etf_metrics],
        "europe_etfs": [asdict(m) for m in europe_etf_metrics],
        "renko": {key: asdict(value) for key, value in renko.items()},
        "news": news,
    }
    with open(args.json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(args.txt, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\nSaved: {args.txt}")
    print(f"Saved: {args.json}")

    if args.telegram:
        telegram_text = report if args.telegram_full else build_telegram_summary(
            theme_scores, stock_metrics, etf_metrics, europe_etf_metrics, renko
        )
        send_telegram(telegram_text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
