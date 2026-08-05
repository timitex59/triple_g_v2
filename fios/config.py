#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration centrale de FIOS.

Tout ce qui est "reglages" vit ici : univers de devises/paires, symboles
TradingView, poids des timeframes, poids du moteur de confluence, seuils de
decision, mappings COT (CFTC) et FRED (macro). Aucune logique metier.
"""

from __future__ import annotations

# --- Univers ---------------------------------------------------------------

CURRENCIES: list[str] = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"]

# Paires analysees. base/quote servent a decomposer la force par devise.
# tv = symbole TradingView (OANDA, comme data_fetcher.py du repo).
# Un score directionnel positif = base plus forte que quote.
PAIRS: list[dict] = [
    {"name": "EURUSD", "base": "EUR", "quote": "USD", "tv": "OANDA:EURUSD"},
    {"name": "GBPUSD", "base": "GBP", "quote": "USD", "tv": "OANDA:GBPUSD"},
    {"name": "AUDUSD", "base": "AUD", "quote": "USD", "tv": "OANDA:AUDUSD"},
    {"name": "NZDUSD", "base": "NZD", "quote": "USD", "tv": "OANDA:NZDUSD"},
    {"name": "USDJPY", "base": "USD", "quote": "JPY", "tv": "OANDA:USDJPY"},
    {"name": "USDCHF", "base": "USD", "quote": "CHF", "tv": "OANDA:USDCHF"},
    {"name": "USDCAD", "base": "USD", "quote": "CAD", "tv": "OANDA:USDCAD"},
    {"name": "EURJPY", "base": "EUR", "quote": "JPY", "tv": "OANDA:EURJPY"},
    {"name": "GBPJPY", "base": "GBP", "quote": "JPY", "tv": "OANDA:GBPJPY"},
    {"name": "AUDJPY", "base": "AUD", "quote": "JPY", "tv": "OANDA:AUDJPY"},
    {"name": "CADJPY", "base": "CAD", "quote": "JPY", "tv": "OANDA:CADJPY"},
    {"name": "EURGBP", "base": "EUR", "quote": "GBP", "tv": "OANDA:EURGBP"},
    {"name": "EURCHF", "base": "EUR", "quote": "CHF", "tv": "OANDA:EURCHF"},
    {"name": "EURAUD", "base": "EUR", "quote": "AUD", "tv": "OANDA:EURAUD"},
    {"name": "GBPCHF", "base": "GBP", "quote": "CHF", "tv": "OANDA:GBPCHF"},
    {"name": "AUDNZD", "base": "AUD", "quote": "NZD", "tv": "OANDA:AUDNZD"},
]

# --- Technique : timeframes ------------------------------------------------

# code interne -> (intervalle TradingView, nb de bougies a charger)
TIMEFRAMES: dict[str, tuple[str, int]] = {
    "D": ("D", 300),
    "H4": ("240", 400),
    "H1": ("60", 400),
}

# Poids relatifs des timeframes dans le score technique (renormalises sur le
# disponible si un TF manque). Le Daily domine (biais de fond), H1 affine.
TF_WEIGHTS: dict[str, float] = {"D": 0.50, "H4": 0.30, "H1": 0.20}

EMA_FAST = 20
EMA_MID = 50
EMA_SLOW = 200
RSI_LEN = 14
ADX_LEN = 14
ATR_LEN = 14

# --- Correlations ----------------------------------------------------------

# Instruments macro suivis (symboles TradingView).
CORRELATION_INSTRUMENTS: dict[str, str] = {
    "DXY": "TVC:DXY",
    "GOLD": "TVC:GOLD",
    "US10Y": "TVC:US10Y",
    "US02Y": "TVC:US02Y",
    "SPX": "OANDA:SPX500USD",
    "VIX": "TVC:VIX",
    "OIL": "TVC:USOIL",
    "BTC": "OANDA:BTCUSD",
}

CORR_LOOKBACK = 60  # bougies daily pour la correlation glissante

# --- COT (CFTC) ------------------------------------------------------------

# Dataset Socrata "Legacy Futures-Only" de la CFTC.
COT_SOCRATA_URL = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"

# Devise -> motif (LIKE) du nom de marche CME dans le rapport COT.
# USD est derive de l'indice dollar (positionnement inverse du panier).
COT_MARKET_PATTERNS: dict[str, str] = {
    "EUR": "EURO FX",
    "JPY": "JAPANESE YEN",
    "GBP": "BRITISH POUND",
    "CHF": "SWISS FRANC",
    "CAD": "CANADIAN DOLLAR",
    "AUD": "AUSTRALIAN DOLLAR",
    "NZD": "NEW ZEALAND DOLLAR",
    "USD": "U.S. DOLLAR INDEX",
}

# --- Fondamental (FRED) ----------------------------------------------------

# Cle API FRED gratuite : https://fred.stlouisfed.org/docs/api/api_key.html
# (variable d'environnement FRED_API_KEY). Sans cle, la famille fondamentale
# est simplement ignoree et son poids redistribue.
#
# Par devise : series FRED cle et sens ("up" = une hausse renforce la devise).
# Couverture volontairement limitee en Phase 1 (taux, rendements, inflation,
# chomage). US = couverture maximale ; les autres se completent en Phase 2.
FRED_SERIES: dict[str, list[dict]] = {
    "USD": [
        {"id": "DFEDTARU", "dir": "up", "label": "Fed funds (haut)"},
        {"id": "DGS10", "dir": "up", "label": "US 10Y"},
        {"id": "DGS2", "dir": "up", "label": "US 2Y"},
        {"id": "UNRATE", "dir": "down", "label": "Chomage US"},
        {"id": "CPIAUCSL", "dir": "up", "label": "CPI US"},
    ],
    "EUR": [
        {"id": "ECBDFR", "dir": "up", "label": "Taux depot BCE"},
        {"id": "IRLTLT01EZM156N", "dir": "up", "label": "10Y zone euro"},
        {"id": "LRHUTTTTEZM156S", "dir": "down", "label": "Chomage zone euro"},
    ],
    "GBP": [
        {"id": "BOERUKM", "dir": "up", "label": "Bank Rate BoE"},
        {"id": "IRLTLT01GBM156N", "dir": "up", "label": "10Y UK"},
        {"id": "LRHUTTTTGBM156S", "dir": "down", "label": "Chomage UK"},
    ],
    "JPY": [
        {"id": "IRSTCI01JPM156N", "dir": "up", "label": "Taux directeur JP"},
        {"id": "IRLTLT01JPM156N", "dir": "up", "label": "10Y JP"},
        {"id": "LRHUTTTTJPM156S", "dir": "down", "label": "Chomage JP"},
    ],
    "CHF": [
        {"id": "IR3TIB01CHM156N", "dir": "up", "label": "Taux 3M CH"},
        {"id": "IRLTLT01CHM156N", "dir": "up", "label": "10Y CH"},
    ],
    "CAD": [
        {"id": "IR3TIB01CAM156N", "dir": "up", "label": "Taux 3M CA"},
        {"id": "IRLTLT01CAM156N", "dir": "up", "label": "10Y CA"},
        {"id": "LRHUTTTTCAM156S", "dir": "down", "label": "Chomage CA"},
    ],
    "AUD": [
        {"id": "IR3TIB01AUM156N", "dir": "up", "label": "Taux 3M AU"},
        {"id": "IRLTLT01AUM156N", "dir": "up", "label": "10Y AU"},
    ],
    "NZD": [
        {"id": "IR3TIB01NZM156N", "dir": "up", "label": "Taux 3M NZ"},
        {"id": "IRLTLT01NZM156N", "dir": "up", "label": "10Y NZ"},
    ],
}

# --- Sentiment retail (OANDA + Myfxbook) -----------------------------------

# OANDA v20 : compte demo gratuit, token dans OANDA_API_TOKEN (env).
# OANDA_ENV = "practice" (demo) ou "live". Le position book donne le %
# long/short des positions retail -> signal CONTRARIEN.
OANDA_HOSTS = {
    "practice": "https://api-fxpractice.oanda.com",
    "live": "https://api-fxtrade.oanda.com",
}

# Myfxbook : compte gratuit (MYFXBOOK_EMAIL / MYFXBOOK_PASSWORD).
MYFXBOOK_LOGIN_URL = "https://www.myfxbook.com/api/login.json"
MYFXBOOK_OUTLOOK_URL = "https://www.myfxbook.com/api/get-community-outlook.json"
MYFXBOOK_LOGOUT_URL = "https://www.myfxbook.com/api/logout.json"

# --- Moteur de confluence --------------------------------------------------

# Poids par famille (renormalises sur les familles reellement disponibles).
# 'sentiment' = COT (institutionnels, suiveurs). 'retail' = OANDA+Myfxbook
# (particuliers, CONTRARIEN). Deux bouts opposes du marche -> vraie confluence.
CONFLUENCE_WEIGHTS: dict[str, float] = {
    "fundamental": 0.28,
    "technical": 0.30,
    "sentiment": 0.15,
    "retail": 0.12,
    "correlation": 0.15,
}

# Seuils de decision sur le score net directionnel (-100..+100).
BUY_THRESHOLD = 25.0
SELL_THRESHOLD = -25.0

# Nb minimal de familles independantes qui doivent converger pour un signal.
MIN_FAMILIES_AGREE = 2

# --- Sorties ---------------------------------------------------------------

JOURNAL_FILE = "fios_journal.json"
REPORT_JSON = "fios_report.json"
