#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Acces aux donnees graphiques via TradingView WebSocket.

Reutilise exactement le moteur eprouve de data_fetcher.py (meme endpoint, meme
protocole socket.io, symboles OANDA), mais generalise l'intervalle pour couvrir
tous les timeframes dont FIOS a besoin (M, W, D, 240=H4, 60=H1, 30, 15).

Un petit cache en memoire evite de retelecharger le meme (symbole, interval)
plusieurs fois dans un meme run.
"""

from __future__ import annotations

import json
import time

import pandas as pd

# On reutilise les helpers bas-niveau du fetcher maitre du repo.
from data_fetcher import create_message, generate_session_id

try:
    from websocket import create_connection
except ImportError:  # pragma: no cover - websocket-client est dans requirements
    create_connection = None  # type: ignore

_WS_URL = "wss://prodata.tradingview.com/socket.io/websocket"
_HEADERS = {
    "Origin": "https://www.tradingview.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

_CACHE: dict[tuple[str, str], pd.DataFrame | None] = {}


def fetch(symbol: str, tv_interval: str, n_candles: int = 400) -> pd.DataFrame | None:
    """Recupere n bougies OHLCV pour un symbole TradingView et un intervalle.

    symbol       : ex "OANDA:EURUSD", "TVC:DXY".
    tv_interval  : ex "D", "W", "M", "240", "60", "30", "15".
    Retour       : DataFrame index datetime UTC, colonnes Open/High/Low/Close/
                   Volume (ordre chronologique), ou None si echec.
    """
    key = (symbol, tv_interval)
    if key in _CACHE:
        return _CACHE[key]

    df = _fetch_raw(symbol, tv_interval, n_candles)
    _CACHE[key] = df
    return df


def _fetch_raw(symbol: str, tv_interval: str, n_candles: int) -> pd.DataFrame | None:
    if create_connection is None:
        return None
    ws = None
    extracted: pd.DataFrame | None = None
    try:
        ws = create_connection(_WS_URL, header=_HEADERS, timeout=10)
        session_id = generate_session_id()
        ws.send(create_message("chart_create_session", [session_id, ""]))
        ws.send(
            create_message(
                "resolve_symbol",
                [
                    session_id,
                    "sds_sym_1",
                    '={"symbol":"%s","adjustment":"splits","session":"regular"}' % symbol,
                ],
            )
        )
        ws.send(
            create_message(
                "create_series",
                [session_id, "sds_1", "s1", "sds_sym_1", tv_interval, n_candles],
            )
        )

        start_t = time.time()
        while time.time() - start_t < 10:
            try:
                res = ws.recv()
            except Exception:
                continue
            if not isinstance(res, str):
                res = bytes(res).decode("utf-8", "ignore")
            if '"s":[' in res:
                start = res.find('"s":[')
                end = res.find('"ns":', start)
                if start != -1 and end != -1:
                    extract_end = end - 1
                    while res[extract_end] not in [",", "}"]:
                        extract_end -= 1
                    raw = res[start + 4 : extract_end]
                    try:
                        data = json.loads(raw)
                        rows = [item["v"] for item in data]
                        extracted = pd.DataFrame(
                            rows,
                            columns=["timestamp", "open", "high", "low", "close", "volume"],
                        )
                        extracted["datetime"] = pd.to_datetime(
                            extracted["timestamp"], unit="s", utc=True
                        )
                        extracted.set_index("datetime", inplace=True)
                        extracted.rename(
                            columns={
                                "open": "Open",
                                "high": "High",
                                "low": "Low",
                                "close": "Close",
                                "volume": "Volume",
                            },
                            inplace=True,
                        )
                        extracted.drop(columns=["timestamp"], inplace=True)
                    except Exception:
                        extracted = None
            if "series_completed" in res:
                break
        return extracted
    except Exception:
        return None
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass


def clear_cache() -> None:
    _CACHE.clear()
