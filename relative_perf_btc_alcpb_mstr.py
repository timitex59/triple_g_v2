#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
relative_perf_btc_alcpb_mstr.py

Suivi de performance relative (base 100) entre BTC, ALCPB (Capital B,
Euronext Paris) et MSTR (Strategy Inc, Nasdaq) -- les deux entreprises
"Bitcoin Treasury" comparees a la performance du sous-jacent qu'elles
accumulent.

Base par defaut: 2024-11-05, date de lancement de la strategie "Bitcoin
Treasury Company" par The Blockchain Group (devenue Capital B / ALCPB).

En plus du graphique de performance indexee, le script calcule les quotients
relatifs entre les trois actifs (ALCPB/BTC, MSTR/BTC, ALCPB/MSTR) et applique
a chaque quotient un PSAR et un RSI(14), en Mensuel / Hebdo / Journalier --
ce qui indique si une action est en train de sur- ou sous-performer BTC (ou
l'autre action), et si ce mouvement relatif est en zone de surachat/survente.

Les bougies "quotient" sont approximees a partir des OHLC journaliers de
chaque actif (close = closeA/closeB, high = highA/lowB, low = lowA/highB,
sur les seules dates ou les deux actifs ont cote), puis reechantillonnees en
Hebdo (W-FRI) / Mensuel (ME). C'est une approximation standard pour un
"ratio chart", pas un vrai OHLC trade.

Classement: chaque (quotient, timeframe) recoit un score dans [-100, +100]
(+-50 pour le cote du SAR, +-50 pour l'ecart du RSI a 50), moyenne ponderee
par TF_WEIGHTS (M=3, W=2, D=1 -- le structurel pese plus que le bruit court
terme) pour donner un score composite par quotient. Les 3 quotients sont
tries du meilleur au plus faible (medailles) dans le message Telegram.

Donnees: fetch_tv_ohlc (TradingView), journalier.
Symboles: INDEX:BTCUSD, EURONEXT:ALCPB, NASDAQ:MSTR.
PSAR: recurrence Pine (ta.sar) validee bar-a-bar vs TradingView (cf. pine_sar
dans imp_trend5_29pairs.py).

Usage:
  python relative_perf_btc_alcpb_mstr.py                    # graphique + quotients, envoi Telegram
  python relative_perf_btc_alcpb_mstr.py --no-send           # calcule + sauvegarde le PNG local, sans envoi
  python relative_perf_btc_alcpb_mstr.py --no-ratios         # ignore le message quotients (SAR/RSI)
  python relative_perf_btc_alcpb_mstr.py --rebase-date 2025-01-01

Le script ne donne pas de conseil financier.
"""

from __future__ import annotations

import argparse
import io
import math
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import requests

from ichimoku_v4 import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, fetch_tv_ohlc, rsi

PARIS_TZ = ZoneInfo("Europe/Paris")

# label -> (symbole TradingView, couleur du trace)
ASSETS = [
    ("BTC", "INDEX:BTCUSD", "#f7931a"),
    ("ALCPB", "EURONEXT:ALCPB", "#1f77b4"),
    ("MSTR", "NASDAQ:MSTR", "#9467bd"),
]
ICONS = {"BTC": "\U0001f7e0", "ALCPB": "\U0001f535", "MSTR": "\U0001f7e3"}

DEFAULT_REBASE_DATE = "2026-09-01"  # fenetre recente -- ajustable via --rebase-date
CANDLES = 1200
CHART_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "relative_perf_btc_alcpb_mstr.png")

# Quotients suivis: (numerateur, denominateur)
RATIO_PAIRS = [("ALCPB", "BTC"), ("MSTR", "BTC"), ("ALCPB", "MSTR")]
RSI_LENGTH = 14
RSI_OVERBOUGHT = 70.0
RSI_OVERSOLD = 30.0
SAR_START, SAR_INC, SAR_MAX = 0.1, 0.1, 0.2
# Timeframes affiches, du plus haut au plus bas
TIMEFRAMES = [("M", "ME", "Mensuel"), ("W", "W-FRI", "Hebdo"), ("D", None, "Journalier")]
MIN_BARS_FOR_STATS = 30

# Poids par timeframe pour le score composite (classement) : le structurel (M)
# pese plus que le swing (W), qui pese plus que le bruit court terme (D).
TF_WEIGHTS = {"M": 3.0, "W": 2.0, "D": 1.0}
MEDALS = ["\U0001f947", "\U0001f948", "\U0001f949"]  # 🥇🥈🥉


def fetch_daily_data() -> dict[str, pd.DataFrame]:
    raw: dict[str, pd.DataFrame] = {}
    for label, symbol, _ in ASSETS:
        df = fetch_tv_ohlc(symbol, "D", CANDLES)
        if df is None or df.empty:
            print(f"{label} ({symbol}): pas de donnees, ignore")
            continue
        raw[label] = df
    return raw


def compute_indexed_series(raw: dict[str, pd.DataFrame], rebase_date: str) -> dict[str, pd.Series]:
    rebase_ts = pd.Timestamp(rebase_date, tz="UTC")
    series: dict[str, pd.Series] = {}
    for label, df in raw.items():
        closes = df["close"]
        closes = closes[closes.index >= rebase_ts]
        if closes.empty:
            print(f"{label}: pas de donnees depuis {rebase_date}, ignore")
            continue
        base = closes.iloc[0]
        series[label] = (closes / base) * 100.0
    return series


def build_chart(series: dict[str, pd.Series], rebase_date: str) -> bytes:
    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    for label, _, color in ASSETS:
        if label not in series:
            continue
        s = series[label]
        ax.plot(s.index, s.values, label=f"{label} ({s.iloc[-1] - 100:+.1f}%)", color=color, linewidth=1.8)

    ax.axhline(100, color="grey", linewidth=0.8, linestyle="--")
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(mticker.ScalarFormatter())
    ax.yaxis.set_minor_formatter(mticker.ScalarFormatter())
    ax.yaxis.set_minor_locator(mticker.LogLocator(subs=tuple(range(2, 10)) + tuple(x / 10 for x in range(11, 20))))
    ax.set_title(f"Performance relative BTC / ALCPB / MSTR -- base 100 le {rebase_date} (echelle log)")
    ax.set_ylabel("Indice (base 100, log)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
    ax.legend(loc="upper left")
    ax.grid(True, which="both", alpha=0.3)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def build_caption(series: dict[str, pd.Series], rebase_date: str) -> str:
    lines = [f"\U0001f4ca BTC / ALCPB / MSTR -- base 100 le {rebase_date}"]
    ranked = sorted(series.items(), key=lambda kv: kv[1].iloc[-1], reverse=True)
    for label, s in ranked:
        icon = ICONS.get(label, "⚪")
        lines.append(f"{icon} {label}\t{s.iloc[-1] - 100:+.1f}%")
    lines.append(f"⏰ {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M')} Paris")
    return "\n".join(lines)


def pine_sar(df: pd.DataFrame, start: float = 0.1, increment: float = 0.1, maximum: float = 0.2) -> list[float]:
    """Recurrence Pine ta.sar, validee bar-a-bar vs TradingView (cf. memoire pine-sar-validated-vs-tradingview).

    Reference: https://www.tradingview.com/pine-script-reference/v6/#fun_ta.sar
    """
    highs, lows, closes = (df[c].astype(float).tolist() for c in ("high", "low", "close"))
    values = [math.nan] * len(df)
    if len(df) < 2:
        return values
    below = closes[1] > closes[0]
    extreme = highs[1] if below else lows[1]
    sar = lows[0] if below else highs[0]
    acceleration = start
    for i in range(1, len(df)):
        first = i == 1
        sar += acceleration * (extreme - sar)
        if below and sar > lows[i]:
            below, first = False, True
            sar, extreme, acceleration = max(highs[i], extreme), lows[i], start
        elif not below and sar < highs[i]:
            below, first = True, True
            sar, extreme, acceleration = min(lows[i], extreme), highs[i], start
        if not first:
            if below and highs[i] > extreme:
                extreme, acceleration = highs[i], min(acceleration + increment, maximum)
            elif not below and lows[i] < extreme:
                extreme, acceleration = lows[i], min(acceleration + increment, maximum)
        if below:
            sar = min(sar, lows[i - 1])
            if i > 1:
                sar = min(sar, lows[i - 2])
        else:
            sar = max(sar, highs[i - 1])
            if i > 1:
                sar = max(sar, highs[i - 2])
        values[i] = sar
    return values


def build_ratio_daily(df_num: pd.DataFrame, df_den: pd.DataFrame) -> pd.DataFrame:
    """Bougies "quotient" journalieres, approximees sur les dates communes aux deux actifs.

    close = closeA/closeB ; high = highA/lowB ; low = lowA/highB (bornes plausibles,
    pas un vrai OHLC -- cf. docstring du module).

    Chaque place fixe l'horaire de sa bougie journaliere a sa propre heure
    d'ouverture (00:00 UTC pour l'index crypto, 07:00 UTC Euronext, 13:30 UTC
    Nasdaq) : on joint donc sur la date calendaire, pas sur l'horodatage exact.
    """
    num_by_date = df_num[["open", "high", "low", "close"]].set_axis(df_num.index.normalize().tz_localize(None), axis=0)
    den_by_date = df_den[["open", "high", "low", "close"]].set_axis(df_den.index.normalize().tz_localize(None), axis=0)
    joined = num_by_date.join(den_by_date, how="inner", lsuffix="_n", rsuffix="_d")
    if joined.empty:
        return joined
    ratio = pd.DataFrame(index=joined.index)
    ratio["open"] = joined["open_n"] / joined["open_d"]
    ratio["high"] = joined["high_n"] / joined["low_d"]
    ratio["low"] = joined["low_n"] / joined["high_d"]
    ratio["close"] = joined["close_n"] / joined["close_d"]
    return ratio


def resample_ratio(daily: pd.DataFrame, rule: str | None) -> pd.DataFrame:
    if rule is None:
        return daily
    agg = daily.resample(rule).agg({"open": "first", "high": "max", "low": "min", "close": "last"})
    return agg.dropna(how="any")


def compute_ratio_stats(ratio_df: pd.DataFrame) -> dict | None:
    if ratio_df is None or len(ratio_df) < MIN_BARS_FOR_STATS:
        return None
    sar_vals = pine_sar(ratio_df, SAR_START, SAR_INC, SAR_MAX)
    rsi_vals = rsi(ratio_df["close"], RSI_LENGTH)
    close = ratio_df["close"].iloc[-1]
    sar = sar_vals[-1]
    rsi_val = rsi_vals.iloc[-1]
    if not math.isfinite(sar) or pd.isna(rsi_val):
        return None
    return {"close": float(close), "sar": float(sar), "bull": close > sar, "rsi": float(rsi_val)}


def compute_all_ratio_stats(raw: dict[str, pd.DataFrame]) -> dict[tuple[str, str], dict[str, dict]]:
    all_stats: dict[tuple[str, str], dict[str, dict]] = {}
    for num, den in RATIO_PAIRS:
        if num not in raw or den not in raw:
            continue
        daily = build_ratio_daily(raw[num], raw[den])
        if daily.empty:
            print(f"{num}/{den}: pas de dates communes, ignore")
            continue
        tf_stats: dict[str, dict] = {}
        for tf_code, rule, _ in TIMEFRAMES:
            stats = compute_ratio_stats(resample_ratio(daily, rule))
            if stats is not None:
                tf_stats[tf_code] = stats
        if tf_stats:
            all_stats[(num, den)] = tf_stats
    return all_stats


def timeframe_score(stats: dict) -> float:
    """Score d'un (ratio, timeframe) dans [-100, +100] : +-50 pour le cote du SAR, +-50 pour l'ecart du RSI a 50."""
    sar_component = 50.0 if stats["bull"] else -50.0
    rsi_component = stats["rsi"] - 50.0
    return sar_component + rsi_component


def ratio_composite_score(tf_stats: dict[str, dict]) -> float | None:
    """Moyenne des timeframe_score ponderee par TF_WEIGHTS -- reste dans [-100, +100]."""
    weighted_sum = weight_total = 0.0
    for tf_code, weight in TF_WEIGHTS.items():
        stats = tf_stats.get(tf_code)
        if stats is None:
            continue
        weighted_sum += weight * timeframe_score(stats)
        weight_total += weight
    if weight_total == 0.0:
        return None
    return weighted_sum / weight_total


def rank_ratios(all_stats: dict[tuple[str, str], dict[str, dict]]) -> list[tuple[tuple[str, str], dict, float]]:
    scored = [(pair, tf_stats, ratio_composite_score(tf_stats)) for pair, tf_stats in all_stats.items()]
    scored = [item for item in scored if item[2] is not None]
    scored.sort(key=lambda item: item[2], reverse=True)
    return scored


def build_ratio_caption(all_stats: dict[tuple[str, str], dict[str, dict]]) -> str:
    lines = ["\U0001f4c8 Quotients relatifs -- SAR & RSI(14)"]
    for rank, ((num, den), tf_stats, score) in enumerate(rank_ratios(all_stats)):
        medal = MEDALS[rank] if rank < len(MEDALS) else f"{rank + 1}."
        lines.append(f"{medal} {num}/{den}\t{score:+.0f}")
        for tf_code, _, tf_label in TIMEFRAMES:
            stats = tf_stats.get(tf_code)
            if stats is None:
                continue
            ball = "\U0001f7e2" if stats["bull"] else "\U0001f534"
            warn = " ⚠️" if stats["rsi"] >= RSI_OVERBOUGHT or stats["rsi"] <= RSI_OVERSOLD else ""
            lines.append(f"{ball} {tf_label}\tRSI {stats['rsi']:.0f}{warn}")
    lines.append(f"⏰ {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M')} Paris")
    return "\n".join(lines)


def send_telegram_photo(photo_bytes: bytes, caption: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram: credentials missing, skip send.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
        files = {"photo": ("relative_perf.png", photo_bytes, "image/png")}
        data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption}
        response = requests.post(url, data=data, files=files, timeout=20)
        ok = bool(response.json().get("ok", False))
        print(f"Telegram: {'sent' if ok else 'failed'}")
        return ok
    except Exception as exc:
        print(f"Telegram: send failed ({exc})")
        return False


def send_telegram_message(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram: credentials missing, skip send.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        ok = bool(response.json().get("ok", False))
        print(f"Telegram: {'sent' if ok else 'failed'}")
        return ok
    except Exception as exc:
        print(f"Telegram: send failed ({exc})")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rebase-date", default=DEFAULT_REBASE_DATE, help="date de base (YYYY-MM-DD), indice 100")
    parser.add_argument("--no-send", action="store_true", help="ne rien envoyer sur Telegram, juste sauvegarder le PNG")
    parser.add_argument("--no-ratios", action="store_true", help="ignorer l'analyse des quotients (SAR/RSI)")
    args = parser.parse_args()

    raw = fetch_daily_data()
    if not raw:
        print("Aucune donnee disponible, arret.")
        return

    series = compute_indexed_series(raw, args.rebase_date)
    if series:
        photo_bytes = build_chart(series, args.rebase_date)
        with open(CHART_PATH, "wb") as handle:
            handle.write(photo_bytes)
        print(f"Graphique sauvegarde: {CHART_PATH}")
        for label, s in series.items():
            print(f"{label}: {s.iloc[-1] - 100:+.2f}% depuis {args.rebase_date}")
        if not args.no_send:
            send_telegram_photo(photo_bytes, build_caption(series, args.rebase_date))
    else:
        print("Aucune serie indexee disponible, graphique ignore.")

    if not args.no_ratios:
        all_stats = compute_all_ratio_stats(raw)
        for rank, ((num, den), tf_stats, score) in enumerate(rank_ratios(all_stats), start=1):
            print(f"#{rank} {num}/{den}: score={score:+.1f}")
            for tf_code, _, tf_label in TIMEFRAMES:
                stats = tf_stats.get(tf_code)
                if stats is None:
                    continue
                state = "bull" if stats["bull"] else "bear"
                print(f"  {tf_label}: close={stats['close']:.4g} sar={stats['sar']:.4g} ({state}) rsi={stats['rsi']:.1f}")
        if all_stats and not args.no_send:
            send_telegram_message(build_ratio_caption(all_stats))


if __name__ == "__main__":
    main()
