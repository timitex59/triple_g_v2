#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
renko_alignment_etf_V1.py
Renko ATR(14) alignment 3M/M/W for a watchlist of Euronext ETFs.
Output format mirrors renko_alignment_nasdaq_V2.py.
"""

import sys
import argparse
import itertools
import json
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

import pytz

import os
from dotenv import load_dotenv
import requests as http_requests

load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

HIGHLIGHT_ETFs = {"PUST", "CL2", "PSP5"}

from renko_nasdaq import (
    fetch_tv_ohlc,
    _fetch_renko_with_fallback,
    px_state,
    green_streak,
)

def send_telegram_html(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = http_requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }, timeout=10)
        return bool(resp.json().get("ok", False))
    except Exception:
        return False

ETFs = [
    ("EURONEXT:WEBH",  "WEBH"),
    ("EURONEXT:PUST",  "PUST"),
    ("EURONEXT:ISRU",  "ISRU"),
    ("EURONEXT:ISUH",  "ISUH"),
    ("EURONEXT:PE500", "PE500"),
    ("EURONEXT:PSPS",  "PSPS"),
    ("EURONEXT:P500H", "P500H"),
    ("EURONEXT:USVE",  "USVE"),
    ("EURONEXT:PNAS",  "PNAS"),
    ("EURONEXT:ISOE",  "ISOE"),
    ("EURONEXT:LQQ",   "LQQ"),
    ("EURONEXT:LWLD",  "LWLD"),
    ("EURONEXT:PAEEM", "PAEEM"),
    ("EURONEXT:PSP5",  "PSP5"),
    ("EURONEXT:CL2",   "CL2"),
]


def parse_args():
    p = argparse.ArgumentParser(description="Renko 3M/M/W alignment — ETF Watchlist V1")
    p.add_argument("--length",      type=int, default=14)
    p.add_argument("--no-telegram", action="store_true")
    p.add_argument("--debug",       action="store_true")
    return p.parse_args()


def scan_one(tv_sym: str, name: str, atr_length: int, debug: bool):
    brick_3m = _fetch_renko_with_fallback(tv_sym, "3M", atr_length, 100, debug)
    if not brick_3m:
        return None
    brick_m = _fetch_renko_with_fallback(tv_sym, "M", atr_length, 200, debug)
    if not brick_m:
        return None
    brick_w = _fetch_renko_with_fallback(tv_sym, "W", atr_length, 200, debug)
    if not brick_w:
        return None

    bars_w = fetch_tv_ohlc(tv_sym, "W", 3)
    if not bars_w or len(bars_w) < 2:
        return None

    curr_close = float(bars_w[-1]["close"])
    prev_close = float(bars_w[-2]["close"])

    last_3m = brick_3m[-1]
    last_m  = brick_m[-1]
    last_w  = brick_w[-1]

    px_3m = px_state(last_3m["open"], last_3m["close"], curr_close)
    px_m  = px_state(last_m["open"],  last_m["close"],  curr_close)
    px_w  = px_state(last_w["open"],  last_w["close"],  curr_close)

    dir_3m = 1 if last_3m["close"] > last_3m["open"] else -1
    dir_m  = 1 if last_m["close"]  > last_m["open"]  else -1
    dir_w  = 1 if last_w["close"]  > last_w["open"]  else -1

    streak_w = green_streak(brick_w)

    chg_abs = curr_close - prev_close
    chg_pct = (chg_abs / prev_close * 100) if prev_close else 0.0

    return {
        "symbol":   tv_sym,
        "name":     name,
        "close":    curr_close,
        "dir_3m":   dir_3m,
        "dir_m":    dir_m,
        "dir_w":    dir_w,
        "px_3m":    px_3m,
        "px_m":     px_m,
        "px_w":     px_w,
        "streak_w": streak_w,
        "chg_abs":  chg_abs,
        "chg_pct":  chg_pct,
    }


def is_full_bull(r: dict) -> bool:
    return (
        r["dir_3m"] == 1 and r["dir_m"] == 1 and r["dir_w"] == 1
        and r["px_3m"] == 1 and r["px_m"] == 1 and r["px_w"] == 1
        and r["streak_w"] >= 1
        and r["chg_pct"] >= 0
    )


def dot(direction: int, px: int) -> str:
    if direction == 1 and px == 1:
        return "🟢"
    if direction == -1 and px == -1:
        return "🔴"
    return "🟡"


def _rsi(series: list[float], length: int = 14) -> float | None:
    """Wilder RSI avec lissage EMA (identique TradingView). Nécessite length*10+ barres."""
    if len(series) < length + 1:
        return None
    changes = [series[i] - series[i - 1] for i in range(1, len(series))]
    gains  = [max(c, 0.0) for c in changes]
    losses = [abs(min(c, 0.0)) for c in changes]
    # Seed avec moyenne simple sur les 'length' premières variations
    avg_gain = sum(gains[:length]) / length
    avg_loss = sum(losses[:length]) / length
    # Lissage de Wilder sur le reste
    for g, l in zip(gains[length:], losses[length:]):
        avg_gain = (avg_gain * (length - 1) + g) / length
        avg_loss = (avg_loss * (length - 1) + l) / length
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))


def compute_individual(etf_list: list[tuple[str, str]], debug: bool = False) -> list[dict]:
    """Fetch 30 daily bars for each ETF, compute individual ROC(14) and RSI(14)."""
    results = []
    for tv_sym, name in etf_list:
        bars = fetch_tv_ohlc(tv_sym, "D", 200)
        if not bars or len(bars) < 16:
            if debug:
                print(f"  {name}: not enough daily bars")
            continue
        closes = [float(b["close"]) for b in bars]
        roc = (closes[-1] - closes[-15]) / abs(closes[-15]) * 100 if closes[-15] != 0 else None
        rsi = _rsi(closes)
        results.append({"name": name, "close": closes[-1], "roc14": roc, "rsi14": rsi})
    return results


def compute_ratios(etf_list: list[tuple[str, str]], debug: bool = False) -> list[dict]:
    """Fetch 30 daily bars for each ETF, compute all pairwise ratios, ROC(14) and RSI(14)."""
    closes: dict[str, list[float]] = {}
    for tv_sym, name in etf_list:
        bars = fetch_tv_ohlc(tv_sym, "D", 200)
        if not bars or len(bars) < 16:
            if debug:
                print(f"  {name}: not enough daily bars for ratio")
            continue
        closes[name] = [float(b["close"]) for b in bars]

    ratios = []
    for (n1, c1), (n2, c2) in itertools.permutations(closes.items(), 2):
        length = min(len(c1), len(c2))
        if length < 16:
            continue
        c1 = c1[-length:]
        c2 = c2[-length:]
        ratio_series = [c1[i] / c2[i] for i in range(length)]
        ratio_now = ratio_series[-1]
        ratio_14  = ratio_series[-15]
        if ratio_14 == 0:
            continue
        roc  = (ratio_now - ratio_14) / abs(ratio_14) * 100
        rsi  = _rsi(ratio_series)
        ratios.append({"pair": f"{n1}/{n2}", "ratio": ratio_now, "roc14": roc, "rsi14": rsi})
    return ratios


METRICS_HISTORY_FILE = "etf_v1_metrics_history.json"
METRICS_ROC_PERIOD   = 14


def load_metrics_history() -> list[dict]:
    try:
        with open(METRICS_HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_metrics_history(history: list[dict]) -> None:
    with open(METRICS_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history[-200:], f, indent=2)


def compute_metrics_roc14(history: list[dict]) -> dict:
    """Return ROC-14 (%) and trend label for rsi/roc/perf using last METRICS_ROC_PERIOD entries."""
    result = {}
    if len(history) <= METRICS_ROC_PERIOD:
        return result
    now   = history[-1]
    ref   = history[-(METRICS_ROC_PERIOD + 1)]
    for key in ("rsi", "roc", "perf"):
        v_now = now.get(key)
        v_ref = ref.get(key)
        if v_now is None or v_ref is None or v_ref == 0:
            continue
        roc14 = (v_now - v_ref) / abs(v_ref) * 100
        result[key] = {"now": v_now, "ref": v_ref, "roc14": roc14, "label": "FORT" if roc14 >= 0 else "FAIBLE"}
    return result


def _rsi_label(v: float) -> str:
    if v > 70:  return "SURACHETÉ"
    if v >= 50: return "HAUSSIER"
    if v >= 30: return "BAISSIER"
    return "SURVENDU"

def _roc_label(v: float) -> str:
    if v > 15:  return "SURCHAUFFE"
    if v >= 5:  return "SAIN"
    if v >= 0:  return "FRAGILE"
    return "BAISSIER"

def _trend_dot(key: str, current_val: float, mr: dict) -> str:
    if key in mr:
        return "🟢" if mr[key]["label"] == "FORT" else "🔴"
    return "🟢" if current_val >= 0 else "🔴"

def _market_status(individuals: list[dict], avg_score: float | None = None, metrics_roc14: dict | None = None) -> str:
    rsi_values = [x["rsi14"] for x in individuals if x["rsi14"] is not None]
    roc_values = [x["roc14"] for x in individuals if x["roc14"] is not None]
    mr = metrics_roc14 or {}
    lines = []
    if rsi_values:
        avg_rsi = sum(rsi_values) / len(rsi_values)
        dot = _trend_dot("rsi", avg_rsi - 50, mr)
        lines.append(f"💹 RSI {avg_rsi:.1f} ({_rsi_label(avg_rsi)}){dot}")
    if roc_values:
        avg_roc = sum(roc_values) / len(roc_values)
        dot = _trend_dot("roc", avg_roc, mr)
        lines.append(f"⚡ ROC {avg_roc:+.1f}% ({_roc_label(avg_roc)}){dot}")
    if avg_score is not None:
        dot = _trend_dot("perf", avg_score, mr)
        lines.append(f"📊 PERF MOYEN {avg_score:+.2f}{dot}")
    return ("\n" + "\n".join(lines)) if lines else ""


def build_full_console(results: list[dict], individuals: list[dict], ratios: list[dict], scores: list[dict], avg_score: float, now_str: str, metrics_roc14: dict | None = None, etf_score_trend: dict | None = None, etf_score_delta: dict | None = None) -> str:
    bull = [r for r in results if is_full_bull(r)]
    bull.sort(key=lambda r: r["chg_pct"], reverse=True)

    lines = ["📊 ETF V1" + _market_status(individuals, avg_score, metrics_roc14), "", "MEILLEUR HEBDO"]
    if not bull:
        lines.append("(aucun)")
    else:
        for r in bull:
            fire = " 🔥" if r["chg_pct"] > 2.0 else ""
            sign = "+" if r["chg_pct"] >= 0 else ""
            lines.append(f"🟢 {r['name']} ({sign}{r['chg_pct']:.2f}%){fire}")

    lines += ["", "📊 ROC(14) & RSI(14) INDIVIDUELS"]
    ind_sorted = sorted(individuals, key=lambda x: x["roc14"] if x["roc14"] is not None else float("-inf"), reverse=True)
    for x in ind_sorted:
        roc_str = f"ROC14={x['roc14']:+.1f}%" if x["roc14"] is not None else "ROC14=N/A"
        rsi_str = f"RSI14={x['rsi14']:.1f}" if x["rsi14"] is not None else "RSI14=N/A"
        lines.append(f"  {x['name']:<6}  {roc_str:<14}  {rsi_str}")

    roc_ratios = sorted([x for x in ratios if 5 <= x["roc14"] <= 10], key=lambda x: x["roc14"], reverse=True)
    lines += ["", "📈 RATIOS ROC(14) [5% – 10%]"]
    lines += [f"  {x['pair']}  ratio={x['ratio']:.4f}  ROC14={x['roc14']:+.1f}%" for x in roc_ratios] or ["(aucun)"]

    rsi_ratios = sorted([x for x in ratios if x["rsi14"] is not None and x["rsi14"] > 55], key=lambda x: x["rsi14"], reverse=True)
    lines += ["", "💪 RATIOS RSI(14) > 55"]
    lines += [f"  {x['pair']}  ratio={x['ratio']:.4f}  RSI14={x['rsi14']:.1f}" for x in rsi_ratios] or ["(aucun)"]

    valid_scores = [s for s in scores if s["score"] is not None]
    avg_score = sum(s["score"] for s in valid_scores) / len(valid_scores) if valid_scores else 0
    trend = etf_score_trend or {}
    deltas = etf_score_delta or {}
    lines += ["", f"🎯 SCORE DE PERFORMANCE (ROC14 × Multiplicateur RSI) — moy. {avg_score:+.2f}"]
    all_deltas = list(deltas.values())
    avg_delta = sum(all_deltas) / len(all_deltas) if all_deltas else None
    for s in scores:
        score_str = f"{s['score']:+.2f}" if s["score"] is not None else "N/A"
        marker = " ★" if s["score"] is not None and s["score"] >= avg_score else ""
        dot = trend.get(s["name"], "")
        rising_marker = " 🚀" if (avg_delta is not None and s["score"] is not None
                                   and s["score"] < avg_score
                                   and deltas.get(s["name"], float("-inf")) > avg_delta) else ""
        lines.append(f"  {s['name']:<6}  {score_str}{marker} {dot}{rising_marker}")

    lines += ["", f"⏰ {now_str} Paris"]
    return "\n".join(lines)


def build_message(results: list[dict], individuals: list[dict], ratios: list[dict], scores: list[dict], avg_score: float, now_str: str, metrics_roc14: dict | None = None, etf_score_trend: dict | None = None, etf_score_delta: dict | None = None) -> str:
    bull = [r for r in results if is_full_bull(r)]
    bull.sort(key=lambda r: r["chg_pct"], reverse=True)

    lines = ["📊 ETF V1" + _market_status(individuals, avg_score, metrics_roc14), "", "MEILLEUR HEBDO"]
    if not bull:
        lines.append("(aucun)")
    else:
        for r in bull:
            fire = " 🔥" if r["chg_pct"] > 2.0 else ""
            sign = "+" if r["chg_pct"] >= 0 else ""
            label = f"{r['name']} ({sign}{r['chg_pct']:.2f}%){fire}"
            if r["name"] in HIGHLIGHT_ETFs:
                label = f"<b>{label}</b>"
            lines.append(f"🟢 {label}")

    above_avg = [s for s in scores if s["score"] is not None and s["score"] >= avg_score]
    below_avg = [s for s in scores if s["score"] is not None and s["score"] < avg_score]
    trend = etf_score_trend or {}
    deltas = etf_score_delta or {}

    lines += ["", "🎯 ETF DYNAMIQUE"]
    for s in above_avg:
        dot = trend.get(s["name"], "")
        d = deltas.get(s["name"])
        delta_str = f" (Δ{d:+.2f})" if d is not None else ""
        label = f"  {s['name']:<6}  {s['score']:+.2f}{delta_str} {dot}"
        if s["name"] in HIGHLIGHT_ETFs:
            label = f"<b>{label}</b>"
        lines.append(label)

    # ETFs non-dynamiques mais dont la progression dépasse la moyenne de tous les deltas
    if deltas:
        all_deltas = list(deltas.values())
        avg_delta = sum(all_deltas) / len(all_deltas)
        rising = [s for s in below_avg if deltas.get(s["name"], float("-inf")) > avg_delta]
        if rising:
            lines += ["", "🚀 EN PROGRESSION"]
            for s in rising:
                d = deltas[s["name"]]
                label = f"  {s['name']:<6}  {s['score']:+.2f} (Δ{d:+.2f})"
                if s["name"] in HIGHLIGHT_ETFs:
                    label = f"<b>{label}</b>"
                lines.append(label)

    lines += ["", f"⏰ {now_str} Paris"]
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    print(f"Scanning {len(ETFs)} ETFs...\n")

    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(scan_one, tv_sym, name, args.length, args.debug): (tv_sym, name)
            for tv_sym, name in ETFs
        }
        for future, (tv_sym, name) in futures.items():
            try:
                r = future.result(timeout=15)
            except FuturesTimeoutError:
                future.cancel()
                print(f"  {name}: timeout — skipped")
                continue
            except Exception as e:
                if args.debug:
                    print(f"  {name}: error — {e}")
                continue
            if r:
                results.append(r)

    paris_tz = pytz.timezone("Europe/Paris")
    now_str  = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M")

    print("Computing individual indicators...")
    individuals = compute_individual(ETFs, debug=args.debug)

    print("Computing pairwise ratios...")
    ratios = compute_ratios(ETFs, debug=args.debug)

    # Calcul des scores (partagé entre terminal et Telegram)
    all_names = [name for _, name in ETFs]
    num_scores: dict[str, list[float]] = {name: [] for name in all_names}
    for x in ratios:
        if x["rsi14"] is None:
            continue
        numerator = x["pair"].split("/")[0]
        if numerator in num_scores:
            num_scores[numerator].append(x["rsi14"] / 100)
    multipliers = {name: sum(v) / len(v) if v else 0.0 for name, v in num_scores.items()}
    ind_map = {x["name"]: x["roc14"] for x in individuals}
    scores = [
        {"name": name, "score": ind_map.get(name, 0) * mult if ind_map.get(name) is not None else None}
        for name, mult in multipliers.items()
    ]
    scores.sort(key=lambda x: x["score"] if x["score"] is not None else float("-inf"), reverse=True)
    valid_scores = [s for s in scores if s["score"] is not None]
    avg_score = sum(s["score"] for s in valid_scores) / len(valid_scores) if valid_scores else 0.0

    # Compute avg_rsi / avg_roc for history
    rsi_values = [x["rsi14"] for x in individuals if x["rsi14"] is not None]
    roc_values = [x["roc14"] for x in individuals if x["roc14"] is not None]
    avg_rsi = sum(rsi_values) / len(rsi_values) if rsi_values else None
    avg_roc = sum(roc_values) / len(roc_values) if roc_values else None

    # Load history, append current entry (globals + per-ETF scores), save
    history = load_metrics_history()
    entry = {"ts": now_str, "rsi": avg_rsi, "roc": avg_roc, "perf": avg_score,
             "scores": {s["name"]: s["score"] for s in scores if s["score"] is not None}}
    history.append(entry)
    save_metrics_history(history)
    metrics_roc14 = compute_metrics_roc14(history)

    # Per-ETF score delta vs 14 runs ago → trend dot + rising ETFs detection
    etf_score_trend: dict[str, str] = {}
    etf_score_delta: dict[str, float] = {}
    if len(history) > METRICS_ROC_PERIOD:
        now_scores = history[-1].get("scores", {})
        ref_scores = history[-(METRICS_ROC_PERIOD + 1)].get("scores", {})
        for name, val_now in now_scores.items():
            val_ref = ref_scores.get(name)
            if val_ref is not None:
                delta = val_now - val_ref
                etf_score_delta[name] = delta
                etf_score_trend[name] = "🟢" if delta >= 0 else "🔴"

    # Affichage complet dans le terminal
    console = build_full_console(results, individuals, ratios, scores, avg_score, now_str, metrics_roc14, etf_score_trend, etf_score_delta)
    print(f"\n{console}\n")

    # Telegram : message résumé uniquement
    telegram_msg = build_message(results, individuals, ratios, scores, avg_score, now_str, metrics_roc14, etf_score_trend, etf_score_delta)
    if not args.no_telegram:
        send_telegram_html(telegram_msg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
