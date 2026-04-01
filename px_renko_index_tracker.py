#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
px_renko_index_tracker.py

Virtual P&L tracker for px_renko_screener_index.py signals.
- Opens a virtual position when a pair appears with bl_confirmed + BULL/BEAR/LONG/SHORT.
- Closes the position when the pair disappears from the active list.
- Sends a Telegram summary each run.
"""

import json
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

PARIS_TZ = ZoneInfo("Europe/Paris")
CAPITAL_PER_PAIR = 10.0       # USD per position
LEVERAGE = 50                  # 50:1 forex leverage
STARTING_CAPITAL = 10_000.0   # USD

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCAN_PATH = os.path.join(SCRIPT_DIR, "px_renko_index_scan.json")
STATE_PATH = os.path.join(SCRIPT_DIR, "px_renko_index_tracker_state.json")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ---------------------------------------------------------------------------
# Spread table (pips)
# ---------------------------------------------------------------------------

SPREAD_PIPS: dict[str, float] = {
    "EURUSD": 1.4, "USDJPY": 1.5, "GBPUSD": 1.5, "USDCHF": 1.6,
    "USDCAD": 1.9, "AUDUSD": 1.4, "NZDUSD": 2.0,
    "EURGBP": 1.8, "EURJPY": 1.8, "EURCAD": 2.5, "EURCHF": 2.2,
    "EURAUD": 2.5, "EURNZD": 3.5, "GBPJPY": 2.8, "GBPCAD": 3.5,
    "GBPCHF": 3.2, "GBPAUD": 3.5, "GBPNZD": 4.5,
    "AUDJPY": 2.0, "AUDCAD": 2.5, "AUDCHF": 2.5, "AUDNZD": 3.0,
    "NZDJPY": 2.5, "NZDCAD": 3.5, "NZDCHF": 3.5,
    "CADJPY": 2.5, "CADCHF": 3.0, "CHFJPY": 2.8,
    "XAUUSD": 3.0,
}
DEFAULT_SPREAD_PIPS = 2.5


def pip_size(pair: str) -> float:
    if pair.endswith("JPY"):
        return 0.01
    if pair == "XAUUSD":
        return 0.1
    return 0.0001


def spread_cost(pair: str) -> float:
    return SPREAD_PIPS.get(pair, DEFAULT_SPREAD_PIPS) * pip_size(pair)


def calc_pips(pair: str, direction: str, entry: float, exit_: float) -> float:
    d = 1 if direction == "BULL" else -1
    return d * (exit_ - entry) / pip_size(pair)


def calc_pnl(direction: str, entry: float, exit_: float) -> float:
    if entry == 0:
        return 0.0
    d = 1 if direction == "BULL" else -1
    return d * (exit_ - entry) / entry * CAPITAL_PER_PAIR * LEVERAGE


# ---------------------------------------------------------------------------
# Price fetch
# ---------------------------------------------------------------------------

_PRICE_CACHE: dict[str, float] = {}


def fetch_price(pair: str) -> float | None:
    if pair in _PRICE_CACHE:
        return _PRICE_CACHE[pair]
    try:
        from heiken_ashi_aligned_v2 import fetch_tv_ohlc
        df = fetch_tv_ohlc(f"OANDA:{pair}", "60", 5)
        if df is None or df.empty:
            return None
        price = float(df["close"].iloc[-1])
        _PRICE_CACHE[pair] = price
        return price
    except Exception:
        return None


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def load_state() -> dict:
    default_state = {
        "open_positions": {},
        "closed_history": [],
        "total_realized_pnl": 0.0,
        "next_trade_id": 1,
    }
    if not os.path.exists(STATE_PATH):
        return default_state
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return default_state
        state = {
            "open_positions": dict(data.get("open_positions", {})),
            "closed_history": list(data.get("closed_history", [])),
            "total_realized_pnl": float(data.get("total_realized_pnl", 0.0)),
            "next_trade_id": int(data.get("next_trade_id", 1)),
        }
        known_ids = []
        for pos in state["open_positions"].values():
            trade_id = pos.get("trade_id")
            if isinstance(trade_id, int):
                known_ids.append(trade_id)
        for item in state["closed_history"]:
            trade_id = item.get("trade_id")
            if isinstance(trade_id, int):
                known_ids.append(trade_id)
        next_trade_id = max(known_ids, default=0) + 1
        if state["next_trade_id"] < next_trade_id:
            state["next_trade_id"] = next_trade_id
        state["updated_at"] = data.get("updated_at")
        return state
    except Exception:
        return default_state


def save_state(state: dict) -> None:
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Signal reader
# ---------------------------------------------------------------------------

def read_signals() -> dict[str, dict]:
    """Read active pairs from px_renko_index_scan.json → {pair: direction}."""
    if not os.path.exists(SCAN_PATH):
        return {}
    try:
        with open(SCAN_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        updated_at = data.get("updated_at")
        signals = {}
        for row in data.get("active_pairs", []):
            pair = row.get("pair")
            direction = row.get("direction")
            signal = row.get("signal") or direction
            if not pair or not direction:
                continue
            signals[pair] = {
                "direction": direction,
                "signal": signal,
                "score": row.get("score"),
                "updated_at": updated_at,
            }
        return signals
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Core tracking
# ---------------------------------------------------------------------------

def process(current_signals: dict[str, dict], state: dict) -> dict:
    open_positions: dict = dict(state.get("open_positions", {}))
    closed_history: list = list(state.get("closed_history", []))
    total_realized: float = float(state.get("total_realized_pnl", 0.0))
    next_trade_id: int = int(state.get("next_trade_id", 1))
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for pos in open_positions.values():
        if pos.get("trade_id") is None:
            pos["trade_id"] = next_trade_id
            next_trade_id += 1

    all_pairs = set(open_positions.keys()) | set(current_signals.keys())
    prices = {p: fetch_price(p) for p in all_pairs}
    prices = {p: v for p, v in prices.items() if v is not None}

    new_closed: list[dict] = []

    # Close positions no longer in signals
    for pair in list(open_positions.keys()):
        pos = open_positions[pair]
        current = current_signals.get(pair)
        if (
            current
            and current.get("direction") == pos["direction"]
            and current.get("signal", current.get("direction")) == pos.get("source_signal", pos["direction"])
        ):
            continue
        half = spread_cost(pair) / 2
        raw = prices.get(pair, pos["entry_price"])
        exit_price = raw - half if pos["direction"] == "BULL" else raw + half
        pips = calc_pips(pair, pos["direction"], pos["entry_price"], exit_price)
        pnl = calc_pnl(pos["direction"], pos["entry_price"], exit_price)
        entry = {
            "trade_id": pos.get("trade_id"),
            "pair": pair,
            "direction": pos["direction"],
            "entry_price": pos["entry_price"],
            "entry_at": pos["entry_at"],
            "exit_price": round(exit_price, 6),
            "exit_at": now_utc,
            "pips": round(pips, 1),
            "pnl_usd": round(pnl, 2),
        }
        closed_history.append(entry)
        new_closed.append(entry)
        total_realized += pnl
        del open_positions[pair]

    # Open new positions
    new_opened: list[str] = []
    for pair, signal_data in current_signals.items():
        direction = signal_data["direction"]
        if pair in open_positions:
            continue
        raw = prices.get(pair)
        if raw is None:
            continue
        half = spread_cost(pair) / 2
        entry_price = raw + half if direction == "BULL" else raw - half
        open_positions[pair] = {
            "trade_id": next_trade_id,
            "pair": pair,
            "direction": direction,
            "source_signal": signal_data.get("signal", direction),
            "source_updated_at": signal_data.get("updated_at"),
            "entry_price": round(entry_price, 6),
            "entry_at": now_utc,
        }
        next_trade_id += 1
        new_opened.append(pair)

    # Latent P&L on open positions
    open_with_pnl: list[dict] = []
    for pair, pos in open_positions.items():
        raw = prices.get(pair, pos["entry_price"])
        half = spread_cost(pair) / 2
        mark = raw - half if pos["direction"] == "BULL" else raw + half
        pips = calc_pips(pair, pos["direction"], pos["entry_price"], mark)
        pnl = calc_pnl(pos["direction"], pos["entry_price"], mark)
        entry_dt = datetime.fromisoformat(pos["entry_at"].replace("Z", "+00:00"))
        days = (datetime.now(timezone.utc) - entry_dt).days
        open_with_pnl.append({
            "trade_id": pos.get("trade_id"),
            "pair": pair,
            "direction": pos["direction"],
            "pips": round(pips, 1),
            "pnl_usd": round(pnl, 2),
            "days": days,
            "entry_at": pos["entry_at"],
        })
    open_with_pnl.sort(key=lambda x: x["pnl_usd"], reverse=True)

    state["open_positions"] = open_positions
    state["closed_history"] = closed_history
    state["total_realized_pnl"] = round(total_realized, 2)
    state["next_trade_id"] = next_trade_id

    return {
        "new_closed": new_closed,
        "new_opened": new_opened,
        "open_with_pnl": open_with_pnl,
        "total_realized": round(total_realized, 2),
        "total_latent": round(sum(x["pnl_usd"] for x in open_with_pnl), 2),
    }


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def build_message(result: dict) -> str:
    lines = ["📊 PX RENKO INDEX TRACKER"]

    if result["new_closed"]:
        lines.append("\nCLOSED")
        for c in result["new_closed"]:
            icon = "✅" if c["pnl_usd"] >= 0 else "❌"
            dir_icon = "🟢" if c["direction"] == "BULL" else "🔴"
            trade_tag = f" #{c['trade_id']}" if c.get("trade_id") else ""
            lines.append(f"{icon} {c['pair']}{trade_tag} {dir_icon} {c['pips']:+.0f} pips ({c['pnl_usd']:+.2f}$)")

    open_pnl = result["open_with_pnl"]
    lines.append(f"\nOPEN ({len(open_pnl)})")
    if open_pnl:
        for o in open_pnl:
            dir_icon = "🟢" if o["direction"] == "BULL" else "🔴"
            new_tag = " 🆕" if o["pair"] in result["new_opened"] else ""
            trade_tag = f" #{o['trade_id']}" if o.get("trade_id") else ""
            lines.append(f"{dir_icon} {o['pair']}{trade_tag} {o['pips']:+.0f} pips ({o['pnl_usd']:+.2f}$){new_tag}")
    else:
        lines.append("Aucune position ouverte")

    real = result["total_realized"]
    lat = result["total_latent"]
    equity = STARTING_CAPITAL + real + lat
    pct = (equity - STARTING_CAPITAL) / STARTING_CAPITAL * 100
    lines.extend([
        f"\nRéalisé: {real:+.2f}$ | Latent: {lat:+.2f}$",
        f"💼 Capital: {equity:,.2f}$ ({pct:+.2f}%)",
        f"⏰ {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M Paris')}",
    ])
    return "\n".join(lines)


def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  Telegram: credentials missing, skip.")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        ok = resp.json().get("ok", False)
        print(f"  Telegram: {'sent' if ok else 'failed'}")
    except Exception as e:
        print(f"  Telegram: error ({e})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    t0 = time.time()

    signals = read_signals()
    print(f"  Active signals: {len(signals)}")
    for pair, direction in sorted(signals.items()):
        print(f"    {pair:<8} {direction}")

    state = load_state()
    result = process(signals, state)
    state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    save_state(state)

    print(f"  Opened: {len(result['new_opened'])} | Closed: {len(result['new_closed'])}")
    print(f"  Open: {len(result['open_with_pnl'])} | Realized: {result['total_realized']:+.2f}$ | Latent: {result['total_latent']:+.2f}$")

    msg = build_message(result)
    print(f"\n{msg}\n")
    send_telegram(msg)

    print(f"  Elapsed: {time.time() - t0:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
