#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
strategy_tracker.py

Track virtual P&L of CONSOLIDATION and HEIKEN_ICHI_V2 strategies.
- When a pair appears in a strategy's signal list → open a $10 virtual position.
- When a pair disappears → close the position and record realized P&L.
- Direction change (BULL→BEAR or vice-versa) → close old, open new.
- Send a Telegram summary each run.
"""

import json
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

PARIS_TZ = ZoneInfo("Europe/Paris")
CAPITAL_PER_PAIR = 10.0  # USD per position
LEVERAGE = 50            # 50:1 forex leverage
STARTING_CAPITAL = 10_000.0  # USD capital de départ par stratégie

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONSOLIDATION_PATH = os.path.join(SCRIPT_DIR, "triple_g_consolidation.json")
HEIKEN_SCAN_PATH = os.path.join(SCRIPT_DIR, "heiken_ashi_scan.json")
STATE_PATH = os.path.join(SCRIPT_DIR, "strategy_tracker_state.json")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


# ---------------------------------------------------------------------------
# Spread table (average pips, standard OANDA-like account, no commission)
# Sources: OANDA, IC Markets, industry averages 2024-2025
# ---------------------------------------------------------------------------

SPREAD_PIPS: dict[str, float] = {
    # Majors (USD on one side, most liquid)
    "EURUSD": 1.4,
    "USDJPY": 1.5,
    "GBPUSD": 1.5,
    "USDCHF": 1.6,
    "USDCAD": 1.9,
    "AUDUSD": 1.4,
    "NZDUSD": 2.0,
    # Minors / Crosses
    "EURGBP": 1.8,
    "EURJPY": 1.8,
    "EURCAD": 2.5,
    "EURCHF": 2.2,
    "EURAUD": 2.5,
    "EURNZD": 3.5,
    "GBPJPY": 2.8,
    "GBPCAD": 3.5,
    "GBPCHF": 3.2,
    "GBPAUD": 3.5,
    "GBPNZD": 4.5,
    "AUDJPY": 2.0,
    "AUDCAD": 2.5,
    "AUDCHF": 2.5,
    "AUDNZD": 3.0,
    "NZDJPY": 2.5,
    "NZDCAD": 3.5,
    "NZDCHF": 3.5,
    "CADJPY": 2.5,
    "CADCHF": 3.0,
    "CHFJPY": 2.8,
    # Metals
    "XAUUSD": 3.0,   # ~$0.30 spread, pip_size=0.1 → 3 pips
}

DEFAULT_SPREAD_PIPS = 2.5  # fallback for unlisted pairs


def spread_cost_price(pair: str) -> float:
    """Return the full spread in price units for the pair."""
    s = SPREAD_PIPS.get(pair, DEFAULT_SPREAD_PIPS)
    return s * pip_size(pair)


# ---------------------------------------------------------------------------
# Price helpers
# ---------------------------------------------------------------------------

_PRICE_CACHE: dict[str, float] = {}


def _fetch_current_price(pair: str) -> float | None:
    """Fetch latest H1 close from TradingView via the heiken_ashi_aligned_v2 fetcher."""
    if pair in _PRICE_CACHE:
        return _PRICE_CACHE[pair]
    try:
        from heiken_ashi_aligned_v2 import fetch_tv_ohlc
        symbol = f"OANDA:{pair}"
        df = fetch_tv_ohlc(symbol, "60", 5)
        if df is None or df.empty:
            return None
        price = float(df["close"].iloc[-1])
        _PRICE_CACHE[pair] = price
        return price
    except Exception:
        return None


def pip_size(pair: str) -> float:
    if pair.endswith("JPY"):
        return 0.01
    if pair == "XAUUSD":
        return 0.1
    return 0.0001


def calc_pips(pair: str, direction: str, entry_price: float, exit_price: float) -> float:
    d = 1 if direction == "BULL" else -1
    return d * (exit_price - entry_price) / pip_size(pair)


def calc_pnl_usd(direction: str, entry_price: float, exit_price: float, capital: float) -> float:
    d = 1 if direction == "BULL" else -1
    if entry_price == 0:
        return 0.0
    return d * (exit_price - entry_price) / entry_price * capital * LEVERAGE


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: dict) -> None:
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Signal readers
# ---------------------------------------------------------------------------

def read_consolidation_signals() -> dict[str, str]:
    """Read CONSOLIDATION tradable_pairs → {pair: direction}."""
    if not os.path.exists(CONSOLIDATION_PATH):
        return {}
    try:
        with open(CONSOLIDATION_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        signals = {}
        for row in data.get("tradable_pairs", []):
            pair = row.get("pair")
            direction = row.get("direction")
            if pair and direction in ("BULL", "BEAR"):
                signals[pair] = direction
        return signals
    except Exception:
        return {}


def read_heiken_signals(top_n: int = 5) -> dict[str, str]:
    """Read HEIKEN_ICHI_V2 aligned_pairs, keep only TOP N by |chg_cc_d1|."""
    if not os.path.exists(HEIKEN_SCAN_PATH):
        return {}
    try:
        with open(HEIKEN_SCAN_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        rows = []
        for row in data.get("aligned_pairs", []):
            pair = row.get("pair")
            direction = row.get("direction")
            chg = row.get("chg_cc_d1", 0)
            if pair and direction in ("BULL", "BEAR"):
                rows.append((pair, direction, abs(chg)))
        rows.sort(key=lambda x: x[2], reverse=True)
        signals = {}
        for pair, direction, _ in rows[:top_n]:
            signals[pair] = direction
        return signals
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Core tracking logic
# ---------------------------------------------------------------------------

def process_strategy(
    strategy_name: str,
    current_signals: dict[str, str],
    state: dict,
) -> dict:
    """Compare current signals with open positions, open/close as needed."""
    strat_state = state.get(strategy_name, {})
    open_positions: dict = dict(strat_state.get("open_positions", {}))
    closed_history: list = list(strat_state.get("closed_history", []))
    total_realized: float = float(strat_state.get("total_realized_pnl", 0.0))

    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Collect all pairs we need prices for
    all_pairs = set(open_positions.keys()) | set(current_signals.keys())
    prices: dict[str, float] = {}
    for pair in all_pairs:
        price = _fetch_current_price(pair)
        if price is not None:
            prices[pair] = price

    new_closed: list[dict] = []

    # 1) Close positions whose pair is no longer in signals or direction changed
    for pair in list(open_positions.keys()):
        pos = open_positions[pair]
        should_close = pair not in current_signals or current_signals.get(pair) != pos["direction"]
        if not should_close:
            continue

        raw_exit = prices.get(pair, pos["entry_price"])
        half_spread = spread_cost_price(pair) / 2
        if pos["direction"] == "BULL":
            exit_price = raw_exit - half_spread   # sell at bid
        else:
            exit_price = raw_exit + half_spread   # buy back at ask
        pips = calc_pips(pair, pos["direction"], pos["entry_price"], exit_price)
        pnl = calc_pnl_usd(pos["direction"], pos["entry_price"], exit_price, CAPITAL_PER_PAIR)

        closed_entry = {
            "pair": pair,
            "direction": pos["direction"],
            "entry_price": pos["entry_price"],
            "entry_at": pos["entry_at"],
            "exit_price": exit_price,
            "exit_at": now_utc,
            "pips": round(pips, 1),
            "pnl_usd": round(pnl, 2),
        }
        closed_history.append(closed_entry)
        new_closed.append(closed_entry)
        total_realized += pnl
        del open_positions[pair]

    # 2) Open new positions
    new_opened: list[str] = []
    for pair, direction in current_signals.items():
        if pair in open_positions:
            continue
        raw_price = prices.get(pair)
        if raw_price is None:
            continue
        half_spread = spread_cost_price(pair) / 2
        if direction == "BULL":
            entry_price = raw_price + half_spread   # buy at ask
        else:
            entry_price = raw_price - half_spread   # sell at bid
        open_positions[pair] = {
            "pair": pair,
            "direction": direction,
            "entry_price": entry_price,
            "entry_at": now_utc,
        }
        new_opened.append(pair)

    # 3) Compute latent P&L for open positions
    open_with_pnl: list[dict] = []
    for pair, pos in open_positions.items():
        raw_price = prices.get(pair, pos["entry_price"])
        half_spread = spread_cost_price(pair) / 2
        if pos["direction"] == "BULL":
            mark_price = raw_price - half_spread   # would sell at bid
        else:
            mark_price = raw_price + half_spread   # would buy back at ask
        pips = calc_pips(pair, pos["direction"], pos["entry_price"], mark_price)
        pnl = calc_pnl_usd(pos["direction"], pos["entry_price"], mark_price, CAPITAL_PER_PAIR)
        entry_dt = datetime.fromisoformat(pos["entry_at"].replace("Z", "+00:00"))
        days = (datetime.now(timezone.utc) - entry_dt).days
        open_with_pnl.append({
            "pair": pair,
            "direction": pos["direction"],
            "pips": round(pips, 1),
            "pnl_usd": round(pnl, 2),
            "days": days,
        })

    open_with_pnl.sort(key=lambda x: x["pnl_usd"], reverse=True)

    updated_strat_state = {
        "open_positions": open_positions,
        "closed_history": closed_history,
        "total_realized_pnl": round(total_realized, 2),
    }

    return {
        "state": updated_strat_state,
        "new_closed": new_closed,
        "new_opened": new_opened,
        "open_with_pnl": open_with_pnl,
        "total_realized": round(total_realized, 2),
        "total_latent": round(sum(x["pnl_usd"] for x in open_with_pnl), 2),
    }


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def build_telegram_message(results: dict[str, dict]) -> str:
    lines = ["📊 STRATEGY TRACKER"]

    grand_realized = 0.0
    grand_latent = 0.0

    for strategy_name, result in results.items():
        lines.extend(["", f"━━ {strategy_name} ━━"])

        # Closed this run
        if result["new_closed"]:
            lines.append("CLOSED")
            for c in result["new_closed"]:
                icon = "✅" if c["pnl_usd"] >= 0 else "❌"
                dir_icon = "🟢" if c["direction"] == "BULL" else "🔴"
                entry_dt = datetime.fromisoformat(c["entry_at"].replace("Z", "+00:00"))
                days = (datetime.now(timezone.utc) - entry_dt).days
                lines.append(
                    f"{icon} {c['pair']} {dir_icon} {c['pips']:+.0f} pips ({c['pnl_usd']:+.2f}$)"
                )

        # Open positions
        open_pnl = result["open_with_pnl"]
        if open_pnl:
            lines.append(f"OPEN ({len(open_pnl)})")
            for o in open_pnl:
                dir_icon = "🟢" if o["direction"] == "BULL" else "🔴"
                new_tag = " 🆕" if o["pair"] in result["new_opened"] else ""
                lines.append(
                    f"{dir_icon} {o['pair']} {o['pips']:+.0f} pips ({o['pnl_usd']:+.2f}$){new_tag}"
                )
        else:
            lines.append("OPEN (0)")

        real = result["total_realized"]
        lat = result["total_latent"]
        equity = STARTING_CAPITAL + real + lat
        pct = (equity - STARTING_CAPITAL) / STARTING_CAPITAL * 100
        lines.append(f"Réalisé: {real:+.2f}$ | Latent: {lat:+.2f}$")
        lines.append(f"💼 Capital: {equity:,.2f}$ ({pct:+.2f}%)")

        grand_realized += real
        grand_latent += lat

    grand_equity = 2 * STARTING_CAPITAL + grand_realized + grand_latent
    grand_pct = (grand_equity - 2 * STARTING_CAPITAL) / (2 * STARTING_CAPITAL) * 100
    lines.extend([
        "",
        f"💰 TOTAL Réalisé: {grand_realized:+.2f}$",
        f"💰 TOTAL Latent: {grand_latent:+.2f}$",
        f"💼 TOTAL Capital: {grand_equity:,.2f}$ / 20,000.00$ ({grand_pct:+.2f}%)",
        f"⏰ {datetime.now(PARIS_TZ).strftime('%Y-%m-%d %H:%M Paris')}",
    ])
    return "\n".join(lines)


def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram: credentials missing, skip send.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(
            url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10
        )
        data = response.json()
        ok = bool(data.get("ok", False))
        print(f"Telegram: {'sent' if ok else 'failed'}")
        return ok
    except Exception as exc:
        print(f"Telegram: send failed ({exc})")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    t0 = time.time()

    conso_signals = read_consolidation_signals()
    heiken_signals = read_heiken_signals()

    print(f"CONSOLIDATION signals: {len(conso_signals)} pairs")
    for pair, direction in sorted(conso_signals.items()):
        print(f"  {pair:<8} {direction}")

    print(f"HEIKEN_ICHI_V2 signals: {len(heiken_signals)} pairs")
    for pair, direction in sorted(heiken_signals.items()):
        print(f"  {pair:<8} {direction}")

    state = load_state()

    conso_result = process_strategy("CONSOLIDATION", conso_signals, state)
    heiken_result = process_strategy("HEIKEN_ICHI_V2", heiken_signals, state)

    state["CONSOLIDATION"] = conso_result["state"]
    state["HEIKEN_ICHI_V2"] = heiken_result["state"]
    state["updated_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    save_state(state)

    results = {"CONSOLIDATION": conso_result, "HEIKEN_ICHI_V2": heiken_result}
    for name, r in results.items():
        print(f"\n{name}:")
        print(f"  Opened: {len(r['new_opened'])}, Closed: {len(r['new_closed'])}")
        print(f"  Open positions: {len(r['open_with_pnl'])}")
        print(f"  Realized: {r['total_realized']:+.2f}$, Latent: {r['total_latent']:+.2f}$")

    msg = build_telegram_message(results)
    send_telegram(msg)

    print(f"\nElapsed: {time.time() - t0:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
