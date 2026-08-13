"""Persistent live-price arrows for Forex Telegram reports."""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")


def load_state(path: str | Path) -> dict:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_state(path: str | Path, state: dict) -> None:
    target = Path(path)
    tmp = target.with_name(f"{target.name}.tmp.{os.getpid()}")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                   encoding="utf-8")
    os.replace(tmp, target)


def _arrow(current: float, reference: object) -> str:
    if not isinstance(reference, (int, float)):
        return "→"
    tolerance = max(abs(current), abs(float(reference)), 1.0) * 1e-10
    if current - float(reference) > tolerance:
        return "↑"
    if current - float(reference) < -tolerance:
        return "↓"
    return "→"


def update(previous: dict | None, prices: dict[str, float], now: datetime
           ) -> tuple[dict, dict[str, dict]]:
    clock = now.astimezone(PARIS)
    today = clock.date().isoformat()
    old = previous if isinstance(previous, dict) else {}
    old_pairs = old.get("pairs", {}) if old.get("date") == today else {}
    trends, pairs = {}, {}
    for pair, raw_price in prices.items():
        if not isinstance(raw_price, (int, float)) or raw_price <= 0:
            continue
        price = float(raw_price)
        prior = old_pairs.get(pair) or {}
        baseline = prior.get("baseline_07h")
        ready = bool(prior.get("baseline_ready"))
        if clock.hour >= 7 and not ready:
            baseline, ready = price, True
        trends[pair] = {"price": price, "vs_07h": _arrow(price, baseline),
                        "vs_previous": _arrow(price, prior.get("previous"))}
        pairs[pair] = {"baseline_07h": baseline, "baseline_ready": ready,
                       "previous": price}
    return {"version": 1, "date": today, "updated_at_paris": clock.isoformat(),
            "pairs": pairs}, trends


def format_price(pair: str, price: float) -> str:
    if pair == "XAUUSD":
        return f"{price:.2f}"
    if pair.endswith("JPY"):
        return f"{price:.3f}"
    return f"{price:.5f}"


def suffix(pair: str, trends: dict[str, dict] | None,
           fallback_price: object = None) -> str:
    trend = (trends or {}).get(pair) or {}
    price = trend.get("price", fallback_price)
    if not isinstance(price, (int, float)):
        return ""
    return (f" ({format_price(pair, float(price))}) "
            f"{trend.get('vs_07h', '→')}{trend.get('vs_previous', '→')}")
