#!/usr/bin/env python3
"""Assistant de recherche Forex indépendant, factuel et auditable.

Il ne modifie aucune stratégie. Il collecte les états existants, consolide les
trades clôturés, calcule les statistiques puis soumet éventuellement ces seules
preuves à deux modèles indépendants. Toute suggestion reste une hypothèse.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

PARIS = ZoneInfo("Europe/Paris")
KNOWLEDGE_BASE_PATH = "forex_ai_knowledge_base.json"
KB_VERSION = 2
MAX_OBSERVATIONS = 1000
MAX_TRADES = 5000
MAX_DAILY_REPORTS = 400
MIN_SAMPLE_FOR_SUGGESTION = 20
FLASH_CLAUDE_MODELS = "claude-sonnet-4-20250514,claude-3-7-sonnet-latest"
EOD_CLAUDE_MODELS = "claude-opus-4-1-20250805,claude-sonnet-4-20250514"
FLASH_OPENAI_MODELS = "gpt-5.1,gpt-5,gpt-4o"
EOD_OPENAI_MODELS = "gpt-5-pro,gpt-5.1,gpt-5"
SOURCE_FILES = {
    "full_alignment": "full_alignment_index.json",
    "vivier_state": "renko_score_29pairs_vivier_state.json",
    "vivier_pips": "renko_vivier_pips.json",
    "fibo_pips": "renko_fibo_50_pips.json",
    "fibo_snapshot": "renko_fibo_50_snapshot.json",
    "fios": "fios_index_chg_state.json",
}


def _read_json(path: str | Path) -> dict:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except Exception as exc:
        print(f"Warning: lecture impossible de {path}: {exc}")
        return {}


def _empty_kb() -> dict:
    return {"version": KB_VERSION, "total_runs_analyzed": 0, "last_updated": None,
            "observations": [], "trade_ledger": [], "daily_reports": [],
            "suggestions": [], "reports_sent": [], "flash_history": [],
            "last_flash_hash": None}


def load_knowledge_base(filepath: str = KNOWLEDGE_BASE_PATH) -> dict:
    """Charge la mémoire et migre sans perte l'ancien format v1."""
    raw = _read_json(filepath) if Path(filepath).exists() else {}
    kb = _empty_kb()
    if not raw:
        return kb
    kb["total_runs_analyzed"] = int(raw.get("total_runs_analyzed") or 0)
    kb["last_updated"] = raw.get("last_updated")
    kb["observations"] = list(raw.get("observations") or raw.get("history") or [])
    kb["trade_ledger"] = list(raw.get("trade_ledger") or [])
    kb["daily_reports"] = list(raw.get("daily_reports") or raw.get("daily_summaries") or [])
    kb["suggestions"] = list(raw.get("suggestions") or raw.get("script_improvements") or [])
    kb["reports_sent"] = list(raw.get("reports_sent") or [])
    kb["flash_history"] = list(raw.get("flash_history") or [])
    kb["last_flash_hash"] = raw.get("last_flash_hash")
    return kb


def save_knowledge_base(data: dict, filepath: str = KNOWLEDGE_BASE_PATH) -> None:
    data["version"] = KB_VERSION
    data["last_updated"] = datetime.now(PARIS).isoformat()
    path = Path(filepath)
    tmp = path.with_suffix(f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def collect_market_snapshot(now: datetime | None = None,
                            source_files: dict[str, str] | None = None) -> dict:
    """Capture détaillée des sorties existantes, sans recalculer les stratégies."""
    now = (now or datetime.now(PARIS)).astimezone(PARIS)
    paths = source_files or SOURCE_FILES
    full = _read_json(paths["full_alignment"])
    vivier = _read_json(paths["vivier_state"])
    fibo = _read_json(paths["fibo_pips"])
    fibo_snapshot_path = paths.get("fibo_snapshot", "renko_fibo_50_snapshot.json")
    fibo_snapshot = _read_json(fibo_snapshot_path) if Path(fibo_snapshot_path).exists() else {}
    fios = _read_json(paths["fios"])
    pairs = full.get("pairs") or {}
    currencies = full.get("currencies") or {}
    strict = {pair: {"direction": int(data.get("full_alignment") or 0),
                     "price": data.get("live_price"), "daily_chg": data.get("daily_chg"),
                     "px_m": data.get("px_m"), "px_w": data.get("px_w"), "px_d": data.get("px_d")}
              for pair, data in pairs.items()
              if int(data.get("full_alignment") or 0) in (-1, 1)}
    vivier_pairs = {pair: {"direction": data.get("direction"),
                           "daily_chg": data.get("daily_chg"),
                           "daily_sar_dir": data.get("daily_sar_dir"),
                           "base_pct": data.get("base_pct"),
                           "fib_position": data.get("fib_position")}
                    for pair, data in (vivier.get("pairs") or {}).items()}
    fios_sections = {pair: {"section": data.get("section"), "direction": data.get("dir")}
                     for pair, data in (fios.get("sections") or {}).items()}
    prices = {pair: data.get("level") for pair, data in (fios.get("pairs") or {}).items()
              if isinstance(data.get("level"), (int, float))}
    if not prices:
        prices = {pair: data.get("live_price") for pair, data in pairs.items()
                  if isinstance(data.get("live_price"), (int, float))}
    return {
        "timestamp": now.isoformat(), "date": now.date().isoformat(),
        "sources_generated_at": {"full_alignment": full.get("generated_at"),
                                 "vivier": vivier.get("updated_at_paris"),
                                 "fios": fios.get("date")},
        "currencies": {cur: {"daily_chg": data.get("daily_chg"),
                             "direction": data.get("full_alignment")}
                       for cur, data in currencies.items()},
        # Le sidecar est ecrit avant les filtres CHG/index du message Telegram.
        "mwd_raw_alignment": strict,
        "full_selected": dict(full.get("selected_pairs") or {}),
        "vivier_pool": vivier_pairs,
        "fibo_sections": dict(fibo_snapshot.get("sections") or {}),
        "fibo_open": {pair: {"direction": data.get("direction"),
                             "entry_price": data.get("entry_price"),
                             "open_pips": data.get("open_pips")}
                      for pair, data in (fibo.get("open") or {}).items()},
        "fios_sections": fios_sections, "prices": prices,
    }


def _trade_id(strategy: str, item: dict) -> str:
    identity = "|".join(str(item.get(key) or "") for key in (
        "pair", "section", "direction", "dir", "start_time_utc", "start_time_paris",
        "entry_time", "entry_date", "end_time_utc", "end_time_paris", "exit_time",
        "exit_date", "start_price", "entry_price", "end_price", "exit_price", "pips"))
    return f"{strategy}:{hashlib.sha256(identity.encode()).hexdigest()[:20]}"


def _normalise_trade(strategy: str, item: dict) -> dict:
    result = {
        "strategy": strategy, "pair": item.get("pair"), "section": item.get("section"),
        "direction": item.get("direction", item.get("dir")),
        "entry_time": item.get("start_time_paris") or item.get("entry_time") or item.get("entry_date"),
        "exit_time": item.get("end_time_paris") or item.get("exit_time") or item.get("exit_date"),
        "entry_price": item.get("start_price", item.get("entry_price")),
        "exit_price": item.get("end_price", item.get("exit_price")),
        "pips": round(float(item.get("pips") or 0.0), 4),
        "close_reason": item.get("close_reason", item.get("reason")),
    }
    result["id"] = _trade_id(strategy, item)
    return result


def collect_closed_trades(source_files: dict[str, str] | None = None) -> list[dict]:
    """Importe uniquement les résultats effectivement clôturés par les trackers."""
    paths = source_files or SOURCE_FILES
    trades = []
    vivier = _read_json(paths["vivier_pips"])
    for day in (vivier.get("days") or {}).values():
        trades.extend(_normalise_trade("VIVIER_CONFIRMED", item)
                      for item in day.get("confirmed_segments") or [])
    fibo = _read_json(paths["fibo_pips"])
    trades.extend(_normalise_trade("RENKO_FIBO_50", item) for item in fibo.get("closed") or [])
    fios = _read_json(paths["fios"])
    for day in ((fios.get("tracking") or {}).get("days") or {}).values():
        for item in day.get("segments") or []:
            strategy = f"FIOS_{str(item.get('section') or 'unknown').upper()}"
            if item.get("pair") == "XAUUSD":
                strategy += "_XAUUSD"
            trades.append(_normalise_trade(strategy, item))
    return trades


def record_run_observation(kb: dict, snapshot: dict,
                           closed_trades: list[dict] | None = None) -> dict:
    observations = kb.setdefault("observations", [])
    if not observations or observations[-1].get("timestamp") != snapshot.get("timestamp"):
        observations.append(snapshot)
        kb["total_runs_analyzed"] = int(kb.get("total_runs_analyzed") or 0) + 1
    kb["observations"] = observations[-MAX_OBSERVATIONS:]
    ledger = {item.get("id"): item for item in kb.setdefault("trade_ledger", []) if item.get("id")}
    for item in closed_trades or []:
        if item.get("id"):
            ledger[item["id"]] = item
    kb["trade_ledger"] = sorted(ledger.values(), key=lambda x: str(x.get("exit_time") or ""))[-MAX_TRADES:]
    return snapshot


def _max_drawdown(values: list[float]) -> float:
    equity = peak = drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def calculate_trade_stats(trades: list[dict]) -> dict:
    values = [float(item.get("pips") or 0.0) for item in trades]
    wins = [v for v in values if v > 0.05]
    losses = [v for v in values if v < -0.05]
    gross_win, gross_loss = sum(wins), abs(sum(losses))
    decided, net = len(wins) + len(losses), sum(values)
    avg_win = gross_win / len(wins) if wins else 0.0
    avg_loss = gross_loss / len(losses) if losses else 0.0
    return {"trades": len(values), "wins": len(wins), "losses": len(losses),
            "flat": len(values) - decided, "net_pips": round(net, 1),
            "gross_win_pips": round(gross_win, 1), "gross_loss_pips": round(-gross_loss, 1),
            "win_rate_pct": round(len(wins) / decided * 100.0, 1) if decided else 0.0,
            "profit_factor": round(gross_win / gross_loss, 2) if gross_loss else None,
            "gat": round(avg_win / avg_loss, 2) if avg_loss else None,
            "expectancy_pips": round(net / len(values), 2) if values else 0.0,
            "max_drawdown_pips": round(_max_drawdown(values), 1),
            "sample_sufficient": len(values) >= MIN_SAMPLE_FOR_SUGGESTION}


def _summarize_window(observations: list[dict]) -> dict:
    if not observations:
        return {"runs": 0, "first_timestamp": None, "last_timestamp": None,
                "selection_frequency": {}, "price_changes": {}, "currency_ranges": {}}
    frequencies = {}
    currency_values = {}
    for obs in observations:
        for section in ("mwd_raw_alignment", "vivier_pool", "fibo_open", "fios_sections"):
            for pair in (obs.get(section) or {}):
                key = f"{section}:{pair}"
                frequencies[key] = frequencies.get(key, 0) + 1
        for currency, data in (obs.get("currencies") or {}).items():
            value = data.get("daily_chg")
            if isinstance(value, (int, float)):
                currency_values.setdefault(currency, []).append(float(value))
    first_prices = observations[0].get("prices") or {}
    last_prices = observations[-1].get("prices") or {}
    price_changes = {}
    for pair in sorted(set(first_prices) & set(last_prices)):
        first, last = first_prices[pair], last_prices[pair]
        if isinstance(first, (int, float)) and isinstance(last, (int, float)) and first:
            price_changes[pair] = {"first": first, "last": last,
                                   "change_pct": round((last - first) / first * 100.0, 4)}
    return {
        "runs": len(observations),
        "first_timestamp": observations[0].get("timestamp"),
        "last_timestamp": observations[-1].get("timestamp"),
        "selection_frequency": dict(sorted(frequencies.items())),
        "price_changes": price_changes,
        "currency_ranges": {cur: {"min_chg": round(min(values), 4),
                                  "max_chg": round(max(values), 4),
                                  "last_chg": round(values[-1], 4)}
                            for cur, values in sorted(currency_values.items())},
        "latest_snapshot": observations[-1],
    }


def build_evidence(kb: dict, now: datetime | None = None) -> dict:
    now = (now or datetime.now(PARIS)).astimezone(PARIS)
    cutoff, recent = now - timedelta(hours=24), []
    for item in kb.get("observations") or []:
        try:
            if datetime.fromisoformat(str(item.get("timestamp"))).astimezone(PARIS) >= cutoff:
                recent.append(item)
        except Exception:
            continue
    grouped = {}
    for trade in kb.get("trade_ledger") or []:
        grouped.setdefault(str(trade.get("strategy") or "UNKNOWN"), []).append(trade)
    return {"generated_at": now.isoformat(),
            "window_24h": _summarize_window(recent),
            "statistics": {name: calculate_trade_stats(items) for name, items in sorted(grouped.items())},
            "methodology": {"minimum_sample": MIN_SAMPLE_FOR_SUGGESTION,
                            "warning": "Les pips XAUUSD ne sont pas directement comparables aux pips Forex.",
                            "ai_role": "Formuler des hypothèses; jamais modifier automatiquement une stratégie."}}


def deterministic_findings(evidence: dict) -> list[str]:
    findings = []
    for strategy, stats in (evidence.get("statistics") or {}).items():
        if not stats.get("trades"):
            continue
        pf = stats.get("profit_factor")
        findings.append(f"{strategy}: {stats['trades']} trades, net {stats['net_pips']:+.1f} pips, "
                        f"NOG {stats['win_rate_pct']:.1f}%, PF {pf if pf is not None else 'N/D'}, "
                        f"DD -{stats['max_drawdown_pips']:.1f}.")
    return findings or ["Aucun trade clôturé exploitable dans les trackers."]


def _signal_direction(value) -> int:
    if value in (1, "BULL", "LONG", "BUY"):
        return 1
    if value in (-1, "BEAR", "SHORT", "SELL"):
        return -1
    return 0


def snapshot_signals(snapshot: dict) -> list[dict]:
    """Normalise les décisions courantes de tous les screeners."""
    signals = []
    weights = {"FULL": 3.0, "FIBO_FULL": 3.0, "FIBO_DAILY": 1.0,
               "FIBO_MW": 1.0, "VIVIER_CONFIRMED": 2.0,
               "VIVIER_POOL": 0.5, "FIOS_CONV": 2.0, "FIOS_TOP": 2.0}

    def add(source, pair, direction):
        direction = _signal_direction(direction)
        if pair and direction:
            signals.append({"source": source, "pair": pair, "direction": direction,
                            "weight": weights[source]})

    for pair, data in (snapshot.get("full_selected") or {}).items():
        add("FULL", pair, data.get("direction"))
    for pair, data in (snapshot.get("vivier_pool") or {}).items():
        direction = _signal_direction(data.get("direction"))
        confirmed = (direction and data.get("daily_sar_dir") == direction
                     and isinstance(data.get("daily_chg"), (int, float))
                     and float(data["daily_chg"]) * direction >= 0.05)
        add("VIVIER_CONFIRMED" if confirmed else "VIVIER_POOL", pair, direction)
    for section, source in (("full_alignment", "FIBO_FULL"),
                            ("daily", "FIBO_DAILY"), ("mw_bias", "FIBO_MW")):
        for data in (snapshot.get("fibo_sections") or {}).get(section) or []:
            add(source, data.get("pair"), data.get("direction"))
    for pair, data in (snapshot.get("fios_sections") or {}).items():
        add("FIOS_CONV" if data.get("section") == "conv" else "FIOS_TOP",
            pair, data.get("direction"))
    return signals


def build_flash_features(kb: dict, max_runs: int = 8) -> dict:
    """Régime, persistance, contradictions et meilleure expression du run."""
    observations = list(kb.get("observations") or [])
    if not observations:
        return {"status": "NO_DATA", "candidates": []}
    today = observations[-1].get("date")
    recent = [obs for obs in observations if obs.get("date") == today][-max_runs:]
    current = snapshot_signals(recent[-1])
    history_counts = {}
    for obs in recent:
        seen = set()
        for signal in snapshot_signals(obs):
            key = (signal["source"], signal["pair"], signal["direction"])
            if key not in seen:
                history_counts[key] = history_counts.get(key, 0) + 1
                seen.add(key)
    currency_scores, currency_crosses = {}, {}
    pair_sources = {}
    for signal in current:
        pair, direction = signal["pair"], signal["direction"]
        persistence = history_counts.get((signal["source"], pair, direction), 1)
        pair_sources.setdefault(pair, []).append({**signal, "persistence_runs": persistence})
        if pair == "XAUUSD" or len(pair) != 6:
            continue
        base, quote = pair[:3], pair[3:]
        contribution = signal["weight"] * direction
        currency_scores[base] = currency_scores.get(base, 0.0) + contribution
        currency_scores[quote] = currency_scores.get(quote, 0.0) - contribution
        currency_crosses.setdefault(base, set()).add(pair)
        currency_crosses.setdefault(quote, set()).add(pair)
    ranked = sorted(currency_scores, key=lambda cur: currency_scores[cur], reverse=True)
    strongest = ranked[0] if ranked else None
    weakest = ranked[-1] if ranked else None
    candidates = []
    for pair, items in pair_sources.items():
        directions = {item["direction"] for item in items}
        contradiction = len(directions) > 1
        for direction in directions:
            aligned = [item for item in items if item["direction"] == direction]
            score = sum(item["weight"] for item in aligned)
            persistence = max(item["persistence_runs"] for item in aligned)
            score += min(persistence, 4) * 0.5
            if contradiction:
                score -= 3.0
            if strongest and weakest and len(pair) == 6:
                if (direction == 1 and pair[:3] == strongest and pair[3:] == weakest) or (
                        direction == -1 and pair[3:] == strongest and pair[:3] == weakest):
                    score += 3.0
            candidates.append({"pair": pair, "direction": "LONG" if direction == 1 else "SHORT",
                               "score": round(score, 2), "sources": sorted(x["source"] for x in aligned),
                               "persistence_runs": persistence, "contradiction": contradiction})
    candidates.sort(key=lambda item: (item["contradiction"], -item["score"], item["pair"]))
    contradictions = [item["pair"] for item in candidates if item["contradiction"]]
    return {
        "status": "OK", "runs_observed": len(recent),
        "strongest_currency": strongest, "weakest_currency": weakest,
        "currency_scores": {cur: round(score, 2) for cur, score in currency_scores.items()},
        "currency_cross_confirmations": {cur: len(pairs) for cur, pairs in currency_crosses.items()},
        "candidates": candidates[:8], "contradictions": sorted(set(contradictions)),
        "gold": [item for item in candidates if item["pair"] == "XAUUSD"][:1],
    }


FLASH_PROMPT = """Tu analyses un snapshot multi-screener Forex déjà calculé. Choisis uniquement parmi les valeurs fournies.
Réponds en JSON strict: strongest_currency, weakest_currency, best_pair, direction, risk, confidence.
best_pair et direction doivent recopier un candidat non contradictoire. Cherche: force/faiblesse multi-cross,
confluence entre moteurs, persistance multi-run, puis absence de contradiction. N'invente aucune paire ni donnée."""


def _query_flash(provider: str, features: dict, api_key: str, timeout: float = 12.0) -> dict | None:
    prompt = json.dumps(features, ensure_ascii=False, sort_keys=True)
    if provider == "claude":
        models = [x.strip() for x in os.getenv(
            "FOREX_BRAIN_FLASH_CLAUDE_MODELS",
            os.getenv("FOREX_BRAIN_CLAUDE_MODELS", FLASH_CLAUDE_MODELS),
        ).split(",") if x.strip()]
        for model in models:
            try:
                body = _post_json("https://api.anthropic.com/v1/messages",
                    {"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    {"model": model, "max_tokens": 250, "temperature": 0, "system": FLASH_PROMPT,
                     "messages": [{"role": "user", "content": prompt}]}, timeout)
                if result := _extract_json(body["content"][0]["text"]):
                    result["reviewer"] = "claude"
                    return result
            except Exception:
                continue
    else:
        models = [x.strip() for x in os.getenv(
            "FOREX_BRAIN_FLASH_OPENAI_MODELS",
            os.getenv("FOREX_BRAIN_OPENAI_MODELS", FLASH_OPENAI_MODELS),
        ).split(",") if x.strip()]
        if result := _query_openai_responses(prompt, FLASH_PROMPT, models, api_key,
                                             max_tokens=800, reasoning="medium", timeout=timeout):
            result["reviewer"] = "openai"
            return result
    return None


def run_flash_reviews(features: dict) -> list[dict]:
    futures = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        if key := os.getenv("ANTHROPIC_API_KEY", "").strip():
            futures.append(pool.submit(_query_flash, "claude", features, key))
        if key := os.getenv("OPENAI_API_KEY", "").strip():
            futures.append(pool.submit(_query_flash, "openai", features, key))
        reviews = []
        for future in as_completed(futures):
            try:
                if result := future.result():
                    reviews.append(result)
            except Exception:
                pass
    return reviews


def format_flash_consensus(features: dict, reviews: list[dict]) -> str | None:
    if features.get("status") != "OK" or not features.get("candidates"):
        return None
    allowed = {(item["pair"], item["direction"]) for item in features["candidates"]
               if not item.get("contradiction")}
    known_currencies = set((features.get("currency_scores") or {}).keys())
    valid = [review for review in reviews
             if (review.get("best_pair"), review.get("direction")) in allowed
             and review.get("strongest_currency") in known_currencies
             and review.get("weakest_currency") in known_currencies]
    shared_pair = None
    if len(valid) == 2 and valid[0].get("best_pair") == valid[1].get("best_pair") \
            and valid[0].get("direction") == valid[1].get("direction"):
        shared_pair = (valid[0]["best_pair"], valid[0]["direction"])
    shared_regime = (len(valid) == 2
                     and valid[0].get("strongest_currency") == valid[1].get("strongest_currency")
                     and valid[0].get("weakest_currency") == valid[1].get("weakest_currency"))
    if shared_pair or shared_regime:
        header = "🤝 CONSENSUS IA (Claude x GPT-5)"
    elif valid:
        header = "🧠 SYNTHÈSE IA MULTI-SCREENER"
    else:
        header = "📊 SYNTHÈSE MULTI-SCREENER"
    strong = (valid[0].get("strongest_currency") if shared_regime else features.get("strongest_currency"))
    weak = (valid[0].get("weakest_currency") if shared_regime else features.get("weakest_currency"))
    crosses = (features.get("currency_cross_confirmations") or {}).get(strong, 0)
    line1 = f"🔥 {strong} fort · {weak} faible · {crosses} cross" if strong and weak else "⚡ Régime devises diffus"
    choice = shared_pair or ((valid[0].get("best_pair"), valid[0].get("direction")) if valid else None)
    if not choice:
        top = next((item for item in features["candidates"] if not item.get("contradiction")), None)
        choice = (top["pair"], top["direction"]) if top else None
    if choice:
        candidate = next(item for item in features["candidates"]
                         if item["pair"] == choice[0] and item["direction"] == choice[1])
        run_count = candidate["persistence_runs"]
        source_count = len(candidate["sources"])
        line2 = (f"🎯 {choice[0]} {choice[1]} · {run_count} run{'s' if run_count > 1 else ''} · "
                 f"{source_count} moteur{'s' if source_count > 1 else ''}")
    else:
        line2 = "⚠️ Aucun cross assez propre"
    return "\n".join((header, line1, line2))


SYSTEM_PROMPT = """Tu es un auditeur quantitatif Forex prudent. Les statistiques JSON sont la seule source de vérité.
N'invente jamais prix, causalité, corrélation ou résultat. Une suggestion n'est autorisée que si sample_sufficient=true.
Réponds en JSON strict: assessment (string), risks (liste), suggestions (liste d'objets: target_script, hypothesis,
evidence={strategy, trades, metric, observed_value}, expected_effect, risk, validation_plan, confidence entre 0 et 1).
strategy, trades et observed_value doivent recopier exactement une ligne des statistics. Sinon suggestions=[].
Aucune suggestion ne doit être appliquée automatiquement."""


def _extract_json(text: str) -> dict | None:
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.I | re.S)
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else None
    except Exception:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if match:
            try:
                value = json.loads(match.group(0))
                return value if isinstance(value, dict) else None
            except Exception:
                pass
    return None


def _post_json(url: str, headers: dict, payload: dict, timeout: float) -> dict:
    request = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode())


def _responses_text(body: dict) -> str:
    if isinstance(body.get("output_text"), str):
        return body["output_text"]
    chunks = []
    for item in body.get("output") or []:
        for content in item.get("content") or []:
            if content.get("type") == "output_text" and isinstance(content.get("text"), str):
                chunks.append(content["text"])
    return "\n".join(chunks)


def _query_openai_responses(user_prompt: str, instructions: str, models: list[str],
                            api_key: str, max_tokens: int, reasoning: str,
                            timeout: float) -> dict | None:
    """OpenAI frontier models via Responses API; legacy GPT-4o remains fallback."""
    for model in models:
        try:
            if model.startswith("gpt-4"):
                body = _post_json(
                    "https://api.openai.com/v1/chat/completions",
                    {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    {"model": model, "temperature": 0, "max_tokens": max_tokens,
                     "response_format": {"type": "json_object"},
                     "messages": [{"role": "system", "content": instructions},
                                  {"role": "user", "content": user_prompt}]}, timeout)
                text = body["choices"][0]["message"]["content"]
            else:
                effort = "high" if model.startswith("gpt-5-pro") else reasoning
                body = _post_json(
                    "https://api.openai.com/v1/responses",
                    {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    {"model": model, "instructions": instructions, "input": user_prompt,
                     "max_output_tokens": max_tokens, "reasoning": {"effort": effort},
                     "text": {"format": {"type": "json_object"}}, "store": False}, timeout)
                text = _responses_text(body)
            if parsed := _extract_json(text):
                parsed["model"] = model
                return parsed
        except Exception as exc:
            print(f"Warning: revue OpenAI {model} indisponible: {type(exc).__name__}")
    return None


def _query_claude(evidence_json: str, api_key: str, timeout: float = 20.0) -> dict | None:
    models = [x.strip() for x in os.getenv(
        "FOREX_BRAIN_EOD_CLAUDE_MODELS",
        os.getenv("FOREX_BRAIN_CLAUDE_MODELS", EOD_CLAUDE_MODELS),
    ).split(",") if x.strip()]
    for model in models:
        try:
            body = _post_json("https://api.anthropic.com/v1/messages",
                {"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                {"model": model, "max_tokens": 900, "temperature": 0, "system": SYSTEM_PROMPT,
                 "messages": [{"role": "user", "content": evidence_json}]}, timeout)
            if parsed := _extract_json(body["content"][0]["text"]):
                parsed["reviewer"] = "claude"
                return parsed
        except Exception as exc:
            print(f"Warning: revue Claude {model} indisponible: {type(exc).__name__}")
    return None


def _query_openai(evidence_json: str, api_key: str, timeout: float = 20.0) -> dict | None:
    models = [x.strip() for x in os.getenv(
        "FOREX_BRAIN_EOD_OPENAI_MODELS",
        os.getenv("FOREX_BRAIN_OPENAI_MODELS", EOD_OPENAI_MODELS),
    ).split(",") if x.strip()]
    parsed = _query_openai_responses(evidence_json, SYSTEM_PROMPT, models, api_key,
                                     max_tokens=3000, reasoning="high", timeout=timeout)
    if parsed:
        parsed["reviewer"] = "openai"
    return parsed


def run_dual_review(evidence: dict) -> list[dict]:
    payload = json.dumps(evidence, ensure_ascii=False, sort_keys=True)
    futures = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        if key := os.getenv("ANTHROPIC_API_KEY", "").strip():
            futures.append(pool.submit(_query_claude, payload, key))
        if key := os.getenv("OPENAI_API_KEY", "").strip():
            futures.append(pool.submit(_query_openai, payload, key))
        reviews = []
        for future in as_completed(futures):
            try:
                if result := future.result():
                    reviews.append(result)
            except Exception:
                pass
    return sorted(reviews, key=lambda item: str(item.get("reviewer")))


def _validated_suggestions(reviews: list[dict], evidence: dict, date_key: str) -> list[dict]:
    statistics = evidence.get("statistics") or {}
    allowed_metrics = {"net_pips", "win_rate_pct", "profit_factor", "gat",
                       "expectancy_pips", "max_drawdown_pips"}
    output = []
    for review in reviews:
        for index, suggestion in enumerate(review.get("suggestions") or [], 1):
            if not isinstance(suggestion, dict):
                continue
            proof = suggestion.get("evidence")
            if not isinstance(proof, dict):
                continue
            strategy = str(proof.get("strategy") or "")
            stats = statistics.get(strategy) or {}
            metric = str(proof.get("metric") or "")
            if (not stats.get("sample_sufficient")
                    or int(proof.get("trades") or -1) != int(stats.get("trades") or 0)
                    or metric not in allowed_metrics
                    or stats.get(metric) is None):
                continue
            try:
                if abs(float(proof.get("observed_value")) - float(stats[metric])) > 1e-6:
                    continue
            except (TypeError, ValueError):
                continue
            item = dict(suggestion)
            item.update({"id": f"SUG-{date_key}-{review.get('reviewer', 'ai').upper()}-{index:02d}",
                         "date": date_key, "reviewer": review.get("reviewer"), "status": "PROPOSED"})
            output.append(item)
    return output


def format_eod_report(evidence: dict, reviews: list[dict], suggestions: list[dict],
                      now: datetime | None = None) -> str:
    now = (now or datetime.now(PARIS)).astimezone(PARIS)
    lines = ["🧠 FOREX RESEARCH — BILAN FACTUEL", "", "📊 STATISTIQUES VÉRIFIÉES"]
    lines.extend(f"• {line}" for line in deterministic_findings(evidence))
    lines.extend(["", f"🔎 REVUES IA : {len(reviews)}/2 disponibles"])
    if reviews:
        for review in reviews:
            label = "CLAUDE" if review.get("reviewer") == "claude" else "OPENAI"
            lines.append(f"• {label} : {str(review.get('assessment') or 'Analyse reçue')[:220]}")
    else:
        lines.append("• Aucune clé IA disponible : aucun faux consensus généré.")
    lines.extend(["", "💡 HYPOTHÈSES À VALIDER"])
    if suggestions:
        lines.extend(f"• {item['id']} — {str(item.get('hypothesis') or '')[:240]}" for item in suggestions[:5])
    else:
        lines.append(f"• Aucune suggestion IA admissible (preuve nominative et minimum {MIN_SAMPLE_FOR_SUGGESTION} trades requis).")
    lines.extend(["", "⚠️ Aucune modification automatique des stratégies.",
                  f"⏰ {now:%d/%m/%Y %H:%M} Paris"])
    return "\n".join(lines)


def send_telegram_message(text: str) -> bool:
    token, chat_id = os.getenv("TELEGRAM_BOT_TOKEN", "").strip(), os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("Warning: identifiants Telegram absents; rapport non envoyé.")
        return False
    try:
        body = _post_json(f"https://api.telegram.org/bot{token}/sendMessage",
                          {"Content-Type": "application/json"},
                          {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, 15.0)
        return bool(body.get("ok"))
    except Exception as exc:
        print(f"Error: envoi Telegram échoué: {exc}")
        return False


def run_eod_analysis(kb: dict, send_tg: bool = True, now: datetime | None = None) -> str:
    now = (now or datetime.now(PARIS)).astimezone(PARIS)
    date_key, evidence = now.date().isoformat(), build_evidence(kb, now)
    reviews = run_dual_review(evidence)
    suggestions = _validated_suggestions(reviews, evidence, date_key)
    known = {item.get("id") for item in kb.setdefault("suggestions", [])}
    kb["suggestions"].extend(item for item in suggestions if item.get("id") not in known)
    report = format_eod_report(evidence, reviews, suggestions, now)
    archived_evidence = {"generated_at": evidence["generated_at"],
                         "window_runs": evidence["window_24h"]["runs"],
                         "statistics": evidence["statistics"],
                         "methodology": evidence["methodology"]}
    kb.setdefault("daily_reports", []).append({"date": date_key, "generated_at": now.isoformat(),
        "evidence": archived_evidence, "reviewers": [x.get("reviewer") for x in reviews],
        "suggestion_ids": [x.get("id") for x in suggestions], "telegram_sent": False})
    kb["daily_reports"] = kb["daily_reports"][-MAX_DAILY_REPORTS:]
    if send_tg:
        sent = send_telegram_message(report)
        kb["daily_reports"][-1]["telegram_sent"] = sent
        if sent and date_key not in kb.setdefault("reports_sent", []):
            kb["reports_sent"].append(date_key)
            print("✅ Rapport Forex Research envoyé sur Telegram.")
        elif not sent:
            print("⚠️ Rapport généré mais non envoyé.")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Assistant indépendant de recherche Forex")
    parser.add_argument("--eod", action="store_true")
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--knowledge-base", default=KNOWLEDGE_BASE_PATH)
    args = parser.parse_args()
    now, kb = datetime.now(PARIS), load_knowledge_base(args.knowledge_base)
    record_run_observation(kb, collect_market_snapshot(now), collect_closed_trades())
    print(f"🧠 Forex Research: run {kb['total_runs_analyzed']}, {len(kb['trade_ledger'])} trades clôturés consolidés.")
    features = build_flash_features(kb)
    flash_reviews = run_flash_reviews(features)
    flash = format_flash_consensus(features, flash_reviews)
    if flash:
        flash_hash = hashlib.sha256(flash.encode("utf-8")).hexdigest()
        changed = flash_hash != kb.get("last_flash_hash")
        print("\n" + flash)
        sent = False
        if changed and not args.no_telegram:
            sent = send_telegram_message(flash)
        elif not changed:
            print("Consensus inchangé : Telegram ignoré.")
        kb.setdefault("flash_history", []).append({
            "timestamp": now.isoformat(), "message": flash,
            "reviewers": [item.get("reviewer") for item in flash_reviews],
            "telegram_sent": sent, "features": features,
        })
        kb["flash_history"] = kb["flash_history"][-400:]
        if sent:
            kb["last_flash_hash"] = flash_hash
    should_eod = args.eod or now.hour == 23
    already_sent = now.date().isoformat() in (kb.get("reports_sent") or [])
    if should_eod and (args.eod or not already_sent):
        print(run_eod_analysis(kb, send_tg=not args.no_telegram, now=now))
    elif should_eod:
        print("Bilan déjà envoyé aujourd'hui : doublon ignoré.")
    else:
        print("Collecte silencieuse; bilan prévu au run de 23 h Paris.")
    save_knowledge_base(kb, args.knowledge_base)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
