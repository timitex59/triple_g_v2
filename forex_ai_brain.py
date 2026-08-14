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
SOURCE_FILES = {
    "full_alignment": "full_alignment_index.json",
    "vivier_state": "renko_score_29pairs_vivier_state.json",
    "vivier_pips": "renko_vivier_pips.json",
    "fibo_pips": "renko_fibo_50_pips.json",
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
            "suggestions": [], "reports_sent": []}


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
        "mwd_raw_alignment": strict, "vivier_pool": vivier_pairs,
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


def _query_claude(evidence_json: str, api_key: str, timeout: float = 20.0) -> dict | None:
    models = [x.strip() for x in os.getenv("FOREX_BRAIN_CLAUDE_MODELS",
              "claude-3-7-sonnet-latest,claude-3-5-sonnet-latest").split(",") if x.strip()]
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
    models = [x.strip() for x in os.getenv("FOREX_BRAIN_OPENAI_MODELS", "gpt-4o,gpt-4o-mini").split(",") if x.strip()]
    for model in models:
        try:
            body = _post_json("https://api.openai.com/v1/chat/completions",
                {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                {"model": model, "temperature": 0, "max_tokens": 900,
                 "response_format": {"type": "json_object"},
                 "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                              {"role": "user", "content": evidence_json}]}, timeout)
            if parsed := _extract_json(body["choices"][0]["message"]["content"]):
                parsed["reviewer"] = "openai"
                return parsed
        except Exception as exc:
            print(f"Warning: revue OpenAI {model} indisponible: {type(exc).__name__}")
    return None


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
