#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Denouement des signaux du journal live + snapshot statistique.

Parcourt fios_journal.json, denoue les entrees encore ouvertes dont l'horizon
est ecoule (via le meme moteur TP/SL en ATR que le backtest), inscrit le
resultat dans l'entree, puis calcule un snapshot de stats ventile par decision,
paire, tranche de confiance, qualite, nb de familles d'accord et combinaison de
familles. C'est ici que le systeme "apprend" quelles combinaisons marchent —
au fil des jours, a mesure que le journal se remplit.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from .. import config as cfg
from . import stats as stats_mod
from .engine import resolve_signal

_TV_BY_NAME = {p["name"]: p for p in cfg.PAIRS}


def _parse_ts(entry: dict) -> datetime | None:
    ts = entry.get("ts")
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def resolve_open_entries(journal: dict, verbose: bool = False) -> int:
    """Denoue les entrees ouvertes murees. Retourne le nb d'entrees denouees."""
    resolved = 0
    for e in journal.get("entries", []):
        res = e.get("result")
        if res and res.get("outcome") in ("WIN", "LOSS"):
            continue  # deja denouee
        pair = _TV_BY_NAME.get(e.get("pair", ""))
        if not pair:
            continue
        direction = 1 if e.get("decision") == "BUY" else -1
        dt = _parse_ts(e)
        if dt is None:
            continue
        out = resolve_signal(pair["tv"], direction, dt)
        if out is None or out.get("outcome") == "OPEN":
            e["result"] = {"outcome": "OPEN"}
            continue
        e["result"] = out
        resolved += 1
        if verbose:
            print(f"  [journal] {e['pair']} {e['decision']} {e['date']} -> "
                  f"{out['outcome']} ({out['exit_reason']}, R={out['r']:+.2f})")
    return resolved


def _trades(journal: dict) -> list[dict]:
    out: list[dict] = []
    for e in journal.get("entries", []):
        res = e.get("result") or {}
        if res.get("outcome") not in ("WIN", "LOSS"):
            continue
        out.append({
            "r": res["r"],
            "outcome": res["outcome"],
            "exit_reason": res.get("exit_reason"),
            "decision": e.get("decision"),
            "pair": e.get("pair"),
            "confidence": e.get("confidence", 0),
            "quality": e.get("quality", 0),
            "agree": e.get("agree", 0),
            "contributions": e.get("contributions", {}),
        })
    return out


def build_stats(journal: dict) -> dict:
    trades = _trades(journal)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "resolved_trades": len(trades),
        "open_signals": sum(
            1 for e in journal.get("entries", [])
            if (e.get("result") or {}).get("outcome") not in ("WIN", "LOSS")
        ),
        "overall": stats_mod.summarize(trades),
        "by_decision": stats_mod.grouped(trades, lambda t: t["decision"] or "?"),
        "by_pair": stats_mod.grouped(trades, lambda t: t["pair"] or "?"),
        "by_confidence": stats_mod.grouped(
            trades, lambda t: stats_mod.confidence_bucket(t["confidence"])),
        "by_quality": stats_mod.grouped(trades, lambda t: f"{t['quality']}★"),
        "by_agree": stats_mod.grouped(trades, lambda t: f"{t['agree']} familles"),
        "by_family_combo": stats_mod.grouped(
            trades, lambda t: stats_mod.family_combo(t["contributions"])),
    }


def load_journal(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "entries" in data:
            return data
    except Exception:
        pass
    return {"schema_version": 1, "entries": [], "updated_at": None}


def save_journal(path: str, journal: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(journal, f, ensure_ascii=False, indent=2)


def save_stats(path: str, stats: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


def resolve_and_snapshot(journal_path: str = cfg.JOURNAL_FILE,
                         stats_path: str = cfg.STATS_FILE,
                         verbose: bool = False) -> dict:
    """Charge le journal, denoue les entrees murees, sauvegarde journal + stats,
    et retourne le snapshot de stats."""
    journal = load_journal(journal_path)
    n = resolve_open_entries(journal, verbose=verbose)
    if n:
        journal["updated_at"] = datetime.now(timezone.utc).isoformat()
        save_journal(journal_path, journal)
    stats = build_stats(journal)
    save_stats(stats_path, stats)
    return stats
