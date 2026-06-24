#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
combo_pipeline.py

Fusionne en UN SEUL message Telegram les resultats de:
  - nasdaq_sector_pipeline.py  -> TOP 3 STOCK (Nasdaq-100) + TOP 3 ETF UCITS
  - middle_pipeline.py         -> TOP 3 MID-CAP TECH (vs IWR)

Il NE recalcule rien: il lit les rapports JSON deja produits par les deux
scripts dans le meme run CI (lus seulement s'ils datent d'aujourd'hui), en
reconstruit les objets et reutilise les classements d'origine -> meme TOP que
les messages separes. Envoi une seule fois par jour (anti-doublon).

Les deux pipelines tournent donc avec --no-telegram (calcul + rapports + sidecar
UCITS pour ETF_V3 inchanges); seul combo envoie le message fusionne.

Produit aussi combo_top_assets.json: registre cumulatif, ordonne et sans
doublon des trois TOP 3. La cle "tickers" contient tout l'univers historique;
"current" contient la selection du jour et "history" conserve un instantane
par date pour comparer ensuite frequence, rangs et force relative.

Usage:
    python combo_pipeline.py                # construit + envoie (si >=20h non requis)
    python combo_pipeline.py --no-telegram  # affichage seul
    python combo_pipeline.py --force        # ignore l'anti-doublon
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import pytz

import middle_pipeline as mid
import nasdaq_sector_pipeline as nsp
from nasdaq_sector_pipeline import (
    AssetMetrics, RenkoConfirmation, format_pct, retained_assets_ranked, send_telegram,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NAS_JSON = nsp.REPORT_JSON
MID_JSON = mid.REPORT_JSON
SEND_STATE = os.path.join(SCRIPT_DIR, "combo_pipeline_send.json")
TOP_ASSETS_JSON = os.path.join(SCRIPT_DIR, "combo_top_assets.json")
PARIS = pytz.timezone("Europe/Paris")


def _load(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _fresh(report: dict, today: str) -> bool:
    """Le rapport est-il du jour ? Compare en DATE PARIS (generated_at est en UTC;
    une simple troncature [:10] casse autour de minuit Paris)."""
    gen = report.get("generated_at", "")
    if not gen:
        return False
    try:
        dt = datetime.fromisoformat(gen)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(PARIS).strftime("%Y-%m-%d") == today
    except Exception:
        return gen[:10] == today


def _stocks(rep: dict) -> list[AssetMetrics]:
    return [AssetMetrics(**m) for m in rep.get("stocks", [])]


def _renko(rep: dict) -> dict:
    return {k: RenkoConfirmation(**v) for k, v in rep.get("renko", {}).items()}


# Noms de secteurs abreges pour la ligne 🔥.
SECTOR_ABBR = {
    "Semiconductors / AI infrastructure": "Semis",
    "Cybersecurity": "Cyber",
    "Cloud / Data software": "Cloud",
    "AI platforms / Mega-cap tech": "IA",
    "Consumer internet / E-commerce": "Internet",
    "Healthcare / Biotech": "Santé",
    "Industrials / Automation": "Indus",
    "Utilities / Power": "Énergie",
    # Sous-themes mid-cap (deja courts; on abrege les plus longs).
    "Distributors": "Distrib",
    "IT / Internet": "IT/Net",
}


def _top_themes(rep: dict, n: int = 2) -> list[str]:
    th = sorted(rep.get("themes", []), key=lambda s: s.get("final_score", 0), reverse=True)
    return [SECTOR_ABBR.get(t["theme"], str(t["theme"]).split(" / ")[0]) for t in th[:n]]


def nasdaq_top_stocks(rep: dict, n: int = 3) -> list[tuple[str, float]]:
    """Meme classement que nasdaq build_telegram_summary: validees Renko d'abord,
    puis repli par force relative."""
    stocks = _stocks(rep)
    etfs = [AssetMetrics(**m) for m in rep.get("etfs", [])]
    renko = _renko(rep)
    ranked = [(r, c, a) for r, c, a in retained_assets_ranked(renko, stocks, etfs)
              if c.asset_type == "STOCK"]
    fallback = sorted([m for m in stocks if m.rel_63d_vs_qqq is not None],
                      key=lambda m: (m.rel_63d_vs_qqq or 0.0, m.score), reverse=True)
    out: list[tuple[str, float]] = []
    used: set[str] = set()
    for _, c, a in ranked:
        if len(out) >= n:
            break
        if a is None or c.ticker in used:
            continue
        out.append((c.ticker, a.rel_63d_vs_qqq)); used.add(c.ticker)
    for a in fallback:
        if len(out) >= n:
            break
        if a.ticker in used:
            continue
        out.append((a.ticker, a.rel_63d_vs_qqq)); used.add(a.ticker)
    return out


def nasdaq_ucits(rep: dict, n: int = 3) -> list[tuple[str, float]]:
    eu = [AssetMetrics(**m) for m in rep.get("europe_etfs", [])]
    ranked = sorted([m for m in eu if m.rel_63d_vs_qqq is not None],
                    key=lambda m: (m.rel_63d_vs_qqq or 0.0, m.score), reverse=True)
    return [(m.ticker, m.rel_63d_vs_qqq) for m in ranked[:n]]


def mid_top_stocks(rep: dict, n: int = 3) -> list[tuple[str, float]]:
    """Meme classement unifie que middle_pipeline (validees Renko d'abord)."""
    ranking = mid.unified_ranking(_stocks(rep), _renko(rep))
    return [(e["ticker"], e["rel"]) for e in ranking[:n]]


def collect_top_assets(nas: dict, midr: dict) -> dict[str, list[tuple[str, float]]]:
    """Retourne exactement les trois classements utilises dans le message."""
    return {
        "large_cap": nasdaq_top_stocks(nas) if nas else [],
        "mid_cap_tech": mid_top_stocks(midr) if midr else [],
        "etf_ucits": nasdaq_ucits(nas) if nas else [],
    }


SECTION_META = {
    "large_cap": {"label": "LARGE CAP", "asset_type": "stock", "benchmark": "QQQ"},
    "mid_cap_tech": {"label": "MID-CAP TECH", "asset_type": "stock", "benchmark": "IWR"},
    "etf_ucits": {"label": "ETF UCITS", "asset_type": "etf", "benchmark": "QQQ"},
}


def _ucits_identifiers(ticker: str) -> dict[str, str]:
    for alternatives in nsp.EUROPE_ETF_ALTERNATIVES.values():
        for item in alternatives:
            if str(item.get("ticker", "")).upper() == ticker.upper():
                return {
                    key: str(item[key])
                    for key in ("yahoo", "tv", "isin")
                    if item.get(key)
                }
    return {}


def build_top_assets_payload(
    selections: dict[str, list[tuple[str, float]]],
    nas: dict,
    midr: dict,
) -> dict:
    """Construit une liste plate, ordonnee et dedupliquee par ticker."""
    assets_by_ticker: dict[str, dict] = {}
    ordered_tickers: list[str] = []
    sections: dict[str, list[dict]] = {}

    for section, rows in selections.items():
        meta = SECTION_META[section]
        section_rows: list[dict] = []
        for rank, (raw_ticker, rel) in enumerate(rows, 1):
            ticker = str(raw_ticker).strip().upper()
            if not ticker:
                continue
            appearance = {
                "section": section,
                "label": meta["label"],
                "asset_type": meta["asset_type"],
                "rank": rank,
                "relative_strength_63d_pct": round(float(rel), 4),
                "benchmark": meta["benchmark"],
            }
            section_rows.append({"ticker": ticker, **appearance})

            if ticker not in assets_by_ticker:
                asset = {
                    "ticker": ticker,
                    "asset_types": [],
                    "sections": [],
                    "appearances": [],
                }
                assets_by_ticker[ticker] = asset
                ordered_tickers.append(ticker)
            asset = assets_by_ticker[ticker]
            if meta["asset_type"] not in asset["asset_types"]:
                asset["asset_types"].append(meta["asset_type"])
            if section == "etf_ucits":
                asset.update(_ucits_identifiers(ticker))
            if section not in asset["sections"]:
                asset["sections"].append(section)
            asset["appearances"].append(appearance)
        sections[section] = section_rows

    now_utc = datetime.now(timezone.utc)
    return {
        "generated_at": now_utc.isoformat(),
        "paris_date": now_utc.astimezone(PARIS).strftime("%Y-%m-%d"),
        "source": "combo_pipeline.py",
        "complete": all(len(selections.get(section, [])) == 3 for section in SECTION_META),
        "count": len(ordered_tickers),
        "tickers": ordered_tickers,
        "assets": [assets_by_ticker[ticker] for ticker in ordered_tickers],
        "sections": sections,
        "source_reports": {
            "nasdaq_generated_at": nas.get("generated_at"),
            "mid_cap_generated_at": midr.get("generated_at"),
        },
    }


def _merge_same_day(previous: dict, latest: dict) -> dict:
    """Fusionne deux executions du meme jour sans perdre une section valide."""
    if not previous:
        return latest

    merged_sections: dict[str, list[dict]] = {}
    for section in SECTION_META:
        new_rows = latest.get("sections", {}).get(section, [])
        old_rows = previous.get("sections", {}).get(section, [])
        merged_sections[section] = new_rows or old_rows

    selections = {
        section: [
            (
                str(row.get("ticker", "")),
                float(row.get("relative_strength_63d_pct", 0.0)),
            )
            for row in rows
            if row.get("ticker")
        ]
        for section, rows in merged_sections.items()
    }
    source_reports = dict(previous.get("source_reports", {}))
    source_reports.update({
        key: value
        for key, value in latest.get("source_reports", {}).items()
        if value
    })
    merged = build_top_assets_payload(
        selections,
        {"generated_at": source_reports.get("nasdaq_generated_at")},
        {"generated_at": source_reports.get("mid_cap_generated_at")},
    )
    merged["generated_at"] = latest.get("generated_at", merged["generated_at"])
    merged["paris_date"] = latest.get("paris_date", merged["paris_date"])
    return merged


def _daily_snapshots(existing: dict) -> list[dict]:
    """Lit le format cumulatif v2 ou migre l'ancien export journalier v1."""
    history = existing.get("history")
    if isinstance(history, list):
        return [item for item in history if isinstance(item, dict) and item.get("paris_date")]
    if existing.get("paris_date") and isinstance(existing.get("tickers"), list):
        return [{
            key: existing.get(key)
            for key in (
                "generated_at", "paris_date", "source", "complete", "count",
                "tickers", "assets", "sections", "source_reports",
            )
        }]
    return []


def _section_stats(appearances: list[dict]) -> dict[str, dict]:
    stats: dict[str, dict] = {}
    by_section: dict[str, list[dict]] = {}
    for appearance in appearances:
        by_section.setdefault(appearance["section"], []).append(appearance)
    for section, rows in by_section.items():
        ranks = [int(row["rank"]) for row in rows]
        latest = max(rows, key=lambda row: row["date"])
        stats[section] = {
            "days_selected": len({row["date"] for row in rows}),
            "best_rank": min(ranks),
            "average_rank": round(sum(ranks) / len(ranks), 4),
            "last_rank": int(latest["rank"]),
            "last_relative_strength_63d_pct": latest["relative_strength_63d_pct"],
        }
    return stats


def merge_top_assets_history(existing: dict, daily: dict) -> dict:
    """Ajoute/met a jour la journee puis reconstruit l'univers cumulatif."""
    snapshots_by_date = {
        item["paris_date"]: item
        for item in _daily_snapshots(existing)
    }
    date = daily["paris_date"]
    snapshots_by_date[date] = _merge_same_day(snapshots_by_date.get(date, {}), daily)
    history = [snapshots_by_date[key] for key in sorted(snapshots_by_date)]

    assets_by_ticker: dict[str, dict] = {}
    ordered_tickers: list[str] = []
    for snapshot in history:
        snapshot_date = snapshot["paris_date"]
        for daily_asset in snapshot.get("assets", []):
            ticker = str(daily_asset.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            if ticker not in assets_by_ticker:
                assets_by_ticker[ticker] = {
                    "ticker": ticker,
                    "asset_types": [],
                    "sections": [],
                    "first_seen_date": snapshot_date,
                    "last_seen_date": snapshot_date,
                    "selection_dates": [],
                    "appearances": [],
                }
                ordered_tickers.append(ticker)
            asset = assets_by_ticker[ticker]
            asset["last_seen_date"] = snapshot_date
            if snapshot_date not in asset["selection_dates"]:
                asset["selection_dates"].append(snapshot_date)
            for asset_type in daily_asset.get("asset_types", []):
                if asset_type not in asset["asset_types"]:
                    asset["asset_types"].append(asset_type)
            for section in daily_asset.get("sections", []):
                if section not in asset["sections"]:
                    asset["sections"].append(section)
            for key in ("yahoo", "tv", "isin"):
                if daily_asset.get(key):
                    asset[key] = daily_asset[key]
            for appearance in daily_asset.get("appearances", []):
                asset["appearances"].append({"date": snapshot_date, **appearance})

    # L'univers global est monotone: une correction ou seconde execution du
    # meme jour peut remplacer l'instantane quotidien, mais ne retire jamais
    # un ticker deja observe du registre cumulatif.
    previous_assets = {
        str(asset.get("ticker", "")).upper(): asset
        for asset in existing.get("assets", [])
        if isinstance(asset, dict) and asset.get("ticker")
    }
    previous_order = [
        str(ticker).upper()
        for ticker in existing.get("tickers", [])
        if str(ticker).strip()
    ]
    for ticker in previous_order:
        if ticker in assets_by_ticker or ticker not in previous_assets:
            continue
        old = previous_assets[ticker]
        assets_by_ticker[ticker] = {
            "ticker": ticker,
            "asset_types": list(old.get("asset_types", [])),
            "sections": list(old.get("sections", [])),
            "first_seen_date": old.get("first_seen_date"),
            "last_seen_date": old.get("last_seen_date"),
            "selection_dates": list(old.get("selection_dates", [])),
            "appearances": list(old.get("appearances", [])),
            **{
                key: old[key]
                for key in ("yahoo", "tv", "isin")
                if old.get(key)
            },
        }

    ordered_tickers = list(dict.fromkeys(
        [ticker for ticker in previous_order if ticker in assets_by_ticker]
        + ordered_tickers
    ))

    cumulative_assets: list[dict] = []
    for ticker in ordered_tickers:
        asset = assets_by_ticker[ticker]
        appearances = asset["appearances"]
        ranks = [int(item["rank"]) for item in appearances]
        asset["days_selected"] = len(asset["selection_dates"])
        asset["total_appearances"] = len(appearances)
        asset["best_rank"] = min(ranks) if ranks else None
        asset["average_rank"] = round(sum(ranks) / len(ranks), 4) if ranks else None
        asset["section_stats"] = _section_stats(appearances)
        cumulative_assets.append(asset)

    current = snapshots_by_date[date]
    return {
        "schema_version": 2,
        "generated_at": daily["generated_at"],
        "paris_date": date,
        "source": "combo_pipeline.py",
        "history_days": len(history),
        "first_history_date": history[0]["paris_date"],
        "last_history_date": history[-1]["paris_date"],
        "count": len(ordered_tickers),
        "tickers": ordered_tickers,
        "assets": cumulative_assets,
        "current": current,
        "history": history,
    }


def save_top_assets(path: str, payload: dict) -> None:
    """Ecriture atomique pour qu'un lecteur ne voie jamais un JSON partiel."""
    tmp_path = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def build_message(
    nas: dict,
    midr: dict,
    selections: dict[str, list[tuple[str, float]]] | None = None,
) -> str | None:
    lines: list[str] = []
    selections = selections or collect_top_assets(nas, midr)

    ns = selections["large_cap"]
    if ns:
        lines.append("📈 LARGE CAP")
        themes = _top_themes(nas)
        if themes:
            lines.append("🔥 " + ", ".join(themes))
        lines += [f"{i}. {t} ({format_pct(rel)})" for i, (t, rel) in enumerate(ns, 1)]

    ms = selections["mid_cap_tech"]
    if ms:
        if lines:
            lines.append("")
        lines.append("📈 MID-CAP TECH")
        themes = _top_themes(midr)
        if themes:
            lines.append("🔥 " + ", ".join(themes))
        lines.append("")
        lines += [f"{i}. {t} ({format_pct(rel)})" for i, (t, rel) in enumerate(ms, 1)]

    uc = selections["etf_ucits"]
    if uc:
        if lines:
            lines.append("")
        lines.append("🇪🇺 TOP 3 ETF UCITS")
        lines += [f"{i}. {t} ({format_pct(rel)})" for i, (t, rel) in enumerate(uc, 1)]

    if not lines:
        return None
    stamp = datetime.now(PARIS).strftime("%Y-%m-%d %H:%M")
    lines += ["", f"⏰ {stamp} Paris"]
    return "\n".join(lines)


def main() -> int:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Message Telegram fusionne Nasdaq + Mid-cap tech.")
    ap.add_argument("--no-telegram", action="store_true", help="Affichage seul, pas d'envoi.")
    ap.add_argument("--force", action="store_true", help="Ignore l'anti-doublon journalier.")
    ap.add_argument(
        "--top-assets-json",
        default=TOP_ASSETS_JSON,
        help="Registre JSON cumulatif et deduplique des trois TOP 3.",
    )
    args = ap.parse_args()

    today = datetime.now(PARIS).strftime("%Y-%m-%d")
    nas = _load(NAS_JSON)
    midr = _load(MID_JSON)
    if not _fresh(nas, today):
        print("Rapport Nasdaq absent ou perime — section ignoree.")
        nas = {}
    if not _fresh(midr, today):
        print("Rapport Mid-cap absent ou perime — section ignoree.")
        midr = {}

    selections = collect_top_assets(nas, midr)
    message = build_message(nas, midr, selections)
    if message is None:
        print("Aucune donnee fraiche — aucun message.")
        return 0
    print("\n" + message + "\n")

    daily_top_assets = build_top_assets_payload(selections, nas, midr)
    try:
        top_assets = merge_top_assets_history(
            _load(args.top_assets_json),
            daily_top_assets,
        )
        save_top_assets(args.top_assets_json, top_assets)
        completeness = "complete" if top_assets["current"]["complete"] else "partielle"
        day_label = "jour" if top_assets["history_days"] == 1 else "jours"
        print(
            f"Historique TOP sauvegarde: {args.top_assets_json} "
            f"({top_assets['count']} tickers cumules, "
            f"{top_assets['history_days']} {day_label}, "
            f"selection du jour {completeness})."
        )
    except Exception as exc:
        print(f"Impossible de sauvegarder la liste TOP: {exc}")

    already = _load(SEND_STATE).get("last_sent_date") == today
    if args.no_telegram:
        print("Telegram desactive (--no-telegram).")
    elif already and not args.force:
        print(f"Deja envoye aujourd'hui ({today}) — 1x/jour, pas de renvoi.")
    else:
        send_telegram(message)
        try:
            with open(SEND_STATE, "w", encoding="utf-8") as f:
                json.dump({"last_sent_date": today}, f)
        except Exception:
            pass
        print("Telegram: message fusionne envoye.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
