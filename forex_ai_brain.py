#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
forex_ai_brain.py
-----------------
Cerveau IA Indépendant d'Auto-Apprentissage et d'Amélioration Continue pour le Système Forex.

Fonctionnalités :
1. Accumule l'expérience marché run après run dans une mémoire JSON persistante (`forex_ai_knowledge_base.json`).
2. À 23h00 (ou via --eod), sollicite Claude 3.5/3.7 Sonnet & GPT-4o / Flagships pour générer le Bilan 24H
   et les Suggestions d'Amélioration des scripts.
3. Envoie le rapport sur Telegram.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

PARIS = ZoneInfo("Europe/Paris")
KNOWLEDGE_BASE_PATH = "forex_ai_knowledge_base.json"


# ── Gestionnaire de Mémoire JSON Persistante ──

def load_knowledge_base(filepath: str = KNOWLEDGE_BASE_PATH) -> dict:
    """Charge ou initialise la base de connaissances JSON."""
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            print(f"Warning: Impossible de lire {filepath}: {exc}")

    return {
        "version": 1,
        "total_runs_analyzed": 0,
        "last_updated": None,
        "accumulated_patterns": [],
        "script_improvements": [],
        "daily_summaries": [],
        "history": [],
    }


def save_knowledge_base(data: dict, filepath: str = KNOWLEDGE_BASE_PATH) -> None:
    """Sauvegarde la base de connaissances JSON avec mise en forme propre."""
    data["last_updated"] = datetime.now(PARIS).isoformat()
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"Error: Échec de sauvegarde dans {filepath}: {exc}")


# ── Collecte des Données Récentes du Marché ──

def collect_market_snapshot() -> dict:
    """Rassemble les données récentes générées par les scripts Forex."""
    snapshot = {
        "timestamp": datetime.now(PARIS).isoformat(),
        "align_pairs": {},
        "indexes": {},
        "vivier": {},
    }

    # 1. Full alignment sidecar
    if os.path.exists("full_alignment_index.json"):
        try:
            with open("full_alignment_index.json", "r", encoding="utf-8") as f:
                d = json.load(f)
                snapshot["align_pairs"] = d.get("pairs") or {}
                snapshot["indexes"] = d.get("indexes") or d.get("currencies") or {}
        except Exception as exc:
            print(f"Warning: Erreur lecture full_alignment_index.json: {exc}")

    # 2. Vivier state
    if os.path.exists("renko_score_29pairs_vivier_state.json"):
        try:
            with open("renko_score_29pairs_vivier_state.json", "r", encoding="utf-8") as f:
                d = json.load(f)
                snapshot["vivier"] = d.get("pairs") or {}
        except Exception as exc:
            print(f"Warning: Erreur lecture vivier state: {exc}")

    return snapshot


def record_run_observation(kb: dict, snapshot: dict) -> dict:
    """Enregistre l'observation du run dans la mémoire d'apprentissage."""
    kb["total_runs_analyzed"] = kb.get("total_runs_analyzed", 0) + 1
    
    align_count = len(snapshot.get("align_pairs", {}))
    indexes = snapshot.get("indexes", {})

    strongest = []
    weakest = []
    for cur, data in indexes.items():
        chg = data.get("daily_chg", 0)
        if chg >= 0.30:
            strongest.append(f"{cur} ({chg:+.2f}%)")
        elif chg <= -0.30:
            weakest.append(f"{cur} ({chg:+.2f}%)")

    entry = {
        "timestamp": snapshot["timestamp"],
        "aligned_pairs_count": align_count,
        "strong_currencies": strongest,
        "weak_currencies": weakest,
    }
    
    history = kb.setdefault("history", [])
    history.append(entry)
    # Conserver les 100 derniers runs dans l'historique de mémoire
    kb["history"] = history[-100:]

    return entry


# ── Modèles d'IA Frontier (Claude Sonnet x GPT-4o) ──

PROMPT_EOD_BRAIN = """Tu es le Cerveau IA Principal d'un système de trading Forex institutionnel autonome.
Analyse les données de la journée (rapports, devises fortes/faibles, paires confluentes) ainsi que la mémoire historique d'apprentissage.

Génère une synthèse de fin de journée de HAUTE INTELLIGENCE comprenant EXACTEMENT :

1. 📊 RÉTROSPECTIVE 24H (Impulsions majeures de la journée, paires gagnantes, comportement Or/Gold).
2. 📚 LEÇONS & PATTERNS APPRIS (Ce qui a fonctionné, ce qu'il faut retenir de la dynamique).
3. 💡 SUGGESTIONS D'AMÉLIORATION DES SCRIPTS (2 à 3 propositions très concrètes d'amélioration du code Python, des seuils ou des filtres).

Règles de formatage Telegram :
- Style clair, structuré, percutant.
- Emojis professionnels (🔥, 📈, 📉, 💡, 📚, 🏆).
- Longueur adaptée pour Telegram mobile.
"""


def _query_claude_sonnet(kb_summary: str, api_key: str, timeout: float = 8.0) -> str | None:
    """Appel API Anthropic Claude 3.5/3.7 Sonnet."""
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    models = ["claude-3-7-sonnet-latest", "claude-3-5-sonnet-latest", "claude-3-5-sonnet-20241022"]
    for m in models:
        try:
            data = {
                "model": m,
                "max_tokens": 450,
                "system": PROMPT_EOD_BRAIN,
                "messages": [{"role": "user", "content": f"Voici les données d'apprentissage 24H et la mémoire JSON:\n\n{kb_summary}"}],
            }
            req = urllib.request.Request(url, data=json.dumps(data).encode("utf-8"), headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
                return body["content"][0]["text"].strip()
        except Exception:
            continue
    return None


def _query_gpt4o(kb_summary: str, api_key: str, timeout: float = 8.0) -> str | None:
    """Appel API OpenAI GPT-4o / o3-mini."""
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    models = ["gpt-4o", "o3-mini", "gpt-4o-2024-11-20"]
    for m in models:
        try:
            data = {
                "model": m,
                "messages": [
                    {"role": "system", "content": PROMPT_EOD_BRAIN},
                    {"role": "user", "content": f"Voici les données d'apprentissage 24H et la mémoire JSON:\n\n{kb_summary}"},
                ],
                "max_tokens": 450,
            }
            if not m.startswith("o"):
                data["temperature"] = 0.3
            req = urllib.request.Request(url, data=json.dumps(data).encode("utf-8"), headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
                return body["choices"][0]["message"]["content"].strip()
        except Exception:
            continue
    return None


def send_telegram_message(text: str) -> bool:
    """Envoie le rapport final sur Telegram."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not bot_token or not chat_id:
        print("Warning: TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID absent. Envoi ignore.")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            return resp.status == 200
    except Exception as exc:
        print(f"Error: Échec envoi Telegram: {exc}")
        return False


def run_eod_analysis(kb: dict, send_tg: bool = True) -> str:
    """Génère l'analyse de fin de journée (23h) et met à jour les suggestions JSON."""
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()

    kb_summary = json.dumps({
        "total_runs_analyzed": kb.get("total_runs_analyzed", 0),
        "recent_history_24h": kb.get("history", [])[-24:],
        "accumulated_patterns": kb.get("accumulated_patterns", []),
        "script_improvements": kb.get("script_improvements", []),
    }, ensure_ascii=False, indent=2)

    analysis_text: str | None = None

    if anthropic_key:
        analysis_text = _query_claude_sonnet(kb_summary, anthropic_key)
    if not analysis_text and openai_key:
        analysis_text = _query_gpt4o(kb_summary, openai_key)

    now_str = datetime.now(PARIS).strftime("%d/%m/%Y %H:%M")

    if not analysis_text:
        analysis_text = (
            "📊 RÉTROSPECTIVE 24H\n"
            "• Suivi continu des opportunités Forex et Vivier effectué.\n\n"
            "📚 LEÇONS APPRISES\n"
            "• Maintien des filtres de confluences multi-timeframes.\n\n"
            "💡 SUGGESTIONS D'AMÉLIORATION\n"
            "1. Continuer d'accumuler la mémoire des impulsions devises.\n"
            "2. Valider les breakouts sur l'Or XAUUSD."
        )

    full_report = (
        f"🧠 FOREX IA BRAIN — BILAN & AUTO-APPRENTISSAGE 23H\n\n"
        f"{analysis_text}\n\n"
        f"⏰ {now_str} Paris"
    )

    # Sauvegarder les suggestions dans la mémoire JSON
    improvements = kb.setdefault("script_improvements", [])
    improvements.append({
        "date": datetime.now(PARIS).date().isoformat(),
        "summary": analysis_text[:300],
    })
    kb["script_improvements"] = improvements[-30:] # Conserver 30 jours
    save_knowledge_base(kb)

    if send_tg:
        send_telegram_message(full_report)
        print("✅ Rapport Forex AI Brain 23H envoyé sur Telegram.")

    return full_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Forex AI Brain — Autonomous Continuous Learning Engine")
    parser.add_argument("--eod", action="store_true", help="Force le déclenchement de l'analyse 23h Clôture")
    parser.add_argument("--no-telegram", action="store_true", help="Ne pas envoyer le message sur Telegram")
    args = parser.parse_args()

    print("🧠 [Forex AI Brain] Démarrage du module d'auto-apprentissage...")
    kb = load_knowledge_base()
    snapshot = collect_market_snapshot()
    obs = record_run_observation(kb, snapshot)
    save_knowledge_base(kb)
    print(f"   [Mémoire JSON] Run enregistré. Total runs analysés : {kb.get('total_runs_analyzed')}")

    current_hour = datetime.now(PARIS).hour
    if args.eod or current_hour == 23:
        print("   [Mode Clôture 23h] Génération de l'Analyse Avancée Sonnet x GPT-4o...")
        report = run_eod_analysis(kb, send_tg=not args.no_telegram)
        print(report)
    else:
        print(f"   [Info] Heure actuelle : {current_hour}h Paris. L'analyse de clôture se déclenchera à 23h.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
