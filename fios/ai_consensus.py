#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fios/ai_consensus.py
--------------------
Moteur de Consensus Dual-IA (Claude x Codex) pour la Synthèse Forex Ultra-Courte.

Analyse l'intégralité des rapports Forex en parallèle via :
1. Anthropic API (Claude 3.5 Haiku / Sonnet)
2. OpenAI API (GPT-4o / GPT-4o-mini / Codex)

Génère une conclusion consensuelle en EXACTEMENT 2 lignes ultra-courtes
(max 22-25 caractères par ligne) pour la bulle étroite Telegram.
"""

from __future__ import annotations

import json
import os
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

PROMPT_SYSTEM = """Tu es un analyste Forex institutionnel. Analyse le rapport Forex complet ci-dessous et résume la situation en EXACTEMENT 2 LIGNES ULTRA-COURTES.

CRITÈRES STRICTS DE FORMATAGE :
1. EXACTEMENT 2 lignes (pas une de plus, pas une de moins).
2. Maximum 22 à 25 caractères PAR LIGNE. Chaque ligne doit être extrêmement courte et tenir sur un écran mobile très étroit sans retour automatique.
3. Utilise 1 emoji au début de chaque ligne (ex: 🔥, 📈, 📉, ⚠️, ⚡, ⚪).
4. Style direct, percutant (pas de phrases complètes, pas d'introduction, pas de politesse).

Exemple de format attendu :
🔥 Sell-Off USD massif
📈 Ruée Or & NZD Fort
"""


def _call_claude(report_text: str, api_key: str, timeout: float = 2.0) -> list[str] | None:
    """Appel API Anthropic Claude."""
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    candidate_models = [
        "claude-3-5-haiku-latest",
        "claude-3-5-sonnet-latest",
        "claude-3-haiku-20240307",
        "claude-3-5-haiku-20241022",
    ]
    for model_name in candidate_models:
        try:
            data = {
                "model": model_name,
                "max_tokens": 100,
                "system": PROMPT_SYSTEM,
                "messages": [{"role": "user", "content": f"Voici le rapport Forex à synthétiser en 2 lignes ultra-courtes:\n\n{report_text[:2500]}"}],
            }
            req = urllib.request.Request(url, data=json.dumps(data).encode("utf-8"), headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
                text = body["content"][0]["text"].strip()
                lines = [line.strip() for line in text.splitlines() if line.strip()]
                if len(lines) >= 2:
                    return lines[:2]
        except Exception:
            continue
    print("Warning: Claude API call failed on all candidate models.")
    return None


def _call_openai(report_text: str, api_key: str, timeout: float = 2.0) -> list[str] | None:
    """Appel API OpenAI / Codex (GPT-4o-mini)."""
    try:
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        data = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": PROMPT_SYSTEM},
                {"role": "user", "content": f"Voici le rapport Forex à synthétiser en 2 lignes ultra-courtes:\n\n{report_text[:2500]}"},
            ],
            "max_tokens": 100,
            "temperature": 0.3,
        }
        req = urllib.request.Request(url, data=json.dumps(data).encode("utf-8"), headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            text = body["choices"][0]["message"]["content"].strip()
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            return lines[:2] if len(lines) >= 2 else None
    except Exception as exc:
        print(f"Warning: OpenAI API call failed/timeout: {exc}")
        return None


def _truncate_line(line: str, max_chars: int = 25) -> str:
    """S'assure qu'une ligne ne dépasse jamais la largeur de la bulle Telegram."""
    line = line.strip()
    if len(line) <= max_chars:
        return line
    return line[:max_chars - 1].rstrip() + "…"


def _fallback_synthesis(report_text: str) -> list[str]:
    """Fallback statique ultra-rapide si les API IA ne répondent pas."""
    lines = []
    if "XAUUSD" in report_text and "🔥" in report_text:
        lines.append("🔥 Poussée Or XAUUSD")
    elif "USD" in report_text and ("↓↓" in report_text or "🔴 USD" in report_text):
        lines.append("🔴 Baisse USD Globale")
    else:
        lines.append("⚡ Momentum Forex Actif")

    if "NZD" in report_text and ("↑↑" in report_text or "🟢 NZD" in report_text):
        lines.append("🟢 Force Devises NZD")
    elif "GBP" in report_text and ("↑↑" in report_text or "🟢 GBP" in report_text):
        lines.append("🟢 Tendance Haussière GBP")
    else:
        lines.append("📊 Suivi Confluence MTF")

    return lines


def generate_ai_consensus(report_text: str, timeout: float = 2.0) -> list[str]:
    """Génère la synthèse IA en interrogeant Claude et Codex en parallèle."""
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()

    claude_lines: list[str] | None = None
    openai_lines: list[str] | None = None

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {}
        if anthropic_key:
            futures[executor.submit(_call_claude, report_text, anthropic_key, timeout)] = "claude"
        if openai_key:
            futures[executor.submit(_call_openai, report_text, openai_key, timeout)] = "openai"

        for future in as_completed(futures):
            name = futures[future]
            try:
                res = future.result()
                if name == "claude":
                    claude_lines = res
                elif name == "openai":
                    openai_lines = res
            except Exception:
                pass

    # Decision logic & consensus header
    header = ""
    retained_lines: list[str] = []

    if claude_lines and openai_lines:
        header = "🤝 CONSENSUS IA (Claude x Codex)"
        # Use Claude line 1 and Codex line 2 or merge clean lines
        retained_lines = [
            _truncate_line(claude_lines[0]),
            _truncate_line(openai_lines[1] if len(openai_lines) > 1 else openai_lines[0]),
        ]
    elif claude_lines:
        header = "🧠 CLAUDE IA FLASH"
        retained_lines = [_truncate_line(l) for l in claude_lines[:2]]
    elif openai_lines:
        header = "🧠 CODEX IA FLASH"
        retained_lines = [_truncate_line(l) for l in openai_lines[:2]]
    else:
        header = "🧠 BILAN IA FLASH"
        retained_lines = [_truncate_line(l) for l in _fallback_synthesis(report_text)]

    return [header] + retained_lines
