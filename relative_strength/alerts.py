# -*- coding: utf-8 -*-
"""
RSB Telegram Alert Engine (Forward Test Production Standard)
============================================================
Génère la suite d'alertes Telegram officielles pour RSB-C1 (Cap 15, 0.10% risk) :
1. WATCH Alert (1/2 confirmation runs)
2. ENTRY Signal Alert (2/2 confirmation runs)
3. CLOSED Trade Alert (Sortie SL, TP, Trailing, EOD, Invalidation)
4. DAILY REPORT (Rapport consolidé de fin de journée)
"""

from typing import List, Dict, Any, Optional
from relative_strength.models import RelativeStrengthState
from relative_strength.execution import TradePosition


def format_watch_alert(state: RelativeStrengthState) -> str:
    """Formate une alerte d'observation WATCH (1/2 runs)."""
    dir_emoji = "🟢" if state.trade_direction == "LONG" else "🔴"
    time_str = state.timestamp.split("T")[1][:5] if "T" in state.timestamp else state.timestamp

    lines = [
        "👀 <b>07H-RSB · WATCH</b>",
        f"<b>{state.pair} {state.trade_direction}</b>",
        f"<code>{state.directional_pips:+.1f} pips</code> depuis 07h",
        f"🔁 <code>{state.confirmation_runs}/2 runs confirmés</code> · 🎯 SWEET SPOT",
        "",
        "⏳ <b>PAS ENCORE D'ENTRÉE</b>"
    ]
    return "\n".join(lines)


def format_entry_alert(
    state: RelativeStrengthState,
    current_active_positions: int,
    max_capacity: int = 15,
    risk_pct: float = 0.10
) -> str:
    """Formate une alerte d'entrée de position (2/2 runs)."""
    dir_emoji = "🟢" if state.trade_direction == "LONG" else "🔴"
    time_str = state.timestamp.split("T")[1][:5] if "T" in state.timestamp else state.timestamp

    if current_active_positions < max_capacity:
        lines = [
            "🔥 <b>07H-RSB · SIGNAL</b>",
            f"{dir_emoji} <b>{state.pair} {state.trade_direction}</b>",
            "",
            f"⏰ <code>{time_str} Paris</code>",
            f"💰 Entry : <code>{state.current_price:.5f}</code>",
            "",
            f"📊 <code>{state.directional_pips:+.1f} pips</code> depuis 07h",
            "✅ <b>BREAKOUT CONFIRMED</b>",
            f"🔁 <code>{state.confirmation_runs}/2 runs</code>",
            "🎯 <b>SWEET SPOT</b>",
            "",
            "🛡 SL : <code>-10 pips</code>",
            "🎯 TP : <code>+20 pips</code>",
            "🔄 Trailing actif à partir de MFE +15p",
            "",
            f"💶 Risque : <code>{risk_pct:.2f} %</code>",
            f"📂 Positions : <b>{current_active_positions + 1} / {max_capacity}</b>",
            "",
            "➡️ <b>TRADE ÉLIGIBLE</b>"
        ]
    else:
        lines = [
            "⚠️ <b>07H-RSB · SIGNAL ÉLIGIBLE</b>",
            f"<b>{state.pair} {state.trade_direction}</b>",
            f"<code>{state.directional_pips:+.1f}p</code> · SWEET SPOT · CONFIRMED",
            "",
            f"⛔ <b>NON OUVERT</b> · Portfolio capacity : <b>{max_capacity} / {max_capacity}</b>",
            "<i>Signal enregistré en Shadow</i>"
        ]

    return "\n".join(lines)


def format_closed_alert(pos: TradePosition) -> str:
    """Formate une alerte de clôture de position."""
    sign = "+" if pos.exit_pips > 0 else ""
    pips_str = f"{sign}{pos.exit_pips:.1f}"

    lines = [
        "🏁 <b>07H-RSB · CLOSED</b>",
        f"<b>{pos.pair} {pos.direction}</b>",
        "",
        f"Entry : <code>{pos.entry_price:.5f}</code>",
        f"Exit : <code>{pos.entry_price + (pos.exit_pips * (0.01 if 'JPY' in pos.pair else 0.0001)):.5f}</code>",
        "",
        f"💰 <b>{pips_str} pips</b>",
        f"📈 MFE : <code>+{pos.mfe_pips:.1f}p</code>",
        f"📉 MAE : <code>{pos.mae_pips:.1f}p</code>",
        "",
        f"🔄 <b>{pos.exit_reason}</b>",
        f"⏱ Durée : <code>{pos.duration_runs}h</code>"
    ]
    return "\n".join(lines)


def format_daily_report(
    date_str: str,
    eligible_count: int,
    opened_count: int,
    shadow_count: int,
    winners_count: int,
    losers_count: int,
    gross_pips: float,
    real_costs_pips: float,
    net_pips: float,
    avg_slippage: float,
    avg_spread: float,
    max_positions: int,
    max_floating_dd_pct: float,
    start_capital: float,
    end_capital: float
) -> str:
    """Formate le rapport quotidien consolidé de fin de journée."""
    lines = [
        "📊 <b>RSB-C1 · DAILY REPORT</b>",
        f"<code>{date_str}</code>",
        "",
        f"Signaux éligibles : <b>{eligible_count}</b>",
        f"Trades ouverts : <b>{opened_count}</b>",
        f"Shadow capacity : <b>{shadow_count}</b>",
        "",
        f"✅ Gagnants : <b>{winners_count}</b>",
        f"❌ Perdants : <b>{losers_count}</b>",
        "",
        f"Pips brut : <code>{gross_pips:+.1f}</code>",
        f"Coûts réels : <code>-{real_costs_pips:.1f}</code>",
        f"Pips net : <code><b>{net_pips:+.1f}</b></code>",
        "",
        f"Slippage moyen : <code>{avg_slippage:.2f}p</code>",
        f"Spread moyen : <code>{avg_spread:.2f}p</code>",
        "",
        f"Max positions : <b>{max_positions}</b>",
        f"Max floating DD : <code>{max_floating_dd_pct:.1f}%</code>",
        "",
        f"Capital :",
        f"<code>{start_capital:,.0f} € → {end_capital:,.0f} €</code>"
    ]
    return "\n".join(lines)
