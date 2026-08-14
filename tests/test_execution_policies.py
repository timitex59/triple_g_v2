# -*- coding: utf-8 -*-
"""
Unit Test Suite for RSB Multi-Policy Execution Framework
=========================================================
Tests unitaires validant l'isolation stricte des 6 portefeuilles, l'exposition directionnelle de C3,
les identifiants trade_id déterministes et la journalisation des signaux refusés.
"""

import sys
import os
import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from relative_strength.models import RelativeStrengthState
from relative_strength.execution import (
    ExecutionManagerA,
    ExecutionManagerB,
    ExecutionManagerC,
    TradePosition,
    RejectedSignal,
    get_directional_currency_exposures,
)


def test_directional_currency_exposure_calculation():
    """Test: Directional Currency Exposure Math for C3."""
    pos1 = TradePosition(
        trade_id="1", variant_id="RSB-C3", pair="NZDUSD", direction="LONG", entry_time="",
        entry_price=0.58, entry_pips_07h=10.0, entry_zone="SWEET_SPOT", opportunity_score=80.0,
        quality_score=80.0, opportunity_rank=1, confirmation_runs=2, base_currency="NZD", quote_currency="USD"
    )
    pos2 = TradePosition(
        trade_id="2", variant_id="RSB-C3", pair="NZDCHF", direction="LONG", entry_time="",
        entry_price=0.50, entry_pips_07h=10.0, entry_zone="SWEET_SPOT", opportunity_score=80.0,
        quality_score=80.0, opportunity_rank=2, confirmation_runs=2, base_currency="NZD", quote_currency="CHF"
    )
    pos3 = TradePosition(
        trade_id="3", variant_id="RSB-C3", pair="AUDNZD", direction="SHORT", entry_time="",
        entry_price=1.08, entry_pips_07h=-10.0, entry_zone="SWEET_SPOT", opportunity_score=80.0,
        quality_score=80.0, opportunity_rank=3, confirmation_runs=2, base_currency="AUD", quote_currency="NZD"
    )

    exposures = get_directional_currency_exposures([pos1, pos2, pos3])
    # NZDUSD LONG  -> NZD +1, USD -1
    # NZDCHF LONG  -> NZD +1, CHF -1
    # AUDNZD SHORT -> AUD -1, NZD +1
    # Total NZD = +3, USD = -1, CHF = -1, AUD = -1
    assert exposures.get("NZD") == 3
    assert exposures.get("USD") == -1
    assert exposures.get("CHF") == -1
    assert exposures.get("AUD") == -1


def test_rsb_c3_currency_exposure_limit_rejection():
    """Test: RSB-C3 rejects trade when directional exposure exceeds limit (<= 2)."""
    mgr_c3 = ExecutionManagerC("C3", max_currency_exposure=2)

    # Simuler 2 positions actives LONG NZD
    mgr_c3.active_positions.append(TradePosition(
        trade_id="t1", variant_id="RSB-C3", pair="NZDUSD", direction="LONG", entry_time="", entry_price=0.58,
        entry_pips_07h=10.0, entry_zone="SWEET_SPOT", opportunity_score=80.0, quality_score=80.0, opportunity_rank=1,
        confirmation_runs=2, base_currency="NZD", quote_currency="USD"
    ))
    mgr_c3.active_positions.append(TradePosition(
        trade_id="t2", variant_id="RSB-C3", pair="NZDCHF", direction="LONG", entry_time="", entry_price=0.50,
        entry_pips_07h=10.0, entry_zone="SWEET_SPOT", opportunity_score=80.0, quality_score=80.0, opportunity_rank=2,
        confirmation_runs=2, base_currency="NZD", quote_currency="CHF"
    ))

    # 3ème signal LONG NZD (NZDJPY LONG)
    nzdjpy_state = RelativeStrengthState(
        pair="NZDJPY", timestamp="2026-08-14T09:45:00", reference_time="07:00:00", reference_price=90.0, reference_source="",
        current_price=90.10, trade_direction="LONG", directional_pips=10.0, threshold_state="BREAKOUT_CONFIRMED",
        entry_zone="SWEET_SPOT", trade_eligibility="ELIGIBLE", is_eligible=True, opportunity_score=85.0, quality_score=85.0,
        opportunity_rank=3, confirmation_runs=2, base_currency="NZD", quote_currency="JPY"
    )

    entries, rejections = mgr_c3.process_run([nzdjpy_state])
    assert len(entries) == 0
    assert len(rejections) == 1
    assert rejections[0].rejection_reason == "CURRENCY_EXPOSURE_LIMIT"


def test_strict_portfolio_isolation():
    """Test: Execution managers A, B, C operate in total state isolation."""
    mgr_a = ExecutionManagerA()
    mgr_b = ExecutionManagerB()
    mgr_c4 = ExecutionManagerC("C4")

    state_eligible_1 = RelativeStrengthState(
        pair="USDCAD", timestamp="2026-08-14T07:45:00", reference_time="07:00:00", reference_price=1.39, reference_source="",
        current_price=1.3890, trade_direction="SHORT", directional_pips=10.0, threshold_state="BREAKOUT_CONFIRMED",
        entry_zone="SWEET_SPOT", trade_eligibility="ELIGIBLE", is_eligible=True, opportunity_score=85.0, quality_score=85.0,
        opportunity_rank=1, confirmation_runs=2, base_currency="USD", quote_currency="CAD"
    )
    
    state_eligible_2 = RelativeStrengthState(
        pair="NZDUSD", timestamp="2026-08-14T07:45:00", reference_time="07:00:00", reference_price=0.58, reference_source="",
        current_price=0.5810, trade_direction="LONG", directional_pips=10.0, threshold_state="BREAKOUT_CONFIRMED",
        entry_zone="SWEET_SPOT", trade_eligibility="ELIGIBLE", is_eligible=True, opportunity_score=80.0, quality_score=80.0,
        opportunity_rank=2, confirmation_runs=2, base_currency="NZD", quote_currency="USD"
    )

    states = [state_eligible_1, state_eligible_2]

    entries_a, _ = mgr_a.process_run(states)
    entries_b, _ = mgr_b.process_run(states)
    entries_c4, _ = mgr_c4.process_run(states)

    # RSB-A prend uniquement le Rank #1 (USDCAD)
    assert len(entries_a) == 1
    assert entries_a[0].pair == "USDCAD"

    # RSB-B prend uniquement le Rank #1 (USDCAD) et bloque NZDUSD car position active
    assert len(entries_b) == 1
    assert entries_b[0].pair == "USDCAD"

    # RSB-C4 prend TOUTES les paires éligibles (USDCAD et NZDUSD)
    assert len(entries_c4) == 2
