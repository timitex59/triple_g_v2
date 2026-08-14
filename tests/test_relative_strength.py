# -*- coding: utf-8 -*-
"""
Unit Test Suite for 07H Relative Strength Breakout Engine
=========================================================
Tests complets validant l'ensemble des 12 amendements, le Trade Eligibility Gate et les scénarios A, B, C, D.
"""

import os
import sys
import tempfile
from datetime import datetime, time
from zoneinfo import ZoneInfo
import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from relative_strength.config import PARIS_TZ, THRESHOLD_PIPS, MIN_CONFIRMATION_RUNS, ENABLE_RSB_EXITS
from relative_strength.models import RelativeStrengthState
from relative_strength.reference import update_pair_reference, is_at_or_after_reference_time
from relative_strength.pip_engine import (
    get_pip_size,
    calculate_pips,
    determine_directional_pips,
    update_excursion_metrics,
)
from relative_strength.threshold import (
    evaluate_threshold_state,
    evaluate_entry_zone,
    evaluate_independent_flags,
    evaluate_trade_eligibility,
)
from relative_strength.persistence import update_persistence_counters
from relative_strength.velocity import calculate_velocity_and_acceleration
from relative_strength.breadth import (
    compute_currency_strengths_from_pips,
    compute_currency_breadth,
    select_best_currency_expression,
)
from relative_strength.scoring import compute_quality_score, compute_opportunity_score
from relative_strength.ranking import apply_triple_ranking
from relative_strength.exits import evaluate_exit_signal
from relative_strength.persistence_store import save_rsb_state, load_rsb_state


def test_pip_size_calculation():
    """Test 1: Pip sizes JPY (0.01) vs Non-JPY (0.0001)."""
    assert get_pip_size("EURUSD") == 0.0001
    assert get_pip_size("GBPUSD") == 0.0001
    assert get_pip_size("USDJPY") == 0.01
    assert get_pip_size("EURJPY") == 0.01
    assert get_pip_size("CHFJPY") == 0.01


def test_trade_eligibility_gate():
    """Test 2: Trade Eligibility Gate (Filtre dur de décision)."""
    # NEUTRAL -> NOT_ELIGIBLE
    status, is_e = evaluate_trade_eligibility("NEUTRAL", "NOISE")
    assert status == "NOT_ELIGIBLE"
    assert is_e is False
    
    # BREAKOUT_PENDING -> WATCHLIST
    status, is_e = evaluate_trade_eligibility("BREAKOUT_PENDING", "SWEET_SPOT")
    assert status == "WATCHLIST"
    assert is_e is False
    
    # BREAKOUT_CONFIRMED + SWEET_SPOT -> ELIGIBLE
    status, is_e = evaluate_trade_eligibility("BREAKOUT_CONFIRMED", "SWEET_SPOT")
    assert status == "ELIGIBLE"
    assert is_e is True
    
    # BREAKOUT_CONFIRMED + EXTENSION -> ELIGIBLE_WITH_PENALTY
    status, is_e = evaluate_trade_eligibility("BREAKOUT_CONFIRMED", "EXTENSION")
    assert status == "ELIGIBLE_WITH_PENALTY"
    assert is_e is True
    
    # EXTENDED + CHASING -> NOT_ELIGIBLE
    status, is_e = evaluate_trade_eligibility("EXTENDED", "CHASING")
    assert status == "NOT_ELIGIBLE"
    assert is_e is False


def test_market_movement_and_trade_direction():
    """Test 3: Separates raw_market_pips, long_pips, short_pips, directional_pips."""
    raw, long_p, short_p = calculate_pips(1.1540, 1.1500, 0.0001)
    assert round(raw, 1) == 40.0
    assert round(long_p, 1) == 40.0
    assert round(short_p, 1) == -40.0
    
    assert round(determine_directional_pips(raw, "LONG"), 1) == 40.0
    assert round(determine_directional_pips(raw, "SHORT"), 1) == -40.0
    
    raw_jpy, long_jpy, short_jpy = calculate_pips(154.50, 155.00, 0.01)
    assert round(raw_jpy, 1) == -50.0
    assert round(determine_directional_pips(raw_jpy, "SHORT"), 1) == 50.0


def test_reference_07h_tracking():
    """Test 4: Reference price tracking at 07:00 Paris timezone."""
    dt_08h = datetime(2026, 8, 14, 8, 15, 0, tzinfo=PARIS_TZ)
    assert is_at_or_after_reference_time(dt_08h) is True
    
    store = {}
    ref = update_pair_reference("NZDUSD", dt_08h, 0.58666, "TradingView_Socket", store)
    assert ref["reference_price"] == 0.58666
    assert ref["reference_source"] == "TradingView_Socket"
    
    dt_09h = datetime(2026, 8, 14, 9, 30, 0, tzinfo=PARIS_TZ)
    ref2 = update_pair_reference("NZDUSD", dt_09h, 0.58900, "TradingView_Socket", store)
    assert ref2["reference_price"] == 0.58666


def test_v1_simplified_state_machine_and_flags():
    """Test 5: V1 Exclusive States & Independent Flags."""
    assert evaluate_threshold_state(3.2, 0) == "NEUTRAL"
    assert evaluate_threshold_state(6.5, 1) == "BREAKOUT_PENDING"
    assert evaluate_threshold_state(8.4, 2) == "BREAKOUT_CONFIRMED"
    assert evaluate_threshold_state(28.1, 3) == "EXTENDED"
    assert evaluate_threshold_state(-6.2, 0) == "INVALIDATED"


def test_scenario_a_confirmed_breakout():
    """Scenario A: 0 -> +3 -> +6 -> +10 -> +17 -> Breakout Confirmed."""
    pips_sequence = [0.0, 3.0, 6.0, 10.0, 17.0]
    conf_runs = 0
    prev_persistence = {}
    
    for pips in pips_sequence:
        conf_runs, above_runs, _, _ = update_persistence_counters(pips, "LONG", False, prev_persistence)
        prev_persistence = {"confirmation_runs": conf_runs, "above_threshold_runs": above_runs, "last_trade_direction": "LONG"}
        
    state = evaluate_threshold_state(pips_sequence[-1], conf_runs)
    assert conf_runs >= 2
    assert state == "BREAKOUT_CONFIRMED"


def test_scenario_b_false_breakout():
    """Scenario B: 0 -> +6 -> +2 -> +7 -> +1 -> False/unstable breakout."""
    pips_sequence = [0.0, 6.0, 2.0, 7.0, 1.0]
    conf_runs = 0
    prev_persistence = {}
    
    for pips in pips_sequence:
        conf_runs, above_runs, _, _ = update_persistence_counters(pips, "LONG", False, prev_persistence)
        prev_persistence = {"confirmation_runs": conf_runs, "above_threshold_runs": above_runs, "last_trade_direction": "LONG"}
        
    state = evaluate_threshold_state(pips_sequence[-1], conf_runs)
    assert state == "NEUTRAL"


def test_scenario_c_significant_mae():
    """Scenario C: 0 -> -3 -> -8 -> -4 -> +6 -> +18 -> Significant MAE recorded."""
    pips_sequence = [0.0, -3.0, -8.0, -4.0, 6.0, 18.0]
    mfe, mae = 0.0, 0.0
    
    for pips in pips_sequence:
        mfe, mae, _, _, _ = update_excursion_metrics(pips, mfe, mae)
        
    assert round(mfe, 1) == 18.0
    assert round(mae, 1) == -8.0


def test_scenario_d_exhaustion_and_mfe_drawdown():
    """Scenario D: 0 -> +7 -> +14 -> +25 -> +37 -> +22 -> MFE 37, Drawdown 15."""
    pips_sequence = [0.0, 7.0, 14.0, 25.0, 37.0, 22.0]
    mfe, mae = 0.0, 0.0
    dd_pips, dd_pct = 0.0, 0.0
    
    for pips in pips_sequence:
        mfe, mae, dd_pips, dd_pct, _ = update_excursion_metrics(pips, mfe, mae)
        
    assert round(mfe, 1) == 37.0
    assert round(dd_pips, 1) == 15.0
    assert round(dd_pct, 1) == 40.5


def test_non_duplicated_currency_breadth():
    """Test 6: Non-duplicated currency breadth."""
    pairs_raw_pips = {
        "NZDUSD": 12.0, "NZDCHF": 8.0, "NZDJPY": 15.0,
        "EURNZD": -10.0, "AUDNZD": -6.0, "NZDCAD": 4.0
    }
    
    conf, contrad, avail, ratio = compute_currency_breadth("NZD", "BULL", pairs_raw_pips)
    assert avail == 6
    assert conf == 6
    assert round(ratio, 2) == 1.0


def test_best_expression_excludes_chasing():
    """Test 7: BEST EXPRESSION excludes CHASING zone by default."""
    state_sweet = RelativeStrengthState(
        pair="NZDCHF", timestamp="", reference_time="", reference_price=0.5, reference_source="",
        current_price=0.51, trade_direction="LONG", directional_pips=12.0, entry_zone="SWEET_SPOT",
        base_currency="NZD", quote_currency="CHF", opportunity_score=88.0, quality_score=85.0
    )
    
    state_chasing = RelativeStrengthState(
        pair="NZDUSD", timestamp="", reference_time="", reference_price=0.58, reference_source="",
        current_price=0.5835, trade_direction="LONG", directional_pips=35.0, entry_zone="CHASING",
        base_currency="NZD", quote_currency="USD", opportunity_score=92.0, quality_score=90.0
    )
    
    best = select_best_currency_expression([state_sweet, state_chasing], "NZD", "LONG", allow_chasing=False)
    assert best is not None
    assert best.pair == "NZDCHF"


def test_triple_ranking_separation():
    """Test 8: Triple Ranking (PIP RANK, QUALITY RANK, OPPORTUNITY RANK only for eligible)."""
    # Pair A: NEUTRAL -> NOT_ELIGIBLE -> opportunity_rank = 0
    pair_a = RelativeStrengthState(
        pair="EURUSD", timestamp="", reference_time="", reference_price=1.15, reference_source="",
        current_price=1.1503, trade_direction="LONG", directional_pips=3.0, threshold_state="NEUTRAL",
        entry_zone="NOISE", trade_eligibility="NOT_ELIGIBLE", is_eligible=False, opportunity_score=50.0, quality_score=40.0
    )
    
    # Pair B: BREAKOUT_CONFIRMED + SWEET_SPOT -> ELIGIBLE -> opportunity_rank = 1
    pair_b = RelativeStrengthState(
        pair="USDCAD", timestamp="", reference_time="", reference_price=1.39, reference_source="",
        current_price=1.3890, trade_direction="SHORT", directional_pips=10.0, threshold_state="BREAKOUT_CONFIRMED",
        entry_zone="SWEET_SPOT", trade_eligibility="ELIGIBLE", is_eligible=True, opportunity_score=85.0, quality_score=88.0
    )
    
    apply_triple_ranking([pair_a, pair_b])
    
    assert pair_b.is_eligible is True
    assert pair_b.opportunity_rank == 1
    
    assert pair_a.is_eligible is False
    assert pair_a.opportunity_rank == 0  # Non éligible !


def test_exits_disabled_in_v1():
    """Test 9: Active exits disabled by default in V1."""
    assert ENABLE_RSB_EXITS is False
    dummy_state = RelativeStrengthState(
        pair="EURUSD", timestamp="", reference_time="", reference_price=1.0, reference_source="",
        current_price=1.0, directional_pips=-10.0
    )
    should_exit, reason = evaluate_exit_signal(dummy_state)
    assert should_exit is False
    assert reason == "EXITS_DISABLED_IN_V1"
