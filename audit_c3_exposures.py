# -*- coding: utf-8 -*-
"""
Audit C3 Max Currency Exposure
==============================
Vérifie pourquoi RSB-C3 enregistrait une exposition max de 4 et assure une limite stricte <= 2.
"""

import sys
import os
import copy
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from relative_strength.execution import ExecutionManagerC, get_directional_currency_exposures, TradePosition

mgr_c3 = ExecutionManagerC("C3", max_currency_exposure=2, strict_rearm=True)

# Test manual sequence of 3 trades
pos1 = TradePosition("1", "RSB-C3", "NZDUSD", "LONG", "", 0.58, 10.0, "SWEET_SPOT", 80, 80, 1, 2, "NZD", "USD")
pos2 = TradePosition("2", "RSB-C3", "NZDCHF", "LONG", "", 0.50, 10.0, "SWEET_SPOT", 80, 80, 2, 2, "NZD", "CHF")

mgr_c3.active_positions = [pos1, pos2]
exp = get_directional_currency_exposures(mgr_c3.active_positions)
print("Expositions initiales (2 trades LONG NZD):", exp)

# Try adding 3rd trade (NZDJPY LONG)
from relative_strength.models import RelativeStrengthState
s3 = RelativeStrengthState(
    pair="NZDJPY", timestamp="2026-08-14T10:00:00", reference_time="07:00:00", reference_price=90.0, reference_source="",
    current_price=90.1, trade_direction="LONG", directional_pips=10.0, threshold_state="BREAKOUT_CONFIRMED",
    entry_zone="SWEET_SPOT", trade_eligibility="ELIGIBLE", is_eligible=True, opportunity_score=80, quality_score=80,
    opportunity_rank=3, confirmation_runs=2, base_currency="NZD", quote_currency="JPY"
)

entries, rejections = mgr_c3.process_run([s3])
print("Nouveaux trades acceptés:", len(entries))
print("Rejets:", [r.rejection_reason for r in rejections])
exp_after = get_directional_currency_exposures(mgr_c3.active_positions)
print("Expositions finales:", exp_after)
