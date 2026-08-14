# -*- coding: utf-8 -*-
"""
Audit de Non-Régression A/B — Diagnostic Trade par Trade
=========================================================
Compare la liste exacte des trades générés par multi_day_backtest_strict_exits.py
et par relative_strength/execution.py (ExecutionManagerA).
"""

import sys
import os
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from multi_day_backtest_strict_exits import run_strict_exits_backtest
from relative_strength.multi_policy_backtest import fetch_multiday_h1_data, run_multi_policy_backtest

print("🔍 COMPARISON AUDIT : HISTORICAL BACKTEST VS MULTI-POLICY FRAMEWORK")
print("=" * 100)

# Run historical backtest A
trades_hist = run_strict_exits_backtest(period="60d", max_trades_per_day=1)
print(f"\n[HISTORICAL BACKTEST A] Total trades: {len(trades_hist)}")
df_hist = pd.DataFrame(trades_hist)
if not df_hist.empty:
    print(df_hist[["entry_time", "pair", "direction", "entry_pips_07h", "exit_reason", "exit_pips", "pips_gain"]].head(10))
