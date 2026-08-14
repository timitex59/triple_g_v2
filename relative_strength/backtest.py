# -*- coding: utf-8 -*-
"""
RSB Parametric Backtesting Engine
=================================
Module de backtest sans lookahead bias (Section 34-39, Amendement #7, #11).
Intègre la Trade Eligibility Gate : entrées autorisées UNIQUEMENT sur paires éligibles (is_eligible == True).
"""

import json
import os
import statistics
from typing import List, Dict, Any, Tuple


def run_parametric_backtest(
    audit_trail_file: str,
    threshold: float = 5.0,
    min_confirmation_runs: int = 2,
    max_entry_pips: float = 25.0
) -> Dict[str, Any]:
    """
    Exécute un backtest paramétrique sur le journal rsb_audit_trail.jsonl.
    """
    if not os.path.exists(audit_trail_file):
        return {
            "error": f"Fichier audit trail introuvable: {audit_trail_file}",
            "number_of_trades": 0
        }

    snapshots = []
    with open(audit_trail_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    snapshots.append(json.loads(line.strip()))
                except Exception:
                    pass

    if not snapshots:
        return {"error": "Aucun snapshot valide trouvé dans l'audit trail", "number_of_trades": 0}

    trades = []
    active_positions: Dict[str, Dict[str, Any]] = {}

    for snapshot in snapshots:
        ts = snapshot.get("timestamp", "")
        pairs = snapshot.get("pairs", {})

        for pair, pdata in pairs.items():
            dir_pips = pdata.get("directional_pips", 0.0)
            conf_runs = pdata.get("confirmation_runs", 0)
            trade_dir = pdata.get("trade_direction", "NEUTRAL")
            state = pdata.get("threshold_state", "NEUTRAL")
            is_eligible = pdata.get("is_eligible", False)
            opp_rank = pdata.get("opportunity_rank", 0)

            # Condition d'entrée STRICTE : La paire DOIT être ÉLIGIBLE (is_eligible == True)
            if pair not in active_positions:
                if is_eligible and conf_runs >= min_confirmation_runs and opp_rank == 1:
                    active_positions[pair] = {
                        "pair": pair,
                        "entry_time": ts,
                        "entry_pips": dir_pips,
                        "direction": trade_dir,
                        "mfe": dir_pips,
                        "mae": pdata.get("mae_pips", 0.0),
                        "survived_threshold": True,
                        "reached_10pips": False,
                        "exit_pips": dir_pips,
                        "is_false_breakout": False
                    }
            else:
                pos = active_positions[pair]
                pos["mfe"] = max(pos["mfe"], dir_pips)
                pos["mae"] = min(pos["mae"], dir_pips)

                if dir_pips < threshold:
                    pos["survived_threshold"] = False

                if dir_pips >= 10.0:
                    pos["reached_10pips"] = True

                if not pos["reached_10pips"] and dir_pips <= threshold:
                    pos["is_false_breakout"] = True

                if state == "INVALIDATED" or dir_pips < -threshold or pdata.get("entry_zone") == "CHASING":
                    pos["exit_pips"] = dir_pips
                    trades.append(pos)
                    del active_positions[pair]

    for pair, pos in active_positions.items():
        trades.append(pos)

    if not trades:
        return {
            "threshold": threshold,
            "min_confirmation_runs": min_confirmation_runs,
            "max_entry_pips": max_entry_pips,
            "number_of_trades": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0
        }

    gains = [t["exit_pips"] for t in trades if t["exit_pips"] > 0]
    pertes = [abs(t["exit_pips"]) for t in trades if t["exit_pips"] < 0]
    
    total_gains = sum(gains)
    total_pertes = sum(pertes)

    win_count = len(gains)
    total_trades = len(trades)
    win_rate = (win_count / total_trades) * 100.0 if total_trades > 0 else 0.0
    
    profit_factor = (total_gains / total_pertes) if total_pertes > 0 else (total_gains if total_gains > 0 else 1.0)
    expectancy = (sum(t["exit_pips"] for t in trades) / total_trades) if total_trades > 0 else 0.0

    mfes = [t["mfe"] for t in trades]
    maes = [t["mae"] for t in trades]

    survived_count = sum(1 for t in trades if t["survived_threshold"])
    false_breakout_count = sum(1 for t in trades if t["is_false_breakout"])

    return {
        "threshold": threshold,
        "min_confirmation_runs": min_confirmation_runs,
        "max_entry_pips": max_entry_pips,
        "number_of_trades": total_trades,
        "win_rate": round(win_rate, 2),
        "average_gain": round(statistics.mean(gains), 2) if gains else 0.0,
        "average_loss": round(statistics.mean(pertes), 2) if pertes else 0.0,
        "profit_factor": round(profit_factor, 2),
        "expectancy": round(expectancy, 2),
        "median_mfe": round(statistics.median(mfes), 2) if mfes else 0.0,
        "median_mae": round(statistics.median(maes), 2) if maes else 0.0,
        "average_mfe": round(statistics.mean(mfes), 2) if mfes else 0.0,
        "average_mae": round(statistics.mean(maes), 2) if maes else 0.0,
        "threshold_survival_rate": round((survived_count / total_trades) * 100.0, 2) if total_trades > 0 else 0.0,
        "false_breakout_rate": round((false_breakout_count / total_trades) * 100.0, 2) if total_trades > 0 else 0.0,
    }


if __name__ == "__main__":
    audit_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rsb_audit_trail.jsonl")
    res = run_parametric_backtest(audit_file, threshold=5.0, min_confirmation_runs=2)
    print("📊 COMPTE-RENDU BACKTEST PARAMÉTRIQUE RSB (WITH TRADE ELIGIBILITY GATE):")
    print(json.dumps(res, indent=2, ensure_ascii=False))
