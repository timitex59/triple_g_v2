#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CLI backtest FIOS (Phase 2).

    python -m fios.backtest                 # backtest historique de la technique
    python -m fios.backtest --mode journal  # denoue le journal live + stats
    python -m fios.backtest --mode both
    python -m fios.backtest --limit 6 --verbose

Historique : rejoue la famille technique sur l'historique TradingView et sort
les stats (edge immediat). Journal : denoue les signaux live murus et met a jour
fios_stats.json (s'enrichit jour apres jour).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

from .. import config as cfg
from .. import tv_feed
from . import history
from . import journal as journal_mod
from . import stats as stats_mod

load_dotenv()
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
except Exception:
    pass


def _fmt_summary(s: dict) -> str:
    if s.get("trades", 0) == 0:
        return "aucun trade"
    pf = s["profit_factor"]
    pf_txt = f"{pf:.2f}" if pf is not None else "∞"
    return (f"{s['trades']:>4} trades | WR {s['win_rate']:>5.1f}% | "
            f"R moy {s['avg_r']:+.2f} | PF {pf_txt:>4} | "
            f"R cumul {s['sum_r']:+.1f} | DD {s['max_drawdown_r']:.1f}R")


def _print_grouped(title: str, groups: dict[str, dict], min_trades: int = 1) -> None:
    print(f"\n{title}")
    rows = [(k, v) for k, v in groups.items() if v.get("trades", 0) >= min_trades]
    if not rows:
        print("  (rien)")
        return
    width = max(len(k) for k, _ in rows)
    for k, v in rows:
        print(f"  {k:<{width}}  {_fmt_summary(v)}")


def run_history(pairs, verbose: bool) -> dict:
    print("FIOS Backtest — famille TECHNIQUE (historique TradingView)")
    print(f"TF={cfg.BT_TF}  SL={cfg.BT_SL_ATR}xATR  TP={cfg.BT_TP_ATR}xATR  "
          f"horizon={cfg.BT_MAX_BARS}  seuil={cfg.BT_ENTRY_THRESHOLD}")
    trades = history.backtest_all(pairs, verbose=verbose)
    overall = stats_mod.summarize(trades)

    print("\n" + "=" * 64)
    print(f"GLOBAL : {_fmt_summary(overall)}")
    print("=" * 64)
    _print_grouped("Par direction :", stats_mod.grouped(trades, lambda t: t["direction"]))
    _print_grouped("Par sortie :", stats_mod.grouped(trades, lambda t: t["exit_reason"]))
    _print_grouped("Par paire (>=5 trades) :",
                   stats_mod.grouped(trades, lambda t: t["pair"]), min_trades=5)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {"tf": cfg.BT_TF, "sl_atr": cfg.BT_SL_ATR, "tp_atr": cfg.BT_TP_ATR,
                   "max_bars": cfg.BT_MAX_BARS, "entry_threshold": cfg.BT_ENTRY_THRESHOLD},
        "n_trades": len(trades),
        "overall": overall,
        "by_direction": stats_mod.grouped(trades, lambda t: t["direction"]),
        "by_exit": stats_mod.grouped(trades, lambda t: t["exit_reason"]),
        "by_pair": stats_mod.grouped(trades, lambda t: t["pair"]),
    }
    with open(cfg.BT_REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nRapport ecrit : {cfg.BT_REPORT_JSON}")
    return report


def run_journal(verbose: bool) -> dict:
    print("\nFIOS Backtest — DENOUEMENT DU JOURNAL LIVE")
    stats = journal_mod.resolve_and_snapshot(verbose=verbose)
    print(f"Trades denoues : {stats['resolved_trades']} | "
          f"signaux ouverts : {stats['open_signals']}")
    if stats["resolved_trades"] == 0:
        print("Journal encore vide/immature — les stats se rempliront jour apres jour.")
        return stats
    print("\n" + "=" * 64)
    print(f"GLOBAL : {_fmt_summary(stats['overall'])}")
    print("=" * 64)
    _print_grouped("Par decision :", stats["by_decision"])
    _print_grouped("Par combinaison de familles :", stats["by_family_combo"])
    _print_grouped("Par nb de familles d'accord :", stats["by_agree"])
    _print_grouped("Par confiance :", stats["by_confidence"])
    _print_grouped("Par qualite :", stats["by_quality"])
    _print_grouped("Par paire :", stats["by_pair"])
    print(f"\nSnapshot ecrit : {cfg.STATS_FILE}")
    return stats


def main() -> None:
    p = argparse.ArgumentParser(description="FIOS — backtest (Phase 2)")
    p.add_argument("--mode", choices=["history", "journal", "both"], default="history")
    p.add_argument("--limit", type=int, default=0, help="Limiter le nb de paires (historique)")
    p.add_argument("--verbose", action="store_true")
    # Overrides de parametres (tuning). Appliques a config avant execution.
    p.add_argument("--sl", type=float, help="Stop en multiples d'ATR (defaut %.1f)" % cfg.BT_SL_ATR)
    p.add_argument("--tp", type=float, help="Cible en multiples d'ATR (defaut %.1f)" % cfg.BT_TP_ATR)
    p.add_argument("--threshold", type=float, help="Seuil |score| d'entree (defaut %.2f)" % cfg.BT_ENTRY_THRESHOLD)
    p.add_argument("--horizon", type=int, help="Horizon max en bougies (defaut %d)" % cfg.BT_MAX_BARS)
    p.add_argument("--tf", choices=list(cfg.TIMEFRAMES.keys()), help="Timeframe de denouement")
    args = p.parse_args()

    # Application des overrides (les fonctions relisent config au moment de l'appel).
    if args.sl is not None:
        cfg.BT_SL_ATR = args.sl
    if args.tp is not None:
        cfg.BT_TP_ATR = args.tp
    if args.threshold is not None:
        cfg.BT_ENTRY_THRESHOLD = args.threshold
    if args.horizon is not None:
        cfg.BT_MAX_BARS = args.horizon
    if args.tf is not None:
        cfg.BT_TF = args.tf

    pairs = cfg.PAIRS[: args.limit] if args.limit else cfg.PAIRS
    if args.mode in ("history", "both"):
        run_history(pairs, args.verbose)
    if args.mode in ("journal", "both"):
        run_journal(args.verbose)
    tv_feed.clear_cache()


if __name__ == "__main__":
    main()
