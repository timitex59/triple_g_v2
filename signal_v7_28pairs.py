#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
signal_v7_28pairs.py
====================
Fusion v6 (1H + Daily + Weekly) + v6 MINI (15m + 1H + Daily).

Objectif:
  - garder la logique de signal principale v6
  - confirmer chaque paire avec la version MINI
  - afficher 🔥 sur les paires v6 confirmees par MINI dans Telegram

Usage :
  python signal_v7_28pairs.py
  python signal_v7_28pairs.py --pair EURJPY
  python signal_v7_28pairs.py --filter ROBUST
  python signal_v7_28pairs.py --loop --interval 3600
  python signal_v7_28pairs.py --no-csv
"""

import argparse
import json
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import signal_v6_28pairs as v6
import signal_v6_28pairs_mini as v6mini


PAIRS_CONFIG = v6.PAIRS_CONFIG
robustness_label = v6.robustness_label
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXIT_STATE_FILE = os.path.join(SCRIPT_DIR, "signal_v7_exit_state.json")
EXIT_START_HOUR = 6
EXIT_END_HOUR = 22


def _empty_exit_state():
    return {
        "date": "",
        "window_active": False,
        "prev_open": {},
        "prev_trail": {},
        "exit_pairs": {},
    }


def _load_exit_state():
    if not os.path.exists(EXIT_STATE_FILE):
        return _empty_exit_state()
    try:
        with open(EXIT_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        st = _empty_exit_state()
        if isinstance(data, dict):
            st.update(data)
        for key in ("prev_open", "prev_trail", "exit_pairs"):
            if not isinstance(st.get(key), dict):
                st[key] = {}
        return st
    except Exception:
        return _empty_exit_state()


def _save_exit_state(state):
    try:
        with open(EXIT_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _collect_open_trail(results):
    seen_open = {}
    seen_trail = {}
    for r in results:
        if r.get("error") or (not r["longs"] and not r["shorts"]):
            continue
        icon = "🟢" if r["longs"] else "🔴"
        has_trail = (r["longs"] and r["lt"]) or (r["shorts"] and r["st_arm"])
        if has_trail:
            seen_trail[r["pair"]] = icon
        else:
            seen_open[r["pair"]] = icon
    return seen_open, seen_trail


def _update_exit_pairs(results):
    now_paris = datetime.now(ZoneInfo("Europe/Paris"))
    today = now_paris.strftime("%Y-%m-%d")
    in_window = EXIT_START_HOUR <= now_paris.hour < EXIT_END_HOUR

    current_open, current_trail = _collect_open_trail(results)
    current_active = set(current_open) | set(current_trail)

    st = _load_exit_state()
    if st.get("date") != today:
        st = _empty_exit_state()
        st["date"] = today

    if not in_window:
        st["window_active"] = False
        st["prev_open"] = current_open
        st["prev_trail"] = current_trail
        st["exit_pairs"] = {}
        _save_exit_state(st)
        return {}

    if not st.get("window_active"):
        st["window_active"] = True
        st["date"] = today
        st["prev_open"] = current_open
        st["prev_trail"] = current_trail
        st["exit_pairs"] = {}
        _save_exit_state(st)
        return {}

    prev_open = st.get("prev_open", {})
    prev_trail = st.get("prev_trail", {})
    exits = st.get("exit_pairs", {})

    for pair in prev_open:
        if pair not in current_active:
            exits[pair] = prev_open.get(pair, "⚪")

    for pair in prev_trail:
        if pair not in current_active:
            exits[pair] = prev_trail.get(pair, "⚪")

    for pair in list(exits.keys()):
        if pair in current_active:
            exits.pop(pair, None)

    st["prev_open"] = current_open
    st["prev_trail"] = current_trail
    st["exit_pairs"] = exits
    st["date"] = today
    st["window_active"] = True
    _save_exit_state(st)
    return exits


def _is_mini_confirmed(main_r, mini_r):
    if main_r.get("error") or mini_r.get("error"):
        return False
    main_bias = main_r.get("biais")
    mini_bias = mini_r.get("biais")
    if main_bias not in ("BULL", "BEAR"):
        return False
    if mini_bias != main_bias:
        return False

    mini_has_activity = bool(mini_r.get("longs") or mini_r.get("shorts") or mini_r.get("sig") != "AUCUN")
    if not mini_has_activity:
        return False

    if main_r.get("longs"):
        return bool(mini_r.get("longs") or ("LONG" in str(mini_r.get("sig", ""))))
    if main_r.get("shorts"):
        return bool(mini_r.get("shorts") or ("SHORT" in str(mini_r.get("sig", ""))))
    return mini_r.get("sig") != "AUCUN"


def telegram_text(results):
    now = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M")
    lines = []

    def fmt_chg(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return "n/a"
        return f"{v:+.2f}%"

    def flame(r):
        return "\U0001F525" if r and r.get("confirmed_mini") else ""

    def chg_icon(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return "\u26AA"
        return "\U0001F7E2" if v >= 0 else "\U0001F534"

    seen_open, seen_trail = _collect_open_trail(results)
    exit_pairs = _update_exit_pairs(results)

    new_entries = [r for r in results if not r.get("error") and r["sig"] != "AUCUN"]

    if seen_trail:
        lines.append("TRAILING")
    elif new_entries:
        lines.append("ENTREE")

    if new_entries:
        if seen_trail:
            lines.append("")
            lines.append("Entree")
        for r in new_entries:
            icon = "\U0001F7E2" if "LONG" in r["sig"] else "\U0001F534"
            chg = r.get("chg_cc_daily")
            lines.append(f"  {icon}{chg_icon(chg)}{r['pair']} ({fmt_chg(chg)}){flame(r)}")

    if seen_open:
        lines.append("")
        lines.append("Open")
        for pair, icon in seen_open.items():
            rr = next((x for x in results if x.get("pair") == pair), None)
            chg = rr.get("chg_cc_daily") if rr else np.nan
            lines.append(f"  {icon}{chg_icon(chg)}{pair} ({fmt_chg(chg)}){flame(rr)}")

    if seen_trail:
        lines.append("")
        lines.append("Trailing")
        for pair, icon in seen_trail.items():
            rr = next((x for x in results if x.get("pair") == pair), None)
            chg = rr.get("chg_cc_daily") if rr else np.nan
            lines.append(f"  {icon}{chg_icon(chg)}{pair} ({fmt_chg(chg)}){flame(rr)}")

    if exit_pairs:
        lines.append("")
        lines.append("EXIT")
        for pair, icon in exit_pairs.items():
            lines.append(f"  {icon}{pair}")

    if not new_entries and not seen_open and not seen_trail and not exit_pairs:
        lines.append("\u26AA Aucune position active")

    lines.append("")
    lines.append(f"\u23F0 {now} Paris")
    return "\n".join(lines)


def export_csv(results, fname="signal_v7_28pairs_current.csv"):
    rows = []
    for r in results:
        if r.get("error"):
            rows.append({"pair": r["pair"], "label": r["label"], "rob": r["rob"], "error": True})
            continue
        rows.append(
            {
                "pair": r["pair"],
                "label": r["label"],
                "rob": r["rob"],
                "timestamp_utc": str(r["ts"])[:16],
                "biais": r["biais"],
                "filters_ok": r["fok"],
                "close": round(r["close"], 5),
                "sar_1h": round(r["sar"], 5),
                "ref_level": round(r["ref"], 5) if not np.isnan(r["ref"]) else "",
                "bull_vw0": round(r["bv"], 5) if not np.isnan(r["bv"]) else "",
                "bear_vw0": round(r["brv"], 5) if not np.isnan(r["brv"]) else "",
                "open_longs": len(r["longs"]),
                "open_shorts": len(r["shorts"]),
                "trailing_long": r["lt"],
                "trailing_short": r["st_arm"],
                "neo_long_armed": r["nla"],
                "neo_short_armed": r["nsa"],
                "signal_last_bar": r["sig"],
                "wr_hist_pct": round(r["wr"], 1),
                "n_trades_hist": r["nt"],
                "watch": " | ".join(r["watch"]),
                "confirmed_mini": bool(r.get("confirmed_mini")),
                "mini_biais": r.get("mini_biais", ""),
            }
        )
    pd.DataFrame(rows).to_csv(fname, index=False)
    return fname


def run_scan(target_pairs, write_csv=True):
    t0 = time.time()
    now = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n  Scan v7 {len(target_pairs)} paires -- {now}")
    print("  Source : TradingView WebSocket (OANDA)")
    print("  Fusion : v6 (1H/D/W) + mini (15m/1H/D)")

    results = []
    for i, pair in enumerate(target_pairs, 1):
        print(f"  [{i:>2}/{len(target_pairs)}] {pair}...", end=" ", flush=True)
        try:
            r_main = v6.analyse(pair)
            r_mini = v6mini.analyse(pair)

            if r_main.get("error"):
                print("ERREUR")
                results.append(r_main)
                continue

            confirmed = _is_mini_confirmed(r_main, r_mini)
            r_main["confirmed_mini"] = confirmed
            r_main["mini_biais"] = r_mini.get("biais", "ERREUR") if not r_mini.get("error") else "ERREUR"

            biais = r_main["biais"]
            pos_s = ""
            if r_main["longs"]:
                pos_s += f"+{len(r_main['longs'])}L"
            if r_main["shorts"]:
                pos_s += f"-{len(r_main['shorts'])}S"
            sig_s = f" *** {r_main['sig']} ***" if r_main["sig"] != "AUCUN" else ""
            flg = " 🔥" if confirmed else ""
            print(f"OK  {biais:<6} {pos_s}{sig_s}{flg}")
            results.append(r_main)
        except Exception as e:
            print(f"ERREUR ({e})")
            results.append(
                {
                    "pair": pair,
                    "error": True,
                    "label": robustness_label(PAIRS_CONFIG[pair][7]),
                    "rob": PAIRS_CONFIG[pair][7],
                }
            )

    v6.print_summary(results)
    v6.print_active_signals(results)

    csv_f = export_csv(results) if write_csv else None
    tg = telegram_text(results)
    v6.send_telegram(tg)

    elapsed = time.time() - t0
    if csv_f:
        print(f"\n  -> {csv_f}")
    else:
        print("\n  -> Export CSV desactive")
    print(f"  Temps total : {elapsed:.1f}s  ({elapsed/len(target_pairs):.1f}s/paire)")
    print(f"{'='*90}\n")
    return results


def main():
    parser = argparse.ArgumentParser(description="Signal v7 fusion LIVE -- 28 paires TradingView")
    parser.add_argument("--pair", default=None, help="Paire unique (ex: EURJPY)")
    parser.add_argument("--filter", default=None, help="Filtrer par label: ROBUST / MOYEN / FRAGILE")
    parser.add_argument("--loop", action="store_true", help="Mode boucle horaire")
    parser.add_argument("--interval", type=int, default=3600, help="Intervalle boucle (secondes)")
    parser.add_argument("--no-csv", action="store_true", help="Desactive la generation du fichier CSV")
    args = parser.parse_args()

    if args.pair:
        p = args.pair.upper()
        if p not in PAIRS_CONFIG:
            print(f"Paire {p} inconnue. Disponibles: {', '.join(PAIRS_CONFIG)}")
            return
        target = [p]
    elif args.filter:
        lbl = args.filter.upper()
        target = [p for p, cfg in PAIRS_CONFIG.items() if robustness_label(cfg[7]) == lbl]
        if not target:
            print(f"Aucune paire avec label '{lbl}'")
            return
        print(f"Filtre '{lbl}' -> {len(target)} paires : {', '.join(target)}")
    else:
        target = list(PAIRS_CONFIG.keys())

    if args.loop:
        print(f"Mode LOOP -- scan toutes les {args.interval}s  (Ctrl+C pour arreter)")
        while True:
            try:
                run_scan(target, write_csv=not args.no_csv)
            except KeyboardInterrupt:
                print("\nArret.")
                break
            except Exception as e:
                print(f"Erreur scan: {e}")
            print(f"Prochain scan dans {args.interval}s...")
            time.sleep(args.interval)
    else:
        run_scan(target, write_csv=not args.no_csv)


if __name__ == "__main__":
    main()
