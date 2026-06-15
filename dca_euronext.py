#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dca_euronext.py

Reproduction Python fidele de l'indicateur Pine "DCA 5% Euronext Special".

Strategie DCA (achat echelonne a la baisse) sur 8 ETF Euronext: quand le close
tombe de DCA_PCT (5%) sous le plus-haut suivi, on entre en cycle; on achete a
chaque palier -5% supplementaire; on sort quand le close repasse au-dessus du
Niveau 0. Trois strategies suivies en parallele:
  - DCA classique : DCA_AMOUNT a chaque palier;
  - Achat groupe  : on attend un retournement PSAR (croisement haussier + bougie
                    verte au-dessus du SAR) puis on achete tous les paliers en
                    attente d'un coup, pres du creux;
  - Achat regulier: REGULAR_AMOUNT le 5 du mois (decale au lundi si week-end).

Donnees: fetch_tv_ohlc (TradingView), journalier, symboles EURONEXT:xxx.

Deux modes (objectif "C"):
  python dca_euronext.py             # LIVE: alertes Telegram pour la derniere barre
  python dca_euronext.py --backtest  # BACKTEST: perf des 3 strategies sur l'historique

Le script ne donne pas de conseil financier.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd

from ichimoku_v4 import fetch_tv_ohlc, send_telegram_message

PARIS_TZ = ZoneInfo("Europe/Paris")

# Envoi Telegram: a partir de SEND_FROM_HOUR (Paris) et une seule fois par jour
# (dedup par date de la derniere barre, memorisee dans STATE_PATH).
SEND_FROM_HOUR = 20
STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dca_euronext_state.json")

# ---- Parametres (identiques au Pine) ----
# Symboles complets EXCHANGE:TICKER (exchange par actif: Euronext + NASDAQ pour SPCX).
ASSETS = [
    "EURONEXT:PUST", "EURONEXT:ISOE", "EURONEXT:PSP5", "EURONEXT:CL2",
    "EURONEXT:LWLD", "EURONEXT:PAEEM", "EURONEXT:WQTM", "EURONEXT:LQQ",
    "EURONEXT:SEME", "NASDAQ:SPCX",
]
DEFAULT_EXCHANGE = "EURONEXT"
DCA_PCT = 0.05
DCA_AMOUNT = 500.0
REGULAR_AMOUNT = 500.0
REGULAR_TICKERS = ["PUST"]         # actif(s) suivi(s) en achat regulier (rappel global)
SAR_START, SAR_STEP, SAR_MAX = 0.1, 0.1, 0.2
CANDLES = 5000                     # profondeur d'historique journalier

# Indice suivi par chaque ETF (affiche dans le message). ISOE non confirme.
INDEX_MAP = {
    "LQQ": "Nasdaq-100 ×2",
    "PUST": "Nasdaq-100",
    "ISOE": "Or",
    "PSP5": "S&P 500",
    "PAEEM": "Pays Émergents",
    "WQTM": "Ordinateur Quantique",
    "LWLD": "Monde ×2",                 # Amundi MSCI World Daily ×2 Leveraged
    "CL2": "USA ×2",                    # Amundi MSCI USA Daily ×2 Leveraged
    "SEME": "Semiconducteurs",          # iShares MSCI Global Semiconductors (IE000I8KRLL9)
    "SPCX": "SpaceX",                   # NASDAQ:SPCX — Space Exploration Technologies Corp
}
DISCLAIMER = "⚠️ Investir = risques"


# --------------------------------------------------------------------------- #
# Indicateurs                                                                  #
# --------------------------------------------------------------------------- #
def parabolic_sar(high: list[float], low: list[float],
                  af_start=SAR_START, af_step=SAR_STEP, af_max=SAR_MAX) -> list[float]:
    """ta.sar(start, inc, max): renvoie la valeur du SAR par barre."""
    n = len(high)
    sar = [math.nan] * n
    if n < 2:
        return sar
    trend = 1 if high[1] >= high[0] else -1
    if trend == 1:
        ep, sar_v = high[0], low[0]
    else:
        ep, sar_v = low[0], high[0]
    af = af_start
    sar[0] = sar_v
    for i in range(1, n):
        sar_v = sar_v + af * (ep - sar_v)
        if trend == 1:
            sar_v = min(sar_v, low[i - 1], low[i - 2] if i >= 2 else low[i - 1])
            if low[i] < sar_v:                       # retournement -> baissier
                trend, sar_v, ep, af = -1, ep, low[i], af_start
            elif high[i] > ep:
                ep, af = high[i], min(af + af_step, af_max)
        else:
            sar_v = max(sar_v, high[i - 1], high[i - 2] if i >= 2 else high[i - 1])
            if high[i] > sar_v:                      # retournement -> haussier
                trend, sar_v, ep, af = 1, ep, high[i], af_start
            elif low[i] < ep:
                ep, af = low[i], min(af + af_step, af_max)
        sar[i] = sar_v
    return sar


def is_regular_buy_day(d: date) -> bool:
    """Le 5 du mois, decale au lundi si le 5 tombe un week-end (=> 6 ou 7)."""
    wd = d.weekday()                                 # lundi=0 ... dimanche=6
    if d.day == 5 and wd not in (5, 6):
        return True
    if d.day == 6 and wd == 0:                       # 5 = dimanche -> lundi 6
        return True
    if d.day == 7 and wd == 0:                       # 5 = samedi -> lundi 7
        return True
    return False


# --------------------------------------------------------------------------- #
# Moteur: rejoue les barres une a une (fidele a l'automate Pine)              #
# --------------------------------------------------------------------------- #
def _strategy_result(invested: float, units: float, close: float) -> dict | None:
    if invested <= 0.0 or units <= 0.0:
        return None
    final = units * close
    return {
        "invested": invested,
        "final": final,
        "gain": final - invested,
        "gain_pct": (final - invested) / invested * 100.0,
        "avg_price": invested / units,
    }


def replay(df: pd.DataFrame, ticker: str) -> tuple[list[dict], dict]:
    """Renvoie (cycles_termines, evenements_de_la_derniere_barre)."""
    o = df["open"].astype(float).tolist()
    h = df["high"].astype(float).tolist()
    low_ = df["low"].astype(float).tolist()
    c = df["close"].astype(float).tolist()
    dts = [pd.Timestamp(t).date() for t in df.index]
    sar = parabolic_sar(h, low_)
    n = len(c)

    trackingHigh = h[0]
    zeroLevel = lastDcaLevel = None
    lastDisplayedZeroClose = None
    dcaIndex = 0
    inDcaCycle = False
    groupedBuyArmed = False
    lastGroupedBuyDcaIndex = 0
    dcaEntryPrices: list[float] = []
    cycleStart = None
    cyc_inv = cyc_u = grp_inv = grp_u = reg_inv = reg_u = 0.0
    prev_irbd = False

    cycles: list[dict] = []
    last: dict = {}

    for i in range(n):
        oi, hi, li, ci, di = o[i], h[i], low_[i], c[i], dts[i]
        psar_i = sar[i]
        psar_cross_up = i > 0 and ci > psar_i and c[i - 1] <= sar[i - 1]
        green = ci > oi
        above_psar = li > psar_i

        irbd = is_regular_buy_day(di)
        regular_due = irbd and not prev_irbd
        regular_signal = regular_due and ticker in REGULAR_TICKERS

        pre_alert = False
        dca_signal_index = None
        grouped_signal = False
        grouped_count = None
        grouped_price = None
        back_to_normal = False
        results = None
        potential_pct = None
        grouped_potential = None

        if not inDcaCycle:
            if hi > trackingHigh:
                trackingHigh = hi
            candidate_zero = trackingHigh * (1.0 - DCA_PCT)
            if ci <= candidate_zero:
                inDcaCycle = True
                zeroLevel = lastDcaLevel = candidate_zero
                dcaIndex = 0
                groupedBuyArmed = False
                lastGroupedBuyDcaIndex = 0
                dcaEntryPrices = []
                cycleStart = di
                cyc_inv = cyc_u = grp_inv = grp_u = reg_inv = reg_u = 0.0
                if regular_due and ci > 0:
                    reg_inv += REGULAR_AMOUNT
                    reg_u += REGULAR_AMOUNT / ci
                if lastDisplayedZeroClose is None or ci < lastDisplayedZeroClose:
                    lastDisplayedZeroClose = ci
                    pre_alert = True
        else:
            next_level = lastDcaLevel * (1.0 - DCA_PCT)
            if ci <= next_level:
                prev_idx = dcaIndex
                while ci <= next_level:
                    lastDcaLevel = next_level
                    dcaIndex += 1
                    dcaEntryPrices.append(ci)
                    if ci > 0:
                        cyc_inv += DCA_AMOUNT
                        cyc_u += DCA_AMOUNT / ci
                    next_level = lastDcaLevel * (1.0 - DCA_PCT)
                if prev_idx == 0 and dcaIndex >= 1:
                    lastDisplayedZeroClose = None
                dca_signal_index = dcaIndex

            if regular_due and ci > 0:
                reg_inv += REGULAR_AMOUNT
                reg_u += REGULAR_AMOUNT / ci

            if dcaIndex >= 1:
                grouped_ready = dcaIndex > lastGroupedBuyDcaIndex
                if grouped_ready and psar_cross_up:
                    groupedBuyArmed = True
                if grouped_ready and groupedBuyArmed and green and above_psar:
                    grouped_signal = True
                    grouped_count = dcaIndex - lastGroupedBuyDcaIndex
                    amt = grouped_count * DCA_AMOUNT
                    if ci > 0:
                        grp_inv += amt
                        grp_u += amt / ci
                    grouped_price = ci
                    grouped_potential = (zeroLevel / ci - 1.0) * 100.0 if zeroLevel and ci > 0 else None
                    lastGroupedBuyDcaIndex = dcaIndex
                    groupedBuyArmed = False

            if dca_signal_index is not None and zeroLevel > 0 and lastDcaLevel > 0:
                potential_pct = (zeroLevel / lastDcaLevel - 1.0) * 100.0

            if ci > zeroLevel:
                back_to_normal = dcaIndex >= 1
                if back_to_normal and dcaEntryPrices:
                    avg_gain = sum((ci / p - 1.0) * 100.0 for p in dcaEntryPrices) / len(dcaEntryPrices)
                    results = {
                        "start": cycleStart, "end": di, "n_dca": dcaIndex,
                        "avg_gain_pct": avg_gain,
                        "dca": _strategy_result(cyc_inv, cyc_u, ci),
                        "grouped": _strategy_result(grp_inv, grp_u, ci),
                        "regular": _strategy_result(reg_inv, reg_u, ci),
                    }
                    cycles.append(results)
                # reset
                inDcaCycle = False
                trackingHigh = hi
                zeroLevel = lastDcaLevel = None
                dcaIndex = 0
                groupedBuyArmed = False
                lastGroupedBuyDcaIndex = 0
                dcaEntryPrices = []
                cyc_inv = cyc_u = grp_inv = grp_u = reg_inv = reg_u = 0.0

        prev_irbd = irbd

        if i == n - 1:
            last = {
                "date": di, "close": ci, "in_cycle": inDcaCycle, "dca_index": dcaIndex,
                "pre_alert": pre_alert, "dca_signal_index": dca_signal_index,
                "potential_pct": potential_pct, "grouped_signal": grouped_signal,
                "grouped_count": grouped_count, "grouped_price": grouped_price,
                "grouped_potential": grouped_potential,
                "back_to_normal": back_to_normal, "results": results,
                "regular_signal": regular_signal,
            }
    return cycles, last


# --------------------------------------------------------------------------- #
# Mode LIVE: alertes Telegram pour la derniere barre                          #
# --------------------------------------------------------------------------- #
def _stamp() -> str:
    return datetime.now(PARIS_TZ).strftime("%Y-%m-%d %H:%M") + " Paris"


def live_alerts(ticker: str, last: dict) -> list[str]:
    """Construit les messages d'alerte pour la derniere barre (fidele au Pine)."""
    msgs = []
    px = f"{last['close']:.2f}"
    idx = INDEX_MAP.get(ticker)

    def _head(hook: str) -> str:
        idx_lines = f"\n{ticker} = {idx}\n{hook}" if idx else ""
        return f"💼 ETF DCA\n\n📊 Actif :  {ticker}{idx_lines}\n\n💰 Prix {px}\n\n"

    # Phase d'accumulation: incitation a entrer. Fin de cycle: on recolte.
    head = _head(f"{idx}, ça t'intéresse ?")
    harvest_head = _head("🍒 Récolte les fruits !")
    foot = f"\n\n{DISCLAIMER}\n⏰ {_stamp()}"
    if last["pre_alert"]:
        msgs.append(head + "🟠 Pré-alerte DCA" + foot)
    if last["dca_signal_index"]:
        n = last["dca_signal_index"]
        txt = "DCA 20+" if n > 20 else f"DCA {n}"
        pot = "" if last["potential_pct"] is None else f"\n📈 Potentiel = {last['potential_pct']:+.2f}%"
        msgs.append(head + f"🔴 Signal: {txt}{pot}" + foot)
    if last["grouped_signal"]:
        pot = "" if last.get("grouped_potential") is None else f"\n📈 Potentiel = {last['grouped_potential']:+.2f}%"
        msgs.append(head + f"🟩 Achat groupé\n📦 DCA groupés = {last['grouped_count']}\n"
                           f"💵 Prix achat = {last['grouped_price']:.2f}{pot}" + foot)
    if last["back_to_normal"] and last["results"]:
        r = last["results"]
        body = harvest_head + f"🟢 Retour à la normale ({r['n_dca']} DCA)\n"
        if r["dca"]:
            body += f"\n📌 DCA — PMA {r['dca']['avg_price']:.2f} · {r['dca']['gain_pct']:+.2f}%"
        body += (f"\n🟩 Groupé — PMA {r['grouped']['avg_price']:.2f} · {r['grouped']['gain_pct']:+.2f}%"
                 if r["grouped"] else "\n🟩 Groupé — aucun achat")
        body += (f"\n⚪ Régulier — PMA {r['regular']['avg_price']:.2f} · {r['regular']['gain_pct']:+.2f}%"
                 if r["regular"] else "\n⚪ Régulier — aucun achat")
        msgs.append(body + foot)
    # NB: l'achat regulier ne produit PAS de message ici (il est global, voir
    # regular_global_message + main): il concerne le(s) actif(s) suivi(s) en
    # regulier, pas l'actif courant de l'iteration.
    return msgs


def regular_global_message() -> str:
    """Message GLOBAL (unique) du jour d'achat regulier — accroche engageante."""
    return ("💼 ETF DCA — Achat régulier\n\n"
            "C'est le grand jour\n\n"
            "📌 Quel Actif tu suis ?\n\n"
            "💶 Quel Montant ?\n\n"
            f"{DISCLAIMER}\n\n"
            f"⏰ {_stamp()}")


# --------------------------------------------------------------------------- #
# Mode BACKTEST: perf des 3 strategies sur l'historique                        #
# --------------------------------------------------------------------------- #
def backtest_report(ticker: str, cycles: list[dict]) -> str:
    if not cycles:
        return f"== {ticker} ==  aucun cycle DCA sur l'historique"
    lines = [f"== {ticker} ==  {len(cycles)} cycles DCA"]
    agg = {k: [0.0, 0.0] for k in ("dca", "grouped", "regular")}   # [invested, final]
    for cy in cycles:
        parts = []
        for k, lbl in (("dca", "DCA"), ("grouped", "Grp"), ("regular", "Reg")):
            s = cy[k]
            if s:
                parts.append(f"{lbl} {s['gain_pct']:+.1f}%")
                agg[k][0] += s["invested"]
                agg[k][1] += s["final"]
        lines.append(f"  {cy['start']}→{cy['end']}  x{cy['n_dca']:<2}  " + " · ".join(parts))
    lines.append("  --- cumul (toutes périodes) ---")
    for k, lbl in (("dca", "DCA classique"), ("grouped", "Achat groupé"), ("regular", "Achat régulier")):
        inv, fin = agg[k]
        if inv > 0:
            lines.append(f"  {lbl:<15} investi {inv:,.0f}€  →  {fin:,.0f}€  "
                         f"({(fin - inv) / inv * 100:+.1f}%)")
    return "\n".join(lines)


def _load_state() -> dict:
    try:
        with open(STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(data: dict) -> None:
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="DCA 5% Euronext — version Python (live + backtest).")
    parser.add_argument("--backtest", action="store_true", help="Mode backtest (perf historique, pas de Telegram).")
    parser.add_argument("--no-telegram", action="store_true", help="Live sans envoi Telegram (affichage seul).")
    parser.add_argument("--force", action="store_true", help="Ignore le garde 20h et la dedup (test).")
    parser.add_argument("--assets", nargs="*", default=ASSETS, help="Sous-ensemble d'actifs.")
    parser.add_argument("--candles", type=int, default=CANDLES, help="Profondeur d'historique (barres).")
    args = parser.parse_args()

    all_alerts: list[str] = []
    last_dates: list[date] = []
    regular_today = False
    for symbol in args.assets:
        sym = symbol if ":" in symbol else f"{DEFAULT_EXCHANGE}:{symbol}"
        ticker = sym.split(":")[-1]
        df = fetch_tv_ohlc(sym, "D", args.candles)
        # >=2 barres suffisent (PSAR a besoin de 2); les nouvelles cotations
        # comme SPCX (SpaceX, IPO recente) sont suivies des qu'elles ont un peu
        # d'historique, le 1er plus-haut servant de reference de drawdown.
        if df is None or df.empty or len(df) < 2:
            print(f"{ticker}: pas de données")
            continue
        cycles, last = replay(df, ticker)

        if args.backtest:
            print(backtest_report(ticker, cycles))
            print()
        else:
            msgs = live_alerts(ticker, last)
            tag = "EN CYCLE" if last.get("in_cycle") else "normal"
            print(f"{ticker}: {last.get('date')} close {last.get('close', float('nan')):.2f} "
                  f"[{tag}, DCA{last.get('dca_index', 0)}] — {len(msgs)} alerte(s)")
            all_alerts.extend(msgs)
            if last.get("regular_signal"):
                regular_today = True
            if last.get("date"):
                last_dates.append(last["date"])

    if args.backtest:
        return 0

    # Achat regulier: UN SEUL message global (le jour venu), en tete des alertes.
    if regular_today:
        all_alerts.insert(0, regular_global_message())

    for m in all_alerts:
        print("\n----- ALERTE -----\n" + m)

    # Envoi: a partir de 20h Paris, une seule fois par jour (dedup par date de barre).
    now = datetime.now(PARIS_TZ)
    data_date = str(max(last_dates)) if last_dates else None
    already = data_date is not None and _load_state().get("last_sent_date") == data_date

    if not all_alerts:
        print("Aucune alerte aujourd'hui.")
    elif args.no_telegram:
        print(f"{len(all_alerts)} alerte(s) — Telegram désactivé (--no-telegram).")
    elif not args.force and now.hour < SEND_FROM_HOUR:
        print(f"Avant {SEND_FROM_HOUR}h ({now:%H:%M} Paris) — envoi différé, {len(all_alerts)} alerte(s) en attente.")
    elif not args.force and already:
        print(f"Déjà envoyé pour la barre du {data_date} — pas de renvoi.")
    else:
        for m in all_alerts:
            send_telegram_message(m)
        if data_date:
            _save_state({"last_sent_date": data_date})
        print(f"\nTelegram: {len(all_alerts)} alerte(s) envoyée(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
