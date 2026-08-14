#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Croisement FULL ALIGNMENT x FIOS (confluence inter-systemes).

Lit le sidecar `full_alignment_index.json` produit par
renko_full_alignment_29pairs.py (force devises via index DXY/EXY/... +
alignement Renko M/W/D) et le confronte a la force composite de FIOS.

Deux methodologies independantes : quand elles pointent la meme devise dans le
meme sens, c'est une confluence a plus haute conviction. Quand elles divergent,
c'est un signal de prudence.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import pytz
from forex_price_trends import suffix as live_price_suffix
from forex_price_trends import update as update_price_trends

from . import config as cfg

SIDECAR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "full_alignment_index.json")
PARIS = pytz.timezone("Europe/Paris")


def _fresh(payload: dict, today: str) -> bool:
    if payload.get("paris_date") == today:
        return True
    gen = payload.get("generated_at")
    if not gen:
        return False
    try:
        dt = datetime.fromisoformat(gen)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(PARIS).strftime("%Y-%m-%d") == today
    except Exception:
        return False


def load_align() -> dict | None:
    """Retourne le payload du sidecar (clés 'currencies' et 'pairs') s'il existe
    et date d'aujourd'hui (heure de Paris), sinon None."""
    try:
        with open(SIDECAR, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return None
    clock = datetime.now(PARIS)
    today = clock.strftime("%Y-%m-%d")
    if not _fresh(payload, today):
        return None
    if not isinstance(payload.get("currencies"), dict) or not payload["currencies"]:
        return None
    return payload


def _px_tag(a: dict) -> str:
    def s(v):
        return "+" if v == 1 else ("-" if v == -1 else "0")
    return f"M{s(a.get('px_m'))} W{s(a.get('px_w'))} D{s(a.get('px_d'))}"


def _align_score(a: dict) -> float:
    """Score pondere M/W/D dans [-1, 1] (poids 3/2/1 comme le scanner)."""
    m = a.get("px_m") or 0
    w = a.get("px_w") or 0
    d = a.get("px_d") or 0
    return (m * 3 + w * 2 + d * 1) / 6.0


def _tf_counts(a: dict) -> tuple[int, int]:
    """Nombre de timeframes M/W/D haussiers (+1) et baissiers (-1)."""
    vals = [a.get("px_m") or 0, a.get("px_w") or 0, a.get("px_d") or 0]
    return sum(1 for v in vals if v == 1), sum(1 for v in vals if v == -1)


def _solid(tf_up: int, tf_down: int, direction: int) -> bool:
    """Alignement solide : au moins 2 TF dans le sens, aucun a contre-sens.
    Ecarte les alignements sur un seul timeframe."""
    if direction > 0:
        return tf_up >= 2 and tf_down == 0
    if direction < 0:
        return tf_down >= 2 and tf_up == 0
    return False


def confluence(composites: dict[str, dict], align: dict) -> list[dict]:
    recs: list[dict] = []
    for cur in cfg.CURRENCIES:
        comp = composites.get(cur)
        a = align.get(cur)
        if not comp or not a:
            continue
        force = float(comp["composite"])
        ascore = _align_score(a)
        fios_dir = 1 if force > 52 else (-1 if force < 48 else 0)
        align_dir = 1 if ascore > 0 else (-1 if ascore < 0 else 0)
        if fios_dir != 0 and fios_dir == align_dir:
            verdict = "bull" if fios_dir > 0 else "bear"
        elif fios_dir != 0 and align_dir != 0 and fios_dir != align_dir:
            verdict = "divergence"
        else:
            verdict = "neutre"
        # Divergence intraday : la structure M/W/D et la variation du jour vont
        # en sens opposes. "repli" = structure haussiere mais jour en baisse
        # (souvent un pullback) ; "rebond" = structure baissiere mais jour en
        # hausse (souvent un rebond a vendre).
        chg = a.get("daily_chg")
        intra = None
        if isinstance(chg, (int, float)) and align_dir != 0:
            if align_dir > 0 and chg < 0:
                intra = "repli"
            elif align_dir < 0 and chg > 0:
                intra = "rebond"
        tf_up, tf_down = _tf_counts(a)
        recs.append({
            "currency": cur,
            "force": round(force, 0),
            "align_score": round(ascore, 2),
            "tag": _px_tag(a),
            "daily_chg": chg,
            "verdict": verdict,
            "intra": intra,
            "tf_up": tf_up, "tf_down": tf_down,
            "strength": round(abs(force - 50) + abs(ascore) * 50, 1),
        })
    return recs


def build_section(composites: dict[str, dict], payload: dict | None) -> list[str]:
    if not payload:
        return []
    recs = confluence(composites, payload.get("currencies", {}))

    def _num(r) -> bool:
        return isinstance(r["daily_chg"], (int, float))

    # STRICT : une devise n'est retenue que si TOUT est aligne — force FIOS,
    # variation du jour, ET alignement RENKO solide (>=2 TF, 0 a contre-sens).
    bulls = sorted([r for r in recs if r["verdict"] == "bull" and _num(r)
                    and r["daily_chg"] > 0 and _solid(r["tf_up"], r["tf_down"], 1)],
                   key=lambda r: r["strength"], reverse=True)
    bears = sorted([r for r in recs if r["verdict"] == "bear" and _num(r)
                    and r["daily_chg"] < 0 and _solid(r["tf_up"], r["tf_down"], -1)],
                   key=lambda r: r["strength"], reverse=True)
    if not bulls and not bears:
        return []

    # Exclus : contradictions (retires de la liste).
    div_fa = [r["currency"] for r in recs if r["verdict"] == "divergence"]
    div_intra = [r["currency"] for r in recs
                 if r.get("intra") and r["verdict"] != "divergence"]
    # Alignement RENKO faible (1 seul TF) sur une devise sinon confluente.
    weak = [r["currency"] for r in recs
            if r["verdict"] in ("bull", "bear") and not r.get("intra")
            and _num(r) and (r["daily_chg"] > 0) == (r["verdict"] == "bull")
            and not _solid(r["tf_up"], r["tf_down"], 1 if r["verdict"] == "bull" else -1)]

    lines = ["🔀 Confluence FULL ALIGN × FIOS (stricte)"]

    def _chg(r):
        c = r["daily_chg"]
        return f" ({c:+.2f}%)" if isinstance(c, (int, float)) else ""

    for r in bulls:
        lines.append(f"🟢 {r['currency']}  FIOS {r['force']:.0f} · Align {r['tag']}{_chg(r)}")
    for r in bears:
        lines.append(f"🔴 {r['currency']}  FIOS {r['force']:.0f} · Align {r['tag']}{_chg(r)}")

    excl = []
    if div_fa:
        excl.append(f"{', '.join(div_fa)} (FIOS×Align)")
    if div_intra:
        excl.append(f"{', '.join(div_intra)} (jour≠structure)")
    if weak:
        excl.append(f"{', '.join(weak)} (align. faible)")
    if excl:
        lines.append("🚫 Exclus : " + " · ".join(excl))
    if bulls and bears:
        lines.append(f"→ Paire confluente : ACHAT {bulls[0]['currency']}{bears[0]['currency']}")
    return lines


def pair_confluence(composites: dict[str, dict], pairs: dict) -> list[dict]:
    """Meme analyse au niveau paire : direction FIOS (force base - force quote),
    alignement RENKO M/W/D de la paire, variation du jour de la paire."""
    recs: list[dict] = []
    for name, a in pairs.items():
        if len(name) != 6:
            continue
        base, quote = name[:3], name[3:]
        cb, cq = composites.get(base), composites.get(quote)
        if not cb or not cq:
            continue
        fios_diff = float(cb["composite"]) - float(cq["composite"])
        ascore = _align_score(a)
        chg = a.get("daily_chg")
        thr = cfg.PAIR_MIN_FIOS_DIFF
        fios_dir = 1 if fios_diff >= thr else (-1 if fios_diff <= -thr else 0)
        align_dir = 1 if ascore > 0 else (-1 if ascore < 0 else 0)
        daily_dir = (1 if isinstance(chg, (int, float)) and chg > 0
                     else (-1 if isinstance(chg, (int, float)) and chg < 0 else 0))
        tf_up, tf_down = _tf_counts(a)
        recs.append({
            "pair": name, "base": base, "quote": quote,
            "fios_diff": round(fios_diff, 0), "tag": _px_tag(a), "daily_chg": chg,
            "fios_dir": fios_dir, "align_dir": align_dir, "daily_dir": daily_dir,
            "tf_up": tf_up, "tf_down": tf_down,
            "strength": round(abs(fios_diff) + abs(ascore) * 50, 1),
        })
    return recs


_INDEX_CHG_STATE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "fios_index_chg_state.json"
)


def _load_index_chg_state(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save_index_chg_state(path: str, state: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass  # non-fatal : la persistance ne doit jamais casser le run FIOS


def _align_strength(r: dict, sign: int, eps: float = 0.005) -> int | None:
    """Nombre d'horizons (run/7h/jour) alignes dans le sens `sign` (0..3), ou
    None si un horizon CONTREDIT le sens (on refuse tout signal contradictoire).
    Un horizon neutre (▪) ne compte pas mais ne disqualifie pas."""
    day = r["day"]
    vals = [r["d_run"], r["d_7h"], day if isinstance(day, (int, float)) else 0.0]
    aligned = 0
    for v in vals:
        if sign > 0:
            if v > eps:
                aligned += 1
            elif v < -eps:
                return None
        else:
            if v < -eps:
                aligned += 1
            elif v > eps:
                return None
    return aligned


def _perfect_pair(strong: str, weak: str, universe: set[str]) -> tuple[str | None, str | None]:
    """Retourne (paire canonique, sens) pour LONG strong / SHORT weak, si la
    combinaison existe dans l'univers des paires reelles."""
    if strong + weak in universe:
        return strong + weak, "LONG"
    if weak + strong in universe:
        return weak + strong, "SHORT"
    return None, None


# CHG%D a contre-sens (en %) pour considerer qu'une paire s'est INVERSEE
# franchement (et doit alors quitter sa section malgre la pseudo-persistance).
_REVERSAL_THR = 0.10
_SECTION_PIPS_START_HOUR = 7
_SECTION_PIPS_END_HOUR = 23


def _convergent_now(rows: list[dict], pair_by_name: dict[str, dict]) -> tuple[dict[str, dict], set[str]]:
    """{pair: {dir(+/-1), tier, chg_abs}} des paires CONVERGENTES ce cycle :
    l'indice designe une base haussiere / quote baissiere (aucun horizon
    contradictoire) ET le momentum PROPRE de la paire confirme (>=2 alignes).
    Renvoie aussi `seen` (tous les candidats indice) pour exclure du top."""
    strongs = [(r, s) for r in rows if (s := _align_strength(r, 1)) and s >= 1]
    weaks = [(r, s) for r in rows if (s := _align_strength(r, -1)) and s >= 1]
    if not strongs or not weaks:
        return {}, set()

    try:
        from .multilayer import PAIRS_ALL
        universe = set(PAIRS_ALL)
    except Exception:
        universe = set()

    out: dict[str, dict] = {}
    seen: set[str] = set()
    for s, s_str in strongs:
        for w, w_str in weaks:
            if s["cur"] == w["cur"]:
                continue
            pair, direction = _perfect_pair(s["cur"], w["cur"], universe)
            if not pair or not direction or pair in seen:
                continue
            seen.add(pair)
            prow = pair_by_name.get(pair)
            if not prow:
                continue
            sign = 1 if direction == "LONG" else -1
            st = _align_strength(prow, sign)
            if st is None or st < 2:
                continue  # non convergente ce cycle
            day = prow.get("day")
            out[pair] = {"dir": sign, "tier": min(s_str, w_str),
                         "chg_abs": abs(day) if isinstance(day, (int, float)) else 0.0}
    return out, seen


def _top_now(pair_rows: list[dict], exclude: set[str],
             top_n: int = 3, eps: float = 0.005) -> dict[str, dict]:
    """{pair: {dir(+/-1 = signe CHG%D), aligned3, chg_abs}} des plus gros movers
    du jour, hors paires deja flaguees par l'indice."""
    def _day(r: dict) -> float:
        d = r["day"]
        return d if isinstance(d, (int, float)) else 0.0

    movers = [r for r in pair_rows if r["cur"] not in exclude and abs(_day(r)) > eps]
    movers.sort(key=lambda r: abs(_day(r)), reverse=True)
    out: dict[str, dict] = {}
    for r in movers[:top_n]:
        d = _day(r)
        out[r["cur"]] = {
            "dir": 1 if d > 0 else -1,
            "aligned3": _align_strength(r, 1) == 3 or _align_strength(r, -1) == 3,
            "chg_abs": abs(d),
        }
    return out


def _render_memory_sections(conv_now: dict[str, dict], top_now: dict[str, dict],
                            pair_by_name: dict[str, dict], prev_sections: dict,
                            price_trends: dict | None = None,
                            ) -> tuple[list[str], list[str], dict]:
    """Pseudo-persistance : une paire selectionnee reste dans SA section meme si
    elle ne qualifie plus (marquee ⚠️ « deviee »). Elle en sort seulement si elle
    MIGRE vers l'autre section (re-inscrite, sans ⚠️) ou si elle s'INVERSE
    franchement (CHG%D a contre-sens > _REVERSAL_THR). Reset quotidien via l'etat
    parent. Retourne (lignes conv, lignes top, nouvel etat des sections)."""
    def cur_day(pair: str) -> float:
        r = pair_by_name.get(pair)
        d = r.get("day") if r else None
        return d if isinstance(d, (int, float)) else 0.0

    new_sections: dict[str, dict] = {}
    conv_items: list[dict] = []   # {pair, dir, chg_abs, mark in "flame"/""/"warn"}
    top_items: list[dict] = []
    handled: set[str] = set()

    # 1. Qualifiants ce cycle -> (re)placent la paire dans sa section, sans ⚠️.
    #    Gere automatiquement la migration d'une section a l'autre.
    for pair, info in conv_now.items():
        new_sections[pair] = {"section": "conv", "dir": info["dir"]}
        conv_items.append({"pair": pair, "dir": info["dir"], "chg_abs": info["chg_abs"],
                           "mark": "flame" if info["tier"] == 3 else ""})
        handled.add(pair)
    for pair, info in top_now.items():
        if pair in handled:
            continue
        new_sections[pair] = {"section": "top", "dir": info["dir"]}
        top_items.append({"pair": pair, "dir": info["dir"], "chg_abs": info["chg_abs"],
                          "mark": "flame" if info["aligned3"] else ""})
        handled.add(pair)

    # 2. Persistants non qualifiants -> gardes avec ⚠️, sauf inversion franche.
    for pair, prev in (prev_sections or {}).items():
        if pair in handled:
            continue
        prev_dir = int(prev.get("dir", 0))
        sec = prev.get("section")
        if sec not in ("conv", "top"):
            continue
        d = cur_day(pair)
        d_sign = 1 if d > 0 else (-1 if d < 0 else 0)
        if prev_dir != 0 and d_sign == -prev_dir and abs(d) > _REVERSAL_THR:
            continue  # inversion franche -> sortie definitive
        new_sections[pair] = {"section": sec, "dir": prev_dir}
        item = {"pair": pair, "dir": prev_dir, "chg_abs": abs(d), "mark": "warn"}
        (conv_items if sec == "conv" else top_items).append(item)

    conv_items.sort(key=lambda x: x["chg_abs"], reverse=True)
    top_items.sort(key=lambda x: x["chg_abs"], reverse=True)

    def _render(title: str, items: list[dict]) -> list[str]:
        if not items:
            return []
        out = [title, ""]
        for it in items:
            icon = "🟢" if it["dir"] == 1 else "🔴"
            mark = " 🔥" if it["mark"] == "flame" else (" ⚠️" if it["mark"] == "warn" else "")
            out.append(f"{icon} {it['pair']}{live_price_suffix(it['pair'], price_trends)}{mark}")
        return out

    return (_render("🎯 PAIRES CONVERGENTES", conv_items),
            _render("🔝 TOP MOMENTUM PAIRES", top_items),
            new_sections)


def _section_pip_size(pair: str) -> float:
    return 0.01 if pair.endswith("JPY") or pair == "XAUUSD" else 0.0001


def _section_segment_pips(segment: dict, price: float) -> float:
    return ((float(price) - float(segment["start_price"]))
            / _section_pip_size(str(segment["pair"]))
            * int(segment["dir"]))


def _section_day(state: dict, day_key: str) -> dict:
    return state.setdefault("days", {}).setdefault(
        day_key, {"date": day_key, "segments": [], "finalized": False}
    )


def _close_section_segment(state: dict, pair: str, segment: dict,
                           price: float, clock: datetime, reason: str) -> None:
    closed = dict(segment)
    closed.update({
        "end_price": float(price),
        "end_time_paris": clock.isoformat(),
        "pips": _section_segment_pips(segment, float(price)),
        "close_reason": reason,
    })
    closed.pop("last_price", None)
    closed.pop("current_pips", None)
    _section_day(state, str(segment["date"]))["segments"].append(closed)
    state.setdefault("open", {}).pop(pair, None)


def _section_period_total(state: dict, section: str, start: str, end: str) -> float:
    total = 0.0
    for day_key, day in (state.get("days") or {}).items():
        if start <= day_key <= end:
            total += sum(
                float(item.get("pips") or 0.0)
                for item in day.get("segments") or []
                if item.get("section") == section
            )
    return total


def _update_section_pip_tracker(previous: dict | None, sections: dict,
                                pair_by_name: dict[str, dict], clock: datetime
                                ) -> tuple[dict, dict | None]:
    """Deux books persistants (convergentes/top), marques aux prix de chaque run.

    Une paire reste ouverte tant que la pseudo-persistance la conserve. Une
    migration ferme l'ancien book et ouvre le nouveau au meme prix. Les books
    sont soldes au dernier run (23h Paris); les cumuls repartent logiquement de
    zero au 1er janvier sans supprimer les archives.
    """
    state = json.loads(json.dumps(previous)) if isinstance(previous, dict) else {}
    state["version"] = 1
    state.setdefault("open", {})
    state.setdefault("days", {})
    state.setdefault("reports_sent", [])
    clock = clock.astimezone(PARIS) if clock.tzinfo else clock.replace(tzinfo=PARIS)
    today = clock.date().isoformat()

    def price_of(pair: str) -> float | None:
        value = (pair_by_name.get(pair) or {}).get("level")
        return float(value) if isinstance(value, (int, float)) and value > 0 else None

    # Un run de cloture manque: figer au dernier prix connu, jamais au prix du
    # lendemain (qui introduirait artificiellement le gap overnight).
    for pair, segment in list(state["open"].items()):
        if segment.get("date") == today:
            continue
        last_price = segment.get("last_price", segment.get("start_price"))
        if isinstance(last_price, (int, float)):
            end_clock = datetime.fromisoformat(str(segment.get("last_time_paris")))
            _close_section_segment(
                state, pair, segment, float(last_price), end_clock, "MISSED_DAY_END"
            )
        old_day = _section_day(state, str(segment["date"]))
        old_day["finalized"] = True

    in_window = _SECTION_PIPS_START_HOUR <= clock.hour <= _SECTION_PIPS_END_HOUR
    day = _section_day(state, today)
    if in_window and not day.get("finalized"):
        for pair, segment in list(state["open"].items()):
            target = sections.get(pair)
            same_book = (target and target.get("section") == segment.get("section")
                         and int(target.get("dir", 0)) == int(segment.get("dir", 0)))
            price = price_of(pair)
            if same_book:
                if price is not None:
                    segment["last_price"] = price
                    segment["last_time_paris"] = clock.isoformat()
                    segment["current_pips"] = _section_segment_pips(segment, price)
                continue
            last_price = price if price is not None else segment.get("last_price")
            if isinstance(last_price, (int, float)):
                reason = "MIGRATION" if target else "SECTION_EXIT"
                _close_section_segment(
                    state, pair, segment, float(last_price), clock, reason
                )

        for pair, target in sections.items():
            if pair in state["open"]:
                continue
            price = price_of(pair)
            if price is None or target.get("section") not in ("conv", "top"):
                continue
            state["open"][pair] = {
                "pair": pair,
                "section": target["section"],
                "dir": int(target["dir"]),
                "date": today,
                "start_price": price,
                "last_price": price,
                "start_time_paris": clock.isoformat(),
                "last_time_paris": clock.isoformat(),
                "current_pips": 0.0,
            }

        if clock.hour >= _SECTION_PIPS_END_HOUR:
            for pair, segment in list(state["open"].items()):
                price = price_of(pair)
                if price is None:
                    price = segment.get("last_price", segment.get("start_price"))
                _close_section_segment(
                    state, pair, segment, float(price), clock, "DAY_END"
                )
            day["finalized"] = True
            day["finalized_at_paris"] = clock.isoformat()

    report = None
    if (clock.hour >= _SECTION_PIPS_END_HOUR and day.get("finalized")
            and today not in state["reports_sent"]):
        week_start = (clock.date() - timedelta(days=clock.weekday())).isoformat()
        month_start = clock.date().replace(day=1).isoformat()
        year_start = clock.date().replace(month=1, day=1).isoformat()
        report = {}
        for section in ("conv", "top"):
            report[section] = {
                "daily": _section_period_total(state, section, today, today),
                "weekly": _section_period_total(state, section, week_start, today),
                "monthly": _section_period_total(state, section, month_start, today),
                "yearly": _section_period_total(state, section, year_start, today),
                "year": clock.year,
            }
        state["reports_sent"].append(today)
        del state["reports_sent"][:-400]
    return state, report


def _append_section_pip_report(lines: list[str], title: str,
                               item: dict | None) -> list[str]:
    if not item:
        return lines
    if not lines:
        lines = [title, ""]
    elif lines[-1] != "":
        lines.append("")
    lines.extend([
        f"📈 Daily : {item['daily']:+.1f}",
        f"📊 Weekly : {item['weekly']:+.1f}",
        f"Monthly : {item['monthly']:+.1f}",
        f"🗓 YTD {item['year']} : {item['yearly']:+.1f}",
    ])
    return lines


def _momentum_rows(items: dict, prev_items: dict) -> tuple[list[dict], dict]:
    """Calcule les 3 horizons (run/7h/jour) de chaque item (devise OU paire) a
    partir de son NIVEAU brut (live_price) + CHG%D, et renvoie l'etat a jour.
    - run : variation vs le cycle precedent
    - 7h  : variation vs le 1er run du jour (reference absolue = 0 a 7h)
    - jour: le CHG%D actuel
    Base sur le prix absolu -> immunise au rollover de fin de journee."""
    rows: list[dict] = []
    new_items: dict[str, dict] = {}
    for name, data in items.items():
        lvl = data.get("live_price")
        chg = data.get("daily_chg")
        if not isinstance(lvl, (int, float)) or lvl <= 0:
            continue  # pas de niveau exploitable -> on ignore cet item

        p = prev_items.get(name) or {}
        baseline = p.get("baseline")
        prev_lvl = p.get("prev")
        if not isinstance(baseline, (int, float)) or baseline <= 0:
            baseline = lvl  # 1er run du jour : reference = maintenant

        d_run = ((lvl / prev_lvl - 1.0) * 100.0) if isinstance(prev_lvl, (int, float)) and prev_lvl > 0 else 0.0
        d_7h = (lvl / baseline - 1.0) * 100.0
        day = chg if isinstance(chg, (int, float)) else None

        rows.append({"cur": name, "d_run": d_run, "d_7h": d_7h,
                     "day": day, "level": float(lvl)})
        new_items[name] = {"baseline": baseline, "prev": lvl}

    # Conserve le baseline des items non vus ce cycle (evite un reset sur un
    # trou transitoire) ; le reset propre se fait au changement de jour.
    for name, p in prev_items.items():
        new_items.setdefault(name, p)
    return rows, new_items


def build_index_momentum_lines(payload: dict | None, state_path: str | None = None) -> list[str]:
    """Momentum 3 horizons (run / 7h / jour) sur le NIVEAU brut des 8 indices
    devises ET des 29 paires. Le momentum des indices est calcule et persiste
    pour l'analyse en arriere-plan mais N'EST PLUS envoye sur Telegram. La sortie
    (Telegram) ne contient que :
      1. les paires CONVERGENTES (indice + momentum propre concordent), triees
         par |CHG%D| decroissant, flamme 🔥 sur les parfaites,
      2. le top des paires qui bougent le plus (decouverte, hors flaguees).
    Etat (baseline 7h + cycle precedent, devises + paires) remis a zero chaque
    nouveau jour (Paris), persiste entre runs CI."""
    if not payload:
        return []
    currencies = payload.get("indexes") or payload.get("currencies") or {}
    if not currencies:
        return []
    pairs = payload.get("pairs") or {}

    path = state_path or _INDEX_CHG_STATE
    clock = datetime.now(PARIS)
    today = clock.strftime("%Y-%m-%d")
    prev = _load_index_chg_state(path)
    same_day = prev.get("date") == today
    cur_rows, new_curs = _momentum_rows(currencies, prev.get("currencies", {}) if same_day else {})
    pair_rows, new_pairs = _momentum_rows(pairs, prev.get("pairs", {}) if same_day else {})
    if not cur_rows:
        _save_index_chg_state(path, {
            "date": today,
            "currencies": new_curs,
            "pairs": new_pairs,
            "sections": prev.get("sections", {}) if same_day else {},
            "tracking": prev.get("tracking", {}),
        })
        return []

    # Le bloc par-indice (8 devises) n'est plus envoye sur Telegram : il reste
    # calcule et persiste ci-dessus pour l'analyse en arriere-plan. On ne rend
    # que les paires convergentes puis le top momentum.
    lines: list[str] = []
    pair_by_name = {r["cur"]: r for r in pair_rows}
    trend_state, price_trends = update_price_trends(
        prev.get("price_trends", {}),
        {name: float(row["level"]) for name, row in pair_by_name.items()},
        clock,
    )
    conv_now, shown = _convergent_now(cur_rows, pair_by_name)
    top_now = _top_now(pair_rows, shown)
    conv_lines, top_lines, new_sections = _render_memory_sections(
        conv_now,
        top_now,
        pair_by_name,
        prev.get("sections", {}) if same_day else {},
        price_trends,
    )
    tracking, pip_report = _update_section_pip_tracker(
        prev.get("tracking", {}), new_sections, pair_by_name, clock
    )
    if pip_report:
        conv_lines = _append_section_pip_report(
            conv_lines, "🎯 PAIRES CONVERGENTES", pip_report.get("conv")
        )
        top_lines = _append_section_pip_report(
            top_lines, "🔝 TOP MOMENTUM PAIRES", pip_report.get("top")
        )
    _save_index_chg_state(path, {
        "date": today,
        "currencies": new_curs,
        "pairs": new_pairs,
        "sections": new_sections,
        "tracking": tracking,
        "price_trends": trend_state,
        "render_trends": price_trends,
    })

    if conv_lines:
        lines.extend(conv_lines)
    if top_lines:
        if lines:
            lines.append("")
        lines.extend(top_lines)

    return lines


def _format_vivier_chg_px_line(pair: str, entry: dict,
                               price_trends: dict | None = None) -> str:
    """Format Vivier line: single icon (🟢 if both green, 🔴 if both red, ⚪ if mixed) + CHG%D + real M/W/D state tag."""
    from renko_score_29pairs_v16 import daily_chg_sar_icon, vivier_flame_label
    direction = int(entry.get("direction", 1))
    c1 = "🟢" if direction == 1 else "🔴"
    c2 = daily_chg_sar_icon(entry.get("daily_chg"), entry.get("daily_sar_dir"))

    if c1 == "🟢" and c2 == "🟢":
        final_icon = "🟢"
    elif c1 == "🔴" and c2 == "🔴":
        final_icon = "🔴"
    else:
        final_icon = "⚪"

    chg = entry.get("daily_chg")
    chg_txt = f"{chg:+.2f}%" if isinstance(chg, (int, float)) else "---"

    px = entry.get("last_px") or {}
    def _sign(v):
        return "+" if v == 1 else ("-" if v == -1 else "0")
    tag = f"M{_sign(px.get('M'))} W{_sign(px.get('W'))} D{_sign(px.get('D'))}"

    line = f"{final_icon} {pair}{live_price_suffix(pair, price_trends)}"
    flame = vivier_flame_label(entry)
    return f"{line} {flame}" if flame else line


def build_vivier_section(price_trends: dict | None = None) -> list[str]:
    """Charge l'état du VIVIER et génère la section VIVIER."""
    try:
        from renko_score_29pairs_v16 import load_vivier_state, vivier_groups
        state = load_vivier_state()
        if not state:
            return []
        bull_vivier, bear_vivier = vivier_groups(state)
        if not bull_vivier and not bear_vivier:
            return []

        lines = ["📊 VIVIER"]
        for pair, entry in bull_vivier:
            lines.append(_format_vivier_chg_px_line(pair, entry, price_trends))
        for pair, entry in bear_vivier:
            lines.append(_format_vivier_chg_px_line(pair, entry, price_trends))

        return lines
    except Exception as exc:
        print(f"Warning: Impossible de charger la section VIVIER: {exc}")
        return []


def build_renko_fibo_50_section(results: dict | None = None) -> list[str]:
    """Section RENKO FIBO 50% (DAILY, FULL ALIGNMENT, BIAIS M/W). Reutilise le
    scan deja calcule par le podium si fourni (evite un double scan)."""
    try:
        from renko_fibo_50_strategy import build_sections, _ICONS, _fmt_chg
        if not results:
            from renko_fibo_50_strategy import scan_all_pairs
            results = scan_all_pairs(length=14, candles=80, workers=10, max_age_bricks=5)
        if not results:
            return []

        daily_alignments, strict_alignments, mw_alignments = build_sections(results)

        lines = ["📊 RENKO FIBO 50%"]
        has_content = False

        for header, rows in (
            ("☀️ DAILY", daily_alignments),
            ("📊 FULL ALIGNMENT", strict_alignments),
            ("🧭 BIAIS M/W", mw_alignments),
        ):
            if not rows:
                continue
            lines.append("")
            lines.append(header)
            for pair, label, chg in sorted(rows, key=lambda r: r[0]):
                lines.append(f"{_ICONS[label]} {pair} · {_fmt_chg(chg)}")
            has_content = True

        return lines if has_content else []
    except Exception as exc:
        print(f"Warning: Impossible de charger la section RENKO FIBO 50%: {exc}")
        return []


def build_multilayer_section(composites: dict[str, dict], payload: dict | None) -> tuple[list[str], dict[str, dict]]:
    """Génère la section Multi-Layer Matrix (Grade A+ et Grade A)."""
    fibo_50_results = {}
    try:
        from renko_fibo_50_strategy import scan_all_pairs
        fibo_50_results = scan_all_pairs(length=14, candles=80, workers=10, max_age_bricks=5) or {}
    except Exception as exc:
        print(f"Warning: Impossible d'exécuter le scan Renko Fibo 50%: {exc}")

    vivier_state = {}
    try:
        from renko_score_29pairs_v16 import load_vivier_state
        vivier_state = load_vivier_state() or {}
    except Exception as exc:
        print(f"Warning: Impossible de charger le state Vivier: {exc}")

    try:
        from .multilayer import compute_multilayer_matrix, format_multilayer_section
        scores = compute_multilayer_matrix(composites, payload, fibo_50_results, vivier_state)
        lines = format_multilayer_section(scores)
        return lines, fibo_50_results
    except Exception as exc:
        print(f"Warning: Erreur dans le calcul Multicouche: {exc}")
        return [], fibo_50_results


def build_pairs_section(composites: dict[str, dict], payload: dict | None) -> list[str]:
    """Message FIOS envoye sur Telegram. Ne contient plus que :
      🤝 CONSENSUS IA · 📊 VIVIER · 🎯 PAIRES CONVERGENTES · 🔝 TOP MOMENTUM PAIRES."""
    if not payload:
        return []
    pairs = payload.get("pairs") or {}
    if not pairs:
        return []

    lines: list[str] = []

    idx_lines = build_index_momentum_lines(payload)
    trend_snapshot = _load_index_chg_state(_INDEX_CHG_STATE).get("render_trends", {})
    vivier_lines = build_vivier_section(trend_snapshot)
    if vivier_lines:
        lines.extend(vivier_lines)

    if idx_lines:
        if lines:
            lines.append("")
        lines.extend(idx_lines)

    return lines
