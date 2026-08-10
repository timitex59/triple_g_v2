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
from datetime import datetime, timezone

import pytz

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
    today = datetime.now(PARIS).strftime("%Y-%m-%d")
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


def _mom_arrow(v: float, eps: float = 0.005) -> str:
    return "▲" if v > eps else ("▼" if v < -eps else "▪")


def _mom_sig(r: dict) -> str:
    """Signature 3 horizons d'une devise, ex. '▲▲▲' ou '▼▼▼'."""
    return f"{_mom_arrow(r['d_run'])}{_mom_arrow(r['d_7h'])}{_mom_arrow(r['day'] or 0.0)}"


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


def _perfect_pairs_lines(rows: list[dict], pair_by_name: dict[str, dict]) -> tuple[list[str], set[str]]:
    """Section unique des paires CONVERGENTES : l'indice designe une base
    haussiere / quote baissiere (aucun horizon contradictoire), ET le momentum
    PROPRE de la paire confirme le sens (>=2 horizons alignes). Triee par |CHG%D|
    de la paire decroissant ; la flamme 🔥 marque les PARFAITES (3/3 des 2 cotes).
    Retourne (lignes, tous les candidats indice) — le 2e sert a exclure du
    leaderboard les paires deja flaguees."""
    strongs = [(r, s) for r in rows if (s := _align_strength(r, 1)) and s >= 1]
    weaks = [(r, s) for r in rows if (s := _align_strength(r, -1)) and s >= 1]
    if not strongs or not weaks:
        return [], set()

    try:
        from .multilayer import PAIRS_ALL
        universe = set(PAIRS_ALL)
    except Exception:
        universe = set()

    seen: set[str] = set()
    shown: list[tuple[float, int, str, str]] = []  # (|CHG%D|, tier, pair, direction)
    for s, s_str in strongs:
        for w, w_str in weaks:
            if s["cur"] == w["cur"]:
                continue
            pair, direction = _perfect_pair(s["cur"], w["cur"], universe)
            if not pair or not direction or pair in seen:
                continue
            seen.add(pair)
            # Convergence : le momentum PROPRE de la paire doit confirmer le sens.
            prow = pair_by_name.get(pair)
            if not prow:
                continue
            sign = 1 if direction == "LONG" else -1
            st = _align_strength(prow, sign)
            if st is None or st < 2:
                continue  # on n'affiche QUE les convergentes
            day = prow.get("day")
            chg_abs = abs(day) if isinstance(day, (int, float)) else 0.0
            shown.append((chg_abs, min(s_str, w_str), pair, direction))

    if not shown:
        return [], seen
    shown.sort(key=lambda x: x[0], reverse=True)

    lines = ["🎯 PAIRES CONVERGENTES", ""]
    for _, tier, pair, direction in shown:
        icon = "🟢" if direction == "LONG" else "🔴"
        flame = " 🔥" if tier == 3 else ""
        lines.append(f"{icon} {pair}{flame}")
    return lines, seen


def _pair_leaderboard_lines(pair_rows: list[dict], exclude: set[str],
                            top_n: int = 3, eps: float = 0.005) -> list[str]:
    """🔝 Paires qui bougent le plus (run-a-run), hors paires deja listees en
    parfaites -> decouvre ce que les indices n'ont pas flague."""
    movers = [r for r in pair_rows if r["cur"] not in exclude and abs(r["d_run"]) > eps]
    if not movers:
        return []
    movers.sort(key=lambda r: abs(r["d_run"]), reverse=True)
    lines = ["🔝 TOP MOMENTUM PAIRES   run / 7h / jour"]
    for r in movers[:top_n]:
        day_txt = f"{r['day']:+.2f}%" if isinstance(r["day"], (int, float)) else "--"
        lines.append(f"{_mom_sig(r)} {r['cur']}  {r['d_run']:+.2f} / {r['d_7h']:+.2f} / {day_txt}")
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

        rows.append({"cur": name, "d_run": d_run, "d_7h": d_7h, "day": day})
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
    today = datetime.now(PARIS).strftime("%Y-%m-%d")
    prev = _load_index_chg_state(path)
    same_day = prev.get("date") == today
    cur_rows, new_curs = _momentum_rows(currencies, prev.get("currencies", {}) if same_day else {})
    pair_rows, new_pairs = _momentum_rows(pairs, prev.get("pairs", {}) if same_day else {})
    _save_index_chg_state(path, {"date": today, "currencies": new_curs, "pairs": new_pairs})

    if not cur_rows:
        return []

    # Le bloc par-indice (8 devises) n'est plus envoye sur Telegram : il reste
    # calcule et persiste ci-dessus pour l'analyse en arriere-plan. On ne rend
    # que les paires convergentes puis le top momentum.
    lines: list[str] = []
    pair_by_name = {r["cur"]: r for r in pair_rows}
    perfect, shown = _perfect_pairs_lines(cur_rows, pair_by_name)
    if perfect:
        lines.extend(perfect)

    lead = _pair_leaderboard_lines(pair_rows, shown)
    if lead:
        if lines:
            lines.append("")
        lines.extend(lead)

    return lines


def _format_vivier_chg_px_line(pair: str, entry: dict) -> str:
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

    line = f"{final_icon} {pair} ({chg_txt}) ({tag})"
    flame = vivier_flame_label(entry)
    return f"{line} {flame}" if flame else line


def build_vivier_section() -> list[str]:
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
            lines.append(_format_vivier_chg_px_line(pair, entry))
        for pair, entry in bear_vivier:
            lines.append(_format_vivier_chg_px_line(pair, entry))

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
    if not payload:
        return []
    pairs = payload.get("pairs") or {}
    if not pairs:
        return []
    recs = pair_confluence(composites, pairs)

    def _big(r) -> bool:
        c = r["daily_chg"]
        return isinstance(c, (int, float)) and abs(c) > cfg.PAIR_MIN_DAILY_CHG

    # STRICT : FIOS (ecart >= seuil) + variation du jour (|chg| > seuil) +
    # alignement RENKO SOLIDE (>=2 TF, 0 a contre-sens), tous dans le meme sens.
    buys = sorted([r for r in recs if r["fios_dir"] > 0 and r["daily_dir"] > 0
                   and _big(r) and _solid(r["tf_up"], r["tf_down"], 1)],
                  key=lambda r: r["strength"], reverse=True)
    sells = sorted([r for r in recs if r["fios_dir"] < 0 and r["daily_dir"] < 0
                    and _big(r) and _solid(r["tf_up"], r["tf_down"], -1)],
                   key=lambda r: r["strength"], reverse=True)

    ml_lines, fibo_results = build_multilayer_section(composites, payload)

    if not buys and not sells and not ml_lines:
        return []  # rien d'aligne -> pas de message (evite le spam horaire)

    lines = []
    if ml_lines:
        lines.extend(ml_lines)
        lines.append("")

    lines.append("🔀 CONFLUENCE")
    lines.append("")
    for r in buys:
        lines.append(f"🟢 {r['pair']} ({r['daily_chg']:+.2f}%) ({r['tag']})")
    for r in sells:
        lines.append(f"🔴 {r['pair']} ({r['daily_chg']:+.2f}%) ({r['tag']})")

    vivier_lines = build_vivier_section()
    if vivier_lines:
        lines.append("")
        lines.extend(vivier_lines)

    fibo_50_lines = build_renko_fibo_50_section(fibo_results)
    if fibo_50_lines:
        lines.append("")
        lines.extend(fibo_50_lines)

    idx_lines = build_index_momentum_lines(payload)
    if idx_lines:
        lines.append("")
        lines.extend(idx_lines)

    return lines
