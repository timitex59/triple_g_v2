"""Scanner dedicated to strict M/W/D Renko full alignment on FX assets.

This is intentionally separate from the VIVIER state machine. It only detects
pairs where price is strictly outside Monthly, Weekly and Daily Renko bricks in
the same direction:

- BULL: M+/W+/D+ => raw score +100%
- BEAR: M-/W-/D- => raw score -100%

It also flags H1 SAR breaks in the same direction for W/M alignment, or the
stronger M/W/D case.
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from ichimoku_v4 import PAIRS_29, send_telegram_message
from renko_score_29pairs_v16 import (
    TFState,
    closed_h1_source,
    f_effective_bias,
    f_px_state,
    fetch_tv_native_renko_ohlc,
    fetch_tv_ohlc,
    parabolic_sar,
    streak_range_from_bricks,
    streaks_from_bricks,
)


PARIS_TZ = ZoneInfo("Europe/Paris")
MID_SAR_STATE_FILE = Path("renko_full_alignment_mid_sar_state.json")
PAIRS_OUT_STATE_FILE = Path("renko_full_alignment_pairs_out_state.json")
PERFORMANCE_STATE_FILE = Path("renko_full_alignment_performance_state.json")
# Meme seuil que VIVIER_PIPS_DAY_RESULT_EPSILON: un trade clos en dessous ne
# compte ni comme gagnant ni comme perdant (bruit de cloture).
PERFORMANCE_FLAT_EPSILON_PIPS = 0.05
# Fenetre de fraicheur du declencheur SAR H1 (🎯) sur STREAKY: reste affiche
# tant que la derniere cassure remonte a au plus N bougies H1 (0 = la bougie
# la plus recente), pas uniquement sur le run exact ou elle s'est produite.
STREAKY_SAR_CONFIRM_WINDOW_BARS = 2
# Sidecar per-devise (force via index DXY/EXY/... + alignement M/W/D) lu par FIOS
# pour croiser cette analyse avec sa force composite (confluence inter-systemes).
INDEX_SIDECAR_FILE = Path("full_alignment_index.json")
PRICE_TREND_STATE_FILE = Path("renko_full_alignment_price_trend_state.json")
MID_SAR_WINDOW_START_HOUR = 7
MID_SAR_WINDOW_END_HOUR = 23
MID_SAR_ALLOWED_TF_PAIRS = {"M/W/D"}

# Poids de la note streak par timeframe: un TF lent engage davantage (meme
# echelle 3/2/1 que WEIGHTS dans renko_score_29pairs_v16.py).
STREAK_TF_WEIGHTS = {"M": 3.0, "W": 2.0, "D": 1.0}
# Renormalise la note ponderee sur -3..+3, l'echelle de la note non ponderee,
# pour que les scores restent comparables d'une version a l'autre.
STREAK_NOTE_NORMALIZER = 3.0 / sum(STREAK_TF_WEIGHTS.values())

# Marqueurs de synchronisation entre le moteur devise (attente) et le moteur
# paire (realise). En dessous de SYNC_MIN_EXPECTED il n'y a pas assez d'ecart
# entre les deux devises pour parler de setup, donc aucun marqueur.
SYNC_MIN_EXPECTED = 0.30
# Au-dela, la paire est consideree comme reellement engagee dans le mouvement.
SYNC_MIN_REALIZED = 0.10
# La section paires est deja classee par produit decroissant: au-dela des
# premieres lignes, le reste n'apporte plus de decision.
PAIR_SECTION_LIMIT = 5

# PERF TOP5: une position deja ouverte tolere de reculer jusqu'a ce rang
# (toujours non-divergente, cf. all_pair_status_rows) avant cloture
# (RANK_EXIT), plutot que de couper des qu'elle quitte le TOP5 stricte --
# seule l'ENTREE reste conditionnee au TOP5 (PAIR_SECTION_LIMIT).
PERFORMANCE_EXTENDED_RANK_LIMIT = 8
# Trailing stop sur les positions ouvertes: s'arme des que le pic de gain
# (last_pips) atteint ce seuil, puis cloture (TRAILING_STOP) si le gain
# redonne plus de PERFORMANCE_TRAILING_STOP_PCT de ce pic. En % du pic
# plutot qu'en pips fixes: s'adapte a l'amplitude propre a chaque paire
# (une paire JPY bouge en pips bien plus large qu'une paire standard).
PERFORMANCE_TRAILING_ARM_PIPS = 10.0
PERFORMANCE_TRAILING_STOP_PCT = 0.30
# Comme le DAY_END de VIVIER (VIVIER_PIPS_END_HOUR_PARIS): toute position
# encore ouverte a cette heure Paris se cloture, quelle que soit sa
# situation (❌, trailing stop ou rang), prioritaire sur ces autres regles.
PERFORMANCE_FORCE_CLOSE_HOUR_PARIS = 23

# Heure Paris a partir de laquelle le premier prix observe du jour devient la
# reference du "premier signal" pour l'icone de continuation de tendance.
TREND_ICON_BASELINE_HOUR_PARIS = 6
# Ecart minimum en pips depuis cette reference avant de considerer que le prix
# est reellement reparti dans le sens oppose: sous ce seuil, un aller-retour
# de quelques pips ne fait pas basculer l'icone.
TREND_ICON_MIN_PIPS = 5.0

FOREX_INDEX_ASSETS: list[dict] = [
    {"pair": "DXY", "tv_symbol": "TVC:DXY", "asset_type": "INDEX", "currency": "USD"},
    {"pair": "EXY", "tv_symbol": "TVC:EXY", "asset_type": "INDEX", "currency": "EUR"},
    {"pair": "BXY", "tv_symbol": "TVC:BXY", "asset_type": "INDEX", "currency": "GBP"},
    {"pair": "JXY", "tv_symbol": "TVC:JXY", "asset_type": "INDEX", "currency": "JPY"},
    {"pair": "SXY", "tv_symbol": "TVC:SXY", "asset_type": "INDEX", "currency": "CHF"},
    {"pair": "CXY", "tv_symbol": "TVC:CXY", "asset_type": "INDEX", "currency": "CAD"},
    {"pair": "AXY", "tv_symbol": "TVC:AXY", "asset_type": "INDEX", "currency": "AUD"},
    {"pair": "ZXY", "tv_symbol": "TVC:ZXY", "asset_type": "INDEX", "currency": "NZD"},
]
# XAUUSD est ecarte de ce scanner: l'or n'a pas d'indice devise, donc pas
# d'attente du moteur devise a comparer, et sa volatilite en % ecrase l'echelle
# des scores face aux paires de devises. PAIRS_29 reste intact: il est partage
# par une quinzaine d'autres scripts.
EXCLUDED_PAIRS = {"XAUUSD"}
FOREX_PAIR_ASSETS: list[dict] = [
    {"pair": pair, "tv_symbol": f"OANDA:{pair}", "asset_type": "PAIR"}
    for pair in PAIRS_29
    if pair not in EXCLUDED_PAIRS
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan the 28 OANDA FX pairs (XAUUSD excluded) and/or currency indices for "
            "strict Monthly/Weekly/Daily Renko full alignment."
        )
    )
    parser.add_argument(
        "--assets",
        choices=("all", "pairs", "indices"),
        default="all",
        help="Asset universe to scan. Default: all = 28 pairs + 8 forex indices.",
    )
    parser.add_argument("--length", type=int, default=14, help="ATR Renko length.")
    parser.add_argument(
        "--candles",
        type=int,
        default=300,
        help="Number of candles/bricks fetched per symbol and timeframe.",
    )
    parser.add_argument(
        "--max-streak",
        type=int,
        default=50,
        help="Cap on consecutive green/red Renko brick streak count.",
    )
    parser.add_argument(
        "--sar-candles",
        type=int,
        default=400,
        help="Number of H1 candles used to detect SAR breaks.",
    )
    parser.add_argument(
        "--telegram",
        action="store_true",
        help="Send the scanner result to Telegram. By default it only prints.",
    )
    parser.add_argument(
        "--mid-sar-state-file",
        default=str(MID_SAR_STATE_FILE),
        help="JSON state file used to persist MID SAR detections between 07:00 and 23:00 Paris.",
    )
    parser.add_argument(
        "--price-trend-state-file",
        default=str(PRICE_TREND_STATE_FILE),
        help="JSON state for pair live-price comparisons versus 07:00 and previous run.",
    )
    parser.add_argument(
        "--pairs-out-state-file",
        default=str(PAIRS_OUT_STATE_FILE),
        help="JSON state tracking pairs that left the PAIRES CHG%%D TOP5 today.",
    )
    parser.add_argument(
        "--performance-state-file",
        default=str(PERFORMANCE_STATE_FILE),
        help="JSON state tracking daily/weekly/monthly/yearly TOP5 pip performance.",
    )
    return parser.parse_args()


def assets_for_scope(scope: str) -> list[dict]:
    if scope == "pairs":
        return list(FOREX_PAIR_ASSETS)
    if scope == "indices":
        return list(FOREX_INDEX_ASSETS)
    return [*FOREX_PAIR_ASSETS, *FOREX_INDEX_ASSETS]


def compute_tf_state_for_symbol(
    tv_symbol: str,
    interval: str,
    length: int,
    candles: int,
    max_streak: int,
    live_price: float,
) -> TFState | None:
    native_bricks = fetch_tv_native_renko_ohlc(
        tv_symbol,
        interval,
        atr_length=length,
        n_bricks=max(candles, max_streak + 1),
    )
    if not native_bricks:
        return None

    bricks: list[tuple[float, float, int]] = []
    for brick in native_bricks:
        renko_open = float(brick["open"])
        renko_close = float(brick["close"])
        direction = 1 if renko_close > renko_open else (-1 if renko_close < renko_open else 0)
        if direction:
            bricks.append((renko_open, renko_close, direction))
    if not bricks:
        return None

    renko_open, renko_close, direction = bricks[-1]
    green_streak, red_streak = streaks_from_bricks(bricks, max_streak)
    streak_count, streak_low, streak_high = streak_range_from_bricks(bricks)
    px_state = f_px_state(renko_open, renko_close, live_price)
    bias = f_effective_bias(px_state, green_streak, red_streak)

    return TFState(
        px_state=px_state,
        bias=bias,
        direction=direction,
        green_streak=green_streak,
        red_streak=red_streak,
        renko_open=renko_open,
        renko_close=renko_close,
        streak_count=streak_count,
        streak_low=streak_low,
        streak_high=streak_high,
    )


def compute_asset_score(asset: dict, length: int, candles: int, max_streak: int) -> dict | None:
    tv_symbol = str(asset["tv_symbol"])
    df_d_live = fetch_tv_ohlc(tv_symbol, "D", max(candles, 50))
    if df_d_live is None or df_d_live.empty:
        return None
    live_price = float(df_d_live["close"].iloc[-1])
    prev_close = float(df_d_live["close"].iloc[-2]) if len(df_d_live) >= 2 else None
    daily_chg = (
        ((live_price - prev_close) / prev_close * 100.0)
        if prev_close is not None and prev_close != 0
        else None
    )

    states: dict[str, TFState] = {}
    for interval in ("M", "W", "D"):
        state = compute_tf_state_for_symbol(
            tv_symbol,
            interval,
            length,
            candles,
            max_streak,
            live_price,
        )
        if state is None:
            return None
        states[interval] = state

    return {
        "pair": asset["pair"],
        "tv_symbol": tv_symbol,
        "asset_type": asset.get("asset_type", "PAIR"),
        "currency": asset.get("currency"),
        "live_price": live_price,
        "daily_chg": daily_chg,
        "states": states,
        "px": {tf: states[tf].px_state for tf in ("M", "W", "D")},
        "bias": {tf: states[tf].bias for tf in ("M", "W", "D")},
    }


def sar_break_state_from_close_sar(closes: list[float], sar_values: list[float]) -> dict:
    """Detect SAR crosses on closed H1 candles.

    BULL SAR break: previous close <= previous SAR and current close > current SAR.
    BEAR SAR break: previous close >= previous SAR and current close < current SAR.
    """
    events: list[dict] = []
    count = min(len(closes), len(sar_values))
    for index in range(1, count):
        close = float(closes[index])
        prev_close = float(closes[index - 1])
        sar = float(sar_values[index])
        prev_sar = float(sar_values[index - 1])

        if prev_close <= prev_sar and close > sar:
            events.append({
                "index": index,
                "direction": 1,
                "level": sar,
                "kind": "SAR BULL",
            })
        elif prev_close >= prev_sar and close < sar:
            events.append({
                "index": index,
                "direction": -1,
                "level": sar,
                "kind": "SAR BEAR",
            })

    last_event = events[-1] if events else None
    last_bar_index = count - 1
    last_bar_direction = (
        int(last_event["direction"])
        if last_event is not None and int(last_event["index"]) == last_bar_index
        else 0
    )
    return {
        "last_bar_sar_break_direction": last_bar_direction,
        "last_bar_sar_break_kind": (
            str(last_event["kind"])
            if last_event is not None and int(last_event["index"]) == last_bar_index
            else ""
        ),
        "last_sar_break_direction": int(last_event["direction"]) if last_event else 0,
        "last_sar_break_kind": str(last_event["kind"]) if last_event else "",
        "last_sar_break_level": float(last_event["level"]) if last_event else None,
        # Anciennete en bougies H1 de la derniere cassure (0 = bougie la plus
        # recente, 1 = l'avant-derniere, etc.). None sans cassure connue.
        "bars_since_last_break": (
            last_bar_index - int(last_event["index"]) if last_event is not None else None
        ),
        "events": events,
    }


def compute_sar_break_state_for_symbol(tv_symbol: str, h1_candles: int = 400) -> dict:
    df = fetch_tv_ohlc(tv_symbol, "60", h1_candles)
    if df is None or df.empty:
        return {"last_bar_sar_break_direction": 0, "last_sar_break_direction": 0}
    closed_df = closed_h1_source(df)
    sar_state = parabolic_sar(closed_df)
    if not sar_state:
        return {"last_bar_sar_break_direction": 0, "last_sar_break_direction": 0}
    sar_values = sar_state.get("sar") or []
    closes = [float(value) for value in closed_df["close"].tolist()]
    return sar_break_state_from_close_sar(closes, sar_values)


def _px(row: dict) -> dict[str, int] | None:
    px = row.get("px") or {}
    if any(px.get(tf) not in (-1, 0, 1) for tf in ("M", "W", "D")):
        return None
    return {tf: int(px[tf]) for tf in ("M", "W", "D")}


def full_alignment_direction(row: dict) -> int:
    """Return +1/-1 for strict M/W/D alignment, else 0."""
    px = _px(row)
    if px is None:
        return 0
    if px["M"] == 1 and px["W"] == 1 and px["D"] == 1:
        return 1
    if px["M"] == -1 and px["W"] == -1 and px["D"] == -1:
        return -1
    return 0


def raw_alignment_score(row: dict) -> float:
    """Raw M/W/D score using the same 3/2/1 weights as the VIVIER scanner."""
    px = _px(row)
    if px is None:
        return 0.0
    return (px["M"] * 3.0 + px["W"] * 2.0 + px["D"] * 1.0) / 6.0 * 100.0


def select_full_alignment_rows(rows: list[dict]) -> list[dict]:
    selected: list[dict] = []
    for row in rows:
        direction = full_alignment_direction(row)
        if direction == 0:
            continue
        enriched = dict(row)
        enriched["full_alignment_direction"] = direction
        enriched["raw_alignment_score"] = raw_alignment_score(row)
        selected.append(enriched)

    def sort_key(row: dict) -> tuple[int, tuple[int, float], int, str]:
        asset_rank = 1 if row.get("asset_type") == "INDEX" else 0
        direction = int(row["full_alignment_direction"])
        return (
            asset_rank,
            _daily_chg_sort_key(row),
            0 if direction == 1 else 1,
            str(row["pair"]),
        )

    return sorted(selected, key=sort_key)


def mid_alignment_candidate(row: dict) -> tuple[int, str]:
    """Return (+/-1, TF label) when at least 2 TFs are aligned."""
    px = _px(row)
    if px is None:
        return 0, ""

    full_direction = full_alignment_direction(row)
    if full_direction != 0:
        return full_direction, "M/W/D"

    for first, second in (("D", "M"), ("D", "W"), ("W", "M")):
        first_value = px[first]
        second_value = px[second]
        if first_value != 0 and first_value == second_value:
            return first_value, f"{first}/{second}"
    return 0, ""


def select_mid_alignment_candidates(rows: list[dict]) -> list[dict]:
    candidates: list[dict] = []
    for row in rows:
        direction, tf_pair = mid_alignment_candidate(row)
        if direction == 0:
            continue
        enriched = dict(row)
        enriched["mid_alignment_direction"] = direction
        enriched["mid_alignment_pair"] = tf_pair
        enriched["raw_alignment_score"] = raw_alignment_score(row)
        candidates.append(enriched)

    def sort_key(row: dict) -> tuple[int, int, int, str]:
        asset_rank = 1 if row.get("asset_type") == "INDEX" else 0
        direction = int(row["mid_alignment_direction"])
        tf_rank = 0 if row.get("mid_alignment_pair") == "M/W/D" else 1
        return (asset_rank, 0 if direction == 1 else 1, tf_rank, row["pair"])

    return sorted(candidates, key=sort_key)


def is_directional_sar_break(row: dict, direction: int) -> bool:
    sar_break = row.get("sar_break") or {}
    return int(sar_break.get("last_bar_sar_break_direction") or 0) == direction


def select_mid_sar_rows(rows: list[dict]) -> list[dict]:
    return [
        row for row in rows
        if row.get("mid_alignment_pair") in MID_SAR_ALLOWED_TF_PAIRS
        and is_directional_sar_break(row, int(row.get("mid_alignment_direction") or 0))
    ]


def has_consecutive_same_sign_tfs(row: dict) -> bool:
    """Return True if the asset has 2 consecutive timeframes with the same non-zero sign (+ or -).

    Consecutive timeframe pairs checked: (Monthly, Weekly) and (Weekly, Daily).
    """
    px = _px(row)
    if px is None:
        return False
    m, w, d = px["M"], px["W"], px["D"]
    if m != 0 and m == w:
        return True
    if w != 0 and w == d:
        return True
    return False


def has_consistent_tf_counts(row: dict) -> bool:
    """Return False if positive daily_chg has count(-) > count(+) or negative daily_chg has count(+) > count(-)."""
    px = _px(row)
    if px is None:
        return False

    daily_chg = row.get("daily_chg")
    if not isinstance(daily_chg, (int, float)):
        return True

    pos_count = sum(1 for tf_val in px.values() if tf_val == 1)
    neg_count = sum(1 for tf_val in px.values() if tf_val == -1)

    if daily_chg > 0 and neg_count > pos_count:
        return False
    if daily_chg < 0 and pos_count > neg_count:
        return False

    return True


def select_index_daily_chg_rows(rows: list[dict], exclude_pairs: set[str] | None = None) -> list[dict]:
    excluded = exclude_pairs or set()
    index_rows = [
        row
        for row in rows
        if row.get("asset_type") == "INDEX"
        and str(row.get("pair") or "") not in excluded
        and isinstance(row.get("daily_chg"), (int, float))
        and abs(float(row["daily_chg"])) >= 0.005
        and has_consecutive_same_sign_tfs(row)
        and has_consistent_tf_counts(row)
    ]
    return sorted(
        index_rows,
        key=lambda row: (*_daily_chg_sort_key(row), str(row.get("pair") or "")),
    )


def _streak_note(row: dict) -> float:
    """Note streak M/W/D ponderee par timeframe.

    Chaque TF vaut le signe de son STATUS (BULL +1, BEAR -1), mais seulement si
    le streak Renko dans ce sens est engage; un streak a 0 annule le TF. La
    contribution est ensuite ponderee (Monthly 3, Weekly 2, Daily 1: un TF lent
    engage davantage), puis renormalisee sur l'echelle -3..+3 pour rester
    comparable a la note non ponderee.

    Ex. GBPUSD M+(3) W+(0) D+(1) -> (3x1 + 2x0 + 1x1) / 2 = 2.0."""
    states = row.get("states") or {}
    bias = row.get("bias") or {}
    total = 0.0
    for tf in ("M", "W", "D"):
        state = states.get(tf)
        raw_direction = bias.get(tf)
        if state is None or raw_direction not in (-1, 1):
            continue
        direction = 1 if raw_direction == 1 else -1
        streak = state.green_streak if direction == 1 else state.red_streak
        if int(streak) > 0:
            total += STREAK_TF_WEIGHTS[tf] * direction
    return total * STREAK_NOTE_NORMALIZER


def strength_score(row: dict) -> float | None:
    """Intensite d'un indice ou d'une paire: |note streak M/W/D| x |CHG%D|.

    La note streak (cf. _streak_note) ne compte que les TF dont le streak Renko
    confirme le STATUS, ponderes 3/2/1: GBPUSD M+(3) W+(0) D+(1) vaut 2.0, pas
    3. Le score est une magnitude: le sens reste porte par l'icone 🟢/🔴."""
    chg = row.get("daily_chg")
    if not isinstance(chg, (int, float)):
        return None
    return abs(_streak_note(row)) * abs(float(chg))


def _strength_sort_key(row: dict) -> tuple[int, float, str]:
    score = strength_score(row)
    if score is None:
        return (1, 0.0, str(row.get("pair") or ""))
    return (0, -score, str(row.get("pair") or ""))


def signed_strength(row: dict) -> float | None:
    """Force signee: le score d'intensite, oriente par le sens du CHG%D.

    C'est exactement ce que la ligne Telegram affiche: 🟢 AUD (+0.60) vaut
    +0.60, 🔴 JPY (+0.15) vaut -0.15. Le lecteur peut donc refaire le calcul
    de tete depuis le message."""
    score = strength_score(row)
    if score is None:
        return None
    chg = row.get("daily_chg")
    return -score if isinstance(chg, (int, float)) and chg < 0 else score


def currency_spread(row: dict, index_by_currency: dict[str, dict] | None) -> float | None:
    """Attente du moteur devise pour une paire: force(base) - force(quote).

    Une quote faible pousse la paire a la hausse autant qu'une base forte, donc
    les deux effets se cumulent sur un axe signe: AUD +0.60 contre JPY -0.15
    donne 0.60 - (-0.15) = +0.75. Renvoie None pour les indices et pour les
    actifs sans indice devise (XAUUSD)."""
    if not index_by_currency or row.get("asset_type") == "INDEX":
        return None
    currencies = _pair_currencies(str(row.get("pair") or ""))
    if currencies is None:
        return None
    base_row = index_by_currency.get(currencies[0])
    quote_row = index_by_currency.get(currencies[1])
    if not base_row or not quote_row:
        return None
    base = signed_strength(base_row)
    quote = signed_strength(quote_row)
    if base is None or quote is None:
        return None
    return base - quote


def is_engine_divergent(row: dict, index_by_currency: dict[str, dict] | None) -> bool:
    """La paire contredit-elle son moteur devise ?

    Le score realise est une magnitude, donc le signe du produit
    realise x attendu est celui de l'attendu: une paire rouge avec un produit
    positif (ou verte avec un produit negatif) va a l'inverse de ce que ses
    devises impliquent. Ces paires sont retirees de la section."""
    expected = currency_spread(row, index_by_currency)
    if expected is None or expected == 0:
        return False
    chg = row.get("daily_chg")
    if not isinstance(chg, (int, float)) or chg == 0:
        return False
    return (chg > 0) != (expected > 0)


def signed_sync_product(row: dict, index_by_currency: dict[str, dict] | None) -> float | None:
    """Produit realise x attendu, avec le signe de l'attendu.

    Le realise etant une magnitude, le signe du produit est celui du moteur
    devise: negatif sur une paire verte (ou positif sur une paire rouge), il
    signale que la paire va a l'inverse de ses devises."""
    expected = currency_spread(row, index_by_currency)
    realized = strength_score(row)
    if expected is None or realized is None:
        return None
    return realized * expected


def sync_product(row: dict, index_by_currency: dict[str, dict] | None) -> float | None:
    """Intensite du produit, sans le signe: sert de classement de la section."""
    product = signed_sync_product(row, index_by_currency)
    return None if product is None else abs(product)


def sync_marker(row: dict, index_by_currency: dict[str, dict] | None) -> str:
    """Marqueur de synchronisation des deux moteurs pour une paire.

    🎯 les deux moteurs disent la meme chose et la paire est deja engagee;
    ⏳ le moteur devise a du carburant mais la paire n'a pas encore casse.
    Rien quand l'ecart entre les deux devises est trop faible pour conclure.
    Les paires divergentes ne sont pas marquees: elles sont filtrees en amont
    par `all_pair_status_rows`."""
    expected = currency_spread(row, index_by_currency)
    realized = strength_score(row)
    if expected is None or realized is None or abs(expected) < SYNC_MIN_EXPECTED:
        return ""
    if is_engine_divergent(row, index_by_currency):
        return ""
    return " 🎯" if realized >= SYNC_MIN_REALIZED else " ⏳"


def all_index_status_rows(rows: list[dict]) -> list[dict]:
    """Tous les indices devises scannes, sans filtre de qualite: la section
    Telegram doit montrer le statut des 8 indices, y compris ceux ecartes de
    `select_index_daily_chg_rows` (CHG%D negligeable ou TF incoherents).

    Classement par score d'intensite decroissant (cf. index_strength_score):
    l'alignement des TF et l'amplitude du jour sont combines en une seule
    valeur, donc un indice a 1 seul TF mais gros CHG%D peut devancer un indice
    aligne qui bouge peu."""
    index_rows = [row for row in rows if row.get("asset_type") == "INDEX"]
    return sorted(index_rows, key=_strength_sort_key)


def all_pair_status_rows(
    rows: list[dict],
    index_by_currency: dict[str, dict] | None = None,
) -> list[dict]:
    """Statut des paires scannees, classees par intensite decroissante.

    Avec `index_by_currency`, la section devient exploitable telle quelle: le
    classement passe sur le produit realise x |attendu|, qui recompense a la
    fois le carburant devise et l'execution de la paire, et deux familles de
    lignes sont retirees:

    - celles qui contredisent leur moteur devise (cf. `is_engine_divergent`);
    - celles dont le produit est nul, c'est-a-dire sans streak Renko confirme
      (realise 0) ou sans ecart entre les deux devises (attendu 0): il n'y a
      rien a y lire. Le test porte sur les valeurs telles qu'affichees, a deux
      decimales: une ligne qui montrerait 0.00 d'un cote ou de l'autre ne dit
      rien de plus qu'un vrai zero."""
    pair_rows = [row for row in rows if row.get("asset_type") != "INDEX"]
    if not index_by_currency:
        return sorted(pair_rows, key=_strength_sort_key)

    kept: list[tuple[float, dict]] = []
    for row in pair_rows:
        if is_engine_divergent(row, index_by_currency):
            continue
        product = sync_product(row, index_by_currency)
        if not product:
            continue
        realized = strength_score(row) or 0.0
        expected = currency_spread(row, index_by_currency) or 0.0
        if round(realized, 2) == 0.0 or round(expected, 2) == 0.0:
            continue
        kept.append((product, row))

    return [
        row for _, row in
        sorted(kept, key=lambda item: (-item[0], str(item[1].get("pair") or "")))
    ]


def _index_rows_by_currency(rows: list[dict]) -> dict[str, dict]:
    return {
        str(row.get("currency")): row
        for row in rows
        if row.get("asset_type") == "INDEX" and row.get("currency")
    }


def _pair_currencies(pair: str) -> tuple[str, str] | None:
    pair = pair.upper()
    if len(pair) != 6:
        return None
    base, quote = pair[:3], pair[3:]
    known_currencies = {str(asset["currency"]) for asset in FOREX_INDEX_ASSETS}
    if base not in known_currencies or quote not in known_currencies:
        return None
    return base, quote


def is_premium_currency_profile(row: dict, index_by_currency: dict[str, dict]) -> bool:
    """Premium profile: full pair alignment + daily CHG not opposed + strong/weak currency indexes."""
    if row.get("asset_type") == "INDEX":
        return False
    direction = int(row.get("full_alignment_direction") or 0)
    if direction == 0:
        return False

    pair_chg = row.get("daily_chg")
    if not isinstance(pair_chg, (int, float)) or pair_chg * direction < 0:
        return False

    currencies = _pair_currencies(str(row.get("pair") or ""))
    if currencies is None:
        return False
    base, quote = currencies
    base_index = index_by_currency.get(base)
    quote_index = index_by_currency.get(quote)
    if base_index is None or quote_index is None:
        return False

    base_chg = base_index.get("daily_chg")
    quote_chg = quote_index.get("daily_chg")
    if not isinstance(base_chg, (int, float)) or not isinstance(quote_chg, (int, float)):
        return False

    base_index_direction = full_alignment_direction(base_index)
    quote_index_direction = full_alignment_direction(quote_index)

    if direction == 1:
        return (
            base_chg > 0
            and quote_chg < 0
            and (base_index_direction == 1 or quote_index_direction == -1)
        )
    return (
        base_chg < 0
        and quote_chg > 0
        and (base_index_direction == -1 or quote_index_direction == 1)
    )


def attach_premium_currency_profiles(rows: list[dict], all_rows: list[dict]) -> list[dict]:
    index_by_currency = _index_rows_by_currency(all_rows)
    enriched_rows: list[dict] = []
    for row in rows:
        enriched = dict(row)
        enriched["premium_currency_profile"] = is_premium_currency_profile(enriched, index_by_currency)
        enriched_rows.append(enriched)
    return enriched_rows


def has_opposite_currency_colors(row: dict, index_by_currency: dict[str, dict]) -> bool:
    """Return True if base and quote currencies have opposite index daily_chg colors.

    For INDEX assets, returns True (no constituent currency pair filtering).
    For PAIR assets:
    - BULL (direction == 1): base index daily_chg > 0 (GREEN) and quote index daily_chg < 0 (RED).
    - BEAR (direction == -1): base index daily_chg < 0 (RED) and quote index daily_chg > 0 (GREEN).
    """
    if row.get("asset_type") == "INDEX":
        return True

    direction = int(
        row.get("full_alignment_direction")
        or row.get("mid_alignment_direction")
        or row.get("direction")
        or 0
    )
    if direction == 0:
        return False

    currencies = _pair_currencies(str(row.get("pair") or ""))
    if currencies is None:
        return False

    base, quote = currencies
    base_index = index_by_currency.get(base)
    quote_index = index_by_currency.get(quote)
    if base_index is None or quote_index is None:
        return False

    base_chg = base_index.get("daily_chg")
    quote_chg = quote_index.get("daily_chg")
    if not isinstance(base_chg, (int, float)) or not isinstance(quote_chg, (int, float)):
        return False

    if direction == 1:
        return base_chg > 0 and quote_chg < 0
    if direction == -1:
        return base_chg < 0 and quote_chg > 0

    return False


def is_daily_chg_aligned(row: dict) -> bool:
    """Return True if daily_chg is strictly aligned with the signal direction and meets minimum magnitude.

    - For PAIR assets: abs(daily_chg) must be >= 0.05% (>= 0.045% unrounded).
    - For INDEX assets: abs(daily_chg) must be >= 0.005%.
    - BULL (direction == 1, green 🟢): daily_chg must be strictly positive (> 0).
    - BEAR (direction == -1, red 🔴): daily_chg must be strictly negative (< 0).
    """
    direction = int(
        row.get("full_alignment_direction")
        or row.get("mid_alignment_direction")
        or row.get("direction")
        or 0
    )
    if direction == 0:
        return False

    daily_chg = row.get("daily_chg")
    if isinstance(daily_chg, (int, float)):
        min_threshold = 0.045 if row.get("asset_type") != "INDEX" else 0.005
        if abs(daily_chg) < min_threshold:
            return False
        if daily_chg * direction <= 0:
            return False

    return has_consistent_tf_counts(row)


def has_at_least_one_valid_index_currency(row: dict, valid_index_currencies: set[str]) -> bool:
    """Return True if asset is INDEX, or if at least one of its constituent currencies is in valid_index_currencies."""
    if row.get("asset_type") == "INDEX":
        return True
    currencies = _pair_currencies(str(row.get("pair") or ""))
    if currencies is None:
        return False
    base, quote = currencies
    return base in valid_index_currencies or quote in valid_index_currencies


def filter_conflicting_currency_pairs(rows: list[dict]) -> list[dict]:
    """Option C: Strictly eliminate all pairs involving any currency that has contradictory directional biases (+1 and -1)."""
    currency_biases: dict[str, set[int]] = {}

    for row in rows:
        if row.get("asset_type") == "INDEX":
            continue
        direction = int(
            row.get("full_alignment_direction")
            or row.get("mid_alignment_direction")
            or row.get("direction")
            or 0
        )
        if direction == 0:
            continue
        currencies = _pair_currencies(str(row.get("pair") or ""))
        if currencies is None:
            continue
        base, quote = currencies
        currency_biases.setdefault(base, set()).add(direction)
        currency_biases.setdefault(quote, set()).add(-direction)

    conflicted_currencies = {
        curr for curr, biases in currency_biases.items()
        if 1 in biases and -1 in biases
    }

    if not conflicted_currencies:
        return rows

    clean_rows: list[dict] = []
    for row in rows:
        if row.get("asset_type") == "INDEX":
            clean_rows.append(row)
            continue
        currencies = _pair_currencies(str(row.get("pair") or ""))
        if currencies is None:
            continue
        base, quote = currencies
        if base in conflicted_currencies or quote in conflicted_currencies:
            continue
        clean_rows.append(row)

    return clean_rows


def default_mid_sar_history_state() -> dict:
    return {"version": 1, "days": {}}


def load_mid_sar_history_state(path: str | Path) -> dict:
    state_path = Path(path)
    if not state_path.exists():
        return default_mid_sar_history_state()
    try:
        loaded = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return default_mid_sar_history_state()
    if not isinstance(loaded, dict):
        return default_mid_sar_history_state()
    loaded.setdefault("version", 1)
    if not isinstance(loaded.get("days"), dict):
        loaded["days"] = {}
    return loaded


def save_mid_sar_history_state(path: str | Path, state: dict) -> None:
    state_path = Path(path)
    if state_path.parent != Path("."):
        state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _is_mid_sar_tracking_window(now: datetime) -> bool:
    paris_now = now.astimezone(PARIS_TZ)
    return MID_SAR_WINDOW_START_HOUR <= paris_now.hour <= MID_SAR_WINDOW_END_HOUR


def _prune_mid_sar_history_state(state: dict, keep_days: int = 45) -> None:
    days = state.setdefault("days", {})
    if not isinstance(days, dict):
        state["days"] = {}
        return
    for day_key in sorted(days)[:-keep_days]:
        days.pop(day_key, None)


def update_mid_sar_history(
    state: dict,
    mid_sar_rows: list[dict],
    now: datetime,
) -> tuple[dict, dict]:
    paris_now = now.astimezone(PARIS_TZ)
    day_key = f"{paris_now:%Y-%m-%d}"
    state.setdefault("version", 1)
    days = state.setdefault("days", {})
    if not isinstance(days, dict):
        days = {}
        state["days"] = days
    day_state = days.setdefault(day_key, {"events": []})
    events = day_state.setdefault("events", [])
    if not isinstance(events, list):
        events = []
        day_state["events"] = events

    if not _is_mid_sar_tracking_window(paris_now):
        _prune_mid_sar_history_state(state)
        return state, day_state

    by_key = {
        str(event.get("key")): event
        for event in events
        if isinstance(event, dict) and event.get("key")
    }
    timestamp = paris_now.isoformat(timespec="minutes")
    for row in mid_sar_rows:
        pair = str(row["pair"])
        direction = int(row.get("mid_alignment_direction") or 0)
        if direction == 0:
            continue
        key = f"{pair}|{direction}"
        event = by_key.get(key)
        if event is None:
            event = {
                "key": key,
                "pair": pair,
                "asset_type": row.get("asset_type", "PAIR"),
                "currency": row.get("currency"),
                "direction": direction,
                "tf_pairs": [],
                "first_seen": timestamp,
                "last_seen": timestamp,
                "count": 0,
            }
            events.append(event)
            by_key[key] = event

        tf_pair = str(row.get("mid_alignment_pair") or "")
        tf_pairs = event.setdefault("tf_pairs", [])
        if tf_pair and tf_pair not in tf_pairs:
            tf_pairs.append(tf_pair)
        event["last_seen"] = timestamp
        event["count"] = int(event.get("count") or 0) + 1

    day_state["events"] = sorted(
        [event for event in events if isinstance(event, dict)],
        key=lambda event: (
            1 if event.get("asset_type") == "INDEX" else 0,
            0 if int(event.get("direction") or 0) == 1 else 1,
            str(event.get("pair") or ""),
        ),
    )
    _prune_mid_sar_history_state(state)
    return state, day_state


def attach_sar_break_states(rows: list[dict], h1_candles: int = 400) -> list[dict]:
    for row in rows:
        tv_symbol = row.get("tv_symbol")
        if isinstance(tv_symbol, str) and tv_symbol:
            row["sar_break"] = compute_sar_break_state_for_symbol(tv_symbol, h1_candles)
    return rows


def _asset_display_name(row: dict) -> str:
    if row.get("asset_type") == "INDEX":
        return str(row.get("currency") or row["pair"])
    return str(row["pair"])


def _daily_chg_sort_key(row: dict) -> tuple[int, float]:
    value = row.get("daily_chg")
    if isinstance(value, (int, float)):
        return (0, -float(value))
    return (1, 0.0)


def load_price_trend_state(path: str | Path) -> dict:
    state_path = Path(path)
    try:
        loaded = json.loads(state_path.read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_price_trend_state(path: str | Path, state: dict) -> None:
    state_path = Path(path)
    tmp_path = state_path.with_name(f"{state_path.name}.tmp")
    tmp_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(state_path)


def _price_arrow(current: float, reference: object) -> str:
    if not isinstance(reference, (int, float)):
        return "→"
    tolerance = max(abs(current), abs(float(reference)), 1.0) * 1e-10
    delta = current - float(reference)
    if delta > tolerance:
        return "↑"
    if delta < -tolerance:
        return "↓"
    return "→"


def _pip_size(pair: str) -> float:
    """Taille de pip standard pour les paires du scanner (JPY = 0.01)."""
    return 0.01 if pair.endswith("JPY") else 0.0001


def _daily_chg_direction(value: object) -> int:
    """Sens (+1/-1/0) du CHG%D, meme logique de signe que `_daily_chg_icon`."""
    if not isinstance(value, (int, float)):
        return 0
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _trend_delta_pips(pair: str, current_price: float, baseline_price: object,
                      baseline_direction: int) -> float | None:
    """Ecart en pips depuis le premier signal du jour, oriente EN FAVEUR de ce
    signal: positif si le prix a avance dans le sens du signal (BEAR: une
    baisse compte positif; BULL: une hausse compte positif), negatif s'il est
    reparti contre. None si le sens du premier signal n'est pas exploitable
    (CHG%D neutre) ou si la reference n'a pas encore ete capturee."""
    if baseline_direction not in (1, -1) or not isinstance(baseline_price, (int, float)):
        return None
    delta_pips = (current_price - float(baseline_price)) / _pip_size(pair)
    return -delta_pips if baseline_direction == -1 else delta_pips


def _trend_continuation_icon(delta_pips: float | None) -> str:
    """✅ si le prix est reste (ou n'est reparti que de peu) dans le sens du
    premier signal du jour, ❌ s'il s'en est ecarte d'au moins
    TREND_ICON_MIN_PIPS dans le sens oppose. Vide sans ecart exploitable
    (cf. `_trend_delta_pips`)."""
    if delta_pips is None:
        return ""
    return "❌" if delta_pips < -TREND_ICON_MIN_PIPS else "✅"


def _format_trend_pips(delta_pips: float) -> str:
    """Ecart en pips affiche, meme convention de precision que le tracker
    VIVIER (`_format_pips`): un chiffre apres la virgule, signe explicite."""
    if abs(delta_pips) < 0.05:
        delta_pips = 0.0
    return f"{delta_pips:+.1f} pips"


def _default_h1_close_near_baseline_hour(tv_symbol: str, today_at_paris: datetime) -> float | None:
    """Cloture H1 la plus proche (au ou juste avant) de
    TREND_ICON_BASELINE_HOUR_PARIS heure Paris, aujourd'hui.

    Sert a reconstituer la vraie reference du premier signal meme quand la
    capture live n'a lieu que plus tard dans la journee (run du matin manque,
    etat corrompu a reparer, etc.): sans ce backfill, la reference serait
    prise sur le prix courant au moment de la reparation, potentiellement tres
    loin de 6H. Retourne None si la donnee H1 est indisponible ou insuffisante;
    l'appelant retombe alors sur le prix live du run en cours."""
    try:
        df = fetch_tv_ohlc(tv_symbol, "60", 48)
        if df is None or df.empty:
            return None
        target = today_at_paris.replace(
            hour=TREND_ICON_BASELINE_HOUR_PARIS, minute=0, second=0, microsecond=0,
        )
        local_index = df.index.tz_convert(PARIS_TZ)
        eligible = df.loc[local_index <= target]
        if eligible.empty:
            return None
        return float(eligible["close"].iloc[-1])
    except Exception:
        return None


def update_price_trends(previous: dict | None, rows: list[dict],
                        now: datetime,
                        h1_close_lookup=None) -> tuple[dict, dict[str, dict]]:
    """Compare les prix live du run a la reference du premier signal du jour
    et au run precedent, et indique si le prix continue dans le sens de ce
    premier signal.

    Toutes les paires sont memorisees, pas seulement celles selectionnees, afin
    qu'une nouvelle apparition ait deja une comparaison avec le cycle precedent.
    La baseline devient la cloture H1 la plus proche de
    TREND_ICON_BASELINE_HOUR_PARIS (6H Paris) aujourd'hui (cf.
    `_default_h1_close_near_baseline_hour`), avec repli sur le prix live du run
    si cette cloture H1 est indisponible. Le sens du CHG%D au moment de la
    capture sert de reference de tendance pour `trend_icon`.

    `h1_close_lookup(tv_symbol, clock) -> float | None` est injectable pour les
    tests; par defaut c'est `_default_h1_close_near_baseline_hour`, qui
    interroge TradingView.

    Chaque entree expose aussi `pips_vs_06h`: le meme ecart que `trend_pips`
    mais signe normalement (prix monte = positif), sans reorientation vers le
    sens du premier signal; et `pips_vs_previous_run`, le meme ecart mais
    depuis le run precedent plutot que depuis 06h -- le T0 du run suivant est
    toujours le prix de celui-ci. Les deux alimentent `eurusd_cross_check_lines`."""
    h1_close_lookup = h1_close_lookup or _default_h1_close_near_baseline_hour
    clock = now.astimezone(PARIS_TZ)
    today = clock.date().isoformat()
    old = previous if isinstance(previous, dict) else {}
    same_day = old.get("date") == today
    old_pairs = old.get("pairs", {}) if same_day else {}
    trends: dict[str, dict] = {}
    new_pairs: dict[str, dict] = {}
    for row in rows:
        if row.get("asset_type") == "INDEX":
            continue
        pair = str(row.get("pair") or "")
        price = row.get("live_price")
        if not pair or not isinstance(price, (int, float)) or price <= 0:
            continue
        price = float(price)
        prior = old_pairs.get(pair) or {}
        baseline = prior.get("baseline_price")
        # `baseline_ready` sans `baseline_price` exploitable signale un etat
        # d'avant le renommage baseline_07h -> baseline_price (ou tout etat
        # corrompu): on force une nouvelle capture plutot que de rester bloque
        # sans reference pour le reste de la journee.
        baseline_ready = bool(prior.get("baseline_ready")) and isinstance(
            baseline, (int, float),
        )
        baseline_direction = prior.get("baseline_direction", 0)
        if clock.hour >= TREND_ICON_BASELINE_HOUR_PARIS and not baseline_ready:
            tv_symbol = str(row.get("tv_symbol") or "")
            backfilled = h1_close_lookup(tv_symbol, clock) if tv_symbol else None
            # Repli sur le prix live du run si la cloture H1 de 6H est
            # indisponible (feed en panne, historique trop court, etc.): mieux
            # vaut une reference approximative que pas de reference du tout.
            baseline = backfilled if isinstance(backfilled, (int, float)) else price
            baseline_ready = True
            baseline_direction = _daily_chg_direction(row.get("daily_chg"))
        previous_price = prior.get("previous")
        trend_pips = _trend_delta_pips(pair, price, baseline, baseline_direction)
        # Ecart brut vs la reference 06h, signe dans le sens normal du prix
        # (positif = prix monte), a la difference de `trend_pips` qui est
        # oriente dans le sens du premier signal du jour. Sert de source
        # continue (run apres run, portee par la meme baseline persistee) a
        # `eurusd_cross_check_lines`, qui la reoriente ensuite par devise.
        pips_vs_06h = (
            (price - float(baseline)) / _pip_size(pair)
            if isinstance(baseline, (int, float)) else None
        )
        # Ecart brut depuis le run precedent (le T0 du run suivant est le
        # prix de ce run-ci, cf. `new_pairs[pair]["previous"]` plus bas): sert
        # a detecter un repli en cours meme quand `pips_vs_06h` reste engage
        # dans le sens du jour. None au tout premier run du jour, faute de
        # run precedent a comparer.
        pips_vs_previous_run = (
            (price - float(previous_price)) / _pip_size(pair)
            if isinstance(previous_price, (int, float)) else None
        )
        trends[pair] = {
            "price": price,
            "vs_06h": _price_arrow(price, baseline),
            "vs_previous": _price_arrow(price, previous_price),
            "trend_icon": _trend_continuation_icon(trend_pips),
            "trend_pips": trend_pips,
            "pips_vs_06h": pips_vs_06h,
            "pips_vs_previous_run": pips_vs_previous_run,
        }
        new_pairs[pair] = {
            "baseline_price": baseline,
            "baseline_ready": baseline_ready,
            "baseline_direction": baseline_direction,
            "previous": price,
        }
    return {
        "version": 2,
        "date": today,
        "updated_at_paris": clock.isoformat(),
        "pairs": new_pairs,
    }, trends


def _daily_chg_icon(value: object) -> str:
    if not isinstance(value, (int, float)):
        return "⚪"
    if value > 0:
        return "🟢"
    if value < 0:
        return "🔴"
    return "⚪"


def _history_asset_display_name(event: dict) -> str:
    if event.get("asset_type") == "INDEX":
        return str(event.get("currency") or event.get("pair") or "")
    return str(event.get("pair") or "")


def _format_history_time_range(event: dict) -> str:
    first_seen = str(event.get("first_seen") or "")
    last_seen = str(event.get("last_seen") or "")
    first_hm = first_seen[11:16] if len(first_seen) >= 16 else ""
    last_hm = last_seen[11:16] if len(last_seen) >= 16 else ""
    if first_hm and last_hm and first_hm != last_hm:
        return f"{first_hm}→{last_hm}"
    return first_hm or last_hm


def _allowed_mid_sar_tf_pairs(tf_pairs: object) -> list[str]:
    if not isinstance(tf_pairs, list):
        return []
    return [
        str(tf_pair)
        for tf_pair in tf_pairs
        if str(tf_pair) in MID_SAR_ALLOWED_TF_PAIRS
    ]


def _format_mid_sar_history_event(event: dict) -> str:
    direction = int(event.get("direction") or 0)
    icon = "🟢" if direction == 1 else "🔴"
    name = _history_asset_display_name(event)
    tf_pairs = _allowed_mid_sar_tf_pairs(event.get("tf_pairs") or [])
    tf_label = "+".join(tf_pairs) or "2TF"
    time_label = _format_history_time_range(event)
    suffix = f" {time_label}" if time_label else ""
    return f"{icon} {name} 🔥 {tf_label}{suffix}"


def _format_close_price(price: float) -> str:
    """Prix de cloture avec le nombre de decimales usuel FX: 3 pour les
    paires cotees en JPY (prix >= 20), 5 sinon."""
    decimals = 3 if price >= 20 else 5
    return f"{price:.{decimals}f}"


def _strength_status_lines(
    status_rows: list[dict],
    index_by_currency: dict[str, dict] | None = None,
    show_close_price: bool = False,
    price_trends: dict[str, dict] | None = None,
) -> list[str]:
    """Lignes compactes `icone NOM (score)` d'une section de statut.

    Avec `index_by_currency`, les paires n'affichent plus leur seul score mais
    le produit realise x attendu (cf. `sync_product`), qui condense en un
    nombre le carburant du moteur devise et l'execution de la paire.

    Avec `show_close_price`, la ligne se limite a `icone NOM (prix)`: ni le
    score/produit ni le marqueur de synchronisation (🎯/⏳) ne sont affiches.
    `price_trends` y ajoute alors ✅/❌ et l'ecart en pips depuis le premier
    signal du jour (6H Paris), oriente en faveur de ce signal (cf.
    `_trend_delta_pips`)."""
    lines: list[str] = []
    for row in status_rows:
        icon = _daily_chg_icon(row.get("daily_chg"))
        name = _asset_display_name(row)
        if show_close_price:
            close_price = row.get("live_price")
            value_txt = (
                f" ({_format_close_price(float(close_price))})"
                if isinstance(close_price, (int, float))
                else ""
            )
            trend = (price_trends or {}).get(str(row.get("pair") or "")) or {}
            trend_icon = trend.get("trend_icon") or ""
            trend_pips = trend.get("trend_pips")
            pips_txt = (
                f" {_format_trend_pips(trend_pips)}"
                if trend_icon and isinstance(trend_pips, (int, float))
                else ""
            )
            trend_txt = f" {trend_icon}{pips_txt}" if trend_icon else ""
            lines.append(f"{icon} {name}{value_txt}{trend_txt}")
            continue
        product = signed_sync_product(row, index_by_currency)
        if product is not None:
            value_txt = f"{product:.4f}"
        else:
            score = strength_score(row)
            value_txt = f"{score:+.2f}" if score is not None else "n/a"
        marker = sync_marker(row, index_by_currency)
        lines.append(f"{icon} {name} ({value_txt}){marker}")
    return lines


def streaky_pairs(rows: list[dict]) -> list[dict]:
    """Paires en alignement strict M/W/D (meme biais BULL ou BEAR sur les
    trois timeframes) avec un streak Renko actif (non nul) sur chacun.

    Le biais peut rester BULL/BEAR meme quand la derniere brique d'un TF est
    opposee (cf. `f_effective_bias`): dans ce cas le streak dans le sens du
    biais tombe a 0 (ex: 'W+(0)' - biais haussier mais aucune brique verte
    consecutive en cours). Ces cas-la sont exclus ici: on ne garde que les
    paires ou le streak est reellement engage sur les trois timeframes."""
    matches: list[dict] = []
    for row in rows:
        if row.get("asset_type") == "INDEX":
            continue
        bias = row.get("bias") or {}
        directions = {bias.get(tf) for tf in ("M", "W", "D")}
        if len(directions) != 1:
            continue
        direction = directions.pop()
        if direction not in (1, -1):
            continue
        states = row.get("states") or {}
        streak_attr = "green_streak" if direction == 1 else "red_streak"
        streaks: dict[str, int] = {}
        for tf in ("M", "W", "D"):
            streak = getattr(states.get(tf), streak_attr, 0)
            if not streak:
                break
            streaks[tf] = int(streak)
        else:
            px = row.get("px") or {}
            # Un TF au streak actif peut quand meme etre "porte" par
            # l'historique plutot que confirme par le prix live: f_effective_bias
            # ne suit le streak que quand px_state == 0 (prix encore a
            # l'interieur de la derniere brique). Le px_state oppose au biais
            # est structurellement impossible ici (le biais suivrait le px
            # directement) -- seul 0 signale un TF "a risque".
            carried_tfs = [tf for tf in ("M", "W", "D") if px.get(tf) != direction]
            matches.append({
                "pair": str(row.get("pair") or ""),
                "direction": direction,
                "streaks": streaks,
                "px": {tf: px.get(tf, 0) for tf in ("M", "W", "D")},
                "carried_tfs": carried_tfs,
            })
    matches.sort(key=lambda item: (-sum(item["streaks"].values()), item["pair"]))
    return matches


def streaky_sar_confirmed(sar_break: dict | None, direction: int) -> bool:
    """True si la derniere cassure SAR H1 connue va dans le sens de
    `direction` et remonte a au plus STREAKY_SAR_CONFIRM_WINDOW_BARS bougies
    H1 (0 = la bougie la plus recente): le declencheur 🎯 reste donc affiche
    quelques bougies apres l'evenement, pas uniquement au run exact ou il
    s'est produit."""
    sar_break = sar_break or {}
    if sar_break.get("last_sar_break_direction") != direction:
        return False
    bars_since = sar_break.get("bars_since_last_break")
    return isinstance(bars_since, int) and bars_since <= STREAKY_SAR_CONFIRM_WINDOW_BARS


def _streaky_lines(entries: list[dict]) -> list[str]:
    px_sign = {1: "+", -1: "-", 0: "0"}
    lines: list[str] = []
    for entry in entries:
        icon = "🟢" if entry["direction"] == 1 else "🔴"
        streaks = entry["streaks"]
        px = entry.get("px") or {}
        parts = " ".join(
            f"{tf}{px_sign.get(px.get(tf), '0')}({streaks[tf]})"
            for tf in ("M", "W", "D")
        )
        risk_txt = " ⚠️" if entry.get("carried_tfs") else ""
        sar_txt = " 🎯" if entry.get("sar_confirmed") else ""
        lines.append(f"{icon} {entry['pair']} {parts}{risk_txt}{sar_txt}")
    return lines


def update_pairs_out_state(
    previous: dict | None, pair_status_rows: list[dict], now: datetime,
) -> tuple[dict, list[dict]]:
    """Suit les paires qui ont appartenu au TOP5 de PAIRES CHG%D aujourd'hui
    et n'y sont plus.

    Une paire entre dans PAIRES OUT des qu'elle quitte le TOP5 (classee 6e ou
    plus loin, ou carrement retiree du filtre `all_pair_status_rows`) apres y
    avoir figure au moins une fois depuis le debut de la journee. Elle y reste
    tant qu'elle n'est pas revenue dans le TOP5: `out_since_paris` se
    reinitialise a chaque nouvelle sortie (elle ne bouge pas tant que la
    paire reste continuellement hors TOP5).

    Chaque entree porte `warning=True` quand la paire est sortie du filtre
    lui-meme (paire devenue divergente de ses devises, ou ecart devise sous
    le seuil de setup: marge cassee). `warning=False` si elle est encore
    filtree/valide, juste classee au-dela du TOP5 (encore dans la marge)."""
    clock = now.astimezone(PARIS_TZ)
    today = clock.date().isoformat()
    old = previous if isinstance(previous, dict) else {}
    same_day = old.get("date") == today
    ever_top5 = set(old.get("ever_top5") or []) if same_day else set()
    old_out_since = dict(old.get("out_since") or {}) if same_day else {}

    filtered_pairs = [str(row.get("pair") or "") for row in pair_status_rows]
    top5_now = set(filtered_pairs[:PAIR_SECTION_LIMIT])
    still_filtered = set(filtered_pairs)

    ever_top5 |= top5_now
    out_pairs = ever_top5 - top5_now

    out_since: dict[str, str] = {
        pair: old_out_since.get(pair) or clock.strftime("%H:%M")
        for pair in out_pairs
    }

    entries = sorted(
        (
            {
                "pair": pair,
                "warning": pair not in still_filtered,
                "out_since_paris": out_since[pair],
            }
            for pair in out_pairs
        ),
        key=lambda item: (item["out_since_paris"], item["pair"]),
    )

    return {
        "version": 1,
        "date": today,
        "ever_top5": sorted(ever_top5),
        "out_since": out_since,
    }, entries


def _pairs_out_lines(
    entries: list[dict], rows_by_pair: dict[str, dict] | None,
) -> list[str]:
    lines: list[str] = []
    for entry in entries:
        pair = str(entry.get("pair") or "")
        row = (rows_by_pair or {}).get(pair) or {}
        icon = _daily_chg_icon(row.get("daily_chg"))
        close_price = row.get("live_price")
        price_txt = (
            f" ({_format_close_price(float(close_price))})"
            if isinstance(close_price, (int, float))
            else ""
        )
        warning_txt = " ⚠️" if entry.get("warning") else ""
        since = entry.get("out_since_paris")
        since_txt = f" (depuis {since})" if since else ""
        lines.append(f"{icon} {pair}{price_txt}{warning_txt}{since_txt}")
    return lines


def update_performance_state(
    previous: dict | None,
    extended_now: object,
    ever_top5: object,
    price_trends: dict[str, dict] | None,
    now: datetime,
) -> tuple[dict, list[dict]]:
    """Suit la performance des paires passees par le TOP5 de PAIRES CHG%D, en
    'trades papier' qui simulent un copy-trading reel plutot que la variation
    brute depuis 6H:

    Ouverture: au 1er run ou une paire apparait dans le TOP5 strict
    (`ever_top5`), au `trend_pips` de CE run (la reference d'entree), pas au
    signal de 6H qui peut avoir couru des heures avant que la paire ne
    devienne visible. Si elle apparait deja en ❌ (signal invalide avant
    meme d'etre vue), aucun trader reel n'ouvrirait: la paire est marquee
    'ignored', impact 0.0 pip, et ne sera plus jamais (re)ouverte ce jour-la.

    Le gain courant d'une position ouverte est ensuite `trend_pips actuel -
    trend_pips d'entree`: comme `trend_pips` (cf. `update_price_trends`)
    partage la meme reference (baseline + sens) sur toute la journee, cette
    soustraction redonne exactement l'ecart de prix reel entre l'entree et
    l'instant present, sans reconstruire le prix ni le sens a la main. Le
    pic (`peak_pips`) de ce gain est memorise a chaque run pour le trailing
    stop ci-dessous.

    Fermeture, au premier des 5 evenements suivants:
    - 23h Paris (SESSION_END, cf. PERFORMANCE_FORCE_CLOSE_HOUR_PARIS): comme
      le DAY_END de VIVIER, TOUTE position encore ouverte a cette heure se
      cloture, quelle que soit sa situation par ailleurs (❌, trailing stop
      ou rang) -- prioritaire sur les 3 regles suivantes, et aucune nouvelle
      position ne s'ouvre plus a partir de cette heure-la;
    - la paire montre ❌ pour la 1ere fois depuis l'ouverture
      (STOP_FIRST_CROSS, prix de ce run);
    - trailing stop (TRAILING_STOP): une fois le pic >=
      PERFORMANCE_TRAILING_ARM_PIPS, le gain a redonne plus de
      PERFORMANCE_TRAILING_STOP_PCT de ce pic;
    - la paire quitte la fenetre tolerie `extended_now` (RANK_EXIT) -- plus
      large que le TOP5 strict qui conditionne l'ENTREE (cf.
      PERFORMANCE_EXTENDED_RANK_LIMIT): une position deja ouverte peut
      reculer jusqu'a ce rang sans etre coupee, tant qu'elle reste
      non-divergente (une paire devenue divergente est deja absente de
      `extended_now`, cf. `all_pair_status_rows`);
    - le changement de date Paris sans qu'un run a 23h n'ait eu lieu
      (SESSION_END aussi, filet de securite si le run de 23h a ete manque):
      tout ce qui restait ouvert se cloture au dernier `trend_pips` connu de
      la veille -- meme filet que VIVIER pour un run de 23h manque.

    Retourne l'etat mis a jour et la liste des trades clotures a ce run
    (vide la plupart du temps)."""
    clock = now.astimezone(PARIS_TZ)
    today = clock.date().isoformat()
    old = previous if isinstance(previous, dict) else {}
    tracking_date = old.get("tracking_date")
    same_day = tracking_date == today
    closed_trades = list(old.get("closed_trades") or [])
    newly_closed: list[dict] = []

    def _close(pair: str, pips: float, reason: str) -> None:
        trade = {
            "date": today, "pair": pair, "pips": round(float(pips), 1),
            "closed_at_paris": clock.isoformat(), "reason": reason,
        }
        closed_trades.append(trade)
        newly_closed.append(trade)

    open_trades: dict[str, dict] = dict(old.get("open_trades") or {}) if same_day else {}

    if tracking_date and not same_day:
        # Filet de securite si le run de 23h a ete manque (panne CI, etc.):
        # tout ce qui restait ouvert se cloture au dernier trend_pips connu,
        # comme le repli "missed 23:00 run" de VIVIER.
        for pair, trade in (old.get("open_trades") or {}).items():
            if trade.get("status") == "open":
                last_pips = trade.get("last_pips")
                if isinstance(last_pips, (int, float)):
                    _close(pair, last_pips, "SESSION_END")
        open_trades = {}

    force_close_now = clock.hour >= PERFORMANCE_FORCE_CLOSE_HOUR_PARIS

    extended_set = set(extended_now or [])
    for pair in set(ever_top5 or []) | set(open_trades):
        pair = str(pair)
        trend = (price_trends or {}).get(pair) or {}
        trend_pips = trend.get("trend_pips")
        trend_icon = trend.get("trend_icon")

        trade = open_trades.get(pair)
        if trade is None:
            # 1ere apparition de cette paire dans le TOP5 aujourd'hui: pas de
            # nouvelle position a l'heure de cloture forcee, ce serait pour
            # la fermer dans la foulee.
            if force_close_now:
                continue
            if trend_icon == "❌":
                open_trades[pair] = {"status": "ignored", "last_pips": 0.0}
                continue
            if not isinstance(trend_pips, (int, float)):
                continue  # pas encore de reference exploitable; on retente au run suivant
            open_trades[pair] = {
                "status": "open", "entry_pips": float(trend_pips),
                "last_pips": 0.0, "peak_pips": 0.0,
            }
            continue

        if trade.get("status") != "open":
            continue  # deja 'ignored' ou 'closed': plus qu'un trade par paire et par jour

        if isinstance(trend_pips, (int, float)):
            trade["last_pips"] = float(trend_pips) - trade["entry_pips"]
            trade["peak_pips"] = max(trade.get("peak_pips", 0.0), trade["last_pips"])

        if force_close_now:
            _close(pair, trade["last_pips"], "SESSION_END")
            trade["status"] = "closed"
        elif trend_icon == "❌":
            _close(pair, trade["last_pips"], "STOP_FIRST_CROSS")
            trade["status"] = "closed"
        elif (
            trade["peak_pips"] >= PERFORMANCE_TRAILING_ARM_PIPS
            and trade["last_pips"] <= trade["peak_pips"] * (1.0 - PERFORMANCE_TRAILING_STOP_PCT)
        ):
            _close(pair, trade["last_pips"], "TRAILING_STOP")
            trade["status"] = "closed"
        elif pair not in extended_set:
            _close(pair, trade["last_pips"], "RANK_EXIT")
            trade["status"] = "closed"

    return {
        "version": 2,
        "tracking_date": today,
        "open_trades": open_trades,
        "closed_trades": closed_trades,
    }, newly_closed


def _performance_period_totals(closed_trades: list[dict], date_keys: set[str]) -> dict:
    pips = [
        float(t["pips"]) for t in closed_trades
        if str(t.get("date") or "") in date_keys
    ]
    winning = [p for p in pips if p > PERFORMANCE_FLAT_EPSILON_PIPS]
    losing = [p for p in pips if p < -PERFORMANCE_FLAT_EPSILON_PIPS]
    return {
        "total_pips": sum(pips),
        "winning_pips": sum(winning),
        "losing_pips": sum(losing),
        "winning_trades": len(winning),
        "losing_trades": len(losing),
        "trades": len(pips),
    }


def _performance_date_keys_between(start, end) -> set[str]:
    days = (end - start).days
    return {(start + timedelta(days=i)).isoformat() for i in range(max(days, 0) + 1)}


def _performance_general_bilan(year_trades: list[dict], year: int) -> dict:
    """Meme formules que `_yearly_general_pip_report` (VIVIER) pour rester
    directement comparable: taux de reussite, taux de pips, profit factor,
    score de robustesse (NS = sqrt(win_rate x pip_rate)), ratio gain/perte
    moyen, et un drawdown max calcule sur la courbe d'equite realisee
    (trades ordonnes par date/heure de cloture)."""
    winning = [t for t in year_trades if float(t["pips"]) > PERFORMANCE_FLAT_EPSILON_PIPS]
    losing = [t for t in year_trades if float(t["pips"]) < -PERFORMANCE_FLAT_EPSILON_PIPS]
    decided = len(winning) + len(losing)
    winning_pips = sum(float(t["pips"]) for t in winning)
    losing_pips = sum(float(t["pips"]) for t in losing)  # <= 0
    win_rate_pct = (len(winning) / decided * 100.0) if decided else 0.0
    pip_rate_pct = (
        (winning_pips - abs(losing_pips)) / winning_pips * 100.0
        if winning_pips > 0.0 else 0.0
    )
    average_win_pips = winning_pips / len(winning) if winning else 0.0
    average_loss_pips = abs(losing_pips) / len(losing) if losing else 0.0

    ordered = sorted(
        year_trades,
        key=lambda t: str(t.get("closed_at_paris") or t.get("date") or ""),
    )
    equity = equity_peak = max_drawdown_pips = 0.0
    for trade in ordered:
        equity += float(trade["pips"])
        equity_peak = max(equity_peak, equity)
        max_drawdown_pips = max(max_drawdown_pips, equity_peak - equity)

    return {
        "year": year,
        "closed_trades": len(year_trades),
        "winning_trades": len(winning),
        "losing_trades": len(losing),
        "flat_trades": len(year_trades) - decided,
        "win_rate_pct": win_rate_pct,
        "winning_pips": winning_pips,
        "losing_pips": losing_pips,
        "pip_rate_pct": pip_rate_pct,
        "profit_factor": (
            winning_pips / abs(losing_pips) if losing_pips < 0.0 else None
        ),
        "ns_score_pct": (
            math.sqrt(win_rate_pct * pip_rate_pct)
            if win_rate_pct >= 0.0 and pip_rate_pct >= 0.0 else None
        ),
        "average_win_pips": average_win_pips,
        "average_loss_pips": average_loss_pips,
        "gain_loss_ratio": (
            average_win_pips / average_loss_pips if average_loss_pips > 0.0 else None
        ),
        "closed_pips": sum(float(t["pips"]) for t in year_trades),
        "max_drawdown_pips": max_drawdown_pips,
    }


def performance_report(state: dict, now: datetime) -> dict:
    """Rapport DAILY/CUMULS + bilan general annuel a partir de l'etat
    persiste par `update_performance_state`."""
    clock = now.astimezone(PARIS_TZ)
    today = clock.date()
    closed_trades = state.get("closed_trades") or []
    year_prefix = f"{today.year:04d}-"
    year_trades = [
        t for t in closed_trades if str(t.get("date") or "").startswith(year_prefix)
    ]

    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    year_start = today.replace(month=1, day=1)

    open_trades = [
        trade for trade in (state.get("open_trades") or {}).values()
        if trade.get("status") == "open"
    ]
    open_pips_total = sum(
        float(trade.get("last_pips") or 0.0)
        for trade in open_trades
        if isinstance(trade.get("last_pips"), (int, float))
    )

    return {
        "daily": _performance_period_totals(closed_trades, {today.isoformat()}),
        "weekly": _performance_period_totals(
            closed_trades, _performance_date_keys_between(week_start, today),
        ),
        "monthly": _performance_period_totals(
            closed_trades, _performance_date_keys_between(month_start, today),
        ),
        "yearly": _performance_period_totals(
            closed_trades, _performance_date_keys_between(year_start, today),
        ),
        "general": _performance_general_bilan(year_trades, today.year),
        "open_pips_total": open_pips_total,
        "open_trades_count": len(open_trades),
    }


def performance_report_lines(report: dict, newly_closed_count: int) -> list[str]:
    """Lignes Telegram: DAILY/CUMULS a chaque run, BILAN GENERAL uniquement
    au run qui vient de clore des trades (changement de jour) -- comme
    VIVIER, ce pave complet n'a pas besoin d'etre repete a chaque cycle.

    La ligne 'Position ouverte', elle, est affichee a CHAQUE run (contrairement
    au BILAN GENERAL): DAILY ne compte que le realise (clos), donc sans elle
    rien ne distinguait 'aucune position en cours' de 'plusieurs positions
    ouvertes qui n'ont juste pas encore cloture' -- les deux affichaient
    +0.0 pips (0 trades) a l'identique."""
    daily = report["daily"]
    weekly = report["weekly"]
    monthly = report["monthly"]
    yearly = report["yearly"]
    open_trades_count = report.get("open_trades_count") or 0
    lines = [
        f"📈 PERF TOP5 — DAILY : {_format_trend_pips(daily['total_pips'])} "
        f"({daily['trades']} trades)",
        f"🟢 {_format_trend_pips(daily['winning_pips'])} | "
        f"🔴 {_format_trend_pips(daily['losing_pips'])}",
    ]
    if open_trades_count > 0:
        lines.append(
            f"🔓 Position ouverte : {_format_trend_pips(report['open_pips_total'])} "
            f"({open_trades_count} position{'s' if open_trades_count > 1 else ''})",
        )
    lines.extend([
        "",
        "📊 CUMULS TOP5",
        f"Weekly : {_format_trend_pips(weekly['total_pips'])}",
        f"Monthly : {_format_trend_pips(monthly['total_pips'])}",
        f"🗓 YTD {report['general']['year']} : {_format_trend_pips(yearly['total_pips'])}",
    ])
    if newly_closed_count > 0:
        general = report["general"]
        profit_factor = general.get("profit_factor")
        ns_score_pct = general.get("ns_score_pct")
        gain_loss_ratio = general.get("gain_loss_ratio")
        lines.extend([
            "",
            f"📋 BILAN GÉNÉRAL {general['year']}",
            f"{general['closed_trades']} trades clôturés",
            f"🟢 {general['winning_trades']} gagnants "
            f"({general['winning_pips']:.1f} pips)",
            f"🔴 {general['losing_trades']} perdants "
            f"({_format_trend_pips(general['losing_pips'])})",
        ])
        if general.get("flat_trades"):
            lines.append(f"⚪ {general['flat_trades']} neutres")
        lines.extend([
            f"NOG · Taux de réussite : {general['win_rate_pct']:.1f}%",
            f"SEL · Taux de pips : {general['pip_rate_pct']:.1f}%",
            "PF · Profit Factor : "
            + (f"{float(profit_factor):.2f}" if profit_factor is not None else "N/D"),
            "NS · Score de robustesse : "
            + (f"{float(ns_score_pct):.1f}%" if ns_score_pct is not None else "N/D"),
            "GAT · Ratio gain/perte moyen : "
            + (f"{float(gain_loss_ratio):.2f}" if gain_loss_ratio is not None else "N/D"),
            f"Max Drawdown clôturé : {-abs(float(general.get('max_drawdown_pips') or 0.0)):.1f} pips",
            f"Gains clôturés : {_format_trend_pips(general['closed_pips'])}",
            f"Position ouverte : {_format_trend_pips(report['open_pips_total'])}",
            f"Total affiché : "
            f"{_format_trend_pips(general['closed_pips'] + report['open_pips_total'])}",
        ])
    return lines


def currency_pip_sum(
    currency: str,
    rows_by_pair: dict[str, dict] | None,
    price_trends: dict[str, dict] | None,
    field: str = "pips_vs_06h",
) -> tuple[float, int]:
    """Somme des pips (champ `field` de `price_trends`, cf. `update_price_trends`)
    de toutes les paires impliquant `currency`, orientee dans le sens de force
    de cette devise: positive quand elle est base et monte, negative quand
    elle est quote et monte (la devise en face s'est renforcee a ses depens).

    Par defaut `field="pips_vs_06h"`: contrairement a un simple CHG%D (qui
    repart de zero a chaque cloture quotidienne), c'est porte par la baseline
    persistee dans `renko_full_alignment_price_trend_state.json` -- le meme
    point de depart reste valable d'un run a l'autre sur toute la journee.
    Avec `field="pips_vs_previous_run"`, la somme porte sur le run precedent
    plutot que sur 06h: sert a reperer un repli en cours (cf.
    `eurusd_cross_check_lines`)."""
    total = 0.0
    count = 0
    for row in (rows_by_pair or {}).values():
        if row.get("asset_type") == "INDEX":
            continue
        pair = str(row.get("pair") or "")
        currencies = _pair_currencies(pair)
        if currencies is None or currency not in currencies:
            continue
        trend = (price_trends or {}).get(pair) or {}
        pips = trend.get(field)
        if not isinstance(pips, (int, float)):
            continue
        base, _quote = currencies
        total += pips if base == currency else -pips
        count += 1
    return total, count


def pair_check_signals(
    pair: str,
    rows_by_pair: dict[str, dict] | None,
    price_trends: dict[str, dict] | None,
    index_by_currency: dict[str, dict] | None = None,
) -> list[tuple[str, str]]:
    """Liste ordonnee de `(label, bille)` parmi INDEX/06h/RUN pour `pair`,
    chacun present seulement si son signal est exploitable -- vide si `pair`
    n'est pas une paire de devises reconnue (cf. `_pair_currencies`) ou sans
    donnees 06h (le seul segment obligatoire). Sens base-fort/quote-faible
    (ou l'inverse) de `pair`, jamais les valeurs qui le produisent (celles-ci
    restent lisibles ailleurs: INDEX CHG%D, PAIRES CHG%D).

    INDEX (avec `index_by_currency`, sinon absent): les indices des deux
    devises de `pair` eux-memes, via le meme score que INDEX CHG%D (cf.
    `signed_strength`) -- un angle independant des paires.

    06h: la somme des pips des 7 paires de chaque devise de `pair` *depuis la
    reference 06h Paris*, suivie en continu run apres run par
    `update_price_trends` -- la meme reference que celle utilisee par PAIRES
    CHG%D, mais agregee ici par devise.

    RUN (des qu'un run precedent existe aujourd'hui, sinon absent): la meme
    somme mais *depuis le run precedent* -- un repli peut demarrer avant que
    le solde 06h ne bascule, donc ce signal le rend visible plus tot. RUN qui
    diverge de 06h signale un repli en cours malgre une tendance journaliere
    toujours engagee."""
    currencies = _pair_currencies(pair)
    if currencies is None:
        return []
    base, quote = currencies

    base_sum, base_count = currency_pip_sum(base, rows_by_pair, price_trends)
    quote_sum, quote_count = currency_pip_sum(quote, rows_by_pair, price_trends)
    if base_count == 0 or quote_count == 0:
        return []
    icon_06h = "🟢" if base_sum > quote_sum else ("🔴" if base_sum < quote_sum else "⚪")

    base_delta, base_delta_count = currency_pip_sum(
        base, rows_by_pair, price_trends, field="pips_vs_previous_run",
    )
    quote_delta, quote_delta_count = currency_pip_sum(
        quote, rows_by_pair, price_trends, field="pips_vs_previous_run",
    )
    icon_run = None
    if base_delta_count > 0 and quote_delta_count > 0:
        icon_run = (
            "🟢" if base_delta > quote_delta else ("🔴" if base_delta < quote_delta else "⚪")
        )

    icon_index = None
    base_index = (index_by_currency or {}).get(base)
    quote_index = (index_by_currency or {}).get(quote)
    if base_index is not None and quote_index is not None:
        base_strength = signed_strength(base_index)
        quote_strength = signed_strength(quote_index)
        if base_strength is not None and quote_strength is not None:
            icon_index = (
                "🟢" if base_strength > quote_strength
                else ("🔴" if base_strength < quote_strength else "⚪")
            )

    signals: list[tuple[str, str]] = []
    if icon_index is not None:
        signals.append(("INDEX", icon_index))
    signals.append(("06h", icon_06h))
    if icon_run is not None:
        signals.append(("RUN", icon_run))
    return signals


def pair_check_lines(
    pair: str,
    rows_by_pair: dict[str, dict] | None,
    price_trends: dict[str, dict] | None,
    index_by_currency: dict[str, dict] | None = None,
) -> list[str]:
    """Section `{PAIR} CHECK`: une ligne labellisee (`INDEX 🟢 · 06h 🟢 · RUN
    🔴`) a partir de `pair_check_signals` -- cf. cette fonction pour le sens
    de chaque bille. Vide si `pair_check_signals` l'est."""
    signals = pair_check_signals(pair, rows_by_pair, price_trends, index_by_currency)
    if not signals:
        return []
    return [f"🧭 {pair} CHECK", " · ".join(f"{label} {icon}" for label, icon in signals)]


def eurusd_cross_check_lines(
    rows_by_pair: dict[str, dict] | None,
    price_trends: dict[str, dict] | None,
    index_by_currency: dict[str, dict] | None = None,
) -> list[str]:
    """Section EURUSD CHECK affichee dans FULL MOMENTUM: fine enveloppe
    EURUSD-only de `pair_check_lines`, la version generalisee utilisee par
    PAIRE_CHECK (`paire_check.py`) pour EURUSD, CHFJPY, USDJPY, etc."""
    return pair_check_lines("EURUSD", rows_by_pair, price_trends, index_by_currency)


def format_full_alignment_message(
    rows: list[dict] | None = None,
    mid_sar_rows: list[dict] | None = None,
    mid_sar_history: dict | None = None,
    index_daily_chg_rows: list[dict] | None = None,
    now: datetime | None = None,
    index_status_rows: list[dict] | None = None,
    pair_status_rows: list[dict] | None = None,
    index_by_currency: dict[str, dict] | None = None,
    rows_by_pair: dict[str, dict] | None = None,
    price_trends: dict[str, dict] | None = None,
    pairs_out: list[dict] | None = None,
    performance_lines: list[str] | None = None,
    streaky: list[dict] | None = None,
) -> str:
    """Message FULL MOMENTUM: statut de tous les indices puis de toutes les
    paires, classes par score d'intensite.

    La liste des paires en alignement strict M/W/D n'est plus rendue: elle
    faisait doublon avec la section PAIRES, ou ces paires figurent deja avec
    leur intensite. `rows` reste accepte pour compatibilite, mais n'est plus
    affiche; la selection continue d'alimenter le sidecar et de conditionner
    l'envoi Telegram cote main().

    `pairs_out` (cf. `update_pairs_out_state`) ajoute la section PAIRES OUT:
    les paires qui ont appartenu au TOP5 aujourd'hui et n'y sont plus,
    marquees ⚠️ si elles sont sorties du filtre lui-meme (marge cassee).

    `performance_lines` (cf. `performance_report_lines`) ajoute le suivi de
    performance du TOP5: pips du jour, cumuls semaine/mois/annee, et un
    bilan general complet (taux de reussite, profit factor, drawdown...) au
    run qui vient de clore les trades de la veille.

    `streaky` (cf. `streaky_pairs`) ajoute la section STREAKY: les paires en
    alignement strict M/W/D (meme biais sur les trois timeframes) avec un
    streak Renko actif sur chacun -- independant du classement PAIRES CHG%D.

    EURUSD CHECK (cf. `eurusd_cross_check_lines`) resume en une ligne, par
    bille, jusqu'a 3 sens EUR fort/USD faible independants -- INDEX (avec
    `index_by_currency`), 06h et RUN (depuis `rows_by_pair`/`price_trends`) --
    sans repeter les valeurs deja lisibles dans INDEX CHG%D et PAIRES CHG%D."""
    now = (now or datetime.now(PARIS_TZ)).astimezone(PARIS_TZ)
    lines = ["📊 FULL MOMENTUM"]

    if index_status_rows is None:
        index_status_rows = index_daily_chg_rows
    if index_status_rows:
        lines.extend(["", "💱 INDEX CHG%D"])
        lines.extend(_strength_status_lines(index_status_rows))
    cross_check = eurusd_cross_check_lines(rows_by_pair, price_trends, index_by_currency)
    if cross_check:
        lines.extend([""])
        lines.extend(cross_check)
    if pair_status_rows:
        lines.extend(["", "💹 PAIRES CHG%D"])
        lines.extend(_strength_status_lines(
            pair_status_rows[:PAIR_SECTION_LIMIT], index_by_currency,
            show_close_price=True, price_trends=price_trends,
        ))
    if streaky:
        lines.extend(["", "🔥 STREAKY"])
        lines.extend(_streaky_lines(streaky))
    if pairs_out:
        lines.extend(["", "📤 PAIRES OUT"])
        lines.extend(_pairs_out_lines(pairs_out, rows_by_pair))
    if performance_lines:
        lines.extend([""])
        lines.extend(performance_lines)
    lines.extend(["", f"⏰ {now:%Y-%m-%d %H:%M} Paris"])
    return "\n".join(lines)


def scan_assets(assets: list[dict], length: int, candles: int, max_streak: int) -> list[dict]:
    rows: list[dict] = []
    for asset in assets:
        label = str(asset["pair"])
        try:
            row = compute_asset_score(asset, length, candles, max_streak)
        except Exception as exc:  # keep the scanner usable if one pair fails
            print(f"{label}: erreur {exc}")
            continue
        if row is not None:
            rows.append(row)
    return rows


def scan_pairs(length: int, candles: int, max_streak: int) -> list[dict]:
    return scan_assets(FOREX_PAIR_ASSETS, length, candles, max_streak)


def _sidecar_row(row: dict) -> dict:
    px = row.get("px") or {}
    return {
        "daily_chg": row.get("daily_chg"),
        # Niveau brut de l'indice/paire (close daily courant). Sert a FIOS pour
        # calculer un momentum robuste (variation du NIVEAU, immunise au rollover
        # du CHG%D qui se renormalise sur la cloture de la veille).
        "live_price": row.get("live_price"),
        "px_m": px.get("M"),
        "px_w": px.get("W"),
        "px_d": px.get("D"),
        "full_alignment": full_alignment_direction(row),
    }


def write_index_sidecar(path: Path, index_by_currency: dict, rows: list[dict],
                        now: datetime, selected_rows: list[dict] | None = None) -> None:
    """Ecrit un sidecar (daily_chg + px M/W/D + alignement strict) par devise ET
    par paire, lu par FIOS pour croiser avec sa force composite. Non-invasif :
    sans effet sur le message Telegram existant."""
    currencies: dict = {cur: _sidecar_row(row) for cur, row in index_by_currency.items()}
    pairs: dict = {
        str(row.get("pair")): _sidecar_row(row)
        for row in rows if row.get("asset_type") != "INDEX"
    }
    payload = {
        "generated_at": now.isoformat(),
        "paris_date": now.strftime("%Y-%m-%d"),
        "source": "renko_full_alignment_29pairs.py",
        "currencies": currencies,
        "pairs": pairs,
        "selected_pairs": {
            str(row.get("pair")): {
                "direction": full_alignment_direction(row),
                "live_price": row.get("live_price"),
                "daily_chg": row.get("daily_chg"),
                "premium_currency_profile": bool(row.get("premium_currency_profile")),
            }
            for row in (selected_rows or []) if row.get("asset_type") != "INDEX"
        },
    }
    try:
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    except Exception as exc:
        print(f"Sidecar index non ecrit: {exc}")


def main() -> int:
    args = parse_args()
    now = datetime.now(PARIS_TZ)
    rows = scan_assets(assets_for_scope(args.assets), args.length, args.candles, args.max_streak)
    price_state = load_price_trend_state(args.price_trend_state_file)
    price_state, price_trends = update_price_trends(price_state, rows, now)
    save_price_trend_state(args.price_trend_state_file, price_state)
    index_by_currency = _index_rows_by_currency(rows)
    rows_by_pair = {str(row.get("pair")): row for row in rows}

    index_daily_chg_rows = select_index_daily_chg_rows(rows)
    valid_index_currencies = {
        str(r.get("currency") or r.get("pair")) for r in index_daily_chg_rows
    }

    selected = select_full_alignment_rows(rows)
    selected = [
        row for row in selected
        if is_daily_chg_aligned(row)
        and has_opposite_currency_colors(row, index_by_currency)
        and has_at_least_one_valid_index_currency(row, valid_index_currencies)
    ]
    selected = filter_conflicting_currency_pairs(selected)
    selected = attach_premium_currency_profiles(selected, rows)
    write_index_sidecar(INDEX_SIDECAR_FILE, index_by_currency, rows, now, selected)

    mid_candidates = select_mid_alignment_candidates(rows)
    mid_candidates = [
        row for row in mid_candidates
        if is_daily_chg_aligned(row)
        and has_opposite_currency_colors(row, index_by_currency)
        and has_at_least_one_valid_index_currency(row, valid_index_currencies)
    ]
    mid_candidates = filter_conflicting_currency_pairs(mid_candidates)
    attach_sar_break_states(mid_candidates, args.sar_candles)
    mid_sar_rows = select_mid_sar_rows(mid_candidates)
    history_state = load_mid_sar_history_state(args.mid_sar_state_file)
    history_state, today_history = update_mid_sar_history(history_state, mid_sar_rows, now)
    save_mid_sar_history_state(args.mid_sar_state_file, history_state)
    index_status_rows = all_index_status_rows(rows)
    pair_status_rows = all_pair_status_rows(rows, index_by_currency)
    streaky = streaky_pairs(rows)
    # Fetch H1 cible: seulement les quelques paires STREAKY, pas les 28.
    streaky_sar_rows = [
        rows_by_pair[m["pair"]] for m in streaky if m["pair"] in rows_by_pair
    ]
    attach_sar_break_states(streaky_sar_rows, args.sar_candles)
    for match in streaky:
        sar_break = (rows_by_pair.get(match["pair"]) or {}).get("sar_break")
        match["sar_confirmed"] = streaky_sar_confirmed(sar_break, match["direction"])
    pairs_out_state = load_price_trend_state(args.pairs_out_state_file)
    pairs_out_state, pairs_out = update_pairs_out_state(pairs_out_state, pair_status_rows, now)
    save_price_trend_state(args.pairs_out_state_file, pairs_out_state)
    performance_state = load_price_trend_state(args.performance_state_file)
    extended_now = [
        str(row.get("pair") or "")
        for row in pair_status_rows[:PERFORMANCE_EXTENDED_RANK_LIMIT]
    ]
    performance_state, newly_closed = update_performance_state(
        performance_state, extended_now, pairs_out_state.get("ever_top5"), price_trends, now,
    )
    save_price_trend_state(args.performance_state_file, performance_state)
    # Rien a montrer avant qu'une premiere paire n'ait jamais atteint le TOP5:
    # evite une section vide (0 pip, 0 trade) au tout premier jour de suivi.
    performance_lines = (
        performance_report_lines(
            performance_report(performance_state, now), len(newly_closed),
        )
        if performance_state.get("open_trades") or performance_state.get("closed_trades")
        else []
    )
    message = format_full_alignment_message(
        selected,
        mid_sar_rows,
        today_history,
        index_daily_chg_rows,
        now=now,
        index_status_rows=index_status_rows,
        pair_status_rows=pair_status_rows,
        index_by_currency=index_by_currency,
        rows_by_pair=rows_by_pair,
        price_trends=price_trends,
        pairs_out=pairs_out,
        performance_lines=performance_lines,
        streaky=streaky,
    )
    print(message)
    if args.telegram:
        # INDEX CHG%D et PAIRES CHG%D sont deux sections independantes, chacune
        # avec sa propre selection/calcul (cf. all_index_status_rows et
        # all_pair_status_rows). Le gate suit desormais ce que le message
        # affiche reellement: il envoie des que l'une des deux a du contenu,
        # au lieu de l'ancienne liste d'alignement strict M/W/D retiree de
        # l'affichage par le commit "retitle FULL MOMENTUM", qui pouvait le
        # laisser silencieux alors que le message avait du contenu.
        if not index_status_rows and not pair_status_rows and not pairs_out and not streaky:
            print("Ni INDEX CHG%D ni PAIRES CHG%D n'ont de contenu : message Telegram ignoré.")
        else:
            send_telegram_message(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
