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
from datetime import datetime
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


def update_price_trends(previous: dict | None, rows: list[dict],
                        now: datetime) -> tuple[dict, dict[str, dict]]:
    """Compare les prix live du run aux references 07h et run precedent.

    Toutes les paires sont memorisees, pas seulement celles selectionnees, afin
    qu'une nouvelle apparition ait deja une comparaison avec le cycle precedent.
    La baseline devient le premier prix observe a partir de 07h Paris.
    """
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
        baseline = prior.get("baseline_07h")
        baseline_ready = bool(prior.get("baseline_ready"))
        if clock.hour >= 7 and not baseline_ready:
            baseline = price
            baseline_ready = True
        previous_price = prior.get("previous")
        trends[pair] = {
            "price": price,
            "vs_07h": _price_arrow(price, baseline),
            "vs_previous": _price_arrow(price, previous_price),
        }
        new_pairs[pair] = {
            "baseline_07h": baseline,
            "baseline_ready": baseline_ready,
            "previous": price,
        }
    return {
        "version": 1,
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
) -> list[str]:
    """Lignes compactes `icone NOM (score)` d'une section de statut.

    Avec `index_by_currency`, les paires n'affichent plus leur seul score mais
    le produit realise x attendu (cf. `sync_product`), qui condense en un
    nombre le carburant du moteur devise et l'execution de la paire.

    Avec `show_close_price`, la ligne se limite a `icone NOM (prix)`: ni le
    score/produit ni le marqueur de synchronisation (🎯/⏳) ne sont affiches."""
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
            lines.append(f"{icon} {name}{value_txt}")
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
) -> str:
    """Message FULL MOMENTUM: statut de tous les indices puis de toutes les
    paires, classes par score d'intensite.

    La liste des paires en alignement strict M/W/D n'est plus rendue: elle
    faisait doublon avec la section PAIRES, ou ces paires figurent deja avec
    leur intensite. `rows` reste accepte pour compatibilite, mais n'est plus
    affiche; la selection continue d'alimenter le sidecar et de conditionner
    l'envoi Telegram cote main()."""
    now = (now or datetime.now(PARIS_TZ)).astimezone(PARIS_TZ)
    lines = ["📊 FULL MOMENTUM"]

    if index_status_rows is None:
        index_status_rows = index_daily_chg_rows
    if index_status_rows:
        lines.extend(["", "💱 INDEX CHG%D"])
        lines.extend(_strength_status_lines(index_status_rows))
    if pair_status_rows:
        lines.extend(["", "💹 PAIRES CHG%D"])
        lines.extend(_strength_status_lines(
            pair_status_rows[:PAIR_SECTION_LIMIT], index_by_currency,
            show_close_price=True,
        ))
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
    pair_rows = [row for row in selected if row.get("asset_type") != "INDEX"]
    message = format_full_alignment_message(
        selected,
        mid_sar_rows,
        today_history,
        index_daily_chg_rows,
        now=now,
        index_status_rows=all_index_status_rows(rows),
        pair_status_rows=all_pair_status_rows(rows, index_by_currency),
        index_by_currency=index_by_currency,
        rows_by_pair=rows_by_pair,
        price_trends=price_trends,
    )
    print(message)
    if args.telegram:
        if not pair_rows:
            print("Aucune paire en alignement strict valide (>= 0.05%) : message Telegram ignoré.")
        else:
            send_telegram_message(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
