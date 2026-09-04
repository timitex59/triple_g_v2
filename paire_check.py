"""PAIRE_CHECK: une section INDEX CHG%D identique a celle de FULL MOMENTUM
(les 8 devises), suivie d'une section BEST PAIRE (une ligne par devise
doublement validee BULL associee a une devise doublement validee BEAR, cf.
`best_pair_names`), puis d'une ligne par paire, billes seules
(`EURUSD 🟢🟢🟢`), a partir des memes signaux INDEX/06h/RUN que
`pair_check_lines` dans renko_full_alignment_29pairs.py (cf.
`pair_check_signals`), pour un jeu de paires choisies -- sans lancer le
scan complet 29 paires de ce dernier.

- INDEX: renko M/W/D complet (cf. `compute_asset_score`) pour les 8 devises,
  comme FULL MOMENTUM -- alimente a la fois la section INDEX CHG%D affichee
  et le segment INDEX de chaque ligne PAIRES.
- 06h / RUN: close Daily (prix + CHG%D) pour le sous-ensemble des 28 paires
  OANDA du scanner qui impliquent au moins une devise des paires suivies --
  assez pour que `currency_pip_sum` retrouve ses 7 paires par devise, sans
  fetcher les 28.

L'etat de reference 06h/run precedent (`update_price_trends`) est persiste
dans son propre fichier, independant de celui de renko_full_alignment_29pairs
(meme mecanisme, cf. ce module, mais un cycle de run distinct).

2026-08-26: chaque ligne INDEX CHG%D porte en plus une boule de consensus
🟢/🔴/⚪ (cf. `currency_consensus_ball`) resumant les 5 votes reellement
distincts de CETTE devise (Renko M/W/D + D1 IMP21 + H1 IMP21, calcules par
`imp_trend_29pairs.compute_currency_imp`/`fetch_currency_imp_rows` -- le
meme moteur Renko+PSAR+IMP21 que pour les 29 paires, rejoue sur le symbole
TradingView de l'indice, TVC:DXY etc., pas juste son score Renko-streak
utilise pour le classement de la section). Les 6 moyennes D1/H1 ne comptent
pas dans ce consensus, meme raisonnement que celui deja applique aux paires
en V2 (cf. imp_trend_29pairs_v2.py): elles derivent toutes de la meme serie
de signaux que leur IMP21, 4 votes qui basculent ensemble ne sont pas 4
confirmations independantes. Une premiere version affichait les 11 boules
brutes avant cette correction (cf. historique git). Ajoute ~40 appels
TradingView par run (5 par devise x 8) en plus des ~32 deja utilises pour
INDEX.

2026-08-27: BEST PAIRE simplifie a la demande de l'utilisateur --
`best_pair_names` (qui remplace l'ancien `best_pair_name`) n'associe plus la
devise au CHG%D le plus fort et celle au CHG%D le plus faible quel que soit
leur consensus, mais uniquement des devises "doublement validees": jour ET
consensus d'accord entre eux (cf. `aligned_currency_names`), en sens inverse
l'une de l'autre (une confirmee forte, l'autre confirmee faible) -- une
devise contradictoire (comme GBP jour rouge/consensus vert) ou indecise
(un ⚪) n'est plus jamais retenue comme "forte" ou "faible" juste parce que
son CHG%D etait le plus extreme. Peut desormais produire 0, 1 ou plusieurs
BEST PAIRE selon le nombre de devises doublement validees de chaque cote --
une ligne par combinaison.

2026-08-27 (plus tard): section(s) `🏆 BEST PAIRE {devise}` dediee(s) (cf.
`--focus-currency`, `DEFAULT_FOCUS_CURRENCIES = ["JPY"]`), ajoutee(s) apres
la section BEST PAIRE generale. `best_pair_names(..., only_currency=devise)`
restreint aux combinaisons impliquant CETTE devise -- elle doublement BULL
associee a chaque devise doublement BEAR, ou l'inverse -- meme logique de
double confirmation que la section generale, juste filtree sur une devise
precise plutot que sur les 8.

2026-09-04: DEFAULT_PAIRS reduit a `["EURUSD"]` et DEFAULT_FOCUS_CURRENCIES
a `[]` -- PAIRE_CHECK se concentre desormais sur EURUSD seul par defaut (cf.
`--pairs`/`--focus-currency` pour revenir a un suivi multi-paires).

2026-09-04 (plus tard): section `📈 TENDANCE` -- pour chaque paire de
`--pairs`, compte run apres run (remis a zero chaque jour Paris) le nombre
de fois ou sa bille INDEX (cf. `pair_index_icon`, meme calcul que la 1ere
bille de `pair_check_signals`: force signee EUR vs USD) est monte (🟢) contre
baissee (🔴), et affiche `{bille} {PAIR} ({pct}%)` -- bille et pourcentage
de la direction majoritaire du jour. Contrairement a PAIRES, disponible des
le 1er run du jour (n'a pas besoin de la reference 06h). Compteur persiste
dans son propre fichier (cf. `INDEX_TREND_STATE_FILE`), independant de
`STATE_FILE` (etat 06h/RUN, remplace entierement a chaque run par
`update_price_trends`, cf. ce module).

2026-09-04 (encore plus tard): l'envoi Telegram (`--telegram`) est desormais
retenu avant `TELEGRAM_SEND_START_HOUR_PARIS` (5h Paris) -- les tout premiers
runs de la nuit tournent (INDEX + TENDANCE se construisent normalement) mais
n'envoient rien tant que 5h Paris n'est pas atteint, le temps que PAIRES et
TENDANCE aient assez de matiere pour etre utiles. Le compteur TENDANCE, lui,
continue de se remettre a zero a minuit Paris comme le reste de l'etat du
script (pas de decalage d'heure sur son propre cycle).

2026-09-04 (encore plus tard): section `PAIRES` retiree du message -- seule
la section `📈 TENDANCE` reste pour suivre `--pairs` (les billes
INDEX/06h/RUN de `pair_check_compact_line` restent calculees en interne,
seulement pour les sections BEST PAIRE). `index_trend_lines` affiche
desormais 2 lignes par paire au lieu d'une (🟢 puis 🔴, part des runs montes
et part des runs baisses sur le total de runs decisifs) plutot qu'une bille
+ % unique de la direction majoritaire -- montre l'ecart entre les 2 sens
sans que le lecteur ait a le deduire, et reste lisible meme a 100%/0% (tout
premiers runs du jour).

2026-09-04 (encore plus tard): `update_index_trend_state` pondere desormais
chaque run par recence (decroissance exponentielle, demi-vie
`INDEX_TREND_HALF_LIFE_HOURS` = 4h) plutot que de compter chaque run a poids
egal -- suite a une anomalie constatee en prod le jour meme: le fichier
d'etat du compteur n'etait pas persiste en CI (cf. commit suivant sur
`.github/workflows/triple_g_workflow.yml`), donc TENDANCE ne comptait jamais
qu'un seul run et restait fige a 100%/0%. Une fois le compteur reellement
cumulatif, un comptage brut aurait pose un autre probleme: un retournement
recent (EURUSD passe de haussier a baissier a 14h20 ce jour-la) restait noye
sous la majorite de la matinee (73%/27% a 16h25 avec un comptage brut, contre
~50%/50% avec la ponderation -- cf. historique git pour le detail du calcul
sur ces 15 runs reels)."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from ichimoku_v4 import fetch_tv_ohlc, send_telegram_message
from imp_trend_29pairs import (
    CURRENCY_SNAPSHOT_MAX_AGE,
    currency_consensus_status,
    currency_daily_status,
    fetch_or_load_currency_data,
)
from renko_full_alignment_29pairs import (
    FOREX_PAIR_ASSETS,
    PARIS_TZ,
    _pair_currencies,
    _strength_status_lines,
    all_index_status_rows,
    load_price_trend_state,
    pair_check_signals,
    save_price_trend_state,
    signed_strength,
    update_price_trends,
)

VOTE_BALL = {"BULL": "🟢", "BEAR": "🔴", "NEUTRAL": "⚪"}

# Modifier cette liste (ou passer --pairs) pour suivre d'autres paires.
DEFAULT_PAIRS = ["EURUSD"]
# Devises avec leur propre section "🏆 BEST PAIRE {devise}" (ou --focus-currency).
DEFAULT_FOCUS_CURRENCIES: list[str] = []
STATE_FILE = Path("paire_check_price_trend_state.json")
# Compteur monte/baisse (bille INDEX) par paire suivie -- fichier propre a ce
# script, distinct de STATE_FILE (etat 06h/RUN, gere par update_price_trends
# qui ne preserve pas de cle en plus des siennes).
INDEX_TREND_STATE_FILE = Path("paire_check_index_trend_state.json")
# Avant cette heure Paris, --telegram construit le message (INDEX, TENDANCE,
# etc. tournent normalement) mais n'envoie rien -- cf. `main`.
TELEGRAM_SEND_START_HOUR_PARIS = 5
# Demi-vie (en heures) de la ponderation par recence du compteur TENDANCE --
# un run perd la moitie de son poids toutes les `INDEX_TREND_HALF_LIFE_HOURS`
# heures, cf. `update_index_trend_state`.
INDEX_TREND_HALF_LIFE_HOURS = 4.0


def needed_currencies(pairs: list[str]) -> set[str]:
    """Devises impliquees par `pairs` (base + quote de chacune)."""
    currencies: set[str] = set()
    for pair in pairs:
        parsed = _pair_currencies(pair)
        if parsed:
            currencies.update(parsed)
    return currencies


def needed_helper_pairs(currencies: set[str]) -> list[str]:
    """Sous-ensemble des 28 paires OANDA du scanner qui impliquent au moins
    une devise de `currencies` -- assez pour que `currency_pip_sum` retrouve
    ses 7 paires par devise, sans fetcher les 28."""
    helper_pairs = []
    for asset in FOREX_PAIR_ASSETS:
        pair = str(asset["pair"])
        parsed = _pair_currencies(pair)
        if parsed and currencies & set(parsed):
            helper_pairs.append(pair)
    return helper_pairs


def fetch_pair_row(pair: str) -> dict | None:
    """Close Daily live + CHG%D pour `pair`: assez pour `update_price_trends`
    (pips depuis 06h / depuis le run precedent), sans le renko M/W/D complet
    -- inutile ici, seul INDEX en a besoin, pour les devises seulement."""
    tv_symbol = f"OANDA:{pair}"
    df = fetch_tv_ohlc(tv_symbol, "D", 50)
    if df is None or df.empty:
        print(f"{pair}: pas de donnees")
        return None
    live_price = float(df["close"].iloc[-1])
    prev_close = float(df["close"].iloc[-2]) if len(df) >= 2 else None
    daily_chg = (
        ((live_price - prev_close) / prev_close * 100.0)
        if prev_close is not None and prev_close != 0 else None
    )
    return {
        "pair": pair,
        "tv_symbol": tv_symbol,
        "asset_type": "PAIR",
        "live_price": live_price,
        "daily_chg": daily_chg,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PAIRE_CHECK: verdict INDEX/06h/RUN pour un jeu de paires choisies.",
    )
    parser.add_argument(
        "--pairs", nargs="+", default=DEFAULT_PAIRS,
        help=f"Paires a verifier (par defaut: {' '.join(DEFAULT_PAIRS)}).",
    )
    parser.add_argument(
        "--focus-currency", nargs="*", default=DEFAULT_FOCUS_CURRENCIES,
        help=f"Devises avec leur propre section BEST PAIRE dediee (par defaut: "
             f"{' '.join(DEFAULT_FOCUS_CURRENCIES)}). Vide (--focus-currency sans argument) pour desactiver.",
    )
    parser.add_argument("--length", type=int, default=14, help="ATR Renko length.")
    parser.add_argument(
        "--candles", type=int, default=300,
        help="Nombre de bougies/briques recuperees par symbole/timeframe pour INDEX.",
    )
    parser.add_argument(
        "--max-streak", type=int, default=50,
        help="Plafond du streak de briques Renko consecutives pour INDEX.",
    )
    parser.add_argument(
        "--imp-h1-candles", type=int, default=5000,
        help="Bougies H1 par devise pour la boule de consensus de INDEX CHG%%D (cf. currency_consensus_ball).",
    )
    parser.add_argument(
        "--imp-d1-candles", type=int, default=2500,
        help="Bougies D1 par devise pour les boules Renko/IMP21 de INDEX CHG%%D.",
    )
    parser.add_argument(
        "--imp-renko-bricks", type=int, default=2500,
        help="Briques Renko M/W/D par devise pour les boules Renko/IMP21 de INDEX CHG%%D.",
    )
    parser.add_argument(
        "--currency-snapshot", type=str, default="currency_snapshot.json",
        help="Fichier partage entre V1/V2/paire_check.py pour reutiliser un fetch devise recent au lieu de "
             "refetcher (cf. imp_trend_29pairs.fetch_or_load_currency_data). Vide pour desactiver.",
    )
    parser.add_argument(
        "--currency-snapshot-max-age", type=int, default=CURRENCY_SNAPSHOT_MAX_AGE,
        help="Age maximum en secondes d'un instantane devise reutilisable.",
    )
    parser.add_argument(
        "--state-file", default=str(STATE_FILE),
        help="Fichier d'etat JSON (reference 06h + run precedent), propre a ce script.",
    )
    parser.add_argument(
        "--index-trend-state-file", default=str(INDEX_TREND_STATE_FILE),
        help="Fichier d'etat JSON du compteur monte/baisse (section TENDANCE), remis a zero chaque jour.",
    )
    parser.add_argument(
        "--telegram", action="store_true",
        help="Envoie le resultat sur Telegram. Par defaut, affichage seul.",
    )
    return parser.parse_args()


def pair_check_compact_line(
    pair: str, rows_by_pair: dict[str, dict], price_trends: dict[str, dict],
    index_by_currency: dict[str, dict],
) -> str | None:
    """`"{PAIR} {billes}"` (ex. `EURUSD 🟢🟢🟢`), billes seules dans l'ordre
    INDEX/06h/RUN, sans label -- cf. `pair_check_signals` pour leur sens.
    None si aucun signal n'est exploitable pour `pair`."""
    signals = pair_check_signals(pair, rows_by_pair, price_trends, index_by_currency)
    if not signals:
        return None
    return f"{pair} " + "".join(icon for _label, icon in signals)


def pair_index_icon(pair: str, index_by_currency: dict[str, dict]) -> str | None:
    """Bille 🟢/🔴/⚪ de la seule comparaison INDEX (force signee, cf.
    `signed_strength`) entre les 2 devises de `pair` -- le meme calcul que la
    1ere bille de `pair_check_signals`, mais dispo independamment de l'etat
    06h/RUN (qui peut etre vide avant que la reference du jour ne soit posee,
    cf. `pair_check_signals` -- return[] tant que `currency_pip_sum` n'a pas
    de compte pour les 2 devises). Sert de base a
    `update_index_trend_state`: elle doit pouvoir compter des le tout premier
    run du jour, meme avant 06h Paris.

    None si `pair` n'est pas une paire de devises reconnue, ou si l'indice
    d'au moins une des 2 devises est absent (fetch echoue)."""
    currencies = _pair_currencies(pair)
    if currencies is None:
        return None
    base, quote = currencies
    base_index = index_by_currency.get(base)
    quote_index = index_by_currency.get(quote)
    if base_index is None or quote_index is None:
        return None
    base_strength = signed_strength(base_index)
    quote_strength = signed_strength(quote_index)
    if base_strength is None or quote_strength is None:
        return None
    if base_strength > quote_strength:
        return "🟢"
    if base_strength < quote_strength:
        return "🔴"
    return "⚪"


def update_index_trend_state(
    state: dict, pairs: list[str], index_by_currency: dict[str, dict], now: datetime,
) -> dict:
    """Ajoute le run courant au poids monte(🟢)/baisse(🔴) de chaque paire de
    `pairs`, base sur `pair_index_icon` -- une bille ⚪ (egalite) ou absente
    (donnees manquantes) ne compte ni pour l'un ni pour l'autre, mais ne fait
    pas non plus perdre l'historique deja accumule.

    Pondere par recence: avant d'ajouter le run courant, le poids deja
    accumule (`weighted_up`/`weighted_down`) est decote d'une decroissance
    exponentielle sur le temps ecoule depuis le run precedent (cf.
    `INDEX_TREND_HALF_LIFE_HOURS` -- un vote perd la moitie de son poids
    toutes les {demi-vie}h). Un simple comptage traite un run vieux de 10h
    exactement comme le dernier -- un retournement recent (cf. celui d'EURUSD
    le 2026-09-04: 11 runs montes de 06h a 13h20, puis 4 runs baisses de 14h20
    a 16h25) restait alors noye dans la majorite du matin (73%/27%) au lieu de
    ressortir. Avec 4h de demi-vie, ce meme historique donne ~50%/50% au
    dernier run: la moitie haussiere de la matinee et le retournement recent
    se contrebalancent, plutot que 73%/27% (comptage brut) ou 0%/100% (bug
    d'origine, cf. historique git -- fichier d'etat pas persiste en CI).

    Remise a zero chaque jour (nouvelle date Paris), meme mecanisme que la
    reference 06h de `update_price_trends` -- coherent avec un message qui
    raisonne "depuis 06h" sur le reste de sa journee (la decroissance seule
    ferait deja tomber un run de la veille a un poids negligeable, mais la
    remise a zero reste plus simple a raisonner)."""
    today = now.astimezone(PARIS_TZ).date().isoformat()
    old = state if isinstance(state, dict) else {}
    old_counts = old.get("pairs", {}) if old.get("date") == today else {}
    new_counts: dict[str, dict] = {}
    for pair in pairs:
        prior = old_counts.get(pair) or {}
        weighted_up = float(prior.get("weighted_up", 0.0))
        weighted_down = float(prior.get("weighted_down", 0.0))
        last_update = _parse_iso_datetime(prior.get("last_update"))
        if last_update is not None:
            elapsed_hours = max((now - last_update).total_seconds(), 0.0) / 3600.0
            decay = 0.5 ** (elapsed_hours / INDEX_TREND_HALF_LIFE_HOURS)
            weighted_up *= decay
            weighted_down *= decay
        icon = pair_index_icon(pair, index_by_currency)
        if icon == "🟢":
            weighted_up += 1.0
        elif icon == "🔴":
            weighted_down += 1.0
        new_counts[pair] = {
            "weighted_up": weighted_up, "weighted_down": weighted_down, "last_update": now.isoformat(),
        }
    return {"date": today, "pairs": new_counts}


def _parse_iso_datetime(value: object) -> datetime | None:
    """`datetime.fromisoformat(value)` tolerant: None si `value` n'est pas une
    chaine ISO exploitable (etat absent, corrompu, ou tout premier run)."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def index_trend_lines(pairs: list[str], trend_state: dict) -> list[str]:
    """Section `📈 TENDANCE`: pour chaque paire de `pairs` ayant un poids
    monte/baisse accumule aujourd'hui (cf. `update_index_trend_state`), 2
    lignes `🟢 {PAIR} ({pct_up}%)` puis `🔴 {PAIR} ({pct_down}%)` -- part
    ponderee par recence des runs montes puis baisses, sur le poids total.
    Les 2 lignes sont toujours affichees ensemble, meme quand l'une est a
    0.00% -- contrairement a une bille+% unique, elles montrent directement
    l'ecart entre les 2 sens sans qu'un lecteur ait a le deduire.
    Vide si aucune paire n'a de poids accumule (ex. tout premier run, ou
    seulement des egalites/donnees manquantes jusqu'ici)."""
    counts_by_pair = trend_state.get("pairs", {}) if isinstance(trend_state, dict) else {}
    lines = []
    for pair in pairs:
        counts = counts_by_pair.get(pair) or {}
        up = float(counts.get("weighted_up", 0.0))
        down = float(counts.get("weighted_down", 0.0))
        total = up + down
        if total <= 0.0:
            continue
        lines.append(f"🟢 {pair} ({up / total * 100.0:.2f}%)")
        lines.append(f"🔴 {pair} ({down / total * 100.0:.2f}%)")
    if not lines:
        return []
    return ["📈 TENDANCE", *lines]


def currency_consensus_ball(currency_row: dict) -> str:
    """Boule unique 🟢/🔴/⚪ resumant une devise -- rendu de
    `imp_trend_29pairs.currency_consensus_status` (le calcul lui-meme, cf. ce
    module pour son detail: 5 votes reellement distincts, seuil
    CURRENCY_CONSENSUS_THRESHOLD). Deplace dans imp_trend_29pairs.py le
    2026-08-27 pour etre reutilisable par `pair_touches_a_divergent_currency`
    (V1/V2) sans creer d'import circulaire (paire_check.py importe deja
    imp_trend_29pairs.py, pas l'inverse)."""
    return VOTE_BALL[currency_consensus_status(currency_row)]


def index_chg_lines(
    index_rows: list[dict], imp_by_currency: dict[str, dict] | None = None,
) -> list[str]:
    """Section `💱 INDEX CHG%D`, identique a celle de FULL MOMENTUM (les 8
    devises, memes icones/valeurs, cf. `all_index_status_rows` et
    `_strength_status_lines`). Vide si `index_rows` l'est.

    Avec `imp_by_currency` (cf. `fetch_currency_imp_rows`), chaque ligne
    reçoit en plus la boule de consensus de sa devise (cf.
    `currency_consensus_ball`) -- absent pour une devise dont le fetch a
    echoue, la ligne reste alors sans boule plutot que d'echouer."""
    sorted_rows = all_index_status_rows(index_rows)
    if not sorted_rows:
        return []
    base_lines = _strength_status_lines(sorted_rows)
    if not imp_by_currency:
        return ["💱 INDEX CHG%D", *base_lines]
    lines = ["💱 INDEX CHG%D"]
    for row, line in zip(sorted_rows, base_lines):
        currency_row = imp_by_currency.get(str(row.get("currency")))
        lines.append(line if currency_row is None else f"{line} {currency_consensus_ball(currency_row)}")
    return lines


KNOWN_PAIR_CODES = {str(asset["pair"]) for asset in FOREX_PAIR_ASSETS}  # les 28 paires OANDA reelles


def aligned_currency_names(
    index_rows: list[dict], imp_by_currency: dict[str, dict],
) -> tuple[list[str], list[str]]:
    """Devises "doublement validees" parmi les 8: jour ET consensus d'accord
    (cf. `imp_trend_29pairs.currency_daily_status`/`currency_consensus_status`
    -- memes calculs que la boule de consensus et le mouvement du jour deja
    affiches sur chaque ligne INDEX CHG%D), dans l'ordre d'intensite deja
    utilise pour cette section (cf. `all_index_status_rows`). Exclut les
    devises contradictoires (jour/consensus opposes, cf.
    `imp_trend_29pairs.currency_diverges_from_its_own_day`) et indecises (au
    moins un ⚪) -- celles-ci ne servent plus a designer un BEST PAIRE.
    Renvoie (bull, bear): les doublement BULL, les doublement BEAR."""
    bull, bear = [], []
    for row in all_index_status_rows(index_rows):
        currency = str(row.get("currency"))
        imp_row = imp_by_currency.get(currency)
        if imp_row is None:
            continue
        daily = currency_daily_status(row.get("daily_chg"))
        consensus = currency_consensus_status(imp_row)
        if daily == "BULL" and consensus == "BULL":
            bull.append(currency)
        elif daily == "BEAR" and consensus == "BEAR":
            bear.append(currency)
    return bull, bear


def best_pair_names(
    index_rows: list[dict], imp_by_currency: dict[str, dict], only_currency: str | None = None,
) -> list[str]:
    """Un BEST PAIRE par combinaison (devise doublement BULL, devise
    doublement BEAR) -- cf. `aligned_currency_names` -- "alignees sur leurs 2
    boules, mais en sens inverse l'une de l'autre" (l'une confirmee forte,
    l'autre confirmee faible), plutot que l'ancien classement par simple
    magnitude de CHG%D qui pouvait retenir une devise contradictoire (comme
    GBP jour rouge/consensus vert) comme si elle etait une "forte" ou une
    "faible" fiable. Peut renvoyer 0, 1 ou plusieurs paires -- une par
    combinaison dont le symbole existe reellement parmi les 28 paires OANDA
    (cf. `KNOWN_PAIR_CODES`; {forte}{faible} si cet ordre existe, sinon
    {faible}{forte} -- meme raisonnement que l'ancien `best_pair_name`,
    cf. historique git: les 8 devises couvrent chaque combinaison de 2
    exactement une fois parmi les 28 paires, jamais les deux sens a la fois,
    et l'inverser ne change pas le sens fort/faible lu ensuite par
    `pair_check_signals`, qui compare directement base et cotee).

    `only_currency` (ex. "JPY") restreint aux combinaisons impliquant cette
    devise -- elle-meme doublement BULL associee a chaque devise doublement
    BEAR, ou l'inverse. Liste vide si `only_currency` n'est ni doublement
    BULL ni doublement BEAR (contradictoire, indecise, ou donnees absentes) --
    cf. `best_pair_lines` pour la section dediee `BEST PAIRE {devise}`."""
    bull, bear = aligned_currency_names(index_rows, imp_by_currency)
    if only_currency is not None:
        if only_currency in bull:
            bull, bear = [only_currency], bear
        elif only_currency in bear:
            bull, bear = bull, [only_currency]
        else:
            return []
    pairs = []
    for strong in bull:
        for weak in bear:
            forward = f"{strong}{weak}"
            if forward in KNOWN_PAIR_CODES:
                pairs.append(forward)
                continue
            reverse = f"{weak}{strong}"
            if reverse in KNOWN_PAIR_CODES:
                pairs.append(reverse)
    return pairs


def best_pair_lines(
    pairs: list[str], rows_by_pair: dict[str, dict], price_trends: dict[str, dict],
    index_by_currency: dict[str, dict], label: str = "🏆 BEST PAIRE",
) -> list[str]:
    """Section `{label}`: une ligne par paire de `pairs` (cf.
    `best_pair_names`), memes billes que les lignes PAIRES (cf.
    `pair_check_compact_line`). Vide si `pairs` l'est, ou si aucune de ses
    billes n'est exploitable (ex. devise absente des paires-support fetchees
    -- voir `main`, qui inclut les devises de `best_pair_names` dans le
    fetch). `label` permet une section dediee par devise (ex. "🏆 BEST PAIRE
    JPY", cf. `--focus-currency`) distincte de la section generale."""
    lines = [
        line for line in (
            pair_check_compact_line(pair, rows_by_pair, price_trends, index_by_currency)
            for pair in pairs
        )
        if line is not None
    ]
    if not lines:
        return []
    return [label, *lines]


def build_message(pairs: list[str], rows_by_pair: dict[str, dict],
                  price_trends: dict[str, dict], index_by_currency: dict[str, dict],
                  now: datetime, imp_by_currency: dict[str, dict] | None = None,
                  focus_currencies: list[str] | None = None,
                  trend_lines: list[str] | None = None) -> tuple[str, bool]:
    """Assemble le message PAIRE_CHECK. Renvoie (message, has_content): le 2e
    indique si au moins une paire de `pairs` (billes INDEX/06h/RUN, encore
    utilisees en interne pour les sections BEST PAIRE meme si `pairs`
    lui-meme n'a plus sa propre section, cf. plus bas) ou TENDANCE est
    exploitable, pour conditionner l'envoi Telegram cote `main` (la section
    INDEX CHG%D seule ne suffit pas a envoyer -- c'est un rappel, pas le
    coeur du message).

    `focus_currencies` (ex. `["JPY"]`) ajoute une section `🏆 BEST PAIRE
    {devise}` par devise, apres la section BEST PAIRE generale -- cf.
    `best_pair_names(..., only_currency=devise)`.

    `trend_lines` (cf. `index_trend_lines`) ajoute la section `📈 TENDANCE`
    en fin de message, avant l'horodatage -- disponible meme quand les
    billes INDEX/06h/RUN de `pairs` sont encore vides (avant 06h Paris),
    puisqu'elle ne depend que de INDEX.

    2026-09-04: `pairs` n'a plus sa propre section `PAIRES` dans le message
    (uniquement TENDANCE desormais) -- `rows_by_pair`/`price_trends` restent
    necessaires pour les billes des sections BEST PAIRE ci-dessous, mais
    `pairs` lui-meme n'est plus lu ici (conserve dans la signature au cas ou
    une section dediee reviendrait -- cf. historique git)."""
    lines = ["📐 PAIRE_CHECK", ""]
    index_rows = list(index_by_currency.values())
    index_lines = index_chg_lines(index_rows, imp_by_currency)
    if index_lines:
        lines.extend(index_lines)
        lines.append("")
    best_lines = best_pair_lines(
        best_pair_names(index_rows, imp_by_currency or {}), rows_by_pair, price_trends, index_by_currency,
    )
    if best_lines:
        lines.extend(best_lines)
        lines.append("")
    has_focus_content = False
    for currency in (focus_currencies or []):
        focus_lines = best_pair_lines(
            best_pair_names(index_rows, imp_by_currency or {}, only_currency=currency),
            rows_by_pair, price_trends, index_by_currency, label=f"🏆 BEST PAIRE {currency}",
        )
        if focus_lines:
            lines.extend(focus_lines)
            lines.append("")
            has_focus_content = True
    if trend_lines:
        lines.extend(trend_lines)
        lines.append("")
    lines.append(f"⏰ {now:%Y-%m-%d %H:%M} Paris")
    has_content = bool(best_lines) or has_focus_content or bool(trend_lines)
    return "\n".join(lines), has_content


def telegram_send_decision(now: datetime, has_content: bool) -> tuple[bool, str | None]:
    """(faut_il_envoyer, raison_si_non): retient l'envoi Telegram avant
    `TELEGRAM_SEND_START_HOUR_PARIS` -- le message se construit et s'affiche
    normalement des le 1er run de la nuit (cf. `main`), mais rien ne part
    tant que 5h Paris n'est pas atteint, le temps que PAIRES/TENDANCE aient
    assez de matiere pour etre utiles. Passe ensuite le relai a
    `has_content` (cf. `build_message`), comme avant l'introduction de cette
    heure de depart."""
    if now.astimezone(PARIS_TZ).hour < TELEGRAM_SEND_START_HOUR_PARIS:
        return False, f"Avant {TELEGRAM_SEND_START_HOUR_PARIS}h Paris: message Telegram retenu."
    if not has_content:
        return False, "Aucune paire exploitable: message Telegram ignoré."
    return True, None


def main() -> int:
    args = parse_args()
    now = datetime.now(PARIS_TZ)

    # INDEX d'abord: les noms des BEST PAIRE (devises dynamiques, cf.
    # `best_pair_names`) doivent etre connus avant de choisir les
    # paires-support a fetcher, pour que leurs billes 06h/RUN soient
    # exploitables comme celles de PAIRES -- pas seulement leur segment INDEX.
    snapshot_path = Path(args.currency_snapshot) if args.currency_snapshot else None
    index_by_currency, imp_by_currency = fetch_or_load_currency_data(
        snapshot_path, args.currency_snapshot_max_age,
        args.length, args.candles, args.max_streak,
        args.imp_h1_candles, args.imp_d1_candles, args.imp_renko_bricks, args.length, args.max_streak,
    )
    index_rows = list(index_by_currency.values())
    best_pairs = best_pair_names(index_rows, imp_by_currency)
    focus_best_pairs = [
        pair
        for currency in args.focus_currency
        for pair in best_pair_names(index_rows, imp_by_currency, only_currency=currency)
    ]

    currencies = needed_currencies(args.pairs)
    for pair in [*best_pairs, *focus_best_pairs]:
        pair_currencies = _pair_currencies(pair)
        if pair_currencies:
            currencies |= set(pair_currencies)
    helper_pairs = needed_helper_pairs(currencies)
    rows = [row for row in (fetch_pair_row(pair) for pair in helper_pairs) if row is not None]
    rows_by_pair = {str(row["pair"]): row for row in rows}

    state = load_price_trend_state(args.state_file)
    new_state, price_trends = update_price_trends(state, rows, now)
    save_price_trend_state(args.state_file, new_state)

    trend_state = load_price_trend_state(args.index_trend_state_file)
    new_trend_state = update_index_trend_state(trend_state, args.pairs, index_by_currency, now)
    save_price_trend_state(args.index_trend_state_file, new_trend_state)
    trend_lines = index_trend_lines(args.pairs, new_trend_state)

    message, has_content = build_message(
        args.pairs, rows_by_pair, price_trends, index_by_currency, now, imp_by_currency, args.focus_currency,
        trend_lines,
    )
    print(message)
    if args.telegram:
        send_ok, held_reason = telegram_send_decision(now, has_content)
        if send_ok:
            send_telegram_message(message)
        else:
            print(held_reason)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
