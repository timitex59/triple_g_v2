#!/usr/bin/env python3
"""Scan imp_trend5's eight status cells for the existing 29 OANDA instruments.

Run: python imp_trend5_29pairs.py [--pairs EURUSD AUDJPY]
Dependencies: pandas, requests, websocket-client, tzdata (same as the old scanner).
Uses the existing unofficial TradingView transport and native ATR Renko feed.
This is a snapshot, not a continuous tick stream. Developing D1/Renko values
can change; independent network requests are not an atomic market snapshot.
Only the table/selection calculations are ported, not IMP trade simulation.
"""
from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

import imp_trend_29pairs as base

load_dotenv(Path(__file__).resolve().parent / '.env')


def pine_sar(df, start=0.1, increment=0.1, maximum=0.2):
    """Pine SAR recurrence, including first-bar update and reversal clamps.

    Reference: https://www.tradingview.com/pine-script-reference/v6/#fun_ta.sar
    """
    highs, lows, closes = (df[c].astype(float).tolist() for c in ('high', 'low', 'close'))
    values = [math.nan] * len(df)
    if len(df) < 2:
        return values
    below = closes[1] > closes[0]
    extreme = highs[1] if below else lows[1]
    sar = lows[0] if below else highs[0]
    acceleration = start
    for i in range(1, len(df)):
        first = i == 1
        sar += acceleration * (extreme - sar)
        if below and sar > lows[i]:
            below, first = False, True
            sar, extreme, acceleration = max(highs[i], extreme), lows[i], start
        elif not below and sar < highs[i]:
            below, first = True, True
            sar, extreme, acceleration = min(lows[i], extreme), highs[i], start
        if not first:
            if below and highs[i] > extreme:
                extreme, acceleration = highs[i], min(acceleration + increment, maximum)
            elif not below and lows[i] < extreme:
                extreme, acceleration = lows[i], min(acceleration + increment, maximum)
        if below:
            sar = min(sar, lows[i-1])
            if i > 1:
                sar = min(sar, lows[i-2])
        else:
            sar = max(sar, highs[i-1])
            if i > 1:
                sar = max(sar, highs[i-2])
        values[i] = sar
    return values


def daily_levels(df, start=0.1, increment=0.1, maximum=0.2):
    sar = pine_sar(df, start, increment, maximum)
    closes = df['close'].astype(float).tolist()
    bull = bear = None
    for i in range(1, len(df)):
        if closes[i-1] <= sar[i-1] and closes[i] > sar[i]:
            bull = sar[i]
        if closes[i-1] >= sar[i-1] and closes[i] < sar[i]:
            bear = sar[i]
    if bull is None or bear is None:
        raise ValueError('Historique D1 insuffisant : les deux croisements PSAR sont requis')
    return sar[-1], bull, bear


def score_snapshot(price, sar, bull, bear, biases):
    """Eight semantic colors: green=1, red=-1, neutral=0."""
    if not all(math.isfinite(x) for x in (price, sar, bull, bear)):
        raise ValueError('Prix ou niveaux non valides')
    low, high = sorted((bull, bear))
    sar_vote = 1 if price > sar else -1 if price < sar else 0
    level_vote = 1 if price > high else -1 if price < low else 0
    mw_bull = biases['M'] == biases['W'] == 1
    mw_bear = biases['M'] == biases['W'] == -1
    votes = [biases['M'], biases['W'], biases['D'], sar_vote,
             1 if mw_bull else -1, sar_vote, -1 if mw_bear else 1, level_vote]
    green, red = votes.count(1), votes.count(-1)
    total = green + red
    direction = 'BEAR' if red > green else 'BULL'
    percent = 100 * max(green, red) / total
    between = low < price < high
    groups = []
    if percent > 60:
        if direction == 'BULL' and price > high:
            groups.append('BULL >60% AU-DESSUS D1')
        elif direction == 'BEAR' and price < low:
            groups.append('BEAR >60% EN DESSOUS D1')
        elif between:
            groups.append('RETRACE >60% ENTRE NIVEAUX D1')
    return dict(direction=direction, percent=percent, green=green, red=red,
                between=between, groups=groups, votes=votes,
                position='ENTRE' if between else 'AU-DESSUS' if price > high
                else 'EN DESSOUS' if price < low else 'SUR UN NIVEAU')


def scan_pair(pair, args):
    d1 = base.fetch_ohlc(pair, 'D', args.d1_candles)
    if len(d1) < 3:
        raise ValueError('Historique D1 insuffisant')
    sar, bull, bear = daily_levels(d1, args.sar_start, args.sar_increment, args.sar_maximum)
    renko = {tf: base.fetch_renko(pair, tf, args.renko_bricks, args.atr_length, 50)[-1]
             for tf in ('M', 'W', 'D')}
    h1 = base.fetch_ohlc(pair, '60', args.h1_candles)
    if len(h1) < 2:
        raise ValueError('Historique H1 insuffisant')
    price = float(h1['close'].iloc[-1])
    # H1 SAR side (price vs SAR) drives the watchlist reveal/delist gate, not just the price.
    h1_sar_last = pine_sar(h1, args.sar_start, args.sar_increment, args.sar_maximum)[-1]
    h1_side = ('above' if price > h1_sar_last else 'below' if price < h1_sar_last else None) \
        if math.isfinite(h1_sar_last) else None
    # Pine's default H1 request uses the developing close despite its input label.
    reference = float(d1['close'].iloc[-2]) if args.price_mode == 'previous-daily' else price
    biases = {tf: base.effective_bias(base.px_state(point, reference), point)
              for tf, point in renko.items()}
    return dict(pair=pair, price=price, daily_sar=sar, daily_bull=bull, daily_bear=bear,
                reference_price=reference, biases=biases,
                h1_bar_time=h1['time'].iloc[-1].isoformat(),
                h1_sar=h1_sar_last, h1_side=h1_side,
                fetched_at=datetime.now(timezone.utc).isoformat(),
                **score_snapshot(price, sar, bull, bear, biases))


def currency_strength(selected, neutral_threshold=0):
    """Net directional votes within a set of rows; each pair has equal weight.

    This is a relative score over whatever rows are passed in, not an
    independent currency index. XAU is retained and explicitly identified
    as gold in the display. A currency is NEUTRE when |score| <= neutral_threshold.
    """
    counts = {}
    for row in selected:
        sign = 1 if row['direction'] == 'BULL' else -1
        for currency, vote in ((row['pair'][:3], sign), (row['pair'][3:], -sign)):
            item = counts.setdefault(currency, dict(currency=currency, positive=0, negative=0))
            item['positive' if vote > 0 else 'negative'] += 1
    result = []
    for item in counts.values():
        score = item['positive'] - item['negative']
        status = 'NEUTRE' if abs(score) <= neutral_threshold else 'FORTE' if score > 0 else 'FAIBLE'
        result.append(item | dict(score=score, pairs=item['positive'] + item['negative'], status=status))
    return sorted(result, key=lambda item: (-item['score'], item['currency']))


def print_currency_strength(strength):
    for status, label in (('FORTE', 'Fortes'), ('FAIBLE', 'Faibles'), ('NEUTRE', 'Neutres')):
        rows = sorted((r for r in strength if r['status'] == status),
                      key=lambda r: (-abs(r['score']), r['currency']))
        values = ', '.join(
            f"{r['currency'] if r['currency'] != 'XAU' else 'XAU (or)'} ({r['score']:+d})"
            for r in rows)
        print(f'  {label} : {values or "aucune"}')


def validate_retrace(retrace_rows, d1_strength):
    """Confirm/reject RETRACE pairs against currency strength derived only
    from the BULL D1 and BEAR D1 sections (see currency_strength). A pair
    touching a currency rated NEUTRE on D1 cannot be judged either way.
    """
    lookup = {item['currency']: item for item in d1_strength}
    report = []
    for row in retrace_rows:
        base, quote = row['pair'][:3], row['pair'][3:]
        base_item, quote_item = lookup.get(base), lookup.get(quote)
        if base_item is None or quote_item is None:
            missing = base if base_item is None else quote
            verdict, reason = 'N/A', f'{missing} absente du D1 BULL/BEAR'
        else:
            neutrals = [c for c, item in ((base, base_item), (quote, quote_item)) if item['status'] == 'NEUTRE']
            if neutrals:
                verdict, reason = 'N/A', f"devise neutre : {', '.join(neutrals)}"
            elif base_item['score'] == quote_item['score']:
                verdict, reason = 'NON CONFIRME', f"{base} et {quote} a egalite ({base_item['score']:+d})"
            else:
                implied = 'BULL' if base_item['score'] > quote_item['score'] else 'BEAR'
                verdict = 'VALIDE' if implied == row['direction'] else 'INVALIDE'
                reason = f"{base} ({base_item['score']:+d}) vs {quote} ({quote_item['score']:+d})"
        report.append(dict(pair=row['pair'], direction=row['direction'], percent=row['percent'],
                            verdict=verdict, reason=reason))
    order = {'VALIDE': 0, 'INVALIDE': 1, 'NON CONFIRME': 2, 'N/A': 3}
    return sorted(report, key=lambda r: (order[r['verdict']], r['pair']))


def print_retrace_validation(report):
    if not report:
        print('  Aucune paire RETRACE a valider.')
        return
    for row in report:
        pct = f"{row['percent']:.1f}".rstrip('0').rstrip('.')
        print(f"  {row['pair']:<7} {row['direction']} ({pct}%)  -> {row['verdict']:<13} [{row['reason']}]")


def summarize(d1_strength, retrace_validation, strong_threshold=2):
    """1) devises dont |score D1| >= strong_threshold ; 2) paires RETRACE VALIDE."""
    strong = sorted((item for item in d1_strength if abs(item['score']) >= strong_threshold),
                     key=lambda r: (-abs(r['score']), r['currency']))
    confirmed = [row for row in retrace_validation if row['verdict'] == 'VALIDE']
    return dict(strong_threshold=strong_threshold, strong_currencies=strong, retrace_confirmed=confirmed)


def print_summary(summary):
    print(f"  Devises avec |score D1| >= {summary['strong_threshold']:g} :")
    if summary['strong_currencies']:
        for item in summary['strong_currencies']:
            label = item['currency'] if item['currency'] != 'XAU' else 'XAU (or)'
            print(f"    {label} ({item['score']:+d})")
    else:
        print('    aucune')
    print('  Paires RETRACE confirmees :')
    if summary['retrace_confirmed']:
        for row in summary['retrace_confirmed']:
            pct = f"{row['percent']:.1f}".rstrip('0').rstrip('.')
            print(f"    {row['pair']:<7} {row['direction']} ({pct}%)")
    else:
        print('    aucune')


def pair_touches_currency(pair, currencies):
    """True si `pair` implique une des devises de `currencies` (base ou quote),
    ou si `currencies` est vide (pas de restriction). Sert a concentrer la
    watchlist RETRACE/OPPORTUNITY sur un sous-ensemble de devises (ex. JPY)
    sans changer le calcul de force par devise, qui reste base sur les 29
    paires (cf. `--focus-currencies` dans `main`)."""
    if not currencies:
        return True
    return pair[:3] in currencies or pair[3:] in currencies


def find_canonical_pair(currency_a, currency_b, all_pairs):
    """The traded symbol (e.g. NZDJPY, not JPYNZD) for two currency codes, or None."""
    wanted = {currency_a, currency_b}
    return next((p for p in all_pairs if {p[:3], p[3:]} == wanted), None)


def build_opportunities(d1_strength, all_pairs, strong_threshold=2):
    """Legitimate associations among devises with |score D1| >= strong_threshold.

    Two strong currencies only form an opportunity when their D1 scores have
    OPPOSITE signs (one bullish, one bearish) -- two currencies strong/weak in
    the same direction (e.g. NZD -5 and CHF -4, both weak) cancel out and are
    not a legitimate pair. Direction follows the stronger leg: base > quote in
    score => BULL, else BEAR. Only pairs that exist among all_pairs are kept.
    """
    strong = [item for item in d1_strength if abs(item['score']) >= strong_threshold]
    result = []
    for a, b in itertools.combinations(strong, 2):
        if (a['score'] > 0) == (b['score'] > 0):
            continue
        pair = find_canonical_pair(a['currency'], b['currency'], all_pairs)
        if pair is None:
            continue
        base_item, quote_item = (a, b) if a['currency'] == pair[:3] else (b, a)
        direction = 'BULL' if base_item['score'] > quote_item['score'] else 'BEAR'
        result.append(dict(pair=pair, direction=direction,
                            base=base_item['currency'], base_score=base_item['score'],
                            quote=quote_item['currency'], quote_score=quote_item['score']))
    return sorted(result, key=lambda r: (-abs(r['base_score'] - r['quote_score']), r['pair']))


def print_opportunities(opportunity_list):
    if not opportunity_list:
        print('  Aucune association legitime (devises fortes de meme signe, ou paire inexistante).')
        return
    for o in opportunity_list:
        ecart = o['base_score'] - o['quote_score']
        print(f"  {o['pair']:<7} {o['direction']} [{o['base']} ({o['base_score']:+d}) vs "
              f"{o['quote']} ({o['quote_score']:+d}), ecart {ecart:+d}]")


def load_watchlist(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (json.JSONDecodeError, OSError):
        return {}


def update_watchlist(watchlist, candidates, h1_sides, now_iso):
    """Accumulate RETRACE/OPPORTUNITY candidates and gate their reveal on the H1 SAR.

    A pair enters the watchlist the first time it appears in RETRACE/OPPORTUNITY,
    but only becomes 'revealed' (shown on Telegram) once price sits on the H1 SAR
    side matching its tracked direction (BULL=above, BEAR=below) -- either right
    away if already aligned, or on a later run once the H1 SAR flips that way.
    A revealed pair is delisted (removed) as soon as price flips to the other
    side of the H1 SAR. An unrevealed pair is dropped once it stops appearing
    in RETRACE/OPPORTUNITY, since there is no more signal to wait to confirm.
    """
    for pair, cand in candidates.items():
        if pair not in watchlist:
            watchlist[pair] = dict(pair=pair, direction=cand['direction'], source=cand['source'],
                                    first_seen_utc=now_iso, revealed=False, revealed_at_utc=None,
                                    h1_side=h1_sides.get(pair))
        elif not watchlist[pair]['revealed']:
            watchlist[pair]['direction'] = cand['direction']
            watchlist[pair]['source'] = cand['source']

    for pair in list(watchlist):
        entry = watchlist[pair]
        side = h1_sides.get(pair)
        if side is None:
            continue
        aligned = (side == 'above' and entry['direction'] == 'BULL') or \
                  (side == 'below' and entry['direction'] == 'BEAR')
        if entry['revealed']:
            if not aligned:
                del watchlist[pair]
                continue
        elif aligned:
            entry['revealed'] = True
            entry['revealed_at_utc'] = now_iso
        entry['h1_side'] = side

    for pair in list(watchlist):
        if not watchlist[pair]['revealed'] and pair not in candidates:
            del watchlist[pair]

    return watchlist


def print_watchlist(watchlist):
    if not watchlist:
        print('  Liste vide.')
        return
    for pair, entry in sorted(watchlist.items()):
        if entry['revealed']:
            state = f"REVELE le {entry['revealed_at_utc']}"
        else:
            state = f"EN ATTENTE (cote H1/SAR actuelle : {entry['h1_side'] or 'inconnue'})"
        print(f"  {pair:<7} {entry['direction']} [{entry['source']}] -> {state}")


def format_forces_lines(summary):
    lines = []
    for item in summary['strong_currencies']:
        label = item['currency'] if item['currency'] != 'XAU' else 'XAU (or)'
        circle = '🟢' if item['score'] > 0 else '🔴'
        lines.append(f"{circle} {label} ({item['score']:+.2f})")
    return lines or ['aucune']


def format_pair_lines(rows):
    lines = [f"{'🟢' if row['direction'] == 'BULL' else '🔴'} {row['pair']}" for row in rows]
    return lines or ['aucune']


def build_telegram_message(summary, revealed_retrace, revealed_opportunity):
    now_paris = datetime.now(base.PARIS).strftime('%Y-%m-%d %H:%M')
    lines = ['📐 TREND', '', '📈 FORCES']
    lines += format_forces_lines(summary)
    lines += ['', 'RETRACE :']
    lines += format_pair_lines(revealed_retrace)
    lines += ['', 'OPPORTUNITY :']
    lines += format_pair_lines(revealed_opportunity)
    lines += ['', f'⏰ {now_paris} Paris']
    return '\n'.join(lines)


def send_telegram_message(text):
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    if not token or not chat_id:
        print('  Telegram non configure (.env manquant ou incomplet) - message non envoye.')
        return False
    try:
        response = requests.post(f'https://api.telegram.org/bot{token}/sendMessage',
                                  data={'chat_id': chat_id, 'text': text}, timeout=15)
        response.raise_for_status()
        return True
    except Exception as exc:
        print(f'  Envoi Telegram echoue : {exc}')
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pairs', nargs='+', default=base.PAIRS_29, choices=base.PAIRS_29)
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--stagger', type=float, default=0.3,
                         help='Delai (s) entre deux soumissions au pool, pour eviter une rafale de connexions et des 429')
    parser.add_argument('--d1-candles', type=int, default=2500)
    parser.add_argument('--h1-candles', type=int, default=300,
                         help='Bougies H1 recuperees pour calculer le SAR horaire (watchlist)')
    parser.add_argument('--renko-bricks', type=int, default=2500)
    parser.add_argument('--atr-length', type=int, default=14)
    parser.add_argument('--sar-start', type=float, default=0.1)
    parser.add_argument('--sar-increment', type=float, default=0.1)
    parser.add_argument('--sar-maximum', type=float, default=0.2)
    parser.add_argument('--price-mode', choices=['h1', 'previous-daily'], default='h1')
    parser.add_argument('--neutral-threshold', type=float, default=0.0,
                         help='|score| en dessous ou egal duquel une devise est NEUTRE sur le D1 BULL+BEAR combine')
    parser.add_argument('--strong-threshold', type=float, default=2.0,
                         help='|score| D1 minimum pour figurer dans le resume des devises fortes/faibles')
    parser.add_argument('--json', type=Path, default=Path('imp_trend5_29pairs.json'))
    parser.add_argument('--watchlist-json', type=Path, default=Path('imp_trend5_watchlist_state.json'),
                         help='Etat persistant de la watchlist RETRACE/OPPORTUNITY (reveal au cross SAR H1)')
    parser.add_argument('--focus-currencies', nargs='*', default=['JPY'],
                         help="Devises sur lesquelles restreindre la watchlist RETRACE/OPPORTUNITY (defaut : JPY). "
                              "Le calcul de force par devise reste base sur les 29 paires ; "
                              "--focus-currencies sans argument desactive la restriction (garde tout).")
    parser.add_argument('--telegram', action='store_true',
                         help='Envoyer le resume RESUME+OPPORTUNITY sur Telegram a la fin du scan')
    args = parser.parse_args()
    if min(args.workers, args.atr_length, args.sar_start, args.sar_increment, args.sar_maximum) <= 0 or args.d1_candles < 3 or args.h1_candles < 2 or args.renko_bricks < 50 or args.stagger < 0:
        parser.error('Parametres positifs requis, au moins 3 bougies D1, 2 bougies H1, 50 briques Renko et stagger >= 0')
    results, errors = [], []
    pairs = list(dict.fromkeys(args.pairs))
    print(f'Scan de {len(pairs)} instruments OANDA...', flush=True)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {}
        for i, pair in enumerate(pairs):
            if i:
                time.sleep(args.stagger)
            futures[pool.submit(scan_pair, pair, args)] = pair
        for future in as_completed(futures):
            pair = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f'{pair}: OK', flush=True)
            except Exception as exc:
                errors.append(dict(pair=pair, error=str(exc)))
                print(f'{pair}: ERREUR - {exc}', flush=True)
    results.sort(key=lambda r: (-r['percent'], r['pair']))
    strength_by_section = {}
    selected_by_group = {}
    print('\nForce par section : solde des votes +1/-1, une voix par paire ; zero = neutre.')
    for group in ('BULL >60% AU-DESSUS D1', 'BEAR >60% EN DESSOUS D1', 'RETRACE >60% ENTRE NIVEAUX D1'):
        print('\n' + group)
        selected = [r for r in results if group in r['groups']]
        selected_by_group[group] = selected
        for r in selected:
            pct = f"{r['percent']:.1f}".rstrip('0').rstrip('.')
            print(f"  {r['pair']:<7} {r['direction']} ({pct}%)")
        if not selected:
            print('  Aucune paire parmi les instruments analyses.')
        strength_by_section[group] = currency_strength(selected)
        if selected:
            print_currency_strength(strength_by_section[group])

    # Force "de reference" : BULL D1 + BEAR D1 uniquement (signaux non ambigus),
    # utilisee pour valider/invalider les paires RETRACE. Une devise NEUTRE
    # (|score| <= neutral-threshold) interdit toute validation de la paire.
    d1_rows = selected_by_group['BULL >60% AU-DESSUS D1'] + selected_by_group['BEAR >60% EN DESSOUS D1']
    d1_strength = currency_strength(d1_rows, args.neutral_threshold)
    print(f"\nForce des devises (D1 seul, BULL+BEAR combines, seuil neutre = {args.neutral_threshold:g}) :")
    print_currency_strength(d1_strength)
    retrace_validation = validate_retrace(selected_by_group['RETRACE >60% ENTRE NIVEAUX D1'], d1_strength)
    print('\nValidation RETRACE via la force D1 (BULL+BEAR uniquement) :')
    print_retrace_validation(retrace_validation)

    summary = summarize(d1_strength, retrace_validation, args.strong_threshold)
    print('\nRESUME')
    print_summary(summary)

    opportunity_list = build_opportunities(d1_strength, base.PAIRS_29, args.strong_threshold)
    print('\nOPPORTUNITY')
    print_opportunities(opportunity_list)

    now_iso = datetime.now(timezone.utc).isoformat()
    h1_sides = {r['pair']: r.get('h1_side') for r in results}
    candidates = {}
    for row in summary['retrace_confirmed']:
        candidates[row['pair']] = dict(direction=row['direction'], source='RETRACE')
    for o in opportunity_list:
        candidates[o['pair']] = dict(direction=o['direction'], source='OPPORTUNITY')
    candidates = {pair: cand for pair, cand in candidates.items()
                  if pair_touches_currency(pair, args.focus_currencies)}
    watchlist = load_watchlist(args.watchlist_json)
    watchlist = update_watchlist(watchlist, candidates, h1_sides, now_iso)
    args.watchlist_json.parent.mkdir(parents=True, exist_ok=True)
    args.watchlist_json.write_text(json.dumps(watchlist, indent=2, allow_nan=False, sort_keys=True),
                                    encoding='utf-8')
    print('\nWATCHLIST (accumulation RETRACE/OPPORTUNITY, revele au cross SAR H1) :')
    print_watchlist(watchlist)
    revealed_retrace = sorted((dict(pair=p, direction=e['direction']) for p, e in watchlist.items()
                                if e['revealed'] and e['source'] == 'RETRACE'), key=lambda r: r['pair'])
    revealed_opportunity = sorted((dict(pair=p, direction=e['direction']) for p, e in watchlist.items()
                                    if e['revealed'] and e['source'] == 'OPPORTUNITY'), key=lambda r: r['pair'])

    telegram_sent = False
    if args.telegram:
        print('\nTelegram :')
        telegram_sent = send_telegram_message(
            build_telegram_message(summary, revealed_retrace, revealed_opportunity))
        if telegram_sent:
            print('  Message envoye.')

    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(dict(time_utc=datetime.now(timezone.utc).isoformat(),
        requested_pairs=pairs, settings=vars(args) | {'json': str(args.json), 'watchlist_json': str(args.watchlist_json)},
        results=results, errors=errors, currency_strength_by_section=strength_by_section,
        d1_currency_strength=d1_strength, retrace_validation=retrace_validation, summary=summary,
        opportunities=opportunity_list, watchlist=watchlist, telegram_sent=telegram_sent),
        indent=2, allow_nan=False), encoding='utf-8')
    print(f'\n{len(results)}/{len(pairs)} analyses. Rapport : {args.json.resolve()}')
    return 1 if errors else 0


if __name__ == '__main__':
    raise SystemExit(main())
