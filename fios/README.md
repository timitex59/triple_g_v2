# FIOS — Forex Intelligence Operating System (Phase 1)

Moteur de **confluence multi-sources** pour le Forex. Aucune décision sur un
seul signal : une reco `BUY`/`SELL` n'apparaît que lorsque plusieurs familles
indépendantes convergent. Sinon `WAIT`. Le moteur explique toujours pourquoi.

Données graphiques : **TradingView WebSocket** (symboles OANDA), comme le reste
du repo (`data_fetcher.py`).

## Ce qui tourne en Phase 1

| Famille | Source | Statut | Clé requise |
|---|---|---|---|
| **Technique** | TradingView (D / H4 / H1) | actif | — |
| **Sentiment / positionnement** | COT CFTC (Legacy futures) | actif | — |
| **Corrélations / flux** | TradingView (DXY, or, US10Y, SPX, VIX, pétrole, BTC) | actif | — |
| **Fondamental** | FRED (taux, rendements, inflation, chômage) | branché | `FRED_API_KEY` |

Chaque famille produit un score 0-100 par devise (50 = neutre). Le moteur de
confluence pondère les familles **présentes** (poids renormalisés) et n'émet un
signal que si le net dépasse le seuil **et** qu'au moins 2 familles convergent.

## Lancement

```bash
python -m fios.run                 # calcul + envoi Telegram
python -m fios.run --no-telegram   # console seulement
python -m fios.run --verbose       # détail par adaptateur
python -m fios.run --limit 6       # limite le nb de paires (tests rapides)
python -m fios.run --no-cot --no-corr --no-fund   # technique seule
```

Dépendances : déjà dans `requirements.txt` du repo (`requests`, `pandas`,
`pytz`, `python-dotenv`, `websocket-client`). Aucune nouvelle dépendance.

## Activer le fondamental (FRED, gratuit)

1. Créer une clé gratuite : https://fred.stlouisfed.org/docs/api/api_key.html
2. Ajouter dans `.env` : `FRED_API_KEY=xxxxxxxx`
3. Relancer : la famille « Fond » passe active et entre dans la confluence.

Sans clé, FIOS ignore proprement le fondamental et redistribue son poids.

## Sorties

- **`fios_report.json`** — snapshot du run (classement devises, signaux,
  contributions par famille). Fichier réécrit à chaque run → destiné à être lu
  par d'autres scripts.
- **`fios_journal.json`** — journal cumulatif des recos actionnables
  (horodatage, décision, scores, contributions, `result: null`). Matière
  première du backtest (Phase 2) et de l'apprentissage adaptatif (Phase 3).

## Architecture

```
fios/
  config.py            réglages : devises, paires, symboles TV, poids, seuils, mappings COT/FRED
  tv_feed.py           moteur TradingView WebSocket (multi-TF), cache mémoire
  indicators.py        EMA / RSI / ADX / ATR (pandas pur)
  adapters/
    base.py            interface FamilyResult (score 0-100 par devise)
    technical.py       score multi-TF par paire → force par devise (TradingView)
    cot.py             positionnement COT CFTC → sentiment par devise
    correlations.py    flux inter-marchés (momentum macro → vent par devise)
    fundamental_fred.py macro FRED (branché sur FRED_API_KEY)
  scoring/
    currency.py        composite par devise (Currency Power Ranking)
    confluence.py      moteur BUY/SELL/WAIT + confiance + qualité
  explain.py           raisons lisibles (gabarit ; LLM en Phase 1.5)
  journal.py           mémoire statistique (fios_journal.json)
  report.py            message Telegram + rapport console
  run.py               orchestrateur CLI
```

Chaque adaptateur est isolé derrière `FamilyResult` : on branche une nouvelle
source (Myfxbook, IG, OANDA, options) sans toucher au moteur.

## Réglages clés (`config.py`)

- `PAIRS` — univers de paires (base/quote/symbole TV).
- `TF_WEIGHTS` — poids des timeframes techniques (D 0.5 / H4 0.3 / H1 0.2).
- `CONFLUENCE_WEIGHTS` — poids des familles (Fond 0.30 / Tech 0.35 / Sent 0.20 / Corr 0.15).
- `BUY_THRESHOLD` / `SELL_THRESHOLD` — seuils du net directionnel (±25).
- `MIN_FAMILIES_AGREE` — familles convergentes minimales (2).

## Feuille de route

- **Phase 1 (fait)** : squelette + technique TradingView + COT + corrélations
  + fondamental FRED + confluence + explication + journal + Telegram.
- **Phase 1.5** : explication LLM (ton hawkish/dovish des discours BC), OANDA
  position book (sentiment retail contrarien), planification CI 1×/jour.
- **Phase 2** : dénouement des signaux dans le journal + backtest (win rate,
  RR, drawdown par combinaison de familles et régime de marché).
- **Phase 3** : pondérations adaptatives (Bayésien puis gradient boosting) —
  entraînées **sur le journal**, jamais en boîte noire.
- **Plugins optionnels** : Myfxbook, IG, CME (volume/options/risk reversals) —
  branchés quand l'accès aux données (souvent payant) est réglé.

> Aucun système ne supprime le risque. FIOS hiérarchise et pondère des signaux
> indépendants pour réduire les faux signaux, pas pour garantir un résultat.
