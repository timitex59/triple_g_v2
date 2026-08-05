# FIOS — Confluence force devises × alignement RENKO M/W/D

Message Telegram unique qui croise **deux vues devises indépendantes** :

1. **Force composite FIOS** par devise (0-100) = moyenne pondérée de :
   - Fondamental (FRED : taux, rendements, inflation, chômage)
   - Technique (TradingView D/H4/H1, EMA/RSI/ADX)
   - Sentiment institutionnel (COT CFTC)
   - Sentiment retail contrarien (OANDA + Myfxbook)
   - Corrélations inter-marchés (DXY, or, rendements US, actions, pétrole, BTC)
2. **Alignement RENKO M/W/D** des index de devises (DXY/EXY/BXY/…), produit par
   `renko_full_alignment_29pairs.py` dans un sidecar `full_alignment_index.json`.

Quand les deux méthodologies pointent la même devise dans le même sens →
confluence haute conviction. Quand elles divergent → prudence.

## Sortie

```
🔀 Confluence FULL ALIGN × FIOS
🟢 AUD  FIOS 67 · Align M+ W+ D+ (+0.14%)
🟢 NZD  FIOS 60 · Align M0 W+ D+ (-0.12%)
🔴 CHF  FIOS 40 · Align M0 W- D0 (+0.19%)
⚠️ Divergences : USD, CAD
→ Paire confluente : ACHAT AUDCHF
⏰ 05/08/2026 07:15 Paris
```

## Lancement

```bash
python -m fios.run                 # calcul + envoi Telegram
python -m fios.run --no-telegram   # affichage seul
python -m fios.run --verbose       # détail par famille
python -m fios.run --no-fund --no-retail   # sous-ensemble de familles
```

Dépendances : `yfinance`, `pandas`, `numpy`, `requests`, `pytz`,
`python-dotenv`, `websocket-client` (déjà dans `requirements.txt`). **Aucune
dépendance LLM.**

## Prérequis

- `.env` : `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `FRED_API_KEY` (gratuit),
  `MYFXBOOK_EMAIL`/`MYFXBOOK_PASSWORD`, `OANDA_API_TOKEN` (optionnel). Chaque
  famille sans identifiants est simplement ignorée (poids redistribué).
- **`renko_full_alignment_29pairs.py` doit tourner AVANT FIOS** pour écrire le
  sidecar `full_alignment_index.json`. En CI, il tourne dans le même job juste
  avant FIOS, donc le sidecar est frais à 07h. Sans sidecar frais, FIOS
  n'envoie rien (proprement).

## Structure

```
fios/
  config.py            devises, paires, symboles TV, poids, mappings COT/FRED
  tv_feed.py           moteur TradingView WebSocket (multi-TF)
  indicators.py        EMA / RSI / ADX / ATR
  adapters/
    base.py            interface FamilyResult (score 0-100 par devise)
    technical.py       force technique multi-TF -> par devise
    cot.py             positionnement COT CFTC
    correlations.py    flux inter-marchés
    fundamental_fred.py macro FRED
    retail.py          sentiment retail contrarien (OANDA + Myfxbook)
  scoring/currency.py  force composite par devise (Currency Power Ranking)
  cross_check.py       lecture du sidecar RENKO + section confluence
  run.py               orchestrateur (message unique)
```
