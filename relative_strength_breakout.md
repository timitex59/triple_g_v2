# Prompt complet — Implémenter le système « 07H Relative Strength Breakout »

Tu es un ingénieur quantitatif senior spécialisé en Forex, Python, systèmes de trading systématiques, analyse multi-devises, scoring de momentum, backtesting et architecture de bots Telegram.

Ta mission est de modifier mon système Forex existant afin d’y intégrer un nouveau moteur appelé :

# 07H RELATIVE STRENGTH BREAKOUT

L’objectif n’est PAS de remplacer les moteurs existants, mais de créer une nouvelle couche d’analyse capable de mesurer, comparer, classer et sélectionner les meilleures paires Forex à partir de leur déplacement en pips depuis le début de la journée.

Le système doit ensuite croiser cette information avec :

* la direction actuelle de la paire ;
* les signaux ↑↑ / ↑↓ / ↓↑ / ↓↓ ;
* les paires convergentes ;
* le Top Momentum ;
* le VIVIER ;
* le FULL ALIGNMENT M/W/D ;
* la force des devises ;
* le nombre de cross confirmant une devise ;
* la persistance du signal sur plusieurs runs ;
* la vitesse du mouvement ;
* l’accélération ;
* le niveau d’extension déjà réalisé ;
* les alertes ⚠️ ;
* les signaux 🔥 ;
* et les éventuels consensus IA déjà existants.

Le système doit être conçu de façon propre, modulaire, testable et configurable.

---

# 1. PRINCIPE FONDAMENTAL

Chaque jour de trading possède un prix de référence correspondant au run de :

## 07:00 heure de Paris

Pour chaque paire Forex suivie, enregistrer le prix observé au premier run disponible à partir de 07:00.

Exemple :

```text
EURUSD prix référence 07h = 1.15400
USDCAD prix référence 07h = 1.39168
NZDUSD prix référence 07h = 0.58666
```

Ce prix devient :

```text
PIPS_07H = 0
```

pour la journée.

Tous les mouvements ultérieurs doivent être exprimés en pips par rapport à cette référence.

IMPORTANT :

Si aucun prix n’est disponible exactement à 07:00, utiliser le premier prix disponible après 07:00.

Ne jamais inventer ou interpoler artificiellement un prix sauf si une fonction d’interpolation est explicitement activée dans la configuration.

Stocker également :

```text
reference_time
reference_price
reference_source
```

---

# 2. CALCUL DES PIPS

Pour les paires non JPY :

```text
pip_size = 0.0001
```

Pour les paires contenant JPY comme devise de cotation :

```text
pip_size = 0.01
```

Exemple :

```text
EURJPY
CADJPY
NZDJPY
USDJPY
```

Pour un mouvement LONG :

```text
raw_pips =
(current_price - reference_price)
/
pip_size
```

Pour un mouvement SHORT :

```text
raw_pips =
(reference_price - current_price)
/
pip_size
```

IMPORTANT :

Le moteur doit pouvoir afficher les deux lectures :

```text
raw_market_pips
directional_pips
```

`raw_market_pips` représente la variation naturelle de la paire.

`directional_pips` représente la variation dans le sens du signal de trading retenu.

Ainsi, un SHORT qui fonctionne doit produire un nombre positif.

---

# 3. THRESHOLD / ZONE DE BRUIT

Créer une zone neutre configurable autour du niveau 0.

Valeur initiale :

```text
threshold_pips = 5
```

Donc :

```text
-5 <= directional_pips <= +5
```

est considéré comme :

```text
NOISE
NEUTRAL
```

Au-dessus de :

```text
+5 pips
```

le mouvement devient :

```text
EXPANSION
```

En dessous de :

```text
-5 pips
```

le mouvement devient :

```text
ADVERSE
INVALIDATION_CANDIDATE
```

Les seuils doivent être configurables.

Prévoir par exemple :

```python
THRESHOLD_PIPS = 5.0
```

et permettre ensuite des backtests avec :

```text
3
5
7.5
10
15 pips
```

---

# 4. ÉTATS DU MOUVEMENT

Chaque paire doit avoir un état calculé.

Exemple :

```text
NEUTRAL
BREAKOUT_PENDING
BREAKOUT_CONFIRMED
EXPANDING
TRENDING
EXTENDED
EXHAUSTION
REVERSAL_WARNING
INVALIDATED
```

Logique initiale proposée :

### NEUTRAL

```text
abs(directional_pips) <= threshold
```

### BREAKOUT_PENDING

```text
directional_pips > threshold
```

mais confirmation insuffisante.

### BREAKOUT_CONFIRMED

```text
directional_pips > threshold
```

pendant au moins :

```text
2 runs consécutifs
```

### EXPANDING

Breakout confirmé et :

```text
directional_pips > previous_directional_pips
```

### TRENDING

Breakout confirmé avec progression positive persistante.

### EXTENDED

Déplacement déjà important depuis 07h.

Valeur initiale :

```text
directional_pips >= 25
```

### EXHAUSTION

Le mouvement reste positif mais :

* vitesse en baisse ;
* accélération négative ;
* apparition de ↑↓ / ↓↑ ;
* apparition d’un ⚠️ ;
* ou perte de force relative.

### REVERSAL_WARNING

Le marché rend une partie significative de son MFE.

### INVALIDATED

Retour sous un niveau d’invalidation défini.

---

# 5. PERSISTANCE MULTI-RUN

Créer un compteur :

```text
confirmation_runs
```

Exemple :

```text
run 1 : +6.2
run 2 : +8.4
run 3 : +12.1
run 4 : +15.7
```

donne :

```text
confirmation_runs = 4
```

Le compteur doit représenter le nombre de runs consécutifs pendant lesquels la paire reste dans un état directionnel compatible avec le trade.

Créer aussi :

```text
above_threshold_runs
signal_direction_runs
convergence_runs
momentum_runs
warning_runs
```

Le système doit distinguer :

```text
8 runs de persistance
```

d’un simple :

```text
🔥 apparaissant sur un seul run
```

La persistance doit avoir un poids important dans le scoring final.

---

# 6. DÉTECTION BREAK + HOLD

Un simple passage au-dessus de +5 pips ne suffit pas.

Définir :

```text
BREAK
```

lorsque :

```text
directional_pips > threshold
```

Définir :

```text
HOLD
```

si le niveau reste supérieur au threshold pendant au moins :

```text
MIN_HOLD_RUNS = 2
```

Exemple favorable :

```text
0
+3
+6
+12
+17
```

=> validé.

Exemple non confirmé :

```text
0
+6
+2
+7
+1
```

=> faux breakout / instable.

Créer éventuellement :

```text
breakout_quality
```

sur 0–100.

---

# 7. VELOCITY

Calculer la vitesse du mouvement.

Formule :

```text
velocity =
(delta directional_pips)
/
(delta time in hours)
```

Exemple :

```text
run précédent = +10 pips
run actuel = +15 pips
intervalle = 30 minutes
```

Alors :

```text
velocity =
5 / 0.5
= 10 pips/hour
```

Stocker :

```text
velocity_pips_per_hour
```

Créer une version normalisée 0–100 :

```text
velocity_score
```

---

# 8. ACCELERATION

Calculer l’accélération :

```text
acceleration =
current_velocity
-
previous_velocity
```

Une paire qui passe par exemple de :

```text
+2 pips/h
à
+8 pips/h
```

doit être considérée comme accélérante.

Créer :

```text
acceleration_score
```

et un état :

```text
ACCELERATING
STEADY
DECELERATING
```

---

# 9. MFE

Calculer le Maximum Favorable Excursion.

```text
MFE =
max(directional_pips depuis la référence)
```

Exemple :

```text
0
+8
+17
+31
+42
+35
```

donne :

```text
MFE = 42
```

Stocker :

```text
mfe_pips
```

---

# 10. MAE

Calculer le Maximum Adverse Excursion.

Exemple :

```text
0
-3
-7
+5
+20
```

donne :

```text
MAE = -7
```

Stocker :

```text
mae_pips
```

Ceci permet de distinguer :

```text
+40 pips avec MAE -1
```

d’un :

```text
+40 pips avec MAE -15
```

Le premier est qualitativement supérieur.

---

# 11. EFFICIENCY RATIO

Créer un indicateur de propreté du mouvement.

Par exemple :

```text
efficiency =
net_directional_pips
/
total_absolute_pip_movement
```

entre :

```text
0 et 1
```

Une paire qui progresse presque en ligne droite aura une forte efficiency.

Une paire très erratique aura une faible efficiency.

Créer :

```text
efficiency_score = efficiency * 100
```

---

# 12. DRAWDOWN DEPUIS LE MFE

Calculer :

```text
drawdown_from_mfe =
mfe_pips - current_directional_pips
```

Puis :

```text
drawdown_pct =
drawdown_from_mfe / mfe_pips
```

Exemple :

```text
MFE = 40
current = 26
```

donne :

```text
drawdown = 14 pips
drawdown_pct = 35%
```

Ce critère doit alimenter les signaux :

```text
EXHAUSTION
REVERSAL_WARNING
EXIT
```

---

# 13. ENTRY ZONES

Créer des zones d’entrée configurables.

Valeurs initiales :

```text
0 → 5 pips
NOISE

5 → 15 pips
SWEET_SPOT

15 → 25 pips
EXTENSION

> 25 pips
CHASING
```

État :

```text
entry_zone
```

Le système doit fortement pénaliser les entrées tardives.

Exemple :

```text
NZDUSD
+32 pips depuis 07h
nouveau signal LONG
```

ne doit pas être traité comme un nouveau signal de même qualité qu’un signal apparaissant à +7 pips.

---

# 14. FORCE DES DEVISES

Le moteur doit travailler au niveau des devises, pas uniquement des paires.

Construire la force relative pour :

```text
USD
EUR
GBP
JPY
CHF
CAD
AUD
NZD
```

Exploiter les données déjà présentes du type :

```text
NZD +0.81%
GBP +0.51%
AUD +0.47%
CAD +0.46%
EUR +0.42%
```

Créer :

```text
currency_strength_score
```

normalisé 0–100.

---

# 15. CURRENCY BREADTH

Créer un indicateur extrêmement important :

```text
currency_breadth
```

Il doit mesurer combien de cross indépendants confirment la force ou la faiblesse d’une devise.

Exemple :

```text
NZDUSD LONG
NZDCHF LONG
NZDJPY LONG
EURNZD SHORT
AUDNZD SHORT
```

Tous confirment :

```text
NZD fort
```

Alors :

```text
NZD breadth = 5 confirmations
```

Calcul possible :

```text
breadth_ratio =
confirming_crosses
/
available_crosses
```

Exemple :

```text
5 / 6 = 0.833
```

soit :

```text
breadth_score = 83.3
```

Prévoir :

```text
breadth_count
breadth_ratio
breadth_score
```

---

# 16. DIFFÉRENTIEL DE FORCE

Pour chaque paire :

```text
strength_differential =
base_currency_strength
-
quote_currency_strength
```

Exemple :

```text
NZD = 85
CHF = 35
```

Alors :

```text
NZDCHF differential = +50
```

Une paire idéale LONG doit généralement opposer :

```text
devise forte
VS
devise faible
```

Une paire SHORT doit opposer :

```text
base faible
VS
quote forte
```

Créer :

```text
strength_differential_score
```

---

# 17. CROSS CONFIRMATION

Créer une validation par plusieurs paires.

Exemple USD faible :

```text
EURUSD LONG
GBPUSD LONG
AUDUSD LONG
NZDUSD LONG
USDCAD SHORT
USDJPY SHORT
USDCHF SHORT
```

Alors :

```text
USD weakness breadth élevé
```

Le système doit détecter automatiquement :

```text
STRONG_CURRENCY
WEAK_CURRENCY
```

et construire les meilleures combinaisons :

```text
strongest currency
vs
weakest currency
```

---

# 18. INTÉGRATION DES SIGNES ↑↑ ↓↓ ↑↓ ↓↑

Conserver leur logique existante.

Définir généralement :

```text
↑↑ = bullish alignment
↓↓ = bearish alignment
↑↓ = divergence / loss of alignment
↓↑ = divergence / transition
```

Ne pas modifier la définition existante du projet si elle diffère.

Le moteur doit lire ces informations comme confirmation ou pénalité.

Pour un LONG :

```text
↑↑ = forte confirmation
↑↓ = pénalité
↓↑ = pénalité
↓↓ = contradiction
```

Pour un SHORT :

```text
↓↓ = forte confirmation
↑↓ = pénalité
↓↑ = pénalité
↑↑ = contradiction
```

---

# 19. GESTION ⚠️

Un ⚠️ ne doit PAS invalider automatiquement un trade.

Créer :

```text
warning_penalty
```

Le poids du warning doit dépendre de :

* sa durée ;
* son type ;
* l’état du prix ;
* la persistance précédente ;
* le nombre de confirmations restantes.

Exemple :

```text
NZDCHF
8 runs positifs
+15 pips
↑↑
puis ⚠️
```

ne doit pas être traité comme :

```text
signal totalement invalidé
```

Le système doit préférer :

```text
TRENDING_WITH_WARNING
```

ou :

```text
MANAGE_POSITION
```

---

# 20. GESTION 🔥

🔥 signifie forte intensité / momentum.

Mais il ne doit pas dominer le scoring.

Un :

```text
🔥 sur 1 run
```

doit être inférieur qualitativement à :

```text
6 runs cohérents
sans 🔥
```

Prévoir :

```text
fire_bonus
```

limité.

Exemple :

```text
+5 points
```

maximum.

---

# 21. SCORE GLOBAL

Créer un score :

```text
RSB_SCORE
```

sur 100.

Proposition initiale :

```text
20% strength differential
15% currency breadth
15% persistence
10% pip expansion
10% velocity
5% acceleration
10% technical alignment
5% convergence
5% efficiency
5% entry quality
```

Puis appliquer des pénalités.

Exemple :

```text
warning penalty
chasing penalty
drawdown penalty
contradiction penalty
high MAE penalty
```

Le score final :

```text
score = max(0, min(100, raw_score - penalties))
```

Les poids doivent être dans un fichier de configuration.

---

# 22. EXEMPLE DE SCORE

Exemple :

```text
NZDUSD LONG

Pips07h           +12
Threshold         confirmed
Persistence       4 runs
Velocity          positive
Acceleration      positive
NZD strength      87
USD strength      30
Differential      +57
NZD breadth       5/6
↑↑                yes
Convergent        yes
🔥                yes
Warning           no
Entry zone        SWEET_SPOT
```

Résultat possible :

```text
RSB_SCORE = 92
```

---

# 23. SIGNALS DE SORTIE

Créer une logique de sortie indépendante de l’entrée.

Conditions possibles :

### EXIT HARD

```text
directional_pips < -threshold
```

ou contradiction majeure.

### EXIT TRAILING

Après un MFE suffisamment élevé :

```text
mfe >= 15 pips
```

activer :

```text
trailing_pips
```

Exemple :

```text
TRAILING_DISTANCE = 10 pips
```

Si :

```text
current_pips <= MFE - 10
```

=> sortie.

### EXIT MOMENTUM LOSS

Si :

```text
velocity < 0
```

et :

```text
acceleration < 0
```

pendant plusieurs runs.

### EXIT ALIGNMENT LOSS

LONG :

```text
↑↑ → ↑↓ → ↓↓
```

SHORT :

```text
↓↓ → ↓↑ → ↑↑
```

### EXIT STRENGTH REVERSAL

Lorsque :

```text
strength_differential
```

se réduit fortement ou change de signe.

---

# 24. TRAILING DYNAMIQUE

Prévoir éventuellement plusieurs niveaux.

Exemple :

```text
MFE < 10
no trailing

MFE >= 10
trailing = 8

MFE >= 20
trailing = 10

MFE >= 30
trailing = 12
```

Ou inversement tester une logique plus serrée.

Le système doit être configurable et backtestable.

---

# 25. NOUVEL AFFICHAGE TELEGRAM

Créer un bloc :

```text
🏆 07H RELATIVE LEADERS
```

Exemple :

```text
🏆 07H RELATIVE LEADERS

1️⃣ USDCAD SHORT
📈 +26.1 pips
🎯 Break +5 : 08:19
🔁 Persistence : 6 runs
⚡ Velocity : +8.4 pips/h
🚀 Acceleration : positive
💪 CAD fort / USD faible
🌐 Breadth : 6/7
🎯 Entry : EXTENSION
🔥 Score : 93/100

2️⃣ EURUSD LONG
📈 +11.2 pips
🎯 Break +5 : 09:19
🔁 Persistence : 4 runs
⚡ Velocity : +4.1 pips/h
💪 EUR fort / USD faible
🌐 Breadth USD weak : 6/7
🎯 Entry : SWEET_SPOT
🔥 Score : 87/100

3️⃣ NZDUSD LONG
📈 +8.3 pips
🔁 Persistence : 3 runs
💪 NZD fort / USD faible
🌐 NZD Breadth : 5/6
🎯 Entry : SWEET_SPOT
🔥 Score : 85/100
```

---

# 26. AFFICHAGE NOISE

Créer :

```text
⚪ 07H NOISE
```

Exemple :

```text
⚪ 07H NOISE

NZDCHF +3.1
EURJPY +1.7
GBPCAD -2.4
```

---

# 27. AFFICHAGE INVALIDATED

Créer :

```text
❌ 07H INVALIDATED
```

Exemple :

```text
❌ CADJPY LONG
-7.5 pips
MAE -11.2
Alignment lost
```

---

# 28. ALERTE PREMIER BREAKOUT

Lorsqu’une paire franchit le threshold pour la première fois :

```text
👀 BREAKOUT WATCH

NZDUSD LONG
+5.7 pips depuis 07h
1er run > threshold
Attente confirmation
```

---

# 29. ALERTE CONFIRMATION

Après le nombre de runs requis :

```text
🔥 07H BREAKOUT CONFIRMED

NZDUSD LONG
+8.9 pips
2 runs > +5
NZD fort
USD faible
Breadth 5/6
↑↑
Score 88
```

---

# 30. ALERTE CHASING

Si le mouvement est déjà trop étendu :

```text
⚠️ LATE ENTRY / CHASING

EURUSD LONG
+31.4 pips depuis 07h
Signal toujours bullish
Mais mouvement déjà étendu

Action :
NE PAS CHASSER
```

---

# 31. HISTORIQUE JOURNALIER

Créer une structure par journée.

Exemple :

```json
{
  "date": "2026-08-14",
  "reference_session": "07:00 Europe/Paris",
  "pairs": {
    "USDCAD": {
      "reference_price": 1.39168,
      "reference_time": "07:19",
      "direction": "SHORT",
      "current_pips": 39.1,
      "mfe": 46.7,
      "mae": 0.0,
      "threshold_break_time": "08:19",
      "confirmation_runs": 12,
      "velocity": 3.4,
      "acceleration": -0.6,
      "score": 91
    }
  }
}
```

---

# 32. PERSISTENCE DES DONNÉES

Les données doivent survivre aux redémarrages.

Utiliser la solution déjà utilisée par le projet si elle existe.

Sinon proposer :

```text
SQLite
PostgreSQL
Supabase
JSON persistant
```

mais privilégier la technologie existante.

Ne pas créer une nouvelle dépendance inutilement.

---

# 33. RESET QUOTIDIEN

À chaque nouvelle journée Forex :

réinitialiser les références.

Timezone obligatoire :

```text
Europe/Paris
```

Référence :

```text
07:00 Paris
```

Attention aux changements heure été / heure hiver.

Utiliser une vraie timezone IANA.

Ne jamais hardcoder UTC+1 ou UTC+2.

---

# 34. BACKTEST

Créer un module de backtest permettant de tester automatiquement :

```text
threshold = 3
threshold = 5
threshold = 7.5
threshold = 10
```

et :

```text
confirmation runs = 1
2
3
4
```

Ainsi que :

```text
entry max = 15
20
25
30 pips
```

---

# 35. MÉTRIQUES DE BACKTEST

Pour chaque combinaison calculer :

```text
Number of trades
Win rate
Average gain
Average loss
Profit Factor
Expectancy
Median MFE
Median MAE
Average MFE
Average MAE
Max Drawdown
Average hold time
False breakout rate
Threshold survival rate
Return-to-noise rate
Late entry rate
```

---

# 36. THRESHOLD SURVIVAL RATE

Créer notamment :

```text
threshold_survival_rate
```

Question :

Après franchissement de +5 pips, quelle proportion des trades ne revient jamais sous +5 avant d’atteindre :

```text
+10
+15
+20
+30
```

C’est une métrique très importante.

---

# 37. FALSE BREAKOUT RATE

Définition initiale :

Une paire franchit :

```text
+5
```

puis revient dans :

```text
-5 → +5
```

avant d’atteindre :

```text
+10
```

=> false breakout.

Cette définition doit être configurable.

---

# 38. TIME TO THRESHOLD

Mesurer :

```text
time_to_threshold
```

Exemple :

```text
07h reference
break +5 à 08h10
```

donne :

```text
1h10
```

Les breakouts précoces peuvent avoir une valeur prédictive différente des breakouts tardifs.

Le backtest doit mesurer cela.

---

# 39. TIME TO MFE

Mesurer :

```text
time_to_mfe
```

et :

```text
time_from_breakout_to_mfe
```

---

# 40. PAIR RANKING À CHAQUE RUN

À chaque run, produire un classement.

Exemple :

```text
07H RANK

1 USDCAD SHORT +26.1
2 EURUSD LONG +14.2
3 NZDUSD LONG +11.8
4 NZDCHF LONG +7.4
5 EURJPY LONG +4.1
```

Mais ne pas classer uniquement par pips.

Afficher également :

```text
RSB_SCORE
```

---

# 41. DEUX CLASSEMENTS

Afficher :

## PIP RANK

Classement purement mécanique par performance depuis 07h.

## QUALITY RANK

Classement par :

```text
RSB_SCORE
```

Exemple :

```text
📈 PIP RANK
1 EURJPY +48.8
2 USDCAD +44.4
3 EURUSD +41.5

🏆 QUALITY RANK
1 USDCAD 94
2 NZDUSD 91
3 NZDCHF 88
```

Cela évite de confondre :

```text
plus gros mouvement
```

et :

```text
meilleur trade exploitable
```

---

# 42. IMPORTANT : ÉVITER LE LOOKAHEAD BIAS

Le système temps réel ne doit JAMAIS utiliser une information future.

Lors du backtest :

à chaque timestamp :

```text
t
```

le moteur n’a le droit d’utiliser que :

```text
data <= t
```

Ne jamais sélectionner une direction ou une paire parce qu’on sait qu’elle sera gagnante ensuite.

---

# 43. IMPORTANT : DISTINGUER EX-ANTE ET EX-POST

Créer deux types d’analyse :

```text
REAL_TIME_SCORE
```

calculé uniquement avec les informations disponibles à l’instant t.

Et :

```text
POST_SESSION_ANALYSIS
```

calculé après la journée pour évaluer ce qui s’est réellement passé.

Ne jamais mélanger les deux.

---

# 44. CORRÉLATION ET RISQUE DE DUPLICATION

Détecter les trades représentant la même thèse.

Exemple :

```text
NZDUSD LONG
NZDCHF LONG
NZDJPY LONG
EURNZD SHORT
AUDNZD SHORT
```

représentent tous :

```text
LONG NZD
```

Créer :

```text
currency_exposure
```

et :

```text
theme_exposure
```

Le système doit signaler :

```text
⚠️ HIGH NZD EXPOSURE
```

si plusieurs trades similaires sont sélectionnés.

---

# 45. DIVERSIFICATION

Si plusieurs paires représentent la même devise forte, sélectionner la meilleure expression.

Exemple :

NZD fort.

Candidates :

```text
NZDUSD
NZDCHF
NZDJPY
EURNZD
AUDNZD
```

Comparer :

```text
strength differential
RSB score
entry zone
spread
persistence
velocity
MAE
efficiency
```

Puis choisir :

```text
BEST EXPRESSION
```

---

# 46. BEST EXPRESSION

Créer un bloc :

```text
💎 BEST CURRENCY EXPRESSION
```

Exemple :

```text
NZD strongest currency

1. NZDCHF LONG — 92
2. NZDUSD LONG — 89
3. NZDJPY LONG — 83

BEST EXPRESSION:
NZDCHF LONG
```

---

# 47. ARCHITECTURE TECHNIQUE

Avant de coder :

1. inspecter tout le repository ;
2. identifier l’architecture existante ;
3. identifier où sont calculés :

   * VIVIER ;
   * TOP MOMENTUM ;
   * CONVERGENT PAIRS ;
   * FULL ALIGNMENT ;
   * currency strength ;
   * consensus IA ;
4. identifier la persistance ;
5. identifier Telegram ;
6. identifier le scheduler ;
7. identifier les modèles de données.

Ne pas dupliquer une fonction existante.

Réutiliser les abstractions existantes.

---

# 48. MODULES CONSEILLÉS

Si l’architecture le permet, créer quelque chose du type :

```text
relative_strength/
    reference.py
    pip_engine.py
    threshold.py
    persistence.py
    velocity.py
    breadth.py
    scoring.py
    ranking.py
    alerts.py
    backtest.py
```

Mais adapter les noms et emplacements au repository existant.

---

# 49. DATACLASS / MODEL

Créer éventuellement une structure :

```python
RelativeStrengthState
```

avec notamment :

```python
pair
timestamp
reference_time
reference_price
current_price
trade_direction

raw_market_pips
directional_pips

threshold_state
threshold_break_time
above_threshold_runs

mfe_pips
mae_pips
drawdown_from_mfe

velocity
acceleration

technical_alignment
warning
fire

base_strength
quote_strength
strength_differential

base_breadth
quote_breadth

convergence
persistence_runs

entry_zone

raw_score
penalty_score
final_score

rank
quality_rank
```

---

# 50. CONFIGURATION

Tout ce qui suit doit être configurable :

```python
REFERENCE_HOUR = "07:00"
TIMEZONE = "Europe/Paris"

THRESHOLD_PIPS = 5.0

MIN_CONFIRMATION_RUNS = 2

SWEET_SPOT_MAX = 15
EXTENSION_MAX = 25

TRAILING_ENABLED = True
TRAILING_DISTANCE = 10

WEIGHT_STRENGTH_DIFF = 0.20
WEIGHT_BREADTH = 0.15
WEIGHT_PERSISTENCE = 0.15
WEIGHT_PIPS = 0.10
WEIGHT_VELOCITY = 0.10
WEIGHT_ACCELERATION = 0.05
WEIGHT_ALIGNMENT = 0.10
WEIGHT_CONVERGENCE = 0.05
WEIGHT_EFFICIENCY = 0.05
WEIGHT_ENTRY = 0.05
```

Ne pas disperser des magic numbers dans le code.

---

# 51. TESTS UNITAIRES

Créer des tests pour :

* calcul pip EURUSD ;
* calcul pip USDJPY ;
* LONG ;
* SHORT ;
* reference price ;
* threshold ;
* confirmation runs ;
* false breakout ;
* MFE ;
* MAE ;
* velocity ;
* acceleration ;
* breadth ;
* score ;
* chasing ;
* trailing ;
* reset journalier ;
* timezone ;
* exposition devise.

---

# 52. TESTS SCÉNARIOS

Créer notamment :

### Test A

```text
0
+3
+6
+10
+17
```

Résultat attendu :

```text
breakout confirmed
```

### Test B

```text
0
+6
+2
+7
+1
```

Résultat :

```text
false / unstable breakout
```

### Test C

```text
0
-3
-8
-4
+6
+18
```

Résultat :

MAE significatif.

### Test D

```text
0
+7
+14
+25
+37
+22
```

Résultat :

```text
MFE 37
drawdown 15
exhaustion / trailing candidate
```

---

# 53. LOGS

Créer des logs explicites.

Exemple :

```text
[RSB] NZDUSD reference initialized 0.58666 @ 07:19
[RSB] NZDUSD threshold break +5.4
[RSB] NZDUSD breakout confirmed after 2 runs
[RSB] NZDUSD score=89.4
[RSB] NZDUSD entry_zone=SWEET_SPOT
```

---

# 54. NE PAS CASSER L’EXISTANT

Exigence critique :

Le système existant doit continuer à fonctionner.

Ne supprimer aucune logique existante sauf bug démontré.

Créer cette fonctionnalité comme une couche supplémentaire.

---

# 55. PAS DE FAUX SIGNAL IA

Le modèle IA ne doit pas inventer un classement.

Les calculs doivent être déterministes.

Gemini peut éventuellement produire une synthèse textuelle à partir des métriques, mais :

```text
pips
scores
ranks
breadth
MFE
MAE
velocity
```

doivent être calculés par le code.

---

# 56. SYNTHÈSE IA

La synthèse IA doit recevoir des données structurées.

Exemple :

```json
{
  "strongest_currency": "NZD",
  "weakest_currency": "USD",
  "best_pair": "NZDUSD",
  "score": 91,
  "directional_pips": 12.4,
  "runs": 4,
  "breadth": 0.83
}
```

L’IA doit uniquement expliquer le résultat.

Elle ne doit jamais recalculer arbitrairement les métriques.

---

# 57. SORTIE IA ATTENDUE

Exemple :

```text
🔥 NZD reste la devise dominante.

NZDUSD bénéficie actuellement :
• d’un différentiel de force élevé ;
• de 5 cross confirmants ;
• de 4 runs au-dessus du threshold ;
• d’un déplacement de +12.4 pips depuis 07h ;
• d’un momentum toujours positif.

La paire reste dans la zone SWEET_SPOT.

🎯 NZDUSD LONG
Score : 91/100
```

---

# 58. PREMIÈRE LIVRAISON ATTENDUE

Ne commence PAS directement à modifier 30 fichiers.

Commence par :

## A — AUDIT

Présente :

```text
1. fichiers concernés
2. architecture actuelle
3. fonctions réutilisables
4. données déjà disponibles
5. données manquantes
6. risques
```

Puis :

## B — PLAN

Présente un plan concret par étapes.

Puis seulement :

## C — IMPLEMENTATION

Implémente progressivement.

---

# 59. APRÈS IMPLÉMENTATION

Fournir :

```text
FILES CREATED
FILES MODIFIED

FEATURES ADDED

TESTS ADDED

TEST RESULTS

KNOWN LIMITATIONS

CONFIGURATION

HOW TO RUN

EXAMPLE OUTPUT
```

---

# 60. CRITÈRE DE RÉUSSITE

À terme, à chaque run le système doit pouvoir répondre automatiquement à quatre questions différentes :

```text
1. Quelle paire a parcouru le plus de pips depuis 07h ?

2. Quelle paire offre actuellement le signal de meilleure qualité ?

3. Quelle devise est intrinsèquement la plus forte / faible ?

4. Quelle paire constitue la meilleure expression de cette divergence de force ?
```

Ce sont quatre questions différentes.

Ne jamais les confondre.

---

# 61. PHILOSOPHIE DU MOTEUR

Le moteur doit suivre cette logique :

```text
PRICE FIRST
→ THRESHOLD
→ PERSISTENCE
→ FORCE
→ BREADTH
→ ALIGNMENT
→ RANKING
→ ENTRY QUALITY
→ RISK
```

Le but est de ne plus choisir une paire uniquement parce qu’elle affiche :

```text
↑↑ 🔥
```

mais parce qu’elle démontre objectivement :

```text
un déplacement réel
+
une persistance
+
une force relative
+
une confirmation multi-cross
+
un timing acceptable
```

---

# 62. RÈGLE FONDAMENTALE

Un signal précoce, persistant et propre doit généralement être mieux noté qu’un signal spectaculaire apparaissant après que le marché a déjà parcouru 30 ou 40 pips.

Autrement dit :

```text
EARLY + CLEAN + PERSISTENT
>
LATE + EXPLOSIVE
```

sauf preuve statistique contraire issue du backtest.

---

# 63. OBJECTIF FINAL

Je veux obtenir un moteur capable de faire automatiquement ce type de distinction :

```text
🏆 BEST QUALITY
USDCAD SHORT
Score 94
+26 pips
Break précoce
Très faible MAE
6 runs
USD faible / CAD fort

📈 BIGGEST MOVE
EURJPY LONG
+48 pips

🔥 STRONGEST CURRENCY
NZD

💎 BEST NZD EXPRESSION
NZDCHF LONG
Score 91

⚠️ TOO LATE
GBPUSD LONG
+31 pips déjà réalisés
CHASING

❌ REJECT
CADJPY LONG
MAE trop élevé
Break tardif
```

C’est cette intelligence décisionnelle que tu dois intégrer au système.

Commence maintenant par auditer le repository existant et indique précisément comment tu vas intégrer ce moteur sans casser l’architecture actuelle.
