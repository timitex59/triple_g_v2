# RENKO FOREX V17 VIVIER

Fichier Pine : `renko_forex_V17_vivier.pine`

## Utilisation

1. Ouvrir une paire Forex OANDA sur un graphique en chandeliers standards 1H.
2. Ouvrir Pine Editor, coller le contenu du fichier, puis cliquer sur **Add to chart**.
3. Conserver les paramètres par défaut pour rester aligné avec
   `renko_score_29pairs_v16.py` : Renko ATR(14), poids 3/2/1, score VIVIER
   minimum 33 %, séparation Fibonacci 50 %, SAR 0.1/0.1/0.2.

Le script est volontairement bloqué sur les clôtures du graphique 1H pour ne
pas modifier le VIVIER sur une bougie encore ouverte.
La plage Fibonacci est remise à zéro au changement de mois civil UTC, comme
dans le scanner Python, et non à l'ouverture de la bougie mensuelle Forex.

## Événements affichés

- `VIVIER BULL/BEAR` : nouvelle entrée du cycle.
- `FLAMME BULL/BEAR` : crossover/crossunder SAR admissible établissant un
  nouveau record directionnel.
- `FIB 1 TOUCH` pour un VIVIER BULL et `FIB 0 TOUCH` pour un VIVIER BEAR.
- `OBJECTIF REPORTÉ` : l'objectif fixe Fibo 1/0 du cycle précédent reste
  affiché lorsque le mois UTC change sans qu'il ait été atteint. Les nouveaux
  niveaux mensuels sont affichés simultanément.
- `OBJECTIF REPORTÉ ATTEINT` : la ligne reportée est arrêtée sur la bougie du
  contact. Si un record SAR existait, la première flamme est réarmée.
- `RÉARMEMENT FLAMME` : le contact Fibo a effacé le record SAR courant ; le
  prochain crossover/crossunder admissible devient une nouvelle première
  flamme.
- `ALIGNEMENT COMPLET` : M/W/D sont strictement alignées et la paire sort du
  VIVIER.
- `VIVIER -> RENKO FIBO` : l'alignement possède aussi la confluence complète
  du score, des streaks Renko et du SAR H1.
- `SUIVI POST-ALIGNEMENT` : la paire est sortie du VIVIER, mais son objectif
  fixe non atteint reste actif et visible.
- `OBJECTIF POST-SIGNAL ATTEINT` : l'objectif conservé après l'alignement a
  été touché. Ce contact termine seulement le suivi de l'objectif et ne réarme
  pas une flamme tant que la paire reste hors VIVIER.
- `OBJECTIF POST-SIGNAL ANNULÉ` : Monthly est devenue Inside ou a changé de
  direction avant le contact.
- `SORTIE VIVIER INVALIDE` : Monthly a changé/est devenue Inside, ou le score
  absolu est passé sous 33 %.

## Règles d'entrée

- Monthly doit être strictement BULL ou BEAR.
- Weekly ou Daily peut être strictement opposée à Monthly ; ou Weekly peut
  déjà être alignée avec Monthly lorsque Daily est Inside.
- Score Renko brut absolu supérieur ou égal à 33 %.
- BULL : prix au plus à Fibo 0.500.
- BEAR : prix au moins à Fibo 0.500.

Le filtre Fibonacci ne retire pas une paire déjà suivie si le prix traverse
ensuite 0.500.

Un seul objectif fixe est conservé par cycle. Il est initialisé à l'entrée du
VIVIER, puis recréé à la première flamme suivant un objectif atteint. S'il
reste intact pendant plusieurs changements de mois, le même objectif demeure
reporté : les objectifs mensuels intermédiaires ne s'accumulent pas.

Une sortie par alignement ne supprime plus cet objectif. Si la paire revient
dans le VIVIER dans le même sens, l'objectif post-alignement est rattaché au
nouveau cycle. Une réentrée dans le sens opposé l'annule et crée un nouvel
objectif directionnel.

## Limite de synchronisation

Pine ne peut pas lire le JSON persistant du scanner Python. Il reconstruit son
propre VIVIER en rejouant l'historique chargé sur le graphique. Les règles sont
les mêmes, mais l'état peut différer si TradingView ne charge pas le début du
cycle ou si le scanner Python n'a pas été exécuté à chaque clôture H1.
