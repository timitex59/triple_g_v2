"""
FIOS — Forex Intelligence Operating System (Phase 1).

Moteur de confluence multi-sources pour le Forex. Chaque famille de signaux
(fondamental, technique, sentiment/positionnement, correlations) est produite
par un adaptateur independant, normalisee sur une echelle 0-100, puis fusionnee
par un moteur de confluence pondere qui ne produit un BUY/SELL que lorsque
plusieurs familles convergent.

Donnees graphiques : TradingView WebSocket (comme le reste du repo).
"""

__version__ = "0.1.0"
