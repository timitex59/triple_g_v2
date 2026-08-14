# -*- coding: utf-8 -*-
"""
RSB Velocity & Acceleration Engine (V1 Raw Values)
=================================================
Calcul de la cinétique du mouvement en pips/heure et pips/heure².
Conforme à l'Amendement #4:
- Conserve les valeurs brutes velocity_pips_per_hour et acceleration_pips_per_hour2.
- Pas de score 0-100 arbitraire sans base statistique.
"""

from typing import Tuple


def calculate_velocity_and_acceleration(
    current_directional_pips: float,
    previous_directional_pips: float,
    delta_time_hours: float,
    previous_velocity: float = 0.0
) -> Tuple[float, float]:
    """
    Calcule la vitesse brute en pips/heure et l'accélération brute en pips/heure².
    
    Formules:
    velocity = (directional_pips_t - directional_pips_t-1) / delta_t_hours
    acceleration = (velocity_t - velocity_t-1) / delta_t_hours
    
    Returns:
    (velocity_pips_per_hour, acceleration_pips_per_hour2)
    """
    if delta_time_hours <= 0:
        return (0.0, 0.0)
    
    delta_pips = current_directional_pips - previous_directional_pips
    velocity_pips_per_hour = delta_pips / delta_time_hours
    
    acceleration_pips_per_hour2 = (velocity_pips_per_hour - previous_velocity) / delta_time_hours
    
    return (velocity_pips_per_hour, acceleration_pips_per_hour2)
