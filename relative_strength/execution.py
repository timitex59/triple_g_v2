# -*- coding: utf-8 -*-
"""
RSB Multi-Policy Execution Framework (Version Audité & Plafonnement Stricte)
=============================================================================
Gestionnaires d'exécution pour les portefeuilles RSB-A, RSB-B, RSB-C1..C4.

Corrections Audit Mathématique & Motif de Rejet Auditable :
1. Application STRICTE du plafonnement de capacité (max_capacity) À CHAQUE SIGNAL.
2. Motif de rejet explicite avec le nom de la paire : POSITION_ALREADY_ACTIVE(pair=...).
3. Interdiction STRICTE d'ouvrir de nouvelles positions lors du dernier run de la journée (is_last_run_of_day=True à 22h00).
"""

import copy
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any
from relative_strength.models import RelativeStrengthState
from relative_strength.config import THRESHOLD_PIPS, SWEET_SPOT_MAX, EXTENSION_MAX


@dataclass
class TradePosition:
    trade_id: str
    variant_id: str
    pair: str
    direction: str  # "LONG", "SHORT"
    entry_time: str
    entry_price: float
    entry_pips_07h: float
    entry_zone: str  # "SWEET_SPOT", "EXTENSION"
    opportunity_score: float
    quality_score: float
    opportunity_rank: int
    confirmation_runs: int
    base_currency: str
    quote_currency: str
    
    # Excursions & PnL
    mfe_pips: float = 0.0
    mae_pips: float = 0.0
    current_pips: float = 0.0
    exit_pips: float = 0.0
    exit_time: Optional[str] = None
    exit_reason: str = "IN_PROGRESS"
    duration_runs: int = 0
    is_false_breakout: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RejectedSignal:
    trade_id: str
    variant_id: str
    timestamp: str
    pair: str
    direction: str
    entry_zone: str
    opportunity_rank: int
    rejection_reason: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def get_directional_currency_exposures(active_positions: List[TradePosition]) -> Dict[str, int]:
    """Calcule l'exposition directionnelle nette par devise."""
    exposures: Dict[str, int] = {}
    for pos in active_positions:
        base = pos.base_currency
        quote = pos.quote_currency
        
        if pos.direction == "LONG":
            exposures[base] = exposures.get(base, 0) + 1
            exposures[quote] = exposures.get(quote, 0) - 1
        elif pos.direction == "SHORT":
            exposures[base] = exposures.get(base, 0) - 1
            exposures[quote] = exposures.get(quote, 0) + 1
            
    return exposures


class BaseExecutionManager:
    """Classe de base abstraite pour les portefeuilles d'exécution."""
    def __init__(
        self,
        variant_id: str,
        sl_pips: float = 10.0,
        tp_pips: float = 20.0,
        trailing_trigger: float = 15.0,
        trailing_dist: float = 10.0,
        use_invalidation_exit: bool = True,
        use_pips_07h_pnl: bool = True
    ):
        self.variant_id = variant_id
        self.sl_pips = sl_pips
        self.tp_pips = tp_pips
        self.trailing_trigger = trailing_trigger
        self.trailing_dist = trailing_dist
        self.use_invalidation_exit = use_invalidation_exit
        self.use_pips_07h_pnl = use_pips_07h_pnl
        
        self.active_positions: List[TradePosition] = []
        self.closed_trades: List[TradePosition] = []
        self.rejected_signals: List[RejectedSignal] = []
        self.disarmed_pairs: Dict[str, bool] = {}

    def update_positions_and_check_exits(self, states: List[RelativeStrengthState], is_last_run_of_day: bool = False):
        states_map = {s.pair: s for s in states}
        still_active = []

        for s in states:
            if not s.is_eligible and self.disarmed_pairs.get(s.pair, False):
                self.disarmed_pairs[s.pair] = False

        for pos in self.active_positions:
            s = states_map.get(pos.pair)
            if not s:
                still_active.append(pos)
                continue

            if self.use_pips_07h_pnl:
                pips_gain = s.directional_pips - pos.entry_pips_07h
            else:
                pip_size = 0.01 if "JPY" in pos.pair else 0.0001
                if pos.direction == "LONG":
                    pips_gain = (s.current_price - pos.entry_price) / pip_size
                else:
                    pips_gain = (pos.entry_price - s.current_price) / pip_size

            pos.current_pips = pips_gain
            pos.mfe_pips = max(pos.mfe_pips, pips_gain)
            pos.mae_pips = min(pos.mae_pips, pips_gain)
            pos.duration_runs += 1

            # 1. Stop Loss (-10.0 pips)
            if pips_gain <= -self.sl_pips:
                pos.exit_pips = -self.sl_pips
                pos.exit_reason = "STOP_LOSS"
                pos.exit_time = s.timestamp
                self.closed_trades.append(pos)
                self.disarmed_pairs[pos.pair] = True
                continue

            # 2. Take Profit (+20.0 pips)
            if pips_gain >= self.tp_pips:
                pos.exit_pips = self.tp_pips
                pos.exit_reason = "TAKE_PROFIT"
                pos.exit_time = s.timestamp
                self.closed_trades.append(pos)
                self.disarmed_pairs[pos.pair] = True
                continue

            # 3. Trailing Stop (MFE >= 15.0p, recul de 10.0p)
            if pos.mfe_pips >= self.trailing_trigger and (pos.mfe_pips - pips_gain) >= self.trailing_dist:
                pos.exit_pips = pos.mfe_pips - self.trailing_dist
                pos.exit_reason = "TRAILING_STOP"
                pos.exit_time = s.timestamp
                self.closed_trades.append(pos)
                self.disarmed_pairs[pos.pair] = True
                continue

            # 4. Invalidation RSB (cur_pips <= -5.0)
            if self.use_invalidation_exit and s.directional_pips <= -5.0:
                pos.exit_pips = pips_gain
                pos.exit_reason = "INVALIDATED"
                pos.exit_time = s.timestamp
                pos.is_false_breakout = True
                self.closed_trades.append(pos)
                self.disarmed_pairs[pos.pair] = True
                continue

            # 5. End of Day Clôture (Dernier run de la journée à 22h00 Paris)
            if is_last_run_of_day:
                pos.exit_pips = pips_gain
                pos.exit_reason = "END_OF_DAY"
                pos.exit_time = s.timestamp
                self.closed_trades.append(pos)
                self.disarmed_pairs[pos.pair] = True
                continue

            still_active.append(pos)

        self.active_positions = still_active


class ExecutionManagerA(BaseExecutionManager):
    """🅰️ RSB-A : Daily Single-Trade Manager (1 seul trade par jour pour tout le portefeuille)."""
    def __init__(self, **kwargs):
        super().__init__("RSB-A", **kwargs)
        self.daily_entered = False

    def process_run(self, states: List[RelativeStrengthState], is_last_run_of_day: bool = False) -> Tuple[List[TradePosition], List[RejectedSignal]]:
        self.update_positions_and_check_exits(states, is_last_run_of_day)

        new_entries = []
        new_rejections = []

        if is_last_run_of_day:
            return new_entries, new_rejections

        eligible_states = [s for s in states if s.is_eligible]
        eligible_states.sort(key=lambda s: s.opportunity_rank)
        top_eligible = eligible_states[0] if eligible_states else None

        for s in eligible_states:
            timestamp_clean = s.timestamp.replace(":", "").replace("-", "").replace("T", "_")[:13]
            t_id = f"{timestamp_clean}-{s.pair}-{s.trade_direction}"

            if self.daily_entered or len(self.active_positions) > 0:
                rej = RejectedSignal(t_id, self.variant_id, s.timestamp, s.pair, s.trade_direction, s.entry_zone, s.opportunity_rank, "DAILY_TRADE_LIMIT_REACHED")
                self.rejected_signals.append(rej)
                new_rejections.append(rej)
            elif s != top_eligible:
                rej = RejectedSignal(t_id, self.variant_id, s.timestamp, s.pair, s.trade_direction, s.entry_zone, s.opportunity_rank, "NOT_RANK_1")
                self.rejected_signals.append(rej)
                new_rejections.append(rej)
            elif not self.daily_entered and s == top_eligible:
                pos = TradePosition(
                    trade_id=t_id, variant_id=self.variant_id, pair=s.pair, direction=s.trade_direction,
                    entry_time=s.timestamp, entry_price=s.current_price, entry_pips_07h=s.directional_pips,
                    entry_zone=s.entry_zone, opportunity_score=s.opportunity_score, quality_score=s.quality_score,
                    opportunity_rank=s.opportunity_rank, confirmation_runs=s.confirmation_runs,
                    base_currency=s.base_currency, quote_currency=s.quote_currency
                )
                self.active_positions.append(pos)
                new_entries.append(pos)
                self.daily_entered = True

        return new_entries, new_rejections


class ExecutionManagerB(BaseExecutionManager):
    """🅱️ RSB-B : Rotation Intraday (1 seule position active à la fois)."""
    def __init__(self, **kwargs):
        super().__init__("RSB-B", **kwargs)

    def process_run(self, states: List[RelativeStrengthState], is_last_run_of_day: bool = False) -> Tuple[List[TradePosition], List[RejectedSignal]]:
        self.update_positions_and_check_exits(states, is_last_run_of_day)

        new_entries = []
        new_rejections = []

        if is_last_run_of_day:
            return new_entries, new_rejections

        eligible_states = [s for s in states if s.is_eligible]
        eligible_states.sort(key=lambda s: s.opportunity_rank)
        top_eligible = eligible_states[0] if eligible_states else None

        for s in eligible_states:
            timestamp_clean = s.timestamp.replace(":", "").replace("-", "").replace("T", "_")[:13]
            t_id = f"{timestamp_clean}-{s.pair}-{s.trade_direction}"

            if len(self.active_positions) > 0:
                active_pair = self.active_positions[0].pair
                rej = RejectedSignal(t_id, self.variant_id, s.timestamp, s.pair, s.trade_direction, s.entry_zone, s.opportunity_rank, f"POSITION_ALREADY_ACTIVE(active={active_pair})")
                self.rejected_signals.append(rej)
                new_rejections.append(rej)
            elif s != top_eligible:
                rej = RejectedSignal(t_id, self.variant_id, s.timestamp, s.pair, s.trade_direction, s.entry_zone, s.opportunity_rank, "NOT_RANK_1")
                self.rejected_signals.append(rej)
                new_rejections.append(rej)
            elif len(self.active_positions) == 0 and s == top_eligible:
                pos = TradePosition(
                    trade_id=t_id, variant_id=self.variant_id, pair=s.pair, direction=s.trade_direction,
                    entry_time=s.timestamp, entry_price=s.current_price, entry_pips_07h=s.directional_pips,
                    entry_zone=s.entry_zone, opportunity_score=s.opportunity_score, quality_score=s.quality_score,
                    opportunity_rank=s.opportunity_rank, confirmation_runs=s.confirmation_runs,
                    base_currency=s.base_currency, quote_currency=s.quote_currency
                )
                self.active_positions.append(pos)
                new_entries.append(pos)

        return new_entries, new_rejections


class ExecutionManagerC(BaseExecutionManager):
    """🅲 RSB-C : All Eligible Manager avec sous-variantes C1, C2, C3, C4."""
    def __init__(self, sub_variant: str = "C4", max_currency_exposure: int = 2, strict_rearm: bool = True, max_capacity: Optional[int] = None, **kwargs):
        super().__init__(f"RSB-{sub_variant}", **kwargs)
        self.sub_variant = sub_variant
        self.max_currency_exposure = max_currency_exposure
        self.strict_rearm = strict_rearm
        self.max_capacity = max_capacity

    def process_run(self, states: List[RelativeStrengthState], is_last_run_of_day: bool = False) -> Tuple[List[TradePosition], List[RejectedSignal]]:
        self.update_positions_and_check_exits(states, is_last_run_of_day)

        new_entries = []
        new_rejections = []

        # Au dernier run de la journée (is_last_run_of_day=True à 22h00), fermer les trades mais NE PAS OUVRIR de nouvelles positions.
        if is_last_run_of_day:
            return new_entries, new_rejections

        active_pairs = {p.pair for p in self.active_positions}
        dynamic_exposures = get_directional_currency_exposures(self.active_positions)
        eligible_states = [s for s in states if s.is_eligible]

        for s in eligible_states:
            timestamp_clean = s.timestamp.replace(":", "").replace("-", "").replace("T", "_")[:13]
            t_id = f"{timestamp_clean}-{s.pair}-{s.trade_direction}"

            # Check 1: Plafonnement strict de la capacité du portefeuille
            if self.max_capacity and len(self.active_positions) >= self.max_capacity:
                rej = RejectedSignal(t_id, self.variant_id, s.timestamp, s.pair, s.trade_direction, s.entry_zone, s.opportunity_rank, f"PORTFOLIO_CAPACITY_FULL({len(self.active_positions)}/{self.max_capacity})")
                self.rejected_signals.append(rej)
                new_rejections.append(rej)
                continue

            # Check 2: Position déjà active sur CETTE PAIRE
            if s.pair in active_pairs:
                rej = RejectedSignal(t_id, self.variant_id, s.timestamp, s.pair, s.trade_direction, s.entry_zone, s.opportunity_rank, f"POSITION_ALREADY_ACTIVE(pair={s.pair})")
                self.rejected_signals.append(rej)
                new_rejections.append(rej)
                continue

            # Check 3: Strict Re-Arming Rule
            if self.strict_rearm and self.disarmed_pairs.get(s.pair, False):
                rej = RejectedSignal(t_id, self.variant_id, s.timestamp, s.pair, s.trade_direction, s.entry_zone, s.opportunity_rank, f"WAITING_REARM({s.pair})")
                self.rejected_signals.append(rej)
                new_rejections.append(rej)
                continue

            # Check 4: Filtre C1 (SWEET SPOT uniquement)
            if self.sub_variant == "C1" and s.entry_zone != "SWEET_SPOT":
                rej = RejectedSignal(t_id, self.variant_id, s.timestamp, s.pair, s.trade_direction, s.entry_zone, s.opportunity_rank, "EXTENSION_NOT_ALLOWED")
                self.rejected_signals.append(rej)
                new_rejections.append(rej)
                continue

            # Check 5: Filtre C3 (Limite Exposition Directionnelle Devise <= 2)
            if self.sub_variant == "C3":
                base = s.base_currency
                quote = s.quote_currency
                
                new_base_exp = dynamic_exposures.get(base, 0) + (1 if s.trade_direction == "LONG" else -1)
                new_quote_exp = dynamic_exposures.get(quote, 0) + (-1 if s.trade_direction == "LONG" else 1)

                if abs(new_base_exp) > self.max_currency_exposure or abs(new_quote_exp) > self.max_currency_exposure:
                    rej = RejectedSignal(t_id, self.variant_id, s.timestamp, s.pair, s.trade_direction, s.entry_zone, s.opportunity_rank, "CURRENCY_EXPOSURE_LIMIT")
                    self.rejected_signals.append(rej)
                    new_rejections.append(rej)
                    continue

            # Signal Accepté
            pos = TradePosition(
                trade_id=t_id, variant_id=self.variant_id, pair=s.pair, direction=s.trade_direction,
                entry_time=s.timestamp, entry_price=s.current_price, entry_pips_07h=s.directional_pips,
                entry_zone=s.entry_zone, opportunity_score=s.opportunity_score, quality_score=s.quality_score,
                opportunity_rank=s.opportunity_rank, confirmation_runs=s.confirmation_runs,
                base_currency=s.base_currency, quote_currency=s.quote_currency
            )
            self.active_positions.append(pos)
            active_pairs.add(s.pair)
            
            if s.trade_direction == "LONG":
                dynamic_exposures[s.base_currency] = dynamic_exposures.get(s.base_currency, 0) + 1
                dynamic_exposures[s.quote_currency] = dynamic_exposures.get(s.quote_currency, 0) - 1
            else:
                dynamic_exposures[s.base_currency] = dynamic_exposures.get(s.base_currency, 0) - 1
                dynamic_exposures[s.quote_currency] = dynamic_exposures.get(s.quote_currency, 0) + 1
                
            new_entries.append(pos)

        return new_entries, new_rejections
