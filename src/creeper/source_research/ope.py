"""Deterministic replay and IPS/DR off-policy evaluation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping, Sequence

from .decision_log import LoggedDecision
from .bandit import validate_probabilities


TargetPolicy = Callable[[LoggedDecision], Mapping[str, float]]
QEstimator = Callable[[LoggedDecision, str], float]


@dataclass(frozen=True)
class ReplayRecord:
    decision_id: str
    chosen_action_id: str
    logging_propensity: float
    target_propensity: float
    reward: float
    importance_weight: float
    replay_match: bool
    ips_contribution: float
    dr_contribution: float | None = None


@dataclass(frozen=True)
class PropensityReplay:
    decision_id: str
    logged_propensity: float
    reconstructed_propensity: float
    absolute_error: float
    matches: bool


def verify_propensity_replay(
    decision: LoggedDecision,
    *,
    target_policy: TargetPolicy,
    tolerance: float = 1e-12,
) -> PropensityReplay:
    target = dict(target_policy(decision))
    validate_probabilities(target)
    if set(target) != set(decision.candidate_action_ids):
        raise ValueError("target policy action set differs from logged candidates")
    reconstructed = float(target[decision.chosen_action_id])
    error = abs(reconstructed - decision.propensity)
    return PropensityReplay(
        decision_id=decision.decision_id,
        logged_propensity=decision.propensity,
        reconstructed_propensity=reconstructed,
        absolute_error=error,
        matches=error <= tolerance,
    )


@dataclass(frozen=True)
class OPEEstimate:
    count: int
    reward_count: int
    replay_match_rate: float
    ips: float
    self_normalized_ips: float
    doubly_robust: float | None
    effective_sample_size: float
    max_importance_weight: float
    records: tuple[ReplayRecord, ...]


def _ess(weights: Sequence[float]) -> float:
    total = sum(weights)
    denom = sum(value * value for value in weights)
    return 0.0 if denom <= 0 else (total * total) / denom


def evaluate_policy(
    decisions: Sequence[LoggedDecision],
    *,
    rewards_by_decision: Mapping[str, float],
    target_policy: TargetPolicy,
    q_estimator: QEstimator | None = None,
    clip_importance_weight: float | None = None,
) -> OPEEstimate:
    if clip_importance_weight is not None and clip_importance_weight <= 0:
        raise ValueError("clip_importance_weight must be positive")
    rows: list[ReplayRecord] = []
    weighted_sum = 0.0
    weight_sum = 0.0
    reward_count = 0
    matches = 0
    dr_sum = 0.0
    weights: list[float] = []

    for decision in decisions:
        if decision.propensity <= 0.0:
            raise ValueError("logged propensity must be positive")
        target = dict(target_policy(decision))
        validate_probabilities(target)
        if set(target) != set(decision.candidate_action_ids):
            raise ValueError("target policy action set differs from logged candidates")
        target_p = float(target.get(decision.chosen_action_id, 0.0))
        raw_weight = target_p / decision.propensity
        weight = (
            min(raw_weight, clip_importance_weight)
            if clip_importance_weight is not None
            else raw_weight
        )
        reward = float(rewards_by_decision.get(decision.decision_id, 0.0))
        if decision.decision_id in rewards_by_decision:
            reward_count += 1
        weighted_sum += weight * reward
        weight_sum += weight
        weights.append(weight)

        target_best = max(target, key=lambda key: (target[key], key))
        replay_match = target_best == decision.chosen_action_id
        matches += int(replay_match)

        dr_value: float | None = None
        if q_estimator is not None:
            q_target = sum(
                float(target[action]) * float(q_estimator(decision, action))
                for action in decision.candidate_action_ids
            )
            q_logged = float(q_estimator(decision, decision.chosen_action_id))
            dr_value = q_target + weight * (reward - q_logged)
            dr_sum += dr_value

        rows.append(
            ReplayRecord(
                decision_id=decision.decision_id,
                chosen_action_id=decision.chosen_action_id,
                logging_propensity=decision.propensity,
                target_propensity=target_p,
                reward=reward,
                importance_weight=weight,
                replay_match=replay_match,
                ips_contribution=weight * reward,
                dr_contribution=dr_value,
            )
        )

    count = len(decisions)
    ips = weighted_sum / count if count else 0.0
    snips = weighted_sum / weight_sum if weight_sum > 0 else 0.0
    dr = (dr_sum / count) if count and q_estimator is not None else None
    return OPEEstimate(
        count=count,
        reward_count=reward_count,
        replay_match_rate=(matches / count) if count else 0.0,
        ips=ips,
        self_normalized_ips=snips,
        doubly_robust=dr,
        effective_sample_size=_ess(weights),
        max_importance_weight=max(weights, default=0.0),
        records=tuple(rows),
    )


def final_reward_map(rewards: Sequence[object]) -> dict[str, float]:
    """Build decision-level FINAL reward without multi-scope double counting.

    L3 deliberately attributes one closed production outcome to several lineage
    scopes (artifact/query/program/root/pivot).  For OPE those are alternate
    views of the same outcome, not independent rewards.  When source/exposure
    identity is present we therefore count a closure once per decision and
    source exposure.  Legacy records without closure identity remain additive.
    """
    grouped: dict[tuple[str, str, str], float] = {}
    legacy: dict[str, float] = {}
    for reward in rewards:
        kind = getattr(reward, "kind", "")
        kind_value = getattr(kind, "value", kind)
        if str(kind_value) != "FINAL":
            continue
        if not bool(getattr(reward, "validation_closed", False)):
            continue
        decision_id = str(getattr(reward, "decision_id", ""))
        if not decision_id:
            continue
        amount = float(getattr(reward, "amount", 0.0))
        source_key = str(getattr(reward, "source_key", ""))
        exposure_id = str(getattr(reward, "exposure_id", ""))
        if source_key or exposure_id:
            key = (decision_id, source_key, exposure_id)
            previous = grouped.get(key)
            if previous is not None and abs(previous - amount) > 1e-12:
                raise ValueError("inconsistent FINAL amount across lineage scopes")
            grouped[key] = amount
        else:
            legacy[decision_id] = legacy.get(decision_id, 0.0) + amount

    result = dict(legacy)
    for (decision_id, _source_key, _exposure_id), amount in grouped.items():
        result[decision_id] = result.get(decision_id, 0.0) + amount
    return result


__all__ = [
    "OPEEstimate",
    "PropensityReplay",
    "QEstimator",
    "ReplayRecord",
    "TargetPolicy",
    "evaluate_policy",
    "final_reward_map",
    "verify_propensity_replay",
]
