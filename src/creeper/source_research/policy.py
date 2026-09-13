"""Hierarchical adaptive research policy for roots, queries, pivots and regions."""
from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Mapping, Sequence

from .bandit import (
    BanditStats,
    mix_probabilities,
    normalized_weights,
    softmax_probabilities,
    total_pulls,
    ucb_score,
    validate_probabilities,
)


class PolicyLevel(StrEnum):
    ROOT = "L0_ROOT"
    QUERY_FAMILY = "L1_QUERY_FAMILY"
    PIVOT_FAMILY = "L2_PIVOT_FAMILY"
    EXPLORATION_REGION = "L3_EXPLORATION_REGION"


class PolicyLifecycle(StrEnum):
    CANDIDATE = "CANDIDATE"
    OFFLINE_EVALUATED = "OFFLINE_EVALUATED"
    CANARY = "CANARY"
    ACTIVE = "ACTIVE"
    RETIRED = "RETIRED"


class ExplorationMode(StrEnum):
    EXPLOIT = "EXPLOIT"
    EXPLORE = "EXPLORE"


@dataclass(frozen=True)
class PolicyConfig:
    policy_id: str
    version: str
    snapshot_id: str
    schema_version: int = 1
    lifecycle: PolicyLifecycle | str = PolicyLifecycle.CANDIDATE
    baseline_version: str = ""
    exploration_fraction: float = 0.075
    ucb_exploration_strength: float = 1.0
    softmax_temperature: float = 1.0
    half_life_seconds: float = 7.0 * 24.0 * 3600.0
    prior_mean: float = 0.0
    prior_pulls: float = 1.0

    def __post_init__(self) -> None:
        if not self.policy_id or not self.version or not self.snapshot_id:
            raise ValueError("policy_id/version/snapshot_id are required")
        object.__setattr__(self, "lifecycle", PolicyLifecycle(self.lifecycle))
        if not 0.0 < self.exploration_fraction < 1.0:
            raise ValueError("exploration_fraction must be nonzero and below 1")
        if self.half_life_seconds <= 0 or self.softmax_temperature <= 0:
            raise ValueError("decay and temperature must be positive")


@dataclass(frozen=True)
class ActionCandidate:
    action_id: str
    level: PolicyLevel | str
    uncertainty: float = 0.0
    novelty: float = 0.0
    orthogonality: float = 0.0
    permanent_semantic_reject: bool = False
    features: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.action_id:
            raise ValueError("action_id is required")
        object.__setattr__(self, "level", PolicyLevel(self.level))
        object.__setattr__(self, "features", dict(self.features))


@dataclass(frozen=True)
class PolicyDecision:
    decision_id: str
    task_id: str
    policy_id: str
    policy_version: str
    policy_snapshot_id: str
    level: PolicyLevel
    context_features: Mapping[str, Any]
    context_hash: str
    candidate_action_ids: tuple[str, ...]
    chosen_action_id: str
    chosen_probability: float
    probabilities: Mapping[str, float]
    scores: Mapping[str, float]
    exploration_mode: ExplorationMode
    timestamp: float
    baseline_version: str = ""
    selection_nonce: str = ""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _stable_hex(*parts: Any) -> str:
    return hashlib.sha256(_canonical_json(parts).encode("utf-8")).hexdigest()


def _draw(probabilities: Mapping[str, float], *, seed: str) -> str:
    validate_probabilities(probabilities)
    rng = random.Random(int(_stable_hex(seed)[:16], 16))
    point = rng.random()
    cumulative = 0.0
    last = ""
    for action_id in sorted(probabilities):
        last = action_id
        cumulative += probabilities[action_id]
        if point <= cumulative:
            return action_id
    return last


class HierarchicalAdaptivePolicy:
    """Decayed UCB policy with explicit exploration mixture and replayable draws."""

    def __init__(self, config: PolicyConfig) -> None:
        self.config = config

    def snapshot_parameters(self) -> dict[str, Any]:
        return {
            "policy_id": self.config.policy_id,
            "version": self.config.version,
            "schema_version": self.config.schema_version,
            "lifecycle": self.config.lifecycle.value,
            "baseline_version": self.config.baseline_version,
            "exploration_fraction": self.config.exploration_fraction,
            "ucb_exploration_strength": self.config.ucb_exploration_strength,
            "softmax_temperature": self.config.softmax_temperature,
            "half_life_seconds": self.config.half_life_seconds,
            "prior_mean": self.config.prior_mean,
            "prior_pulls": self.config.prior_pulls,
        }

    @classmethod
    def from_snapshot(
        cls, *, snapshot_id: str, parameters: Mapping[str, Any]
    ) -> "HierarchicalAdaptivePolicy":
        return cls(
            PolicyConfig(
                policy_id=str(parameters["policy_id"]),
                version=str(parameters["version"]),
                snapshot_id=snapshot_id,
                schema_version=int(parameters.get("schema_version", 1)),
                lifecycle=str(parameters.get("lifecycle", "CANDIDATE")),
                baseline_version=str(parameters.get("baseline_version", "")),
                exploration_fraction=float(parameters.get("exploration_fraction", 0.075)),
                ucb_exploration_strength=float(
                    parameters.get("ucb_exploration_strength", 1.0)
                ),
                softmax_temperature=float(parameters.get("softmax_temperature", 1.0)),
                half_life_seconds=float(
                    parameters.get("half_life_seconds", 7.0 * 24.0 * 3600.0)
                ),
                prior_mean=float(parameters.get("prior_mean", 0.0)),
                prior_pulls=float(parameters.get("prior_pulls", 1.0)),
            )
        )

    def _distributions(
        self,
        *,
        level: PolicyLevel | str,
        candidates: Sequence[ActionCandidate],
        stats_by_arm: Mapping[str, BanditStats | object],
        now: float,
    ) -> tuple[dict[str, float], dict[str, float], dict[str, float], dict[str, float]]:
        level = PolicyLevel(level)
        allowed = [
            item
            for item in candidates
            if item.level is level and not item.permanent_semantic_reject
        ]
        if not allowed:
            raise ValueError(f"no eligible candidates for {level.value}")
        converted: dict[str, BanditStats] = {}
        for item in allowed:
            raw = stats_by_arm.get(item.action_id)
            converted[item.action_id] = (
                raw if isinstance(raw, BanditStats) else BanditStats.from_object(raw)
            ) if raw is not None else BanditStats(item.action_id)
        pulls = total_pulls(list(converted.values()))
        scores = {
            action_id: ucb_score(
                stats,
                total_pulls=pulls,
                now=now,
                half_life_seconds=self.config.half_life_seconds,
                exploration_strength=self.config.ucb_exploration_strength,
                prior_mean=self.config.prior_mean,
                prior_pulls=self.config.prior_pulls,
            )
            for action_id, stats in converted.items()
        }
        exploit = softmax_probabilities(
            scores, temperature=self.config.softmax_temperature
        )
        explore = normalized_weights({
            item.action_id: 1.0
            + max(0.0, item.uncertainty)
            + max(0.0, item.novelty)
            + max(0.0, item.orthogonality)
            for item in allowed
        })
        probabilities = mix_probabilities(
            exploit,
            explore,
            exploration_fraction=self.config.exploration_fraction,
        )
        validate_probabilities(probabilities)
        return exploit, explore, probabilities, scores

    def probabilities(
        self,
        *,
        level: PolicyLevel | str,
        candidates: Sequence[ActionCandidate],
        stats_by_arm: Mapping[str, BanditStats | object],
        now: float,
    ) -> tuple[dict[str, float], dict[str, float]]:
        _, _, probabilities, scores = self._distributions(
            level=level, candidates=candidates, stats_by_arm=stats_by_arm, now=now
        )
        return probabilities, scores

    def promote(self, target: PolicyLifecycle | str) -> "HierarchicalAdaptivePolicy":
        target = PolicyLifecycle(target)
        current = self.config.lifecycle
        chain = (
            PolicyLifecycle.CANDIDATE,
            PolicyLifecycle.OFFLINE_EVALUATED,
            PolicyLifecycle.CANARY,
            PolicyLifecycle.ACTIVE,
        )
        if target is PolicyLifecycle.RETIRED:
            if current is PolicyLifecycle.RETIRED:
                return self
            if current is not PolicyLifecycle.ACTIVE:
                raise ValueError("only ACTIVE policy can transition to RETIRED")
            return HierarchicalAdaptivePolicy(replace(self.config, lifecycle=target))
        if current is PolicyLifecycle.RETIRED:
            raise ValueError("retired policy is terminal")
        try:
            expected = chain[chain.index(current) + 1]
        except (ValueError, IndexError) as exc:
            raise ValueError(f"cannot promote from {current.value}") from exc
        if target is not expected:
            raise ValueError(f"invalid policy transition {current.value} -> {target.value}")
        return HierarchicalAdaptivePolicy(replace(self.config, lifecycle=target))

    def select(
        self,
        *,
        task_id: str,
        level: PolicyLevel | str,
        candidates: Sequence[ActionCandidate],
        stats_by_arm: Mapping[str, BanditStats | object],
        context_features: Mapping[str, Any],
        timestamp: float,
        selection_nonce: str = "",
    ) -> PolicyDecision:
        if not task_id:
            raise ValueError("task_id is required")
        level = PolicyLevel(level)
        context = dict(context_features)
        context_hash = _stable_hex(level.value, context)
        exploit, explore, probabilities, scores = self._distributions(
            level=level, candidates=candidates, stats_by_arm=stats_by_arm, now=timestamp
        )
        seed = _stable_hex(
            self.config.snapshot_id,
            task_id,
            level.value,
            context_hash,
            selection_nonce,
        )
        rng = random.Random(int(_stable_hex(seed, "mode")[:16], 16))
        mode = (
            ExplorationMode.EXPLORE
            if rng.random() < self.config.exploration_fraction
            else ExplorationMode.EXPLOIT
        )
        component = explore if mode is ExplorationMode.EXPLORE else exploit
        chosen = _draw(component, seed=_stable_hex(seed, "action", mode.value))
        decision_id = "decision:" + _stable_hex(
            task_id,
            self.config.policy_id,
            self.config.version,
            self.config.snapshot_id,
            level.value,
            context_hash,
            selection_nonce,
        )
        return PolicyDecision(
            decision_id=decision_id,
            task_id=task_id,
            policy_id=self.config.policy_id,
            policy_version=self.config.version,
            policy_snapshot_id=self.config.snapshot_id,
            level=level,
            context_features=context,
            context_hash=context_hash,
            candidate_action_ids=tuple(sorted(probabilities)),
            chosen_action_id=chosen,
            chosen_probability=float(probabilities[chosen]),
            probabilities=dict(probabilities),
            scores=dict(scores),
            exploration_mode=mode,
            timestamp=float(timestamp),
            baseline_version=self.config.baseline_version,
            selection_nonce=selection_nonce,
        )


__all__ = [
    "ActionCandidate",
    "ExplorationMode",
    "HierarchicalAdaptivePolicy",
    "PolicyConfig",
    "PolicyDecision",
    "PolicyLevel",
    "PolicyLifecycle",
]
