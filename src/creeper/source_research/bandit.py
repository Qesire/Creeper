"""Dependency-free adaptive bandit primitives for L7.

The policy layer operates on derived scheduling statistics only. Immutable facts
and FINAL reward authority remain in the L3 research kernel.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping, Sequence


@dataclass(frozen=True)
class BanditStats:
    arm_id: str
    pulls: int = 0
    proxy_reward: float = 0.0
    final_reward: float = 0.0
    decayed_reward: float = 0.0
    updated_at: float = 0.0

    @classmethod
    def from_object(cls, value: object) -> "BanditStats":
        return cls(
            arm_id=str(getattr(value, "arm_id")),
            pulls=max(0, int(getattr(value, "pulls", 0))),
            proxy_reward=float(getattr(value, "proxy_reward", 0.0)),
            final_reward=float(getattr(value, "final_reward", 0.0)),
            decayed_reward=float(getattr(value, "decayed_reward", 0.0)),
            updated_at=float(getattr(value, "updated_at", 0.0)),
        )

    @property
    def authoritative_reward(self) -> float:
        """Prefer FINAL reward; use proxy only while no FINAL exists."""
        return self.final_reward if self.final_reward != 0.0 else self.proxy_reward


def decay_factor(*, age_seconds: float, half_life_seconds: float) -> float:
    if half_life_seconds <= 0:
        raise ValueError("half_life_seconds must be positive")
    if age_seconds <= 0:
        return 1.0
    return math.exp(-math.log(2.0) * age_seconds / half_life_seconds)


def decayed_reward_view(
    stats: BanditStats,
    *,
    now: float,
    half_life_seconds: float,
) -> float:
    """Return a decayed scheduling view without mutating durable facts."""
    age = max(0.0, float(now) - float(stats.updated_at))
    base = stats.decayed_reward if stats.decayed_reward != 0.0 else stats.authoritative_reward
    return base * decay_factor(age_seconds=age, half_life_seconds=half_life_seconds)


def ucb_score(
    stats: BanditStats,
    *,
    total_pulls: int,
    now: float,
    half_life_seconds: float,
    exploration_strength: float = 1.0,
    prior_mean: float = 0.0,
    prior_pulls: float = 1.0,
) -> float:
    if exploration_strength < 0:
        raise ValueError("exploration_strength must be non-negative")
    if prior_pulls <= 0:
        raise ValueError("prior_pulls must be positive")
    pulls = max(0, int(stats.pulls))
    reward = decayed_reward_view(stats, now=now, half_life_seconds=half_life_seconds)
    mean = (reward + prior_mean * prior_pulls) / (pulls + prior_pulls)
    exploration = exploration_strength * math.sqrt(
        math.log(max(2.0, float(total_pulls) + 1.0)) / (pulls + prior_pulls)
    )
    return mean + exploration


def softmax_probabilities(
    scores: Mapping[str, float], *, temperature: float = 1.0
) -> dict[str, float]:
    if not scores:
        raise ValueError("scores must be non-empty")
    if temperature <= 0:
        raise ValueError("temperature must be positive")
    maximum = max(float(v) for v in scores.values())
    weights = {
        key: math.exp((float(value) - maximum) / temperature)
        for key, value in scores.items()
    }
    total = sum(weights.values())
    if not math.isfinite(total) or total <= 0:
        p = 1.0 / len(weights)
        return {key: p for key in weights}
    return {key: value / total for key, value in weights.items()}


def normalized_weights(weights: Mapping[str, float]) -> dict[str, float]:
    if not weights:
        raise ValueError("weights must be non-empty")
    clean = {key: max(0.0, float(value)) for key, value in weights.items()}
    total = sum(clean.values())
    if total <= 0:
        p = 1.0 / len(clean)
        return {key: p for key in clean}
    return {key: value / total for key, value in clean.items()}


def mix_probabilities(
    exploit: Mapping[str, float],
    explore: Mapping[str, float],
    *,
    exploration_fraction: float,
) -> dict[str, float]:
    if set(exploit) != set(explore):
        raise ValueError("exploit/explore arms must match")
    if not 0.0 <= exploration_fraction <= 1.0:
        raise ValueError("exploration_fraction must be in [0, 1]")
    exploit_n = normalized_weights(exploit)
    explore_n = normalized_weights(explore)
    eps = float(exploration_fraction)
    result = {
        key: (1.0 - eps) * exploit_n[key] + eps * explore_n[key]
        for key in exploit_n
    }
    # Normalize again to remove floating point drift.
    return normalized_weights(result)


def validate_probabilities(probabilities: Mapping[str, float], *, tol: float = 1e-9) -> None:
    if not probabilities:
        raise ValueError("probabilities must be non-empty")
    if any((not math.isfinite(v)) or v < 0.0 or v > 1.0 for v in probabilities.values()):
        raise ValueError("invalid probability")
    if abs(sum(probabilities.values()) - 1.0) > tol:
        raise ValueError("probabilities must sum to one")


def total_pulls(stats: Sequence[BanditStats]) -> int:
    return sum(max(0, int(item.pulls)) for item in stats)


__all__ = [
    "BanditStats",
    "decay_factor",
    "decayed_reward_view",
    "mix_probabilities",
    "normalized_weights",
    "softmax_probabilities",
    "total_pulls",
    "ucb_score",
    "validate_probabilities",
]
