"""Drift helpers over derived scheduling views; immutable facts are untouched."""
from __future__ import annotations

from dataclasses import dataclass
from statistics import fmean
from typing import Sequence

from .bandit import BanditStats, decayed_reward_view


@dataclass(frozen=True)
class DriftSignal:
    detected: bool
    reference_mean: float
    recent_mean: float
    absolute_delta: float
    relative_delta: float
    threshold: float


def detect_mean_drift(
    reference: Sequence[float],
    recent: Sequence[float],
    *,
    relative_threshold: float = 0.5,
    minimum_absolute_delta: float = 0.0,
) -> DriftSignal:
    if relative_threshold < 0 or minimum_absolute_delta < 0:
        raise ValueError("drift thresholds must be non-negative")
    ref = fmean(reference) if reference else 0.0
    cur = fmean(recent) if recent else 0.0
    absolute = abs(cur - ref)
    scale = max(abs(ref), 1e-12)
    relative = absolute / scale
    return DriftSignal(
        detected=(
            bool(reference)
            and bool(recent)
            and absolute >= minimum_absolute_delta
            and relative >= relative_threshold
        ),
        reference_mean=ref,
        recent_mean=cur,
        absolute_delta=absolute,
        relative_delta=relative,
        threshold=relative_threshold,
    )


def decayed_stats_view(
    stats: Sequence[BanditStats | object],
    *,
    now: float,
    half_life_seconds: float,
) -> tuple[BanditStats, ...]:
    result: list[BanditStats] = []
    for item in stats:
        source = item if isinstance(item, BanditStats) else BanditStats.from_object(item)
        result.append(
            BanditStats(
                arm_id=source.arm_id,
                pulls=source.pulls,
                proxy_reward=source.proxy_reward,
                final_reward=source.final_reward,
                decayed_reward=decayed_reward_view(
                    source, now=now, half_life_seconds=half_life_seconds
                ),
                updated_at=source.updated_at,
            )
        )
    return tuple(result)


__all__ = ["DriftSignal", "decayed_stats_view", "detect_mean_drift"]
