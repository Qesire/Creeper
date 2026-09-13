"""Baseline rebase planning that recomputes derived novelty without deleting facts."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, Iterable, TypeVar


T = TypeVar("T")


@dataclass(frozen=True)
class RebasePlan:
    previous_baseline_version: str
    new_baseline_version: str
    stale_policy: bool
    rebuild_required: bool
    preserve_facts: bool = True


@dataclass(frozen=True)
class RebasedFact(Generic[T]):
    fact: T
    novel: bool
    derived_reward: float


def plan_rebase(*, policy_baseline_version: str, new_baseline_version: str) -> RebasePlan:
    if not new_baseline_version:
        raise ValueError("new_baseline_version is required")
    stale = bool(policy_baseline_version) and policy_baseline_version != new_baseline_version
    return RebasePlan(
        previous_baseline_version=policy_baseline_version,
        new_baseline_version=new_baseline_version,
        stale_policy=stale,
        rebuild_required=stale,
        preserve_facts=True,
    )


def recompute_novelty_view(
    facts: Iterable[T],
    *,
    is_novel: Callable[[T], bool],
    reward_of: Callable[[T], float],
) -> tuple[RebasedFact[T], ...]:
    """Return a new derived view while retaining original fact objects verbatim."""
    result: list[RebasedFact[T]] = []
    for fact in facts:
        novel = bool(is_novel(fact))
        result.append(
            RebasedFact(
                fact=fact,
                novel=novel,
                derived_reward=float(reward_of(fact)) if novel else 0.0,
            )
        )
    return tuple(result)


__all__ = ["RebasePlan", "RebasedFact", "plan_rebase", "recompute_novelty_view"]
