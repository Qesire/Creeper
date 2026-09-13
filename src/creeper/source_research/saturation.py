"""Operational cooling/saturation with explicit 429 and revisit-TTL semantics."""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from enum import StrEnum
from typing import Deque


class SaturationState(StrEnum):
    ACTIVE = "ACTIVE"
    COOLING = "COOLING"
    SATURATED = "SATURATED"


@dataclass(frozen=True)
class FamilyObservation:
    timestamp: float
    final_reward: float = 0.0
    duplicate_ratio: float = 0.0
    overlap_ratio: float = 0.0
    status_code: int | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("duplicate_ratio", self.duplicate_ratio),
            ("overlap_ratio", self.overlap_ratio),
        ):
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0,1]")


@dataclass(frozen=True)
class FamilyStatus:
    state: SaturationState
    until: float | None
    reason: str
    samples: int


class SaturationTracker:
    def __init__(
        self,
        *,
        window_size: int = 20,
        minimum_samples: int = 5,
        low_final_threshold: float = 0.05,
        duplicate_threshold: float = 0.8,
        overlap_threshold: float = 0.8,
        saturation_ttl_seconds: float = 6.0 * 3600.0,
        rate_limit_cooldown_seconds: float = 15.0 * 60.0,
    ) -> None:
        if window_size < 1 or minimum_samples < 1 or minimum_samples > window_size:
            raise ValueError("invalid window/minimum_samples")
        if saturation_ttl_seconds <= 0 or rate_limit_cooldown_seconds <= 0:
            raise ValueError("TTLs must be positive")
        self.window_size = window_size
        self.minimum_samples = minimum_samples
        self.low_final_threshold = low_final_threshold
        self.duplicate_threshold = duplicate_threshold
        self.overlap_threshold = overlap_threshold
        self.saturation_ttl_seconds = saturation_ttl_seconds
        self.rate_limit_cooldown_seconds = rate_limit_cooldown_seconds
        self._history: dict[str, Deque[FamilyObservation]] = defaultdict(
            lambda: deque(maxlen=self.window_size)
        )
        self._until: dict[str, float] = {}
        self._state: dict[str, SaturationState] = {}
        self._reason: dict[str, str] = {}

    def observe(self, family_id: str, observation: FamilyObservation) -> FamilyStatus:
        if not family_id:
            raise ValueError("family_id is required")
        history = self._history[family_id]
        history.append(observation)

        # 429 is operational backpressure only; it never creates or clears
        # semantic saturation.  An already-saturated family keeps its longer
        # saturation TTL instead of being accidentally shortened by a 429.
        if observation.status_code == 429:
            current = self.status(family_id, now=observation.timestamp)
            if current.state is SaturationState.SATURATED:
                return current
            self._state[family_id] = SaturationState.COOLING
            self._until[family_id] = observation.timestamp + self.rate_limit_cooldown_seconds
            self._reason[family_id] = "RATE_LIMIT_429"
            return self.status(family_id, now=observation.timestamp)

        usable = [item for item in history if item.status_code != 429]
        if len(usable) >= self.minimum_samples:
            avg_final = sum(item.final_reward for item in usable) / len(usable)
            avg_dup = sum(item.duplicate_ratio for item in usable) / len(usable)
            avg_overlap = sum(item.overlap_ratio for item in usable) / len(usable)
            if (
                avg_final <= self.low_final_threshold
                and avg_dup >= self.duplicate_threshold
                and avg_overlap >= self.overlap_threshold
            ):
                self._state[family_id] = SaturationState.SATURATED
                self._until[family_id] = observation.timestamp + self.saturation_ttl_seconds
                self._reason[family_id] = "LOW_FINAL_HIGH_DUPLICATE_OVERLAP"
        return self.status(family_id, now=observation.timestamp)

    def status(self, family_id: str, *, now: float) -> FamilyStatus:
        state = self._state.get(family_id, SaturationState.ACTIVE)
        until = self._until.get(family_id)
        reason = self._reason.get(family_id, "")
        if until is not None and now >= until:
            # TTL expiry makes the family eligible for a deterministic revisit.
            state = SaturationState.ACTIVE
            until = None
            reason = "REVISIT_TTL_EXPIRED"
            self._state[family_id] = state
            self._until.pop(family_id, None)
            self._reason[family_id] = reason
        return FamilyStatus(
            state=state,
            until=until,
            reason=reason,
            samples=len(self._history.get(family_id, ())),
        )

    def eligible(self, family_id: str, *, now: float) -> bool:
        return self.status(family_id, now=now).state is SaturationState.ACTIVE


__all__ = [
    "FamilyObservation",
    "FamilyStatus",
    "SaturationState",
    "SaturationTracker",
]
