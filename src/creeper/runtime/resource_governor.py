"""Resource state machine for bounded local jobs."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping

from creeper.scheduler.credits import ResourceCredits


class GovernorState(StrEnum):
    NORMAL = "normal"
    THROTTLED = "throttled"
    DRAIN_ONLY = "drain_only"
    EMERGENCY_STOP = "emergency_stop"


@dataclass(frozen=True)
class ResourceSample:
    rss_bytes: int
    disk_free_bytes: int
    cpu_percent: float = 0.0
    provider_pressure: float = 0.0


class ResourceGovernor:
    def __init__(
        self,
        *,
        rss_throttle_bytes: int,
        rss_stop_bytes: int,
        disk_throttle_bytes: int,
        disk_stop_bytes: int,
        provider_drain_pressure: float = 1.0,
        provider_throttle_pressure: float = 0.8,
    ):
        if not (0 <= disk_stop_bytes <= disk_throttle_bytes):
            raise ValueError("disk thresholds must be stop <= throttle")
        if not (0 <= rss_throttle_bytes <= rss_stop_bytes):
            raise ValueError("RSS thresholds must be throttle <= stop")
        self.rss_throttle_bytes = rss_throttle_bytes
        self.rss_stop_bytes = rss_stop_bytes
        self.disk_throttle_bytes = disk_throttle_bytes
        self.disk_stop_bytes = disk_stop_bytes
        self.provider_drain_pressure = provider_drain_pressure
        self.provider_throttle_pressure = provider_throttle_pressure

    def evaluate(self, sample: ResourceSample) -> GovernorState:
        if (
            sample.rss_bytes >= self.rss_stop_bytes
            or sample.disk_free_bytes <= self.disk_stop_bytes
        ):
            return GovernorState.EMERGENCY_STOP
        if sample.provider_pressure >= self.provider_drain_pressure:
            return GovernorState.DRAIN_ONLY
        if (
            sample.rss_bytes >= self.rss_throttle_bytes
            or sample.disk_free_bytes <= self.disk_throttle_bytes
            or sample.provider_pressure >= self.provider_throttle_pressure
        ):
            return GovernorState.THROTTLED
        return GovernorState.NORMAL

    def credits(
        self,
        sample: ResourceSample,
        capacities: Mapping[str, int],
    ) -> ResourceCredits:
        """Return stage credits for the state represented by ``sample``."""

        state = self.evaluate(sample)
        values = dict(capacities)
        if any(not isinstance(value, int) or value < 0 for value in values.values()):
            raise ValueError("resource capacities must be non-negative integers")

        resource_names = {"source_fetch", "parse", "commit"}
        evidence_capacities = {
            provider: capacity
            for provider, capacity in values.items()
            if provider not in resource_names
        }

        if state is GovernorState.EMERGENCY_STOP:
            return ResourceCredits(0, 0, {provider: 0 for provider in evidence_capacities}, 0)
        if state is GovernorState.DRAIN_ONLY:
            return ResourceCredits(
                0,
                values.get("parse", 0),
                {provider: 0 for provider in evidence_capacities},
                values.get("commit", 0),
            )
        if state is GovernorState.THROTTLED:
            return ResourceCredits(
                self._throttled(values.get("source_fetch", 0)),
                self._throttled(values.get("parse", 0)),
                {
                    provider: self._throttled(capacity)
                    for provider, capacity in evidence_capacities.items()
                },
                self._throttled(values.get("commit", 0)),
            )
        return ResourceCredits(
            values.get("source_fetch", 0),
            values.get("parse", 0),
            evidence_capacities,
            values.get("commit", 0),
        )

    @staticmethod
    def _throttled(capacity: int) -> int:
        return (capacity + 1) // 2
