"""Resource state machine for bounded local jobs."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


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
