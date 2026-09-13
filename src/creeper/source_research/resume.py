"""Restart and recovery helpers for the durable research frontier."""
from __future__ import annotations

from dataclasses import dataclass

from .models import FrontierTask
from .registry import ResearchRegistry


@dataclass(frozen=True)
class ResumeReport:
    reclaimed_leases: int
    ready_tasks: tuple[FrontierTask, ...]
    rebuilt_policy_version: str = ""


class ResearchResumeManager:
    def __init__(self, registry: ResearchRegistry) -> None:
        self.registry = registry

    def recover(
        self,
        *,
        now: float | None = None,
        policy_version: str = "",
        expected_schema_version: int | None = None,
    ) -> ResumeReport:
        reclaimed = self.registry.reclaim_stale_leases(now=now)
        rebuilt = ""
        if policy_version and expected_schema_version is not None:
            stats = self.registry.arm_stats(policy_version=policy_version)
            mismatch = any(
                item.schema_version != expected_schema_version for item in stats
            )
            missing = not stats and self.registry.has_decisions(
                policy_version=policy_version
            )
            if mismatch or missing:
                self.registry.rebuild_arm_stats(
                    policy_version=policy_version,
                    schema_version=expected_schema_version,
                )
                rebuilt = policy_version
        return ResumeReport(
            reclaimed_leases=reclaimed,
            ready_tasks=self.registry.ready_frontier(now=now),
            rebuilt_policy_version=rebuilt,
        )


__all__ = ["ResearchResumeManager", "ResumeReport"]
