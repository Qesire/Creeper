"""Bounded, durable admission for platform-by-year traversals.

Platform enumeration is a separate production lane.  Admission accepts only
an authority-scoped observation that can be traced back to a source and a
reservoir, then writes the idempotent task identity to the ControlStore.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from creeper.authority.normalizer import normalize_official
from creeper.evidence.platform_harvest import (
    PlatformYearHarvestTask,
    platform_year_exposure_id,
)


@dataclass(frozen=True)
class PlatformYearObservation:
    """A durable source/index observation eligible for platform expansion."""

    provider: str
    subject: str
    target_year: int
    request_template_hash: str
    policy_version: str
    source_key: str
    reservoir_id: str
    authority_digest: str
    exposure_id: str | None = None
    origin_decision: str = "eligible-source-index"

    def __post_init__(self) -> None:
        subject = normalize_official(self.subject)
        if subject is None:
            raise ValueError("platform observation subject must be a valid hostname")
        object.__setattr__(self, "subject", subject)
        if (
            isinstance(self.target_year, bool)
            or not isinstance(self.target_year, int)
            or not 1996 <= self.target_year <= 2001
        ):
            raise ValueError(
                "platform observation year must be an integer within 1996-2001"
            )
        required = (
            ("provider", self.provider),
            ("request_template_hash", self.request_template_hash),
            ("policy_version", self.policy_version),
            ("source_key", self.source_key),
            ("reservoir_id", self.reservoir_id),
            ("authority_digest", self.authority_digest),
            ("origin_decision", self.origin_decision),
        )
        for name, value in required:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"platform observation {name} is required")
        if self.exposure_id is not None and (
            not isinstance(self.exposure_id, str) or not self.exposure_id.strip()
        ):
            raise ValueError("platform observation exposure_id must be non-empty")

    @property
    def durable_exposure_id(self) -> str:
        return self.exposure_id or platform_year_exposure_id(
            provider=self.provider,
            subject=self.subject,
            target_year=self.target_year,
            request_template_hash=self.request_template_hash,
            policy_version=self.policy_version,
            source_key=self.source_key,
            reservoir_id=self.reservoir_id,
            authority_digest=self.authority_digest,
        )


@dataclass(frozen=True)
class PlatformYearAdmissionPolicy:
    """Independent bounded budget for platform task creation."""

    max_tasks: int = 1

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_tasks, bool)
            or not isinstance(self.max_tasks, int)
            or self.max_tasks < 1
        ):
            raise ValueError(
                "platform admission max_tasks must be a positive integer"
            )


@dataclass(frozen=True)
class PlatformAdmissionReport:
    admitted: int = 0
    idempotent: int = 0
    blocked: int = 0
    tasks: tuple[PlatformYearHarvestTask, ...] = ()


class PlatformYearAdmission:
    """Turn bounded source/index observations into durable platform tasks."""

    def __init__(self, control_store, *, policy: PlatformYearAdmissionPolicy | None = None):
        self.control_store = control_store
        self.policy = policy or PlatformYearAdmissionPolicy()

    def admit(
        self,
        observations: Iterable[PlatformYearObservation],
    ) -> PlatformAdmissionReport:
        admitted = idempotent = blocked = 0
        tasks: list[PlatformYearHarvestTask] = []
        for raw_observation in observations:
            observation = (
                raw_observation
                if isinstance(raw_observation, PlatformYearObservation)
                else PlatformYearObservation(**raw_observation)
            )
            harvest_id = self.control_store.platform_year_harvest_identity(
                provider=observation.provider,
                subject=observation.subject,
                target_year=observation.target_year,
                request_template_hash=observation.request_template_hash,
                policy_version=observation.policy_version,
                source_key=observation.source_key,
                reservoir_id=observation.reservoir_id,
                exposure_id=observation.durable_exposure_id,
                authority_digest=observation.authority_digest,
            )
            existing = self.control_store.get_platform_year_harvest(harvest_id)
            if existing is not None:
                idempotent += 1
                tasks.append(existing)
                continue
            if admitted >= self.policy.max_tasks:
                blocked += 1
                continue
            task = self.control_store.enqueue_platform_year_harvest(
                provider=observation.provider,
                subject=observation.subject,
                target_year=observation.target_year,
                request_template_hash=observation.request_template_hash,
                policy_version=observation.policy_version,
                source_key=observation.source_key,
                reservoir_id=observation.reservoir_id,
                exposure_id=observation.durable_exposure_id,
                authority_digest=observation.authority_digest,
                origin_decision=observation.origin_decision,
            )
            admitted += 1
            tasks.append(task)
        return PlatformAdmissionReport(
            admitted=admitted,
            idempotent=idempotent,
            blocked=blocked,
            tasks=tuple(tasks),
        )


__all__ = [
    "PlatformAdmissionReport",
    "PlatformYearAdmission",
    "PlatformYearAdmissionPolicy",
    "PlatformYearObservation",
]
