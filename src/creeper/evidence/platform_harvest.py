"""Durable, resumable platform-by-year evidence harvesting.

The bounded domain probe in providers is intentionally not a completeness
mechanism.  This module defines a separate control-plane work type whose
continuation cursor, ownership lease, and cumulative accounting survive process
restart.  One execution consumes at most one provider page.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Callable, Mapping, Protocol

from creeper.authority.normalizer import normalize_official
from creeper.evidence.policies import EvidenceCapsule

if TYPE_CHECKING:
    from creeper.storage.control_store import ControlStore
    from creeper.storage.evidence_store import EvidenceStore


class PlatformHarvestState(StrEnum):
    READY = "READY"
    RUNNING = "RUNNING"
    PARTIAL = "PARTIAL"
    RETRYABLE = "RETRYABLE"
    COMPLETE = "COMPLETE"
    FAILED_INVALID = "FAILED_INVALID"


CLAIMABLE_PLATFORM_HARVEST_STATES = frozenset(
    {
        PlatformHarvestState.READY,
        PlatformHarvestState.PARTIAL,
        PlatformHarvestState.RETRYABLE,
    }
)


def platform_year_harvest_id(
    *,
    provider: str,
    subject: str,
    target_year: int,
    request_template_hash: str,
    policy_version: str,
) -> str:
    normalized = normalize_official(subject)
    if normalized is None:
        raise ValueError("platform harvest subject must be a valid hostname")
    if not 1996 <= int(target_year) <= 2001:
        raise ValueError("platform harvest year must be within 1996-2001")
    if not provider.strip() or not request_template_hash.strip() or not policy_version.strip():
        raise ValueError("platform harvest identity fields must be non-empty")
    payload = json.dumps(
        {
            "provider": provider,
            "subject": normalized,
            "target_year": int(target_year),
            "request_template_hash": request_template_hash,
            "policy_version": policy_version,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "platform-year:" + hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class PlatformYearHarvestTask:
    harvest_id: str
    provider: str
    subject: str
    target_year: int
    request_template_hash: str
    policy_version: str
    resume_key: str | None = None
    page_number: int = 0
    state: PlatformHarvestState = PlatformHarvestState.READY
    attempt: int = 0
    retry_at: float | None = None
    claimed_by: str | None = None
    lease_expires_at: float | None = None
    rows_seen: int = 0
    unique_host_years_seen: int = 0
    requests: int = 0
    bytes: int = 0
    elapsed_seconds: float = 0.0
    baseline_external_host_years: int = 0
    final_eed: float = 0.0
    created_at: float = 0.0
    updated_at: float = 0.0
    completed_at: float | None = None

    def __post_init__(self) -> None:
        normalized = normalize_official(self.subject)
        if normalized is None:
            raise ValueError("platform harvest subject must be a valid hostname")
        object.__setattr__(self, "subject", normalized)
        object.__setattr__(self, "state", PlatformHarvestState(self.state))
        if not 1996 <= int(self.target_year) <= 2001:
            raise ValueError("platform harvest year must be within 1996-2001")
        expected = platform_year_harvest_id(
            provider=self.provider,
            subject=normalized,
            target_year=self.target_year,
            request_template_hash=self.request_template_hash,
            policy_version=self.policy_version,
        )
        if self.harvest_id != expected:
            raise ValueError("platform harvest id does not match durable identity")
        if self.page_number < 0 or self.attempt < 0:
            raise ValueError("platform harvest counters must be non-negative")
        if any(
            value < 0
            for value in (
                self.rows_seen,
                self.unique_host_years_seen,
                self.requests,
                self.bytes,
                self.elapsed_seconds,
                self.baseline_external_host_years,
                self.final_eed,
            )
        ):
            raise ValueError("platform harvest accounting must be non-negative")


@dataclass(frozen=True)
class PlatformYearHarvestResult:
    harvest_id: str
    provider: str
    subject: str
    target_year: int
    request_template_hash: str
    policy_version: str
    resume_key_used: str | None
    state: PlatformHarvestState
    next_resume_key: str | None = None
    capsules: tuple[EvidenceCapsule, ...] = ()
    rows_seen: int = 0
    requests: int = 0
    bytes: int = 0
    elapsed_seconds: float = 0.0
    exhaustive: bool = False
    error: str | None = None

    def __post_init__(self) -> None:
        normalized = normalize_official(self.subject)
        if normalized is None:
            raise ValueError("platform harvest result subject must be valid")
        object.__setattr__(self, "subject", normalized)
        state = PlatformHarvestState(self.state)
        object.__setattr__(self, "state", state)
        if state not in {
            PlatformHarvestState.PARTIAL,
            PlatformHarvestState.RETRYABLE,
            PlatformHarvestState.COMPLETE,
            PlatformHarvestState.FAILED_INVALID,
        }:
            raise ValueError("provider result must be a page-completion state")
        if state is PlatformHarvestState.PARTIAL and not self.next_resume_key:
            raise ValueError("partial platform harvest must preserve a continuation")
        if state is PlatformHarvestState.COMPLETE:
            if self.next_resume_key is not None or not self.exhaustive:
                raise ValueError(
                    "complete platform harvest requires explicit provider exhaustion"
                )
        elif self.exhaustive:
            raise ValueError("only COMPLETE may claim provider exhaustion")
        if any(
            value < 0
            for value in (self.rows_seen, self.requests, self.bytes, self.elapsed_seconds)
        ):
            raise ValueError("platform harvest result accounting must be non-negative")
        seen: set[tuple[str, int]] = set()
        for capsule in self.capsules:
            if capsule.provider != self.provider:
                raise ValueError("platform harvest capsule provider mismatch")
            if capsule.policy_version != self.policy_version:
                raise ValueError("platform harvest capsule policy mismatch")
            if int(capsule.year) != int(self.target_year):
                raise ValueError("platform harvest capsule must match target year")
            hostname = normalize_official(capsule.hostname)
            if hostname is None or not (
                hostname == normalized or hostname.endswith("." + normalized)
            ):
                raise ValueError("platform harvest capsule is outside subject domain")
            identity = (hostname, int(capsule.year))
            if identity in seen:
                raise ValueError("platform page keeps at most one capsule per host-year")
            seen.add(identity)


class AsyncPlatformYearHarvestProvider(Protocol):
    async def harvest_platform_year(
        self,
        task: PlatformYearHarvestTask,
    ) -> PlatformYearHarvestResult:
        """Consume one bounded page for one provider/subject/year work item."""


@dataclass(frozen=True)
class PlatformYearHarvestWorkerReport:
    claimed: int = 0
    pages_committed: int = 0
    complete: int = 0
    partial: int = 0
    retryable: int = 0
    failed_invalid: int = 0
    inserted_capsules: int = 0
    rows_seen: int = 0
    provider_requests: int = 0
    bytes: int = 0


class PlatformYearHarvestWorker:
    """Execute a separately-budgeted resumable platform-year lane.

    Evidence is persisted before the control cursor advances.  A crash in
    between therefore replays the old page after lease expiry; EvidenceStore's
    canonical host-year identity makes that replay safe and prevents evidence
    loss.
    """

    def __init__(
        self,
        *,
        control_store: "ControlStore",
        evidence_store: "EvidenceStore",
        providers: Mapping[str, AsyncPlatformYearHarvestProvider],
        owner: str,
        claim_batch_size: int = 1,
        lease_seconds: float = 300.0,
        retry_base_seconds: float = 30.0,
        retry_max_seconds: float = 3_600.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not owner:
            raise ValueError("platform harvest owner is required")
        if claim_batch_size < 1:
            raise ValueError("claim_batch_size must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if retry_base_seconds < 0 or retry_max_seconds < retry_base_seconds:
            raise ValueError("invalid retry policy")
        self.control_store = control_store
        self.evidence_store = evidence_store
        self.providers = dict(providers)
        self.owner = owner
        self.claim_batch_size = int(claim_batch_size)
        self.lease_seconds = float(lease_seconds)
        self.retry_base_seconds = float(retry_base_seconds)
        self.retry_max_seconds = float(retry_max_seconds)
        self.clock = clock

    def _retry_at(self, attempt: int) -> float:
        exponent = max(0, int(attempt) - 1)
        delay = min(self.retry_max_seconds, self.retry_base_seconds * (2**exponent))
        return float(self.clock()) + float(delay)

    async def run_once(self) -> PlatformYearHarvestWorkerReport:
        tasks = self.control_store.claim_platform_year_harvests(
            owner=self.owner,
            limit=self.claim_batch_size,
            lease_seconds=self.lease_seconds,
            providers=tuple(self.providers),
        )
        if not tasks:
            return PlatformYearHarvestWorkerReport()

        pages_committed = complete = partial = retryable = failed_invalid = 0
        inserted_capsules = rows_seen = provider_requests = bytes_seen = 0
        for task in tasks:
            provider = self.providers.get(task.provider)
            if provider is None:
                result = PlatformYearHarvestResult(
                    harvest_id=task.harvest_id,
                    provider=task.provider,
                    subject=task.subject,
                    target_year=task.target_year,
                    request_template_hash=task.request_template_hash,
                    policy_version=task.policy_version,
                    resume_key_used=task.resume_key,
                    state=PlatformHarvestState.RETRYABLE,
                    error=f"unknown platform provider: {task.provider}",
                )
            else:
                try:
                    result = await provider.harvest_platform_year(task)
                except Exception as exc:
                    result = PlatformYearHarvestResult(
                        harvest_id=task.harvest_id,
                        provider=task.provider,
                        subject=task.subject,
                        target_year=task.target_year,
                        request_template_hash=task.request_template_hash,
                        policy_version=task.policy_version,
                        resume_key_used=task.resume_key,
                        state=PlatformHarvestState.RETRYABLE,
                        error=str(exc) or type(exc).__name__,
                    )

            # Never advance the durable resume cursor before proof is durable.
            inserted_capsules += self.evidence_store.put_many(result.capsules)
            retry_at = (
                self._retry_at(task.attempt)
                if result.state is PlatformHarvestState.RETRYABLE
                else None
            )
            self.control_store.finish_platform_year_harvest_page(
                result,
                owner=self.owner,
                retry_at=retry_at,
            )

            if result.state in {
                PlatformHarvestState.PARTIAL,
                PlatformHarvestState.COMPLETE,
            }:
                pages_committed += 1
            if result.state is PlatformHarvestState.COMPLETE:
                complete += 1
            elif result.state is PlatformHarvestState.PARTIAL:
                partial += 1
            elif result.state is PlatformHarvestState.RETRYABLE:
                retryable += 1
            elif result.state is PlatformHarvestState.FAILED_INVALID:
                failed_invalid += 1
            rows_seen += result.rows_seen
            provider_requests += result.requests
            bytes_seen += result.bytes

        return PlatformYearHarvestWorkerReport(
            claimed=len(tasks),
            pages_committed=pages_committed,
            complete=complete,
            partial=partial,
            retryable=retryable,
            failed_invalid=failed_invalid,
            inserted_capsules=inserted_capsules,
            rows_seen=rows_seen,
            provider_requests=provider_requests,
            bytes=bytes_seen,
        )
