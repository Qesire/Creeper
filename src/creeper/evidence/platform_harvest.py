"""Durable, resumable platform-by-year evidence harvesting.

The bounded domain probe in providers is intentionally not a completeness
mechanism.  This module defines a separate control-plane work type whose
continuation cursor, ownership lease, and cumulative accounting survive process
restart.  One execution consumes at most one provider page.
"""

from __future__ import annotations

import hashlib
import json
import math
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
    source_key: str = "",
    reservoir_id: str = "",
    exposure_id: str = "",
    authority_digest: str = "",
) -> str:
    normalized = normalize_official(subject)
    if normalized is None:
        raise ValueError("platform harvest subject must be a valid hostname")
    if (
        isinstance(target_year, bool)
        or not isinstance(target_year, int)
        or not 1996 <= target_year <= 2001
    ):
        raise ValueError("platform harvest year must be an integer within 1996-2001")
    if any(
        not isinstance(value, str) or not value.strip()
        for value in (provider, request_template_hash, policy_version)
    ):
        raise ValueError("platform harvest identity fields must be non-empty")
    identity = {
        "provider": provider,
        "subject": normalized,
        "target_year": target_year,
        "request_template_hash": request_template_hash,
        "policy_version": policy_version,
    }
    lineage = {
        "source_key": source_key,
        "reservoir_id": reservoir_id,
        "exposure_id": exposure_id,
        "authority_digest": authority_digest,
    }
    if any(lineage.values()):
        if not all(value.strip() for value in lineage.values()):
            raise ValueError("platform task lineage must be complete")
        identity.update(lineage)
    payload = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "platform-year:" + hashlib.sha256(payload).hexdigest()


def platform_year_exposure_id(
    *,
    provider: str,
    subject: str,
    target_year: int,
    request_template_hash: str,
    policy_version: str,
    source_key: str,
    reservoir_id: str,
    authority_digest: str,
) -> str:
    """Return a deterministic exposure identity for an admitted task."""

    task_id = platform_year_harvest_id(
        provider=provider,
        subject=subject,
        target_year=target_year,
        request_template_hash=request_template_hash,
        policy_version=policy_version,
        source_key=source_key,
        reservoir_id=reservoir_id,
        exposure_id="pending",
        authority_digest=authority_digest,
    )
    return "platform-exposure:" + hashlib.sha256(task_id.encode("utf-8")).hexdigest()


def platform_authority_digest(
    *,
    baseline_signature: str,
    model_signature: str,
) -> str:
    """Return the durable identity for the paired platform authority."""

    if not baseline_signature.strip() or not model_signature.strip():
        raise ValueError("platform authority signatures are required")
    payload = json.dumps(
        {
            "baseline_signature": baseline_signature,
            "model_signature": model_signature,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def platform_year_request_template_hash(
    *,
    endpoint: str,
    subject: str,
    target_year: int,
    policy_version: str,
    provider: str = "wayback",
    limit: int = 1_000,
) -> str:
    """Build the same deterministic request identity used by the provider."""

    normalized = normalize_official(subject)
    if normalized is None:
        raise ValueError("platform harvest subject must be a valid hostname")
    if (
        isinstance(target_year, bool)
        or not isinstance(target_year, int)
        or not 1996 <= target_year <= 2001
    ):
        raise ValueError("platform harvest year must be an integer within 1996-2001")
    if any(
        not isinstance(value, str) or not value.strip()
        for value in (endpoint, provider, policy_version)
    ):
        raise ValueError("platform request identity fields must be non-empty")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("platform request limit must be a positive integer")
    payload = json.dumps(
        {
            "template_version": "wayback-platform-year-v1",
            "provider": provider,
            "endpoint": endpoint,
            "subject": normalized,
            "target_year": target_year,
            "policy_version": policy_version,
            "matchType": "domain",
            "from": f"{target_year}0101000000",
            "to": f"{target_year}1231235959",
            "output": "json",
            "fl": "urlkey,timestamp,original,statuscode,digest,length",
            "filter": "statuscode:[23][0-9][0-9]",
            "gzip": "false",
            "showResumeKey": "true",
            "limit": str(limit),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


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
    source_key: str = ""
    reservoir_id: str = ""
    exposure_id: str = ""
    authority_digest: str = ""
    origin_decision: str = ""
    evidence_frontier: int = 0
    terminal_reason: str | None = None

    def __post_init__(self) -> None:
        normalized = normalize_official(self.subject)
        if normalized is None:
            raise ValueError("platform harvest subject must be a valid hostname")
        object.__setattr__(self, "subject", normalized)
        object.__setattr__(self, "state", PlatformHarvestState(self.state))
        if (
            isinstance(self.target_year, bool)
            or not isinstance(self.target_year, int)
            or not 1996 <= self.target_year <= 2001
        ):
            raise ValueError(
                "platform harvest year must be an integer within 1996-2001"
            )
        expected = platform_year_harvest_id(
            provider=self.provider,
            subject=normalized,
            target_year=self.target_year,
            request_template_hash=self.request_template_hash,
            policy_version=self.policy_version,
            source_key=self.source_key,
            reservoir_id=self.reservoir_id,
            exposure_id=self.exposure_id,
            authority_digest=self.authority_digest,
        )
        if self.harvest_id != expected:
            raise ValueError("platform harvest id does not match durable identity")
        integer_counters = (
            self.page_number,
            self.attempt,
            self.rows_seen,
            self.unique_host_years_seen,
            self.requests,
            self.bytes,
            self.baseline_external_host_years,
            self.evidence_frontier,
        )
        if any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            for value in integer_counters
        ):
            raise ValueError(
                "platform harvest counters must be non-negative integers"
            )
        for value in (self.elapsed_seconds, self.final_eed):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise ValueError(
                    "platform harvest floating accounting must be finite and non-negative"
                )
        for name in (
            "retry_at", "lease_expires_at", "created_at", "updated_at", "completed_at"
        ):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise ValueError(f"{name} must be finite and non-negative")
        lineage = (
            self.source_key,
            self.reservoir_id,
            self.exposure_id,
            self.authority_digest,
        )
        if any(lineage) and not all(value.strip() for value in lineage):
            raise ValueError("platform harvest lineage must be complete")


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
        object.__setattr__(self, "capsules", tuple(self.capsules))
        if (
            isinstance(self.target_year, bool)
            or not isinstance(self.target_year, int)
            or not 1996 <= self.target_year <= 2001
        ):
            raise ValueError(
                "platform harvest result year must be an integer within 1996-2001"
            )
        if not isinstance(self.exhaustive, bool):
            raise ValueError("platform harvest exhaustive must be a boolean")
        state = PlatformHarvestState(self.state)
        object.__setattr__(self, "state", state)
        if state not in {
            PlatformHarvestState.PARTIAL,
            PlatformHarvestState.RETRYABLE,
            PlatformHarvestState.COMPLETE,
            PlatformHarvestState.FAILED_INVALID,
        }:
            raise ValueError("provider result must be a page-completion state")
        if state is PlatformHarvestState.PARTIAL:
            if not self.next_resume_key:
                raise ValueError(
                    "partial platform harvest must preserve a continuation"
                )
            if self.next_resume_key == self.resume_key_used:
                raise ValueError(
                    "partial platform harvest must advance the continuation"
                )
        elif self.next_resume_key is not None:
            raise ValueError(
                "only PARTIAL platform harvest results may publish a continuation"
            )
        if state is PlatformHarvestState.COMPLETE:
            if self.next_resume_key is not None or not self.exhaustive:
                raise ValueError(
                    "complete platform harvest requires explicit provider exhaustion"
                )
        elif self.exhaustive:
            raise ValueError("only COMPLETE may claim provider exhaustion")
        for value in (self.rows_seen, self.requests, self.bytes):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(
                    "platform harvest result counts must be non-negative integers"
                )
        if (
            isinstance(self.elapsed_seconds, bool)
            or not isinstance(self.elapsed_seconds, (int, float))
            or not math.isfinite(float(self.elapsed_seconds))
            or self.elapsed_seconds < 0
        ):
            raise ValueError(
                "platform harvest result elapsed_seconds must be finite and non-negative"
            )
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
        if (
            isinstance(claim_batch_size, bool)
            or not isinstance(claim_batch_size, int)
            or claim_batch_size < 1
        ):
            raise ValueError("claim_batch_size must be a positive integer")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(float(lease_seconds))
            or lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be finite and positive")
        if (
            isinstance(retry_base_seconds, bool)
            or not isinstance(retry_base_seconds, (int, float))
            or isinstance(retry_max_seconds, bool)
            or not isinstance(retry_max_seconds, (int, float))
            or not math.isfinite(float(retry_base_seconds))
            or not math.isfinite(float(retry_max_seconds))
            or retry_base_seconds < 0
            or retry_max_seconds < retry_base_seconds
        ):
            raise ValueError("invalid retry policy")
        if not providers:
            raise ValueError("at least one platform harvest provider is required")
        if any(
            not isinstance(name, str) or not name.strip() or provider is None
            for name, provider in providers.items()
        ):
            raise ValueError("platform harvest providers must be named instances")
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
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 0:
            raise ValueError("attempt must be a non-negative integer")
        exponent = max(0, attempt - 1)
        base = self.retry_base_seconds
        maximum = self.retry_max_seconds
        if base <= 0.0 or maximum <= 0.0:
            delay = 0.0
        elif base >= maximum:
            delay = maximum
        else:
            cap_exponent = max(0, math.ceil(math.log2(maximum / base)))
            delay = min(maximum, base * (2.0 ** min(exponent, cap_exponent)))
        now = self.clock()
        if (
            isinstance(now, bool)
            or not isinstance(now, (int, float))
            or not math.isfinite(float(now))
            or now < 0
        ):
            raise ValueError("retry clock must be finite and non-negative")
        return float(now) + float(delay)

    async def run_once(self) -> PlatformYearHarvestWorkerReport:
        tasks = self.control_store.claim_platform_year_harvests(
            owner=self.owner,
            limit=self.claim_batch_size,
            lease_seconds=self.lease_seconds,
            providers=tuple(self.providers),
        )
        if not tasks:
            return PlatformYearHarvestWorkerReport()

        # Platform tasks admitted under the current authority get a source-run
        # projection before proof is written.  Older tasks without a matching
        # authority still retain their exposure and can be safely replayed, but
        # they are intentionally not turned into a guessed FINAL reward.
        from creeper.source_discovery.registry import SourceDiscoveryRegistry

        source_registry = SourceDiscoveryRegistry(self.control_store)
        authority = source_registry.current_scout_authority

        pages_committed = complete = partial = retryable = failed_invalid = 0
        inserted_capsules = rows_seen = provider_requests = bytes_seen = 0
        for task in tasks:
            authority_matches = (
                authority is not None
                and platform_authority_digest(
                    baseline_signature=authority[0],
                    model_signature=authority[1],
                )
                == task.authority_digest
            )
            exposure = self.control_store.ensure_platform_year_exposure(
                task,
                authority=authority if authority_matches else None,
            )
            if authority_matches and exposure is not None:
                source_registry.begin_source_run_for_exposure(
                    task.source_key,
                    reservoir_id=task.reservoir_id,
                    lease_id=task.exposure_id,
                    exposure_id=exposure.exposure_id,
                    baseline_signature=authority[0],
                    model_signature=authority[1],
                )
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
            if task.source_key:
                from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
                from creeper.storage.evidence_store import EvidenceTaskProvenance

                task_key = EvidenceQueryKey(
                    task.subject,
                    TemporalScope(task.target_year, task.target_year),
                    task.provider,
                    task.policy_version,
                )
                provenance = EvidenceTaskProvenance(
                    key=task_key,
                    source_key=task.source_key,
                    reservoir_id=task.reservoir_id,
                    lease_id=task.exposure_id,
                    committed_at=float(self.clock()),
                    task_kind="platform_year",
                )
                inserted_capsules += self.evidence_store.put_many_with_task_provenance(
                    (capsule, provenance) for capsule in result.capsules
                )
            else:
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
            if task.source_key:
                stored = self.control_store.get_platform_year_harvest(task.harvest_id)
                assert stored is not None
                frontier = self.evidence_store.max_platform_year_provenance_sequence(
                    source_key=task.source_key,
                    reservoir_id=task.reservoir_id,
                    exposure_id=task.exposure_id,
                )
                self.control_store.record_platform_year_exposure_progress(
                    task.harvest_id,
                    state=(
                        "READ_COMPLETE"
                        if result.state is PlatformHarvestState.COMPLETE
                        else "RUNNING"
                    ),
                    provider_requests=stored.requests,
                    provider_bytes=stored.bytes,
                    provider_elapsed_seconds=stored.elapsed_seconds,
                    evidence_frontier=frontier,
                    accepted_host_years=stored.unique_host_years_seen,
                )
                if authority_matches and result.state is PlatformHarvestState.COMPLETE:
                    # A platform page has no source-file read phase.  Mark only
                    # the read gate here; readiness still owns the evidence
                    # frontier, FINAL EED, and terminal publication.
                    source_registry.record_source_run_read(
                        task.source_key,
                        reservoir_id=task.reservoir_id,
                        lease_id=task.exposure_id,
                        baseline_signature=authority[0],
                        model_signature=authority[1],
                        source_records=stored.rows_seen,
                        bytes_read=0,
                        source_requests=0,
                        read_complete=True,
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
