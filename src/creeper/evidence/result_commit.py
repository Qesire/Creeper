"""Shared central commit semantics for local and Fabric evidence execution."""

from __future__ import annotations

from dataclasses import dataclass
import math
import time
from typing import Callable

from creeper.evidence.policies import (
    CDXQueryState,
    DomainEvidenceQueryResult,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
)
from creeper.storage.commit_writer import CommitWriter
from creeper.storage.control_store import ControlStore, EvidenceTask
from creeper.storage.evidence_store import EvidenceStore, EvidenceTaskProvenance


def evidence_retry_at(
    attempt: int,
    *,
    base_seconds: float,
    max_seconds: float,
    clock: Callable[[], float] = time.time,
) -> float:
    """Return the common durable retry deadline for local or Fabric execution."""

    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 0:
        raise ValueError("attempt must be a non-negative integer")
    for name, value in (
        ("base_seconds", base_seconds),
        ("max_seconds", max_seconds),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value < 0
        ):
            raise ValueError(f"{name} must be finite and non-negative")
    if max_seconds < base_seconds:
        raise ValueError("max_seconds must not be below base_seconds")
    if base_seconds <= 0.0 or max_seconds <= 0.0:
        delay = 0.0
    elif base_seconds >= max_seconds:
        delay = float(max_seconds)
    else:
        cap_exponent = max(
            0,
            math.ceil(math.log2(float(max_seconds) / float(base_seconds))),
        )
        delay = min(
            float(max_seconds),
            float(base_seconds)
            * (2.0 ** min(max(0, attempt - 1), cap_exponent)),
        )
    now = clock()
    if (
        isinstance(now, bool)
        or not isinstance(now, (int, float))
        or not math.isfinite(float(now))
        or now < 0
    ):
        raise ValueError("retry clock must be finite and non-negative")
    return float(now) + delay


@dataclass(frozen=True, slots=True)
class EvidenceCommitOutcome:
    terminal: int = 0
    retryable: int = 0
    inserted_capsules: int = 0


class EvidenceResultCommitter:
    """Commit one already-executed provider result through Creeper authority.

    This class owns no provider/network lifetime. It is deliberately reusable by
    the in-process AsyncEvidenceWorker and the distributed Fabric inbox bridge,
    so remote execution cannot fork the evidence state machine.
    """

    def __init__(
        self,
        control_store: ControlStore,
        evidence_store: EvidenceStore,
        *,
        owner: str,
        retry_at: Callable[[int], float],
    ) -> None:
        if not owner.strip():
            raise ValueError("owner is required")
        self.control_store = control_store
        self.evidence_store = evidence_store
        self.owner = owner
        self.retry_at = retry_at

    def _record_attempt_metric(
        self,
        task: EvidenceTask,
        result: EvidenceQueryResult | RangeEvidenceQueryResult | DomainEvidenceQueryResult,
    ) -> None:
        self.control_store.record_evidence_task_attempt_metric(
            result.key or task.key,
            attempt=task.attempt,
            state=result.state,
            provider_requests=result.provider_requests,
            provider_elapsed_milliseconds=result.provider_elapsed_milliseconds,
            pages_seen=result.pages_seen,
            records_seen=result.records_seen,
        )

    def _persist_positive_capsules(
        self,
        task: EvidenceTask,
        result: RangeEvidenceQueryResult | DomainEvidenceQueryResult,
        *,
        domain: bool = False,
    ) -> int:
        capsule_rows = tuple(result.capsules)
        if not capsule_rows:
            return 0
        key = result.key or task.key
        origin = self.control_store.primary_evidence_task_origin(key)
        provenance = EvidenceTaskProvenance(
            key=key,
            source_key="" if origin is None else origin[0],
            reservoir_id="" if origin is None else origin[1],
            lease_id="" if origin is None else origin[2],
            committed_at=float(self.control_store.clock()),
        )
        inserted = self.evidence_store.put_many_with_task_provenance(
            (capsule, provenance) for capsule in capsule_rows
        )
        if domain:
            self.control_store.attribute_domain_task_host_years(
                key,
                capsule_rows,
            )
        else:
            self.control_store.attribute_task_host_years(
                key,
                (capsule.year for capsule in capsule_rows),
            )
        return inserted

    def commit(
        self,
        task: EvidenceTask,
        result: EvidenceQueryResult | RangeEvidenceQueryResult | DomainEvidenceQueryResult,
    ) -> EvidenceCommitOutcome:
        if result.key is None:
            raise ValueError("provider result must preserve EvidenceQueryKey")
        if result.key != task.key:
            raise ValueError("provider result key does not match claimed task")

        self._record_attempt_metric(task, result)

        if isinstance(result, DomainEvidenceQueryResult):
            inserted = self._persist_positive_capsules(
                task,
                result,
                domain=True,
            )
            if result.state in {
                CDXQueryState.DECOMPOSED,
                CDXQueryState.INVALID,
            }:
                self.control_store.finish_range_task(
                    result.key,
                    result.state,
                    followup_keys=(),
                    owner=self.owner,
                )
                return EvidenceCommitOutcome(
                    terminal=1,
                    inserted_capsules=inserted,
                )
            if result.state is CDXQueryState.TRANSIENT_ERROR:
                self.control_store.finish_evidence_task(
                    result.key,
                    result.state,
                    owner=self.owner,
                    retry_at=self.retry_at(task.attempt),
                )
                return EvidenceCommitOutcome(
                    retryable=1,
                    inserted_capsules=inserted,
                )
            raise ValueError(
                f"unsupported domain provider state: {result.state}"
            )

        if isinstance(result, RangeEvidenceQueryResult):
            inserted = self._persist_positive_capsules(task, result)
            if result.state in {
                CDXQueryState.PASS,
                CDXQueryState.EMPTY_EXHAUSTIVE,
                CDXQueryState.INVALID,
            }:
                self.control_store.finish_range_task(
                    result.key,
                    result.state,
                    followup_keys=(),
                    owner=self.owner,
                )
                return EvidenceCommitOutcome(
                    terminal=1,
                    inserted_capsules=inserted,
                )
            if result.state in {
                CDXQueryState.DECOMPOSED,
                CDXQueryState.INCOMPLETE,
                CDXQueryState.TRANSIENT_ERROR,
            }:
                retry_state = (
                    CDXQueryState.INCOMPLETE
                    if result.state is CDXQueryState.DECOMPOSED
                    else result.state
                )
                self.control_store.finish_evidence_task(
                    result.key,
                    retry_state,
                    owner=self.owner,
                    retry_at=self.retry_at(task.attempt),
                )
                return EvidenceCommitOutcome(
                    retryable=1,
                    inserted_capsules=inserted,
                )
            raise ValueError(f"unsupported provider state: {result.state}")

        if result.state in {
            CDXQueryState.PASS,
            CDXQueryState.EMPTY_EXHAUSTIVE,
            CDXQueryState.INVALID,
        }:
            writer = CommitWriter(
                self.evidence_store,
                self.control_store,
                owner=self.owner,
                flush_count=1,
            )
            try:
                writer.submit(result.capsule, result)
            finally:
                writer.close()
            return EvidenceCommitOutcome(
                terminal=1,
                inserted_capsules=writer.inserted_capsules,
            )

        if result.state in {
            CDXQueryState.INCOMPLETE,
            CDXQueryState.TRANSIENT_ERROR,
        }:
            self.control_store.finish_evidence_task(
                result.key,
                result.state,
                owner=self.owner,
                retry_at=self.retry_at(task.attempt),
            )
            return EvidenceCommitOutcome(retryable=1)

        raise ValueError(f"unsupported provider state: {result.state}")
