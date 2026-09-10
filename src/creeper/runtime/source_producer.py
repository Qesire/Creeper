"""Production source side of the Creeper pipeline.

SourceProducer deliberately does no evidence-provider network I/O. It converts
finite Reservoir leases into direct EvidenceCapsules and durable EvidenceTasks,
then returns. Independent AsyncEvidenceWorkers drain those tasks concurrently.

Producer admission uses a crash-safe durable capacity reservation so multiple
producer processes cannot outrun the evidence backlog high-water mark.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.planner import EvidencePlanner
from creeper.evidence.policies import EvidenceQueryKey
from creeper.records.models import HostObservation
from creeper.runtime.queues import BoundedQueues
from creeper.scheduler.admission import CapacityReservation, EvidenceBacklogAdmission
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import WorkLease
from creeper.scheduler.priority import LeaseCandidate
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


@dataclass(frozen=True)
class SourceProducerReport:
    leases_succeeded: int = 0
    source_records: int = 0
    observations: int = 0
    evidence_tasks_enqueued: int = 0
    direct_capsules_committed: int = 0
    admission_blocked: bool = False
    max_source_record_queue_depth: int = 0
    max_observation_queue_depth: int = 0

    def as_dict(self) -> dict[str, object]:
        return self.__dict__.copy()


class SourceProducer:
    """Produce durable evidence work without sharing provider lifetimes."""

    def __init__(
        self,
        *,
        baseline: BaselineIndex,
        control_store: ControlStore,
        evidence_store: EvidenceStore,
        scheduler: GlobalScheduler,
        candidates: Iterable[LeaseCandidate],
        adapters: Mapping[str, object],
        backlog_capacities: Mapping[str, int],
        queue_capacities: Mapping[str, int],
        evidence_policy_version: str = "cdx-v1",
        owner: str = "source-producer",
        baseline_batch_size: int = 50_000,
        reservation_grace_seconds: float = 30.0,
    ) -> None:
        if baseline_batch_size < 1:
            raise ValueError("baseline_batch_size must be positive")
        if reservation_grace_seconds < 0:
            raise ValueError("reservation_grace_seconds must be non-negative")
        capacities = dict(backlog_capacities)
        if any(
            not provider or not isinstance(value, int) or value < 0
            for provider, value in capacities.items()
        ):
            raise ValueError("backlog capacities must be non-negative integers")
        if not capacities:
            raise ValueError("at least one evidence backlog capacity is required")

        self.baseline = baseline
        self.control_store = control_store
        self.evidence_store = evidence_store
        self.scheduler = scheduler
        self.candidates = tuple(candidates)
        self.adapters = dict(adapters)
        self.backlog_capacities = capacities
        self.queue_capacities = dict(queue_capacities)
        self.evidence_policy_version = evidence_policy_version
        self.owner = owner
        self.baseline_batch_size = baseline_batch_size
        self.reservation_grace_seconds = reservation_grace_seconds
        self.evidence_planner = EvidencePlanner()
        self.admission = EvidenceBacklogAdmission(control_store)

    @staticmethod
    def _put(queue, value: object) -> int:
        queue.put_nowait(value)
        return queue.qsize()

    def _adapter_for(self, candidate: LeaseCandidate) -> object:
        if candidate.reservoir is None:
            raise ValueError("a source candidate requires a reservoir")
        try:
            return self.adapters[candidate.reservoir.adapter_id]
        except KeyError as exc:
            raise KeyError(f"no adapter: {candidate.reservoir.adapter_id}") from exc

    def _grant_fresh_lease(
        self,
    ) -> tuple[LeaseCandidate, WorkLease, CapacityReservation] | None:
        self.control_store.recover_expired_leases()
        for candidate in self.scheduler.rank(self.candidates):
            template = candidate.lease
            if template is None:
                raise ValueError("source candidate requires a lease template")
            provider = candidate.evidence_provider
            try:
                capacity = self.backlog_capacities[provider]
            except KeyError as exc:
                raise KeyError(f"no backlog capacity configured for {provider}") from exc

            reservation = self.admission.try_reserve(
                provider=provider,
                amount=candidate.expected_evidence_tasks,
                capacity=capacity,
                ttl_seconds=template.max_seconds + self.reservation_grace_seconds,
            )
            if reservation is None:
                continue
            try:
                lease = self.control_store.grant_fresh_lease(
                    candidate.reservoir_id,
                    owner=self.owner,
                    max_records=template.max_records,
                    max_requests=template.max_requests,
                    max_bytes=template.max_bytes,
                    max_seconds=template.max_seconds,
                    resource_class=template.resource_class,
                    expected_evidence_tasks=candidate.expected_evidence_tasks,
                    expected_novel_eed=candidate.expected_novel_eed,
                    now=time.time(),
                )
                if lease is None:
                    self.admission.release(reservation)
                    continue
                self.admission.bind_lease(reservation, lease.lease_id)
                return candidate, lease, reservation
            except BaseException:
                self.admission.release(reservation)
                raise
        return None

    def run_once(self) -> SourceProducerReport:
        queues = BoundedQueues(**self.queue_capacities)
        granted = self._grant_fresh_lease()
        if granted is None:
            # A READY source can be temporarily ungrantable solely because all
            # provider backlog capacity is reserved/occupied. Exposing this bit
            # lets an outer supervisor sleep the producer instead of busy-looping.
            ready_exists = any(
                candidate.reservoir is not None
                and getattr(candidate.reservoir.state, "value", candidate.reservoir.state)
                == "ready"
                for candidate in self.candidates
            )
            return SourceProducerReport(admission_blocked=ready_exists)

        candidate, lease, reservation = granted
        provider = candidate.evidence_provider
        running = lease.start()
        self.control_store.save_lease(running)
        source_records = observations = enqueued = direct_committed = 0
        max_source = max_observations = 0
        direct_capsules = []
        scheduled_keys: dict[EvidenceQueryKey, None] = {}
        result = None
        source_finalized = False

        try:
            adapter = self._adapter_for(candidate)
            execute = getattr(adapter, "execute")
            extract_hosts = getattr(adapter, "extract_hosts")
            records, result = execute(running)
            if (
                result.records == 0
                and result.next_cursor == running.cursor_start
                and result.next_cursor is not None
            ):
                raise RuntimeError("lease made no cursor progress")

            pending: list[HostObservation] = []

            def enqueue_external_keys(keys: Iterable[EvidenceQueryKey]) -> None:
                nonlocal enqueued
                fresh = [key for key in keys if key not in scheduled_keys]
                if not fresh:
                    return
                for key in fresh:
                    scheduled_keys[key] = None
                enqueued += self.admission.enqueue_reserved(reservation, fresh)

            def resolve_pending() -> None:
                nonlocal direct_capsules
                if not pending:
                    return
                hostnames = [observation.hostname for observation in pending]
                resolved: dict[str, tuple[int, bool]] = {}
                local_masks = self.evidence_store.resolve_year_masks(hostnames)
                for batch in self.baseline.iter_resolve_batches(
                    hostnames,
                    input_batch_size=self.baseline_batch_size,
                ):
                    resolved.update(batch)
                allow_direct = (
                    candidate.reservoir is not None
                    and candidate.reservoir.evidence_mode == "direct_year"
                )
                for item in pending:
                    annual_mask, _candidate = resolved.get(item.hostname, (0, False))
                    plan = self.evidence_planner.plan(
                        item,
                        official_mask=annual_mask,
                        local_mask=local_masks.get(item.hostname, 0),
                        provider=provider,
                        policy_version=self.evidence_policy_version,
                        allow_direct=allow_direct,
                    )
                    direct_capsules.extend(plan.direct_capsules)
                    enqueue_external_keys(plan.external_keys)
                pending.clear()

            for record in records:
                max_source = max(
                    max_source,
                    self._put(queues.source_record_queue, record),
                )
                current = queues.source_record_queue.get_nowait()
                source_records += 1
                for observation in extract_hosts(current):
                    max_observations = max(
                        max_observations,
                        self._put(queues.observation_queue, observation),
                    )
                    pending.append(queues.observation_queue.get_nowait())
                    observations += 1
                    if len(pending) >= self.baseline_batch_size:
                        resolve_pending()
            resolve_pending()

            if direct_capsules:
                direct_committed += self.evidence_store.put_many(direct_capsules)
            assert result is not None
            self.control_store.finalize_lease(
                running,
                next_cursor=result.next_cursor,
                exhausted=result.next_cursor is None,
            )
            source_finalized = True
            self.admission.release(reservation)

            return SourceProducerReport(
                leases_succeeded=1,
                source_records=source_records,
                observations=observations,
                evidence_tasks_enqueued=enqueued,
                direct_capsules_committed=direct_committed,
                max_source_record_queue_depth=max_source,
                max_observation_queue_depth=max_observations,
            )
        except BaseException:
            if not source_finalized:
                self.control_store.abort_lease(running)
            self.admission.release(reservation)
            raise
