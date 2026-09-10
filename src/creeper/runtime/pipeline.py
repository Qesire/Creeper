"""Synchronous, bounded V2.2 production-runtime foundation.

The first runtime intentionally executes one lease at a time.  It is the
correctness boundary for later provider pools: every unit of work is granted
by :class:`GlobalScheduler`, queues have explicit capacities, and evidence
task state is durable before network work starts.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
import time

from creeper.authority.baseline_index import YEAR_BITS, BaselineIndex
from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.evidence.providers.cdx import Transport, query_year
from creeper.records.models import HostObservation, SourceRecord
from creeper.runtime.queues import BoundedQueues
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import WorkLease
from creeper.scheduler.priority import LeaseCandidate
from creeper.storage.commit_writer import CommitWriter
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


@dataclass(frozen=True)
class SyncRuntimeReport:
    """One bounded run result, suitable for the first ``creeper run --once``."""

    leases_succeeded: int = 0
    source_records: int = 0
    observations: int = 0
    evidence_tasks_enqueued: int = 0
    evidence_tasks_completed: int = 0
    evidence_capsules_committed: int = 0
    max_source_record_queue_depth: int = 0
    max_observation_queue_depth: int = 0
    max_evidence_queue_depth: int = 0
    max_commit_queue_depth: int = 0

    def as_dict(self) -> dict[str, int]:
        return self.__dict__.copy()


class SyncRuntime:
    """Execute a single finite lease without unbounded in-memory backlogs."""

    def __init__(
        self,
        *,
        baseline: BaselineIndex,
        control_store: ControlStore,
        evidence_store: EvidenceStore,
        scheduler: GlobalScheduler,
        candidates: Iterable[LeaseCandidate],
        adapters: Mapping[str, object],
        evidence_transport: Transport,
        queue_capacities: Mapping[str, int],
        evidence_provider: str = "wayback",
        evidence_policy_version: str = "cdx-v1",
        owner: str = "sync-runtime",
        baseline_batch_size: int = 50_000,
    ) -> None:
        if baseline_batch_size < 1:
            raise ValueError("baseline_batch_size must be positive")
        self.baseline = baseline
        self.control_store = control_store
        self.evidence_store = evidence_store
        self.scheduler = scheduler
        self.candidates = tuple(candidates)
        self.adapters = dict(adapters)
        self.evidence_transport = evidence_transport
        self.queue_capacities = dict(queue_capacities)
        self.evidence_provider = evidence_provider
        self.evidence_policy_version = evidence_policy_version
        self.owner = owner
        self.baseline_batch_size = baseline_batch_size

    def _candidate_for(self, lease: WorkLease) -> LeaseCandidate:
        for candidate in self.candidates:
            if candidate.reservoir_id == lease.reservoir_id:
                return candidate
        raise KeyError(f"no candidate for lease reservoir: {lease.reservoir_id}")

    def _adapter_for(self, candidate: LeaseCandidate) -> object:
        if candidate.reservoir is None:
            raise ValueError("a runtime lease candidate requires a reservoir")
        try:
            return self.adapters[candidate.reservoir.adapter_id]
        except KeyError as exc:
            raise KeyError(f"no adapter: {candidate.reservoir.adapter_id}") from exc

    @staticmethod
    def _missing_years(observation: HostObservation, annual_mask: int, evidence_store: EvidenceStore) -> tuple[int, ...]:
        years: list[int] = []
        local_mask = 0
        for capsule in evidence_store.for_hostname(observation.hostname):
            local_mask |= YEAR_BITS.get(capsule.year, 0)
        direct_mask = observation.direct_year_mask
        if observation.source_year in YEAR_BITS:
            direct_mask |= YEAR_BITS[observation.source_year]
        for year, bit in YEAR_BITS.items():
            if direct_mask & bit and not (annual_mask | local_mask) & bit:
                years.append(year)
        return tuple(years)

    @staticmethod
    def _put(queue, value: object) -> int:
        queue.put_nowait(value)
        return queue.qsize()

    def _enqueue_observation_tasks(
        self,
        queues: BoundedQueues,
        observation: HostObservation,
        annual_mask: int,
        provider: str,
    ) -> tuple[int, int]:
        """Return (enqueued, high-water) for this observation.

        Credit capacity is checked before durable enqueueing, so a lease can
        never turn a finite provider pool into an unbounded task store.
        """
        enqueued = high_water = 0
        for year in self._missing_years(observation, annual_mask, self.evidence_store):
            if queues.evidence_task_queue.full():
                break
            balance = self.scheduler.ledger.balance(provider)
            if balance.available + balance.reserved < 1:
                break
            key = EvidenceQueryKey(
                observation.hostname,
                TemporalScope(year, year),
                provider,
                self.evidence_policy_version,
            )
            if self.control_store.enqueue_evidence_tasks([key]):
                self.scheduler.ledger.note_queued(provider)
                high_water = max(high_water, self._put(queues.evidence_task_queue, key))
                enqueued += 1
        return enqueued, high_water

    def run_once(self) -> SyncRuntimeReport:
        queues = BoundedQueues(**self.queue_capacities)
        lease = self.scheduler.grant_next(self.candidates, owner=self.owner)
        if lease is None:
            return SyncRuntimeReport()
        if lease.expires_at is None:
            lease = replace(lease, expires_at=time.time() + lease.max_seconds)

        candidate = self._candidate_for(lease)
        provider = candidate.evidence_provider or self.evidence_provider
        self.control_store.save_lease(lease)
        running = lease.start()
        self.control_store.save_lease(running)
        source_records = observations = enqueued = completed = capsules = 0
        max_source = max_observations = max_evidence = max_commits = 0
        reserved_remaining = candidate.expected_evidence_tasks if candidate.evidence_mode != "direct_year" else 0

        try:
            adapter = self._adapter_for(candidate)
            execute = getattr(adapter, "execute")
            extract_hosts = getattr(adapter, "extract_hosts")
            records, _result = execute(running)
            pending: list[HostObservation] = []

            def resolve_pending() -> None:
                nonlocal enqueued, max_evidence
                if not pending:
                    return
                hostnames = [observation.hostname for observation in pending]
                resolved: dict[str, tuple[int, bool]] = {}
                for batch in self.baseline.iter_resolve_batches(
                    hostnames, input_batch_size=self.baseline_batch_size
                ):
                    resolved.update(batch)
                for item in pending:
                    annual_mask, _candidate = resolved.get(item.hostname, (0, False))
                    count, high_water = self._enqueue_observation_tasks(
                        queues, item, annual_mask, provider
                    )
                    enqueued += count
                    max_evidence = max(max_evidence, high_water)
                pending.clear()

            for record in records:
                max_source = max(max_source, self._put(queues.source_record_queue, record))
                current = queues.source_record_queue.get_nowait()
                source_records += 1
                for observation in extract_hosts(current):
                    max_observations = max(
                        max_observations, self._put(queues.observation_queue, observation)
                    )
                    pending.append(queues.observation_queue.get_nowait())
                    observations += 1
                    if len(pending) >= self.baseline_batch_size:
                        resolve_pending()
            resolve_pending()

            keys: list[EvidenceQueryKey] = []
            while not queues.evidence_task_queue.empty():
                keys.append(queues.evidence_task_queue.get_nowait())
            if keys:
                claimed = self.control_store.claim_evidence_tasks(
                    owner=self.owner, limit=len(keys), keys=keys
                )
                # Every dequeued key consumes a queued credit.  Tasks skipped
                # because another runner completed them are completed locally.
                self.scheduler.ledger.claim_evidence(provider, len(keys))
                claimed_by_key = {task.key: task for task in claimed}
                writer = CommitWriter(self.evidence_store, self.control_store, owner=self.owner)
                for key in keys:
                    if key not in claimed_by_key:
                        self.scheduler.ledger.complete_evidence(provider)
                        continue
                    result = query_year(
                        key.hostname,
                        key.temporal_scope.year_from,
                        self.evidence_transport,
                        provider=key.provider,
                        policy_version=key.policy_version,
                    )
                    max_commits = max(max_commits, self._put(queues.commits, result))
                    queues.commits.get_nowait()
                    writer.submit(result.capsule, result)
                    completed += 1
                    if result.capsule is not None:
                        capsules += 1
                    self.scheduler.ledger.complete_evidence(provider)
                writer.close()

            if reserved_remaining:
                balance = self.scheduler.ledger.balance(provider)
                self.scheduler.ledger.release_evidence(provider, min(reserved_remaining, balance.reserved))
            succeeded = running.complete()
            self.control_store.save_lease(succeeded)
            return SyncRuntimeReport(
                leases_succeeded=1,
                source_records=source_records,
                observations=observations,
                evidence_tasks_enqueued=enqueued,
                evidence_tasks_completed=completed,
                evidence_capsules_committed=capsules,
                max_source_record_queue_depth=max_source,
                max_observation_queue_depth=max_observations,
                max_evidence_queue_depth=max_evidence,
                max_commit_queue_depth=max_commits,
            )
        except BaseException:
            if reserved_remaining:
                balance = self.scheduler.ledger.balance(provider)
                self.scheduler.ledger.release_evidence(provider, min(reserved_remaining, balance.reserved))
            self.control_store.save_lease(running.abort())
            raise
