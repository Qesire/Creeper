"""Synchronous, bounded V2.2 production-runtime foundation.

The runtime deliberately keeps source acquisition and provider evidence as
separate ownership domains. A source lease ends after its observations have
been reconciled, direct evidence committed, and external EvidenceTasks made
durable. Slow provider queries therefore cannot expire or reopen a Reservoir
lease. The synchronous implementation may execute those durable tasks in the
same ``run_once`` call, but only after source progress is committed.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import time

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.planner import EvidencePlanner
from creeper.evidence.policies import EvidenceQueryKey
from creeper.evidence.providers.cdx import Transport, query_year
from creeper.records.models import HostObservation
from creeper.runtime.queues import BoundedQueues
from creeper.runtime.submission import RuntimeSubmissionContext, build_runtime_snapshot
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
    snapshot_ready: bool | None = None
    novel_records: int = 0
    max_source_record_queue_depth: int = 0
    max_observation_queue_depth: int = 0
    max_evidence_queue_depth: int = 0
    max_commit_queue_depth: int = 0

    def as_dict(self) -> dict[str, object]:
        return self.__dict__.copy()


class SyncRuntime:
    """Execute one finite source lease and then its newly durable evidence work."""

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
        submission_context: RuntimeSubmissionContext | None = None,
        snapshot_id: str = "runtime-snapshot",
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
        self.evidence_planner = EvidencePlanner()
        self.submission_context = submission_context
        if not isinstance(snapshot_id, str) or not snapshot_id.strip():
            raise ValueError("snapshot_id must be a non-empty string")
        self.snapshot_id = snapshot_id

    def _adapter_for(self, candidate: LeaseCandidate) -> object:
        if candidate.reservoir is None:
            raise ValueError("a runtime lease candidate requires a reservoir")
        try:
            return self.adapters[candidate.reservoir.adapter_id]
        except KeyError as exc:
            raise KeyError(f"no adapter: {candidate.reservoir.adapter_id}") from exc

    @staticmethod
    def _put(queue, value: object) -> int:
        queue.put_nowait(value)
        return queue.qsize()

    def _grant_fresh_lease(self, *, owner: str) -> tuple[LeaseCandidate, WorkLease] | None:
        """Rank candidates, then atomically claim a fresh persisted lease."""
        self.control_store.recover_expired_leases()
        for candidate in self.scheduler.rank(self.candidates):
            template = candidate.lease
            if template is None:
                raise ValueError("runtime lease candidate requires a lease template")
            lease = self.control_store.grant_fresh_lease(
                candidate.reservoir_id,
                owner=owner,
                max_records=template.max_records,
                max_requests=template.max_requests,
                max_bytes=template.max_bytes,
                max_seconds=template.max_seconds,
                resource_class=template.resource_class,
                expected_evidence_tasks=candidate.expected_evidence_tasks,
                expected_novel_eed=candidate.expected_novel_eed,
                now=time.time(),
            )
            if lease is not None:
                return candidate, lease
        return None

    def _execute_scheduled_evidence(
        self,
        *,
        keys: Iterable[EvidenceQueryKey],
        provider: str,
        queues: BoundedQueues,
    ) -> tuple[int, int, int, int]:
        """Execute newly scheduled durable tasks after source progress commits.

        Returns ``(completed, inserted_capsules, max_evidence_depth,
        max_commit_depth)``. Provider capacity is interpreted as the maximum
        synchronous claim batch; a zero-capacity provider leaves all tasks
        durable for a later evidence worker.
        """
        ordered = list(keys)
        if not ordered:
            return 0, 0, 0, 0
        capacity = self.scheduler.ledger.balance(provider).capacity
        if capacity < 1:
            return 0, 0, 0, 0

        completed = max_evidence = max_commits = 0
        writer = CommitWriter(
            self.evidence_store,
            self.control_store,
            owner=self.owner,
            flush_count=max(1, min(capacity, 128)),
        )
        batch_size = max(1, min(queues.evidence_task_queue.maxsize, capacity, 128))
        try:
            for start in range(0, len(ordered), batch_size):
                batch = ordered[start : start + batch_size]
                for key in batch:
                    max_evidence = max(
                        max_evidence,
                        self._put(queues.evidence_task_queue, key),
                    )
                claim_keys: list[EvidenceQueryKey] = []
                while not queues.evidence_task_queue.empty():
                    claim_keys.append(queues.evidence_task_queue.get_nowait())
                claimed = self.control_store.claim_evidence_tasks(
                    owner=self.owner,
                    limit=len(claim_keys),
                    keys=claim_keys,
                )
                claimed_by_key = {task.key for task in claimed}
                for key in claim_keys:
                    if key not in claimed_by_key:
                        continue
                    query_result = query_year(
                        key.hostname,
                        key.temporal_scope.year_from,
                        self.evidence_transport,
                        provider=key.provider,
                        policy_version=key.policy_version,
                    )
                    max_commits = max(
                        max_commits,
                        self._put(queues.commits, query_result),
                    )
                    queues.commits.get_nowait()
                    writer.submit(query_result.capsule, query_result)
                    completed += 1
        finally:
            writer.close()
        return completed, writer.inserted_capsules, max_evidence, max_commits

    def run_once(self) -> SyncRuntimeReport:
        queues = BoundedQueues(**self.queue_capacities)
        granted = self._grant_fresh_lease(owner=self.owner)
        if granted is None:
            return SyncRuntimeReport()
        candidate, lease = granted
        provider = candidate.evidence_provider or self.evidence_provider
        running = lease.start()
        self.control_store.save_lease(running)
        source_records = observations = enqueued = completed = capsules = 0
        max_source = max_observations = max_evidence = max_commits = 0
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
                enqueued += self.control_store.enqueue_evidence_tasks(fresh)

            def resolve_pending() -> None:
                nonlocal direct_capsules
                if not pending:
                    return
                hostnames = [observation.hostname for observation in pending]
                resolved: dict[str, tuple[int, bool]] = {}
                local_masks = self.evidence_store.resolve_year_masks(hostnames)
                for batch in self.baseline.iter_resolve_batches(
                    hostnames, input_batch_size=self.baseline_batch_size
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
                capsules += self.evidence_store.put_many(direct_capsules)
            assert result is not None
            self.control_store.finalize_lease(
                running,
                next_cursor=result.next_cursor,
                exhausted=result.next_cursor is None,
            )
            source_finalized = True

            (
                completed,
                external_capsules,
                max_evidence,
                max_commits,
            ) = self._execute_scheduled_evidence(
                keys=scheduled_keys,
                provider=provider,
                queues=queues,
            )
            capsules += external_capsules

            snapshot_ready: bool | None = None
            novel_records = 0
            if self.submission_context is not None:
                snapshot = build_runtime_snapshot(
                    context=self.submission_context,
                    evidence_store=self.evidence_store,
                    baseline=self.baseline,
                    snapshot_id=self.snapshot_id,
                )
                snapshot_ready = snapshot.ready
                novel_records = len(snapshot.novel_records)
            return SyncRuntimeReport(
                leases_succeeded=1,
                source_records=source_records,
                observations=observations,
                evidence_tasks_enqueued=enqueued,
                evidence_tasks_completed=completed,
                evidence_capsules_committed=capsules,
                snapshot_ready=snapshot_ready,
                novel_records=novel_records,
                max_source_record_queue_depth=max_source,
                max_observation_queue_depth=max_observations,
                max_evidence_queue_depth=max_evidence,
                max_commit_queue_depth=max_commits,
            )
        except BaseException:
            if not source_finalized:
                self.control_store.abort_lease(running)
            raise
