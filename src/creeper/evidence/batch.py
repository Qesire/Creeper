"""Bounded, resumable evidence execution over exact hostname-year tasks."""

from __future__ import annotations

import json
import os
import time
import uuid
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    TemporalScope,
)
from creeper.evidence.providers.cdx import Transport, query_year
from creeper.storage.commit_writer import CommitWriter
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


TERMINAL_STATES = {
    CDXQueryState.PASS.value,
    CDXQueryState.EMPTY_EXHAUSTIVE.value,
    CDXQueryState.INVALID.value,
}


@dataclass(frozen=True)
class EvidenceBatchReport:
    scheduled: int
    executed: int
    skipped: int
    accepted: int
    states: dict[str, int]
    pages_seen: int
    records_seen: int
    elapsed_seconds: float

    def as_dict(self) -> dict[str, object]:
        return {
            "scheduled": self.scheduled,
            "executed": self.executed,
            "skipped": self.skipped,
            "accepted": self.accepted,
            "states": dict(self.states),
            "pages_seen": self.pages_seen,
            "records_seen": self.records_seen,
            "elapsed_seconds": self.elapsed_seconds,
            "requests_per_second": self.executed / self.elapsed_seconds
            if self.elapsed_seconds
            else 0.0,
        }


class EvidenceBatchRunner:
    """Run finite tasks and checkpoint terminal outcomes after each task."""

    def __init__(
        self,
        transport: Transport,
        *,
        audit_path: Path,
        store: EvidenceStore | None = None,
        provider: str = "cdx",
        policy_version: str = "cdx-v1",
        control_store: ControlStore | None = None,
        task_batch_size: int = 256,
        lease_seconds: float = 300.0,
        owner: str | None = None,
    ):
        if task_batch_size < 1:
            raise ValueError("task_batch_size must be positive")
        self.transport = transport
        self.audit_path = audit_path
        self.store = store
        self.provider = provider
        self.policy_version = policy_version
        self.task_batch_size = task_batch_size
        self.lease_seconds = lease_seconds
        self.owner = owner or f"batch-{os.getpid()}-{uuid.uuid4().hex}"
        self._owns_control_store = control_store is None
        self.control_store = control_store or ControlStore(
            audit_path.with_name(f"{audit_path.stem}.control.sqlite3")
        )

    def _task_batches(
        self, tasks: Iterable[tuple[str, int]]
    ) -> Iterator[list[tuple[str, int]]]:
        batch: list[tuple[str, int]] = []
        for hostname, year in tasks:
            batch.append((str(hostname), int(year)))
            if len(batch) >= self.task_batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _query_key(self, hostname: str, year: int) -> EvidenceQueryKey:
        return EvidenceQueryKey(
            hostname,
            TemporalScope(year, year),
            self.provider,
            self.policy_version,
        )

    @staticmethod
    def _audit_record(result: EvidenceQueryResult) -> dict[str, object]:
        payload: dict[str, object] = {
            "hostname": result.hostname,
            "year": result.year,
            "state": result.state.value,
            "pages_seen": result.pages_seen,
            "records_seen": result.records_seen,
            "error": result.error,
        }
        if result.key is not None:
            payload["provider"] = result.key.provider
            payload["policy_version"] = result.key.policy_version
            payload["year_from"] = result.key.temporal_scope.year_from
            payload["year_to"] = result.key.temporal_scope.year_to
        if result.capsule is not None:
            payload["capsule"] = result.capsule.__dict__
        return payload

    def run(self, tasks: Iterable[tuple[str, int]]) -> EvidenceBatchReport:
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        started = time.perf_counter()
        scheduled = executed = skipped = accepted = pages = records = 0
        states: Counter[str] = Counter()
        with self.audit_path.open("a", encoding="utf-8") as audit:
            for task_batch in self._task_batches(tasks):
                scheduled += len(task_batch)
                keys: list[EvidenceQueryKey] = []
                invalid_tasks: list[tuple[str, int]] = []
                for hostname, year in task_batch:
                    try:
                        key = self._query_key(hostname, year)
                    except ValueError:
                        invalid_tasks.append((hostname, year))
                    else:
                        if key not in keys:
                            keys.append(key)
                self.control_store.enqueue_evidence_tasks(keys)
                claimed = self.control_store.claim_evidence_tasks(
                    owner=self.owner,
                    limit=len(keys),
                    lease_seconds=self.lease_seconds,
                    keys=keys,
                )
                skipped += len(keys) - len(claimed)
                claimed_by_key = {task.key: task for task in claimed}
                ordered_claimed = [
                    claimed_by_key[key] for key in keys if key in claimed_by_key
                ]
                writer = (
                    CommitWriter(self.store, self.control_store, owner=self.owner)
                    if self.store is not None
                    else None
                )
                audit_records: list[dict[str, object]] = []
                for task in ordered_claimed:
                    result = query_year(
                        task.key.hostname,
                        task.key.temporal_scope.year_from,
                        self.transport,
                        provider=self.provider,
                        policy_version=self.policy_version,
                    )
                    executed += 1
                    state = result.state.value
                    states[state] += 1
                    pages += result.pages_seen
                    records += result.records_seen
                    if result.capsule is not None:
                        accepted += 1
                    if writer is not None:
                        writer.submit(result.capsule, result)
                    else:
                        self.control_store.finish_evidence_task(
                            result.key or task.key,
                            result.state,
                            owner=self.owner,
                        )
                    audit_records.append(self._audit_record(result))
                for hostname, year in invalid_tasks:
                    result = query_year(
                        hostname,
                        year,
                        self.transport,
                        provider=self.provider,
                        policy_version=self.policy_version,
                    )
                    executed += 1
                    state = result.state.value
                    states[state] += 1
                    pages += result.pages_seen
                    records += result.records_seen
                    audit_records.append(self._audit_record(result))
                if writer is not None:
                    writer.flush()
                for record in audit_records:
                    audit.write(json.dumps(record, ensure_ascii=False) + "\n")
                audit.flush()
        elapsed = time.perf_counter() - started
        return EvidenceBatchReport(
            scheduled=scheduled,
            executed=executed,
            skipped=skipped,
            accepted=accepted,
            states=dict(states),
            pages_seen=pages,
            records_seen=records,
            elapsed_seconds=round(elapsed, 6),
        )
