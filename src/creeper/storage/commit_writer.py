"""Single-writer batching for evidence capsules and task state."""

from __future__ import annotations

import time
from collections.abc import Callable

from creeper.evidence.policies import EvidenceCapsule, EvidenceQueryResult
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class CommitWriter:
    """Batch evidence writes while keeping the in-memory pending set bounded.

    ``flush_interval_seconds`` is checked on each submit; the synchronous
    runtime has no background timer. Count-based flushing provides the hard
    memory bound, while the interval limits how long a low-throughput worker
    holds completed task results before making them durable.
    """

    def __init__(
        self,
        evidence_store: EvidenceStore,
        control_store: ControlStore,
        *,
        owner: str,
        flush_count: int = 1_000,
        flush_interval_seconds: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        if flush_count < 1:
            raise ValueError("flush_count must be positive")
        if flush_interval_seconds <= 0:
            raise ValueError("flush_interval_seconds must be positive")
        self.evidence_store = evidence_store
        self.control_store = control_store
        self.owner = owner
        self.flush_count = flush_count
        self.flush_interval_seconds = flush_interval_seconds
        self.clock = clock
        self._pending: list[tuple[EvidenceCapsule | None, EvidenceQueryResult]] = []
        self._closed = False
        self._last_flush = float(clock())
        self.inserted_capsules = 0
        self.finished_tasks = 0

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def submit(self, capsule: EvidenceCapsule | None, result: EvidenceQueryResult) -> None:
        if self._closed:
            raise RuntimeError("commit writer is closed")
        self._pending.append((capsule, result))
        now = float(self.clock())
        if (
            len(self._pending) >= self.flush_count
            or now - self._last_flush >= self.flush_interval_seconds
        ):
            self.flush(now=now)

    def flush(self, *, now: float | None = None) -> int:
        if not self._pending:
            if now is not None:
                self._last_flush = float(now)
            return 0
        pending = self._pending
        capsules = [capsule for capsule, _ in pending if capsule is not None]
        if capsules:
            # Publish non-authoritative lineage first. Readiness can observe a
            # new EvidenceStore host-year immediately after put_many returns;
            # origin-first ordering prevents that concurrent consumer from
            # permanently advancing past an otherwise attributable host-year.
            for capsule, result in pending:
                if capsule is None or result.key is None:
                    continue
                self.control_store.attribute_task_host_years(
                    result.key,
                    (capsule.year,),
                )
            self.inserted_capsules += self.evidence_store.put_many(capsules)
        results = [result for _, result in pending if result.key is not None]
        if results:
            self.finished_tasks += self.control_store.finish_evidence_tasks(
                results, owner=self.owner
            )
        self._pending = []
        self._last_flush = float(self.clock()) if now is None else float(now)
        return len(pending)

    def close(self) -> None:
        if not self._closed:
            self.flush()
            self._closed = True
