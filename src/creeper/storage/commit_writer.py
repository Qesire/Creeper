"""Single-writer batching for evidence capsules and task state."""

from __future__ import annotations

from collections.abc import Iterable

from creeper.evidence.policies import EvidenceCapsule, EvidenceQueryResult
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class CommitWriter:
    def __init__(self, evidence_store: EvidenceStore, control_store: ControlStore, *, owner: str):
        self.evidence_store = evidence_store
        self.control_store = control_store
        self.owner = owner
        self._pending: list[tuple[EvidenceCapsule | None, EvidenceQueryResult]] = []
        self._closed = False

    def submit(self, capsule: EvidenceCapsule | None, result: EvidenceQueryResult) -> None:
        if self._closed:
            raise RuntimeError("commit writer is closed")
        self._pending.append((capsule, result))

    def flush(self) -> int:
        if not self._pending:
            return 0
        pending = self._pending
        capsules = [capsule for capsule, _ in pending if capsule is not None]
        if capsules:
            self.evidence_store.put_many(capsules)
        results = [result for _, result in pending if result.key is not None]
        if results:
            self.control_store.finish_evidence_tasks(results, owner=self.owner)
        self._pending = []
        return len(pending)

    def close(self) -> None:
        if not self._closed:
            self.flush()
            self._closed = True
