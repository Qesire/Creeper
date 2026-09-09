"""Bounded, resumable evidence execution over exact hostname-year tasks."""

from __future__ import annotations

import json
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from creeper.evidence.policies import CDXQueryState, EvidenceCapsule, EvidenceQueryResult
from creeper.evidence.providers.cdx import Transport, query_year
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
    ):
        self.transport = transport
        self.audit_path = audit_path
        self.store = store
        self.provider = provider
        self.policy_version = policy_version

    def _terminal_tasks(self) -> set[tuple[str, int]]:
        if not self.audit_path.is_file():
            return set()
        terminal: set[tuple[str, int]] = set()
        with self.audit_path.open("r", encoding="utf-8") as audit:
            for raw in audit:
                try:
                    record = json.loads(raw)
                    if record.get("state") in TERMINAL_STATES:
                        terminal.add((str(record["hostname"]), int(record["year"])))
                except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                    continue
        return terminal

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
        if result.capsule is not None:
            payload["capsule"] = result.capsule.__dict__
        return payload

    def run(self, tasks: Iterable[tuple[str, int]]) -> EvidenceBatchReport:
        task_list = list(dict.fromkeys((str(hostname), int(year)) for hostname, year in tasks))
        terminal = self._terminal_tasks()
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        started = time.perf_counter()
        executed = skipped = accepted = pages = records = 0
        states: Counter[str] = Counter()
        with self.audit_path.open("a", encoding="utf-8") as audit:
            for hostname, year in task_list:
                if (hostname, year) in terminal:
                    skipped += 1
                    continue
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
                if result.capsule is not None:
                    accepted += 1
                    if self.store is not None:
                        self.store.put(result.capsule)
                audit.write(json.dumps(self._audit_record(result), ensure_ascii=False) + "\n")
                audit.flush()
                if state in TERMINAL_STATES:
                    terminal.add((result.hostname, result.year))
        elapsed = time.perf_counter() - started
        return EvidenceBatchReport(
            scheduled=len(task_list),
            executed=executed,
            skipped=skipped,
            accepted=accepted,
            states=dict(states),
            pages_seen=pages,
            records_seen=records,
            elapsed_seconds=round(elapsed, 6),
        )
