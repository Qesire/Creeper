"""Bounded queues shared by the synchronous runtime stages."""

from __future__ import annotations

import queue
from typing import Any


class BoundedQueues:
    """Named bounded queues for the runtime pipeline."""

    def __init__(
        self,
        *,
        source_records: int,
        observations: int,
        evidence_tasks: int,
        commits: int,
    ) -> None:
        capacities = {
            "source_records": source_records,
            "observations": observations,
            "evidence_tasks": evidence_tasks,
            "commits": commits,
        }
        if any(not isinstance(value, int) or value < 1 for value in capacities.values()):
            raise ValueError("queue capacities must be positive integers")

        self.source_records: queue.Queue[Any] = queue.Queue(maxsize=source_records)
        self.observations: queue.Queue[Any] = queue.Queue(maxsize=observations)
        self.evidence_tasks: queue.Queue[Any] = queue.Queue(maxsize=evidence_tasks)
        self.commits: queue.Queue[Any] = queue.Queue(maxsize=commits)

    @property
    def source_record_queue(self) -> queue.Queue[Any]:
        return self.source_records

    @property
    def observation_queue(self) -> queue.Queue[Any]:
        return self.observations

    @property
    def evidence_task_queue(self) -> queue.Queue[Any]:
        return self.evidence_tasks

    @property
    def commit_queue(self) -> queue.Queue[Any]:
        return self.commits
