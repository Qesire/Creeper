"""Wire-neutral models for distributed Creeper workers and authority."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping

from creeper.distributed.identity import batch_id, work_key


class Capability(StrEnum):
    THIN_QUERY = "THIN_QUERY"
    ONLINE_QUERY = "ONLINE_QUERY"
    STREAMING_BULK = "STREAMING_BULK"
    WEB_DISCOVERY = "WEB_DISCOVERY"
    RDAP = "RDAP"
    SOURCE_RESEARCH = "SOURCE_RESEARCH"


class TaskClass(StrEnum):
    SOURCE_SHARD = "SOURCE_SHARD"
    HOST_BATCH = "HOST_BATCH"
    SOURCE_PAGE = "SOURCE_PAGE"
    PROBE = "PROBE"


@dataclass(frozen=True)
class WorkerDescriptor:
    worker_id: str
    runtime_class: str
    region: str
    architecture: str
    memory_bytes: int
    cpu_count: int
    network_class: str
    capabilities: tuple[str, ...]
    producers: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.worker_id.strip():
            raise ValueError("worker_id is required")
        if not all(
            value.strip()
            for value in (
                self.runtime_class,
                self.region,
                self.architecture,
                self.network_class,
            )
        ):
            raise ValueError("worker runtime, region, architecture and network class are required")
        if self.memory_bytes < 0 or self.cpu_count < 1:
            raise ValueError("invalid worker capacity")
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("worker capabilities must be unique")
        if len(set(self.producers)) != len(self.producers):
            raise ValueError("worker producers must be unique")
        if any(not producer.strip() for producer in self.producers):
            raise ValueError("worker producer names must be non-empty")


@dataclass(frozen=True)
class WorkDefinition:
    producer: str
    task_class: TaskClass
    input_identity: str
    coverage: Mapping[str, Any]
    partition: str
    algorithm_version: str
    required_capabilities: tuple[str, ...]
    priority: float = 0.0

    def __post_init__(self) -> None:
        if not self.producer.strip() or not self.input_identity.strip():
            raise ValueError("producer and input_identity are required")
        if not self.algorithm_version.strip():
            raise ValueError("algorithm_version is required")
        if len(set(self.required_capabilities)) != len(self.required_capabilities):
            raise ValueError("required capabilities must be unique")

    @property
    def work_key(self) -> str:
        return work_key(
            producer=self.producer,
            input_identity=self.input_identity,
            coverage=self.coverage,
            partition=self.partition,
            algorithm_version=self.algorithm_version,
        )


@dataclass(frozen=True)
class TaskLease:
    task_id: str
    work_key: str
    worker_id: str
    generation: int
    lease_deadline: float
    attempt: int
    work: WorkDefinition
    cursor: str | None = None


@dataclass(frozen=True)
class ResultBatch:
    task_id: str
    generation: int
    sequence_no: int
    results: tuple[Mapping[str, Any], ...]
    cursor_after: str | None = None

    def __post_init__(self) -> None:
        if not self.task_id.strip() or self.generation < 1 or self.sequence_no < 0:
            raise ValueError("invalid result batch identity")

    @property
    def batch_id(self) -> str:
        return batch_id(self.task_id, self.sequence_no)


@dataclass(frozen=True)
class ProviderPermit:
    permit_id: str
    provider: str
    worker_id: str
    task_id: str
    generation: int
    allowed_requests: int
    max_inflight: int
    expires_at: float
