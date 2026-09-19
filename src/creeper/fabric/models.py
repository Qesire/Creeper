"""Wire-neutral models and deterministic identities for Fabric v2."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping

FABRIC_PROTOCOL_VERSION = "creeper-fabric-v2"


class FabricCapability(StrEnum):
    SEARCH_STRUCTURED = "SEARCH_STRUCTURED"
    HTTP_FETCH = "HTTP_FETCH"
    SOURCE_SCOUT = "SOURCE_SCOUT"
    ADAPTER_LLM = "ADAPTER_LLM"
    STREAM_BULK = "STREAM_BULK"
    EVIDENCE_QUERY = "EVIDENCE_QUERY"
    AUTHORITY_REDUCE = "AUTHORITY_REDUCE"


class FabricWorkClass(StrEnum):
    RESIDUAL_SEARCH = "RESIDUAL_SEARCH"
    SOURCE_TRIAGE = "SOURCE_TRIAGE"
    SOURCE_SCOUT = "SOURCE_SCOUT"
    ADAPTER_COMPILE = "ADAPTER_COMPILE"
    RESERVOIR_PRODUCE = "RESERVOIR_PRODUCE"
    EVIDENCE_COMPLETE = "EVIDENCE_COMPLETE"
    REDUCE_COMMIT = "REDUCE_COMMIT"


class FabricTaskState(StrEnum):
    READY = "READY"
    LEASED = "LEASED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


def canonical_json(value: Mapping[str, Any] | list[Any] | tuple[Any, ...] | Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def deterministic_work_key(
    *,
    work_class: FabricWorkClass,
    producer: str,
    algorithm_version: str,
    partition_key: str,
    input_identity: str,
    coverage: Mapping[str, Any],
    dependency_work_keys: tuple[str, ...] = (),
) -> str:
    payload = canonical_json(
        {
            "work_class": FabricWorkClass(work_class).value,
            "producer": producer.strip(),
            "algorithm_version": algorithm_version.strip(),
            "partition_key": partition_key.strip(),
            "input_identity": input_identity.strip(),
            "coverage": dict(coverage),
            "dependency_work_keys": sorted(set(dependency_work_keys)),
        }
    )
    return "work:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class WorkSpec:
    work_class: FabricWorkClass
    producer: str
    algorithm_version: str
    partition_key: str
    input_identity: str
    coverage: Mapping[str, Any]
    required_capabilities: tuple[FabricCapability, ...]
    priority: float = 0.0
    queue: str = "default"
    max_attempts: int = 8
    provider: str | None = None
    min_memory_bytes: int = 0
    network_class: str | None = None
    dependency_work_keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "work_class", FabricWorkClass(self.work_class))
        object.__setattr__(
            self,
            "required_capabilities",
            tuple(FabricCapability(item) for item in self.required_capabilities),
        )
        object.__setattr__(self, "coverage", dict(self.coverage))
        object.__setattr__(
            self,
            "dependency_work_keys",
            tuple(sorted(set(self.dependency_work_keys))),
        )
        for name in (
            "producer",
            "algorithm_version",
            "partition_key",
            "input_identity",
            "queue",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")
        if len(set(self.required_capabilities)) != len(self.required_capabilities):
            raise ValueError("required_capabilities must be unique")
        if (
            isinstance(self.priority, bool)
            or not isinstance(self.priority, (int, float))
            or not math.isfinite(float(self.priority))
        ):
            raise ValueError("priority must be finite")
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts < 1
        ):
            raise ValueError("max_attempts must be a positive integer")
        if (
            isinstance(self.min_memory_bytes, bool)
            or not isinstance(self.min_memory_bytes, int)
            or self.min_memory_bytes < 0
        ):
            raise ValueError("min_memory_bytes must be non-negative")
        if self.provider is not None and not self.provider.strip():
            raise ValueError("provider must be non-empty when set")
        if self.network_class is not None and not self.network_class.strip():
            raise ValueError("network_class must be non-empty when set")

    @property
    def work_key(self) -> str:
        return deterministic_work_key(
            work_class=self.work_class,
            producer=self.producer,
            algorithm_version=self.algorithm_version,
            partition_key=self.partition_key,
            input_identity=self.input_identity,
            coverage=self.coverage,
            dependency_work_keys=self.dependency_work_keys,
        )


@dataclass(frozen=True, slots=True)
class WorkerDescriptor:
    worker_id: str
    region: str
    runtime_class: str
    architecture: str
    network_class: str
    cpu_count: int
    memory_bytes: int
    capabilities: tuple[FabricCapability, ...]
    allowed_providers: tuple[str, ...] = ()
    max_concurrency: int = 1
    labels: Mapping[str, str] = field(default_factory=dict)
    protocol_version: str = FABRIC_PROTOCOL_VERSION
    edition: str = "unknown"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "capabilities",
            tuple(FabricCapability(item) for item in self.capabilities),
        )
        object.__setattr__(self, "allowed_providers", tuple(self.allowed_providers))
        object.__setattr__(self, "labels", dict(self.labels))
        for name in (
            "worker_id",
            "region",
            "runtime_class",
            "architecture",
            "network_class",
            "protocol_version",
            "edition",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("worker capabilities must be unique")
        if len(set(self.allowed_providers)) != len(self.allowed_providers):
            raise ValueError("allowed_providers must be unique")
        if any(not value.strip() for value in self.allowed_providers):
            raise ValueError("allowed provider names must be non-empty")
        if (
            isinstance(self.cpu_count, bool)
            or not isinstance(self.cpu_count, int)
            or self.cpu_count < 1
        ):
            raise ValueError("cpu_count must be positive")
        if (
            isinstance(self.memory_bytes, bool)
            or not isinstance(self.memory_bytes, int)
            or self.memory_bytes < 0
        ):
            raise ValueError("memory_bytes must be non-negative")
        if (
            isinstance(self.max_concurrency, bool)
            or not isinstance(self.max_concurrency, int)
            or self.max_concurrency < 1
        ):
            raise ValueError("max_concurrency must be positive")


@dataclass(frozen=True, slots=True)
class LeaseToken:
    task_id: str
    work_key: str
    worker_id: str
    lease_epoch: int
    lease_deadline: float
    attempt: int
    work: WorkSpec
    cursor: Mapping[str, Any] | None = None
    next_sequence_no: int = 0

    def __post_init__(self) -> None:
        for name in ("task_id", "work_key", "worker_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")
        if self.work_key != self.work.work_key:
            raise ValueError("lease work_key disagrees with WorkSpec")
        for name in ("lease_epoch", "attempt"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 1
            ):
                raise ValueError(f"{name} must be positive")
        if (
            isinstance(self.next_sequence_no, bool)
            or not isinstance(self.next_sequence_no, int)
            or self.next_sequence_no < 0
        ):
            raise ValueError("next_sequence_no must be non-negative")
        if (
            isinstance(self.lease_deadline, bool)
            or not isinstance(self.lease_deadline, (int, float))
            or not math.isfinite(float(self.lease_deadline))
            or self.lease_deadline <= 0
        ):
            raise ValueError("lease_deadline must be finite and positive")
        if self.cursor is not None:
            object.__setattr__(self, "cursor", dict(self.cursor))


@dataclass(frozen=True, slots=True)
class ResultBatch:
    task_id: str
    lease_epoch: int
    sequence_no: int
    results: tuple[Mapping[str, Any], ...]
    cursor_after: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.task_id.strip():
            raise ValueError("task_id is required")
        if (
            isinstance(self.lease_epoch, bool)
            or not isinstance(self.lease_epoch, int)
            or self.lease_epoch < 1
        ):
            raise ValueError("lease_epoch must be positive")
        if (
            isinstance(self.sequence_no, bool)
            or not isinstance(self.sequence_no, int)
            or self.sequence_no < 0
        ):
            raise ValueError("sequence_no must be non-negative")
        object.__setattr__(self, "results", tuple(dict(item) for item in self.results))
        if self.cursor_after is not None:
            object.__setattr__(self, "cursor_after", dict(self.cursor_after))

    @property
    def payload_digest(self) -> str:
        payload = canonical_json(
            {
                "task_id": self.task_id,
                "lease_epoch": self.lease_epoch,
                "sequence_no": self.sequence_no,
                "results": self.results,
                "cursor_after": self.cursor_after,
            }
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @property
    def batch_key(self) -> str:
        return f"{self.task_id}:{self.sequence_no}"


@dataclass(frozen=True, slots=True)
class ProviderPermit:
    permit_id: str
    provider: str
    worker_id: str
    task_id: str
    lease_epoch: int
    allowed_requests: int
    expires_at: float

    def __post_init__(self) -> None:
        for name in ("permit_id", "provider", "worker_id", "task_id"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} is required")
        if self.lease_epoch < 1 or self.allowed_requests < 1:
            raise ValueError("invalid provider permit counters")
        if not math.isfinite(float(self.expires_at)) or self.expires_at <= 0:
            raise ValueError("expires_at must be finite and positive")
