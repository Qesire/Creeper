"""Wire-neutral models for Creeper Fabric v2."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping

from creeper.distributed.edition import FABRIC_EDITION_VERSION, FABRIC_PROTOCOL_VERSION
from creeper.distributed.identity import artifact_id, batch_id, work_key


class Capability(StrEnum):
    RESIDUAL_QUERY = "RESIDUAL_QUERY"
    HOST_RESOLUTION = "HOST_RESOLUTION"
    STREAMING_BULK = "STREAMING_BULK"
    EVIDENCE_QUERY = "EVIDENCE_QUERY"
    RDAP = "RDAP"
    ARTIFACT_FETCH = "ARTIFACT_FETCH"
    REGION_PROBE = "REGION_PROBE"


class TaskClass(StrEnum):
    RESIDUAL_QUERY = "RESIDUAL_QUERY"
    SOURCE_SHARD = "SOURCE_SHARD"
    HOST_BATCH = "HOST_BATCH"
    EVIDENCE_BATCH = "EVIDENCE_BATCH"
    REGION_PROBE = "REGION_PROBE"


@dataclass(frozen=True, slots=True)
class WorkerDescriptor:
    worker_id: str
    worker_instance_id: str
    runtime_class: str
    region: str
    architecture: str
    memory_bytes: int
    cpu_count: int
    network_class: str
    capabilities: tuple[str, ...]
    producers: tuple[str, ...] = ()
    allowed_providers: tuple[str, ...] = ()
    daily_egress_budget_bytes: int = 0
    protocol_version: str = FABRIC_PROTOCOL_VERSION
    edition_version: str = FABRIC_EDITION_VERSION

    def __post_init__(self) -> None:
        for name in (
            "worker_id",
            "worker_instance_id",
            "runtime_class",
            "region",
            "architecture",
            "network_class",
            "protocol_version",
            "edition_version",
        ):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} is required")
        if self.memory_bytes < 0 or self.cpu_count < 1:
            raise ValueError("invalid worker capacity")
        if self.daily_egress_budget_bytes < 0:
            raise ValueError("daily egress budget must be non-negative")
        for name in ("capabilities", "producers", "allowed_providers"):
            values = tuple(getattr(self, name))
            if len(values) != len(set(values)) or any(not str(v).strip() for v in values):
                raise ValueError(f"{name} must contain unique non-empty values")
        if not self.capabilities or not self.producers:
            raise ValueError(
                "workers must explicitly declare non-empty capabilities and producers"
            )


@dataclass(frozen=True, slots=True)
class WorkDefinition:
    producer: str
    task_class: TaskClass
    input_identity: str
    payload: Mapping[str, Any]
    partition: str
    algorithm_version: str
    required_capabilities: tuple[str, ...]
    required_providers: tuple[str, ...] = ()
    priority: float = 0.0
    max_attempts: int = 8
    not_before: float = 0.0

    def __post_init__(self) -> None:
        if not self.producer.strip() or not self.input_identity.strip():
            raise ValueError("producer and input_identity are required")
        if not self.algorithm_version.strip():
            raise ValueError("algorithm_version is required")
        if len(self.required_capabilities) != len(set(self.required_capabilities)):
            raise ValueError("required capabilities must be unique")
        if len(self.required_providers) != len(set(self.required_providers)):
            raise ValueError("required providers must be unique")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if not math.isfinite(float(self.priority)):
            raise ValueError("priority must be finite")
        if not math.isfinite(float(self.not_before)) or self.not_before < 0:
            raise ValueError("not_before must be finite and non-negative")

    @property
    def work_key(self) -> str:
        return work_key(
            producer=self.producer,
            input_identity=self.input_identity,
            payload=self.payload,
            partition=self.partition,
            algorithm_version=self.algorithm_version,
        )


@dataclass(frozen=True, slots=True)
class TaskLease:
    task_id: str
    work_key: str
    worker_id: str
    worker_instance_id: str
    generation: int
    lease_deadline: float
    attempt: int
    work: WorkDefinition
    cursor: str | None = None
    next_sequence_no: int = 0

    def __post_init__(self) -> None:
        if self.generation < 1 or self.attempt < 1 or self.next_sequence_no < 0:
            raise ValueError("invalid lease counters")


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    uri: str
    sha256: str
    size_bytes: int
    content_type: str = "application/octet-stream"
    compression: str = "none"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.uri.strip():
            raise ValueError("artifact uri is required")
        artifact_id(self.sha256)
        if self.size_bytes < 0:
            raise ValueError("artifact size must be non-negative")
        if not self.content_type.strip() or not self.compression.strip():
            raise ValueError("artifact content_type and compression are required")

    @property
    def artifact_id(self) -> str:
        return artifact_id(self.sha256)


@dataclass(frozen=True, slots=True)
class ResultBatch:
    task_id: str
    generation: int
    sequence_no: int
    results: tuple[Mapping[str, Any], ...]
    artifacts: tuple[ArtifactRef, ...] = ()
    cursor_after: str | None = None
    final: bool = False

    def __post_init__(self) -> None:
        if not self.task_id.strip() or self.generation < 1 or self.sequence_no < 0:
            raise ValueError("invalid result batch identity")

    @property
    def batch_id(self) -> str:
        return batch_id(self.task_id, self.sequence_no)


@dataclass(frozen=True, slots=True)
class ProviderPermit:
    permit_id: str
    request_id: str
    provider: str
    worker_id: str
    worker_instance_id: str
    task_id: str
    generation: int
    allowed_requests: int
    max_inflight: int
    expires_at: float
