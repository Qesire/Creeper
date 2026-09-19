"""Distributed Creeper vNext control-plane primitives.

This package is deliberately side-by-side with the current production runtime.
It adds cross-machine authority semantics without changing existing local
execution paths until the distributed production gates are satisfied.
"""

from creeper.distributed.authority_store import (
    BatchConflictError,
    DistributedAuthorityStore,
    StaleLeaseError,
)
from creeper.distributed.models import (
    Capability,
    ProviderPermit,
    ResultBatch,
    TaskClass,
    TaskLease,
    WorkDefinition,
    WorkerDescriptor,
)

__all__ = [
    "BatchConflictError",
    "Capability",
    "DistributedAuthorityStore",
    "ProviderPermit",
    "ResultBatch",
    "StaleLeaseError",
    "TaskClass",
    "TaskLease",
    "WorkDefinition",
    "WorkerDescriptor",
]
