"""Deterministic identities for the distributed evidence fabric."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from creeper.authority.normalizer import normalize_official


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def stable_identity(kind: str, payload: Mapping[str, Any]) -> str:
    """Hash a typed canonical payload.

    The type prefix prevents accidental cross-namespace collisions when two
    objects happen to serialize to identical JSON.
    """

    if not kind.strip():
        raise ValueError("identity kind is required")
    digest = hashlib.sha256()
    digest.update(kind.encode("utf-8"))
    digest.update(b"\0")
    digest.update(_canonical_json(payload).encode("utf-8"))
    return digest.hexdigest()


def canonical_hostname(hostname: str) -> str:
    normalized = normalize_official(hostname)
    if normalized is None:
        raise ValueError(f"invalid hostname: {hostname!r}")
    return normalized


def host_id(hostname: str) -> str:
    return stable_identity("host", {"hostname": canonical_hostname(hostname)})


def host_year_id(hostname: str, year: int) -> str:
    if not 1996 <= int(year) <= 2001:
        raise ValueError("year must be within 1996-2001")
    return stable_identity(
        "host-year",
        {"hostname": canonical_hostname(hostname), "year": int(year)},
    )


def source_id(
    source_type: str,
    canonical_locator: str,
    semantic_parameters: Mapping[str, Any] | None = None,
) -> str:
    if not source_type.strip() or not canonical_locator.strip():
        raise ValueError("source_type and canonical_locator are required")
    return stable_identity(
        "source",
        {
            "source_type": source_type.strip(),
            "locator": canonical_locator.strip(),
            "semantic_parameters": dict(semantic_parameters or {}),
        },
    )


def source_candidate_id(canonical_url: str) -> str:
    if not canonical_url.strip():
        raise ValueError("canonical source URL is required")
    return stable_identity(
        "source-candidate",
        {"url": canonical_url.strip()},
    )


def evidence_id(
    *,
    hostname: str,
    year: int,
    evidence_class: str,
    source: str,
    timestamp: str,
    locator: str,
) -> str:
    if not evidence_class.strip() or not source.strip() or not locator.strip():
        raise ValueError("evidence class, source, and locator are required")
    return stable_identity(
        "evidence",
        {
            "hostname": canonical_hostname(hostname),
            "year": int(year),
            "evidence_class": evidence_class.strip(),
            "source": source.strip(),
            "timestamp": str(timestamp),
            "locator": locator.strip(),
        },
    )


def work_key(
    *,
    producer: str,
    input_identity: str,
    coverage: Mapping[str, Any],
    partition: str,
    algorithm_version: str,
) -> str:
    if not producer.strip() or not input_identity.strip() or not algorithm_version.strip():
        raise ValueError("producer, input_identity and algorithm_version are required")
    return stable_identity(
        "work",
        {
            "producer": producer.strip(),
            "input": input_identity.strip(),
            "coverage": dict(coverage),
            "partition": partition.strip(),
            "algorithm_version": algorithm_version.strip(),
        },
    )


def resolution_key(
    *,
    hostname: str,
    provider: str,
    scope: str,
    coverage: Mapping[str, Any],
    resolver_version: str,
) -> str:
    if not provider.strip() or not scope.strip() or not resolver_version.strip():
        raise ValueError("provider, scope and resolver_version are required")
    return stable_identity(
        "resolution",
        {
            "hostname": canonical_hostname(hostname),
            "provider": provider.strip(),
            "scope": scope.strip(),
            "coverage": dict(coverage),
            "resolver_version": resolver_version.strip(),
        },
    )


def batch_id(task_id: str, sequence_no: int) -> str:
    if not task_id.strip() or sequence_no < 0:
        raise ValueError("task_id is required and sequence_no must be non-negative")
    return stable_identity(
        "result-batch",
        {"task_id": task_id.strip(), "sequence_no": int(sequence_no)},
    )
