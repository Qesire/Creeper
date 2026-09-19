"""Deterministic identities for distributed Creeper."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from creeper.authority.normalizer import normalize_official


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def stable_identity(kind: str, payload: Mapping[str, Any]) -> str:
    if not kind.strip():
        raise ValueError("identity kind is required")
    digest = hashlib.sha256()
    digest.update(kind.encode("utf-8"))
    digest.update(b"\0")
    digest.update(canonical_json(payload).encode("utf-8"))
    return digest.hexdigest()


def canonical_hostname(hostname: str) -> str:
    value = normalize_official(hostname)
    if value is None:
        raise ValueError(f"invalid hostname: {hostname!r}")
    return value


def work_key(
    *,
    producer: str,
    input_identity: str,
    payload: Mapping[str, Any],
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
            "payload": dict(payload),
            "partition": partition.strip(),
            "algorithm_version": algorithm_version.strip(),
        },
    )


def batch_id(task_id: str, sequence_no: int) -> str:
    if not task_id.strip() or sequence_no < 0:
        raise ValueError("invalid result batch identity")
    return stable_identity(
        "result-batch",
        {"task_id": task_id.strip(), "sequence_no": int(sequence_no)},
    )


def artifact_id(sha256_hex: str) -> str:
    value = sha256_hex.strip().lower()
    if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
        raise ValueError("artifact sha256 must be a 64-character hex digest")
    return stable_identity("artifact", {"sha256": value})
