"""Durable contracts for bounded deterministic exploration regions."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class RegionState(StrEnum):
    PROPOSED = "PROPOSED"
    VALIDATED = "VALIDATED"
    READY = "READY"
    RUNNING = "RUNNING"
    EXHAUSTED = "EXHAUSTED"
    HOLD = "HOLD"
    REJECTED = "REJECTED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"


_REGION_TRANSITIONS: dict[RegionState, frozenset[RegionState]] = {
    RegionState.PROPOSED: frozenset({RegionState.VALIDATED, RegionState.REJECTED}),
    RegionState.VALIDATED: frozenset(
        {RegionState.READY, RegionState.HOLD, RegionState.REJECTED}
    ),
    RegionState.READY: frozenset(
        {RegionState.RUNNING, RegionState.HOLD, RegionState.REJECTED}
    ),
    RegionState.RUNNING: frozenset(
        {
            RegionState.EXHAUSTED,
            RegionState.HOLD,
            RegionState.FAILED_RETRYABLE,
            RegionState.REJECTED,
        }
    ),
    RegionState.FAILED_RETRYABLE: frozenset(
        {RegionState.READY, RegionState.RUNNING, RegionState.REJECTED}
    ),
    RegionState.HOLD: frozenset({RegionState.READY, RegionState.REJECTED}),
    RegionState.EXHAUSTED: frozenset(),
    RegionState.REJECTED: frozenset(),
}


def legal_region_transition(current: RegionState, target: RegionState) -> bool:
    """Return whether the durable region state machine permits this transition."""
    return RegionState(target) in _REGION_TRANSITIONS[RegionState(current)]


@dataclass(frozen=True)
class ExplorationRegion:
    """Persisted semantic identity for one finite deterministic search region."""

    region_id: str
    surface_kind: str
    root: str
    purpose: str
    query_family_json: str
    enumerator_spec_json: str
    artifact_predicate_json: str
    hard_bounds_json: str
    stop_conditions_json: str
    expected_source_family: str
    expected_contract_family: str
    context_hash: str
    created_by_episode_id: str | None = None
    compiler_version: str = "v7-integrated-l1"
    region_key: str = ""
    state: RegionState = RegionState.PROPOSED
    state_reason: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0

    def __post_init__(self) -> None:
        for name in (
            "region_id",
            "surface_kind",
            "root",
            "purpose",
            "expected_source_family",
            "expected_contract_family",
            "context_hash",
            "compiler_version",
        ):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} is required")
        for name in (
            "query_family_json",
            "enumerator_spec_json",
            "artifact_predicate_json",
            "hard_bounds_json",
            "stop_conditions_json",
        ):
            value = str(getattr(self, name))
            try:
                json.loads(value)
            except (TypeError, json.JSONDecodeError) as exc:
                raise ValueError(f"{name} must contain valid JSON") from exc
        object.__setattr__(self, "state", RegionState(self.state))
        key = self.region_key or self.compute_region_key()
        if not key.startswith("region:"):
            raise ValueError("region_key must use the region: prefix")
        object.__setattr__(self, "region_key", key)

    def semantic_identity(self) -> dict[str, Any]:
        """Fields that alter deterministic execution and therefore dedup identity."""
        return {
            "surface_kind": self.surface_kind,
            "root": self.root,
            "purpose": self.purpose,
            "query_family": json.loads(self.query_family_json),
            "enumerator": json.loads(self.enumerator_spec_json),
            "artifact_predicate": json.loads(self.artifact_predicate_json),
            "hard_bounds": json.loads(self.hard_bounds_json),
            "stop_conditions": json.loads(self.stop_conditions_json),
            "expected_source_family": self.expected_source_family,
            "expected_contract_family": self.expected_contract_family,
            "context_hash": self.context_hash,
            "compiler_version": self.compiler_version,
        }

    def compute_region_key(self) -> str:
        payload = json.dumps(
            self.semantic_identity(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        return "region:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()
