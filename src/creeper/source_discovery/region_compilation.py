"""Versioned, deterministic compilation contracts for V7 exploration regions."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from creeper.source_discovery.models import SourceCandidate
from creeper.source_discovery.query_family import QueryFamily


COMPILER_VERSION = "v7-deterministic-1"
ENUMERATOR_VERSION = "v7-enumerators-1"


@dataclass(frozen=True)
class ExecutionBounds:
    max_queries: int = 10_000
    max_pages: int = 10_000
    max_artifacts: int = 100_000
    max_requests: int = 10_000
    max_bytes: int = 1_000_000_000
    max_wall_seconds: float = 3600.0

    def __post_init__(self) -> None:
        for name in ("max_queries", "max_pages", "max_artifacts", "max_requests", "max_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.max_wall_seconds < 0:
            raise ValueError("max_wall_seconds must be non-negative")


@dataclass(frozen=True)
class EnumeratorSpec:
    kind: str
    config: Mapping[str, Any]

    def __post_init__(self) -> None:
        kind = str(self.kind).upper()
        allowed = {"STATIC_LIST", "HTML_CATALOG", "INTEGER_PAGINATION", "CURSOR_API", "FILENAME_PATTERN"}
        if kind not in allowed:
            raise ValueError(f"unsupported deterministic enumerator: {kind}")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "config", dict(self.config))


@dataclass(frozen=True)
class CompiledScoutPlan:
    region_id: str
    region_key: str
    source_family: str
    surface_kind: str
    root: str
    query_family: QueryFamily | None
    enumerator: EnumeratorSpec
    artifact_predicate: Callable[[str], bool] | None
    hard_bounds: ExecutionBounds
    stop_conditions: tuple[str, ...]
    expected_contract_family: str | None = None
    compiler_version: str = COMPILER_VERSION
    enumerator_version: str = ENUMERATOR_VERSION

    def __post_init__(self) -> None:
        if not self.region_id.strip() or not self.region_key.strip():
            raise ValueError("region identity is required")
        if not self.source_family.strip() or not self.root.strip():
            raise ValueError("region source family and root are required")
        if not self.stop_conditions:
            raise ValueError("at least one stop condition is required")
        if self.query_family is not None and self.query_family.cardinality > self.hard_bounds.max_queries:
            raise ValueError("query-family expansion exceeds max_queries")


def compile_query_family(
    template: str,
    dimensions: Mapping[str, Any],
    *,
    max_queries: int,
) -> QueryFamily:
    """Materialize and validate a finite query family before any I/O."""
    family = QueryFamily.from_mapping(template, dimensions)
    family.expand(max_queries=max_queries)
    return family
