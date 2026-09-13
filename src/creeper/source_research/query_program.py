"""Deterministic query-program construction and seed replay identity."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .models import QueryProgram, RootQuery, canonical_seed_identity


def seed_identity(
    root_id: str,
    seed_library_version: str,
    query_text: str,
    native_filters: Mapping[str, Any] | None = None,
) -> str:
    return canonical_seed_identity(root_id, seed_library_version, query_text, native_filters)


def build_seed_program(
    *,
    root_id: str,
    strategy: str,
    seed_library_version: str,
    queries: Iterable[RootQuery | str],
    hard_max_requests: int,
    stop_conditions: Iterable[str],
) -> QueryProgram:
    normalized: list[RootQuery] = []
    seen: set[str] = set()
    for item in queries:
        original = item if isinstance(item, RootQuery) else RootQuery(root_id, str(item))
        query = RootQuery(
            root_id=root_id,
            query_text=original.query_text,
            max_pages=original.max_pages,
            max_wall_seconds=original.max_wall_seconds,
            page_size=original.page_size,
            native_filters=original.native_filters,
            expected_signal=original.expected_signal,
            expected_artifact_family=original.expected_artifact_family,
            seed_library_version=seed_library_version,
        )
        if query.seed_identity in seen:
            continue
        seen.add(query.seed_identity)
        normalized.append(query)
    return QueryProgram(
        root_id=root_id,
        strategy=strategy,
        queries=tuple(normalized),
        hard_max_requests=hard_max_requests,
        stop_conditions=tuple(stop_conditions),
        seed_library_version=seed_library_version,
        compiler_version="deterministic-seed-v1",
        source="SEED",
    )


__all__ = ["QueryProgram", "RootQuery", "build_seed_program", "seed_identity"]
