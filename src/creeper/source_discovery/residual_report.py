"""Read-only calibration report for deterministic residual search.

The report intentionally observes existing control-plane tables without changing
scheduler state. It distinguishes cumulative SearchCell metrics, per-query-shape
search economics, and provider discovery footprint. Provider-level FINAL EED is
not reported because current durable attribution is cell/search-episode based,
not provider based.
"""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from creeper.source_discovery.residual_search import (
    MECHANISM_QUERY_TERMS,
    SearchCell,
    query_program_length,
)


REPORT_VERSION = "residual-search-calibration-v1"
_REQUIRED_TABLES = frozenset(
    {
        "residual_search_meta",
        "residual_search_cells",
        "residual_search_episode_cells",
        "residual_search_references",
        "source_search_episodes",
    }
)
_QUERY_SHAPES = ("STRICT_4D", "RELAX_INSTITUTION")


class ResidualSearchReportError(RuntimeError):
    """Raised when a control database cannot support a trustworthy report."""


def _rows(
    connection: sqlite3.Connection,
    sql: str,
    parameters: Iterable[object] = (),
) -> list[dict[str, Any]]:
    cursor = connection.execute(sql, tuple(parameters))
    columns = tuple(item[0] for item in cursor.description or ())
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    value = float(numerator) / float(denominator)
    return value if math.isfinite(value) else 0.0


def _cell_from_row(row: dict[str, Any]) -> SearchCell:
    return SearchCell(
        mechanism=str(row["mechanism"]),
        institution=str(row["institution"]),
        period=str(row["period"]),
        artifact=str(row["artifact"]),
    )


def _cell_payload(row: dict[str, Any]) -> dict[str, Any]:
    cell = _cell_from_row(row)
    program_length = query_program_length(cell)
    attempts = int(row["attempts"])
    results = int(row["result_count"])
    duplicates = int(row["duplicate_results"])
    qualified = int(row["qualified_roots"])
    new_families = int(row["new_families"])
    cost = float(row["search_cost_seconds"])
    eed = float(row["accepted_novel_eed"])
    cursor = int(row["variant_cursor"])
    return {
        "cell_key": str(row["cell_key"]),
        "mechanism": cell.mechanism,
        "institution": cell.institution,
        "period": cell.period,
        "artifact": cell.artifact,
        "state": str(row["state"]),
        "attempts": attempts,
        "program_length": program_length,
        "variant_cursor": cursor,
        "program_coverage_fraction": min(1.0, _safe_ratio(cursor, program_length)),
        "result_count": results,
        "duplicate_results": duplicates,
        "duplicate_fraction": _safe_ratio(duplicates, results),
        "unique_roots": int(row["unique_roots"]),
        "new_families": new_families,
        "new_family_fraction": _safe_ratio(new_families, results),
        "qualified_roots": qualified,
        "qualified_fraction": _safe_ratio(qualified, results),
        "accepted_novel_eed": eed,
        "search_cost_seconds": cost,
        "accepted_novel_eed_per_search_second": _safe_ratio(eed, cost),
        "last_searched_at": row["last_searched_at"],
    }


def _sum_cells(cells: Iterable[dict[str, Any]]) -> dict[str, Any]:
    items = list(cells)
    results = sum(int(item["result_count"]) for item in items)
    duplicates = sum(int(item["duplicate_results"]) for item in items)
    qualified = sum(int(item["qualified_roots"]) for item in items)
    new_families = sum(int(item["new_families"]) for item in items)
    eed = sum(float(item["accepted_novel_eed"]) for item in items)
    cost = sum(float(item["search_cost_seconds"]) for item in items)
    return {
        "cells": len(items),
        "attempts": sum(int(item["attempts"]) for item in items),
        "result_count": results,
        "duplicate_results": duplicates,
        "duplicate_fraction": _safe_ratio(duplicates, results),
        "unique_roots": sum(int(item["unique_roots"]) for item in items),
        "new_families": new_families,
        "new_family_fraction": _safe_ratio(new_families, results),
        "qualified_roots": qualified,
        "qualified_fraction": _safe_ratio(qualified, results),
        "accepted_novel_eed": eed,
        "search_cost_seconds": cost,
        "accepted_novel_eed_per_search_second": _safe_ratio(eed, cost),
        "mean_program_coverage_fraction": (
            sum(float(item["program_coverage_fraction"]) for item in items) / len(items)
            if items
            else 0.0
        ),
    }


def _group_cells(
    cells: list[dict[str, Any]],
    dimension: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in cells:
        grouped[str(item[dimension])].append(item)
    output: list[dict[str, Any]] = []
    for value, members in sorted(grouped.items()):
        payload = _sum_cells(members)
        payload[dimension] = value
        output.append(payload)
    return output


def _shape_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = _rows(
        connection,
        """
        SELECT
            m.episode_id,
            m.cell_key,
            e.query,
            e.started_at,
            e.search_cost_seconds,
            e.accepted_novel_eed,
            e.accepted_proposals,
            e.new_sources,
            c.mechanism,
            c.institution,
            c.period,
            c.artifact
        FROM residual_search_episode_cells AS m
        JOIN source_search_episodes AS e
          ON e.episode_id = m.episode_id
        JOIN residual_search_cells AS c
          ON c.cell_key = m.cell_key
        ORDER BY m.cell_key, e.started_at, m.episode_id
        """,
    )
    sequence: dict[str, int] = defaultdict(int)
    output: list[dict[str, Any]] = []
    for row in rows:
        cell_key = str(row["cell_key"])
        index = sequence[cell_key]
        sequence[cell_key] += 1
        mechanism = str(row["mechanism"])
        terms = MECHANISM_QUERY_TERMS[mechanism]
        step = index % (len(terms) * len(_QUERY_SHAPES))
        phrase = terms[step // len(_QUERY_SHAPES)]
        shape = _QUERY_SHAPES[step % len(_QUERY_SHAPES)]
        output.append(
            {
                "episode_id": str(row["episode_id"]),
                "cell_key": cell_key,
                "mechanism": mechanism,
                "institution": str(row["institution"]),
                "period": str(row["period"]),
                "artifact": str(row["artifact"]),
                "query_shape": shape,
                "mechanism_phrase": phrase,
                "query": str(row["query"]),
                "search_cost_seconds": float(row["search_cost_seconds"]),
                "accepted_proposals": int(row["accepted_proposals"]),
                "new_sources": int(row["new_sources"]),
                "accepted_novel_eed": float(row["accepted_novel_eed"]),
            }
        )
    return output


def _group_shape_rows(
    episodes: list[dict[str, Any]],
    key_fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for item in episodes:
        key = tuple(str(item[field]) for field in key_fields)
        grouped[key].append(item)
    output: list[dict[str, Any]] = []
    for key, members in sorted(grouped.items()):
        cost = sum(float(item["search_cost_seconds"]) for item in members)
        eed = sum(float(item["accepted_novel_eed"]) for item in members)
        payload: dict[str, Any] = {
            field: value for field, value in zip(key_fields, key, strict=True)
        }
        payload.update(
            {
                "episodes": len(members),
                "search_cost_seconds": cost,
                "accepted_proposals": sum(
                    int(item["accepted_proposals"]) for item in members
                ),
                "new_sources": sum(int(item["new_sources"]) for item in members),
                "accepted_novel_eed": eed,
                "accepted_novel_eed_per_search_second": _safe_ratio(eed, cost),
            }
        )
        output.append(payload)
    return output


def _provider_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    return _rows(
        connection,
        """
        WITH dataset_provider_count AS (
            SELECT dataset_key, COUNT(DISTINCT provider) AS provider_count
            FROM residual_search_references
            GROUP BY dataset_key
        ),
        family_provider_count AS (
            SELECT family_key, COUNT(DISTINCT provider) AS provider_count
            FROM residual_search_references
            GROUP BY family_key
        )
        SELECT
            r.provider AS provider,
            COUNT(*) AS references,
            SUM(r.qualified) AS qualified_references,
            COUNT(DISTINCT r.url_key) AS distinct_urls,
            COUNT(DISTINCT r.artifact_key) AS distinct_artifacts,
            COUNT(DISTINCT r.dataset_key) AS distinct_datasets,
            COUNT(DISTINCT r.family_key) AS distinct_families,
            COUNT(DISTINCT CASE
                WHEN d.provider_count = 1 THEN r.dataset_key END
            ) AS exclusive_datasets,
            COUNT(DISTINCT CASE
                WHEN d.provider_count > 1 THEN r.dataset_key END
            ) AS shared_datasets,
            COUNT(DISTINCT CASE
                WHEN f.provider_count = 1 THEN r.family_key END
            ) AS exclusive_families,
            COUNT(DISTINCT CASE
                WHEN f.provider_count > 1 THEN r.family_key END
            ) AS shared_families,
            AVG(r.relevance_score) AS mean_relevance_score
        FROM residual_search_references AS r
        JOIN dataset_provider_count AS d
          ON d.dataset_key = r.dataset_key
        JOIN family_provider_count AS f
          ON f.family_key = r.family_key
        GROUP BY r.provider
        ORDER BY r.provider
        """,
    )


def _provider_overlap(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = _rows(
        connection,
        """
        SELECT
            a.provider AS provider_a,
            b.provider AS provider_b,
            COUNT(DISTINCT a.dataset_key) AS shared_datasets,
            COUNT(DISTINCT a.family_key) AS shared_families
        FROM residual_search_references AS a
        JOIN residual_search_references AS b
          ON a.provider < b.provider
         AND (
              a.dataset_key = b.dataset_key
              OR a.family_key = b.family_key
         )
        GROUP BY a.provider, b.provider
        ORDER BY a.provider, b.provider
        """,
    )
    return rows


def build_residual_search_report(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    """Build a calibration report from an initialized control database."""

    missing = sorted(_REQUIRED_TABLES - _table_names(connection))
    if missing:
        raise ResidualSearchReportError(
            "control database is missing residual-search tables: " + ", ".join(missing)
        )

    profile_row = connection.execute(
        "SELECT value FROM residual_search_meta WHERE key='search_profile'"
    ).fetchone()
    profile = None if profile_row is None else str(profile_row[0])

    cell_rows = _rows(
        connection,
        """
        SELECT
            cell_key, mechanism, institution, period, artifact, state,
            attempts, result_count, duplicate_results, unique_roots,
            new_families, qualified_roots, accepted_novel_eed,
            search_cost_seconds, variant_cursor, last_searched_at
        FROM residual_search_cells
        ORDER BY mechanism, institution, period, artifact
        """,
    )
    cells = [_cell_payload(row) for row in cell_rows]
    summary = _sum_cells(cells)
    state_counts: dict[str, int] = defaultdict(int)
    for item in cells:
        state_counts[str(item["state"])] += 1
    summary["states"] = dict(sorted(state_counts.items()))

    episodes = _shape_rows(connection)
    providers = _provider_rows(connection)
    for item in providers:
        references = int(item["references"])
        qualified = int(item["qualified_references"] or 0)
        item["qualified_fraction"] = _safe_ratio(qualified, references)
        item["mean_relevance_score"] = float(item["mean_relevance_score"] or 0.0)

    return {
        "report_version": REPORT_VERSION,
        "search_profile": profile,
        "summary": summary,
        "query_shapes": _group_shape_rows(episodes, ("query_shape",)),
        "query_program_steps": _group_shape_rows(
            episodes,
            ("mechanism", "mechanism_phrase", "query_shape"),
        ),
        "providers": providers,
        "provider_overlap": _provider_overlap(connection),
        "dimensions": {
            "mechanism": _group_cells(cells, "mechanism"),
            "institution": _group_cells(cells, "institution"),
            "artifact": _group_cells(cells, "artifact"),
        },
        "cells": cells,
        "limitations": {
            "provider_final_eed_attribution_available": False,
            "provider_metrics_are_discovery_identity_footprints": True,
            "cell_result_metrics_are_cumulative": True,
            "query_shape_result_counts_available": False,
            "query_shape_economics_use_search_episode_attribution": True,
        },
    }


def load_residual_search_report(path: Path) -> dict[str, Any]:
    """Open one control database read-only and build its calibration report."""

    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ResidualSearchReportError(f"control database does not exist: {resolved}")
    connection = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    try:
        return build_residual_search_report(connection)
    finally:
        connection.close()
