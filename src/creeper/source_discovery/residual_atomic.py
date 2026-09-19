"""Atomic durable commit for one deterministic residual-search result batch.

Network I/O stays outside the transaction.  Once a bounded provider batch has
completed successfully, every durable consequence of that batch is committed in
one SQLite transaction:

    search episode -> identity -> proposal -> SearchCell economics -> closure

This is deliberately separate from the generic registry helpers.  Those helpers
remain independently transactional for ordinary LLM/source-discovery paths,
while deterministic residual search needs a stronger all-or-nothing boundary so
a process crash cannot turn a half-written novel result into a duplicate on the
retry of the same finite query variant.
"""

from __future__ import annotations

import math
import sqlite3
import uuid
from dataclasses import dataclass, replace
from typing import Callable

from creeper.source_discovery.deterministic_search import (
    DeterministicSearchBatch,
    candidate_from_result,
)
from creeper.source_discovery.models import (
    SourceCandidate,
    SourceState,
    is_common_crawl_provenance,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.research_leads import ResearchLeadLedger
from creeper.source_discovery.residual_search import (
    QueryPlan,
    ResidualSearchLedger,
    query_program_length,
)
from creeper.source_discovery.search_identity import (
    CanonicalSearchResult,
    IdentityRegistration,
    SearchIdentityLedger,
)


@dataclass(frozen=True, slots=True)
class AtomicResidualCommitResult:
    episode_id: str
    registered_count: int
    new_source_count: int
    dropped_count: int


FaultInjector = Callable[[str], None]


def _same_connection(
    registry: SourceDiscoveryRegistry,
    coverage: ResidualSearchLedger,
    identities: SearchIdentityLedger,
    research_leads: ResearchLeadLedger | None = None,
) -> sqlite3.Connection:
    connection = registry.connection
    if coverage.connection is not connection or identities.connection is not connection:
        raise RuntimeError(
            "atomic residual commit requires registry, coverage and identity ledgers "
            "to share one SQLite connection"
        )
    if research_leads is not None and research_leads.connection is not connection:
        raise RuntimeError(
            "research-lead ledger must share the atomic residual SQLite connection"
        )
    return connection


def _identity_exists(
    connection: sqlite3.Connection,
    table: str,
    column: str,
    value: str,
) -> bool:
    return (
        connection.execute(
            f"SELECT 1 FROM {table} WHERE {column}=? LIMIT 1",
            (value,),
        ).fetchone()
        is not None
    )


def _register_identity_locked(
    connection: sqlite3.Connection,
    identities: SearchIdentityLedger,
    *,
    cell_key: str,
    result: CanonicalSearchResult,
) -> IdentityRegistration:
    """SearchIdentityLedger.register(), without opening/closing a transaction."""

    now = float(identities.clock())
    if not math.isfinite(now) or now < 0:
        raise ValueError("search identity clock must be finite and non-negative")
    registration = IdentityRegistration(
        new_url=not _identity_exists(
            connection, "residual_search_urls", "url_key", result.url_key
        ),
        new_artifact=not _identity_exists(
            connection,
            "residual_search_artifacts",
            "artifact_key",
            result.artifact_key,
        ),
        new_dataset=not _identity_exists(
            connection,
            "residual_search_datasets",
            "dataset_key",
            result.dataset_key,
        ),
        new_family=not _identity_exists(
            connection,
            "residual_search_families",
            "family_key",
            result.family_key,
        ),
    )
    connection.execute(
        "INSERT INTO residual_search_urls VALUES(?,?,?,?) "
        "ON CONFLICT(url_key) DO UPDATE SET last_seen_at=excluded.last_seen_at",
        (result.url_key, result.canonical_url, now, now),
    )
    connection.execute(
        "INSERT INTO residual_search_artifacts VALUES(?,?,?,?) "
        "ON CONFLICT(artifact_key) DO UPDATE SET last_seen_at=excluded.last_seen_at",
        (result.artifact_key, result.url_key, now, now),
    )
    connection.execute(
        "INSERT INTO residual_search_families VALUES(?,?,?,?) "
        "ON CONFLICT(family_key) DO UPDATE SET last_seen_at=excluded.last_seen_at",
        (result.family_key, result.family_label, now, now),
    )
    connection.execute(
        "INSERT INTO residual_search_datasets VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(dataset_key) DO UPDATE SET last_seen_at=excluded.last_seen_at",
        (
            result.dataset_key,
            result.family_key,
            result.raw.title,
            result.raw.publisher,
            now,
            now,
        ),
    )
    connection.execute(
        """
        INSERT INTO residual_search_references
        VALUES(?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(provider,provider_result_id,cell_key) DO UPDATE SET
            url_key=excluded.url_key,
            artifact_key=excluded.artifact_key,
            dataset_key=excluded.dataset_key,
            family_key=excluded.family_key,
            relevance_score=excluded.relevance_score,
            qualified=excluded.qualified,
            seen_at=excluded.seen_at
        """,
        (
            result.raw.provider,
            result.raw.provider_result_id,
            cell_key,
            result.url_key,
            result.artifact_key,
            result.dataset_key,
            result.family_key,
            result.relevance_score,
            int(result.qualified),
            now,
        ),
    )
    return registration


def _register_proposal_locked(
    connection: sqlite3.Connection,
    registry: SourceDiscoveryRegistry,
    *,
    candidate: SourceCandidate,
    episode_id: str,
) -> bool:
    """SourceDiscoveryRegistry.register_proposal(), without an inner commit."""

    if is_common_crawl_provenance(
        candidate.source_family,
        candidate.canonical_entrypoint,
        candidate.discovered_by,
    ):
        raise ValueError("Common Crawl corpus is excluded from the active candidate pool")
    now = registry._now()
    inserted = (
        connection.execute(
            """
            INSERT OR IGNORE INTO source_candidates(
                source_key, canonical_entrypoint, source_family, source_level,
                discovered_by, discovery_strategy, expected_year_from,
                expected_year_to, expected_volume, temporal_semantics_prior,
                enumerability_prior, direct_evidence_prior,
                baseline_overlap_prior, access_cost_prior, adapter_cost_prior,
                confidence, state, state_reason, activation_retry_at,
                activation_attempts, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                candidate.source_key,
                candidate.canonical_entrypoint,
                candidate.source_family,
                candidate.level.value,
                candidate.discovered_by,
                candidate.discovery_strategy,
                candidate.expected_year_from,
                candidate.expected_year_to,
                candidate.expected_volume,
                candidate.temporal_semantics_prior,
                candidate.enumerability_prior,
                candidate.direct_evidence_prior,
                candidate.baseline_overlap_prior,
                candidate.access_cost_prior,
                candidate.adapter_cost_prior,
                candidate.confidence,
                candidate.state.value,
                candidate.state_reason,
                candidate.activation_retry_at,
                candidate.activation_attempts,
                now,
                now,
            ),
        ).rowcount
        == 1
    )
    if (
        connection.execute(
            "SELECT 1 FROM source_candidates WHERE source_key=?",
            (candidate.source_key,),
        ).fetchone()
        is None
    ):
        raise RuntimeError("candidate identity collision prevented residual proposal commit")
    connection.execute(
        """
        INSERT INTO source_proposals(
            proposal_id, episode_id, source_key, discovered_by,
            discovery_strategy, source_family, source_level, confidence,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"proposal:{uuid.uuid4().hex}",
            episode_id,
            candidate.source_key,
            candidate.discovered_by,
            candidate.discovery_strategy,
            candidate.source_family,
            candidate.level.value,
            candidate.confidence,
            now,
        ),
    )
    return inserted


def _should_saturate_locked(
    connection: sqlite3.Connection,
    coverage: ResidualSearchLedger,
    plan: QueryPlan,
) -> bool:
    row = connection.execute(
        """
        SELECT attempts, result_count, duplicate_results, new_families,
               qualified_roots
        FROM residual_search_cells
        WHERE cell_key=?
        """,
        (plan.cell.key,),
    ).fetchone()
    if row is None:
        raise RuntimeError("residual search cell disappeared during atomic commit")
    attempts = int(row["attempts"])
    result_count = int(row["result_count"])
    duplicate_results = int(row["duplicate_results"])
    new_families = int(row["new_families"])
    qualified_roots = int(row["qualified_roots"])
    if result_count == 0:
        return attempts >= query_program_length(plan.cell)

    policy = coverage.policy
    if attempts < policy.saturation_min_attempts:
        return False
    if result_count < policy.saturation_min_results:
        return False
    duplicate_fraction = min(1.0, duplicate_results / result_count)
    new_family_fraction = min(1.0, new_families / result_count)
    qualified_fraction = min(1.0, qualified_roots / result_count)
    duplicate_saturation = (
        duplicate_fraction >= policy.saturation_duplicate_fraction
        and new_family_fraction <= policy.saturation_max_new_family_fraction
    )
    relevance_saturation = (
        qualified_fraction <= policy.saturation_max_qualified_fraction
    )
    return duplicate_saturation or relevance_saturation


def commit_deterministic_residual_batch(
    registry: SourceDiscoveryRegistry,
    coverage: ResidualSearchLedger,
    identities: SearchIdentityLedger,
    *,
    plan: QueryPlan,
    batch: DeterministicSearchBatch,
    search_cost_seconds: float,
    candidate_cap: int,
    research_leads: ResearchLeadLedger | None = None,
    fault_injector: FaultInjector | None = None,
) -> AtomicResidualCommitResult:
    """Commit one completed QueryPlan all-or-nothing.

    ``fault_injector`` is intentionally test-only: raising from any named stage
    proves that SQLite rollback restores the exact pre-episode state.
    """

    if (
        isinstance(search_cost_seconds, bool)
        or not isinstance(search_cost_seconds, (int, float))
        or not math.isfinite(float(search_cost_seconds))
        or search_cost_seconds < 0
    ):
        raise ValueError("search_cost_seconds must be finite and non-negative")
    if isinstance(candidate_cap, bool) or not isinstance(candidate_cap, int) or candidate_cap < 1:
        raise ValueError("candidate_cap must be a positive integer")

    connection = _same_connection(registry, coverage, identities, research_leads)
    if connection.in_transaction:
        raise RuntimeError("atomic residual commit requires a clean SQLite transaction boundary")

    def checkpoint(name: str) -> None:
        if fault_injector is not None:
            fault_injector(name)

    episode_id = f"search:{uuid.uuid4().hex}"
    registered_count = 0
    new_source_count = 0
    dropped = 0
    connection.execute("BEGIN IMMEDIATE")
    try:
        started_at = registry._now()
        connection.execute(
            """
            INSERT INTO source_search_episodes(
                episode_id, strategy, backend, query, actor, started_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                episode_id,
                f"RESIDUAL_CELL:{plan.cell.mechanism}",
                batch.backend,
                batch.query,
                batch.actor,
                started_at,
            ),
        )
        cell_now = float(coverage.clock())
        if not math.isfinite(cell_now) or cell_now < 0:
            raise ValueError("residual search clock must be finite and non-negative")
        connection.execute(
            """
            INSERT INTO residual_search_cells(
                cell_key, mechanism, institution, period, artifact, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(cell_key) DO NOTHING
            """,
            (
                plan.cell.key,
                plan.cell.mechanism,
                plan.cell.institution,
                plan.cell.period,
                plan.cell.artifact,
                cell_now,
            ),
        )
        connection.execute(
            """
            INSERT INTO residual_search_episode_cells(
                episode_id, cell_key, credited_eed, updated_at
            ) VALUES (?, ?, 0, ?)
            """,
            (episode_id, plan.cell.key, cell_now),
        )
        checkpoint("after_episode")

        duplicate_results = 0
        unique_roots = 0
        new_families = 0
        qualified_roots = 0
        family_labels: set[str] = set()
        seen_sources: set[str] = set()

        for result in batch.results:
            registration = _register_identity_locked(
                connection,
                identities,
                cell_key=plan.cell.key,
                result=result,
            )
            family = " ".join(result.family_label.lower().split())
            if family:
                family_labels.add(family)
            duplicate_results += int(not registration.new_family)
            unique_roots += int(registration.new_dataset)
            new_families += int(registration.new_family)

            if research_leads is not None:
                hard_negative = research_leads.hard_negative_match(result)
                if hard_negative is not None:
                    research_leads.record_match_locked(hard_negative.lead_id)
                    dropped += 1
                    continue

            if not result.qualified or not registration.new_dataset:
                dropped += 1
                continue
            qualified_roots += 1
            if registered_count >= candidate_cap:
                dropped += 1
                continue

            recovery = None
            provenance = None
            if research_leads is not None:
                recovery_target = research_leads.lead_for_cell(plan.cell.key)
                recovery = research_leads.exact_recovery_match(
                    plan.cell.key,
                    result,
                )
                if recovery_target is not None and recovery is None:
                    # Targeted-recovery cells are exact finite programs, not a
                    # side door back into broad source discovery. Non-matching
                    # results remain in four-level identity memory only.
                    dropped += 1
                    continue

            candidate = candidate_from_result(plan, result)
            if research_leads is not None:
                if recovery is not None:
                    candidate = replace(
                        candidate,
                        source_family=f"RESEARCH_RECOVERY:{recovery.lead_id}",
                        discovery_strategy="RESEARCH_LEAD_RECOVERY",
                    )
                provenance = research_leads.provenance_hold_match(result)
                if provenance is not None:
                    candidate = replace(
                        candidate,
                        expected_year_from=None,
                        expected_year_to=None,
                        temporal_semantics_prior=0.0,
                        direct_evidence_prior=0.0,
                        state=SourceState.HOLD,
                        state_reason=(
                            "RESEARCH_PROVENANCE_HOLD:" + provenance.lead_id
                        ),
                    )
            if candidate.source_key in seen_sources:
                dropped += 1
                continue
            seen_sources.add(candidate.source_key)
            if (
                registry.suppression_reason(candidate) is not None
                or is_common_crawl_provenance(
                    candidate.source_family,
                    candidate.canonical_entrypoint,
                    candidate.discovered_by,
                )
            ):
                dropped += 1
                continue

            inserted = _register_proposal_locked(
                connection,
                registry,
                candidate=candidate,
                episode_id=episode_id,
            )
            if research_leads is not None:
                if recovery is not None:
                    research_leads.record_match_locked(
                        recovery.lead_id,
                        source_key=candidate.source_key,
                    )
                if provenance is not None:
                    research_leads.record_match_locked(
                        provenance.lead_id,
                        source_key=candidate.source_key,
                    )
            registered_count += 1
            new_source_count += int(inserted)

        checkpoint("after_identity_and_proposals")

        raw_count = len(batch.results)
        cell_now = float(coverage.clock())
        if not math.isfinite(cell_now) or cell_now < 0:
            raise ValueError("residual search clock must be finite and non-negative")
        changed = connection.execute(
            """
            UPDATE residual_search_cells
            SET state = 'ACTIVE',
                attempts = attempts + 1,
                result_count = result_count + ?,
                duplicate_results = duplicate_results + ?,
                unique_roots = unique_roots + ?,
                new_families = new_families + ?,
                qualified_roots = qualified_roots + ?,
                search_cost_seconds = search_cost_seconds + ?,
                variant_cursor = variant_cursor + 1,
                last_searched_at = ?,
                updated_at = ?
            WHERE cell_key = ?
            """,
            (
                raw_count,
                duplicate_results,
                unique_roots,
                new_families,
                qualified_roots,
                float(search_cost_seconds),
                cell_now,
                cell_now,
                plan.cell.key,
            ),
        ).rowcount
        if changed != 1:
            raise RuntimeError("residual search cell update failed during atomic commit")
        for family in family_labels:
            connection.execute(
                """
                INSERT INTO residual_search_cell_families(
                    cell_key, family_key, hit_count, last_seen_at
                ) VALUES (?, ?, 1, ?)
                ON CONFLICT(cell_key, family_key) DO UPDATE SET
                    hit_count = hit_count + 1,
                    last_seen_at = excluded.last_seen_at
                """,
                (plan.cell.key, family, cell_now),
            )
        if _should_saturate_locked(connection, coverage, plan):
            terminal_state = "SATURATED"
        else:
            cursor_row = connection.execute(
                """
                SELECT variant_cursor
                FROM residual_search_cells
                WHERE cell_key=?
                """,
                (plan.cell.key,),
            ).fetchone()
            if cursor_row is None:
                raise RuntimeError(
                    "residual search cell disappeared before terminal-state check"
                )
            terminal_state = (
                "EXHAUSTED"
                if int(cursor_row["variant_cursor"])
                >= query_program_length(plan.cell)
                else None
            )
        if terminal_state is not None:
            connection.execute(
                """
                UPDATE residual_search_cells
                SET state=?, updated_at=?
                WHERE cell_key=?
                """,
                (terminal_state, cell_now, plan.cell.key),
            )
        checkpoint("after_cell")

        finished_at = registry._now()
        changed = connection.execute(
            """
            UPDATE source_search_episodes
            SET finished_at=?, search_cost_seconds=?,
                accepted_proposals=?, new_sources=?
            WHERE episode_id=? AND finished_at IS NULL
            """,
            (
                finished_at,
                float(search_cost_seconds),
                registered_count,
                new_source_count,
                episode_id,
            ),
        ).rowcount
        if changed != 1:
            raise RuntimeError("residual search episode closure failed during atomic commit")
        checkpoint("before_commit")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise

    return AtomicResidualCommitResult(
        episode_id=episode_id,
        registered_count=registered_count,
        new_source_count=new_source_count,
        dropped_count=dropped,
    )
