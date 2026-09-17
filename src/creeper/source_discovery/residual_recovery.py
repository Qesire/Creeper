"""Restart/upgrade recovery for deterministic residual-search durability.

Two historical protocol versions could persist state that the current runtime
must not interpret as completed coverage:

* a QueryPlan could be committed after only a subset of configured providers
  returned successfully;
* HTTP 2xx with an unrecognized provider response envelope could be normalized
  to a legitimate empty result.

Additionally, pre-atomic runtimes could crash between identity/proposal writes
and SearchCell/episode closure.  The current commit path prevents new partial
states, but startup must conservatively neutralize any state already present.

Recovery never touches baseline/evidence/production authority.  It operates only
on residual-search coverage, search-result identity/reference memory, unfinished
residual search episodes and their proposal attribution.
"""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass

from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.residual_search import ResidualSearchLedger
from creeper.source_discovery.search_identity import SearchIdentityLedger


RESIDUAL_PROTOCOL_REVISION = "residual-provider-completion-v2"
_REVISION_KEY = "residual_protocol_revision"
_RECOVERY_AUDIT_KEY = "residual_protocol_recovery_last"


@dataclass(frozen=True, slots=True)
class ResidualRecoveryReport:
    protocol_reset: bool
    dirty_episodes: int
    reset_cells: int
    references_removed: int
    proposals_removed: int
    reward_attributions_removed: int
    orphan_identities_removed: int

    @property
    def changed(self) -> bool:
        return self.protocol_reset or self.dirty_episodes > 0


def _same_connection(
    registry: SourceDiscoveryRegistry,
    coverage: ResidualSearchLedger,
    identities: SearchIdentityLedger,
) -> sqlite3.Connection:
    connection = registry.connection
    if coverage.connection is not connection or identities.connection is not connection:
        raise RuntimeError(
            "residual recovery requires registry, coverage and identity ledgers "
            "to share one SQLite connection"
        )
    return connection


def _placeholders(values: tuple[str, ...]) -> str:
    return ",".join("?" for _ in values)


def recover_residual_protocol_state(
    registry: SourceDiscoveryRegistry,
    coverage: ResidualSearchLedger,
    identities: SearchIdentityLedger,
) -> ResidualRecoveryReport:
    """Converge legacy residual state before the first planning cycle.

    A protocol revision mismatch intentionally resets all residual SearchCell
    coverage and four-level search identity.  Candidate/evidence/production
    state remains intact; rediscovery therefore cannot manufacture score, while
    the finite query program is allowed to execute under the new completion
    semantics.

    Once the revision is current, recovery is cell-local: only SearchCells bound
    to unfinished legacy residual episodes are reopened and have their
    references discarded.  Identity rows that remain referenced by other cells
    survive.
    """

    connection = _same_connection(registry, coverage, identities)
    if connection.in_transaction:
        raise RuntimeError("residual recovery requires a clean SQLite transaction boundary")

    revision_row = connection.execute(
        "SELECT value FROM residual_search_meta WHERE key=?",
        (_REVISION_KEY,),
    ).fetchone()
    previous_revision = None if revision_row is None else str(revision_row["value"])
    protocol_reset = previous_revision != RESIDUAL_PROTOCOL_REVISION

    dirty_rows = connection.execute(
        """
        SELECT episode_id
        FROM source_search_episodes
        WHERE strategy LIKE 'RESIDUAL_CELL:%'
          AND finished_at IS NULL
        ORDER BY started_at, episode_id
        """
    ).fetchall()
    dirty_episode_ids = tuple(str(row["episode_id"]) for row in dirty_rows)

    dirty_cell_keys: tuple[str, ...] = ()
    if dirty_episode_ids:
        marks = _placeholders(dirty_episode_ids)
        rows = connection.execute(
            f"""
            SELECT DISTINCT cell_key
            FROM residual_search_episode_cells
            WHERE episode_id IN ({marks})
            ORDER BY cell_key
            """,
            dirty_episode_ids,
        ).fetchall()
        dirty_cell_keys = tuple(str(row["cell_key"]) for row in rows)

    if protocol_reset:
        rows = connection.execute(
            "SELECT cell_key FROM residual_search_cells ORDER BY cell_key"
        ).fetchall()
        reset_cell_keys = tuple(str(row["cell_key"]) for row in rows)
    else:
        reset_cell_keys = dirty_cell_keys

    if not protocol_reset and not dirty_episode_ids:
        return ResidualRecoveryReport(
            protocol_reset=False,
            dirty_episodes=0,
            reset_cells=0,
            references_removed=0,
            proposals_removed=0,
            reward_attributions_removed=0,
            orphan_identities_removed=0,
        )

    now = float(registry._now())
    if not math.isfinite(now) or now < 0:
        raise ValueError("residual recovery clock must be finite and non-negative")

    references_removed = 0
    proposals_removed = 0
    reward_attributions_removed = 0
    orphan_identities_removed = 0

    connection.execute("BEGIN IMMEDIATE")
    try:
        if protocol_reset:
            references_removed = connection.execute(
                "DELETE FROM residual_search_references"
            ).rowcount
            connection.execute("DELETE FROM residual_search_episode_cells")
            connection.execute("DELETE FROM residual_search_cell_families")
            connection.execute(
                """
                UPDATE residual_search_cells
                SET state='OPEN',
                    attempts=0,
                    result_count=0,
                    duplicate_results=0,
                    unique_roots=0,
                    new_families=0,
                    qualified_roots=0,
                    accepted_novel_eed=0,
                    search_cost_seconds=0,
                    variant_cursor=0,
                    last_searched_at=NULL,
                    updated_at=?
                """,
                (now,),
            )
        elif reset_cell_keys:
            marks = _placeholders(reset_cell_keys)
            references_removed = connection.execute(
                f"DELETE FROM residual_search_references WHERE cell_key IN ({marks})",
                reset_cell_keys,
            ).rowcount
            connection.execute(
                f"DELETE FROM residual_search_episode_cells WHERE cell_key IN ({marks})",
                reset_cell_keys,
            )
            connection.execute(
                f"DELETE FROM residual_search_cell_families WHERE cell_key IN ({marks})",
                reset_cell_keys,
            )
            connection.execute(
                f"""
                UPDATE residual_search_cells
                SET state='OPEN',
                    attempts=0,
                    result_count=0,
                    duplicate_results=0,
                    unique_roots=0,
                    new_families=0,
                    qualified_roots=0,
                    accepted_novel_eed=0,
                    search_cost_seconds=0,
                    variant_cursor=0,
                    last_searched_at=NULL,
                    updated_at=?
                WHERE cell_key IN ({marks})
                """,
                (now, *reset_cell_keys),
            )

        if dirty_episode_ids:
            marks = _placeholders(dirty_episode_ids)
            proposals_removed = connection.execute(
                f"DELETE FROM source_proposals WHERE episode_id IN ({marks})",
                dirty_episode_ids,
            ).rowcount
            reward_attributions_removed = connection.execute(
                f"""
                DELETE FROM source_search_reward_attribution
                WHERE episode_id IN ({marks})
                """,
                dirty_episode_ids,
            ).rowcount
            connection.execute(
                f"DELETE FROM residual_search_episode_cells WHERE episode_id IN ({marks})",
                dirty_episode_ids,
            )
            connection.execute(
                f"DELETE FROM source_search_episodes WHERE episode_id IN ({marks})",
                dirty_episode_ids,
            )

        # The four identity tables are derived search memory.  Once references
        # are removed, rows with no surviving reference must not influence
        # novelty decisions on a retry.
        for table, column in (
            ("residual_search_urls", "url_key"),
            ("residual_search_artifacts", "artifact_key"),
            ("residual_search_datasets", "dataset_key"),
            ("residual_search_families", "family_key"),
        ):
            orphan_identities_removed += connection.execute(
                f"""
                DELETE FROM {table}
                WHERE {column} NOT IN (
                    SELECT DISTINCT {column}
                    FROM residual_search_references
                )
                """
            ).rowcount

        connection.execute(
            """
            INSERT INTO residual_search_meta(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (_REVISION_KEY, RESIDUAL_PROTOCOL_REVISION, now),
        )
        audit = json.dumps(
            {
                "revision": RESIDUAL_PROTOCOL_REVISION,
                "previous_revision": previous_revision,
                "protocol_reset": protocol_reset,
                "dirty_episodes": len(dirty_episode_ids),
                "reset_cells": len(reset_cell_keys),
                "references_removed": references_removed,
                "proposals_removed": proposals_removed,
                "reward_attributions_removed": reward_attributions_removed,
                "orphan_identities_removed": orphan_identities_removed,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        connection.execute(
            """
            INSERT INTO residual_search_meta(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (_RECOVERY_AUDIT_KEY, audit, now),
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise

    return ResidualRecoveryReport(
        protocol_reset=protocol_reset,
        dirty_episodes=len(dirty_episode_ids),
        reset_cells=len(reset_cell_keys),
        references_removed=references_removed,
        proposals_removed=proposals_removed,
        reward_attributions_removed=reward_attributions_removed,
        orphan_identities_removed=orphan_identities_removed,
    )
