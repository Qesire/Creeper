"""Durable control-plane state for resumable evidence work."""

from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from creeper.authority.baseline_index import YEAR_BITS
from creeper.authority.normalizer import normalize_official
from creeper.evidence.actions import (
    ACTION_PRIOR_STRENGTH,
    ACTION_RETRY_PENALTY,
    EvidenceActionKind,
    EvidenceActionValueStats,
    action_prior_yield,
    classify_evidence_action,
    posterior_host_year_yield,
)
from creeper.evidence.rdap_candidates import rdap_parent_candidate
from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    TemporalScope,
)


TERMINAL_STATES = frozenset({
    CDXQueryState.PASS.value,
    CDXQueryState.EMPTY_EXHAUSTIVE.value,
    CDXQueryState.DECOMPOSED.value,
    CDXQueryState.INVALID.value,
})
RETRYABLE_STATES = frozenset({
    CDXQueryState.INCOMPLETE.value,
    CDXQueryState.TRANSIENT_ERROR.value,
})
CLAIMABLE_STATES = frozenset({
    CDXQueryState.PENDING.value,
    *RETRYABLE_STATES,
})


@dataclass(frozen=True)
class EvidenceTask:
    key: EvidenceQueryKey
    state: str
    attempt: int
    retry_at: float | None
    lease_owner: str | None
    lease_until: float | None


class ControlStore:
    """SQLite-WAL authority for evidence task identity and ownership."""

    def __init__(
        self,
        path: Path,
        *,
        default_lease_seconds: float = 300.0,
        clock=time.time,
    ):
        if default_lease_seconds < 0:
            raise ValueError("default_lease_seconds must be non-negative")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.create_function(
            "creeper_tld",
            1,
            lambda hostname: (
                str(hostname).rsplit(".", 1)[-1].lower()
                if isinstance(hostname, str) and "." in hostname
                else ""
            ),
            deterministic=True,
        )
        self.default_lease_seconds = default_lease_seconds
        self.clock = clock
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS evidence_tasks (
                hostname TEXT NOT NULL,
                year_from INTEGER NOT NULL,
                year_to INTEGER NOT NULL,
                provider TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                state TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0,
                retry_at REAL,
                lease_owner TEXT,
                lease_until REAL,
                eed_weight REAL NOT NULL DEFAULT 0,
                PRIMARY KEY(hostname, year_from, year_to, provider, policy_version)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_evidence_tasks_claim
                ON evidence_tasks(state, retry_at, lease_until);
            CREATE INDEX IF NOT EXISTS idx_evidence_tasks_provider_claim
                ON evidence_tasks(provider, state, retry_at, lease_until);
            CREATE TABLE IF NOT EXISTS evidence_task_fanout_reservations (
                hostname TEXT NOT NULL,
                year_from INTEGER NOT NULL,
                year_to INTEGER NOT NULL,
                provider TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                amount INTEGER NOT NULL CHECK(amount > 0),
                PRIMARY KEY(hostname, year_from, year_to, provider, policy_version),
                FOREIGN KEY(
                    hostname, year_from, year_to, provider, policy_version
                ) REFERENCES evidence_tasks(
                    hostname, year_from, year_to, provider, policy_version
                )
            ) WITHOUT ROWID;
            INSERT OR IGNORE INTO evidence_task_fanout_reservations(
                hostname, year_from, year_to, provider, policy_version, amount
            )
            SELECT hostname, year_from, year_to, provider, policy_version,
                   (year_to - year_from)
            FROM evidence_tasks
            WHERE year_to > year_from
              AND policy_version NOT LIKE 'cdx-domain-%'
              AND provider <> 'rdap'
              AND state IN ('pending', 'incomplete', 'transient_error');
            CREATE TABLE IF NOT EXISTS runtime_checkpoints (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS source_domains (
                domain_id TEXT PRIMARY KEY,
                family TEXT NOT NULL,
                discovery_mechanism TEXT NOT NULL,
                temporal_from INTEGER NOT NULL,
                temporal_to INTEGER NOT NULL,
                state TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS reservoirs (
                reservoir_id TEXT PRIMARY KEY,
                domain_id TEXT NOT NULL,
                adapter_id TEXT NOT NULL,
                root_locator TEXT NOT NULL,
                enumeration_kind TEXT NOT NULL,
                capacity_lower INTEGER NOT NULL,
                capacity_upper INTEGER,
                cursor TEXT,
                evidence_mode TEXT NOT NULL,
                state TEXT NOT NULL,
                FOREIGN KEY(domain_id) REFERENCES source_domains(domain_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_reservoirs_domain
                ON reservoirs(domain_id, state);
            CREATE TABLE IF NOT EXISTS source_activations (
                source_key TEXT PRIMARY KEY,
                domain_id TEXT NOT NULL,
                reservoir_id TEXT NOT NULL UNIQUE,
                adapter_id TEXT NOT NULL,
                adapter_kind TEXT NOT NULL,
                root_locator TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                activation_state TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY(domain_id) REFERENCES source_domains(domain_id),
                FOREIGN KEY(reservoir_id) REFERENCES reservoirs(reservoir_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS work_leases (
                lease_id TEXT PRIMARY KEY,
                reservoir_id TEXT NOT NULL,
                cursor_start TEXT,
                cursor_end TEXT,
                max_records INTEGER NOT NULL,
                max_requests INTEGER NOT NULL,
                max_bytes INTEGER NOT NULL,
                max_seconds REAL NOT NULL,
                resource_class TEXT NOT NULL,
                expected_evidence_tasks INTEGER NOT NULL DEFAULT 0,
                expected_novel_eed REAL NOT NULL DEFAULT 0,
                owner TEXT,
                expires_at REAL NOT NULL,
                state TEXT NOT NULL,
                FOREIGN KEY(reservoir_id) REFERENCES reservoirs(reservoir_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_work_leases_recovery
                ON work_leases(state, expires_at);
            CREATE TABLE IF NOT EXISTS evidence_task_origins (
                hostname TEXT NOT NULL,
                year_from INTEGER NOT NULL,
                year_to INTEGER NOT NULL,
                provider TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                source_key TEXT NOT NULL,
                reservoir_id TEXT NOT NULL,
                lease_id TEXT NOT NULL,
                first_observed_at REAL NOT NULL,
                PRIMARY KEY(
                    hostname, year_from, year_to, provider, policy_version,
                    source_key, reservoir_id, lease_id
                ),
                FOREIGN KEY(
                    hostname, year_from, year_to, provider, policy_version
                ) REFERENCES evidence_tasks(
                    hostname, year_from, year_to, provider, policy_version
                ),
                FOREIGN KEY(reservoir_id) REFERENCES reservoirs(reservoir_id),
                FOREIGN KEY(lease_id) REFERENCES work_leases(lease_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_evidence_task_origins_source
                ON evidence_task_origins(source_key, first_observed_at);
            CREATE TABLE IF NOT EXISTS evidence_task_attempt_metrics (
                hostname TEXT NOT NULL,
                year_from INTEGER NOT NULL,
                year_to INTEGER NOT NULL,
                provider TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                state TEXT NOT NULL,
                task_kind TEXT NOT NULL,
                provider_requests INTEGER NOT NULL CHECK(provider_requests >= 0),
                provider_elapsed_milliseconds INTEGER NOT NULL
                    CHECK(provider_elapsed_milliseconds >= 0),
                pages_seen INTEGER NOT NULL CHECK(pages_seen >= 0),
                records_seen INTEGER NOT NULL CHECK(records_seen >= 0),
                recorded_at REAL NOT NULL,
                PRIMARY KEY(
                    hostname, year_from, year_to, provider, policy_version, attempt
                ),
                FOREIGN KEY(
                    hostname, year_from, year_to, provider, policy_version
                ) REFERENCES evidence_tasks(
                    hostname, year_from, year_to, provider, policy_version
                )
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_evidence_task_attempt_metrics_kind
                ON evidence_task_attempt_metrics(task_kind, state);
            CREATE TABLE IF NOT EXISTS evidence_action_cost_stats (
                task_kind TEXT PRIMARY KEY,
                attempts INTEGER NOT NULL CHECK(attempts >= 0),
                provider_requests INTEGER NOT NULL CHECK(provider_requests >= 0),
                provider_elapsed_milliseconds INTEGER NOT NULL
                    CHECK(provider_elapsed_milliseconds >= 0),
                pages_seen INTEGER NOT NULL CHECK(pages_seen >= 0),
                records_seen INTEGER NOT NULL CHECK(records_seen >= 0),
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS evidence_action_final_rewards (
                task_kind TEXT PRIMARY KEY,
                final_novel_host_years INTEGER NOT NULL
                    CHECK(final_novel_host_years >= 0),
                final_novel_eed REAL NOT NULL CHECK(final_novel_eed >= 0),
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS evidence_action_reward_authority (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                baseline_signature TEXT NOT NULL,
                model_signature TEXT NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS evidence_host_year_origins (
                hostname TEXT NOT NULL,
                year INTEGER NOT NULL,
                source_key TEXT NOT NULL,
                reservoir_id TEXT NOT NULL,
                lease_id TEXT NOT NULL,
                evidence_provider TEXT NOT NULL,
                task_year_from INTEGER,
                task_year_to INTEGER,
                task_policy_version TEXT,
                attributed_at REAL NOT NULL,
                PRIMARY KEY(hostname, year),
                FOREIGN KEY(reservoir_id) REFERENCES reservoirs(reservoir_id),
                FOREIGN KEY(lease_id) REFERENCES work_leases(lease_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_evidence_host_year_origins_source
                ON evidence_host_year_origins(source_key, year);
            CREATE TABLE IF NOT EXISTS evidence_host_year_task_kinds (
                hostname TEXT NOT NULL,
                year INTEGER NOT NULL,
                task_kind TEXT NOT NULL,
                evidence_provider TEXT NOT NULL,
                task_year_from INTEGER,
                task_year_to INTEGER,
                task_policy_version TEXT,
                attributed_at REAL NOT NULL,
                PRIMARY KEY(hostname, year)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_evidence_host_year_task_kinds_kind
                ON evidence_host_year_task_kinds(task_kind, year);
            CREATE TABLE IF NOT EXISTS domain_fanout_state (
                parent_hostname TEXT PRIMARY KEY,
                observed_self INTEGER NOT NULL DEFAULT 0 CHECK(observed_self IN (0,1)),
                child_count INTEGER NOT NULL DEFAULT 0 CHECK(child_count >= 0),
                child_sketch INTEGER NOT NULL DEFAULT 0 CHECK(child_sketch >= 0),
                query_enqueued INTEGER NOT NULL DEFAULT 0 CHECK(query_enqueued IN (0,1)),
                rdap_enqueued INTEGER NOT NULL DEFAULT 0 CHECK(rdap_enqueued IN (0,1)),
                first_source_key TEXT,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS domain_fanout_members (
                parent_hostname TEXT NOT NULL,
                child_hostname TEXT NOT NULL,
                PRIMARY KEY(parent_hostname, child_hostname),
                FOREIGN KEY(parent_hostname)
                    REFERENCES domain_fanout_state(parent_hostname)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_domain_fanout_ready
                ON domain_fanout_state(query_enqueued, observed_self, child_count);
            """
        )
        evidence_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(evidence_tasks)"
            ).fetchall()
        }
        if "eed_weight" not in evidence_columns:
            self.connection.execute(
                "ALTER TABLE evidence_tasks ADD COLUMN "
                "eed_weight REAL NOT NULL DEFAULT 0"
            )
        fanout_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(domain_fanout_state)"
            ).fetchall()
        }
        if "rdap_enqueued" not in fanout_columns:
            self.connection.execute(
                "ALTER TABLE domain_fanout_state ADD COLUMN "
                "rdap_enqueued INTEGER NOT NULL DEFAULT 0 "
                "CHECK(rdap_enqueued IN (0,1))"
            )
        if "child_sketch" not in fanout_columns:
            self.connection.execute(
                "ALTER TABLE domain_fanout_state ADD COLUMN "
                "child_sketch INTEGER NOT NULL DEFAULT 0 "
                "CHECK(child_sketch >= 0)"
            )
        # V3 initially persisted every parent/child pair plus self-only host
        # rows. child_count is already materialized, so those rows are no
        # longer needed after the fixed-sketch migration. DELETE makes their
        # pages reusable by SQLite without an unsafe blocking VACUUM.
        self.connection.executescript(
            """
            DELETE FROM domain_fanout_members;
            DELETE FROM domain_fanout_state
            WHERE child_count = 0 AND query_enqueued = 0;
            """
        )
        self.connection.execute(
            """
            INSERT OR IGNORE INTO evidence_action_cost_stats(
                task_kind, attempts, provider_requests,
                provider_elapsed_milliseconds, pages_seen, records_seen,
                updated_at
            )
            SELECT task_kind,
                   COUNT(*),
                   COALESCE(SUM(provider_requests), 0),
                   COALESCE(SUM(provider_elapsed_milliseconds), 0),
                   COALESCE(SUM(pages_seen), 0),
                   COALESCE(SUM(records_seen), 0),
                   ?
            FROM evidence_task_attempt_metrics
            GROUP BY task_kind
            """,
            (float(self.clock()),),
        )
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS eed_tld_weights (
                tld TEXT PRIMARY KEY,
                weight REAL NOT NULL CHECK(weight >= 0)
            ) WITHOUT ROWID;
            CREATE TRIGGER IF NOT EXISTS trg_evidence_task_eed_weight
            AFTER INSERT ON evidence_tasks
            BEGIN
                UPDATE evidence_tasks
                SET eed_weight = COALESCE(
                    (
                        SELECT weight FROM eed_tld_weights
                        WHERE tld = creeper_tld(NEW.hostname)
                    ),
                    0
                )
                WHERE hostname = NEW.hostname
                  AND year_from = NEW.year_from
                  AND year_to = NEW.year_to
                  AND provider = NEW.provider
                  AND policy_version = NEW.policy_version;
            END;
            """
        )
        self.connection.commit()

    @staticmethod
    def _values(key: EvidenceQueryKey) -> tuple[object, ...]:
        scope = key.temporal_scope
        return (
            key.hostname,
            scope.year_from,
            scope.year_to,
            key.provider,
            key.policy_version,
        )

    @staticmethod
    def _key(row: sqlite3.Row) -> EvidenceQueryKey:
        return EvidenceQueryKey(
            row["hostname"],
            TemporalScope(row["year_from"], row["year_to"]),
            row["provider"],
            row["policy_version"],
        )

    @classmethod
    def _task(cls, row: sqlite3.Row) -> EvidenceTask:
        return EvidenceTask(
            key=cls._key(row),
            state=str(row["state"]),
            attempt=int(row["attempt"]),
            retry_at=row["retry_at"],
            lease_owner=row["lease_owner"],
            lease_until=row["lease_until"],
        )

    def record_domain_fanout_observations(
        self,
        hostnames: Iterable[str],
        *,
        source_key: str | None = None,
    ) -> int:
        """Accumulate bounded distinct-child fanout sketches per parent.

        Storage is O(parent domains), not O(observed hostnames): each parent
        owns one 63-bit sketch. Hash collisions can only under-count fanout,
        which is a safe false-negative for the amplification threshold.
        """
        values = list(dict.fromkeys(
            hostname
            for raw in hostnames
            if (hostname := normalize_official(str(raw))) is not None
        ))
        if not values:
            return 0

        batch_bits: dict[str, int] = {}
        for hostname in values:
            labels = hostname.split(".")
            if len(labels) < 3:
                continue
            parent_labels = labels[1:]
            # Do not manufacture broad ccTLD suffixes such as co.uk. A
            # three-label parent such as example.co.uk remains eligible.
            if len(parent_labels[-1]) == 2 and len(parent_labels) < 3:
                continue
            parent = ".".join(parent_labels)
            digest = hashlib.blake2s(
                hostname.encode("utf-8"),
                digest_size=8,
            ).digest()
            bit = 1 << (int.from_bytes(digest, "big") % 63)
            batch_bits[parent] = batch_bits.get(parent, 0) | bit

        if not batch_bits:
            return 0

        now = float(self.clock())
        before = self.connection.total_changes
        with self.connection:
            for parent, bits in batch_bits.items():
                row = self.connection.execute(
                    """
                    SELECT child_count, child_sketch, first_source_key
                    FROM domain_fanout_state
                    WHERE parent_hostname = ?
                    """,
                    (parent,),
                ).fetchone()
                if row is None:
                    combined = bits
                    count = combined.bit_count()
                    self.connection.execute(
                        """
                        INSERT INTO domain_fanout_state(
                            parent_hostname, observed_self, child_count,
                            child_sketch, query_enqueued, rdap_enqueued,
                            first_source_key, updated_at
                        ) VALUES (?, 0, ?, ?, 0, 0, ?, ?)
                        """,
                        (parent, count, combined, source_key, now),
                    )
                else:
                    combined = int(row["child_sketch"] or 0) | bits
                    count = max(
                        int(row["child_count"] or 0),
                        combined.bit_count(),
                    )
                    self.connection.execute(
                        """
                        UPDATE domain_fanout_state
                        SET child_count = ?,
                            child_sketch = ?,
                            first_source_key = COALESCE(first_source_key, ?),
                            updated_at = ?
                        WHERE parent_hostname = ?
                        """,
                        (count, combined, source_key, now, parent),
                    )
        return self.connection.total_changes - before

    def ready_domain_fanout_candidates(
        self,
        *,
        min_children: int = 4,
        limit: int = 16,
    ) -> list[str]:
        if min_children < 1 or limit < 1:
            raise ValueError("domain fanout thresholds must be positive")
        rows = self.connection.execute(
            """
            SELECT parent_hostname
            FROM domain_fanout_state
            WHERE query_enqueued = 0
              AND child_count >= ?
            ORDER BY child_count DESC, parent_hostname
            LIMIT ?
            """,
            (int(min_children), int(limit)),
        ).fetchall()
        return [str(row["parent_hostname"]) for row in rows]

    def mark_domain_fanout_enqueued(self, parents: Iterable[str]) -> int:
        values = list(dict.fromkeys(
            hostname
            for raw in parents
            if (hostname := normalize_official(str(raw))) is not None
        ))
        if not values:
            return 0
        before = self.connection.total_changes
        now = float(self.clock())
        with self.connection:
            self.connection.executemany(
                """
                UPDATE domain_fanout_state
                SET query_enqueued = 1, updated_at = ?
                WHERE parent_hostname = ? AND query_enqueued = 0
                """,
                [(now, parent) for parent in values],
            )
        return self.connection.total_changes - before

    def ready_rdap_candidates(
        self,
        *,
        min_children: int = 1,
        limit: int = 16,
    ) -> list[str]:
        """Return likely registrable parent hosts not yet sent to RDAP."""
        if min_children < 1 or limit < 1:
            raise ValueError("RDAP fanout thresholds must be positive")
        rows = self.connection.execute(
            """
            SELECT parent_hostname
            FROM domain_fanout_state
            WHERE observed_self = 1
              AND rdap_enqueued = 0
              AND (
                    child_count >= ?
                    OR (
                        LENGTH(parent_hostname)
                        - LENGTH(REPLACE(parent_hostname, '.', ''))
                    ) = 1
              )
            ORDER BY child_count DESC, parent_hostname
            LIMIT ?
            """,
            (int(min_children), int(limit)),
        ).fetchall()
        return [str(row["parent_hostname"]) for row in rows]

    def mark_rdap_enqueued(self, hostnames: Iterable[str]) -> int:
        values = list(dict.fromkeys(
            hostname
            for raw in hostnames
            if (hostname := normalize_official(str(raw))) is not None
        ))
        if not values:
            return 0
        before = self.connection.total_changes
        now = float(self.clock())
        with self.connection:
            self.connection.executemany(
                """
                UPDATE domain_fanout_state
                SET rdap_enqueued = 1, updated_at = ?
                WHERE parent_hostname = ? AND rdap_enqueued = 0
                """,
                [(now, hostname) for hostname in values],
            )
        return self.connection.total_changes - before

    def set_eed_tld_weights(self, weights: dict[str, object]) -> int:
        """Persist official EED weights and revalue all durable evidence work."""
        normalized: dict[str, float] = {}
        for raw_tld, raw_weight in weights.items():
            tld = str(raw_tld).strip().lower().lstrip(".")
            weight = float(raw_weight)
            if tld and weight >= 0:
                normalized[tld] = weight
        before = self.connection.total_changes
        with self.connection:
            self.connection.execute("DELETE FROM eed_tld_weights")
            self.connection.executemany(
                "INSERT INTO eed_tld_weights(tld, weight) VALUES (?, ?)",
                sorted(normalized.items()),
            )
            self.connection.execute(
                """
                UPDATE evidence_tasks
                SET eed_weight = COALESCE(
                    (
                        SELECT weight
                        FROM eed_tld_weights
                        WHERE tld = creeper_tld(evidence_tasks.hostname)
                    ),
                    0
                )
                """
            )
        return self.connection.total_changes - before

    def resolve_provider_coverage_masks(
        self,
        hostnames: Iterable[str],
        *,
        provider: str,
        policy_version: str,
        chunk_size: int = 800,
    ) -> dict[str, int]:
        """Return years already exhaustively covered by one evidence provider.

        PASS and EMPTY_EXHAUSTIVE tasks both imply that the provider completed
        the full temporal scope. PASS years with accepted captures are already
        represented in EvidenceStore; the remaining years in that completed
        scope are reusable negative knowledge. INVALID/retryable tasks do not
        establish coverage.
        """
        if not provider or not policy_version:
            raise ValueError("provider and policy_version are required")
        if chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        values = list(dict.fromkeys(str(item) for item in hostnames if str(item)))
        result = {hostname: 0 for hostname in values}
        if not values:
            return result
        limit = min(int(chunk_size), 800)
        for start in range(0, len(values), limit):
            chunk = values[start:start + limit]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.connection.execute(
                f"""
                SELECT hostname, year_from, year_to
                FROM evidence_tasks
                WHERE hostname IN ({placeholders})
                  AND provider = ?
                  AND policy_version = ?
                  AND state IN (?, ?)
                """,
                [
                    *chunk,
                    provider,
                    policy_version,
                    CDXQueryState.PASS.value,
                    CDXQueryState.EMPTY_EXHAUSTIVE.value,
                ],
            ).fetchall()
            for row in rows:
                mask = result.get(str(row["hostname"]), 0)
                for year in range(int(row["year_from"]), int(row["year_to"]) + 1):
                    mask |= YEAR_BITS.get(year, 0)
                result[str(row["hostname"])] = mask
        return result

    def backfill_rdap_tasks_from_wayback(
        self,
        *,
        limit: int = 64,
        policy_version: str = "rdap-registration-v1",
    ) -> int:
        """Seed RDAP work from durable Wayback backlog without source replay.

        This is an outage-isolation path: if Wayback backlog backpressure blocks
        the source producer, already-discovered hostnames can still feed RDAP's
        independent provider budget. EvidenceTask identity gives global
        deduplication; deterministic first source origin is copied when present.
        """
        if limit < 1:
            raise ValueError("limit must be positive")
        scan_limit = max(limit * 8, limit)
        rows = self.connection.execute(
            """
            SELECT e.hostname,
                   (
                       SELECT o.source_key
                       FROM evidence_task_origins o
                       WHERE o.hostname = e.hostname
                         AND o.year_from = e.year_from
                         AND o.year_to = e.year_to
                         AND o.provider = e.provider
                         AND o.policy_version = e.policy_version
                       ORDER BY o.first_observed_at, o.source_key,
                                o.reservoir_id, o.lease_id
                       LIMIT 1
                   ) AS source_key,
                   (
                       SELECT o.reservoir_id
                       FROM evidence_task_origins o
                       WHERE o.hostname = e.hostname
                         AND o.year_from = e.year_from
                         AND o.year_to = e.year_to
                         AND o.provider = e.provider
                         AND o.policy_version = e.policy_version
                       ORDER BY o.first_observed_at, o.source_key,
                                o.reservoir_id, o.lease_id
                       LIMIT 1
                   ) AS reservoir_id,
                   (
                       SELECT o.lease_id
                       FROM evidence_task_origins o
                       WHERE o.hostname = e.hostname
                         AND o.year_from = e.year_from
                         AND o.year_to = e.year_to
                         AND o.provider = e.provider
                         AND o.policy_version = e.policy_version
                       ORDER BY o.first_observed_at, o.source_key,
                                o.reservoir_id, o.lease_id
                       LIMIT 1
                   ) AS lease_id,
                   (
                       SELECT o.first_observed_at
                       FROM evidence_task_origins o
                       WHERE o.hostname = e.hostname
                         AND o.year_from = e.year_from
                         AND o.year_to = e.year_to
                         AND o.provider = e.provider
                         AND o.policy_version = e.policy_version
                       ORDER BY o.first_observed_at, o.source_key,
                                o.reservoir_id, o.lease_id
                       LIMIT 1
                   ) AS first_observed_at
            FROM evidence_tasks e
            WHERE e.provider = 'wayback'
              AND e.state IN (?, ?, ?)
            ORDER BY e.eed_weight DESC, e.year_from, e.hostname
            LIMIT ?
            """,
            (
                CDXQueryState.PENDING.value,
                CDXQueryState.INCOMPLETE.value,
                CDXQueryState.TRANSIENT_ERROR.value,
                scan_limit,
            ),
        ).fetchall()

        candidates: dict[str, tuple[object, object, object, object]] = {}
        for row in rows:
            parent = rdap_parent_candidate(str(row["hostname"]))
            if parent is None or parent in candidates:
                continue
            candidates[parent] = (
                row["source_key"],
                row["reservoir_id"],
                row["lease_id"],
                row["first_observed_at"],
            )
            if len(candidates) >= limit:
                break
        if not candidates:
            return 0

        inserted = 0
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            for hostname, origin in candidates.items():
                key = EvidenceQueryKey(
                    hostname,
                    TemporalScope(1996, 2001),
                    "rdap",
                    policy_version,
                )
                changed = self.connection.execute(
                    """
                    INSERT OR IGNORE INTO evidence_tasks(
                        hostname, year_from, year_to, provider,
                        policy_version, state
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (*self._values(key), CDXQueryState.PENDING.value),
                ).rowcount
                if not changed:
                    continue
                inserted += 1
                source_key, reservoir_id, lease_id, observed_at = origin
                if (
                    source_key is not None
                    and reservoir_id is not None
                    and lease_id is not None
                ):
                    self.connection.execute(
                        """
                        INSERT OR IGNORE INTO evidence_task_origins(
                            hostname, year_from, year_to, provider,
                            policy_version, source_key, reservoir_id,
                            lease_id, first_observed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            *self._values(key),
                            str(source_key),
                            str(reservoir_id),
                            str(lease_id),
                            now if observed_at is None else float(observed_at),
                        ),
                    )
            self.connection.commit()
            return inserted
        except BaseException:
            self.connection.rollback()
            raise

    def enqueue_evidence_tasks(self, keys: Iterable[EvidenceQueryKey]) -> int:
        rows = [(*self._values(key), CDXQueryState.PENDING.value) for key in keys]
        if not rows:
            return 0
        with self.connection:
            cursor = self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_tasks(
                    hostname, year_from, year_to, provider, policy_version, state
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        # rowcount reflects direct task inserts only; total_changes would also
        # include the operational EED-weight trigger and break this API's
        # long-standing "number of new tasks" contract.
        return max(0, int(cursor.rowcount))


    def record_evidence_task_attempt_metric(
        self,
        key: EvidenceQueryKey,
        *,
        attempt: int,
        state: CDXQueryState | str,
        provider_requests: int,
        provider_elapsed_milliseconds: int,
        pages_seen: int,
        records_seen: int,
        recorded_at: float | None = None,
    ) -> bool:
        """Persist operational provider cost for one durable task attempt.

        This table is non-authoritative. It exists only to measure the real
        provider cost of exact/range strategies and source-origin yield.
        """
        if attempt < 1:
            raise ValueError("attempt must be positive")
        metrics = (
            provider_requests,
            provider_elapsed_milliseconds,
            pages_seen,
            records_seen,
        )
        if any(int(value) < 0 for value in metrics):
            raise ValueError("attempt metrics must be non-negative")
        value = state.value if isinstance(state, CDXQueryState) else str(state)
        task_kind = classify_evidence_action(key).value
        when = float(self.clock()) if recorded_at is None else float(recorded_at)
        with self.connection:
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO evidence_task_attempt_metrics(
                    hostname, year_from, year_to, provider, policy_version,
                    attempt, state, task_kind, provider_requests,
                    provider_elapsed_milliseconds, pages_seen, records_seen,
                    recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    *self._values(key),
                    int(attempt),
                    value,
                    task_kind,
                    int(provider_requests),
                    int(provider_elapsed_milliseconds),
                    int(pages_seen),
                    int(records_seen),
                    when,
                ),
            )
            inserted = int(cursor.rowcount or 0) == 1
            if inserted:
                self.connection.execute(
                    """
                    INSERT INTO evidence_action_cost_stats(
                        task_kind, attempts, provider_requests,
                        provider_elapsed_milliseconds, pages_seen, records_seen,
                        updated_at
                    ) VALUES (?, 1, ?, ?, ?, ?, ?)
                    ON CONFLICT(task_kind) DO UPDATE SET
                        attempts = attempts + 1,
                        provider_requests = provider_requests
                            + excluded.provider_requests,
                        provider_elapsed_milliseconds =
                            provider_elapsed_milliseconds
                            + excluded.provider_elapsed_milliseconds,
                        pages_seen = pages_seen + excluded.pages_seen,
                        records_seen = records_seen + excluded.records_seen,
                        updated_at = excluded.updated_at
                    """,
                    (
                        task_kind,
                        int(provider_requests),
                        int(provider_elapsed_milliseconds),
                        int(pages_seen),
                        int(records_seen),
                        when,
                    ),
                )
        return inserted

    def publish_evidence_action_final_rewards(
        self,
        attribution: dict[str, dict[str, object]],
        *,
        baseline_signature: str,
        model_signature: str,
    ) -> bool:
        """Publish cumulative formal readiness reward for provider action kinds.

        The baseline/model signatures are part of the authority identity. A
        changed authority atomically clears old rewards before the new
        cumulative readiness totals are installed.
        """

        if not baseline_signature or not model_signature:
            raise ValueError("action reward authority signatures are required")
        normalized: dict[str, tuple[int, float]] = {}
        for raw_kind, payload in attribution.items():
            try:
                kind = EvidenceActionKind(str(raw_kind))
            except ValueError:
                # Direct source evidence is a readiness task kind but not a
                # provider queue action, so it is intentionally ignored here.
                continue
            host_years = int(payload.get("novel_host_years", 0))
            eed = float(payload.get("novel_eed", 0.0))
            if host_years < 0 or eed < 0:
                raise ValueError("final evidence action rewards must be non-negative")
            normalized[kind.value] = (host_years, eed)

        now = float(self.clock())
        changed_authority = False
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """
                SELECT baseline_signature, model_signature
                FROM evidence_action_reward_authority
                WHERE singleton = 1
                """
            ).fetchone()
            changed_authority = (
                row is None
                or str(row["baseline_signature"]) != baseline_signature
                or str(row["model_signature"]) != model_signature
            )
            if changed_authority:
                self.connection.execute(
                    "DELETE FROM evidence_action_final_rewards"
                )
            self.connection.execute(
                """
                INSERT INTO evidence_action_reward_authority(
                    singleton, baseline_signature, model_signature, updated_at
                ) VALUES (1, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    baseline_signature = excluded.baseline_signature,
                    model_signature = excluded.model_signature,
                    updated_at = excluded.updated_at
                """,
                (baseline_signature, model_signature, now),
            )
            for kind, (host_years, eed) in normalized.items():
                self.connection.execute(
                    """
                    INSERT INTO evidence_action_final_rewards(
                        task_kind, final_novel_host_years,
                        final_novel_eed, updated_at
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(task_kind) DO UPDATE SET
                        final_novel_host_years =
                            excluded.final_novel_host_years,
                        final_novel_eed = excluded.final_novel_eed,
                        updated_at = excluded.updated_at
                    """,
                    (kind, host_years, eed, now),
                )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return changed_authority

    def evidence_action_value_summary(
        self,
    ) -> dict[str, EvidenceActionValueStats]:
        """Return formal reward/cost posterior for each provider action class."""

        rows = {
            str(row["task_kind"]): row
            for row in self.connection.execute(
                """
                SELECT c.task_kind,
                       c.attempts,
                       c.provider_requests,
                       c.provider_elapsed_milliseconds,
                       COALESCE(r.final_novel_host_years, 0)
                           AS final_novel_host_years,
                       COALESCE(r.final_novel_eed, 0)
                           AS final_novel_eed
                FROM evidence_action_cost_stats c
                LEFT JOIN evidence_action_final_rewards r
                  ON r.task_kind = c.task_kind
                """
            )
        }
        rewards = {
            str(row["task_kind"]): row
            for row in self.connection.execute(
                """
                SELECT task_kind, final_novel_host_years, final_novel_eed
                FROM evidence_action_final_rewards
                """
            )
        }
        result: dict[str, EvidenceActionValueStats] = {}
        for kind in EvidenceActionKind:
            row = rows.get(kind.value)
            reward = rewards.get(kind.value)
            attempts = int(row["attempts"]) if row is not None else 0
            requests = int(row["provider_requests"]) if row is not None else 0
            elapsed = (
                int(row["provider_elapsed_milliseconds"])
                if row is not None
                else 0
            )
            host_years = (
                int(
                    row["final_novel_host_years"]
                    if row is not None
                    else reward["final_novel_host_years"]
                )
                if row is not None or reward is not None
                else 0
            )
            final_eed = (
                float(
                    row["final_novel_eed"]
                    if row is not None
                    else reward["final_novel_eed"]
                )
                if row is not None or reward is not None
                else 0.0
            )
            effective_requests = max(requests, attempts)
            posterior = posterior_host_year_yield(
                kind,
                final_novel_host_years=host_years,
                provider_requests=requests,
                attempts=attempts,
            )
            result[kind.value] = EvidenceActionValueStats(
                action_kind=kind,
                attempts=attempts,
                provider_requests=requests,
                provider_elapsed_milliseconds=elapsed,
                final_novel_host_years=host_years,
                final_novel_eed=final_eed,
                posterior_host_years_per_request=posterior,
                final_eed_per_request=(
                    final_eed / effective_requests
                    if effective_requests > 0
                    else 0.0
                ),
            )
        return result

    def evidence_attempt_metric_summary(self) -> dict[str, dict[str, int]]:
        """Aggregate provider attempt cost by task kind without materializing rows."""
        result: dict[str, dict[str, int]] = {}
        for row in self.connection.execute(
            """
            SELECT task_kind,
                   COUNT(*) AS attempts,
                   COALESCE(SUM(provider_requests), 0) AS provider_requests,
                   COALESCE(SUM(provider_elapsed_milliseconds), 0) AS elapsed_ms,
                   COALESCE(SUM(pages_seen), 0) AS pages_seen,
                   COALESCE(SUM(records_seen), 0) AS records_seen
            FROM evidence_task_attempt_metrics
            GROUP BY task_kind
            ORDER BY task_kind
            """
        ):
            result[str(row["task_kind"])] = {
                "attempts": int(row["attempts"]),
                "provider_requests": int(row["provider_requests"]),
                "provider_elapsed_milliseconds": int(row["elapsed_ms"]),
                "pages_seen": int(row["pages_seen"]),
                "records_seen": int(row["records_seen"]),
            }
        return result

    def evidence_task_origin_coverage(self) -> dict[str, dict[str, int]]:
        """Count durable tasks with source lineage, grouped by task state."""
        result: dict[str, dict[str, int]] = {}
        for row in self.connection.execute(
            """
            SELECT e.state AS state,
                   COUNT(*) AS tasks,
                   SUM(
                       CASE WHEN EXISTS (
                           SELECT 1
                           FROM evidence_task_origins o
                           WHERE o.hostname = e.hostname
                             AND o.year_from = e.year_from
                             AND o.year_to = e.year_to
                             AND o.provider = e.provider
                             AND o.policy_version = e.policy_version
                       ) THEN 1 ELSE 0 END
                   ) AS with_origin
            FROM evidence_tasks e
            GROUP BY e.state
            ORDER BY e.state
            """
        ):
            result[str(row["state"])] = {
                "tasks": int(row["tasks"]),
                "with_origin": int(row["with_origin"] or 0),
            }
        return result

    def source_provider_request_totals(self) -> dict[str, int]:
        """Attribute provider requests to each task's deterministic primary source.

        The aggregation is one SQL statement so validation remains cheap at
        million-task scale. The scalar origin lookup uses the task-origin
        primary-key prefix and deterministic first-touch ordering.
        """
        rows = self.connection.execute(
            """
            SELECT source_key, SUM(provider_requests) AS provider_requests
            FROM (
                SELECT m.provider_requests AS provider_requests,
                       COALESCE(
                           (
                               SELECT o.source_key
                               FROM evidence_task_origins o
                               WHERE o.hostname = m.hostname
                                 AND o.year_from = m.year_from
                                 AND o.year_to = m.year_to
                                 AND o.provider = m.provider
                                 AND o.policy_version = m.policy_version
                               ORDER BY o.first_observed_at, o.source_key,
                                        o.reservoir_id, o.lease_id
                               LIMIT 1
                           ),
                           '__unattributed__'
                       ) AS source_key
                FROM evidence_task_attempt_metrics m
            )
            GROUP BY source_key
            ORDER BY source_key
            """
        ).fetchall()
        return {
            str(row["source_key"]): int(row["provider_requests"] or 0)
            for row in rows
        }

    def record_evidence_task_origins(
        self,
        keys: Iterable[EvidenceQueryKey],
        *,
        source_key: str,
        reservoir_id: str,
        lease_id: str,
        observed_at: float | None = None,
    ) -> int:
        """Record non-authoritative source lineage for durable evidence work."""
        if not source_key.strip() or not reservoir_id.strip() or not lease_id.strip():
            raise ValueError("source_key, reservoir_id, and lease_id are required")
        key_list = list(dict.fromkeys(keys))
        if not key_list:
            return 0
        when = float(self.clock()) if observed_at is None else float(observed_at)
        before = self.connection.total_changes
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_task_origins(
                    hostname, year_from, year_to, provider, policy_version,
                    source_key, reservoir_id, lease_id, first_observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        *self._values(key),
                        source_key,
                        reservoir_id,
                        lease_id,
                        when,
                    )
                    for key in key_list
                ],
            )
        return self.connection.total_changes - before

    def attribute_task_host_years(
        self,
        key: EvidenceQueryKey,
        years: Iterable[int],
        *,
        attributed_at: float | None = None,
    ) -> int:
        """Assign first-touch operational credit to positive task host-years.

        Task-kind attribution is independent of source lineage, so even legacy
        backlog can be measured as exact/range yield once it completes under
        this code. Source attribution remains optional and preserves the
        existing deterministic primary-source semantics.
        """
        year_list = sorted(set(int(year) for year in years))
        if not year_list:
            return 0
        scope = key.temporal_scope
        if any(year < scope.year_from or year > scope.year_to for year in year_list):
            raise ValueError("attributed year falls outside evidence task scope")
        when = float(self.clock()) if attributed_at is None else float(attributed_at)
        task_kind = (
            "rdap"
            if key.provider == "rdap"
            else "exact"
            if scope.year_from == scope.year_to
            else "range"
        )

        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_host_year_task_kinds(
                    hostname, year, task_kind, evidence_provider,
                    task_year_from, task_year_to, task_policy_version,
                    attributed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        key.hostname,
                        year,
                        task_kind,
                        key.provider,
                        scope.year_from,
                        scope.year_to,
                        key.policy_version,
                        when,
                    )
                    for year in year_list
                ],
            )

        origin = self.connection.execute(
            """
            SELECT source_key, reservoir_id, lease_id
            FROM evidence_task_origins
            WHERE hostname = ? AND year_from = ? AND year_to = ?
              AND provider = ? AND policy_version = ?
            ORDER BY first_observed_at, source_key, reservoir_id, lease_id
            LIMIT 1
            """,
            self._values(key),
        ).fetchone()
        if origin is None:
            return 0

        before = self.connection.total_changes
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_host_year_origins(
                    hostname, year, source_key, reservoir_id, lease_id,
                    evidence_provider, task_year_from, task_year_to,
                    task_policy_version, attributed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        key.hostname,
                        year,
                        str(origin["source_key"]),
                        str(origin["reservoir_id"]),
                        str(origin["lease_id"]),
                        key.provider,
                        scope.year_from,
                        scope.year_to,
                        key.policy_version,
                        when,
                    )
                    for year in year_list
                ],
            )
        return self.connection.total_changes - before

    def attribute_domain_task_host_years(
        self,
        key: EvidenceQueryKey,
        capsules: Iterable[EvidenceCapsule],
        *,
        attributed_at: float | None = None,
    ) -> int:
        """Credit multi-host positives from one bounded domain-scope task."""
        values = list(dict.fromkeys(
            (capsule.hostname, int(capsule.year), capsule.provider)
            for capsule in capsules
        ))
        if not values:
            return 0
        when = float(self.clock()) if attributed_at is None else float(attributed_at)
        scope = key.temporal_scope
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_host_year_task_kinds(
                    hostname, year, task_kind, evidence_provider,
                    task_year_from, task_year_to, task_policy_version,
                    attributed_at
                ) VALUES (?, ?, 'domain', ?, ?, ?, ?, ?)
                """,
                [
                    (
                        hostname, year, provider,
                        scope.year_from, scope.year_to, key.policy_version, when,
                    )
                    for hostname, year, provider in values
                ],
            )

        origin = self.connection.execute(
            """
            SELECT source_key, reservoir_id, lease_id
            FROM evidence_task_origins
            WHERE hostname = ? AND year_from = ? AND year_to = ?
              AND provider = ? AND policy_version = ?
            ORDER BY first_observed_at, source_key, reservoir_id, lease_id
            LIMIT 1
            """,
            self._values(key),
        ).fetchone()
        if origin is None:
            return 0
        before = self.connection.total_changes
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_host_year_origins(
                    hostname, year, source_key, reservoir_id, lease_id,
                    evidence_provider, task_year_from, task_year_to,
                    task_policy_version, attributed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        hostname,
                        year,
                        str(origin["source_key"]),
                        str(origin["reservoir_id"]),
                        str(origin["lease_id"]),
                        provider,
                        scope.year_from,
                        scope.year_to,
                        key.policy_version,
                        when,
                    )
                    for hostname, year, provider in values
                ],
            )
        return self.connection.total_changes - before

    def attribute_direct_host_years(
        self,
        host_years: Iterable[tuple[str, int, str]],
        *,
        source_key: str,
        reservoir_id: str,
        lease_id: str,
        attributed_at: float | None = None,
    ) -> int:
        """Assign first-touch source credit for direct-year evidence capsules."""
        if not source_key.strip() or not reservoir_id.strip() or not lease_id.strip():
            raise ValueError("source_key, reservoir_id, and lease_id are required")
        values = list(dict.fromkeys(
            (str(hostname), int(year), str(provider))
            for hostname, year, provider in host_years
        ))
        if not values:
            return 0
        when = float(self.clock()) if attributed_at is None else float(attributed_at)
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_host_year_task_kinds(
                    hostname, year, task_kind, evidence_provider,
                    task_year_from, task_year_to, task_policy_version,
                    attributed_at
                ) VALUES (?, ?, 'direct', ?, NULL, NULL, NULL, ?)
                """,
                [
                    (hostname, year, provider, when)
                    for hostname, year, provider in values
                ],
            )
        before = self.connection.total_changes
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_host_year_origins(
                    hostname, year, source_key, reservoir_id, lease_id,
                    evidence_provider, task_year_from, task_year_to,
                    task_policy_version, attributed_at
                ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
                """,
                [
                    (
                        hostname,
                        year,
                        source_key,
                        reservoir_id,
                        lease_id,
                        provider,
                        when,
                    )
                    for hostname, year, provider in values
                ],
            )
        return self.connection.total_changes - before

    def resolve_host_year_task_kinds(
        self,
        host_years: Iterable[tuple[str, int]],
        *,
        chunk_size: int = 400,
    ) -> dict[tuple[str, int], str]:
        """Resolve first-touch exact/range/direct strategy for host-years."""
        if chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        values = list(dict.fromkeys(
            (str(hostname), int(year)) for hostname, year in host_years
        ))
        result: dict[tuple[str, int], str] = {}
        limit = min(int(chunk_size), 400)
        for start in range(0, len(values), limit):
            chunk = values[start:start + limit]
            predicates = " OR ".join("(hostname = ? AND year = ?)" for _ in chunk)
            params: list[object] = []
            for hostname, year in chunk:
                params.extend((hostname, year))
            for row in self.connection.execute(
                f"""
                SELECT hostname, year, task_kind
                FROM evidence_host_year_task_kinds
                WHERE {predicates}
                """,
                params,
            ):
                result[(str(row["hostname"]), int(row["year"]))] = str(
                    row["task_kind"]
                )
        return result

    def resolve_primary_source_origins(
        self,
        host_years: Iterable[tuple[str, int]],
        *,
        chunk_size: int = 400,
    ) -> dict[tuple[str, int], str]:
        """Resolve first-touch source keys for host-years in bounded SQL chunks."""
        if chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        values = list(dict.fromkeys(
            (str(hostname), int(year)) for hostname, year in host_years
        ))
        result: dict[tuple[str, int], str] = {}
        limit = min(int(chunk_size), 400)
        for start in range(0, len(values), limit):
            chunk = values[start:start + limit]
            predicates = " OR ".join("(hostname = ? AND year = ?)" for _ in chunk)
            params: list[object] = []
            for hostname, year in chunk:
                params.extend((hostname, year))
            for row in self.connection.execute(
                f"""
                SELECT hostname, year, source_key
                FROM evidence_host_year_origins
                WHERE {predicates}
                """,
                params,
            ):
                result[(str(row["hostname"]), int(row["year"]))] = str(
                    row["source_key"]
                )
        return result

    def claim_evidence_tasks(
        self,
        *,
        owner: str,
        limit: int,
        lease_seconds: float | None = None,
        keys: Iterable[EvidenceQueryKey] | None = None,
    ) -> list[EvidenceTask]:
        if not owner:
            raise ValueError("owner is required")
        if limit < 1:
            return []
        if lease_seconds is None:
            lease_seconds = self.default_lease_seconds
        if lease_seconds < 0:
            raise ValueError("lease_seconds must be non-negative")
        now = float(self.clock())
        key_values = list(keys) if keys is not None else None
        clauses = [
            "state IN (?, ?, ?)",
            "(lease_until IS NULL OR lease_until <= ?)",
            "(retry_at IS NULL OR retry_at <= ?)",
        ]
        params: list[object] = [
            CDXQueryState.PENDING.value,
            CDXQueryState.INCOMPLETE.value,
            CDXQueryState.TRANSIENT_ERROR.value,
            now,
            now,
        ]
        if key_values is not None:
            if not key_values:
                return []
            key_clauses = []
            for key in key_values:
                key_clauses.append(
                    "(hostname = ? AND year_from = ? AND year_to = ? "
                    "AND provider = ? AND policy_version = ?)"
                )
                params.extend(self._values(key))
            clauses.append("(" + " OR ".join(key_clauses) + ")")
        params.append(limit)
        query = (
            "WITH eligible AS ("
            "SELECT e.*, CASE "
            "WHEN e.policy_version LIKE 'cdx-domain-%' THEN 'domain' "
            "WHEN e.provider = 'rdap' THEN 'rdap' "
            "WHEN e.year_to > e.year_from THEN 'range' "
            "ELSE 'exact' END AS action_kind "
            "FROM evidence_tasks e WHERE "
            + " AND ".join(clauses)
            + ") "
            "SELECT eligible.* FROM eligible "
            "LEFT JOIN evidence_action_cost_stats cost "
            "ON cost.task_kind = eligible.action_kind "
            "LEFT JOIN evidence_action_final_rewards reward "
            "ON reward.task_kind = eligible.action_kind "
            "ORDER BY "
            "(eligible.eed_weight * "
            "(COALESCE(reward.final_novel_host_years, 0) + "
            f"{ACTION_PRIOR_STRENGTH} * CASE eligible.action_kind "
            f"WHEN 'domain' THEN {action_prior_yield(EvidenceActionKind.DOMAIN)} "
            f"WHEN 'rdap' THEN {action_prior_yield(EvidenceActionKind.RDAP)} "
            f"WHEN 'range' THEN {action_prior_yield(EvidenceActionKind.RANGE)} "
            f"ELSE {action_prior_yield(EvidenceActionKind.EXACT)} END) "
            "/ (MAX(COALESCE(cost.provider_requests, 0), "
            "COALESCE(cost.attempts, 0)) + "
            f"{ACTION_PRIOR_STRENGTH}) "
            f"/ (1.0 + {ACTION_RETRY_PENALTY} * eligible.attempt)) DESC, "
            "CASE eligible.action_kind "
            "WHEN 'domain' THEN 3 WHEN 'rdap' THEN 2 "
            "WHEN 'range' THEN 1 ELSE 0 END DESC, "
            "eligible.eed_weight DESC, "
            "(eligible.year_to - eligible.year_from) DESC, "
            "eligible.year_from, eligible.hostname, eligible.year_to, "
            "eligible.provider, eligible.policy_version LIMIT ?"
        )
        lease_until = now + float(lease_seconds)
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            rows = self.connection.execute(query, params).fetchall()
            for row in rows:
                self.connection.execute(
                    """
                    UPDATE evidence_tasks
                    SET lease_owner = ?, lease_until = ?, attempt = attempt + 1
                    WHERE hostname = ? AND year_from = ? AND year_to = ?
                      AND provider = ? AND policy_version = ?
                    """,
                    (owner, lease_until, *self._values(self._key(row))),
                )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return [
            EvidenceTask(
                key=self._key(row),
                state=str(row["state"]),
                attempt=int(row["attempt"]) + 1,
                retry_at=row["retry_at"],
                lease_owner=owner,
                lease_until=lease_until,
            )
            for row in rows
        ]

    def finish_evidence_tasks(
        self,
        results: Iterable[EvidenceQueryResult],
        *,
        owner: str,
    ) -> int:
        """Finish a batch of claimed evidence tasks atomically."""
        if not owner:
            raise ValueError("owner is required")
        return self._finish_evidence_results(results, owner=owner, retry_at=None)

    def finish_range_task(
        self,
        key: EvidenceQueryKey,
        state: CDXQueryState | str,
        *,
        followup_keys: Iterable[EvidenceQueryKey] = (),
        owner: str,
    ) -> int:
        """Finish a terminal range task and enqueue exact-year follow-ups atomically."""
        if not owner:
            raise ValueError("owner is required")
        if key.temporal_scope.year_from == key.temporal_scope.year_to:
            raise ValueError("range task must span multiple years")
        value = state.value if isinstance(state, CDXQueryState) else str(state)
        if value not in TERMINAL_STATES:
            raise ValueError("range task can only finish in a terminal state")
        exact = list(followup_keys)
        for followup in exact:
            scope = followup.temporal_scope
            if scope.year_from != scope.year_to:
                raise ValueError("range follow-up tasks must be exact-year keys")
            if (
                followup.hostname != key.hostname
                or followup.provider != key.provider
                or followup.policy_version != key.policy_version
                or not key.temporal_scope.year_from <= scope.year_from <= key.temporal_scope.year_to
            ):
                raise ValueError("range follow-up key does not match parent range")

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """
                SELECT state FROM evidence_tasks
                WHERE hostname = ? AND year_from = ? AND year_to = ?
                  AND provider = ? AND policy_version = ? AND lease_owner = ?
                """,
                (*self._values(key), owner),
            ).fetchone()
            if row is None:
                raise KeyError("range task not found or not owned by caller")
            self.connection.execute(
                """
                UPDATE evidence_tasks
                SET state = ?, retry_at = NULL, lease_owner = NULL, lease_until = NULL
                WHERE hostname = ? AND year_from = ? AND year_to = ?
                  AND provider = ? AND policy_version = ? AND lease_owner = ?
                """,
                (value, *self._values(key), owner),
            )
            cursor = self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_tasks(
                    hostname, year_from, year_to, provider, policy_version, state
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [(*self._values(followup), CDXQueryState.PENDING.value) for followup in exact],
            )
            created = max(0, int(cursor.rowcount))
            # Range tasks reserve their worst-case net fanout capacity at
            # initial admission. The parent is becoming terminal in this same
            # transaction, so deleting the token after child insertion converts
            # reserved capacity into durable exact-year rows atomically.
            self.connection.execute(
                """
                DELETE FROM evidence_task_fanout_reservations
                WHERE hostname = ? AND year_from = ? AND year_to = ?
                  AND provider = ? AND policy_version = ?
                """,
                self._values(key),
            )
            # Exact-year follow-ups are a refinement of the same source work.
            # Preserve every parent origin so later first-touch host-year
            # attribution remains connected to the original reservoir/lease.
            for followup in exact:
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO evidence_task_origins(
                        hostname, year_from, year_to, provider, policy_version,
                        source_key, reservoir_id, lease_id, first_observed_at
                    )
                    SELECT ?, ?, ?, ?, ?,
                           source_key, reservoir_id, lease_id, first_observed_at
                    FROM evidence_task_origins
                    WHERE hostname = ? AND year_from = ? AND year_to = ?
                      AND provider = ? AND policy_version = ?
                    """,
                    (*self._values(followup), *self._values(key)),
                )
            self.connection.commit()
            return created
        except BaseException:
            self.connection.rollback()
            raise

    def _finish_evidence_results(
        self,
        results: Iterable[EvidenceQueryResult],
        *,
        owner: str | None,
        retry_at: float | None,
    ) -> int:
        keyed_results = [result for result in results if result.key is not None]
        if not keyed_results:
            return 0
        updates: list[tuple[str, None, object, ...]] = []
        seen: set[EvidenceQueryKey] = set()
        for result in keyed_results:
            key = result.key
            assert key is not None
            if key in seen:
                raise ValueError(f"duplicate evidence task key: {key}")
            seen.add(key)
            value = result.state.value if isinstance(result.state, CDXQueryState) else str(result.state)
            if value not in TERMINAL_STATES | RETRYABLE_STATES:
                raise ValueError(f"unsupported evidence task state: {value}")
            updates.append((value, retry_at, *self._values(key)))

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            for result in keyed_results:
                key = result.key
                assert key is not None
                ownership = " AND lease_owner = ?" if owner is not None else ""
                params = (*self._values(key), owner) if owner is not None else self._values(key)
                row = self.connection.execute(
                    """
                    SELECT 1 FROM evidence_tasks
                    WHERE hostname = ? AND year_from = ? AND year_to = ?
                      AND provider = ? AND policy_version = ?
                    """ + ownership,
                    params,
                ).fetchone()
                if row is None:
                    raise KeyError("evidence task not found or not owned by caller")
            ownership = " AND lease_owner = ?" if owner is not None else ""
            self.connection.executemany(
                """
                UPDATE evidence_tasks
                SET state = ?, retry_at = ?, lease_owner = NULL, lease_until = NULL
                WHERE hostname = ? AND year_from = ? AND year_to = ?
                  AND provider = ? AND policy_version = ?
                """ + ownership,
                [(*update, owner) if owner is not None else update for update in updates],
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return len(updates)

    def finish_evidence_task(
        self,
        key: EvidenceQueryKey,
        state: CDXQueryState | str,
        *,
        owner: str | None = None,
        retry_at: float | None = None,
    ) -> None:
        result = EvidenceQueryResult(
            hostname=key.hostname,
            year=key.temporal_scope.year_from,
            state=state if isinstance(state, CDXQueryState) else CDXQueryState(str(state)),
            key=key,
        )
        self._finish_evidence_results([result], owner=owner, retry_at=retry_at)

    def get_evidence_task(self, key: EvidenceQueryKey) -> EvidenceTask | None:
        row = self.connection.execute(
            """
            SELECT * FROM evidence_tasks
            WHERE hostname = ? AND year_from = ? AND year_to = ?
              AND provider = ? AND policy_version = ?
            """,
            self._values(key),
        ).fetchone()
        return None if row is None else self._task(row)

    def list_evidence_tasks(self) -> list[EvidenceTask]:
        rows = self.connection.execute(
            "SELECT * FROM evidence_tasks ORDER BY hostname, year_from, provider, policy_version"
        ).fetchall()
        return [self._task(row) for row in rows]

    def evidence_task_state_counts(self) -> dict[str, int]:
        """Aggregate the durable evidence backlog without materializing tasks."""
        counts = {state.value: 0 for state in CDXQueryState}
        for row in self.connection.execute(
            "SELECT state, COUNT(*) AS count FROM evidence_tasks GROUP BY state"
        ):
            counts[str(row["state"])] = int(row["count"])
        return counts

    def reservoir_state_counts(self) -> dict[str, int]:
        return {
            str(row["state"]): int(row["count"])
            for row in self.connection.execute(
                "SELECT state, COUNT(*) AS count FROM reservoirs GROUP BY state"
            )
        }

    def work_lease_state_counts(self) -> dict[str, int]:
        return {
            str(row["state"]): int(row["count"])
            for row in self.connection.execute(
                "SELECT state, COUNT(*) AS count FROM work_leases GROUP BY state"
            )
        }

    def set_checkpoint(self, key: str, value: str) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT INTO runtime_checkpoints(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def get_checkpoint(self, key: str) -> str | None:
        row = self.connection.execute(
            "SELECT value FROM runtime_checkpoints WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row[0])

    @staticmethod
    def _value(value: Any, default: Any = None) -> Any:
        if value is None:
            return default
        return getattr(value, "value", value)

    @staticmethod
    def _field(obj: Any, name: str, default: Any = None) -> Any:
        return getattr(obj, name, default)

    @staticmethod
    def _runtime_types() -> tuple[Any, Any, Any, Any]:
        # Imported lazily so the evidence-only ControlStore remains importable
        # while the source/lease model slice is developed independently.
        from creeper.scheduler.leases import LeaseState
        from creeper.sources.domains import SourceDomain
        from creeper.sources.reservoirs import Reservoir, ReservoirState

        return SourceDomain, Reservoir, ReservoirState, LeaseState

    def save_domain(self, domain: Any) -> None:
        temporal = self._field(domain, "temporal_scope")
        if temporal is None or len(temporal) != 2:
            raise ValueError("domain temporal_scope must be a (from, to) pair")
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_domains(
                    domain_id, family, discovery_mechanism,
                    temporal_from, temporal_to, state
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain_id) DO UPDATE SET
                    family=excluded.family,
                    discovery_mechanism=excluded.discovery_mechanism,
                    temporal_from=excluded.temporal_from,
                    temporal_to=excluded.temporal_to,
                    state=excluded.state
                """,
                (
                    self._field(domain, "domain_id"),
                    self._value(self._field(domain, "family"), ""),
                    self._field(domain, "discovery_mechanism"),
                    temporal[0], temporal[1],
                    self._value(self._field(domain, "state"), "unexplored"),
                ),
            )

    def get_domain(self, domain_id: str) -> Any | None:
        row = self.connection.execute(
            "SELECT * FROM source_domains WHERE domain_id = ?", (domain_id,)
        ).fetchone()
        if row is None:
            return None
        SourceDomain, _, _, _ = self._runtime_types()
        from creeper.sources.domains import DomainState
        return SourceDomain(
            domain_id=row["domain_id"],
            family=row["family"],
            discovery_mechanism=row["discovery_mechanism"],
            temporal_scope=(row["temporal_from"], row["temporal_to"]),
            state=DomainState(row["state"]),
        )

    def save_reservoir(self, reservoir: Any) -> None:
        if self.get_domain(self._field(reservoir, "domain_id")) is None:
            raise KeyError(f"domain not found: {self._field(reservoir, 'domain_id')}")
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO reservoirs(
                    reservoir_id, domain_id, adapter_id, root_locator,
                    enumeration_kind, capacity_lower, capacity_upper, cursor,
                    evidence_mode, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(reservoir_id) DO UPDATE SET
                    domain_id=excluded.domain_id, adapter_id=excluded.adapter_id,
                    root_locator=excluded.root_locator,
                    enumeration_kind=excluded.enumeration_kind,
                    capacity_lower=excluded.capacity_lower,
                    capacity_upper=excluded.capacity_upper,
                    cursor=excluded.cursor, evidence_mode=excluded.evidence_mode,
                    state=excluded.state
                """,
                (
                    self._field(reservoir, "reservoir_id"),
                    self._field(reservoir, "domain_id"),
                    self._field(reservoir, "adapter_id"),
                    self._field(reservoir, "root_locator"),
                    self._value(self._field(reservoir, "enumeration_kind"), ""),
                    self._field(reservoir, "capacity_lower"),
                    self._field(reservoir, "capacity_upper"),
                    self._field(reservoir, "cursor"),
                    self._value(self._field(reservoir, "evidence_mode"), "discovery_only"),
                    self._value(self._field(reservoir, "state"), "discovered"),
                ),
            )

    def save_activation(
        self,
        *,
        source_key: str,
        domain: Any,
        reservoir: Any,
        adapter_kind: str,
        config_hash: str,
        activation_state: str = "ACTIVE",
    ) -> None:
        """Atomically persist discovery-to-production activation lineage."""
        if not source_key.strip() or not adapter_kind.strip() or not config_hash.strip():
            raise ValueError("activation identity fields are required")
        temporal = self._field(domain, "temporal_scope")
        if temporal is None or len(temporal) != 2:
            raise ValueError("domain temporal_scope must be a (from, to) pair")
        now = float(self.clock())
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_domains(
                    domain_id, family, discovery_mechanism,
                    temporal_from, temporal_to, state
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain_id) DO NOTHING
                """,
                (
                    self._field(domain, "domain_id"),
                    self._value(self._field(domain, "family"), ""),
                    self._field(domain, "discovery_mechanism"),
                    temporal[0], temporal[1],
                    self._value(self._field(domain, "state"), "UNEXPLORED"),
                ),
            )
            self.connection.execute(
                """
                INSERT INTO reservoirs(
                    reservoir_id, domain_id, adapter_id, root_locator,
                    enumeration_kind, capacity_lower, capacity_upper, cursor,
                    evidence_mode, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(reservoir_id) DO NOTHING
                """,
                (
                    self._field(reservoir, "reservoir_id"),
                    self._field(reservoir, "domain_id"),
                    self._field(reservoir, "adapter_id"),
                    self._field(reservoir, "root_locator"),
                    self._value(self._field(reservoir, "enumeration_kind"), ""),
                    self._field(reservoir, "capacity_lower"),
                    self._field(reservoir, "capacity_upper"),
                    self._field(reservoir, "cursor"),
                    self._value(self._field(reservoir, "evidence_mode"), "discovery_only"),
                    self._value(self._field(reservoir, "state"), "DISCOVERED"),
                ),
            )
            self.connection.execute(
                """
                INSERT INTO source_activations(
                    source_key, domain_id, reservoir_id, adapter_id, adapter_kind,
                    root_locator, config_hash, activation_state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    updated_at=excluded.updated_at,
                    activation_state=excluded.activation_state
                """,
                (
                    source_key,
                    self._field(domain, "domain_id"),
                    self._field(reservoir, "reservoir_id"),
                    self._field(reservoir, "adapter_id"),
                    adapter_kind,
                    self._field(reservoir, "root_locator"),
                    config_hash,
                    activation_state,
                    now,
                    now,
                ),
            )

    def get_activation(self, source_key: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM source_activations WHERE source_key = ?", (source_key,)
        ).fetchone()
        return None if row is None else {key: row[key] for key in row.keys()}

    def get_reservoir(self, reservoir_id: str) -> Any | None:
        row = self.connection.execute(
            "SELECT * FROM reservoirs WHERE reservoir_id = ?", (reservoir_id,)
        ).fetchone()
        if row is None:
            return None
        _, Reservoir, ReservoirState, _ = self._runtime_types()
        return Reservoir(
            reservoir_id=row["reservoir_id"], domain_id=row["domain_id"],
            adapter_id=row["adapter_id"], root_locator=row["root_locator"],
            enumeration_kind=row["enumeration_kind"],
            capacity_lower=row["capacity_lower"], capacity_upper=row["capacity_upper"],
            cursor=row["cursor"], evidence_mode=row["evidence_mode"],
            state=ReservoirState(row["state"]),
        )

    def save_lease(self, lease: Any) -> None:
        from creeper.scheduler.leases import LeaseState

        state = self._value(self._field(lease, "state"), "created")
        if state == LeaseState.RUNNING.value:
            row = self.connection.execute(
                "SELECT state FROM work_leases WHERE lease_id = ?",
                (self._field(lease, "lease_id"),),
            ).fetchone()
            if row is None or row["state"] != LeaseState.GRANTED.value:
                raise ValueError("a lease must be persisted as GRANTED before RUNNING")
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO work_leases(
                    lease_id, reservoir_id, cursor_start, cursor_end, max_records,
                    max_requests, max_bytes, max_seconds, resource_class,
                    expected_evidence_tasks, expected_novel_eed, owner,
                    expires_at, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(lease_id) DO UPDATE SET
                    reservoir_id=excluded.reservoir_id, cursor_start=excluded.cursor_start,
                    cursor_end=excluded.cursor_end, max_records=excluded.max_records,
                    max_requests=excluded.max_requests, max_bytes=excluded.max_bytes,
                    max_seconds=excluded.max_seconds, resource_class=excluded.resource_class,
                    expected_evidence_tasks=excluded.expected_evidence_tasks,
                    expected_novel_eed=excluded.expected_novel_eed, owner=excluded.owner,
                    expires_at=excluded.expires_at, state=excluded.state
                """,
                (
                    self._field(lease, "lease_id"), self._field(lease, "reservoir_id"),
                    self._field(lease, "cursor_start"), self._field(lease, "cursor_end"),
                    self._field(lease, "max_records"), self._field(lease, "max_requests"),
                    self._field(lease, "max_bytes"), self._field(lease, "max_seconds"),
                    self._value(self._field(lease, "resource_class"), "general"),
                    self._field(lease, "expected_evidence_tasks", 0),
                    self._field(lease, "expected_novel_eed", 0.0),
                    self._field(lease, "owner"), self._field(lease, "expires_at"), state,
                ),
            )

    def grant_fresh_lease(
        self,
        reservoir_id: str,
        *,
        owner: str,
        max_records: int,
        max_requests: int,
        max_bytes: int,
        max_seconds: float,
        resource_class: str,
        expected_evidence_tasks: int,
        expected_novel_eed: float,
        now: float,
        lease_ttl_seconds: float | None = None,
    ) -> Any | None:
        """Atomically claim a READY reservoir with a new cursor-backed lease."""
        from creeper.scheduler.leases import LeaseState, WorkLease
        from creeper.sources.reservoirs import ReservoirState

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            reservoir = self.connection.execute(
                "SELECT * FROM reservoirs WHERE reservoir_id = ?",
                (reservoir_id,),
            ).fetchone()
            if reservoir is None or reservoir["state"] != ReservoirState.READY.value:
                self.connection.commit()
                return None

            lease = WorkLease.create(
                reservoir_id=reservoir_id,
                cursor_start=reservoir["cursor"],
                max_records=max_records,
                max_requests=max_requests,
                max_bytes=max_bytes,
                max_seconds=max_seconds,
                resource_class=resource_class,
                expected_evidence_tasks=expected_evidence_tasks,
                expected_novel_eed=expected_novel_eed,
                now=float(now),
                expires_at=(
                    None
                    if lease_ttl_seconds is None
                    else float(now) + float(lease_ttl_seconds)
                ),
            ).grant(owner=owner)
            self.connection.execute(
                """
                INSERT INTO work_leases(
                    lease_id, reservoir_id, cursor_start, cursor_end, max_records,
                    max_requests, max_bytes, max_seconds, resource_class,
                    expected_evidence_tasks, expected_novel_eed, owner,
                    expires_at, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lease.lease_id,
                    lease.reservoir_id,
                    lease.cursor_start,
                    lease.cursor_end,
                    lease.max_records,
                    lease.max_requests,
                    lease.max_bytes,
                    lease.max_seconds,
                    lease.resource_class,
                    lease.expected_evidence_tasks,
                    lease.expected_novel_eed,
                    lease.owner,
                    lease.expires_at,
                    LeaseState.GRANTED.value,
                ),
            )
            changed = self.connection.execute(
                """
                UPDATE reservoirs SET state = ?
                WHERE reservoir_id = ? AND state = ?
                """,
                (
                    ReservoirState.LEASED.value,
                    reservoir_id,
                    ReservoirState.READY.value,
                ),
            ).rowcount
            if changed != 1:
                raise RuntimeError("reservoir changed while granting lease")
            self.connection.commit()
            return lease
        except BaseException:
            self.connection.rollback()
            raise

    def renew_lease(
        self,
        lease: Any,
        *,
        ttl_seconds: float,
        now: float | None = None,
    ) -> float:
        """Extend one still-live owned source lease without reviving expiry.

        max_seconds remains the adapter work budget. expires_at is an
        ownership/visibility deadline and may be renewed while the producer is
        alive. An already-expired deadline fails closed because another
        producer may legally recover the reservoir.
        """
        from creeper.scheduler.leases import LeaseState

        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        lease_id = self._field(lease, "lease_id")
        owner = self._field(lease, "owner")
        if not lease_id or not owner:
            raise ValueError("an owned lease is required")
        current = float(self.clock()) if now is None else float(now)
        new_expiry = current + float(ttl_seconds)
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """
                SELECT state, owner, expires_at
                FROM work_leases
                WHERE lease_id = ?
                """,
                (lease_id,),
            ).fetchone()
            if (
                row is None
                or row["owner"] != owner
                or row["state"] not in {
                    LeaseState.GRANTED.value,
                    LeaseState.RUNNING.value,
                }
            ):
                raise ValueError("lease is not live or is not owned by caller")
            if row["expires_at"] is not None and float(row["expires_at"]) <= current:
                raise RuntimeError("source lease expired before renewal")
            effective_expiry = max(
                new_expiry,
                float(row["expires_at"] or 0.0),
            )
            self.connection.execute(
                "UPDATE work_leases SET expires_at = ? WHERE lease_id = ?",
                (effective_expiry, lease_id),
            )
            self.connection.commit()
            return effective_expiry
        except BaseException:
            self.connection.rollback()
            raise

    def finalize_lease(
        self,
        lease: Any,
        *,
        next_cursor: str | None,
        exhausted: bool,
    ) -> None:
        """Atomically finish a running lease and advance its reservoir.

        The lease row and reservoir row form one progress unit.  A successful
        non-EOF execution returns the reservoir to READY at ``next_cursor``;
        an EOF execution makes it EXHAUSTED.  Both updates are guarded by the
        persisted lease owner/state so a stale worker cannot advance a source.
        """
        from creeper.scheduler.leases import LeaseState
        from creeper.sources.reservoirs import ReservoirState

        lease_id = self._field(lease, "lease_id")
        reservoir_id = self._field(lease, "reservoir_id")
        owner = self._field(lease, "owner")
        if not lease_id or not reservoir_id or not owner:
            raise ValueError("a running lease with an owner is required")
        if not exhausted and next_cursor is None:
            raise ValueError("a non-exhausted lease must provide next_cursor")

        target = ReservoirState.EXHAUSTED if exhausted else ReservoirState.READY
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            lease_row = self.connection.execute(
                "SELECT state, owner, reservoir_id FROM work_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
            if (
                lease_row is None
                or lease_row["state"] != LeaseState.RUNNING.value
                or lease_row["owner"] != owner
                or lease_row["reservoir_id"] != reservoir_id
            ):
                raise ValueError("lease is not running or is not owned by caller")
            lease_changed = self.connection.execute(
                "UPDATE work_leases SET state = ? WHERE lease_id = ? AND state = ? AND owner = ?",
                (LeaseState.SUCCEEDED.value, lease_id, LeaseState.RUNNING.value, owner),
            ).rowcount
            if lease_changed != 1:
                raise RuntimeError("lease changed while finalizing")
            reservoir_changed = self.connection.execute(
                """
                UPDATE reservoirs SET state = ?, cursor = ?
                WHERE reservoir_id = ? AND state IN (?, ?)
                """,
                (
                    target.value,
                    next_cursor,
                    reservoir_id,
                    ReservoirState.LEASED.value,
                    ReservoirState.RUNNING.value,
                ),
            ).rowcount
            if reservoir_changed != 1:
                raise ValueError("reservoir is not owned by running lease")
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def abort_lease(self, lease: Any) -> None:
        """Abort a lease and restore its reservoir to the original cursor."""
        from creeper.scheduler.leases import LeaseState
        from creeper.sources.reservoirs import ReservoirState

        lease_id = self._field(lease, "lease_id")
        reservoir_id = self._field(lease, "reservoir_id")
        owner = self._field(lease, "owner")
        if not lease_id or not reservoir_id or not owner:
            raise ValueError("an owned lease is required")

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            lease_row = self.connection.execute(
                "SELECT state, owner, reservoir_id, cursor_start FROM work_leases "
                "WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
            if lease_row is None:
                raise KeyError(f"unknown lease: {lease_id}")
            if lease_row["owner"] != owner or lease_row["reservoir_id"] != reservoir_id:
                raise ValueError("lease is not owned by caller")
            if lease_row["state"] not in {
                LeaseState.GRANTED.value,
                LeaseState.RUNNING.value,
                LeaseState.PAUSED.value,
                LeaseState.PREEMPTED.value,
            }:
                raise ValueError("lease is not active")
            self.connection.execute(
                "UPDATE work_leases SET state = ? WHERE lease_id = ?",
                (LeaseState.ABORTED.value, lease_id),
            )
            changed = self.connection.execute(
                """
                UPDATE reservoirs SET state = ?, cursor = ?
                WHERE reservoir_id = ? AND state IN (?, ?, ?, ?)
                """,
                (
                    ReservoirState.READY.value,
                    lease_row["cursor_start"],
                    reservoir_id,
                    ReservoirState.LEASED.value,
                    ReservoirState.RUNNING.value,
                    ReservoirState.PAUSED.value,
                    ReservoirState.PREEMPTED.value,
                ),
            ).rowcount
            if changed != 1:
                raise ValueError("reservoir is not owned by active lease")
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def get_lease(self, lease_id: str) -> Any | None:
        row = self.connection.execute(
            "SELECT * FROM work_leases WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        if row is None:
            return None
        _, _, _, LeaseState = self._runtime_types()
        from creeper.scheduler.leases import WorkLease
        return WorkLease(
            lease_id=row["lease_id"], reservoir_id=row["reservoir_id"],
            cursor_start=row["cursor_start"], cursor_end=row["cursor_end"],
            max_records=row["max_records"], max_requests=row["max_requests"],
            max_bytes=row["max_bytes"], max_seconds=row["max_seconds"],
            resource_class=row["resource_class"],
            expected_evidence_tasks=row["expected_evidence_tasks"],
            expected_novel_eed=row["expected_novel_eed"], owner=row["owner"],
            expires_at=row["expires_at"], state=LeaseState(row["state"]),
        )

    def recover_expired_leases(self, *, now: float | None = None) -> int:
        from creeper.scheduler.leases import LeaseState
        from creeper.sources.reservoirs import ReservoirState

        if now is None:
            now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            rows = self.connection.execute(
                """
                SELECT lease_id, reservoir_id, cursor_start
                FROM work_leases
                WHERE expires_at <= ? AND state IN (?, ?)
                """,
                (
                    float(now),
                    LeaseState.GRANTED.value,
                    LeaseState.RUNNING.value,
                ),
            ).fetchall()
            for row in rows:
                self.connection.execute(
                    "UPDATE work_leases SET state = ? WHERE lease_id = ?",
                    (LeaseState.EXPIRED.value, row["lease_id"]),
                )
                self.connection.execute(
                    """
                    UPDATE reservoirs
                    SET state = ?, cursor = ?
                    WHERE reservoir_id = ? AND state IN (?, ?, ?, ?)
                    """,
                    (
                        ReservoirState.READY.value,
                        row["cursor_start"],
                        row["reservoir_id"],
                        ReservoirState.LEASED.value,
                        ReservoirState.RUNNING.value,
                        ReservoirState.PAUSED.value,
                        ReservoirState.PREEMPTED.value,
                    ),
                )
            self.connection.commit()
            return len(rows)
        except BaseException:
            self.connection.rollback()
            raise

    def close(self) -> None:
        self.connection.close()
