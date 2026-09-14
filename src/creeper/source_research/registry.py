"""Durable research graph/frontier/reward registry on the shared ControlStore.

The registry owns only additive source_research schema.  It deliberately reuses
ControlStore.connection, preserving the existing SQLite-WAL authority.
"""
from __future__ import annotations

import json
import math
import sqlite3
import time
from collections.abc import Iterable
from dataclasses import replace
from typing import Any

from .models import (
    ArmStats, ArtifactLead, DecisionRecord, FrontierState, FrontierTask,
    LearningEpoch, NegativeKnowledge, PolicySnapshot, QueryProgram, QueryState,
    ResearchEdge, ResearchNode, RewardKind, RewardRecord, RewardScope,
    RootQuery, RootSurface, RuleRecord, SearchCheckpoint, SearchHit, stable_hash,
)


def _dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _load(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


class ResearchRegistry:
    """One durable research-state kernel sharing ControlStore's connection."""

    def __init__(self, control_store: Any, *, clock=time.time) -> None:
        self.control_store = control_store
        self.connection: sqlite3.Connection = control_store.connection
        self.connection.row_factory = sqlite3.Row
        self.clock = clock
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS research_roots (
                root_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                canonical_locator TEXT NOT NULL,
                capabilities_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS research_query_programs (
                program_id TEXT PRIMARY KEY,
                root_id TEXT NOT NULL,
                strategy TEXT NOT NULL,
                seed_library_version TEXT NOT NULL,
                compiler_version TEXT NOT NULL,
                context_hash TEXT NOT NULL,
                hard_max_requests INTEGER NOT NULL CHECK(hard_max_requests > 0),
                requests_started INTEGER NOT NULL DEFAULT 0
                    CHECK(requests_started >= 0),
                stop_conditions_json TEXT NOT NULL,
                source TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at REAL NOT NULL,
                FOREIGN KEY(root_id) REFERENCES research_roots(root_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_program_root
                ON research_query_programs(root_id, state, created_at);

            CREATE TABLE IF NOT EXISTS research_queries (
                query_id TEXT PRIMARY KEY,
                program_id TEXT NOT NULL,
                root_id TEXT NOT NULL,
                seed_identity TEXT NOT NULL UNIQUE,
                query_text TEXT NOT NULL,
                normalized_query TEXT NOT NULL,
                native_filters_json TEXT NOT NULL,
                expected_signal TEXT NOT NULL,
                expected_artifact_family TEXT NOT NULL,
                max_pages INTEGER NOT NULL,
                max_wall_seconds REAL NOT NULL,
                page_size INTEGER NOT NULL,
                state TEXT NOT NULL,
                checkpoint_json TEXT NOT NULL,
                pages_completed INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                wall_seconds_used REAL NOT NULL DEFAULT 0
                    CHECK(wall_seconds_used >= 0),
                retry_at REAL,
                last_error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY(program_id) REFERENCES research_query_programs(program_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_queries_work
                ON research_queries(state, retry_at, root_id, updated_at);

            CREATE TABLE IF NOT EXISTS research_nodes (
                node_id TEXT PRIMARY KEY,
                canonical_key TEXT NOT NULL UNIQUE,
                node_kind TEXT NOT NULL,
                root_id TEXT NOT NULL,
                provider_native_id TEXT NOT NULL,
                provider_url TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_nodes_root_kind
                ON research_nodes(root_id, node_kind, last_seen_at);

            CREATE TABLE IF NOT EXISTS research_edges (
                edge_id TEXT PRIMARY KEY,
                from_node_id TEXT NOT NULL,
                to_node_id TEXT NOT NULL,
                relation TEXT NOT NULL,
                root_id TEXT NOT NULL,
                query_id TEXT NOT NULL,
                pivot_id TEXT NOT NULL,
                observed_at REAL NOT NULL,
                FOREIGN KEY(from_node_id) REFERENCES research_nodes(node_id),
                FOREIGN KEY(to_node_id) REFERENCES research_nodes(node_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_edges_from
                ON research_edges(from_node_id, relation, observed_at);

            CREATE TABLE IF NOT EXISTS research_artifact_lineage (
                lineage_id TEXT PRIMARY KEY,
                artifact_id TEXT NOT NULL,
                artifact_identity TEXT NOT NULL,
                locator TEXT NOT NULL,
                immutable_identity TEXT NOT NULL,
                checksum TEXT NOT NULL,
                persistent_id TEXT NOT NULL,
                parent_persistent_id TEXT NOT NULL,
                root_id TEXT NOT NULL,
                query_id TEXT NOT NULL,
                program_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                pivot_id TEXT NOT NULL,
                source_key TEXT NOT NULL,
                source_exposure_id TEXT NOT NULL,
                decision_id TEXT NOT NULL,
                created_at REAL NOT NULL,
                UNIQUE(
                    artifact_id, root_id, query_id, program_id, node_id,
                    pivot_id, source_key, source_exposure_id, decision_id
                )
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_artifact_identity
                ON research_artifact_lineage(artifact_identity, created_at);
            CREATE INDEX IF NOT EXISTS idx_research_artifact_source
                ON research_artifact_lineage(source_key, source_exposure_id, created_at);

            CREATE TABLE IF NOT EXISTS research_source_decision_lineage (
                source_key TEXT PRIMARY KEY,
                decision_id TEXT NOT NULL,
                scope_kind TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                bound_at REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_source_decision
                ON research_source_decision_lineage(decision_id, bound_at);

            CREATE TABLE IF NOT EXISTS research_lineage_exposures (
                lineage_id TEXT NOT NULL,
                source_key TEXT NOT NULL,
                exposure_id TEXT NOT NULL,
                bound_at REAL NOT NULL,
                PRIMARY KEY(lineage_id, exposure_id),
                FOREIGN KEY(lineage_id)
                    REFERENCES research_artifact_lineage(lineage_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_lineage_exposure
                ON research_lineage_exposures(
                    source_key, exposure_id, bound_at
                );

            CREATE TABLE IF NOT EXISTS research_rewards (
                reward_id TEXT PRIMARY KEY,
                reward_kind TEXT NOT NULL,
                reward_scope TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                source_key TEXT NOT NULL,
                exposure_id TEXT NOT NULL,
                decision_id TEXT NOT NULL,
                amount REAL NOT NULL,
                validation_closed INTEGER NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                policy_version TEXT NOT NULL,
                created_at REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_rewards_entity
                ON research_rewards(reward_kind, reward_scope, entity_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_research_rewards_decision
                ON research_rewards(decision_id, reward_kind, created_at);

            CREATE TABLE IF NOT EXISTS research_negative_knowledge (
                negative_id TEXT PRIMARY KEY,
                root_id TEXT NOT NULL,
                scope_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                reason TEXT NOT NULL,
                exhaustive INTEGER NOT NULL,
                context_hash TEXT NOT NULL,
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL,
                observations INTEGER NOT NULL DEFAULT 1,
                UNIQUE(root_id, scope_kind, scope_key, reason, context_hash)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS research_frontier_tasks (
                task_id TEXT PRIMARY KEY,
                task_kind TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                state TEXT NOT NULL,
                priority REAL NOT NULL,
                checkpoint_json TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0 CHECK(attempt >= 0),
                retry_at REAL,
                lease_owner TEXT,
                lease_until REAL,
                policy_version TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                last_error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(task_kind, entity_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_frontier_claim
                ON research_frontier_tasks(state, retry_at, lease_until, priority, updated_at);

            CREATE TABLE IF NOT EXISTS research_decisions (
                decision_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                arm_id TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                policy_snapshot_id TEXT NOT NULL,
                propensity REAL NOT NULL CHECK(propensity > 0 AND propensity <= 1),
                context_hash TEXT NOT NULL,
                chosen_at REAL NOT NULL,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(task_id) REFERENCES research_frontier_tasks(task_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_decisions_policy
                ON research_decisions(policy_version, arm_id, chosen_at);

            CREATE TABLE IF NOT EXISTS research_policy_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                policy_version TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                parameters_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_policy_active
                ON research_policy_snapshots(active, policy_version, created_at);

            CREATE TABLE IF NOT EXISTS research_arm_stats (
                arm_id TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                pulls INTEGER NOT NULL DEFAULT 0,
                proxy_reward REAL NOT NULL DEFAULT 0,
                final_reward REAL NOT NULL DEFAULT 0,
                final_observation_count INTEGER NOT NULL DEFAULT 0
                    CHECK(final_observation_count >= 0),
                decayed_reward REAL NOT NULL DEFAULT 0,
                schema_version INTEGER NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(arm_id, policy_version)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS research_rule_registry (
                rule_id TEXT PRIMARY KEY,
                rule_kind TEXT NOT NULL,
                version TEXT NOT NULL,
                context_hash TEXT NOT NULL,
                deterministic_payload_json TEXT NOT NULL,
                state TEXT NOT NULL,
                validation_digest TEXT NOT NULL,
                reuse_count INTEGER NOT NULL DEFAULT 0 CHECK(reuse_count >= 0),
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_rules_state
                ON research_rule_registry(state, rule_kind, updated_at);

            CREATE TABLE IF NOT EXISTS research_learning_epochs (
                epoch_id TEXT PRIMARY KEY,
                policy_version TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                started_at REAL NOT NULL,
                ended_at REAL,
                state TEXT NOT NULL,
                checkpoint_json TEXT NOT NULL,
                notes TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_research_epochs_policy
                ON research_learning_epochs(policy_version, state, started_at);
            """
        )
        # Additive migrations for databases created by earlier L3 revisions.
        program_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(research_query_programs)"
            ).fetchall()
        }
        if "requests_started" not in program_columns:
            self.connection.execute(
                """
                ALTER TABLE research_query_programs
                ADD COLUMN requests_started INTEGER NOT NULL DEFAULT 0
                CHECK(requests_started >= 0)
                """
            )

        query_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(research_queries)"
            ).fetchall()
        }
        if "wall_seconds_used" not in query_columns:
            self.connection.execute(
                """
                ALTER TABLE research_queries
                ADD COLUMN wall_seconds_used REAL NOT NULL DEFAULT 0
                CHECK(wall_seconds_used >= 0)
                """
            )

        arm_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(research_arm_stats)"
            ).fetchall()
        }
        if "final_observation_count" not in arm_columns:
            self.connection.execute(
                """
                ALTER TABLE research_arm_stats
                ADD COLUMN final_observation_count INTEGER NOT NULL DEFAULT 0
                CHECK(final_observation_count >= 0)
                """
            )
            # Arm stats are derived policy state.  Old rows encode the former
            # "FINAL=0 means missing" semantics and must not survive a schema
            # migration as if they were authoritative facts.
            self.connection.execute("DELETE FROM research_arm_stats")
        self.connection.commit()

    # roots / programs / queries -------------------------------------------------
    def upsert_root(self, root: RootSurface) -> str:
        now = self.clock()
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO research_roots(
                    root_id,kind,canonical_locator,capabilities_json,metadata_json,
                    active,created_at,updated_at
                ) VALUES(?,?,?,?,?,1,?,?)
                ON CONFLICT(root_id) DO UPDATE SET
                    kind=excluded.kind,
                    canonical_locator=excluded.canonical_locator,
                    capabilities_json=excluded.capabilities_json,
                    metadata_json=excluded.metadata_json,
                    active=1,
                    updated_at=excluded.updated_at
                """,
                (
                    root.root_id, root.kind.value, root.canonical_locator,
                    _dump(list(root.capabilities)), _dump(dict(root.metadata)), now, now,
                ),
            )
        return root.root_id

    def register_program(self, program: QueryProgram) -> tuple[str, tuple[str, ...]]:
        now = self.clock()
        query_ids: list[str] = []
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO research_query_programs(
                    program_id,root_id,strategy,seed_library_version,
                    compiler_version,context_hash,hard_max_requests,
                    stop_conditions_json,source,state,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,'READY',?)
                """,
                (
                    program.program_id, program.root_id, program.strategy,
                    program.seed_library_version, program.compiler_version,
                    program.context_hash, program.hard_max_requests,
                    _dump(list(program.stop_conditions)), program.source, now,
                ),
            )
            for query in program.queries:
                query_ids.append(self._register_query_locked(program.program_id, query, now)[0])
        return program.program_id, tuple(query_ids)

    def _register_query_locked(
        self, program_id: str, query: RootQuery, now: float
    ) -> tuple[str, bool]:
        existing = self.connection.execute(
            "SELECT query_id FROM research_queries WHERE seed_identity=?",
            (query.seed_identity,),
        ).fetchone()
        if existing is not None:
            return str(existing["query_id"]), False
        self.connection.execute(
            """
            INSERT INTO research_queries(
                query_id,program_id,root_id,seed_identity,query_text,
                normalized_query,native_filters_json,expected_signal,
                expected_artifact_family,max_pages,max_wall_seconds,page_size,
                state,checkpoint_json,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'READY','{}',?,?)
            """,
            (
                query.query_id, program_id, query.root_id, query.seed_identity,
                query.query_text, " ".join(query.query_text.split()).casefold(),
                _dump(dict(query.native_filters)), query.expected_signal,
                query.expected_artifact_family, query.max_pages,
                query.max_wall_seconds, query.page_size, now, now,
            ),
        )
        return query.query_id, True

    def register_query(self, program_id: str, query: RootQuery) -> tuple[str, bool]:
        now = self.clock()
        with self.connection:
            return self._register_query_locked(program_id, query, now)

    def reserve_program_request(self, program_id: str) -> bool:
        """Atomically reserve one provider request from a program hard budget.

        Reservation happens before any network I/O. Failed, retryable and
        timed-out provider calls intentionally still consume a reservation:
        hard_max_requests is a cost/safety ceiling, not a success counter.
        """
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """
                SELECT state, hard_max_requests, requests_started
                FROM research_query_programs
                WHERE program_id=?
                """,
                (program_id,),
            ).fetchone()
            if row is None:
                raise KeyError(program_id)
            if (
                str(row["state"]) != "READY"
                or int(row["requests_started"]) >= int(row["hard_max_requests"])
            ):
                self.connection.commit()
                return False
            changed = self.connection.execute(
                """
                UPDATE research_query_programs
                SET requests_started=requests_started+1
                WHERE program_id=?
                  AND state='READY'
                  AND requests_started<hard_max_requests
                """,
                (program_id,),
            ).rowcount
            self.connection.commit()
            return changed == 1
        except BaseException:
            self.connection.rollback()
            raise

    def program_request_budget(self, program_id: str) -> tuple[int, int]:
        row = self.connection.execute(
            """
            SELECT requests_started, hard_max_requests
            FROM research_query_programs
            WHERE program_id=?
            """,
            (program_id,),
        ).fetchone()
        if row is None:
            raise KeyError(program_id)
        return int(row["requests_started"]), int(row["hard_max_requests"])

    def exhaust_program_budget(self, program_id: str) -> None:
        """Close unstarted work after the immutable request ceiling is spent."""
        with self.connection:
            row = self.connection.execute(
                """
                SELECT hard_max_requests, requests_started
                FROM research_query_programs
                WHERE program_id=?
                """,
                (program_id,),
            ).fetchone()
            if row is None:
                raise KeyError(program_id)
            if int(row["requests_started"]) < int(row["hard_max_requests"]):
                return
            self.connection.execute(
                """
                UPDATE research_query_programs
                SET state='EXHAUSTED'
                WHERE program_id=? AND state='READY'
                """,
                (program_id,),
            )
            self.connection.execute(
                """
                UPDATE research_queries
                SET state='EXHAUSTED', retry_at=NULL,
                    last_error=CASE
                        WHEN last_error='' THEN 'program hard request budget exhausted'
                        ELSE last_error
                    END,
                    updated_at=?
                WHERE program_id=? AND state IN ('READY','RETRYABLE')
                """,
                (self.clock(), program_id),
            )

    def get_root(self, root_id: str) -> RootSurface:
        row = self.connection.execute(
            "SELECT * FROM research_roots WHERE root_id=? AND active=1",
            (root_id,),
        ).fetchone()
        if row is None:
            raise KeyError(root_id)
        return RootSurface(
            root_id=str(row["root_id"]),
            kind=str(row["kind"]),
            canonical_locator=str(row["canonical_locator"]),
            capabilities=tuple(_load(row["capabilities_json"], [])),
            metadata=_load(row["metadata_json"], {}),
        )

    def get_query(self, query_id: str) -> tuple[RootQuery, str]:
        row = self.get_query_row(query_id)
        return (
            RootQuery(
                root_id=str(row["root_id"]),
                query_text=str(row["query_text"]),
                max_pages=int(row["max_pages"]),
                max_wall_seconds=float(row["max_wall_seconds"]),
                page_size=int(row["page_size"]),
                native_filters=_load(row["native_filters_json"], {}),
                expected_signal=str(row["expected_signal"]),
                expected_artifact_family=str(row["expected_artifact_family"]),
                query_id=str(row["query_id"]),
            ),
            str(row["program_id"]),
        )

    def ensure_query_frontier(self, *, limit: int = 100) -> int:
        """Create idempotent durable QUERY tasks for executable query rows."""
        if limit < 1:
            return 0
        now = float(self.clock())
        rows = self.connection.execute(
            """
            SELECT q.query_id
            FROM research_queries AS q
            JOIN research_query_programs AS p
              ON p.program_id=q.program_id
            JOIN research_roots AS r
              ON r.root_id=q.root_id
            WHERE q.state IN ('READY','RETRYABLE')
              AND (q.retry_at IS NULL OR q.retry_at<=?)
              AND p.state='READY'
              AND r.active=1
            ORDER BY q.updated_at, q.query_id
            LIMIT ?
            """,
            (now, int(limit)),
        ).fetchall()
        inserted = 0
        for row in rows:
            query_id = str(row["query_id"])
            task_id = stable_hash("frontier-query", query_id)
            before = self.connection.total_changes
            self.enqueue_frontier(
                FrontierTask(
                    task_id=task_id,
                    task_kind="QUERY",
                    entity_id=query_id,
                )
            )
            inserted += int(self.connection.total_changes > before)
        return inserted

    def latest_decision_for_task(self, task_id: str) -> DecisionRecord | None:
        row = self.connection.execute(
            """
            SELECT decision_id
            FROM research_decisions
            WHERE task_id=?
            ORDER BY chosen_at DESC, decision_id DESC
            LIMIT 1
            """,
            (task_id,),
        ).fetchone()
        if row is None:
            return None
        return self.get_decision(str(row["decision_id"]))

    def query_checkpoint(self, query_id: str) -> SearchCheckpoint | None:
        row = self.connection.execute(
            "SELECT checkpoint_json FROM research_queries WHERE query_id=?",
            (query_id,),
        ).fetchone()
        if row is None:
            raise KeyError(query_id)
        return SearchCheckpoint.from_dict(_load(row["checkpoint_json"], {}))

    def update_query_checkpoint(
        self,
        query_id: str,
        *,
        checkpoint: SearchCheckpoint | None,
        state: QueryState | str,
        pages_delta: int = 0,
        wall_seconds_delta: float = 0.0,
        retry_at: float | None = None,
        last_error: str = "",
    ) -> None:
        state = QueryState(state)
        if pages_delta < 0:
            raise ValueError("pages_delta must be non-negative")
        wall_seconds_delta = float(wall_seconds_delta)
        if not math.isfinite(wall_seconds_delta) or wall_seconds_delta < 0:
            raise ValueError("wall_seconds_delta must be finite and non-negative")
        now = self.clock()
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE research_queries SET
                    checkpoint_json=?, state=?,
                    pages_completed=pages_completed+?,
                    attempts=attempts+CASE WHEN ? IN ('RUNNING','RETRYABLE','FAILED') THEN 1 ELSE 0 END,
                    wall_seconds_used=wall_seconds_used+?,
                    retry_at=?, last_error=?, updated_at=?
                WHERE query_id=?
                """,
                (
                    _dump(checkpoint.as_dict() if checkpoint else {}),
                    state.value, pages_delta, state.value, wall_seconds_delta,
                    retry_at, last_error, now, query_id,
                ),
            ).rowcount
        if changed != 1:
            raise KeyError(query_id)

    def get_query_row(self, query_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM research_queries WHERE query_id=?", (query_id,)
        ).fetchone()
        if row is None:
            raise KeyError(query_id)
        return row

    # graph / artifacts ---------------------------------------------------------
    def upsert_hit(self, hit: SearchHit) -> ResearchNode:
        return self.upsert_node(ResearchNode.from_hit(hit))

    def upsert_node(self, node: ResearchNode) -> ResearchNode:
        now = self.clock()
        existing = self.connection.execute(
            "SELECT node_id FROM research_nodes WHERE canonical_key=?",
            (node.canonical_key,),
        ).fetchone()
        if existing is not None:
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE research_nodes SET last_seen_at=?,
                        title=CASE WHEN title='' THEN ? ELSE title END,
                        description=CASE WHEN description='' THEN ? ELSE description END
                    WHERE node_id=?
                    """,
                    (now, node.title, node.description, existing["node_id"]),
                )
            row = self.connection.execute(
                "SELECT * FROM research_nodes WHERE node_id=?", (existing["node_id"],)
            ).fetchone()
            return self._node_from_row(row)

        with self.connection:
            self.connection.execute(
                """
                INSERT INTO research_nodes(
                    node_id,canonical_key,node_kind,root_id,provider_native_id,
                    provider_url,title,description,metadata_json,
                    first_seen_at,last_seen_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    node.node_id, node.canonical_key, node.kind.value, node.root_id,
                    node.provider_native_id, node.provider_url, node.title,
                    node.description, _dump(dict(node.metadata)), now, now,
                ),
            )
        return node

    @staticmethod
    def _node_from_row(row: sqlite3.Row) -> ResearchNode:
        return ResearchNode(
            node_id=row["node_id"], canonical_key=row["canonical_key"],
            kind=row["node_kind"], root_id=row["root_id"],
            provider_native_id=row["provider_native_id"],
            provider_url=row["provider_url"], title=row["title"],
            description=row["description"], metadata=_load(row["metadata_json"], {}),
        )

    def add_edge(self, edge: ResearchEdge) -> bool:
        with self.connection:
            changed = self.connection.execute(
                """
                INSERT OR IGNORE INTO research_edges(
                    edge_id,from_node_id,to_node_id,relation,root_id,
                    query_id,pivot_id,observed_at
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    edge.edge_id, edge.from_node_id, edge.to_node_id, edge.relation,
                    edge.root_id, edge.query_id, edge.pivot_id,
                    edge.observed_at or self.clock(),
                ),
            ).rowcount
        return changed == 1

    def register_artifact_lead(
        self, lead: ArtifactLead
    ) -> tuple[str, str, bool]:
        artifact_id = lead.artifact_id
        lineage_id = stable_hash(
            "artifact-lineage", artifact_id, lead.root_id, lead.query_id,
            lead.program_id, lead.source_node_id, lead.pivot_id, lead.decision_id,
        )
        now = self.clock()
        with self.connection:
            changed = self.connection.execute(
                """
                INSERT OR IGNORE INTO research_artifact_lineage(
                    lineage_id,artifact_id,artifact_identity,locator,
                    immutable_identity,checksum,persistent_id,parent_persistent_id,
                    root_id,query_id,program_id,node_id,pivot_id,source_key,
                    source_exposure_id,decision_id,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'','',?,?)
                """,
                (
                    lineage_id, artifact_id, lead.artifact_identity, lead.locator,
                    lead.immutable_identity or "", lead.checksum or "",
                    lead.persistent_id or "", lead.parent_persistent_id or "",
                    lead.root_id, lead.query_id, lead.program_id,
                    lead.source_node_id, lead.pivot_id, lead.decision_id, now,
                ),
            ).rowcount
        return artifact_id, lineage_id, changed == 1

    def bind_artifact_source(
        self,
        artifact_id: str,
        *,
        source_key: str,
        source_exposure_id: str = "",
    ) -> int:
        if not source_key:
            raise ValueError("source_key is required")
        with self.connection:
            # Binding is intentionally two-phase. Research can identify the
            # durable source before production creates an exposure; later the
            # same source may fill the still-empty exposure exactly once.
            # Neither source identity nor a non-empty exposure may be rebound.
            changed = self.connection.execute(
                """
                UPDATE research_artifact_lineage
                SET source_key = CASE
                        WHEN source_key='' THEN ? ELSE source_key
                    END,
                    source_exposure_id = CASE
                        WHEN source_exposure_id='' THEN ? ELSE source_exposure_id
                    END
                WHERE artifact_id=?
                  AND (source_key='' OR source_key=?)
                  AND (
                      source_exposure_id=''
                      OR source_exposure_id=?
                      OR ?=''
                  )
                """,
                (
                    source_key,
                    source_exposure_id,
                    artifact_id,
                    source_key,
                    source_exposure_id,
                    source_exposure_id,
                ),
            ).rowcount
        return int(changed)

    def artifact_lineage(
        self, *, source_key: str = "", exposure_id: str = ""
    ) -> tuple[sqlite3.Row, ...]:
        sql = "SELECT * FROM research_artifact_lineage WHERE 1=1"
        params: list[Any] = []
        if source_key:
            sql += " AND source_key=?"
            params.append(source_key)
        if exposure_id:
            sql += " AND source_exposure_id=?"
            params.append(exposure_id)
        sql += " ORDER BY created_at,lineage_id"
        return tuple(self.connection.execute(sql, params).fetchall())

    def bind_source_decision(
        self,
        *,
        source_key: str,
        decision_id: str,
        scope_kind: str,
        entity_id: str,
    ) -> bool:
        """Bind the first discovery decision for a source exactly once.

        This is intentionally first-touch attribution. If another region later
        rediscovers the same canonical source, it cannot steal or duplicate the
        eventual production FINAL credit.
        """
        if not source_key or not decision_id or not scope_kind or not entity_id:
            raise ValueError(
                "source_key, decision_id, scope_kind and entity_id are required"
            )
        self.get_decision(decision_id)
        with self.connection:
            changed = self.connection.execute(
                """
                INSERT OR IGNORE INTO research_source_decision_lineage(
                    source_key,decision_id,scope_kind,entity_id,bound_at
                ) VALUES(?,?,?,?,?)
                """,
                (
                    source_key,
                    decision_id,
                    scope_kind,
                    entity_id,
                    self.clock(),
                ),
            ).rowcount
        return changed == 1

    def source_decision_id(self, source_key: str) -> str:
        row = self.connection.execute(
            """
            SELECT decision_id
            FROM research_source_decision_lineage
            WHERE source_key=?
            """,
            (source_key,),
        ).fetchone()
        return "" if row is None else str(row["decision_id"])

    def bind_source_exposure_lineage(
        self,
        *,
        source_key: str,
        exposure_id: str,
    ) -> int:
        """Bind one production exposure to every causal lineage for its source.

        A source may be measured in multiple independent production exposures.
        This additive mapping preserves every observation without mutating the
        immutable artifact lineage or stealing it from an earlier exposure.
        """
        if not source_key or not exposure_id:
            raise ValueError("source_key and exposure_id are required")
        now = self.clock()
        with self.connection:
            changed = self.connection.execute(
                """
                INSERT OR IGNORE INTO research_lineage_exposures(
                    lineage_id,source_key,exposure_id,bound_at
                )
                SELECT lineage_id,source_key,?,?
                FROM research_artifact_lineage
                WHERE source_key=?
                """,
                (exposure_id, now, source_key),
            ).rowcount
        return int(changed)

    def artifact_lineage_for_exposure(
        self,
        *,
        source_key: str,
        exposure_id: str,
    ) -> tuple[sqlite3.Row, ...]:
        """Return causal lineage for one concrete production observation."""
        rows = tuple(
            self.connection.execute(
                """
                SELECT l.*
                FROM research_artifact_lineage AS l
                JOIN research_lineage_exposures AS x
                  ON x.lineage_id=l.lineage_id
                WHERE x.source_key=? AND x.exposure_id=?
                ORDER BY l.created_at,l.lineage_id
                """,
                (source_key, exposure_id),
            ).fetchall()
        )
        if rows:
            return rows
        # Backward compatibility for databases populated before the additive
        # exposure mapping existed.
        return self.artifact_lineage(
            source_key=source_key,
            exposure_id=exposure_id,
        )

    # frontier -----------------------------------------------------------------
    def enqueue_frontier(self, task: FrontierTask) -> str:
        now = self.clock()
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO research_frontier_tasks(
                    task_id,task_kind,entity_id,state,priority,checkpoint_json,
                    attempt,retry_at,lease_owner,lease_until,policy_version,
                    schema_version,last_error,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    task.task_id, task.task_kind, task.entity_id, task.state.value,
                    task.priority, _dump(dict(task.checkpoint)), task.attempt,
                    task.retry_at, task.lease_owner, task.lease_until,
                    task.policy_version, task.schema_version, task.last_error,
                    now, now,
                ),
            )
        row = self.connection.execute(
            "SELECT task_id FROM research_frontier_tasks WHERE task_kind=? AND entity_id=?",
            (task.task_kind, task.entity_id),
        ).fetchone()
        return str(row["task_id"])

    def claim_frontier(
        self,
        *,
        owner: str,
        lease_seconds: float,
        now: float | None = None,
        task_kind: str = "",
    ) -> FrontierTask | None:
        if not owner or lease_seconds <= 0:
            raise ValueError("owner and positive lease_seconds are required")
        now = self.clock() if now is None else now
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            sql = """
                SELECT * FROM research_frontier_tasks
                WHERE state IN ('READY','RETRYABLE')
                  AND (retry_at IS NULL OR retry_at<=?)
            """
            params: list[Any] = [now]
            if task_kind:
                sql += " AND task_kind=?"
                params.append(task_kind)
            sql += " ORDER BY priority DESC, updated_at ASC, task_id ASC LIMIT 1"
            row = self.connection.execute(sql, params).fetchone()
            if row is None:
                self.connection.commit()
                return None
            lease_until = now + lease_seconds
            changed = self.connection.execute(
                """
                UPDATE research_frontier_tasks
                SET state='CLAIMED',lease_owner=?,lease_until=?,
                    attempt=attempt+1,updated_at=?
                WHERE task_id=? AND state IN ('READY','RETRYABLE')
                """,
                (owner, lease_until, now, row["task_id"]),
            ).rowcount
            if changed != 1:
                self.connection.rollback()
                return None
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return self.get_frontier(str(row["task_id"]))

    def claim_frontier_task(
        self,
        task_id: str,
        *,
        owner: str,
        lease_seconds: float,
        now: float | None = None,
    ) -> FrontierTask | None:
        if not task_id or not owner or lease_seconds <= 0:
            raise ValueError("task_id, owner and positive lease_seconds are required")
        now = self.clock() if now is None else now
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            row = self.connection.execute(
                """
                SELECT *
                FROM research_frontier_tasks
                WHERE task_id=?
                  AND state IN ('READY','RETRYABLE')
                  AND (retry_at IS NULL OR retry_at<=?)
                """,
                (task_id, now),
            ).fetchone()
            if row is None:
                self.connection.commit()
                return None
            changed = self.connection.execute(
                """
                UPDATE research_frontier_tasks
                SET state='CLAIMED', lease_owner=?, lease_until=?,
                    attempt=attempt+1, updated_at=?
                WHERE task_id=? AND state IN ('READY','RETRYABLE')
                """,
                (owner, now + lease_seconds, now, task_id),
            ).rowcount
            if changed != 1:
                self.connection.rollback()
                return None
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return self.get_frontier(task_id)

    def get_frontier(self, task_id: str) -> FrontierTask:
        row = self.connection.execute(
            "SELECT * FROM research_frontier_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if row is None:
            raise KeyError(task_id)
        return self._frontier_from_row(row)

    @staticmethod
    def _frontier_from_row(row: sqlite3.Row) -> FrontierTask:
        return FrontierTask(
            task_id=row["task_id"], task_kind=row["task_kind"],
            entity_id=row["entity_id"], state=row["state"],
            priority=float(row["priority"]),
            checkpoint=_load(row["checkpoint_json"], {}),
            attempt=int(row["attempt"]), retry_at=row["retry_at"],
            lease_owner=row["lease_owner"], lease_until=row["lease_until"],
            policy_version=row["policy_version"],
            schema_version=int(row["schema_version"]),
            last_error=row["last_error"],
        )

    def heartbeat_frontier(
        self, task_id: str, *, owner: str, lease_seconds: float, now: float | None = None
    ) -> bool:
        now = self.clock() if now is None else now
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE research_frontier_tasks SET lease_until=?,updated_at=?
                WHERE task_id=? AND state='CLAIMED' AND lease_owner=?
                """,
                (now + lease_seconds, now, task_id, owner),
            ).rowcount
        return changed == 1

    def finish_frontier(
        self,
        task_id: str,
        *,
        state: FrontierState | str,
        checkpoint: dict[str, Any] | None = None,
        retry_at: float | None = None,
        last_error: str = "",
    ) -> None:
        state = FrontierState(state)
        if state is FrontierState.CLAIMED:
            raise ValueError("finish state cannot be CLAIMED")
        now = self.clock()
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE research_frontier_tasks SET state=?,checkpoint_json=?,
                    retry_at=?,lease_owner=NULL,lease_until=NULL,last_error=?,updated_at=?
                WHERE task_id=?
                """,
                (
                    state.value, _dump(checkpoint or {}), retry_at,
                    last_error, now, task_id,
                ),
            ).rowcount
        if changed != 1:
            raise KeyError(task_id)

    def reclaim_stale_leases(self, *, now: float | None = None) -> int:
        now = self.clock() if now is None else now
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE research_frontier_tasks SET
                    state='READY',lease_owner=NULL,lease_until=NULL,updated_at=?
                WHERE state='CLAIMED' AND lease_until IS NOT NULL AND lease_until<=?
                """,
                (now, now),
            ).rowcount
        return int(changed)

    def ready_frontier(self, *, now: float | None = None) -> tuple[FrontierTask, ...]:
        now = self.clock() if now is None else now
        rows = self.connection.execute(
            """
            SELECT * FROM research_frontier_tasks
            WHERE state IN ('READY','RETRYABLE')
              AND (retry_at IS NULL OR retry_at<=?)
            ORDER BY priority DESC,updated_at ASC,task_id ASC
            """,
            (now,),
        ).fetchall()
        return tuple(self._frontier_from_row(row) for row in rows)

    # policy / rules ------------------------------------------------------------
    def record_decision(self, decision: DecisionRecord) -> bool:
        with self.connection:
            changed = self.connection.execute(
                """
                INSERT OR IGNORE INTO research_decisions(
                    decision_id,task_id,arm_id,policy_version,policy_snapshot_id,
                    propensity,context_hash,chosen_at,metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    decision.decision_id, decision.task_id, decision.arm_id,
                    decision.policy_version, decision.policy_snapshot_id,
                    decision.propensity, decision.context_hash, decision.chosen_at,
                    _dump(dict(decision.metadata)),
                ),
            ).rowcount
        return changed == 1

    def get_decision(self, decision_id: str) -> DecisionRecord:
        """Return one immutable propensity-bearing decision record."""
        row = self.connection.execute(
            "SELECT * FROM research_decisions WHERE decision_id=?",
            (decision_id,),
        ).fetchone()
        if row is None:
            raise KeyError(decision_id)
        return DecisionRecord(
            decision_id=row["decision_id"],
            task_id=row["task_id"],
            arm_id=row["arm_id"],
            policy_version=row["policy_version"],
            policy_snapshot_id=row["policy_snapshot_id"],
            propensity=float(row["propensity"]),
            context_hash=row["context_hash"],
            chosen_at=float(row["chosen_at"]),
            metadata=_load(row["metadata_json"], {}),
        )

    def get_decision_lineage(
        self, decision_id: str, *, strict: bool = True
    ) -> tuple[DecisionRecord, ...]:
        """Expand a leaf decision into an ordered L0→leaf ancestry.

        L7 currently records the complete parent list in each lower-level
        decision's context metadata.  Recursive expansion also supports a future
        writer that stores only immediate parents.  Missing ancestors fail
        closed by default so FINAL reward is never partially attributed.
        """
        result: list[DecisionRecord] = []
        emitted: set[str] = set()
        visiting: set[str] = set()

        def visit(current_id: str) -> None:
            if current_id in emitted:
                return
            if current_id in visiting:
                raise ValueError("decision lineage cycle detected")
            try:
                decision = self.get_decision(current_id)
            except KeyError:
                if strict:
                    raise
                return
            visiting.add(current_id)
            for parent_id in decision.parent_decision_ids:
                visit(parent_id)
            visiting.remove(current_id)
            emitted.add(current_id)
            result.append(decision)

        visit(decision_id)
        return tuple(result)

    def has_decisions(self, *, policy_version: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM research_decisions WHERE policy_version=? LIMIT 1",
            (policy_version,),
        ).fetchone()
        return row is not None

    def active_policy_snapshot(self) -> PolicySnapshot | None:
        row = self.connection.execute(
            """
            SELECT *
            FROM research_policy_snapshots
            WHERE active=1
            ORDER BY created_at DESC, snapshot_id DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        return PolicySnapshot(
            snapshot_id=str(row["snapshot_id"]),
            policy_version=str(row["policy_version"]),
            schema_version=int(row["schema_version"]),
            parameters=_load(row["parameters_json"], {}),
            created_at=float(row["created_at"]),
            active=True,
        )

    def latest_decision_for_arm(self, arm_id: str) -> DecisionRecord | None:
        row = self.connection.execute(
            """
            SELECT decision_id
            FROM research_decisions
            WHERE arm_id=?
            ORDER BY chosen_at DESC, decision_id DESC
            LIMIT 1
            """,
            (arm_id,),
        ).fetchone()
        if row is None:
            return None
        return self.get_decision(str(row["decision_id"]))

    def upsert_policy_snapshot(self, snapshot: PolicySnapshot) -> None:
        with self.connection:
            if snapshot.active:
                self.connection.execute(
                    "UPDATE research_policy_snapshots SET active=0 WHERE policy_version=?",
                    (snapshot.policy_version,),
                )
            self.connection.execute(
                """
                INSERT INTO research_policy_snapshots(
                    snapshot_id,policy_version,schema_version,parameters_json,
                    created_at,active
                ) VALUES(?,?,?,?,?,?)
                ON CONFLICT(snapshot_id) DO UPDATE SET
                    parameters_json=excluded.parameters_json,
                    active=excluded.active
                """,
                (
                    snapshot.snapshot_id, snapshot.policy_version,
                    snapshot.schema_version, _dump(dict(snapshot.parameters)),
                    snapshot.created_at, int(snapshot.active),
                ),
            )

    def upsert_rule(self, rule: RuleRecord) -> None:
        now = self.clock()
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO research_rule_registry(
                    rule_id,rule_kind,version,context_hash,
                    deterministic_payload_json,state,validation_digest,
                    reuse_count,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(rule_id) DO UPDATE SET
                    deterministic_payload_json=excluded.deterministic_payload_json,
                    state=excluded.state,
                    validation_digest=excluded.validation_digest,
                    updated_at=excluded.updated_at
                """,
                (
                    rule.rule_id, rule.rule_kind, rule.version, rule.context_hash,
                    _dump(dict(rule.deterministic_payload)), rule.state.value,
                    rule.validation_digest, rule.reuse_count, now, now,
                ),
            )

    def increment_rule_reuse(self, rule_id: str) -> int:
        now = self.clock()
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE research_rule_registry
                SET reuse_count=reuse_count+1,updated_at=? WHERE rule_id=?
                """,
                (now, rule_id),
            ).rowcount
        if changed != 1:
            raise KeyError(rule_id)
        row = self.connection.execute(
            "SELECT reuse_count FROM research_rule_registry WHERE rule_id=?",
            (rule_id,),
        ).fetchone()
        return int(row["reuse_count"])

    def start_learning_epoch(self, epoch: LearningEpoch) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO research_learning_epochs(
                    epoch_id,policy_version,schema_version,started_at,state,
                    checkpoint_json,notes
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    epoch.epoch_id, epoch.policy_version, epoch.schema_version,
                    self.clock(), epoch.state, _dump(dict(epoch.checkpoint)), epoch.notes,
                ),
            )

    # negative knowledge --------------------------------------------------------
    def record_negative(self, item: NegativeKnowledge) -> bool:
        if not item.exhaustive:
            raise ValueError("incomplete pagination cannot become negative knowledge")
        now = self.clock()
        with self.connection:
            changed = self.connection.execute(
                """
                INSERT INTO research_negative_knowledge(
                    negative_id,root_id,scope_kind,scope_key,reason,exhaustive,
                    context_hash,first_seen_at,last_seen_at,observations
                ) VALUES(?,?,?,?,?,1,?,?,?,1)
                ON CONFLICT(root_id,scope_kind,scope_key,reason,context_hash)
                DO UPDATE SET
                    last_seen_at=excluded.last_seen_at,
                    observations=research_negative_knowledge.observations+1
                """,
                (
                    item.negative_id, item.root_id, item.scope_kind,
                    item.scope_key, item.reason, item.context_hash, now, now,
                ),
            ).rowcount
        return changed >= 1

    # reward closure ------------------------------------------------------------
    def record_reward(self, reward: RewardRecord) -> bool:
        if reward.kind is RewardKind.FINAL and not reward.validation_closed:
            raise ValueError("FINAL reward requires validation closure")
        if reward.scope is RewardScope.DECISION:
            self.get_decision(reward.decision_id)
        with self.connection:
            changed = self.connection.execute(
                """
                INSERT OR IGNORE INTO research_rewards(
                    reward_id,reward_kind,reward_scope,entity_id,source_key,
                    exposure_id,decision_id,amount,validation_closed,
                    idempotency_key,policy_version,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    reward.reward_id, reward.kind.value, reward.scope.value,
                    reward.entity_id, reward.source_key, reward.exposure_id,
                    reward.decision_id, reward.amount,
                    int(reward.validation_closed), reward.idempotency_key,
                    reward.policy_version, self.clock(),
                ),
            ).rowcount
        return changed == 1

    def close_final_reward(
        self,
        *,
        source_key: str,
        exposure_id: str,
        final_eed: float,
        validation_closed: bool,
        policy_version: str = "",
        idempotency_token: str = "",
    ) -> int:
        if not validation_closed:
            raise ValueError("FINAL reward can be attributed only after validation closes")
        if final_eed < 0:
            raise ValueError("final_eed must be non-negative")
        token = idempotency_token or stable_hash("final-close", source_key, exposure_id)
        rows = self.artifact_lineage_for_exposure(
            source_key=source_key,
            exposure_id=exposure_id,
        )

        # Entity-scope rows preserve causal lineage for diagnostics.  They are
        # not policy observations and therefore do not drive ArmStats.
        entities: set[tuple[RewardScope, str, str]] = {
            (RewardScope.SOURCE, source_key, "")
        }
        leaf_decision_ids: set[str] = set()
        direct_source_decision = self.source_decision_id(source_key)
        if direct_source_decision:
            leaf_decision_ids.add(direct_source_decision)
        for row in rows:
            leaf_decision_id = str(row["decision_id"] or "")
            if leaf_decision_id:
                leaf_decision_ids.add(leaf_decision_id)
            for scope, column in (
                (RewardScope.ARTIFACT, "artifact_id"),
                (RewardScope.QUERY, "query_id"),
                (RewardScope.PROGRAM, "program_id"),
                (RewardScope.ROOT, "root_id"),
                (RewardScope.PIVOT, "pivot_id"),
            ):
                entity = str(row[column] or "")
                if entity:
                    entities.add((scope, entity, leaf_decision_id))

        # A validated environment outcome is credited exactly once to every
        # hierarchical decision in the leaf ancestry.  DECISION rows are the
        # only FINAL rows consumed by rebuild_arm_stats().
        decision_records: dict[str, DecisionRecord] = {}
        for leaf_decision_id in sorted(leaf_decision_ids):
            for decision in self.get_decision_lineage(
                leaf_decision_id, strict=True
            ):
                decision_records[decision.decision_id] = decision
        for decision_id in decision_records:
            entities.add((RewardScope.DECISION, decision_id, decision_id))

        written = 0
        for scope, entity_id, decision_id in sorted(
            entities, key=lambda item: (item[0].value, item[1], item[2])
        ):
            key = stable_hash("final-attribution", token, scope.value, entity_id)
            decision = decision_records.get(decision_id)
            reward_policy_version = (
                decision.policy_version
                if scope is RewardScope.DECISION and decision is not None
                else policy_version
            )
            reward = RewardRecord(
                reward_id=stable_hash("reward", key),
                kind=RewardKind.FINAL,
                scope=scope,
                entity_id=entity_id,
                amount=final_eed,
                source_key=source_key,
                exposure_id=exposure_id,
                decision_id=decision_id,
                validation_closed=True,
                policy_version=reward_policy_version,
                idempotency_key=key,
            )
            written += int(self.record_reward(reward))
        return written

    def rewards(
        self,
        *,
        kind: RewardKind | str | None = None,
        policy_version: str = "",
    ) -> tuple[RewardRecord, ...]:
        sql = "SELECT * FROM research_rewards WHERE 1=1"
        params: list[Any] = []
        if kind is not None:
            sql += " AND reward_kind=?"
            params.append(RewardKind(kind).value)
        if policy_version:
            sql += " AND policy_version=?"
            params.append(policy_version)
        sql += " ORDER BY created_at,reward_id"
        rows = self.connection.execute(sql, params).fetchall()
        return tuple(
            RewardRecord(
                reward_id=row["reward_id"], kind=row["reward_kind"],
                scope=row["reward_scope"], entity_id=row["entity_id"],
                amount=float(row["amount"]), source_key=row["source_key"],
                exposure_id=row["exposure_id"], decision_id=row["decision_id"],
                validation_closed=bool(row["validation_closed"]),
                policy_version=row["policy_version"],
                idempotency_key=row["idempotency_key"],
            )
            for row in rows
        )

    def rebuild_arm_stats(
        self, *, policy_version: str, schema_version: int
    ) -> tuple[ArmStats, ...]:
        now = self.clock()
        rows = self.connection.execute(
            """
            SELECT d.arm_id,
                   COUNT(DISTINCT d.decision_id) AS pulls,
                   COALESCE(SUM(
                       CASE WHEN r.reward_kind='PROXY' THEN r.amount ELSE 0 END
                   ),0) AS proxy_reward,
                   COALESCE(SUM(
                       CASE
                           WHEN r.reward_kind='FINAL'
                            AND r.reward_scope='DECISION'
                           THEN r.amount ELSE 0
                       END
                   ),0) AS final_reward,
                   COALESCE(SUM(
                       CASE
                           WHEN r.reward_kind='FINAL'
                            AND r.reward_scope='DECISION'
                           THEN 1 ELSE 0
                       END
                   ),0) AS final_observation_count
            FROM research_decisions d
            LEFT JOIN research_rewards r ON r.decision_id=d.decision_id
            WHERE d.policy_version=?
            GROUP BY d.arm_id
            ORDER BY d.arm_id
            """,
            (policy_version,),
        ).fetchall()
        with self.connection:
            self.connection.execute(
                "DELETE FROM research_arm_stats WHERE policy_version=?",
                (policy_version,),
            )
            for row in rows:
                proxy = float(row["proxy_reward"])
                final = float(row["final_reward"])
                final_count = int(row["final_observation_count"])
                self.connection.execute(
                    """
                    INSERT INTO research_arm_stats(
                        arm_id,policy_version,pulls,proxy_reward,final_reward,
                        final_observation_count,decayed_reward,
                        schema_version,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row["arm_id"], policy_version, int(row["pulls"]), proxy,
                        final, final_count,
                        final if final_count > 0 else proxy,
                        schema_version, now,
                    ),
                )
        return self.arm_stats(policy_version=policy_version)

    def arm_stats(self, *, policy_version: str) -> tuple[ArmStats, ...]:
        rows = self.connection.execute(
            """
            SELECT * FROM research_arm_stats
            WHERE policy_version=? ORDER BY arm_id
            """,
            (policy_version,),
        ).fetchall()
        return tuple(
            ArmStats(
                arm_id=row["arm_id"], policy_version=row["policy_version"],
                pulls=int(row["pulls"]),
                proxy_reward=float(row["proxy_reward"]),
                final_reward=float(row["final_reward"]),
                decayed_reward=float(row["decayed_reward"]),
                schema_version=int(row["schema_version"]),
                updated_at=float(row["updated_at"]),
                final_observation_count=int(row["final_observation_count"]),
            )
            for row in rows
        )


__all__ = ["ResearchRegistry"]
