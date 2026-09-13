"""Durable research graph/frontier/reward registry on the shared ControlStore.

The registry owns only additive source_research schema.  It deliberately reuses
ControlStore.connection, preserving the existing SQLite-WAL authority.
"""
from __future__ import annotations

import json
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
        retry_at: float | None = None,
        last_error: str = "",
    ) -> None:
        state = QueryState(state)
        if pages_delta < 0:
            raise ValueError("pages_delta must be non-negative")
        now = self.clock()
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE research_queries SET
                    checkpoint_json=?, state=?,
                    pages_completed=pages_completed+?,
                    attempts=attempts+CASE WHEN ? IN ('RUNNING','RETRYABLE','FAILED') THEN 1 ELSE 0 END,
                    retry_at=?, last_error=?, updated_at=?
                WHERE query_id=?
                """,
                (
                    _dump(checkpoint.as_dict() if checkpoint else {}),
                    state.value, pages_delta, state.value, retry_at,
                    last_error, now, query_id,
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
            changed = self.connection.execute(
                """
                UPDATE research_artifact_lineage
                SET source_key=?, source_exposure_id=?
                WHERE artifact_id=? AND source_key=''
                """,
                (source_key, source_exposure_id, artifact_id),
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

    def has_decisions(self, *, policy_version: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM research_decisions WHERE policy_version=? LIMIT 1",
            (policy_version,),
        ).fetchone()
        return row is not None

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
        rows = self.artifact_lineage(source_key=source_key, exposure_id=exposure_id)

        entities: set[tuple[RewardScope, str, str]] = {
            (RewardScope.SOURCE, source_key, "")
        }
        for row in rows:
            for scope, column in (
                (RewardScope.ARTIFACT, "artifact_id"),
                (RewardScope.QUERY, "query_id"),
                (RewardScope.PROGRAM, "program_id"),
                (RewardScope.ROOT, "root_id"),
                (RewardScope.PIVOT, "pivot_id"),
            ):
                entity = str(row[column] or "")
                if entity:
                    entities.add((scope, entity, str(row["decision_id"] or "")))

        written = 0
        for scope, entity_id, decision_id in sorted(
            entities, key=lambda item: (item[0].value, item[1], item[2])
        ):
            key = stable_hash("final-attribution", token, scope.value, entity_id)
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
                policy_version=policy_version,
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
                   COALESCE(SUM(CASE WHEN r.reward_kind='PROXY' THEN r.amount ELSE 0 END),0)
                       AS proxy_reward,
                   COALESCE(SUM(CASE WHEN r.reward_kind='FINAL' THEN r.amount ELSE 0 END),0)
                       AS final_reward
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
                self.connection.execute(
                    """
                    INSERT INTO research_arm_stats(
                        arm_id,policy_version,pulls,proxy_reward,final_reward,
                        decayed_reward,schema_version,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        row["arm_id"], policy_version, int(row["pulls"]), proxy,
                        final, final if final else proxy, schema_version, now,
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
            )
            for row in rows
        )


__all__ = ["ResearchRegistry"]
