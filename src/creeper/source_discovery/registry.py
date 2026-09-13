"""SQLite-backed registry for source discovery, lineage, and search attribution."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass

from creeper.scheduler.leases import StateTransitionError
from creeper.source_discovery.models import (
    MeasurementMode,
    ScoutMeasurement,
    SearchEpisode,
    SourceCandidate,
    SourceLevel,
    SourceState,
    StrategyReward,
    SuppressionScope,
    is_common_crawl_provenance,
)
from creeper.storage.control_store import ControlStore


_TRANSITIONS: dict[SourceState, frozenset[SourceState]] = {
    SourceState.DISCOVERED: frozenset(
        {SourceState.TRIAGED, SourceState.HOLD, SourceState.REJECTED}
    ),
    SourceState.TRIAGED: frozenset(
        {SourceState.SCOUT_READY, SourceState.HOLD, SourceState.REJECTED}
    ),
    SourceState.SCOUT_READY: frozenset(
        {SourceState.SCOUTING, SourceState.HOLD, SourceState.REJECTED}
    ),
    SourceState.SCOUTING: frozenset(
        {SourceState.WARM, SourceState.HOLD, SourceState.REJECTED}
    ),
    SourceState.WARM: frozenset(
        {SourceState.ACTIVE, SourceState.HOLD, SourceState.REJECTED}
    ),
    SourceState.ACTIVE: frozenset(
        {SourceState.WARM, SourceState.HOLD, SourceState.EXHAUSTED}
    ),
    SourceState.HOLD: frozenset(
        {SourceState.TRIAGED, SourceState.SCOUT_READY, SourceState.REJECTED}
    ),
    SourceState.REJECTED: frozenset(),
    SourceState.EXHAUSTED: frozenset(),
}


@dataclass(frozen=True)
class SourceRunOutcome:
    source_key: str
    reservoir_id: str
    lease_id: str
    baseline_signature: str
    model_signature: str
    read_started: float | None
    read_finished: float | None
    source_records: int
    bytes_read: int
    source_requests: int
    evidence_tasks_created: int
    evidence_tasks_terminal: int
    direct_capsules_committed: int
    provider_requests: int
    provider_elapsed_seconds: float
    candidate_duplicates: int
    baseline_duplicates: int
    accepted_host_years: int
    final_accepted_eed: float
    read_complete: bool
    validation_complete: bool
    closed: bool
    closed_at: float | None
    max_evidence_sequence: int
    created_at: float
    updated_at: float

    @property
    def resource_cost_seconds(self) -> float:
        read_elapsed = 0.0
        if self.read_started is not None and self.read_finished is not None:
            read_elapsed = max(0.0, self.read_finished - self.read_started)
        return read_elapsed + max(0.0, self.provider_elapsed_seconds)



class SourceDiscoveryRegistry:
    """Durable control-plane registry shared by search, scout, and scheduler.

    The registry intentionally uses the existing ControlStore connection so
    source identity, graph lineage, and later Reservoir promotion can live under
    the same SQLite-WAL authority. URL crawling itself is not implemented here;
    a mature crawler such as Scrapy owns request scheduling and JOBDIR state.
    """

    def __init__(
        self,
        control_store: ControlStore,
        *,
        max_graph_hops: int = 4,
        clock=time.time,
    ) -> None:
        if max_graph_hops < 1:
            raise ValueError("max_graph_hops must be positive")
        self.control_store = control_store
        self.connection = control_store.connection
        self.max_graph_hops = max_graph_hops
        self.clock = clock
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS source_candidates (
                source_key TEXT PRIMARY KEY,
                canonical_entrypoint TEXT NOT NULL UNIQUE,
                source_family TEXT NOT NULL,
                source_level TEXT NOT NULL,
                discovered_by TEXT NOT NULL,
                discovery_strategy TEXT NOT NULL,
                expected_year_from INTEGER,
                expected_year_to INTEGER,
                expected_volume INTEGER,
                temporal_semantics_prior REAL NOT NULL,
                enumerability_prior REAL NOT NULL,
                direct_evidence_prior REAL NOT NULL,
                baseline_overlap_prior REAL NOT NULL,
                access_cost_prior REAL NOT NULL,
                adapter_cost_prior REAL NOT NULL,
                confidence REAL NOT NULL,
                state TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_source_candidates_state
                ON source_candidates(state, source_level, confidence);

            CREATE TABLE IF NOT EXISTS source_search_episodes (
                episode_id TEXT PRIMARY KEY,
                strategy TEXT NOT NULL,
                backend TEXT NOT NULL,
                query TEXT NOT NULL,
                actor TEXT NOT NULL,
                started_at REAL NOT NULL,
                finished_at REAL,
                search_cost_seconds REAL NOT NULL DEFAULT 0,
                accepted_novel_eed REAL NOT NULL DEFAULT 0
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_source_search_strategy
                ON source_search_episodes(strategy, finished_at);

            CREATE TABLE IF NOT EXISTS source_llm_episodes (
                episode_id TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                backend TEXT NOT NULL,
                actor TEXT NOT NULL,
                context_hash TEXT NOT NULL,
                prompt_version TEXT NOT NULL,
                started_at REAL NOT NULL,
                finished_at REAL,
                cost_seconds REAL NOT NULL DEFAULT 0
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS source_llm_hypotheses (
                hypothesis_id TEXT PRIMARY KEY,
                episode_id TEXT NOT NULL,
                action TEXT NOT NULL,
                confidence REAL NOT NULL,
                raw_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                FOREIGN KEY(episode_id) REFERENCES source_llm_episodes(episode_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_source_llm_hypothesis_episode
                ON source_llm_hypotheses(episode_id, created_at);

            CREATE TABLE IF NOT EXISTS source_llm_source_attribution (
                source_key TEXT NOT NULL,
                hypothesis_id TEXT NOT NULL,
                credited_eed REAL NOT NULL DEFAULT 0,
                PRIMARY KEY(source_key, hypothesis_id),
                FOREIGN KEY(source_key) REFERENCES source_candidates(source_key),
                FOREIGN KEY(hypothesis_id) REFERENCES source_llm_hypotheses(hypothesis_id)
            ) WITHOUT ROWID;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_source_llm_single_credit
                ON source_llm_source_attribution(source_key);

            CREATE TABLE IF NOT EXISTS source_final_rewards (
                source_key TEXT PRIMARY KEY,
                final_accepted_eed REAL NOT NULL,
                cost_seconds REAL NOT NULL DEFAULT 0,
                baseline_signature TEXT,
                model_signature TEXT,
                closed_runs INTEGER NOT NULL DEFAULT 0,
                zero_runs INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                FOREIGN KEY(source_key) REFERENCES source_candidates(source_key)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS source_run_outcomes (
                source_key TEXT NOT NULL,
                reservoir_id TEXT NOT NULL,
                lease_id TEXT NOT NULL,
                baseline_signature TEXT NOT NULL,
                model_signature TEXT NOT NULL,
                read_started REAL,
                read_finished REAL,
                source_records INTEGER NOT NULL DEFAULT 0 CHECK(source_records >= 0),
                bytes_read INTEGER NOT NULL DEFAULT 0 CHECK(bytes_read >= 0),
                source_requests INTEGER NOT NULL DEFAULT 0 CHECK(source_requests >= 0),
                evidence_tasks_created INTEGER NOT NULL DEFAULT 0 CHECK(evidence_tasks_created >= 0),
                evidence_tasks_terminal INTEGER NOT NULL DEFAULT 0 CHECK(evidence_tasks_terminal >= 0),
                direct_capsules_committed INTEGER NOT NULL DEFAULT 0 CHECK(direct_capsules_committed >= 0),
                provider_requests INTEGER NOT NULL DEFAULT 0 CHECK(provider_requests >= 0),
                provider_elapsed_seconds REAL NOT NULL DEFAULT 0 CHECK(provider_elapsed_seconds >= 0),
                candidate_duplicates INTEGER NOT NULL DEFAULT 0 CHECK(candidate_duplicates >= 0),
                baseline_duplicates INTEGER NOT NULL DEFAULT 0 CHECK(baseline_duplicates >= 0),
                accepted_host_years INTEGER NOT NULL DEFAULT 0 CHECK(accepted_host_years >= 0),
                final_accepted_eed REAL NOT NULL DEFAULT 0 CHECK(final_accepted_eed >= 0),
                read_complete INTEGER NOT NULL DEFAULT 0 CHECK(read_complete IN (0,1)),
                validation_complete INTEGER NOT NULL DEFAULT 0 CHECK(validation_complete IN (0,1)),
                closed INTEGER NOT NULL DEFAULT 0 CHECK(closed IN (0,1)),
                closed_at REAL,
                max_evidence_sequence INTEGER NOT NULL DEFAULT 0 CHECK(max_evidence_sequence >= 0),
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(
                    source_key, reservoir_id, lease_id,
                    baseline_signature, model_signature
                ),
                FOREIGN KEY(source_key) REFERENCES source_candidates(source_key)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_source_run_outcomes_source_authority
                ON source_run_outcomes(
                    source_key, baseline_signature, model_signature,
                    closed, closed_at
                );
            CREATE INDEX IF NOT EXISTS idx_source_run_outcomes_lease
                ON source_run_outcomes(lease_id, closed);

            CREATE TABLE IF NOT EXISTS source_overlap_sketches (
                source_key TEXT PRIMARY KEY,
                width INTEGER NOT NULL,
                sketch_json TEXT NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY(source_key) REFERENCES source_candidates(source_key)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS source_proposals (
                proposal_id TEXT PRIMARY KEY,
                episode_id TEXT,
                source_key TEXT NOT NULL,
                discovered_by TEXT NOT NULL,
                discovery_strategy TEXT NOT NULL,
                source_family TEXT NOT NULL,
                source_level TEXT NOT NULL,
                confidence REAL NOT NULL,
                created_at REAL NOT NULL,
                FOREIGN KEY(episode_id) REFERENCES source_search_episodes(episode_id),
                FOREIGN KEY(source_key) REFERENCES source_candidates(source_key)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_source_proposals_source
                ON source_proposals(source_key, created_at);
            CREATE INDEX IF NOT EXISTS idx_source_proposals_episode
                ON source_proposals(episode_id, source_key);

            CREATE TABLE IF NOT EXISTS source_edges (
                parent_key TEXT NOT NULL,
                child_key TEXT NOT NULL,
                relation TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY(parent_key, child_key, relation),
                FOREIGN KEY(parent_key) REFERENCES source_candidates(source_key),
                FOREIGN KEY(child_key) REFERENCES source_candidates(source_key)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_source_edges_child
                ON source_edges(child_key, parent_key);

            CREATE TABLE IF NOT EXISTS source_triage_metrics (
                source_key TEXT PRIMARY KEY,
                status_code INTEGER,
                method TEXT,
                content_type TEXT,
                content_length INTEGER,
                range_supported INTEGER,
                observed_at REAL NOT NULL,
                FOREIGN KEY(source_key) REFERENCES source_candidates(source_key)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS source_scout_metrics (
                source_key TEXT PRIMARY KEY,
                sampled_records INTEGER NOT NULL,
                unique_hosts INTEGER NOT NULL,
                novel_hosts INTEGER NOT NULL,
                direct_host_years INTEGER NOT NULL,
                requests INTEGER NOT NULL,
                bytes_read INTEGER NOT NULL,
                elapsed_seconds REAL NOT NULL,
                novel_eed REAL NOT NULL,
                measurement_mode TEXT NOT NULL DEFAULT 'HOST_ONLY',
                observed_host_year_pairs INTEGER NOT NULL DEFAULT 0,
                novel_host_year_pairs INTEGER NOT NULL DEFAULT 0,
                novel_pair_eed REAL NOT NULL DEFAULT 0,
                singleton_observations INTEGER NOT NULL DEFAULT 0,
                doubleton_observations INTEGER NOT NULL DEFAULT 0,
                estimated_unseen_fraction REAL NOT NULL DEFAULT 0,
                baseline_signature TEXT,
                model_signature TEXT,
                measured_at REAL NOT NULL,
                FOREIGN KEY(source_key) REFERENCES source_candidates(source_key)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS source_search_reward_attribution (
                source_key TEXT PRIMARY KEY,
                episode_id TEXT NOT NULL,
                credited_eed REAL NOT NULL DEFAULT 0,
                reward_kind TEXT NOT NULL DEFAULT 'legacy',
                baseline_signature TEXT,
                model_signature TEXT,
                FOREIGN KEY(source_key) REFERENCES source_candidates(source_key),
                FOREIGN KEY(episode_id) REFERENCES source_search_episodes(episode_id)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS source_scout_authority (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                baseline_signature TEXT NOT NULL,
                model_signature TEXT NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS source_suppressions (
                scope_type TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL,
                PRIMARY KEY(scope_type, scope_key)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_source_suppressions_expiry
                ON source_suppressions(expires_at);
            """
        )
        scout_columns = {
            str(row[1])
            for row in self.connection.execute(
                "PRAGMA table_info(source_scout_metrics)"
            ).fetchall()
        }
        scout_migrations = {
            "measurement_mode": "ALTER TABLE source_scout_metrics ADD COLUMN measurement_mode TEXT NOT NULL DEFAULT 'HOST_ONLY'",
            "observed_host_year_pairs": "ALTER TABLE source_scout_metrics ADD COLUMN observed_host_year_pairs INTEGER NOT NULL DEFAULT 0",
            "novel_host_year_pairs": "ALTER TABLE source_scout_metrics ADD COLUMN novel_host_year_pairs INTEGER NOT NULL DEFAULT 0",
            "novel_pair_eed": "ALTER TABLE source_scout_metrics ADD COLUMN novel_pair_eed REAL NOT NULL DEFAULT 0",
            "singleton_observations": "ALTER TABLE source_scout_metrics ADD COLUMN singleton_observations INTEGER NOT NULL DEFAULT 0",
            "doubleton_observations": "ALTER TABLE source_scout_metrics ADD COLUMN doubleton_observations INTEGER NOT NULL DEFAULT 0",
            "estimated_unseen_fraction": "ALTER TABLE source_scout_metrics ADD COLUMN estimated_unseen_fraction REAL NOT NULL DEFAULT 0",
            "baseline_signature": "ALTER TABLE source_scout_metrics ADD COLUMN baseline_signature TEXT",
            "model_signature": "ALTER TABLE source_scout_metrics ADD COLUMN model_signature TEXT",
        }
        for name, statement in scout_migrations.items():
            if name not in scout_columns:
                self.connection.execute(statement)

        final_reward_columns = {
            str(row[1])
            for row in self.connection.execute(
                "PRAGMA table_info(source_final_rewards)"
            ).fetchall()
        }
        final_reward_migrations = {
            "baseline_signature": "ALTER TABLE source_final_rewards ADD COLUMN baseline_signature TEXT",
            "model_signature": "ALTER TABLE source_final_rewards ADD COLUMN model_signature TEXT",
            "closed_runs": "ALTER TABLE source_final_rewards ADD COLUMN closed_runs INTEGER NOT NULL DEFAULT 0",
            "zero_runs": "ALTER TABLE source_final_rewards ADD COLUMN zero_runs INTEGER NOT NULL DEFAULT 0",
        }
        for name, statement in final_reward_migrations.items():
            if name not in final_reward_columns:
                self.connection.execute(statement)

        attribution_columns = {
            str(row[1])
            for row in self.connection.execute(
                "PRAGMA table_info(source_search_reward_attribution)"
            ).fetchall()
        }
        attribution_migrations = {
            "reward_kind": "ALTER TABLE source_search_reward_attribution ADD COLUMN reward_kind TEXT NOT NULL DEFAULT 'legacy'",
            "baseline_signature": "ALTER TABLE source_search_reward_attribution ADD COLUMN baseline_signature TEXT",
            "model_signature": "ALTER TABLE source_search_reward_attribution ADD COLUMN model_signature TEXT",
        }
        for name, statement in attribution_migrations.items():
            if name not in attribution_columns:
                self.connection.execute(statement)
        self.connection.commit()

    @staticmethod
    def _candidate_from_row(row: sqlite3.Row) -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=str(row["canonical_entrypoint"]),
            source_family=str(row["source_family"]),
            level=SourceLevel(str(row["source_level"])),
            discovered_by=str(row["discovered_by"]),
            discovery_strategy=str(row["discovery_strategy"]),
            expected_year_from=row["expected_year_from"],
            expected_year_to=row["expected_year_to"],
            expected_volume=row["expected_volume"],
            temporal_semantics_prior=float(row["temporal_semantics_prior"]),
            enumerability_prior=float(row["enumerability_prior"]),
            direct_evidence_prior=float(row["direct_evidence_prior"]),
            baseline_overlap_prior=float(row["baseline_overlap_prior"]),
            access_cost_prior=float(row["access_cost_prior"]),
            adapter_cost_prior=float(row["adapter_cost_prior"]),
            confidence=float(row["confidence"]),
            state=SourceState(str(row["state"])),
        )

    def _load_scout_authority(self) -> tuple[str, str] | None:
        row = self.connection.execute(
            """
            SELECT baseline_signature, model_signature
            FROM source_scout_authority
            WHERE singleton = 1
            """
        ).fetchone()
        if row is None:
            return None
        return (
            str(row["baseline_signature"]),
            str(row["model_signature"]),
        )

    @property
    def current_scout_authority(self) -> tuple[str, str] | None:
        # Read the one-row marker so independent long-running processes observe
        # authority replacement without restarting or hashing large assets.
        return self._load_scout_authority()

    def is_current_scout_authority(
        self,
        baseline_signature: str,
        model_signature: str,
    ) -> bool:
        return self._load_scout_authority() == (
            baseline_signature,
            model_signature,
        )

    def set_scout_authority(
        self,
        *,
        baseline_signature: str,
        model_signature: str,
    ) -> bool:
        """Install current scout authority and invalidate stale proxy credit.

        Historical measurements remain durable. Only proxy reward credit is
        removed, and the potentially non-constant scan occurs strictly when the
        authority tuple actually changes.
        """
        if (
            not isinstance(baseline_signature, str)
            or not baseline_signature.strip()
            or not isinstance(model_signature, str)
            or not model_signature.strip()
        ):
            raise ValueError("scout authority signatures must be non-empty")
        authority = (baseline_signature, model_signature)
        persisted = self._load_scout_authority()
        if persisted == authority:
            return False

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            stale = self.connection.execute(
                """
                SELECT a.episode_id, SUM(a.credited_eed) AS credited_eed
                FROM source_search_reward_attribution a
                WHERE (
                    a.reward_kind = 'scout_proxy'
                    AND (
                        COALESCE(a.baseline_signature, '') != ?
                        OR COALESCE(a.model_signature, '') != ?
                    )
                ) OR (
                    a.reward_kind = 'legacy'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM source_final_rewards f
                        WHERE f.source_key = a.source_key
                    )
                )
                GROUP BY a.episode_id
                """,
                authority,
            ).fetchall()
            for row in stale:
                credit = float(row["credited_eed"] or 0.0)
                if credit <= 0:
                    continue
                self.connection.execute(
                    """
                    UPDATE source_search_episodes
                    SET accepted_novel_eed = MAX(
                        0,
                        accepted_novel_eed - ?
                    )
                    WHERE episode_id = ?
                    """,
                    (credit, str(row["episode_id"])),
                )
            self.connection.execute(
                """
                UPDATE source_search_reward_attribution
                SET credited_eed = 0
                WHERE (
                    reward_kind = 'scout_proxy'
                    AND (
                        COALESCE(baseline_signature, '') != ?
                        OR COALESCE(model_signature, '') != ?
                    )
                ) OR (
                    reward_kind = 'legacy'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM source_final_rewards f
                        WHERE f.source_key =
                            source_search_reward_attribution.source_key
                    )
                )
                """,
                authority,
            )
            self.connection.execute(
                """
                INSERT INTO source_scout_authority(
                    singleton, baseline_signature, model_signature, updated_at
                ) VALUES (1, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    baseline_signature = excluded.baseline_signature,
                    model_signature = excluded.model_signature,
                    updated_at = excluded.updated_at
                """,
                (
                    baseline_signature,
                    model_signature,
                    float(self.clock()),
                ),
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return True

    def _measurement_from_row(self, row: sqlite3.Row) -> ScoutMeasurement:
        return ScoutMeasurement(
            sampled_records=int(row["sampled_records"]),
            unique_hosts=int(row["unique_hosts"]),
            novel_hosts=int(row["novel_hosts"]),
            direct_host_years=int(row["direct_host_years"]),
            requests=int(row["requests"]),
            bytes_read=int(row["bytes_read"]),
            elapsed_seconds=float(row["elapsed_seconds"]),
            novel_eed=float(row["novel_eed"]),
            measurement_mode=MeasurementMode(str(row["measurement_mode"])),
            observed_host_year_pairs=int(row["observed_host_year_pairs"]),
            novel_host_year_pairs=int(row["novel_host_year_pairs"]),
            novel_pair_eed=float(row["novel_pair_eed"]),
            singleton_observations=int(row["singleton_observations"]),
            doubleton_observations=int(row["doubleton_observations"]),
            estimated_unseen_fraction=float(row["estimated_unseen_fraction"]),
            minhash_values=self.get_overlap_sketch(str(row["source_key"])) or (),
        )

    def begin_search_episode(
        self,
        *,
        strategy: str,
        backend: str,
        query: str,
        actor: str,
        episode_id: str | None = None,
    ) -> SearchEpisode:
        if not all(isinstance(item, str) and item.strip() for item in (strategy, backend, query, actor)):
            raise ValueError("search episode fields must be non-empty strings")
        episode_id = episode_id or f"search:{uuid.uuid4().hex}"
        started_at = float(self.clock())
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_search_episodes(
                    episode_id, strategy, backend, query, actor, started_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (episode_id, strategy, backend, query, actor, started_at),
            )
        return SearchEpisode(episode_id, strategy, backend, query, actor, started_at)

    def get_search_episode(self, episode_id: str) -> SearchEpisode | None:
        row = self.connection.execute(
            "SELECT * FROM source_search_episodes WHERE episode_id = ?",
            (episode_id,),
        ).fetchone()
        if row is None:
            return None
        return SearchEpisode(
            episode_id=str(row["episode_id"]),
            strategy=str(row["strategy"]),
            backend=str(row["backend"]),
            query=str(row["query"]),
            actor=str(row["actor"]),
            started_at=float(row["started_at"]),
            finished_at=row["finished_at"],
            search_cost_seconds=float(row["search_cost_seconds"]),
            accepted_novel_eed=float(row["accepted_novel_eed"]),
        )

    def finish_search_episode(self, episode_id: str, *, search_cost_seconds: float) -> SearchEpisode:
        if search_cost_seconds < 0:
            raise ValueError("search_cost_seconds must be non-negative")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                "SELECT finished_at FROM source_search_episodes WHERE episode_id = ?",
                (episode_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown search episode: {episode_id}")
            if row["finished_at"] is not None:
                raise StateTransitionError("search episode is already finished")
            self.connection.execute(
                """
                UPDATE source_search_episodes
                SET finished_at = ?, search_cost_seconds = ?
                WHERE episode_id = ?
                """,
                (now, float(search_cost_seconds), episode_id),
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        episode = self.get_search_episode(episode_id)
        assert episode is not None
        return episode

    def credit_search_episode(self, episode_id: str, *, accepted_novel_eed: float) -> SearchEpisode:
        if accepted_novel_eed < 0:
            raise ValueError("accepted_novel_eed must be non-negative")
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE source_search_episodes
                SET accepted_novel_eed = accepted_novel_eed + ?
                WHERE episode_id = ?
                """,
                (float(accepted_novel_eed), episode_id),
            ).rowcount
        if changed != 1:
            raise KeyError(f"unknown search episode: {episode_id}")
        episode = self.get_search_episode(episode_id)
        assert episode is not None
        return episode

    def strategy_rewards(self) -> list[StrategyReward]:
        rows = self.connection.execute(
            """
            SELECT strategy,
                   COUNT(*) AS episodes,
                   SUM(accepted_novel_eed) AS accepted_novel_eed,
                   SUM(search_cost_seconds) AS search_cost_seconds
            FROM source_search_episodes
            WHERE finished_at IS NOT NULL
            GROUP BY strategy
            ORDER BY strategy
            """
        ).fetchall()
        return [
            StrategyReward(
                strategy=str(row["strategy"]),
                episodes=int(row["episodes"]),
                accepted_novel_eed=float(row["accepted_novel_eed"] or 0.0),
                search_cost_seconds=float(row["search_cost_seconds"] or 0.0),
            )
            for row in rows
        ]

    def begin_llm_episode(
        self,
        *,
        episode_id: str,
        task_type: str,
        backend: str,
        actor: str,
        context_hash: str,
        prompt_version: str,
    ) -> None:
        """Persist the parent-process decision to invoke one Codex subagent."""
        if not all(
            isinstance(value, str) and value.strip()
            for value in (
                episode_id,
                task_type,
                backend,
                actor,
                prompt_version,
            )
        ):
            raise ValueError("LLM episode identity fields are required")
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_llm_episodes(
                    episode_id, task_type, backend, actor, context_hash,
                    prompt_version, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    episode_id,
                    task_type,
                    backend,
                    actor,
                    context_hash,
                    prompt_version,
                    float(self.clock()),
                ),
            )

    def finish_llm_episode(
        self,
        episode_id: str,
        *,
        cost_seconds: float,
    ) -> None:
        if cost_seconds < 0:
            raise ValueError("LLM episode cost must be non-negative")
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE source_llm_episodes
                SET finished_at = ?, cost_seconds = ?
                WHERE episode_id = ? AND finished_at IS NULL
                """,
                (float(self.clock()), float(cost_seconds), episode_id),
            ).rowcount
        if changed != 1:
            raise KeyError(f"unknown or finished LLM episode: {episode_id}")

    def register_llm_hypothesis(
        self,
        episode_id: str,
        hypothesis: dict[str, object],
    ) -> None:
        hypothesis_id = hypothesis.get("hypothesis_id")
        action = hypothesis.get("action")
        confidence = hypothesis.get("confidence", 0.0)
        if not isinstance(hypothesis_id, str) or not hypothesis_id.strip():
            raise ValueError("LLM hypothesis_id is required")
        if not isinstance(action, str) or not action.strip():
            raise ValueError("LLM hypothesis action is required")
        if (
            isinstance(confidence, bool)
            or not isinstance(confidence, (int, float))
            or not 0 <= float(confidence) <= 1
        ):
            raise ValueError("LLM hypothesis confidence must be within [0, 1]")
        raw_json = json.dumps(
            hypothesis,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_llm_hypotheses(
                    hypothesis_id, episode_id, action, confidence,
                    raw_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    hypothesis_id,
                    episode_id,
                    action,
                    float(confidence),
                    raw_json,
                    float(self.clock()),
                ),
            )

    def link_llm_source(
        self,
        source_key: str,
        *,
        hypothesis_id: str,
    ) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO source_llm_source_attribution(
                    source_key, hypothesis_id, credited_eed
                ) VALUES (?, ?, 0)
                """,
                (source_key, hypothesis_id),
            )

    @staticmethod
    def _source_run_from_row(row: sqlite3.Row) -> SourceRunOutcome:
        return SourceRunOutcome(
            source_key=str(row["source_key"]),
            reservoir_id=str(row["reservoir_id"]),
            lease_id=str(row["lease_id"]),
            baseline_signature=str(row["baseline_signature"]),
            model_signature=str(row["model_signature"]),
            read_started=(
                None if row["read_started"] is None else float(row["read_started"])
            ),
            read_finished=(
                None if row["read_finished"] is None else float(row["read_finished"])
            ),
            source_records=int(row["source_records"]),
            bytes_read=int(row["bytes_read"]),
            source_requests=int(row["source_requests"]),
            evidence_tasks_created=int(row["evidence_tasks_created"]),
            evidence_tasks_terminal=int(row["evidence_tasks_terminal"]),
            direct_capsules_committed=int(row["direct_capsules_committed"]),
            provider_requests=int(row["provider_requests"]),
            provider_elapsed_seconds=float(row["provider_elapsed_seconds"]),
            candidate_duplicates=int(row["candidate_duplicates"]),
            baseline_duplicates=int(row["baseline_duplicates"]),
            accepted_host_years=int(row["accepted_host_years"]),
            final_accepted_eed=float(row["final_accepted_eed"]),
            read_complete=bool(row["read_complete"]),
            validation_complete=bool(row["validation_complete"]),
            closed=bool(row["closed"]),
            closed_at=None if row["closed_at"] is None else float(row["closed_at"]),
            max_evidence_sequence=int(row["max_evidence_sequence"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )

    def begin_source_run(
        self,
        source_key: str,
        *,
        reservoir_id: str,
        lease_id: str,
        baseline_signature: str,
        model_signature: str,
        read_started: float | None = None,
    ) -> SourceRunOutcome:
        """Register one production lease under an immutable authority identity.

        Registration is explicit so upgrading an existing runtime cannot
        retroactively turn historical leases with incomplete telemetry into
        zero-reward training examples.
        """
        if self.get_candidate(source_key) is None:
            raise KeyError(f"unknown source: {source_key}")
        values = (reservoir_id, lease_id, baseline_signature, model_signature)
        if any(not isinstance(value, str) or not value.strip() for value in values):
            raise ValueError("source-run identity and authority are required")
        now = float(self.clock())
        started = now if read_started is None else float(read_started)
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO source_run_outcomes(
                    source_key, reservoir_id, lease_id,
                    baseline_signature, model_signature,
                    read_started, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_key,
                    reservoir_id,
                    lease_id,
                    baseline_signature,
                    model_signature,
                    started,
                    now,
                    now,
                ),
            )
        row = self.get_source_run_outcome(
            source_key,
            reservoir_id=reservoir_id,
            lease_id=lease_id,
            baseline_signature=baseline_signature,
            model_signature=model_signature,
        )
        assert row is not None
        return row

    def get_source_run_outcome(
        self,
        source_key: str,
        *,
        reservoir_id: str,
        lease_id: str,
        baseline_signature: str,
        model_signature: str,
    ) -> SourceRunOutcome | None:
        row = self.connection.execute(
            """
            SELECT *
            FROM source_run_outcomes
            WHERE source_key = ?
              AND reservoir_id = ?
              AND lease_id = ?
              AND baseline_signature = ?
              AND model_signature = ?
            """,
            (
                source_key,
                reservoir_id,
                lease_id,
                baseline_signature,
                model_signature,
            ),
        ).fetchone()
        return None if row is None else self._source_run_from_row(row)

    def list_source_run_outcomes(
        self,
        source_key: str | None = None,
        *,
        baseline_signature: str | None = None,
        model_signature: str | None = None,
        closed_only: bool = False,
    ) -> list[SourceRunOutcome]:
        if (baseline_signature is None) != (model_signature is None):
            raise ValueError(
                "baseline_signature and model_signature must be provided together"
            )
        clauses: list[str] = []
        params: list[object] = []
        if source_key is not None:
            clauses.append("source_key = ?")
            params.append(source_key)
        if baseline_signature is not None:
            clauses.extend(
                ["baseline_signature = ?", "model_signature = ?"]
            )
            params.extend((baseline_signature, model_signature))
        if closed_only:
            clauses.append("closed = 1")
        where = "" if not clauses else "WHERE " + " AND ".join(clauses)
        rows = self.connection.execute(
            f"""
            SELECT *
            FROM source_run_outcomes
            {where}
            ORDER BY COALESCE(closed_at, updated_at), source_key, lease_id
            """,
            params,
        ).fetchall()
        return [self._source_run_from_row(row) for row in rows]

    def record_source_run_read(
        self,
        source_key: str,
        *,
        reservoir_id: str,
        lease_id: str,
        baseline_signature: str,
        model_signature: str,
        source_records: int,
        bytes_read: int,
        source_requests: int,
        candidate_duplicates: int = 0,
        baseline_duplicates: int = 0,
        read_finished: float | None = None,
        read_complete: bool = True,
    ) -> SourceRunOutcome:
        metrics = (
            source_records,
            bytes_read,
            source_requests,
            candidate_duplicates,
            baseline_duplicates,
        )
        if any(int(value) < 0 for value in metrics):
            raise ValueError("source-run read counters must be non-negative")
        now = float(self.clock())
        finished = (
            None
            if not read_complete and read_finished is None
            else now if read_finished is None else float(read_finished)
        )
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            changed = self.connection.execute(
                """
                UPDATE source_run_outcomes
                SET source_records = ?,
                    bytes_read = ?,
                    source_requests = ?,
                    candidate_duplicates = ?,
                    baseline_duplicates = ?,
                    read_finished = ?,
                    read_complete = ?,
                    updated_at = ?
                WHERE source_key = ?
                  AND reservoir_id = ?
                  AND lease_id = ?
                  AND baseline_signature = ?
                  AND model_signature = ?
                  AND closed = 0
                """,
                (
                    int(source_records),
                    int(bytes_read),
                    int(source_requests),
                    int(candidate_duplicates),
                    int(baseline_duplicates),
                    finished,
                    int(bool(read_complete)),
                    now,
                    source_key,
                    reservoir_id,
                    lease_id,
                    baseline_signature,
                    model_signature,
                ),
            ).rowcount
            if changed != 1:
                raise KeyError("unknown or closed source run")
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        row = self.get_source_run_outcome(
            source_key,
            reservoir_id=reservoir_id,
            lease_id=lease_id,
            baseline_signature=baseline_signature,
            model_signature=model_signature,
        )
        assert row is not None
        return row

    def record_source_run_validation(
        self,
        source_key: str,
        *,
        reservoir_id: str,
        lease_id: str,
        baseline_signature: str,
        model_signature: str,
        evidence_tasks_created: int,
        evidence_tasks_terminal: int,
        direct_capsules_committed: int,
        provider_requests: int,
        provider_elapsed_seconds: float,
        accepted_host_years: int,
        final_accepted_eed: float,
        max_evidence_sequence: int,
        validation_complete: bool,
    ) -> SourceRunOutcome:
        integer_metrics = (
            evidence_tasks_created,
            evidence_tasks_terminal,
            direct_capsules_committed,
            provider_requests,
            accepted_host_years,
            max_evidence_sequence,
        )
        if any(int(value) < 0 for value in integer_metrics):
            raise ValueError("source-run validation counters must be non-negative")
        if int(evidence_tasks_terminal) > int(evidence_tasks_created):
            raise ValueError("terminal evidence tasks cannot exceed created tasks")
        if provider_elapsed_seconds < 0 or final_accepted_eed < 0:
            raise ValueError("source-run final reward and cost must be non-negative")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            changed = self.connection.execute(
                """
                UPDATE source_run_outcomes
                SET evidence_tasks_created = ?,
                    evidence_tasks_terminal = ?,
                    direct_capsules_committed = ?,
                    provider_requests = ?,
                    provider_elapsed_seconds = ?,
                    accepted_host_years = ?,
                    final_accepted_eed = ?,
                    max_evidence_sequence = ?,
                    validation_complete = ?,
                    updated_at = ?
                WHERE source_key = ?
                  AND reservoir_id = ?
                  AND lease_id = ?
                  AND baseline_signature = ?
                  AND model_signature = ?
                  AND closed = 0
                """,
                (
                    int(evidence_tasks_created),
                    int(evidence_tasks_terminal),
                    int(direct_capsules_committed),
                    int(provider_requests),
                    float(provider_elapsed_seconds),
                    int(accepted_host_years),
                    float(final_accepted_eed),
                    int(max_evidence_sequence),
                    int(bool(validation_complete)),
                    now,
                    source_key,
                    reservoir_id,
                    lease_id,
                    baseline_signature,
                    model_signature,
                ),
            ).rowcount
            if changed != 1:
                raise KeyError("unknown or closed source run")
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        row = self.get_source_run_outcome(
            source_key,
            reservoir_id=reservoir_id,
            lease_id=lease_id,
            baseline_signature=baseline_signature,
            model_signature=model_signature,
        )
        assert row is not None
        return row

    def _publish_source_aggregate_locked(
        self,
        source_key: str,
        *,
        baseline_signature: str,
        model_signature: str,
    ) -> None:
        aggregate = self.connection.execute(
            """
            SELECT
                COUNT(*) AS closed_runs,
                SUM(CASE WHEN final_accepted_eed = 0 THEN 1 ELSE 0 END) AS zero_runs,
                SUM(final_accepted_eed) AS final_accepted_eed,
                SUM(
                    provider_elapsed_seconds
                    + CASE
                        WHEN read_started IS NOT NULL
                         AND read_finished IS NOT NULL
                        THEN MAX(0, read_finished - read_started)
                        ELSE 0
                      END
                ) AS cost_seconds
            FROM source_run_outcomes
            WHERE source_key = ?
              AND baseline_signature = ?
              AND model_signature = ?
              AND closed = 1
            """,
            (source_key, baseline_signature, model_signature),
        ).fetchone()
        closed_runs = int(aggregate["closed_runs"] or 0)
        if closed_runs < 1:
            return
        final_accepted_eed = float(aggregate["final_accepted_eed"] or 0.0)
        cost_seconds = float(aggregate["cost_seconds"] or 0.0)
        zero_runs = int(aggregate["zero_runs"] or 0)
        self.connection.execute(
            """
            INSERT INTO source_final_rewards(
                source_key, final_accepted_eed, cost_seconds,
                baseline_signature, model_signature,
                closed_runs, zero_runs, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_key) DO UPDATE SET
                final_accepted_eed = excluded.final_accepted_eed,
                cost_seconds = excluded.cost_seconds,
                baseline_signature = excluded.baseline_signature,
                model_signature = excluded.model_signature,
                closed_runs = excluded.closed_runs,
                zero_runs = excluded.zero_runs,
                updated_at = excluded.updated_at
            """,
            (
                source_key,
                final_accepted_eed,
                cost_seconds,
                baseline_signature,
                model_signature,
                closed_runs,
                zero_runs,
                float(self.clock()),
            ),
        )
        self._attribute_search_reward_locked(
            source_key,
            accepted_novel_eed=final_accepted_eed,
            reward_kind="final",
            baseline_signature=baseline_signature,
            model_signature=model_signature,
        )
        self.connection.execute(
            """
            UPDATE source_llm_source_attribution
            SET credited_eed = ?
            WHERE source_key = ?
            """,
            (final_accepted_eed, source_key),
        )

    def close_source_run(
        self,
        source_key: str,
        *,
        reservoir_id: str,
        lease_id: str,
        baseline_signature: str,
        model_signature: str,
    ) -> bool:
        """Publish a FINAL run only after read, evidence and readiness closure."""
        current = self.current_scout_authority
        authority = (baseline_signature, model_signature)
        if current is not None and current != authority:
            return False

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """
                SELECT *
                FROM source_run_outcomes
                WHERE source_key = ?
                  AND reservoir_id = ?
                  AND lease_id = ?
                  AND baseline_signature = ?
                  AND model_signature = ?
                """,
                (
                    source_key,
                    reservoir_id,
                    lease_id,
                    baseline_signature,
                    model_signature,
                ),
            ).fetchone()
            if row is None:
                raise KeyError("unknown source run")
            if bool(row["closed"]):
                self.connection.commit()
                return False
            if not bool(row["read_complete"]):
                self.connection.commit()
                return False
            if not bool(row["validation_complete"]):
                self.connection.commit()
                return False
            if int(row["evidence_tasks_terminal"]) != int(
                row["evidence_tasks_created"]
            ):
                self.connection.commit()
                return False

            now = float(self.clock())
            changed = self.connection.execute(
                """
                UPDATE source_run_outcomes
                SET closed = 1, closed_at = ?, updated_at = ?
                WHERE source_key = ?
                  AND reservoir_id = ?
                  AND lease_id = ?
                  AND baseline_signature = ?
                  AND model_signature = ?
                  AND closed = 0
                """,
                (
                    now,
                    now,
                    source_key,
                    reservoir_id,
                    lease_id,
                    baseline_signature,
                    model_signature,
                ),
            ).rowcount
            if changed != 1:
                self.connection.rollback()
                return False
            self._publish_source_aggregate_locked(
                source_key,
                baseline_signature=baseline_signature,
                model_signature=model_signature,
            )
            self.connection.commit()
            return True
        except BaseException:
            self.connection.rollback()
            raise

    def record_final_reward(
        self,
        source_key: str,
        *,
        final_accepted_eed: float,
        cost_seconds: float = 0.0,
        baseline_signature: str | None = None,
        model_signature: str | None = None,
    ) -> None:
        """Publish a source-level FINAL projection.

        V5 source-run accounting should normally call close_source_run().
        This compatibility API remains for existing callers and tests. When a
        current authority exists it is attached to the projection so stale
        values cannot train scheduling after an authority cutover.
        """
        if final_accepted_eed < 0 or cost_seconds < 0:
            raise ValueError("final reward and cost must be non-negative")
        if self.get_candidate(source_key) is None:
            raise KeyError(f"unknown source: {source_key}")
        if (baseline_signature is None) != (model_signature is None):
            raise ValueError(
                "baseline_signature and model_signature must be provided together"
            )
        if baseline_signature is None:
            authority = self.current_scout_authority
            if authority is not None:
                baseline_signature, model_signature = authority
        now = float(self.clock())
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_final_rewards(
                    source_key, final_accepted_eed, cost_seconds,
                    baseline_signature, model_signature,
                    closed_runs, zero_runs, updated_at
                ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    final_accepted_eed = excluded.final_accepted_eed,
                    cost_seconds = excluded.cost_seconds,
                    baseline_signature = excluded.baseline_signature,
                    model_signature = excluded.model_signature,
                    closed_runs = excluded.closed_runs,
                    zero_runs = excluded.zero_runs,
                    updated_at = excluded.updated_at
                """,
                (
                    source_key,
                    float(final_accepted_eed),
                    float(cost_seconds),
                    baseline_signature,
                    model_signature,
                    int(float(final_accepted_eed) == 0.0),
                    now,
                ),
            )
            self._attribute_search_reward_locked(
                source_key,
                accepted_novel_eed=float(final_accepted_eed),
                reward_kind="final",
                baseline_signature=baseline_signature,
                model_signature=model_signature,
            )
            self.connection.execute(
                """
                UPDATE source_llm_source_attribution
                SET credited_eed = ?
                WHERE source_key = ?
                """,
                (float(final_accepted_eed), source_key),
            )

    def reset_final_rewards(self) -> None:
        """Invalidate current FINAL projection without erasing run audit rows."""
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            rows = self.connection.execute(
                """
                SELECT episode_id, credited_eed
                FROM source_search_reward_attribution
                WHERE reward_kind = 'final'
                """
            ).fetchall()
            for row in rows:
                credit = float(row["credited_eed"] or 0.0)
                if credit:
                    self.connection.execute(
                        """
                        UPDATE source_search_episodes
                        SET accepted_novel_eed = MAX(0, accepted_novel_eed - ?)
                        WHERE episode_id = ?
                        """,
                        (credit, str(row["episode_id"])),
                    )
            self.connection.execute(
                """
                UPDATE source_search_reward_attribution
                SET credited_eed = 0,
                    reward_kind = 'invalidated_final',
                    baseline_signature = NULL,
                    model_signature = NULL
                WHERE reward_kind = 'final'
                """
            )
            self.connection.execute(
                """
                UPDATE source_llm_source_attribution
                SET credited_eed = 0
                WHERE source_key IN (
                    SELECT source_key FROM source_final_rewards
                )
                """
            )
            self.connection.execute("DELETE FROM source_final_rewards")
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def llm_task_rewards(self) -> list[dict[str, object]]:
        rows = self.connection.execute(
            """
            WITH episode_cost AS (
                SELECT task_type,
                       COUNT(*) AS episodes,
                       SUM(cost_seconds) AS cost_seconds
                FROM source_llm_episodes
                WHERE finished_at IS NOT NULL
                GROUP BY task_type
            ),
            task_reward AS (
                SELECT e.task_type,
                       COALESCE(SUM(a.credited_eed), 0) AS credited_eed
                FROM source_llm_episodes e
                LEFT JOIN source_llm_hypotheses h
                  ON h.episode_id = e.episode_id
                LEFT JOIN source_llm_source_attribution a
                  ON a.hypothesis_id = h.hypothesis_id
                WHERE e.finished_at IS NOT NULL
                GROUP BY e.task_type
            )
            SELECT c.task_type, c.episodes, c.cost_seconds,
                   COALESCE(r.credited_eed, 0) AS credited_eed
            FROM episode_cost c
            LEFT JOIN task_reward r ON r.task_type = c.task_type
            ORDER BY c.task_type
            """
        ).fetchall()
        return [
            {
                "task_type": str(row["task_type"]),
                "episodes": int(row["episodes"]),
                "credited_eed": float(row["credited_eed"] or 0.0),
                "cost_seconds": float(row["cost_seconds"] or 0.0),
            }
            for row in rows
        ]

    def record_overlap_sketch(
        self,
        source_key: str,
        values: tuple[int, ...],
    ) -> None:
        if not values:
            raise ValueError("overlap sketch cannot be empty")
        if self.get_candidate(source_key) is None:
            raise KeyError(f"unknown source: {source_key}")
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_overlap_sketches(
                    source_key, width, sketch_json, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    width = excluded.width,
                    sketch_json = excluded.sketch_json,
                    updated_at = excluded.updated_at
                """,
                (
                    source_key,
                    len(values),
                    json.dumps(list(values), separators=(",", ":")),
                    float(self.clock()),
                ),
            )

    def get_overlap_sketch(
        self,
        source_key: str,
    ) -> tuple[int, ...] | None:
        row = self.connection.execute(
            """
            SELECT width, sketch_json
            FROM source_overlap_sketches
            WHERE source_key = ?
            """,
            (source_key,),
        ).fetchone()
        if row is None:
            return None
        values = tuple(int(value) for value in json.loads(row["sketch_json"]))
        if len(values) != int(row["width"]):
            raise ValueError("stored overlap sketch width mismatch")
        return values

    def register_proposal(
        self,
        candidate: SourceCandidate,
        *,
        episode_id: str | None = None,
        proposal_id: str | None = None,
    ) -> tuple[SourceCandidate, bool]:
        """Persist one proposal event and deduplicate the underlying resource."""
        if is_common_crawl_provenance(
            candidate.source_family,
            candidate.canonical_entrypoint,
            candidate.discovered_by,
        ):
            raise ValueError(
                "Common Crawl corpus is excluded from the active candidate pool"
            )
        now = float(self.clock())
        proposal_id = proposal_id or f"proposal:{uuid.uuid4().hex}"
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            if episode_id is not None:
                exists = self.connection.execute(
                    "SELECT 1 FROM source_search_episodes WHERE episode_id = ?",
                    (episode_id,),
                ).fetchone()
                if exists is None:
                    raise KeyError(f"unknown search episode: {episode_id}")
            inserted = self.connection.execute(
                """
                INSERT OR IGNORE INTO source_candidates(
                    source_key, canonical_entrypoint, source_family, source_level,
                    discovered_by, discovery_strategy, expected_year_from,
                    expected_year_to, expected_volume, temporal_semantics_prior,
                    enumerability_prior, direct_evidence_prior,
                    baseline_overlap_prior, access_cost_prior, adapter_cost_prior,
                    confidence, state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    now,
                    now,
                ),
            ).rowcount == 1
            self.connection.execute(
                """
                INSERT INTO source_proposals(
                    proposal_id, episode_id, source_key, discovered_by,
                    discovery_strategy, source_family, source_level, confidence,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    proposal_id,
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
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        stored = self.get_candidate(candidate.source_key)
        assert stored is not None
        return stored, inserted

    def proposal_count(self, source_key: str) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) AS n FROM source_proposals WHERE source_key = ?",
            (source_key,),
        ).fetchone()
        return int(row["n"])

    def get_candidate(self, source_key: str) -> SourceCandidate | None:
        row = self.connection.execute(
            "SELECT * FROM source_candidates WHERE source_key = ?",
            (source_key,),
        ).fetchone()
        return None if row is None else self._candidate_from_row(row)

    def list_candidates(
        self,
        *,
        state: SourceState | None = None,
    ) -> list[SourceCandidate]:
        if state is None:
            rows = self.connection.execute("SELECT * FROM source_candidates").fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM source_candidates WHERE state = ?",
                (SourceState(state).value,),
            ).fetchall()
        return [self._candidate_from_row(row) for row in rows]

    def reconcile_exhausted_activations(self) -> int:
        """Mirror terminal production Reservoir state back into discovery.

        Discovery activation capacity is defined by productive ACTIVE sources,
        not historical activations. Once the durable production Reservoir is
        exhausted, the corresponding candidate must leave ACTIVE so the
        manager can promote another WARM source automatically.
        """
        rows = self.connection.execute(
            """
            SELECT sc.source_key
            FROM source_candidates AS sc
            JOIN source_activations AS sa ON sa.source_key = sc.source_key
            JOIN reservoirs AS r ON r.reservoir_id = sa.reservoir_id
            WHERE sc.state = ? AND r.state = ?
            ORDER BY sc.source_key
            """,
            (SourceState.ACTIVE.value, "EXHAUSTED"),
        ).fetchall()
        changed = 0
        for row in rows:
            self.transition(str(row["source_key"]), SourceState.EXHAUSTED)
            changed += 1
        return changed

    def list_candidates_in_states(
        self,
        states: Iterable[SourceState],
    ) -> list[SourceCandidate]:
        """Read only scheduling-relevant source states from the hot path."""
        values = tuple(dict.fromkeys(SourceState(state).value for state in states))
        if not values:
            return []
        placeholders = ",".join("?" for _ in values)
        rows = self.connection.execute(
            f"SELECT * FROM source_candidates WHERE state IN ({placeholders})",
            values,
        ).fetchall()
        return [self._candidate_from_row(row) for row in rows]

    def inventory(self) -> dict[SourceState, int]:
        result = {state: 0 for state in SourceState}
        for row in self.connection.execute(
            "SELECT state, COUNT(*) AS n FROM source_candidates GROUP BY state"
        ):
            result[SourceState(str(row["state"]))] = int(row["n"])
        return result

    def transition(self, source_key: str, target: SourceState) -> SourceCandidate:
        target = SourceState(target)
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                "SELECT state FROM source_candidates WHERE source_key = ?",
                (source_key,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown source: {source_key}")
            current = SourceState(str(row["state"]))
            if target not in _TRANSITIONS[current]:
                raise StateTransitionError(
                    f"invalid source transition: {current} -> {target}"
                )
            requires_current_measurement = (
                target is SourceState.ACTIVE
                or (
                    current is SourceState.SCOUTING
                    and target is SourceState.WARM
                )
            )
            if requires_current_measurement:
                measured = self.connection.execute(
                    """
                    SELECT
                        m.baseline_signature,
                        m.model_signature,
                        a.baseline_signature AS current_baseline_signature,
                        a.model_signature AS current_model_signature
                    FROM source_scout_metrics m
                    LEFT JOIN source_scout_authority a ON a.singleton = 1
                    WHERE m.source_key = ?
                    """,
                    (source_key,),
                ).fetchone()
                eligible = measured is not None
                if (
                    eligible
                    and measured["current_baseline_signature"] is not None
                ):
                    eligible = (
                        str(measured["baseline_signature"] or "")
                        == str(measured["current_baseline_signature"])
                        and str(measured["model_signature"] or "")
                        == str(measured["current_model_signature"])
                    )
                if not eligible:
                    raise StateTransitionError(
                        "current-authority measured scout evidence is required "
                        "before warm/active promotion"
                    )
            self.connection.execute(
                "UPDATE source_candidates SET state = ?, updated_at = ? WHERE source_key = ?",
                (target.value, now, source_key),
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        candidate = self.get_candidate(source_key)
        assert candidate is not None
        return candidate

    def _attribute_search_reward_locked(
        self,
        source_key: str,
        *,
        accepted_novel_eed: float,
        reward_kind: str = "final",
        baseline_signature: str | None = None,
        model_signature: str | None = None,
    ) -> None:
        """Idempotently attribute proxy/final yield to one originating search.

        A final accepted reward, when present, always supersedes scout proxy
        yield. Duplicate proposals do not multiply reward.
        """
        final_row = self.connection.execute(
            """
            SELECT f.final_accepted_eed,
                   f.baseline_signature,
                   f.model_signature
            FROM source_final_rewards f
            LEFT JOIN source_scout_authority a ON a.singleton = 1
            WHERE f.source_key = ?
              AND (
                  a.singleton IS NULL
                  OR (
                      f.baseline_signature = a.baseline_signature
                      AND f.model_signature = a.model_signature
                  )
              )
            """,
            (source_key,),
        ).fetchone()
        if final_row is not None:
            accepted_novel_eed = float(final_row["final_accepted_eed"])
            reward_kind = "final"
            baseline_signature = final_row["baseline_signature"]
            model_signature = final_row["model_signature"]
        attribution = self.connection.execute(
            """
            SELECT episode_id, credited_eed
            FROM source_search_reward_attribution
            WHERE source_key = ?
            """,
            (source_key,),
        ).fetchone()
        if attribution is None:
            proposal = self.connection.execute(
                """
                SELECT episode_id
                FROM source_proposals
                WHERE source_key = ? AND episode_id IS NOT NULL
                ORDER BY created_at, proposal_id
                LIMIT 1
                """,
                (source_key,),
            ).fetchone()
            if proposal is None:
                return
            episode_id = str(proposal["episode_id"])
            old_credit = 0.0
            self.connection.execute(
                """
                INSERT INTO source_search_reward_attribution(
                    source_key, episode_id, credited_eed, reward_kind,
                    baseline_signature, model_signature
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source_key,
                    episode_id,
                    float(accepted_novel_eed),
                    reward_kind,
                    baseline_signature,
                    model_signature,
                ),
            )
        else:
            episode_id = str(attribution["episode_id"])
            old_credit = float(attribution["credited_eed"])
            self.connection.execute(
                """
                UPDATE source_search_reward_attribution
                SET credited_eed = ?,
                    reward_kind = ?,
                    baseline_signature = ?,
                    model_signature = ?
                WHERE source_key = ?
                """,
                (
                    float(accepted_novel_eed),
                    reward_kind,
                    baseline_signature,
                    model_signature,
                    source_key,
                ),
            )
        delta = float(accepted_novel_eed) - old_credit
        if delta:
            self.connection.execute(
                """
                UPDATE source_search_episodes
                SET accepted_novel_eed = MAX(0, accepted_novel_eed + ?)
                WHERE episode_id = ?
                """,
                (delta, episode_id),
            )

    def record_triage_observation(
        self,
        source_key: str,
        *,
        status_code: int | None,
        method: str | None,
        content_type: str | None,
        content_length: int | None,
        range_supported: bool | None,
    ) -> None:
        if self.get_candidate(source_key) is None:
            raise KeyError(f"unknown source: {source_key}")
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_triage_metrics(
                    source_key, status_code, method, content_type,
                    content_length, range_supported, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    status_code = excluded.status_code,
                    method = excluded.method,
                    content_type = excluded.content_type,
                    content_length = excluded.content_length,
                    range_supported = excluded.range_supported,
                    observed_at = excluded.observed_at
                """,
                (
                    source_key,
                    status_code,
                    method,
                    content_type,
                    content_length,
                    None
                    if range_supported is None
                    else int(range_supported),
                    float(self.clock()),
                ),
            )

    def get_triage_observation(
        self,
        source_key: str,
    ) -> dict[str, object] | None:
        row = self.connection.execute(
            """
            SELECT status_code, method, content_type, content_length,
                   range_supported, observed_at
            FROM source_triage_metrics
            WHERE source_key = ?
            """,
            (source_key,),
        ).fetchone()
        if row is None:
            return None
        return {
            "status_code": row["status_code"],
            "method": row["method"],
            "content_type": row["content_type"],
            "content_length": row["content_length"],
            "range_supported": (
                None
                if row["range_supported"] is None
                else bool(row["range_supported"])
            ),
            "observed_at": float(row["observed_at"]),
        }

    def record_scout_measurement(
        self,
        source_key: str,
        measurement: ScoutMeasurement,
        *,
        baseline_signature: str | None = None,
        model_signature: str | None = None,
    ) -> bool:
        if self.get_candidate(source_key) is None:
            raise KeyError(f"unknown source: {source_key}")
        if (baseline_signature is None) != (model_signature is None):
            raise ValueError(
                "baseline_signature and model_signature must be provided together"
            )

        # Authority selection and the metric write share one IMMEDIATE
        # transaction. That prevents an authority replacement from interleaving
        # between the comparison and durable attribution.
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            current_authority = self._load_scout_authority()
            measurement_authority = (
                current_authority
                if baseline_signature is None
                else (baseline_signature, model_signature)
            )
            if measurement_authority is not None and (
                not measurement_authority[0]
                or not measurement_authority[1]
            ):
                raise ValueError("scout authority signatures must be non-empty")
            eligible = (
                current_authority is None
                or measurement_authority == current_authority
            )

            # A late result from an old measured executor may be retained only
            # when no current-authority row exists. It must never clobber a
            # remeasurement that already completed under the new authority.
            preserve_current = False
            if not eligible and current_authority is not None:
                preserve_current = (
                    self.connection.execute(
                        """
                        SELECT 1
                        FROM source_scout_metrics
                        WHERE source_key = ?
                          AND baseline_signature = ?
                          AND model_signature = ?
                        """,
                        (
                            source_key,
                            current_authority[0],
                            current_authority[1],
                        ),
                    ).fetchone()
                    is not None
                )

            if not preserve_current:
                now = float(self.clock())
                self.connection.execute(
                    """
                    INSERT INTO source_scout_metrics(
                        source_key, sampled_records, unique_hosts, novel_hosts,
                        direct_host_years, requests, bytes_read, elapsed_seconds,
                        novel_eed, measurement_mode, observed_host_year_pairs,
                        novel_host_year_pairs, novel_pair_eed,
                        singleton_observations, doubleton_observations,
                        estimated_unseen_fraction, baseline_signature,
                        model_signature, measured_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_key) DO UPDATE SET
                        sampled_records = excluded.sampled_records,
                        unique_hosts = excluded.unique_hosts,
                        novel_hosts = excluded.novel_hosts,
                        direct_host_years = excluded.direct_host_years,
                        requests = excluded.requests,
                        bytes_read = excluded.bytes_read,
                        elapsed_seconds = excluded.elapsed_seconds,
                        novel_eed = excluded.novel_eed,
                        measurement_mode = excluded.measurement_mode,
                        observed_host_year_pairs = excluded.observed_host_year_pairs,
                        novel_host_year_pairs = excluded.novel_host_year_pairs,
                        novel_pair_eed = excluded.novel_pair_eed,
                        singleton_observations = excluded.singleton_observations,
                        doubleton_observations = excluded.doubleton_observations,
                        estimated_unseen_fraction = excluded.estimated_unseen_fraction,
                        baseline_signature = excluded.baseline_signature,
                        model_signature = excluded.model_signature,
                        measured_at = excluded.measured_at
                    """,
                    (
                        source_key,
                        measurement.sampled_records,
                        measurement.unique_hosts,
                        measurement.novel_hosts,
                        measurement.direct_host_years,
                        measurement.requests,
                        measurement.bytes_read,
                        measurement.elapsed_seconds,
                        measurement.novel_eed,
                        measurement.measurement_mode.value,
                        measurement.observed_host_year_pairs,
                        measurement.novel_host_year_pairs,
                        measurement.novel_pair_eed,
                        measurement.singleton_observations,
                        measurement.doubleton_observations,
                        measurement.estimated_unseen_fraction,
                        (
                            None
                            if measurement_authority is None
                            else measurement_authority[0]
                        ),
                        (
                            None
                            if measurement_authority is None
                            else measurement_authority[1]
                        ),
                        now,
                    ),
                )
                if measurement.minhash_values:
                    self.connection.execute(
                        """
                        INSERT INTO source_overlap_sketches(
                            source_key, width, sketch_json, updated_at
                        ) VALUES (?, ?, ?, ?)
                        ON CONFLICT(source_key) DO UPDATE SET
                            width = excluded.width,
                            sketch_json = excluded.sketch_json,
                            updated_at = excluded.updated_at
                        """,
                        (
                            source_key,
                            len(measurement.minhash_values),
                            json.dumps(
                                list(measurement.minhash_values),
                                separators=(",", ":"),
                            ),
                            now,
                        ),
                    )

            if eligible:
                self._attribute_search_reward_locked(
                    source_key,
                    accepted_novel_eed=measurement.novel_eed_for_ranking,
                    reward_kind="scout_proxy",
                    baseline_signature=(
                        None
                        if measurement_authority is None
                        else measurement_authority[0]
                    ),
                    model_signature=(
                        None
                        if measurement_authority is None
                        else measurement_authority[1]
                    ),
                )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return eligible

    def get_scout_measurement(
        self,
        source_key: str,
        *,
        baseline_signature: str | None = None,
        model_signature: str | None = None,
    ) -> ScoutMeasurement | None:
        if (baseline_signature is None) != (model_signature is None):
            raise ValueError(
                "baseline_signature and model_signature must be provided together"
            )
        explicit_authority = (
            None
            if baseline_signature is None
            else (baseline_signature, model_signature)
        )
        row = self.connection.execute(
            """
            SELECT
                m.*,
                a.baseline_signature AS current_baseline_signature,
                a.model_signature AS current_model_signature
            FROM source_scout_metrics m
            LEFT JOIN source_scout_authority a ON a.singleton = 1
            WHERE m.source_key = ?
            """,
            (source_key,),
        ).fetchone()
        if row is None:
            return None
        required_authority = explicit_authority
        if required_authority is None and row["current_baseline_signature"] is not None:
            required_authority = (
                str(row["current_baseline_signature"]),
                str(row["current_model_signature"]),
            )
        if required_authority is not None and (
            str(row["baseline_signature"] or "") != required_authority[0]
            or str(row["model_signature"] or "") != required_authority[1]
        ):
            return None
        return self._measurement_from_row(row)

    def rank_scout_candidates(self, *, limit: int) -> list[SourceCandidate]:
        if limit < 1:
            return []
        candidates = [
            candidate
            for candidate in self.list_candidates(state=SourceState.SCOUT_READY)
            if self.suppression_reason(candidate) is None
        ]
        candidates.sort(key=lambda item: (-item.scout_priority, item.source_key))
        return candidates[:limit]

    def suppress(
        self,
        scope: SuppressionScope,
        scope_key: str,
        *,
        reason: str,
        ttl_seconds: float | None = None,
    ) -> None:
        scope = SuppressionScope(scope)
        if not scope_key.strip() or not reason.strip():
            raise ValueError("suppression key and reason are required")
        if ttl_seconds is not None and ttl_seconds < 0:
            raise ValueError("ttl_seconds must be non-negative")
        now = float(self.clock())
        expires_at = None if ttl_seconds is None else now + float(ttl_seconds)
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_suppressions(
                    scope_type, scope_key, reason, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(scope_type, scope_key) DO UPDATE SET
                    reason = excluded.reason,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at
                """,
                (scope.value, scope_key, reason, now, expires_at),
            )

    def suppress_candidate(
        self,
        candidate: SourceCandidate,
        *,
        reason: str,
        scope: SuppressionScope = SuppressionScope.SOURCE,
        ttl_seconds: float | None = None,
    ) -> None:
        scope = SuppressionScope(scope)
        key = {
            SuppressionScope.SOURCE: candidate.source_key,
            SuppressionScope.FAMILY: candidate.source_family,
            SuppressionScope.ORIGIN: candidate.origin,
        }[scope]
        self.suppress(scope, key, reason=reason, ttl_seconds=ttl_seconds)

    def suppression_reason(self, candidate: SourceCandidate) -> str | None:
        now = float(self.clock())
        scopes = (
            (SuppressionScope.SOURCE.value, candidate.source_key),
            (SuppressionScope.FAMILY.value, candidate.source_family),
            (SuppressionScope.ORIGIN.value, candidate.origin),
        )
        for scope_type, scope_key in scopes:
            row = self.connection.execute(
                """
                SELECT reason FROM source_suppressions
                WHERE scope_type = ? AND scope_key = ?
                  AND (expires_at IS NULL OR expires_at > ?)
                """,
                (scope_type, scope_key, now),
            ).fetchone()
            if row is not None:
                return str(row["reason"])
        return None

    def prune_expired_suppressions(self) -> int:
        before = self.connection.total_changes
        with self.connection:
            self.connection.execute(
                "DELETE FROM source_suppressions WHERE expires_at IS NOT NULL AND expires_at <= ?",
                (float(self.clock()),),
            )
        return self.connection.total_changes - before

    def _max_ancestor_hops(self, source_key: str) -> int:
        row = self.connection.execute(
            """
            WITH RECURSIVE ancestors(node, depth) AS (
                SELECT parent_key, 1 FROM source_edges WHERE child_key = ?
                UNION ALL
                SELECT e.parent_key, a.depth + 1
                FROM source_edges e JOIN ancestors a ON e.child_key = a.node
                WHERE a.depth <= ?
            )
            SELECT COALESCE(MAX(depth), 0) AS depth FROM ancestors
            """,
            (source_key, self.max_graph_hops),
        ).fetchone()
        return int(row["depth"])

    def _max_descendant_hops(self, source_key: str) -> int:
        row = self.connection.execute(
            """
            WITH RECURSIVE descendants(node, depth) AS (
                SELECT child_key, 1 FROM source_edges WHERE parent_key = ?
                UNION ALL
                SELECT e.child_key, d.depth + 1
                FROM source_edges e JOIN descendants d ON e.parent_key = d.node
                WHERE d.depth <= ?
            )
            SELECT COALESCE(MAX(depth), 0) AS depth FROM descendants
            """,
            (source_key, self.max_graph_hops),
        ).fetchone()
        return int(row["depth"])

    def _would_cycle(self, parent_key: str, child_key: str) -> bool:
        row = self.connection.execute(
            """
            WITH RECURSIVE descendants(node) AS (
                SELECT child_key FROM source_edges WHERE parent_key = ?
                UNION
                SELECT e.child_key
                FROM source_edges e JOIN descendants d ON e.parent_key = d.node
            )
            SELECT 1 FROM descendants WHERE node = ? LIMIT 1
            """,
            (child_key, parent_key),
        ).fetchone()
        return row is not None

    def add_edge(
        self,
        parent_key: str,
        child_key: str,
        *,
        relation: str = "enumerates",
    ) -> bool:
        """Add one source-level DAG edge, atomically rejecting loops and depth blowup."""
        if parent_key == child_key:
            raise ValueError("source graph self-edge is forbidden")
        if not relation.strip():
            raise ValueError("source relation is required")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            found = self.connection.execute(
                "SELECT COUNT(*) AS n FROM source_candidates WHERE source_key IN (?, ?)",
                (parent_key, child_key),
            ).fetchone()
            if int(found["n"]) != 2:
                raise KeyError("both source graph endpoints must already exist")
            duplicate = self.connection.execute(
                """
                SELECT 1 FROM source_edges
                WHERE parent_key = ? AND child_key = ? AND relation = ?
                """,
                (parent_key, child_key, relation),
            ).fetchone()
            if duplicate is not None:
                self.connection.commit()
                return False
            if self._would_cycle(parent_key, child_key):
                raise ValueError("source graph edge would create a cycle")
            full_hops = (
                self._max_ancestor_hops(parent_key)
                + 1
                + self._max_descendant_hops(child_key)
            )
            if full_hops > self.max_graph_hops:
                raise ValueError(
                    f"source graph edge exceeds max_graph_hops={self.max_graph_hops}"
                )
            self.connection.execute(
                """
                INSERT INTO source_edges(parent_key, child_key, relation, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (parent_key, child_key, relation, float(self.clock())),
            )
            self.connection.commit()
            return True
        except BaseException:
            self.connection.rollback()
            raise

    def children(self, source_key: str) -> list[str]:
        return [
            str(row["child_key"])
            for row in self.connection.execute(
                "SELECT child_key FROM source_edges WHERE parent_key = ? ORDER BY child_key",
                (source_key,),
            )
        ]
