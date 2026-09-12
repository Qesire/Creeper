"""SQLite-backed registry for source discovery, lineage, and search attribution."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections.abc import Iterable

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

            CREATE TABLE IF NOT EXISTS source_final_rewards (
                source_key TEXT PRIMARY KEY,
                final_accepted_eed REAL NOT NULL,
                cost_seconds REAL NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                FOREIGN KEY(source_key) REFERENCES source_candidates(source_key)
            ) WITHOUT ROWID;

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
                measured_at REAL NOT NULL,
                FOREIGN KEY(source_key) REFERENCES source_candidates(source_key)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS source_search_reward_attribution (
                source_key TEXT PRIMARY KEY,
                episode_id TEXT NOT NULL,
                credited_eed REAL NOT NULL DEFAULT 0,
                FOREIGN KEY(source_key) REFERENCES source_candidates(source_key),
                FOREIGN KEY(episode_id) REFERENCES source_search_episodes(episode_id)
            ) WITHOUT ROWID;

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
        columns = {
            str(row[1])
            for row in self.connection.execute(
                "PRAGMA table_info(source_scout_metrics)"
            ).fetchall()
        }
        migrations = {
            "measurement_mode": "ALTER TABLE source_scout_metrics ADD COLUMN measurement_mode TEXT NOT NULL DEFAULT 'HOST_ONLY'",
            "observed_host_year_pairs": "ALTER TABLE source_scout_metrics ADD COLUMN observed_host_year_pairs INTEGER NOT NULL DEFAULT 0",
            "novel_host_year_pairs": "ALTER TABLE source_scout_metrics ADD COLUMN novel_host_year_pairs INTEGER NOT NULL DEFAULT 0",
            "novel_pair_eed": "ALTER TABLE source_scout_metrics ADD COLUMN novel_pair_eed REAL NOT NULL DEFAULT 0",
        }
        for name, statement in migrations.items():
            if name not in columns:
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

    @staticmethod
    def _measurement_from_row(row: sqlite3.Row) -> ScoutMeasurement:
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

    def record_final_reward(
        self,
        source_key: str,
        *,
        final_accepted_eed: float,
        cost_seconds: float = 0.0,
    ) -> None:
        """Close proxy reward with final accepted competition value.

        Until this method is called, measured scout EED is an explicit proxy.
        Once a final value exists it becomes the reward attributed to the
        originating search strategy and Codex hypothesis.
        """
        if final_accepted_eed < 0 or cost_seconds < 0:
            raise ValueError("final reward and cost must be non-negative")
        if self.get_candidate(source_key) is None:
            raise KeyError(f"unknown source: {source_key}")
        now = float(self.clock())
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_final_rewards(
                    source_key, final_accepted_eed, cost_seconds, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    final_accepted_eed = excluded.final_accepted_eed,
                    cost_seconds = excluded.cost_seconds,
                    updated_at = excluded.updated_at
                """,
                (
                    source_key,
                    float(final_accepted_eed),
                    float(cost_seconds),
                    now,
                ),
            )
            self._attribute_search_reward_locked(
                source_key,
                accepted_novel_eed=float(final_accepted_eed),
            )
            self.connection.execute(
                """
                UPDATE source_llm_source_attribution
                SET credited_eed = ?
                WHERE source_key = ?
                """,
                (float(final_accepted_eed), source_key),
            )

    def llm_task_rewards(self) -> list[dict[str, object]]:
        rows = self.connection.execute(
            """
            SELECT e.task_type,
                   COUNT(DISTINCT e.episode_id) AS episodes,
                   COALESCE(SUM(a.credited_eed), 0) AS credited_eed,
                   COALESCE(SUM(DISTINCT e.cost_seconds), 0) AS cost_seconds
            FROM source_llm_episodes e
            LEFT JOIN source_llm_hypotheses h
              ON h.episode_id = e.episode_id
            LEFT JOIN source_llm_source_attribution a
              ON a.hypothesis_id = h.hypothesis_id
            WHERE e.finished_at IS NOT NULL
            GROUP BY e.task_type
            ORDER BY e.task_type
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
            if target in {SourceState.WARM, SourceState.ACTIVE}:
                measured = self.connection.execute(
                    "SELECT 1 FROM source_scout_metrics WHERE source_key = ?",
                    (source_key,),
                ).fetchone()
                if measured is None:
                    raise StateTransitionError(
                        "measured scout evidence is required before warm/active promotion"
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
    ) -> None:
        """Idempotently attribute proxy/final yield to one originating search.

        A final accepted reward, when present, always supersedes scout proxy
        yield. Duplicate proposals do not multiply reward.
        """
        final_row = self.connection.execute(
            """
            SELECT final_accepted_eed
            FROM source_final_rewards
            WHERE source_key = ?
            """,
            (source_key,),
        ).fetchone()
        if final_row is not None:
            accepted_novel_eed = float(final_row["final_accepted_eed"])
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
                    source_key, episode_id, credited_eed
                ) VALUES (?, ?, ?)
                """,
                (source_key, episode_id, float(accepted_novel_eed)),
            )
        else:
            episode_id = str(attribution["episode_id"])
            old_credit = float(attribution["credited_eed"])
            self.connection.execute(
                """
                UPDATE source_search_reward_attribution
                SET credited_eed = ?
                WHERE source_key = ?
                """,
                (float(accepted_novel_eed), source_key),
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

    def record_scout_measurement(
        self,
        source_key: str,
        measurement: ScoutMeasurement,
    ) -> None:
        if self.get_candidate(source_key) is None:
            raise KeyError(f"unknown source: {source_key}")
        now = float(self.clock())
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_scout_metrics(
                    source_key, sampled_records, unique_hosts, novel_hosts,
                    direct_host_years, requests, bytes_read, elapsed_seconds,
                    novel_eed, measurement_mode, observed_host_year_pairs,
                    novel_host_year_pairs, novel_pair_eed, measured_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    now,
                ),
            )
            self._attribute_search_reward_locked(
                source_key,
                accepted_novel_eed=measurement.novel_eed_for_ranking,
            )

    def get_scout_measurement(self, source_key: str) -> ScoutMeasurement | None:
        row = self.connection.execute(
            "SELECT * FROM source_scout_metrics WHERE source_key = ?",
            (source_key,),
        ).fetchone()
        return None if row is None else self._measurement_from_row(row)

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
