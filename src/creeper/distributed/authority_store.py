"""Durable authority core for Creeper's distributed execution fabric.

The store is intentionally independent from the current ControlStore.  It is a
side-by-side vNext control plane whose invariants can be validated locally
before any cloud worker becomes a correctness dependency.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from uuid import uuid4

from creeper.authority.baseline_index import BaselineIndex, YEAR_BITS
from creeper.authority.normalizer import normalize_official
from creeper.distributed.edition import (
    FABRIC_EVIDENCE_ONLY_PRODUCERS,
    FABRIC_PROTOCOL_VERSION,
    FABRIC_THIN_MAX_ESTIMATED_RESPONSE_BYTES,
    FABRIC_THIN_MAX_PROVIDER_REQUESTS,
)
from creeper.distributed.identity import (
    evidence_id,
    host_id,
    host_year_id,
    source_candidate_id,
)
from creeper.distributed.search_campaign import SearchCampaign
from creeper.distributed.urlcanon import canonical_http_url
from creeper.evidence.contracts import resolve_source_evidence_contract
from creeper.distributed.models import (
    ProviderPermit,
    ResultBatch,
    TaskClass,
    TaskLease,
    WorkDefinition,
    WorkerDescriptor,
)


class StaleLeaseError(RuntimeError):
    """Raised when a request does not own the current lease generation."""


class BatchConflictError(RuntimeError):
    """Raised when one BatchID is replayed with different contents."""


class BatchSequenceError(RuntimeError):
    """Raised when a new ResultBatch arrives out of checkpoint order."""


class WorkerRejectedError(RuntimeError):
    """Raised when an unknown or revoked worker attempts authority actions."""


class AuthorityNotReadyError(RuntimeError):
    """Required local authority state is unavailable; fail closed."""


class ProviderRegionNotQualifiedError(RuntimeError):
    """A formal provider request was attempted from an unqualified region."""


class FabricProtocolMismatchError(RuntimeError):
    """Worker and Authority speak incompatible derivative protocols."""


class ProviderAccessDeniedError(RuntimeError):
    """Worker is not permitted to access the requested provider."""


class WorkerEgressBudgetExceededError(RuntimeError):
    """Worker exhausted its local daily provider egress budget."""


def _json(value) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


class DistributedAuthorityStore:
    """SQLite-WAL source of truth for distributed work ownership.

    Correctness properties enforced here:

    * UNIQUE(work_key) prevents duplicate logical work admission.
    * lease_generation is monotonically incremented on every claim/reclaim.
    * every mutating worker request is fenced by owner + generation + deadline.
    * UNIQUE(batch_id) makes result batches idempotent.
    * provider permits are issued from one global provider budget, independent
      of worker region.
    """

    def __init__(
        self,
        path: Path,
        *,
        baseline_index: BaselineIndex | None = None,
        clock=time.time,
    ) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.baseline_index = baseline_index
        self.connection = sqlite3.connect(path, timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS distributed_workers (
                worker_id TEXT PRIMARY KEY,
                runtime_class TEXT NOT NULL,
                region TEXT NOT NULL,
                architecture TEXT NOT NULL,
                memory_bytes INTEGER NOT NULL CHECK(memory_bytes >= 0),
                cpu_count INTEGER NOT NULL CHECK(cpu_count >= 1),
                network_class TEXT NOT NULL,
                capabilities_json TEXT NOT NULL,
                producers_json TEXT NOT NULL DEFAULT '[]',
                allowed_providers_json TEXT NOT NULL DEFAULT '[]',
                daily_egress_budget_bytes INTEGER NOT NULL DEFAULT 0
                    CHECK(daily_egress_budget_bytes >= 0),
                protocol_version TEXT NOT NULL DEFAULT 'creeper-fabric-v1',
                edition_version TEXT NOT NULL DEFAULT '0.1.0-dev',
                last_heartbeat REAL NOT NULL,
                revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0, 1))
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS distributed_work (
                task_id TEXT PRIMARY KEY,
                work_key TEXT NOT NULL UNIQUE,
                producer TEXT NOT NULL,
                task_class TEXT NOT NULL,
                input_identity TEXT NOT NULL,
                coverage_json TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                required_capabilities_json TEXT NOT NULL,
                priority REAL NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                lease_owner TEXT,
                lease_generation INTEGER NOT NULL DEFAULT 0
                    CHECK(lease_generation >= 0),
                lease_deadline REAL,
                attempt INTEGER NOT NULL DEFAULT 0 CHECK(attempt >= 0),
                cursor TEXT,
                next_sequence_no INTEGER NOT NULL DEFAULT 0
                    CHECK(next_sequence_no >= 0),
                last_error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_distributed_work_claim
                ON distributed_work(state, priority DESC, created_at);

            CREATE TABLE IF NOT EXISTS distributed_result_batches (
                batch_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK(generation >= 1),
                sequence_no INTEGER NOT NULL CHECK(sequence_no >= 0),
                payload_json TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                cursor_after TEXT,
                committed_at REAL NOT NULL,
                UNIQUE(task_id, sequence_no),
                FOREIGN KEY(task_id) REFERENCES distributed_work(task_id)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS distributed_host_candidates (
                candidate_id TEXT PRIMARY KEY,
                hostname TEXT NOT NULL UNIQUE,
                first_task_id TEXT NOT NULL,
                first_source TEXT NOT NULL,
                first_locator TEXT NOT NULL,
                discovery_count INTEGER NOT NULL DEFAULT 1
                    CHECK(discovery_count >= 1),
                state TEXT NOT NULL DEFAULT 'DISCOVERED',
                last_error TEXT,
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL,
                FOREIGN KEY(first_task_id) REFERENCES distributed_work(task_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_distributed_host_candidates_state
                ON distributed_host_candidates(state, discovery_count DESC);

            CREATE TABLE IF NOT EXISTS distributed_source_candidates (
                candidate_id TEXT PRIMARY KEY,
                canonical_url TEXT NOT NULL UNIQUE,
                candidate_type TEXT NOT NULL,
                parser_kind TEXT NOT NULL,
                first_task_id TEXT NOT NULL,
                first_referrer_url TEXT NOT NULL,
                discovery_count INTEGER NOT NULL DEFAULT 1
                    CHECK(discovery_count >= 1),
                state TEXT NOT NULL DEFAULT 'DISCOVERED',
                admitted_task_id TEXT,
                last_error TEXT,
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL,
                FOREIGN KEY(first_task_id) REFERENCES distributed_work(task_id),
                FOREIGN KEY(admitted_task_id) REFERENCES distributed_work(task_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_distributed_source_candidates_state
                ON distributed_source_candidates(state, candidate_type);

            CREATE TABLE IF NOT EXISTS distributed_resolution_coverage (
                hostname TEXT NOT NULL,
                provider TEXT NOT NULL,
                scope TEXT NOT NULL,
                resolver_version TEXT NOT NULL,
                year_mask INTEGER NOT NULL DEFAULT 0 CHECK(year_mask >= 0),
                updated_at REAL NOT NULL,
                PRIMARY KEY(hostname, provider, scope, resolver_version)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS distributed_hy_probe_decisions (
                task_id TEXT NOT NULL,
                hy_id TEXT NOT NULL,
                hostname TEXT NOT NULL,
                year INTEGER NOT NULL CHECK(year BETWEEN 1996 AND 2001),
                locator TEXT NOT NULL,
                status TEXT NOT NULL,
                first_seen_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(task_id, hy_id),
                FOREIGN KEY(task_id) REFERENCES distributed_work(task_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_distributed_hy_probe_hy
                ON distributed_hy_probe_decisions(hy_id, status);

            CREATE TABLE IF NOT EXISTS distributed_host_year_ledger (
                hy_id TEXT PRIMARY KEY,
                hostname TEXT NOT NULL,
                year INTEGER NOT NULL CHECK(year BETWEEN 1996 AND 2001),
                evidence_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                accepted_at REAL NOT NULL,
                UNIQUE(hostname, year),
                FOREIGN KEY(task_id) REFERENCES distributed_work(task_id)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS distributed_evidence_ledger (
                evidence_id TEXT PRIMARY KEY,
                hy_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK(generation >= 1),
                evidence_json TEXT NOT NULL,
                committed_at REAL NOT NULL,
                FOREIGN KEY(task_id) REFERENCES distributed_work(task_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_distributed_evidence_hy
                ON distributed_evidence_ledger(hy_id);

            CREATE TABLE IF NOT EXISTS distributed_request_nonces (
                worker_id TEXT NOT NULL,
                nonce TEXT NOT NULL,
                seen_at REAL NOT NULL,
                PRIMARY KEY(worker_id, nonce)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_distributed_request_nonces_seen
                ON distributed_request_nonces(seen_at);

            CREATE TABLE IF NOT EXISTS distributed_worker_egress_daily (
                worker_id TEXT NOT NULL,
                day_key INTEGER NOT NULL,
                response_bytes INTEGER NOT NULL DEFAULT 0
                    CHECK(response_bytes >= 0),
                updated_at REAL NOT NULL,
                PRIMARY KEY(worker_id, day_key),
                FOREIGN KEY(worker_id) REFERENCES distributed_workers(worker_id)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS distributed_provider_regions (
                provider TEXT NOT NULL,
                region TEXT NOT NULL,
                state TEXT NOT NULL,
                samples INTEGER NOT NULL DEFAULT 0 CHECK(samples >= 0),
                successes INTEGER NOT NULL DEFAULT 0 CHECK(successes >= 0),
                timeouts INTEGER NOT NULL DEFAULT 0 CHECK(timeouts >= 0),
                throttles INTEGER NOT NULL DEFAULT 0 CHECK(throttles >= 0),
                policy_blocks INTEGER NOT NULL DEFAULT 0 CHECK(policy_blocks >= 0),
                total_latency_ms REAL NOT NULL DEFAULT 0
                    CHECK(total_latency_ms >= 0),
                response_bytes INTEGER NOT NULL DEFAULT 0
                    CHECK(response_bytes >= 0),
                updated_at REAL NOT NULL,
                PRIMARY KEY(provider, region)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS distributed_provider_budgets (
                provider TEXT PRIMARY KEY,
                requests_per_second REAL NOT NULL
                    CHECK(requests_per_second > 0),
                max_global_inflight INTEGER NOT NULL
                    CHECK(max_global_inflight >= 1),
                require_qualified_region INTEGER NOT NULL DEFAULT 1
                    CHECK(require_qualified_region IN (0, 1)),
                next_request_at REAL NOT NULL DEFAULT 0,
                cooldown_until REAL NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS distributed_provider_permits (
                permit_id TEXT PRIMARY KEY,
                request_id TEXT,
                provider TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK(generation >= 1),
                allowed_requests INTEGER NOT NULL CHECK(allowed_requests >= 1),
                max_inflight INTEGER NOT NULL CHECK(max_inflight >= 1),
                expires_at REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1)),
                issued_at REAL NOT NULL,
                status_code INTEGER,
                FOREIGN KEY(provider)
                    REFERENCES distributed_provider_budgets(provider),
                FOREIGN KEY(worker_id)
                    REFERENCES distributed_workers(worker_id),
                FOREIGN KEY(task_id)
                    REFERENCES distributed_work(task_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_distributed_provider_permit_active
                ON distributed_provider_permits(provider, active, expires_at);
            """
        )
        work_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(distributed_work)"
            )
        }
        if "next_sequence_no" not in work_columns:
            self.connection.execute(
                """
                ALTER TABLE distributed_work
                ADD COLUMN next_sequence_no INTEGER NOT NULL DEFAULT 0
                    CHECK(next_sequence_no >= 0)
                """
            )
        source_candidate_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(distributed_source_candidates)"
            )
        }
        if "admitted_task_id" not in source_candidate_columns:
            self.connection.execute(
                """
                ALTER TABLE distributed_source_candidates
                ADD COLUMN admitted_task_id TEXT
                """
            )
        if "last_error" not in source_candidate_columns:
            self.connection.execute(
                """
                ALTER TABLE distributed_source_candidates
                ADD COLUMN last_error TEXT
                """
            )
        worker_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(distributed_workers)"
            )
        }
        if "producers_json" not in worker_columns:
            self.connection.execute(
                """
                ALTER TABLE distributed_workers
                ADD COLUMN producers_json TEXT NOT NULL DEFAULT '[]'
                """
            )
        if "allowed_providers_json" not in worker_columns:
            self.connection.execute(
                """
                ALTER TABLE distributed_workers
                ADD COLUMN allowed_providers_json TEXT NOT NULL DEFAULT '[]'
                """
            )
        if "daily_egress_budget_bytes" not in worker_columns:
            self.connection.execute(
                """
                ALTER TABLE distributed_workers
                ADD COLUMN daily_egress_budget_bytes INTEGER
                    NOT NULL DEFAULT 0
                    CHECK(daily_egress_budget_bytes >= 0)
                """
            )
        if "protocol_version" not in worker_columns:
            self.connection.execute(
                """
                ALTER TABLE distributed_workers
                ADD COLUMN protocol_version TEXT NOT NULL
                    DEFAULT 'creeper-fabric-v1'
                """
            )
        if "edition_version" not in worker_columns:
            self.connection.execute(
                """
                ALTER TABLE distributed_workers
                ADD COLUMN edition_version TEXT NOT NULL
                    DEFAULT '0.1.0-dev'
                """
            )
        permit_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(distributed_provider_permits)"
            )
        }
        if "request_id" not in permit_columns:
            self.connection.execute(
                """
                ALTER TABLE distributed_provider_permits
                ADD COLUMN request_id TEXT
                """
            )
        self.connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_distributed_provider_permit_request
            ON distributed_provider_permits(
                provider, worker_id, task_id, generation, request_id
            )
            WHERE request_id IS NOT NULL
            """
        )
        budget_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(distributed_provider_budgets)"
            )
        }
        if "require_qualified_region" not in budget_columns:
            self.connection.execute(
                """
                ALTER TABLE distributed_provider_budgets
                ADD COLUMN require_qualified_region INTEGER
                    NOT NULL DEFAULT 1
                    CHECK(require_qualified_region IN (0, 1))
                """
            )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def consume_request_nonce(
        self,
        worker_id: str,
        nonce: str,
        *,
        retention_seconds: float = 600.0,
    ) -> bool:
        """Persistently consume a request nonce.

        Returns False for a replay. Nonces are retained longer than the normal
        HMAC timestamp-skew window so an Authority restart cannot reopen the
        replay window.
        """

        if not worker_id.strip() or not nonce.strip() or retention_seconds <= 0:
            raise ValueError("invalid nonce")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute(
                "DELETE FROM distributed_request_nonces WHERE seen_at < ?",
                (now - float(retention_seconds),),
            )
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO distributed_request_nonces(
                    worker_id, nonce, seen_at
                ) VALUES (?, ?, ?)
                """,
                (worker_id, nonce, now),
            )
            self.connection.commit()
            return cursor.rowcount == 1
        except Exception:
            self.connection.rollback()
            raise

    def register_worker(self, worker: WorkerDescriptor) -> None:
        if worker.protocol_version != FABRIC_PROTOCOL_VERSION:
            raise FabricProtocolMismatchError(
                f"authority={FABRIC_PROTOCOL_VERSION} "
                f"worker={worker.protocol_version}"
            )
        now = float(self.clock())
        self.connection.execute(
            """
            INSERT INTO distributed_workers(
                worker_id, runtime_class, region, architecture, memory_bytes,
                cpu_count, network_class, capabilities_json, producers_json,
                allowed_providers_json, daily_egress_budget_bytes,
                protocol_version, edition_version, last_heartbeat
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                runtime_class = excluded.runtime_class,
                region = excluded.region,
                architecture = excluded.architecture,
                memory_bytes = excluded.memory_bytes,
                cpu_count = excluded.cpu_count,
                network_class = excluded.network_class,
                capabilities_json = excluded.capabilities_json,
                producers_json = excluded.producers_json,
                allowed_providers_json = excluded.allowed_providers_json,
                daily_egress_budget_bytes =
                    excluded.daily_egress_budget_bytes,
                protocol_version = excluded.protocol_version,
                edition_version = excluded.edition_version,
                last_heartbeat = excluded.last_heartbeat
            """,
            (
                worker.worker_id,
                worker.runtime_class,
                worker.region,
                worker.architecture,
                int(worker.memory_bytes),
                int(worker.cpu_count),
                worker.network_class,
                _json(sorted(worker.capabilities)),
                _json(sorted(worker.producers)),
                _json(sorted(worker.allowed_providers)),
                int(worker.daily_egress_budget_bytes),
                worker.protocol_version,
                worker.edition_version,
                now,
            ),
        )
        self.connection.commit()

    def heartbeat(self, worker_id: str) -> None:
        now = float(self.clock())
        cursor = self.connection.execute(
            """
            UPDATE distributed_workers
            SET last_heartbeat = ?
            WHERE worker_id = ? AND revoked = 0
            """,
            (now, worker_id),
        )
        if cursor.rowcount != 1:
            self.connection.rollback()
            raise WorkerRejectedError(worker_id)
        self.connection.commit()

    def revoke_worker(self, worker_id: str) -> None:
        self.connection.execute(
            "UPDATE distributed_workers SET revoked = 1 WHERE worker_id = ?",
            (worker_id,),
        )
        self.connection.commit()

    def _worker_capabilities(self, worker_id: str) -> set[str]:
        row = self.connection.execute(
            """
            SELECT capabilities_json, revoked
            FROM distributed_workers
            WHERE worker_id = ?
            """,
            (worker_id,),
        ).fetchone()
        if row is None or int(row["revoked"]):
            raise WorkerRejectedError(worker_id)
        return set(json.loads(str(row["capabilities_json"])))

    def _worker_producers(self, worker_id: str) -> set[str]:
        row = self.connection.execute(
            """
            SELECT producers_json, revoked
            FROM distributed_workers
            WHERE worker_id = ?
            """,
            (worker_id,),
        ).fetchone()
        if row is None or int(row["revoked"]):
            raise WorkerRejectedError(worker_id)
        return set(json.loads(str(row["producers_json"])))

    def _worker_allowed_providers(self, worker_id: str) -> set[str]:
        row = self.connection.execute(
            """
            SELECT allowed_providers_json, revoked
            FROM distributed_workers
            WHERE worker_id = ?
            """,
            (worker_id,),
        ).fetchone()
        if row is None or int(row["revoked"]):
            raise WorkerRejectedError(worker_id)
        return set(json.loads(str(row["allowed_providers_json"])))

    @staticmethod
    def _egress_day_key(now: float) -> int:
        return int(float(now) // 86_400)

    def worker_egress_snapshot(
        self,
        worker_id: str,
        *,
        now: float | None = None,
    ) -> dict[str, int]:
        current = float(self.clock()) if now is None else float(now)
        worker = self.connection.execute(
            """
            SELECT daily_egress_budget_bytes, revoked
            FROM distributed_workers
            WHERE worker_id = ?
            """,
            (worker_id,),
        ).fetchone()
        if worker is None or int(worker["revoked"]):
            raise WorkerRejectedError(worker_id)
        day_key = self._egress_day_key(current)
        row = self.connection.execute(
            """
            SELECT response_bytes
            FROM distributed_worker_egress_daily
            WHERE worker_id = ? AND day_key = ?
            """,
            (worker_id, day_key),
        ).fetchone()
        used = 0 if row is None else int(row["response_bytes"])
        return {
            "day_key": day_key,
            "budget_bytes": int(worker["daily_egress_budget_bytes"]),
            "used_bytes": used,
        }

    def _worker_egress_available(
        self,
        worker_id: str,
        *,
        now: float,
    ) -> bool:
        snapshot = self.worker_egress_snapshot(worker_id, now=now)
        budget = int(snapshot["budget_bytes"])
        return budget == 0 or int(snapshot["used_bytes"]) < budget

    def _worker_runtime_class(self, worker_id: str) -> str:
        row = self.connection.execute(
            """
            SELECT runtime_class, revoked
            FROM distributed_workers
            WHERE worker_id = ?
            """,
            (worker_id,),
        ).fetchone()
        if row is None or int(row["revoked"]):
            raise WorkerRejectedError(worker_id)
        return str(row["runtime_class"])

    @staticmethod
    def _work_runtime_eligible(
        row: sqlite3.Row,
        *,
        runtime_class: str,
    ) -> bool:
        if runtime_class != "cloudflare_worker":
            return True
        if str(row["producer"]) != "ThinHistoricalQueryProducer":
            return False
        if str(row["task_class"]) != TaskClass.HOST_BATCH.value:
            return False
        coverage = json.loads(str(row["coverage_json"]))
        if coverage.get("thin_eligible") is not True:
            return False
        try:
            requests = int(coverage["max_provider_requests"])
            estimated_bytes = int(coverage["estimated_response_bytes"])
            year_from = int(coverage["year_from"])
            year_to = int(coverage["year_to"])
        except (KeyError, TypeError, ValueError):
            return False
        return (
            requests <= FABRIC_THIN_MAX_PROVIDER_REQUESTS
            and estimated_bytes <= FABRIC_THIN_MAX_ESTIMATED_RESPONSE_BYTES
            and requests >= 1
            and estimated_bytes >= 1
            and 1996 <= year_from == year_to <= 2001
        )

    def _worker_region(self, worker_id: str) -> str:
        row = self.connection.execute(
            """
            SELECT region, revoked FROM distributed_workers
            WHERE worker_id = ?
            """,
            (worker_id,),
        ).fetchone()
        if row is None or int(row["revoked"]):
            raise WorkerRejectedError(worker_id)
        return str(row["region"])

    def _work_region_eligible(
        self,
        row: sqlite3.Row,
        *,
        worker_region: str,
        worker_runtime_class: str,
        allowed_providers: set[str],
    ) -> bool:
        is_probe = str(row["task_class"]) == TaskClass.PROBE.value
        is_cloudflare_thin = (
            worker_runtime_class == "cloudflare_worker"
            and str(row["producer"]) == "ThinHistoricalQueryProducer"
            and str(row["task_class"]) == TaskClass.HOST_BATCH.value
        )
        coverage = json.loads(str(row["coverage_json"]))
        if is_probe:
            target_region = str(coverage.get("target_region", "")).strip()
            if target_region and target_region != worker_region:
                return False
        raw_providers = coverage.get("providers")
        if raw_providers is None:
            one = coverage.get("provider")
            providers = [one] if isinstance(one, str) and one.strip() else []
        elif isinstance(raw_providers, list):
            providers = [
                str(value)
                for value in raw_providers
                if isinstance(value, str) and value.strip()
            ]
        else:
            raise ValueError("coverage.providers must be a list of provider names")
        for provider in providers:
            if provider not in allowed_providers:
                return False
            if is_probe or is_cloudflare_thin:
                continue
            budget = self.connection.execute(
                """
                SELECT require_qualified_region
                FROM distributed_provider_budgets
                WHERE provider = ?
                """,
                (provider,),
            ).fetchone()
            if budget is None:
                return False
            if not int(budget["require_qualified_region"]):
                continue
            region = self.connection.execute(
                """
                SELECT state FROM distributed_provider_regions
                WHERE provider = ? AND region = ?
                """,
                (provider, worker_region),
            ).fetchone()
            if region is None or str(region["state"]) != "QUALIFIED":
                return False
        return True

    @staticmethod
    def _work_from_row(row: sqlite3.Row) -> WorkDefinition:
        return WorkDefinition(
            producer=str(row["producer"]),
            task_class=TaskClass(str(row["task_class"])),
            input_identity=str(row["input_identity"]),
            coverage=json.loads(str(row["coverage_json"])),
            partition=str(row["partition_key"]),
            algorithm_version=str(row["algorithm_version"]),
            required_capabilities=tuple(
                json.loads(str(row["required_capabilities_json"]))
            ),
            priority=float(row["priority"]),
        )

    def admit_work(self, work: WorkDefinition) -> str:
        """Admit logical work exactly once and return its deterministic task id."""

        now = float(self.clock())
        task_id = work.work_key
        self.connection.execute(
            """
            INSERT OR IGNORE INTO distributed_work(
                task_id, work_key, producer, task_class, input_identity,
                coverage_json, partition_key, algorithm_version,
                required_capabilities_json, priority, state, created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'READY', ?, ?)
            """,
            (
                task_id,
                work.work_key,
                work.producer,
                work.task_class.value,
                work.input_identity,
                _json(dict(work.coverage)),
                work.partition,
                work.algorithm_version,
                _json(list(work.required_capabilities)),
                float(work.priority),
                now,
                now,
            ),
        )
        row = self.connection.execute(
            "SELECT * FROM distributed_work WHERE work_key = ?",
            (work.work_key,),
        ).fetchone()
        assert row is not None
        existing = self._work_from_row(row)
        if (
            existing.producer != work.producer
            or existing.task_class != work.task_class
            or existing.input_identity != work.input_identity
            or dict(existing.coverage) != dict(work.coverage)
            or existing.partition != work.partition
            or existing.algorithm_version != work.algorithm_version
            or tuple(existing.required_capabilities)
            != tuple(work.required_capabilities)
        ):
            self.connection.rollback()
            raise ValueError("existing work_key has incompatible work definition")
        if float(work.priority) > float(row["priority"]):
            self.connection.execute(
                """
                UPDATE distributed_work
                SET priority = ?, updated_at = ?
                WHERE work_key = ?
                """,
                (float(work.priority), now, work.work_key),
            )
        self.connection.commit()
        return task_id

    def admit_search_slice(
        self,
        *,
        campaign: SearchCampaign,
        seed: int,
        slot_start: int,
        slot_count: int,
        search_endpoint: str,
        query_param: str = "q",
        provider: str = "web_search",
        max_response_bytes: int = 1024 * 1024,
        max_links_per_query: int = 128,
        priority: float = 0.0,
        algorithm_version: str = "fabric-seeded-search-v1",
    ) -> str:
        endpoint = canonical_http_url(search_endpoint)
        query_param = query_param.strip()
        provider = provider.strip()
        if (
            int(seed) < 0
            or int(slot_start) < 0
            or not 1 <= int(slot_count) <= 16
            or not query_param
            or not provider
            or int(max_response_bytes) < 4096
            or int(max_links_per_query) < 1
        ):
            raise ValueError("invalid seeded search admission")
        work = WorkDefinition(
            producer="SeededSearchProducer",
            task_class=TaskClass.SEARCH_SLICE,
            input_identity=campaign.campaign_id,
            coverage={
                "campaign_id": campaign.campaign_id,
                "campaign": campaign.as_dict(),
                "seed": int(seed),
                "slot_start": int(slot_start),
                "slot_count": int(slot_count),
                "search_endpoint": endpoint,
                "query_param": query_param,
                "provider": provider,
                "max_response_bytes": int(max_response_bytes),
                "max_links_per_query": int(max_links_per_query),
            },
            partition=f"{int(seed)}:{int(slot_start)}:{int(slot_count)}",
            algorithm_version=algorithm_version,
            required_capabilities=("SEARCH_QUERY",),
            priority=float(priority),
        )
        return self.admit_work(work)

    def admit_historical_exploration_work(
        self,
        *,
        url: str,
        archive_providers: tuple[str, ...],
        seed: int = 0,
        priority: float = 0.0,
        algorithm_version: str = "fabric-historical-crawler-v1",
    ) -> str:
        canonical_url = canonical_http_url(url)
        providers = tuple(
            dict.fromkeys(("web_discovery", *archive_providers))
        )
        if (
            not archive_providers
            or any(not value.strip() for value in providers)
            or int(seed) < 0
        ):
            raise ValueError("invalid historical exploration admission")
        work = WorkDefinition(
            producer="HistoricalCrawlerProducer",
            task_class=TaskClass.SOURCE_PAGE,
            input_identity=canonical_url,
            coverage={
                "url": canonical_url,
                "seed": int(seed),
                "providers": list(providers),
                "archive_providers": list(archive_providers),
            },
            partition=f"crawl:{int(seed)}",
            algorithm_version=algorithm_version,
            required_capabilities=("WEB_DISCOVERY", "ONLINE_QUERY"),
            priority=float(priority),
        )
        return self.admit_work(work)

    def admit_seeded_exploration(
        self,
        *,
        campaign: SearchCampaign,
        seed: int,
        slot_start: int,
        slot_count: int,
        search_endpoint: str,
        archive_providers: tuple[str, ...],
        query_param: str = "q",
        search_provider: str = "web_search",
        priority: float = 0.0,
        algorithm_version: str = "fabric-seeded-exploration-v1",
    ) -> str:
        endpoint = canonical_http_url(search_endpoint)
        search_provider = search_provider.strip()
        query_param = query_param.strip()
        providers = tuple(
            dict.fromkeys(
                (search_provider, "web_discovery", *archive_providers)
            )
        )
        if (
            int(seed) < 0
            or int(slot_start) < 0
            or not 1 <= int(slot_count) <= 8
            or not archive_providers
            or not search_provider
            or not query_param
            or any(not value.strip() for value in providers)
        ):
            raise ValueError("invalid seeded exploration admission")
        work = WorkDefinition(
            producer="SeededExplorationProducer",
            task_class=TaskClass.SEARCH_SLICE,
            input_identity=campaign.campaign_id,
            coverage={
                "campaign_id": campaign.campaign_id,
                "campaign": campaign.as_dict(),
                "seed": int(seed),
                "slot_start": int(slot_start),
                "slot_count": int(slot_count),
                "search_endpoint": endpoint,
                "query_param": query_param,
                "search_provider": search_provider,
                "providers": list(providers),
                "archive_providers": list(archive_providers),
            },
            partition=f"explore:{int(seed)}:{int(slot_start)}:{int(slot_count)}",
            algorithm_version=algorithm_version,
            required_capabilities=(
                "SEARCH_QUERY",
                "WEB_DISCOVERY",
                "ONLINE_QUERY",
            ),
            priority=float(priority),
        )
        return self.admit_work(work)

    def admit_source_page_work(
        self,
        *,
        url: str,
        max_bytes: int = 2 * 1024 * 1024,
        max_links: int = 512,
        priority: float = 0.0,
        algorithm_version: str = "fabric-source-discovery-v1",
    ) -> str:
        canonical_url = canonical_http_url(url)
        if max_bytes < 1024 or max_links < 1:
            raise ValueError("invalid source page admission limits")
        work = WorkDefinition(
            producer="SourceDiscoveryProducer",
            task_class=TaskClass.SOURCE_PAGE,
            input_identity=canonical_url,
            coverage={
                "url": canonical_url,
                "provider": "web_discovery",
                "max_bytes": int(max_bytes),
                "max_links": int(max_links),
            },
            partition="page",
            algorithm_version=algorithm_version,
            required_capabilities=("WEB_DISCOVERY",),
            priority=float(priority),
        )
        return self.admit_work(work)

    def promote_source_candidates(
        self,
        *,
        limit: int = 64,
        include_source_pages: bool = False,
    ) -> list[dict[str, str]]:
        """Deterministically promote safe discovery candidates into work.

        Only CDX/CDXJ artifacts are automatically eligible for direct-year
        SOURCE_SHARD work. Other artifact families are held for a future
        reviewed adapter. Generic pages remain DISCOVERED unless recursion is
        explicitly enabled.
        """

        if limit < 1:
            raise ValueError("promotion limit must be positive")
        rows = self.connection.execute(
            """
            SELECT * FROM distributed_source_candidates
            WHERE state = 'DISCOVERED'
            ORDER BY
                CASE candidate_type
                    WHEN 'bulk_artifact' THEN 0
                    ELSE 1
                END,
                discovery_count DESC,
                canonical_url
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        promoted: list[dict[str, str]] = []

        for row in rows:
            candidate_id = str(row["candidate_id"])
            candidate_type = str(row["candidate_type"])
            parser_kind = str(row["parser_kind"])
            canonical_url = str(row["canonical_url"])
            task_id: str | None = None
            state = "DISCOVERED"
            error: str | None = None
            try:
                if candidate_type == "bulk_artifact":
                    if parser_kind in {"cdx", "cdxj"}:
                        task_id = self.admit_bulk_source_work(
                            source_id=candidate_id,
                            source_locator=canonical_url,
                            partition="all",
                        )
                        state = "ADMITTED"
                    else:
                        state = "HELD_UNSUPPORTED"
                        error = (
                            "bulk parser family is not yet authorized for "
                            "distributed direct-year production"
                        )
                elif candidate_type == "source_page" and include_source_pages:
                    task_id = self.admit_source_page_work(url=canonical_url)
                    state = "ADMITTED"
                else:
                    continue
            except Exception as exc:
                state = "ERROR"
                error = f"{type(exc).__name__}: {exc}"

            self.connection.execute(
                """
                UPDATE distributed_source_candidates
                SET state = ?, admitted_task_id = ?, last_error = ?,
                    last_seen_at = ?
                WHERE candidate_id = ?
                """,
                (
                    state,
                    task_id,
                    error,
                    float(self.clock()),
                    candidate_id,
                ),
            )
            self.connection.commit()
            promoted.append(
                {
                    "candidate_id": candidate_id,
                    "state": state,
                    "task_id": "" if task_id is None else task_id,
                }
            )
        return promoted

    def host_candidate_count(self) -> int:
        return int(
            self.connection.execute(
                "SELECT COUNT(*) FROM distributed_host_candidates"
            ).fetchone()[0]
        )

    def host_candidate_rows(self) -> list[dict[str, object]]:
        return [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT * FROM distributed_host_candidates
                ORDER BY discovery_count DESC, hostname
                """
            ).fetchall()
        ]

    def promote_host_candidates(
        self,
        *,
        physical_providers: tuple[str, ...],
        coverage_provider: str,
        resolver_version: str,
        limit: int = 256,
        priority: float = 0.0,
    ) -> list[dict[str, object]]:
        if (
            limit < 1
            or not physical_providers
            or not coverage_provider.strip()
            or not resolver_version.strip()
        ):
            raise ValueError("invalid host candidate promotion configuration")
        rows = self.connection.execute(
            """
            SELECT * FROM distributed_host_candidates
            WHERE state = 'DISCOVERED'
            ORDER BY discovery_count DESC, first_seen_at, hostname
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        promoted: list[dict[str, object]] = []
        for row in rows:
            hostname = str(row["hostname"])
            state = "DISCOVERED"
            error: str | None = None
            task_ids: tuple[str, ...] = ()
            try:
                task_ids = self.admit_host_resolution_work(
                    hostname=hostname,
                    physical_providers=physical_providers,
                    coverage_provider=coverage_provider,
                    resolver_version=resolver_version,
                    year_from=1996,
                    year_to=2001,
                    priority=priority,
                )
                state = "ADMITTED" if task_ids else "COVERED"
            except Exception as exc:
                state = "ERROR"
                error = f"{type(exc).__name__}: {exc}"

            self.connection.execute(
                """
                UPDATE distributed_host_candidates
                SET state = ?, last_error = ?, last_seen_at = ?
                WHERE candidate_id = ?
                """,
                (
                    state,
                    error,
                    float(self.clock()),
                    str(row["candidate_id"]),
                ),
            )
            self.connection.commit()
            promoted.append(
                {
                    "hostname": hostname,
                    "state": state,
                    "task_ids": list(task_ids),
                }
            )
        return promoted

    def source_candidate_count(self) -> int:
        return int(
            self.connection.execute(
                "SELECT COUNT(*) FROM distributed_source_candidates"
            ).fetchone()[0]
        )

    def source_candidate_rows(self) -> list[dict[str, object]]:
        return [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT * FROM distributed_source_candidates
                ORDER BY candidate_type, canonical_url
                """
            ).fetchall()
        ]

    def admit_bulk_source_work(
        self,
        *,
        source_id: str,
        source_locator: str,
        partition: str,
        cursor_end: str | None = None,
        priority: float = 0.0,
        algorithm_version: str = "distributed-bulk-v1",
    ) -> str:
        """Admit one direct-year structured source shard.

        The Authority, not the worker, decides whether the source parser is
        allowed to create annual evidence.
        """

        source_id = source_id.strip()
        source_locator = source_locator.strip()
        partition = partition.strip()
        if not source_id or not source_locator or not partition:
            raise ValueError("bulk source identity, locator and partition are required")
        contract = resolve_source_evidence_contract(source_locator)
        if not contract.grants_direct_web_year:
            raise ValueError(
                "bulk direct-year admission requires a DIRECT_WEB_YEAR contract"
            )
        coverage: dict[str, object] = {
            "source_id": source_id,
            "source_locator": source_locator,
            "evidence_contract_id": contract.contract_id,
            "evidence_contract_version": contract.policy_version,
            "parser_kind": contract.parser_kind,
        }
        if cursor_end is not None:
            value = str(cursor_end).strip()
            if not value:
                raise ValueError("cursor_end must be non-empty when supplied")
            coverage["cursor_end"] = value
        work = WorkDefinition(
            producer="BulkHistoricalIndexProducer",
            task_class=TaskClass.SOURCE_SHARD,
            input_identity=source_id,
            coverage=coverage,
            partition=partition,
            algorithm_version=algorithm_version,
            required_capabilities=("STREAMING_BULK",),
            priority=float(priority),
        )
        return self.admit_work(work)

    def admit_region_probe_work(
        self,
        *,
        provider: str,
        probe_hostname: str,
        target_region: str,
        year: int = 2001,
        samples: int = 3,
        priority: float = 100.0,
        algorithm_version: str = "fabric-region-probe-v1",
    ) -> str:
        provider = provider.strip()
        hostname = normalize_official(probe_hostname)
        target_region = target_region.strip()
        year = int(year)
        samples = int(samples)
        if (
            not provider
            or hostname is None
            or not target_region
            or year not in YEAR_BITS
            or not 1 <= samples <= 10
        ):
            raise ValueError("invalid provider region probe admission")
        work = WorkDefinition(
            producer="RegionProbeProducer",
            task_class=TaskClass.PROBE,
            input_identity=provider,
            coverage={
                "provider": provider,
                "probe_hostname": hostname,
                "target_region": target_region,
                "year": year,
                "samples": samples,
            },
            partition=f"{provider}:{target_region}:{hostname}:{year}",
            algorithm_version=algorithm_version,
            required_capabilities=("ONLINE_QUERY",),
            priority=float(priority),
        )
        return self.admit_work(work)

    def admit_thin_host_probe(
        self,
        *,
        hostname: str,
        provider: str,
        year: int,
        estimated_response_bytes: int = 64 * 1024,
        priority: float = 0.0,
        algorithm_version: str = "fabric-thin-positive-v1",
    ) -> str:
        """Admit one single-request positive-only exact-year probe."""

        normalized = normalize_official(hostname)
        provider = provider.strip()
        year = int(year)
        estimated_response_bytes = int(estimated_response_bytes)
        if (
            normalized is None
            or not provider
            or year not in YEAR_BITS
            or estimated_response_bytes < 1
            or estimated_response_bytes > FABRIC_THIN_MAX_ESTIMATED_RESPONSE_BYTES
        ):
            raise ValueError("invalid thin host probe")
        work = WorkDefinition(
            producer="ThinHistoricalQueryProducer",
            task_class=TaskClass.HOST_BATCH,
            input_identity=normalized,
            coverage={
                "scope": "THIN_POSITIVE_PROBE",
                "provider": provider,
                "year_from": year,
                "year_to": year,
                "thin_eligible": True,
                "max_provider_requests": FABRIC_THIN_MAX_PROVIDER_REQUESTS,
                "estimated_response_bytes": estimated_response_bytes,
            },
            partition=f"{provider}:{year}",
            algorithm_version=algorithm_version,
            required_capabilities=("THIN_QUERY",),
            priority=float(priority),
        )
        return self.admit_work(work)

    def admit_host_resolution_work(
        self,
        *,
        hostname: str,
        physical_providers: tuple[str, ...],
        coverage_provider: str,
        resolver_version: str,
        year_from: int = 1996,
        year_to: int = 2001,
        producer: str = "HistoricalQueryProducer",
        algorithm_version: str | None = None,
        required_capabilities: tuple[str, ...] = ("ONLINE_QUERY",),
        priority: float = 0.0,
    ) -> tuple[str, ...]:
        """Admit only uncovered HOST resolution intervals.

        Coverage is keyed by the logical provider-set identity rather than a
        single physical archive.  Physical providers remain in the work
        payload so claim-time region qualification can require every provider
        that the resolver may contact.
        """

        normalized = normalize_official(hostname)
        if (
            normalized is None
            or not physical_providers
            or any(not provider.strip() for provider in physical_providers)
            or not coverage_provider.strip()
            or not resolver_version.strip()
            or not producer.strip()
        ):
            raise ValueError("invalid host resolution admission")
        providers = tuple(dict.fromkeys(physical_providers))
        uncovered = self.uncovered_resolution_intervals(
            hostname=normalized,
            provider=coverage_provider,
            scope="HOST",
            resolver_version=resolver_version,
            year_from=year_from,
            year_to=year_to,
        )
        missing_mask = 0
        for missing_from, missing_to in uncovered:
            missing_mask |= self._year_interval_mask(
                missing_from,
                missing_to,
            )

        if self.baseline_index is not None:
            missing_mask &= ~int(self.baseline_index.year_mask(normalized))

        accepted_mask = 0
        for row in self.connection.execute(
            """
            SELECT year FROM distributed_host_year_ledger
            WHERE hostname = ?
            """,
            (normalized,),
        ).fetchall():
            year = int(row["year"])
            if year in YEAR_BITS:
                accepted_mask |= YEAR_BITS[year]
        missing_mask &= ~accepted_mask

        missing: list[tuple[int, int]] = []
        start: int | None = None
        previous: int | None = None
        for year in range(int(year_from), int(year_to) + 1):
            if not (missing_mask & YEAR_BITS[year]):
                if start is not None and previous is not None:
                    missing.append((start, previous))
                    start = previous = None
                continue
            if start is None:
                start = year
            previous = year
        if start is not None and previous is not None:
            missing.append((start, previous))

        admitted: list[str] = []
        version = (
            resolver_version
            if algorithm_version is None
            else str(algorithm_version)
        )
        for missing_from, missing_to in missing:
            work = WorkDefinition(
                producer=producer,
                task_class=TaskClass.HOST_BATCH,
                input_identity=normalized,
                coverage={
                    "scope": "HOST",
                    "year_from": missing_from,
                    "year_to": missing_to,
                    "providers": list(providers),
                    "coverage_provider": coverage_provider,
                    "resolver_version": resolver_version,
                },
                partition=f"{missing_from}-{missing_to}",
                algorithm_version=version,
                required_capabilities=required_capabilities,
                priority=float(priority),
            )
            admitted.append(self.admit_work(work))
        return tuple(admitted)

    def claim_work(
        self,
        worker_id: str,
        *,
        lease_seconds: float = 300.0,
    ) -> TaskLease | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = float(self.clock())
        capabilities = self._worker_capabilities(worker_id)
        producers = self._worker_producers(worker_id)
        allowed_providers = self._worker_allowed_providers(worker_id)
        worker_region = self._worker_region(worker_id)
        runtime_class = self._worker_runtime_class(worker_id)
        egress_available = self._worker_egress_available(
            worker_id,
            now=now,
        )
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            chosen = None
            offset = 0
            while chosen is None:
                rows = self.connection.execute(
                    """
                    SELECT *
                    FROM distributed_work
                    WHERE state = 'READY'
                       OR (
                            state = 'LEASED'
                            AND lease_deadline IS NOT NULL
                            AND lease_deadline <= ?
                       )
                    ORDER BY priority DESC, created_at, work_key
                    LIMIT 256 OFFSET ?
                    """,
                    (now, offset),
                ).fetchall()
                if not rows:
                    break
                for row in rows:
                    required = set(
                        json.loads(str(row["required_capabilities_json"]))
                    )
                    if not required.issubset(capabilities):
                        continue
                    if producers and str(row["producer"]) not in producers:
                        continue
                    if not self._work_runtime_eligible(
                        row,
                        runtime_class=runtime_class,
                    ):
                        continue
                    coverage = json.loads(str(row["coverage_json"]))
                    has_provider = bool(
                        coverage.get("provider")
                        or coverage.get("providers")
                    )
                    if has_provider and not egress_available:
                        continue
                    if not self._work_region_eligible(
                        row,
                        worker_region=worker_region,
                        worker_runtime_class=runtime_class,
                        allowed_providers=allowed_providers,
                    ):
                        continue
                    chosen = row
                    break
                offset += len(rows)
            if chosen is None:
                self.connection.commit()
                return None

            generation = int(chosen["lease_generation"]) + 1
            attempt = int(chosen["attempt"]) + 1
            deadline = now + float(lease_seconds)
            self.connection.execute(
                """
                UPDATE distributed_work
                SET state = 'LEASED',
                    lease_owner = ?,
                    lease_generation = ?,
                    lease_deadline = ?,
                    attempt = ?,
                    updated_at = ?
                WHERE task_id = ?
                """,
                (
                    worker_id,
                    generation,
                    deadline,
                    attempt,
                    now,
                    str(chosen["task_id"]),
                ),
            )
            self.connection.commit()
            return TaskLease(
                task_id=str(chosen["task_id"]),
                work_key=str(chosen["work_key"]),
                worker_id=worker_id,
                generation=generation,
                lease_deadline=deadline,
                attempt=attempt,
                work=self._work_from_row(chosen),
                cursor=(
                    None
                    if chosen["cursor"] is None
                    else str(chosen["cursor"])
                ),
                next_sequence_no=int(chosen["next_sequence_no"]),
            )
        except Exception:
            self.connection.rollback()
            raise

    def _assert_active_lease(
        self,
        task_id: str,
        worker_id: str,
        generation: int,
        *,
        now: float,
    ) -> sqlite3.Row:
        worker = self.connection.execute(
            """
            SELECT revoked FROM distributed_workers
            WHERE worker_id = ?
            """,
            (worker_id,),
        ).fetchone()
        if worker is None or int(worker["revoked"]):
            raise WorkerRejectedError(worker_id)

        row = self.connection.execute(
            "SELECT * FROM distributed_work WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if (
            row is None
            or str(row["state"]) != "LEASED"
            or str(row["lease_owner"]) != worker_id
            or int(row["lease_generation"]) != int(generation)
            or row["lease_deadline"] is None
            or float(row["lease_deadline"]) <= now
        ):
            raise StaleLeaseError(
                f"stale lease task={task_id} worker={worker_id} "
                f"generation={generation}"
            )
        return row

    def renew_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        generation: int,
        lease_seconds: float = 300.0,
    ) -> TaskLease:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )
            deadline = now + float(lease_seconds)
            self.connection.execute(
                """
                UPDATE distributed_work
                SET lease_deadline = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (deadline, now, task_id),
            )
            self.connection.commit()
            return TaskLease(
                task_id=task_id,
                work_key=str(row["work_key"]),
                worker_id=worker_id,
                generation=int(generation),
                lease_deadline=deadline,
                attempt=int(row["attempt"]),
                work=self._work_from_row(row),
                cursor=None if row["cursor"] is None else str(row["cursor"]),
                next_sequence_no=int(row["next_sequence_no"]),
            )
        except Exception:
            self.connection.rollback()
            raise

    @staticmethod
    def _host_candidate_from_result(
        item: Mapping[str, object],
    ) -> tuple[str, str, str, str] | None:
        if str(item.get("kind", "")) != "HOST_CANDIDATE":
            return None
        raw_hostname = item.get("hostname")
        if not isinstance(raw_hostname, str):
            raise ValueError("HOST_CANDIDATE hostname must be a string")
        hostname = normalize_official(raw_hostname)
        source = str(item.get("source", "")).strip()
        locator = str(item.get("locator", "")).strip()
        if hostname is None or not source or not locator:
            raise ValueError("invalid HOST_CANDIDATE")
        return host_id(hostname), hostname, source, locator

    def _commit_host_candidates(
        self,
        task_id: str,
        results: tuple[Mapping[str, object], ...],
        *,
        now: float,
    ) -> None:
        unique: dict[str, tuple[str, str, str]] = {}
        for item in results:
            parsed = self._host_candidate_from_result(item)
            if parsed is None:
                continue
            candidate_id, hostname, source, locator = parsed
            unique[candidate_id] = (hostname, source, locator)

        for candidate_id, (hostname, source, locator) in unique.items():
            self.connection.execute(
                """
                INSERT INTO distributed_host_candidates(
                    candidate_id, hostname, first_task_id, first_source,
                    first_locator, discovery_count, state,
                    first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, 1, 'DISCOVERED', ?, ?)
                ON CONFLICT(candidate_id) DO UPDATE SET
                    discovery_count =
                        distributed_host_candidates.discovery_count + 1,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    candidate_id,
                    hostname,
                    task_id,
                    source,
                    locator,
                    now,
                    now,
                ),
            )

    @staticmethod
    def _source_candidate_from_result(
        item: Mapping[str, object],
    ) -> tuple[str, str, str, str, str] | None:
        if str(item.get("kind", "")) != "SOURCE_CANDIDATE":
            return None
        canonical_url = canonical_http_url(str(item.get("url", "")))
        candidate_type = str(item.get("candidate_type", "")).strip()
        parser_kind = str(item.get("parser_kind", "")).strip()
        referrer_url = canonical_http_url(str(item.get("referrer_url", "")))
        if candidate_type not in {"bulk_artifact", "source_page"}:
            raise ValueError("invalid source candidate type")
        if parser_kind not in {
            "",
            "cdx",
            "cdxj",
            "warc_arc",
            "jsonl",
            "delimited",
            "lines",
        }:
            raise ValueError("invalid source candidate parser kind")
        if candidate_type == "bulk_artifact" and not parser_kind:
            raise ValueError("bulk artifact candidate requires parser_kind")
        candidate_id = source_candidate_id(canonical_url)
        return (
            candidate_id,
            canonical_url,
            candidate_type,
            parser_kind,
            referrer_url,
        )

    def _commit_source_candidates(
        self,
        task_id: str,
        results: tuple[Mapping[str, object], ...],
        *,
        now: float,
    ) -> None:
        unique: dict[str, tuple[str, str, str, str]] = {}
        for item in results:
            parsed = self._source_candidate_from_result(item)
            if parsed is None:
                continue
            candidate_id, canonical_url, candidate_type, parser_kind, referrer = parsed
            unique[candidate_id] = (
                canonical_url,
                candidate_type,
                parser_kind,
                referrer,
            )

        for candidate_id, (
            canonical_url,
            candidate_type,
            parser_kind,
            referrer,
        ) in unique.items():
            self.connection.execute(
                """
                INSERT INTO distributed_source_candidates(
                    candidate_id, canonical_url, candidate_type, parser_kind,
                    first_task_id, first_referrer_url, discovery_count,
                    state, first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, 'DISCOVERED', ?, ?)
                ON CONFLICT(candidate_id) DO UPDATE SET
                    candidate_type = CASE
                        WHEN excluded.candidate_type = 'bulk_artifact'
                            THEN excluded.candidate_type
                        ELSE distributed_source_candidates.candidate_type
                    END,
                    parser_kind = CASE
                        WHEN excluded.parser_kind <> ''
                            THEN excluded.parser_kind
                        ELSE distributed_source_candidates.parser_kind
                    END,
                    discovery_count =
                        distributed_source_candidates.discovery_count + 1,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    candidate_id,
                    canonical_url,
                    candidate_type,
                    parser_kind,
                    task_id,
                    referrer,
                    now,
                    now,
                ),
            )

    @staticmethod
    def _batch_payload(batch: ResultBatch) -> str:
        return _json(
            {
                # Generation is a fencing credential, not part of BatchID or
                # logical batch content. A new lease generation must therefore
                # receive ALREADY_COMMITTED when it replays an identical
                # task/sequence batch after an ACK was lost.
                "task_id": batch.task_id,
                "sequence_no": int(batch.sequence_no),
                "results": [dict(item) for item in batch.results],
                "cursor_after": batch.cursor_after,
            }
        )

    def commit_result_batch(
        self,
        batch: ResultBatch,
        *,
        worker_id: str,
    ) -> bool:
        """Commit a batch once.

        Returns True for the first logical commit and False for an exact replay.
        A replay with the same BatchID but different contents is rejected.
        """

        now = float(self.clock())
        payload = self._batch_payload(batch)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self.connection.execute(
                """
                SELECT payload_json, payload_digest
                FROM distributed_result_batches
                WHERE batch_id = ?
                """,
                (batch.batch_id,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["payload_digest"]) != digest
                    or str(existing["payload_json"]) != payload
                ):
                    raise BatchConflictError(batch.batch_id)
                self.connection.commit()
                return False

            task_row = self._assert_active_lease(
                batch.task_id,
                worker_id,
                batch.generation,
                now=now,
            )
            if (
                str(task_row["producer"]) in FABRIC_EVIDENCE_ONLY_PRODUCERS
                and batch.results
            ):
                raise ValueError(
                    "evidence-only exploration tasks may not export raw results"
                )
            expected_sequence = int(task_row["next_sequence_no"])
            if int(batch.sequence_no) != expected_sequence:
                raise BatchSequenceError(
                    f"task={batch.task_id} expected={expected_sequence} "
                    f"received={batch.sequence_no}"
                )
            self.connection.execute(
                """
                INSERT INTO distributed_result_batches(
                    batch_id, task_id, generation, sequence_no, payload_json,
                    payload_digest, cursor_after, committed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch.batch_id,
                    batch.task_id,
                    int(batch.generation),
                    int(batch.sequence_no),
                    payload,
                    digest,
                    batch.cursor_after,
                    now,
                ),
            )
            self._commit_source_candidates(
                batch.task_id,
                batch.results,
                now=now,
            )
            self._commit_host_candidates(
                batch.task_id,
                batch.results,
                now=now,
            )
            self.connection.execute(
                """
                UPDATE distributed_work
                SET cursor = COALESCE(?, cursor),
                    next_sequence_no = ?,
                    updated_at = ?
                WHERE task_id = ?
                """,
                (
                    batch.cursor_after,
                    expected_sequence + 1,
                    now,
                    batch.task_id,
                ),
            )
            self.connection.commit()
            return True
        except Exception:
            self.connection.rollback()
            raise

    def finish_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        generation: int,
    ) -> None:
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )
            self.connection.execute(
                """
                UPDATE distributed_work
                SET state = 'COMPLETE',
                    lease_owner = NULL,
                    lease_deadline = NULL,
                    updated_at = ?
                WHERE task_id = ?
                """,
                (now, task_id),
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def fail_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        generation: int,
        error: str,
        retryable: bool = True,
    ) -> None:
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )
            self.connection.execute(
                """
                UPDATE distributed_work
                SET state = ?,
                    lease_owner = NULL,
                    lease_deadline = NULL,
                    last_error = ?,
                    updated_at = ?
                WHERE task_id = ?
                """,
                ("READY" if retryable else "FAILED", error, now, task_id),
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    @staticmethod
    def _year_interval_mask(year_from: int, year_to: int) -> int:
        if not 1996 <= int(year_from) <= int(year_to) <= 2001:
            raise ValueError("coverage years must be within 1996-2001")
        mask = 0
        for year in range(int(year_from), int(year_to) + 1):
            mask |= YEAR_BITS[year]
        return mask

    def record_complete_resolution_coverage(
        self,
        task_id: str,
        *,
        worker_id: str,
        generation: int,
        hostname: str,
        provider: str,
        scope: str,
        resolver_version: str,
        year_from: int,
        year_to: int,
    ) -> int:
        """Idempotently mark an interval as completely resolved."""

        normalized = normalize_official(hostname)
        if (
            normalized is None
            or not provider.strip()
            or not scope.strip()
            or not resolver_version.strip()
        ):
            raise ValueError("invalid resolution coverage identity")
        interval_mask = self._year_interval_mask(year_from, year_to)
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )
            row = self.connection.execute(
                """
                SELECT year_mask FROM distributed_resolution_coverage
                WHERE hostname = ? AND provider = ? AND scope = ?
                  AND resolver_version = ?
                """,
                (normalized, provider, scope, resolver_version),
            ).fetchone()
            old_mask = 0 if row is None else int(row["year_mask"])
            new_mask = old_mask | interval_mask
            self.connection.execute(
                """
                INSERT INTO distributed_resolution_coverage(
                    hostname, provider, scope, resolver_version,
                    year_mask, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(hostname, provider, scope, resolver_version)
                DO UPDATE SET
                    year_mask = excluded.year_mask,
                    updated_at = excluded.updated_at
                """,
                (
                    normalized,
                    provider,
                    scope,
                    resolver_version,
                    new_mask,
                    now,
                ),
            )
            self.connection.commit()
            return new_mask
        except Exception:
            self.connection.rollback()
            raise

    def uncovered_resolution_intervals(
        self,
        *,
        hostname: str,
        provider: str,
        scope: str,
        resolver_version: str,
        year_from: int,
        year_to: int,
    ) -> tuple[tuple[int, int], ...]:
        """Subtract exact durable coverage and return contiguous missing ranges."""

        normalized = normalize_official(hostname)
        if (
            normalized is None
            or not provider.strip()
            or not scope.strip()
            or not resolver_version.strip()
        ):
            raise ValueError("invalid resolution coverage identity")
        target_mask = self._year_interval_mask(year_from, year_to)
        row = self.connection.execute(
            """
            SELECT year_mask FROM distributed_resolution_coverage
            WHERE hostname = ? AND provider = ? AND scope = ?
              AND resolver_version = ?
            """,
            (normalized, provider, scope, resolver_version),
        ).fetchone()
        covered = 0 if row is None else int(row["year_mask"])
        missing = target_mask & ~covered
        intervals: list[tuple[int, int]] = []
        start: int | None = None
        previous: int | None = None
        for year in range(int(year_from), int(year_to) + 1):
            if not (missing & YEAR_BITS[year]):
                if start is not None and previous is not None:
                    intervals.append((start, previous))
                    start = previous = None
                continue
            if start is None:
                start = year
            previous = year
        if start is not None and previous is not None:
            intervals.append((start, previous))
        return tuple(intervals)

    def probe_host_years(
        self,
        task_id: str,
        *,
        worker_id: str,
        generation: int,
        probes: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        """Resolve minimal HY probes against immutable baseline + accepted ledger."""

        if self.baseline_index is None:
            raise AuthorityNotReadyError(
                "HY admission requires a bound local baseline index"
            )
        now = float(self.clock())
        normalized: list[tuple[str, int, str, str]] = []
        seen: set[str] = set()
        for probe in probes:
            raw_hostname = probe.get("hostname")
            year = int(probe.get("year", 0))
            locator = str(probe.get("locator", "")).strip()
            if not isinstance(raw_hostname, str):
                raise ValueError("HY probe hostname must be a string")
            hostname = normalize_official(raw_hostname)
            if hostname is None or year not in YEAR_BITS or not locator:
                raise ValueError("invalid HY probe")
            hyid = host_year_id(hostname, year)
            if hyid in seen:
                continue
            seen.add(hyid)
            normalized.append((hostname, year, locator, hyid))

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )
            accepted: set[str] = set()
            hyids = [hyid for _hostname, _year, _locator, hyid in normalized]
            for start in range(0, len(hyids), 900):
                chunk = hyids[start : start + 900]
                if not chunk:
                    continue
                placeholders = ",".join("?" for _ in chunk)
                accepted.update(
                    str(row["hy_id"])
                    for row in self.connection.execute(
                        f"""
                        SELECT hy_id FROM distributed_host_year_ledger
                        WHERE hy_id IN ({placeholders})
                        """,
                        chunk,
                    )
                )
            baseline_masks: dict[str, int] = {}
            if normalized:
                baseline_masks = {
                    hostname: int(mask)
                    for hostname, (mask, _candidate) in self.baseline_index.resolve_batch(
                        [hostname for hostname, _year, _locator, _hyid in normalized]
                    ).items()
                }

            decisions: list[dict[str, object]] = []
            for hostname, year, locator, hyid in normalized:
                if hyid in accepted:
                    status = "KNOWN_ACCEPTED"
                elif baseline_masks.get(hostname, 0) & YEAR_BITS[year]:
                    status = "KNOWN_BASELINE"
                else:
                    status = "NEED_FULL_EVIDENCE"
                self.connection.execute(
                    """
                    INSERT INTO distributed_hy_probe_decisions(
                        task_id, hy_id, hostname, year, locator, status,
                        first_seen_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(task_id, hy_id) DO UPDATE SET
                        locator = excluded.locator,
                        status = excluded.status,
                        updated_at = excluded.updated_at
                    """,
                    (
                        task_id,
                        hyid,
                        hostname,
                        year,
                        locator,
                        status,
                        now,
                        now,
                    ),
                )
                decisions.append(
                    {"hostname": hostname, "year": year, "status": status}
                )
            self.connection.commit()
            return decisions
        except Exception:
            self.connection.rollback()
            raise

    def commit_full_host_year_evidence(
        self,
        task_id: str,
        *,
        worker_id: str,
        generation: int,
        evidence: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        """Commit full proof only for HYs previously admitted by HY_PROBE."""

        if self.baseline_index is None:
            raise AuthorityNotReadyError(
                "HY admission requires a bound local baseline index"
            )
        now = float(self.clock())
        prepared: list[tuple[str, int, str, str, str, str]] = []
        for item in evidence:
            raw_hostname = item.get("hostname")
            year = int(item.get("year", 0))
            if not isinstance(raw_hostname, str):
                raise ValueError("HY evidence hostname must be a string")
            hostname = normalize_official(raw_hostname)
            evidence_class = str(item.get("evidence_class", "")).strip()
            source = str(item.get("source", "")).strip()
            timestamp = str(item.get("timestamp", "")).strip()
            locator = str(item.get("locator", "")).strip()
            if (
                hostname is None
                or year not in YEAR_BITS
                or not evidence_class
                or not source
                or not timestamp
                or not locator
            ):
                raise ValueError("invalid full HY evidence")
            if not timestamp.startswith(str(year)):
                raise ValueError("HY evidence timestamp year mismatch")
            hyid = host_year_id(hostname, year)
            evid = evidence_id(
                hostname=hostname,
                year=year,
                evidence_class=evidence_class,
                source=source,
                timestamp=timestamp,
                locator=locator,
            )
            canonical = _json(dict(item) | {"hostname": hostname, "year": year})
            prepared.append(
                (hostname, year, hyid, evid, locator, canonical)
            )

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )
            results: list[dict[str, object]] = []
            for hostname, year, hyid, evid, locator, canonical in prepared:
                accepted = self.connection.execute(
                    """
                    SELECT evidence_id FROM distributed_host_year_ledger
                    WHERE hy_id = ?
                    """,
                    (hyid,),
                ).fetchone()
                if accepted is not None:
                    results.append(
                        {
                            "hostname": hostname,
                            "year": year,
                            "status": "KNOWN_ACCEPTED",
                            "evidence_id": str(accepted["evidence_id"]),
                        }
                    )
                    continue

                probe = self.connection.execute(
                    """
                    SELECT status, locator
                    FROM distributed_hy_probe_decisions
                    WHERE task_id = ? AND hy_id = ?
                    """,
                    (task_id, hyid),
                ).fetchone()
                if probe is None:
                    raise ValueError(
                        "full HY evidence requires prior task-local HY_PROBE"
                    )
                if str(probe["status"]) != "NEED_FULL_EVIDENCE":
                    raise ValueError(
                        "full HY evidence was not admitted by HY_PROBE"
                    )
                if str(probe["locator"]) != locator:
                    raise ValueError(
                        "full HY evidence locator does not match HY_PROBE"
                    )

                # Re-check immutable baseline at commit time. This is cheap and
                # makes the authority invariant explicit even if a caller
                # constructed the probe state using an older test fixture.
                if self.baseline_index.year_mask(hostname) & YEAR_BITS[year]:
                    self.connection.execute(
                        """
                        UPDATE distributed_hy_probe_decisions
                        SET status = 'KNOWN_BASELINE', updated_at = ?
                        WHERE task_id = ? AND hy_id = ?
                        """,
                        (now, task_id, hyid),
                    )
                    results.append(
                        {
                            "hostname": hostname,
                            "year": year,
                            "status": "KNOWN_BASELINE",
                        }
                    )
                    continue

                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO distributed_evidence_ledger(
                        evidence_id, hy_id, task_id, generation,
                        evidence_json, committed_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (evid, hyid, task_id, generation, canonical, now),
                )
                inserted = self.connection.execute(
                    """
                    INSERT OR IGNORE INTO distributed_host_year_ledger(
                        hy_id, hostname, year, evidence_id, task_id, accepted_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (hyid, hostname, year, evid, task_id, now),
                ).rowcount
                if inserted:
                    status = "ACCEPTED"
                    chosen_evidence_id = evid
                else:
                    row = self.connection.execute(
                        """
                        SELECT evidence_id FROM distributed_host_year_ledger
                        WHERE hy_id = ?
                        """,
                        (hyid,),
                    ).fetchone()
                    assert row is not None
                    status = "KNOWN_ACCEPTED"
                    chosen_evidence_id = str(row["evidence_id"])
                self.connection.execute(
                    """
                    UPDATE distributed_hy_probe_decisions
                    SET status = ?, updated_at = ?
                    WHERE task_id = ? AND hy_id = ?
                    """,
                    (
                        "KNOWN_ACCEPTED" if status == "KNOWN_ACCEPTED" else "ACCEPTED",
                        now,
                        task_id,
                        hyid,
                    ),
                )
                results.append(
                    {
                        "hostname": hostname,
                        "year": year,
                        "status": status,
                        "evidence_id": chosen_evidence_id,
                    }
                )
            self.connection.commit()
            return results
        except Exception:
            self.connection.rollback()
            raise

    def accepted_host_year_count(self) -> int:
        return int(
            self.connection.execute(
                "SELECT COUNT(*) FROM distributed_host_year_ledger"
            ).fetchone()[0]
        )

    def record_provider_region_observation(
        self,
        provider: str,
        *,
        worker_id: str,
        task_id: str,
        generation: int,
        connect_success: bool,
        status_code: int | None,
        latency_ms: float,
        response_bytes: int,
        timeout: bool = False,
        policy_block: bool = False,
    ) -> str:
        """Record one low-rate qualification probe and return derived state."""

        if (
            not provider.strip()
            or latency_ms < 0
            or response_bytes < 0
        ):
            raise ValueError("invalid provider-region observation")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )
            worker = self.connection.execute(
                """
                SELECT region FROM distributed_workers
                WHERE worker_id = ? AND revoked = 0
                """,
                (worker_id,),
            ).fetchone()
            if worker is None:
                raise WorkerRejectedError(worker_id)
            region = str(worker["region"])
            existing = self.connection.execute(
                """
                SELECT * FROM distributed_provider_regions
                WHERE provider = ? AND region = ?
                """,
                (provider, region),
            ).fetchone()
            samples = 1 + (0 if existing is None else int(existing["samples"]))
            success = bool(connect_success) and not bool(timeout)
            successes = (1 if success else 0) + (
                0 if existing is None else int(existing["successes"])
            )
            timeouts = (1 if timeout else 0) + (
                0 if existing is None else int(existing["timeouts"])
            )
            throttles = (1 if status_code == 429 else 0) + (
                0 if existing is None else int(existing["throttles"])
            )
            policy_blocks = (1 if policy_block or status_code == 403 else 0) + (
                0 if existing is None else int(existing["policy_blocks"])
            )
            total_latency = float(latency_ms) + (
                0.0 if existing is None else float(existing["total_latency_ms"])
            )
            total_bytes = int(response_bytes) + (
                0 if existing is None else int(existing["response_bytes"])
            )

            if samples < 3:
                state = "UNKNOWN"
            elif policy_blocks / samples >= 0.8:
                state = "BLOCKED"
            elif timeouts / samples >= 0.5 or throttles / samples >= 0.5:
                state = "DEGRADED"
            elif successes / samples >= 0.8:
                state = "QUALIFIED"
            else:
                state = "DEGRADED"

            self.connection.execute(
                """
                INSERT INTO distributed_provider_regions(
                    provider, region, state, samples, successes, timeouts,
                    throttles, policy_blocks, total_latency_ms,
                    response_bytes, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, region) DO UPDATE SET
                    state = excluded.state,
                    samples = excluded.samples,
                    successes = excluded.successes,
                    timeouts = excluded.timeouts,
                    throttles = excluded.throttles,
                    policy_blocks = excluded.policy_blocks,
                    total_latency_ms = excluded.total_latency_ms,
                    response_bytes = excluded.response_bytes,
                    updated_at = excluded.updated_at
                """,
                (
                    provider,
                    region,
                    state,
                    samples,
                    successes,
                    timeouts,
                    throttles,
                    policy_blocks,
                    total_latency,
                    total_bytes,
                    now,
                ),
            )
            self.connection.commit()
            return state
        except Exception:
            self.connection.rollback()
            raise

    def provider_region_snapshot(
        self,
        provider: str,
        region: str,
    ) -> dict[str, object] | None:
        row = self.connection.execute(
            """
            SELECT * FROM distributed_provider_regions
            WHERE provider = ? AND region = ?
            """,
            (provider, region),
        ).fetchone()
        if row is None:
            return None
        samples = int(row["samples"])
        return {
            "provider": str(row["provider"]),
            "region": str(row["region"]),
            "state": str(row["state"]),
            "samples": samples,
            "success_rate": (
                0.0 if samples == 0 else int(row["successes"]) / samples
            ),
            "timeout_rate": (
                0.0 if samples == 0 else int(row["timeouts"]) / samples
            ),
            "throttle_rate": (
                0.0 if samples == 0 else int(row["throttles"]) / samples
            ),
            "policy_block_rate": (
                0.0 if samples == 0 else int(row["policy_blocks"]) / samples
            ),
            "mean_latency_ms": (
                0.0 if samples == 0 else float(row["total_latency_ms"]) / samples
            ),
            "response_bytes": int(row["response_bytes"]),
            "updated_at": float(row["updated_at"]),
        }

    def configure_provider_budget(
        self,
        provider: str,
        *,
        requests_per_second: float,
        max_global_inflight: int,
        require_qualified_region: bool = True,
    ) -> None:
        if (
            not provider.strip()
            or requests_per_second <= 0
            or max_global_inflight < 1
        ):
            raise ValueError("invalid provider budget")
        now = float(self.clock())
        self.connection.execute(
            """
            INSERT INTO distributed_provider_budgets(
                provider, requests_per_second, max_global_inflight,
                require_qualified_region,
                next_request_at, cooldown_until, updated_at
            ) VALUES (?, ?, ?, ?, 0, 0, ?)
            ON CONFLICT(provider) DO UPDATE SET
                requests_per_second = excluded.requests_per_second,
                max_global_inflight = excluded.max_global_inflight,
                require_qualified_region = excluded.require_qualified_region,
                updated_at = excluded.updated_at
            """,
            (
                provider.strip(),
                float(requests_per_second),
                int(max_global_inflight),
                1 if require_qualified_region else 0,
                now,
            ),
        )
        self.connection.commit()

    def issue_provider_permit(
        self,
        provider: str,
        *,
        worker_id: str,
        task_id: str,
        generation: int,
        request_id: str | None = None,
        ttl_seconds: float = 30.0,
    ) -> ProviderPermit | None:
        """Issue one globally paced external-request permit.

        The first implementation intentionally grants one request per permit.
        This makes global rate/inflight correctness explicit before introducing
        larger batched permits or adaptive scheduling.
        """

        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        request_id = (
            uuid4().hex
            if request_id is None
            else request_id.strip()
        )
        if not request_id:
            raise ValueError("provider permit request_id is required")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            task_row = self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )

            existing = self.connection.execute(
                """
                SELECT * FROM distributed_provider_permits
                WHERE provider = ? AND worker_id = ? AND task_id = ?
                  AND generation = ? AND request_id = ?
                """,
                (
                    provider,
                    worker_id,
                    task_id,
                    int(generation),
                    request_id,
                ),
            ).fetchone()
            if existing is not None:
                if int(existing["active"]) and float(existing["expires_at"]) > now:
                    self.connection.commit()
                    return ProviderPermit(
                        permit_id=str(existing["permit_id"]),
                        request_id=request_id,
                        provider=str(existing["provider"]),
                        worker_id=str(existing["worker_id"]),
                        task_id=str(existing["task_id"]),
                        generation=int(existing["generation"]),
                        allowed_requests=int(existing["allowed_requests"]),
                        max_inflight=int(existing["max_inflight"]),
                        expires_at=float(existing["expires_at"]),
                    )
                if not int(existing["active"]) and existing["status_code"] is not None:
                    raise ValueError(
                        "provider permit request_id has already completed"
                    )
                self.connection.execute(
                    """
                    DELETE FROM distributed_provider_permits
                    WHERE permit_id = ?
                    """,
                    (str(existing["permit_id"]),),
                )

            allowed_providers = self._worker_allowed_providers(worker_id)
            if provider not in allowed_providers:
                raise ProviderAccessDeniedError(
                    f"worker={worker_id} provider={provider}"
                )
            if not self._worker_egress_available(worker_id, now=now):
                raise WorkerEgressBudgetExceededError(
                    f"worker={worker_id} daily egress budget exhausted"
                )
            budget = self.connection.execute(
                """
                SELECT * FROM distributed_provider_budgets
                WHERE provider = ?
                """,
                (provider,),
            ).fetchone()
            if budget is None:
                raise KeyError(f"provider budget not configured: {provider}")

            if (
                int(budget["require_qualified_region"])
                and str(task_row["task_class"]) != TaskClass.PROBE.value
            ):
                worker = self.connection.execute(
                    """
                    SELECT region, runtime_class
                    FROM distributed_workers
                    WHERE worker_id = ? AND revoked = 0
                    """,
                    (worker_id,),
                ).fetchone()
                if worker is None:
                    raise WorkerRejectedError(worker_id)
                runtime_class = str(worker["runtime_class"])
                is_cloudflare_thin = (
                    runtime_class == "cloudflare_worker"
                    and str(task_row["producer"]) == "ThinHistoricalQueryProducer"
                    and str(task_row["task_class"]) == TaskClass.HOST_BATCH.value
                )
                if is_cloudflare_thin:
                    qualified = {"state": "QUALIFIED"}
                else:
                    region = str(worker["region"])
                    qualified = self.connection.execute(
                    """
                    SELECT state FROM distributed_provider_regions
                    WHERE provider = ? AND region = ?
                    """,
                        (provider, region),
                    ).fetchone()
                if qualified is None or str(qualified["state"]) != "QUALIFIED":
                    region_label = (
                        "cloudflare-dynamic-edge"
                        if is_cloudflare_thin
                        else str(worker["region"])
                    )
                    raise ProviderRegionNotQualifiedError(
                        f"provider={provider} region={region_label}"
                    )

            self.connection.execute(
                """
                UPDATE distributed_provider_permits
                SET active = 0
                WHERE provider = ? AND active = 1 AND expires_at <= ?
                """,
                (provider, now),
            )
            if now < float(budget["cooldown_until"]):
                self.connection.commit()
                return None
            if now < float(budget["next_request_at"]):
                self.connection.commit()
                return None

            active = int(
                self.connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM distributed_provider_permits
                    WHERE provider = ? AND active = 1 AND expires_at > ?
                    """,
                    (provider, now),
                ).fetchone()[0]
            )
            max_inflight = int(budget["max_global_inflight"])
            if active >= max_inflight:
                self.connection.commit()
                return None

            permit_id = str(uuid4())
            expires_at = now + float(ttl_seconds)
            self.connection.execute(
                """
                INSERT INTO distributed_provider_permits(
                    permit_id, request_id, provider, worker_id, task_id,
                    generation, allowed_requests, max_inflight, expires_at,
                    active, issued_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, 1, ?)
                """,
                (
                    permit_id,
                    request_id,
                    provider,
                    worker_id,
                    task_id,
                    int(generation),
                    max_inflight,
                    expires_at,
                    now,
                ),
            )
            next_request_at = max(
                now,
                float(budget["next_request_at"]),
            ) + 1.0 / float(budget["requests_per_second"])
            self.connection.execute(
                """
                UPDATE distributed_provider_budgets
                SET next_request_at = ?, updated_at = ?
                WHERE provider = ?
                """,
                (next_request_at, now, provider),
            )
            self.connection.commit()
            return ProviderPermit(
                permit_id=permit_id,
                request_id=request_id,
                provider=provider,
                worker_id=worker_id,
                task_id=task_id,
                generation=int(generation),
                allowed_requests=1,
                max_inflight=max_inflight,
                expires_at=expires_at,
            )
        except Exception:
            self.connection.rollback()
            raise

    def report_provider_permit(
        self,
        permit_id: str,
        *,
        worker_id: str,
        status_code: int | None = None,
        cooldown_seconds: float = 0.0,
        response_bytes: int = 0,
    ) -> None:
        if cooldown_seconds < 0 or response_bytes < 0:
            raise ValueError(
                "cooldown_seconds and response_bytes must be non-negative"
            )
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """
                SELECT * FROM distributed_provider_permits
                WHERE permit_id = ?
                """,
                (permit_id,),
            ).fetchone()
            if row is None or str(row["worker_id"]) != worker_id:
                raise WorkerRejectedError(worker_id)
            if not int(row["active"]):
                # Provider reports are idempotent. In particular, replaying a
                # previously accepted 429/503 must not extend global cooldown.
                self.connection.commit()
                return
            self.connection.execute(
                """
                UPDATE distributed_provider_permits
                SET active = 0, status_code = ?
                WHERE permit_id = ?
                """,
                (status_code, permit_id),
            )
            if response_bytes:
                day_key = self._egress_day_key(now)
                self.connection.execute(
                    """
                    INSERT INTO distributed_worker_egress_daily(
                        worker_id, day_key, response_bytes, updated_at
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(worker_id, day_key) DO UPDATE SET
                        response_bytes =
                            distributed_worker_egress_daily.response_bytes
                            + excluded.response_bytes,
                        updated_at = excluded.updated_at
                    """,
                    (
                        worker_id,
                        day_key,
                        int(response_bytes),
                        now,
                    ),
                )
            if status_code in {429, 503}:
                delay = float(cooldown_seconds)
                budget = self.connection.execute(
                    """
                    SELECT cooldown_until
                    FROM distributed_provider_budgets
                    WHERE provider = ?
                    """,
                    (str(row["provider"]),),
                ).fetchone()
                assert budget is not None
                self.connection.execute(
                    """
                    UPDATE distributed_provider_budgets
                    SET cooldown_until = ?, updated_at = ?
                    WHERE provider = ?
                    """,
                    (
                        max(float(budget["cooldown_until"]), now + delay),
                        now,
                        str(row["provider"]),
                    ),
                )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def provider_budget_snapshot(self, provider: str) -> dict[str, float | int]:
        now = float(self.clock())
        row = self.connection.execute(
            """
            SELECT * FROM distributed_provider_budgets
            WHERE provider = ?
            """,
            (provider,),
        ).fetchone()
        if row is None:
            raise KeyError(provider)
        active = int(
            self.connection.execute(
                """
                SELECT COUNT(*)
                FROM distributed_provider_permits
                WHERE provider = ? AND active = 1 AND expires_at > ?
                """,
                (provider, now),
            ).fetchone()[0]
        )
        return {
            "requests_per_second": float(row["requests_per_second"]),
            "max_global_inflight": int(row["max_global_inflight"]),
            "require_qualified_region": int(row["require_qualified_region"]),
            "active_inflight": active,
            "next_request_at": float(row["next_request_at"]),
            "cooldown_until": float(row["cooldown_until"]),
        }

    def fabric_status_snapshot(self) -> dict[str, object]:
        """Compact local-only operational snapshot for deployment health."""

        task_states = {
            str(row["state"]): int(row["count"])
            for row in self.connection.execute(
                """
                SELECT state, COUNT(*) AS count
                FROM distributed_work
                GROUP BY state
                ORDER BY state
                """
            ).fetchall()
        }
        producer_states = {
            f"{row['producer']}:{row['state']}": int(row["count"])
            for row in self.connection.execute(
                """
                SELECT producer, state, COUNT(*) AS count
                FROM distributed_work
                GROUP BY producer, state
                ORDER BY producer, state
                """
            ).fetchall()
        }
        candidate_states = {
            str(row["state"]): int(row["count"])
            for row in self.connection.execute(
                """
                SELECT state, COUNT(*) AS count
                FROM distributed_source_candidates
                GROUP BY state
                ORDER BY state
                """
            ).fetchall()
        }
        workers = [
            {
                "worker_id": str(row["worker_id"]),
                "runtime_class": str(row["runtime_class"]),
                "region": str(row["region"]),
                "last_heartbeat": float(row["last_heartbeat"]),
                "revoked": bool(row["revoked"]),
            }
            for row in self.connection.execute(
                """
                SELECT worker_id, runtime_class, region,
                       last_heartbeat, revoked
                FROM distributed_workers
                ORDER BY worker_id
                """
            ).fetchall()
        ]
        providers = [
            {
                "provider": str(row["provider"]),
                "requests_per_second": float(row["requests_per_second"]),
                "max_global_inflight": int(row["max_global_inflight"]),
                "require_qualified_region": bool(
                    row["require_qualified_region"]
                ),
                "next_request_at": float(row["next_request_at"]),
                "cooldown_until": float(row["cooldown_until"]),
            }
            for row in self.connection.execute(
                """
                SELECT * FROM distributed_provider_budgets
                ORDER BY provider
                """
            ).fetchall()
        ]
        return {
            "tasks_by_state": task_states,
            "tasks_by_producer_state": producer_states,
            "accepted_host_years": self.accepted_host_year_count(),
            "source_candidates_by_state": candidate_states,
            "workers": workers,
            "provider_budgets": providers,
        }

    def task_row(self, task_id: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM distributed_work WHERE task_id = ?",
            (task_id,),
        ).fetchone()

    def batch_count(self, task_id: str) -> int:
        return int(
            self.connection.execute(
                """
                SELECT COUNT(*) FROM distributed_result_batches
                WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()[0]
        )
