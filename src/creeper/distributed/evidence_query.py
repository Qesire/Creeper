"""Distributed provider execution for Creeper evidence tasks.

Workers execute provider I/O only. Evidence acceptance, proof persistence,
terminal queue state, retry scheduling and FINAL attribution remain central.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any, Callable

from creeper.distributed.http_transport import build_authority_transport
from creeper.distributed.models import (
    Capability,
    ResultBatch,
    TaskClass,
    TaskLease,
    WorkDefinition,
)
from creeper.distributed.provider_gate import DistributedProviderGate
from creeper.distributed.worker import DistributedProducer, ProducerContext
from creeper.evidence.policies import (
    CDXQueryState,
    DomainEvidenceQueryResult,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
    TemporalScope,
)
from creeper.evidence.providers.async_cdx import AsyncWaybackCDXClient
from creeper.evidence.providers.async_rdap import AsyncRDAPClient
from creeper.evidence.providers.multi_cdx import (
    AsyncArquivoCDXClient,
    AsyncCDXProviderPool,
    CDXProviderConfig,
)
from creeper.evidence.result_commit import (
    EvidenceResultCommitter,
    evidence_retry_at,
)
from creeper.storage.control_store import ControlStore, EvidenceTask
from creeper.storage.evidence_queue import DurableEvidenceQueue
from creeper.storage.evidence_store import EvidenceStore


PRODUCER_NAME = "EvidenceQueryProducer"
PRODUCER_VERSION = "fabric-evidence-query-v1"


def serialize_key(key: EvidenceQueryKey) -> dict[str, Any]:
    return {
        "hostname": key.hostname,
        "year_from": key.temporal_scope.year_from,
        "year_to": key.temporal_scope.year_to,
        "provider": key.provider,
        "policy_version": key.policy_version,
    }


def deserialize_key(raw: Mapping[str, Any]) -> EvidenceQueryKey:
    return EvidenceQueryKey(
        hostname=str(raw["hostname"]),
        temporal_scope=TemporalScope(
            int(raw["year_from"]),
            int(raw["year_to"]),
        ),
        provider=str(raw["provider"]),
        policy_version=str(raw["policy_version"]),
    )


def serialize_capsule(capsule: EvidenceCapsule) -> dict[str, Any]:
    return {
        "hostname": capsule.hostname,
        "year": capsule.year,
        "provider": capsule.provider,
        "temporal_semantics": capsule.temporal_semantics,
        "evidence_timestamp": capsule.evidence_timestamp,
        "source_locator": capsule.source_locator,
        "payload_hash": capsule.payload_hash,
        "policy_version": capsule.policy_version,
        "evidence_type": capsule.evidence_type,
        "source_id": capsule.source_id,
        "original_url": capsule.original_url,
        "record_locator": capsule.record_locator,
        "extraction_method": capsule.extraction_method,
    }


def deserialize_capsule(raw: Mapping[str, Any]) -> EvidenceCapsule:
    return EvidenceCapsule(
        hostname=str(raw["hostname"]),
        year=int(raw["year"]),
        provider=str(raw["provider"]),
        temporal_semantics=str(raw["temporal_semantics"]),
        evidence_timestamp=str(raw["evidence_timestamp"]),
        source_locator=str(raw["source_locator"]),
        payload_hash=str(raw["payload_hash"]),
        policy_version=str(raw["policy_version"]),
        evidence_type=str(raw.get("evidence_type", "")),
        source_id=str(raw.get("source_id", "")),
        original_url=str(raw.get("original_url", "")),
        record_locator=str(raw.get("record_locator", "")),
        extraction_method=str(raw.get("extraction_method", "")),
    )


def _accounting(result) -> dict[str, Any]:
    return {
        "pages_seen": result.pages_seen,
        "records_seen": result.records_seen,
        "provider_requests": result.provider_requests,
        "provider_elapsed_milliseconds": result.provider_elapsed_milliseconds,
        "error": result.error,
    }


def serialize_result(
    result: EvidenceQueryResult | RangeEvidenceQueryResult | DomainEvidenceQueryResult,
) -> dict[str, Any]:
    if result.key is None:
        raise ValueError("distributed evidence result requires query key")
    base = {
        "key": serialize_key(result.key),
        "state": result.state.value,
        **_accounting(result),
    }
    if isinstance(result, DomainEvidenceQueryResult):
        return {
            "kind": "EVIDENCE_DOMAIN_RESULT",
            **base,
            "domain": result.domain,
            "capsules": [serialize_capsule(item) for item in result.capsules],
        }
    if isinstance(result, RangeEvidenceQueryResult):
        return {
            "kind": "EVIDENCE_RANGE_RESULT",
            **base,
            "hostname": result.hostname,
            "candidate_years": list(result.candidate_years),
            "followup_years": list(result.followup_years),
            "capsules": [serialize_capsule(item) for item in result.capsules],
        }
    return {
        "kind": "EVIDENCE_EXACT_RESULT",
        **base,
        "hostname": result.hostname,
        "year": result.year,
        "capsule": (
            None if result.capsule is None
            else serialize_capsule(result.capsule)
        ),
    }


def deserialize_result(
    raw: Mapping[str, Any],
) -> EvidenceQueryResult | RangeEvidenceQueryResult | DomainEvidenceQueryResult:
    key_raw = raw.get("key")
    if not isinstance(key_raw, Mapping):
        raise ValueError("distributed evidence result requires key object")
    key = deserialize_key(key_raw)
    state = CDXQueryState(str(raw["state"]))
    common = {
        "pages_seen": int(raw.get("pages_seen", 0)),
        "records_seen": int(raw.get("records_seen", 0)),
        "provider_requests": int(raw.get("provider_requests", 0)),
        "provider_elapsed_milliseconds": int(
            raw.get("provider_elapsed_milliseconds", 0)
        ),
        "error": (
            None if raw.get("error") is None else str(raw.get("error"))
        ),
    }
    kind = str(raw.get("kind", ""))
    if kind == "EVIDENCE_DOMAIN_RESULT":
        capsules_raw = raw.get("capsules", ())
        if not isinstance(capsules_raw, list):
            raise ValueError("domain capsules must be an array")
        return DomainEvidenceQueryResult(
            domain=str(raw["domain"]),
            key=key,
            state=state,
            capsules=tuple(
                deserialize_capsule(item)
                for item in capsules_raw
                if isinstance(item, Mapping)
            ),
            **common,
        )
    if kind == "EVIDENCE_RANGE_RESULT":
        capsules_raw = raw.get("capsules", ())
        candidate_years = raw.get("candidate_years", ())
        followup_years = raw.get("followup_years", ())
        if (
            not isinstance(capsules_raw, list)
            or not isinstance(candidate_years, list)
            or not isinstance(followup_years, list)
        ):
            raise ValueError("range result arrays are malformed")
        return RangeEvidenceQueryResult(
            hostname=str(raw["hostname"]),
            key=key,
            state=state,
            candidate_years=tuple(int(v) for v in candidate_years),
            followup_years=tuple(int(v) for v in followup_years),
            capsules=tuple(
                deserialize_capsule(item)
                for item in capsules_raw
                if isinstance(item, Mapping)
            ),
            **common,
        )
    if kind == "EVIDENCE_EXACT_RESULT":
        capsule_raw = raw.get("capsule")
        if capsule_raw is not None and not isinstance(capsule_raw, Mapping):
            raise ValueError("exact result capsule must be an object or null")
        return EvidenceQueryResult(
            hostname=str(raw["hostname"]),
            year=int(raw["year"]),
            state=state,
            capsule=(
                None
                if capsule_raw is None
                else deserialize_capsule(capsule_raw)
            ),
            key=key,
            **common,
        )
    raise ValueError(f"unexpected distributed evidence result kind: {kind}")


def _task_identity(task: EvidenceTask) -> str:
    key = task.key
    scope = key.temporal_scope
    return (
        f"{key.provider}:{key.hostname}:{scope.year_from}-{scope.year_to}:"
        f"{key.policy_version}:attempt:{task.attempt}"
    )


def evidence_work_definition(
    task: EvidenceTask,
    *,
    cdx_provider_configs: tuple[CDXProviderConfig, ...] = (),
    rdap_endpoint: str = "https://rdap.org/domain",
    rdap_timeout: float = 20.0,
    priority: float = 0.0,
) -> WorkDefinition:
    key = task.key
    if key.provider == "wayback":
        if not cdx_provider_configs:
            raise ValueError("wayback Fabric work requires CDX provider configs")
        provider_payload = {
            "kind": "wayback",
            "cdx_providers": [item.as_dict() for item in cdx_provider_configs],
        }
        required_providers = tuple(item.name for item in cdx_provider_configs)
        required_capabilities = (Capability.EVIDENCE_QUERY.value,)
    elif key.provider == "rdap":
        provider_payload = {
            "kind": "rdap",
            "endpoint": str(rdap_endpoint),
            "timeout": float(rdap_timeout),
        }
        required_providers = ("rdap",)
        required_capabilities = (
            Capability.EVIDENCE_QUERY.value,
            Capability.RDAP.value,
        )
    else:
        raise ValueError(
            f"unsupported distributed evidence provider: {key.provider}"
        )
    return WorkDefinition(
        producer=PRODUCER_NAME,
        task_class=TaskClass.EVIDENCE_BATCH,
        input_identity=_task_identity(task),
        payload={
            "key": serialize_key(key),
            "attempt": task.attempt,
            "provider": provider_payload,
        },
        partition=f"{key.provider}:{key.hostname}",
        algorithm_version=PRODUCER_VERSION,
        required_capabilities=required_capabilities,
        required_providers=required_providers,
        priority=float(priority),
        max_attempts=8,
    )


def _transient_for(
    key: EvidenceQueryKey,
    error: str,
) -> EvidenceQueryResult | RangeEvidenceQueryResult:
    scope = key.temporal_scope
    if scope.year_from != scope.year_to:
        return RangeEvidenceQueryResult(
            hostname=key.hostname,
            key=key,
            state=CDXQueryState.TRANSIENT_ERROR,
            error=error,
        )
    return EvidenceQueryResult(
        hostname=key.hostname,
        year=scope.year_from,
        state=CDXQueryState.TRANSIENT_ERROR,
        error=error,
        key=key,
    )


class EvidenceQueryProducer(DistributedProducer):
    """Execute one evidence-provider task without committing evidence authority."""

    async def _wayback_provider(
        self,
        stack: AsyncExitStack,
        lease: TaskLease,
        context: ProducerContext,
        raw_specs: list[object],
    ) -> AsyncCDXProviderPool:
        configs = tuple(
            CDXProviderConfig.from_mapping(item)
            for item in raw_specs
            if isinstance(item, Mapping)
        )
        if not configs or set(item.name for item in configs) != set(
            lease.work.required_providers
        ):
            raise ValueError("CDX provider payload disagrees with work authority")
        clients: dict[str, AsyncWaybackCDXClient] = {}
        inflight: dict[str, int] = {}
        weights: dict[str, float] = {}
        for config in configs:
            gate = DistributedProviderGate(
                context.client,
                context.keeper,
                config.name,
            )
            transport = build_authority_transport(
                acquire=gate.acquire,
                report=gate.report,
                max_connections=config.max_connections,
                max_keepalive_connections=config.max_keepalive_connections,
                keepalive_expiry_seconds=config.keepalive_expiry_seconds,
            )
            client_type = (
                AsyncArquivoCDXClient
                if config.dialect == "arquivo"
                else AsyncWaybackCDXClient
            )
            clients[config.name] = client_type(
                endpoint=config.endpoint,
                provider="wayback",
                source_id=config.name,
                limit=config.row_limit,
                timeout=config.timeout,
                max_retries=config.max_retries,
                requests_per_second=0.0,
                max_connections=config.max_connections,
                max_keepalive_connections=config.max_keepalive_connections,
                keepalive_expiry_seconds=config.keepalive_expiry_seconds,
                throttle_floor_seconds=config.throttle_floor_seconds,
                transport=transport,
            )
            inflight[config.name] = config.max_inflight
            weights[config.name] = config.weight
        pool = AsyncCDXProviderPool(
            clients,
            logical_provider="wayback",
            inflight=inflight,
            weights=weights,
            owns_clients=True,
        )
        return await stack.enter_async_context(pool)

    async def _rdap_provider(
        self,
        stack: AsyncExitStack,
        lease: TaskLease,
        context: ProducerContext,
        raw: Mapping[str, Any],
    ) -> AsyncRDAPClient:
        if tuple(lease.work.required_providers) != ("rdap",):
            raise ValueError("RDAP provider payload disagrees with work authority")
        gate = DistributedProviderGate(
            context.client,
            context.keeper,
            "rdap",
        )
        transport = build_authority_transport(
            acquire=gate.acquire,
            report=gate.report,
            max_connections=4,
            max_keepalive_connections=2,
            keepalive_expiry_seconds=20.0,
        )
        provider = AsyncRDAPClient(
            endpoint=str(raw.get("endpoint", "https://rdap.org/domain")),
            timeout=float(raw.get("timeout", 20.0)),
            requests_per_second=0.0,
            transport=transport,
        )
        return await stack.enter_async_context(provider)

    async def run(
        self,
        lease: TaskLease,
        context: ProducerContext,
    ):
        raw = lease.work.payload
        key_raw = raw.get("key")
        provider_raw = raw.get("provider")
        if not isinstance(key_raw, Mapping) or not isinstance(
            provider_raw, Mapping
        ):
            raise ValueError("invalid distributed evidence work payload")
        key = deserialize_key(key_raw)
        attempt = int(raw["attempt"])
        if attempt < 1:
            raise ValueError("distributed evidence attempt must be positive")

        try:
            async with AsyncExitStack() as stack:
                kind = str(provider_raw.get("kind", ""))
                if kind == "wayback":
                    specs = provider_raw.get("cdx_providers")
                    if not isinstance(specs, list):
                        raise ValueError("wayback work requires provider array")
                    provider = await self._wayback_provider(
                        stack,
                        lease,
                        context,
                        specs,
                    )
                elif kind == "rdap":
                    provider = await self._rdap_provider(
                        stack,
                        lease,
                        context,
                        provider_raw,
                    )
                else:
                    raise ValueError(
                        f"unsupported distributed evidence provider kind: {kind}"
                    )

                scope = key.temporal_scope
                if scope.year_from == scope.year_to:
                    result = await provider.query_key(key)
                else:
                    result = await provider.query_range(key)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            result = _transient_for(
                key,
                str(exc) or type(exc).__name__,
            )

        yield ResultBatch(
            task_id=lease.task_id,
            generation=lease.generation,
            sequence_no=lease.next_sequence_no,
            results=(serialize_result(result),),
            cursor_after="EOF",
            final=True,
        )


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("expected JSON object")


@dataclass(frozen=True, slots=True)
class EvidenceBridgeReport:
    dispatched: int = 0
    committed: int = 0
    terminal: int = 0
    retryable: int = 0
    inserted_capsules: int = 0
    stale: int = 0
    failed: int = 0
    quarantined: int = 0
    renewed: int = 0


class DistributedEvidenceBridge:
    """Durable ownership bridge between ControlStore evidence work and Fabric."""

    def __init__(
        self,
        fabric_store,
        control_store: ControlStore,
        evidence_store: EvidenceStore,
        *,
        cdx_provider_configs: tuple[CDXProviderConfig, ...] = (),
        rdap_endpoint: str = "https://rdap.org/domain",
        rdap_timeout: float = 20.0,
        owner: str = "fabric:evidence",
        lease_seconds: float = 900.0,
        retry_base_seconds: float = 30.0,
        retry_max_seconds: float = 3600.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not owner.strip() or lease_seconds <= 0:
            raise ValueError("invalid distributed evidence bridge configuration")
        self.fabric_store = fabric_store
        self.control_store = control_store
        self.evidence_store = evidence_store
        self.owner = owner
        self.lease_seconds = float(lease_seconds)
        self.cdx_provider_configs = tuple(cdx_provider_configs)
        self.rdap_endpoint = str(rdap_endpoint)
        self.rdap_timeout = float(rdap_timeout)
        self.retry_base_seconds = float(retry_base_seconds)
        self.retry_max_seconds = float(retry_max_seconds)
        self.clock = clock
        self.queue = DurableEvidenceQueue(control_store)
        self.committer = EvidenceResultCommitter(
            control_store,
            evidence_store,
            owner=owner,
            retry_at=self._retry_at,
        )
        self._initialize()

    def _initialize(self) -> None:
        self.control_store.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS fabric_evidence_dispatch_v1 (
                hostname TEXT NOT NULL,
                year_from INTEGER NOT NULL,
                year_to INTEGER NOT NULL,
                provider TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                fabric_task_id TEXT NOT NULL UNIQUE,
                fabric_work_key TEXT NOT NULL,
                state TEXT NOT NULL,
                last_error TEXT,
                dispatched_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(
                    hostname,year_from,year_to,provider,policy_version,attempt
                )
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_fabric_evidence_dispatch_state
                ON fabric_evidence_dispatch_v1(state, updated_at);
            """
        )

    def _retry_at(self, attempt: int) -> float:
        return evidence_retry_at(
            attempt,
            base_seconds=self.retry_base_seconds,
            max_seconds=self.retry_max_seconds,
            clock=self.clock,
        )

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

    def _current_task(self, key: EvidenceQueryKey) -> EvidenceTask | None:
        row = self.control_store.connection.execute(
            """
            SELECT state,attempt,retry_at,lease_owner,lease_until
            FROM evidence_tasks
            WHERE hostname=? AND year_from=? AND year_to=?
              AND provider=? AND policy_version=?
            """,
            self._values(key),
        ).fetchone()
        if row is None:
            return None
        return EvidenceTask(
            key=key,
            state=str(row["state"]),
            attempt=int(row["attempt"]),
            retry_at=row["retry_at"],
            lease_owner=row["lease_owner"],
            lease_until=row["lease_until"],
        )

    def _dispatch_row(self, fabric_task_id: str):
        return self.control_store.connection.execute(
            """
            SELECT * FROM fabric_evidence_dispatch_v1
            WHERE fabric_task_id=?
            """,
            (fabric_task_id,),
        ).fetchone()

    def _mapping_key(self, row) -> EvidenceQueryKey:
        return EvidenceQueryKey(
            str(row["hostname"]),
            TemporalScope(int(row["year_from"]), int(row["year_to"])),
            str(row["provider"]),
            str(row["policy_version"]),
        )

    def _mapping_state(
        self,
        fabric_task_id: str,
        state: str,
        *,
        error: str | None = None,
    ) -> None:
        with self.control_store.connection:
            self.control_store.connection.execute(
                """
                UPDATE fabric_evidence_dispatch_v1
                SET state=?, last_error=?, updated_at=?
                WHERE fabric_task_id=?
                """,
                (
                    state,
                    None if error is None else error[:2000],
                    float(self.clock()),
                    fabric_task_id,
                ),
            )

    def renew_active(self) -> int:
        rows = self.control_store.connection.execute(
            """
            SELECT * FROM fabric_evidence_dispatch_v1
            WHERE state='ACTIVE'
            ORDER BY dispatched_at
            """
        ).fetchall()
        keys: list[EvidenceQueryKey] = []
        for row in rows:
            key = self._mapping_key(row)
            current = self._current_task(key)
            if (
                current is None
                or current.attempt != int(row["attempt"])
                or current.lease_owner != self.owner
            ):
                self._mapping_state(
                    str(row["fabric_task_id"]),
                    "STALE",
                    error="ControlStore evidence ownership changed",
                )
                continue
            keys.append(key)
        if not keys:
            return 0
        renewed = self.queue.renew(
            keys,
            owner=self.owner,
            lease_seconds=self.lease_seconds,
        )
        if renewed != len(keys):
            for key in keys:
                current = self._current_task(key)
                if current is None or current.lease_owner != self.owner:
                    row = self.control_store.connection.execute(
                        """
                        SELECT fabric_task_id FROM fabric_evidence_dispatch_v1
                        WHERE hostname=? AND year_from=? AND year_to=?
                          AND provider=? AND policy_version=?
                          AND attempt=? AND state='ACTIVE'
                        """,
                        (*self._values(key), current.attempt if current else -1),
                    ).fetchone()
                    if row is not None:
                        self._mapping_state(
                            str(row["fabric_task_id"]),
                            "STALE",
                            error="ControlStore evidence lease could not renew",
                        )
        return renewed

    def dispatch(
        self,
        *,
        limit: int = 32,
        providers: tuple[str, ...] = ("wayback", "rdap"),
    ) -> int:
        if limit < 1:
            return 0
        self.reconcile_failed_work()
        self.renew_active()
        tasks = self.queue.claim(
            owner=self.owner,
            limit=limit,
            providers=providers,
            lease_seconds=self.lease_seconds,
        )
        dispatched = 0
        for task in tasks:
            work = evidence_work_definition(
                task,
                cdx_provider_configs=self.cdx_provider_configs,
                rdap_endpoint=self.rdap_endpoint,
                rdap_timeout=self.rdap_timeout,
            )
            try:
                fabric_task_id, _inserted = self.fabric_store.admit_work(work)
            except Exception as exc:
                self.control_store.finish_evidence_task(
                    task.key,
                    CDXQueryState.TRANSIENT_ERROR,
                    owner=self.owner,
                    retry_at=self._retry_at(task.attempt),
                )
                raise RuntimeError(
                    "failed to admit Fabric evidence work"
                ) from exc
            now = float(self.clock())
            with self.control_store.connection:
                prior = self.control_store.connection.execute(
                    """
                    SELECT fabric_task_id,fabric_work_key
                    FROM fabric_evidence_dispatch_v1
                    WHERE hostname=? AND year_from=? AND year_to=?
                      AND provider=? AND policy_version=? AND attempt=?
                    """,
                    (*self._values(task.key), task.attempt),
                ).fetchone()
                if prior is not None and (
                    str(prior["fabric_task_id"]) != fabric_task_id
                    or str(prior["fabric_work_key"]) != work.work_key
                ):
                    raise RuntimeError(
                        "evidence attempt changed Fabric work identity"
                    )
                self.control_store.connection.execute(
                    """
                    INSERT INTO fabric_evidence_dispatch_v1(
                        hostname,year_from,year_to,provider,policy_version,
                        attempt,fabric_task_id,fabric_work_key,state,
                        dispatched_at,updated_at
                    ) VALUES (?,?,?,?,?,?,?,?, 'ACTIVE',?,?)
                    ON CONFLICT(
                        hostname,year_from,year_to,provider,policy_version,attempt
                    ) DO UPDATE SET
                        state='ACTIVE', updated_at=excluded.updated_at
                    """,
                    (
                        *self._values(task.key),
                        task.attempt,
                        fabric_task_id,
                        work.work_key,
                        now,
                        now,
                    ),
                )
            dispatched += 1
        return dispatched

    def _release_dead_mapping(self, row, *, reason: str) -> None:
        key = self._mapping_key(row)
        current = self._current_task(key)
        if (
            current is not None
            and current.attempt == int(row["attempt"])
            and current.lease_owner == self.owner
        ):
            self.control_store.finish_evidence_task(
                key,
                CDXQueryState.TRANSIENT_ERROR,
                owner=self.owner,
                retry_at=self._retry_at(current.attempt),
            )
        self._mapping_state(
            str(row["fabric_task_id"]),
            "FAILED",
            error=reason,
        )

    def reconcile_failed_work(self) -> int:
        rows = self.control_store.connection.execute(
            """
            SELECT * FROM fabric_evidence_dispatch_v1
            WHERE state='ACTIVE'
            ORDER BY dispatched_at
            """
        ).fetchall()
        changed = 0
        for row in rows:
            try:
                task = self.fabric_store.task_row(str(row["fabric_task_id"]))
            except Exception:
                continue
            if str(task["state"]) != "DEAD":
                continue
            self._release_dead_mapping(
                row,
                reason=str(task.get("last_error") or "Fabric work exhausted"),
            )
            changed += 1
        return changed

    def drain(self, *, limit: int = 64) -> EvidenceBridgeReport:
        terminal = retryable = inserted = stale = failed = quarantined = 0
        committed = 0
        renewed = self.renew_active()
        self.reconcile_failed_work()
        for batch_row in self.fabric_store.unconsumed_batches(limit=limit):
            fabric_task_id = str(batch_row["task_id"])
            try:
                fabric_task = self.fabric_store.task_row(fabric_task_id)
            except Exception:
                continue
            if str(fabric_task["producer"]) != PRODUCER_NAME:
                continue

            batch_id = str(batch_row["batch_id"])
            mapping = self._dispatch_row(fabric_task_id)
            if mapping is None:
                failed += 1
                quarantined += int(
                    self.fabric_store.mark_batch_consume_failed(
                        batch_id,
                        "Fabric evidence batch has no dispatch mapping",
                    )
                )
                continue
            if str(mapping["state"]) != "ACTIVE":
                self.fabric_store.mark_batch_consumed(batch_id)
                stale += 1
                continue

            key = self._mapping_key(mapping)
            current = self._current_task(key)
            if (
                current is None
                or current.attempt != int(mapping["attempt"])
                or current.lease_owner != self.owner
            ):
                self._mapping_state(
                    fabric_task_id,
                    "STALE",
                    error="stale evidence attempt returned from Fabric",
                )
                self.fabric_store.mark_batch_consumed(batch_id)
                stale += 1
                continue

            try:
                payload = _json_object(batch_row["payload_json"])
                raw_results = payload.get("results")
                if not isinstance(raw_results, list) or len(raw_results) != 1:
                    raise ValueError(
                        "Fabric evidence batch must contain exactly one result"
                    )
                result_raw = raw_results[0]
                if not isinstance(result_raw, Mapping):
                    raise ValueError("Fabric evidence result must be an object")
                result = deserialize_result(result_raw)
                work_payload = _json_object(fabric_task["payload_json"])
                if int(work_payload.get("attempt", -1)) != current.attempt:
                    raise ValueError("Fabric evidence attempt disagrees with mapping")
                if result.key != current.key:
                    raise ValueError("Fabric evidence result key disagrees with mapping")

                outcome = self.committer.commit(current, result)
                terminal += outcome.terminal
                retryable += outcome.retryable
                inserted += outcome.inserted_capsules
                self._mapping_state(fabric_task_id, "CONSUMED")
                self.fabric_store.mark_batch_consumed(batch_id)
                committed += 1
            except Exception as exc:
                failed += 1
                is_quarantined = self.fabric_store.mark_batch_consume_failed(
                    batch_id,
                    f"{type(exc).__name__}: {exc}",
                )
                quarantined += int(is_quarantined)
                if is_quarantined:
                    self._release_dead_mapping(
                        mapping,
                        reason=f"poison evidence result: {type(exc).__name__}: {exc}",
                    )
        return EvidenceBridgeReport(
            committed=committed,
            terminal=terminal,
            retryable=retryable,
            inserted_capsules=inserted,
            stale=stale,
            failed=failed,
            quarantined=quarantined,
            renewed=renewed,
        )
