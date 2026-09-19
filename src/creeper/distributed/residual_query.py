"""Distributed execution bridge for deterministic residual search."""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import AsyncExitStack
from dataclasses import asdict, dataclass
from typing import Any, Mapping

import httpx

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
from creeper.source_discovery.deterministic_search import (
    DataCiteSearchProvider,
    DeterministicSearchBatch,
    DeterministicSearchPolicy,
    HarvardDataverseSearchProvider,
    InternetArchiveSearchProvider,
    ZenodoSearchProvider,
    classify_result,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.residual_atomic import (
    AtomicResidualCommitResult,
    commit_deterministic_residual_batch,
)
from creeper.source_discovery.residual_search import (
    QueryPlan,
    ResidualSearchLedger,
    SearchCell,
)
from creeper.source_discovery.search_identity import (
    RawSearchResult,
    SearchIdentityLedger,
)


PRODUCER_NAME = "ResidualQueryProducer"
PRODUCER_VERSION = "fabric-residual-query-v1"
DEFAULT_PROVIDERS = (
    "datacite",
    "zenodo",
    "harvard_dataverse",
    "internet_archive",
)


def serialize_query_plan(plan: QueryPlan) -> dict[str, Any]:
    return {
        "cell": {
            "mechanism": plan.cell.mechanism,
            "institution": plan.cell.institution,
            "period": plan.cell.period,
            "artifact": plan.cell.artifact,
        },
        "query": plan.query,
        "variant": plan.variant,
        "exclusions": list(plan.exclusions),
        "score": plan.score,
        "mechanism_phrase": plan.mechanism_phrase,
        "include_institution": plan.include_institution,
        "query_shape": plan.query_shape,
    }


def deserialize_query_plan(raw: Mapping[str, Any]) -> QueryPlan:
    cell_raw = raw.get("cell")
    if not isinstance(cell_raw, Mapping):
        raise ValueError("distributed residual plan requires a cell object")
    return QueryPlan(
        cell=SearchCell(
            mechanism=str(cell_raw["mechanism"]),
            institution=str(cell_raw["institution"]),
            period=str(cell_raw["period"]),
            artifact=str(cell_raw["artifact"]),
        ),
        query=str(raw["query"]),
        variant=int(raw["variant"]),
        exclusions=tuple(str(item) for item in raw.get("exclusions", ())),
        score=float(raw["score"]),
        mechanism_phrase=str(raw["mechanism_phrase"]),
        include_institution=bool(raw["include_institution"]),
        query_shape=str(raw["query_shape"]),
    )


def serialize_raw_result(result: RawSearchResult) -> dict[str, Any]:
    return {
        "kind": "RESIDUAL_RAW_RESULT",
        "provider": result.provider,
        "provider_result_id": result.provider_result_id,
        "url": result.url,
        "title": result.title,
        "description": result.description,
        "publisher": result.publisher,
        "creators": list(result.creators),
        "publication_year": result.publication_year,
        "resource_type": result.resource_type,
        "identifiers": list(result.identifiers),
        "content_length": result.content_length,
        "etag": result.etag,
        "checksum_sha256": result.checksum_sha256,
    }


def deserialize_raw_result(raw: Mapping[str, Any]) -> RawSearchResult:
    if str(raw.get("kind", "")) != "RESIDUAL_RAW_RESULT":
        raise ValueError("unexpected distributed residual result kind")
    return RawSearchResult(
        provider=str(raw["provider"]),
        provider_result_id=str(raw["provider_result_id"]),
        url=str(raw["url"]),
        title=str(raw.get("title", "")),
        description=str(raw.get("description", "")),
        publisher=str(raw.get("publisher", "")),
        creators=tuple(str(item) for item in raw.get("creators", ())),
        publication_year=(
            None
            if raw.get("publication_year") is None
            else int(raw["publication_year"])
        ),
        resource_type=str(raw.get("resource_type", "")),
        identifiers=tuple(str(item) for item in raw.get("identifiers", ())),
        content_length=(
            None
            if raw.get("content_length") is None
            else int(raw["content_length"])
        ),
        etag=None if raw.get("etag") is None else str(raw["etag"]),
        checksum_sha256=(
            None
            if raw.get("checksum_sha256") is None
            else str(raw["checksum_sha256"])
        ),
    )


def residual_work_definition(
    plan: QueryPlan,
    *,
    policy: DeterministicSearchPolicy,
    providers: tuple[str, ...] = DEFAULT_PROVIDERS,
    priority: float | None = None,
) -> WorkDefinition:
    if not providers or len(providers) != len(set(providers)):
        raise ValueError("distributed residual providers must be unique and non-empty")
    payload = {
        "plan": serialize_query_plan(plan),
        "policy": asdict(policy),
        "providers": list(providers),
    }
    return WorkDefinition(
        producer=PRODUCER_NAME,
        task_class=TaskClass.RESIDUAL_QUERY,
        input_identity=f"{plan.cell.key}:variant:{plan.variant}",
        payload=payload,
        partition=plan.cell.key,
        algorithm_version=PRODUCER_VERSION,
        required_capabilities=(Capability.RESIDUAL_QUERY.value,),
        required_providers=tuple(providers),
        priority=float(plan.score if priority is None else priority),
        max_attempts=8,
    )


def _provider(
    name: str,
    client: httpx.AsyncClient,
    *,
    timeout_seconds: float,
):
    if name == "datacite":
        return DataCiteSearchProvider(client, timeout_seconds=timeout_seconds)
    if name == "zenodo":
        return ZenodoSearchProvider(client, timeout_seconds=timeout_seconds)
    if name == "harvard_dataverse":
        return HarvardDataverseSearchProvider(
            client,
            timeout_seconds=timeout_seconds,
        )
    if name == "internet_archive":
        return InternetArchiveSearchProvider(
            client,
            timeout_seconds=timeout_seconds,
        )
    raise ValueError(f"unsupported distributed residual provider: {name}")


class ResidualQueryProducer(DistributedProducer):
    """Execute provider I/O only; classification remains central."""

    async def run(
        self,
        lease: TaskLease,
        context: ProducerContext,
    ):
        raw_payload = lease.work.payload
        plan_raw = raw_payload.get("plan")
        policy_raw = raw_payload.get("policy")
        provider_raw = raw_payload.get("providers")
        if (
            not isinstance(plan_raw, Mapping)
            or not isinstance(policy_raw, Mapping)
            or not isinstance(provider_raw, list)
        ):
            raise ValueError("invalid distributed residual work payload")
        plan = deserialize_query_plan(plan_raw)
        policy = DeterministicSearchPolicy(
            results_per_provider=int(policy_raw["results_per_provider"]),
            max_total_results=int(policy_raw["max_total_results"]),
            min_relevance_score=float(policy_raw["min_relevance_score"]),
            timeout_seconds=float(policy_raw["timeout_seconds"]),
        )
        providers = tuple(str(item) for item in provider_raw)
        if (
            not providers
            or len(providers) != len(set(providers))
            or set(providers) != set(lease.work.required_providers)
        ):
            raise ValueError("provider payload disagrees with work authority")

        started = time.perf_counter()
        async with AsyncExitStack() as stack:
            provider_objects = []
            for name in providers:
                gate = DistributedProviderGate(
                    context.client,
                    context.keeper,
                    name,
                )
                transport = build_authority_transport(
                    acquire=gate.acquire,
                    report=gate.report,
                    max_connections=8,
                    max_keepalive_connections=4,
                    keepalive_expiry_seconds=20.0,
                )
                client = await stack.enter_async_context(
                    httpx.AsyncClient(
                        transport=transport,
                        timeout=policy.timeout_seconds,
                        trust_env=False,
                    )
                )
                provider_objects.append(
                    _provider(
                        name,
                        client,
                        timeout_seconds=policy.timeout_seconds,
                    )
                )

            calls = [
                provider.search(plan, limit=policy.results_per_provider)
                for provider in provider_objects
            ]
            outcomes = await asyncio.gather(*calls, return_exceptions=True)
            errors = [
                (provider.name, outcome)
                for provider, outcome in zip(
                    provider_objects,
                    outcomes,
                    strict=True,
                )
                if isinstance(outcome, Exception)
            ]
            if errors:
                detail = "; ".join(
                    f"{name}:{type(error).__name__}"
                    for name, error in errors
                )
                raise RuntimeError(
                    "distributed residual provider set incomplete: " + detail
                )

            buckets = [
                tuple(outcome)
                for outcome in outcomes
                if not isinstance(outcome, Exception)
            ]
            raw_results: list[RawSearchResult] = []
            max_depth = max((len(bucket) for bucket in buckets), default=0)
            for rank in range(max_depth):
                for bucket in buckets:
                    if rank >= len(bucket):
                        continue
                    raw_results.append(bucket[rank])
                    if len(raw_results) >= policy.max_total_results:
                        break
                if len(raw_results) >= policy.max_total_results:
                    break

        results = [serialize_raw_result(item) for item in raw_results]
        results.append(
            {
                "kind": "RESIDUAL_SUMMARY",
                "backend": "+".join(providers),
                "query": plan.query,
                "search_cost_seconds": max(0.0, time.perf_counter() - started),
                "provider_count": len(providers),
            }
        )
        yield ResultBatch(
            task_id=lease.task_id,
            generation=lease.generation,
            sequence_no=lease.next_sequence_no,
            results=tuple(results),
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
class ResidualDrainReport:
    committed: tuple[AtomicResidualCommitResult, ...] = ()
    failed: int = 0
    quarantined: int = 0


class DistributedResidualBridge:
    """Trusted central admission/consume bridge for residual search."""

    def __init__(
        self,
        fabric_store,
        registry: SourceDiscoveryRegistry,
        coverage: ResidualSearchLedger,
        identities: SearchIdentityLedger,
        *,
        policy: DeterministicSearchPolicy,
        providers: tuple[str, ...] = DEFAULT_PROVIDERS,
        candidate_cap: int = 16,
    ) -> None:
        if candidate_cap < 1:
            raise ValueError("candidate_cap must be positive")
        self.fabric_store = fabric_store
        self.registry = registry
        self.coverage = coverage
        self.identities = identities
        self.policy = policy
        self.providers = tuple(providers)
        self.candidate_cap = int(candidate_cap)

    def submit(self, plan: QueryPlan) -> tuple[str, bool]:
        return self.fabric_store.admit_work(
            residual_work_definition(
                plan,
                policy=self.policy,
                providers=self.providers,
            )
        )

    def _plan_for_task(self, task_row) -> QueryPlan:
        payload = _json_object(task_row["payload_json"])
        plan_raw = payload.get("plan")
        if not isinstance(plan_raw, Mapping):
            raise ValueError("Fabric residual task is missing QueryPlan")
        return deserialize_query_plan(plan_raw)

    def _policy_for_task(self, task_row) -> DeterministicSearchPolicy:
        payload = _json_object(task_row["payload_json"])
        raw = payload.get("policy")
        if not isinstance(raw, Mapping):
            raise ValueError("Fabric residual task is missing policy")
        return DeterministicSearchPolicy(
            results_per_provider=int(raw["results_per_provider"]),
            max_total_results=int(raw["max_total_results"]),
            min_relevance_score=float(raw["min_relevance_score"]),
            timeout_seconds=float(raw["timeout_seconds"]),
        )

    def drain(self, *, limit: int = 64) -> ResidualDrainReport:
        committed: list[AtomicResidualCommitResult] = []
        failed = 0
        quarantined = 0
        for row in self.fabric_store.unconsumed_batches(limit=limit):
            task = self.fabric_store.task_row(str(row["task_id"]))
            if str(task["producer"]) != PRODUCER_NAME:
                continue
            batch_id = str(row["batch_id"])
            try:
                payload = _json_object(row["payload_json"])
                raw_results = payload.get("results")
                if not isinstance(raw_results, list):
                    raise ValueError(
                        "Fabric residual batch results must be an array"
                    )

                summary: Mapping[str, Any] | None = None
                provider_rows: list[Mapping[str, Any]] = []
                for item in raw_results:
                    if not isinstance(item, Mapping):
                        raise ValueError(
                            "Fabric residual result must be an object"
                        )
                    kind = str(item.get("kind", ""))
                    if kind == "RESIDUAL_SUMMARY":
                        if summary is not None:
                            raise ValueError(
                                "Fabric residual batch has duplicate summary"
                            )
                        summary = item
                    elif kind == "RESIDUAL_RAW_RESULT":
                        provider_rows.append(item)
                    else:
                        raise ValueError(
                            f"unexpected Fabric residual result kind: {kind}"
                        )
                if summary is None:
                    raise ValueError(
                        "Fabric residual batch is missing summary"
                    )

                plan = self._plan_for_task(task)
                policy = self._policy_for_task(task)
                canonical = []
                for item in provider_rows:
                    raw = deserialize_raw_result(item)
                    classified = classify_result(plan, raw, policy=policy)
                    if classified is not None:
                        canonical.append(classified)
                batch = DeterministicSearchBatch(
                    backend=str(summary["backend"]),
                    query=str(summary["query"]),
                    actor="deterministic:fabric-residual",
                    results=tuple(canonical),
                    search_cost_seconds=float(
                        summary["search_cost_seconds"]
                    ),
                )
                result = commit_deterministic_residual_batch(
                    self.registry,
                    self.coverage,
                    self.identities,
                    plan=plan,
                    batch=batch,
                    search_cost_seconds=float(
                        summary["search_cost_seconds"]
                    ),
                    candidate_cap=self.candidate_cap,
                    idempotency_key=batch_id,
                )
                # Marking consumed is intentionally after domain commit. A
                # crash between these writes replays the batch, while the
                # ControlStore idempotency marker makes its domain effect
                # exactly-once.
                self.fabric_store.mark_batch_consumed(batch_id)
                committed.append(result)
            except Exception as exc:
                failed += 1
                is_quarantined = self.fabric_store.mark_batch_consume_failed(
                    batch_id,
                    f"{type(exc).__name__}: {exc}",
                )
                quarantined += int(is_quarantined)
        return ResidualDrainReport(
            committed=tuple(committed),
            failed=failed,
            quarantined=quarantined,
        )

