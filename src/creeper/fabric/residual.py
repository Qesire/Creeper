"""Distributed residual-search execution and authority-local reduction."""

from __future__ import annotations

import time
from collections.abc import Mapping

import httpx

from creeper.source_discovery.deterministic_search import (
    DataCiteSearchProvider,
    DeterministicSearchBatch,
    DeterministicSearchPolicy,
    HarvardDataverseSearchProvider,
    InternetArchiveSearchProvider,
    ZenodoSearchProvider,
    classify_result,
)
from creeper.source_discovery.research_leads import ResearchLeadLedger
from creeper.source_discovery.residual_atomic import (
    AtomicResidualCommitResult,
    commit_deterministic_residual_batch,
)
from creeper.source_discovery.residual_search import QueryPlan, ResidualSearchLedger, SearchCell
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.search_identity import RawSearchResult, SearchIdentityLedger

from .models import (
    FabricCapability,
    FabricWorkClass,
    WorkerDescriptor,
)
from .worker import FabricTaskSession, PermanentTaskError


def query_plan_from_payload(payload: Mapping[str, object]) -> QueryPlan:
    raw_cell = payload.get("cell")
    if not isinstance(raw_cell, Mapping):
        raise ValueError("residual work requires a cell object")
    cell = SearchCell(
        mechanism=str(raw_cell["mechanism"]),
        institution=str(raw_cell["institution"]),
        period=str(raw_cell["period"]),
        artifact=str(raw_cell["artifact"]),
    )
    expected_key = raw_cell.get("key")
    if expected_key is not None and str(expected_key) != cell.key:
        raise ValueError("residual cell key disagrees with cell dimensions")
    raw_exclusions = payload.get("exclusions", ())
    if not isinstance(raw_exclusions, (list, tuple)):
        raise ValueError("residual exclusions must be a sequence")
    return QueryPlan(
        cell=cell,
        query=str(payload["query"]),
        variant=int(payload["variant"]),
        exclusions=tuple(str(item) for item in raw_exclusions),
        score=float(payload["score"]),
        mechanism_phrase=str(payload["mechanism_phrase"]),
        include_institution=bool(payload["include_institution"]),
        query_shape=str(payload["query_shape"]),
    )


def raw_result_payload(result: RawSearchResult) -> dict[str, object]:
    return {
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


def raw_result_from_payload(payload: Mapping[str, object]) -> RawSearchResult:
    creators = payload.get("creators", ())
    identifiers = payload.get("identifiers", ())
    if not isinstance(creators, (list, tuple)) or not isinstance(
        identifiers, (list, tuple)
    ):
        raise ValueError("raw search creators/identifiers must be sequences")
    publication_year = payload.get("publication_year")
    content_length = payload.get("content_length")
    return RawSearchResult(
        provider=str(payload["provider"]),
        provider_result_id=str(payload["provider_result_id"]),
        url=str(payload["url"]),
        title=str(payload.get("title", "")),
        description=str(payload.get("description", "")),
        publisher=str(payload.get("publisher", "")),
        creators=tuple(str(item) for item in creators),
        publication_year=(
            None if publication_year is None else int(publication_year)
        ),
        resource_type=str(payload.get("resource_type", "")),
        identifiers=tuple(str(item) for item in identifiers),
        content_length=(
            None if content_length is None else int(content_length)
        ),
        etag=(
            None if payload.get("etag") is None else str(payload["etag"])
        ),
        checksum_sha256=(
            None
            if payload.get("checksum_sha256") is None
            else str(payload["checksum_sha256"])
        ),
    )


def build_residual_provider(
    provider_name: str,
    client: httpx.AsyncClient,
    *,
    timeout_seconds: float,
):
    if provider_name == "datacite":
        return DataCiteSearchProvider(
            client,
            timeout_seconds=timeout_seconds,
        )
    if provider_name == "zenodo":
        return ZenodoSearchProvider(
            client,
            timeout_seconds=timeout_seconds,
        )
    if provider_name == "harvard_dataverse":
        return HarvardDataverseSearchProvider(
            client,
            timeout_seconds=timeout_seconds,
        )
    if provider_name == "internet_archive":
        return InternetArchiveSearchProvider(
            client,
            timeout_seconds=timeout_seconds,
        )
    raise ValueError(f"unsupported residual provider: {provider_name}")


def provider_request_reservation(provider_name: str) -> int:
    # DataCite/Zenodo/Dataverse are one HTTP request per bounded search slice.
    # Internet Archive expands at most 8 metadata items after one search query.
    return 9 if provider_name == "internet_archive" else 1


class ResidualProviderHandler:
    """Remote worker handler for exactly one deterministic provider slice."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        policy: DeterministicSearchPolicy | None = None,
    ) -> None:
        self.client = client
        self.policy = policy or DeterministicSearchPolicy()

    async def __call__(self, session: FabricTaskSession) -> None:
        lease = await session.snapshot()
        if lease.work.work_class is not FabricWorkClass.RESIDUAL_SEARCH:
            raise PermanentTaskError("residual handler received wrong work class")
        coverage = dict(lease.work.coverage)
        provider_name = str(coverage.get("provider", ""))
        if not provider_name or provider_name != lease.work.provider:
            raise PermanentTaskError("residual provider identity mismatch")
        plan = query_plan_from_payload(coverage)

        permit = await session.provider_permit(
            provider_name,
            allowed_requests=provider_request_reservation(provider_name),
            ttl_seconds=max(30.0, self.policy.timeout_seconds * 2.0),
            retry_after_seconds=0.25,
        )
        if permit is None:
            raise RuntimeError("provider budget unavailable")

        provider = build_residual_provider(
            provider_name,
            self.client,
            timeout_seconds=self.policy.timeout_seconds,
        )
        started = time.perf_counter()
        try:
            results = await provider.search(
                plan,
                limit=self.policy.results_per_provider,
            )
        except httpx.HTTPStatusError as exc:
            status = int(exc.response.status_code)
            await session.client.provider_report(
                permit,
                status_code=status,
                throttled=status in {429, 503},
                cooldown_seconds=30.0 if status in {429, 503} else 0.0,
            )
            raise
        except BaseException:
            await session.client.provider_report(
                permit,
                status_code=None,
            )
            raise
        elapsed = max(0.0, time.perf_counter() - started)
        await session.client.provider_report(
            permit,
            status_code=200,
        )
        await session.commit_batch(
            [
                {
                    "kind": "RESIDUAL_PROVIDER_RESULT",
                    "provider": provider_name,
                    "search_cost_seconds": elapsed,
                    "results": [raw_result_payload(item) for item in results],
                }
            ],
            cursor_after={"done": True},
        )


class ResidualAuthorityReducer:
    """Authority-only fan-in reducer preserving current residual semantics."""

    def __init__(
        self,
        fabric_store,
        registry: SourceDiscoveryRegistry,
        coverage: ResidualSearchLedger,
        identities: SearchIdentityLedger,
        *,
        policy: DeterministicSearchPolicy | None = None,
        research_leads: ResearchLeadLedger | None = None,
        candidate_cap: int = 16,
    ) -> None:
        if candidate_cap < 1:
            raise ValueError("candidate_cap must be positive")
        self.fabric_store = fabric_store
        self.registry = registry
        self.coverage = coverage
        self.identities = identities
        self.policy = policy or DeterministicSearchPolicy()
        self.research_leads = research_leads
        self.candidate_cap = int(candidate_cap)

    @staticmethod
    def authority_worker_descriptor(
        worker_id: str = "authority-residual-reducer",
    ) -> WorkerDescriptor:
        return WorkerDescriptor(
            worker_id=worker_id,
            region="authority",
            runtime_class="authority",
            architecture="authority",
            network_class="private",
            cpu_count=1,
            memory_bytes=0,
            capabilities=(FabricCapability.AUTHORITY_REDUCE,),
            allowed_providers=(),
            max_concurrency=1,
            edition="authority-local",
        )

    def _collect_parent_results(
        self,
        task_id: str,
        expected_providers: tuple[str, ...],
    ) -> tuple[dict[str, tuple[RawSearchResult, ...]], float]:
        dependencies = self.fabric_store.dependency_results(task_id)
        buckets: dict[str, tuple[RawSearchResult, ...]] = {}
        total_cost = 0.0
        for _work_key, batches in dependencies.items():
            if len(batches) != 1:
                raise RuntimeError(
                    "residual provider slice must commit exactly one batch"
                )
            payload = batches[0]["payload"]
            if not isinstance(payload, Mapping):
                raise RuntimeError("invalid residual parent payload")
            rows = payload.get("results")
            if not isinstance(rows, list) or len(rows) != 1:
                raise RuntimeError(
                    "residual provider slice must emit one result envelope"
                )
            envelope = rows[0]
            if not isinstance(envelope, Mapping):
                raise RuntimeError("invalid residual result envelope")
            if str(envelope.get("kind", "")) != "RESIDUAL_PROVIDER_RESULT":
                raise RuntimeError("unexpected residual result kind")
            provider = str(envelope.get("provider", ""))
            if provider in buckets:
                raise RuntimeError("duplicate residual provider result")
            raw_rows = envelope.get("results", ())
            if not isinstance(raw_rows, list) or any(
                not isinstance(item, Mapping) for item in raw_rows
            ):
                raise RuntimeError("invalid residual provider result list")
            buckets[provider] = tuple(
                raw_result_from_payload(item) for item in raw_rows
            )
            total_cost += max(
                0.0,
                float(envelope.get("search_cost_seconds", 0.0)),
            )

        expected = tuple(dict.fromkeys(expected_providers))
        if set(buckets) != set(expected) or len(buckets) != len(expected):
            raise RuntimeError(
                "residual reducer requires exactly one successful result from "
                "every configured provider"
            )
        return buckets, total_cost

    def reduce_lease(self, lease) -> AtomicResidualCommitResult:
        if lease.work.work_class is not FabricWorkClass.REDUCE_COMMIT:
            raise PermanentTaskError("reducer received wrong work class")
        coverage = dict(lease.work.coverage)
        raw_plan = coverage.get("plan")
        providers_raw = coverage.get("providers")
        if not isinstance(raw_plan, Mapping) or not isinstance(
            providers_raw, list
        ):
            raise PermanentTaskError("invalid residual reducer coverage")
        providers = tuple(str(item) for item in providers_raw)
        plan = query_plan_from_payload(raw_plan)
        buckets, total_cost = self._collect_parent_results(
            lease.task_id,
            providers,
        )

        # Match DeterministicSearchExecutor: provider round-robin first, global
        # cap second, central classification last.
        raw_results: list[RawSearchResult] = []
        max_depth = max((len(buckets[name]) for name in providers), default=0)
        for rank in range(max_depth):
            for provider in providers:
                items = buckets[provider]
                if rank >= len(items):
                    continue
                raw_results.append(items[rank])
                if len(raw_results) >= self.policy.max_total_results:
                    break
            if len(raw_results) >= self.policy.max_total_results:
                break

        canonical = []
        for result in raw_results:
            try:
                classified = classify_result(
                    plan,
                    result,
                    policy=self.policy,
                )
            except ValueError:
                continue
            if classified is not None:
                canonical.append(classified)

        batch = DeterministicSearchBatch(
            backend="+".join(providers),
            query=plan.query,
            actor="fabric:residual-reducer",
            results=tuple(canonical),
            search_cost_seconds=total_cost,
        )
        return commit_deterministic_residual_batch(
            self.registry,
            self.coverage,
            self.identities,
            plan=plan,
            batch=batch,
            search_cost_seconds=total_cost,
            candidate_cap=self.candidate_cap,
            research_leads=self.research_leads,
        )

    def run_once(
        self,
        *,
        worker_id: str = "authority-residual-reducer",
        lease_seconds: float = 60.0,
    ) -> AtomicResidualCommitResult | None:
        descriptor = self.authority_worker_descriptor(worker_id)
        self.fabric_store.register_worker(descriptor)
        lease = self.fabric_store.claim(
            worker_id,
            queue="authority",
            lease_seconds=lease_seconds,
        )
        if lease is None:
            return None
        try:
            result = self.reduce_lease(lease)
            self.fabric_store.complete(lease)
            return result
        except BaseException as exc:
            try:
                self.fabric_store.fail(
                    lease,
                    error=f"{type(exc).__name__}: {exc}",
                    retryable=False,
                )
            except BaseException:
                pass
            raise
