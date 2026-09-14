"""Distributed HOST_BATCH producer backed by the existing multi-CDX pool."""

from __future__ import annotations

from collections.abc import Mapping

import httpx

from creeper.authority.normalizer import normalize_official
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.lease_keeper import LeaseKeeper
from creeper.distributed.models import TaskClass, TaskLease
from creeper.distributed.provider_gate import DistributedProviderGate
from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    RangeEvidenceQueryResult,
    TemporalScope,
)
from creeper.evidence.providers.async_cdx import AsyncWaybackCDXClient
from creeper.evidence.providers.multi_cdx import (
    AsyncArquivoCDXClient,
    AsyncCDXProviderPool,
    CDXProviderConfig,
)


class IncompleteHostResolution(RuntimeError):
    """At least one target year lacks exhaustive provider coverage."""


def build_distributed_cdx_pool(
    configs: tuple[CDXProviderConfig, ...],
    coordinator: CoordinatorClient,
    keeper: LeaseKeeper,
    *,
    transports: Mapping[str, httpx.AsyncBaseTransport] | None = None,
    logical_provider: str = "wayback",
) -> AsyncCDXProviderPool:
    if not configs:
        raise ValueError("at least one CDX provider is required")
    transport_map = dict(transports or {})
    clients: dict[str, AsyncWaybackCDXClient] = {}
    inflight: dict[str, int] = {}
    weights: dict[str, float] = {}
    for config in configs:
        if config.name in clients:
            raise ValueError(f"duplicate CDX provider name: {config.name}")
        gate = DistributedProviderGate(
            coordinator,
            keeper,
            config.name,
            throttle_floor_seconds=config.throttle_floor_seconds,
        )
        client_type = (
            AsyncArquivoCDXClient
            if config.dialect == "arquivo"
            else AsyncWaybackCDXClient
        )
        clients[config.name] = client_type(
            endpoint=config.endpoint,
            provider=logical_provider,
            source_id=config.name,
            limit=config.row_limit,
            timeout=config.timeout,
            max_retries=config.max_retries,
            requests_per_second=0.0,
            # Global Authority pacing is the source of truth in distributed
            # mode. A second local RPS limiter would only reduce utilization.
            max_connections=config.max_connections,
            max_keepalive_connections=config.max_keepalive_connections,
            keepalive_expiry_seconds=config.keepalive_expiry_seconds,
            throttle_floor_seconds=config.throttle_floor_seconds,
            transport=transport_map.get(config.name),
            request_permit=gate.acquire,
            request_report=gate.report,
        )
        inflight[config.name] = config.max_inflight
        weights[config.name] = config.weight
    pool = AsyncCDXProviderPool(
        clients,
        logical_provider=logical_provider,
        inflight=inflight,
        weights=weights,
        owns_clients=True,
    )
    pool.configs = {config.name: config for config in configs}
    return pool


class DistributedHostQueryProducer:
    """Resolve one hostname across a target interval and admit only novel HYs."""

    def __init__(
        self,
        configs: tuple[CDXProviderConfig, ...],
        *,
        policy_version: str = "cdx-v1",
        transports: Mapping[str, httpx.AsyncBaseTransport] | None = None,
    ) -> None:
        if not configs or not policy_version.strip():
            raise ValueError("distributed host query provider config is required")
        self.configs = configs
        self.policy_version = policy_version
        self.transports = dict(transports or {})

    async def __call__(
        self,
        lease: TaskLease,
        coordinator: CoordinatorClient,
        keeper: LeaseKeeper,
    ) -> None:
        if lease.work.task_class is not TaskClass.HOST_BATCH:
            raise ValueError("DistributedHostQueryProducer requires HOST_BATCH")
        hostname = normalize_official(lease.work.input_identity)
        if hostname is None:
            raise ValueError("HOST_BATCH input_identity must be a hostname")
        coverage = dict(lease.work.coverage)
        year_from = int(coverage.get("year_from", 0))
        year_to = int(coverage.get("year_to", 0))
        if not 1996 <= year_from <= year_to <= 2001:
            raise ValueError("HOST_BATCH year coverage must be within 1996-2001")

        keeper.assert_owned()
        pool = build_distributed_cdx_pool(
            self.configs,
            coordinator,
            keeper,
            transports=self.transports,
        )
        try:
            key = EvidenceQueryKey(
                hostname,
                TemporalScope(year_from, year_to),
                "wayback",
                self.policy_version,
            )
            raw = await pool.query_range(key)
        finally:
            await pool.aclose()

        if not isinstance(raw, RangeEvidenceQueryResult):
            raise ValueError("HOST_BATCH unexpectedly returned a domain result")

        keeper.assert_owned()
        capsules = list(raw.capsules)
        if capsules:
            decisions = await coordinator.hy_probe(
                keeper.lease,
                [
                    {
                        "hostname": capsule.hostname,
                        "year": capsule.year,
                        "locator": (
                            capsule.record_locator
                            or capsule.source_locator
                            or capsule.original_url
                        ),
                    }
                    for capsule in capsules
                ],
            )
            need = {
                (decision.hostname, decision.year)
                for decision in decisions
                if decision.status == "NEED_FULL_EVIDENCE"
            }
            if need:
                full = []
                for capsule in capsules:
                    if (capsule.hostname, capsule.year) not in need:
                        continue
                    full.append(
                        {
                            "hostname": capsule.hostname,
                            "year": capsule.year,
                            "evidence_class": capsule.evidence_type,
                            "source": capsule.source_id or capsule.provider,
                            "timestamp": capsule.evidence_timestamp,
                            "locator": (
                                capsule.record_locator
                                or capsule.source_locator
                                or capsule.original_url
                            ),
                            "original_url": capsule.original_url,
                            "provider": capsule.provider,
                            "policy_version": capsule.policy_version,
                            "payload_hash": capsule.payload_hash,
                            "extraction_method": capsule.extraction_method,
                        }
                    )
                await coordinator.hy_full(keeper.lease, full)

        # PASS with no followups means every missing year is a legitimate
        # provider-exhaustive negative. EMPTY_EXHAUSTIVE is likewise complete.
        if raw.state in {
            CDXQueryState.PASS,
            CDXQueryState.EMPTY_EXHAUSTIVE,
        } and not raw.followup_years:
            return

        # Positive evidence may already have been admitted above, but this task
        # must not be marked complete while any provider coverage is unresolved.
        raise IncompleteHostResolution(
            f"host resolution incomplete: state={raw.state.value}, "
            f"followup_years={raw.followup_years}"
        )
