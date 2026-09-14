"""Multi-archive CDX provider pool.

The durable evidence queue keeps one logical provider identity ("wayback") so a
hostname/year task exists only once. This pool maps that logical task onto
multiple independent CDX Server endpoints. Different logical tasks are spread
across endpoints with weighted rendezvous ordering; a single logical task never
queries two endpoints concurrently.

Positive evidence from any archive is sufficient. Negative authority is only
returned after every currently usable endpoint has completed the exact-year
query exhaustively. Range probes deliberately use one endpoint and decompose
missing years into exact-year tasks, avoiding redundant wide scans while still
recovering recall across the full pool.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
from collections import Counter
from dataclasses import dataclass, replace
from typing import Mapping, Sequence

from creeper.evidence.policies import (
    CDXQueryState,
    DomainEvidenceQueryResult,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
)

from .async_cdx import AsyncWaybackCDXClient


@dataclass(frozen=True)
class CDXServiceConfig:
    name: str
    endpoint: str
    requests_per_second: float = 0.5
    max_inflight: int = 2
    weight: float = 1.0

    def __post_init__(self) -> None:
        name = self.name.strip().lower()
        endpoint = self.endpoint.strip()
        if not name or not endpoint:
            raise ValueError("CDX service name and endpoint are required")
        if self.requests_per_second < 0:
            raise ValueError("CDX service RPS must be non-negative")
        if self.max_inflight < 1:
            raise ValueError("CDX service max_inflight must be positive")
        if not math.isfinite(self.weight) or self.weight <= 0:
            raise ValueError("CDX service weight must be finite and positive")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "endpoint", endpoint)


# These are bootstrap CDX Server surfaces, not evidence authority declarations.
# Each response is still validated locally by AsyncWaybackCDXClient.
DEFAULT_CDX_SERVICE_CONFIGS: tuple[CDXServiceConfig, ...] = (
    CDXServiceConfig(
        "wayback",
        "https://web.archive.org/cdx/search/cdx",
        requests_per_second=0.5,
        max_inflight=4,
        weight=4.0,
    ),
    CDXServiceConfig(
        "arquivo",
        "https://arquivo.pt/wayback/cdx",
        requests_per_second=0.5,
        max_inflight=3,
        weight=3.0,
    ),
    CDXServiceConfig(
        "stanford",
        "https://swap.stanford.edu/was/cdx",
        requests_per_second=0.25,
        max_inflight=2,
        weight=1.5,
    ),
    CDXServiceConfig(
        "icelandic",
        "https://vefsafn.is/is/cdx",
        requests_per_second=0.25,
        max_inflight=2,
        weight=1.0,
    ),
    CDXServiceConfig(
        "estonian",
        "https://veebiarhiiv.digar.ee/a/cdx",
        requests_per_second=0.25,
        max_inflight=2,
        weight=1.0,
    ),
)


class AsyncCDXProviderPool:
    """One logical CDX provider backed by multiple independent archives."""

    provider = "wayback"

    _NUMERIC_METRICS = frozenset(
        {
            "http_requests",
            "throttle_responses",
            "transport_errors",
            "circuit_open_events",
            "circuit_fast_failures",
            "http_elapsed_milliseconds",
            "cooldown_wait_milliseconds",
            "rate_limit_wait_milliseconds",
            "retry_backoff_wait_milliseconds",
            "request_start_segments",
            "request_start_gaps",
            "request_start_gap_milliseconds",
            "request_start_excess_gap_milliseconds",
        }
    )
    _COUNTER_METRICS = frozenset(
        {
            "transport_error_counts",
            "http_latency_buckets",
            "http_status_counts",
            "request_start_gap_buckets",
        }
    )

    def __init__(
        self,
        services: Mapping[str, AsyncWaybackCDXClient],
        *,
        configs: Sequence[CDXServiceConfig],
        logical_provider: str = "wayback",
    ) -> None:
        self.provider = logical_provider
        self.services = dict(services)
        self.configs = {item.name: item for item in configs}
        if not self.provider:
            raise ValueError("logical provider is required")
        if not self.services:
            raise ValueError("at least one CDX service is required")
        if set(self.services) != set(self.configs):
            raise ValueError("CDX service clients/configs must have identical names")
        for name, client in self.services.items():
            if client.provider != name:
                raise ValueError("CDX child client provider must equal service name")
        self._semaphores = {
            name: asyncio.Semaphore(self.configs[name].max_inflight)
            for name in self.services
        }
        self._disabled: set[str] = set()
        self.service_attempts: Counter[str] = Counter()
        self.service_passes: Counter[str] = Counter()
        self.service_empty_exhaustive: Counter[str] = Counter()
        self.service_transient_errors: Counter[str] = Counter()

    @property
    def total_max_inflight(self) -> int:
        return sum(
            self.configs[name].max_inflight
            for name in self.services
            if name not in self._disabled
        )

    @property
    def total_configured_requests_per_second(self) -> float:
        return sum(
            self.configs[name].requests_per_second
            for name in self.services
            if name not in self._disabled
        )

    @property
    def active_service_names(self) -> tuple[str, ...]:
        return tuple(
            name for name in self.services
            if name not in self._disabled
        )

    def __getattr__(self, name: str):
        if name in self._NUMERIC_METRICS:
            return sum(
                int(getattr(client, name, 0))
                for client in self.services.values()
            )
        if name in self._COUNTER_METRICS:
            total: Counter = Counter()
            for client in self.services.values():
                total.update(getattr(client, name, {}))
            return total
        raise AttributeError(name)

    @staticmethod
    def _identity(key: EvidenceQueryKey) -> str:
        scope = key.temporal_scope
        return (
            f"{key.hostname}\0{scope.year_from}\0{scope.year_to}"
            f"\0{key.policy_version}"
        )

    def _ordered_names(self, key: EvidenceQueryKey) -> tuple[str, ...]:
        """Weighted rendezvous ordering spreads primaries without duplication."""
        identity = self._identity(key)
        scores: list[tuple[float, str]] = []
        for name in self.active_service_names:
            digest = hashlib.blake2b(
                f"{identity}\0{name}".encode("utf-8"),
                digest_size=8,
            ).digest()
            raw = int.from_bytes(digest, "big")
            # Weighted rendezvous: lower exponential race time wins.
            u = (raw + 1.0) / ((1 << 64) + 1.0)
            score = -math.log(u) / self.configs[name].weight
            scores.append((score, name))
        return tuple(name for _score, name in sorted(scores))

    @staticmethod
    def _child_key(key: EvidenceQueryKey, provider: str) -> EvidenceQueryKey:
        return EvidenceQueryKey(
            key.hostname,
            key.temporal_scope,
            provider,
            key.policy_version,
        )

    def _map_capsule(
        self,
        capsule: EvidenceCapsule,
        *,
        key: EvidenceQueryKey,
        service_name: str,
    ) -> EvidenceCapsule:
        record_locator = capsule.record_locator
        if not record_locator.startswith(service_name + ":"):
            record_locator = f"{service_name}:{record_locator}"
        return replace(
            capsule,
            provider=key.provider,
            source_id=service_name,
            record_locator=record_locator,
            extraction_method=(
                f"{capsule.extraction_method}|cdx_service={service_name}"
            ),
        )

    async def _exact_from(
        self,
        name: str,
        key: EvidenceQueryKey,
    ) -> EvidenceQueryResult:
        child = self.services[name]
        child_key = self._child_key(key, name)
        self.service_attempts[name] += 1
        async with self._semaphores[name]:
            result = await child.query_key(child_key)
        if result.state is CDXQueryState.PASS:
            self.service_passes[name] += 1
        elif result.state is CDXQueryState.EMPTY_EXHAUSTIVE:
            self.service_empty_exhaustive[name] += 1
        elif result.state is CDXQueryState.TRANSIENT_ERROR:
            self.service_transient_errors[name] += 1
        elif result.state is CDXQueryState.INVALID:
            # An invalid protocol/response from a configured endpoint cannot
            # establish negative evidence. Stop assigning new tasks to it for
            # this process; a restart/probe may rehabilitate it later.
            self._disabled.add(name)
        return result

    async def query_key(self, key: EvidenceQueryKey) -> EvidenceQueryResult:
        if key.provider != self.provider:
            raise ValueError("logical CDX pool received a mismatched provider key")
        names = self._ordered_names(key)
        if not names:
            return EvidenceQueryResult(
                key.hostname,
                key.temporal_scope.year_from,
                CDXQueryState.TRANSIENT_ERROR,
                error="no active CDX services",
                key=key,
            )

        pages = records = requests = elapsed = 0
        errors: list[str] = []
        saw_non_exhaustive = False
        year = key.temporal_scope.year_from
        for name in names:
            result = await self._exact_from(name, key)
            pages += result.pages_seen
            records += result.records_seen
            requests += result.provider_requests
            elapsed += result.provider_elapsed_milliseconds
            if result.state is CDXQueryState.PASS and result.capsule is not None:
                return EvidenceQueryResult(
                    key.hostname,
                    year,
                    CDXQueryState.PASS,
                    capsule=self._map_capsule(
                        result.capsule,
                        key=key,
                        service_name=name,
                    ),
                    pages_seen=pages,
                    records_seen=records,
                    provider_requests=requests,
                    provider_elapsed_milliseconds=elapsed,
                    key=key,
                )
            if result.state is CDXQueryState.EMPTY_EXHAUSTIVE:
                continue
            saw_non_exhaustive = True
            if result.error:
                errors.append(f"{name}:{result.error}")

        if saw_non_exhaustive:
            state = (
                CDXQueryState.TRANSIENT_ERROR
                if errors
                else CDXQueryState.INCOMPLETE
            )
            error = "; ".join(errors) or "one or more CDX services were incomplete"
        else:
            state = CDXQueryState.EMPTY_EXHAUSTIVE
            error = None
        return EvidenceQueryResult(
            key.hostname,
            year,
            state,
            pages_seen=pages,
            records_seen=records,
            provider_requests=requests,
            provider_elapsed_milliseconds=elapsed,
            error=error,
            key=key,
        )

    async def _one_range_service(
        self,
        name: str,
        key: EvidenceQueryKey,
    ) -> RangeEvidenceQueryResult | DomainEvidenceQueryResult:
        child = self.services[name]
        child_key = self._child_key(key, name)
        self.service_attempts[name] += 1
        async with self._semaphores[name]:
            result = await child.query_range(child_key)
        if result.state is CDXQueryState.TRANSIENT_ERROR:
            self.service_transient_errors[name] += 1
        elif result.state is CDXQueryState.INVALID:
            self._disabled.add(name)
        elif result.capsules:
            self.service_passes[name] += 1
        elif result.state is CDXQueryState.EMPTY_EXHAUSTIVE:
            self.service_empty_exhaustive[name] += 1
        return result

    async def query_range(
        self,
        key: EvidenceQueryKey,
    ) -> RangeEvidenceQueryResult | DomainEvidenceQueryResult:
        if key.provider != self.provider:
            raise ValueError("logical CDX pool received a mismatched provider key")
        names = self._ordered_names(key)
        if not names:
            if key.policy_version.startswith("cdx-domain-"):
                return DomainEvidenceQueryResult(
                    domain=key.hostname,
                    key=key,
                    state=CDXQueryState.TRANSIENT_ERROR,
                    error="no active CDX services",
                )
            return RangeEvidenceQueryResult(
                hostname=key.hostname,
                key=key,
                state=CDXQueryState.TRANSIENT_ERROR,
                error="no active CDX services",
            )

        pages = records = requests = elapsed = 0
        errors: list[str] = []
        for name in names:
            result = await self._one_range_service(name, key)
            pages += result.pages_seen
            records += result.records_seen
            requests += result.provider_requests
            elapsed += result.provider_elapsed_milliseconds
            if result.state in {
                CDXQueryState.TRANSIENT_ERROR,
                CDXQueryState.INVALID,
                CDXQueryState.INCOMPLETE,
            }:
                if result.error:
                    errors.append(f"{name}:{result.error}")
                continue

            mapped = tuple(
                self._map_capsule(capsule, key=key, service_name=name)
                for capsule in result.capsules
            )
            if isinstance(result, DomainEvidenceQueryResult):
                # Domain amplification has no negative authority. One usable
                # archive is enough; different domain tasks are distributed
                # across the pool by rendezvous ordering.
                return DomainEvidenceQueryResult(
                    domain=key.hostname,
                    key=key,
                    state=CDXQueryState.DECOMPOSED,
                    capsules=mapped,
                    pages_seen=pages,
                    records_seen=records,
                    provider_requests=requests,
                    provider_elapsed_milliseconds=elapsed,
                )

            positive = tuple(sorted({capsule.year for capsule in mapped}))
            expected = tuple(
                range(
                    key.temporal_scope.year_from,
                    key.temporal_scope.year_to + 1,
                )
            )
            missing = tuple(year for year in expected if year not in positive)
            if not missing:
                return RangeEvidenceQueryResult(
                    hostname=key.hostname,
                    key=key,
                    state=CDXQueryState.PASS,
                    candidate_years=positive,
                    capsules=mapped,
                    pages_seen=pages,
                    records_seen=records,
                    provider_requests=requests,
                    provider_elapsed_milliseconds=elapsed,
                )

            # Never let one archive's negative range coverage suppress another
            # archive. Exact-year children use the full pool and stop on the
            # first positive result.
            return RangeEvidenceQueryResult(
                hostname=key.hostname,
                key=key,
                state=CDXQueryState.DECOMPOSED,
                candidate_years=positive,
                followup_years=missing,
                capsules=mapped,
                pages_seen=pages,
                records_seen=records,
                provider_requests=requests,
                provider_elapsed_milliseconds=elapsed,
            )

        if key.policy_version.startswith("cdx-domain-"):
            return DomainEvidenceQueryResult(
                domain=key.hostname,
                key=key,
                state=CDXQueryState.TRANSIENT_ERROR,
                pages_seen=pages,
                records_seen=records,
                provider_requests=requests,
                provider_elapsed_milliseconds=elapsed,
                error="; ".join(errors) or "no usable CDX domain service",
            )
        return RangeEvidenceQueryResult(
            hostname=key.hostname,
            key=key,
            state=CDXQueryState.TRANSIENT_ERROR,
            pages_seen=pages,
            records_seen=records,
            provider_requests=requests,
            provider_elapsed_milliseconds=elapsed,
            error="; ".join(errors) or "no usable CDX range service",
        )
