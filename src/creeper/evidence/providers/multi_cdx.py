"""Deterministic multi-endpoint CDX provider pool.

One logical evidence lane ("wayback") fans independent tasks across multiple
physical CDX services. A task has exactly one primary physical provider chosen
by weighted rendezvous hashing. Other providers are contacted only as ordered
fallbacks, so the pool can use all configured network budgets concurrently
without issuing duplicate requests for the same task in parallel.

Physical provenance is preserved in EvidenceCapsule.source_id while the
capsule.provider remains the logical queue provider required by the durable
EvidenceQueryKey contract.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import math
from typing import Any

from creeper.evidence.policies import (
    CDXQueryState,
    DomainEvidenceQueryResult,
    EvidenceQueryKey,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
    TemporalScope,
)
from creeper.evidence.providers.async_cdx import (
    AsyncWaybackCDXClient,
    _RequestAccounting,
)


@dataclass(frozen=True)
class CDXProviderConfig:
    """One physical CDX service with an independent rate/concurrency budget."""

    name: str
    endpoint: str
    requests_per_second: float = 0.5
    max_inflight: int = 4
    max_connections: int = 8
    max_keepalive_connections: int = 4
    keepalive_expiry_seconds: float = 30.0
    throttle_floor_seconds: float = 2.0
    timeout: float = 30.0
    max_retries: int = 3
    weight: float = 1.0
    dialect: str = "wayback"
    row_limit: int = 150_000

    def __post_init__(self) -> None:
        for field_name, value in (("name", self.name), ("endpoint", self.endpoint)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"CDX provider {field_name} is required")
        integer_limits = (
            ("row_limit", self.row_limit, 1),
            ("max_inflight", self.max_inflight, 1),
            ("max_connections", self.max_connections, 1),
            ("max_keepalive_connections", self.max_keepalive_connections, 0),
            ("max_retries", self.max_retries, 0),
        )
        for field_name, value, minimum in integer_limits:
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < minimum
            ):
                raise ValueError(
                    f"CDX provider {field_name} must be an integer >= {minimum}"
                )
        if self.max_keepalive_connections > self.max_connections:
            raise ValueError("invalid CDX provider keepalive connection limit")

        nonnegative_floats = (
            ("requests_per_second", self.requests_per_second),
            ("throttle_floor_seconds", self.throttle_floor_seconds),
        )
        for field_name, value in nonnegative_floats:
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise ValueError(
                    f"CDX provider {field_name} must be finite and non-negative"
                )
        positive_floats = (
            ("keepalive_expiry_seconds", self.keepalive_expiry_seconds),
            ("timeout", self.timeout),
            ("weight", self.weight),
        )
        for field_name, value in positive_floats:
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise ValueError(
                    f"CDX provider {field_name} must be finite and positive"
                )
        if not isinstance(self.dialect, str) or self.dialect not in {
            "wayback",
            "arquivo",
        }:
            raise ValueError("unsupported CDX provider dialect")

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        defaults: "CDXProviderConfig | None" = None,
    ) -> "CDXProviderConfig":
        if not isinstance(value, Mapping):
            raise ValueError("CDX provider specification must be an object")
        base = defaults or cls(
            name="default",
            endpoint="https://web.archive.org/cdx/search/cdx",
        )

        def item(name: str, fallback: Any) -> Any:
            return value.get(name, fallback)

        provider_name = item("name", item("id", base.name))
        return cls(
            name=provider_name,
            endpoint=item("endpoint", base.endpoint),
            requests_per_second=item(
                "requests_per_second", base.requests_per_second
            ),
            max_inflight=item("max_inflight", base.max_inflight),
            max_connections=item("max_connections", base.max_connections),
            max_keepalive_connections=item(
                "max_keepalive_connections",
                base.max_keepalive_connections,
            ),
            keepalive_expiry_seconds=item(
                "keepalive_expiry_seconds", base.keepalive_expiry_seconds
            ),
            throttle_floor_seconds=item(
                "throttle_floor_seconds", base.throttle_floor_seconds
            ),
            timeout=item("timeout", base.timeout),
            max_retries=item("max_retries", base.max_retries),
            weight=item("weight", base.weight),
            dialect=item("dialect", base.dialect),
            row_limit=item("row_limit", base.row_limit),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "endpoint": self.endpoint,
            "requests_per_second": self.requests_per_second,
            "max_inflight": self.max_inflight,
            "max_connections": self.max_connections,
            "max_keepalive_connections": self.max_keepalive_connections,
            "keepalive_expiry_seconds": self.keepalive_expiry_seconds,
            "throttle_floor_seconds": self.throttle_floor_seconds,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "weight": self.weight,
            "dialect": self.dialect,
            "row_limit": self.row_limit,
        }


class AsyncArquivoCDXClient(AsyncWaybackCDXClient):
    """Arquivo.pt CDX dialect with the common Creeper evidence contract.

    Arquivo uses year-valued from/to parameters, the fields selector, and
    object-shaped JSON rows. It does not expose Wayback resume-key semantics on
    this endpoint, so a full page is deliberately treated as incomplete.
    """

    @staticmethod
    def _parse_arquivo_payload(payload: bytes) -> list[dict[str, object]]:
        text = payload.decode("utf-8", errors="replace").strip()
        if not text:
            return []
        try:
            import json
            value = json.loads(text)
        except json.JSONDecodeError:
            rows: list[dict[str, object]] = []
            for line in text.splitlines():
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    rows.append(item)
            return rows
        rows: list[dict[str, object]] = []
        if isinstance(value, dict):
            rows.append(dict(value))
        elif isinstance(value, list):
            if value and isinstance(value[0], list):
                header = value[0]
                if not all(isinstance(field, str) for field in header):
                    return []
                for raw in value[1:]:
                    if not isinstance(raw, list):
                        continue
                    rows.append(
                        {
                            str(field): raw[index]
                            for index, field in enumerate(header)
                            if index < len(raw)
                        }
                    )
            else:
                rows.extend(
                    dict(item) for item in value if isinstance(item, dict)
                )
        normalized_rows: list[dict[str, object]] = []
        for item in rows:
            row = dict(item)
            if "original" not in row and "url" in row:
                row["original"] = row["url"]
            if "statuscode" not in row and "status" in row:
                row["statuscode"] = row["status"]
            if "mimetype" not in row and "mime" in row:
                row["mimetype"] = row["mime"]
            normalized_rows.append(row)
        return normalized_rows

    async def iter_range_pages(
        self,
        hostname: str,
        year_from: int,
        year_to: int,
        *,
        page_limit: int | None = None,
        accounting: _RequestAccounting | None = None,
    ):
        if not 1996 <= year_from <= year_to <= 2001:
            raise ValueError("year range must be within 1996-2001")
        effective_limit = self.limit if page_limit is None else int(page_limit)
        if effective_limit < 1:
            raise ValueError("page_limit must be positive")
        params = {
            "url": f"http://{hostname}/",
            "matchType": "host",
            "from": str(year_from),
            "to": str(year_to),
            "output": "json",
            "fl": "url,timestamp,status,mime,digest,length,offset,filename",
            # Arquivo's CDX dialect uses pywb-style filter operators. Filter
            # before limit so exact-year page_limit=1 cannot repeatedly select
            # an unusable 4xx/5xx capture ahead of later valid evidence.
            "filter": "~status:[23][0-9][0-9]",
            "limit": str(effective_limit),
        }
        response = await self._get(params, accounting=accounting)
        rows = self._parse_arquivo_payload(response.content)
        yield rows, len(rows) < effective_limit


class AsyncCDXProviderPool:
    """One logical provider backed by multiple independently throttled CDX APIs."""

    def __init__(
        self,
        clients: Mapping[str, AsyncWaybackCDXClient],
        *,
        logical_provider: str = "wayback",
        inflight: Mapping[str, int] | None = None,
        weights: Mapping[str, float] | None = None,
        owns_clients: bool = False,
    ) -> None:
        if not logical_provider.strip():
            raise ValueError("logical_provider is required")
        if not clients:
            raise ValueError("at least one physical CDX provider is required")
        names = tuple(clients)
        if any(not name.strip() for name in names):
            raise ValueError("physical CDX provider names must be non-empty")
        if len(set(names)) != len(names):
            raise ValueError("physical CDX provider names must be unique")
        self.provider = logical_provider
        self.clients = dict(clients)
        self._owns_clients = owns_clients
        self._closed = False

        configured_inflight = dict(inflight or {})
        configured_weights = dict(weights or {})
        self._inflight: dict[str, int] = {}
        self._weights: dict[str, float] = {}
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        for name, client in self.clients.items():
            if client.provider != self.provider:
                raise ValueError(
                    "physical CDX clients must share the logical queue provider"
                )
            limit = int(configured_inflight.get(name, 1))
            weight = float(configured_weights.get(name, 1.0))
            if limit < 1 or weight <= 0:
                raise ValueError("CDX provider inflight and weight must be positive")
            self._inflight[name] = limit
            self._weights[name] = weight
            self._semaphores[name] = asyncio.Semaphore(limit)

        self.primary_assignments: Counter[str] = Counter()
        self.provider_attempts: Counter[str] = Counter()
        self.provider_passes: Counter[str] = Counter()
        self.provider_empty_exhaustive: Counter[str] = Counter()
        self.provider_retryable: Counter[str] = Counter()
        self.failover_attempts = 0

    @classmethod
    def from_configs(
        cls,
        configs: tuple[CDXProviderConfig, ...],
        *,
        logical_provider: str = "wayback",
    ) -> "AsyncCDXProviderPool":
        if not configs:
            raise ValueError("at least one CDX provider config is required")
        clients: dict[str, AsyncWaybackCDXClient] = {}
        inflight: dict[str, int] = {}
        weights: dict[str, float] = {}
        for config in configs:
            if config.name in clients:
                raise ValueError(f"duplicate CDX provider name: {config.name}")
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
                requests_per_second=config.requests_per_second,
                max_connections=config.max_connections,
                max_keepalive_connections=config.max_keepalive_connections,
                keepalive_expiry_seconds=config.keepalive_expiry_seconds,
                throttle_floor_seconds=config.throttle_floor_seconds,
            )
            inflight[config.name] = config.max_inflight
            weights[config.name] = config.weight
        pool = cls(
            clients,
            logical_provider=logical_provider,
            inflight=inflight,
            weights=weights,
            owns_clients=True,
        )
        pool.configs = {config.name: config for config in configs}
        return pool

    async def __aenter__(self) -> "AsyncCDXProviderPool":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_clients:
            await asyncio.gather(
                *(client.aclose() for client in self.clients.values()),
                return_exceptions=False,
            )

    @property
    def total_max_inflight(self) -> int:
        return sum(self._inflight.values())

    @property
    def requests_per_second(self) -> float:
        return sum(float(client.requests_per_second) for client in self.clients.values())

    @property
    def http_requests(self) -> int:
        return sum(int(client.http_requests) for client in self.clients.values())

    @property
    def throttle_responses(self) -> int:
        return sum(int(client.throttle_responses) for client in self.clients.values())

    @property
    def transport_errors(self) -> int:
        return sum(int(client.transport_errors) for client in self.clients.values())

    @property
    def circuit_open_events(self) -> int:
        return sum(int(client.circuit_open_events) for client in self.clients.values())

    @property
    def circuit_fast_failures(self) -> int:
        return sum(int(client.circuit_fast_failures) for client in self.clients.values())

    @property
    def http_elapsed_milliseconds(self) -> int:
        return sum(
            int(client.http_elapsed_milliseconds) for client in self.clients.values()
        )

    @property
    def cooldown_wait_milliseconds(self) -> int:
        return sum(
            int(client.cooldown_wait_milliseconds) for client in self.clients.values()
        )

    @property
    def rate_limit_wait_milliseconds(self) -> int:
        return sum(
            int(client.rate_limit_wait_milliseconds) for client in self.clients.values()
        )

    @property
    def retry_backoff_wait_milliseconds(self) -> int:
        return sum(
            int(client.retry_backoff_wait_milliseconds)
            for client in self.clients.values()
        )

    @property
    def request_start_segments(self) -> int:
        return sum(int(client.request_start_segments) for client in self.clients.values())

    @property
    def request_start_gaps(self) -> int:
        return sum(int(client.request_start_gaps) for client in self.clients.values())

    @property
    def request_start_gap_milliseconds(self) -> int:
        return sum(
            int(client.request_start_gap_milliseconds)
            for client in self.clients.values()
        )

    @property
    def request_start_excess_gap_milliseconds(self) -> int:
        return sum(
            int(client.request_start_excess_gap_milliseconds)
            for client in self.clients.values()
        )

    @staticmethod
    def _sum_counter(clients: Mapping[str, AsyncWaybackCDXClient], attr: str) -> Counter:
        total: Counter = Counter()
        for client in clients.values():
            total.update(getattr(client, attr))
        return total

    @property
    def transport_error_counts(self) -> Counter:
        return self._sum_counter(self.clients, "transport_error_counts")

    @property
    def http_status_counts(self) -> Counter:
        return self._sum_counter(self.clients, "http_status_counts")

    @property
    def http_latency_buckets(self) -> Counter:
        return self._sum_counter(self.clients, "http_latency_buckets")

    @property
    def request_start_gap_buckets(self) -> Counter:
        return self._sum_counter(self.clients, "request_start_gap_buckets")

    def telemetry_snapshot(self) -> dict[str, dict[str, int | float]]:
        result: dict[str, dict[str, int | float]] = {}
        for name, client in self.clients.items():
            result[name] = {
                "http_requests": int(client.http_requests),
                "throttle_responses": int(client.throttle_responses),
                "transport_errors": int(client.transport_errors),
                "http_elapsed_milliseconds": int(client.http_elapsed_milliseconds),
                "rate_limit_wait_milliseconds": int(
                    client.rate_limit_wait_milliseconds
                ),
                "cooldown_wait_milliseconds": int(
                    client.cooldown_wait_milliseconds
                ),
                "primary_assignments": int(self.primary_assignments[name]),
                "attempts": int(self.provider_attempts[name]),
                "passes": int(self.provider_passes[name]),
                "empty_exhaustive": int(self.provider_empty_exhaustive[name]),
                "retryable": int(self.provider_retryable[name]),
                "configured_requests_per_second": float(
                    client.requests_per_second
                ),
                "max_inflight": int(self._inflight[name]),
                "row_limit": int(client.limit),
            }
        return result

    def _provider_order(self, key: EvidenceQueryKey) -> tuple[str, ...]:
        seed = (
            f"{key.hostname}\0{key.temporal_scope.year_from}\0"
            f"{key.temporal_scope.year_to}\0{key.policy_version}"
        ).encode("utf-8")
        ranked: list[tuple[float, str]] = []
        for name in self.clients:
            digest = hashlib.blake2b(
                seed + b"\0" + name.encode("utf-8"),
                digest_size=8,
            ).digest()
            integer = int.from_bytes(digest, "big")
            uniform = (integer + 1.0) / (2**64 + 1.0)
            score = -math.log(uniform) / self._weights[name]
            ranked.append((score, name))
        ranked.sort()
        order = tuple(name for _score, name in ranked)
        self.primary_assignments[order[0]] += 1
        return order

    async def _call_exact(
        self,
        name: str,
        key: EvidenceQueryKey,
    ) -> EvidenceQueryResult:
        self.provider_attempts[name] += 1
        async with self._semaphores[name]:
            result = await self.clients[name].query_key(key)
        if result.state is CDXQueryState.PASS:
            self.provider_passes[name] += 1
        elif result.state is CDXQueryState.EMPTY_EXHAUSTIVE:
            self.provider_empty_exhaustive[name] += 1
        elif result.state in {
            CDXQueryState.INCOMPLETE,
            CDXQueryState.TRANSIENT_ERROR,
            CDXQueryState.INVALID,
        }:
            self.provider_retryable[name] += 1
        return result

    async def _call_range(
        self,
        name: str,
        key: EvidenceQueryKey,
    ) -> RangeEvidenceQueryResult | DomainEvidenceQueryResult:
        self.provider_attempts[name] += 1
        async with self._semaphores[name]:
            result = await self.clients[name].query_range(key)
        if result.state is CDXQueryState.PASS:
            self.provider_passes[name] += 1
        elif result.state is CDXQueryState.EMPTY_EXHAUSTIVE:
            self.provider_empty_exhaustive[name] += 1
        elif result.state in {
            CDXQueryState.INCOMPLETE,
            CDXQueryState.TRANSIENT_ERROR,
            CDXQueryState.INVALID,
        }:
            self.provider_retryable[name] += 1
        return result

    @staticmethod
    def _error_summary(
        attempts: list[tuple[str, object]],
    ) -> str:
        parts = []
        for name, result in attempts:
            state = getattr(result, "state", "unknown")
            error = getattr(result, "error", None)
            parts.append(f"{name}:{state}" + (f":{error}" if error else ""))
        return "; ".join(parts)

    async def query_key(self, key: EvidenceQueryKey) -> EvidenceQueryResult:
        if key.provider != self.provider:
            raise ValueError("CDX pool provider mismatch")
        order = self._provider_order(key)
        attempts: list[tuple[str, EvidenceQueryResult]] = []
        requests = elapsed = pages = records = 0
        states: list[CDXQueryState] = []
        for index, name in enumerate(order):
            if index:
                self.failover_attempts += 1
            result = await self._call_exact(name, key)
            attempts.append((name, result))
            states.append(result.state)
            requests += result.provider_requests
            elapsed += result.provider_elapsed_milliseconds
            pages += result.pages_seen
            records += result.records_seen
            if result.state is CDXQueryState.PASS:
                return EvidenceQueryResult(
                    hostname=result.hostname,
                    year=result.year,
                    state=result.state,
                    capsule=result.capsule,
                    pages_seen=pages,
                    records_seen=records,
                    provider_requests=requests,
                    provider_elapsed_milliseconds=elapsed,
                    error=result.error,
                    key=key,
                )

        if states and all(state is CDXQueryState.EMPTY_EXHAUSTIVE for state in states):
            state = CDXQueryState.EMPTY_EXHAUSTIVE
        elif states and all(
            state in {CDXQueryState.EMPTY_EXHAUSTIVE, CDXQueryState.INVALID}
            for state in states
        ) and CDXQueryState.INVALID in states:
            # INVALID is permanent. If every provider is already terminal and
            # at least one cannot answer this query, retrying the same logical
            # task cannot improve coverage.
            state = CDXQueryState.INVALID
        elif CDXQueryState.TRANSIENT_ERROR in states:
            state = CDXQueryState.TRANSIENT_ERROR
        else:
            state = CDXQueryState.INCOMPLETE
        return EvidenceQueryResult(
            hostname=key.hostname,
            year=key.temporal_scope.year_from,
            state=state,
            pages_seen=pages,
            records_seen=records,
            provider_requests=requests,
            provider_elapsed_milliseconds=elapsed,
            error=self._error_summary(attempts),
            key=key,
        )

    async def query_range(
        self,
        key: EvidenceQueryKey,
    ) -> RangeEvidenceQueryResult | DomainEvidenceQueryResult:
        if key.provider != self.provider:
            raise ValueError("CDX pool provider mismatch")
        order = self._provider_order(key)
        attempts: list[
            tuple[str, RangeEvidenceQueryResult | DomainEvidenceQueryResult]
        ] = []

        # Domain amplification is intentionally non-exhaustive. One successful
        # physical provider is enough; fallbacks are used only for failures.
        if key.policy_version.startswith("cdx-domain-"):
            requests = elapsed = pages = records = 0
            for index, name in enumerate(order):
                if index:
                    self.failover_attempts += 1
                result = await self._call_range(name, key)
                attempts.append((name, result))
                requests += result.provider_requests
                elapsed += result.provider_elapsed_milliseconds
                pages += result.pages_seen
                records += result.records_seen
                if (
                    isinstance(result, DomainEvidenceQueryResult)
                    and result.state is CDXQueryState.DECOMPOSED
                ):
                    return DomainEvidenceQueryResult(
                        domain=result.domain,
                        key=key,
                        state=result.state,
                        capsules=result.capsules,
                        pages_seen=pages,
                        records_seen=records,
                        provider_requests=requests,
                        provider_elapsed_milliseconds=elapsed,
                        error=result.error,
                    )
            state = (
                CDXQueryState.INVALID
                if attempts
                and all(
                    result.state is CDXQueryState.INVALID
                    for _name, result in attempts
                )
                else CDXQueryState.TRANSIENT_ERROR
            )
            return DomainEvidenceQueryResult(
                domain=key.hostname,
                key=key,
                state=state,
                pages_seen=pages,
                records_seen=records,
                provider_requests=requests,
                provider_elapsed_milliseconds=elapsed,
                error=self._error_summary(attempts),
            )

        expected_years = tuple(
            range(key.temporal_scope.year_from, key.temporal_scope.year_to + 1)
        )
        capsules_by_year = {}
        requests = elapsed = pages = records = 0
        exhaustive_results = 0
        usable_results = 0
        saw_transient = False

        for index, name in enumerate(order):
            if index:
                self.failover_attempts += 1
            raw = await self._call_range(name, key)
            attempts.append((name, raw))
            if not isinstance(raw, RangeEvidenceQueryResult):
                raise ValueError("exact/range CDX task returned domain result")
            requests += raw.provider_requests
            elapsed += raw.provider_elapsed_milliseconds
            pages += raw.pages_seen
            records += raw.records_seen
            for capsule in raw.capsules:
                capsules_by_year.setdefault(capsule.year, capsule)

            if raw.state in {
                CDXQueryState.PASS,
                CDXQueryState.EMPTY_EXHAUSTIVE,
            }:
                exhaustive_results += 1
                usable_results += 1
            elif raw.state is CDXQueryState.DECOMPOSED:
                usable_results += 1
            elif raw.state is CDXQueryState.TRANSIENT_ERROR:
                saw_transient = True

            if len(capsules_by_year) == len(expected_years):
                break

        positive_years = tuple(sorted(capsules_by_year))
        missing_years = tuple(
            year for year in expected_years if year not in capsules_by_year
        )
        all_physical_exhaustive = (
            len(attempts) == len(order)
            and exhaustive_results == len(order)
        )

        # Host-first closure: a range task remains one durable backlog object.
        # If range pagination cannot prove every missing year, resolve only
        # those years through bounded exact queries *inside this logical task*
        # instead of expanding six durable children and pre-reserving them.
        resolved_years = set(positive_years)
        if all_physical_exhaustive:
            # Every independent provider has completely enumerated this host
            # range, so absence is authoritative for the remaining years.
            # Without this closure the pool incorrectly reports INCOMPLETE and
            # retries years that all providers already proved empty.
            resolved_years.update(missing_years)
        exact_attempts: list[tuple[str, EvidenceQueryResult]] = []
        exact_saw_transient = False
        exact_invalid_years: set[int] = set()
        if missing_years and not all_physical_exhaustive:
            exact_keys = tuple(
                EvidenceQueryKey(
                    key.hostname,
                    TemporalScope(year, year),
                    key.provider,
                    key.policy_version,
                )
                for year in missing_years
            )
            exact_results = await asyncio.gather(
                *(self.query_key(exact_key) for exact_key in exact_keys)
            )
            for year, result in zip(
                missing_years,
                exact_results,
                strict=True,
            ):
                exact_attempts.append((f"exact:{year}", result))
                requests += result.provider_requests
                elapsed += result.provider_elapsed_milliseconds
                pages += result.pages_seen
                records += result.records_seen
                if result.state is CDXQueryState.PASS:
                    if result.capsule is not None:
                        capsules_by_year.setdefault(year, result.capsule)
                    resolved_years.add(year)
                elif result.state is CDXQueryState.EMPTY_EXHAUSTIVE:
                    resolved_years.add(year)
                elif result.state is CDXQueryState.INVALID:
                    exact_invalid_years.add(year)
                elif result.state is CDXQueryState.TRANSIENT_ERROR:
                    exact_saw_transient = True

        positive_years = tuple(sorted(capsules_by_year))
        unresolved_years = tuple(
            year for year in expected_years if year not in resolved_years
        )
        if not unresolved_years:
            state = (
                CDXQueryState.PASS
                if positive_years
                else CDXQueryState.EMPTY_EXHAUSTIVE
            )
        elif unresolved_years and set(unresolved_years).issubset(
            exact_invalid_years
        ):
            # Exact-year fallback already exhausted all configured providers
            # into permanent INVALID/empty outcomes for every unresolved year.
            # Preserve any positive capsules but terminate the impossible gap.
            state = CDXQueryState.INVALID
        else:
            state = (
                CDXQueryState.TRANSIENT_ERROR
                if saw_transient or exact_saw_transient
                else CDXQueryState.INCOMPLETE
            )

        errors = self._error_summary(attempts)
        if exact_attempts:
            exact_errors = self._error_summary(exact_attempts)
            errors = "; ".join(part for part in (errors, exact_errors) if part)

        return RangeEvidenceQueryResult(
            hostname=key.hostname,
            key=key,
            state=state,
            candidate_years=positive_years,
            followup_years=(),
            capsules=tuple(capsules_by_year[year] for year in positive_years),
            pages_seen=pages,
            records_seen=records,
            provider_requests=requests,
            provider_elapsed_milliseconds=elapsed,
            error=(
                None
                if state in {
                    CDXQueryState.PASS,
                    CDXQueryState.EMPTY_EXHAUSTIVE,
                }
                else errors
            ),
        )
