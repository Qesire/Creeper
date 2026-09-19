"""Remote hostname consumption for exploration tasks.

Exploration workers use discovered hostnames only as task-local intermediate
state. They immediately consume those hostnames through CDX/Arquivo and send
only positive host-year evidence through the Authority HY admission protocol.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import httpx

from creeper.authority.normalizer import normalize_official
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.host_query import build_distributed_cdx_pool
from creeper.distributed.lease_keeper import LeaseKeeper
from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    RangeEvidenceQueryResult,
    TemporalScope,
)
from creeper.evidence.providers.multi_cdx import CDXProviderConfig


@dataclass(frozen=True)
class RemoteResolutionOutcome:
    hostname: str
    positive_years: tuple[int, ...]
    provider_complete: bool

    @property
    def historical(self) -> bool:
        return bool(self.positive_years)


class RemoteHostnameResolver:
    """Consume task-local hostname candidates without exporting raw candidates."""

    def __init__(
        self,
        configs: tuple[CDXProviderConfig, ...],
        coordinator: CoordinatorClient,
        keeper: LeaseKeeper,
        *,
        policy_version: str = "fabric-exploration-cdx-v1",
        transports: Mapping[str, httpx.AsyncBaseTransport] | None = None,
    ) -> None:
        if not configs or not policy_version.strip():
            raise ValueError("remote hostname resolver requires CDX providers")
        self.configs = configs
        self.coordinator = coordinator
        self.keeper = keeper
        self.policy_version = policy_version
        self.transports = dict(transports or {})
        self._pool = None
        self._cache: dict[str, RemoteResolutionOutcome] = {}

    async def __aenter__(self) -> "RemoteHostnameResolver":
        self._pool = build_distributed_cdx_pool(
            self.configs,
            self.coordinator,
            self.keeper,
            transports=self.transports,
        )
        return self

    async def __aexit__(self, _exc_type, _exc, _tb) -> None:
        if self._pool is not None:
            await self._pool.aclose()
            self._pool = None

    @staticmethod
    def _locator(capsule) -> str:
        return (
            capsule.record_locator
            or capsule.source_locator
            or capsule.original_url
        )

    async def _admit_positive_capsules(self, capsules) -> tuple[int, ...]:
        # One probe per HY, even when multiple physical providers returned it.
        by_hy = {}
        for capsule in capsules:
            key = (capsule.hostname, int(capsule.year))
            by_hy.setdefault(key, capsule)
        selected = list(by_hy.values())
        if not selected:
            return ()

        decisions = await self.coordinator.hy_probe(
            self.keeper.lease,
            [
                {
                    "hostname": capsule.hostname,
                    "year": int(capsule.year),
                    "locator": self._locator(capsule),
                }
                for capsule in selected
            ],
        )
        need = {
            (decision.hostname, int(decision.year))
            for decision in decisions
            if decision.status == "NEED_FULL_EVIDENCE"
        }
        full = []
        for capsule in selected:
            key = (capsule.hostname, int(capsule.year))
            if key not in need:
                continue
            full.append(
                {
                    "hostname": capsule.hostname,
                    "year": int(capsule.year),
                    "evidence_class": capsule.evidence_type,
                    "source": capsule.source_id or capsule.provider,
                    "timestamp": capsule.evidence_timestamp,
                    "locator": self._locator(capsule),
                    "original_url": capsule.original_url,
                    "provider": capsule.provider,
                    "policy_version": capsule.policy_version,
                    "payload_hash": capsule.payload_hash,
                    "extraction_method": capsule.extraction_method,
                }
            )
        if full:
            await self.coordinator.hy_full(self.keeper.lease, full)
        return tuple(sorted({int(capsule.year) for capsule in selected}))

    async def consume(
        self,
        hostname: str,
        *,
        year_from: int = 1996,
        year_to: int = 2001,
    ) -> RemoteResolutionOutcome:
        normalized = normalize_official(hostname)
        if normalized is None:
            raise ValueError("remote hostname resolver received invalid hostname")
        if not 1996 <= int(year_from) <= int(year_to) <= 2001:
            raise ValueError("remote hostname resolver years must be 1996-2001")

        cache_key = f"{normalized}:{int(year_from)}:{int(year_to)}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached
        if self._pool is None:
            raise RuntimeError("RemoteHostnameResolver must be used as async context")

        self.keeper.assert_owned()
        key = EvidenceQueryKey(
            normalized,
            TemporalScope(int(year_from), int(year_to)),
            "wayback",
            self.policy_version,
        )

        if int(year_from) == int(year_to):
            raw = await self._pool.query_key(key)
            capsules = () if raw.capsule is None else (raw.capsule,)
            complete = raw.state in {
                CDXQueryState.PASS,
                CDXQueryState.EMPTY_EXHAUSTIVE,
            }
        else:
            raw = await self._pool.query_range(key)
            if not isinstance(raw, RangeEvidenceQueryResult):
                raise ValueError("remote hostname range returned invalid result")
            capsules = raw.capsules
            complete = (
                raw.state
                in {CDXQueryState.PASS, CDXQueryState.EMPTY_EXHAUSTIVE}
                and not raw.followup_years
            )

        self.keeper.assert_owned()
        positive_years = await self._admit_positive_capsules(capsules)
        outcome = RemoteResolutionOutcome(
            hostname=normalized,
            positive_years=positive_years,
            provider_complete=complete,
        )
        self._cache[cache_key] = outcome
        return outcome
