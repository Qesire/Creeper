"""Independent Local Authority reconciliation loop for Creeper Fabric."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from creeper.distributed.authority_store import DistributedAuthorityStore


@dataclass(frozen=True)
class ReconcileReport:
    promoted: int = 0
    admitted: int = 0
    held: int = 0
    covered: int = 0
    errors: int = 0
    source_promoted: int = 0
    host_promoted: int = 0


class AuthorityReconciler:
    """Promote durable discovery candidates without using the main scheduler."""

    def __init__(
        self,
        store: DistributedAuthorityStore,
        *,
        promotion_batch_size: int = 64,
        include_source_pages: bool = False,
        host_promotion_batch_size: int = 256,
        host_physical_providers: tuple[str, ...] = (),
        host_coverage_provider: str = "",
        host_resolver_version: str = "fabric-host-v1",
    ) -> None:
        if promotion_batch_size < 1 or host_promotion_batch_size < 1:
            raise ValueError("promotion batch sizes must be positive")
        if host_physical_providers and (
            not host_coverage_provider.strip()
            or not host_resolver_version.strip()
        ):
            raise ValueError("invalid host promotion resolver configuration")
        self.store = store
        self.promotion_batch_size = int(promotion_batch_size)
        self.include_source_pages = bool(include_source_pages)
        self.host_promotion_batch_size = int(host_promotion_batch_size)
        self.host_physical_providers = tuple(host_physical_providers)
        self.host_coverage_provider = host_coverage_provider
        self.host_resolver_version = host_resolver_version

    def run_once(self) -> ReconcileReport:
        source_rows = self.store.promote_source_candidates(
            limit=self.promotion_batch_size,
            include_source_pages=self.include_source_pages,
        )
        host_rows: list[dict[str, object]] = []
        if self.host_physical_providers:
            host_rows = self.store.promote_host_candidates(
                physical_providers=self.host_physical_providers,
                coverage_provider=self.host_coverage_provider,
                resolver_version=self.host_resolver_version,
                limit=self.host_promotion_batch_size,
            )
        source_states = [str(row["state"]) for row in source_rows]
        host_states = [str(row["state"]) for row in host_rows]
        states = source_states + host_states
        return ReconcileReport(
            promoted=len(states),
            admitted=sum(state == "ADMITTED" for state in states),
            held=sum(state == "HELD_UNSUPPORTED" for state in states),
            covered=sum(state == "COVERED" for state in states),
            errors=sum(state == "ERROR" for state in states),
            source_promoted=len(source_rows),
            host_promoted=len(host_rows),
        )

    async def run_forever(
        self,
        *,
        interval_seconds: float,
        stop: asyncio.Event,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        while not stop.is_set():
            self.run_once()
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            except TimeoutError:
                continue
