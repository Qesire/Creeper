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
    errors: int = 0


class AuthorityReconciler:
    """Promote durable discovery candidates without using the main scheduler."""

    def __init__(
        self,
        store: DistributedAuthorityStore,
        *,
        promotion_batch_size: int = 64,
        include_source_pages: bool = False,
    ) -> None:
        if promotion_batch_size < 1:
            raise ValueError("promotion_batch_size must be positive")
        self.store = store
        self.promotion_batch_size = int(promotion_batch_size)
        self.include_source_pages = bool(include_source_pages)

    def run_once(self) -> ReconcileReport:
        rows = self.store.promote_source_candidates(
            limit=self.promotion_batch_size,
            include_source_pages=self.include_source_pages,
        )
        states = [row["state"] for row in rows]
        return ReconcileReport(
            promoted=len(rows),
            admitted=sum(state == "ADMITTED" for state in states),
            held=sum(state == "HELD_UNSUPPORTED" for state in states),
            errors=sum(state == "ERROR" for state in states),
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
