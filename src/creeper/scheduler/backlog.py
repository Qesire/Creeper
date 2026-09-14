"""Reconstruct volatile scheduling credits from durable evidence tasks."""

from __future__ import annotations

from dataclasses import dataclass
import math

from creeper.evidence.policies import CDXQueryState
from creeper.scheduler.credits import CreditLedger
from creeper.storage.control_store import ControlStore


@dataclass(frozen=True)
class ProviderBacklog:
    provider: str
    queued: int
    claimed: int

    def __post_init__(self) -> None:
        if not isinstance(self.provider, str) or not self.provider.strip():
            raise ValueError("provider backlog requires a provider")
        for name in ("queued", "claimed"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")


def load_provider_backlog(
    control_store: ControlStore,
    *,
    now: float | None = None,
) -> dict[str, ProviderBacklog]:
    """Aggregate nonterminal durable tasks by provider.

    A task with live lease ownership is ``claimed``. All other nonterminal
    tasks, including future retry work and expired claims, are ``queued`` for
    backlog/high-water accounting even when they are not immediately claimable.
    """
    current_raw = control_store.clock() if now is None else now
    if (
        isinstance(current_raw, bool)
        or not isinstance(current_raw, (int, float))
        or not math.isfinite(float(current_raw))
        or current_raw < 0
    ):
        raise ValueError("backlog snapshot time must be finite and non-negative")
    current = float(current_raw)
    rows = control_store.connection.execute(
        """
        SELECT
            provider,
            SUM(CASE
                WHEN lease_owner IS NOT NULL AND lease_until IS NOT NULL
                     AND lease_until > ? THEN 1 ELSE 0 END) AS claimed,
            SUM(CASE
                WHEN lease_owner IS NULL OR lease_until IS NULL
                     OR lease_until <= ? THEN 1 ELSE 0 END) AS queued
        FROM evidence_tasks
        WHERE state IN (?, ?, ?)
        GROUP BY provider
        ORDER BY provider
        """,
        (
            current,
            current,
            CDXQueryState.PENDING.value,
            CDXQueryState.INCOMPLETE.value,
            CDXQueryState.TRANSIENT_ERROR.value,
        ),
    ).fetchall()
    return {
        str(row["provider"]): ProviderBacklog(
            provider=str(row["provider"]),
            queued=int(row["queued"] or 0),
            claimed=int(row["claimed"] or 0),
        )
        for row in rows
    }


def restore_credit_ledger(
    ledger: CreditLedger,
    control_store: ControlStore,
    *,
    now: float | None = None,
) -> dict[str, ProviderBacklog]:
    """Make an in-memory CreditLedger reflect the durable queue snapshot."""
    backlog = load_provider_backlog(control_store, now=now)
    for provider in ledger.providers():
        state = backlog.get(provider, ProviderBacklog(provider, 0, 0))
        ledger.restore_backlog(
            provider,
            queued=state.queued,
            claimed=state.claimed,
            reserved=0,
        )
    return backlog
