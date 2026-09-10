"""Reconstruct volatile scheduling credits from durable evidence tasks."""

from __future__ import annotations

from dataclasses import dataclass

from creeper.evidence.policies import CDXQueryState
from creeper.scheduler.credits import CreditLedger
from creeper.storage.control_store import ControlStore


@dataclass(frozen=True)
class ProviderBacklog:
    provider: str
    queued: int
    claimed: int


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
    current = float(control_store.clock()) if now is None else float(now)
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
