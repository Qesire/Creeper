"""Public source-discovery coordinator facade.

The generic coordinator implementation lives in :mod:`coordinator_core`.  This
facade keeps its public API stable while strengthening deterministic residual
search with an all-or-nothing SQLite commit boundary.
"""

from __future__ import annotations

from creeper.source_discovery import coordinator_core as _core
from creeper.source_discovery.residual_atomic import commit_deterministic_residual_batch

# Preserve the historical module surface, including the few underscore helpers
# used by tests/debugging.  Base-class method globals intentionally remain in
# coordinator_core; only the residual durable-commit hook is overridden below.
for _name in dir(_core):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_core, _name)


class SourceDiscoveryCoordinator(_core.SourceDiscoveryCoordinator):
    """Coordinator with crash-safe deterministic residual result commits."""

    def _commit_deterministic_searches(self, plans, outcomes, counts) -> None:
        if not plans:
            return
        scheduler = self.manager.residual_search_scheduler
        if scheduler is None or self.search_identity_ledger is None:
            raise RuntimeError(
                "deterministic search plans require residual and identity ledgers"
            )
        coverage = scheduler.ledger
        now = self._retry_now()
        candidate_cap = max(1, self.manager.targets.triage_batch * 2)

        for plan, outcome in zip(plans, outcomes, strict=True):
            retry_key = f"residual:{plan.cell.key}"
            if outcome.error is not None:
                self._search_retry_deadlines[retry_key] = (
                    now + self.failure_retry_seconds
                )
                counts["search_failures"] += 1
                counts["deterministic_search_failures"] += 1
                continue

            self._search_retry_deadlines.pop(retry_key, None)
            batch = outcome.value
            assert batch is not None
            cost = (
                outcome.elapsed_seconds
                if batch.search_cost_seconds is None
                else batch.search_cost_seconds
            )
            committed = commit_deterministic_residual_batch(
                self.registry,
                coverage,
                self.search_identity_ledger,
                plan=plan,
                batch=batch,
                search_cost_seconds=cost,
                candidate_cap=candidate_cap,
            )
            counts["search_episodes"] += 1
            counts["deterministic_search_episodes"] += 1
            counts["search_candidates_registered"] += committed.registered_count
            counts["search_candidates_dropped"] += committed.dropped_count
