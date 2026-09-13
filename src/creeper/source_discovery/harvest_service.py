"""Execution bridge from marginal region portfolio to exact evidence harvest."""

from __future__ import annotations

from dataclasses import dataclass

from creeper.source_discovery.harvest import (
    RegionHarvestExecutor,
    RegionHarvestReport,
)
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.portfolio import (
    RegionPortfolioPlan,
    RegionPortfolioPlanner,
)


@dataclass(frozen=True)
class RegionHarvestServiceReport:
    selected_regions: tuple[str, ...]
    claim_skipped_regions: tuple[str, ...]
    completed_regions: tuple[str, ...]
    incomplete_regions: tuple[str, ...]
    failed_regions: tuple[str, ...]
    errors: tuple[str, ...]
    bytes_read: int
    requests: int
    direct_capsules_planned: int
    direct_capsules_inserted: int
    baseline_suppressed_host_years: int
    existing_evidence_suppressed_host_years: int
    portfolio_estimated_marginal_eed: float
    portfolio_estimated_harvest_bytes: int
    recovered_expired_claims: int


class RegionHarvestService:
    """Select and execute a finite marginal-EED harvest portfolio.

    The portfolio planner remains non-mutating. Each selected region is claimed
    atomically immediately before I/O, so multiple service processes may plan
    concurrently without downloading the same region at the same time.
    """

    def __init__(
        self,
        registry: IndexSpaceRegistry,
        *,
        portfolio_planner: RegionPortfolioPlanner,
        harvest_executor: RegionHarvestExecutor,
    ) -> None:
        if portfolio_planner.registry is not registry:
            raise ValueError("portfolio planner and service must share one registry")
        if harvest_executor.registry is not registry:
            raise ValueError("harvest executor and service must share one registry")
        self.registry = registry
        self.portfolio_planner = portfolio_planner
        self.harvest_executor = harvest_executor

    @staticmethod
    def _accumulate(
        report: RegionHarvestReport,
        counters: dict[str, int],
    ) -> None:
        counters["bytes_read"] += report.bytes_read
        counters["requests"] += report.requests
        counters["planned"] += report.direct_capsules_planned
        counters["inserted"] += report.direct_capsules_inserted
        counters["baseline_suppressed"] += (
            report.baseline_suppressed_host_years
        )
        counters["existing_suppressed"] += (
            report.existing_evidence_suppressed_host_years
        )

    def run_once(
        self,
        *,
        max_regions: int = 4,
        byte_budget: int | None = None,
        index_keys: set[str] | frozenset[str] | tuple[str, ...] | None = None,
        continue_on_error: bool = True,
    ) -> RegionHarvestServiceReport:
        """Plan once, then execute each virtual selection at most once."""

        recovered = self.registry.recover_expired_harvest_claims()
        plan: RegionPortfolioPlan = self.portfolio_planner.plan(
            max_regions=max_regions,
            byte_budget=byte_budget,
            index_keys=index_keys,
            per_region_overhead_bytes=(
                self.harvest_executor.policy.boundary_record_max_bytes + 1
            ),
        )
        selected = tuple(item.region.region_key for item in plan.selections)
        claim_skipped: list[str] = []
        completed: list[str] = []
        incomplete: list[str] = []
        failed: list[str] = []
        errors: list[str] = []
        counters = {
            "bytes_read": 0,
            "requests": 0,
            "planned": 0,
            "inserted": 0,
            "baseline_suppressed": 0,
            "existing_suppressed": 0,
        }

        for estimate in plan.selections:
            region_key = estimate.region.region_key
            try:
                report = self.harvest_executor.harvest(region_key)
            except BaseException as exc:
                failed.append(region_key)
                errors.append(
                    f"{region_key}: {type(exc).__name__}: {exc}"
                )
                if not continue_on_error:
                    raise
                continue

            if report is None:
                claim_skipped.append(region_key)
                continue
            self._accumulate(report, counters)
            if report.completed:
                completed.append(region_key)
            else:
                incomplete.append(region_key)

        return RegionHarvestServiceReport(
            selected_regions=selected,
            claim_skipped_regions=tuple(claim_skipped),
            completed_regions=tuple(completed),
            incomplete_regions=tuple(incomplete),
            failed_regions=tuple(failed),
            errors=tuple(errors),
            bytes_read=counters["bytes_read"],
            requests=counters["requests"],
            direct_capsules_planned=counters["planned"],
            direct_capsules_inserted=counters["inserted"],
            baseline_suppressed_host_years=counters["baseline_suppressed"],
            existing_evidence_suppressed_host_years=(
                counters["existing_suppressed"]
            ),
            portfolio_estimated_marginal_eed=plan.total_marginal_eed,
            portfolio_estimated_harvest_bytes=plan.total_harvest_bytes,
            recovered_expired_claims=recovered,
        )
