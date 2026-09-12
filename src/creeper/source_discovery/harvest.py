"""Exact, resumable harvest of portfolio-selected historical index regions.

Tomography may summarize a host with a six-bit year mask. Harvest may not: each
accepted direct host-year must retain a real source record with its own capture
timestamp and byte locator. This executor reuses StructuredProductionAdapter,
EvidencePlanner, BaselineIndex and EvidenceStore, and only adds region claims,
per-year witness reduction, and bounded resume semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from creeper.authority.baseline_index import YEAR_BITS, BaselineIndex
from creeper.evidence.planner import EvidencePlanner
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord
from creeper.scheduler.leases import WorkLease
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import HarvestRegion, RegionState
from creeper.sources.archive.host_year import (
    ContiguousHostYearWitnessReducer,
    HostYearWitnessGroup,
)
from creeper.sources.production import StructuredProductionAdapter
from creeper.sources.reservoirs import Reservoir
from creeper.storage.evidence_store import EvidenceStore


class RegionHarvestError(ValueError):
    """Selected region cannot be harvested under the proven authority."""


@dataclass(frozen=True)
class RegionHarvestPolicy:
    max_seconds: float = 300.0
    baseline_batch_size: int = 20_000
    max_records_per_lease: int = 100_000
    policy_version: str = "historical-region-v1"
    claim_grace_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.max_seconds <= 0:
            raise ValueError("max_seconds must be positive")
        if self.baseline_batch_size < 1:
            raise ValueError("baseline_batch_size must be positive")
        if self.max_records_per_lease < 1:
            raise ValueError("max_records_per_lease must be positive")
        if not self.policy_version.strip():
            raise ValueError("policy_version is required")
        if self.claim_grace_seconds < 0:
            raise ValueError("claim_grace_seconds must be non-negative")


@dataclass(frozen=True)
class RegionHarvestReport:
    region_key: str
    index_key: str
    completed: bool
    source_records: int
    host_groups: int
    exact_witnesses: int
    baseline_suppressed_host_years: int
    existing_evidence_suppressed_host_years: int
    direct_capsules_planned: int
    direct_capsules_inserted: int
    bytes_read: int
    requests: int
    elapsed_seconds: float
    resume_cursor: int | None


def _cursor_value(cursor: str | None) -> int | None:
    if cursor is None:
        return None
    if not cursor.startswith("byte:"):
        raise RegionHarvestError("structured harvest returned a non-byte cursor")
    value = cursor.removeprefix("byte:")
    if not value.isdigit():
        raise RegionHarvestError("structured harvest returned an invalid byte cursor")
    return int(value)


class RegionHarvestExecutor:
    """Commit exact direct evidence from one claimed HARVEST_READY region."""

    def __init__(
        self,
        *,
        registry: IndexSpaceRegistry,
        baseline: BaselineIndex,
        evidence_store: EvidenceStore,
        owner: str = "region-harvester",
        policy: RegionHarvestPolicy | None = None,
    ) -> None:
        if not owner.strip():
            raise ValueError("harvest owner is required")
        self.registry = registry
        self.baseline = baseline
        self.evidence_store = evidence_store
        self.owner = owner
        self.policy = policy or RegionHarvestPolicy()
        self.planner = EvidencePlanner()

    @staticmethod
    def _validate_region(
        region: HarvestRegion,
        index,
    ) -> None:
        if region.state is not RegionState.HARVESTING:
            raise RegionHarvestError("region must be claimed before harvest")
        if region.byte_start is None or region.byte_end is None:
            raise RegionHarvestError("exact region harvest requires finite byte bounds")
        if region.index_key != index.index_key:
            raise RegionHarvestError("region does not belong to supplied index")
        if index.capabilities.format not in {"CDX", "CDXJ"}:
            raise RegionHarvestError("exact direct harvest currently requires CDX/CDXJ")
        if not index.capabilities.direct_evidence_authority:
            raise RegionHarvestError("index lacks direct-evidence authority")
        if urlsplit(index.locator).path.lower().endswith(".gz"):
            raise RegionHarvestError(
                "compressed CDX/CDXJ regions are not byte-addressable for exact harvest"
            )

    def _flush_groups(
        self,
        groups: list[HostYearWitnessGroup],
        *,
        counters: dict[str, int],
    ) -> None:
        if not groups:
            return
        hostnames = [group.hostname for group in groups]
        official = self.baseline.resolve_batch(hostnames)
        local = self.evidence_store.resolve_year_masks(hostnames)
        capsules = []

        for group in groups:
            counters["host_groups"] += 1
            counters["exact_witnesses"] += len(group.witnesses)
            official_mask = official.get(group.hostname, (0, False))[0]
            local_mask = local.get(group.hostname, 0)

            for witness in group.witnesses:
                bit = YEAR_BITS[witness.year]
                if official_mask & bit:
                    counters["baseline_suppressed"] += 1
                    continue
                if local_mask & bit:
                    counters["existing_suppressed"] += 1
                    continue

                observation = HostObservation(
                    hostname=witness.hostname,
                    source_id=witness.source_id,
                    locator=witness.locator,
                    scope=CandidateSourceScope.LOCAL_DISCOVERY,
                    source_year=witness.year,
                    source_time=witness.source_time,
                    record_type=witness.record_type,
                    artifact_ref=witness.artifact_ref,
                    direct_year_mask=bit,
                    original_url=witness.original_url,
                )
                plan = self.planner.plan(
                    observation,
                    official_mask=official_mask,
                    local_mask=local_mask,
                    provider="unused-direct-region",
                    policy_version=self.policy.policy_version,
                    allow_direct=True,
                    range_first_fraction=0.0,
                )
                if len(plan.direct_capsules) != 1 or plan.external_keys:
                    raise RegionHarvestError(
                        "exact direct witness did not resolve to one direct capsule"
                    )
                capsules.extend(plan.direct_capsules)
                # Prevent duplicate planning within this batch even before the
                # EvidenceStore transaction becomes visible to another lookup.
                local_mask |= bit

        counters["planned"] += len(capsules)
        if capsules:
            counters["inserted"] += self.evidence_store.put_many(capsules)
        groups.clear()

    def harvest(self, region_key: str) -> RegionHarvestReport | None:
        """Claim and advance one region; return None when another owner won."""

        ttl = self.policy.max_seconds + self.policy.claim_grace_seconds
        claimed = self.registry.claim_region_for_harvest(
            region_key,
            owner=self.owner,
            ttl_seconds=ttl,
        )
        if claimed is None:
            return None

        index = self.registry.get_index(claimed.index_key)
        if index is None:
            # A region without its index is an integrity error, not a transient
            # transport failure. Release ownership so the operator can repair
            # metadata without waiting for the TTL.
            self.registry.release_region_harvest(
                region_key,
                owner=self.owner,
            )
            raise RegionHarvestError("claimed region has no source index")

        try:
            self._validate_region(claimed, index)
        except BaseException:
            self.registry.release_region_harvest(
                region_key,
                owner=self.owner,
            )
            raise

        start = self.registry.get_region_harvest_cursor(region_key)
        if start is None:
            assert claimed.byte_start is not None
            start = claimed.byte_start
        assert claimed.byte_end is not None
        end_exclusive = claimed.byte_end + 1
        if start >= end_exclusive:
            self.registry.complete_region_harvest(
                region_key,
                owner=self.owner,
            )
            return RegionHarvestReport(
                region_key=region_key,
                index_key=index.index_key,
                completed=True,
                source_records=0,
                host_groups=0,
                exact_witnesses=0,
                baseline_suppressed_host_years=0,
                existing_evidence_suppressed_host_years=0,
                direct_capsules_planned=0,
                direct_capsules_inserted=0,
                bytes_read=0,
                requests=0,
                elapsed_seconds=0.0,
                resume_cursor=None,
            )

        reservoir = Reservoir(
            reservoir_id=index.source_key,
            domain_id=index.factory_key,
            adapter_id=f"structured:region:{index.capabilities.format.lower()}",
            root_locator=index.locator,
            enumeration_kind="byte_region",
            capacity_lower=0,
            evidence_mode="direct_year",
        )
        adapter = StructuredProductionAdapter(reservoir)
        # One preceding-byte boundary check is permitted for nonzero starts.
        logical_bytes = end_exclusive - start
        max_bytes = logical_bytes + (1 if start > 0 else 0)
        lease = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            cursor_start=f"byte:{start}",
            cursor_end=f"byte:{end_exclusive}",
            max_records=min(
                max(1, logical_bytes + 1),
                self.policy.max_records_per_lease,
            ),
            max_requests=1,
            max_bytes=max_bytes,
            max_seconds=self.policy.max_seconds,
        )
        reducer = ContiguousHostYearWitnessReducer()
        pending: list[HostYearWitnessGroup] = []
        counters = {
            "host_groups": 0,
            "exact_witnesses": 0,
            "baseline_suppressed": 0,
            "existing_suppressed": 0,
            "planned": 0,
            "inserted": 0,
        }
        prior_cursor = start

        def emit(record: SourceRecord) -> None:
            group = reducer.feed(record)
            if group is not None:
                pending.append(group)
                if len(pending) >= self.policy.baseline_batch_size:
                    self._flush_groups(pending, counters=counters)

        try:
            result = adapter.execute_stream(lease, emit)
            final_group = reducer.finish()
            if final_group is not None:
                pending.append(final_group)
            self._flush_groups(pending, counters=counters)

            resume_cursor = _cursor_value(result.next_cursor)
            completed = resume_cursor is None
            if completed:
                self.registry.complete_region_harvest(
                    region_key,
                    owner=self.owner,
                )
            else:
                if resume_cursor <= prior_cursor:
                    raise RegionHarvestError(
                        "incomplete region harvest made no cursor progress"
                    )
                self.registry.release_region_harvest(
                    region_key,
                    owner=self.owner,
                    resume_cursor=resume_cursor,
                )

            return RegionHarvestReport(
                region_key=region_key,
                index_key=index.index_key,
                completed=completed,
                source_records=result.records,
                host_groups=counters["host_groups"],
                exact_witnesses=counters["exact_witnesses"],
                baseline_suppressed_host_years=counters["baseline_suppressed"],
                existing_evidence_suppressed_host_years=(
                    counters["existing_suppressed"]
                ),
                direct_capsules_planned=counters["planned"],
                direct_capsules_inserted=counters["inserted"],
                bytes_read=result.bytes_read,
                requests=result.requests,
                elapsed_seconds=result.elapsed_seconds,
                resume_cursor=resume_cursor,
            )
        except BaseException:
            adapter.close()
            # Keep any pre-existing resume cursor. Evidence writes are
            # idempotent, so replay after a hard failure is safe.
            try:
                current = self.registry.get_region(region_key)
                if current is not None and current.state is RegionState.HARVESTING:
                    self.registry.release_region_harvest(
                        region_key,
                        owner=self.owner,
                        resume_cursor=prior_cursor,
                    )
            except (KeyError, ValueError):
                pass
            raise
        finally:
            adapter.close()
