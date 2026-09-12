"""Exact, resumable harvest of portfolio-selected historical index regions.

Tomography may summarize a host with a six-bit year mask. Harvest may not: each
accepted direct host-year must retain a real source record with its own capture
timestamp and byte locator. This executor reuses StructuredProductionAdapter,
EvidencePlanner, BaselineIndex and EvidenceStore, and only adds region claims,
per-year witness reduction, and bounded resume semantics.
"""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
import time
from urllib.parse import unquote, urlsplit

import httpx

from creeper.authority.baseline_index import YEAR_BITS, BaselineIndex
from creeper.evidence.planner import EvidencePlanner
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord
from creeper.runtime.http import configured_http_proxy
from creeper.scheduler.leases import LeaseResult, WorkLease
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import HarvestRegion, RegionState
from creeper.sources.archive.cdx import parse_cdx_line
from creeper.sources.archive.cdxj import parse_cdxj_line
from creeper.sources.archive.host_year import (
    ContiguousHostYearWitnessReducer,
    HostYearWitnessGroup,
)
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
    boundary_record_max_bytes: int = 4 * 1024 * 1024

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
        if self.boundary_record_max_bytes < 1:
            raise ValueError("boundary_record_max_bytes must be positive")


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
        http_client: httpx.Client | None = None,
    ) -> None:
        if not owner.strip():
            raise ValueError("harvest owner is required")
        self.registry = registry
        self.baseline = baseline
        self.evidence_store = evidence_store
        self.owner = owner
        self.policy = policy or RegionHarvestPolicy()
        self.http_client = http_client
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
        source_key: str | None = None,
        reservoir_id: str | None = None,
        lease_id: str | None = None,
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
            if source_key is not None:
                if reservoir_id is None or lease_id is None:
                    raise RegionHarvestError(
                        "direct region attribution requires reservoir and lease identity"
                    )
                # Publish lineage before EvidenceStore makes the host-year visible
                # to readiness. This matches SourceProducer's authority ordering
                # and prevents a racing readiness cycle from losing source credit.
                self.registry.control_store.attribute_direct_host_years(
                    (
                        (capsule.hostname, capsule.year, capsule.provider)
                        for capsule in capsules
                    ),
                    source_key=source_key,
                    reservoir_id=reservoir_id,
                    lease_id=lease_id,
                )
            counters["inserted"] += self.evidence_store.put_many(capsules)
        groups.clear()

    @staticmethod
    def _parse_region_record(index, raw: bytes, *, line_start: int) -> SourceRecord | None:
        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        locator = f"{index.locator}:byte:{line_start}"
        if index.capabilities.format == "CDXJ":
            return parse_cdxj_line(
                line,
                source_id=index.source_key,
                locator=locator,
            )
        return parse_cdx_line(
            line,
            source_id=index.source_key,
            locator=locator,
        )

    def _execute_local_region(
        self,
        *,
        index,
        lease: WorkLease,
        emit_record,
    ) -> LeaseResult:
        """Harvest records whose *start offsets* belong to one local region.

        A trailing record may extend beyond the byte partition and is still
        owned by this region when its first byte is inside the partition. This
        makes adjacent partitions lossless without double-counting boundary
        records.
        """

        start = _cursor_value(lease.cursor_start)
        end_exclusive = _cursor_value(lease.cursor_end)
        if start is None or end_exclusive is None:
            raise RegionHarvestError("local region harvest requires byte cursors")
        if end_exclusive <= start:
            return LeaseResult(lease.lease_id, next_cursor=None)

        parsed = urlsplit(index.locator)
        if parsed.scheme == "file":
            path = Path(unquote(parsed.path))
        elif parsed.scheme:
            raise RegionHarvestError(
                f"unsupported local region scheme: {parsed.scheme}"
            )
        else:
            path = Path(index.locator)
        size = path.stat().st_size
        if end_exclusive > size:
            raise RegionHarvestError(
                "local source size changed after region bounds were established"
            )

        emitted = 0
        bytes_read = 0
        downstream_wait = 0.0
        started = time.monotonic()
        cursor = start
        stopped = False
        max_record = self.policy.boundary_record_max_bytes

        with path.open("rb") as source:
            if start > 0:
                source.seek(start - 1)
                previous = source.read(1)
                bytes_read += len(previous)
                source.seek(start)
                if previous != b"\n":
                    # The record began before this region, so it is not owned
                    # here. Search only inside the core partition; if there is
                    # no newline before the end, this region owns zero rows.
                    remaining = end_exclusive - start
                    while remaining > 0:
                        chunk = source.read(min(64 * 1024, remaining))
                        if not chunk:
                            cursor = end_exclusive
                            break
                        bytes_read += len(chunk)
                        boundary = chunk.find(b"\n")
                        if boundary >= 0:
                            consumed = boundary + 1
                            cursor += consumed
                            source.seek(cursor)
                            break
                        cursor += len(chunk)
                        remaining -= len(chunk)
                    if cursor >= end_exclusive:
                        return LeaseResult(
                            lease_id=lease.lease_id,
                            records=0,
                            requests=0,
                            bytes_read=bytes_read,
                            elapsed_seconds=max(
                                0.0,
                                time.monotonic() - started,
                            ),
                            next_cursor=None,
                        )
            else:
                source.seek(start)

            while cursor < end_exclusive:
                if (
                    time.monotonic() - started - downstream_wait
                    >= self.policy.max_seconds
                ):
                    stopped = True
                    break
                line_start = cursor
                raw = source.readline(max_record + 1)
                if not raw:
                    cursor = size
                    break
                bytes_read += len(raw)
                cursor = source.tell()
                if len(raw) > max_record:
                    raise RegionHarvestError(
                        "CDX/CDXJ record crossing a region boundary exceeds "
                        "boundary_record_max_bytes"
                    )
                if not raw.endswith(b"\n") and cursor < size:
                    raise RegionHarvestError(
                        "bounded local read ended before a complete CDX/CDXJ record"
                    )
                record = self._parse_region_record(
                    index,
                    raw,
                    line_start=line_start,
                )
                if record is not None:
                    emit_started = time.monotonic()
                    emit_record(record)
                    downstream_wait += time.monotonic() - emit_started
                    emitted += 1
                if emitted >= lease.max_records:
                    stopped = cursor < end_exclusive
                    break
                if (
                    time.monotonic() - started - downstream_wait
                    >= self.policy.max_seconds
                ):
                    stopped = cursor < end_exclusive
                    break

        return LeaseResult(
            lease_id=lease.lease_id,
            records=emitted,
            requests=0,
            bytes_read=bytes_read,
            elapsed_seconds=max(
                0.0,
                time.monotonic() - started - downstream_wait,
            ),
            next_cursor=(
                f"byte:{cursor}"
                if stopped and cursor < end_exclusive
                else None
            ),
        )

    def _execute_http_region(
        self,
        *,
        index,
        lease: WorkLease,
        emit_record,
    ) -> LeaseResult:
        """Stream one finite HTTP byte region with lossless boundary ownership.

        The selected region owns every record whose start offset is inside
        [start, end). A bounded tail is requested so the final owned record can
        finish after the partition boundary. The next partition will discard
        that same tail record because its start offset is before its own start.
        """

        start = _cursor_value(lease.cursor_start)
        end_exclusive = _cursor_value(lease.cursor_end)
        if start is None or end_exclusive is None:
            raise RegionHarvestError("HTTP region harvest requires byte cursors")
        if end_exclusive <= start:
            return LeaseResult(lease.lease_id, next_cursor=None)

        request_start = start - 1 if start > 0 else start
        request_end = (
            end_exclusive - 1 + self.policy.boundary_record_max_bytes
        )
        if index.content_length is not None:
            request_end = min(request_end, int(index.content_length) - 1)
        headers = {
            "Range": f"bytes={request_start}-{request_end}",
            "Accept-Encoding": "identity",
        }
        timeout = httpx.Timeout(self.policy.max_seconds)
        own_client = self.http_client is None
        context = (
            httpx.Client(
                follow_redirects=True,
                timeout=timeout,
                proxy=configured_http_proxy(),
                trust_env=False,
                headers={"User-Agent": "Creeper-historical-index/2.2"},
            )
            if own_client
            else nullcontext(self.http_client)
        )

        emitted = 0
        bytes_read = 0
        downstream_wait = 0.0
        started = time.monotonic()
        cursor = start
        buffer = b""
        boundary_ready = start == 0
        prefix_checked = start == 0
        stopped = False
        completed = False
        total_size: int | None = None
        returned_end: int | None = None

        def emit_raw(raw: bytes, *, line_start: int) -> None:
            nonlocal emitted, downstream_wait
            if len(raw) > self.policy.boundary_record_max_bytes:
                raise RegionHarvestError(
                    "CDX/CDXJ record crossing a region boundary exceeds "
                    "boundary_record_max_bytes"
                )
            record = self._parse_region_record(
                index,
                raw,
                line_start=line_start,
            )
            if record is not None:
                emit_started = time.monotonic()
                emit_record(record)
                downstream_wait += time.monotonic() - emit_started
                emitted += 1

        with context as client:
            assert client is not None
            with client.stream(
                "GET",
                index.locator,
                headers=headers,
                timeout=timeout,
            ) as response:
                if response.status_code != 206:
                    raise RegionHarvestError(
                        "remote exact harvest requires HTTP 206 Range response"
                    )
                content_range = response.headers.get("content-range", "")
                if (
                    not content_range.lower().startswith("bytes ")
                    or "/" not in content_range
                ):
                    raise RegionHarvestError(
                        "remote Range response omitted Content-Range"
                    )
                try:
                    range_part, total_part = (
                        content_range.split(" ", 1)[1].split("/", 1)
                    )
                    returned_start_text, returned_end_text = range_part.split(
                        "-", 1
                    )
                    returned_start = int(returned_start_text)
                    returned_end = int(returned_end_text)
                    total_size = int(total_part)
                except (ValueError, IndexError) as exc:
                    raise RegionHarvestError(
                        "remote Range response has invalid Content-Range"
                    ) from exc
                if returned_start != request_start:
                    raise RegionHarvestError(
                        "remote Range response starts at an unexpected byte"
                    )
                if returned_end < end_exclusive - 1:
                    raise RegionHarvestError(
                        "remote Range response ended before the selected region"
                    )
                if returned_end > request_end:
                    raise RegionHarvestError(
                        "remote Range response exceeded requested byte interval"
                    )
                if (
                    index.content_length is not None
                    and total_size != int(index.content_length)
                ):
                    raise RegionHarvestError(
                        "remote source size changed after index compilation"
                    )

                for chunk in response.iter_raw(chunk_size=64 * 1024):
                    if not chunk:
                        continue
                    bytes_read += len(chunk)
                    buffer += chunk

                    if not boundary_ready:
                        if not prefix_checked:
                            if not buffer:
                                continue
                            previous = buffer[:1]
                            buffer = buffer[1:]
                            prefix_checked = True
                            if previous == b"\n":
                                boundary_ready = True
                                cursor = start
                        if not boundary_ready:
                            core_remaining = end_exclusive - cursor
                            search = buffer[: max(0, core_remaining)]
                            boundary = search.find(b"\n")
                            if boundary < 0:
                                if len(buffer) >= core_remaining:
                                    # The whole selected region lies inside a
                                    # record that began before it. Nothing here
                                    # is owned by this partition.
                                    completed = True
                                    break
                                continue
                            buffer = buffer[boundary + 1 :]
                            cursor += boundary + 1
                            boundary_ready = True
                            if cursor >= end_exclusive:
                                completed = True
                                break

                    while boundary_ready and cursor < end_exclusive:
                        boundary = buffer.find(b"\n")
                        if boundary < 0:
                            if len(buffer) > self.policy.boundary_record_max_bytes:
                                raise RegionHarvestError(
                                    "CDX/CDXJ record crossing a region boundary "
                                    "exceeds boundary_record_max_bytes"
                                )
                            break
                        raw = buffer[: boundary + 1]
                        buffer = buffer[boundary + 1 :]
                        line_start = cursor
                        cursor += len(raw)
                        emit_raw(raw, line_start=line_start)
                        if cursor >= end_exclusive:
                            completed = True
                            break
                        if emitted >= lease.max_records:
                            stopped = True
                            break
                        if (
                            time.monotonic() - started - downstream_wait
                            >= self.policy.max_seconds
                        ):
                            stopped = True
                            break
                    if completed or stopped:
                        break

                if (
                    not completed
                    and not stopped
                    and boundary_ready
                    and cursor < end_exclusive
                ):
                    assert returned_end is not None
                    assert total_size is not None
                    if returned_end == total_size - 1:
                        # A final line at EOF is valid even without a newline.
                        if buffer:
                            line_start = cursor
                            emit_raw(buffer, line_start=line_start)
                            cursor += len(buffer)
                            buffer = b""
                        completed = True
                    else:
                        raise RegionHarvestError(
                            "selected region ends inside a record larger than "
                            "boundary_record_max_bytes"
                        )

        return LeaseResult(
            lease_id=lease.lease_id,
            records=emitted,
            requests=1,
            bytes_read=bytes_read,
            elapsed_seconds=max(
                0.0,
                time.monotonic() - started - downstream_wait,
            ),
            next_cursor=(
                f"byte:{cursor}"
                if stopped and cursor < end_exclusive
                else None
            ),
        )

    def harvest(self, region_key: str) -> RegionHarvestReport | None:
        """Claim and advance one region; return None when another owner won."""

        harvest_started = time.monotonic()
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
                elapsed_seconds=max(
                    0.0,
                    time.monotonic() - harvest_started,
                ),
                resume_cursor=None,
            )

        # The transport may read one preceding byte plus a bounded tail to
        # complete the last record whose start offset belongs to this region.
        logical_bytes = end_exclusive - start
        max_bytes = (
            logical_bytes
            + (1 if start > 0 else 0)
            + self.policy.boundary_record_max_bytes
        )
        lease = WorkLease.create(
            reservoir_id=index.source_key,
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

        activation = self.registry.control_store.get_activation(
            index.source_key
        )
        origin_reservoir_id = (
            None
            if activation is None
            else str(activation["reservoir_id"])
        )

        def flush_pending() -> None:
            self._flush_groups(
                pending,
                counters=counters,
                source_key=(
                    index.source_key
                    if origin_reservoir_id is not None
                    else None
                ),
                reservoir_id=origin_reservoir_id,
                lease_id=lease.lease_id,
            )

        def emit(record: SourceRecord) -> None:
            group = reducer.feed(record)
            if group is not None:
                pending.append(group)
                if len(pending) >= self.policy.baseline_batch_size:
                    flush_pending()

        try:
            scheme = urlsplit(index.locator).scheme.lower()
            if scheme in {"http", "https"}:
                result = self._execute_http_region(
                    index=index,
                    lease=lease,
                    emit_record=emit,
                )
            elif scheme in {"", "file"}:
                result = self._execute_local_region(
                    index=index,
                    lease=lease,
                    emit_record=emit,
                )
            else:
                raise RegionHarvestError(
                    f"unsupported exact region transport: {scheme}"
                )
            final_group = reducer.finish()
            if final_group is not None:
                pending.append(final_group)
            flush_pending()

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
                elapsed_seconds=max(
                    0.0,
                    time.monotonic() - harvest_started,
                ),
                resume_cursor=resume_cursor,
            )
        except BaseException:
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
