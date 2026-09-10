"""Adapter for a bounded, user-supplied historical hostname list."""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path

from creeper.authority.normalizer import normalize_official
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord, iter_source_records
from creeper.scheduler.leases import LeaseResult, WorkLease
from creeper.sources.reservoirs import ReservoirEstimate


class StaticDatasetAdapter:
    def __init__(
        self, path: Path, source_id: str = "local_dataset", source_year: int | None = None
    ):
        self.path = path
        self.source_id = source_id
        self.source_year = source_year
        self.adapter_id = source_id

    def estimate(self) -> ReservoirEstimate:
        with self.path.open("rb") as source:
            records = sum(1 for _ in source)
        return ReservoirEstimate(capacity_lower=records, capacity_upper=records)

    def execute(self, lease: WorkLease) -> tuple[Iterator[SourceRecord], LeaseResult]:
        if lease.reservoir_id != self.source_id:
            raise ValueError("lease reservoir_id does not match adapter source_id")

        start = int(lease.cursor_start or "1")
        end = int(lease.cursor_end) if lease.cursor_end is not None else None
        if start < 1 or (end is not None and end < start):
            raise ValueError("line cursor must be a positive inclusive range")

        records: list[SourceRecord] = []
        bytes_read = 0
        started = time.monotonic()
        next_cursor: str | None = str(start)

        if lease.max_requests > 0 and lease.max_seconds > 0:
            with self.path.open("rb") as source:
                for current, raw_line in enumerate(source, 1):
                    if current < start:
                        continue
                    if end is not None and current > end:
                        next_cursor = str(current)
                        break
                    if len(records) >= lease.max_records:
                        next_cursor = str(current)
                        break
                    if bytes_read + len(raw_line) > lease.max_bytes:
                        next_cursor = str(current)
                        break
                    if time.monotonic() - started >= lease.max_seconds:
                        next_cursor = str(current)
                        break
                    records.append(
                        SourceRecord(
                            source_id=self.source_id,
                            locator=f"{self.path}:{current}",
                            payload=raw_line.decode("utf-8", errors="replace").rstrip("\r\n"),
                            scope=CandidateSourceScope.LOCAL_DISCOVERY,
                            source_year=self.source_year,
                        )
                    )
                    bytes_read += len(raw_line)
                    next_cursor = str(current + 1)
                else:
                    next_cursor = None

        elapsed = time.monotonic() - started
        return iter(records), LeaseResult(
            lease_id=lease.lease_id,
            records=len(records),
            requests=1 if records or lease.max_requests > 0 and lease.max_seconds > 0 else 0,
            bytes_read=bytes_read,
            elapsed_seconds=elapsed,
            next_cursor=next_cursor,
        )

    def enumerate(self):
        yield from iter_source_records(self.path, self.source_id, CandidateSourceScope.LOCAL_DISCOVERY)

    def extract_hosts(self, record: SourceRecord):
        hostname = normalize_official(record.payload)
        if hostname:
            yield HostObservation(
                hostname, record.source_id, record.locator, record.scope, record.source_year
            )
