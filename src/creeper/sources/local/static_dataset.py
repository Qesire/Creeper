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

        start = int(lease.cursor_start or "0")
        end = int(lease.cursor_end) if lease.cursor_end is not None else None
        if start < 0 or (end is not None and end < start):
            raise ValueError("byte cursor must be a non-negative range")

        records: list[SourceRecord] = []
        bytes_read = 0
        started = time.monotonic()
        next_cursor: str | None = str(start)
        request_allowed = lease.max_requests > 0 and lease.max_seconds > 0

        if request_allowed:
            with self.path.open("rb") as source:
                source.seek(start)
                while True:
                    offset = source.tell()
                    if end is not None and offset >= end:
                        next_cursor = str(offset)
                        break
                    if len(records) >= lease.max_records:
                        break
                    if time.monotonic() - started >= lease.max_seconds:
                        break

                    raw_line = source.readline()
                    if not raw_line:
                        next_cursor = None
                        break

                    if bytes_read + len(raw_line) > lease.max_bytes:
                        # Leave the cursor at this record so the caller can
                        # handle an unrepresentable lease explicitly.
                        next_cursor = str(offset)
                        break

                    records.append(
                        SourceRecord(
                            source_id=self.source_id,
                            locator=f"{self.path}:{offset}",
                            payload=raw_line.decode("utf-8", errors="replace").rstrip("\r\n"),
                            scope=CandidateSourceScope.LOCAL_DISCOVERY,
                            source_year=self.source_year,
                        )
                    )
                    bytes_read += len(raw_line)
                    next_cursor = str(source.tell())
                    if len(records) >= lease.max_records:
                        # Distinguish a lease boundary from EOF without
                        # consuming the next record. This lets the runtime
                        # mark a final bounded lease EXHAUSTED immediately.
                        probe_position = source.tell()
                        if not source.read(1):
                            next_cursor = None
                        else:
                            source.seek(probe_position)

        elapsed = time.monotonic() - started
        return iter(records), LeaseResult(
            lease_id=lease.lease_id,
            records=len(records),
            requests=1 if request_allowed else 0,
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
