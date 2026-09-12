"""Factories that connect durable Reservoir rows to production adapters."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
import csv
from dataclasses import replace
import io
import json
from pathlib import Path
import re
import time
from urllib.parse import urlsplit

import fsspec

from creeper.authority.normalizer import normalize_official
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord
from creeper.scheduler.leases import LeaseResult, WorkLease
from creeper.sources.archive.warc_source import WarcSourceLeaseExecutor
from creeper.sources.archive.cdxj import parse_cdxj_line
from creeper.sources.archive.cdx import parse_cdx_line
from creeper.sources.reservoirs import Reservoir


class ProductionAdapterError(ValueError):
    """Raised when a persisted Reservoir has no safe production adapter."""


class WarcProductionAdapter:
    """Map WARC metadata observations into the common source contract."""

    def __init__(self, reservoir: Reservoir) -> None:
        self.adapter_id = reservoir.adapter_id
        self.source_id = reservoir.reservoir_id
        self._executor = WarcSourceLeaseExecutor(
            reservoir.root_locator,
            source_id=reservoir.reservoir_id,
        )

    def execute(self, lease: WorkLease) -> tuple[Iterator[SourceRecord], LeaseResult]:
        result = self._executor.execute(
            cursor=lease.cursor_start,
            max_scanned_records=lease.max_records,
            max_archive_bytes=lease.max_bytes,
        )
        records = tuple(
            SourceRecord(
                source_id=observation.source_id,
                locator=observation.locator,
                payload=observation.target_uri,
                scope=CandidateSourceScope.LOCAL_DISCOVERY,
                source_year=observation.year_hint,
                record_type=observation.record_type,
                artifact_ref=observation.locator,
                year_hint_mask=observation.year_hint_mask,
                direct_year_mask=observation.direct_year_mask,
            )
            for observation in result.observations
        )
        return iter(records), LeaseResult(
            lease_id=lease.lease_id,
            records=len(records),
            requests=1,
            bytes_read=result.bytes_advanced,
            elapsed_seconds=0.0,
            next_cursor=result.next_cursor,
        )

    def execute_stream(
        self,
        lease: WorkLease,
        emit_record: Callable[[SourceRecord], None],
    ) -> LeaseResult:
        """Read and emit one bounded structured lease incrementally.

        cursor_end is an optional exclusive byte boundary. Normal source
        production leaves it unset. Region harvest uses it to reuse this mature
        reader without allowing a selected region to bleed into adjacent bytes.
        When a bounded region starts in the middle of a line, that fragment is
        discarded; when it ends in the middle of a line, the trailing fragment
        is discarded. Every emitted CDX/CDXJ row is therefore a real complete
        source record wholly represented by the selected byte interval.
        """

        start = self._cursor_value(lease.cursor_start)
        end = (
            self._cursor_value(lease.cursor_end)
            if lease.cursor_end is not None
            else None
        )
        if end is not None and end < start:
            raise ValueError("structured cursor_end must not precede cursor_start")

        emitted = 0
        started = time.monotonic()
        downstream_wait_seconds = 0.0
        bytes_read = 0
        next_cursor: str | None = f"byte:{start}"
        if lease.max_requests <= 0 or lease.max_seconds <= 0:
            return LeaseResult(lease.lease_id, next_cursor=next_cursor)
        if end is not None and end == start:
            return LeaseResult(lease.lease_id, next_cursor=None)

        source, opened = self._ensure_stream(start)
        try:
            # Region boundaries are not guaranteed to coincide with line
            # boundaries. Inspect one preceding byte to distinguish an exact
            # boundary from a mid-line seek, then discard only a true fragment.
            if (
                end is not None
                and start > 0
                and self._pending_line is None
            ):
                source.seek(start - 1)
                previous = source.read(1)
                bytes_read += len(previous)
                source.seek(start)
                if previous != b"\n":
                    remaining = end - start
                    fragment = source.readline(remaining)
                    bytes_read += len(fragment)
                    next_cursor = f"byte:{source.tell()}"
                    if (
                        not fragment.endswith((b"\n", b"\r"))
                        and source.tell() >= end
                    ):
                        next_cursor = None
                        self.close()

            while (
                self._stream is not None
                and emitted < lease.max_records
                and bytes_read < lease.max_bytes
            ):
                if (
                    time.monotonic() - started - downstream_wait_seconds
                    >= lease.max_seconds
                ):
                    break

                if self._pending_line is not None:
                    offset, raw = self._pending_line
                    self._pending_line = None
                else:
                    offset = source.tell()
                    if end is not None:
                        remaining = end - offset
                        if remaining <= 0:
                            next_cursor = None
                            self.close()
                            break
                        raw = source.readline(remaining)
                    else:
                        raw = source.readline()

                if not raw:
                    next_cursor = None
                    self.close()
                    break

                if bytes_read + len(raw) > lease.max_bytes:
                    if bytes_read == 0:
                        self.close()
                        raise ProductionAdapterError(
                            "structured source record exceeds lease max_bytes"
                        )
                    self._pending_line = (offset, raw)
                    next_cursor = f"byte:{offset}"
                    break

                bytes_read += len(raw)

                if (
                    end is not None
                    and source.tell() >= end
                    and not raw.endswith((b"\n", b"\r"))
                ):
                    next_cursor = None
                    self.close()
                    break

                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                locator = f"{self.source}:byte:{offset}"
                record = (
                    parse_cdxj_line(
                        line,
                        source_id=self.source_id,
                        locator=locator,
                    )
                    if self.kind == "cdxj"
                    else parse_cdx_line(
                        line,
                        source_id=self.source_id,
                        locator=locator,
                    )
                    if self.kind == "cdx"
                    else self._generic_record(line, locator=locator)
                )
                if (
                    record is not None
                    and record.source_year in range(1996, 2002)
                    and record.direct_year_mask == 0
                ):
                    record = replace(
                        record,
                        year_hint_mask=1 << (record.source_year - 1996),
                    )
                if record is not None:
                    emit_started = time.monotonic()
                    emit_record(record)
                    downstream_wait_seconds += time.monotonic() - emit_started
                    emitted += 1
                next_cursor = f"byte:{source.tell()}"
                if end is not None and source.tell() >= end:
                    next_cursor = None
                    self.close()
                    break
        except BaseException:
            if self._pending_line is None:
                self.close()
            raise

        return LeaseResult(
            lease_id=lease.lease_id,
            records=emitted,
            requests=1 if opened else 0,
            bytes_read=bytes_read,
            elapsed_seconds=max(
                0.0,
                time.monotonic() - started - downstream_wait_seconds,
            ),
            next_cursor=next_cursor,
        )

    def execute(self, lease: WorkLease) -> tuple[Iterator[SourceRecord], LeaseResult]:
        records: list[SourceRecord] = []
        result = self.execute_stream(lease, records.append)
        return iter(records), result

    def extract_hosts(self, record: SourceRecord) -> Iterable[HostObservation]:
        raw = record.payload.strip()
        parsed = urlsplit(raw)
        hostname = normalize_official(parsed.hostname or raw)
        if hostname is None:
            return ()
        return (
            HostObservation(
                hostname=hostname,
                source_id=record.source_id,
                locator=record.locator,
                scope=record.scope,
                source_year=record.source_year,
                source_time=record.source_time,
                record_type=record.record_type,
                artifact_ref=record.artifact_ref,
                direct_year_mask=record.direct_year_mask,
                year_hint_mask=record.year_hint_mask,
            ),
        )


class ProductionAdapterFactory:
    """Open only adapter families with an explicit production implementation."""

    @staticmethod
    def open(
        reservoir: Reservoir,
        *,
        temporal_scope: tuple[int, int] | None = None,
    ) -> object:
        if reservoir.adapter_id.startswith("warc_arc:"):
            return WarcProductionAdapter(reservoir)
        if reservoir.adapter_id.startswith("structured:"):
            return StructuredProductionAdapter(
                reservoir,
                temporal_scope=temporal_scope,
            )
        if reservoir.adapter_id.startswith("static:"):
            from creeper.sources.local.static_dataset import StaticDatasetAdapter

            return StaticDatasetAdapter(Path(reservoir.root_locator), source_id=reservoir.reservoir_id)
        raise ProductionAdapterError(
            f"no production adapter registered for {reservoir.adapter_id}"
        )
