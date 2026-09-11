"""Factories that connect durable Reservoir rows to production adapters."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import replace
import json
from pathlib import Path
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

    def extract_hosts(self, record: SourceRecord) -> Iterable[HostObservation]:
        parsed = urlsplit(record.payload)
        raw_hostname = parsed.hostname or record.payload
        hostname = normalize_official(raw_hostname)
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


class StructuredProductionAdapter:
    """Bounded byte-cursor reader for CDXJ and line-oriented web datasets."""

    def __init__(self, reservoir: Reservoir) -> None:
        self.adapter_id = reservoir.adapter_id
        self.source_id = reservoir.reservoir_id
        self.source = reservoir.root_locator
        self.kind = self._kind_from_locator(self.source)

    @staticmethod
    def _kind_from_locator(locator: str) -> str:
        path = urlsplit(locator).path.lower()
        if path.endswith(".cdxj"):
            return "cdxj"
        if path.endswith((".cdx", ".cdx.gz")):
            return "cdx"
        if path.endswith((".jsonl", ".jsonl.gz")):
            return "jsonl"
        if path.endswith((".csv", ".csv.gz", ".tsv", ".tsv.gz")):
            return "delimited"
        return "lines"

    @staticmethod
    def _cursor_value(cursor: str | None) -> int:
        if cursor in {None, "", "0"}:
            return 0
        if not cursor.startswith("byte:") or not cursor.removeprefix("byte:").isdigit():
            raise ValueError("invalid structured source cursor; expected byte:<offset>")
        return int(cursor.removeprefix("byte:"))

    def execute(self, lease: WorkLease) -> tuple[Iterator[SourceRecord], LeaseResult]:
        start = self._cursor_value(lease.cursor_start)
        records: list[SourceRecord] = []
        started = time.monotonic()
        bytes_read = 0
        next_cursor: str | None = f"byte:{start}"
        if lease.max_requests <= 0 or lease.max_seconds <= 0:
            return iter(()), LeaseResult(lease.lease_id, next_cursor=next_cursor)

        with fsspec.open(self.source, "rb", block_size=4 * 1024 * 1024).open() as source:
            source.seek(start)
            while len(records) < lease.max_records and bytes_read < lease.max_bytes:
                if time.monotonic() - started >= lease.max_seconds:
                    break
                offset = source.tell()
                raw = source.readline()
                if not raw:
                    next_cursor = None
                    break
                if bytes_read + len(raw) > lease.max_bytes:
                    next_cursor = f"byte:{offset}"
                    break
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                locator = f"{self.source}:byte:{offset}"
                record = (
                    parse_cdxj_line(line, source_id=self.source_id, locator=locator)
                    if self.kind == "cdxj"
                    else parse_cdx_line(line, source_id=self.source_id, locator=locator)
                    if self.kind == "cdx"
                    else SourceRecord(
                        source_id=self.source_id,
                        locator=locator,
                        payload=line,
                        scope=CandidateSourceScope.LOCAL_DISCOVERY,
                    )
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
                    records.append(record)
                bytes_read += len(raw)
                next_cursor = f"byte:{source.tell()}"

        return iter(records), LeaseResult(
            lease_id=lease.lease_id,
            records=len(records),
            requests=1,
            bytes_read=bytes_read,
            elapsed_seconds=time.monotonic() - started,
            next_cursor=next_cursor,
        )

    def extract_hosts(self, record: SourceRecord) -> Iterable[HostObservation]:
        raw = record.payload.strip()
        if self.kind == "jsonl":
            try:
                value = json.loads(raw)
            except json.JSONDecodeError:
                value = None
            if isinstance(value, dict):
                raw = next(
                    (value[key] for key in ("hostname", "host", "url", "original")
                     if isinstance(value.get(key), str) and value[key].strip()),
                    "",
                )
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
    def open(reservoir: Reservoir) -> object:
        if reservoir.adapter_id.startswith("warc_arc:"):
            return WarcProductionAdapter(reservoir)
        if reservoir.adapter_id.startswith("structured:"):
            return StructuredProductionAdapter(reservoir)
        if reservoir.adapter_id.startswith("static:"):
            from creeper.sources.local.static_dataset import StaticDatasetAdapter

            return StaticDatasetAdapter(Path(reservoir.root_locator), source_id=reservoir.reservoir_id)
        raise ProductionAdapterError(
            f"no production adapter registered for {reservoir.adapter_id}"
        )
