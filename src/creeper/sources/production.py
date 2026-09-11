"""Factories that connect durable Reservoir rows to production adapters."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
import csv
from dataclasses import replace
import io
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
    """Persistent cursor reader for line-oriented historical datasets.

    Uncompressed streams seek directly by byte offset. Gzip streams use logical
    *decompressed* byte offsets and keep one decompressor alive across normal
    sequential leases, so a long-running producer remains O(N). After a crash,
    reopening at a nonzero gzip cursor may replay decompression once; that is a
    recovery cost rather than a per-lease cost.
    """

    def __init__(
        self,
        reservoir: Reservoir,
        *,
        temporal_scope: tuple[int, int] | None = None,
    ) -> None:
        self.adapter_id = reservoir.adapter_id
        self.source_id = reservoir.reservoir_id
        self.source = reservoir.root_locator
        self.kind = self._kind_from_locator(self.source)
        self.temporal_scope = temporal_scope
        path = urlsplit(self.source).path.lower()
        self.compressed = path.endswith(".gz")
        self._stream = None
        self._pending_line: tuple[int, bytes] | None = None

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

    @staticmethod
    def _hostname_from_scalar(value: object) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if "://" in text or text.startswith("//"):
            parsed = urlsplit(text if not text.startswith("//") else "http:" + text)
            return normalize_official(parsed.hostname or "")
        if "/" in text:
            parsed = urlsplit("http://" + text)
            return normalize_official(parsed.hostname or "")
        return normalize_official(text)

    @staticmethod
    def _year_from_scalar(value: object) -> int | None:
        if isinstance(value, bool):
            return None
        text = str(value).strip() if isinstance(value, (int, float, str)) else ""
        if len(text) < 4 or not text[:4].isdigit():
            return None
        year = int(text[:4])
        return year if 1996 <= year <= 2001 else None

    def _default_source_year(self) -> int | None:
        if self.temporal_scope is None:
            return None
        year_from, year_to = self.temporal_scope
        return year_from if year_from == year_to and 1996 <= year_from <= 2001 else None

    def _open_stream(self):
        options = {
            "block_size": 4 * 1024 * 1024,
        }
        if self.compressed:
            options["compression"] = "gzip"
        return fsspec.open(self.source, "rb", **options).open()

    def _ensure_stream(self, start: int):
        opened = False
        if self._stream is None or getattr(self._stream, "closed", False):
            self._stream = self._open_stream()
            opened = True

        if self._pending_line is not None and self._pending_line[0] == start:
            # The underlying stream is already positioned after this buffered
            # line. Consume it on this lease without an expensive gzip rewind.
            return self._stream, opened

        self._pending_line = None
        if self._stream.tell() != start:
            self._stream.seek(start)
        return self._stream, opened

    def close(self) -> None:
        stream = self._stream
        self._stream = None
        self._pending_line = None
        if stream is not None and not getattr(stream, "closed", False):
            stream.close()

    def _generic_record(
        self,
        line: str,
        *,
        locator: str,
    ) -> SourceRecord | None:
        payload = line.strip()
        source_year = self._default_source_year()
        source_time: str | None = None
        record_type = "STRUCTURED_LINE"

        if self.kind == "jsonl":
            try:
                value = json.loads(payload)
            except json.JSONDecodeError:
                return None
            if not isinstance(value, dict):
                return None
            lowered = {str(key).strip().lower(): item for key, item in value.items()}
            selected: str | None = None
            for key in ("hostname", "host", "domain", "url", "original", "original_url", "uri"):
                raw = lowered.get(key)
                if isinstance(raw, str) and self._hostname_from_scalar(raw) is not None:
                    selected = raw.strip()
                    break
            if selected is None:
                return None
            payload = selected
            for key in (
                "year", "source_year", "capture_year", "timestamp",
                "date", "warc_date", "crawl_date",
            ):
                if key not in lowered:
                    continue
                year = self._year_from_scalar(lowered[key])
                if year is not None:
                    source_year = year
                    source_time = str(lowered[key]).strip()
                    break
            record_type = "STRUCTURED_JSONL"

        elif self.kind == "delimited":
            path = urlsplit(self.source).path.lower()
            delimiter = "\t" if path.endswith((".tsv", ".tsv.gz")) else ","
            try:
                row = next(csv.reader(io.StringIO(payload), delimiter=delimiter))
            except (StopIteration, csv.Error):
                return None
            selected: str | None = None
            for cell in row:
                if selected is None and self._hostname_from_scalar(cell) is not None:
                    selected = cell.strip()
                if source_year is None:
                    year = self._year_from_scalar(cell)
                    if year is not None:
                        source_year = year
                        source_time = cell.strip()
            if selected is None:
                return None
            payload = selected
            record_type = "STRUCTURED_DELIMITED"

        return SourceRecord(
            source_id=self.source_id,
            locator=locator,
            payload=payload,
            scope=CandidateSourceScope.LOCAL_DISCOVERY,
            source_year=source_year,
            source_time=source_time,
            record_type=record_type,
            artifact_ref=self.source,
        )

    def execute(self, lease: WorkLease) -> tuple[Iterator[SourceRecord], LeaseResult]:
        start = self._cursor_value(lease.cursor_start)
        records: list[SourceRecord] = []
        started = time.monotonic()
        bytes_read = 0
        next_cursor: str | None = f"byte:{start}"
        if lease.max_requests <= 0 or lease.max_seconds <= 0:
            return iter(()), LeaseResult(lease.lease_id, next_cursor=next_cursor)

        source, opened = self._ensure_stream(start)
        try:
            while len(records) < lease.max_records and bytes_read < lease.max_bytes:
                if time.monotonic() - started >= lease.max_seconds:
                    break

                if self._pending_line is not None:
                    offset, raw = self._pending_line
                    self._pending_line = None
                else:
                    offset = source.tell()
                    raw = source.readline()

                if not raw:
                    next_cursor = None
                    self.close()
                    break

                if bytes_read + len(raw) > lease.max_bytes:
                    if bytes_read == 0:
                        self._pending_line = (offset, raw)
                        raise ProductionAdapterError(
                            "structured source record exceeds lease max_bytes"
                        )
                    self._pending_line = (offset, raw)
                    next_cursor = f"byte:{offset}"
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
                    records.append(record)
                bytes_read += len(raw)
                next_cursor = f"byte:{source.tell()}"
        except BaseException:
            # Keep a buffered over-budget line alive, but reset the stream for
            # all other failures so the next durable retry starts from cursor.
            if self._pending_line is None:
                self.close()
            raise

        return iter(records), LeaseResult(
            lease_id=lease.lease_id,
            records=len(records),
            requests=1 if opened else 0,
            bytes_read=bytes_read,
            elapsed_seconds=time.monotonic() - started,
            next_cursor=next_cursor,
        )

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
