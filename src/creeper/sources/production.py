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
        """Emit WARC-derived records into the runtime pipeline.

        The current WARC metadata reader materializes one bounded archive lease
        internally, but emission begins immediately afterwards and shares the
        same SourceProducer pipeline contract as structured/static sources.
        """
        records, result = self.execute(lease)
        for record in records:
            emit_record(record)
        return result

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
                original_url=record.payload,
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
        if path.endswith((".cdxj", ".cdxj.gz")):
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
            try:
                parsed = urlsplit(text if not text.startswith("//") else "http:" + text)
            except ValueError:
                return None
            return normalize_official(parsed.hostname or "")
        if "/" in text:
            try:
                parsed = urlsplit("http://" + text)
            except ValueError:
                return None
            return normalize_official(parsed.hostname or "")
        return normalize_official(text)

    @staticmethod
    def _year_from_scalar(value: object) -> int | None:
        if isinstance(value, bool):
            return None
        text = str(value).strip() if isinstance(value, (int, float, str)) else ""
        if not (
            re.fullmatch(r"\d{4}", text)
            or re.fullmatch(r"\d{8,14}", text)
            or re.fullmatch(r"\d{4}[-/]\d{1,2}(?:[-/]\d{1,2})?(?:[T ][0-9:]+Z?)?", text)
        ):
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
            explicit_year: int | None = None
            explicit_time: str | None = None
            for cell in row:
                if selected is None and self._hostname_from_scalar(cell) is not None:
                    selected = cell.strip()
                if explicit_year is None:
                    year = self._year_from_scalar(cell)
                    if year is not None:
                        explicit_year = year
                        explicit_time = cell.strip()
            if selected is None:
                return None
            if explicit_year is not None:
                source_year = explicit_year
                source_time = explicit_time
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
        is discarded. Every emitted row is therefore a complete source record
        wholly represented by the selected byte interval.
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
            if end is not None and start > 0 and self._pending_line is None:
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
                original_url=record.payload,
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
