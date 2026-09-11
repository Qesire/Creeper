"""Deterministic measured-yield scouting for concrete bulk sources.

The scout deliberately supports only formats with a conservative parser contract.
It reads a bounded prefix, extracts exact hostnames, performs one batch baseline
reconciliation, and computes Equivalent-English Domain yield from the official
weight model. WARC/ARC metadata is streamed through warcio; unsupported formats
remain HOLD rather than being guessed.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import math
import time
import zlib
from dataclasses import dataclass
from decimal import Decimal
from pathlib import PurePosixPath
from urllib.parse import urlsplit

import httpx
from creeper.authority.baseline_index import BaselineIndex, YEAR_BITS
from creeper.authority.normalizer import normalize_official
from creeper.source_discovery.coordinator import ScoutDisposition, ScoutResult
from creeper.source_discovery.models import (
    MeasurementMode,
    ScoutMeasurement,
    SourceCandidate,
)
from creeper.sources.archive.cdxj import parse_cdxj_line
from creeper.sources.archive.cdx import parse_cdx_line
from creeper.sources.archive.warc import WarcFormatError, iter_warc_target_records


@dataclass(frozen=True)
class MeasuredYieldScoutPolicy:
    max_download_bytes: int = 8 * 1024 * 1024
    max_decompressed_bytes: int = 32 * 1024 * 1024
    max_records: int = 5_000
    max_line_bytes: int = 64 * 1024
    sample_windows: int = 4
    target_year_from: int = 1996
    target_year_to: int = 2001
    min_unique_hosts: int = 100
    min_novel_hosts: int = 10
    min_novel_fraction: float = 0.01
    min_novel_eed: float = 1.0
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        for name in (
            "max_download_bytes",
            "max_decompressed_bytes",
            "max_records",
            "max_line_bytes",
            "sample_windows",
            "min_unique_hosts",
            "min_novel_hosts",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if (
            not isinstance(self.target_year_from, int)
            or isinstance(self.target_year_from, bool)
            or not isinstance(self.target_year_to, int)
            or isinstance(self.target_year_to, bool)
            or self.target_year_from > self.target_year_to
        ):
            raise ValueError("target year bounds must be integers with from <= to")
        if not math.isfinite(self.min_novel_fraction) or not 0 <= self.min_novel_fraction <= 1:
            raise ValueError("min_novel_fraction must be within [0, 1]")
        if not math.isfinite(self.min_novel_eed) or self.min_novel_eed < 0:
            raise ValueError("min_novel_eed must be finite and non-negative")
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


@dataclass(frozen=True)
class PrefixDownload:
    """One bounded HTTP range probe with explicit byte-position metadata."""

    payload: bytes
    content_type: str
    truncated: bool
    range_start: int = 0
    range_end: int | None = None
    total_size: int | None = None


@dataclass(frozen=True)
class SampleDownload:
    """One logical scout sample assembled from one or more bounded ranges."""

    payload: bytes
    content_type: str
    truncated: bool
    requests: int
    bytes_read: int


@dataclass(frozen=True)
class ParsedHostSample:
    """Bounded host sample plus optional exact source-year observations.

    ``__iter__`` intentionally preserves the historical ``sampled, hosts =
    _extract_hosts(...)`` helper contract used by older callers and tests.
    New callers should use the explicit fields so year-aware measurements are
    not accidentally reduced to hostname-only novelty.
    """

    sampled_records: int
    hosts: set[str]
    host_year_pairs: set[tuple[str, int]]
    measurement_mode: MeasurementMode

    def __iter__(self):
        yield self.sampled_records
        yield self.hosts


def _parse_content_range(value: str | None) -> tuple[int, int, int | None] | None:
    if not value:
        return None
    text = value.strip().lower()
    if not text.startswith("bytes ") or "/" not in text or "-" not in text:
        return None
    span, total_raw = text[6:].split("/", 1)
    start_raw, end_raw = span.split("-", 1)
    try:
        start = int(start_raw)
        end = int(end_raw)
        total = None if total_raw == "*" else int(total_raw)
    except ValueError:
        return None
    if start < 0 or end < start or (total is not None and total <= end):
        return None
    return start, end, total


def _explicitly_truncated(
    response: httpx.Response,
    *,
    bytes_read: int,
    overflowed_chunk: bool,
) -> bool:
    """Return true only when the bounded probe is provably incomplete.

    A valid ``Content-Range`` is authoritative. If a server ignores Range and
    returns 200, a larger ``Content-Length`` also proves truncation. Finally, a
    chunk larger than the remaining budget proves that bytes were discarded.
    Ambiguous exact-boundary responses fail closed as *not* truncated.
    """

    if overflowed_chunk:
        return True

    content_range = _parse_content_range(response.headers.get("content-range"))
    if content_range is not None:
        start, end, total = content_range
        if start == 0 and total is not None:
            return end + 1 < total

    raw_length = response.headers.get("content-length")
    if raw_length and raw_length.isdigit():
        # A 206 response without Content-Range is still useful partial-content
        # metadata when it carried a non-empty response body. The explicit
        # length check preserves fail-closed behavior for metadata-free probes.
        if response.status_code == 206 and int(raw_length) > 0:
            return True
        return int(raw_length) > bytes_read
    return False


async def _iter_response_raw(response: httpx.Response):
    """Yield raw response bytes for both streamed and buffered responses."""
    if response.is_stream_consumed:
        yield response.content
        return
    async for chunk in response.aiter_raw():
        yield chunk


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


def _mapping_hostname(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    lowered = {str(key).strip().lower(): value for key, value in payload.items()}
    for key in ("hostname", "host", "domain"):
        hostname = _hostname_from_scalar(lowered.get(key))
        if hostname is not None:
            return hostname
    for key in ("url", "original", "original_url", "uri"):
        hostname = _hostname_from_scalar(lowered.get(key))
        if hostname is not None:
            return hostname
    return None


def _year_from_scalar(value: object, *, policy: MeasuredYieldScoutPolicy) -> int | None:
    """Extract a bounded source year from common date/timestamp fields."""
    if isinstance(value, bool):
        return None
    text = str(value).strip() if isinstance(value, (int, float, str)) else ""
    if not text:
        return None
    prefix = text[:4]
    if len(prefix) != 4 or not prefix.isdigit():
        return None
    year = int(prefix)
    if not policy.target_year_from <= year <= policy.target_year_to:
        return None
    return year


def _mapping_host_year(
    payload: object,
    *,
    policy: MeasuredYieldScoutPolicy,
) -> tuple[str | None, int | None]:
    if not isinstance(payload, dict):
        return None, None
    lowered = {str(key).strip().lower(): value for key, value in payload.items()}
    hostname = _mapping_hostname(payload)
    for key in (
        "year",
        "source_year",
        "capture_year",
        "timestamp",
        "date",
        "warc_date",
        "crawl_date",
    ):
        if key not in lowered:
            continue
        raw_value = lowered[key]
        year = _year_from_scalar(lowered.get(key), policy=policy)
        if year is not None:
            return hostname, year
        raw_text = str(raw_value).strip()
        if len(raw_text) >= 4 and raw_text[:4].isdigit():
            # A recognized date outside the competition window is not an
            # undated hostname and must not enter the sample as HOST_ONLY.
            return None, None
    return hostname, None


def _structured_measurement_mode(
    *,
    host_year_pairs: set[tuple[str, int]],
    saw_undated_host: bool,
) -> MeasurementMode:
    # Mixed records are deliberately conservative: do not rank undated hosts
    # as if they carried the dated source's year semantics.
    if host_year_pairs and not saw_undated_host:
        return MeasurementMode.HOST_YEAR
    return MeasurementMode.HOST_ONLY


def _suffix(path: str) -> tuple[str, bool]:
    name = PurePosixPath(path).name.lower()
    compressed = name.endswith(".gz")
    if compressed:
        name = name[:-3]
    return PurePosixPath(name).suffix.lower(), compressed


def _inflate_gzip_prefix(payload: bytes, limit: int) -> bytes:
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        decoded = decoder.decompress(payload, limit + 1)
    except zlib.error as exc:
        raise ValueError(f"invalid gzip source prefix: {exc}") from exc
    if len(decoded) > limit:
        decoded = decoded[:limit]
    return decoded


def _enforce_gzip_expansion_budget(
    payload: bytes,
    limit: int,
    *,
    allow_truncated: bool,
) -> None:
    """Reject a compressed WARC prefix whose decoded bytes exceed ``limit``.

    WARC gzip commonly uses concatenated record members. ``gzip.GzipFile``
    handles concatenated members and we read it in small chunks so a highly
    compressible record cannot turn the bounded network probe into an unbounded
    decompression allocation. A prefix may legitimately end mid-member; EOF in
    that case is not evidence of a bad source and is left for ``warcio`` to
    interpret as a bounded sample.
    """
    total = 0
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload), mode="rb") as stream:
            while total <= limit:
                chunk = stream.read(min(64 * 1024, limit - total + 1))
                if not chunk:
                    return
                total += len(chunk)
                if total > limit:
                    raise ValueError(
                        f"gzip source prefix exceeds decompressed budget={limit}"
                    )
    except EOFError as exc:
        if allow_truncated:
            # A probe that exhausted its network byte budget may legitimately
            # stop inside the final gzip member.
            return
        raise ValueError("truncated gzip source before probe byte budget") from exc
    except (gzip.BadGzipFile, zlib.error) as exc:
        raise ValueError(f"invalid gzip source prefix: {exc}") from exc


def _iter_text_lines(payload: bytes, *, max_line_bytes: int):
    text = payload.decode("utf-8", errors="replace")
    for line in text.splitlines():
        encoded = line.encode("utf-8", errors="replace")
        if len(encoded) <= max_line_bytes:
            yield line


def _is_warc_resource(*, suffix: str, content_type: str) -> bool:
    if suffix in {".warc", ".arc"}:
        return True
    media_type = content_type.split(";", 1)[0].strip().lower()
    return media_type in {
        "application/warc",
        "application/x-warc",
        "application/arc",
        "application/x-arc",
    }


def _extract_warc_hosts(
    payload: bytes,
    *,
    policy: MeasuredYieldScoutPolicy,
    allow_truncated_tail: bool = False,
) -> ParsedHostSample:
    """Sample target-year hostnames from WARC/ARC metadata only.

    ``ArchiveIterator`` consumes records sequentially and handles canonical
    record-compressed ``.warc.gz`` / ``.arc.gz`` streams itself. Creeper never
    opens ``content_stream()`` here: scout measurement needs only capture date
    and target URI, not archived page payload.
    """
    sampled = 0
    hosts: set[str] = set()
    host_year_pairs: set[tuple[str, int]] = set()
    stream = io.BytesIO(payload)
    try:
        for record in iter_warc_target_records(stream):
            if sampled >= policy.max_records:
                break
            sampled += 1
            if (
                record.source_year is None
                or not policy.target_year_from <= record.source_year <= policy.target_year_to
            ):
                continue
            hostname = _hostname_from_scalar(record.target_uri)
            if hostname is not None:
                hosts.add(hostname)
                assert record.source_year is not None
                host_year_pairs.add((hostname, record.source_year))
    except WarcFormatError:
        # A Range probe may end in the middle of the final canonical gzip
        # member. Complete records before that boundary are still a valid
        # *scout sample*. Production archive leases never enable this path and
        # remain strict about any malformed/truncated archive.
        if not allow_truncated_tail or sampled == 0:
            raise
    return ParsedHostSample(
        sampled_records=sampled,
        hosts=hosts,
        host_year_pairs=host_year_pairs,
        measurement_mode=MeasurementMode.HOST_YEAR,
    )


def _extract_hosts(
    payload: bytes,
    *,
    url: str,
    content_type: str,
    policy: MeasuredYieldScoutPolicy,
    truncated: bool = False,
) -> ParsedHostSample | None:
    suffix, compressed = _suffix(urlsplit(url).path)
    lower_type = content_type.lower()
    if _is_warc_resource(suffix=suffix, content_type=content_type):
        if compressed or payload.startswith(b"\x1f\x8b"):
            _enforce_gzip_expansion_budget(
                payload,
                policy.max_decompressed_bytes,
                allow_truncated=truncated,
            )
        return _extract_warc_hosts(
            payload,
            policy=policy,
            allow_truncated_tail=truncated,
        )
    if compressed:
        payload = _inflate_gzip_prefix(payload, policy.max_decompressed_bytes)

    lines = _iter_text_lines(payload, max_line_bytes=policy.max_line_bytes)
    hosts: set[str] = set()
    host_year_pairs: set[tuple[str, int]] = set()
    saw_undated_host = False
    sampled = 0

    if suffix in {".cdxj", ".cdx"}:
        for line in lines:
            if sampled >= policy.max_records:
                break
            sampled += 1
            record = (
                parse_cdxj_line(line, source_id="measured-scout", locator=str(sampled))
                if suffix == ".cdxj"
                else parse_cdx_line(line, source_id="measured-scout", locator=str(sampled))
            )
            if (
                record is None
                or record.source_year is None
                or not policy.target_year_from <= record.source_year <= policy.target_year_to
            ):
                continue
            hostname = _hostname_from_scalar(record.payload)
            if hostname is not None:
                hosts.add(hostname)
                host_year_pairs.add((hostname, record.source_year))
        return ParsedHostSample(
            sampled_records=sampled,
            hosts=hosts,
            host_year_pairs=host_year_pairs,
            measurement_mode=MeasurementMode.HOST_YEAR,
        )

    if suffix in {".jsonl", ".ndjson"} or "ndjson" in lower_type:
        for line in lines:
            if sampled >= policy.max_records:
                break
            if not line.strip():
                continue
            sampled += 1
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            hostname, year = _mapping_host_year(value, policy=policy)
            if hostname is not None:
                hosts.add(hostname)
                if year is None:
                    saw_undated_host = True
                else:
                    host_year_pairs.add((hostname, year))
        return ParsedHostSample(
            sampled_records=sampled,
            hosts=hosts,
            host_year_pairs=host_year_pairs,
            measurement_mode=_structured_measurement_mode(
                host_year_pairs=host_year_pairs,
                saw_undated_host=saw_undated_host,
            ),
        )

    if suffix in {".csv", ".tsv"} or "text/csv" in lower_type or "tab-separated-values" in lower_type:
        text = payload.decode("utf-8", errors="replace")
        dialect = "excel-tab" if suffix == ".tsv" or "tab-separated-values" in lower_type else "excel"
        rows = csv.reader(io.StringIO(text), dialect=dialect)
        try:
            first = next(rows)
        except StopIteration:
            return ParsedHostSample(
                sampled_records=0,
                hosts=hosts,
                host_year_pairs=host_year_pairs,
                measurement_mode=MeasurementMode.HOST_ONLY,
            )

        known_fields = {
            "hostname", "host", "domain", "url", "original", "original_url", "uri",
            "year", "source_year", "capture_year", "timestamp", "date", "warc_date",
            "crawl_date",
        }
        normalized_header = [cell.strip().lower() for cell in first]
        header_positions = {
            name: index
            for index, name in enumerate(normalized_header)
            if name in known_fields
        }

        def consume_scalar_row(row: list[str]) -> None:
            nonlocal saw_undated_host
            for cell in row:
                hostname = _hostname_from_scalar(cell)
                if hostname is not None:
                    hosts.add(hostname)
                    saw_undated_host = True
                    return

        if header_positions:
            for row in rows:
                if sampled >= policy.max_records:
                    break
                sampled += 1
                mapped = {
                    name: row[index]
                    for name, index in header_positions.items()
                    if index < len(row)
                }
                hostname, year = _mapping_host_year(mapped, policy=policy)
                if hostname is not None:
                    hosts.add(hostname)
                    if year is None:
                        saw_undated_host = True
                    else:
                        host_year_pairs.add((hostname, year))
        else:
            if sampled < policy.max_records:
                sampled += 1
                consume_scalar_row(first)
            for row in rows:
                if sampled >= policy.max_records:
                    break
                sampled += 1
                consume_scalar_row(row)
        return ParsedHostSample(
            sampled_records=sampled,
            hosts=hosts,
            host_year_pairs=host_year_pairs,
            measurement_mode=_structured_measurement_mode(
                host_year_pairs=host_year_pairs,
                saw_undated_host=saw_undated_host,
            ),
        )

    if suffix in {"", ".txt", ".list"} or lower_type.startswith("text/plain"):
        for line in lines:
            if sampled >= policy.max_records:
                break
            if not line.strip():
                continue
            sampled += 1
            hostname = _hostname_from_scalar(line)
            if hostname is not None:
                hosts.add(hostname)
        return ParsedHostSample(
            sampled_records=sampled,
            hosts=hosts,
            host_year_pairs=set(),
            measurement_mode=MeasurementMode.HOST_ONLY,
        )

    return None


class MeasuredYieldScoutExecutor:
    """Measure concrete source yield without granting evidence authority."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        baseline: BaselineIndex,
        english_weights: dict[str, Decimal],
        *,
        policy: MeasuredYieldScoutPolicy | None = None,
        clock=time.perf_counter,
    ) -> None:
        self.client = client
        self.baseline = baseline
        self.english_weights = dict(english_weights)
        self.policy = policy or MeasuredYieldScoutPolicy()
        self.clock = clock

    @staticmethod
    def _windowable_line_resource(url: str) -> bool:
        suffix, compressed = _suffix(urlsplit(url).path)
        return (not compressed) and suffix in {
            ".cdx",
            ".cdxj",
            ".jsonl",
            ".ndjson",
            ".txt",
            ".list",
        }

    async def _download_prefix(
        self,
        url: str,
        *,
        start: int = 0,
        max_bytes: int | None = None,
    ) -> PrefixDownload:
        budget = self.policy.max_download_bytes if max_bytes is None else int(max_bytes)
        if start < 0 or budget < 1:
            raise ValueError("invalid measured scout range")
        payload = bytearray()
        overflowed_chunk = False
        headers = {
            "Range": f"bytes={start}-{start + budget - 1}",
            "Accept-Encoding": "identity",
        }
        timeout = httpx.Timeout(self.policy.timeout_seconds)
        async with self.client.stream(
            "GET",
            url,
            headers=headers,
            timeout=timeout,
        ) as response:
            if response.status_code in {404, 410}:
                return PrefixDownload(
                    b"",
                    "__permanent_missing__",
                    False,
                    range_start=start,
                )
            response.raise_for_status()
            async for chunk in _iter_response_raw(response):
                remaining = budget - len(payload)
                if remaining <= 0:
                    overflowed_chunk = True
                    break
                if len(chunk) > remaining:
                    payload.extend(chunk[:remaining])
                    overflowed_chunk = True
                    break
                payload.extend(chunk)
                if len(payload) >= budget:
                    break
            body = bytes(payload)
            parsed_range = _parse_content_range(
                response.headers.get("content-range")
            )
            range_start = start
            range_end = (
                start + len(body) - 1
                if body
                else None
            )
            total_size = None
            if parsed_range is not None:
                range_start, parsed_end, total_size = parsed_range
                range_end = parsed_end
            return PrefixDownload(
                payload=body,
                content_type=response.headers.get("content-type", ""),
                truncated=_explicitly_truncated(
                    response,
                    bytes_read=len(body),
                    overflowed_chunk=overflowed_chunk,
                ),
                range_start=range_start,
                range_end=range_end,
                total_size=total_size,
            )

    @staticmethod
    def _trim_line_window(download: PrefixDownload) -> bytes:
        payload = download.payload
        if not payload:
            return payload
        if download.range_start > 0:
            boundary = payload.find(b"\n")
            if boundary < 0:
                return b""
            payload = payload[boundary + 1 :]
        if (
            download.total_size is None
            or download.range_end is None
            or download.range_end + 1 < download.total_size
        ):
            boundary = payload.rfind(b"\n")
            if boundary < 0:
                return b""
            payload = payload[: boundary + 1]
        return payload

    async def _download_sample(self, url: str) -> SampleDownload:
        if (
            self.policy.sample_windows <= 1
            or not self._windowable_line_resource(url)
        ):
            first = await self._download_prefix(url)
            return SampleDownload(
                payload=first.payload,
                content_type=first.content_type,
                truncated=first.truncated,
                requests=1,
                bytes_read=len(first.payload),
            )

        per_window = max(
            1,
            self.policy.max_download_bytes // self.policy.sample_windows,
        )
        first = await self._download_prefix(
            url,
            start=0,
            max_bytes=per_window,
        )
        if (
            first.content_type == "__permanent_missing__"
            or first.total_size is None
            or first.total_size <= per_window
        ):
            return SampleDownload(
                payload=first.payload,
                content_type=first.content_type,
                truncated=first.truncated,
                requests=1,
                bytes_read=len(first.payload),
            )

        max_start = max(0, first.total_size - per_window)
        starts = []
        for index in range(self.policy.sample_windows):
            if self.policy.sample_windows == 1:
                offset = 0
            else:
                offset = round(
                    max_start * index / (self.policy.sample_windows - 1)
                )
            if offset not in starts:
                starts.append(offset)

        downloads = [first]
        for offset in starts[1:]:
            probe = await self._download_prefix(
                url,
                start=offset,
                max_bytes=per_window,
            )
            # A server that ignores non-zero ranges usually returns byte 0.
            # Ignore that duplicate rather than biasing the sample toward the
            # prefix or exceeding the deterministic download budget.
            if offset > 0 and probe.range_start != offset:
                continue
            downloads.append(probe)

        chunks = [
            self._trim_line_window(download)
            for download in downloads
        ]
        combined = b"\n".join(chunk for chunk in chunks if chunk)
        return SampleDownload(
            payload=combined,
            content_type=first.content_type,
            truncated=True,
            requests=len(downloads),
            bytes_read=sum(len(download.payload) for download in downloads),
        )


    def _measurement(
        self,
        *,
        parsed: ParsedHostSample,
        bytes_read: int,
        elapsed: float,
        requests: int = 1,
    ) -> ScoutMeasurement:
        resolved = self.baseline.resolve_batch(parsed.hosts)
        novel = [
            hostname
            for hostname in parsed.hosts
            if resolved.get(hostname, (0, False))[0] == 0
        ]
        novel_eed = Decimal("0")
        for hostname in novel:
            tld = hostname.rsplit(".", 1)[-1]
            novel_eed += self.english_weights.get(tld, Decimal("0"))
        novel_pairs = {
            (hostname, year)
            for hostname, year in parsed.host_year_pairs
            if YEAR_BITS.get(year, 0)
            and not resolved.get(hostname, (0, False))[0] & YEAR_BITS[year]
        }
        novel_pair_eed = Decimal("0")
        for hostname, _year in novel_pairs:
            tld = hostname.rsplit(".", 1)[-1]
            novel_pair_eed += self.english_weights.get(tld, Decimal("0"))
        return ScoutMeasurement(
            sampled_records=parsed.sampled_records,
            unique_hosts=len(parsed.hosts),
            novel_hosts=len(novel),
            direct_host_years=0,
            requests=requests,
            bytes_read=bytes_read,
            elapsed_seconds=elapsed,
            novel_eed=float(novel_eed),
            measurement_mode=parsed.measurement_mode,
            observed_host_year_pairs=len(parsed.host_year_pairs),
            novel_host_year_pairs=len(novel_pairs),
            novel_pair_eed=float(novel_pair_eed),
        )

    async def __call__(self, candidate: SourceCandidate) -> ScoutResult:
        started = float(self.clock())
        download = await self._download_sample(candidate.canonical_entrypoint)
        if download.content_type == "__permanent_missing__":
            return ScoutResult(ScoutDisposition.HOLD, reason="bulk source returned HTTP 404/410")
        try:
            parsed = _extract_hosts(
                download.payload,
                url=candidate.canonical_entrypoint,
                content_type=download.content_type,
                policy=self.policy,
                truncated=download.truncated,
            )
        except (WarcFormatError, ValueError, csv.Error) as exc:
            detail = str(exc).strip().replace("\n", " ")[:240]
            measurement = None
            # Preserve the historical fail-closed distinction: a payload that
            # advertises no recognizable WARC/ARC framing is not measurable.
            # A recognizable archive prefix may still report a zero-yield
            # measurement for auditability without being promoted.
            if download.payload.lstrip().startswith((b"WARC/", b"ARC/", b"\x1f\x8b")):
                elapsed = max(0.0, float(self.clock()) - started)
                measurement = self._measurement(
                    parsed=ParsedHostSample(
                        sampled_records=0,
                        hosts=set(),
                        host_year_pairs=set(),
                        measurement_mode=MeasurementMode.HOST_ONLY,
                    ),
                    bytes_read=download.bytes_read,
                    elapsed=elapsed,
                    requests=download.requests,
                )
            return ScoutResult(
                ScoutDisposition.HOLD,
                measurement=measurement,
                reason=(
                    f"measured source parse failed closed: {detail}; "
                    "measured sample has too few unique hostnames"
                ),
            )
        if parsed is None:
            return ScoutResult(
                ScoutDisposition.HOLD,
                reason="unsupported measured source format; requires a format-specific mature parser",
            )
        elapsed = max(0.0, float(self.clock()) - started)
        measurement = self._measurement(
            parsed=parsed,
            bytes_read=download.bytes_read,
            elapsed=elapsed,
            requests=download.requests,
        )
        observed_count = measurement.observed_count_for_threshold
        novel_count = measurement.novel_count_for_threshold
        if observed_count < self.policy.min_unique_hosts:
            return ScoutResult(
                ScoutDisposition.HOLD,
                measurement=measurement,
                reason="measured sample has too few unique hostnames",
            )
        novel_fraction = novel_count / observed_count
        if (
            novel_count < self.policy.min_novel_hosts
            or novel_fraction < self.policy.min_novel_fraction
            or measurement.novel_eed_for_ranking < self.policy.min_novel_eed
        ):
            return ScoutResult(
                ScoutDisposition.HOLD,
                measurement=measurement,
                reason="measured baseline-external/EED yield below warm threshold",
            )
        return ScoutResult(
            ScoutDisposition.WARM,
            measurement=measurement,
            reason="bounded deterministic sample met warm-yield thresholds",
        )
