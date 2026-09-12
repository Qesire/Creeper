"""Bounded region probes for line-oriented historical archive indexes.

The implementation deliberately reuses Creeper's existing CDX/CDXJ parsers and
BaselineIndex.  It does not implement a new archive index server.  Remote
uncompressed indexes are sampled with explicit HTTP byte ranges; local indexes
use ordinary seek/read.  Compressed indexes fail closed because compressed-byte
offsets are not line/key offsets in the decompressed CDX stream.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from urllib.parse import unquote, urlsplit
from urllib.request import url2pathname

import httpx

from creeper.authority.baseline_index import BaselineIndex
from creeper.source_discovery.index_space import HarvestRegion, RegionSynopsis, SourceIndexSpec
from creeper.sources.archive.host_year import (
    HostYearMask,
    iter_cdx_host_year_masks,
    summarize_region,
)


class RegionProbeError(ValueError):
    """A region cannot be safely sampled with the proven source capabilities."""


@dataclass(frozen=True)
class RegionProbePolicy:
    max_sample_bytes: int = 512 * 1024
    sample_windows: int = 4
    timeout_seconds: float = 20.0
    baseline_batch_size: int = 50_000
    minhash_width: int = 64

    def __post_init__(self) -> None:
        if self.max_sample_bytes < 1:
            raise ValueError("max_sample_bytes must be positive")
        if self.sample_windows < 1:
            raise ValueError("sample_windows must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.baseline_batch_size < 1:
            raise ValueError("baseline_batch_size must be positive")
        if self.minhash_width < 1:
            raise ValueError("minhash_width must be positive")


@dataclass(frozen=True)
class SampledByteRange:
    start: int
    end: int
    bytes_read: int

    def __post_init__(self) -> None:
        if self.start < 0 or self.end < self.start or self.bytes_read < 0:
            raise ValueError("invalid sampled byte range")


@dataclass(frozen=True)
class RegionProbeResult:
    region: HarvestRegion
    synopsis: RegionSynopsis
    sampled_ranges: tuple[SampledByteRange, ...]


def _is_compressed(locator: str) -> bool:
    return urlsplit(locator).path.lower().endswith(
        (".gz", ".bz2", ".xz", ".zst", ".zip")
    )


def _local_path(locator: str) -> Path | None:
    parsed = urlsplit(locator)
    if parsed.scheme == "":
        return Path(locator)
    if parsed.scheme != "file":
        return None
    if parsed.netloc not in {"", "localhost"}:
        raise RegionProbeError("file:// region probes must reference the local host")
    return Path(url2pathname(unquote(parsed.path)))


def _merge_host_masks(values: list[HostYearMask]) -> list[HostYearMask]:
    """Union a bounded probe sample by hostname.

    A probe may read several non-contiguous windows from one sorted index.
    Treating each window independently would over-count a hostname that happens
    to appear in multiple windows, so the bounded sample is reconciled here.
    """

    merged: dict[str, HostYearMask] = {}
    for value in values:
        existing = merged.get(value.hostname)
        if existing is None:
            merged[value.hostname] = value
            continue
        merged[value.hostname] = HostYearMask(
            hostname=value.hostname,
            year_mask=existing.year_mask | value.year_mask,
            capture_count=existing.capture_count + value.capture_count,
            source_id=existing.source_id,
            first_locator=existing.first_locator,
            last_locator=value.last_locator,
        )
    return [merged[key] for key in sorted(merged)]


def _trim_complete_lines(
    payload: bytes,
    *,
    start: int,
    end: int,
    object_end: int,
) -> bytes:
    """Keep only complete lines wholly represented by this sampled window.

    For a non-zero range we conservatively discard the first line because the
    window may begin in its middle.  At a non-terminal end we similarly discard
    the final partial line.  Losing at most two records is preferable to
    manufacturing malformed CDX rows or double-counting boundary fragments.
    """

    if not payload:
        return b""
    view = payload
    if start > 0:
        boundary = view.find(b"\n")
        if boundary < 0:
            return b""
        view = view[boundary + 1 :]
    if end < object_end:
        boundary = view.rfind(b"\n")
        if boundary < 0:
            return b""
        view = view[: boundary + 1]
    return view


class RegionProbeExecutor:
    """Measure one CDX/CDXJ region with finite byte and request budgets."""

    def __init__(
        self,
        baseline: BaselineIndex,
        english_weights: dict[str, Decimal],
        *,
        client: httpx.AsyncClient | None = None,
        policy: RegionProbePolicy | None = None,
    ) -> None:
        self.baseline = baseline
        self.english_weights = dict(english_weights)
        self.client = client
        self.policy = policy or RegionProbePolicy()

    @staticmethod
    def _validate_index(index: SourceIndexSpec) -> None:
        if index.capabilities.format not in {"CDX", "CDXJ"}:
            raise RegionProbeError(
                f"region tomography currently requires CDX/CDXJ, got "
                f"{index.capabilities.format}"
            )
        if _is_compressed(index.locator):
            raise RegionProbeError(
                "compressed CDX/CDXJ cannot use decompressed byte-region tomography"
            )
        parsed = urlsplit(index.locator)
        if parsed.scheme in {"http", "https"} and not index.capabilities.range_supported:
            raise RegionProbeError(
                "remote byte-region tomography requires verified HTTP Range support"
            )
        if parsed.scheme not in {"", "file", "http", "https"}:
            raise RegionProbeError(
                f"unsupported region-probe scheme: {parsed.scheme!r}"
            )

    async def _remote_size(self, locator: str) -> tuple[int, int]:
        if self.client is None:
            raise RegionProbeError("remote region probe requires an HTTP client")
        timeout = httpx.Timeout(self.policy.timeout_seconds)
        response = await self.client.head(
            locator,
            headers={"Accept-Encoding": "identity"},
            timeout=timeout,
        )
        requests = 1
        if response.status_code < 400:
            raw = response.headers.get("content-length")
            if raw is not None and raw.isdigit():
                return int(raw), requests

        # Some archive hosts do not implement HEAD correctly. A one-byte Range
        # request still discovers the object size from Content-Range without
        # downloading the object.
        response = await self.client.get(
            locator,
            headers={
                "Range": "bytes=0-0",
                "Accept-Encoding": "identity",
            },
            timeout=timeout,
        )
        requests += 1
        if response.status_code != 206:
            raise RegionProbeError(
                "unable to determine remote object size with bounded Range access"
            )
        content_range = response.headers.get("content-range", "")
        if "/" not in content_range:
            raise RegionProbeError("Range response omitted total object size")
        raw_total = content_range.rsplit("/", 1)[-1].strip()
        if not raw_total.isdigit():
            raise RegionProbeError("Range response has invalid total object size")
        return int(raw_total), requests

    async def _object_size(self, index: SourceIndexSpec) -> tuple[int, int]:
        if index.content_length is not None:
            return index.content_length, 0
        path = _local_path(index.locator)
        if path is not None:
            try:
                return path.stat().st_size, 0
            except OSError as exc:
                raise RegionProbeError(f"unable to stat local index: {exc}") from exc
        return await self._remote_size(index.locator)

    async def _read_range(
        self,
        locator: str,
        *,
        start: int,
        end: int,
        object_end: int,
    ) -> tuple[bytes, int]:
        path = _local_path(locator)
        if path is not None:
            try:
                with path.open("rb") as source:
                    source.seek(start)
                    payload = source.read(end - start + 1)
            except OSError as exc:
                raise RegionProbeError(f"unable to read local index range: {exc}") from exc
            return (
                _trim_complete_lines(
                    payload,
                    start=start,
                    end=start + max(0, len(payload) - 1),
                    object_end=object_end,
                ),
                0,
            )

        if self.client is None:
            raise RegionProbeError("remote region probe requires an HTTP client")
        timeout = httpx.Timeout(self.policy.timeout_seconds)
        response = await self.client.get(
            locator,
            headers={
                "Range": f"bytes={start}-{end}",
                "Accept-Encoding": "identity",
            },
            timeout=timeout,
        )
        if response.status_code != 206:
            raise RegionProbeError(
                f"remote source ignored/failed bounded Range request: "
                f"HTTP {response.status_code}"
            )
        payload = response.content
        if len(payload) > end - start + 1:
            raise RegionProbeError("Range response exceeded requested byte budget")
        return (
            _trim_complete_lines(
                payload,
                start=start,
                end=start + max(0, len(payload) - 1),
                object_end=object_end,
            ),
            1,
        )

    @staticmethod
    def _window_ranges(
        *,
        start: int,
        end: int,
        budget: int,
        windows: int,
    ) -> tuple[tuple[int, int], ...]:
        region_size = end - start + 1
        if region_size <= budget:
            return ((start, end),)

        windows = min(windows, budget, region_size)
        per_window = max(1, budget // windows)
        max_start = max(start, end - per_window + 1)
        starts: list[int] = []
        for index in range(windows):
            offset = (
                start
                if windows == 1
                else round(start + (max_start - start) * index / (windows - 1))
            )
            if offset not in starts:
                starts.append(offset)
        return tuple(
            (offset, min(end, offset + per_window - 1))
            for offset in starts
        )

    async def probe(
        self,
        index: SourceIndexSpec,
        region: HarvestRegion,
    ) -> RegionProbeResult:
        self._validate_index(index)
        if region.index_key != index.index_key:
            raise RegionProbeError("region does not belong to supplied index")

        object_size, metadata_requests = await self._object_size(index)
        if object_size < 1:
            raise RegionProbeError("cannot probe an empty historical index")
        object_end = object_size - 1
        region_start = region.byte_start if region.byte_start is not None else 0
        region_end = region.byte_end if region.byte_end is not None else object_end
        if region_start > object_end:
            raise RegionProbeError("region starts beyond end of source")
        region_end = min(region_end, object_end)
        normalized_region = replace(
            region,
            byte_start=region_start,
            byte_end=region_end,
        )
        region_size = region_end - region_start + 1
        ranges = self._window_ranges(
            start=region_start,
            end=region_end,
            budget=min(self.policy.max_sample_bytes, region_size),
            windows=self.policy.sample_windows,
        )

        host_masks: list[HostYearMask] = []
        sampled_ranges: list[SampledByteRange] = []
        source_requests = metadata_requests
        bytes_read = 0
        source_format = index.capabilities.format

        for start, end in ranges:
            payload, requests = await self._read_range(
                index.locator,
                start=start,
                end=end,
                object_end=object_end,
            )
            source_requests += requests
            raw_bytes = end - start + 1
            bytes_read += raw_bytes
            sampled_ranges.append(
                SampledByteRange(start=start, end=end, bytes_read=raw_bytes)
            )
            if not payload:
                continue
            text = payload.decode("utf-8", errors="replace")
            host_masks.extend(
                iter_cdx_host_year_masks(
                    text.splitlines(),
                    index_format=source_format,
                    source_id=index.source_key,
                    locator_prefix=f"{index.locator}:bytes={start}-{end}",
                )
            )

        merged = _merge_host_masks(host_masks)
        complete = (
            len(ranges) == 1
            and ranges[0] == (region_start, region_end)
        )
        coverage = min(1.0, bytes_read / max(1, region_size))
        synopsis = summarize_region(
            merged,
            self.baseline,
            self.english_weights,
            region_key=normalized_region.region_key,
            bytes_read=bytes_read,
            requests=source_requests,
            confidence=coverage,
            complete=complete,
            batch_size=self.policy.baseline_batch_size,
            minhash_width=self.policy.minhash_width,
        )
        return RegionProbeResult(
            region=normalized_region,
            synopsis=synopsis,
            sampled_ranges=tuple(sampled_ranges),
        )
