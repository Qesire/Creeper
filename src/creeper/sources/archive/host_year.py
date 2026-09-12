"""Streaming host-year reduction for timestamp-bearing archive indexes.

Production CDX/CDXJ processing should discard capture-level multiplicity as
early as possible.  These primitives reuse Creeper's existing mature line
parsers, collapse contiguous captures into a six-bit 1996-2001 year mask, and
perform bounded baseline reconciliation without retaining the full hostname set.

The reducer assumes input is grouped by hostname, which is the normal property
of SURT/urlkey-sorted CDX/CDXJ indexes.  It deliberately does not pretend that an
arbitrary URL list has that property.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from decimal import Decimal
from itertools import islice
from urllib.parse import urlsplit

from creeper.authority.baseline_index import (
    ALL_YEAR_MASK,
    YEAR_BITS,
    BaselineIndex,
    novel_year_mask,
)
from creeper.authority.normalizer import normalize_official
from creeper.records.models import SourceRecord
from creeper.source_discovery.index_space import RegionSynopsis
from creeper.source_discovery.models import MeasurementMode
from creeper.source_discovery.overlap import MinHashAccumulator
from creeper.sources.archive.cdx import parse_cdx_line
from creeper.sources.archive.cdxj import parse_cdxj_line


@dataclass(frozen=True)
class HostYearMask:
    """All target-year captures for one contiguous hostname run."""

    hostname: str
    year_mask: int
    capture_count: int
    source_id: str
    first_locator: str
    last_locator: str

    def __post_init__(self) -> None:
        normalized = normalize_official(self.hostname)
        if normalized is None or normalized != self.hostname:
            raise ValueError("hostname must be official-normalized")
        if self.year_mask <= 0 or self.year_mask & ~ALL_YEAR_MASK:
            raise ValueError("year_mask must contain only target-year bits")
        if self.capture_count < 1:
            raise ValueError("capture_count must be positive")
        if not self.source_id or not self.first_locator or not self.last_locator:
            raise ValueError("source and locator provenance are required")

    @property
    def host_year_count(self) -> int:
        return self.year_mask.bit_count()


@dataclass(frozen=True)
class NovelHostYearMask:
    """One reduced hostname after exact annual-baseline subtraction."""

    hostname: str
    source_year_mask: int
    baseline_year_mask: int
    novel_year_mask: int
    capture_count: int
    source_id: str
    first_locator: str
    last_locator: str

    @property
    def novel_host_year_count(self) -> int:
        return self.novel_year_mask.bit_count()


def _hostname_from_record(record: SourceRecord) -> str | None:
    payload = record.payload.strip()
    if not payload:
        return None
    if "://" in payload or payload.startswith("//"):
        parsed = urlsplit(payload if not payload.startswith("//") else "http:" + payload)
        return normalize_official(parsed.hostname or "")
    return normalize_official(payload)


class ContiguousHostYearReducer:
    """Stateful O(1)-hostname reducer for grouped capture streams."""

    def __init__(self, *, target_mask: int = ALL_YEAR_MASK) -> None:
        if target_mask <= 0 or target_mask & ~ALL_YEAR_MASK:
            raise ValueError("target_mask must contain target-year bits")
        self.target_mask = int(target_mask)
        self._hostname: str | None = None
        self._mask = 0
        self._capture_count = 0
        self._source_id = ""
        self._first_locator = ""
        self._last_locator = ""

    def _emit(self) -> HostYearMask | None:
        if self._hostname is None or self._mask == 0:
            return None
        return HostYearMask(
            hostname=self._hostname,
            year_mask=self._mask,
            capture_count=self._capture_count,
            source_id=self._source_id,
            first_locator=self._first_locator,
            last_locator=self._last_locator,
        )

    def feed(self, record: SourceRecord) -> HostYearMask | None:
        hostname = _hostname_from_record(record)
        year = record.source_year
        bit = YEAR_BITS.get(year, 0) if year is not None else 0
        bit &= self.target_mask
        if hostname is None or bit == 0:
            return None

        emitted: HostYearMask | None = None
        if self._hostname is not None and hostname != self._hostname:
            emitted = self._emit()
            self._mask = 0
            self._capture_count = 0
            self._source_id = ""
            self._first_locator = ""
            self._last_locator = ""

        if self._hostname != hostname:
            self._hostname = hostname
            self._source_id = record.source_id
            self._first_locator = record.locator

        self._mask |= bit
        self._capture_count += 1
        self._last_locator = record.locator
        return emitted

    def finish(self) -> HostYearMask | None:
        emitted = self._emit()
        self._hostname = None
        self._mask = 0
        self._capture_count = 0
        self._source_id = ""
        self._first_locator = ""
        self._last_locator = ""
        return emitted


def iter_host_year_masks(
    records: Iterable[SourceRecord],
    *,
    target_mask: int = ALL_YEAR_MASK,
) -> Iterator[HostYearMask]:
    reducer = ContiguousHostYearReducer(target_mask=target_mask)
    for record in records:
        emitted = reducer.feed(record)
        if emitted is not None:
            yield emitted
    emitted = reducer.finish()
    if emitted is not None:
        yield emitted


def iter_cdx_host_year_masks(
    lines: Iterable[str],
    *,
    index_format: str,
    source_id: str,
    locator_prefix: str,
    target_mask: int = ALL_YEAR_MASK,
) -> Iterator[HostYearMask]:
    """Parse and reduce a grouped CDX/CDXJ line stream.

    Existing parse_cdx_line / parse_cdxj_line remain the format authority.  This
    layer only removes repeated captures once those parsers have accepted them.
    """

    normalized_format = index_format.strip().upper()
    if normalized_format not in {"CDX", "CDXJ"}:
        raise ValueError("index_format must be CDX or CDXJ")

    def records() -> Iterator[SourceRecord]:
        parser = parse_cdx_line if normalized_format == "CDX" else parse_cdxj_line
        for line_number, line in enumerate(lines, 1):
            record = parser(
                line,
                source_id=source_id,
                locator=f"{locator_prefix}:{line_number}",
            )
            if record is not None:
                yield record

    yield from iter_host_year_masks(records(), target_mask=target_mask)


def _batched(
    values: Iterable[HostYearMask],
    *,
    batch_size: int,
) -> Iterator[list[HostYearMask]]:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    source = iter(values)
    while True:
        batch = list(islice(source, batch_size))
        if not batch:
            return
        yield batch


def iter_baseline_difference(
    values: Iterable[HostYearMask],
    baseline: BaselineIndex,
    *,
    batch_size: int = 50_000,
) -> Iterator[NovelHostYearMask]:
    """Subtract annual baseline membership in bounded batches."""

    for batch in _batched(values, batch_size=batch_size):
        resolved = baseline.resolve_batch(item.hostname for item in batch)
        for item in batch:
            baseline_mask = resolved.get(item.hostname, (0, False))[0]
            novel_mask = novel_year_mask(item.year_mask, baseline_mask)
            yield NovelHostYearMask(
                hostname=item.hostname,
                source_year_mask=item.year_mask,
                baseline_year_mask=baseline_mask,
                novel_year_mask=novel_mask,
                capture_count=item.capture_count,
                source_id=item.source_id,
                first_locator=item.first_locator,
                last_locator=item.last_locator,
            )


def _histogram_tuple(counter: Counter) -> tuple[tuple, ...]:
    return tuple(sorted(counter.items(), key=lambda item: item[0]))


def summarize_region(
    values: Iterable[HostYearMask],
    baseline: BaselineIndex,
    english_weights: Mapping[str, Decimal | float | int],
    *,
    region_key: str,
    bytes_read: int,
    requests: int,
    confidence: float,
    complete: bool,
    batch_size: int = 50_000,
    minhash_width: int = 64,
) -> RegionSynopsis:
    """Build a bounded-memory baseline-aware synopsis for one region.

    The MinHash represents *baseline-external host-year pairs*, because those
    are the objects whose overlap matters to marginal competition reward.
    """

    observed_years: Counter[int] = Counter()
    novel_years: Counter[int] = Counter()
    tld_host_years: Counter[str] = Counter()
    sketch = MinHashAccumulator(width=minhash_width)
    sampled_records = 0
    unique_hosts = 0
    novel_hosts = 0
    observed_pairs = 0
    novel_pairs = 0
    novel_eed = Decimal("0")

    for batch in _batched(values, batch_size=batch_size):
        resolved = baseline.resolve_batch(item.hostname for item in batch)
        for item in batch:
            sampled_records += item.capture_count
            unique_hosts += 1
            tld = item.hostname.rsplit(".", 1)[-1]
            baseline_mask = resolved.get(item.hostname, (0, False))[0]
            novel_mask = novel_year_mask(item.year_mask, baseline_mask)
            if novel_mask:
                novel_hosts += 1

            for year, bit in YEAR_BITS.items():
                if item.year_mask & bit:
                    observed_years[year] += 1
                    tld_host_years[tld] += 1
                    observed_pairs += 1
                if novel_mask & bit:
                    novel_years[year] += 1
                    novel_pairs += 1
                    novel_eed += Decimal(str(english_weights.get(tld, 0)))
                    sketch.update(f"{item.hostname}\t{year}")

    return RegionSynopsis(
        region_key=region_key,
        sampled_records=sampled_records,
        unique_hosts=unique_hosts,
        novel_hosts=novel_hosts,
        observed_host_year_pairs=observed_pairs,
        novel_host_year_pairs=novel_pairs,
        novel_eed=float(novel_eed),
        bytes_read=bytes_read,
        requests=requests,
        measurement_mode=MeasurementMode.HOST_YEAR,
        observed_year_histogram=_histogram_tuple(observed_years),
        novel_year_histogram=_histogram_tuple(novel_years),
        tld_host_year_histogram=_histogram_tuple(tld_host_years),
        minhash_values=sketch.sketch().values,
        confidence=confidence,
        complete=complete,
    )
