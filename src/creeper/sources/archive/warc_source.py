"""Production-facing WARC/ARC source lease adapter.

The adapter intentionally stops at a neutral observation contract.  Runtime
code can map :class:`WarcSourceObservation` into Creeper's canonical
``SourceRecord`` without duplicating archive parsing, cursor, or budget logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from os import PathLike

from creeper.sources.archive.warc import WarcMetadataLease, read_warc_source_lease


@dataclass(frozen=True)
class WarcSourceObservation:
    source_id: str
    locator: str
    target_uri: str
    year_hint: int
    record_type: str

    @property
    def year_hint_mask(self) -> int:
        """Competition-year hint mask derived from WARC-Date metadata.

        This is deliberately only a discovery hint. The evidence planner must
        still obtain accepted evidence through an approved provider/policy.
        """

        if 1996 <= self.year_hint <= 2001:
            return 1 << (self.year_hint - 1996)
        return 0

    @property
    def direct_year_mask(self) -> int:
        """WARC discovery metadata never self-authorizes direct evidence."""

        return 0


@dataclass(frozen=True)
class WarcSourceLeaseResult:
    observations: tuple[WarcSourceObservation, ...]
    next_cursor: str | None
    exhausted: bool
    scanned_records: int
    bytes_advanced: int


class WarcSourceLeaseExecutor:
    """Execute bounded local WARC/ARC metadata leases with durable byte cursors.

    ``year_hint`` remains a discovery-time temporal hint. This adapter never
    grants direct evidence authority; an approved TemporalPolicy must do that
    downstream.
    """

    def __init__(
        self,
        source: str | PathLike[str],
        *,
        source_id: str,
        target_year_from: int = 1996,
        target_year_to: int = 2001,
        remote_block_size: int = 4 * 1024 * 1024,
        max_record_content_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if not source_id.strip():
            raise ValueError("source_id must be non-empty")
        if target_year_from > target_year_to:
            raise ValueError("target_year_from must not exceed target_year_to")
        self.source = source
        self.source_id = source_id
        if (
            not isinstance(remote_block_size, int)
            or isinstance(remote_block_size, bool)
            or remote_block_size < 1
        ):
            raise ValueError("remote_block_size must be a positive integer")
        if (
            not isinstance(max_record_content_bytes, int)
            or isinstance(max_record_content_bytes, bool)
            or max_record_content_bytes < 1
        ):
            raise ValueError("max_record_content_bytes must be a positive integer")
        self.target_year_from = target_year_from
        self.target_year_to = target_year_to
        self.remote_block_size = remote_block_size
        self.max_record_content_bytes = max_record_content_bytes

    def execute(
        self,
        *,
        cursor: str | None,
        max_scanned_records: int,
        max_archive_bytes: int,
    ) -> WarcSourceLeaseResult:
        lease: WarcMetadataLease = read_warc_source_lease(
            self.source,
            cursor=cursor,
            max_scanned_records=max_scanned_records,
            max_archive_bytes=max_archive_bytes,
            target_year_from=self.target_year_from,
            target_year_to=self.target_year_to,
            remote_block_size=self.remote_block_size,
            max_record_content_bytes=self.max_record_content_bytes,
        )
        observations = tuple(
            WarcSourceObservation(
                source_id=self.source_id,
                locator=f"warc-byte:{record.offset}:{record.length}",
                target_uri=record.target_uri,
                year_hint=record.source_year,
                record_type=record.record_type,
            )
            for record in lease.records
            if record.target_uri is not None and record.source_year is not None
        )
        return WarcSourceLeaseResult(
            observations=observations,
            next_cursor=lease.next_cursor,
            exhausted=lease.exhausted,
            scanned_records=lease.scanned_records,
            bytes_advanced=lease.bytes_advanced,
        )
