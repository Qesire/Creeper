"""Eligibility and lifecycle helpers for selective historical-index execution."""

from __future__ import annotations

from urllib.parse import urlsplit

from creeper.source_discovery.index_space import (
    RegionState,
    SourceIndexSpec,
)


TERMINAL_LEAF_STATES = frozenset({
    RegionState.HARVESTED,
    RegionState.DROPPED,
})


def index_region_optimizer_eligible(index: SourceIndexSpec) -> bool:
    """Return whether current region probe + exact harvest code can own an index.

    Compressed CDX/CDXJ stays on the mature sequential producer path because
    compressed byte offsets are not decompressed record offsets. Remote indexes
    additionally require verified HTTP Range support; local files can seek
    directly without that transport capability bit.
    """

    if not index.capabilities.direct_evidence_authority:
        return False
    if index.capabilities.format not in {"CDX", "CDXJ"}:
        return False
    parsed = urlsplit(index.locator)
    if parsed.path.lower().endswith((".gz", ".bz2", ".xz", ".zst", ".zip")):
        return False
    if parsed.scheme in {"", "file"}:
        return True
    if parsed.scheme in {"http", "https"}:
        return bool(index.capabilities.range_supported)
    return False
