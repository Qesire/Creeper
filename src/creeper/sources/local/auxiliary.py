"""V3 auxiliary historical URL lists as a provenance-preserving source."""

from __future__ import annotations

from pathlib import Path

from creeper.authority.normalizer import normalize_official
from creeper.authority.paths import find_baseline_dir
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord


class V3AuxiliaryURLAdapter:
    """Enumerate only the non-authoritative V3 deduplicated URL lists.

    These files are allowed for candidate discovery by the V3 package guide,
    but they never become annual authority records automatically. Every record
    carries the exact file and line number so later evidence can be audited.
    """

    scope = CandidateSourceScope.LOCAL_DISCOVERY

    def __init__(self, task_root: Path):
        self.task_root = task_root
        self.directory = find_baseline_dir(task_root, require_annual=False)

    def enumerate(self, *, limit_per_file: int | None = None, total_limit: int | None = None):
        if limit_per_file is not None and limit_per_file < 1:
            raise ValueError("limit_per_file must be positive")
        if total_limit is not None and total_limit < 1:
            raise ValueError("total_limit must be positive")
        emitted = 0
        for path in sorted(self.directory.glob("deduplicated_urls_*.txt")):
            source_id = f"v3_auxiliary:{path.stem}"
            with path.open("r", encoding="utf-8", errors="replace") as source:
                emitted_in_file = 0
                for line_number, line in enumerate(source, 1):
                    if limit_per_file is not None and emitted_in_file >= limit_per_file:
                        break
                    if total_limit is not None and emitted >= total_limit:
                        return
                    yield SourceRecord(
                        source_id=source_id,
                        locator=f"{path}:{line_number}",
                        payload=line.rstrip("\n"),
                        scope=self.scope,
                    )
                    emitted += 1
                    emitted_in_file += 1

    def extract_hosts(self, record: SourceRecord):
        hostname = normalize_official(record.payload)
        if hostname:
            yield HostObservation(
                hostname=hostname,
                source_id=record.source_id,
                locator=record.locator,
                scope=record.scope,
            )
