"""ISC survey adapter; records remain reference candidates until validated."""

from __future__ import annotations

from pathlib import Path

from creeper.authority.normalizer import normalize_official
from creeper.authority.paths import find_baseline_dir
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord, iter_source_records


class ISCAdapter:
    source_id = "isc_reference"

    def __init__(self, root: Path):
        self.root = root

    def enumerate(self):
        directory = find_baseline_dir(self.root) / "isc_survey_hostnames"
        for path in sorted(directory.glob("*.txt")):
            year = int(path.name[:4]) if path.name[:4].isdigit() else None
            yield from iter_source_records(
                path, self.source_id, CandidateSourceScope.ISC_REFERENCE, year
            )

    def extract_hosts(self, record: SourceRecord):
        hostname = normalize_official(record.payload)
        if hostname:
            yield HostObservation(
                hostname, record.source_id, record.locator, record.scope, record.source_year
            )
