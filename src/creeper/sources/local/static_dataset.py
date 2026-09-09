"""Adapter for a bounded, user-supplied historical hostname list."""

from __future__ import annotations

from pathlib import Path

from creeper.authority.normalizer import normalize_official
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord, iter_source_records


class StaticDatasetAdapter:
    def __init__(self, path: Path, source_id: str = "local_dataset"):
        self.path = path
        self.source_id = source_id

    def enumerate(self):
        yield from iter_source_records(self.path, self.source_id, CandidateSourceScope.LOCAL_DISCOVERY)

    def extract_hosts(self, record: SourceRecord):
        hostname = normalize_official(record.payload)
        if hostname:
            yield HostObservation(hostname, record.source_id, record.locator, record.scope)
