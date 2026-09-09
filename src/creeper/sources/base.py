"""Interfaces shared by bounded local and HTTP source adapters."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Protocol

from creeper.records.models import HostObservation, SourceRecord


class SourceAdapter(Protocol):
    source_id: str

    def enumerate(self) -> Iterator[SourceRecord]: ...

    def extract_hosts(self, record: SourceRecord) -> Iterable[HostObservation]: ...
