"""Interfaces shared by bounded local and HTTP source adapters."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Protocol, TYPE_CHECKING

from creeper.records.models import HostObservation, SourceRecord

if TYPE_CHECKING:
    from creeper.scheduler.leases import LeaseResult, WorkLease
    from creeper.sources.reservoirs import ReservoirEstimate


class SourceAdapter(Protocol):
    source_id: str

    def enumerate(self) -> Iterator[SourceRecord]: ...

    def extract_hosts(self, record: SourceRecord) -> Iterable[HostObservation]: ...


class ReservoirAdapter(Protocol):
    """Adapter for finite, scheduler-controlled reservoir work."""

    adapter_id: str

    def estimate(self) -> "ReservoirEstimate": ...

    def execute(self, lease: "WorkLease") -> tuple[Iterator[SourceRecord], "LeaseResult"]: ...
