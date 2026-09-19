"""Distributed direct host-year production from structured historical indexes."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import urlsplit

from creeper.authority.baseline_index import YEAR_BITS
from creeper.authority.normalizer import normalize_official
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.lease_keeper import LeaseKeeper
from creeper.distributed.models import TaskClass, TaskLease
from creeper.evidence.contracts import (
    bind_contract_to_adapter_id,
    resolve_source_evidence_contract,
)
from creeper.records.models import SourceRecord
from creeper.scheduler.leases import WorkLease
from creeper.sources.production import StructuredProductionAdapter
from creeper.sources.reservoirs import Reservoir


@dataclass(frozen=True)
class BulkChunkLimits:
    max_records: int = 5_000
    max_bytes: int = 32 * 1024 * 1024
    max_seconds: float = 5.0
    probe_batch_size: int = 2_000

    def __post_init__(self) -> None:
        if (
            self.max_records < 1
            or self.max_bytes < 1
            or self.max_seconds <= 0
            or self.probe_batch_size < 1
        ):
            raise ValueError("bulk chunk limits must be positive")


class BulkHistoricalIndexProducer:
    """Stream direct-year CDX/CDXJ sources with two-stage HY admission.

    The producer deliberately accepts only sources whose local evidence
    contract grants DIRECT_WEB_YEAR. Discovery-only data belongs to a separate
    producer and can never be upgraded to annual evidence here.
    """

    EOF_CURSOR = "EOF"

    def __init__(
        self,
        *,
        limits: BulkChunkLimits | None = None,
    ) -> None:
        self.limits = limits or BulkChunkLimits()

    @staticmethod
    def _source_spec(lease: TaskLease) -> tuple[str, str, str | None]:
        coverage = dict(lease.work.coverage)
        locator = str(coverage.get("source_locator", "")).strip()
        source_id = str(
            coverage.get("source_id", lease.work.input_identity)
        ).strip()
        cursor_end_raw = coverage.get("cursor_end")
        cursor_end = (
            None if cursor_end_raw is None else str(cursor_end_raw).strip()
        )
        if not locator or not source_id:
            raise ValueError(
                "SOURCE_SHARD requires source_locator and source identity"
            )
        if cursor_end == "":
            cursor_end = None
        return locator, source_id, cursor_end

    @staticmethod
    def _build_adapter(
        locator: str,
        source_id: str,
    ) -> StructuredProductionAdapter:
        contract = resolve_source_evidence_contract(locator)
        if not contract.grants_direct_web_year:
            raise ValueError(
                "BulkHistoricalIndexProducer accepts only direct-year "
                "structured evidence contracts"
            )
        adapter_id = bind_contract_to_adapter_id(
            "structured:distributed-bulk",
            contract,
        )
        reservoir = Reservoir(
            reservoir_id=source_id,
            domain_id="distributed-bulk",
            adapter_id=adapter_id,
            root_locator=locator,
            enumeration_kind="cursor",
            capacity_lower=0,
            evidence_mode=contract.evidence_mode,
        )
        return StructuredProductionAdapter(
            reservoir,
            evidence_contract=contract,
        )

    @staticmethod
    def _record_host(record: SourceRecord) -> str | None:
        raw = record.payload.strip()
        if not raw:
            return None
        parsed = urlsplit(raw)
        return normalize_official(parsed.hostname or raw)

    @classmethod
    def _direct_records(
        cls,
        records: Iterable[SourceRecord],
    ) -> dict[tuple[str, int], SourceRecord]:
        unique: dict[tuple[str, int], SourceRecord] = {}
        for record in records:
            year = record.source_year
            if year not in YEAR_BITS:
                continue
            if not (record.direct_year_mask & YEAR_BITS[year]):
                continue
            hostname = cls._record_host(record)
            if hostname is None:
                continue
            unique.setdefault((hostname, year), record)
        return unique

    async def _admit_records(
        self,
        lease: TaskLease,
        coordinator: CoordinatorClient,
        keeper: LeaseKeeper,
        records: Iterable[SourceRecord],
    ) -> None:
        unique = self._direct_records(records)
        items = list(unique.items())
        for start in range(0, len(items), self.limits.probe_batch_size):
            keeper.assert_owned()
            chunk = items[start : start + self.limits.probe_batch_size]
            decisions = await coordinator.hy_probe(
                keeper.lease,
                [
                    {
                        "hostname": hostname,
                        "year": year,
                        "locator": record.locator,
                    }
                    for (hostname, year), record in chunk
                ],
            )
            need = {
                (decision.hostname, decision.year)
                for decision in decisions
                if decision.status == "NEED_FULL_EVIDENCE"
            }
            if not need:
                continue
            full = []
            for (hostname, year), record in chunk:
                if (hostname, year) not in need:
                    continue
                timestamp = str(record.source_time or "").strip()
                if not timestamp:
                    raise ValueError(
                        "direct-year bulk evidence requires source_time"
                    )
                full.append(
                    {
                        "hostname": hostname,
                        "year": year,
                        "evidence_class": record.evidence_type,
                        "source": record.source_id,
                        "timestamp": timestamp,
                        "locator": record.locator,
                        "original_url": record.payload,
                        "record_type": record.record_type,
                        "artifact_ref": record.artifact_ref,
                        "temporal_semantics": record.temporal_semantics,
                        "evidence_contract_id": record.evidence_contract_id,
                        "evidence_contract_version": (
                            record.evidence_contract_version
                        ),
                    }
                )
            if full:
                await coordinator.hy_full(keeper.lease, full)

    async def __call__(
        self,
        lease: TaskLease,
        coordinator: CoordinatorClient,
        keeper: LeaseKeeper,
    ) -> None:
        if lease.work.task_class is not TaskClass.SOURCE_SHARD:
            raise ValueError(
                "BulkHistoricalIndexProducer requires SOURCE_SHARD"
            )
        if lease.cursor == self.EOF_CURSOR:
            return

        locator, source_id, cursor_end = self._source_spec(lease)
        adapter = self._build_adapter(locator, source_id)
        cursor = lease.cursor
        sequence_no = int(lease.next_sequence_no)

        try:
            while True:
                keeper.assert_owned()
                local_lease = WorkLease.create(
                    reservoir_id=source_id,
                    cursor_start=cursor,
                    cursor_end=cursor_end,
                    max_records=self.limits.max_records,
                    max_requests=1,
                    max_bytes=self.limits.max_bytes,
                    max_seconds=self.limits.max_seconds,
                )

                def read_chunk():
                    records, result = adapter.execute(local_lease)
                    return list(records), result

                records, result = await asyncio.to_thread(read_chunk)
                keeper.assert_owned()
                await self._admit_records(
                    keeper.lease,
                    coordinator,
                    keeper,
                    records,
                )

                checkpoint = (
                    self.EOF_CURSOR
                    if result.next_cursor is None
                    else result.next_cursor
                )
                status = await coordinator.commit_batch(
                    keeper.lease,
                    sequence_no=sequence_no,
                    results=[],
                    cursor_after=checkpoint,
                )
                if status not in {"COMMITTED", "ALREADY_COMMITTED"}:
                    raise RuntimeError(
                        f"unexpected checkpoint status: {status}"
                    )
                sequence_no += 1
                cursor = checkpoint
                if checkpoint == self.EOF_CURSOR:
                    return
                if not records and result.bytes_read == 0:
                    raise RuntimeError(
                        "bulk structured source made no cursor progress"
                    )
        finally:
            adapter.close()
