"""Distributed byte-range harvest for evidence-grade historical indexes.

Remote workers only perform transport, parsing, and contiguous host-year
reduction. Baseline subtraction, EvidencePlanner decisions, EvidenceStore
writes, source attribution, and region lifecycle transitions remain on the
central historical-index runtime.
"""

from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator, Mapping
from dataclasses import asdict
from urllib.parse import urlsplit

import httpx

from creeper.authority.baseline_index import YEAR_BITS
from creeper.distributed.models import (
    ResultBatch,
    TaskClass,
    TaskLease,
    WorkDefinition,
)
from creeper.distributed.worker import ProducerContext
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import SourceRecord
from creeper.scheduler.leases import LeaseResult, WorkLease
from creeper.source_discovery.harvest import (
    RegionHarvestError,
    RegionHarvestExecutor,
    _cursor_value,
)
from creeper.source_discovery.index_identity import (
    HistoricalIndexObjectIdentity,
    ensure_same_historical_index_object,
    remote_identity_from_headers,
)
from creeper.source_discovery.index_space import RegionState
from creeper.sources.archive.cdx import parse_cdx_line
from creeper.sources.archive.cdxj import parse_cdxj_line
from creeper.sources.archive.host_year import (
    ContiguousHostYearWitnessReducer,
    HostYearWitness,
    HostYearWitnessGroup,
)


PRODUCER_NAME = "BulkShardProducer"
ALGORITHM_VERSION = "bulk-shard-v1"


def _json_object(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        decoded=json.loads(value)
        if isinstance(decoded,dict):
            return decoded
    raise ValueError("Fabric payload must be a JSON object")


def _identity_payload(identity: HistoricalIndexObjectIdentity) -> dict[str, object]:
    return asdict(identity)


def _identity_from_payload(raw: object) -> HistoricalIndexObjectIdentity:
    if not isinstance(raw,Mapping):
        raise ValueError("bulk shard requires an object identity")
    return HistoricalIndexObjectIdentity(
        kind=str(raw["kind"]),
        content_length=(
            None if raw.get("content_length") is None
            else int(raw["content_length"])
        ),
        etag=None if raw.get("etag") is None else str(raw["etag"]),
        last_modified=(
            None if raw.get("last_modified") is None
            else str(raw["last_modified"])
        ),
        local_device=(
            None if raw.get("local_device") is None
            else int(raw["local_device"])
        ),
        local_inode=(
            None if raw.get("local_inode") is None
            else int(raw["local_inode"])
        ),
        local_mtime_ns=(
            None if raw.get("local_mtime_ns") is None
            else int(raw["local_mtime_ns"])
        ),
        sampled_fingerprint=(
            None if raw.get("sampled_fingerprint") is None
            else str(raw["sampled_fingerprint"])
        ),
    )


def serialize_witness_group(group: HostYearWitnessGroup) -> dict[str, object]:
    return {
        "kind":"BULK_WITNESS_GROUP",
        "hostname":group.hostname,
        "capture_count":group.capture_count,
        "witnesses":[
            {
                "hostname":item.hostname,
                "year":item.year,
                "source_id":item.source_id,
                "locator":item.locator,
                "source_time":item.source_time,
                "record_type":item.record_type,
                "artifact_ref":item.artifact_ref,
                "original_url":item.original_url,
            }
            for item in group.witnesses
        ],
    }


def deserialize_witness_group(raw: Mapping[str, object]) -> HostYearWitnessGroup:
    if str(raw.get("kind",""))!="BULK_WITNESS_GROUP":
        raise ValueError("unexpected bulk witness result kind")
    witnesses_raw=raw.get("witnesses")
    if not isinstance(witnesses_raw,list):
        raise ValueError("bulk witness group must contain witnesses")
    witnesses=[]
    for item in witnesses_raw:
        if not isinstance(item,Mapping):
            raise ValueError("bulk witness must be an object")
        witnesses.append(
            HostYearWitness(
                hostname=str(item["hostname"]),
                year=int(item["year"]),
                source_id=str(item["source_id"]),
                locator=str(item["locator"]),
                source_time=(
                    None if item.get("source_time") is None
                    else str(item["source_time"])
                ),
                record_type=str(item.get("record_type","")),
                artifact_ref=str(item.get("artifact_ref","")),
                original_url=str(item["original_url"]),
            )
        )
    return HostYearWitnessGroup(
        hostname=str(raw["hostname"]),
        witnesses=tuple(witnesses),
        capture_count=int(raw["capture_count"]),
    )


def witness_to_source_record(witness: HostYearWitness) -> SourceRecord:
    return SourceRecord(
        source_id=witness.source_id,
        locator=witness.locator,
        payload=witness.original_url,
        scope=CandidateSourceScope.LOCAL_DISCOVERY,
        source_year=witness.year,
        record_type=witness.record_type,
        source_time=witness.source_time,
        artifact_ref=witness.artifact_ref,
        direct_year_mask=YEAR_BITS[witness.year],
    )


def bulk_shard_work_definition(
    *,
    region_key: str,
    index_key: str,
    source_key: str,
    locator: str,
    index_format: str,
    byte_start: int,
    byte_end_exclusive: int,
    expected_identity: HistoricalIndexObjectIdentity,
    boundary_record_max_bytes: int,
    timeout_seconds: float,
    group_batch_size: int = 256,
) -> WorkDefinition:
    if urlsplit(locator).scheme.lower() not in {"http","https"}:
        raise ValueError("distributed bulk shard requires HTTP(S)")
    normalized_format=index_format.strip().upper()
    if normalized_format not in {"CDX","CDXJ"}:
        raise ValueError("distributed bulk shard requires CDX/CDXJ")
    if byte_start<0 or byte_end_exclusive<=byte_start:
        raise ValueError("invalid bulk shard byte interval")
    if (
        boundary_record_max_bytes<1
        or timeout_seconds<=0
        or group_batch_size<1
    ):
        raise ValueError("invalid bulk shard execution policy")
    if expected_identity.kind!="remote" or not expected_identity.is_verifiable:
        raise ValueError("distributed bulk shard requires bound remote identity")
    payload={
        "region_key":region_key,
        "index_key":index_key,
        "source_key":source_key,
        "locator":locator,
        "index_format":normalized_format,
        "byte_start":int(byte_start),
        "byte_end_exclusive":int(byte_end_exclusive),
        "expected_identity":_identity_payload(expected_identity),
        "boundary_record_max_bytes":int(boundary_record_max_bytes),
        "timeout_seconds":float(timeout_seconds),
        "group_batch_size":int(group_batch_size),
    }
    return WorkDefinition(
        producer=PRODUCER_NAME,
        task_class=TaskClass.SOURCE_SHARD,
        input_identity=(
            f"{region_key}:{byte_start}:{byte_end_exclusive}:"
            f"{expected_identity.content_length}:"
            f"{expected_identity.etag or expected_identity.last_modified or expected_identity.sampled_fingerprint}"
        ),
        payload=payload,
        partition=region_key,
        algorithm_version=ALGORITHM_VERSION,
        required_capabilities=("STREAMING_BULK","ARTIFACT_FETCH"),
        required_providers=(),
        priority=1.0,
        max_attempts=8,
    )


class BulkShardProducer:
    """Stream one immutable HTTP range and return reduced real witnesses."""

    @staticmethod
    def _parse_record(
        *,
        index_format: str,
        source_key: str,
        locator: str,
        raw: bytes,
        line_start: int,
    ) -> SourceRecord | None:
        line=raw.decode("utf-8",errors="replace").rstrip("\r\n")
        record_locator=f"{locator}:byte:{line_start}"
        if index_format=="CDXJ":
            return parse_cdxj_line(
                line,
                source_id=source_key,
                locator=record_locator,
            )
        return parse_cdx_line(
            line,
            source_id=source_key,
            locator=record_locator,
        )

    async def run(
        self,
        lease: TaskLease,
        context: ProducerContext,
    ) -> AsyncIterator[ResultBatch]:
        payload=_json_object(lease.work.payload)
        locator=str(payload["locator"])
        index_format=str(payload["index_format"]).upper()
        source_key=str(payload["source_key"])
        expected=_identity_from_payload(payload["expected_identity"])
        end_exclusive=int(payload["byte_end_exclusive"])
        boundary_max=int(payload["boundary_record_max_bytes"])
        timeout_seconds=float(payload["timeout_seconds"])
        group_batch_size=int(payload["group_batch_size"])

        start=(
            int(lease.cursor.removeprefix("byte:"))
            if lease.cursor is not None and lease.cursor.startswith("byte:")
            else int(payload["byte_start"])
        )
        if start>=end_exclusive:
            yield ResultBatch(
                task_id=lease.task_id,
                generation=lease.generation,
                sequence_no=lease.next_sequence_no,
                results=(
                    {
                        "kind":"BULK_SUMMARY",
                        "records":0,
                        "bytes_read":0,
                        "requests":0,
                        "elapsed_seconds":0.0,
                    },
                ),
                cursor_after=None,
                final=True,
            )
            return

        request_start=start-1 if start>0 else start
        expected_size=expected.content_length
        assert expected_size is not None
        request_end=min(
            end_exclusive-1+boundary_max,
            expected_size-1,
        )
        headers={
            "Range":f"bytes={request_start}-{request_end}",
            "Accept-Encoding":"identity",
        }
        if expected.if_range_validator is not None:
            headers["If-Range"]=expected.if_range_validator

        sequence=lease.next_sequence_no
        reducer=ContiguousHostYearWitnessReducer()
        pending: list[dict[str,object]]=[]
        buffer=b""
        boundary_ready=start==0
        prefix_checked=start==0
        cursor=start
        parsed_records=0
        bytes_read=0
        started=time.monotonic()

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(timeout_seconds),
            trust_env=False,
            headers={"User-Agent":"Creeper-fabric-bulk/1.0"},
        ) as client:
            async with client.stream(
                "GET",
                locator,
                headers=headers,
            ) as response:
                if response.status_code!=206:
                    if response.status_code==200 and expected.if_range_validator:
                        raise RegionHarvestError(
                            "historical index object identity changed after tomography"
                        )
                    raise RegionHarvestError(
                        "distributed bulk harvest requires HTTP 206"
                    )
                content_range=response.headers.get("content-range","")
                if (
                    not content_range.lower().startswith("bytes ")
                    or "/" not in content_range
                ):
                    raise RegionHarvestError(
                        "bulk Range response omitted Content-Range"
                    )
                try:
                    range_part,total_part=content_range.split(" ",1)[1].split("/",1)
                    returned_start_text,returned_end_text=range_part.split("-",1)
                    returned_start=int(returned_start_text)
                    returned_end=int(returned_end_text)
                    total_size=int(total_part)
                except (ValueError,IndexError) as exc:
                    raise RegionHarvestError(
                        "bulk Range response has invalid Content-Range"
                    ) from exc
                if returned_start!=request_start:
                    raise RegionHarvestError("bulk Range response start mismatch")
                if returned_end<end_exclusive-1 or returned_end>request_end:
                    raise RegionHarvestError("bulk Range response bounds mismatch")
                observed=remote_identity_from_headers(
                    response.headers,
                    content_length=total_size,
                )
                ensure_same_historical_index_object(expected,observed)

                async for chunk in response.aiter_raw(chunk_size=64*1024):
                    context.keeper.assert_owned()
                    if not chunk:
                        continue
                    bytes_read+=len(chunk)
                    buffer+=chunk

                    if not boundary_ready:
                        if not prefix_checked:
                            if not buffer:
                                continue
                            previous=buffer[:1]
                            buffer=buffer[1:]
                            prefix_checked=True
                            if previous==b"\n":
                                boundary_ready=True
                                cursor=start
                        if not boundary_ready:
                            core_remaining=end_exclusive-cursor
                            search=buffer[:max(0,core_remaining)]
                            boundary=search.find(b"\n")
                            if boundary<0:
                                if len(buffer)>=core_remaining:
                                    buffer=b""
                                    cursor=end_exclusive
                                    break
                                continue
                            buffer=buffer[boundary+1:]
                            cursor+=boundary+1
                            boundary_ready=True
                            if cursor>=end_exclusive:
                                break

                    while boundary_ready and cursor<end_exclusive:
                        boundary=buffer.find(b"\n")
                        if boundary<0:
                            if len(buffer)>boundary_max:
                                raise RegionHarvestError(
                                    "bulk CDX/CDXJ record exceeds boundary limit"
                                )
                            break
                        raw=buffer[:boundary+1]
                        buffer=buffer[boundary+1:]
                        line_start=cursor
                        cursor+=len(raw)
                        record=self._parse_record(
                            index_format=index_format,
                            source_key=source_key,
                            locator=locator,
                            raw=raw,
                            line_start=line_start,
                        )
                        if record is None:
                            continue
                        parsed_records+=1
                        group=reducer.feed(record)
                        if group is None:
                            continue
                        pending.append(serialize_witness_group(group))
                        # The reducer already consumed the first record of the
                        # next hostname. Committing its byte start as cursor is
                        # the crash-safe replay point for the next generation.
                        safe_cursor=line_start
                        if len(pending)>=group_batch_size:
                            yield ResultBatch(
                                task_id=lease.task_id,
                                generation=lease.generation,
                                sequence_no=sequence,
                                results=tuple(pending),
                                cursor_after=f"byte:{safe_cursor}",
                                final=False,
                            )
                            sequence+=1
                            pending=[]

                if cursor<end_exclusive and boundary_ready:
                    if returned_end==total_size-1:
                        if buffer:
                            line_start=cursor
                            record=self._parse_record(
                                index_format=index_format,
                                source_key=source_key,
                                locator=locator,
                                raw=buffer,
                                line_start=line_start,
                            )
                            cursor+=len(buffer)
                            if record is not None:
                                parsed_records+=1
                                group=reducer.feed(record)
                                if group is not None:
                                    pending.append(serialize_witness_group(group))
                    else:
                        raise RegionHarvestError(
                            "bulk shard ends inside a record larger than boundary limit"
                        )

        final_group=reducer.finish()
        if final_group is not None:
            pending.append(serialize_witness_group(final_group))
        elapsed=max(0.0,time.monotonic()-started)
        pending.append(
            {
                "kind":"BULK_SUMMARY",
                "records":parsed_records,
                "bytes_read":bytes_read,
                "requests":1,
                "elapsed_seconds":elapsed,
            }
        )
        yield ResultBatch(
            task_id=lease.task_id,
            generation=lease.generation,
            sequence_no=sequence,
            results=tuple(pending),
            cursor_after=None,
            final=True,
        )


class FabricRegionHarvestExecutor(RegionHarvestExecutor):
    """Use Fabric for HTTP transport while retaining central commit semantics."""

    def __init__(
        self,
        *,
        fabric_store,
        fabric_poll_seconds: float = 0.25,
        fabric_wait_seconds: float | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if fabric_poll_seconds<=0:
            raise ValueError("fabric_poll_seconds must be positive")
        if fabric_wait_seconds is not None and fabric_wait_seconds<=0:
            raise ValueError("fabric_wait_seconds must be positive")
        self.fabric_store=fabric_store
        self.fabric_poll_seconds=float(fabric_poll_seconds)
        self.fabric_wait_seconds=(
            max(30.0,self.policy.max_seconds+self.policy.claim_grace_seconds)
            if fabric_wait_seconds is None
            else float(fabric_wait_seconds)
        )
        self._active_region_key: str | None=None
        self._pending_batch_ids: dict[str,list[str]]={}

    def reconcile_consumed_harvest_batches(self, *, limit: int = 512) -> int:
        consumed=0
        for row in self.fabric_store.unconsumed_batches(limit=limit):
            task=self.fabric_store.task_row(str(row["task_id"]))
            if str(task["producer"])!=PRODUCER_NAME:
                continue
            payload=_json_object(task["payload_json"])
            region_key=str(payload.get("region_key",""))
            region=self.registry.get_region(region_key) if region_key else None
            if region is not None and region.state is RegionState.HARVESTED:
                consumed+=int(
                    self.fabric_store.mark_batch_consumed(str(row["batch_id"]))
                )
        return consumed

    def harvest(self, region_key: str, *, exposure_id: str | None = None):
        self.reconcile_consumed_harvest_batches()
        self._active_region_key=region_key
        self._pending_batch_ids[region_key]=[]
        try:
            report=super().harvest(region_key,exposure_id=exposure_id)
            if report is not None:
                for batch_id in self._pending_batch_ids.get(region_key,()):
                    try:
                        self.fabric_store.mark_batch_consumed(batch_id)
                    except Exception:
                        # Region/evidence state already committed. Leaving the
                        # batch unconsumed is safe; reconciliation handles it.
                        pass
            return report
        finally:
            self._active_region_key=None
            self._pending_batch_ids.pop(region_key,None)

    def _execute_http_region(
        self,
        *,
        index,
        lease: WorkLease,
        emit_record,
    ) -> LeaseResult:
        region_key=self._active_region_key
        if region_key is None:
            raise RegionHarvestError("Fabric harvest has no active region")
        start=_cursor_value(lease.cursor_start)
        end_exclusive=_cursor_value(lease.cursor_end)
        if start is None or end_exclusive is None:
            raise RegionHarvestError("Fabric harvest requires byte cursors")
        expected=self.registry.get_object_identity(index.index_key)
        if expected is None:
            raise RegionHarvestError(
                "distributed harvest requires tomography-bound object identity"
            )
        work=bulk_shard_work_definition(
            region_key=region_key,
            index_key=index.index_key,
            source_key=index.source_key,
            locator=index.locator,
            index_format=index.capabilities.format,
            byte_start=start,
            byte_end_exclusive=end_exclusive,
            expected_identity=expected,
            boundary_record_max_bytes=self.policy.boundary_record_max_bytes,
            timeout_seconds=self.policy.max_seconds,
        )
        task_id,_inserted=self.fabric_store.admit_work(work)
        seen:set[str]=set()
        summary: Mapping[str,object] | None=None
        deadline=time.monotonic()+self.fabric_wait_seconds

        while True:
            for row in self.fabric_store.unconsumed_batches_for_task(
                task_id,
                limit=256,
            ):
                batch_id=str(row["batch_id"])
                if batch_id in seen:
                    continue
                seen.add(batch_id)
                payload=_json_object(row["payload_json"])
                raw_results=payload.get("results")
                if not isinstance(raw_results,list):
                    self.fabric_store.mark_batch_consume_failed(
                        batch_id,
                        "Bulk shard results must be an array",
                    )
                    raise RegionHarvestError("invalid Fabric bulk result batch")
                for item in raw_results:
                    if not isinstance(item,Mapping):
                        raise RegionHarvestError(
                            "Fabric bulk result must be an object"
                        )
                    kind=str(item.get("kind",""))
                    if kind=="BULK_WITNESS_GROUP":
                        group=deserialize_witness_group(item)
                        for witness in group.witnesses:
                            emit_record(witness_to_source_record(witness))
                    elif kind=="BULK_SUMMARY":
                        if summary is not None:
                            raise RegionHarvestError(
                                "Fabric bulk task emitted duplicate summary"
                            )
                        summary=item
                    else:
                        raise RegionHarvestError(
                            f"unexpected Fabric bulk result kind: {kind}"
                        )
                self._pending_batch_ids[region_key].append(batch_id)

            task=self.fabric_store.task_row(task_id)
            state=str(task["state"])
            if state=="DEAD":
                raise RegionHarvestError(
                    f"Fabric bulk task died: {task['last_error']}"
                )
            if state=="COMPLETE" and summary is not None:
                return LeaseResult(
                    lease_id=lease.lease_id,
                    records=int(summary.get("records",0)),
                    requests=int(summary.get("requests",1)),
                    bytes_read=int(summary.get("bytes_read",0)),
                    elapsed_seconds=float(summary.get("elapsed_seconds",0.0)),
                    next_cursor=None,
                )
            if time.monotonic()>=deadline:
                raise RegionHarvestError(
                    "timed out waiting for Fabric bulk worker"
                )
            time.sleep(self.fabric_poll_seconds)
