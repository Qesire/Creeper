"""Production source side of the Creeper pipeline.

SourceProducer deliberately does no evidence-provider network I/O. It converts
finite Reservoir leases into direct EvidenceCapsules and durable EvidenceTasks,
then returns. Independent AsyncEvidenceWorkers drain those tasks concurrently.

Producer admission uses a crash-safe durable capacity reservation so multiple
producer processes cannot outrun the evidence backlog high-water mark.
"""

from __future__ import annotations

import queue
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING

from creeper.authority.baseline_index import BaselineIndex, YEAR_BITS
from creeper.evidence.planner import EvidencePlanner
from creeper.evidence.router import EvidenceRouter
from creeper.evidence.rdap_candidates import rdap_parent_candidate
from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.records.candidates import CandidateRecord
from creeper.records.models import HostObservation
from creeper.runtime.queues import BoundedQueues
from creeper.scheduler.admission import CapacityReservation, EvidenceBacklogAdmission
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import WorkLease
from creeper.scheduler.priority import LeaseCandidate
from creeper.sources.reservoirs import ReservoirState
from creeper.storage.candidate_store import CandidateStore
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore

if TYPE_CHECKING:
    from creeper.source_discovery.registry import SourceDiscoveryRegistry


@dataclass(frozen=True)
class SourceProducerReport:
    leases_succeeded: int = 0
    source_records: int = 0
    observations: int = 0
    evidence_tasks_enqueued: int = 0
    direct_capsules_committed: int = 0
    admission_blocked: bool = False
    max_source_record_queue_depth: int = 0
    max_observation_queue_depth: int = 0
    pipeline_batches: int = 0
    source_queue_block_milliseconds: int = 0
    observation_queue_block_milliseconds: int = 0
    baseline_lookup_milliseconds: int = 0
    planning_commit_milliseconds: int = 0
    planning_observations: int = 0
    effective_range_first_fraction: float = 0.0

    def as_dict(self) -> dict[str, object]:
        return self.__dict__.copy()


def _coalesce_planning_observations(
    observations: Iterable[HostObservation],
) -> list[HostObservation]:
    """Keep one deterministic witness for each planner-equivalent observation.

    Archive indexes often contain many captures of the same hostname in the
    same year. Baseline lookup was already hostname-deduplicated, but the
    EvidencePlanner still saw every capture. The planner only depends on the
    fields in this key; locator/time affect provenance, not the decision.
    Keeping the first observation therefore preserves one real witness while
    removing redundant planning work.

    Distinct source years, direct masks, hint masks, source scopes, or sources
    remain separate so no temporal/evidence semantics are merged.
    """

    selected: dict[
        tuple[str, str, object, int | None, int, int],
        HostObservation,
    ] = {}
    for observation in observations:
        key = (
            observation.hostname,
            observation.source_id,
            observation.scope,
            observation.source_year,
            int(observation.direct_year_mask),
            int(observation.year_hint_mask),
        )
        selected.setdefault(key, observation)
    return list(selected.values())


class SourceProducer:
    """Produce durable evidence work without sharing provider lifetimes."""

    def __init__(
        self,
        *,
        baseline: BaselineIndex,
        control_store: ControlStore,
        evidence_store: EvidenceStore,
        scheduler: GlobalScheduler,
        candidate_store: CandidateStore | None = None,
        source_registry: "SourceDiscoveryRegistry | None" = None,
        candidates: Iterable[LeaseCandidate],
        adapters: Mapping[str, object],
        backlog_capacities: Mapping[str, int],
        queue_capacities: Mapping[str, int],
        evidence_policy_version: str = "cdx-v1",
        range_first_fraction: float = 0.0,
        domain_fanout_min_children: int = 4,
        domain_fanout_batch_size: int = 16,
        rdap_fanout_min_children: int = 1,
        rdap_batch_size: int = 16,
        owner: str = "source-producer",
        baseline_batch_size: int = 50_000,
        pipeline_batch_size: int = 1_000,
        extract_workers: int = 2,
        reservation_grace_seconds: float = 30.0,
    ) -> None:
        if baseline_batch_size < 1:
            raise ValueError("baseline_batch_size must be positive")
        if pipeline_batch_size < 1:
            raise ValueError("pipeline_batch_size must be positive")
        if (
            not isinstance(extract_workers, int)
            or isinstance(extract_workers, bool)
            or extract_workers < 1
        ):
            raise ValueError("extract_workers must be a positive integer")
        if reservation_grace_seconds < 0:
            raise ValueError("reservation_grace_seconds must be non-negative")
        if not 0.0 <= float(range_first_fraction) <= 1.0:
            raise ValueError("range_first_fraction must be between 0 and 1")
        if domain_fanout_min_children < 2 or domain_fanout_batch_size < 1:
            raise ValueError("invalid domain fanout thresholds")
        if rdap_fanout_min_children < 1 or rdap_batch_size < 1:
            raise ValueError("invalid RDAP candidate thresholds")
        capacities = dict(backlog_capacities)
        if any(
            not provider or not isinstance(value, int) or value < 0
            for provider, value in capacities.items()
        ):
            raise ValueError("backlog capacities must be non-negative integers")
        # Direct-year sources do not require any external provider lane.
        # An empty mapping is therefore valid; discovery-only sources simply
        # remain admission-blocked until their provider capacity is configured.

        self.baseline = baseline
        self.control_store = control_store
        self.evidence_store = evidence_store
        self.candidate_store = candidate_store
        self.source_registry = source_registry
        self.scheduler = scheduler
        self.candidates = tuple(candidates)
        self.adapters = dict(adapters)
        self.backlog_capacities = capacities
        self.queue_capacities = dict(queue_capacities)
        self.evidence_policy_version = evidence_policy_version
        self.range_first_fraction = float(range_first_fraction)
        self.domain_fanout_min_children = int(domain_fanout_min_children)
        self.domain_fanout_batch_size = int(domain_fanout_batch_size)
        self.rdap_fanout_min_children = int(rdap_fanout_min_children)
        self.rdap_batch_size = int(rdap_batch_size)
        self.owner = owner
        self.baseline_batch_size = baseline_batch_size
        self.pipeline_batch_size = pipeline_batch_size
        self.extract_workers = extract_workers
        self.reservation_grace_seconds = reservation_grace_seconds
        self.evidence_planner = EvidencePlanner()
        self.admission = EvidenceBacklogAdmission(control_store)
        self.evidence_router = EvidenceRouter(
            control_store,
            self.admission,
            backlog_capacities=self.backlog_capacities,
        )

    def refresh_workset(
        self,
        *,
        candidates: Iterable[LeaseCandidate],
        adapters: Mapping[str, object],
    ) -> None:
        """Replace the lightweight production workset without reopening stores.

        Long-lived source services call this after discovery activation changes.
        BaselineIndex, SQLite connections, scheduler/admission state, and cached
        adapters remain alive across leases.
        """
        self.candidates = tuple(candidates)
        self.adapters = dict(adapters)


    def _adapter_for(self, candidate: LeaseCandidate) -> object:
        if candidate.reservoir is None:
            raise ValueError("a source candidate requires a reservoir")
        try:
            return self.adapters[candidate.reservoir.adapter_id]
        except KeyError as exc:
            raise KeyError(f"no adapter: {candidate.reservoir.adapter_id}") from exc

    def _grant_fresh_lease(
        self,
    ) -> tuple[LeaseCandidate, WorkLease, CapacityReservation | None] | None:
        self.control_store.recover_expired_leases()
        for candidate in self.scheduler.rank(self.candidates):
            template = candidate.lease
            if template is None:
                raise ValueError("source candidate requires a lease template")
            if candidate.reservoir is None:
                raise ValueError("source candidate requires a reservoir")
            provider = candidate.evidence_provider
            direct_source = candidate.reservoir.evidence_mode == "direct_year"

            postprocess_ttl = max(
                60.0,
                min(300.0, float(template.max_seconds)),
                self.reservation_grace_seconds,
            )
            ownership_ttl = float(template.max_seconds) + postprocess_ttl
            reservation: CapacityReservation | None = None
            if not direct_source:
                capacity = self.backlog_capacities.get(provider, 0)
                if capacity <= 0:
                    continue
                reservation_amount = (
                    candidate.expected_evidence_tasks
                    if candidate.reservation_evidence_tasks is None
                    else candidate.reservation_evidence_tasks
                )
                reservation = self.admission.try_reserve(
                    provider=provider,
                    amount=reservation_amount,
                    capacity=capacity,
                    ttl_seconds=ownership_ttl,
                )
                if reservation is None:
                    continue
            try:
                lease = self.control_store.grant_fresh_lease(
                    candidate.reservoir_id,
                    owner=self.owner,
                    max_records=template.max_records,
                    max_requests=template.max_requests,
                    max_bytes=template.max_bytes,
                    max_seconds=template.max_seconds,
                    resource_class=template.resource_class,
                    expected_evidence_tasks=(
                        0 if direct_source else candidate.expected_evidence_tasks
                    ),
                    expected_novel_eed=candidate.expected_novel_eed,
                    now=float(self.control_store.clock()),
                    lease_ttl_seconds=ownership_ttl,
                )
                if lease is None:
                    self.admission.release(reservation)
                    continue
                if reservation is not None:
                    self.admission.bind_lease(reservation, lease.lease_id)
                return candidate, lease, reservation
            except BaseException:
                self.admission.release(reservation)
                raise
        return None

    def _has_durable_ready_source(self) -> bool:
        """Read persisted state instead of stale candidate snapshots."""
        for candidate in self.candidates:
            reservoir = self.control_store.get_reservoir(candidate.reservoir_id)
            if reservoir is not None and reservoir.state is ReservoirState.READY:
                return True
        return False

    def run_once(self) -> SourceProducerReport:
        effective_range_first_fraction = (
            self.control_store.recommended_range_first_fraction(
                self.range_first_fraction
            )
        )
        queues = BoundedQueues(**self.queue_capacities)
        # Spillover routing is provider-specific and opportunistic.  Failure to
        # make room in one lane (especially Wayback) cannot prevent the source
        # scheduler from considering direct-year reservoirs below.
        self.evidence_router.flush_pending(
            ttl_seconds=max(60.0, self.reservation_grace_seconds),
            max_needs=256,
        )
        granted = self._grant_fresh_lease()
        if granted is None:
            # A READY durable source with no grant means admission/backpressure
            # prevented execution. If no READY source remains, this is ordinary
            # idle/exhaustion instead of a reason to wait for the evidence queue.
            return SourceProducerReport(
                admission_blocked=self._has_durable_ready_source(),
                effective_range_first_fraction=effective_range_first_fraction,
            )

        candidate, lease, reservation = granted
        provider = candidate.evidence_provider
        origin_source_key = candidate.source_key or candidate.reservoir_id
        running = lease.start()
        self.control_store.save_lease(running)
        run_authority: tuple[str, str] | None = None
        if self.source_registry is not None and candidate.source_key is not None:
            run_authority = self.source_registry.current_scout_authority
            if run_authority is not None:
                self.source_registry.begin_source_run(
                    candidate.source_key,
                    reservoir_id=candidate.reservoir_id,
                    lease_id=running.lease_id,
                    baseline_signature=run_authority[0],
                    model_signature=run_authority[1],
                )
        source_records = observations = planning_observations = 0
        enqueued = direct_committed = 0
        max_source = max_observations = 0
        pipeline_batches = 0
        source_queue_block_ms = observation_queue_block_ms = 0
        baseline_lookup_ms = planning_commit_ms = 0
        scheduled_keys: dict[EvidenceQueryKey, None] = {}
        result = None
        source_finalized = False
        postprocess_ttl = max(
            60.0,
            min(300.0, float(running.max_seconds)),
            self.reservation_grace_seconds,
        )
        next_renew_at = 0.0

        def keep_ownership_live(*, force: bool = False) -> None:
            nonlocal reservation, next_renew_at
            now = float(self.control_store.clock())
            if not force and now < next_renew_at:
                return
            if reservation is not None:
                reservation = self.admission.renew(
                    reservation,
                    ttl_seconds=postprocess_ttl,
                )
            self.control_store.renew_lease(
                running,
                ttl_seconds=postprocess_ttl,
                now=now,
            )
            next_renew_at = now + max(1.0, postprocess_ttl / 3.0)

        try:
            adapter = self._adapter_for(candidate)
            execute = getattr(adapter, "execute")
            execute_stream = getattr(adapter, "execute_stream", None)
            extract_hosts = getattr(adapter, "extract_hosts")
            records = None
            if not callable(execute_stream):
                records, result = execute(running)
            # Renew before launching pipeline threads. Streaming adapters may
            # remain active while downstream queues are backpressured.
            keep_ownership_live(force=True)

            pending: list[HostObservation] = []

            def enqueue_external_keys(keys: Iterable[EvidenceQueryKey]) -> None:
                nonlocal enqueued
                routed = self.evidence_router.route_external(
                    keys,
                    preferred_provider=provider,
                )
                fresh = [key for key in routed if key not in scheduled_keys]
                if not fresh:
                    return
                for key in fresh:
                    scheduled_keys[key] = None
                if reservation is None:
                    routed_result = self.evidence_router.enqueue_or_stage(
                        fresh,
                        source_key=origin_source_key,
                        reservoir_id=candidate.reservoir_id,
                        lease_id=running.lease_id,
                        ttl_seconds=postprocess_ttl,
                        preferred_provider=provider,
                    )
                    enqueued += routed_result.enqueued
                    return
                enqueued += self.admission.enqueue_reserved(
                    reservation,
                    fresh,
                    source_key=origin_source_key,
                    reservoir_id=candidate.reservoir_id,
                    lease_id=running.lease_id,
                )

            def enqueue_auxiliary_keys(
                aux_provider: str,
                keys: Iterable[EvidenceQueryKey],
            ) -> int:
                """Route an auxiliary lane without borrowing Wayback capacity."""
                nonlocal enqueued
                rows = list(dict.fromkeys(keys))
                if not rows:
                    return 0
                routed_result = self.evidence_router.enqueue_or_stage(
                    rows,
                    source_key=origin_source_key,
                    reservoir_id=candidate.reservoir_id,
                    lease_id=running.lease_id,
                    ttl_seconds=postprocess_ttl,
                    preferred_provider=aux_provider,
                )
                enqueued += routed_result.enqueued
                return routed_result.enqueued

            def resolve_pending() -> None:
                nonlocal direct_committed, pipeline_batches, planning_observations
                nonlocal baseline_lookup_ms, planning_commit_ms
                if not pending:
                    return
                planning_pending = _coalesce_planning_observations(pending)
                planning_observations += len(planning_pending)
                keep_ownership_live()
                pipeline_batches += 1
                # High-frequency archive indexes may repeat the same host
                # tens of thousands of times inside one lease. Resolve each
                # unique hostname once, then update the in-memory masks as work
                # is planned so later observations in this batch are free.
                hostnames = list(
                    dict.fromkeys(observation.hostname for observation in planning_pending)
                )
                self.control_store.record_domain_fanout_observations(
                    hostnames,
                    source_key=origin_source_key,
                )
                resolved: dict[str, tuple[int, bool]] = {}
                lookup_started = time.monotonic()
                local_masks = self.evidence_store.resolve_year_masks(hostnames)
                provider_coverage_masks = (
                    self.control_store.resolve_provider_coverage_masks(
                        hostnames,
                        provider=provider,
                        policy_version=self.evidence_policy_version,
                    )
                )
                for batch in self.baseline.iter_resolve_batches(
                    hostnames,
                    input_batch_size=self.baseline_batch_size,
                ):
                    resolved.update(batch)
                    keep_ownership_live()
                baseline_lookup_ms += max(
                    0,
                    int(round((time.monotonic() - lookup_started) * 1000.0)),
                )
                if self.candidate_store is not None:
                    self.candidate_store.record_observations(
                        CandidateRecord(
                            hostname=item.hostname,
                            source_id=item.source_id,
                            scope=item.scope,
                            source_locator=item.locator,
                            source_year=item.source_year,
                        )
                        for item in planning_pending
                    )
                    self.candidate_store.mark_baseline_overlap_many(
                        hostname
                        for hostname, (annual_mask, _candidate) in resolved.items()
                        if annual_mask
                    )
                    self.candidate_store.mark_annual_evidence_obtained_many(
                        hostname
                        for hostname, mask in local_masks.items()
                        if mask
                    )
                planning_started = time.monotonic()
                batch_direct_capsules = []
                allow_direct = (
                    candidate.reservoir is not None
                    and candidate.reservoir.evidence_mode == "direct_year"
                )
                for item in planning_pending:
                    annual_mask, _candidate = resolved.get(item.hostname, (0, False))
                    plan = self.evidence_planner.plan(
                        item,
                        official_mask=annual_mask,
                        local_mask=local_masks.get(item.hostname, 0),
                        provider=provider,
                        policy_version=self.evidence_policy_version,
                        allow_direct=allow_direct,
                        external_covered_mask=provider_coverage_masks.get(
                            item.hostname, 0
                        ),
                        range_first_fraction=effective_range_first_fraction,
                    )
                    if plan.direct_capsules:
                        for capsule in plan.direct_capsules:
                            bit = YEAR_BITS.get(capsule.year, 0)
                            if local_masks.get(item.hostname, 0) & bit:
                                continue
                            batch_direct_capsules.append(capsule)
                            local_masks[item.hostname] = (
                                local_masks.get(item.hostname, 0) | bit
                            )
                    if plan.external_keys:
                        enqueue_external_keys(plan.external_keys)
                        scheduled_mask = provider_coverage_masks.get(
                            item.hostname, 0
                        )
                        for key in plan.external_keys:
                            for year in range(
                                key.temporal_scope.year_from,
                                key.temporal_scope.year_to + 1,
                            ):
                                scheduled_mask |= YEAR_BITS.get(year, 0)
                        provider_coverage_masks[item.hostname] = scheduled_mask

                if not allow_direct:
                    domain_parents = self.control_store.ready_domain_fanout_candidates(
                        min_children=self.domain_fanout_min_children,
                        limit=min(self.domain_fanout_batch_size, len(planning_pending)),
                    )
                    if domain_parents:
                        domain_keys = tuple(
                            EvidenceQueryKey(
                                hostname=parent,
                                temporal_scope=TemporalScope(1996, 2001),
                                provider=provider,
                                policy_version="cdx-domain-v1",
                            )
                            for parent in domain_parents
                        )
                        enqueue_external_keys(domain_keys)
                        self.control_store.mark_domain_fanout_enqueued(
                            domain_parents
                        )

                    rdap_hosts = list(dict.fromkeys(
                        rdap_candidate
                        for hostname in hostnames
                        if (
                            rdap_candidate := rdap_parent_candidate(hostname)
                        ) is not None
                    ))[: self.rdap_batch_size]
                    if rdap_hosts:
                        rdap_keys = tuple(
                            EvidenceQueryKey(
                                hostname=hostname,
                                temporal_scope=TemporalScope(1996, 2001),
                                provider="rdap",
                                policy_version="rdap-registration-v1",
                            )
                            for hostname in rdap_hosts
                        )
                        # EvidenceTask identity already provides durable global
                        # deduplication, so RDAP discovery needs no per-host
                        # side table or enqueue flag.
                        enqueue_auxiliary_keys("rdap", rdap_keys)

                if batch_direct_capsules:
                    # Commit evidence proof first. Incremental readiness can
                    # recover direct source identity from persisted capsules if
                    # this process dies before ControlStore attribution commits.
                    # This avoids orphan first-touch credit with no evidence.
                    direct_committed += self.evidence_store.put_many(
                        batch_direct_capsules
                    )
                    self.control_store.attribute_direct_host_years(
                        (
                            (capsule.hostname, capsule.year, capsule.provider)
                            for capsule in batch_direct_capsules
                        ),
                        source_key=origin_source_key,
                        reservoir_id=candidate.reservoir_id,
                        lease_id=running.lease_id,
                    )
                    if self.candidate_store is not None:
                        self.candidate_store.mark_annual_evidence_obtained_many(
                            capsule.hostname
                            for capsule in batch_direct_capsules
                        )
                planning_commit_ms += max(
                    0,
                    int(round((time.monotonic() - planning_started) * 1000.0)),
                )
                pending.clear()

            # True bounded producer-consumer pipeline:
            #
            # adapter records -> source_record_queue -> extraction workers
            # -> observation_queue -> batched lookup/planning/authority writer.
            #
            # SQLite authority stays on this thread; source iteration and host
            # extraction continue concurrently until bounded queues apply
            # backpressure.
            source_sentinel = object()
            observation_sentinel = object()
            pipeline_stop = Event()
            pipeline_lock = Lock()
            pipeline_errors: queue.Queue[BaseException] = queue.Queue()

            def put_with_backpressure(target, value, *, kind: str) -> None:
                nonlocal source_queue_block_ms, observation_queue_block_ms
                started_wait = time.monotonic()
                while not pipeline_stop.is_set():
                    try:
                        target.put(value, timeout=0.1)
                        waited = max(
                            0,
                            int(round((time.monotonic() - started_wait) * 1000.0)),
                        )
                        with pipeline_lock:
                            if kind == "source":
                                source_queue_block_ms += waited
                            else:
                                observation_queue_block_ms += waited
                        return
                    except queue.Full:
                        continue
                raise RuntimeError("source pipeline cancelled")

            def acquire_records() -> None:
                nonlocal source_records, max_source, result

                def emit_record(record) -> None:
                    nonlocal source_records, max_source
                    with pipeline_lock:
                        sequence = source_records
                        source_records += 1
                    put_with_backpressure(
                        queues.source_record_queue,
                        (sequence, record),
                        kind="source",
                    )
                    with pipeline_lock:
                        max_source = max(
                            max_source,
                            queues.source_record_queue.qsize(),
                        )

                try:
                    if callable(execute_stream):
                        result = execute_stream(running, emit_record)
                    else:
                        assert records is not None
                        for record in records:
                            emit_record(record)
                except BaseException as exc:
                    pipeline_errors.put(exc)
                    pipeline_stop.set()
                finally:
                    for _ in range(self.extract_workers):
                        try:
                            put_with_backpressure(
                                queues.source_record_queue,
                                source_sentinel,
                                kind="source",
                            )
                        except BaseException:
                            break

            def extract_observations() -> None:
                nonlocal observations, max_observations
                try:
                    while not pipeline_stop.is_set():
                        try:
                            current = queues.source_record_queue.get(timeout=0.1)
                        except queue.Empty:
                            continue
                        if current is source_sentinel:
                            break
                        sequence, record = current
                        extracted = tuple(extract_hosts(record))
                        put_with_backpressure(
                            queues.observation_queue,
                            (sequence, extracted),
                            kind="observation",
                        )
                        with pipeline_lock:
                            observations += len(extracted)
                            max_observations = max(
                                max_observations,
                                queues.observation_queue.qsize(),
                            )
                except BaseException as exc:
                    pipeline_errors.put(exc)
                    pipeline_stop.set()
                finally:
                    try:
                        put_with_backpressure(
                            queues.observation_queue,
                            observation_sentinel,
                            kind="observation",
                        )
                    except BaseException:
                        pass

            acquisition = Thread(
                target=acquire_records,
                name=f"{self.owner}-source-acquire",
                daemon=True,
            )
            extractors = [
                Thread(
                    target=extract_observations,
                    name=f"{self.owner}-extract-{index}",
                    daemon=True,
                )
                for index in range(self.extract_workers)
            ]
            acquisition.start()
            for worker in extractors:
                worker.start()

            finished_extractors = 0
            next_sequence = 0
            reorder_buffer: dict[int, tuple[HostObservation, ...]] = {}
            try:
                while finished_extractors < self.extract_workers:
                    if not pipeline_errors.empty():
                        raise pipeline_errors.get()
                    try:
                        item = queues.observation_queue.get(timeout=0.1)
                    except queue.Empty:
                        keep_ownership_live()
                        continue
                    if item is observation_sentinel:
                        finished_extractors += 1
                        continue
                    sequence, extracted = item
                    reorder_buffer[int(sequence)] = tuple(extracted)
                    while next_sequence in reorder_buffer:
                        pending.extend(reorder_buffer.pop(next_sequence))
                        next_sequence += 1
                        if len(pending) >= self.pipeline_batch_size:
                            resolve_pending()
                # Every emitted source record must reach the deterministic
                # reorder boundary exactly once before lease finalization.
                if reorder_buffer:
                    for sequence in sorted(reorder_buffer):
                        if sequence != next_sequence:
                            raise RuntimeError(
                                "source extraction pipeline lost record ordering"
                            )
                        pending.extend(reorder_buffer[sequence])
                        next_sequence += 1
                if next_sequence != source_records:
                    raise RuntimeError(
                        "source extraction pipeline lost source records"
                    )
                resolve_pending()
                if not pipeline_errors.empty():
                    raise pipeline_errors.get()
            finally:
                pipeline_stop.set()
                acquisition.join(timeout=5.0)
                for worker in extractors:
                    worker.join(timeout=5.0)
            assert result is not None
            if (
                result.records == 0
                and result.next_cursor == running.cursor_start
                and result.next_cursor is not None
            ):
                raise RuntimeError("lease made no cursor progress")
            if (
                self.source_registry is not None
                and candidate.source_key is not None
                and run_authority is not None
            ):
                self.source_registry.record_source_run_read(
                    candidate.source_key,
                    reservoir_id=candidate.reservoir_id,
                    lease_id=running.lease_id,
                    baseline_signature=run_authority[0],
                    model_signature=run_authority[1],
                    source_records=source_records,
                    bytes_read=result.bytes_read,
                    source_requests=result.requests,
                    read_complete=True,
                )
            self.control_store.finalize_lease(
                running,
                next_cursor=result.next_cursor,
                exhausted=result.next_cursor is None,
            )
            source_finalized = True
            self.admission.release(reservation)

            return SourceProducerReport(
                leases_succeeded=1,
                source_records=source_records,
                observations=observations,
                planning_observations=planning_observations,
                evidence_tasks_enqueued=enqueued,
                direct_capsules_committed=direct_committed,
                max_source_record_queue_depth=max_source,
                max_observation_queue_depth=max_observations,
                pipeline_batches=pipeline_batches,
                source_queue_block_milliseconds=source_queue_block_ms,
                observation_queue_block_milliseconds=observation_queue_block_ms,
                baseline_lookup_milliseconds=baseline_lookup_ms,
                planning_commit_milliseconds=planning_commit_ms,
                effective_range_first_fraction=effective_range_first_fraction,
            )
        except BaseException:
            if not source_finalized:
                self.control_store.abort_lease(running)
            self.admission.release(reservation)
            raise

    def run_forever(
        self,
        *,
        stop_event,
        idle_backoff_seconds: float = 1.0,
        max_idle_backoff_seconds: float = 60.0,
        sleep_fn=time.sleep,
    ) -> SourceProducerReport:
        """Continuously consume durable source leases until a stop is requested."""
        if idle_backoff_seconds <= 0:
            raise ValueError("idle_backoff_seconds must be positive")
        if max_idle_backoff_seconds < idle_backoff_seconds:
            raise ValueError(
                "max_idle_backoff_seconds must not be below idle_backoff_seconds"
            )

        total = SourceProducerReport()
        idle = float(idle_backoff_seconds)
        while not stop_event.is_set():
            report = self.run_once()
            total = SourceProducerReport(
                leases_succeeded=total.leases_succeeded + report.leases_succeeded,
                source_records=total.source_records + report.source_records,
                observations=total.observations + report.observations,
                planning_observations=(
                    total.planning_observations + report.planning_observations
                ),
                evidence_tasks_enqueued=(
                    total.evidence_tasks_enqueued + report.evidence_tasks_enqueued
                ),
                direct_capsules_committed=(
                    total.direct_capsules_committed + report.direct_capsules_committed
                ),
                admission_blocked=total.admission_blocked or report.admission_blocked,
                max_source_record_queue_depth=max(
                    total.max_source_record_queue_depth,
                    report.max_source_record_queue_depth,
                ),
                max_observation_queue_depth=max(
                    total.max_observation_queue_depth,
                    report.max_observation_queue_depth,
                ),
                pipeline_batches=total.pipeline_batches + report.pipeline_batches,
                source_queue_block_milliseconds=(
                    total.source_queue_block_milliseconds
                    + report.source_queue_block_milliseconds
                ),
                observation_queue_block_milliseconds=(
                    total.observation_queue_block_milliseconds
                    + report.observation_queue_block_milliseconds
                ),
                baseline_lookup_milliseconds=(
                    total.baseline_lookup_milliseconds
                    + report.baseline_lookup_milliseconds
                ),
                planning_commit_milliseconds=(
                    total.planning_commit_milliseconds
                    + report.planning_commit_milliseconds
                ),
                effective_range_first_fraction=(
                    report.effective_range_first_fraction
                ),
            )
            if report.leases_succeeded:
                idle = float(idle_backoff_seconds)
                continue
            if stop_event.is_set():
                break
            sleep_fn(idle)
            idle = min(float(max_idle_backoff_seconds), idle * 2.0)
        return total
