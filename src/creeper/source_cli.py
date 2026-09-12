"""Dedicated source-producer process entry point.

Production source acquisition is intentionally a different process from
EvidenceWorker. The two communicate only through ControlStore's durable
EvidenceTask backlog.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
from threading import Event
import time
import tomllib

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.planner import EvidencePlanner
from creeper.runtime.source_producer import SourceProducer, SourceProducerReport
from creeper.scheduler.admission import EvidenceBacklogAdmission
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.local.static_dataset import StaticDatasetAdapter
from creeper.sources.production import ProductionAdapterFactory
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.source_discovery.activation import SourceActivationCompiler
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore
from creeper.storage.telemetry_store import RuntimeTelemetryStore


def _path(value: object, *, config_path: Path, name: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty path")
    path = Path(value)
    return path if path.is_absolute() else config_path.parent / path


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return float(value)


def _fraction(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number between 0 and 1")
    result = float(value)
    if not 0.0 <= result <= 1.0:
        raise ValueError(f"{name} must be between 0 and 1")
    return result



class StaticSourceRuntime:
    """Persistent runtime for one configured static discovery reservoir.

    Long-running WebBase production frequently operates with only a handful of
    free evidence slots. Reusing BaselineIndex, SQLite handles, and the dataset
    adapter avoids rebuilding the local runtime every time Wayback frees one
    slot.
    """

    def __init__(
        self,
        config_path: Path,
        *,
        config: dict[str, object],
        limits: dict[str, object],
        owner: str,
    ) -> None:
        self.config_path = Path(config_path)
        self.owner = owner
        self.backlog_capacity = _positive_int(
            limits.get("evidence_backlog_capacity"),
            "evidence_backlog_capacity",
        )
        self.queue_capacities = {
            "source_records": _positive_int(
                limits.get("queue_source_records"), "queue_source_records"
            ),
            "observations": _positive_int(
                limits.get("queue_observations"), "queue_observations"
            ),
            "evidence_tasks": _positive_int(
                limits.get("queue_evidence_tasks"), "queue_evidence_tasks"
            ),
            "commits": _positive_int(
                limits.get("queue_commits"), "queue_commits"
            ),
        }
        self.max_records = _positive_int(
            limits.get("lease_max_records"), "lease_max_records"
        )
        self.max_requests = _positive_int(
            limits.get("lease_max_requests"), "lease_max_requests"
        )
        self.max_bytes = _positive_int(
            limits.get("lease_max_bytes"), "lease_max_bytes"
        )
        self.max_seconds = _positive_float(
            limits.get("lease_max_seconds"), "lease_max_seconds"
        )
        self.range_first_fraction = _fraction(
            config.get("range_first_fraction", 0.10),
            "range_first_fraction",
        )

        baseline_path = _path(
            config.get("baseline_index"),
            config_path=self.config_path,
            name="baseline_index",
        )
        dataset_path = _path(
            config.get("dataset"),
            config_path=self.config_path,
            name="dataset",
        )
        runtime_root = _path(
            config.get("runtime_data_root"),
            config_path=self.config_path,
            name="runtime_data_root",
        )
        self.source_id = config.get("source_id", "local_dataset")
        self.domain_id = config.get("domain_id", "local_static_dataset")
        self.reservoir_id = config.get("reservoir_id", self.source_id)
        self.source_year = config.get("source_year")
        for name, value in (
            ("source_id", self.source_id),
            ("domain_id", self.domain_id),
            ("reservoir_id", self.reservoir_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if self.source_id != self.reservoir_id:
            raise ValueError("local static source_id and reservoir_id must match")
        if (
            isinstance(self.source_year, bool)
            or not isinstance(self.source_year, int)
            or not 1996 <= self.source_year <= 2001
        ):
            raise ValueError("source_year must be between 1996 and 2001")

        self.baseline = BaselineIndex(baseline_path)
        self.control = ControlStore(runtime_root / "control.sqlite3")
        self.evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
        self.adapter = StaticDatasetAdapter(
            dataset_path,
            source_id=self.source_id,
            source_year=self.source_year,
        )
        domain = SourceDomain(
            domain_id=self.domain_id,
            family="LOCAL_STATIC_DATASET",
            discovery_mechanism="configured local file",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id=self.reservoir_id,
            domain_id=self.domain_id,
            adapter_id=self.adapter.adapter_id,
            root_locator=str(dataset_path),
            enumeration_kind="line_file",
            capacity_lower=0,
            capacity_upper=None,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        if self.control.get_domain(self.domain_id) is None:
            self.control.save_domain(domain)
        if self.control.get_reservoir(self.reservoir_id) is None:
            self.control.save_reservoir(reservoir)

        self.producer = SourceProducer(
            baseline=self.baseline,
            control_store=self.control,
            evidence_store=self.evidence,
            scheduler=GlobalScheduler(
                CreditLedger({"wayback": self.backlog_capacity})
            ),
            candidates=(),
            adapters={self.adapter.adapter_id: self.adapter},
            backlog_capacities={"wayback": self.backlog_capacity},
            queue_capacities=self.queue_capacities,
            range_first_fraction=self.range_first_fraction,
            owner=self.owner,
        )

    def close(self) -> None:
        close = getattr(self.adapter, "close", None)
        if callable(close):
            close()
        self.evidence.close()
        self.control.close()
        self.baseline.close()

    def __enter__(self) -> "StaticSourceRuntime":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def run_once(self) -> dict[str, object]:
        reservoir = self.control.get_reservoir(self.reservoir_id)
        if reservoir is None:
            raise RuntimeError(
                f"static reservoir disappeared: {self.reservoir_id}"
            )
        if reservoir.state is ReservoirState.EXHAUSTED:
            return SourceProducerReport().as_dict()

        headroom = self.producer.admission.available_capacity(
            provider="wayback",
            capacity=self.backlog_capacity,
        )
        capacity_per_record = (
            EvidencePlanner.MAX_BACKLOG_CAPACITY_PER_OBSERVATION
        )
        lease_records = min(
            self.max_records,
            headroom // capacity_per_record,
        )
        if lease_records < 1:
            return SourceProducerReport(
                admission_blocked=reservoir.state is ReservoirState.READY
            ).as_dict()
        expected_tasks = lease_records
        reservation_tasks = lease_records * capacity_per_record

        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            cursor_start=reservoir.cursor,
            max_records=lease_records,
            max_requests=self.max_requests,
            max_bytes=self.max_bytes,
            max_seconds=self.max_seconds,
            expected_evidence_tasks=expected_tasks,
            expected_novel_eed=float(lease_records),
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=float(lease_records),
            costs=ResourceCost(
                general_network=0,
                # Wayback request capacity is the dominant scarce resource.
                # One discovery-only evidence task is therefore one unit of
                # evidence-network cost, rather than assigning every lease the
                # same constant cost regardless of how much backlog it creates.
                evidence_network=float(expected_tasks),
                cpu=1,
                ssd=1,
            ),
            reservoir=reservoir,
            lease=template,
            evidence_provider="wayback",
            expected_evidence_tasks=expected_tasks,
            reservation_evidence_tasks=reservation_tasks,
        )
        self.producer.refresh_workset(
            candidates=(candidate,),
            adapters={self.adapter.adapter_id: self.adapter},
        )
        return self.producer.run_once().as_dict()


class ActivatedSourceRuntime:
    """Persistent production runtime for discovery-activated sources.

    The expensive BaselineIndex and SQLite handles live for the process
    lifetime. Active-source metadata is refreshed before each lease so newly
    promoted reservoirs enter service without restarting the producer.
    Production adapters are cached by durable adapter id.
    """

    def __init__(
        self,
        config_path: Path,
        *,
        config: dict[str, object],
        limits: dict[str, object],
        owner: str,
    ) -> None:
        self.config_path = Path(config_path)
        self.owner = owner
        self.backlog_capacity = _positive_int(
            limits.get("evidence_backlog_capacity"),
            "evidence_backlog_capacity",
        )
        self.queue_capacities = {
            "source_records": _positive_int(
                limits.get("queue_source_records"), "queue_source_records"
            ),
            "observations": _positive_int(
                limits.get("queue_observations"), "queue_observations"
            ),
            "evidence_tasks": _positive_int(
                limits.get("queue_evidence_tasks"), "queue_evidence_tasks"
            ),
            "commits": _positive_int(
                limits.get("queue_commits"), "queue_commits"
            ),
        }
        self.max_records = _positive_int(
            limits.get("lease_max_records"), "lease_max_records"
        )
        self.max_requests = _positive_int(
            limits.get("lease_max_requests"), "lease_max_requests"
        )
        self.max_bytes = _positive_int(
            limits.get("lease_max_bytes"), "lease_max_bytes"
        )
        self.max_seconds = _positive_float(
            limits.get("lease_max_seconds"), "lease_max_seconds"
        )
        self.range_first_fraction = _fraction(
            config.get("range_first_fraction", 0.10),
            "range_first_fraction",
        )
        baseline_path = _path(
            config.get("baseline_index"),
            config_path=self.config_path,
            name="baseline_index",
        )
        runtime_root = _path(
            config.get("runtime_data_root"),
            config_path=self.config_path,
            name="runtime_data_root",
        )
        self.baseline = BaselineIndex(baseline_path)
        self.control = ControlStore(runtime_root / "control.sqlite3")
        self.evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.compiler = SourceActivationCompiler(
            self.control,
            registry=self.registry,
        )
        self.adapter_cache: dict[str, object] = {}
        self.producer = SourceProducer(
            baseline=self.baseline,
            control_store=self.control,
            evidence_store=self.evidence,
            scheduler=GlobalScheduler(
                CreditLedger({"wayback": self.backlog_capacity})
            ),
            candidates=(),
            adapters={},
            backlog_capacities={"wayback": self.backlog_capacity},
            queue_capacities=self.queue_capacities,
            range_first_fraction=self.range_first_fraction,
            owner=self.owner,
        )

    def close(self) -> None:
        for adapter in self.adapter_cache.values():
            close = getattr(adapter, "close", None)
            if callable(close):
                close()
        self.adapter_cache.clear()
        self.evidence.close()
        self.control.close()
        self.baseline.close()

    def __enter__(self) -> "ActivatedSourceRuntime":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _expected_lease_eed(self, source_key: str) -> float:
        measurement = self.registry.get_scout_measurement(source_key)
        if measurement is None or measurement.sampled_records <= 0:
            return 0.0
        return (
            measurement.novel_eed_for_ranking
            * float(self.max_records)
            / float(measurement.sampled_records)
        )

    def refresh_workset(self) -> int:
        candidates: list[LeaseCandidate] = []
        adapters: dict[str, object] = {}
        wayback_headroom = self.producer.admission.available_capacity(
            provider="wayback",
            capacity=self.backlog_capacity,
        )
        for spec in self.compiler.compile_active():
            reservoir = self.control.get_reservoir(spec.reservoir_id)
            if reservoir is None:
                raise ValueError(
                    f"activated reservoir disappeared: {spec.reservoir_id}"
                )
            if reservoir.state is ReservoirState.EXHAUSTED:
                continue
            adapter = self.adapter_cache.get(reservoir.adapter_id)
            if adapter is None:
                adapter = ProductionAdapterFactory.open(
                    reservoir,
                    temporal_scope=spec.temporal_scope,
                )
                self.adapter_cache[reservoir.adapter_id] = adapter
            adapters[reservoir.adapter_id] = adapter
            if reservoir.evidence_mode == "direct_year":
                lease_records = self.max_records
                expected_tasks = 0
                reservation_tasks = 0
            else:
                # Supported production adapters emit at most one HostObservation
                # per source record, but one observation can expand across six
                # competition-year backlog slots after bounded-range fanout,
                # plus one bounded domain-amplification task.
                # Size the lease from the hard capacity bound, not the expected
                # request cost, so admission remains fail-closed.
                capacity_per_record = (
                    EvidencePlanner.MAX_BACKLOG_CAPACITY_PER_OBSERVATION
                )
                lease_records = min(
                    self.max_records,
                    wayback_headroom // capacity_per_record,
                )
                expected_tasks = lease_records
                reservation_tasks = lease_records * capacity_per_record
                if lease_records < 1:
                    continue
            expected_eed = self._expected_lease_eed(spec.source_key)
            if self.max_records > 0:
                expected_eed *= lease_records / self.max_records
            template = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                cursor_start=reservoir.cursor,
                max_records=lease_records,
                max_requests=self.max_requests,
                max_bytes=self.max_bytes,
                max_seconds=self.max_seconds,
                expected_evidence_tasks=expected_tasks,
                expected_novel_eed=expected_eed,
            )
            candidates.append(
                LeaseCandidate(
                    reservoir_id=reservoir.reservoir_id,
                    expected_novel_eed=expected_eed,
                    costs=ResourceCost(
                        general_network=1,
                        # Score discovery sources against the actual scarce
                        # downstream work they create. Because expected_eed and
                        # expected_tasks scale with lease size, this makes the
                        # scheduler prefer expected Novel EED per Wayback task.
                        # Direct-year sources bypass Wayback and pay zero here.
                        evidence_network=(
                            0
                            if reservoir.evidence_mode == "direct_year"
                            else float(expected_tasks)
                        ),
                        cpu=1,
                        ssd=1,
                    ),
                    reservoir=reservoir,
                    lease=template,
                    evidence_mode=reservoir.evidence_mode,
                    expected_evidence_tasks=expected_tasks,
                    reservation_evidence_tasks=reservation_tasks,
                    source_key=spec.source_key,
                )
            )
        # Do not retain adapter objects for exhausted/deactivated sources
        # across a multi-hour autonomous run. Current adapters hold no durable
        # authority; cursor state lives in ControlStore.
        active_adapter_ids = set(adapters)
        for adapter_id, adapter in tuple(self.adapter_cache.items()):
            if adapter_id in active_adapter_ids:
                continue
            close = getattr(adapter, "close", None)
            if callable(close):
                close()
            self.adapter_cache.pop(adapter_id, None)
        self.producer.refresh_workset(
            candidates=candidates,
            adapters=adapters,
        )
        return len(candidates)

    def run_once(self) -> dict[str, object]:
        self.refresh_workset()
        return self.producer.run_once().as_dict()


def run_once(config_path: Path, *, owner: str) -> dict[str, object]:
    with config_path.open("rb") as source:
        config = tomllib.load(source)
    limits = config.get("limits")
    if not isinstance(limits, dict):
        raise ValueError("limits table is required")

    if config.get("source_mode", "static") == "activated":
        return _run_activated_once(
            config_path,
            config=config,
            limits=limits,
            owner=owner,
        )

    with StaticSourceRuntime(
        config_path,
        config=config,
        limits=limits,
        owner=owner,
    ) as runtime:
        return runtime.run_once()


def _run_activated_once(
    config_path: Path,
    *,
    config: dict[str, object],
    limits: dict[str, object],
    owner: str,
) -> dict[str, object]:
    """Run one lease from discovery ACTIVE candidates."""
    with ActivatedSourceRuntime(
        config_path,
        config=config,
        limits=limits,
        owner=owner,
    ) as runtime:
        return runtime.run_once()


def _empty_watch_total() -> dict[str, object]:
    return {
        "leases_succeeded": 0,
        "source_records": 0,
        "observations": 0,
        "evidence_tasks_enqueued": 0,
        "direct_capsules_committed": 0,
        "admission_blocked": False,
        "max_source_record_queue_depth": 0,
        "max_observation_queue_depth": 0,
    }


def _accumulate_watch_report(
    total: dict[str, object],
    report: dict[str, object],
) -> None:
    for key in (
        "leases_succeeded",
        "source_records",
        "observations",
        "evidence_tasks_enqueued",
        "direct_capsules_committed",
    ):
        total[key] = int(total[key]) + int(report[key])
    total["admission_blocked"] = bool(total["admission_blocked"]) or bool(
        report["admission_blocked"]
    )
    for key in (
        "max_source_record_queue_depth",
        "max_observation_queue_depth",
    ):
        total[key] = max(int(total[key]), int(report[key]))


def _record_source_telemetry(
    telemetry: RuntimeTelemetryStore,
    report: dict[str, object],
) -> None:
    telemetry.add_counters(
        {
            "source_leases_succeeded": int(report["leases_succeeded"]),
            "source_records": int(report["source_records"]),
            "source_observations": int(report["observations"]),
            "source_evidence_tasks_enqueued": int(
                report["evidence_tasks_enqueued"]
            ),
            "source_direct_capsules_committed": int(
                report["direct_capsules_committed"]
            ),
            "source_admission_blocked_events": int(
                bool(report["admission_blocked"])
            ),
        }
    )
    telemetry.set_max_gauges(
        {
            "source_max_record_queue_depth": int(
                report["max_source_record_queue_depth"]
            ),
            "source_max_observation_queue_depth": int(
                report["max_observation_queue_depth"]
            ),
        }
    )


def _watch_loop(
    next_report,
    *,
    stop_event: Event,
    idle_backoff_seconds: float,
    max_idle_backoff_seconds: float,
    sleep_fn,
    report_observer=None,
) -> dict[str, object]:
    total = _empty_watch_total()
    idle = float(idle_backoff_seconds)
    while not stop_event.is_set():
        report = next_report()
        _accumulate_watch_report(total, report)
        if report_observer is not None:
            report_observer(report)
        if report["leases_succeeded"]:
            idle = float(idle_backoff_seconds)
            continue
        if stop_event.is_set():
            break
        if report["admission_blocked"]:
            # Backpressure is a temporary capacity condition, not source idle.
            # Poll at the short cadence so newly terminal evidence slots are
            # refilled promptly instead of waiting through a 60-second
            # exponential idle backoff.
            sleep_fn(float(idle_backoff_seconds))
            idle = float(idle_backoff_seconds)
            continue
        sleep_fn(idle)
        idle = min(float(max_idle_backoff_seconds), idle * 2.0)
    return total


def run_watch(
    config_path: Path,
    *,
    owner: str,
    stop_event: Event | None = None,
    idle_backoff_seconds: float = 1.0,
    max_idle_backoff_seconds: float = 60.0,
    sleep_fn=time.sleep,
) -> dict[str, object]:
    """Run a long-lived producer without reopening the activated runtime."""
    if idle_backoff_seconds <= 0:
        raise ValueError("idle_backoff_seconds must be positive")
    if max_idle_backoff_seconds < idle_backoff_seconds:
        raise ValueError(
            "max_idle_backoff_seconds must not be below idle_backoff_seconds"
        )
    stop_event = stop_event or Event()
    config_path = Path(config_path)
    with config_path.open("rb") as source:
        config = tomllib.load(source)
    limits = config.get("limits")
    if not isinstance(limits, dict):
        raise ValueError("limits table is required")
    runtime_value = config.get("runtime_data_root")
    telemetry: RuntimeTelemetryStore | None = None
    if isinstance(runtime_value, str) and runtime_value.strip():
        runtime_root = _path(
            runtime_value,
            config_path=config_path,
            name="runtime_data_root",
        )
        telemetry = RuntimeTelemetryStore(runtime_root / "telemetry.sqlite3")

    try:
        def observer(report: dict[str, object]) -> None:
            if telemetry is not None:
                _record_source_telemetry(telemetry, report)

        if config.get("source_mode", "static") == "activated":
            runtime_factory = ActivatedSourceRuntime
        elif all(
            isinstance(config.get(name), str) and str(config.get(name)).strip()
            for name in ("runtime_data_root", "baseline_index", "dataset")
        ):
            runtime_factory = StaticSourceRuntime
        else:
            # Preserve the lightweight mocked/embedded watch contract used by
            # tests and callers that inject run_once without a full production
            # configuration. Real static production configs always take the
            # persistent runtime path above.
            return _watch_loop(
                lambda: run_once(config_path, owner=owner),
                stop_event=stop_event,
                idle_backoff_seconds=idle_backoff_seconds,
                max_idle_backoff_seconds=max_idle_backoff_seconds,
                sleep_fn=sleep_fn,
                report_observer=observer,
            )

        with runtime_factory(
            config_path,
            config=config,
            limits=limits,
            owner=owner,
        ) as runtime:
            return _watch_loop(
                runtime.run_once,
                stop_event=stop_event,
                idle_backoff_seconds=idle_backoff_seconds,
                max_idle_backoff_seconds=max_idle_backoff_seconds,
                sleep_fn=sleep_fn,
                report_observer=observer,
            )
    finally:
        if telemetry is not None:
            telemetry.close()

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper-source-producer")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--watch", action="store_true")
    parser.add_argument("config", type=Path)
    parser.add_argument("--owner", default=f"source-producer-{os.getpid()}")
    args = parser.parse_args(argv)
    try:
        if args.once:
            report = run_once(args.config, owner=args.owner)
        else:
            stop = Event()
            for signum in (signal.SIGINT, signal.SIGTERM):
                signal.signal(signum, lambda _signal, _frame: stop.set())
            report = run_watch(args.config, owner=args.owner, stop_event=stop)
    except (OSError, tomllib.TOMLDecodeError, ValueError, KeyError) as exc:
        parser.error(f"invalid source producer configuration: {exc}")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
