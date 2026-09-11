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
from creeper.runtime.source_producer import SourceProducer
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
            owner=self.owner,
        )

    def close(self) -> None:
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
                adapter = ProductionAdapterFactory.open(reservoir)
                self.adapter_cache[reservoir.adapter_id] = adapter
            adapters[reservoir.adapter_id] = adapter
            if reservoir.evidence_mode == "direct_year":
                lease_records = self.max_records
                expected_tasks = 0
            else:
                # Every currently supported discovery-only production adapter
                # emits at most one host observation / provider task per source
                # record. Shrink the lease to the durable queue headroom rather
                # than requiring the whole configured lease to fit at once.
                lease_records = min(self.max_records, wayback_headroom)
                expected_tasks = lease_records
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
                        evidence_network=(
                            0 if reservoir.evidence_mode == "direct_year" else 1
                        ),
                        cpu=1,
                        ssd=1,
                    ),
                    reservoir=reservoir,
                    lease=template,
                    evidence_mode=reservoir.evidence_mode,
                    expected_evidence_tasks=expected_tasks,
                )
            )
        # Do not retain adapter objects for exhausted/deactivated sources
        # across a multi-hour autonomous run. Current adapters hold no durable
        # authority; cursor state lives in ControlStore.
        active_adapter_ids = set(adapters)
        self.adapter_cache = {
            adapter_id: adapter
            for adapter_id, adapter in self.adapter_cache.items()
            if adapter_id in active_adapter_ids
        }
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
        return _run_activated_once(config_path, config=config, limits=limits, owner=owner)

    backlog_capacity = _positive_int(
        limits.get("evidence_backlog_capacity"),
        "evidence_backlog_capacity",
    )
    queue_capacities = {
        "source_records": _positive_int(
            limits.get("queue_source_records"), "queue_source_records"
        ),
        "observations": _positive_int(
            limits.get("queue_observations"), "queue_observations"
        ),
        "evidence_tasks": _positive_int(
            limits.get("queue_evidence_tasks"), "queue_evidence_tasks"
        ),
        "commits": _positive_int(limits.get("queue_commits"), "queue_commits"),
    }
    max_records = _positive_int(limits.get("lease_max_records"), "lease_max_records")
    max_requests = _positive_int(limits.get("lease_max_requests"), "lease_max_requests")
    max_bytes = _positive_int(limits.get("lease_max_bytes"), "lease_max_bytes")
    max_seconds = _positive_float(limits.get("lease_max_seconds"), "lease_max_seconds")

    baseline_path = _path(
        config.get("baseline_index"), config_path=config_path, name="baseline_index"
    )
    dataset_path = _path(config.get("dataset"), config_path=config_path, name="dataset")
    runtime_root = _path(
        config.get("runtime_data_root"),
        config_path=config_path,
        name="runtime_data_root",
    )
    source_id = config.get("source_id", "local_dataset")
    domain_id = config.get("domain_id", "local_static_dataset")
    reservoir_id = config.get("reservoir_id", source_id)
    source_year = config.get("source_year")
    for name, value in (
        ("source_id", source_id),
        ("domain_id", domain_id),
        ("reservoir_id", reservoir_id),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a non-empty string")
    if source_id != reservoir_id:
        raise ValueError("local static source_id and reservoir_id must match")
    if (
        isinstance(source_year, bool)
        or not isinstance(source_year, int)
        or not 1996 <= source_year <= 2001
    ):
        raise ValueError("source_year must be between 1996 and 2001")

    baseline = BaselineIndex(baseline_path)
    control = ControlStore(runtime_root / "control.sqlite3")
    evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
    try:
        adapter = StaticDatasetAdapter(
            dataset_path,
            source_id=source_id,
            source_year=source_year,
        )
        domain = SourceDomain(
            domain_id=domain_id,
            family="LOCAL_STATIC_DATASET",
            discovery_mechanism="configured local file",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id=reservoir_id,
            domain_id=domain_id,
            adapter_id=adapter.adapter_id,
            root_locator=str(dataset_path),
            enumeration_kind="line_file",
            capacity_lower=0,
            capacity_upper=None,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        stored_domain = control.get_domain(domain_id)
        if stored_domain is None:
            control.save_domain(domain)
        stored_reservoir = control.get_reservoir(reservoir_id)
        if stored_reservoir is None:
            control.save_reservoir(reservoir)
        else:
            reservoir = stored_reservoir

        # This configured local source contributes one target-year hint per
        # record, so max_records is a conservative upper bound on newly created
        # EvidenceTasks for one lease. Other adapters must supply their own
        # conservative bound when constructing LeaseCandidate.
        expected_tasks = max_records
        template = WorkLease.create(
            reservoir_id=reservoir_id,
            cursor_start=reservoir.cursor,
            max_records=max_records,
            max_requests=max_requests,
            max_bytes=max_bytes,
            max_seconds=max_seconds,
            expected_evidence_tasks=expected_tasks,
            expected_novel_eed=float(max_records),
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir_id,
            expected_novel_eed=float(max_records),
            costs=ResourceCost(general_network=0, evidence_network=1, cpu=1, ssd=1),
            reservoir=reservoir,
            lease=template,
            evidence_provider="wayback",
            expected_evidence_tasks=expected_tasks,
        )
        scheduler = GlobalScheduler(CreditLedger({"wayback": backlog_capacity}))
        producer = SourceProducer(
            baseline=baseline,
            control_store=control,
            evidence_store=evidence,
            scheduler=scheduler,
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            backlog_capacities={"wayback": backlog_capacity},
            queue_capacities=queue_capacities,
            owner=owner,
        )
        return producer.run_once().as_dict()
    finally:
        evidence.close()
        control.close()
        baseline.close()


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


def _watch_loop(
    next_report,
    *,
    stop_event: Event,
    idle_backoff_seconds: float,
    max_idle_backoff_seconds: float,
    sleep_fn,
) -> dict[str, object]:
    total = _empty_watch_total()
    idle = float(idle_backoff_seconds)
    while not stop_event.is_set():
        report = next_report()
        _accumulate_watch_report(total, report)
        if report["leases_succeeded"]:
            idle = float(idle_backoff_seconds)
            continue
        if stop_event.is_set():
            break
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

    if config.get("source_mode", "static") == "activated":
        with ActivatedSourceRuntime(
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
            )

    return _watch_loop(
        lambda: run_once(config_path, owner=owner),
        stop_event=stop_event,
        idle_backoff_seconds=idle_backoff_seconds,
        max_idle_backoff_seconds=max_idle_backoff_seconds,
        sleep_fn=sleep_fn,
    )


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
