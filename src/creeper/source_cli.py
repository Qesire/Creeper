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
import tomllib

from creeper.authority.baseline_index import BaselineIndex
from creeper.runtime.source_producer import SourceProducer
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.local.static_dataset import StaticDatasetAdapter
from creeper.sources.reservoirs import Reservoir, ReservoirState
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


def run_once(config_path: Path, *, owner: str) -> dict[str, object]:
    with config_path.open("rb") as source:
        config = tomllib.load(source)
    limits = config.get("limits")
    if not isinstance(limits, dict):
        raise ValueError("limits table is required")

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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper-source-producer")
    parser.add_argument("--once", action="store_true", required=True)
    parser.add_argument("config", type=Path)
    parser.add_argument("--owner", default=f"source-producer-{os.getpid()}")
    args = parser.parse_args(argv)
    try:
        report = run_once(args.config, owner=args.owner)
    except (OSError, tomllib.TOMLDecodeError, ValueError, KeyError) as exc:
        parser.error(f"invalid source producer configuration: {exc}")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
