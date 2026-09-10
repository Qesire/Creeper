"""Small dependency-free command line entry point for local runs."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
import tomllib

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.eed import calculate_eed
from creeper.authority.manifest import build_manifest
from creeper.evidence.providers.cdx import WaybackCDXClient, query_year
from creeper.runtime.doctor import run_doctor
from creeper.runtime.pipeline import SyncRuntime
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.scheduler.leases import WorkLease
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


def _run_once(config_path: Path) -> dict[str, int]:
    with config_path.open("rb") as source:
        config = tomllib.load(source)
    limits = config.get("limits")
    if not isinstance(limits, dict):
        raise ValueError("limits table is required")

    queue_capacities = {
        "source_records": _positive_int(limits.get("queue_source_records"), "queue_source_records"),
        "observations": _positive_int(limits.get("queue_observations"), "queue_observations"),
        "evidence_tasks": _positive_int(limits.get("queue_evidence_tasks"), "queue_evidence_tasks"),
        "commits": _positive_int(limits.get("queue_commits"), "queue_commits"),
    }
    max_records = _positive_int(limits.get("lease_max_records"), "lease_max_records")
    max_requests = _positive_int(limits.get("lease_max_requests"), "lease_max_requests")
    max_bytes = _positive_int(limits.get("lease_max_bytes"), "lease_max_bytes")
    max_seconds = _positive_float(limits.get("lease_max_seconds"), "lease_max_seconds")
    evidence_capacity = _positive_int(limits.get("evidence_capacity"), "evidence_capacity")

    baseline_path = _path(config.get("baseline_index"), config_path=config_path, name="baseline_index")
    dataset_path = _path(config.get("dataset"), config_path=config_path, name="dataset")
    runtime_root = _path(
        config.get("runtime_data_root"), config_path=config_path, name="runtime_data_root"
    )
    source_id = config.get("source_id", "local_dataset")
    domain_id = config.get("domain_id", "local_static_dataset")
    reservoir_id = config.get("reservoir_id", source_id)
    source_year = config.get("source_year")
    if isinstance(source_id, bool) or not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("source_id must be a non-empty string")
    if isinstance(domain_id, bool) or not isinstance(domain_id, str) or not domain_id.strip():
        raise ValueError("domain_id must be a non-empty string")
    if isinstance(reservoir_id, bool) or not isinstance(reservoir_id, str) or not reservoir_id.strip():
        raise ValueError("reservoir_id must be a non-empty string")
    if source_id != reservoir_id:
        raise ValueError("local static source_id and reservoir_id must match")
    if isinstance(source_year, bool) or not isinstance(source_year, int) or not 1996 <= source_year <= 2001:
        raise ValueError("source_year must be between 1996 and 2001")

    baseline = BaselineIndex(baseline_path)
    control = ControlStore(runtime_root / "control.sqlite3")
    evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
    try:
        adapter = StaticDatasetAdapter(
            dataset_path, source_id=source_id, source_year=source_year
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
        lease = WorkLease.create(
            reservoir_id=reservoir_id,
            cursor_start="1",
            max_records=max_records,
            max_requests=max_requests,
            max_bytes=max_bytes,
            max_seconds=max_seconds,
            expected_evidence_tasks=max_records,
            expected_novel_eed=float(max_records),
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir_id,
            expected_novel_eed=float(max_records),
            costs=ResourceCost(general_network=0, evidence_network=1, cpu=1, ssd=1),
            reservoir=reservoir,
            lease=lease,
            expected_evidence_tasks=max_records,
            evidence_provider="wayback",
        )
        control.save_domain(domain)
        control.save_reservoir(reservoir)

        def local_empty_transport(_hostname: str, _year: int):
            return [([], True)]

        runtime = SyncRuntime(
            baseline=baseline,
            control_store=control,
            evidence_store=evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": evidence_capacity})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            evidence_transport=local_empty_transport,
            queue_capacities=queue_capacities,
            evidence_provider="wayback",
        )
        return runtime.run_once().as_dict()
    finally:
        evidence.close()
        control.close()
        baseline.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest = subparsers.add_parser("manifest")
    manifest.add_argument("task_root", type=Path)
    manifest.add_argument("output", type=Path)
    manifest.add_argument("--source-archive", type=Path)

    eed = subparsers.add_parser("eed")
    eed.add_argument("input", type=Path)
    eed.add_argument("model", type=Path)

    baseline = subparsers.add_parser("build-baseline")
    baseline.add_argument("task_root", type=Path)
    baseline.add_argument("output", type=Path)
    baseline.add_argument("--batch-size", type=int, default=50_000)

    evidence = subparsers.add_parser("evidence-query")
    evidence.add_argument("hostname")
    evidence.add_argument("year", type=int)
    evidence.add_argument("--endpoint", default="https://web.archive.org/cdx/search/cdx")
    evidence.add_argument("--timeout", type=float, default=30.0)
    evidence.add_argument("--max-retries", type=int, default=3)

    doctor = subparsers.add_parser("doctor")
    doctor.add_argument("task_root", type=Path)
    doctor.add_argument("data_root", type=Path)
    doctor.add_argument("--min-free-gb", type=float, default=20.0)

    run = subparsers.add_parser("run")
    run.add_argument("--once", action="store_true", required=True)
    run.add_argument("config", type=Path)

    args = parser.parse_args(argv)
    if args.command == "run":
        try:
            report = _run_once(args.config)
        except (OSError, tomllib.TOMLDecodeError, ValueError, KeyError) as exc:
            parser.error(f"invalid run configuration: {exc}")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    if args.command == "manifest":
        print(json.dumps(build_manifest(args.task_root, args.output, source_archive_path=args.source_archive), indent=2))
        return 0
    if args.command == "eed":
        summary, rows = calculate_eed(args.input, args.model)
        print(json.dumps({"summary": summary, "tld_breakdown": rows}, indent=2))
        return 0
    if args.command == "build-baseline":
        index = BaselineIndex.build(args.task_root, args.output, batch_size=args.batch_size)
        print(json.dumps(index.counts(), indent=2))
        index.close()
        return 0
    if args.command == "evidence-query":
        client = WaybackCDXClient(
            endpoint=args.endpoint,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )
        result = query_year(args.hostname, args.year, client, provider="wayback-cdx")
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
        return 0
    if args.command == "doctor":
        report = run_doctor(
            args.task_root,
            args.data_root,
            min_free_bytes=int(args.min_free_gb * 1024**3),
        )
        payload = {**asdict(report), "ready": report.ready}
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0 if report.ready else 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
