"""Local-only administrative CLI for Creeper Fabric Authority state."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.distributed.authority_store import DistributedAuthorityStore
from creeper.authority.normalizer import normalize_official
from creeper.distributed.config import load_authority_config
from creeper.distributed.host_query import distributed_cdx_resolver_identity
from creeper.distributed.provider_catalog import canonical_cdx_provider_configs
from creeper.distributed.search_campaign import SearchCampaign


def _csv_tuple(value: str) -> tuple[str, ...]:
    result = tuple(item.strip() for item in value.split(",") if item.strip())
    if not result:
        raise argparse.ArgumentTypeError("at least one provider is required")
    return result


def _open_store(config_path: Path) -> tuple[DistributedAuthorityStore, BaselineIndex]:
    config = load_authority_config(config_path)
    if not config.baseline_index.is_file():
        raise FileNotFoundError(config.baseline_index)
    baseline = BaselineIndex(config.baseline_index)
    baseline.counts()
    store = DistributedAuthorityStore(
        config.database,
        baseline_index=baseline,
    )
    return store, baseline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="creeper-fabric-control",
        description="Local-only Creeper Fabric Authority control surface.",
    )
    parser.add_argument("--config", type=Path, required=True)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="Print Authority task/provider status as JSON.")

    probe = sub.add_parser(
        "probe",
        help="Admit one provider-region qualification probe task.",
    )
    probe.add_argument("--provider", required=True)
    probe.add_argument("--hostname", required=True)
    probe.add_argument("--region", required=True)
    probe.add_argument("--year", type=int, default=2001)
    probe.add_argument("--samples", type=int, default=3)
    probe.add_argument("--priority", type=float, default=100.0)

    thin = sub.add_parser(
        "thin",
        help="Admit one positive-only exact-year thin probe.",
    )
    thin.add_argument("--hostname", required=True)
    thin.add_argument("--provider", required=True)
    thin.add_argument("--year", type=int, required=True)
    thin.add_argument(
        "--estimated-response-bytes",
        type=int,
        default=64 * 1024,
    )
    thin.add_argument("--priority", type=float, default=0.0)

    explore = sub.add_parser(
        "explore",
        help="Admit one evidence-only historical crawler root.",
    )
    explore.add_argument("--url", required=True)
    explore.add_argument(
        "--archive-providers",
        type=_csv_tuple,
        required=True,
        metavar="P1,P2",
    )
    explore.add_argument("--seed", type=int, default=0)
    explore.add_argument("--priority", type=float, default=0.0)

    seeded = sub.add_parser(
        "seeded-explore",
        help="Admit one frozen SearchCampaign exploration slice.",
    )
    seeded.add_argument("--campaign", type=Path, required=True)
    seeded.add_argument("--seed", type=int, required=True)
    seeded.add_argument("--slot-start", type=int, default=0)
    seeded.add_argument("--slot-count", type=int, default=4)
    seeded.add_argument("--search-endpoint", required=True)
    seeded.add_argument("--query-param", default="q")
    seeded.add_argument("--search-provider", default="web_search")
    seeded.add_argument(
        "--archive-providers",
        type=_csv_tuple,
        required=True,
        metavar="P1,P2",
    )
    seeded.add_argument("--priority", type=float, default=0.0)

    host = sub.add_parser(
        "host",
        help="Admit one hostname for 1996-2001 archive resolution.",
    )
    host.add_argument("--hostname", required=True)
    host.add_argument(
        "--archive-providers",
        type=_csv_tuple,
        default=("internet_archive", "arquivo_pt"),
        metavar="P1,P2",
    )
    host.add_argument("--year-from", type=int, default=1996)
    host.add_argument("--year-to", type=int, default=2001)
    host.add_argument("--priority", type=float, default=0.0)

    hosts = sub.add_parser(
        "hosts",
        help="Stream one-hostname-per-line input into archive resolution.",
    )
    hosts.add_argument("--input", type=Path, required=True)
    hosts.add_argument(
        "--archive-providers",
        type=_csv_tuple,
        default=("internet_archive", "arquivo_pt"),
        metavar="P1,P2",
    )
    hosts.add_argument("--year-from", type=int, default=1996)
    hosts.add_argument("--year-to", type=int, default=2001)
    hosts.add_argument("--priority", type=float, default=0.0)

    bulk = sub.add_parser(
        "bulk",
        help="Admit one structured CDX/CDXJ bulk source shard.",
    )
    bulk.add_argument("--source-id", required=True)
    bulk.add_argument("--locator", required=True)
    bulk.add_argument("--partition", default="all")
    bulk.add_argument("--priority", type=float, default=0.0)

    return parser


def _load_campaign(path: Path) -> SearchCampaign:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("campaign JSON must be an object")
    return SearchCampaign.from_mapping(raw)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store, baseline = _open_store(args.config)
    try:
        if args.command == "status":
            print(
                json.dumps(
                    store.fabric_status_snapshot(),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "probe":
            task_id = store.admit_region_probe_work(
                provider=args.provider,
                probe_hostname=args.hostname,
                target_region=args.region,
                year=args.year,
                samples=args.samples,
                priority=args.priority,
            )
            print(task_id)
            return 0

        if args.command == "thin":
            task_id = store.admit_thin_host_probe(
                hostname=args.hostname,
                provider=args.provider,
                year=args.year,
                estimated_response_bytes=args.estimated_response_bytes,
                priority=args.priority,
            )
            print(task_id)
            return 0

        if args.command == "explore":
            task_id = store.admit_historical_exploration_work(
                url=args.url,
                archive_providers=args.archive_providers,
                seed=args.seed,
                priority=args.priority,
            )
            print(task_id)
            return 0

        if args.command == "seeded-explore":
            campaign = _load_campaign(args.campaign)
            task_id = store.admit_seeded_exploration(
                campaign=campaign,
                seed=args.seed,
                slot_start=args.slot_start,
                slot_count=args.slot_count,
                search_endpoint=args.search_endpoint,
                query_param=args.query_param,
                search_provider=args.search_provider,
                archive_providers=args.archive_providers,
                priority=args.priority,
            )
            print(task_id)
            return 0

        if args.command in {"host", "hosts"}:
            provider_configs = canonical_cdx_provider_configs(
                args.archive_providers
            )
            (
                _provider_set_digest,
                coverage_provider,
                resolver_version,
            ) = distributed_cdx_resolver_identity(provider_configs)

            if args.command == "host":
                task_ids = store.admit_host_resolution_work(
                    hostname=args.hostname,
                    physical_providers=tuple(
                        config.name for config in provider_configs
                    ),
                    coverage_provider=coverage_provider,
                    resolver_version=resolver_version,
                    year_from=args.year_from,
                    year_to=args.year_to,
                    priority=args.priority,
                )
                print(
                    json.dumps(
                        {
                            "hostname": normalize_official(args.hostname),
                            "task_ids": list(task_ids),
                            "coverage_provider": coverage_provider,
                            "resolver_version": resolver_version,
                        },
                        sort_keys=True,
                    )
                )
                return 0

            if not args.input.is_file():
                raise FileNotFoundError(args.input)
            totals = {
                "input_lines": 0,
                "valid_hostnames": 0,
                "admitted_tasks": 0,
                "already_covered": 0,
                "invalid": 0,
            }
            with args.input.open("r", encoding="utf-8", errors="replace") as handle:
                for raw in handle:
                    totals["input_lines"] += 1
                    value = raw.strip()
                    if not value or value.startswith("#"):
                        continue
                    hostname = normalize_official(value)
                    if hostname is None:
                        totals["invalid"] += 1
                        continue
                    totals["valid_hostnames"] += 1
                    task_ids = store.admit_host_resolution_work(
                        hostname=hostname,
                        physical_providers=tuple(
                            config.name for config in provider_configs
                        ),
                        coverage_provider=coverage_provider,
                        resolver_version=resolver_version,
                        year_from=args.year_from,
                        year_to=args.year_to,
                        priority=args.priority,
                    )
                    if task_ids:
                        totals["admitted_tasks"] += len(task_ids)
                    else:
                        totals["already_covered"] += 1
            print(
                json.dumps(
                    {
                        **totals,
                        "coverage_provider": coverage_provider,
                        "resolver_version": resolver_version,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "bulk":
            task_id = store.admit_bulk_source_work(
                source_id=args.source_id,
                source_locator=args.locator,
                partition=args.partition,
                priority=args.priority,
            )
            print(task_id)
            return 0

        raise AssertionError(args.command)
    finally:
        store.close()
        baseline.close()


if __name__ == "__main__":
    raise SystemExit(main())
