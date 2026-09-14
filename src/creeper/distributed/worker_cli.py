"""Command-line runtime for a replaceable distributed Creeper VM worker."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from creeper.distributed.bulk_index import BulkHistoricalIndexProducer
from creeper.distributed.config import load_worker_config
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.host_query import DistributedHostQueryProducer
from creeper.distributed.region_probe import RegionProbeProducer
from creeper.distributed.seeded_search import SeededSearchProducer
from creeper.distributed.source_discovery import SourceDiscoveryProducer
from creeper.distributed.thin_query import ThinHistoricalQueryProducer
from creeper.distributed.worker import DistributedWorker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="creeper-fabric-worker",
        description="Run a pull-based Creeper Fabric worker.",
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Execute at most one claimed task and exit.",
    )
    return parser


async def _run(config_path: Path, *, once: bool) -> int:
    config = load_worker_config(config_path)
    secret = config.load_secret()
    capabilities = set(config.descriptor.capabilities)
    producers = {}

    if "ONLINE_QUERY" in capabilities:
        producers["HistoricalQueryProducer"] = DistributedHostQueryProducer(
            config.cdx_providers
        )
        producers["RegionProbeProducer"] = RegionProbeProducer(
            config.cdx_providers
        )
    if "THIN_QUERY" in capabilities:
        producers["ThinHistoricalQueryProducer"] = ThinHistoricalQueryProducer(
            config.cdx_providers
        )
    if "STREAMING_BULK" in capabilities:
        producers["BulkHistoricalIndexProducer"] = BulkHistoricalIndexProducer()
    if "WEB_DISCOVERY" in capabilities:
        producers["SourceDiscoveryProducer"] = SourceDiscoveryProducer()
    if "SEARCH_QUERY" in capabilities:
        producers["SeededSearchProducer"] = SeededSearchProducer()
    async with CoordinatorClient(
        config.coordinator_url,
        worker_id=config.descriptor.worker_id,
        secret=secret,
    ) as client:
        worker = DistributedWorker(
            client,
            config.descriptor,
            producers,
            lease_seconds=config.lease_seconds,
        )
        while True:
            report = await worker.run_once()
            if once:
                return 1 if report.failed or report.lost_lease else 0
            if not report.claimed or report.failed or report.lost_lease:
                await asyncio.sleep(config.poll_seconds)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return asyncio.run(_run(args.config, once=bool(args.once)))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
