"""Command-line runtime for a replaceable distributed Creeper VM worker."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from creeper.distributed.config import load_worker_config
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.host_query import DistributedHostQueryProducer
from creeper.distributed.region_probe import RegionProbeProducer
from creeper.distributed.worker import DistributedWorker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="creeper-distributed-worker",
        description="Run a pull-based Creeper distributed worker.",
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
    host_query = DistributedHostQueryProducer(config.cdx_providers)
    region_probe = RegionProbeProducer(config.cdx_providers)
    producers = {
        "HistoricalQueryProducer": host_query,
        "RegionProbeProducer": region_probe,
    }
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
