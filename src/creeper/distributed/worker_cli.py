"""Run an outbound-only Creeper Fabric v2 worker."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from creeper.distributed.config import load_worker_config
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.residual_query import PRODUCER_NAME, ResidualQueryProducer
from creeper.distributed.worker import DistributedWorker
from creeper.distributed.worker_spool import WorkerResultSpool


async def _run(config_path: Path) -> None:
    config=load_worker_config(config_path)
    secret=config.load_secret()
    producers={}
    if PRODUCER_NAME in config.descriptor.producers:
        producers[PRODUCER_NAME]=ResidualQueryProducer()
    unknown=set(config.descriptor.producers)-set(producers)
    if unknown:
        raise RuntimeError(
            "no installed Fabric producer for: "+",".join(sorted(unknown))
        )

    spool=WorkerResultSpool(config.spool_database)
    try:
        async with CoordinatorClient(
            config.coordinator_url,
            worker_id=config.descriptor.worker_id,
            worker_instance_id=config.descriptor.worker_instance_id,
            secret=secret,
        ) as client:
            worker=DistributedWorker(
                client,
                config.descriptor,
                producers,
                spool,
                poll_seconds=config.poll_seconds,
                claim_wait_seconds=config.claim_wait_seconds,
                lease_seconds=config.lease_seconds,
                heartbeat_seconds=config.heartbeat_seconds,
            )
            await worker.run_forever()
    finally:
        spool.close()


def main(argv:list[str]|None=None)->int:
    parser=argparse.ArgumentParser(prog="creeper-fabric-worker")
    parser.add_argument("--config",type=Path,required=True)
    args=parser.parse_args(argv)
    asyncio.run(_run(args.config))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
