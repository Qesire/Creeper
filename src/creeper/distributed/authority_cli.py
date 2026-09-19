"""Run the Fabric v2 Authority HTTP service."""

from __future__ import annotations

import argparse
from pathlib import Path

from aiohttp import web

from creeper.distributed.authority_api import create_authority_app
from creeper.distributed.config import load_authority_config,load_worker_credentials
from creeper.distributed.store_factory import open_authority_store


def main(argv: list[str]|None=None)->int:
    parser=argparse.ArgumentParser(prog="creeper-fabric-authority")
    parser.add_argument("--config",type=Path,required=True)
    args=parser.parse_args(argv)
    config=load_authority_config(args.config)
    store=open_authority_store(config.database)
    for budget in config.provider_budgets:
        store.configure_provider_budget(
            budget.name,
            requests_per_second=budget.requests_per_second,
            max_global_inflight=budget.max_global_inflight,
            require_qualified_region=budget.require_qualified_region,
        )
    credentials=load_worker_credentials(config.credentials_file)
    app=create_authority_app(
        store,
        credentials,
        max_clock_skew_seconds=config.max_clock_skew_seconds,
    )

    async def close_store(_app):
        store.close()

    app.on_cleanup.append(close_store)
    web.run_app(app,host=config.host,port=config.port)
    return 0


if __name__=="__main__":
    raise SystemExit(main())
