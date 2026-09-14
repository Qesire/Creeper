"""Command-line service for the local distributed Creeper Authority."""

from __future__ import annotations

import argparse
from pathlib import Path

from aiohttp import web

from creeper.authority.baseline_index import BaselineIndex
from creeper.distributed.authority_api import create_authority_app
from creeper.distributed.authority_store import DistributedAuthorityStore
from creeper.distributed.config import (
    load_authority_config,
    load_worker_credentials,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="creeper-distributed-authority",
        description="Run the local Creeper distributed Authority API.",
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--allow-public-bind",
        action="store_true",
        help=(
            "Allow a non-loopback listen address. Normally the Authority must "
            "remain loopback-only behind an outbound tunnel."
        ),
    )
    return parser


def _assert_safe_bind(host: str, *, allow_public_bind: bool) -> None:
    loopback = {"127.0.0.1", "::1", "localhost"}
    if host not in loopback and not allow_public_bind:
        raise RuntimeError(
            "refusing non-loopback Authority bind; use a tunnel or explicitly "
            "pass --allow-public-bind"
        )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_authority_config(args.config)
    _assert_safe_bind(
        config.host,
        allow_public_bind=bool(args.allow_public_bind),
    )
    credentials = load_worker_credentials(config.credentials_file)
    baseline = BaselineIndex(config.baseline_index)
    store = DistributedAuthorityStore(
        config.database,
        baseline_index=baseline,
    )
    for budget in config.provider_budgets:
        store.configure_provider_budget(
            budget.name,
            requests_per_second=budget.requests_per_second,
            max_global_inflight=budget.max_global_inflight,
            require_qualified_region=budget.require_qualified_region,
        )

    app = create_authority_app(
        store,
        credentials,
        max_clock_skew_seconds=config.max_clock_skew_seconds,
    )

    async def cleanup(_app: web.Application) -> None:
        store.close()
        baseline.close()

    app.on_cleanup.append(cleanup)
    web.run_app(
        app,
        host=config.host,
        port=config.port,
        print=None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
