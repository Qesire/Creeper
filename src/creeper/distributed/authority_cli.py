"""Command-line service for the local distributed Creeper Authority."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from aiohttp import web

from creeper.authority.baseline_index import BaselineIndex
from creeper.distributed.authority_api import create_authority_app
from creeper.distributed.authority_store import DistributedAuthorityStore
from creeper.distributed.config import (
    load_authority_config,
    load_worker_credentials,
)
from creeper.distributed.reconcile import AuthorityReconciler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="creeper-fabric-authority",
        description="Run the local Creeper Fabric Authority API.",
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
    credentials = load_worker_credentials(
        config.credentials_file,
        allow_empty=True,
    )
    if not config.baseline_index.is_file():
        raise FileNotFoundError(config.baseline_index)
    baseline = BaselineIndex(config.baseline_index)
    metadata = baseline.connection.execute(
        """
        SELECT value FROM authority_metadata
        WHERE key = 'authority_digest'
        """
    ).fetchone()
    if metadata is None or not str(metadata[0]).strip():
        baseline.close()
        raise RuntimeError(
            "distributed Authority requires an authority-bound baseline index"
        )
    # Force schema validation before opening the network listener.
    baseline.counts()
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
    reconciler = AuthorityReconciler(
        store,
        promotion_batch_size=config.promotion_batch_size,
        include_source_pages=config.auto_promote_source_pages,
        host_promotion_batch_size=config.host_promotion_batch_size,
        host_physical_providers=config.host_resolution_providers,
        host_coverage_provider=config.host_coverage_provider,
        host_resolver_version=config.host_resolver_version,
    )

    async def reconciliation_context(_app: web.Application):
        stop = asyncio.Event()
        task = asyncio.create_task(
            reconciler.run_forever(
                interval_seconds=config.reconcile_interval_seconds,
                stop=stop,
            ),
            name="creeper-fabric-authority-reconcile",
        )
        try:
            yield
        finally:
            stop.set()
            await task

    async def cleanup(_app: web.Application) -> None:
        store.close()
        baseline.close()

    app.cleanup_ctx.append(reconciliation_context)
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
