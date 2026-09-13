"""Bounded orchestration for adaptive historical-index tomography."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import RegionState
from creeper.source_discovery.region_probe import (
    RegionProbeExecutor,
    RegionProbeResult,
)
from creeper.source_discovery.tomography import (
    RegionTomographyPlanner,
    TomographyAction,
    TomographyActionKind,
)


@dataclass(frozen=True)
class RegionTomographyReport:
    index_key: str
    probe_attempts: int
    probes_succeeded: int
    probes_failed: int
    bytes_read: int
    requests: int
    harvest_ready_regions: tuple[str, ...]
    dropped_regions: tuple[str, ...]
    errors: tuple[str, ...]


class RegionTomographyService:
    """Execute a finite number of probe decisions without harvesting authority."""

    def __init__(
        self,
        registry: IndexSpaceRegistry,
        *,
        probe_executor: RegionProbeExecutor,
        planner: RegionTomographyPlanner,
        probe_parallelism: int = 4,
    ) -> None:
        if probe_parallelism < 1:
            raise ValueError("probe_parallelism must be positive")
        if planner.registry is not registry:
            raise ValueError("planner and service must share one index registry")
        self.registry = registry
        self.probe_executor = probe_executor
        self.planner = planner
        self.probe_parallelism = int(probe_parallelism)

    async def _probe_actions(
        self,
        index_key: str,
        actions: list[TomographyAction],
    ) -> list[tuple[TomographyAction, RegionProbeResult | BaseException]]:
        index = self.registry.get_index(index_key)
        if index is None:
            raise KeyError(f"unknown source index: {index_key}")
        expected_identity = self.registry.get_object_identity(index_key)
        semaphore = asyncio.Semaphore(self.probe_parallelism)

        async def one(
            action: TomographyAction,
        ) -> tuple[TomographyAction, RegionProbeResult | BaseException]:
            async with semaphore:
                try:
                    result = await self.probe_executor.probe(
                        index,
                        action.region,
                        expected_identity=expected_identity,
                    )
                except BaseException as exc:
                    return action, exc
                return action, result

        return list(await asyncio.gather(*(one(action) for action in actions)))

    async def run_once(
        self,
        index_key: str,
        *,
        max_probe_actions: int = 8,
    ) -> RegionTomographyReport:
        """Probe/refine within one hard action budget.

        A failed region is attempted at most once in this invocation. Its durable
        state remains DISCOVERED so a later invocation may retry after transport
        recovery. Positive terminal regions are only marked HARVEST_READY; this
        service never consumes them or writes competition evidence.
        """

        if max_probe_actions < 1:
            raise ValueError("max_probe_actions must be positive")
        if self.registry.get_index(index_key) is None:
            raise KeyError(f"unknown source index: {index_key}")

        attempted: set[str] = set()
        attempts = 0
        succeeded = 0
        bytes_read = 0
        requests = 0
        errors: list[str] = []

        while attempts < max_probe_actions:
            remaining = max_probe_actions - attempts
            planned = self.planner.advance(
                index_key,
                max_actions=max(remaining, self.probe_parallelism),
            )
            probes = [
                action
                for action in planned
                if action.kind is TomographyActionKind.PROBE
                and action.region.region_key not in attempted
            ][:remaining]
            if not probes:
                break

            attempted.update(action.region.region_key for action in probes)
            attempts += len(probes)
            results = await self._probe_actions(index_key, probes)
            for action, result in results:
                if isinstance(result, BaseException):
                    errors.append(
                        f"{action.region.region_key}: "
                        f"{type(result).__name__}: {result}"
                    )
                    continue
                # Bind immutable object identity before any synopsis can
                # become durable authority. A crash after this bind is safe;
                # the inverse ordering could leave a synopsis detached from the
                # object it measured.
                try:
                    self.registry.bind_object_identity(
                        index_key,
                        result.object_identity,
                    )
                except (KeyError, ValueError) as exc:
                    errors.append(
                        f"{action.region.region_key}: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    continue
                # A probe may learn the root bounds from metadata. Preserve the
                # same region identity while making those byte bounds durable.
                self.registry.put_region(result.region)
                self.registry.record_synopsis(result.synopsis)
                succeeded += 1
                bytes_read += result.synopsis.bytes_read
                requests += result.synopsis.requests

        # Let the planner classify newly probed terminal leaves before
        # producing the report. No probe from this final plan is executed.
        self.planner.advance(
            index_key,
            max_actions=max(1, self.probe_parallelism),
        )
        regions = self.registry.list_regions(index_key)
        harvest_ready = tuple(
            sorted(
                region.region_key
                for region in regions
                if region.state is RegionState.HARVEST_READY
            )
        )
        dropped = tuple(
            sorted(
                region.region_key
                for region in regions
                if region.state is RegionState.DROPPED
            )
        )
        return RegionTomographyReport(
            index_key=index_key,
            probe_attempts=attempts,
            probes_succeeded=succeeded,
            probes_failed=attempts - succeeded,
            bytes_read=bytes_read,
            requests=requests,
            harvest_ready_regions=harvest_ready,
            dropped_regions=dropped,
            errors=tuple(errors),
        )
