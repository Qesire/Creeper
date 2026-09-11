"""Resource state machine for bounded local jobs."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
import shutil
from typing import Iterable, Mapping

from creeper.scheduler.credits import ResourceCredits


class GovernorState(StrEnum):
    NORMAL = "normal"
    THROTTLED = "throttled"
    DRAIN_ONLY = "drain_only"
    EMERGENCY_STOP = "emergency_stop"


@dataclass(frozen=True)
class ResourceSample:
    rss_bytes: int
    disk_free_bytes: int
    cpu_percent: float = 0.0
    provider_pressure: float = 0.0


class ResourceGovernor:
    def __init__(
        self,
        *,
        rss_throttle_bytes: int,
        rss_stop_bytes: int,
        disk_throttle_bytes: int,
        disk_stop_bytes: int,
        provider_drain_pressure: float = 1.0,
        provider_throttle_pressure: float = 0.8,
    ):
        if not (0 <= disk_stop_bytes <= disk_throttle_bytes):
            raise ValueError("disk thresholds must be stop <= throttle")
        if not (0 <= rss_throttle_bytes <= rss_stop_bytes):
            raise ValueError("RSS thresholds must be throttle <= stop")
        self.rss_throttle_bytes = rss_throttle_bytes
        self.rss_stop_bytes = rss_stop_bytes
        self.disk_throttle_bytes = disk_throttle_bytes
        self.disk_stop_bytes = disk_stop_bytes
        self.provider_drain_pressure = provider_drain_pressure
        self.provider_throttle_pressure = provider_throttle_pressure

    def evaluate(self, sample: ResourceSample) -> GovernorState:
        if (
            sample.rss_bytes >= self.rss_stop_bytes
            or sample.disk_free_bytes <= self.disk_stop_bytes
        ):
            return GovernorState.EMERGENCY_STOP
        # Low disk is qualitatively different from high RSS: continuing
        # producer/evidence writes can consume the remaining filesystem headroom.
        # Enter local drain-only mode before the hard stop instead of merely
        # reducing acquisition pressure.
        if (
            sample.disk_free_bytes <= self.disk_throttle_bytes
            or sample.provider_pressure >= self.provider_drain_pressure
        ):
            return GovernorState.DRAIN_ONLY
        if (
            sample.rss_bytes >= self.rss_throttle_bytes
            or sample.provider_pressure >= self.provider_throttle_pressure
        ):
            return GovernorState.THROTTLED
        return GovernorState.NORMAL

    def credits(
        self,
        sample: ResourceSample,
        capacities: Mapping[str, int],
    ) -> ResourceCredits:
        """Return stage credits for the state represented by ``sample``."""

        state = self.evaluate(sample)
        values = dict(capacities)
        if any(not isinstance(value, int) or value < 0 for value in values.values()):
            raise ValueError("resource capacities must be non-negative integers")

        resource_names = {"source_fetch", "parse", "commit"}
        evidence_capacities = {
            provider: capacity
            for provider, capacity in values.items()
            if provider not in resource_names
        }

        if state is GovernorState.EMERGENCY_STOP:
            return ResourceCredits(0, 0, {provider: 0 for provider in evidence_capacities}, 0)
        if state is GovernorState.DRAIN_ONLY:
            return ResourceCredits(
                0,
                values.get("parse", 0),
                {provider: 0 for provider in evidence_capacities},
                values.get("commit", 0),
            )
        if state is GovernorState.THROTTLED:
            return ResourceCredits(
                self._throttled(values.get("source_fetch", 0)),
                self._throttled(values.get("parse", 0)),
                {
                    provider: self._throttled(capacity)
                    for provider, capacity in evidence_capacities.items()
                },
                self._throttled(values.get("commit", 0)),
            )
        return ResourceCredits(
            values.get("source_fetch", 0),
            values.get("parse", 0),
            evidence_capacities,
            values.get("commit", 0),
        )

    @staticmethod
    def _throttled(capacity: int) -> int:
        return (capacity + 1) // 2


_STATE_SEVERITY = {
    GovernorState.NORMAL: 0,
    GovernorState.THROTTLED: 1,
    GovernorState.DRAIN_ONLY: 2,
    GovernorState.EMERGENCY_STOP: 3,
}


class StabilizedResourceGovernor:
    """Escalate immediately but require consecutive healthy samples to recover."""

    def __init__(
        self,
        governor: ResourceGovernor,
        *,
        recovery_samples: int = 5,
    ) -> None:
        if not isinstance(recovery_samples, int) or recovery_samples < 1:
            raise ValueError("recovery_samples must be a positive integer")
        self.governor = governor
        self.recovery_samples = recovery_samples
        self.state = GovernorState.NORMAL
        self._recovery_streak = 0

    def update(self, sample: ResourceSample) -> GovernorState:
        raw = self.governor.evaluate(sample)
        current_rank = _STATE_SEVERITY[self.state]
        raw_rank = _STATE_SEVERITY[raw]
        if raw_rank > current_rank:
            self.state = raw
            self._recovery_streak = 0
            return self.state
        if raw is self.state:
            self._recovery_streak = 0
            return self.state

        self._recovery_streak += 1
        if self._recovery_streak >= self.recovery_samples:
            self.state = raw
            self._recovery_streak = 0
        return self.state


class LocalResourceSampler:
    """Sample aggregate Creeper process-tree RSS and runtime-filesystem free space.

    Linux `/proc` is used deliberately because Creeper's autonomous runtime is
    deployed on Linux and this avoids adding a dependency solely for two small
    metrics. Missing/exited PIDs are ignored so sampling remains race-safe while
    the supervisor starts or stops children.
    """

    def __init__(
        self,
        disk_path: Path,
        *,
        proc_root: Path = Path("/proc"),
    ) -> None:
        self.disk_path = Path(disk_path)
        self.proc_root = Path(proc_root)

    @staticmethod
    def _status_values(path: Path) -> tuple[int, int] | None:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
            return None
        ppid = None
        rss_kib = 0
        for line in text.splitlines():
            if line.startswith("PPid:"):
                try:
                    ppid = int(line.split()[1])
                except (IndexError, ValueError):
                    return None
            elif line.startswith("VmRSS:"):
                try:
                    rss_kib = int(line.split()[1])
                except (IndexError, ValueError):
                    rss_kib = 0
        if ppid is None:
            return None
        return ppid, rss_kib * 1024

    def process_tree_rss(self, root_pids: Iterable[int]) -> int:
        roots = {
            int(pid)
            for pid in root_pids
            if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0
        }
        if not roots:
            return 0

        parent_by_pid: dict[int, int] = {}
        rss_by_pid: dict[int, int] = {}
        try:
            entries = tuple(self.proc_root.iterdir())
        except OSError as exc:
            raise RuntimeError(f"cannot inspect process table: {self.proc_root}") from exc
        for entry in entries:
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            values = self._status_values(entry / "status")
            if values is None:
                continue
            ppid, rss_bytes = values
            parent_by_pid[pid] = ppid
            rss_by_pid[pid] = rss_bytes

        children: dict[int, list[int]] = {}
        for pid, ppid in parent_by_pid.items():
            children.setdefault(ppid, []).append(pid)

        seen: set[int] = set()
        stack = list(roots)
        total = 0
        while stack:
            pid = stack.pop()
            if pid in seen:
                continue
            seen.add(pid)
            total += rss_by_pid.get(pid, 0)
            stack.extend(children.get(pid, ()))
        return total

    def sample(
        self,
        root_pids: Iterable[int],
        *,
        provider_pressure: float = 0.0,
    ) -> ResourceSample:
        try:
            disk_free = int(shutil.disk_usage(self.disk_path).free)
        except OSError as exc:
            raise RuntimeError(
                f"cannot sample runtime filesystem: {self.disk_path}"
            ) from exc
        return ResourceSample(
            rss_bytes=self.process_tree_rss(root_pids),
            disk_free_bytes=disk_free,
            provider_pressure=float(provider_pressure),
        )
