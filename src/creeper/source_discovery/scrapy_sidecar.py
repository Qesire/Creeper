"""Dependency-free bridge to the isolated Scrapy acquisition sidecar.

Scrapy owns URL scheduling, duplicate filtering, retries, politeness and JOBDIR
persistence. This module binds one Creeper source identity to one Scrapy job,
launches the locked sidecar process, and validates its append-only JSONL feed.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import signal
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from creeper.source_discovery.models import canonicalize_source_entrypoint


_SOURCE_KEY_RE = re.compile(r"^src:[0-9a-f]{64}$")
_BINDING_FILE = "creeper-source-binding.json"


@dataclass(frozen=True)
class ScrapyScoutSpec:
    source_key: str
    start_url: str
    jobdir: Path
    spool_path: Path
    max_pages: int = 100
    max_depth: int = 2
    max_seconds: int = 120
    max_memory_mb: int = 512
    follow_query: bool = False

    def __post_init__(self) -> None:
        if not _SOURCE_KEY_RE.fullmatch(self.source_key):
            raise ValueError("source_key must be a stable Creeper src:<sha256> identity")
        object.__setattr__(self, "start_url", canonicalize_source_entrypoint(self.start_url))
        object.__setattr__(self, "jobdir", Path(self.jobdir))
        object.__setattr__(self, "spool_path", Path(self.spool_path))
        for name in ("max_pages", "max_seconds", "max_memory_mb"):
            value = getattr(self, name)
            if not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if not isinstance(self.max_depth, int) or self.max_depth < 0:
            raise ValueError("max_depth must be a non-negative integer")
        if self.spool_path.suffix.lower() not in {".jsonl", ".jl"}:
            raise ValueError("Scrapy scout spool must use a JSONL extension")

    def argv(self, *, uv_executable: str = "uv") -> list[str]:
        jobdir = str(self.jobdir.resolve())
        spool = str(self.spool_path.resolve())
        return [
            uv_executable,
            "run",
            "--locked",
            "scrapy",
            "crawl",
            "bounded_links",
            "-a",
            f"start_url={self.start_url}",
            "-a",
            f"source_key={self.source_key}",
            "-a",
            f"follow_query={'true' if self.follow_query else 'false'}",
            # Lowercase -o intentionally appends. The bridge repairs an
            # uncommitted crash tail before every resumed invocation.
            "-o",
            f"{spool}:jsonlines",
            "-s",
            f"JOBDIR={jobdir}",
            "-s",
            f"DEPTH_LIMIT={self.max_depth}",
            "-s",
            f"CLOSESPIDER_PAGECOUNT={self.max_pages}",
            "-s",
            f"CLOSESPIDER_TIMEOUT={self.max_seconds}",
            "-s",
            "MEMUSAGE_ENABLED=True",
            "-s",
            f"MEMUSAGE_LIMIT_MB={self.max_memory_mb}",
        ]


@dataclass(frozen=True)
class ScrapyScoutRun:
    returncode: int | None
    elapsed_seconds: float
    timed_out: bool
    spool_path: Path
    jobdir: Path
    spool_start_offset: int = 0
    spool_end_offset: int = 0

    @property
    def succeeded(self) -> bool:
        return self.returncode == 0 and not self.timed_out


@dataclass(frozen=True)
class ScrapyLinkDiscovery:
    source_key: str
    page_url: str
    discovered_url: str
    anchor_text: str
    depth: int
    same_site: bool


def _binding_payload(spec: ScrapyScoutSpec) -> dict[str, str]:
    return {
        "source_key": spec.source_key,
        "start_url": spec.start_url,
        "spool_path": str(spec.spool_path.resolve()),
    }


def prepare_jobdir_binding(spec: ScrapyScoutSpec) -> Path:
    """Bind a JOBDIR to exactly one Creeper source and one append spool."""
    jobdir = spec.jobdir.resolve()
    existed = jobdir.exists()
    jobdir.mkdir(parents=True, exist_ok=True)
    binding_path = jobdir / _BINDING_FILE
    expected = _binding_payload(spec)

    if binding_path.exists():
        try:
            stored = json.loads(binding_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid Scrapy JOBDIR binding: {binding_path}") from exc
        if stored != expected:
            raise ValueError("Scrapy JOBDIR is already bound to a different source or spool")
        return binding_path

    if existed and any(jobdir.iterdir()):
        raise ValueError("refusing to adopt a non-empty unbound Scrapy JOBDIR")

    temporary = binding_path.with_name(f".{binding_path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(expected, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, binding_path)
    return binding_path


def prepare_append_spool(path: Path, *, scan_chunk_bytes: int = 64 * 1024) -> int:
    """Return the committed append offset after dropping one crash-partial tail.

    A newline is the JSONL commit marker. If a killed feed exporter leaves bytes
    after the final newline, only those uncommitted bytes are truncated. The
    operation scans backward in bounded chunks instead of loading a large spool.
    """
    if scan_chunk_bytes < 1:
        raise ValueError("scan_chunk_bytes must be positive")
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        return 0
    with path.open("r+b") as stream:
        stream.seek(0, os.SEEK_END)
        size = stream.tell()
        if size == 0:
            return 0
        stream.seek(size - 1)
        if stream.read(1) == b"\n":
            return size

        position = size
        committed = 0
        while position > 0:
            start = max(0, position - scan_chunk_bytes)
            stream.seek(start)
            chunk = stream.read(position - start)
            newline = chunk.rfind(b"\n")
            if newline >= 0:
                committed = start + newline + 1
                break
            position = start
        stream.truncate(committed)
        stream.flush()
        os.fsync(stream.fileno())
        return committed


def _spool_size(path: Path, *, floor: int = 0) -> int:
    try:
        return max(floor, Path(path).stat().st_size)
    except FileNotFoundError:
        return floor


class ScrapyScoutLauncher:
    """Launch the isolated uv/Scrapy project without importing it into core."""

    def __init__(
        self,
        project_dir: Path,
        *,
        uv_executable: str = "uv",
        hard_timeout_grace_seconds: float = 30.0,
        termination_grace_seconds: float = 5.0,
        clock=time.monotonic,
    ) -> None:
        self.project_dir = Path(project_dir).resolve()
        if not (self.project_dir / "pyproject.toml").is_file():
            raise ValueError("Scrapy sidecar project_dir must contain pyproject.toml")
        if not (self.project_dir / "uv.lock").is_file():
            raise ValueError("Scrapy sidecar project_dir must contain uv.lock")
        if hard_timeout_grace_seconds <= 0:
            raise ValueError("hard_timeout_grace_seconds must be positive")
        if termination_grace_seconds <= 0:
            raise ValueError("termination_grace_seconds must be positive")
        self.uv_executable = uv_executable
        self.hard_timeout_grace_seconds = float(hard_timeout_grace_seconds)
        self.termination_grace_seconds = float(termination_grace_seconds)
        self.clock = clock

    @staticmethod
    def _signal_pid_group(pid: int, sig: signal.Signals) -> None:
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            pass

    def _terminate_sync_process_group(self, process: subprocess.Popen) -> None:
        if process.poll() is not None:
            return
        self._signal_pid_group(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=self.termination_grace_seconds)
            return
        except subprocess.TimeoutExpired:
            pass
        self._signal_pid_group(process.pid, signal.SIGKILL)
        process.wait()

    def run(self, spec: ScrapyScoutSpec) -> ScrapyScoutRun:
        """Synchronous smoke/CLI path; production coordinators use run_async."""
        prepare_jobdir_binding(spec)
        start_offset = prepare_append_spool(spec.spool_path)
        started = float(self.clock())
        process = subprocess.Popen(
            spec.argv(uv_executable=self.uv_executable),
            cwd=self.project_dir,
            start_new_session=True,
        )
        try:
            returncode = process.wait(
                timeout=spec.max_seconds + self.hard_timeout_grace_seconds
            )
        except subprocess.TimeoutExpired:
            self._terminate_sync_process_group(process)
            return ScrapyScoutRun(
                returncode=None,
                elapsed_seconds=max(0.0, float(self.clock()) - started),
                timed_out=True,
                spool_path=spec.spool_path,
                jobdir=spec.jobdir,
                spool_start_offset=start_offset,
                spool_end_offset=_spool_size(spec.spool_path, floor=start_offset),
            )
        return ScrapyScoutRun(
            returncode=int(returncode),
            elapsed_seconds=max(0.0, float(self.clock()) - started),
            timed_out=False,
            spool_path=spec.spool_path,
            jobdir=spec.jobdir,
            spool_start_offset=start_offset,
            spool_end_offset=_spool_size(spec.spool_path, floor=start_offset),
        )

    @staticmethod
    def _signal_process_group(process: asyncio.subprocess.Process, sig: signal.Signals) -> None:
        if process.returncode is not None:
            return
        ScrapyScoutLauncher._signal_pid_group(process.pid, sig)

    async def _terminate_process_group(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        self._signal_process_group(process, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=self.termination_grace_seconds)
            return
        except TimeoutError:
            pass
        self._signal_process_group(process, signal.SIGKILL)
        await process.wait()

    async def run_async(self, spec: ScrapyScoutSpec) -> ScrapyScoutRun:
        """Run one sidecar without blocking the source-discovery event loop."""
        prepare_jobdir_binding(spec)
        start_offset = prepare_append_spool(spec.spool_path)
        started = float(self.clock())
        process = await asyncio.create_subprocess_exec(
            *spec.argv(uv_executable=self.uv_executable),
            cwd=self.project_dir,
            start_new_session=True,
        )
        try:
            try:
                returncode = await asyncio.wait_for(
                    process.wait(),
                    timeout=spec.max_seconds + self.hard_timeout_grace_seconds,
                )
            except TimeoutError:
                await self._terminate_process_group(process)
                return ScrapyScoutRun(
                    returncode=None,
                    elapsed_seconds=max(0.0, float(self.clock()) - started),
                    timed_out=True,
                    spool_path=spec.spool_path,
                    jobdir=spec.jobdir,
                    spool_start_offset=start_offset,
                    spool_end_offset=_spool_size(spec.spool_path, floor=start_offset),
                )
        except asyncio.CancelledError:
            await self._terminate_process_group(process)
            raise
        return ScrapyScoutRun(
            returncode=int(returncode),
            elapsed_seconds=max(0.0, float(self.clock()) - started),
            timed_out=False,
            spool_path=spec.spool_path,
            jobdir=spec.jobdir,
            spool_start_offset=start_offset,
            spool_end_offset=_spool_size(spec.spool_path, floor=start_offset),
        )


def _http_url(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty URL")
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in {"http", "https"} or parsed.hostname is None:
        raise ValueError(f"{field} must be an absolute HTTP(S) URL")
    return value


def iter_scrapy_link_discoveries(
    path: Path,
    *,
    expected_source_key: str,
    max_line_bytes: int = 1 << 20,
    start_offset: int = 0,
    end_offset: int | None = None,
) -> Iterator[ScrapyLinkDiscovery]:
    """Stream a committed JSONL byte range with bounded line memory."""
    if not _SOURCE_KEY_RE.fullmatch(expected_source_key):
        raise ValueError("expected_source_key is invalid")
    if max_line_bytes < 1:
        raise ValueError("max_line_bytes must be positive")
    if start_offset < 0 or (end_offset is not None and end_offset < start_offset):
        raise ValueError("invalid Scrapy JSONL byte range")

    path = Path(path)
    size = path.stat().st_size
    limit = size if end_offset is None else end_offset
    if limit > size:
        raise ValueError("Scrapy JSONL end_offset exceeds file size")

    with path.open("rb") as stream:
        if start_offset:
            if start_offset > size:
                raise ValueError("Scrapy JSONL start_offset exceeds file size")
            stream.seek(start_offset - 1)
            if stream.read(1) != b"\n":
                raise ValueError("Scrapy JSONL start_offset is not a committed line boundary")
        stream.seek(start_offset)
        line_number = 0
        while stream.tell() < limit:
            remaining = limit - stream.tell()
            raw = stream.readline(min(max_line_bytes + 1, remaining))
            if not raw:
                break
            line_number += 1
            if len(raw) > max_line_bytes:
                raise ValueError(f"Scrapy JSONL line {line_number} exceeds max_line_bytes")
            if not raw.strip():
                continue
            terminated = raw.endswith(b"\n")
            at_segment_end = stream.tell() >= limit
            # Newline is the commit marker; even a syntactically complete final
            # object without it is treated as an uncommitted crash tail.
            if not terminated and at_segment_end:
                break
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"invalid Scrapy JSONL row {line_number}") from exc
            if not isinstance(payload, dict) or payload.get("record_type") != "LINK_DISCOVERY":
                raise ValueError(f"unexpected Scrapy record at line {line_number}")
            source_key = payload.get("source_key")
            if source_key != expected_source_key:
                raise ValueError(f"source_key mismatch at Scrapy JSONL line {line_number}")
            depth = payload.get("depth")
            same_site = payload.get("same_site")
            anchor_text = payload.get("anchor_text", "")
            if not isinstance(depth, int) or depth < 0:
                raise ValueError(f"invalid depth at Scrapy JSONL line {line_number}")
            if not isinstance(same_site, bool):
                raise ValueError(f"invalid same_site flag at Scrapy JSONL line {line_number}")
            if not isinstance(anchor_text, str):
                raise ValueError(f"invalid anchor_text at Scrapy JSONL line {line_number}")
            yield ScrapyLinkDiscovery(
                source_key=source_key,
                page_url=_http_url(payload.get("page_url"), field="page_url"),
                discovered_url=_http_url(
                    payload.get("discovered_url"), field="discovered_url"
                ),
                anchor_text=anchor_text,
                depth=depth,
                same_site=same_site,
            )
