"""Dependency-free bridge to the isolated Scrapy acquisition sidecar.

Scrapy owns URL scheduling, duplicate filtering, retries, politeness and JOBDIR
persistence.  This module only binds one Creeper source identity to one Scrapy
job, launches the locked sidecar process, and validates its JSONL feed.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
import re
import subprocess
import time
from collections.abc import Iterator
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
            # Lowercase -o intentionally appends.  A resumed JOBDIR must not
            # destroy link records emitted by an earlier bounded batch.
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
    """Bind a JOBDIR to exactly one Creeper source and one append spool.

    Scrapy documents JOBDIR as per-crawl persistent state.  Reusing it for a
    different source can mix scheduler and dupefilter state, so fail closed.
    """
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


class ScrapyScoutLauncher:
    """Launch the isolated uv/Scrapy project without importing it into core."""

    def __init__(
        self,
        project_dir: Path,
        *,
        uv_executable: str = "uv",
        hard_timeout_grace_seconds: int = 30,
        clock=time.monotonic,
    ) -> None:
        self.project_dir = Path(project_dir).resolve()
        if not (self.project_dir / "pyproject.toml").is_file():
            raise ValueError("Scrapy sidecar project_dir must contain pyproject.toml")
        if not (self.project_dir / "uv.lock").is_file():
            raise ValueError("Scrapy sidecar project_dir must contain uv.lock")
        if hard_timeout_grace_seconds < 1:
            raise ValueError("hard_timeout_grace_seconds must be positive")
        self.uv_executable = uv_executable
        self.hard_timeout_grace_seconds = hard_timeout_grace_seconds
        self.clock = clock

    def run(self, spec: ScrapyScoutSpec) -> ScrapyScoutRun:
        prepare_jobdir_binding(spec)
        spec.spool_path.resolve().parent.mkdir(parents=True, exist_ok=True)
        started = float(self.clock())
        try:
            completed = subprocess.run(
                spec.argv(uv_executable=self.uv_executable),
                cwd=self.project_dir,
                check=False,
                timeout=spec.max_seconds + self.hard_timeout_grace_seconds,
            )
        except subprocess.TimeoutExpired:
            return ScrapyScoutRun(
                returncode=None,
                elapsed_seconds=max(0.0, float(self.clock()) - started),
                timed_out=True,
                spool_path=spec.spool_path,
                jobdir=spec.jobdir,
            )
        return ScrapyScoutRun(
            returncode=int(completed.returncode),
            elapsed_seconds=max(0.0, float(self.clock()) - started),
            timed_out=False,
            spool_path=spec.spool_path,
            jobdir=spec.jobdir,
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
) -> Iterator[ScrapyLinkDiscovery]:
    """Stream and validate Scrapy JSONL output with bounded line memory.

    A process crash can leave one incomplete final JSON line.  Only that final
    unterminated invalid row is ignored; malformed committed rows fail closed.
    """
    if not _SOURCE_KEY_RE.fullmatch(expected_source_key):
        raise ValueError("expected_source_key is invalid")
    if max_line_bytes < 1:
        raise ValueError("max_line_bytes must be positive")

    with Path(path).open("rb") as stream:
        line_number = 0
        while True:
            raw = stream.readline(max_line_bytes + 1)
            if not raw:
                break
            line_number += 1
            if len(raw) > max_line_bytes:
                raise ValueError(f"Scrapy JSONL line {line_number} exceeds max_line_bytes")
            if not raw.strip():
                continue
            terminated = raw.endswith(b"\n")
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                if not terminated and stream.read(1) == b"":
                    break
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
