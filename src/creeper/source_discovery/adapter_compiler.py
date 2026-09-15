"""Bounded, proposal-only adapter compiler protocol.

Unknown source formats are not allowed to inject Python into Creeper.  A child
LLM may only propose an AdapterDSLSpec.  The parent validates the proposal
against the exact bounded scout sample before the spec can become durable.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import signal
import time
import uuid
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from creeper.sources.adapter_dsl import (
    AdapterDSLSpec,
    AdapterDSLValidation,
    validate_adapter_dsl,
)


class AdapterCompileState(StrEnum):
    PENDING = "PENDING"
    COMPILED = "COMPILED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class AdapterCompileRequest:
    source_key: str
    locator: str
    content_type: str
    preview: bytes
    year_from: int = 1996
    year_to: int = 2001

    def __post_init__(self) -> None:
        if not isinstance(self.source_key, str) or not self.source_key.strip():
            raise ValueError("adapter compile source_key is required")
        if not isinstance(self.locator, str) or not self.locator.strip():
            raise ValueError("adapter compile locator is required")
        if not isinstance(self.content_type, str):
            raise ValueError("adapter compile content_type must be a string")
        if not isinstance(self.preview, bytes) or not self.preview:
            raise ValueError("adapter compile preview must be non-empty bytes")
        if len(self.preview) > 64 * 1024:
            raise ValueError("adapter compile preview exceeds 64 KiB")
        if not 1996 <= self.year_from <= self.year_to <= 2001:
            raise ValueError("adapter compile years must be within 1996-2001")

    @property
    def sample_sha256(self) -> str:
        return hashlib.sha256(self.preview).hexdigest()


@dataclass(frozen=True, slots=True)
class AdapterCompileJob:
    source_key: str
    locator: str
    content_type: str
    preview: bytes
    sample_sha256: str
    year_from: int
    year_to: int
    attempts: int = 0

    def __post_init__(self) -> None:
        request = AdapterCompileRequest(
            source_key=self.source_key,
            locator=self.locator,
            content_type=self.content_type,
            preview=self.preview,
            year_from=self.year_from,
            year_to=self.year_to,
        )
        if self.sample_sha256 != request.sample_sha256:
            raise ValueError("adapter compile job sample digest mismatch")
        if isinstance(self.attempts, bool) or not isinstance(self.attempts, int) or self.attempts < 0:
            raise ValueError("adapter compile attempts must be non-negative")


@dataclass(frozen=True, slots=True)
class CompiledAdapterResult:
    spec: AdapterDSLSpec
    validation: AdapterDSLValidation
    backend: str
    actor: str
    cost_seconds: float

    def __post_init__(self) -> None:
        if not self.backend.strip() or not self.actor.strip():
            raise ValueError("adapter compiler attribution is required")
        if (
            isinstance(self.cost_seconds, bool)
            or not isinstance(self.cost_seconds, (int, float))
            or not math.isfinite(float(self.cost_seconds))
            or self.cost_seconds < 0
        ):
            raise ValueError("adapter compiler cost must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class AdapterCompilerPolicy:
    timeout_seconds: float = 120.0
    termination_grace_seconds: float = 2.0
    max_response_bytes: int = 64 * 1024
    max_attempts: int = 3
    min_records: int = 3
    min_host_fraction: float = 0.80
    min_timed_fraction: float = 0.80

    def __post_init__(self) -> None:
        for name in ("timeout_seconds", "termination_grace_seconds"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise ValueError(f"{name} must be finite and positive")
        for name in ("max_response_bytes", "max_attempts", "min_records"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("min_host_fraction", "min_timed_fraction"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not 0.5 <= float(value) <= 1.0
            ):
                raise ValueError(f"{name} must be within [0.5,1]")


class AdapterCompilerProtocolError(RuntimeError):
    pass


class CommandAdapterCompiler:
    """Invoke one read-only child and validate its DSL proposal locally."""

    def __init__(
        self,
        command: tuple[str, ...] | list[str],
        invocation_root: Path,
        *,
        backend: str,
        actor: str,
        cwd: Path | None = None,
        policy: AdapterCompilerPolicy | None = None,
        clock=time.perf_counter,
    ) -> None:
        command = tuple(command)
        if not command or any(not isinstance(item, str) or not item for item in command):
            raise ValueError("adapter compiler command must be non-empty")
        if not backend.strip() or not actor.strip():
            raise ValueError("adapter compiler backend and actor are required")
        self.command = command
        self.invocation_root = Path(invocation_root).resolve()
        self.backend = backend.strip()
        self.actor = actor.strip()
        self.cwd = None if cwd is None else Path(cwd).resolve()
        self.policy = policy or AdapterCompilerPolicy()
        self.clock = clock

    @staticmethod
    def _request_payload(job: AdapterCompileJob) -> dict[str, object]:
        preview_text = job.preview.decode("utf-8", errors="replace")
        return {
            "contract": "creeper.adapter-compiler.v1",
            "authority": "parser_proposal_only",
            "source": {
                "source_key": job.source_key,
                "locator": job.locator,
                "content_type": job.content_type,
                "sample_sha256": job.sample_sha256,
                "target_year_from": job.year_from,
                "target_year_to": job.year_to,
            },
            "sample": {
                "encoding": "utf-8-replace",
                "text": preview_text,
            },
            "allowed_dsl": {
                "kind": ["whitespace_columns"],
                "host_column": "integer 0..31",
                "timestamp_column": "null or integer 0..31 distinct from host_column",
                "timestamp_kind": ["none", "year_prefix", "unix_seconds"],
                "skip_lines": "integer 0..64",
                "comment_prefixes": "array of <=8 literal prefixes, each <=16 chars",
                "min_columns": "integer 1..64 containing selected columns",
                "policy_version": "adapter-dsl-v1",
            },
            "requirements": {
                "no_code": True,
                "no_regex": True,
                "no_shell": True,
                "no_evidence_authority": True,
                "must_explain_columns_from_sample": True,
            },
        }

    @staticmethod
    def _spec_from_payload(payload: object) -> AdapterDSLSpec:
        if not isinstance(payload, dict):
            raise AdapterCompilerProtocolError("adapter compiler response must be an object")
        unknown = set(payload) - {"spec", "reason"}
        if unknown:
            raise AdapterCompilerProtocolError(
                f"unknown adapter compiler response fields: {sorted(unknown)}"
            )
        raw_spec = payload.get("spec")
        if not isinstance(raw_spec, dict):
            raise AdapterCompilerProtocolError("adapter compiler response requires spec")
        allowed = {
            "kind",
            "host_column",
            "timestamp_column",
            "timestamp_kind",
            "skip_lines",
            "comment_prefixes",
            "min_columns",
            "policy_version",
        }
        if set(raw_spec) != allowed:
            raise AdapterCompilerProtocolError("adapter compiler spec fields mismatch")
        prefixes = raw_spec.get("comment_prefixes")
        if not isinstance(prefixes, list) or any(
            not isinstance(item, str) for item in prefixes
        ):
            raise AdapterCompilerProtocolError("adapter compiler comment_prefixes invalid")
        normalized = dict(raw_spec)
        normalized["comment_prefixes"] = tuple(prefixes)
        try:
            return AdapterDSLSpec(**normalized)
        except (TypeError, ValueError) as exc:
            raise AdapterCompilerProtocolError(f"invalid adapter DSL proposal: {exc}") from exc

    async def _terminate(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        try:
            process.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=self.policy.termination_grace_seconds,
            )
            return
        except TimeoutError:
            pass
        try:
            process.kill()
        except ProcessLookupError:
            return
        await process.wait()

    async def __call__(self, job: AdapterCompileJob) -> CompiledAdapterResult:
        started = float(self.clock())
        invocation_id = "adapter-" + uuid.uuid4().hex
        invocation_dir = self.invocation_root / invocation_id
        invocation_dir.mkdir(parents=True, exist_ok=False)
        request_path = invocation_dir / "request.json"
        response_path = invocation_dir / "response.json"
        request_path.write_text(
            json.dumps(
                self._request_payload(job),
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            ),
            encoding="utf-8",
        )

        process = await asyncio.create_subprocess_exec(
            *self.command,
            "--request",
            str(request_path),
            "--response",
            str(response_path),
            cwd=None if self.cwd is None else str(self.cwd),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        try:
            _stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.policy.timeout_seconds,
            )
        except TimeoutError as exc:
            await self._terminate(process)
            raise TimeoutError("adapter compiler child timed out") from exc

        if process.returncode != 0:
            detail = stderr.decode("utf-8", errors="replace")[-2000:]
            raise RuntimeError(
                f"adapter compiler child exited {process.returncode}: {detail}"
            )
        if not response_path.is_file():
            raise AdapterCompilerProtocolError(
                "adapter compiler completed without response file"
            )
        if response_path.stat().st_size > self.policy.max_response_bytes:
            raise AdapterCompilerProtocolError("adapter compiler response exceeds limit")
        try:
            payload = json.loads(response_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise AdapterCompilerProtocolError("adapter compiler response is not JSON") from exc
        spec = self._spec_from_payload(payload)
        validation = validate_adapter_dsl(
            spec,
            job.preview,
            year_from=job.year_from,
            year_to=job.year_to,
            min_records=self.policy.min_records,
            min_host_fraction=self.policy.min_host_fraction,
            min_timed_fraction=self.policy.min_timed_fraction,
        )
        cost = max(0.0, float(self.clock()) - started)
        return CompiledAdapterResult(
            spec=spec,
            validation=validation,
            backend=self.backend,
            actor=self.actor,
            cost_seconds=cost,
        )


def bounded_compile_preview(payload: bytes, *, max_bytes: int = 64 * 1024) -> bytes | None:
    """Return a text-like prefix suitable for the proposal-only compiler."""

    if not payload or max_bytes < 1:
        return None
    preview = payload[:max_bytes]
    if b"\x00" in preview[:4096]:
        return None
    decoded = preview.decode("utf-8", errors="replace")
    if not decoded.strip():
        return None
    replacement_fraction = decoded.count("\ufffd") / max(1, len(decoded))
    if replacement_fraction > 0.05:
        return None
    return preview
