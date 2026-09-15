"""Bounded declarative LLM adapter compiler for otherwise-unparsed sources.

The child process cannot return executable code and cannot grant evidence
authority. It may only propose one of Creeper's existing mature parser families
plus optional structured field mappings. The caller must validate the proposal
against the same bounded source sample before persisting any format/schema fact.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import os
import signal
import time
import uuid
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from creeper.source_discovery.models import SourceCandidate
from creeper.sources.format_binding import SourceFormatObservation
from creeper.sources.locator import format_path_from_locator
from creeper.sources.schema_binding import SourceRecordSchema
from creeper.sources.schema_detection import validate_record_schema_proposal


class AdapterCompilerProtocolError(RuntimeError):
    """The child compiler violated the bounded declarative protocol."""


@dataclass(frozen=True, slots=True)
class AdapterCompilerPolicy:
    timeout_seconds: float = 120.0
    termination_grace_seconds: float = 2.0
    max_sample_bytes: int = 64 * 1024
    max_response_bytes: int = 64 * 1024

    def __post_init__(self) -> None:
        for name in ("timeout_seconds", "termination_grace_seconds"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0
            ):
                raise ValueError(f"{name} must be finite and positive")
        for name in ("max_sample_bytes", "max_response_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class AdapterProposal:
    parser_kind: str
    hostname_field: str | None = None
    timestamp_field: str | None = None
    delimiter: str | None = None
    rationale: str = ""

    def __post_init__(self) -> None:
        parser = str(self.parser_kind).strip().lower()
        if parser not in {"jsonl", "delimited", "lines"}:
            raise ValueError("adapter parser_kind must be jsonl, delimited, or lines")
        object.__setattr__(self, "parser_kind", parser)
        host = None if self.hostname_field is None else str(self.hostname_field).strip()
        timestamp = (
            None if self.timestamp_field is None
            else str(self.timestamp_field).strip()
        )
        if parser in {"jsonl", "delimited"}:
            if not host or not timestamp:
                raise ValueError(
                    "structured adapter proposal requires hostname/timestamp fields"
                )
        elif host is not None or timestamp is not None or self.delimiter is not None:
            raise ValueError("lines adapter proposal cannot define structured fields")
        if parser == "delimited":
            if self.delimiter not in {",", "\t", ";", "|"}:
                raise ValueError("delimited proposal requires a supported delimiter")
            for name, value in (
                ("hostname_field", host),
                ("timestamp_field", timestamp),
            ):
                if value is None or not value.startswith("column:"):
                    raise ValueError(
                        f"{name} must use column:N for delimited proposals"
                    )
                try:
                    index = int(value.split(":", 1)[1])
                except ValueError as exc:
                    raise ValueError(f"{name} has invalid column index") from exc
                if index < 0:
                    raise ValueError(f"{name} column index must be non-negative")
        elif parser == "jsonl" and self.delimiter is not None:
            raise ValueError("jsonl proposal cannot define a delimiter")
        rationale = str(self.rationale).strip()
        object.__setattr__(self, "hostname_field", host)
        object.__setattr__(self, "timestamp_field", timestamp)
        object.__setattr__(self, "rationale", rationale[:1000])


@dataclass(frozen=True, slots=True)
class AdapterCompileTrace:
    episode_id: str
    backend: str
    actor: str
    prompt_version: str
    context_hash: str
    elapsed_seconds: float
    status: str
    proposal: AdapterProposal | None

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, str) and value.strip()
            for value in (
                self.episode_id,
                self.backend,
                self.actor,
                self.prompt_version,
                self.context_hash,
                self.status,
            )
        ):
            raise ValueError("adapter compile trace identity fields are required")
        if (
            isinstance(self.elapsed_seconds, bool)
            or not isinstance(self.elapsed_seconds, (int, float))
            or not math.isfinite(float(self.elapsed_seconds))
            or self.elapsed_seconds < 0
        ):
            raise ValueError("adapter compile elapsed_seconds must be non-negative")


def _proposal_from_payload(payload: object) -> AdapterProposal | None:
    if not isinstance(payload, dict):
        raise AdapterCompilerProtocolError("adapter response must be a JSON object")
    allowed = {"status", "adapter"}
    if set(payload) - allowed:
        raise AdapterCompilerProtocolError("adapter response has unsupported fields")
    status = payload.get("status")
    if status == "unsupported":
        if payload.get("adapter") not in (None, {}):
            raise AdapterCompilerProtocolError(
                "unsupported response must not include an adapter"
            )
        return None
    if status != "compiled":
        raise AdapterCompilerProtocolError(
            "adapter response status must be compiled or unsupported"
        )
    raw = payload.get("adapter")
    if not isinstance(raw, dict):
        raise AdapterCompilerProtocolError("compiled response requires adapter")
    allowed_adapter = {
        "parser_kind",
        "hostname_field",
        "timestamp_field",
        "delimiter",
        "rationale",
    }
    unknown = set(raw) - allowed_adapter
    if unknown:
        raise AdapterCompilerProtocolError(
            f"adapter proposal has unsupported fields: {sorted(unknown)}"
        )
    try:
        return AdapterProposal(
            parser_kind=raw["parser_kind"],
            hostname_field=raw.get("hostname_field"),
            timestamp_field=raw.get("timestamp_field"),
            delimiter=raw.get("delimiter"),
            rationale=raw.get("rationale", ""),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AdapterCompilerProtocolError(
            f"invalid adapter proposal: {exc}"
        ) from exc


def _bounded_sample_text(payload: bytes, *, limit: int) -> tuple[str, bool]:
    raw = payload[:limit]
    compressed = raw.startswith(b"\x1f\x8b")
    if compressed:
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            raw = decoder.decompress(raw, limit)
        except zlib.error:
            raw = b""
    return raw.decode("utf-8", errors="replace"), compressed


def validate_adapter_proposal(
    proposal: AdapterProposal,
    *,
    payload: bytes,
    locator: str,
    content_type: str,
    known_format: SourceFormatObservation | None,
) -> tuple[SourceFormatObservation, SourceRecordSchema | None] | None:
    """Convert an LLM proposal into locally validated durable facts."""

    if (
        known_format is not None
        and known_format.parser_kind != proposal.parser_kind
    ):
        return None
    compression = (
        known_format.compression
        if known_format is not None
        else (
            "gzip"
            if payload.startswith(b"\x1f\x8b")
            or format_path_from_locator(locator).endswith(".gz")
            else "none"
        )
    )
    if proposal.parser_kind in {"jsonl", "delimited"}:
        proposed_format = (
            known_format
            if known_format is not None
            else SourceFormatObservation(
                parser_kind=proposal.parser_kind,
                compression=compression,
                detection_method="llm_parser_locally_validated",
                confidence=0.95,
                content_type=content_type,
            )
        )
        schema = validate_record_schema_proposal(
            payload=payload,
            format_observation=proposed_format,
            hostname_field=proposal.hostname_field or "",
            timestamp_field=proposal.timestamp_field or "",
            delimiter=proposal.delimiter,
        )
        if schema is None:
            return None
        if known_format is None:
            proposed_format = SourceFormatObservation(
                parser_kind=proposal.parser_kind,
                compression=compression,
                detection_method="llm_schema_locally_validated",
                confidence=max(0.95, schema.confidence),
                content_type=content_type,
            )
        return proposed_format, schema

    # Plain line sources never become direct evidence from an LLM proposal.
    # The caller still has to parse real hostnames from the sample before this
    # format observation is persisted.
    return (
        known_format
        if known_format is not None
        else SourceFormatObservation(
            parser_kind="lines",
            compression=compression,
            detection_method="llm_parser_pending_sample_validation",
            confidence=0.90,
            content_type=content_type,
        ),
        None,
    )


class AdapterCompilerExecutor(Protocol):
    async def __call__(
        self,
        candidate: SourceCandidate,
        payload: bytes,
        *,
        content_type: str,
        format_observation: SourceFormatObservation | None,
    ) -> AdapterCompileTrace: ...


class CommandAdapterCompilerExecutor:
    """Invoke the configured LLM child command under a non-code protocol."""

    prompt_version = "adapter-compiler-v1"

    def __init__(
        self,
        command: tuple[str, ...] | list[str],
        invocation_root: Path,
        *,
        backend: str,
        actor: str,
        cwd: Path | None = None,
        policy: AdapterCompilerPolicy | None = None,
        clock=time.monotonic,
    ) -> None:
        command = tuple(command)
        if not command or any(
            not isinstance(item, str) or not item for item in command
        ):
            raise ValueError("adapter compiler command must be non-empty")
        if not backend.strip() or not actor.strip():
            raise ValueError("adapter compiler backend and actor are required")
        self.command = command
        self.invocation_root = Path(invocation_root).resolve()
        self.backend = backend
        self.actor = actor
        self.cwd = None if cwd is None else Path(cwd).resolve()
        self.policy = policy or AdapterCompilerPolicy()
        self.clock = clock

    def _request_payload(
        self,
        candidate: SourceCandidate,
        payload: bytes,
        *,
        content_type: str,
        format_observation: SourceFormatObservation | None,
        episode_id: str,
    ) -> dict[str, object]:
        sample_text, gzip_magic = _bounded_sample_text(
            payload,
            limit=self.policy.max_sample_bytes,
        )
        sample_bytes = payload[: self.policy.max_sample_bytes]
        context = {
            "source": {
                "entrypoint": candidate.canonical_entrypoint,
                "family": candidate.source_family,
                "expected_year_from": candidate.expected_year_from,
                "expected_year_to": candidate.expected_year_to,
            },
            "content_type": content_type,
            "known_format": (
                None
                if format_observation is None
                else {
                    "parser_kind": format_observation.parser_kind,
                    "compression": format_observation.compression,
                    "detection_method": format_observation.detection_method,
                }
            ),
            "sample": {
                "text": sample_text,
                "base64_prefix": base64.b64encode(sample_bytes).decode("ascii"),
                "gzip_magic": gzip_magic,
                "bytes": len(sample_bytes),
                "truncated": len(payload) > len(sample_bytes),
            },
        }
        context_hash = hashlib.sha256(
            json.dumps(
                context,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return {
            "contract": "creeper.adapter-compiler.v1",
            "prompt_version": self.prompt_version,
            "episode_id": episode_id,
            "execution": {
                "mode": "SUBAGENT",
                "authority": "proposal_only",
                "network_search_forbidden": True,
                "executable_code_forbidden": True,
            },
            "objective": (
                "identify an existing Creeper parser and, when possible, the "
                "item-level hostname/URL and timestamp/year fields"
            ),
            "target_year_from": 1996,
            "target_year_to": 2001,
            "allowed_parser_kinds": ["jsonl", "delimited", "lines"],
            "requirements": {
                "jsonl_fields": (
                    "return literal object keys present in the sample"
                ),
                "delimited_fields": (
                    "return zero-based column:N selectors and one of comma, "
                    "tab, semicolon, or pipe delimiters"
                ),
                "timestamp_semantics": (
                    "field must contain record observation/capture time or year, "
                    "not dataset publication year"
                ),
                "do_not_guess": True,
                "no_urls_or_search_results": True,
            },
            "context_hash": context_hash,
            "context": context,
            "requested_output": {
                "status": "compiled or unsupported",
                "adapter": {
                    "parser_kind": "jsonl | delimited | lines",
                    "hostname_field": "literal key or column:N; omit for lines",
                    "timestamp_field": "literal key or column:N; omit for lines",
                    "delimiter": "one character for delimited; otherwise null",
                    "rationale": "brief sample-grounded explanation",
                },
            },
        }

    async def __call__(
        self,
        candidate: SourceCandidate,
        payload: bytes,
        *,
        content_type: str,
        format_observation: SourceFormatObservation | None,
    ) -> AdapterCompileTrace:
        episode_id = f"llm:adapter:{uuid.uuid4().hex}"
        request_payload = self._request_payload(
            candidate,
            payload,
            content_type=content_type,
            format_observation=format_observation,
            episode_id=episode_id,
        )
        context_hash = str(request_payload["context_hash"])
        self.invocation_root.mkdir(parents=True, exist_ok=True)
        invocation_dir = self.invocation_root / episode_id.replace(":", "-")
        invocation_dir.mkdir(parents=False, exist_ok=False)
        request_path = invocation_dir / "request.json"
        response_path = invocation_dir / "response.json"
        request_path.write_text(
            json.dumps(request_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        started = float(self.clock())
        process = await asyncio.create_subprocess_exec(
            *self.command,
            "--request",
            str(request_path),
            "--response",
            str(response_path),
            cwd=(None if self.cwd is None else str(self.cwd)),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=self.policy.timeout_seconds,
            )
        except asyncio.TimeoutError:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(
                    process.wait(),
                    timeout=self.policy.termination_grace_seconds,
                )
            except asyncio.TimeoutError:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await process.wait()
            raise AdapterCompilerProtocolError("adapter compiler timed out")

        elapsed = max(0.0, float(self.clock()) - started)
        if process.returncode != 0:
            raise AdapterCompilerProtocolError(
                f"adapter compiler exited with status {process.returncode}"
            )
        if not response_path.is_file():
            raise AdapterCompilerProtocolError(
                "adapter compiler did not write response file"
            )
        size = response_path.stat().st_size
        if size < 1 or size > self.policy.max_response_bytes:
            raise AdapterCompilerProtocolError(
                "adapter compiler response size is invalid"
            )
        try:
            response = json.loads(response_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AdapterCompilerProtocolError(
                "adapter compiler response is not valid JSON"
            ) from exc
        proposal = _proposal_from_payload(response)
        return AdapterCompileTrace(
            episode_id=episode_id,
            backend=self.backend,
            actor=self.actor,
            prompt_version=self.prompt_version,
            context_hash=context_hash,
            elapsed_seconds=elapsed,
            status="unsupported" if proposal is None else "compiled",
            proposal=proposal,
        )
