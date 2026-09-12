"""Bounded subprocess bridge for untrusted source-search agents.

The agent receives one finite SearchDirective and may only return candidate
metadata through a size-limited JSON file. It never receives the registry path or
SQLite connection. The coordinator remains responsible for deduplication,
suppression, directive budget enforcement, attribution, and durable commits.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from creeper.source_discovery.admission import SearchAdmissionPolicy
from creeper.source_discovery.coordinator import SearchBatch
from creeper.source_discovery.manager import SearchDirective
from creeper.source_discovery.models import SourceCandidate, SourceLevel, SourceState


class SearchAgentProtocolError(RuntimeError):
    """Raised when an agent invocation violates the proposal-file contract."""


@dataclass(frozen=True)
class CommandAgentSearchPolicy:
    timeout_seconds: float = 120.0
    termination_grace_seconds: float = 2.0
    max_response_bytes: int = 2 * 1024 * 1024
    max_returned_candidates: int = 2_000

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0 or self.termination_grace_seconds <= 0:
            raise ValueError("agent search timeouts must be positive")
        if self.max_response_bytes < 1 or self.max_returned_candidates < 1:
            raise ValueError("agent search response limits must be positive")


def _candidate_from_payload(payload: Any, *, strategy: str, actor: str) -> SourceCandidate:
    if not isinstance(payload, dict):
        raise SearchAgentProtocolError("agent candidate must be a JSON object")
    allowed = {
        "canonical_entrypoint",
        "source_family",
        "level",
        "expected_year_from",
        "expected_year_to",
        "expected_volume",
        "temporal_semantics_prior",
        "enumerability_prior",
        "direct_evidence_prior",
        "baseline_overlap_prior",
        "access_cost_prior",
        "adapter_cost_prior",
        "confidence",
    }
    unknown = set(payload) - allowed
    if unknown:
        raise SearchAgentProtocolError(
            f"unknown agent candidate fields: {sorted(unknown)}"
        )
    try:
        candidate = SourceCandidate(
            canonical_entrypoint=payload["canonical_entrypoint"],
            source_family=payload["source_family"],
            level=SourceLevel(payload["level"]),
            discovered_by=actor,
            discovery_strategy=strategy,
            expected_year_from=payload.get("expected_year_from"),
            expected_year_to=payload.get("expected_year_to"),
            expected_volume=payload.get("expected_volume"),
            temporal_semantics_prior=payload.get("temporal_semantics_prior", 0.0),
            enumerability_prior=payload.get("enumerability_prior", 0.0),
            direct_evidence_prior=payload.get("direct_evidence_prior", 0.0),
            baseline_overlap_prior=payload.get("baseline_overlap_prior", 0.5),
            access_cost_prior=payload.get("access_cost_prior", 1.0),
            adapter_cost_prior=payload.get("adapter_cost_prior", 1.0),
            confidence=payload.get("confidence", 0.0),
            state=SourceState.DISCOVERED,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SearchAgentProtocolError(f"invalid agent candidate: {exc}") from exc
    return candidate


class CommandAgentSearchExecutor:
    """Invoke a configured agent command with a file-based bounded contract.

    The command is launched as::

        <command...> --request REQUEST.json --response RESPONSE.json

    It must exit zero and write one JSON object with ``query`` and ``candidates``.
    ``backend`` and ``actor`` are fixed by Creeper configuration, not trusted from
    the child process. Invocation directories are retained for reproducibility and
    forensic inspection.
    """

    def __init__(
        self,
        command: tuple[str, ...] | list[str],
        invocation_root: Path,
        *,
        backend: str,
        actor: str,
        cwd: Path | None = None,
        policy: CommandAgentSearchPolicy | None = None,
        admission_policy: SearchAdmissionPolicy | None = None,
        clock=time.monotonic,
    ) -> None:
        command = tuple(command)
        if not command or any(not isinstance(item, str) or not item for item in command):
            raise ValueError("agent search command must contain non-empty arguments")
        if not backend.strip() or not actor.strip():
            raise ValueError("agent search backend and actor are required")
        self.command = command
        self.invocation_root = Path(invocation_root).resolve()
        self.backend = backend
        self.actor = actor
        self.cwd = None if cwd is None else Path(cwd).resolve()
        self.policy = policy or CommandAgentSearchPolicy()
        self.admission_policy = admission_policy
        self.clock = clock

    def _request_payload(self, directive: SearchDirective) -> dict[str, object]:
        payload: dict[str, object] = {
            "contract": "creeper.search-agent.v1",
            "kind": directive.kind.value,
            "strategy": directive.strategy,
            "desired_candidates": directive.desired_candidates,
            "subject": directive.subject,
            "reason": directive.reason,
            "target_year_from": 1996,
            "target_year_to": 2001,
            "requirements": {
                "prefer_metasources": True,
                "prefer_direct_evidence_bulk": True,
                "direct_evidence_suffixes": [
                    ".cdx", ".cdx.gz", ".cdxj", ".cdxj.gz"
                ],
                "direct_evidence_semantics": (
                    "capture timestamp + original URL rows for 1996-2001"
                ),
                "prefer_catalogs_that_enumerate_direct_evidence_bulk": True,
                "resource_priority": [
                    "official archive directory or manifest enumerating CDX/CDXJ",
                    "exact CDX/CDXJ bulk index with target-period captures",
                    "archive collection manifest enumerating WARC/ARC or indexes",
                    "large historical URL/domain dump overlapping 1996-2001",
                    "generic historical source only if no bulk enumerator exists",
                ],
                "search_targets": [
                    "national libraries and web archives",
                    "university or research web-archive datasets",
                    "public archive data-package manifests",
                    "directory indexes and machine-readable file manifests",
                ],
                "avoid_low_yield": [
                    "ordinary archived pages",
                    "single-site snapshots",
                    "undated seed lists without bulk provenance",
                    "search results that do not expose a finite enumerable resource",
                ],
                "exploit_subject_origin": (
                    "when strategy=EXPLOIT_DIRECT_ORIGIN, search only the "
                    "given origin for sibling indexes, manifests, or catalogs"
                ),
                "source_identity": "exact resource URL",
                "evidence_claims_are_not_authorized": True,
            },
        }
        if self.admission_policy is not None:
            admission = self.admission_policy
            payload["admission"] = {
                "min_expected_volume": admission.min_expected_volume,
                "direct_min_expected_volume": admission.direct_min_expected_volume,
                "min_enumerability_prior": admission.min_enumerability_prior,
                "min_confidence": admission.min_confidence,
                "require_year_bounds": admission.require_year_bounds,
                "direct_evidence_year_bounds_optional": True,
                "target_year_from": admission.target_year_from,
                "target_year_to": admission.target_year_to,
            }
        return payload

    async def _terminate_group(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(
                process.wait(), timeout=self.policy.termination_grace_seconds
            )
            return
        except TimeoutError:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.wait()

    def _read_response(
        self,
        path: Path,
        *,
        strategy: str,
    ) -> tuple[
        str,
        tuple[SourceCandidate, ...],
        tuple[dict[str, str], ...],
        int,
    ]:
        try:
            size = path.stat().st_size
        except FileNotFoundError as exc:
            raise SearchAgentProtocolError("agent exited without response JSON") from exc
        if size > self.policy.max_response_bytes:
            raise SearchAgentProtocolError(
                f"agent response exceeds max_response_bytes={self.policy.max_response_bytes}"
            )
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SearchAgentProtocolError(f"invalid agent response JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise SearchAgentProtocolError("agent response must be a JSON object")
        unknown = set(payload) - {"query", "candidates"}
        if unknown:
            raise SearchAgentProtocolError(
                f"unknown agent response fields: {sorted(unknown)}"
            )
        query = payload.get("query")
        candidates = payload.get("candidates")
        if not isinstance(query, str) or not query.strip():
            raise SearchAgentProtocolError("agent response query must be non-empty")
        if not isinstance(candidates, list):
            raise SearchAgentProtocolError("agent response candidates must be a list")
        if len(candidates) > self.policy.max_returned_candidates:
            raise SearchAgentProtocolError(
                "agent returned more candidates than max_returned_candidates"
            )

        accepted: list[SourceCandidate] = []
        rejected: list[dict[str, str]] = []
        for item in candidates:
            candidate = _candidate_from_payload(
                item,
                strategy=strategy,
                actor=self.actor,
            )
            reason = (
                None
                if self.admission_policy is None
                else self.admission_policy.rejection_reason(candidate)
            )
            if reason is None:
                accepted.append(candidate)
            else:
                rejected.append(
                    {
                        "source_key": candidate.source_key,
                        "canonical_entrypoint": candidate.canonical_entrypoint,
                        "reason": reason,
                    }
                )
        return query, tuple(accepted), tuple(rejected), len(candidates)

    @staticmethod
    def _write_admission_audit(
        path: Path,
        *,
        raw_candidate_count: int,
        accepted: tuple[SourceCandidate, ...],
        rejected: tuple[dict[str, str], ...],
    ) -> None:
        payload = {
            "contract": "creeper.search-admission.v1",
            "raw_candidate_count": raw_candidate_count,
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "accepted_source_keys": [candidate.source_key for candidate in accepted],
            "rejected": list(rejected),
        }
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)

    async def __call__(self, directive: SearchDirective) -> SearchBatch:
        invocation_id = uuid.uuid4().hex
        invocation_dir = self.invocation_root / invocation_id
        invocation_dir.mkdir(parents=True, exist_ok=False)
        request_path = invocation_dir / "request.json"
        response_path = invocation_dir / "response.json"
        admission_path = invocation_dir / "admission.json"
        request_path.write_text(
            json.dumps(
                self._request_payload(directive),
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        started = float(self.clock())
        process = await asyncio.create_subprocess_exec(
            *self.command,
            "--request",
            str(request_path),
            "--response",
            str(response_path),
            cwd=self.cwd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            try:
                returncode = await asyncio.wait_for(
                    process.wait(), timeout=self.policy.timeout_seconds
                )
            except TimeoutError:
                await self._terminate_group(process)
                raise TimeoutError(
                    f"source search agent exceeded {self.policy.timeout_seconds:g}s timeout"
                )
        except asyncio.CancelledError:
            await self._terminate_group(process)
            raise

        if returncode != 0:
            raise RuntimeError(f"source search agent failed rc={returncode}")

        query, candidates, rejected, raw_candidate_count = self._read_response(
            response_path,
            strategy=directive.strategy,
        )
        self._write_admission_audit(
            admission_path,
            raw_candidate_count=raw_candidate_count,
            accepted=candidates,
            rejected=rejected,
        )
        elapsed = max(0.0, float(self.clock()) - started)
        return SearchBatch(
            backend=self.backend,
            query=query,
            actor=self.actor,
            candidates=candidates,
            search_cost_seconds=elapsed,
        )
