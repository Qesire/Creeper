"""Deterministic compiler for v3 LLM research-region proposals.

The compiler is deliberately independent of the coordinator.  It validates and
normalizes proposal data before another component turns compiled plans into
source candidates or region work.
"""
from __future__ import annotations

import itertools
import math
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable
from urllib.parse import urlsplit

from creeper.source_discovery.models import canonicalize_source_entrypoint, is_common_crawl_provenance


class ResearchCompilerError(ValueError):
    """A proposal is not safe or finite enough to compile."""


class RegionSurfaceKind(StrEnum):
    WEB_SEARCH = "WEB_SEARCH"
    HTML_CATALOG = "HTML_CATALOG"
    HTTP_API = "HTTP_API"
    REPOSITORY = "REPOSITORY"
    MANIFEST = "MANIFEST"
    FILENAME_FAMILY = "FILENAME_FAMILY"
    LOCAL_HINT = "LOCAL_HINT"


class RegionEnumeratorKind(StrEnum):
    STATIC_LIST = "STATIC_LIST"
    HTML_CATALOG = "HTML_CATALOG"
    INTEGER_PAGINATION = "INTEGER_PAGINATION"
    CURSOR_API = "CURSOR_API"
    FILENAME_PATTERN = "FILENAME_PATTERN"
    MANIFEST_FILE = "MANIFEST_FILE"


class RegionState(StrEnum):
    PROPOSED = "PROPOSED"
    VALIDATED = "VALIDATED"
    READY = "READY"
    RUNNING = "RUNNING"
    EXHAUSTED = "EXHAUSTED"
    REJECTED = "REJECTED"


_ALLOWED_TASKS = frozenset({
    "DISCOVER_NEW_SOURCE", "EXPLOIT_SUCCESS_PATTERN",
    "INTERPRET_STRUCTURE", "RECOVER_STAGNATION",
    "INTERPRET_EVIDENCE_CONTRACT",
})


@dataclass(frozen=True)
class ResearchCompilerPolicy:
    min_expected_fanout: int = 100
    max_regions_per_response: int = 8
    max_query_expansion: int = 512
    max_template_expansion: int = 4096
    target_year_from: int = 1996
    target_year_to: int = 2001

    def __post_init__(self) -> None:
        if self.min_expected_fanout < 1 or self.max_regions_per_response < 1:
            raise ValueError("region limits must be positive")
        if self.max_query_expansion < 1 or self.max_template_expansion < 1:
            raise ValueError("expansion limits must be positive")
        if self.target_year_from > self.target_year_to:
            raise ValueError("target years are reversed")


@dataclass(frozen=True)
class CompiledScoutPlan:
    proposal_id: str
    region_key: str
    surface_kind: RegionSurfaceKind
    root: str
    query_family: dict[str, Any]
    enumerator_spec: dict[str, Any]
    artifact_predicate: dict[str, Any]
    hard_bounds: dict[str, int]
    stop_conditions: tuple[str, ...]
    expected_source_family: str
    expected_contract_family: str
    expected_mechanism: str
    expected_fanout: int
    confidence: float
    validation: dict[str, Any]
    state: RegionState = RegionState.VALIDATED
    context_hash: str = ""
    created_by_episode_id: str = ""

    def as_region(self) -> dict[str, Any]:
        return {
            "region_id": self.proposal_id,
            "region_key": self.region_key,
            "source": self.root,
            "surface_kind": self.surface_kind.value,
            "root": self.root,
            "query_family": self.query_family,
            "enumerator_spec": self.enumerator_spec,
            "artifact_predicate": self.artifact_predicate,
            "hard_bounds": self.hard_bounds,
            "stop_conditions": list(self.stop_conditions),
            "expected_contract_family": self.expected_contract_family,
            "state": self.state.value,
            "context_hash": self.context_hash,
            "created_by_episode_id": self.created_by_episode_id,
        }


class ResearchCompiler:
    """Validate finite region proposals in the frozen parent-side order."""

    def __init__(
        self,
        *,
        policy: ResearchCompilerPolicy | None = None,
        negative_knowledge: Callable[[str], bool] | None = None,
    ) -> None:
        self.policy = policy or ResearchCompilerPolicy()
        self.negative_knowledge = negative_knowledge

    def compile_response(
        self,
        response: dict[str, Any],
        *,
        context_hash: str = "",
        episode_id: str = "",
        task_type: str | None = None,
    ) -> tuple[CompiledScoutPlan, ...]:
        if not isinstance(response, dict):
            raise ResearchCompilerError("research response must be an object")
        if set(response) - {"query", "regions", "contract_proposals"}:
            raise ResearchCompilerError("unknown research response fields")
        if not isinstance(response.get("query"), str) or not response["query"].strip():
            raise ResearchCompilerError("research query must be non-empty")
        regions = response.get("regions", [])
        if not isinstance(regions, list):
            raise ResearchCompilerError("regions must be an array")
        if len(regions) > self.policy.max_regions_per_response:
            raise ResearchCompilerError("too many regions in one response")
        if task_type is not None and task_type not in _ALLOWED_TASKS:
            raise ResearchCompilerError("unsupported research task type")
        plans = tuple(self._compile_region(item, context_hash, episode_id) for item in regions)
        keys = [plan.region_key for plan in plans]
        if len(keys) != len(set(keys)):
            raise ResearchCompilerError("duplicate region keys")
        return plans

    def _compile_region(self, raw: Any, context_hash: str, episode_id: str) -> CompiledScoutPlan:
        if not isinstance(raw, dict):
            raise ResearchCompilerError("region must be an object")
        required = {
            "proposal_id", "surface_kind", "root", "purpose", "query_family",
            "enumerator", "artifact_predicate", "hard_bounds", "stop_conditions",
            "expected_source_family", "expected_contract_family",
            "expected_mechanism", "expected_fanout", "confidence", "validation",
        }
        if set(raw) != required:
            missing = sorted(required - set(raw))
            extra = sorted(set(raw) - required)
            raise ResearchCompilerError(f"region fields invalid missing={missing} extra={extra}")
        proposal_id = raw["proposal_id"]
        if not isinstance(proposal_id, str) or not proposal_id.strip():
            raise ResearchCompilerError("proposal_id must be non-empty")
        try:
            surface = RegionSurfaceKind(raw["surface_kind"])
            enumerator = RegionEnumeratorKind(raw["enumerator"])
        except (TypeError, ValueError) as exc:
            raise ResearchCompilerError("unsupported surface or enumerator") from exc
        root = canonicalize_source_entrypoint(raw["root"])
        if is_common_crawl_provenance(root, raw["expected_source_family"], raw["expected_contract_family"]):
            raise ResearchCompilerError("Common Crawl active discovery is excluded")
        if self.negative_knowledge is not None and self.negative_knowledge(root):
            raise ResearchCompilerError("root is blocked by negative knowledge")
        if not isinstance(raw["purpose"], str) or not raw["purpose"].strip():
            raise ResearchCompilerError("purpose must be non-empty")
        if not isinstance(raw["query_family"], dict):
            raise ResearchCompilerError("query_family must be an object")
        if not isinstance(raw["artifact_predicate"], dict):
            raise ResearchCompilerError("artifact_predicate must be an object")
        if not isinstance(raw["validation"], dict):
            raise ResearchCompilerError("validation must be an object")
        bounds = self._bounds(raw["hard_bounds"])
        stops = self._stops(raw["stop_conditions"])
        fanout = self._fanout(raw["query_family"], raw["enumerator"], bounds, raw["expected_fanout"])
        if fanout < self.policy.min_expected_fanout and not self._small_region_exception(raw, surface):
            raise ResearchCompilerError("expected fanout is below minimum")
        confidence = raw["confidence"]
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise ResearchCompilerError("confidence must be between 0 and 1")
        for name in ("expected_source_family", "expected_contract_family", "expected_mechanism"):
            if not isinstance(raw[name], str) or not raw[name].strip():
                raise ResearchCompilerError(f"{name} must be non-empty")
        return CompiledScoutPlan(
            proposal_id=proposal_id.strip(),
            region_key=self._region_key(root, raw["query_family"], raw["enumerator"]),
            surface_kind=surface,
            root=root,
            query_family=dict(raw["query_family"]),
            enumerator_spec={"kind": enumerator.value},
            artifact_predicate=dict(raw["artifact_predicate"]),
            hard_bounds=bounds,
            stop_conditions=stops,
            expected_source_family=raw["expected_source_family"].strip(),
            expected_contract_family=raw["expected_contract_family"].strip(),
            expected_mechanism=raw["expected_mechanism"].strip(),
            expected_fanout=fanout,
            confidence=float(confidence),
            validation=dict(raw["validation"]),
            context_hash=context_hash,
            created_by_episode_id=episode_id,
        )

    def _bounds(self, raw: Any) -> dict[str, int]:
        if not isinstance(raw, dict) or not raw:
            raise ResearchCompilerError("hard_bounds must be a non-empty object")
        allowed = {"max_requests", "max_pages", "max_artifacts", "max_bytes", "max_seconds", "max_items"}
        if set(raw) - allowed:
            raise ResearchCompilerError("unknown hard bound")
        result: dict[str, int] = {}
        for key, value in raw.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ResearchCompilerError("hard bounds must be positive integers")
            result[key] = value
        return result

    @staticmethod
    def _stops(raw: Any) -> tuple[str, ...]:
        if not isinstance(raw, list) or not raw or any(not isinstance(item, str) or not item.strip() for item in raw):
            raise ResearchCompilerError("stop_conditions must be a non-empty string array")
        return tuple(dict.fromkeys(item.strip() for item in raw))

    def _fanout(self, query_family: dict[str, Any], enumerator: str, bounds: dict[str, int], declared: Any) -> int:
        if isinstance(declared, bool) or not isinstance(declared, int) or declared < 1:
            raise ResearchCompilerError("expected_fanout must be a positive integer")
        cardinality = 1
        for value in query_family.values():
            if isinstance(value, list):
                if not value or any(isinstance(item, (dict, list)) for item in value):
                    raise ResearchCompilerError("query family lists must be finite scalar arrays")
                cardinality *= len(set(map(str, value)))
        if enumerator == RegionEnumeratorKind.INTEGER_PAGINATION.value:
            cardinality *= bounds.get("max_pages", bounds.get("max_requests", 1))
        elif enumerator == RegionEnumeratorKind.FILENAME_PATTERN.value:
            cardinality *= bounds.get("max_items", bounds.get("max_artifacts", 1))
        elif enumerator in {RegionEnumeratorKind.CURSOR_API.value, RegionEnumeratorKind.HTML_CATALOG.value}:
            cardinality *= bounds.get("max_pages", bounds.get("max_requests", 1))
        computed = max(1, cardinality)
        if declared > self.policy.max_template_expansion:
            raise ResearchCompilerError("declared fanout exceeds policy")
        return min(declared, computed) if computed < declared else computed

    def _small_region_exception(self, raw: dict[str, Any], surface: RegionSurfaceKind) -> bool:
        return surface in {
            RegionSurfaceKind.HTML_CATALOG,
            RegionSurfaceKind.MANIFEST,
            RegionSurfaceKind.REPOSITORY,
        }

    @staticmethod
    def _region_key(root: str, query_family: dict[str, Any], enumerator: str) -> str:
        import hashlib, json
        encoded = json.dumps({"root": root, "query_family": query_family, "enumerator": enumerator}, sort_keys=True, separators=(",", ":"))
        return "region:" + hashlib.sha256(encoded.encode()).hexdigest()
