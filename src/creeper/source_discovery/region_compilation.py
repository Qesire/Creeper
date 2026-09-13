"""Deterministic compilation contracts for V7 exploration regions."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from creeper.source_discovery.query_family import QueryFamily
from creeper.source_discovery.research_models import ExplorationRegion

COMPILER_VERSION = "v7-integrated-l1"
ENUMERATOR_VERSION = "v7-enumerators-2"
_ENUMERATOR_KINDS = frozenset(
    {
        "STATIC_LIST",
        "HTML_CATALOG",
        "INTEGER_PAGINATION",
        "CURSOR_API",
        "FILENAME_PATTERN",
    }
)


@dataclass(frozen=True)
class ExecutionBounds:
    max_queries: int = 10_000
    max_pages: int = 10_000
    max_artifacts: int = 100_000
    max_requests: int = 10_000
    max_bytes: int = 1_000_000_000
    max_wall_seconds: float = 3600.0

    def __post_init__(self) -> None:
        for name in (
            "max_queries",
            "max_pages",
            "max_artifacts",
            "max_requests",
            "max_bytes",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.max_wall_seconds <= 0:
            raise ValueError("max_wall_seconds must be positive")


@dataclass(frozen=True)
class EnumeratorSpec:
    kind: str
    config: Mapping[str, Any]

    def __post_init__(self) -> None:
        kind = str(self.kind).upper()
        if kind not in _ENUMERATOR_KINDS:
            raise ValueError(f"unsupported deterministic enumerator: {kind}")
        config = dict(self.config)
        _validate_enumerator_config(kind, config)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "config", config)


@dataclass(frozen=True)
class CompiledScoutPlan:
    region_id: str
    region_key: str
    source_family: str
    surface_kind: str
    root: str
    query_family: QueryFamily | None
    enumerator: EnumeratorSpec
    artifact_predicate: Callable[[str], bool] | None
    hard_bounds: ExecutionBounds
    stop_conditions: tuple[str, ...]
    expected_contract_family: str | None = None
    compiler_version: str = COMPILER_VERSION
    enumerator_version: str = ENUMERATOR_VERSION

    def __post_init__(self) -> None:
        if not self.region_id.strip() or not self.region_key.strip():
            raise ValueError("region identity is required")
        if not self.source_family.strip() or not self.root.strip():
            raise ValueError("region source family and root are required")
        if not self.stop_conditions:
            raise ValueError("at least one stop condition is required")
        if (
            self.query_family is not None
            and self.query_family.cardinality > self.hard_bounds.max_queries
        ):
            raise ValueError("query-family expansion exceeds max_queries")
        _validate_plan_cardinality(self)


def _validate_enumerator_config(kind: str, config: Mapping[str, Any]) -> None:
    if kind == "STATIC_LIST":
        urls = config.get("urls", ())
        if isinstance(urls, (str, bytes)) or not isinstance(urls, Sequence):
            raise ValueError("STATIC_LIST urls must be a finite sequence")
        return

    if kind == "FILENAME_PATTERN":
        if not str(config.get("template", "")).strip():
            raise ValueError("FILENAME_PATTERN requires template")
        dimensions = config.get("dimensions", {})
        if not isinstance(dimensions, Mapping):
            raise ValueError("FILENAME_PATTERN dimensions must be a mapping")
        for name, values in dimensions.items():
            if (
                not isinstance(name, str)
                or not name
                or isinstance(values, (str, bytes))
                or not isinstance(values, Sequence)
                or not values
            ):
                raise ValueError(
                    "FILENAME_PATTERN dimensions must be finite non-empty sequences"
                )
        return

    if kind == "HTML_CATALOG":
        if not str(config.get("root", "")).strip():
            raise ValueError("HTML_CATALOG requires root")
        policy = str(config.get("origin_policy", "")).upper()
        if policy not in {"SAME_ORIGIN", "SAME_HOST", "ALLOWLIST"}:
            raise ValueError(
                "HTML_CATALOG requires explicit origin_policy "
                "(SAME_ORIGIN, SAME_HOST, or ALLOWLIST)"
            )
        if policy == "ALLOWLIST" and not tuple(config.get("allowed_origins", ())):
            raise ValueError("ALLOWLIST origin policy requires allowed_origins")
        return

    if kind == "INTEGER_PAGINATION":
        required = ("url_template", "max_page", "terminal_condition")
        if any(not str(config.get(name, "")).strip() for name in required):
            raise ValueError(
                "INTEGER_PAGINATION requires url_template, max_page, "
                "and terminal_condition"
            )
        start = int(config.get("start", 1))
        step = int(config.get("step", 1))
        max_page = int(config["max_page"])
        if start < 0 or step <= 0 or max_page < start:
            raise ValueError("invalid INTEGER_PAGINATION bounds")
        return

    if kind == "CURSOR_API":
        for name in ("endpoint", "record_selector", "next_cursor_selector"):
            if not str(config.get(name, "")).strip():
                raise ValueError(f"CURSOR_API requires {name}")
        return


def _finite_product(dimensions: Mapping[str, Any]) -> int:
    cardinality = 1
    for values in dimensions.values():
        cardinality *= len(values)
    return cardinality


def _validate_plan_cardinality(plan: CompiledScoutPlan) -> None:
    config = plan.enumerator.config
    if plan.enumerator.kind == "STATIC_LIST" and plan.query_family is None:
        if len(tuple(config.get("urls", ()))) > plan.hard_bounds.max_artifacts:
            raise ValueError("STATIC_LIST exceeds max_artifacts before execution")
    elif plan.enumerator.kind == "FILENAME_PATTERN" and plan.query_family is None:
        cardinality = _finite_product(config.get("dimensions", {}))
        if cardinality > plan.hard_bounds.max_artifacts:
            raise ValueError(
                "FILENAME_PATTERN expansion exceeds max_artifacts before execution"
            )
    elif plan.enumerator.kind == "INTEGER_PAGINATION":
        start = int(config.get("start", 1))
        step = int(config.get("step", 1))
        max_page = int(config["max_page"])
        pages = ((max_page - start) // step) + 1
        if pages > plan.hard_bounds.max_pages:
            raise ValueError(
                "INTEGER_PAGINATION range exceeds max_pages before execution"
            )


def compile_query_family(
    template: str,
    dimensions: Mapping[str, Any],
    *,
    max_queries: int,
) -> QueryFamily:
    family = QueryFamily.from_mapping(template, dimensions)
    family.expand(max_queries=max_queries)
    return family


def compile_artifact_predicate(
    spec: Mapping[str, Any] | None,
) -> Callable[[str], bool] | None:
    if not spec:
        return None
    allowed = {"suffix", "prefix", "contains", "regex"}
    unknown = set(spec) - allowed
    if unknown:
        raise ValueError(f"unsupported artifact predicate keys: {sorted(unknown)}")
    suffixes = tuple(str(value).lower() for value in spec.get("suffix", ()))
    prefixes = tuple(str(value) for value in spec.get("prefix", ()))
    contains = tuple(str(value).lower() for value in spec.get("contains", ()))
    patterns = tuple(re.compile(str(value)) for value in spec.get("regex", ()))

    def predicate(url: str) -> bool:
        lowered = url.lower()
        if suffixes and not lowered.endswith(suffixes):
            return False
        if prefixes and not url.startswith(prefixes):
            return False
        if contains and not all(token in lowered for token in contains):
            return False
        if patterns and not any(pattern.search(url) for pattern in patterns):
            return False
        return True

    return predicate


def compile_region(region: ExplorationRegion) -> CompiledScoutPlan:
    """Reconstruct a finite executable plan from durable region JSON."""
    bounds_raw = dict(json.loads(region.hard_bounds_json))
    aliases = {
        "max_seconds": "max_wall_seconds",
        "max_items": "max_artifacts",
    }
    for source, target in aliases.items():
        if source in bounds_raw and target not in bounds_raw:
            bounds_raw[target] = bounds_raw.pop(source)
    bounds = ExecutionBounds(**bounds_raw)

    family_raw = json.loads(region.query_family_json)
    family: QueryFamily | None = None
    if family_raw:
        if "template" not in family_raw:
            raise ValueError("query_family_json requires template")
        family = compile_query_family(
            str(family_raw["template"]),
            family_raw.get("dimensions", {}),
            max_queries=bounds.max_queries,
        )

    enum_raw = dict(json.loads(region.enumerator_spec_json))
    kind = enum_raw.pop("kind", enum_raw.pop("enumerator", None))
    if kind is None:
        raise ValueError("enumerator_spec_json requires kind")
    config = enum_raw.pop("config", enum_raw)
    enumerator = EnumeratorSpec(str(kind), config)

    predicate_raw = json.loads(region.artifact_predicate_json)
    predicate = compile_artifact_predicate(predicate_raw)

    stop_raw = json.loads(region.stop_conditions_json)
    if isinstance(stop_raw, str):
        stop_conditions = (stop_raw,)
    elif isinstance(stop_raw, Mapping):
        stop_conditions = tuple(str(value) for value in stop_raw.values())
    elif isinstance(stop_raw, Sequence):
        stop_conditions = tuple(str(value) for value in stop_raw)
    else:
        raise ValueError("stop_conditions_json must be string/list/object")
    stop_conditions = tuple(value for value in stop_conditions if value)
    if not stop_conditions:
        raise ValueError("stop_conditions_json must contain at least one condition")

    return CompiledScoutPlan(
        region_id=region.region_id,
        region_key=region.region_key,
        source_family=region.expected_source_family,
        surface_kind=region.surface_kind,
        root=region.root,
        query_family=family,
        enumerator=enumerator,
        artifact_predicate=predicate,
        hard_bounds=bounds,
        stop_conditions=stop_conditions,
        expected_contract_family=region.expected_contract_family,
        compiler_version=region.compiler_version,
    )
