"""Gate and validate optional LLM root-query compilation.

The compiler accepts a single bounded model call and emits deterministic
program descriptions. Adapters retain ownership of pagination, checkpoints,
hit normalization, artifact resolution, and all evidence authority decisions.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlparse

from .context import ResearchCompilerContext
from .protocol import RootQuery, RootQueryProgram


class RootQueryCompilerError(ValueError):
    """Raised when a model response cannot cross the proposal boundary."""


class CompilerGateError(RootQueryCompilerError):
    """Raised when deterministic work or cooldown blocks an LLM call."""


ModelCall = Callable[[ResearchCompilerContext], Mapping[str, Any]]


class RootQueryCompiler:
    MAX_QUERIES = 80
    MAX_PROGRAMS = 8
    MAX_HARD_REQUESTS = 80
    FORBIDDEN_FILTERS = {"cursor", "page", "start", "offset", "hit", "artifact"}

    def __init__(self, model_call: ModelCall) -> None:
        self.model_call = model_call

    def _check_gate(self, context: ResearchCompilerContext) -> None:
        if not context.seed_current_program_exhausted:
            raise CompilerGateError("deterministic seed program is not exhausted")
        if context.equivalent_unexecuted_program:
            raise CompilerGateError("equivalent deterministic program is unexecuted")
        if not context.cooldown_satisfied:
            raise CompilerGateError("LLM cooldown is not satisfied")

    @staticmethod
    def _mapping(value: Any, name: str) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise RootQueryCompilerError(f"{name} must be an object")
        return value

    def _query(self, raw: Any, context: ResearchCompilerContext) -> RootQuery:
        item = self._mapping(raw, "query")
        allowed = {"query", "filters", "expected_signal", "expected_family", "max_pages"}
        if set(item) - allowed:
            raise RootQueryCompilerError("query contains forbidden fields")
        query = item.get("query")
        if not isinstance(query, str) or not query.strip():
            raise RootQueryCompilerError("query must be non-empty")
        filters = item.get("filters", {})
        if not isinstance(filters, Mapping):
            raise RootQueryCompilerError("filters must be an object")
        unknown = set(filters) & self.FORBIDDEN_FILTERS
        if unknown or any(f"filter:{key}" not in context.root_capabilities for key in filters):
            raise RootQueryCompilerError("unsupported API filters")
        pages = item.get("max_pages", 1)
        if not isinstance(pages, int) or isinstance(pages, bool) or not 1 <= pages <= 100:
            raise RootQueryCompilerError("max_pages must be a positive bounded integer")
        parsed = urlparse(query.strip())
        if parsed.scheme in {"http", "https"} and parsed.netloc and not filters:
            raise RootQueryCompilerError("ordinary URL proposals are not root query programs")
        return RootQuery(
            query=query.strip(),
            filters=dict(filters),
            expected_signal=str(item.get("expected_signal", "")).strip(),
            expected_family=str(item.get("expected_family", "")).strip(),
            max_pages=pages,
        )

    def compile_root_query_program(self, context: ResearchCompilerContext) -> RootQueryProgram:
        self._check_gate(context)
        payload = self._mapping(self.model_call(context), "model response")
        programs = payload.get("programs")
        if not isinstance(programs, list) or not programs:
            raise RootQueryCompilerError("programs must be a non-empty array")
        if len(programs) > self.MAX_PROGRAMS:
            raise RootQueryCompilerError("too many programs in one model call")
        first = self._mapping(programs[0], "program")
        allowed = {"root_id", "strategy", "queries", "hard_max_requests", "stop_conditions"}
        if set(first) - allowed:
            raise RootQueryCompilerError("program contains forbidden fields")
        if first.get("root_id") != context.root_id:
            raise RootQueryCompilerError("program root_id does not match context")
        strategy = first.get("strategy")
        queries = first.get("queries")
        if not isinstance(strategy, str) or not strategy.strip():
            raise RootQueryCompilerError("strategy must be non-empty")
        if not isinstance(queries, list) or len(queries) > self.MAX_QUERIES:
            raise RootQueryCompilerError("queries must be a finite array of at most 80 items")
        seen = set(context.recent_query_hashes)
        normalized = tuple(q for q in (self._query(item, context) for item in queries) if q.query_hash not in seen)
        bounds = first.get("hard_max_requests")
        if not isinstance(bounds, int) or not 1 <= bounds <= self.MAX_HARD_REQUESTS:
            raise RootQueryCompilerError("hard_max_requests must be within 1..80")
        stops = first.get("stop_conditions")
        if not isinstance(stops, list) or not stops or any(not isinstance(v, str) or not v.strip() for v in stops):
            raise RootQueryCompilerError("stop_conditions must be a non-empty string array")
        return RootQueryProgram(
            root_id=context.root_id,
            strategy=strategy.strip(),
            queries=normalized,
            hard_max_requests=bounds,
            stop_conditions=tuple(v.strip() for v in stops),
            context_hash=context.context_hash,
        )

    def _call(self, context: ResearchCompilerContext) -> Mapping[str, Any]:
        self._check_gate(context)
        return self._mapping(self.model_call(context), "model response")

    def classify_result_cluster(
        self, context: ResearchCompilerContext, cluster: Mapping[str, Any]
    ) -> tuple[dict[str, Any], ...]:
        """Classify an unresolved cluster, never an individual search hit."""
        if not isinstance(cluster.get("cluster_id"), str) or not cluster["cluster_id"].strip():
            raise RootQueryCompilerError("cluster_id is required")
        size = cluster.get("size")
        if not isinstance(size, int) or size < 2:
            raise RootQueryCompilerError("classification requires a result cluster")
        payload = self._call(context)
        items = payload.get("classifications", [])
        if not isinstance(items, list) or not items:
            raise RootQueryCompilerError("classifications must be a non-empty array")
        allowed = {"cluster_id", "classification", "reusable_surface", "rationale"}
        result: list[dict[str, Any]] = []
        for item in items:
            value = self._mapping(item, "classification")
            if set(value) - allowed:
                raise RootQueryCompilerError("classification contains forbidden fields")
            if value.get("cluster_id") != cluster["cluster_id"]:
                raise RootQueryCompilerError("classification cluster_id mismatch")
            if not isinstance(value.get("classification"), str) or not value["classification"].strip():
                raise RootQueryCompilerError("classification label is required")
            result.append(dict(value))
        return tuple(result)

    def compile_pivot_program(
        self, context: ResearchCompilerContext
    ) -> tuple[RootQueryProgram, ...]:
        payload = self._call(context)
        raw = payload.get("pivot_programs")
        if not isinstance(raw, list):
            raise RootQueryCompilerError("pivot_programs must be an array")
        result: list[RootQueryProgram] = []
        for item in raw:
            value = self._mapping(item, "pivot program")
            result.append(self._compile_program(value, context))
        return tuple(result)

    def _compile_program(
        self, value: Mapping[str, Any], context: ResearchCompilerContext
    ) -> RootQueryProgram:
        allowed = {"root_id", "strategy", "queries", "hard_max_requests", "stop_conditions"}
        if set(value) - allowed:
            raise RootQueryCompilerError("program contains forbidden fields")
        if value.get("root_id") != context.root_id:
            raise RootQueryCompilerError("program root_id does not match context")
        queries = value.get("queries")
        if not isinstance(queries, list) or not queries or len(queries) > self.MAX_QUERIES:
            raise RootQueryCompilerError("program queries must be finite and non-empty")
        strategy = value.get("strategy")
        bounds = value.get("hard_max_requests")
        stops = value.get("stop_conditions")
        if not isinstance(strategy, str) or not strategy.strip():
            raise RootQueryCompilerError("strategy must be non-empty")
        if not isinstance(bounds, int) or not 1 <= bounds <= self.MAX_HARD_REQUESTS:
            raise RootQueryCompilerError("hard_max_requests must be within 1..80")
        if not isinstance(stops, list) or not stops:
            raise RootQueryCompilerError("stop_conditions must be non-empty")
        normalized = tuple(self._query(item, context) for item in queries)
        return RootQueryProgram(
            root_id=context.root_id,
            strategy=strategy.strip(),
            queries=normalized,
            hard_max_requests=bounds,
            stop_conditions=tuple(stops),
            context_hash=context.context_hash,
        )

    def propose_new_root(
        self, context: ResearchCompilerContext
    ) -> tuple[dict[str, Any], ...]:
        payload = self._call(context)
        raw = payload.get("new_root_hypotheses")
        if not isinstance(raw, list):
            raise RootQueryCompilerError("new_root_hypotheses must be an array")
        result: list[dict[str, Any]] = []
        for item in raw:
            value = self._mapping(item, "new root hypothesis")
            required = {"kind", "entrypoint", "capabilities", "rationale"}
            if not required <= set(value):
                raise RootQueryCompilerError("new root requires capability-bearing metadata")
            entrypoint = value["entrypoint"]
            capabilities = value["capabilities"]
            if not isinstance(entrypoint, str) or not entrypoint.strip():
                raise RootQueryCompilerError("new root entrypoint is required")
            if not isinstance(capabilities, list) or not capabilities:
                raise RootQueryCompilerError("new root capabilities are required")
            if urlparse(entrypoint).path in {"", "/"} and len(capabilities) < 2:
                raise RootQueryCompilerError("ordinary URL proposal is not a reusable root")
            result.append(dict(value))
        return tuple(result)

    def recover_root_stagnation(
        self, context: ResearchCompilerContext
    ) -> RootQueryProgram:
        payload = self._call(context)
        recovery = payload.get("recovery_program")
        if recovery is None:
            raise RootQueryCompilerError("recovery_program is required")
        return self._compile_program(self._mapping(recovery, "recovery program"), context)
