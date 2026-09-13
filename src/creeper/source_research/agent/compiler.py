"""Unified structural compiler for all bounded LLM research proposals.

The compiler is proposal-only.  It validates finite output and returns typed
objects; it does not persist rules, schedule work, promote policies, mutate
EvidenceStore, or authorize submission.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlparse

from .context import LearningCompilerContext, ResearchCompilerContext
from .protocol import (
    ContractFamilyProposal,
    ExplorationRegionProposal,
    FamilyRecognizerProposal,
    NegativeRuleProposal,
    PivotProgramProposal,
    PivotRuleProposal,
    ProposalEnvelope,
    ProposalType,
    QueryFamilyProposal,
    QueryProgramProposal,
    RootQuery,
    RootQueryProgram,
    RootSurfaceProposal,
    TASK_ALLOWED_PROPOSALS,
    UnifiedLLMTask,
)


class UnifiedCompilerError(ValueError):
    """A child response cannot cross the proposal boundary."""


class CompilerGateError(UnifiedCompilerError):
    """The parent-side deterministic or learning gate is not open."""


class RootQueryCompilerError(UnifiedCompilerError):
    """Compatibility error name retained for V7.1 consumers."""


_FORBIDDEN_RESPONSE_FIELDS = {
    "artifact",
    "artifacts",
    "cursor",
    "hits",
    "metadata",
    "next",
    "page",
    "resumptiontoken",
    "evidence",
    "accepted_eed",
    "submission",
    "policy_action",
}

_FORBIDDEN_FILTERS = {
    "cursor",
    "page",
    "start",
    "offset",
    "hit",
    "artifact",
}

_EXECUTION_TYPES = {
    ProposalType.EXPLORATION_REGION,
    ProposalType.CONTRACT_FAMILY,
}
_RESEARCH_TYPES = {
    ProposalType.QUERY_PROGRAM,
    ProposalType.PIVOT_PROGRAM,
    ProposalType.ROOT_SURFACE,
}
_LEARNING_TYPES = {
    ProposalType.QUERY_FAMILY,
    ProposalType.PIVOT_RULE,
    ProposalType.NEGATIVE_RULE,
    ProposalType.FAMILY_RECOGNIZER,
}


class UnifiedResearchCompiler:
    """Compile one schema-shaped child response into bounded proposal objects."""

    MAX_PROPOSALS = 8
    MAX_QUERIES = 80
    MAX_HARD_REQUESTS = 80
    MAX_TEMPLATE_EXPANSION = 4096

    def compile(
        self,
        payload: Mapping[str, Any],
        *,
        task_type: UnifiedLLMTask | str,
        context: ResearchCompilerContext | LearningCompilerContext | None = None,
        context_hash: str = "",
    ) -> ProposalEnvelope:
        task = self._task(task_type)
        value = self._mapping(payload, "model response")
        self._reject_forbidden_keys(value, "model response")
        allowed_top = {"query", "proposals", "contract", "context_hash"}
        if set(value) - allowed_top:
            raise UnifiedCompilerError("model response contains unknown fields")
        query = self._text(value.get("query"), "query")
        raw_proposals = value.get("proposals")
        if not isinstance(raw_proposals, list):
            raise UnifiedCompilerError("proposals must be an array")
        if not 1 <= len(raw_proposals) <= self.MAX_PROPOSALS:
            raise UnifiedCompilerError(
                f"proposals must contain 1..{self.MAX_PROPOSALS} items"
            )

        self._check_gate(task, context)
        expected = TASK_ALLOWED_PROPOSALS[task]
        compiled = []
        resolved_context_hash = context_hash or (
            context.context_hash if context is not None else ""
        )
        for raw in raw_proposals:
            item = self._mapping(raw, "proposal")
            self._reject_forbidden_keys(item, "proposal")
            type_value = item.get("type")
            try:
                proposal_type = ProposalType(type_value)
            except (TypeError, ValueError) as exc:
                raise UnifiedCompilerError("unsupported proposal type") from exc
            if proposal_type not in expected:
                raise UnifiedCompilerError(
                    f"{proposal_type.value} is not allowed for {task.value}"
                )
            compiled.append(
                self._compile_proposal(
                    proposal_type,
                    item,
                    context=context,
                    context_hash=resolved_context_hash,
                )
            )

        reuse_keys = [
            getattr(item, "reuse_key", "")
            for item in compiled
            if getattr(item, "reuse_key", "")
        ]
        if len(reuse_keys) != len(set(reuse_keys)):
            raise UnifiedCompilerError("duplicate reuse_key in one model response")

        return ProposalEnvelope(
            query=query,
            task_type=task,
            context_hash=resolved_context_hash,
            proposals=tuple(compiled),
        )

    @staticmethod
    def _task(value: UnifiedLLMTask | str) -> UnifiedLLMTask:
        try:
            return value if isinstance(value, UnifiedLLMTask) else UnifiedLLMTask(value)
        except ValueError as exc:
            raise UnifiedCompilerError("unsupported task type") from exc

    @staticmethod
    def _mapping(value: Any, name: str) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise UnifiedCompilerError(f"{name} must be an object")
        if any(not isinstance(key, str) for key in value):
            raise UnifiedCompilerError(f"{name} contains non-string fields")
        return value

    @staticmethod
    def _text(value: Any, name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise UnifiedCompilerError(f"{name} must be a non-empty string")
        return value.strip()

    @classmethod
    def _reject_forbidden_keys(
        cls, value: Mapping[str, Any], name: str
    ) -> None:
        forbidden = {
            key.casefold()
            for key in value
            if key.casefold() in _FORBIDDEN_RESPONSE_FIELDS
        }
        if forbidden:
            raise UnifiedCompilerError(
                f"{name} contains forbidden authority/runtime fields: "
                + ", ".join(sorted(forbidden))
            )

    @staticmethod
    def _check_gate(
        task: UnifiedLLMTask,
        context: ResearchCompilerContext | LearningCompilerContext | None,
    ) -> None:
        if task in {
            UnifiedLLMTask.COMPILE_ROOT_QUERY_PROGRAM,
            UnifiedLLMTask.COMPILE_PIVOT_PROGRAM,
            UnifiedLLMTask.PROPOSE_NEW_ROOT,
            UnifiedLLMTask.RECOVER_ROOT_STAGNATION,
            UnifiedLLMTask.CLASSIFY_RESULT_CLUSTER,
        }:
            if not isinstance(context, ResearchCompilerContext):
                raise CompilerGateError("research task requires ResearchCompilerContext")
            if context.equivalent_unexecuted_program:
                raise CompilerGateError(
                    "equivalent deterministic program is unexecuted"
                )
            if not context.cooldown_satisfied:
                raise CompilerGateError("LLM cooldown is not satisfied")
            if (
                task is UnifiedLLMTask.COMPILE_ROOT_QUERY_PROGRAM
                and not context.seed_current_program_exhausted
            ):
                raise CompilerGateError(
                    "deterministic seed/current program is not exhausted"
                )

        if task in {
            UnifiedLLMTask.DISTILL_SUCCESS_MOTIF,
            UnifiedLLMTask.MUTATE_PRODUCTIVE_QUERY_FAMILY,
            UnifiedLLMTask.DISTILL_NEGATIVE_CLUSTER,
            UnifiedLLMTask.PROPOSE_ORTHOGONAL_ROOT,
            UnifiedLLMTask.INTERPRET_POLICY_DRIFT,
        }:
            if not isinstance(context, LearningCompilerContext):
                raise CompilerGateError("learning task requires LearningCompilerContext")
            if not context.learning_epoch_ready:
                raise CompilerGateError("learning epoch is not ready")
            if not context.minimum_batch_satisfied:
                raise CompilerGateError("minimum learning batch is not satisfied")
            if not context.replay_available:
                raise CompilerGateError("historical replay is unavailable")
            if not context.final_reward_available:
                raise CompilerGateError("FINAL delayed reward is unavailable")

    def _compile_proposal(
        self,
        proposal_type: ProposalType,
        raw: Mapping[str, Any],
        *,
        context: ResearchCompilerContext | LearningCompilerContext | None,
        context_hash: str,
    ):
        if proposal_type is ProposalType.EXPLORATION_REGION:
            return self._region(raw)
        if proposal_type is ProposalType.CONTRACT_FAMILY:
            return self._contract(raw)
        if proposal_type in {
            ProposalType.QUERY_PROGRAM,
            ProposalType.PIVOT_PROGRAM,
        }:
            return self._query_program(
                raw,
                pivot=proposal_type is ProposalType.PIVOT_PROGRAM,
                context=context,
                context_hash=context_hash,
            )
        if proposal_type is ProposalType.ROOT_SURFACE:
            return self._root_surface(raw)
        if proposal_type in _LEARNING_TYPES:
            return self._learning_rule(proposal_type, raw)
        raise UnifiedCompilerError("unsupported proposal type")

    def _strict(
        self,
        raw: Mapping[str, Any],
        *,
        allowed: set[str],
        required: set[str],
        name: str,
    ) -> None:
        unknown = set(raw) - allowed
        missing = required - set(raw)
        if unknown or missing:
            raise UnifiedCompilerError(
                f"{name} fields invalid missing={sorted(missing)} "
                f"extra={sorted(unknown)}"
            )

    def _bounds(self, raw: Any) -> dict[str, int]:
        value = self._mapping(raw, "hard_bounds")
        if not value:
            raise UnifiedCompilerError("hard_bounds must be non-empty")
        result: dict[str, int] = {}
        for key, bound in value.items():
            if (
                isinstance(bound, bool)
                or not isinstance(bound, int)
                or bound < 1
            ):
                raise UnifiedCompilerError(
                    "hard_bounds values must be positive integers"
                )
            result[key] = bound
        if result.get("max_requests", 1) > self.MAX_HARD_REQUESTS:
            raise UnifiedCompilerError(
                f"max_requests exceeds {self.MAX_HARD_REQUESTS}"
            )
        return result

    @staticmethod
    def _stops(raw: Any) -> tuple[str, ...]:
        if (
            not isinstance(raw, list)
            or not raw
            or any(not isinstance(item, str) or not item.strip() for item in raw)
        ):
            raise UnifiedCompilerError(
                "stop_conditions must be a non-empty string array"
            )
        return tuple(dict.fromkeys(item.strip() for item in raw))

    @staticmethod
    def _confidence(raw: Any) -> float:
        if (
            isinstance(raw, bool)
            or not isinstance(raw, (int, float))
            or not 0 <= raw <= 1
        ):
            raise UnifiedCompilerError("confidence must be between 0 and 1")
        return float(raw)

    def _region(self, raw: Mapping[str, Any]) -> ExplorationRegionProposal:
        fields = {
            "type",
            "proposal_id",
            "surface_kind",
            "root",
            "purpose",
            "query_family",
            "enumerator",
            "artifact_predicate",
            "hard_bounds",
            "stop_conditions",
            "expected_source_family",
            "expected_contract_family",
            "expected_mechanism",
            "expected_fanout",
            "reuse_key",
            "confidence",
            "validation",
        }
        self._strict(raw, allowed=fields, required=fields, name="region")
        fanout = raw["expected_fanout"]
        if (
            isinstance(fanout, bool)
            or not isinstance(fanout, int)
            or fanout < 1
            or fanout > self.MAX_TEMPLATE_EXPANSION
        ):
            raise UnifiedCompilerError(
                "expected_fanout must be within 1..4096"
            )
        for name in ("query_family", "artifact_predicate", "validation"):
            self._mapping(raw[name], name)
        return ExplorationRegionProposal(
            proposal_id=self._text(raw["proposal_id"], "proposal_id"),
            surface_kind=self._text(raw["surface_kind"], "surface_kind"),
            root=self._text(raw["root"], "root"),
            purpose=self._text(raw["purpose"], "purpose"),
            query_family=dict(raw["query_family"]),
            enumerator=self._text(raw["enumerator"], "enumerator"),
            artifact_predicate=dict(raw["artifact_predicate"]),
            hard_bounds=self._bounds(raw["hard_bounds"]),
            stop_conditions=self._stops(raw["stop_conditions"]),
            expected_source_family=self._text(
                raw["expected_source_family"], "expected_source_family"
            ),
            expected_contract_family=self._text(
                raw["expected_contract_family"], "expected_contract_family"
            ),
            expected_mechanism=self._text(
                raw["expected_mechanism"], "expected_mechanism"
            ),
            expected_fanout=fanout,
            reuse_key=self._text(raw["reuse_key"], "reuse_key"),
            confidence=self._confidence(raw["confidence"]),
            validation=dict(raw["validation"]),
        )

    def _contract(self, raw: Mapping[str, Any]) -> ContractFamilyProposal:
        fields = {
            "type",
            "proposal_id",
            "contract_family",
            "recognition",
            "extraction",
            "hard_bounds",
            "stop_conditions",
            "expected_mechanism",
            "reuse_key",
            "confidence",
            "validation",
        }
        self._strict(raw, allowed=fields, required=fields, name="contract proposal")
        recognition = self._mapping(raw["recognition"], "recognition")
        extraction = self._mapping(raw["extraction"], "extraction")
        validation = self._mapping(raw["validation"], "validation")
        return ContractFamilyProposal(
            proposal_id=self._text(raw["proposal_id"], "proposal_id"),
            contract_family=self._text(raw["contract_family"], "contract_family"),
            recognition=dict(recognition),
            extraction=dict(extraction),
            hard_bounds=self._bounds(raw["hard_bounds"]),
            stop_conditions=self._stops(raw["stop_conditions"]),
            expected_mechanism=self._text(
                raw["expected_mechanism"], "expected_mechanism"
            ),
            reuse_key=self._text(raw["reuse_key"], "reuse_key"),
            confidence=self._confidence(raw["confidence"]),
            validation=dict(validation),
        )

    def _root_query(
        self,
        raw: Any,
        *,
        context: ResearchCompilerContext | None,
    ) -> RootQuery:
        item = self._mapping(raw, "query")
        allowed = {
            "query",
            "filters",
            "expected_signal",
            "expected_family",
            "max_pages",
        }
        if set(item) - allowed:
            raise UnifiedCompilerError("query contains forbidden fields")
        query = self._text(item.get("query"), "query")
        filters = self._mapping(item.get("filters", {}), "filters")
        if set(filters) & _FORBIDDEN_FILTERS:
            raise UnifiedCompilerError("unsupported API filters")
        if context is not None and any(
            f"filter:{key}" not in context.root_capabilities for key in filters
        ):
            raise UnifiedCompilerError("unsupported root-native filter")
        pages = item.get("max_pages", 1)
        if (
            isinstance(pages, bool)
            or not isinstance(pages, int)
            or not 1 <= pages <= self.MAX_HARD_REQUESTS
        ):
            raise UnifiedCompilerError("max_pages must be within 1..80")
        parsed = urlparse(query)
        if parsed.scheme in {"http", "https"} and parsed.netloc and not filters:
            raise UnifiedCompilerError(
                "ordinary URL proposal is not a root query program"
            )
        return RootQuery(
            query=query,
            filters=dict(filters),
            expected_signal=str(item.get("expected_signal", "")).strip(),
            expected_family=str(item.get("expected_family", "")).strip(),
            max_pages=pages,
        )

    def _query_program(
        self,
        raw: Mapping[str, Any],
        *,
        pivot: bool,
        context: ResearchCompilerContext | LearningCompilerContext | None,
        context_hash: str,
    ) -> QueryProgramProposal | PivotProgramProposal:
        fields = {
            "type",
            "proposal_id",
            "root_id",
            "strategy",
            "queries",
            "hard_max_requests",
            "stop_conditions",
            "reuse_key",
        }
        self._strict(raw, allowed=fields, required=fields, name="query program")
        if not isinstance(context, ResearchCompilerContext):
            raise UnifiedCompilerError(
                "query programs require ResearchCompilerContext"
            )
        if raw["root_id"] != context.root_id:
            raise UnifiedCompilerError("program root_id does not match context")
        queries = raw["queries"]
        if (
            not isinstance(queries, list)
            or not queries
            or len(queries) > self.MAX_QUERIES
        ):
            raise UnifiedCompilerError(
                "queries must contain 1..80 finite query objects"
            )
        seen = set(context.recent_query_hashes)
        compiled_queries = tuple(
            query
            for query in (
                self._root_query(item, context=context) for item in queries
            )
            if query.query_hash not in seen
        )
        if not compiled_queries:
            raise UnifiedCompilerError(
                "all proposed queries duplicate recent deterministic work"
            )
        hard_max = raw["hard_max_requests"]
        if (
            isinstance(hard_max, bool)
            or not isinstance(hard_max, int)
            or not 1 <= hard_max <= self.MAX_HARD_REQUESTS
        ):
            raise UnifiedCompilerError(
                "hard_max_requests must be within 1..80"
            )
        cls = PivotProgramProposal if pivot else QueryProgramProposal
        return cls(
            proposal_id=self._text(raw["proposal_id"], "proposal_id"),
            root_id=self._text(raw["root_id"], "root_id"),
            strategy=self._text(raw["strategy"], "strategy"),
            queries=compiled_queries,
            hard_max_requests=hard_max,
            stop_conditions=self._stops(raw["stop_conditions"]),
            reuse_key=self._text(raw["reuse_key"], "reuse_key"),
            context_hash=context_hash,
        )

    def _root_surface(self, raw: Mapping[str, Any]) -> RootSurfaceProposal:
        fields = {
            "type",
            "proposal_id",
            "kind",
            "entrypoint",
            "capabilities",
            "rationale",
            "hard_bounds",
            "stop_conditions",
            "reuse_key",
            "confidence",
        }
        self._strict(raw, allowed=fields, required=fields, name="root surface")
        caps = raw["capabilities"]
        if (
            not isinstance(caps, list)
            or not caps
            or any(not isinstance(item, str) or not item.strip() for item in caps)
        ):
            raise UnifiedCompilerError(
                "root capabilities must be non-empty strings"
            )
        entrypoint = self._text(raw["entrypoint"], "entrypoint")
        parsed = urlparse(entrypoint)
        if parsed.scheme in {"http", "https"} and parsed.path in {"", "/"} and len(caps) < 2:
            raise UnifiedCompilerError(
                "ordinary URL proposal is not a reusable root surface"
            )
        return RootSurfaceProposal(
            proposal_id=self._text(raw["proposal_id"], "proposal_id"),
            kind=self._text(raw["kind"], "kind"),
            entrypoint=entrypoint,
            capabilities=tuple(item.strip() for item in caps),
            rationale=self._text(raw["rationale"], "rationale"),
            hard_bounds=self._bounds(raw["hard_bounds"]),
            stop_conditions=self._stops(raw["stop_conditions"]),
            reuse_key=self._text(raw["reuse_key"], "reuse_key"),
            confidence=self._confidence(raw["confidence"]),
        )

    def _learning_rule(
        self,
        proposal_type: ProposalType,
        raw: Mapping[str, Any],
    ):
        fields = {
            "type",
            "proposal_id",
            "generalization_scope",
            "preconditions",
            "bounded_expansion",
            "hard_bounds",
            "stop_conditions",
            "expected_mechanism",
            "failure_modes",
            "reuse_key",
            "confidence",
        }
        self._strict(raw, allowed=fields, required=fields, name="learning rule")
        scope = self._mapping(raw["generalization_scope"], "generalization_scope")
        preconditions = self._mapping(raw["preconditions"], "preconditions")
        expansion = self._mapping(raw["bounded_expansion"], "bounded_expansion")
        failure_modes = raw["failure_modes"]
        if (
            not isinstance(failure_modes, list)
            or not failure_modes
            or any(
                not isinstance(item, str) or not item.strip()
                for item in failure_modes
            )
        ):
            raise UnifiedCompilerError(
                "failure_modes must contain non-empty strings"
            )
        classes = {
            ProposalType.QUERY_FAMILY: QueryFamilyProposal,
            ProposalType.PIVOT_RULE: PivotRuleProposal,
            ProposalType.NEGATIVE_RULE: NegativeRuleProposal,
            ProposalType.FAMILY_RECOGNIZER: FamilyRecognizerProposal,
        }
        cls = classes[proposal_type]
        return cls(
            proposal_id=self._text(raw["proposal_id"], "proposal_id"),
            generalization_scope=dict(scope),
            preconditions=dict(preconditions),
            bounded_expansion=dict(expansion),
            hard_bounds=self._bounds(raw["hard_bounds"]),
            stop_conditions=self._stops(raw["stop_conditions"]),
            expected_mechanism=self._text(
                raw["expected_mechanism"], "expected_mechanism"
            ),
            failure_modes=tuple(item.strip() for item in failure_modes),
            reuse_key=self._text(raw["reuse_key"], "reuse_key"),
            confidence=self._confidence(raw["confidence"]),
        )


ModelCall = Callable[[ResearchCompilerContext], Mapping[str, Any]]


class RootQueryCompiler:
    """V7.1-compatible facade backed by the same L6 validation rules."""

    MAX_QUERIES = 80
    MAX_PROGRAMS = 8
    MAX_HARD_REQUESTS = 80

    def __init__(self, model_call: ModelCall) -> None:
        self.model_call = model_call
        self._compiler = UnifiedResearchCompiler()

    @staticmethod
    def _check_call_gate(context: ResearchCompilerContext) -> None:
        if context.equivalent_unexecuted_program:
            raise CompilerGateError(
                "equivalent deterministic program is unexecuted"
            )
        if not context.cooldown_satisfied:
            raise CompilerGateError("LLM cooldown is not satisfied")

    def _check_gate(self, context: ResearchCompilerContext) -> None:
        self._check_call_gate(context)
        if not context.seed_current_program_exhausted:
            raise CompilerGateError(
                "deterministic seed/current program is not exhausted"
            )

    @staticmethod
    def _mapping(value: Any, name: str) -> Mapping[str, Any]:
        try:
            return UnifiedResearchCompiler._mapping(value, name)
        except UnifiedCompilerError as exc:
            raise RootQueryCompilerError(str(exc)) from exc

    def _model_payload(
        self, context: ResearchCompilerContext
    ) -> Mapping[str, Any]:
        payload = self._mapping(self.model_call(context), "model response")
        try:
            UnifiedResearchCompiler._reject_forbidden_keys(
                payload, "model response"
            )
        except UnifiedCompilerError as exc:
            raise RootQueryCompilerError(str(exc)) from exc
        return payload

    def _query(
        self, raw: Any, context: ResearchCompilerContext
    ) -> RootQuery:
        try:
            return self._compiler._root_query(raw, context=context)
        except UnifiedCompilerError as exc:
            raise RootQueryCompilerError(str(exc)) from exc

    def _compile_legacy_program(
        self,
        raw: Mapping[str, Any],
        context: ResearchCompilerContext,
    ) -> RootQueryProgram:
        allowed = {
            "root_id",
            "strategy",
            "queries",
            "hard_max_requests",
            "stop_conditions",
        }
        if set(raw) - allowed:
            raise RootQueryCompilerError("program contains forbidden fields")
        if raw.get("root_id") != context.root_id:
            raise RootQueryCompilerError(
                "program root_id does not match context"
            )
        strategy = raw.get("strategy")
        if not isinstance(strategy, str) or not strategy.strip():
            raise RootQueryCompilerError("strategy must be non-empty")
        queries = raw.get("queries")
        if (
            not isinstance(queries, list)
            or not queries
            or len(queries) > self.MAX_QUERIES
        ):
            raise RootQueryCompilerError(
                "program queries must be finite and non-empty"
            )
        normalized = tuple(self._query(item, context) for item in queries)
        bounds = raw.get("hard_max_requests")
        if (
            isinstance(bounds, bool)
            or not isinstance(bounds, int)
            or not 1 <= bounds <= self.MAX_HARD_REQUESTS
        ):
            raise RootQueryCompilerError(
                "hard_max_requests must be within 1..80"
            )
        stops = raw.get("stop_conditions")
        try:
            stop_conditions = self._compiler._stops(stops)
        except UnifiedCompilerError as exc:
            raise RootQueryCompilerError(str(exc)) from exc
        return RootQueryProgram(
            root_id=context.root_id,
            strategy=strategy.strip(),
            queries=normalized,
            hard_max_requests=bounds,
            stop_conditions=stop_conditions,
            context_hash=context.context_hash,
        )

    def compile_root_query_program(
        self, context: ResearchCompilerContext
    ) -> RootQueryProgram:
        self._check_gate(context)
        payload = self._model_payload(context)
        programs = payload.get("programs")
        if (
            not isinstance(programs, list)
            or not programs
            or len(programs) > self.MAX_PROGRAMS
        ):
            raise RootQueryCompilerError(
                "programs must contain 1..8 items"
            )
        program = self._compile_legacy_program(
            self._mapping(programs[0], "program"), context
        )
        recent = set(context.recent_query_hashes)
        filtered = tuple(
            query for query in program.queries if query.query_hash not in recent
        )
        if not filtered:
            raise RootQueryCompilerError(
                "all proposed queries duplicate recent deterministic work"
            )
        return RootQueryProgram(
            root_id=program.root_id,
            strategy=program.strategy,
            queries=filtered,
            hard_max_requests=program.hard_max_requests,
            stop_conditions=program.stop_conditions,
            context_hash=program.context_hash,
        )

    def _call(
        self, context: ResearchCompilerContext
    ) -> Mapping[str, Any]:
        self._check_call_gate(context)
        return self._model_payload(context)

    def classify_result_cluster(
        self,
        context: ResearchCompilerContext,
        cluster: Mapping[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        cluster_id = cluster.get("cluster_id")
        size = cluster.get("size")
        if not isinstance(cluster_id, str) or not cluster_id.strip():
            raise RootQueryCompilerError("cluster_id is required")
        if isinstance(size, bool) or not isinstance(size, int) or size < 2:
            raise RootQueryCompilerError(
                "classification requires a result cluster"
            )
        payload = self._call(context)
        items = payload.get("classifications")
        if not isinstance(items, list) or not items:
            raise RootQueryCompilerError(
                "classifications must be a non-empty array"
            )
        allowed = {
            "cluster_id",
            "classification",
            "reusable_surface",
            "rationale",
        }
        result = []
        for item in items:
            value = self._mapping(item, "classification")
            if set(value) - allowed:
                raise RootQueryCompilerError(
                    "classification contains forbidden fields"
                )
            if value.get("cluster_id") != cluster_id:
                raise RootQueryCompilerError(
                    "classification cluster_id mismatch"
                )
            label = value.get("classification")
            if not isinstance(label, str) or not label.strip():
                raise RootQueryCompilerError(
                    "classification label is required"
                )
            result.append(dict(value))
        return tuple(result)

    def compile_pivot_program(
        self, context: ResearchCompilerContext
    ) -> tuple[RootQueryProgram, ...]:
        payload = self._call(context)
        raw = payload.get("pivot_programs")
        if not isinstance(raw, list) or len(raw) > self.MAX_PROGRAMS:
            raise RootQueryCompilerError(
                "pivot_programs must be an array of at most 8 items"
            )
        return tuple(
            self._compile_legacy_program(
                self._mapping(item, "pivot program"), context
            )
            for item in raw
        )

    def propose_new_root(
        self, context: ResearchCompilerContext
    ) -> tuple[dict[str, Any], ...]:
        payload = self._call(context)
        raw = payload.get("new_root_hypotheses")
        if not isinstance(raw, list) or len(raw) > self.MAX_PROGRAMS:
            raise RootQueryCompilerError(
                "new_root_hypotheses must be an array of at most 8 items"
            )
        result = []
        for item in raw:
            value = self._mapping(item, "new root hypothesis")
            allowed = {"kind", "entrypoint", "capabilities", "rationale"}
            if set(value) != allowed:
                raise RootQueryCompilerError(
                    "new root hypothesis has invalid fields"
                )
            entrypoint = value.get("entrypoint")
            capabilities = value.get("capabilities")
            if not isinstance(entrypoint, str) or not entrypoint.strip():
                raise RootQueryCompilerError(
                    "new root entrypoint is required"
                )
            if (
                not isinstance(capabilities, list)
                or not capabilities
                or any(
                    not isinstance(item, str) or not item.strip()
                    for item in capabilities
                )
            ):
                raise RootQueryCompilerError(
                    "new root capabilities are required"
                )
            parsed = urlparse(entrypoint)
            if parsed.path in {"", "/"} and len(capabilities) < 2:
                raise RootQueryCompilerError(
                    "ordinary URL proposal is not a reusable root"
                )
            result.append(dict(value))
        return tuple(result)

    def recover_root_stagnation(
        self, context: ResearchCompilerContext
    ) -> RootQueryProgram:
        payload = self._call(context)
        recovery = payload.get("recovery_program")
        if recovery is None:
            raise RootQueryCompilerError("recovery_program is required")
        return self._compile_legacy_program(
            self._mapping(recovery, "recovery program"), context
        )


LearningModelCall = Callable[[LearningCompilerContext], Mapping[str, Any]]


class PolicyDistillerCompiler:
    """Compile policy proposals only; promotion/activation remain outside L6."""

    def __init__(self, model_call: LearningModelCall) -> None:
        self.model_call = model_call
        self.compiler = UnifiedResearchCompiler()

    def compile(
        self,
        context: LearningCompilerContext,
        *,
        task_type: UnifiedLLMTask,
    ) -> ProposalEnvelope:
        if task_type not in {
            UnifiedLLMTask.DISTILL_SUCCESS_MOTIF,
            UnifiedLLMTask.MUTATE_PRODUCTIVE_QUERY_FAMILY,
            UnifiedLLMTask.DISTILL_NEGATIVE_CLUSTER,
            UnifiedLLMTask.PROPOSE_ORTHOGONAL_ROOT,
            UnifiedLLMTask.INTERPRET_POLICY_DRIFT,
        }:
            raise UnifiedCompilerError("not a policy-distillation task")
        return self.compiler.compile(
            self.model_call(context),
            task_type=task_type,
            context=context,
        )
