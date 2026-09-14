"""L9 composition bridge for Creeper research/discovery/production planes.

This module contains integration-only glue.  It does not create a second
authority: both registries must share the same ControlStore SQLite connection.
Root metadata and LLM output remain proposal/discovery state only; FINAL reward
is projected only from validation-closed production exposures.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from creeper.source_discovery.artifact_intake import (
    MetadataArtifactAdmission,
    assess_artifact_metadata,
)
from creeper.source_discovery.models import (
    SourceCandidate,
    SourceLevel,
    is_direct_evidence_entrypoint,
)
from creeper.source_discovery.region_compilation import compile_region
from creeper.source_discovery.research_compiler import ResearchCompiler
from creeper.source_discovery.research_models import ExplorationRegion, RegionState
from creeper.source_discovery.research_trigger import ResearchDirective
from creeper.source_research.agent.context import (
    ResearchCompilerContext,
    UnifiedCompilerRequest,
)
from creeper.source_research.agent.protocol import (
    ContractFamilyProposal,
    PivotProgramProposal,
    ProposalEnvelope,
    ProposalPlane,
    QueryProgramProposal,
    RootSurfaceProposal,
    UnifiedLLMTask,
)
from creeper.source_research.feedback import ResearchFeedback
from creeper.source_research.models import (
    ArtifactLead,
    QueryProgram,
    QueryState,
    RootKind,
    RootQuery,
    RootSurface,
    RuleRecord,
    RuleState,
    SearchHit,
    stable_hash,
)
from creeper.source_research.registry import ResearchRegistry
from creeper.source_research.resolver import resolve_node


@dataclass(frozen=True)
class IntegratedResearchResult:
    call_identity: str
    envelope: ProposalEnvelope | None
    suppressed: bool = False
    typed_context: ResearchCompilerContext | None = None


@dataclass(frozen=True)
class RootPageResult:
    query_id: str
    hits: int
    artifacts: int
    sources_inserted: int
    terminal: bool
    retryable: bool
    metadata_accepted: int = 0
    metadata_held: int = 0
    metadata_rejected: int = 0


class ResearchIntegrationBridge:
    """Join L1/L2/L3/L4-L6/L8 and production without merging authorities."""

    PROMPT_VERSION = "unified-research-compiler-v1"

    def __init__(
        self,
        research: ResearchRegistry,
        discovery: Any,
        *,
        clock=time.time,
        llm_claim_stale_seconds: float = 900.0,
    ) -> None:
        if research.connection is not discovery.connection:
            raise ValueError(
                "research and discovery must share one ControlStore connection"
            )
        if llm_claim_stale_seconds <= 0:
            raise ValueError("llm_claim_stale_seconds must be positive")
        self.research = research
        self.discovery = discovery
        self.connection = research.connection
        self.clock = clock
        self.llm_claim_stale_seconds = float(llm_claim_stale_seconds)
        self.feedback = ResearchFeedback(research)
        self.execution_compiler = ResearchCompiler()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self.connection:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS research_llm_call_claims(
                    call_identity TEXT PRIMARY KEY,
                    context_hash TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    prompt_version TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('RUNNING','COMPLETE','FAILED')),
                    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                    started_at REAL NOT NULL,
                    finished_at REAL,
                    cost_seconds REAL NOT NULL DEFAULT 0 CHECK(cost_seconds >= 0),
                    last_error TEXT NOT NULL DEFAULT ''
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_research_llm_call_context
                    ON research_llm_call_claims(context_hash, task_type, state);

                CREATE TABLE IF NOT EXISTS research_artifact_prefilter(
                    artifact_identity TEXT NOT NULL,
                    query_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    locator TEXT NOT NULL,
                    admission TEXT NOT NULL,
                    format_kind TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    semantic_hits_json TEXT NOT NULL,
                    evaluated_at REAL NOT NULL,
                    PRIMARY KEY(artifact_identity,query_id,node_id)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_research_artifact_prefilter_admission
                    ON research_artifact_prefilter(admission, query_id, evaluated_at);

                CREATE TABLE IF NOT EXISTS research_final_projection(
                    source_key TEXT NOT NULL,
                    exposure_id TEXT NOT NULL,
                    baseline_signature TEXT NOT NULL,
                    model_signature TEXT NOT NULL,
                    final_eed REAL NOT NULL CHECK(final_eed >= 0),
                    rewards_written INTEGER NOT NULL DEFAULT 0 CHECK(rewards_written >= 0),
                    synced_at REAL NOT NULL,
                    PRIMARY KEY(
                        source_key, exposure_id, baseline_signature, model_signature
                    )
                ) WITHOUT ROWID;
                """
            )

    def llm_gate_state(
        self,
        context_hash: str,
    ) -> tuple[str | None, float | None, int]:
        """Recover persistent L8 concurrency/cooldown facts after restart."""
        now = float(self.clock())
        stale_before = now - self.llm_claim_stale_seconds
        active = self.connection.execute(
            """
            SELECT call_identity
            FROM research_llm_call_claims
            WHERE state='RUNNING' AND started_at>?
            ORDER BY started_at DESC, call_identity
            LIMIT 1
            """,
            (stale_before,),
        ).fetchone()
        last = self.connection.execute(
            "SELECT MAX(started_at) AS started_at FROM research_llm_call_claims"
        ).fetchone()
        failures = self.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM research_llm_call_claims
            WHERE context_hash=? AND state='FAILED'
            """,
            (context_hash,),
        ).fetchone()
        return (
            None if active is None else str(active["call_identity"]),
            (
                None
                if last is None or last["started_at"] is None
                else float(last["started_at"])
            ),
            0 if failures is None else int(failures["n"]),
        )

    @staticmethod
    def build_execution_request(directive: ResearchDirective) -> UnifiedCompilerRequest:
        task = UnifiedLLMTask(directive.task_type)
        return UnifiedCompilerRequest(
            task_type=task,
            plane=ProposalPlane.EXECUTION,
            trigger_reason=directive.trigger_reason.value,
            objective=directive.reason,
            context={
                "strategy": directive.strategy,
                "subject": directive.subject,
                "desired_regions": directive.desired_regions,
                "authority": "proposal_only",
            },
            context_hash=directive.context_key,
        )

    def build_root_research_context(
        self,
        root_id: str,
        *,
        cooldown_satisfied: bool,
    ) -> ResearchCompilerContext:
        root = self.research.get_root(root_id)
        rows = self.connection.execute(
            """
            SELECT *
            FROM research_queries
            WHERE root_id=?
            ORDER BY updated_at DESC, query_id DESC
            LIMIT 64
            """,
            (root_id,),
        ).fetchall()
        active_states = {"READY", "RUNNING", "RETRYABLE"}
        equivalent_unexecuted = any(
            str(row["state"]) in active_states for row in rows
        )
        exhausted = bool(rows) and not equivalent_unexecuted
        metrics_available = any(
            str(row["state"]) in {"COMPLETE", "EXHAUSTED"}
            for row in rows
        )
        recent_hashes: list[str] = []
        for row in rows:
            query = RootQuery(
                root_id=str(row["root_id"]),
                query_text=str(row["query_text"]),
                max_pages=int(row["max_pages"]),
                max_wall_seconds=float(row["max_wall_seconds"]),
                page_size=int(row["page_size"]),
                native_filters=json.loads(str(row["native_filters_json"]) or "{}"),
                expected_signal=str(row["expected_signal"]),
                expected_artifact_family=str(row["expected_artifact_family"]),
                query_id=str(row["query_id"]),
            )
            recent_hashes.append(query.query_hash)
        negatives = self.connection.execute(
            """
            SELECT reason
            FROM research_negative_knowledge
            WHERE root_id=?
            ORDER BY last_seen_at DESC
            LIMIT 32
            """,
            (root_id,),
        ).fetchall()
        return ResearchCompilerContext(
            root_id=root.root_id,
            root_capabilities=root.capabilities,
            seed_current_program_exhausted=exhausted,
            equivalent_unexecuted_program=equivalent_unexecuted,
            cooldown_satisfied=bool(cooldown_satisfied),
            deterministic_seed_search_available=True,
            metrics_available=metrics_available,
            recent_query_hashes=tuple(recent_hashes),
            negative_knowledge=tuple(str(row["reason"]) for row in negatives),
        )

    @staticmethod
    def build_root_research_request(
        context: ResearchCompilerContext,
        *,
        task_type: UnifiedLLMTask = UnifiedLLMTask.COMPILE_ROOT_QUERY_PROGRAM,
        trigger_reason: str = "ROOT_PROGRAM_EXHAUSTED",
    ) -> UnifiedCompilerRequest:
        return UnifiedCompilerRequest(
            task_type=task_type,
            plane=ProposalPlane.RESEARCH,
            trigger_reason=trigger_reason,
            objective=(
                "propose a bounded orthogonal structured-root program that "
                "maximizes marginal FINAL accepted Novel EED per total cost"
            ),
            context=context.as_prompt_payload(),
            context_hash=context.context_hash,
        )

    def try_claim_llm_call(self, request: UnifiedCompilerRequest) -> bool:
        now = float(self.clock())
        stale_before = now - self.llm_claim_stale_seconds
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """
                SELECT state, started_at
                FROM research_llm_call_claims
                WHERE call_identity = ?
                """,
                (request.call_identity,),
            ).fetchone()
            if row is not None:
                state = str(row["state"])
                if state == "COMPLETE":
                    self.connection.commit()
                    return False
                if state == "RUNNING" and float(row["started_at"]) > stale_before:
                    self.connection.commit()
                    return False
                self.connection.execute(
                    """
                    UPDATE research_llm_call_claims
                    SET context_hash=?, task_type=?, prompt_version=?,
                        state='RUNNING', attempts=attempts+1, started_at=?,
                        finished_at=NULL, cost_seconds=0, last_error=''
                    WHERE call_identity=?
                    """,
                    (
                        request.context_hash,
                        request.task_type.value,
                        request.prompt_version,
                        now,
                        request.call_identity,
                    ),
                )
            else:
                self.connection.execute(
                    """
                    INSERT INTO research_llm_call_claims(
                        call_identity,context_hash,task_type,prompt_version,state,
                        attempts,started_at
                    ) VALUES(?,?,?,?,'RUNNING',1,?)
                    """,
                    (
                        request.call_identity,
                        request.context_hash,
                        request.task_type.value,
                        request.prompt_version,
                        now,
                    ),
                )
            self.connection.commit()
            return True
        except BaseException:
            self.connection.rollback()
            raise

    def finish_llm_call(
        self,
        call_identity: str,
        *,
        success: bool,
        cost_seconds: float,
        error: str = "",
    ) -> None:
        if cost_seconds < 0:
            raise ValueError("LLM cost_seconds must be non-negative")
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE research_llm_call_claims
                SET state=?, finished_at=?, cost_seconds=?, last_error=?
                WHERE call_identity=? AND state='RUNNING'
                """,
                (
                    "COMPLETE" if success else "FAILED",
                    float(self.clock()),
                    float(cost_seconds),
                    error[:1000],
                    call_identity,
                ),
            ).rowcount
        if changed != 1:
            row = self.connection.execute(
                "SELECT state FROM research_llm_call_claims WHERE call_identity=?",
                (call_identity,),
            ).fetchone()
            if row is None or (success and str(row["state"]) != "COMPLETE"):
                raise KeyError(f"unknown/non-running LLM call: {call_identity}")

    async def execute_unified(
        self,
        directive: ResearchDirective,
        executor: Any,
    ) -> IntegratedResearchResult:
        request = self.build_execution_request(directive)
        if not self.try_claim_llm_call(request):
            return IntegratedResearchResult(
                call_identity=request.call_identity,
                envelope=None,
                suppressed=True,
            )
        started = time.perf_counter()
        try:
            envelope = await executor(request)
        except BaseException as exc:
            self.finish_llm_call(
                request.call_identity,
                success=False,
                cost_seconds=max(0.0, time.perf_counter() - started),
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        return IntegratedResearchResult(
            call_identity=request.call_identity,
            envelope=envelope,
        )

    async def execute_root_research(
        self,
        root_id: str,
        executor: Any,
        *,
        cooldown_satisfied: bool = True,
    ) -> IntegratedResearchResult:
        context = self.build_root_research_context(
            root_id,
            cooldown_satisfied=cooldown_satisfied,
        )
        request = self.build_root_research_request(context)
        if not self.try_claim_llm_call(request):
            return IntegratedResearchResult(
                call_identity=request.call_identity,
                envelope=None,
                suppressed=True,
                typed_context=context,
            )
        started = time.perf_counter()
        try:
            envelope = await executor(request, context=context)
        except BaseException as exc:
            self.finish_llm_call(
                request.call_identity,
                success=False,
                cost_seconds=max(0.0, time.perf_counter() - started),
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        return IntegratedResearchResult(
            call_identity=request.call_identity,
            envelope=envelope,
            typed_context=context,
        )

    @staticmethod
    def _root_kind_from_proposal(value: str) -> RootKind:
        text = str(value).strip().upper()
        if "OAI" in text:
            return RootKind.OAI
        if "CODE" in text or "GITHUB" in text:
            return RootKind.CODE
        if "ARCHIVE" in text:
            return RootKind.ARCHIVE
        if any(token in text for token in ("REPOSITORY", "API", "CATALOG", "DATA")):
            return RootKind.STRUCTURED_REPOSITORY
        return RootKind.GENERIC

    def commit_root_research_result(
        self,
        result: IntegratedResearchResult,
        *,
        elapsed_seconds: float,
    ) -> int:
        if result.suppressed:
            return 0
        envelope = result.envelope
        context = result.typed_context
        if envelope is None or context is None:
            raise ValueError("root research result requires envelope and typed context")
        if envelope.task_type not in {
            UnifiedLLMTask.COMPILE_ROOT_QUERY_PROGRAM,
            UnifiedLLMTask.COMPILE_PIVOT_PROGRAM,
            UnifiedLLMTask.PROPOSE_NEW_ROOT,
            UnifiedLLMTask.RECOVER_ROOT_STAGNATION,
            UnifiedLLMTask.CLASSIFY_RESULT_CLUSTER,
        }:
            raise ValueError("root research result has non-RESEARCH task type")
        committed = 0
        try:
            for proposal in envelope.proposals:
                if isinstance(proposal, (QueryProgramProposal, PivotProgramProposal)):
                    queries = tuple(
                        RootQuery(
                            root_id=proposal.root_id,
                            query_text=item.query,
                            native_filters=dict(item.filters),
                            expected_signal=item.expected_signal,
                            expected_artifact_family=item.expected_family,
                            max_pages=item.max_pages,
                            max_wall_seconds=60.0,
                            page_size=100,
                            seed_library_version="integrated-l6-v1",
                        )
                        for item in proposal.queries
                    )
                    self.research.register_program(
                        QueryProgram(
                            root_id=proposal.root_id,
                            strategy=proposal.strategy,
                            queries=queries,
                            hard_max_requests=proposal.hard_max_requests,
                            stop_conditions=proposal.stop_conditions,
                            compiler_version="integrated-l6-v1",
                            context_hash=proposal.context_hash,
                            program_id=proposal.program_id,
                            source=(
                                "LLM_PIVOT"
                                if isinstance(proposal, PivotProgramProposal)
                                else "LLM_RESEARCH"
                            ),
                        )
                    )
                    committed += 1
                elif isinstance(proposal, RootSurfaceProposal):
                    root_id = stable_hash(
                        "root-proposal",
                        proposal.reuse_key,
                        proposal.entrypoint,
                    )
                    self.research.upsert_root(
                        RootSurface(
                            root_id=root_id,
                            kind=self._root_kind_from_proposal(proposal.kind),
                            canonical_locator=proposal.entrypoint,
                            capabilities=proposal.capabilities,
                            metadata={
                                "rationale": proposal.rationale,
                                "reuse_key": proposal.reuse_key,
                                "confidence": proposal.confidence,
                                "authority": "proposal_only",
                                "created_by_call": result.call_identity,
                            },
                        )
                    )
                    committed += 1
                else:
                    raise ValueError(
                        "unsupported RESEARCH-plane proposal in root commit"
                    )
        except BaseException as exc:
            self.finish_llm_call(
                result.call_identity,
                success=False,
                cost_seconds=max(0.0, elapsed_seconds),
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        self.finish_llm_call(
            result.call_identity,
            success=True,
            cost_seconds=max(0.0, elapsed_seconds),
        )
        return committed

    def commit_execution_result(
        self,
        directive: ResearchDirective,
        result: IntegratedResearchResult,
        elapsed_seconds: float,
    ) -> None:
        if result.suppressed:
            return
        envelope = result.envelope
        if envelope is None:
            raise ValueError("non-suppressed research result requires an envelope")
        if envelope.task_type.value != directive.task_type:
            raise ValueError("research result task type does not match directive")
        try:
            regions, contracts = self.execution_compiler.compile_execution_envelope(
                envelope,
                episode_id=result.call_identity,
            )
            for plan in regions:
                region = ExplorationRegion(
                    region_id=stable_hash(
                        "region-proposal",
                        plan.proposal_id,
                        plan.context_hash,
                        plan.root,
                        plan.reuse_key,
                    ),
                    surface_kind=plan.surface_kind.value,
                    root=plan.root,
                    purpose=plan.purpose or directive.reason,
                    query_family_json=json.dumps(
                        plan.query_family,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    enumerator_spec_json=json.dumps(
                        plan.enumerator_spec,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    artifact_predicate_json=json.dumps(
                        plan.artifact_predicate,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    hard_bounds_json=json.dumps(
                        plan.hard_bounds,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    stop_conditions_json=json.dumps(list(plan.stop_conditions)),
                    expected_source_family=plan.expected_source_family,
                    expected_contract_family=plan.expected_contract_family,
                    context_hash=plan.context_hash or directive.context_key,
                    created_by_episode_id=result.call_identity,
                    state=RegionState.PROPOSED,
                )
                # Parent-side L1 compilation is the final executability gate.
                compile_region(region)
                stored, inserted = self.discovery.register_region(region)
                if inserted:
                    stored = self.discovery.transition_region(
                        stored.region_id,
                        RegionState.VALIDATED,
                        reason="validated L6 bounded proposal",
                    )
                    self.discovery.transition_region(
                        stored.region_id,
                        RegionState.READY,
                        reason="ready for deterministic execution",
                    )

            for plan in contracts:
                payload = {
                    "contract_family": plan.contract_family,
                    "recognition": plan.recognition,
                    "extraction": plan.extraction,
                    "hard_bounds": plan.hard_bounds,
                    "stop_conditions": list(plan.stop_conditions),
                    "expected_mechanism": plan.expected_mechanism,
                    "reuse_key": plan.reuse_key,
                    "confidence": plan.confidence,
                    "validation": plan.validation,
                    "blocked_subject": directive.subject or "",
                    "trigger_reason": directive.trigger_reason.value,
                    "research_strategy": directive.strategy,
                    "call_identity": result.call_identity,
                    "authority": "proposal_only",
                }
                self.research.upsert_rule(
                    RuleRecord(
                        rule_id=stable_hash(
                            "contract-rule",
                            plan.contract_family,
                            plan.reuse_key,
                            plan.context_hash,
                        ),
                        rule_kind="CONTRACT_FAMILY",
                        version="integrated-l6-v1",
                        context_hash=plan.context_hash or directive.context_key,
                        deterministic_payload=payload,
                        state=RuleState.CANDIDATE,
                        validation_digest=stable_hash(
                            "contract-validation", plan.validation
                        ),
                    )
                )
        except BaseException as exc:
            self.finish_llm_call(
                result.call_identity,
                success=False,
                cost_seconds=max(0.0, elapsed_seconds),
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        self.finish_llm_call(
            result.call_identity,
            success=True,
            cost_seconds=max(0.0, elapsed_seconds),
        )

    @staticmethod
    def _kernel_lead(
        lead: ArtifactLead,
        *,
        node_id: str,
        query_id: str,
        program_id: str,
        pivot_id: str,
        decision_id: str,
    ) -> ArtifactLead:
        return ArtifactLead(
            root_id=lead.root_id,
            provider_native_id=lead.provider_native_id,
            locator=lead.locator,
            content_type=lead.content_type,
            size=lead.size,
            checksum=lead.checksum,
            persistent_id=lead.persistent_id,
            parent_persistent_id=lead.parent_persistent_id,
            kind=lead.kind,
            immutable_identity=lead.immutable_identity,
            source_node_id=node_id,
            query_id=query_id,
            program_id=program_id,
            pivot_id=pivot_id,
            decision_id=decision_id,
        )

    def register_artifact_source(
        self,
        lead: ArtifactLead,
        *,
        node_id: str,
        query: RootQuery,
        program_id: str,
        pivot_id: str = "",
        decision_id: str = "",
    ) -> tuple[str, SourceCandidate, bool]:
        if lead.root_id != query.root_id:
            raise ValueError("artifact/query root mismatch")
        kernel = self._kernel_lead(
            lead,
            node_id=node_id,
            query_id=query.query_id,
            program_id=program_id,
            pivot_id=pivot_id,
            decision_id=decision_id,
        )
        artifact_id, _lineage_id, _ = self.research.register_artifact_lead(kernel)

        from creeper.source_discovery.artifact_intake import classify_artifact

        format_kind, _compression = classify_artifact(
            kernel.locator,
            kernel.content_type or None,
        )
        family = (
            query.expected_artifact_family.strip()
            or (format_kind if format_kind != "UNKNOWN" else "ARTIFACT")
        )
        direct = is_direct_evidence_entrypoint(kernel.locator)
        candidate = SourceCandidate(
            canonical_entrypoint=kernel.locator,
            source_family=family,
            level=SourceLevel.SOURCE,
            discovered_by=f"research-root:{kernel.root_id}",
            discovery_strategy="structured_root_artifact",
            temporal_semantics_prior=0.0,
            enumerability_prior=0.0,
            direct_evidence_prior=1.0 if direct else 0.0,
            baseline_overlap_prior=0.5,
            access_cost_prior=1.0,
            adapter_cost_prior=0.0,
            confidence=1.0,
        )
        stored, inserted = self.discovery.register_proposal(candidate)
        self.research.bind_artifact_source(
            artifact_id,
            source_key=stored.source_key,
        )
        return artifact_id, stored, inserted

    def _prefilter_artifact_lead(
        self,
        lead: ArtifactLead,
        *,
        node: Any,
        query: RootQuery,
    ) -> MetadataArtifactAdmission:
        assessment = assess_artifact_metadata(
            locator=lead.locator,
            content_type=lead.content_type or None,
            filename=lead.provider_native_id,
            title=str(getattr(node, "title", "") or ""),
            description=str(getattr(node, "description", "") or ""),
            metadata=dict(getattr(node, "metadata", {}) or {}),
            expected_artifact_family=query.expected_artifact_family,
        )
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO research_artifact_prefilter(
                    artifact_identity,query_id,node_id,locator,admission,
                    format_kind,reason,semantic_hits_json,evaluated_at
                ) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(artifact_identity,query_id,node_id) DO UPDATE SET
                    admission=excluded.admission,
                    format_kind=excluded.format_kind,
                    reason=excluded.reason,
                    semantic_hits_json=excluded.semantic_hits_json,
                    evaluated_at=excluded.evaluated_at
                """,
                (
                    lead.artifact_identity,
                    query.query_id,
                    str(getattr(node, "node_id", "") or ""),
                    lead.locator,
                    assessment.admission.value,
                    assessment.format_kind,
                    assessment.reason,
                    json.dumps(
                        list(assessment.semantic_hits),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    float(self.clock()),
                ),
            )
        return assessment.admission

    @staticmethod
    def _match_node(lead: ArtifactLead, nodes: dict[str, Any]) -> Any | None:
        if lead.provider_native_id in nodes:
            return nodes[lead.provider_native_id]
        for native_id, node in nodes.items():
            if (
                lead.provider_native_id.startswith(native_id + ":")
                or lead.provider_native_id == "file:" + native_id
                or native_id.startswith(lead.provider_native_id + ":")
            ):
                return node
        if len(nodes) == 1:
            return next(iter(nodes.values()))
        return None

    async def execute_root_query_page(
        self,
        adapter: Any,
        query: RootQuery,
        *,
        program_id: str,
        pivot_id: str = "",
        decision_id: str = "",
    ) -> RootPageResult:
        checkpoint = self.research.query_checkpoint(query.query_id)
        self.research.update_query_checkpoint(
            query.query_id,
            checkpoint=checkpoint,
            state=QueryState.RUNNING,
        )
        try:
            page = await adapter.search(query, checkpoint)
            nodes: dict[str, Any] = {}
            artifacts = 0
            sources_inserted = 0
            metadata_accepted = 0
            metadata_held = 0
            metadata_rejected = 0
            seen_lineages: set[tuple[str, str]] = set()

            def admit_lead(lead: ArtifactLead, node: Any) -> bool:
                nonlocal metadata_accepted, metadata_held, metadata_rejected
                admission = self._prefilter_artifact_lead(
                    lead,
                    node=node,
                    query=query,
                )
                if admission is MetadataArtifactAdmission.ACCEPT:
                    metadata_accepted += 1
                    return True
                if admission is MetadataArtifactAdmission.HOLD:
                    metadata_held += 1
                else:
                    metadata_rejected += 1
                return False

            for hit in page.hits:
                if hit.root_id != query.root_id or hit.query_id != query.query_id:
                    raise ValueError("adapter returned hit outside query identity")
                node = self.research.upsert_hit(hit)
                nodes[hit.provider_native_id] = node

                resolved = resolve_node(
                    node,
                    query_id=query.query_id,
                    program_id=program_id,
                )
                explicit = tuple(await adapter.resolve(hit))
                lead_by_identity = {
                    lead.artifact_identity: lead
                    for lead in (
                        tuple(
                            item
                            for item in explicit
                            if isinstance(item, ArtifactLead)
                        )
                        + resolved.artifact_leads
                    )
                }
                for lead in lead_by_identity.values():
                    if not admit_lead(lead, node):
                        continue
                    artifact_id, _candidate, inserted = self.register_artifact_source(
                        lead,
                        node_id=node.node_id,
                        query=query,
                        program_id=program_id,
                        pivot_id=pivot_id,
                        decision_id=decision_id,
                    )
                    key = (artifact_id, node.node_id)
                    if key not in seen_lineages:
                        artifacts += 1
                        seen_lineages.add(key)
                    sources_inserted += int(inserted)

                # New roots remain proposal/research state only. Registration
                # alone never creates a query or provider request.
                for lead in resolved.new_root_leads:
                    self.research.upsert_root(
                        RootSurface(
                            root_id=stable_hash(
                                "resolved-root",
                                lead.kind.value,
                                lead.entrypoint,
                            ),
                            kind=lead.kind,
                            canonical_locator=lead.entrypoint,
                            capabilities=lead.capabilities,
                            metadata={
                                "discovered_from_node_id": (
                                    lead.discovered_from_node_id
                                ),
                                "rationale": lead.rationale,
                                "authority": "proposal_only",
                            },
                        )
                    )

            for lead in page.artifact_leads:
                node = self._match_node(lead, nodes)
                if node is None:
                    synthetic = SearchHit(
                        root_id=query.root_id,
                        query_id=query.query_id,
                        provider_native_id=lead.provider_native_id,
                        provider_url=lead.locator,
                        provider_type="ARTIFACT",
                    )
                    node = self.research.upsert_hit(synthetic)
                if not admit_lead(lead, node):
                    continue
                artifact_id, _candidate, inserted = self.register_artifact_source(
                    lead,
                    node_id=node.node_id,
                    query=query,
                    program_id=program_id,
                    pivot_id=pivot_id,
                    decision_id=decision_id,
                )
                key = (artifact_id, node.node_id)
                if key not in seen_lineages:
                    artifacts += 1
                    seen_lineages.add(key)
                sources_inserted += int(inserted)

            retryable = page.retry_after is not None
            if retryable:
                state = QueryState.RETRYABLE
                next_checkpoint = page.next_checkpoint or checkpoint
                retry_at = float(self.clock()) + float(page.retry_after or 0.0)
            elif page.terminal:
                state = QueryState.COMPLETE
                next_checkpoint = page.next_checkpoint
                retry_at = None
            else:
                state = QueryState.READY
                next_checkpoint = page.next_checkpoint
                retry_at = None
            self.research.update_query_checkpoint(
                query.query_id,
                checkpoint=next_checkpoint,
                state=state,
                pages_delta=1,
                retry_at=retry_at,
            )
            return RootPageResult(
                query_id=query.query_id,
                hits=len(page.hits),
                artifacts=artifacts,
                sources_inserted=sources_inserted,
                terminal=bool(page.terminal),
                retryable=retryable,
                metadata_accepted=metadata_accepted,
                metadata_held=metadata_held,
                metadata_rejected=metadata_rejected,
            )
        except BaseException as exc:
            self.research.update_query_checkpoint(
                query.query_id,
                checkpoint=checkpoint,
                state=QueryState.RETRYABLE,
                retry_at=float(self.clock()) + 30.0,
                last_error=f"{type(exc).__name__}: {exc}"[:1000],
            )
            raise

    def sync_closed_final_rewards(self, *, limit: int = 100) -> int:
        """Project closed production exposures into L3 delayed reward exactly once."""
        if limit < 1:
            return 0
        rows = self.connection.execute(
            """
            SELECT r.*
            FROM source_run_outcomes AS r
            LEFT JOIN research_final_projection AS p
              ON p.source_key=r.source_key
             AND p.exposure_id=r.exposure_id
             AND p.baseline_signature=r.baseline_signature
             AND p.model_signature=r.model_signature
            WHERE r.closed=1
              AND r.exposure_id IS NOT NULL
              AND r.exposure_id!=''
              AND p.source_key IS NULL
              AND (
                  NOT EXISTS (
                      SELECT 1
                      FROM source_candidates AS sc
                      WHERE sc.source_key=r.source_key
                        AND sc.discovery_strategy='structured_root_artifact'
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM research_artifact_lineage AS l
                      WHERE l.source_key=r.source_key
                  )
              )
            ORDER BY r.closed_at, r.source_key, r.lease_id
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        synced = 0
        for row in rows:
            source_key = str(row["source_key"])
            exposure_id = str(row["exposure_id"])
            baseline = str(row["baseline_signature"])
            model = str(row["model_signature"])
            final_eed = float(row["final_accepted_eed"])

            lineage_rows = self.research.artifact_lineage(
                source_key=source_key
            )
            candidate = self.discovery.get_candidate(source_key)
            if (
                candidate is not None
                and candidate.discovery_strategy == "structured_root_artifact"
                and not lineage_rows
            ):
                # Defensive race guard. The SQL predicate above normally keeps
                # this row out of the batch. If lineage visibility changes
                # between selection and projection, leave the FINAL unprojected
                # so the next cycle can attribute it to the full causal path.
                continue

            # Every production exposure is a distinct delayed-reward
            # observation. Bind it additively to the complete causal source
            # lineage; do not overwrite an earlier exposure on the artifact row.
            self.research.bind_source_exposure_lineage(
                source_key=source_key,
                exposure_id=exposure_id,
            )

            # Preserve the legacy single exposure column as a first-observation
            # audit hint only. The many-to-many table above is authoritative for
            # repeated production observations.
            for lineage in lineage_rows:
                if not str(lineage["source_exposure_id"] or ""):
                    self.research.bind_artifact_source(
                        str(lineage["artifact_id"]),
                        source_key=source_key,
                        source_exposure_id=exposure_id,
                    )

            rewards = self.feedback.close_final(
                source_key=source_key,
                exposure_id=exposure_id,
                final_eed=final_eed,
                validation_closed=True,
                idempotency_token=stable_hash(
                    "production-final",
                    source_key,
                    exposure_id,
                    baseline,
                    model,
                ),
            )
            with self.connection:
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO research_final_projection(
                        source_key,exposure_id,baseline_signature,model_signature,
                        final_eed,rewards_written,synced_at
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        source_key,
                        exposure_id,
                        baseline,
                        model,
                        final_eed,
                        int(rewards),
                        float(self.clock()),
                    ),
                )
            synced += 1
        return synced


__all__ = [
    "IntegratedResearchResult",
    "ResearchIntegrationBridge",
    "RootPageResult",
]
