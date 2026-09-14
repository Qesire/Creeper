from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import httpx

from creeper.source_discovery.manager import SourceReservoirManager
from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.research_trigger import (
    ResearchDirective,
    ResearchTriggerReason,
    ResearchTriggerSnapshot,
)
from creeper.source_research.adapters.base import (
    ArtifactLead,
    RootQuery as AdapterRootQuery,
    SearchHit,
    SearchPage,
)
from creeper.source_research.integration import (
    IntegratedResearchResult,
    ResearchIntegrationBridge,
)
from creeper.source_research.models import (
    DecisionRecord,
    FrontierState,
    FrontierTask,
    PolicySnapshot,
    QueryProgram,
    QueryState,
    RootKind,
    RootQuery,
    RootSurface,
)
from creeper.source_research.agent.protocol import (
    ProposalEnvelope,
    QueryProgramProposal,
    RootQuery as ProposalRootQuery,
    UnifiedLLMTask,
)
from creeper.source_research.policy import (
    HierarchicalAdaptivePolicy,
    PolicyConfig,
    PolicyLifecycle,
)
from creeper.source_research.registry import ResearchRegistry
from creeper.source_discovery_service import (
    _root_query_runtime_adapters,
    _unified_research_directive_provider,
)
from creeper.storage.control_store import ControlStore


class _OnePageRoot:
    async def search(self, query, checkpoint):
        return SearchPage(
            hits=(
                SearchHit(
                    root_id=query.root_id,
                    query_id=query.query_id,
                    provider_native_id="dataset:1",
                    provider_url="https://repo.example/datasets/1",
                    provider_type="DATASET",
                    title="historical web index",
                ),
            ),
            artifact_leads=(
                ArtifactLead(
                    root_id=query.root_id,
                    provider_native_id="dataset:1:file:index",
                    locator="https://repo.example/files/index.cdxj",
                    content_type="text/x-cdxj",
                    checksum="sha256:closure",
                ),
            ),
            terminal=True,
        )

    async def resolve(self, hit):
        return ()


class _MetadataOnlyRoot:
    async def search(self, query, checkpoint):
        return SearchPage(
            hits=(
                SearchHit(
                    root_id=query.root_id,
                    query_id=query.query_id,
                    provider_native_id="record:metadata-only",
                    provider_url="https://repo.example/record/1",
                    provider_type="RECORD",
                    title="metadata-only artifact record",
                    metadata={
                        "files": (
                            {
                                "id": "file:metadata-only",
                                "url": "https://repo.example/files/metadata.cdxj",
                                "checksum": "sha256:metadata-only",
                            },
                        ),
                    },
                ),
            ),
            terminal=True,
        )

    async def resolve(self, hit):
        return ()


class _CountingRoot:
    def __init__(self, *, terminal: bool = False) -> None:
        self.calls = 0
        self.terminal = terminal

    async def search(self, query, checkpoint):
        self.calls += 1
        return SearchPage(terminal=self.terminal)

    async def resolve(self, hit):
        return ()


class _SlowRoot:
    def __init__(self) -> None:
        self.calls = 0

    async def search(self, query, checkpoint):
        self.calls += 1
        await asyncio.sleep(0.05)
        return SearchPage(terminal=True)

    async def resolve(self, hit):
        return ()


class L9ResearchRuntimeClosureTests(unittest.IsolatedAsyncioTestCase):
    async def test_root_query_wall_budget_cancels_stalled_provider(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(research, discovery)
                root = RootSurface(
                    root_id="root:slow",
                    kind=RootKind.STRUCTURED_REPOSITORY,
                    canonical_locator="https://repo.example/api",
                )
                research.upsert_root(root)
                query = AdapterRootQuery(
                    "query:slow",
                    root.root_id,
                    "historical crawl",
                    3,
                    0.01,
                )
                program = QueryProgram(
                    root_id=root.root_id,
                    strategy="wall-bound",
                    queries=(query,),
                    hard_max_requests=3,
                    stop_conditions=("wall_budget",),
                    program_id="program:slow",
                )
                research.register_program(program)
                adapter = _SlowRoot()

                result = await bridge.execute_root_query_page(
                    adapter,
                    query,
                    program_id=program.program_id,
                )

                self.assertTrue(result.terminal)
                self.assertFalse(result.retryable)
                self.assertEqual(adapter.calls, 1)
                row = research.get_query_row(query.query_id)
                self.assertEqual(row["state"], QueryState.EXHAUSTED.value)
                self.assertGreater(float(row["wall_seconds_used"]), 0.0)
                self.assertEqual(
                    research.program_request_budget(program.program_id),
                    (1, 3),
                )
            finally:
                control.close()

    async def test_program_request_budget_blocks_second_query_before_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(research, discovery)
                root = RootSurface(
                    root_id="root:budget",
                    kind=RootKind.STRUCTURED_REPOSITORY,
                    canonical_locator="https://repo.example/api",
                )
                research.upsert_root(root)
                first = AdapterRootQuery(
                    "query:budget:1", root.root_id, "historical crawl one", 2, 5.0
                )
                second = AdapterRootQuery(
                    "query:budget:2", root.root_id, "historical crawl two", 2, 5.0
                )
                program = QueryProgram(
                    root_id=root.root_id,
                    strategy="request-bound",
                    queries=(first, second),
                    hard_max_requests=1,
                    stop_conditions=("request_budget",),
                    program_id="program:budget",
                )
                research.register_program(program)
                adapter = _CountingRoot(terminal=True)

                first_result = await bridge.execute_root_query_page(
                    adapter,
                    first,
                    program_id=program.program_id,
                )
                second_result = await bridge.execute_root_query_page(
                    adapter,
                    second,
                    program_id=program.program_id,
                )

                self.assertTrue(first_result.terminal)
                self.assertTrue(second_result.terminal)
                self.assertEqual(adapter.calls, 1)
                self.assertEqual(
                    research.program_request_budget(program.program_id),
                    (1, 1),
                )
                self.assertEqual(
                    research.get_query_row(second.query_id)["state"],
                    QueryState.EXHAUSTED.value,
                )
            finally:
                control.close()

    async def test_query_page_budget_is_enforced_above_adapter(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(research, discovery)
                root = RootSurface(
                    root_id="root:pages",
                    kind=RootKind.STRUCTURED_REPOSITORY,
                    canonical_locator="https://repo.example/api",
                )
                research.upsert_root(root)
                query = AdapterRootQuery(
                    "query:pages", root.root_id, "historical crawl", 1, 5.0
                )
                program = QueryProgram(
                    root_id=root.root_id,
                    strategy="page-bound",
                    queries=(query,),
                    hard_max_requests=5,
                    stop_conditions=("page_budget",),
                    program_id="program:pages",
                )
                research.register_program(program)
                adapter = _CountingRoot(terminal=False)

                first = await bridge.execute_root_query_page(
                    adapter,
                    query,
                    program_id=program.program_id,
                )
                second = await bridge.execute_root_query_page(
                    adapter,
                    query,
                    program_id=program.program_id,
                )

                self.assertTrue(first.terminal)
                self.assertTrue(second.terminal)
                self.assertEqual(adapter.calls, 1)
                self.assertEqual(
                    research.get_query_row(query.query_id)["state"],
                    QueryState.EXHAUSTED.value,
                )
            finally:
                control.close()

    async def test_last_budget_request_failure_does_not_requeue_frontier(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(research, discovery)
                root = RootSurface(
                    root_id="datacite",
                    kind=RootKind.STRUCTURED_REPOSITORY,
                    canonical_locator="https://api.datacite.org/dois",
                )
                research.upsert_root(root)
                query = AdapterRootQuery(
                    "query:last-budget",
                    root.root_id,
                    "historical crawl",
                    2,
                    5.0,
                )
                program = QueryProgram(
                    root_id=root.root_id,
                    strategy="last-budget-failure",
                    queries=(query,),
                    hard_max_requests=1,
                    stop_conditions=("request_budget",),
                    program_id="program:last-budget",
                )
                research.register_program(program)
                calls = 0

                def handler(request: httpx.Request) -> httpx.Response:
                    nonlocal calls
                    calls += 1
                    raise httpx.ConnectError("synthetic provider failure", request=request)

                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler),
                    trust_env=False,
                ) as client:
                    planner, executor = _root_query_runtime_adapters(
                        research,
                        bridge,
                        client,
                        parallelism=1,
                    )
                    tasks = planner()
                    self.assertEqual(len(tasks), 1)
                    with self.assertRaises(httpx.ConnectError):
                        await executor(tasks[0])

                    self.assertEqual(calls, 1)
                    self.assertEqual(
                        research.get_query_row(query.query_id)["state"],
                        QueryState.EXHAUSTED.value,
                    )
                    self.assertEqual(
                        research.get_frontier(tasks[0].task_id).state,
                        FrontierState.DONE,
                    )
                    self.assertEqual(planner(), ())
            finally:
                control.close()

    def test_metadata_suppression_fails_closed_on_schema_fault(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                ResearchIntegrationBridge(research, discovery)
                candidate, _ = discovery.register_proposal(
                    SourceCandidate(
                        canonical_entrypoint="https://objects.example/broken.pdf",
                        source_family="GENERIC",
                        level=SourceLevel.SOURCE,
                        discovered_by="test",
                        discovery_strategy="META_SOURCE_SEARCH",
                        confidence=1.0,
                    )
                )
                control.connection.execute(
                    """
                    ALTER TABLE research_artifact_prefilter
                    RENAME COLUMN locator TO broken_locator
                    """
                )
                with self.assertRaises(sqlite3.OperationalError):
                    discovery.suppression_reason(candidate)
            finally:
                control.close()

    async def test_metadata_resolver_promotes_only_concrete_artifact_leads(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(research, discovery)
                root = RootSurface(
                    root_id="root:metadata",
                    kind=RootKind.STRUCTURED_REPOSITORY,
                    canonical_locator="https://repo.example/api",
                    capabilities=("search", "metadata"),
                )
                research.upsert_root(root)
                query = AdapterRootQuery(
                    "query:metadata",
                    root.root_id,
                    "historical metadata",
                    1,
                    10.0,
                )
                program = QueryProgram(
                    root_id=root.root_id,
                    strategy="metadata-resolution",
                    queries=(query,),
                    hard_max_requests=1,
                    stop_conditions=("terminal_page",),
                    program_id="program:metadata",
                )
                research.register_program(program)

                result = await bridge.execute_root_query_page(
                    _MetadataOnlyRoot(),
                    query,
                    program_id=program.program_id,
                )
                self.assertEqual(result.hits, 1)
                self.assertEqual(result.artifacts, 1)
                self.assertEqual(result.sources_inserted, 1)
                candidates = discovery.list_candidates()
                self.assertEqual(len(candidates), 1)
                self.assertEqual(
                    candidates[0].canonical_entrypoint,
                    "https://repo.example/files/metadata.cdxj",
                )
                self.assertEqual(candidates[0].direct_evidence_prior, 1.0)
            finally:
                control.close()

    async def test_root_artifact_source_final_policy_closes_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(research, discovery)

                root = RootSurface(
                    root_id="root:repo",
                    kind=RootKind.STRUCTURED_REPOSITORY,
                    canonical_locator="https://repo.example/api",
                    capabilities=("SEARCH", "ARTIFACTS"),
                )
                research.upsert_root(root)

                query = AdapterRootQuery(
                    "query:repo:1",
                    root.root_id,
                    "historical web index",
                    1,
                    10.0,
                    expected_artifact_family="CDXJ",
                )
                program = QueryProgram(
                    root_id=root.root_id,
                    strategy="structured-repository-search",
                    queries=(query,),
                    hard_max_requests=1,
                    stop_conditions=("terminal_page",),
                    program_id="program:repo:1",
                )
                research.register_program(program)

                research.enqueue_frontier(
                    FrontierTask(
                        task_id="frontier:repo:1",
                        task_kind="PATH",
                        entity_id=query.query_id,
                        policy_version="policy:test",
                    )
                )

                decision = DecisionRecord(
                    decision_id="decision:repo:1",
                    task_id="frontier:repo:1",
                    arm_id="root:repo",
                    policy_version="policy:test",
                    policy_snapshot_id="snapshot:test",
                    propensity=1.0,
                    context_hash="ctx:test",
                    chosen_at=1.0,
                )
                self.assertTrue(research.record_decision(decision))

                page = await bridge.execute_root_query_page(
                    _OnePageRoot(),
                    query,
                    program_id=program.program_id,
                    decision_id=decision.decision_id,
                )
                self.assertEqual(page.sources_inserted, 1)
                self.assertEqual(page.artifacts, 1)

                candidates = discovery.list_candidates()
                self.assertEqual(len(candidates), 1)
                source_key = candidates[0].source_key
                lineage = research.artifact_lineage(source_key=source_key)
                self.assertEqual(len(lineage), 1)
                self.assertEqual(lineage[0]["decision_id"], decision.decision_id)
                self.assertEqual(lineage[0]["query_id"], query.query_id)
                self.assertEqual(lineage[0]["program_id"], program.program_id)

                authority = ("baseline:test", "model:test")
                run = discovery.begin_source_run(
                    source_key,
                    reservoir_id="reservoir:test",
                    lease_id="lease:test",
                    baseline_signature=authority[0],
                    model_signature=authority[1],
                )
                self.assertIsNotNone(run.exposure_id)
                discovery.record_source_run_read(
                    source_key,
                    reservoir_id="reservoir:test",
                    lease_id="lease:test",
                    baseline_signature=authority[0],
                    model_signature=authority[1],
                    source_records=1,
                    bytes_read=128,
                    source_requests=1,
                )
                discovery.record_source_run_validation(
                    source_key,
                    reservoir_id="reservoir:test",
                    lease_id="lease:test",
                    baseline_signature=authority[0],
                    model_signature=authority[1],
                    evidence_tasks_created=0,
                    evidence_tasks_terminal=0,
                    direct_capsules_committed=1,
                    provider_requests=0,
                    provider_elapsed_seconds=0.0,
                    accepted_host_years=1,
                    final_accepted_eed=2.5,
                    max_evidence_sequence=1,
                    validation_complete=True,
                )
                self.assertTrue(
                    discovery.close_source_run(
                        source_key,
                        reservoir_id="reservoir:test",
                        lease_id="lease:test",
                        baseline_signature=authority[0],
                        model_signature=authority[1],
                    )
                )

                self.assertEqual(bridge.sync_closed_final_rewards(), 1)
                self.assertEqual(bridge.sync_closed_final_rewards(), 0)

                stats = research.rebuild_arm_stats(
                    policy_version="policy:test",
                    schema_version=2,
                )
                self.assertEqual(len(stats), 1)
                self.assertEqual(stats[0].arm_id, "root:repo")
                self.assertEqual(stats[0].pulls, 1)
                self.assertEqual(stats[0].final_observation_count, 1)
                self.assertEqual(stats[0].final_reward, 2.5)
                self.assertEqual(stats[0].authoritative_reward, 2.5)

                exposure_lineage = research.artifact_lineage_for_exposure(
                    source_key=source_key,
                    exposure_id=run.exposure_id or "",
                )
                self.assertEqual(len(exposure_lineage), 1)
                self.assertEqual(
                    exposure_lineage[0]["decision_id"],
                    decision.decision_id,
                )
            finally:
                control.close()

    def test_l6_research_query_program_commits_back_to_durable_frontier(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(research, discovery)
                research.upsert_root(
                    RootSurface(
                        root_id="datacite",
                        kind=RootKind.STRUCTURED_REPOSITORY,
                        canonical_locator="https://api.datacite.org/dois",
                        capabilities=("search", "cursor", "content_urls"),
                    )
                )
                seed = RootQuery(
                    root_id="datacite",
                    query_text="seed web archive",
                    max_pages=1,
                    max_wall_seconds=30.0,
                    page_size=10,
                )
                research.register_program(
                    QueryProgram(
                        root_id="datacite",
                        strategy="seed",
                        queries=(seed,),
                        hard_max_requests=1,
                        stop_conditions=("terminal_page",),
                    )
                )
                research.update_query_checkpoint(
                    seed.query_id,
                    checkpoint=None,
                    state=QueryState.COMPLETE,
                    pages_delta=1,
                )

                context = bridge.build_root_research_context(
                    "datacite",
                    cooldown_satisfied=True,
                )
                self.assertTrue(context.seed_current_program_exhausted)
                self.assertFalse(context.equivalent_unexecuted_program)
                self.assertTrue(context.metrics_available)

                request = bridge.build_root_research_request(context)
                self.assertTrue(bridge.try_claim_llm_call(request))
                proposal = QueryProgramProposal(
                    proposal_id="proposal:next",
                    root_id="datacite",
                    strategy="orthogonal structured query",
                    queries=(
                        ProposalRootQuery(
                            query="historical crawl corpus",
                            filters={},
                            expected_signal="reusable artifact",
                            expected_family="",
                            max_pages=1,
                        ),
                    ),
                    hard_max_requests=1,
                    stop_conditions=("terminal_page",),
                    reuse_key="reuse:next",
                    context_hash=context.context_hash,
                )
                result = IntegratedResearchResult(
                    call_identity=request.call_identity,
                    envelope=ProposalEnvelope(
                        query="compile next root query",
                        task_type=UnifiedLLMTask.COMPILE_ROOT_QUERY_PROGRAM,
                        context_hash=context.context_hash,
                        proposals=(proposal,),
                    ),
                    typed_context=context,
                )
                self.assertEqual(
                    bridge.commit_root_research_result(
                        result,
                        elapsed_seconds=0.1,
                    ),
                    1,
                )
                new_rows = research.connection.execute(
                    """
                    SELECT query_id, state
                    FROM research_queries
                    WHERE root_id='datacite' AND query_text=?
                    """,
                    ("historical crawl corpus",),
                ).fetchall()
                self.assertEqual(len(new_rows), 1)
                self.assertEqual(new_rows[0]["state"], QueryState.READY.value)
                self.assertEqual(research.ensure_query_frontier(limit=10), 1)
                ready = [
                    task
                    for task in research.ready_frontier()
                    if task.task_kind == "QUERY"
                    and task.entity_id == str(new_rows[0]["query_id"])
                ]
                self.assertEqual(len(ready), 1)
            finally:
                control.close()

    async def test_service_runtime_consumes_durable_root_query_frontier(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            calls: list[str] = []
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(research, discovery)
                research.upsert_root(
                    RootSurface(
                        root_id="datacite",
                        kind=RootKind.STRUCTURED_REPOSITORY,
                        canonical_locator="https://api.datacite.org/dois",
                        capabilities=("search", "artifacts"),
                    )
                )
                query = RootQuery(
                    root_id="datacite",
                    query_text="historical web index",
                    max_pages=1,
                    max_wall_seconds=30.0,
                    page_size=10,
                    expected_artifact_family="CDXJ",
                )
                program = QueryProgram(
                    root_id="datacite",
                    strategy="seed",
                    queries=(query,),
                    hard_max_requests=1,
                    stop_conditions=("terminal_page",),
                )
                research.register_program(program)

                def handler(request: httpx.Request) -> httpx.Response:
                    calls.append(str(request.url))
                    return httpx.Response(
                        200,
                        json={
                            "data": [
                                {
                                    "id": "10.1234/root-runtime",
                                    "attributes": {
                                        "titles": [{"title": "historical index"}],
                                        "contentUrl": [
                                            "https://objects.example/history.cdxj",
                                            "https://objects.example/paper.pdf",
                                        ],
                                    },
                                }
                            ],
                            "links": {"next": None},
                        },
                    )

                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler),
                    trust_env=False,
                ) as client:
                    planner, executor = _root_query_runtime_adapters(
                        research,
                        bridge,
                        client,
                        parallelism=1,
                    )
                    tasks = planner()
                    self.assertEqual(len(tasks), 1)
                    self.assertEqual(tasks[0].state, FrontierState.CLAIMED)

                    result = await executor(tasks[0])
                    self.assertTrue(result.terminal)
                    self.assertEqual(result.sources_inserted, 1)
                    self.assertEqual(result.metadata_accepted, 1)
                    self.assertEqual(result.metadata_rejected, 1)
                    self.assertEqual(result.metadata_held, 0)
                    self.assertEqual(len(calls), 1)

                    query_row = research.get_query_row(query.query_id)
                    self.assertEqual(query_row["state"], QueryState.COMPLETE.value)
                    self.assertEqual(
                        research.get_frontier(tasks[0].task_id).state,
                        FrontierState.DONE,
                    )
                    candidates = discovery.list_candidates()
                    self.assertEqual(len(candidates), 1)
                    self.assertEqual(
                        candidates[0].canonical_entrypoint,
                        "https://objects.example/history.cdxj",
                    )
                    self.assertEqual(candidates[0].direct_evidence_prior, 1.0)
                    self.assertEqual(
                        candidates[0].discovery_strategy,
                        "structured_root_artifact",
                    )
                    lineage = research.artifact_lineage(
                        source_key=candidates[0].source_key
                    )
                    self.assertEqual(len(lineage), 1)
                    self.assertEqual(lineage[0]["query_id"], query.query_id)
                    self.assertEqual(lineage[0]["program_id"], program.program_id)
                    self.assertEqual(lineage[0]["root_id"], "datacite")
                    prefilter = research.connection.execute(
                        """
                        SELECT admission, locator
                        FROM research_artifact_prefilter
                        WHERE query_id=?
                        ORDER BY locator
                        """,
                        (query.query_id,),
                    ).fetchall()
                    self.assertEqual(
                        [(row["admission"], row["locator"]) for row in prefilter],
                        [
                            ("ACCEPT", "https://objects.example/history.cdxj"),
                            ("REJECT", "https://objects.example/paper.pdf"),
                        ],
                    )

                    rejected, inserted = discovery.register_proposal(
                        SourceCandidate(
                            canonical_entrypoint="https://objects.example/paper.pdf",
                            source_family="GENERIC",
                            level=SourceLevel.SOURCE,
                            discovered_by="legacy-agent:test",
                            discovery_strategy="META_SOURCE_SEARCH",
                            confidence=1.0,
                        )
                    )
                    self.assertTrue(inserted)
                    self.assertIn(
                        "metadata prefilter reject",
                        discovery.suppression_reason(rejected) or "",
                    )

                    # Terminal durable work is not replayed on the next cycle.
                    self.assertEqual(planner(), ())
                    self.assertEqual(len(calls), 1)
            finally:
                control.close()

    def test_root_research_cannot_monopolize_generic_llm_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = [1000.0]
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(
                    research,
                    discovery,
                    clock=lambda: now[0],
                )
                research.upsert_root(
                    RootSurface(
                        root_id="datacite",
                        kind=RootKind.STRUCTURED_REPOSITORY,
                        canonical_locator="https://api.datacite.org/dois",
                        capabilities=("search", "cursor"),
                    )
                )
                seed = RootQuery(
                    root_id="datacite",
                    query_text="seed",
                    max_pages=1,
                    max_wall_seconds=30.0,
                    page_size=10,
                )
                research.register_program(
                    QueryProgram(
                        root_id="datacite",
                        strategy="seed",
                        queries=(seed,),
                        hard_max_requests=1,
                        stop_conditions=("terminal_page",),
                    )
                )
                research.update_query_checkpoint(
                    seed.query_id,
                    checkpoint=None,
                    state=QueryState.COMPLETE,
                    pages_delta=1,
                )

                manager = SourceReservoirManager(discovery)
                config = SimpleNamespace(
                    agent=SimpleNamespace(
                        min_seconds_between_starts=0.0,
                        same_context_failure_cooldown_seconds=0.0,
                    )
                )
                provider = _unified_research_directive_provider(
                    manager,
                    research,
                    bridge,
                    config,
                )
                snapshot = ResearchTriggerSnapshot(
                    ready_minutes=0.0,
                    context_hash="generic:empty",
                    now=now[0],
                )

                first = provider(snapshot)
                self.assertIsNotNone(first)
                self.assertEqual(
                    first.task_type,
                    "COMPILE_ROOT_QUERY_PROGRAM",
                )

                root_context = bridge.build_root_research_context(
                    "datacite",
                    cooldown_satisfied=True,
                )
                root_request = bridge.build_root_research_request(root_context)
                self.assertTrue(bridge.try_claim_llm_call(root_request))
                bridge.finish_llm_call(
                    root_request.call_identity,
                    success=True,
                    cost_seconds=0.1,
                )

                # One root-compiler call consumes the root-research share. The
                # next allowed slow call must return to generic source-family
                # discovery instead of compiling another known-root query.
                second = provider(snapshot)
                self.assertIsNotNone(second)
                self.assertEqual(second.task_type, "DISCOVER_NEW_SOURCE")
            finally:
                control.close()

    async def test_active_l7_policy_closes_root_query_final_reward(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            calls: list[str] = []
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(research, discovery)

                policy = HierarchicalAdaptivePolicy(
                    PolicyConfig(
                        policy_id="root-query-runtime",
                        version="policy:l7:test",
                        snapshot_id="snapshot:l7:test",
                        schema_version=2,
                        lifecycle=PolicyLifecycle.ACTIVE,
                        exploration_fraction=0.1,
                    )
                )
                research.upsert_policy_snapshot(
                    PolicySnapshot(
                        snapshot_id=policy.config.snapshot_id,
                        policy_version=policy.config.version,
                        schema_version=policy.config.schema_version,
                        parameters=policy.snapshot_parameters(),
                        created_at=1.0,
                        active=True,
                    )
                )

                research.upsert_root(
                    RootSurface(
                        root_id="datacite",
                        kind=RootKind.STRUCTURED_REPOSITORY,
                        canonical_locator="https://api.datacite.org/dois",
                        capabilities=("search", "artifacts"),
                    )
                )
                q1 = RootQuery(
                    root_id="datacite",
                    query_text="historical web dataset",
                    max_pages=1,
                    max_wall_seconds=30.0,
                    page_size=10,
                    expected_artifact_family="CDXJ",
                )
                q2 = RootQuery(
                    root_id="datacite",
                    query_text="early web corpus",
                    max_pages=1,
                    max_wall_seconds=30.0,
                    page_size=10,
                    expected_artifact_family="CDXJ",
                )
                research.register_program(
                    QueryProgram(
                        root_id="datacite",
                        strategy="adaptive-test",
                        queries=(q1, q2),
                        hard_max_requests=2,
                        stop_conditions=("terminal_page",),
                    )
                )

                def handler(request: httpx.Request) -> httpx.Response:
                    calls.append(str(request.url))
                    return httpx.Response(
                        200,
                        json={
                            "data": [
                                {
                                    "id": "10.1234/adaptive-runtime",
                                    "attributes": {
                                        "titles": [{"title": "adaptive result"}],
                                        "contentUrl": [
                                            "https://objects.example/adaptive.cdxj"
                                        ],
                                    },
                                }
                            ],
                            "links": {"next": None},
                        },
                    )

                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler),
                    trust_env=False,
                ) as client:
                    planner, executor = _root_query_runtime_adapters(
                        research,
                        bridge,
                        client,
                        parallelism=1,
                    )
                    tasks = planner()
                    self.assertEqual(len(tasks), 1)
                    task = tasks[0]
                    self.assertIn(task.entity_id, {q1.query_id, q2.query_id})
                    self.assertEqual(task.state, FrontierState.CLAIMED)
                    leaf_id = str(task.checkpoint["decision_id"])
                    lineage = research.get_decision_lineage(leaf_id)
                    self.assertEqual(len(lineage), 2)
                    root_decision, query_decision = lineage
                    self.assertEqual(root_decision.arm_id, "datacite")
                    self.assertEqual(query_decision.arm_id, task.entity_id)
                    self.assertEqual(
                        set(
                            query_decision.metadata["candidate_action_ids"]
                        ),
                        {q1.query_id, q2.query_id},
                    )
                    self.assertGreater(query_decision.propensity, 0.0)
                    self.assertLessEqual(query_decision.propensity, 1.0)
                    self.assertEqual(
                        query_decision.parent_decision_ids,
                        (root_decision.decision_id,),
                    )

                    result = await executor(task)
                    self.assertTrue(result.terminal)
                    self.assertEqual(result.sources_inserted, 1)
                    self.assertEqual(len(calls), 1)

                candidates = discovery.list_candidates()
                self.assertEqual(len(candidates), 1)
                candidate = candidates[0]
                authority = ("baseline:l7", "model:l7")
                run = discovery.begin_source_run(
                    candidate.source_key,
                    reservoir_id="reservoir:l7",
                    lease_id="lease:l7",
                    baseline_signature=authority[0],
                    model_signature=authority[1],
                )
                discovery.record_source_run_read(
                    candidate.source_key,
                    reservoir_id="reservoir:l7",
                    lease_id="lease:l7",
                    baseline_signature=authority[0],
                    model_signature=authority[1],
                    source_records=1,
                    bytes_read=128,
                    source_requests=1,
                )
                discovery.record_source_run_validation(
                    candidate.source_key,
                    reservoir_id="reservoir:l7",
                    lease_id="lease:l7",
                    baseline_signature=authority[0],
                    model_signature=authority[1],
                    evidence_tasks_created=0,
                    evidence_tasks_terminal=0,
                    direct_capsules_committed=1,
                    provider_requests=0,
                    provider_elapsed_seconds=0.0,
                    accepted_host_years=1,
                    final_accepted_eed=4.0,
                    max_evidence_sequence=1,
                    validation_complete=True,
                )
                self.assertTrue(
                    discovery.close_source_run(
                        candidate.source_key,
                        reservoir_id="reservoir:l7",
                        lease_id="lease:l7",
                        baseline_signature=authority[0],
                        model_signature=authority[1],
                    )
                )
                self.assertEqual(bridge.sync_closed_final_rewards(), 1)
                stats = {
                    item.arm_id: item
                    for item in research.rebuild_arm_stats(
                        policy_version=policy.config.version,
                        schema_version=policy.config.schema_version,
                    )
                }
                self.assertIn("datacite", stats)
                self.assertIn(task.entity_id, stats)
                self.assertEqual(stats["datacite"].final_observation_count, 1)
                self.assertEqual(stats["datacite"].final_reward, 4.0)
                self.assertEqual(
                    stats[task.entity_id].final_observation_count,
                    1,
                )
                self.assertEqual(stats[task.entity_id].final_reward, 4.0)
                exposure_lineage = research.artifact_lineage_for_exposure(
                    source_key=candidate.source_key,
                    exposure_id=run.exposure_id or "",
                )
                self.assertEqual(len(exposure_lineage), 1)
                self.assertEqual(
                    exposure_lineage[0]["decision_id"],
                    query_decision.decision_id,
                )
            finally:
                control.close()

    def test_llm_gate_state_survives_bridge_reconstruction(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = [100.0]
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(
                    research,
                    discovery,
                    clock=lambda: now[0],
                )
                directive = ResearchDirective(
                    task_type="DISCOVER_NEW_SOURCE",
                    trigger_reason=ResearchTriggerReason.FRONTIER_EXHAUSTED,
                    strategy="META_SOURCE_SEARCH",
                    subject=None,
                    desired_regions=1,
                    reason="no deterministic frontier remains",
                    context_key="ctx:persistent-gate",
                )
                request = bridge.build_execution_request(directive)
                self.assertTrue(bridge.try_claim_llm_call(request))
                active, last_started, failures = bridge.llm_gate_state(
                    request.context_hash
                )
                self.assertEqual(active, request.call_identity)
                self.assertEqual(last_started, 100.0)
                self.assertEqual(failures, 0)

                now[0] = 101.0
                bridge.finish_llm_call(
                    request.call_identity,
                    success=False,
                    cost_seconds=1.0,
                    error="fixture failure",
                )

                # Reconstruct the integration object as a process restart would.
                reopened = ResearchIntegrationBridge(
                    research,
                    discovery,
                    clock=lambda: now[0],
                )
                active, last_started, failures = reopened.llm_gate_state(
                    request.context_hash
                )
                self.assertIsNone(active)
                self.assertEqual(last_started, 100.0)
                self.assertEqual(failures, 1)
            finally:
                control.close()

    async def test_closed_final_waits_for_structured_root_lineage(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                discovery = SourceDiscoveryRegistry(control)
                research = ResearchRegistry(control)
                bridge = ResearchIntegrationBridge(research, discovery)

                candidate, inserted = discovery.register_proposal(
                    SourceCandidate(
                        canonical_entrypoint="https://repo.example/race.cdxj",
                        source_family="CDXJ",
                        level=SourceLevel.SOURCE,
                        discovered_by="research-root:root:race",
                        discovery_strategy="structured_root_artifact",
                        direct_evidence_prior=1.0,
                        confidence=1.0,
                    )
                )
                self.assertTrue(inserted)

                research.enqueue_frontier(
                    FrontierTask(
                        task_id="frontier:race",
                        task_kind="PATH",
                        entity_id="query:race",
                        policy_version="policy:race",
                    )
                )
                decision = DecisionRecord(
                    decision_id="decision:race",
                    task_id="frontier:race",
                    arm_id="root:race",
                    policy_version="policy:race",
                    policy_snapshot_id="snapshot:race",
                    propensity=1.0,
                    context_hash="ctx:race",
                    chosen_at=1.0,
                )
                self.assertTrue(research.record_decision(decision))

                authority = ("baseline:race", "model:race")
                run = discovery.begin_source_run(
                    candidate.source_key,
                    reservoir_id="reservoir:race",
                    lease_id="lease:race",
                    baseline_signature=authority[0],
                    model_signature=authority[1],
                )
                discovery.record_source_run_read(
                    candidate.source_key,
                    reservoir_id="reservoir:race",
                    lease_id="lease:race",
                    baseline_signature=authority[0],
                    model_signature=authority[1],
                    source_records=1,
                    bytes_read=64,
                    source_requests=1,
                )
                discovery.record_source_run_validation(
                    candidate.source_key,
                    reservoir_id="reservoir:race",
                    lease_id="lease:race",
                    baseline_signature=authority[0],
                    model_signature=authority[1],
                    evidence_tasks_created=0,
                    evidence_tasks_terminal=0,
                    direct_capsules_committed=1,
                    provider_requests=0,
                    provider_elapsed_seconds=0.0,
                    accepted_host_years=1,
                    final_accepted_eed=3.0,
                    max_evidence_sequence=1,
                    validation_complete=True,
                )
                self.assertTrue(
                    discovery.close_source_run(
                        candidate.source_key,
                        reservoir_id="reservoir:race",
                        lease_id="lease:race",
                        baseline_signature=authority[0],
                        model_signature=authority[1],
                    )
                )

                # The source proposal is visible, but its causal artifact
                # lineage is intentionally delayed. FINAL must not seal early.
                self.assertEqual(bridge.sync_closed_final_rewards(), 0)
                projection_count = control.connection.execute(
                    "SELECT COUNT(*) FROM research_final_projection"
                ).fetchone()[0]
                self.assertEqual(projection_count, 0)

                lead = ArtifactLead(
                    root_id="root:race",
                    provider_native_id="race:file",
                    locator=candidate.canonical_entrypoint,
                    checksum="sha256:race",
                    source_node_id="node:race",
                    query_id="query:race",
                    program_id="program:race",
                    decision_id=decision.decision_id,
                )
                artifact_id, _lineage_id, created = (
                    research.register_artifact_lead(lead)
                )
                self.assertTrue(created)
                research.bind_artifact_source(
                    artifact_id,
                    source_key=candidate.source_key,
                )

                self.assertEqual(bridge.sync_closed_final_rewards(), 1)
                self.assertEqual(bridge.sync_closed_final_rewards(), 0)
                stats = research.rebuild_arm_stats(
                    policy_version="policy:race",
                    schema_version=2,
                )
                self.assertEqual(len(stats), 1)
                self.assertEqual(stats[0].final_observation_count, 1)
                self.assertEqual(stats[0].final_reward, 3.0)
                self.assertEqual(
                    len(
                        research.artifact_lineage_for_exposure(
                            source_key=candidate.source_key,
                            exposure_id=run.exposure_id or "",
                        )
                    ),
                    1,
                )
            finally:
                control.close()


if __name__ == "__main__":
    unittest.main()
