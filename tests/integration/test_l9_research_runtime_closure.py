from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import httpx

from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.research_trigger import (
    ResearchDirective,
    ResearchTriggerReason,
)
from creeper.source_research.adapters.base import (
    ArtifactLead,
    RootQuery as AdapterRootQuery,
    SearchHit,
    SearchPage,
)
from creeper.source_research.integration import ResearchIntegrationBridge
from creeper.source_research.models import (
    DecisionRecord,
    FrontierState,
    FrontierTask,
    QueryProgram,
    QueryState,
    RootKind,
    RootQuery,
    RootSurface,
)
from creeper.source_research.registry import ResearchRegistry
from creeper.source_discovery_service import _root_query_runtime_adapters
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


class L9ResearchRuntimeClosureTests(unittest.IsolatedAsyncioTestCase):
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

                query = RootQuery(
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
                                            "https://objects.example/history.cdxj"
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

                    # Terminal durable work is not replayed on the next cycle.
                    self.assertEqual(planner(), ())
                    self.assertEqual(len(calls), 1)
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
