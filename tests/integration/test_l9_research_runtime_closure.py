from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_research.adapters.base import (
    ArtifactLead,
    RootQuery,
    SearchHit,
    SearchPage,
)
from creeper.source_research.integration import ResearchIntegrationBridge
from creeper.source_research.models import (
    DecisionRecord,
    QueryProgram,
    RootKind,
    RootSurface,
)
from creeper.source_research.registry import ResearchRegistry
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


if __name__ == "__main__":
    unittest.main()
