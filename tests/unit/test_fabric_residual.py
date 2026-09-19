from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.fabric.bridge import residual_search_dag
from creeper.fabric.models import (
    FabricCapability,
    FabricTaskState,
    ResultBatch,
    WorkerDescriptor,
)
from creeper.fabric.residual import ResidualAuthorityReducer, raw_result_payload
from creeper.fabric.store import SQLiteFabricStore
from creeper.source_discovery.deterministic_search import DeterministicSearchPolicy
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.residual_search import (
    QueryPlan,
    ResidualSearchLedger,
    SearchCell,
)
from creeper.source_discovery.search_identity import (
    RawSearchResult,
    SearchIdentityLedger,
)
from creeper.storage.control_store import ControlStore


class FabricResidualDagTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.control = ControlStore(root / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.coverage = ResidualSearchLedger(self.registry.connection)
        self.identities = SearchIdentityLedger(self.registry.connection)
        self.fabric = SQLiteFabricStore(root / "fabric.sqlite3")
        self.cell = SearchCell(
            mechanism="proxy_access",
            institution="university",
            period="1998",
            artifact="trace",
        )
        self.coverage.ensure_cell(self.cell)
        self.plan = QueryPlan(
            cell=self.cell,
            query='"1998" "proxy" "university" "trace"',
            variant=0,
            exclusions=(),
            score=5.0,
            mechanism_phrase="proxy",
            include_institution=True,
            query_shape="STRICT_4D",
        )

    def tearDown(self) -> None:
        self.fabric.close()
        self.control.close()
        self.tmp.cleanup()

    @staticmethod
    def worker(worker_id: str, provider: str) -> WorkerDescriptor:
        return WorkerDescriptor(
            worker_id=worker_id,
            region="test",
            runtime_class="fixture",
            architecture="x86_64",
            network_class="public",
            cpu_count=1,
            memory_bytes=1024**3,
            capabilities=(FabricCapability.SEARCH_STRUCTURED,),
            allowed_providers=(provider,),
            max_concurrency=1,
            edition="test",
        )

    def _finish_provider(
        self,
        worker: WorkerDescriptor,
        provider: str,
        result: RawSearchResult,
        cost: float,
    ) -> None:
        self.fabric.register_worker(worker)
        lease = self.fabric.claim(
            worker.worker_id,
            queue="network",
            lease_seconds=60,
        )
        self.assertIsNotNone(lease)
        assert lease is not None
        self.assertEqual(lease.work.provider, provider)
        batch = ResultBatch(
            task_id=lease.task_id,
            lease_epoch=lease.lease_epoch,
            sequence_no=0,
            results=(
                {
                    "kind": "RESIDUAL_PROVIDER_RESULT",
                    "provider": provider,
                    "search_cost_seconds": cost,
                    "results": [raw_result_payload(result)],
                },
            ),
            cursor_after={"done": True},
        )
        self.fabric.commit_batch(lease, batch)
        self.fabric.complete(lease)

    def test_two_provider_fanout_reduces_once_after_barrier(self) -> None:
        dag = residual_search_dag(
            self.plan,
            ("datacite", "zenodo"),
        )
        for work in dag.provider_slices:
            self.fabric.submit_work(work)
        reducer_task_id = self.fabric.submit_work(dag.reducer)

        reducer = ResidualAuthorityReducer(
            self.fabric,
            self.registry,
            self.coverage,
            self.identities,
            policy=DeterministicSearchPolicy(
                results_per_provider=10,
                max_total_results=10,
                min_relevance_score=0.55,
            ),
            candidate_cap=8,
        )
        reducer_worker = reducer.authority_worker_descriptor()
        self.fabric.register_worker(reducer_worker)

        # No provider has completed, so the authority reducer cannot claim.
        self.assertIsNone(
            self.fabric.claim(
                reducer_worker.worker_id,
                queue="authority",
                lease_seconds=60,
            )
        )

        datacite = RawSearchResult(
            provider="datacite",
            provider_result_id="doi:proxy98",
            url="https://repo.example/proxy98.txt",
            title="1998 university proxy trace dataset",
            publisher="Example University",
            resource_type="Dataset",
        )
        zenodo = RawSearchResult(
            provider="zenodo",
            provider_result_id="z:proxy98",
            url="https://mirror.example/proxy98.log",
            title="1998 proxy access trace dataset",
            publisher="Zenodo",
            resource_type="file",
        )
        self._finish_provider(
            self.worker("worker-datacite", "datacite"),
            "datacite",
            datacite,
            1.25,
        )
        self.assertIsNone(
            self.fabric.claim(
                reducer_worker.worker_id,
                queue="authority",
                lease_seconds=60,
            )
        )
        self._finish_provider(
            self.worker("worker-zenodo", "zenodo"),
            "zenodo",
            zenodo,
            2.50,
        )

        result = reducer.run_once()

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.registered_count, 2)
        stats = self.coverage.stats(self.cell)
        self.assertEqual(stats.attempts, 1)
        self.assertEqual(stats.variant_cursor, 1)
        self.assertAlmostEqual(stats.search_cost_seconds, 3.75)
        self.assertEqual(len(self.registry.list_candidates()), 2)
        self.assertEqual(
            self.fabric.task_row(reducer_task_id)["state"],
            FabricTaskState.SUCCEEDED.value,
        )
        self.assertEqual(
            self.registry.connection.execute(
                """
                SELECT COUNT(*) AS n
                FROM residual_search_external_commits
                """
            ).fetchone()["n"],
            1,
        )

    def test_residual_dag_identity_changes_with_provider_set(self) -> None:
        two = residual_search_dag(self.plan, ("datacite", "zenodo"))
        three = residual_search_dag(
            self.plan,
            ("datacite", "zenodo", "harvard_dataverse"),
        )

        self.assertNotEqual(two.reducer.work_key, three.reducer.work_key)
        self.assertEqual(
            set(two.reducer.dependency_work_keys),
            {item.work_key for item in two.provider_slices},
        )
        self.assertEqual(
            [item.work.provider if hasattr(item, "work") else item.provider for item in ()],
            [],
        )


if __name__ == "__main__":
    unittest.main()
