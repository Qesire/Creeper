from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from creeper.distributed.authority_store import DistributedAuthorityStore
from creeper.distributed.models import (
    Capability,
    ResultBatch,
    WorkerDescriptor,
)
from creeper.distributed.residual_query import (
    DistributedResidualBridge,
    PRODUCER_NAME,
    serialize_raw_result,
)
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


class DistributedResidualBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp=tempfile.TemporaryDirectory()
        root=Path(self.tmp.name)
        self.control=ControlStore(root/"control.sqlite3")
        self.registry=SourceDiscoveryRegistry(self.control)
        self.coverage=ResidualSearchLedger(self.registry.connection)
        self.identities=SearchIdentityLedger(self.registry.connection)
        self.fabric=DistributedAuthorityStore(root/"fabric.sqlite3")
        self.providers=(
            "datacite",
            "zenodo",
            "harvard_dataverse",
            "internet_archive",
        )
        self.fabric.register_worker(
            WorkerDescriptor(
                worker_id="worker",
                worker_instance_id="instance",
                runtime_class="full",
                region="sg",
                architecture="x86_64",
                memory_bytes=8*1024**3,
                cpu_count=4,
                network_class="public",
                capabilities=(Capability.RESIDUAL_QUERY.value,),
                producers=(PRODUCER_NAME,),
                allowed_providers=self.providers,
            )
        )
        self.policy=DeterministicSearchPolicy(
            results_per_provider=10,
            max_total_results=40,
            min_relevance_score=0.55,
            timeout_seconds=10.0,
        )
        self.bridge=DistributedResidualBridge(
            self.fabric,
            self.registry,
            self.coverage,
            self.identities,
            policy=self.policy,
            providers=self.providers,
            candidate_cap=8,
        )
        self.cell=SearchCell(
            mechanism="proxy_access",
            institution="university",
            period="1998",
            artifact="trace",
        )
        self.coverage.ensure_cell(self.cell)
        self.plan=QueryPlan(
            cell=self.cell,
            query='"1998" "proxy" "university" "trace"',
            variant=0,
            exclusions=(),
            score=2.0,
            mechanism_phrase="proxy",
            include_institution=True,
            query_shape="STRICT_4D",
        )

    def tearDown(self) -> None:
        self.fabric.close()
        self.control.close()
        self.tmp.cleanup()

    def _complete_remote_task(self) -> str:
        task_id,inserted=self.bridge.submit(self.plan)
        self.assertTrue(inserted)
        lease=self.fabric.claim_work("worker","instance",lease_seconds=60)
        self.assertIsNotNone(lease)
        assert lease is not None
        raw=RawSearchResult(
            provider="datacite",
            provider_result_id="fixture:1",
            url="https://repo.example/proxy98.zip",
            title="1998 University Proxy Trace Dataset",
            description="HTTP proxy access trace",
            publisher="Example University",
            publication_year=1998,
            resource_type="Dataset",
        )
        batch=ResultBatch(
            task_id=task_id,
            generation=lease.generation,
            sequence_no=0,
            results=(
                serialize_raw_result(raw),
                {
                    "kind":"RESIDUAL_SUMMARY",
                    "backend":"+",
                    "query":self.plan.query,
                    "search_cost_seconds":0.25,
                    "provider_count":4,
                },
            ),
            final=True,
        )
        self.fabric.commit_result_batch(
            batch,
            worker_id="worker",
            worker_instance_id="instance",
        )
        return batch.batch_id

    def test_remote_raw_results_are_reclassified_and_atomically_committed(self) -> None:
        self._complete_remote_task()

        report=self.bridge.drain()

        self.assertEqual(report.failed,0)
        self.assertEqual(report.quarantined,0)
        self.assertEqual(len(report.committed),1)
        stats=self.coverage.stats(self.cell)
        self.assertEqual(stats.variant_cursor,1)
        self.assertEqual(stats.attempts,1)
        self.assertEqual(stats.result_count,1)
        self.assertEqual(len(self.registry.list_candidates()),1)
        self.assertEqual(self.fabric.unconsumed_batches(),())

    def test_domain_replay_uses_batch_idempotency_key(self) -> None:
        batch_id=self._complete_remote_task()
        first=self.bridge.drain()
        self.assertEqual(len(first.committed),1)
        stats=self.coverage.stats(self.cell)
        self.assertEqual(stats.variant_cursor,1)

        with self.fabric.connection:
            self.fabric.connection.execute(
                """
                UPDATE fabric_result_batches
                SET consumed_at=NULL, consume_attempts=0,
                    consume_error=NULL, quarantined=0
                WHERE batch_id=?
                """,
                (batch_id,),
            )
        second=self.bridge.drain()

        self.assertEqual(len(second.committed),1)
        replay_stats=self.coverage.stats(self.cell)
        self.assertEqual(replay_stats.variant_cursor,1)
        self.assertEqual(replay_stats.attempts,1)
        self.assertEqual(
            self.registry.connection.execute(
                "SELECT COUNT(*) AS n FROM source_search_episodes"
            ).fetchone()["n"],
            1,
        )

    def test_malformed_result_is_recorded_as_consume_failure(self) -> None:
        task_id,_=self.bridge.submit(self.plan)
        lease=self.fabric.claim_work("worker","instance",lease_seconds=60)
        assert lease is not None
        batch=ResultBatch(
            task_id=task_id,
            generation=lease.generation,
            sequence_no=0,
            results=({"kind":"UNKNOWN"},),
            final=True,
        )
        self.fabric.commit_result_batch(
            batch,
            worker_id="worker",
            worker_instance_id="instance",
        )

        report=self.bridge.drain()

        self.assertEqual(report.failed,1)
        self.assertEqual(report.committed,())
        row=self.fabric.connection.execute(
            """
            SELECT consume_attempts, consume_error, quarantined
            FROM fabric_result_batches
            WHERE batch_id=?
            """,
            (batch.batch_id,),
        ).fetchone()
        self.assertEqual(row["consume_attempts"],1)
        self.assertIn("unexpected Fabric residual result kind",row["consume_error"])
        self.assertEqual(row["quarantined"],0)


if __name__=="__main__":
    unittest.main()
