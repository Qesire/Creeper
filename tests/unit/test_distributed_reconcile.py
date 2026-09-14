from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.distributed.authority_store import DistributedAuthorityStore
from creeper.distributed.models import (
    Capability,
    ResultBatch,
    WorkerDescriptor,
)
from creeper.distributed.reconcile import AuthorityReconciler


class DistributedReconcileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = DistributedAuthorityStore(
            Path(self.tmp.name) / "authority.sqlite3"
        )
        self.worker = WorkerDescriptor(
            worker_id="discovery-worker",
            runtime_class="vm",
            region="test",
            architecture="x86_64",
            memory_bytes=1024**3,
            cpu_count=2,
            network_class="public",
            capabilities=(Capability.WEB_DISCOVERY.value,),
            allowed_providers=("web_discovery",),
        )
        self.store.register_worker(self.worker)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def _commit_candidates(self) -> str:
        task_id = self.store.admit_source_page_work(
            url="https://sources.test/index.html"
        )
        lease = self.store.claim_work(self.worker.worker_id, lease_seconds=60)
        assert lease is not None
        self.store.commit_result_batch(
            ResultBatch(
                task_id=lease.task_id,
                generation=lease.generation,
                sequence_no=lease.next_sequence_no,
                results=(
                    {
                        "kind": "SOURCE_CANDIDATE",
                        "url": "https://data.test/archive.cdxj.gz",
                        "candidate_type": "bulk_artifact",
                        "parser_kind": "cdxj",
                        "referrer_url": "https://sources.test/index.html",
                    },
                    {
                        "kind": "SOURCE_CANDIDATE",
                        "url": "https://data.test/archive.warc.gz",
                        "candidate_type": "bulk_artifact",
                        "parser_kind": "warc_arc",
                        "referrer_url": "https://sources.test/index.html",
                    },
                    {
                        "kind": "SOURCE_CANDIDATE",
                        "url": "https://data.test/more.html",
                        "candidate_type": "source_page",
                        "parser_kind": "",
                        "referrer_url": "https://sources.test/index.html",
                    },
                ),
                cursor_after="EOF",
            ),
            worker_id=lease.worker_id,
        )
        return task_id

    def test_default_reconciler_promotes_direct_bulk_only(self) -> None:
        self._commit_candidates()
        report = AuthorityReconciler(
            self.store,
            promotion_batch_size=16,
            include_source_pages=False,
        ).run_once()

        self.assertEqual(report.promoted, 2)
        self.assertEqual(report.admitted, 1)
        self.assertEqual(report.held, 1)
        self.assertEqual(report.errors, 0)

        rows = {
            str(row["canonical_url"]): row
            for row in self.store.source_candidate_rows()
        }
        cdxj = rows["https://data.test/archive.cdxj.gz"]
        self.assertEqual(cdxj["state"], "ADMITTED")
        task_id = str(cdxj["admitted_task_id"])
        task = self.store.task_row(task_id)
        self.assertEqual(task["producer"], "BulkHistoricalIndexProducer")
        self.assertEqual(task["task_class"], "SOURCE_SHARD")

        warc = rows["https://data.test/archive.warc.gz"]
        self.assertEqual(warc["state"], "HELD_UNSUPPORTED")
        self.assertIn("not yet authorized", str(warc["last_error"]))

        page = rows["https://data.test/more.html"]
        self.assertEqual(page["state"], "DISCOVERED")
        self.assertIsNone(page["admitted_task_id"])

    def test_explicit_page_promotion_is_idempotent(self) -> None:
        self._commit_candidates()
        reconciler = AuthorityReconciler(
            self.store,
            promotion_batch_size=16,
            include_source_pages=True,
        )
        first = reconciler.run_once()
        second = reconciler.run_once()

        self.assertEqual(first.admitted, 2)
        self.assertEqual(first.held, 1)
        self.assertEqual(second.promoted, 0)
        page = next(
            row
            for row in self.store.source_candidate_rows()
            if row["canonical_url"] == "https://data.test/more.html"
        )
        self.assertEqual(page["state"], "ADMITTED")
        page_task = self.store.task_row(str(page["admitted_task_id"]))
        self.assertEqual(page_task["producer"], "SourceDiscoveryProducer")


if __name__ == "__main__":
    unittest.main()
