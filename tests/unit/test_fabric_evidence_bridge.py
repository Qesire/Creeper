from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.distributed.authority_store import DistributedAuthorityStore
from creeper.distributed.evidence_query import (
    DistributedEvidenceBridge,
    deserialize_result,
    serialize_result,
)
from creeper.distributed.models import (
    Capability,
    ResultBatch,
    WorkerDescriptor,
)
from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
    TemporalScope,
)
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_queue import DurableEvidenceQueue
from creeper.storage.evidence_store import EvidenceStore


class FabricEvidenceBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.fabric=DistributedAuthorityStore(self.root/"fabric.sqlite3")
        self.control=ControlStore(self.root/"control.sqlite3")
        self.evidence=EvidenceStore(self.root/"evidence.sqlite3")
        self.bridge=DistributedEvidenceBridge(
            self.fabric,
            self.control,
            self.evidence,
            owner="fabric:evidence:test",
            lease_seconds=300.0,
            retry_base_seconds=0.0,
            retry_max_seconds=0.0,
        )
        self.worker=WorkerDescriptor(
            worker_id="worker-evidence",
            worker_instance_id="instance-1",
            runtime_class="full",
            region="sg",
            architecture="x86_64",
            memory_bytes=8*1024**3,
            cpu_count=4,
            network_class="public",
            capabilities=(
                Capability.EVIDENCE_QUERY.value,
                Capability.RDAP.value,
            ),
            producers=("EvidenceQueryProducer",),
            allowed_providers=("rdap",),
        )
        self.fabric.register_worker(self.worker)
        self.fabric.configure_provider_budget(
            "rdap",
            requests_per_second=10.0,
            max_global_inflight=4,
            require_qualified_region=False,
        )

    def tearDown(self) -> None:
        self.evidence.close()
        self.control.close()
        self.fabric.close()
        self.tmp.cleanup()

    @staticmethod
    def key() -> EvidenceQueryKey:
        return EvidenceQueryKey(
            "example.com",
            TemporalScope(1998,1998),
            "rdap",
            "rdap-v1",
        )

    @staticmethod
    def capsule() -> EvidenceCapsule:
        return EvidenceCapsule(
            hostname="example.com",
            year=1998,
            provider="rdap",
            temporal_semantics="registration_event_year",
            evidence_timestamp="1998-02-03T00:00:00Z",
            source_locator="https://rdap.example/domain/example.com",
            payload_hash="a"*64,
            policy_version="rdap-v1",
            evidence_type="rdap_registration_event",
            source_id="rdap",
            original_url="https://rdap.example/domain/example.com",
            record_locator="rdap:example.com:registration",
            extraction_method="rdap_registration_event",
        )

    def _dispatch_and_claim(self):
        key=self.key()
        self.control.enqueue_evidence_tasks([key])
        self.assertEqual(
            self.bridge.dispatch(limit=1,providers=("rdap",)),
            1,
        )
        lease=self.fabric.claim_work(
            self.worker.worker_id,
            self.worker.worker_instance_id,
            lease_seconds=60.0,
        )
        self.assertIsNotNone(lease)
        assert lease is not None
        return key,lease

    def test_remote_result_commits_through_central_evidence_authority(self) -> None:
        key,lease=self._dispatch_and_claim()
        result=EvidenceQueryResult(
            hostname=key.hostname,
            year=1998,
            state=CDXQueryState.PASS,
            capsule=self.capsule(),
            key=key,
            provider_requests=1,
            provider_elapsed_milliseconds=25,
            pages_seen=1,
            records_seen=1,
        )
        batch=ResultBatch(
            task_id=lease.task_id,
            generation=lease.generation,
            sequence_no=lease.next_sequence_no,
            results=(serialize_result(result),),
            cursor_after="EOF",
            final=True,
        )
        self.fabric.commit_result_batch(
            batch,
            worker_id=self.worker.worker_id,
            worker_instance_id=self.worker.worker_instance_id,
        )

        report=self.bridge.drain()

        self.assertEqual(report.committed,1)
        self.assertEqual(report.terminal,1)
        self.assertEqual(report.inserted_capsules,1)
        self.assertEqual(self.evidence.count(),1)
        stored=self.control.get_evidence_task(key)
        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored.state,CDXQueryState.PASS.value)
        self.assertIsNone(stored.lease_owner)
        self.assertEqual(self.fabric.unconsumed_batches(),())
        # Lost-domain-ACK replay becomes a no-op because the Fabric inbox row is
        # already consumed and EvidenceStore proof is durable.
        self.assertEqual(self.bridge.drain().committed,0)
        self.assertEqual(self.evidence.count(),1)

    def test_stale_remote_attempt_cannot_commit_evidence(self) -> None:
        key,lease=self._dispatch_and_claim()
        current=self.control.get_evidence_task(key)
        assert current is not None
        self.control.finish_evidence_task(
            key,
            CDXQueryState.TRANSIENT_ERROR,
            owner="fabric:evidence:test",
            retry_at=0.0,
        )
        queue=DurableEvidenceQueue(self.control)
        replacement=queue.claim(
            owner="other-worker",
            limit=1,
            providers=("rdap",),
            lease_seconds=300.0,
        )
        self.assertEqual(len(replacement),1)
        self.assertEqual(replacement[0].attempt,current.attempt+1)

        stale=EvidenceQueryResult(
            hostname=key.hostname,
            year=1998,
            state=CDXQueryState.PASS,
            capsule=self.capsule(),
            key=key,
        )
        self.fabric.commit_result_batch(
            ResultBatch(
                task_id=lease.task_id,
                generation=lease.generation,
                sequence_no=lease.next_sequence_no,
                results=(serialize_result(stale),),
                final=True,
            ),
            worker_id=self.worker.worker_id,
            worker_instance_id=self.worker.worker_instance_id,
        )

        report=self.bridge.drain()

        self.assertEqual(report.stale,1)
        self.assertEqual(report.committed,0)
        self.assertEqual(self.evidence.count(),0)
        self.assertEqual(
            self.control.get_evidence_task(key).lease_owner,
            "other-worker",
        )


class FabricEvidenceWireTests(unittest.TestCase):
    def test_range_result_round_trip_preserves_authority_fields(self) -> None:
        key=EvidenceQueryKey(
            "example.org",
            TemporalScope(1998,2000),
            "wayback",
            "cdx-v1",
        )
        capsule=EvidenceCapsule(
            hostname="example.org",
            year=1999,
            provider="wayback",
            temporal_semantics="capture_timestamp_year",
            evidence_timestamp="19990102030405",
            source_locator="https://example.org/a",
            payload_hash="b"*64,
            policy_version="cdx-v1",
            evidence_type="exact_host_cdx_capture",
            source_id="arquivo",
            original_url="https://example.org/a",
            record_locator="arquivo:row:1",
            extraction_method="cdx_row",
        )
        result=RangeEvidenceQueryResult(
            hostname="example.org",
            key=key,
            state=CDXQueryState.PASS,
            candidate_years=(1999,),
            capsules=(capsule,),
            pages_seen=2,
            records_seen=10,
            provider_requests=2,
            provider_elapsed_milliseconds=50,
        )

        restored=deserialize_result(serialize_result(result))

        self.assertEqual(restored,result)


if __name__=="__main__":
    unittest.main()
