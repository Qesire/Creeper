from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.distributed.coordinator_client import (
    CoordinatorTransportError,
    StaleLeaseCoordinatorError,
)
from creeper.distributed.models import (
    Capability,
    ResultBatch,
    TaskClass,
    WorkDefinition,
    WorkerDescriptor,
)
from creeper.distributed.worker import DistributedWorker
from creeper.distributed.worker_spool import WorkerResultSpool


class _Client:
    def __init__(self, *, stale: bool = False, transport: bool = False) -> None:
        self.stale = stale
        self.transport = transport
        self.registered = []
        self.committed = []

    async def register(self, descriptor) -> None:
        self.registered.append(descriptor)

    async def commit_batch(self, batch) -> str:
        self.committed.append(batch)
        if self.transport:
            raise CoordinatorTransportError("offline")
        if self.stale:
            raise StaleLeaseCoordinatorError("fenced")
        return "ALREADY_COMMITTED"


class _Producer:
    async def run(self, lease, context):  # pragma: no cover - initialize-only fixture
        if False:
            yield lease


def _descriptor(instance: str) -> WorkerDescriptor:
    return WorkerDescriptor(
        worker_id="worker-a",
        worker_instance_id=instance,
        runtime_class="full",
        region="sg",
        architecture="x86_64",
        memory_bytes=8 * 1024**3,
        cpu_count=4,
        network_class="public",
        capabilities=(Capability.RESIDUAL_QUERY.value,),
        producers=("fixture",),
        allowed_providers=("datacite",),
    )


def _batch(*, generation: int = 1, sequence: int = 0) -> ResultBatch:
    return ResultBatch(
        task_id="task-a",
        generation=generation,
        sequence_no=sequence,
        results=({"kind": "fixture"},),
        cursor_after=str(sequence + 1),
    )


class WorkerSpoolRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "worker-spool.sqlite3"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_auto_instance_reuses_prior_incarnation_only_with_pending_outbox(self) -> None:
        spool = WorkerResultSpool(self.path)
        try:
            self.assertEqual(
                spool.resolve_worker_instance_id("instance-1", auto=True),
                "instance-1",
            )
            spool.put(_batch())
        finally:
            spool.close()

        reopened = WorkerResultSpool(self.path)
        try:
            self.assertEqual(
                reopened.resolve_worker_instance_id("instance-2", auto=True),
                "instance-1",
            )
            self.assertEqual(reopened.pending_count(), 1)
            reopened.ack(_batch().batch_id)
            self.assertEqual(
                reopened.resolve_worker_instance_id("instance-3", auto=True),
                "instance-3",
            )
        finally:
            reopened.close()

    def test_explicit_instance_never_reuses_spool_identity(self) -> None:
        spool = WorkerResultSpool(self.path)
        try:
            spool.resolve_worker_instance_id("old", auto=True)
            spool.put(_batch())
            self.assertEqual(
                spool.resolve_worker_instance_id("explicit-new", auto=False),
                "explicit-new",
            )
        finally:
            spool.close()

    async def test_initialize_replays_and_acks_pending_batch(self) -> None:
        spool = WorkerResultSpool(self.path)
        spool.put(_batch())
        client = _Client()
        worker = DistributedWorker(
            client,
            _descriptor("instance-1"),
            {"fixture": _Producer()},
            spool,
        )
        try:
            await worker.initialize()
            self.assertEqual(len(client.registered), 1)
            self.assertEqual(len(client.committed), 1)
            self.assertEqual(spool.pending(), ())
        finally:
            spool.close()

    async def test_initialize_discards_fenced_generation_only(self) -> None:
        spool = WorkerResultSpool(self.path)
        spool.put(_batch(generation=1, sequence=0))
        spool.put(_batch(generation=2, sequence=1))
        client = _Client(stale=True)
        worker = DistributedWorker(
            client,
            _descriptor("instance-1"),
            {"fixture": _Producer()},
            spool,
        )
        try:
            await worker.initialize()
            self.assertEqual(len(client.committed), 2)
            self.assertEqual(spool.pending(), ())
        finally:
            spool.close()

    async def test_initialize_keeps_outbox_on_transport_failure(self) -> None:
        spool = WorkerResultSpool(self.path)
        spool.put(_batch())
        client = _Client(transport=True)
        worker = DistributedWorker(
            client,
            _descriptor("instance-1"),
            {"fixture": _Producer()},
            spool,
        )
        try:
            with self.assertRaises(CoordinatorTransportError):
                await worker.initialize()
            self.assertEqual(len(spool.pending()), 1)
        finally:
            spool.close()


if __name__ == "__main__":
    unittest.main()
