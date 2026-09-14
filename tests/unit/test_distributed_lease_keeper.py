from __future__ import annotations

import asyncio
import time
import unittest

from creeper.distributed.coordinator_client import CoordinatorError
from creeper.distributed.lease_keeper import LeaseKeeper, LeaseLostError
from creeper.distributed.models import (
    Capability,
    TaskClass,
    TaskLease,
    WorkDefinition,
)


class FailingCoordinator:
    async def renew(self, _lease, *, lease_seconds: float):
        raise CoordinatorError("fixture authority unavailable")


class DistributedLeaseKeeperTests(unittest.IsolatedAsyncioTestCase):
    async def test_authority_transport_loss_marks_lease_lost_before_expiry(self) -> None:
        now = time.time()
        lease = TaskLease(
            task_id="task-a",
            work_key="work-a",
            worker_id="worker-a",
            generation=1,
            lease_deadline=now + 0.20,
            attempt=1,
            work=WorkDefinition(
                producer="HistoricalQueryProducer",
                task_class=TaskClass.HOST_BATCH,
                input_identity="example.com",
                coverage={"year_from": 1996, "year_to": 2001},
                partition="0",
                algorithm_version="v1",
                required_capabilities=(Capability.ONLINE_QUERY.value,),
            ),
        )
        keeper = LeaseKeeper(
            FailingCoordinator(),
            lease,
            lease_seconds=0.20,
            renew_fraction=0.25,
            min_renew_interval=0.01,
        )

        async with keeper:
            await asyncio.sleep(0.08)
            self.assertTrue(keeper.lost)
            with self.assertRaises(LeaseLostError):
                keeper.assert_owned()

    async def test_stopped_keeper_does_not_renew_after_clean_completion(self) -> None:
        class CountingCoordinator:
            def __init__(self) -> None:
                self.renewals = 0

            async def renew(self, lease, *, lease_seconds: float):
                self.renewals += 1
                return lease

        coordinator = CountingCoordinator()
        now = time.time()
        lease = TaskLease(
            task_id="task-b",
            work_key="work-b",
            worker_id="worker-a",
            generation=1,
            lease_deadline=now + 30,
            attempt=1,
            work=WorkDefinition(
                producer="TestProducer",
                task_class=TaskClass.HOST_BATCH,
                input_identity="example.com",
                coverage={"year_from": 1996, "year_to": 2001},
                partition="0",
                algorithm_version="v1",
                required_capabilities=(Capability.ONLINE_QUERY.value,),
            ),
        )
        keeper = LeaseKeeper(
            coordinator,
            lease,
            lease_seconds=30,
            min_renew_interval=0.01,
        )
        async with keeper:
            keeper.assert_owned()
        await asyncio.sleep(0)
        self.assertEqual(coordinator.renewals, 0)


if __name__ == "__main__":
    unittest.main()
