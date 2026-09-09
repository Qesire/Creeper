import queue
import unittest

from creeper.runtime.queues import BoundedQueues


class BoundedQueuesTests(unittest.TestCase):
    def test_named_queues_have_configured_capacities(self):
        queues = BoundedQueues(
            source_records=1,
            observations=2,
            evidence_tasks=3,
            commits=4,
        )

        self.assertEqual(queues.source_records.maxsize, 1)
        self.assertEqual(queues.observations.maxsize, 2)
        self.assertEqual(queues.evidence_tasks.maxsize, 3)
        self.assertEqual(queues.commits.maxsize, 4)

    def test_source_record_queue_is_bounded(self):
        queues = BoundedQueues(
            source_records=1,
            observations=1,
            evidence_tasks=1,
            commits=1,
        )
        queues.source_records.put_nowait("first")

        with self.assertRaises(queue.Full):
            queues.source_records.put_nowait("second")


if __name__ == "__main__":
    unittest.main()
