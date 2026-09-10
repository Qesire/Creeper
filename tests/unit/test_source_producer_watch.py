from __future__ import annotations

from threading import Event
import unittest
from unittest.mock import Mock

from creeper.runtime.source_producer import SourceProducer, SourceProducerReport


class SourceProducerWatchTests(unittest.TestCase):
    def test_watch_accumulates_work_and_stops_after_idle(self) -> None:
        producer = object.__new__(SourceProducer)
        producer.run_once = Mock(
            side_effect=[
                SourceProducerReport(leases_succeeded=1, source_records=3),
                SourceProducerReport(admission_blocked=True),
            ]
        )
        stop = Event()
        sleeps: list[float] = []

        report = producer.run_forever(
            stop_event=stop,
            idle_backoff_seconds=0.25,
            max_idle_backoff_seconds=1.0,
            sleep_fn=lambda seconds: (sleeps.append(seconds), stop.set()),
        )

        self.assertEqual(report.leases_succeeded, 1)
        self.assertEqual(report.source_records, 3)
        self.assertTrue(report.admission_blocked)
        self.assertEqual(sleeps, [0.25])
        self.assertEqual(producer.run_once.call_count, 2)

    def test_watch_does_not_claim_work_when_stop_is_already_set(self) -> None:
        producer = object.__new__(SourceProducer)
        producer.run_once = Mock()
        stop = Event()
        stop.set()

        report = producer.run_forever(stop_event=stop, sleep_fn=Mock())

        self.assertEqual(report.leases_succeeded, 0)
        producer.run_once.assert_not_called()


if __name__ == "__main__":
    unittest.main()
