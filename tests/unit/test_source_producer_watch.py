from __future__ import annotations

from threading import Event
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from creeper.runtime.source_producer import SourceProducer, SourceProducerReport
from creeper.storage.control_store import ControlStore


class SourceProducerWatchTests(unittest.TestCase):
    def make_producer(self, **overrides):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        control = ControlStore(Path(tmp.name) / "control.sqlite3")
        self.addCleanup(control.close)
        values = dict(
            baseline=Mock(),
            control_store=control,
            evidence_store=Mock(),
            scheduler=Mock(),
            candidates=(),
            adapters={},
            backlog_capacities={"wayback": 1},
            queue_capacities={},
        )
        values.update(overrides)
        return SourceProducer(**values)

    def test_constructor_rejects_lossy_or_nonfinite_configuration(self) -> None:
        cases = (
            ({"domain_fanout_min_children": 4.5}, "positive integer"),
            ({"rdap_batch_size": 1.5}, "positive integer"),
            ({"baseline_batch_size": True}, "positive integer"),
            ({"reservation_grace_seconds": float("nan")}, "finite and non-negative"),
            ({"range_first_fraction": float("nan")}, "finite within"),
        )
        for overrides, pattern in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ValueError, pattern):
                    self.make_producer(**overrides)

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
