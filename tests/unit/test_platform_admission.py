from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.evidence.platform_admission import (
    PlatformYearAdmission,
    PlatformYearAdmissionPolicy,
    PlatformYearObservation,
)
from creeper.storage.control_store import ControlStore


class PlatformYearAdmissionTests(unittest.TestCase):
    def test_admits_scoped_source_index_observation_with_separate_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            admission = PlatformYearAdmission(
                control,
                policy=PlatformYearAdmissionPolicy(max_tasks=1),
            )
            first = PlatformYearObservation(
                provider="wayback",
                subject="example.com",
                target_year=1997,
                request_template_hash="template-1997",
                policy_version="platform-v1",
                source_key="source:example",
                reservoir_id="reservoir:example",
                authority_digest="authority-v1",
            )
            second = PlatformYearObservation(
                provider="wayback",
                subject="example.net",
                target_year=1998,
                request_template_hash="template-1998",
                policy_version="platform-v1",
                source_key="source:other",
                reservoir_id="reservoir:other",
                authority_digest="authority-v1",
            )

            report = admission.admit([first, second])

            self.assertEqual(report.admitted, 1)
            self.assertEqual(report.blocked, 1)
            self.assertEqual(len(report.tasks), 1)
            task = report.tasks[0]
            self.assertEqual(task.source_key, first.source_key)
            self.assertEqual(task.reservoir_id, first.reservoir_id)
            self.assertTrue(task.exposure_id)
            self.assertEqual(task.authority_digest, first.authority_digest)
            control.close()

    def test_missing_scope_or_authority_is_rejected(self):
        cases = (
            {"source_key": "", "reservoir_id": "reservoir", "authority_digest": "a"},
            {"source_key": "source", "reservoir_id": "", "authority_digest": "a"},
            {"source_key": "source", "reservoir_id": "reservoir", "authority_digest": ""},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                values = {
                    "provider": "wayback",
                    "subject": "example.com",
                    "target_year": 1997,
                    "request_template_hash": "template",
                    "policy_version": "platform-v1",
                    **overrides,
                }
                with self.assertRaises(ValueError):
                    PlatformYearObservation(**values)

    def test_repeated_admission_is_idempotent_and_does_not_consume_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            admission = PlatformYearAdmission(
                control,
                policy=PlatformYearAdmissionPolicy(max_tasks=1),
            )
            observation = PlatformYearObservation(
                provider="wayback",
                subject="example.com",
                target_year=1997,
                request_template_hash="template",
                policy_version="platform-v1",
                source_key="source:example",
                reservoir_id="reservoir:example",
                authority_digest="authority-v1",
            )

            first = admission.admit([observation])
            second = admission.admit([observation])

            self.assertEqual(first.admitted, 1)
            self.assertEqual(second.admitted, 0)
            self.assertEqual(second.idempotent, 1)
            self.assertEqual(len(control.list_platform_year_harvests()), 1)
            control.close()


if __name__ == "__main__":
    unittest.main()
