import json
import tempfile
import tomllib
import unittest
from pathlib import Path
from threading import Event
from unittest.mock import patch

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.source_cli import (
    ActivatedSourceRuntime,
    SourceProducerMode,
    _watch_loop,
    parse_source_producer_intent,
    run_once,
    run_watch,
)
from creeper.source_discovery.models import ScoutMeasurement, SourceCandidate, SourceLevel, SourceState
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


class SourceProducerCliTests(unittest.TestCase):
    def test_missing_source_mode_defaults_to_activated(self):
        intent = parse_source_producer_intent({})
        self.assertEqual(intent.mode, SourceProducerMode.ACTIVATED)
        self.assertFalse(intent.explicitly_configured)

    def test_explicit_static_mode_is_preserved(self):
        intent = parse_source_producer_intent(
            {"source_mode": "static", "dataset": "fixture.txt"}
        )
        self.assertEqual(intent.mode, SourceProducerMode.STATIC)
        self.assertTrue(intent.explicitly_configured)

    def test_invalid_source_mode_fails(self):
        with self.assertRaisesRegex(ValueError, "source_mode"):
            parse_source_producer_intent({"source_mode": "hybrid"})

    def test_legacy_static_dataset_without_explicit_mode_fails(self):
        with self.assertRaisesRegex(ValueError, "explicit"):
            parse_source_producer_intent({"dataset": "fixture.txt"})

    def test_explicit_activated_mode_does_not_require_dataset(self):
        intent = parse_source_producer_intent({"source_mode": "activated"})
        self.assertEqual(intent.mode, SourceProducerMode.ACTIVATED)

    def test_v4_activated_example_remains_valid(self):
        path = Path("conf/creeper.activated.example.toml")
        with path.open("rb") as stream:
            config = tomllib.load(stream)
        intent = parse_source_producer_intent(config)
        self.assertEqual(intent.mode, SourceProducerMode.ACTIVATED)
        self.assertNotIn("dataset", config)

    def test_watch_reuses_durable_once_runner_until_stop(self):
        stop = Event()
        reports = iter(
            [
                {
                    "leases_succeeded": 1,
                    "source_records": 2,
                    "observations": 2,
                    "evidence_tasks_enqueued": 2,
                    "direct_capsules_committed": 0,
                    "admission_blocked": False,
                    "max_source_record_queue_depth": 1,
                    "max_observation_queue_depth": 1,
                },
                {
                    "leases_succeeded": 0,
                    "source_records": 0,
                    "observations": 0,
                    "evidence_tasks_enqueued": 0,
                    "direct_capsules_committed": 0,
                    "admission_blocked": True,
                    "max_source_record_queue_depth": 0,
                    "max_observation_queue_depth": 0,
                },
            ]
        )

        def fake_once(config, *, owner):
            return next(reports)

        result = _watch_loop(
            lambda: fake_once(None, owner="test"),
            stop_event=stop,
            idle_backoff_seconds=1.0,
            max_idle_backoff_seconds=60.0,
            sleep_fn=lambda _: stop.set(),
        )

        self.assertEqual(result["leases_succeeded"], 1)
        self.assertEqual(result["source_records"], 2)
        self.assertTrue(result["admission_blocked"])

    def test_admission_backpressure_uses_short_fixed_poll(self):
        stop = Event()
        sleeps: list[float] = []
        blocked = {
            "leases_succeeded": 0,
            "source_records": 0,
            "observations": 0,
            "evidence_tasks_enqueued": 0,
            "direct_capsules_committed": 0,
            "admission_blocked": True,
            "max_source_record_queue_depth": 0,
            "max_observation_queue_depth": 0,
        }

        def sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) >= 3:
                stop.set()

        result = _watch_loop(
            lambda: blocked,
            stop_event=stop,
            idle_backoff_seconds=1.0,
            max_idle_backoff_seconds=60.0,
            sleep_fn=sleep,
        )

        self.assertEqual(sleeps, [1.0, 1.0, 1.0])
        self.assertTrue(result["admission_blocked"])

    def test_static_watch_reuses_one_persistent_runtime(self):
        stop = Event()
        reports = iter(
            [
                {
                    "leases_succeeded": 1,
                    "source_records": 1,
                    "observations": 1,
                    "evidence_tasks_enqueued": 1,
                    "direct_capsules_committed": 0,
                    "admission_blocked": False,
                    "max_source_record_queue_depth": 1,
                    "max_observation_queue_depth": 1,
                },
                {
                    "leases_succeeded": 0,
                    "source_records": 0,
                    "observations": 0,
                    "evidence_tasks_enqueued": 0,
                    "direct_capsules_committed": 0,
                    "admission_blocked": True,
                    "max_source_record_queue_depth": 0,
                    "max_observation_queue_depth": 0,
                },
            ]
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "watch-static.toml"
            config.write_text(
                "\n".join(
                    [
                        'source_mode = "static"',
                        f'runtime_data_root = "{root / "runtime"}"',
                        f'baseline_index = "{root / "baseline.sqlite3"}"',
                        f'dataset = "{root / "hosts.txt"}"',
                        "",
                        "[limits]",
                        "queue_source_records = 4",
                        "queue_observations = 4",
                        "queue_evidence_tasks = 4",
                        "queue_commits = 4",
                        "lease_max_records = 4",
                        "lease_max_requests = 4",
                        "lease_max_bytes = 4096",
                        "lease_max_seconds = 30",
                        "evidence_backlog_capacity = 10",
                    ]
                ),
                encoding="utf-8",
            )

            with patch("creeper.source_cli.StaticSourceRuntime") as runtime_cls:
                runtime = runtime_cls.return_value.__enter__.return_value
                runtime.run_once.side_effect = lambda: next(reports)
                result = run_watch(
                    config,
                    owner="persistent-static-test",
                    stop_event=stop,
                    sleep_fn=lambda _: stop.set(),
                )

        runtime_cls.assert_called_once()
        self.assertEqual(runtime.run_once.call_count, 2)
        self.assertEqual(result["leases_succeeded"], 1)
        self.assertTrue(result["admission_blocked"])

    def test_activated_runtime_shrinks_lease_to_backlog_headroom(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_root = task_root / "merged260909-3"
            baseline_root.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
            (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline_path = root / "baseline.sqlite3"
            BaselineIndex.build(task_root, baseline_path).close()

            runtime_root = root / "runtime"
            runtime_root.mkdir()
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                registry = SourceDiscoveryRegistry(control)
                candidate = SourceCandidate(
                    canonical_entrypoint="https://archive.example/source.txt",
                    source_family="BULK_ARTIFACT",
                    level=SourceLevel.SOURCE,
                    discovered_by="test",
                    discovery_strategy="fixture",
                    expected_year_from=1996,
                    expected_year_to=2001,
                    expected_volume=1000,
                    enumerability_prior=1.0,
                    confidence=1.0,
                    state=SourceState.ACTIVE,
                )
                registry.register_proposal(candidate)
                registry.record_scout_measurement(
                    candidate.source_key,
                    ScoutMeasurement(
                        sampled_records=100,
                        unique_hosts=100,
                        novel_hosts=50,
                        direct_host_years=0,
                        requests=1,
                        bytes_read=1024,
                        elapsed_seconds=1.0,
                        novel_eed=50.0,
                    ),
                )
                for index in range(2):
                    control.enqueue_evidence_tasks(
                        [
                            EvidenceQueryKey(
                                f"occupied-{index}.example",
                                TemporalScope(1997, 1997),
                                "wayback",
                                "cdx-v1",
                            )
                        ]
                    )
            finally:
                control.close()

            config = {
                "source_mode": "activated",
                "baseline_index": str(baseline_path),
                "runtime_data_root": str(runtime_root),
            }
            limits = {
                "queue_source_records": 10,
                "queue_observations": 10,
                "queue_evidence_tasks": 10,
                "queue_commits": 10,
                "lease_max_records": 4,
                "lease_max_requests": 4,
                "lease_max_bytes": 4096,
                "lease_max_seconds": 30,
                "evidence_backlog_capacity": 16,
            }
            with ActivatedSourceRuntime(
                root / "activated.toml",
                config=config,
                limits=limits,
                owner="headroom-test",
            ) as runtime:
                count = runtime.refresh_workset()
                self.assertEqual(count, 1)
                self.assertAlmostEqual(runtime.producer.range_first_fraction, 0.10)
                lease = runtime.producer.candidates[0].lease
                assert lease is not None
                self.assertEqual(lease.max_records, 2)
                self.assertEqual(lease.expected_evidence_tasks, 2)
                candidate_runtime = runtime.producer.candidates[0]
                self.assertAlmostEqual(candidate_runtime.expected_novel_eed, 1.0)
                self.assertEqual(candidate_runtime.costs.evidence_network, 2.0)
                self.assertEqual(candidate_runtime.reservation_evidence_tasks, 14)

    def test_activated_runtime_delegates_only_optimizer_eligible_direct_indexes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_root = task_root / "merged260909-3"
            baseline_root.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_root / f"{year}.txt").write_text(
                    "",
                    encoding="utf-8",
                )
            (baseline_root / "candidate_pool.txt").write_text(
                "",
                encoding="utf-8",
            )
            baseline_path = root / "baseline.sqlite3"
            BaselineIndex.build(task_root, baseline_path).close()
            runtime_root = root / "runtime"
            runtime_root.mkdir()

            plain = root / "plain.cdxj"
            plain.write_text(
                'com,plain)/ 19980101000000 '
                '{"url":"http://plain.com/"}\n',
                encoding="utf-8",
            )
            compressed = root / "compressed.cdxj.gz"
            compressed.write_bytes(b"fixture")

            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                registry = SourceDiscoveryRegistry(control)
                candidates = []
                for path in (plain, compressed):
                    candidate = SourceCandidate(
                        canonical_entrypoint=(
                            f"https://archive.example/{path.name}"
                        ),
                        source_family="BULK_ARTIFACT",
                        level=SourceLevel.SOURCE,
                        discovered_by="test",
                        discovery_strategy="fixture",
                        expected_year_from=1996,
                        expected_year_to=2001,
                        expected_volume=100,
                        enumerability_prior=1.0,
                        confidence=1.0,
                        state=SourceState.ACTIVE,
                    )
                    registry.register_proposal(candidate)
                    registry.record_scout_measurement(
                        candidate.source_key,
                        ScoutMeasurement(
                            sampled_records=10,
                            unique_hosts=10,
                            novel_hosts=10,
                            direct_host_years=10,
                            requests=1,
                            bytes_read=1024,
                            elapsed_seconds=1.0,
                            novel_eed=10.0,
                        ),
                    )
                    registry.record_triage_observation(
                        candidate.source_key,
                        status_code=200,
                        method="HEAD",
                        content_type="application/octet-stream",
                        content_length=path.stat().st_size,
                        range_supported=True,
                    )
                    candidates.append(candidate)
            finally:
                control.close()

            config = {
                "source_mode": "activated",
                "baseline_index": str(baseline_path),
                "runtime_data_root": str(runtime_root),
                "historical_index": {"enabled": True},
            }
            limits = {
                "queue_source_records": 10,
                "queue_observations": 10,
                "queue_evidence_tasks": 10,
                "queue_commits": 10,
                "lease_max_records": 4,
                "lease_max_requests": 4,
                "lease_max_bytes": 4096,
                "lease_max_seconds": 30,
                "evidence_backlog_capacity": 16,
            }
            with ActivatedSourceRuntime(
                root / "activated.toml",
                config=config,
                limits=limits,
                owner="delegation-test",
            ) as runtime:
                count = runtime.refresh_workset()

                self.assertEqual(count, 1)
                self.assertEqual(
                    runtime.historical_index_delegated_sources,
                    1,
                )
                self.assertEqual(len(runtime.producer.candidates), 1)
                self.assertEqual(
                    runtime.producer.candidates[0].source_key,
                    candidates[1].source_key,
                )

    def test_static_runtime_shrinks_lease_to_backlog_headroom(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_root = task_root / "merged260909-3"
            baseline_root.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
            (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline_path = root / "baseline.sqlite3"
            BaselineIndex.build(task_root, baseline_path).close()

            dataset = root / "hosts.txt"
            dataset.write_text(
                "\n".join(
                    [
                        "one.example",
                        "two.example",
                        "three.example",
                        "four.example",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                for index in range(3):
                    control.enqueue_evidence_tasks(
                        [
                            EvidenceQueryKey(
                                f"occupied-{index}.example",
                                TemporalScope(1997, 1997),
                                "wayback",
                                "cdx-v1",
                            )
                        ]
                    )
            finally:
                control.close()

            config = root / "static.toml"
            config.write_text(
                "\n".join(
                    [
                        'source_mode = "static"',
                        f'baseline_index = {json.dumps(str(baseline_path))}',
                        f'dataset = {json.dumps(str(dataset))}',
                        f'runtime_data_root = {json.dumps(str(runtime_root))}',
                        'source_id = "webbase-static"',
                        'domain_id = "webbase-domain"',
                        'reservoir_id = "webbase-static"',
                        "source_year = 2001",
                        "",
                        "[limits]",
                        "queue_source_records = 8",
                        "queue_observations = 8",
                        "queue_evidence_tasks = 8",
                        "queue_commits = 8",
                        "lease_max_records = 4",
                        "lease_max_requests = 4",
                        "lease_max_bytes = 4096",
                        "lease_max_seconds = 30",
                        "evidence_backlog_capacity = 10",
                    ]
                ),
                encoding="utf-8",
            )

            report = run_once(config, owner="static-headroom-test")

            self.assertEqual(report["leases_succeeded"], 1)
            self.assertEqual(report["source_records"], 1)
            self.assertEqual(report["evidence_tasks_enqueued"], 2)
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                reservoir = control.get_reservoir("webbase-static")
                self.assertIsNotNone(reservoir)
                self.assertIsNotNone(reservoir.cursor)
                self.assertEqual(
                    sum(
                        control.evidence_task_state_counts().values()
                    ),
                    5,
                )
            finally:
                control.close()

    def test_static_runtime_does_not_advance_when_backlog_is_full(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_root = task_root / "merged260909-3"
            baseline_root.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
            (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline_path = root / "baseline.sqlite3"
            BaselineIndex.build(task_root, baseline_path).close()

            dataset = root / "hosts.txt"
            dataset.write_text("blocked.example\n", encoding="utf-8")
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                for index in range(2):
                    control.enqueue_evidence_tasks(
                        [
                            EvidenceQueryKey(
                                f"occupied-full-{index}.example",
                                TemporalScope(1997, 1997),
                                "wayback",
                                "cdx-v1",
                            )
                        ]
                    )
            finally:
                control.close()

            config = root / "static-full.toml"
            config.write_text(
                "\n".join(
                    [
                        'source_mode = "static"',
                        f'baseline_index = {json.dumps(str(baseline_path))}',
                        f'dataset = {json.dumps(str(dataset))}',
                        f'runtime_data_root = {json.dumps(str(runtime_root))}',
                        'source_id = "webbase-full"',
                        'domain_id = "webbase-full-domain"',
                        'reservoir_id = "webbase-full"',
                        "source_year = 2001",
                        "",
                        "[limits]",
                        "queue_source_records = 4",
                        "queue_observations = 4",
                        "queue_evidence_tasks = 4",
                        "queue_commits = 4",
                        "lease_max_records = 4",
                        "lease_max_requests = 4",
                        "lease_max_bytes = 4096",
                        "lease_max_seconds = 30",
                        "evidence_backlog_capacity = 2",
                    ]
                ),
                encoding="utf-8",
            )

            report = run_once(config, owner="static-full-test")

            self.assertEqual(report["leases_succeeded"], 0)
            self.assertTrue(report["admission_blocked"])
            self.assertEqual(report["source_records"], 0)
            self.assertEqual(report["rdap_shadow_tasks_enqueued"], 2)
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                reservoir = control.get_reservoir("webbase-full")
                self.assertIsNotNone(reservoir)
                self.assertIsNone(reservoir.cursor)
                self.assertEqual(
                    sum(control.evidence_task_state_counts().values()),
                    4,
                )
            finally:
                control.close()

    def test_once_mode_stops_at_durable_evidence_task(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_root = task_root / "merged260909-3"
            baseline_root.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
            (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline_path = root / "baseline.sqlite3"
            BaselineIndex.build(task_root, baseline_path).close()

            dataset = root / "hosts.txt"
            dataset.write_text("novel.example\n", encoding="utf-8")
            runtime_root = root / "runtime"
            config = root / "creeper.toml"
            config.write_text(
                "\n".join(
                    [
                        'source_mode = "static"',
                        f'baseline_index = {json.dumps(str(baseline_path))}',
                        f'dataset = {json.dumps(str(dataset))}',
                        f'runtime_data_root = {json.dumps(str(runtime_root))}',
                        'source_id = "fixture"',
                        'domain_id = "fixture-domain"',
                        'reservoir_id = "fixture"',
                        "source_year = 1997",
                        "",
                        "[limits]",
                        "queue_source_records = 2",
                        "queue_observations = 2",
                        "queue_evidence_tasks = 2",
                        "queue_commits = 2",
                        "lease_max_records = 1",
                        "lease_max_requests = 1",
                        "lease_max_bytes = 1024",
                        "lease_max_seconds = 30",
                        "evidence_backlog_capacity = 7",
                    ]
                ),
                encoding="utf-8",
            )

            report = run_once(config, owner="source-cli-test")

            self.assertEqual(report["leases_succeeded"], 1)
            self.assertEqual(report["evidence_tasks_enqueued"], 2)
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                key = EvidenceQueryKey(
                    "novel.example",
                    TemporalScope(1997, 1997),
                    "wayback",
                    "cdx-v1",
                )
                task = control.get_evidence_task(key)
                self.assertIsNotNone(task)
                self.assertEqual(task.state, "pending")
                self.assertIsNone(task.lease_owner)
            finally:
                control.close()


    def test_reviewed_jsonl_registry_flows_through_standard_runtime_and_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_root = task_root / "merged260912-3"
            baseline_root.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
            (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline_path = root / "baseline.sqlite3"
            BaselineIndex.build(task_root, baseline_path).close()

            runtime_root = root / "runtime"
            runtime_root.mkdir()
            locator = "https://trusted.example/releases/2026/history.jsonl"
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                registry = SourceDiscoveryRegistry(control)
                candidate = SourceCandidate(
                    canonical_entrypoint=locator,
                    source_family="REVIEWED_STRUCTURED",
                    level=SourceLevel.SOURCE,
                    discovered_by="test",
                    discovery_strategy="fixture",
                    expected_year_from=1996,
                    expected_year_to=2001,
                    expected_volume=100,
                    enumerability_prior=1.0,
                    confidence=1.0,
                    state=SourceState.ACTIVE,
                )
                registry.register_proposal(candidate)
                registry.record_scout_measurement(
                    candidate.source_key,
                    ScoutMeasurement(
                        sampled_records=10,
                        unique_hosts=10,
                        novel_hosts=8,
                        direct_host_years=0,
                        requests=1,
                        bytes_read=512,
                        elapsed_seconds=1.0,
                        novel_eed=8.0,
                    ),
                )
            finally:
                control.close()

            contract_registry = root / "reviewed-source-contracts.json"
            contract_registry.write_text(
                json.dumps(
                    {
                        "registry_version": "reviewed-source-contract-registry-v1",
                        "entries": [
                            {
                                "contract_id": "trusted-history-jsonl-v1",
                                "locator": locator,
                                "authority": "DIRECT_WEB_YEAR",
                                "parser_kind": "jsonl",
                                "hostname_field": "url",
                                "timestamp_field": "timestamp",
                                "temporal_semantics": "reviewed_capture_timestamp",
                                "evidence_type": "reviewed_historical_web_record",
                                "policy_version": "trusted-history-policy-v1",
                                "custodian": "fixture-custodian",
                                "edition": "2026-09-13",
                                "review_note": "fixture reviewed direct source",
                                "source_identity": {
                                    "kind": "immutable_locator",
                                    "value": locator,
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            config = {
                "source_mode": "activated",
                "baseline_index": str(baseline_path),
                "runtime_data_root": str(runtime_root),
                "evidence_contract_registry": str(contract_registry),
            }
            limits = {
                "queue_source_records": 10,
                "queue_observations": 10,
                "queue_evidence_tasks": 10,
                "queue_commits": 10,
                "lease_max_records": 4,
                "lease_max_requests": 4,
                "lease_max_bytes": 4096,
                "lease_max_seconds": 30,
                "evidence_backlog_capacity": 16,
            }

            with ActivatedSourceRuntime(
                root / "activated.toml",
                config=config,
                limits=limits,
                owner="reviewed-runtime-1",
            ) as runtime:
                self.assertEqual(runtime.refresh_workset(), 1)
                reservoir = runtime.control.get_reservoir(
                    runtime.producer.candidates[0].reservoir_id
                )
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                self.assertEqual(reservoir.evidence_mode, "direct_year")
                first_adapter_id = reservoir.adapter_id
                self.assertIn(":rsi1:", first_adapter_id)
                self.assertIn(":evc1:", first_adapter_id)

            with ActivatedSourceRuntime(
                root / "activated.toml",
                config=config,
                limits=limits,
                owner="reviewed-runtime-2",
            ) as restarted:
                self.assertEqual(restarted.refresh_workset(), 1)
                reservoir = restarted.control.get_reservoir(
                    restarted.producer.candidates[0].reservoir_id
                )
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                self.assertEqual(reservoir.evidence_mode, "direct_year")
                self.assertEqual(reservoir.adapter_id, first_adapter_id)



if __name__ == "__main__":
    unittest.main()
