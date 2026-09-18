from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.coordinator import (
    ScoutDisposition,
    ScoutResult,
    SearchBatch,
    SourceDiscoveryCoordinator,
    TriageResult,
)
from creeper.source_discovery.manager import (
    SourceIntelligenceTask,
    SourcePoolTargets,
    SourceReservoirManager,
)
from creeper.source_discovery.models import (
    ScoutMeasurement,
    SourceCandidate,
    SourceLevel,
    SourceState,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.unknown_format import (
    UnknownFormatProtocolError,
    make_unknown_format_reason,
    parse_unknown_format_reason,
    validate_adapter_proposal,
)
from creeper.sources.format_binding import (
    SourceFormatObservation,
    bind_format_to_adapter_id,
)
from creeper.sources.production import StructuredProductionAdapter
from creeper.sources.reservoirs import Reservoir
from creeper.sources.schema_binding import (
    SourceRecordSchema,
    bind_schema_to_adapter_id,
)
from creeper.storage.control_store import ControlStore


class UnknownFormatProtocolTests(unittest.TestCase):
    SAMPLE = (
        b'{"endpoint":"https://alpha.example.com/a","seen":"1998-01-02"}\n'
        b'{"endpoint":"https://beta.example.com/b","seen":"1999-02-03"}\n'
        b'{"endpoint":"https://gamma.example.com/c","seen":"2000-03-04"}\n'
        b'{"endpoint":"https://delta.example.com/d","seen":"2001-04-05"}\n'
    )

    def test_case_round_trip_is_bounded_and_text_only(self) -> None:
        reason = make_unknown_format_reason(
            self.SAMPLE,
            content_type="application/octet-stream",
            truncated=True,
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertLessEqual(len(reason), 8 * 1024)
        case = parse_unknown_format_reason(reason)
        self.assertIsNotNone(case)
        assert case is not None
        self.assertEqual(case.content_type, "application/octet-stream")
        self.assertEqual(case.compression, "none")
        self.assertTrue(case.truncated)
        self.assertIn('"endpoint"', case.preview_text)

        self.assertIsNone(
            make_unknown_format_reason(
                b"\x00\x01\x02\x03" * 128,
                content_type="application/octet-stream",
                truncated=False,
            )
        )

    def test_validated_jsonl_binding_is_discovery_only(self) -> None:
        reason = make_unknown_format_reason(
            self.SAMPLE,
            content_type="application/octet-stream",
            truncated=False,
        )
        assert reason is not None
        case = parse_unknown_format_reason(reason)
        assert case is not None
        format_observation, schema = validate_adapter_proposal(
            {
                "parser_kind": "jsonl",
                "compression": "none",
                "hostname_field": "endpoint",
                "timestamp_field": "seen",
                "delimiter": None,
            },
            case,
        )
        self.assertEqual(format_observation.parser_kind, "jsonl")
        self.assertGreaterEqual(format_observation.confidence, 0.90)
        self.assertEqual(schema.hostname_field, "endpoint")
        self.assertEqual(schema.timestamp_field, "seen")
        self.assertFalse(schema.direct_year_eligible)

    def test_adapter_proposal_cannot_request_new_code_or_authority(self) -> None:
        reason = make_unknown_format_reason(
            self.SAMPLE,
            content_type="text/plain",
            truncated=False,
        )
        assert reason is not None
        case = parse_unknown_format_reason(reason)
        assert case is not None
        with self.assertRaisesRegex(
            UnknownFormatProtocolError,
            "unknown adapter proposal fields",
        ):
            validate_adapter_proposal(
                {
                    "parser_kind": "jsonl",
                    "compression": "none",
                    "hostname_field": "endpoint",
                    "timestamp_field": "seen",
                    "delimiter": None,
                    "python_code": "exec('unsafe')",
                },
                case,
            )
        with self.assertRaisesRegex(
            UnknownFormatProtocolError,
            "jsonl or delimited",
        ):
            validate_adapter_proposal(
                {
                    "parser_kind": "custom_python",
                    "compression": "none",
                    "hostname_field": "endpoint",
                    "timestamp_field": "seen",
                    "delimiter": None,
                },
                case,
            )


class UnknownFormatCoordinatorLoopTests(unittest.IsolatedAsyncioTestCase):
    SAMPLE = UnknownFormatProtocolTests.SAMPLE

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.control = ControlStore(self.root / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.candidate = SourceCandidate(
            canonical_entrypoint="https://data.example/opaque-resource",
            source_family="UNKNOWN_TEST",
            level=SourceLevel.SOURCE,
            discovered_by="fixture",
            discovery_strategy="fixture",
            expected_volume=10_000,
            confidence=0.8,
        )
        self.registry.register_proposal(self.candidate)
        self.registry.transition(self.candidate.source_key, SourceState.HOLD)
        reason = make_unknown_format_reason(
            self.SAMPLE,
            content_type="application/octet-stream",
            truncated=False,
        )
        assert reason is not None
        self.registry.set_state_reason(self.candidate.source_key, reason)

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def manager(self) -> SourceReservoirManager:
        return SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=0,
                warm_target=0,
                cold_min=0,
                cold_target=0,
                triage_batch=4,
                scout_parallelism=1,
                max_search_directives=1,
            ),
        )

    async def test_compile_adapter_binds_then_requeues_for_measured_scout(self) -> None:
        seen_tasks: list[SourceIntelligenceTask] = []

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            raise AssertionError("no triage expected")

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            raise AssertionError("re-scout occurs on the next cycle")

        async def compile_adapter(directive) -> SearchBatch:
            seen_tasks.append(directive.task_type)
            self.assertEqual(directive.task_type, SourceIntelligenceTask.COMPILE_ADAPTER)
            self.assertEqual(directive.subject, self.candidate.canonical_entrypoint)
            return SearchBatch(
                backend="fixture-llm",
                query="interpret supplied bounded sample only",
                actor="agent:fixture",
                llm_episode_id="llm:adapter-fixture",
                llm_task_type=directive.task_type.value,
                context_hash="fixture-context",
                prompt_version="source-intelligence-v2",
                adapter_proposals=(
                    {
                        "parser_kind": "jsonl",
                        "compression": "none",
                        "hostname_field": "endpoint",
                        "timestamp_field": "seen",
                        "delimiter": None,
                    },
                ),
            )

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.root / "coordinator.lock",
            triage_executor=triage,
            scout_executor=scout,
            search_executor=compile_adapter,
        )
        report = await coordinator.run_once()

        self.assertEqual(seen_tasks, [SourceIntelligenceTask.COMPILE_ADAPTER])
        self.assertEqual(report.adapter_bindings_applied, 1)
        stored = self.registry.get_candidate(self.candidate.source_key)
        assert stored is not None
        self.assertEqual(stored.state, SourceState.SCOUT_READY)
        self.assertEqual(stored.state_reason, "")
        format_observation = self.registry.get_format_observation(
            self.candidate.source_key
        )
        schema = self.registry.get_schema_observation(self.candidate.source_key)
        assert format_observation is not None
        assert schema is not None
        self.assertEqual(format_observation.parser_kind, "jsonl")
        self.assertEqual(schema.hostname_field, "endpoint")
        self.assertFalse(schema.direct_year_eligible)

        async def measured_scout(candidate: SourceCandidate) -> ScoutResult:
            self.assertEqual(candidate.source_key, self.candidate.source_key)
            return ScoutResult(
                ScoutDisposition.WARM,
                measurement=ScoutMeasurement(
                    sampled_records=4,
                    unique_hosts=4,
                    novel_hosts=4,
                    direct_host_years=0,
                    requests=1,
                    bytes_read=len(self.SAMPLE),
                    elapsed_seconds=0.1,
                    novel_eed=4.0,
                ),
                format_observation=self.registry.get_format_observation(
                    candidate.source_key
                ),
                schema_observation=self.registry.get_schema_observation(
                    candidate.source_key
                ),
            )

        async def no_search(_directive) -> SearchBatch:
            raise AssertionError("validated binding must not trigger another LLM call")

        coordinator2 = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.root / "coordinator.lock",
            triage_executor=triage,
            scout_executor=measured_scout,
            search_executor=no_search,
        )
        report2 = await coordinator2.run_once()
        self.assertEqual(report2.scouted_warm, 1)
        final = self.registry.get_candidate(self.candidate.source_key)
        assert final is not None
        self.assertEqual(final.state, SourceState.WARM)

    async def test_invalid_adapter_proposal_stays_hold_and_backs_off(self) -> None:
        async def triage(_candidate: SourceCandidate) -> TriageResult:
            raise AssertionError("no triage expected")

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            raise AssertionError("no scout expected")

        async def bad_adapter(directive) -> SearchBatch:
            return SearchBatch(
                backend="fixture-llm",
                query="bad interpretation",
                actor="agent:fixture",
                llm_episode_id="llm:bad-adapter",
                llm_task_type=directive.task_type.value,
                adapter_proposals=(
                    {
                        "parser_kind": "jsonl",
                        "compression": "none",
                        "hostname_field": "missing",
                        "timestamp_field": "seen",
                        "delimiter": None,
                    },
                ),
            )

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.root / "coordinator.lock",
            triage_executor=triage,
            scout_executor=scout,
            search_executor=bad_adapter,
            retry_clock=lambda: 10.0,
        )
        report = await coordinator.run_once()
        self.assertEqual(report.adapter_bindings_applied, 0)
        self.assertEqual(report.search_failures, 1)
        stored = self.registry.get_candidate(self.candidate.source_key)
        assert stored is not None
        self.assertEqual(stored.state, SourceState.HOLD)
        self.assertIsNotNone(parse_unknown_format_reason(stored.state_reason))


class UnknownFormatProductionBindingTests(unittest.TestCase):
    def test_discovery_only_reader_honors_nonstandard_bound_fields(self) -> None:
        format_observation = SourceFormatObservation(
            parser_kind="jsonl",
            compression="none",
            detection_method="llm_declarative_validated",
            confidence=1.0,
            content_type="application/octet-stream",
            policy_version="source-format-llm-layout-v1",
        )
        schema = SourceRecordSchema(
            parser_kind="jsonl",
            hostname_field="endpoint",
            timestamp_field="seen",
            delimiter=None,
            detection_method="llm_declarative_validated",
            confidence=1.0,
            sample_records=4,
            matched_records=4,
            direct_year_eligible=False,
            policy_version="record-schema-llm-layout-v1",
        )
        adapter_id = bind_format_to_adapter_id(
            "structured:unknown-format-fixture",
            format_observation,
        )
        adapter_id = bind_schema_to_adapter_id(adapter_id, schema)
        reservoir = Reservoir(
            reservoir_id="reservoir:unknown-format-fixture",
            domain_id="domain:unknown-format-fixture",
            adapter_id=adapter_id,
            root_locator="https://data.example/opaque-resource",
            enumeration_kind="structured_records",
            capacity_lower=0,
            evidence_mode="discovery_only",
        )
        adapter = StructuredProductionAdapter(reservoir)
        record = adapter._generic_record(
            '{"endpoint":"https://alpha.example.com/path","seen":"1998-01-02"}',
            locator="fixture:0",
        )
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.payload, "https://alpha.example.com/path")
        self.assertEqual(record.source_year, 1998)
        self.assertEqual(record.direct_year_mask, 0)
        hosts = tuple(adapter.extract_hosts(record))
        self.assertEqual(len(hosts), 1)
        self.assertEqual(hosts[0].hostname, "alpha.example.com")
        self.assertEqual(hosts[0].direct_year_mask, 0)


if __name__ == "__main__":
    unittest.main()
