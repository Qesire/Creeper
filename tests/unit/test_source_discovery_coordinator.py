from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.deterministic_search import DeterministicSearchBatch
from creeper.source_discovery.coordinator import (
    CoordinatorBusyError,
    ScoutDisposition,
    ScoutResult,
    SearchBatch,
    SourceDiscoveryCoordinator,
    TriageDisposition,
    TriageResult,
)
from creeper.source_discovery.manager import SourcePoolTargets, SourceReservoirManager
from creeper.source_discovery.models import (
    ScoutMeasurement,
    SourceCandidate,
    SourceLevel,
    SourceState,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.residual_search import (
    ResidualSearchLedger,
    SearchCell,
    SearchCellScheduler,
)
from creeper.source_discovery.search_identity import (
    RawSearchResult,
    SearchIdentityLedger,
    canonicalize_search_result,
)
from creeper.source_discovery.unknown_format import make_unknown_format_reason
from creeper.sources.format_binding import SourceFormatObservation
from creeper.sources.layout_binding import SourceRecordLayout
from creeper.sources.schema_binding import SourceRecordSchema
from creeper.storage.control_store import ControlStore


class SourceDiscoveryCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.control = ControlStore(self.root / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.lock_path = self.root / "source-discovery.lock"

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    @staticmethod
    def candidate(name: str, *, strategy: str = "META_SOURCE_SEARCH") -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=f"https://example.com/{name}/",
            source_family="TEST_FAMILY",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy=strategy,
            expected_volume=1000,
            confidence=0.7,
        )

    def hold_unknown_format_source(
        self,
        name: str = "format-blocker",
    ) -> SourceCandidate:
        reason = make_unknown_format_reason(
            (
                b'{"host":"a.example"}\n'
                b'{"host":"b.example"}\n'
                b'{"host":"c.example"}\n'
            ),
            content_type="application/octet-stream",
            truncated=False,
        )
        assert reason is not None
        candidate = SourceCandidate(
            canonical_entrypoint=f"https://opaque.example/{name}.data",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="deterministic:test",
            discovery_strategy="FIXTURE",
            expected_volume=100_000,
            enumerability_prior=1.0,
            confidence=0.8,
            state=SourceState.HOLD,
            state_reason=reason,
        )
        stored, _inserted = self.registry.register_proposal(candidate)
        return stored


    def to_scout_ready(self, candidate: SourceCandidate) -> None:
        self.registry.register_proposal(candidate)
        self.registry.transition(candidate.source_key, SourceState.TRIAGED)
        self.registry.transition(candidate.source_key, SourceState.SCOUT_READY)

    @staticmethod
    def measurement() -> ScoutMeasurement:
        return ScoutMeasurement(
            sampled_records=100,
            unique_hosts=80,
            novel_hosts=20,
            direct_host_years=4,
            requests=3,
            bytes_read=4096,
            elapsed_seconds=1.0,
            novel_eed=8.0,
        )

    def manager(self, **overrides) -> SourceReservoirManager:
        values = dict(
            active_min=0,
            active_target=0,
            warm_min=0,
            warm_target=0,
            cold_min=0,
            cold_target=0,
            triage_batch=8,
            scout_parallelism=4,
            max_search_directives=3,
        )
        values.update(overrides)
        return SourceReservoirManager(self.registry, targets=SourcePoolTargets(**values))

    def test_coordinator_rejects_invalid_parallelism_and_retry_timing(self) -> None:
        common = dict(
            registry=self.registry,
            manager=self.manager(),
            lock_path=self.lock_path,
            triage_executor=lambda item: item,
            scout_executor=lambda item: item,
            search_executor=lambda item: item,
        )
        with self.assertRaisesRegex(ValueError, "triage_parallelism"):
            SourceDiscoveryCoordinator(
                **common,
                triage_parallelism=True,
            )
        with self.assertRaisesRegex(ValueError, "failure_retry_seconds"):
            SourceDiscoveryCoordinator(
                **common,
                failure_retry_seconds=float("nan"),
            )

    def test_coordinator_rejects_nonfinite_retry_clock_before_backoff_state(self) -> None:
        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=lambda item: item,
            scout_executor=lambda item: item,
            search_executor=lambda item: item,
            retry_clock=lambda: float("nan"),
        )
        with self.assertRaisesRegex(ValueError, "retry clock must be finite"):
            coordinator._eligible_search_directives(())
        self.assertEqual(coordinator._search_retry_deadlines, {})

    def test_search_batch_rejects_nonfinite_cost_before_commit(self) -> None:
        with self.assertRaisesRegex(ValueError, "finite and non-negative"):
            SearchBatch(
                backend="test",
                query="nan cost",
                actor="agent:test",
                search_cost_seconds=float("nan"),
            )

    def test_search_batch_rejects_invalid_hypothesis_before_commit(self) -> None:
        with self.assertRaisesRegex(ValueError, "hypothesis action"):
            SearchBatch(
                backend="test",
                query="bad hypothesis",
                actor="agent:test",
                llm_episode_id="llm:test",
                llm_task_type="DISCOVER_NEW_SOURCE",
                hypotheses=(
                    {
                        "hypothesis_id": "llm:test:h1",
                        "action": "",
                        "confidence": 0.5,
                    },
                ),
            )

    def test_search_batch_rejects_whitespace_llm_task_before_commit(self) -> None:
        with self.assertRaisesRegex(ValueError, "llm_task_type"):
            SearchBatch(
                backend="test",
                query="bad task type",
                actor="agent:test",
                llm_episode_id="llm:test",
                llm_task_type="   ",
            )

    def test_search_batch_rejects_non_json_hypothesis_before_commit(self) -> None:
        with self.assertRaisesRegex(ValueError, "JSON-serializable"):
            SearchBatch(
                backend="test",
                query="bad json",
                actor="agent:test",
                llm_episode_id="llm:test",
                llm_task_type="DISCOVER_NEW_SOURCE",
                hypotheses=(
                    {
                        "hypothesis_id": "llm:test:h1",
                        "action": "DISCOVER",
                        "confidence": 0.5,
                        "unsupported": {"not-json"},
                    },
                ),
            )

    def test_search_batch_rejects_llm_lineage_without_episode(self) -> None:
        with self.assertRaisesRegex(ValueError, "require llm_episode_id"):
            SearchBatch(
                backend="test",
                query="bad lineage",
                actor="agent:test",
                hypotheses=(
                    {
                        "hypothesis_id": "h1",
                        "action": "DISCOVER",
                        "confidence": 0.5,
                    },
                ),
            )

    def test_search_batch_rejects_unknown_hypothesis_attribution(self) -> None:
        candidate = self.candidate("lineage")
        with self.assertRaisesRegex(ValueError, "unknown hypothesis_id"):
            SearchBatch(
                backend="test",
                query="bad attribution",
                actor="agent:test",
                candidates=(candidate,),
                llm_episode_id="llm:test",
                llm_task_type="DISCOVER_NEW_SOURCE",
                hypotheses=(
                    {
                        "hypothesis_id": "llm:test:h1",
                        "action": "DISCOVER",
                        "confidence": 0.5,
                    },
                ),
                hypothesis_attribution=(
                    (candidate.source_key, "llm:test:missing"),
                ),
            )

    async def test_search_triage_and_scout_io_overlap_but_commits_complete_serially(self) -> None:
        discovered = self.candidate("triage")
        self.registry.register_proposal(discovered)
        scout = self.candidate("scout")
        self.to_scout_ready(scout)
        self.hold_unknown_format_source("overlap")

        labels: set[str] = set()
        all_stages_entered = asyncio.Event()

        async def gate(label: str) -> None:
            labels.add(label)
            if {"triage", "scout", "search"}.issubset(labels):
                all_stages_entered.set()
            await asyncio.wait_for(all_stages_entered.wait(), timeout=1.0)

        async def triage_executor(_candidate: SourceCandidate) -> TriageResult:
            await gate("triage")
            return TriageResult(TriageDisposition.SCOUT)

        async def scout_executor(_candidate: SourceCandidate) -> ScoutResult:
            await gate("scout")
            return ScoutResult(ScoutDisposition.WARM, self.measurement())

        async def search_executor(directive) -> SearchBatch:
            await gate("search")
            found = SourceCandidate(
                canonical_entrypoint=f"https://search.example/{directive.strategy.lower()}/",
                source_family="SEARCHED",
                level=SourceLevel.COLLECTION,
                discovered_by="untrusted-agent-label",
                discovery_strategy="UNTRUSTED",
                expected_volume=5000,
                confidence=0.6,
            )
            return SearchBatch(
                backend="test-search",
                query=f"query:{directive.strategy}",
                actor="agent:test",
                candidates=(found,),
                search_cost_seconds=0.25,
            )

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(cold_min=3, cold_target=5),
            lock_path=self.lock_path,
            triage_executor=triage_executor,
            scout_executor=scout_executor,
            search_executor=search_executor,
        )

        report = await coordinator.run_once()

        self.assertTrue({"triage", "scout", "search"}.issubset(labels))
        self.assertEqual(
            self.registry.get_candidate(discovered.source_key).state,
            SourceState.SCOUT_READY,
        )
        self.assertEqual(self.registry.get_candidate(scout.source_key).state, SourceState.WARM)
        self.assertEqual(report.triaged_to_scout, 1)
        self.assertEqual(report.scouted_warm, 1)
        self.assertGreaterEqual(report.search_episodes, 1)
        searched = [
            item
            for item in self.registry.list_candidates(state=SourceState.DISCOVERED)
            if item.canonical_entrypoint.startswith("https://search.example/")
        ]
        self.assertGreaterEqual(len(searched), 1)
        self.assertEqual(searched[0].discovered_by, "agent:test")

    async def test_scout_format_observation_is_committed_by_coordinator(self) -> None:
        candidate = self.candidate("opaque-format")
        self.to_scout_ready(candidate)
        observation = SourceFormatObservation(
            parser_kind="jsonl",
            compression="gzip",
            detection_method="content_signature",
            confidence=0.97,
            content_type="application/octet-stream",
        )

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            raise AssertionError("no triage expected")

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(
                ScoutDisposition.WARM,
                measurement=self.measurement(),
                format_observation=observation,
            )

        async def search(_directive) -> SearchBatch:
            raise AssertionError("no search expected")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
        )

        report = await coordinator.run_once()

        self.assertEqual(report.scouted_warm, 1)
        self.assertEqual(
            self.registry.get_format_observation(candidate.source_key),
            observation,
        )
        self.assertEqual(
            self.registry.get_candidate(candidate.source_key).state,
            SourceState.WARM,
        )

    async def test_scout_layout_observation_is_committed_by_coordinator(self) -> None:
        candidate = self.candidate("opaque-layout")
        self.to_scout_ready(candidate)
        layout = SourceRecordLayout(
            parser_kind="jsonl",
            hostname_field="endpoint",
            delimiter=None,
            detection_method="stable_json_host_field",
            confidence=1.0,
            sample_records=8,
            matched_records=8,
        )

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            raise AssertionError("no triage expected")

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(
                ScoutDisposition.WARM,
                measurement=self.measurement(),
                layout_observation=layout,
            )

        async def search(_directive) -> SearchBatch:
            raise AssertionError("no search expected")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
        )

        report = await coordinator.run_once()

        self.assertEqual(report.scouted_warm, 1)
        self.assertEqual(
            self.registry.get_layout_observation(candidate.source_key),
            layout,
        )
        self.assertEqual(
            self.registry.get_candidate(candidate.source_key).state,
            SourceState.WARM,
        )

    async def test_scout_schema_observation_is_committed_by_coordinator(self) -> None:
        candidate = self.candidate("opaque-schema")
        self.to_scout_ready(candidate)
        schema = SourceRecordSchema(
            parser_kind="jsonl",
            hostname_field="url",
            timestamp_field="capture_year",
            delimiter=None,
            detection_method="stable_json_fields",
            confidence=1.0,
            sample_records=8,
            matched_records=8,
        )

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            raise AssertionError("no triage expected")

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(
                ScoutDisposition.WARM,
                measurement=self.measurement(),
                schema_observation=schema,
            )

        async def search(_directive) -> SearchBatch:
            raise AssertionError("no search expected")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
        )

        report = await coordinator.run_once()

        self.assertEqual(report.scouted_warm, 1)
        self.assertEqual(
            self.registry.get_schema_observation(candidate.source_key),
            schema,
        )

    async def test_conflicting_schema_observation_holds_only_that_source(self) -> None:
        candidate = self.candidate("opaque-schema-conflict")
        self.to_scout_ready(candidate)
        first = SourceRecordSchema(
            parser_kind="delimited",
            hostname_field="column:0",
            timestamp_field="column:1",
            delimiter=",",
            detection_method="stable_delimited_columns",
            confidence=1.0,
            sample_records=8,
            matched_records=8,
        )
        second = SourceRecordSchema(
            parser_kind="delimited",
            hostname_field="column:1",
            timestamp_field="column:0",
            delimiter=",",
            detection_method="stable_delimited_columns",
            confidence=1.0,
            sample_records=8,
            matched_records=8,
        )
        self.registry.record_schema_observation(candidate.source_key, first)

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            raise AssertionError("no triage expected")

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(
                ScoutDisposition.WARM,
                measurement=self.measurement(),
                schema_observation=second,
            )

        async def search(_directive) -> SearchBatch:
            raise AssertionError("no search expected")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
        )

        report = await coordinator.run_once()

        self.assertEqual(report.scout_failures, 1)
        self.assertEqual(report.scouted_hold, 1)
        self.assertEqual(
            self.registry.get_schema_observation(candidate.source_key),
            first,
        )
        reason = self.registry.suppression_reason(
            self.registry.get_candidate(candidate.source_key)
        )
        self.assertIsNotNone(reason)
        self.assertIn("schema observation failed closed", reason)

    async def test_conflicting_format_observation_holds_only_that_source(self) -> None:
        candidate = self.candidate("opaque-format-conflict")
        self.to_scout_ready(candidate)
        first = SourceFormatObservation(
            parser_kind="jsonl",
            compression="none",
            detection_method="content_signature",
            confidence=0.97,
        )
        second = SourceFormatObservation(
            parser_kind="cdxj",
            compression="none",
            detection_method="content_signature",
            confidence=0.98,
        )
        self.registry.record_format_observation(candidate.source_key, first)

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            raise AssertionError("no triage expected")

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(
                ScoutDisposition.WARM,
                measurement=self.measurement(),
                format_observation=second,
            )

        async def search(_directive) -> SearchBatch:
            raise AssertionError("no search expected")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
        )

        report = await coordinator.run_once()

        self.assertEqual(report.scout_failures, 1)
        self.assertEqual(report.scouted_hold, 1)
        stored = self.registry.get_candidate(candidate.source_key)
        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored.state, SourceState.HOLD)
        self.assertEqual(
            self.registry.get_format_observation(candidate.source_key),
            first,
        )
        reason = self.registry.suppression_reason(stored)
        self.assertIsNotNone(reason)
        self.assertIn("format observation failed closed", reason)

    async def test_deterministic_search_commits_identity_and_coverage_serially(self) -> None:
        cell = SearchCell(
            mechanism="proxy_access",
            institution="university",
            period="1998",
            artifact="trace",
        )
        coverage = ResidualSearchLedger(self.registry.connection)
        coverage.ensure_cell(cell)
        scheduler = SearchCellScheduler(coverage)
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=0,
                warm_target=0,
                cold_min=1,
                cold_target=1,
                triage_batch=4,
                scout_parallelism=1,
                max_search_directives=1,
            ),
            residual_search_scheduler=scheduler,
        )
        identities = SearchIdentityLedger(self.registry.connection)

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            raise AssertionError("new search results are committed after this cycle")

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            raise AssertionError("no scout expected")

        async def agent_search(_directive) -> SearchBatch:
            raise AssertionError("ordinary LLM refill must be suppressed")

        async def deterministic(plan) -> DeterministicSearchBatch:
            first = canonicalize_search_result(
                RawSearchResult(
                    provider="fixture",
                    provider_result_id="r1",
                    url="https://repo.example/proxy98.zip",
                    title="1998 University Proxy Trace Dataset",
                    publisher="Example University",
                    identifiers=("10.1234/proxy98",),
                ),
                relevance_score=1.0,
                qualified=True,
            )
            mirror = canonicalize_search_result(
                RawSearchResult(
                    provider="fixture",
                    provider_result_id="r2",
                    url="https://mirror.example/proxy98.zip",
                    title="1998 University Proxy Trace Dataset",
                    publisher="Example University",
                    identifiers=("10.1234/proxy98",),
                ),
                relevance_score=1.0,
                qualified=True,
            )
            return DeterministicSearchBatch(
                backend="fixture",
                query=plan.query,
                actor="deterministic:test",
                results=(first, mirror),
                search_cost_seconds=0.1,
            )

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            manager,
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=agent_search,
            deterministic_search_executor=deterministic,
            search_identity_ledger=identities,
        )

        report = await coordinator.run_once()

        self.assertEqual(report.deterministic_search_plans_planned, 1)
        self.assertEqual(report.deterministic_search_episodes, 1)
        self.assertEqual(report.search_episodes, 1)
        self.assertEqual(report.search_candidates_registered, 1)
        self.assertEqual(report.search_candidates_dropped, 1)
        stats = coverage.stats(cell)
        self.assertEqual(stats.attempts, 1)
        self.assertEqual(stats.result_count, 2)
        self.assertEqual(stats.duplicate_results, 1)
        self.assertEqual(stats.unique_roots, 1)
        self.assertEqual(stats.new_families, 1)
        self.assertEqual(stats.qualified_roots, 1)
        discovered = self.registry.list_candidates(state=SourceState.DISCOVERED)
        self.assertEqual(len(discovered), 1)
        self.assertEqual(discovered[0].discovered_by, "deterministic:fixture")

    async def test_background_cdxj_is_deferred_while_foreground_search_runs(self) -> None:
        bulk = SourceCandidate(
            canonical_entrypoint="https://archive.example/shard.cdxj",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="arquivo-catalog:test",
            discovery_strategy="DETERMINISTIC_AUDITED_CATALOG_EXPANSION",
            expected_volume=100_000,
            temporal_semantics_prior=1.0,
            enumerability_prior=1.0,
            direct_evidence_prior=1.0,
            confidence=1.0,
        )
        self.to_scout_ready(bulk)
        self.hold_unknown_format_source("foreground-format")
        scout_calls = 0
        search_calls = 0

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            return TriageResult(TriageDisposition.SCOUT)

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            nonlocal scout_calls
            scout_calls += 1
            return ScoutResult(ScoutDisposition.WARM, self.measurement())

        async def search(_directive) -> SearchBatch:
            nonlocal search_calls
            search_calls += 1
            return SearchBatch(backend="test", query="foreground", actor="test")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(cold_min=1, cold_target=1),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
        )

        report = await coordinator.run_once()

        self.assertGreaterEqual(search_calls, 1)
        self.assertEqual(scout_calls, 0)
        self.assertEqual(report.background_bulk_deferred, 1)
        self.assertEqual(report.background_bulk_steps, 0)
        self.assertEqual(
            self.registry.get_candidate(bulk.source_key).state,
            SourceState.SCOUT_READY,
        )

    async def test_background_cdxj_scout_runs_only_when_foreground_is_idle(self) -> None:
        bulk = SourceCandidate(
            canonical_entrypoint="https://archive.example/shard.cdxj",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="arquivo-catalog:test",
            discovery_strategy="DETERMINISTIC_AUDITED_CATALOG_EXPANSION",
            expected_volume=100_000,
            temporal_semantics_prior=1.0,
            enumerability_prior=1.0,
            direct_evidence_prior=1.0,
            confidence=1.0,
        )
        self.to_scout_ready(bulk)
        scout_calls = 0

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            raise AssertionError("no triage expected")

        async def scout(candidate: SourceCandidate) -> ScoutResult:
            nonlocal scout_calls
            scout_calls += 1
            self.assertEqual(candidate.source_key, bulk.source_key)
            return ScoutResult(ScoutDisposition.WARM, self.measurement())

        async def search(_directive) -> SearchBatch:
            raise AssertionError("background CDXJ must not invoke search")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(cold_min=0, cold_target=0),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
        )

        report = await coordinator.run_once()

        self.assertEqual(scout_calls, 1)
        self.assertEqual(report.background_bulk_steps, 1)
        self.assertEqual(report.background_bulk_deferred, 0)
        self.assertEqual(
            self.registry.get_candidate(bulk.source_key).state,
            SourceState.WARM,
        )

    async def test_posix_flock_rejects_second_local_coordinator(self) -> None:
        candidate = self.candidate("blocking-triage")
        self.registry.register_proposal(candidate)
        entered = asyncio.Event()
        release = asyncio.Event()

        async def blocking_triage(_candidate: SourceCandidate) -> TriageResult:
            entered.set()
            await release.wait()
            return TriageResult(TriageDisposition.SCOUT)

        async def unused_scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(ScoutDisposition.HOLD)

        async def unused_search(_directive) -> SearchBatch:
            return SearchBatch(backend="test", query="unused", actor="test")

        manager = self.manager()
        first = SourceDiscoveryCoordinator(
            self.registry,
            manager,
            lock_path=self.lock_path,
            triage_executor=blocking_triage,
            scout_executor=unused_scout,
            search_executor=unused_search,
        )
        second = SourceDiscoveryCoordinator(
            self.registry,
            manager,
            lock_path=self.lock_path,
            triage_executor=blocking_triage,
            scout_executor=unused_scout,
            search_executor=unused_search,
        )

        running = asyncio.create_task(first.run_once())
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        with self.assertRaises(CoordinatorBusyError):
            await second.run_once()
        release.set()
        await asyncio.wait_for(running, timeout=1.0)

    async def test_startup_recovers_stranded_scout_with_retry_suppression(self) -> None:
        candidate = self.candidate("stranded")
        self.to_scout_ready(candidate)
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            return TriageResult(TriageDisposition.SCOUT)

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            raise AssertionError("suppressed recovered scout must not run in same cycle")

        async def search(_directive) -> SearchBatch:
            return SearchBatch(backend="test", query="unused", actor="test")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
            failure_retry_seconds=60.0,
        )

        report = await coordinator.run_once()

        recovered = self.registry.get_candidate(candidate.source_key)
        self.assertEqual(report.recovered_scouts, 1)
        self.assertEqual(recovered.state, SourceState.SCOUT_READY)
        self.assertIsNotNone(self.registry.suppression_reason(recovered))

    async def test_scout_exception_returns_claim_to_retryable_scout_ready(self) -> None:
        candidate = self.candidate("transient")
        self.to_scout_ready(candidate)

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            return TriageResult(TriageDisposition.SCOUT)

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            raise RuntimeError("temporary provider failure")

        async def search(_directive) -> SearchBatch:
            return SearchBatch(backend="test", query="unused", actor="test")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
            failure_retry_seconds=60.0,
        )

        report = await coordinator.run_once()

        retried = self.registry.get_candidate(candidate.source_key)
        self.assertEqual(report.scout_failures, 1)
        self.assertEqual(retried.state, SourceState.SCOUT_READY)
        self.assertIn("temporary provider failure", self.registry.suppression_reason(retried) or "")

    async def test_scout_children_are_committed_after_io_with_dedup_and_lineage(self) -> None:
        parent = self.candidate("parent")
        self.to_scout_ready(parent)
        child = SourceCandidate(
            canonical_entrypoint="https://resources.example/catalog/",
            source_family="RESOURCE_CATALOG",
            level=SourceLevel.COLLECTION,
            discovered_by="scrapy_sidecar",
            discovery_strategy="DETERMINISTIC_LINK_EXPANSION",
            confidence=0.8,
        )

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            return TriageResult(TriageDisposition.SCOUT)

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(
                ScoutDisposition.HOLD,
                discovered_candidates=(child, child, parent),
                edge_relation="links_to_resource",
            )

        async def search(_directive) -> SearchBatch:
            return SearchBatch(backend="test", query="unused", actor="test")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
        )

        report = await coordinator.run_once()

        self.assertEqual(self.registry.get_candidate(parent.source_key).state, SourceState.HOLD)
        stored_child = self.registry.get_candidate(child.source_key)
        self.assertIsNotNone(stored_child)
        self.assertEqual(stored_child.state, SourceState.DISCOVERED)
        self.assertEqual(self.registry.children(parent.source_key), [child.source_key])
        self.assertEqual(report.scout_children_registered, 1)
        self.assertEqual(report.scout_edges_added, 1)
        self.assertEqual(report.scout_children_dropped, 2)

    async def test_search_executor_cannot_overfill_directive_budget(self) -> None:
        self.hold_unknown_format_source("budget-cap")

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            return TriageResult(TriageDisposition.SCOUT)

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(ScoutDisposition.HOLD)

        async def search(directive) -> SearchBatch:
            candidates = tuple(self.candidate(f"found-{index}") for index in range(4))
            return SearchBatch(
                backend="test-search",
                query=f"query:{directive.strategy}",
                actor="agent:test",
                candidates=candidates,
                search_cost_seconds=1.0,
            )

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(cold_min=1, cold_target=1),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
        )

        report = await coordinator.run_once()

        self.assertEqual(report.search_candidates_registered, 1)
        self.assertEqual(report.search_candidates_dropped, 3)
        self.assertEqual(len(self.registry.list_candidates(state=SourceState.DISCOVERED)), 1)


if __name__ == "__main__":
    unittest.main()
