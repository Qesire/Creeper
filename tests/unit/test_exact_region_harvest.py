from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import httpx

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceCapsule
from creeper.source_discovery.harvest import (
    RegionHarvestError,
    RegionHarvestExecutor,
    RegionHarvestPolicy,
)
from creeper.source_discovery.harvest_service import RegionHarvestService
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import (
    RegionKind,
    RegionState,
    RegionSynopsis,
    child_region,
    compile_candidate_index_space,
)
from creeper.source_discovery.models import (
    MeasurementMode,
    SourceCandidate,
    SourceLevel,
)
from creeper.source_discovery.portfolio import (
    RegionPortfolioPlanner,
    RegionPortfolioPolicy,
)
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class ExactRegionHarvestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        task_root = self.root / "task"
        baseline_root = task_root / "merged260909-3"
        baseline_root.mkdir(parents=True)
        for year in range(1996, 2002):
            value = "known.com\n" if year == 1996 else ""
            (baseline_root / f"{year}.txt").write_text(value, encoding="utf-8")
        (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
        self.baseline = BaselineIndex.build(
            task_root,
            self.root / "baseline.sqlite3",
        )
        self.control = ControlStore(self.root / "control.sqlite3")
        self.evidence = EvidenceStore(self.root / "evidence.sqlite3")
        self.registry = IndexSpaceRegistry(self.control)
        self.counter = 0

    def tearDown(self) -> None:
        self.evidence.close()
        self.control.close()
        self.baseline.close()
        self.tmp.cleanup()

    def _compiled_local(
        self,
        path: Path,
        *,
        direct_authority: bool = True,
    ):
        self.counter += 1
        candidate = SourceCandidate(
            canonical_entrypoint=(
                f"https://archive{self.counter}.example/{path.name}"
            ),
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="DIRECT_EVIDENCE_BULK",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=10_000,
            direct_evidence_prior=1.0 if direct_authority else 0.0,
            enumerability_prior=1.0,
            confidence=0.9,
        )
        compiled = compile_candidate_index_space(
            candidate,
            content_length=path.stat().st_size,
            direct_evidence_authority=direct_authority,
        )
        index = replace(compiled.index, locator=str(path))
        root_region = replace(compiled.root_region, locator=str(path))
        return replace(compiled, index=index, root_region=root_region)

    def _register_ready(self, compiled):
        self.registry.register_index_space(compiled)
        self.registry.mark_region_state(
            compiled.root_region.region_key,
            RegionState.HARVEST_READY,
        )
        region = self.registry.get_region(compiled.root_region.region_key)
        assert region is not None
        return region

    @staticmethod
    def _line(host: str, year: int, stamp: str, suffix: str = "") -> str:
        return (
            f"com,{host})/{suffix} {year}{stamp} "
            f'{{"url":"http://{host}.com/{suffix}"}}\n'
        )

    def test_exact_harvest_keeps_one_real_witness_per_year(self) -> None:
        path = self.root / "exact.cdxj"
        path.write_text(
            "".join(
                [
                    self._line("known", 1996, "0101000000", "a"),
                    self._line("known", 1998, "0202000000", "b"),
                    self._line("novel", 1997, "0303000000", "first"),
                    self._line("novel", 1997, "0404000000", "duplicate"),
                    self._line("novel", 2001, "0505000000", "last"),
                ]
            ),
            encoding="utf-8",
        )
        compiled = self._compiled_local(path)
        region = self._register_ready(compiled)
        executor = RegionHarvestExecutor(
            registry=self.registry,
            baseline=self.baseline,
            evidence_store=self.evidence,
            policy=RegionHarvestPolicy(baseline_batch_size=1),
        )

        report = executor.harvest(region.region_key)

        self.assertIsNotNone(report)
        assert report is not None
        self.assertTrue(report.completed)
        self.assertEqual(report.source_records, 5)
        self.assertEqual(report.host_groups, 2)
        self.assertEqual(report.exact_witnesses, 4)
        self.assertEqual(report.baseline_suppressed_host_years, 1)
        self.assertEqual(report.existing_evidence_suppressed_host_years, 0)
        self.assertEqual(report.direct_capsules_planned, 3)
        self.assertEqual(report.direct_capsules_inserted, 3)
        self.assertIsNone(report.resume_cursor)
        self.assertEqual(
            self.registry.get_region(region.region_key).state,
            RegionState.HARVESTED,
        )

        known = self.evidence.for_hostname("known.com")
        self.assertEqual([(item.year, item.evidence_timestamp) for item in known], [
            (1998, "19980202000000"),
        ])

        novel = self.evidence.for_hostname("novel.com")
        self.assertEqual(
            [(item.year, item.evidence_timestamp) for item in novel],
            [
                (1997, "19970303000000"),
                (2001, "20010505000000"),
            ],
        )
        self.assertTrue(novel[0].record_locator.endswith(":byte:" + novel[0].record_locator.rsplit(":byte:", 1)[-1]))
        self.assertEqual(novel[0].original_url, "http://novel.com/first")
        self.assertEqual(novel[1].original_url, "http://novel.com/last")
        self.assertNotEqual(novel[0].record_locator, novel[1].record_locator)

    def test_region_owns_records_by_start_offset_without_partial_evidence(self) -> None:
        lines = [
            self._line("alpha", 1998, "0101000000"),
            self._line("beta", 1998, "0101000000"),
            self._line("gamma", 1998, "0101000000"),
        ]
        payload = "".join(lines)
        path = self.root / "bounded.cdxj"
        path.write_text(payload, encoding="utf-8")
        compiled = self._compiled_local(path)
        self.registry.register_index_space(compiled)

        first_end = len(lines[0].encode("utf-8"))
        second_end = first_end + len(lines[1].encode("utf-8"))
        start = 7
        end_exclusive = second_end + 7
        region = child_region(
            compiled.root_region,
            kind=RegionKind.BYTE_RANGE,
            byte_start=start,
            byte_end=end_exclusive - 1,
        )
        self.registry.put_region(region)
        self.registry.mark_region_state(
            region.region_key,
            RegionState.HARVEST_READY,
        )
        executor = RegionHarvestExecutor(
            registry=self.registry,
            baseline=self.baseline,
            evidence_store=self.evidence,
        )

        report = executor.harvest(region.region_key)

        self.assertIsNotNone(report)
        assert report is not None
        self.assertTrue(report.completed)
        # The region owns gamma because gamma starts before end_exclusive,
        # so a lossless harvest may read past the byte partition to finish that
        # record. It must still stay bounded by the remaining object bytes plus
        # the one preceding-byte boundary check.
        self.assertLessEqual(
            report.bytes_read,
            len(payload.encode("utf-8")) - start + 1,
        )
        self.assertEqual(self.evidence.for_hostname("alpha.com"), [])
        beta = self.evidence.for_hostname("beta.com")
        self.assertEqual(len(beta), 1)
        self.assertEqual(beta[0].year, 1998)
        gamma = self.evidence.for_hostname("gamma.com")
        self.assertEqual(len(gamma), 1)
        self.assertEqual(gamma[0].year, 1998)

    def test_remote_range_harvest_owns_trailing_cross_boundary_row(self) -> None:
        lines = [
            self._line("alpha", 1998, "0101000000"),
            self._line("beta", 1998, "0101000000"),
            self._line("gamma", 1998, "0101000000"),
        ]
        payload = "".join(lines).encode("utf-8")
        self.counter += 1
        candidate = SourceCandidate(
            canonical_entrypoint=(
                f"https://archive{self.counter}.example/remote.cdxj"
            ),
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="DIRECT_EVIDENCE_BULK",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=10_000,
            direct_evidence_prior=1.0,
            enumerability_prior=1.0,
            confidence=1.0,
        )
        compiled = compile_candidate_index_space(
            candidate,
            range_supported=True,
            content_length=len(payload),
            direct_evidence_authority=True,
        )
        self.registry.register_index_space(compiled)

        first_end = len(lines[0].encode("utf-8"))
        second_end = first_end + len(lines[1].encode("utf-8"))
        start = 7
        end_exclusive = second_end + 7
        region = child_region(
            compiled.root_region,
            kind=RegionKind.BYTE_RANGE,
            byte_start=start,
            byte_end=end_exclusive - 1,
        )
        self.registry.put_region(region)
        self.registry.mark_region_state(
            region.region_key,
            RegionState.HARVEST_READY,
        )

        def handler(request: httpx.Request) -> httpx.Response:
            raw = request.headers.get("range", "")
            self.assertTrue(raw.startswith("bytes="))
            bounds = raw.removeprefix("bytes=")
            left, right = bounds.split("-", 1)
            request_start = int(left)
            request_end = min(int(right), len(payload) - 1)
            body = payload[request_start:request_end + 1]
            return httpx.Response(
                206,
                headers={
                    "Content-Range": (
                        f"bytes {request_start}-{request_end}/{len(payload)}"
                    ),
                    "Content-Length": str(len(body)),
                    "Accept-Ranges": "bytes",
                },
                stream=httpx.ByteStream(body),
                request=request,
            )

        client = httpx.Client(transport=httpx.MockTransport(handler))
        try:
            executor = RegionHarvestExecutor(
                registry=self.registry,
                baseline=self.baseline,
                evidence_store=self.evidence,
                http_client=client,
            )

            report = executor.harvest(region.region_key)
        finally:
            client.close()

        self.assertIsNotNone(report)
        assert report is not None
        self.assertTrue(report.completed)
        self.assertEqual(self.evidence.for_hostname("alpha.com"), [])
        beta = self.evidence.for_hostname("beta.com")
        self.assertEqual(len(beta), 1)
        self.assertEqual(beta[0].year, 1998)
        gamma = self.evidence.for_hostname("gamma.com")
        self.assertEqual(len(gamma), 1)
        self.assertEqual(gamma[0].year, 1998)

    def test_adjacent_regions_cover_crossing_record_exactly_once(self) -> None:
        lines = [
            self._line("alpha", 1997, "0101000000"),
            self._line("beta", 1998, "0101000000"),
            self._line("gamma", 1999, "0101000000"),
        ]
        path = self.root / "adjacent.cdxj"
        payload = "".join(lines).encode("utf-8")
        path.write_bytes(payload)
        compiled = self._compiled_local(path)
        self.registry.register_index_space(compiled)

        alpha_end = len(lines[0].encode("utf-8"))
        beta_end = alpha_end + len(lines[1].encode("utf-8"))
        # Split in the middle of beta. Left owns beta because beta starts
        # before the split; right must discard the beta tail and begin at gamma.
        split = alpha_end + max(1, (beta_end - alpha_end) // 2)
        left = child_region(
            compiled.root_region,
            kind=RegionKind.BYTE_RANGE,
            byte_start=0,
            byte_end=split - 1,
        )
        right = child_region(
            compiled.root_region,
            kind=RegionKind.BYTE_RANGE,
            byte_start=split,
            byte_end=len(payload) - 1,
        )
        for region in (left, right):
            self.registry.put_region(region)
            self.registry.mark_region_state(
                region.region_key,
                RegionState.HARVEST_READY,
            )

        executor = RegionHarvestExecutor(
            registry=self.registry,
            baseline=self.baseline,
            evidence_store=self.evidence,
        )
        left_report = executor.harvest(left.region_key)
        right_report = executor.harvest(right.region_key)

        self.assertIsNotNone(left_report)
        self.assertIsNotNone(right_report)
        assert left_report is not None and right_report is not None
        self.assertTrue(left_report.completed)
        self.assertTrue(right_report.completed)
        self.assertEqual(self.evidence.host_year_count(), 3)
        self.assertEqual(
            [(item.hostname, item.year) for item in self.evidence.host_years_after(0)],
            [
                ("alpha.com", 1997),
                ("beta.com", 1998),
                ("gamma.com", 1999),
            ],
        )
        self.assertEqual(len(self.evidence.for_hostname("beta.com")), 1)

    def test_small_record_limit_resumes_at_exact_line_boundary(self) -> None:
        path = self.root / "resume.cdxj"
        path.write_text(
            "".join(
                [
                    self._line("alpha", 1997, "0101000000"),
                    self._line("beta", 1998, "0101000000"),
                    self._line("gamma", 1999, "0101000000"),
                ]
            ),
            encoding="utf-8",
        )
        compiled = self._compiled_local(path)
        region = self._register_ready(compiled)
        executor = RegionHarvestExecutor(
            registry=self.registry,
            baseline=self.baseline,
            evidence_store=self.evidence,
            policy=RegionHarvestPolicy(
                max_records_per_lease=1,
                baseline_batch_size=1,
            ),
        )

        first = executor.harvest(region.region_key)
        self.assertIsNotNone(first)
        assert first is not None
        self.assertFalse(first.completed)
        self.assertIsNotNone(first.resume_cursor)
        cursor1 = first.resume_cursor
        self.assertEqual(
            self.registry.get_region(region.region_key).state,
            RegionState.HARVEST_READY,
        )
        self.assertEqual(
            self.registry.get_region_harvest_cursor(region.region_key),
            cursor1,
        )

        second = executor.harvest(region.region_key)
        self.assertIsNotNone(second)
        assert second is not None
        self.assertFalse(second.completed)
        self.assertGreater(second.resume_cursor or 0, cursor1 or 0)

        third = executor.harvest(region.region_key)
        self.assertIsNotNone(third)
        assert third is not None
        self.assertTrue(third.completed)
        self.assertEqual(
            self.registry.get_region(region.region_key).state,
            RegionState.HARVESTED,
        )
        self.assertIsNone(
            self.registry.get_region_harvest_cursor(region.region_key)
        )
        self.assertEqual(self.evidence.host_year_count(), 3)

    def test_existing_evidence_is_suppressed_before_direct_capsule_write(self) -> None:
        path = self.root / "existing.cdxj"
        path.write_text(
            "".join(
                [
                    self._line("novel", 1998, "0101000000", "old"),
                    self._line("novel", 1999, "0101000000", "new"),
                ]
            ),
            encoding="utf-8",
        )
        self.evidence.put(
            EvidenceCapsule(
                hostname="novel.com",
                year=1998,
                provider="seed",
                temporal_semantics="seed",
                evidence_timestamp="19980101000000",
                source_locator="seed",
                payload_hash="seed-1998",
                policy_version="seed-v1",
            )
        )
        compiled = self._compiled_local(path)
        region = self._register_ready(compiled)
        executor = RegionHarvestExecutor(
            registry=self.registry,
            baseline=self.baseline,
            evidence_store=self.evidence,
        )

        report = executor.harvest(region.region_key)

        self.assertIsNotNone(report)
        assert report is not None
        self.assertEqual(report.existing_evidence_suppressed_host_years, 1)
        self.assertEqual(report.direct_capsules_inserted, 1)
        self.assertEqual(
            [item.year for item in self.evidence.for_hostname("novel.com")],
            [1998, 1999],
        )

    def test_service_closes_portfolio_selection_to_harvested_evidence(self) -> None:
        path = self.root / "service.cdxj"
        path.write_text(
            self._line("novel", 1998, "0101000000"),
            encoding="utf-8",
        )
        compiled = self._compiled_local(path)
        self.registry.register_index_space(compiled)
        self.registry.record_synopsis(
            RegionSynopsis(
                region_key=compiled.root_region.region_key,
                sampled_records=1,
                unique_hosts=1,
                novel_hosts=1,
                observed_host_year_pairs=1,
                novel_host_year_pairs=1,
                novel_eed=1.0,
                bytes_read=path.stat().st_size,
                requests=1,
                measurement_mode=MeasurementMode.HOST_YEAR,
                minhash_values=(11, 13, 17, 19),
                confidence=1.0,
                complete=True,
            )
        )
        self.registry.mark_region_state(
            compiled.root_region.region_key,
            RegionState.HARVEST_READY,
        )
        portfolio = RegionPortfolioPlanner(
            self.registry,
            policy=RegionPortfolioPolicy(confidence_floor=1.0),
        )
        harvest = RegionHarvestExecutor(
            registry=self.registry,
            baseline=self.baseline,
            evidence_store=self.evidence,
        )
        service = RegionHarvestService(
            self.registry,
            portfolio_planner=portfolio,
            harvest_executor=harvest,
        )

        report = service.run_once(max_regions=1)

        self.assertEqual(
            report.selected_regions,
            (compiled.root_region.region_key,),
        )
        self.assertEqual(
            report.completed_regions,
            (compiled.root_region.region_key,),
        )
        self.assertEqual(report.incomplete_regions, ())
        self.assertEqual(report.failed_regions, ())
        self.assertEqual(report.claim_skipped_regions, ())
        self.assertEqual(report.direct_capsules_inserted, 1)
        self.assertEqual(self.evidence.host_year_count(), 1)
        self.assertEqual(
            self.registry.get_region(compiled.root_region.region_key).state,
            RegionState.HARVESTED,
        )

    def test_source_size_drift_after_partitioning_fails_closed(self) -> None:
        path = self.root / "drift.cdxj"
        path.write_text(
            self._line("novel", 1998, "0101000000"),
            encoding="utf-8",
        )
        compiled = self._compiled_local(path)
        region = self._register_ready(compiled)
        # Simulate a mutable remote/local artifact being replaced or appended
        # after tomography established byte authority.
        with path.open("ab") as stream:
            stream.write(
                self._line("late", 1999, "0101000000").encode("utf-8")
            )
        executor = RegionHarvestExecutor(
            registry=self.registry,
            baseline=self.baseline,
            evidence_store=self.evidence,
        )

        with self.assertRaisesRegex(
            RegionHarvestError,
            "source size changed",
        ):
            executor.harvest(region.region_key)

        self.assertEqual(
            self.registry.get_region(region.region_key).state,
            RegionState.HARVEST_READY,
        )
        self.assertEqual(self.evidence.host_year_count(), 0)

    def test_direct_authority_failure_releases_claim_immediately(self) -> None:
        path = self.root / "no-authority.cdxj"
        path.write_text(
            self._line("novel", 1998, "0101000000"),
            encoding="utf-8",
        )
        compiled = self._compiled_local(path, direct_authority=False)
        region = self._register_ready(compiled)
        executor = RegionHarvestExecutor(
            registry=self.registry,
            baseline=self.baseline,
            evidence_store=self.evidence,
        )

        with self.assertRaisesRegex(RegionHarvestError, "authority"):
            executor.harvest(region.region_key)

        self.assertEqual(
            self.registry.get_region(region.region_key).state,
            RegionState.HARVEST_READY,
        )

    def test_harvest_claim_expires_and_can_be_recovered(self) -> None:
        path = self.root / "claim.cdxj"
        path.write_text(
            self._line("novel", 1998, "0101000000"),
            encoding="utf-8",
        )
        now = [100.0]
        control = ControlStore(self.root / "claim-control.sqlite3")
        try:
            registry = IndexSpaceRegistry(control, clock=lambda: now[0])
            compiled = self._compiled_local(path)
            registry.register_index_space(compiled)
            registry.mark_region_state(
                compiled.root_region.region_key,
                RegionState.HARVEST_READY,
            )

            first = registry.claim_region_for_harvest(
                compiled.root_region.region_key,
                owner="worker-a",
                ttl_seconds=10,
            )
            self.assertIsNotNone(first)
            self.assertIsNone(
                registry.claim_region_for_harvest(
                    compiled.root_region.region_key,
                    owner="worker-b",
                    ttl_seconds=10,
                )
            )

            now[0] = 111.0
            second = registry.claim_region_for_harvest(
                compiled.root_region.region_key,
                owner="worker-b",
                ttl_seconds=10,
            )
            self.assertIsNotNone(second)
            with self.assertRaisesRegex(ValueError, "not owned"):
                registry.complete_region_harvest(
                    compiled.root_region.region_key,
                    owner="worker-a",
                )
            registry.release_region_harvest(
                compiled.root_region.region_key,
                owner="worker-b",
                resume_cursor=0,
            )
            self.assertEqual(
                registry.get_region(compiled.root_region.region_key).state,
                RegionState.HARVEST_READY,
            )
        finally:
            control.close()


if __name__ == "__main__":
    unittest.main()
