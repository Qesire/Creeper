from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs

import httpx

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import (
    baseline_authority_signature,
    eed_model_authority_signature,
)
from creeper.evidence.platform_harvest import (
    PlatformHarvestState,
    PlatformYearHarvestWorker,
    platform_authority_digest,
)
from creeper.evidence.platform_admission import (
    PlatformYearAdmission,
    PlatformYearAdmissionPolicy,
    PlatformYearObservation,
)
from creeper.evidence.providers.async_cdx import AsyncWaybackCDXClient
from creeper.runtime.readiness import IncrementalReadinessRuntime
from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.production_value import ProductionValueModel
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class PlatformYearHarvestIntegrationTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _authority_fixture(root: Path) -> tuple[Path, Path, str, str]:
        model = root / "eed-model.json"
        model.write_text(
            json.dumps(
                {
                    "tld": ["com", "org", "net"],
                    "lang": ["eng", "eng", "eng"],
                    "perc_of_tld": ["50", "100", "75"],
                }
            ),
            encoding="utf-8",
        )
        task_root = root / "baseline-task"
        merged = task_root / "merged"
        merged.mkdir(parents=True)
        for year in range(1996, 2002):
            (merged / f"{year}.txt").write_text("", encoding="utf-8")
        (merged / "candidate_pool.txt").write_text("", encoding="utf-8")
        baseline = root / "baseline.sqlite3"
        BaselineIndex.build(task_root, baseline).close()
        baseline_signature = baseline_authority_signature(baseline)
        model_signature = eed_model_authority_signature(model)
        return baseline, model, baseline_signature, model_signature

    async def test_three_page_harvest_survives_restart_and_completes_exact_set(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            control_path = root / "control.sqlite3"
            evidence_path = root / "evidence.sqlite3"
            seen_resume: list[str | None] = []

            async def handler(request):
                query = parse_qs(request.url.query.decode())
                resume = query.get("resumeKey", [None])[0]
                seen_resume.append(resume)
                if resume is None:
                    payload = [
                        ["urlkey", "timestamp", "original", "statuscode"],
                        ["com,example,a)/x", "19970101000000", "http://a.example.com/x", "200"],
                        ["com,example,a)/y", "19970102000000", "http://a.example.com/y", "200"],
                        ["com,example,b)/", "19970103000000", "http://b.example.com/", "302"],
                        ["resume-1"],
                    ]
                elif resume == "resume-1":
                    payload = [
                        ["urlkey", "timestamp", "original", "statuscode"],
                        ["com,example,b)/again", "19970104000000", "http://b.example.com/again", "200"],
                        ["com,example,c)/", "19970105000000", "http://c.example.com/", "200"],
                        ["resume-2"],
                    ]
                elif resume == "resume-2":
                    payload = [
                        ["urlkey", "timestamp", "original", "statuscode"],
                        ["com,example,d)/", "19970106000000", "http://d.example.com/", "200"],
                        ["com,example,e)/", "19980101000000", "http://e.example.com/", "200"],
                    ]
                else:
                    raise AssertionError(f"unexpected resume key: {resume}")
                return httpx.Response(
                    200,
                    content=json.dumps(payload).encode(),
                    request=request,
                )

            async def make_client():
                return AsyncWaybackCDXClient(
                    transport=httpx.MockTransport(handler),
                    max_retries=0,
                    limit=100,
                )

            bootstrap = await make_client()
            template_hash = bootstrap.platform_year_request_template_hash(
                "example.com",
                1997,
                policy_version="platform-v1",
            )
            await bootstrap.aclose()

            control = ControlStore(control_path)
            admission = PlatformYearAdmission(
                control,
                policy=PlatformYearAdmissionPolicy(max_tasks=1),
            )
            admission_report = admission.admit(
                [
                    PlatformYearObservation(
                        provider="wayback",
                        subject="example.com",
                        target_year=1997,
                        request_template_hash=template_hash,
                        policy_version="platform-v1",
                        source_key="source:example",
                        reservoir_id="reservoir:example",
                        authority_digest="authority-v1",
                    )
                ]
            )
            self.assertEqual(admission_report.admitted, 1)
            seed = admission_report.tasks[0]
            control.close()

            # Restart the entire control/evidence/provider stack after every
            # page.  The continuation must come only from durable state.
            for expected_state in (
                PlatformHarvestState.PARTIAL,
                PlatformHarvestState.PARTIAL,
                PlatformHarvestState.COMPLETE,
            ):
                control = ControlStore(control_path)
                evidence = EvidenceStore(evidence_path)
                client = await make_client()
                worker = PlatformYearHarvestWorker(
                    control_store=control,
                    evidence_store=evidence,
                    providers={"wayback": client},
                    owner="platform-worker",
                    claim_batch_size=1,
                    retry_base_seconds=0,
                )
                report = await worker.run_once()
                current = control.get_platform_year_harvest(seed.harvest_id)

                self.assertEqual(report.claimed, 1)
                self.assertIsNotNone(current)
                self.assertEqual(current.state, expected_state)

                await client.aclose()
                evidence.close()
                control.close()

            control = ControlStore(control_path)
            evidence = EvidenceStore(evidence_path)
            final = control.get_platform_year_harvest(seed.harvest_id)
            self.assertIsNotNone(final)
            self.assertEqual(final.state, PlatformHarvestState.COMPLETE)
            self.assertEqual(final.page_number, 3)
            self.assertEqual(final.requests, 3)
            self.assertEqual(final.rows_seen, 7)
            self.assertEqual(final.unique_host_years_seen, 4)
            self.assertIsNone(final.resume_key)
            self.assertIsNotNone(final.completed_at)
            self.assertEqual(final.source_key, "source:example")
            self.assertEqual(final.reservoir_id, "reservoir:example")
            self.assertEqual(final.authority_digest, "authority-v1")
            self.assertTrue(final.exposure_id)
            self.assertEqual(seen_resume, [None, "resume-1", "resume-2"])

            canonical = evidence.canonical_host_year_capsules()
            self.assertEqual(
                {(item.hostname, item.year) for item in canonical},
                {
                    ("a.example.com", 1997),
                    ("b.example.com", 1997),
                    ("c.example.com", 1997),
                    ("d.example.com", 1997),
                },
            )
            self.assertTrue(
                all(
                    item.extraction_method == "cdx_harvest_platform_year"
                    for item in canonical
                )
            )
            provenance = evidence.resolve_platform_year_provenance(
                source_key="source:example",
                reservoir_id="reservoir:example",
                exposure_id=final.exposure_id,
            )
            self.assertEqual(len(provenance), 4)
            self.assertEqual(
                {(item.hostname, item.year) for item in provenance},
                {(item.hostname, item.year) for item in canonical},
            )
            self.assertEqual(
                control.get_production_exposure(final.exposure_id).state.value,
                "READ_COMPLETE",
            )
            frontier = evidence.max_platform_year_provenance_sequence(
                source_key="source:example",
                reservoir_id="reservoir:example",
                exposure_id=final.exposure_id,
            )
            closed = control.finalize_platform_year_harvest(
                final.harvest_id,
                final_eed=1.25,
                accepted_host_years=4,
                evidence_frontier=frontier,
                authority_digest="authority-v1",
            )
            self.assertTrue(closed)
            finalized = control.get_platform_year_harvest(final.harvest_id)
            self.assertEqual(finalized.final_eed, 1.25)
            self.assertEqual(finalized.evidence_frontier, frontier)
            self.assertEqual(
                control.get_production_exposure(final.exposure_id).state.value,
                "FINAL_CLOSED",
            )
            self.assertTrue(
                control.finalize_platform_year_harvest(
                    final.harvest_id,
                    final_eed=1.25,
                    accepted_host_years=4,
                    evidence_frontier=frontier,
                    authority_digest="authority-v1",
                )
            )
            evidence.close()
            control.close()

    async def test_one_page_local_budget_stops_partial_without_false_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            calls = 0

            async def handler(request):
                nonlocal calls
                calls += 1
                payload = [
                    ["urlkey", "timestamp", "original", "statuscode"],
                    ["com,example,a)/", "19970101000000", "http://a.example.com/", "200"],
                    ["resume-next"],
                ]
                return httpx.Response(
                    200,
                    content=json.dumps(payload).encode(),
                    request=request,
                )

            control = ControlStore(root / "control.sqlite3")
            evidence = EvidenceStore(root / "evidence.sqlite3")
            client = AsyncWaybackCDXClient(
                transport=httpx.MockTransport(handler),
                max_retries=0,
                limit=100,
            )
            template_hash = client.platform_year_request_template_hash(
                "example.com",
                1997,
                policy_version="platform-v1",
            )
            task = PlatformYearAdmission(control).admit(
                [
                    PlatformYearObservation(
                        provider="wayback",
                        subject="example.com",
                        target_year=1997,
                        request_template_hash=template_hash,
                        policy_version="platform-v1",
                        source_key="source:example",
                        reservoir_id="reservoir:example",
                        authority_digest="authority-v1",
                    )
                ]
            ).tasks[0]
            worker = PlatformYearHarvestWorker(
                control_store=control,
                evidence_store=evidence,
                providers={"wayback": client},
                owner="one-page-budget",
                claim_batch_size=1,
            )

            report = await worker.run_once()
            stored = control.get_platform_year_harvest(task.harvest_id)

            self.assertEqual(calls, 1)
            self.assertEqual(report.pages_committed, 1)
            self.assertEqual(report.partial, 1)
            self.assertEqual(report.complete, 0)
            self.assertEqual(stored.state, PlatformHarvestState.PARTIAL)
            self.assertEqual(stored.resume_key, "resume-next")
            self.assertIsNone(stored.completed_at)
            self.assertEqual(
                control.get_production_exposure(stored.exposure_id).state.value,
                "RUNNING",
            )

            await client.aclose()
            evidence.close()
            control.close()

    async def test_complete_platform_task_closes_final_source_reward(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            baseline, model, baseline_signature, model_signature = (
                self._authority_fixture(root)
            )
            control = ControlStore(runtime_root / "control.sqlite3")
            registry = SourceDiscoveryRegistry(control)
            registry.set_scout_authority(
                baseline_signature=baseline_signature,
                model_signature=model_signature,
            )
            candidate = SourceCandidate(
                canonical_entrypoint="https://archive.example/platform.jsonl",
                source_family="BULK_ARTIFACT",
                level=SourceLevel.SOURCE,
                discovered_by="integration",
                discovery_strategy="PLATFORM_FINAL_REWARD",
                expected_volume=10,
                temporal_semantics_prior=1.0,
                enumerability_prior=1.0,
                direct_evidence_prior=1.0,
                confidence=1.0,
            )
            registry.register_proposal(candidate)
            authority_digest = platform_authority_digest(
                baseline_signature=baseline_signature,
                model_signature=model_signature,
            )

            async def handler(request):
                payload = [
                    ["urlkey", "timestamp", "original", "statuscode"],
                    [
                        "com,example,a)/",
                        "19970101000000",
                        "http://a.example.com/",
                        "200",
                    ],
                ]
                return httpx.Response(
                    200,
                    content=json.dumps(payload).encode(),
                    request=request,
                )

            client = AsyncWaybackCDXClient(
                transport=httpx.MockTransport(handler),
                max_retries=0,
                limit=100,
            )
            template_hash = client.platform_year_request_template_hash(
                "example.com",
                1997,
                policy_version="platform-v1",
            )
            task = PlatformYearAdmission(control).admit(
                [
                    PlatformYearObservation(
                        provider="wayback",
                        subject="example.com",
                        target_year=1997,
                        request_template_hash=template_hash,
                        policy_version="platform-v1",
                        source_key=candidate.source_key,
                        reservoir_id="reservoir:platform",
                        authority_digest=authority_digest,
                    )
                ]
            ).tasks[0]
            evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
            worker = PlatformYearHarvestWorker(
                control_store=control,
                evidence_store=evidence,
                providers={"wayback": client},
                owner="platform-final-reward",
                claim_batch_size=1,
                retry_base_seconds=0,
            )
            report = await worker.run_once()
            self.assertEqual(report.complete, 1)
            exposure = control.get_production_exposure(task.exposure_id)
            self.assertIsNotNone(exposure)
            assert exposure is not None
            self.assertEqual(exposure.state.value, "READ_COMPLETE")
            run = registry.get_source_run_outcome(
                candidate.source_key,
                reservoir_id="reservoir:platform",
                lease_id=task.exposure_id,
                baseline_signature=baseline_signature,
                model_signature=model_signature,
            )
            self.assertIsNotNone(run)
            assert run is not None
            self.assertTrue(run.read_complete)
            control.close()
            evidence.close()
            await client.aclose()

            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="10",
            ) as readiness:
                readiness.sync_until_current()

            control = ControlStore(runtime_root / "control.sqlite3")
            registry = SourceDiscoveryRegistry(control)
            final_exposure = control.get_production_exposure(task.exposure_id)
            self.assertIsNotNone(final_exposure)
            assert final_exposure is not None
            self.assertEqual(final_exposure.state.value, "FINAL_CLOSED")
            self.assertGreater(final_exposure.final_accepted_eed, 0.0)
            final_task = control.get_platform_year_harvest(task.harvest_id)
            self.assertIsNotNone(final_task)
            assert final_task is not None
            self.assertEqual(final_task.terminal_reason, "finalized")
            self.assertEqual(final_task.final_eed, final_exposure.final_accepted_eed)
            final_run = registry.get_source_run_outcome(
                candidate.source_key,
                reservoir_id="reservoir:platform",
                lease_id=task.exposure_id,
                baseline_signature=baseline_signature,
                model_signature=model_signature,
            )
            self.assertIsNotNone(final_run)
            assert final_run is not None
            self.assertTrue(final_run.closed)
            estimate = ProductionValueModel(registry).estimate(
                candidate,
                baseline_signature=baseline_signature,
                model_signature=model_signature,
            )
            self.assertEqual(estimate.closed_runs, 1)
            self.assertEqual(estimate.zero_runs, 0)
            control.close()


if __name__ == "__main__":
    unittest.main()
