from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs

import httpx

from creeper.evidence.platform_harvest import (
    PlatformHarvestState,
    PlatformYearHarvestWorker,
)
from creeper.evidence.providers.async_cdx import AsyncWaybackCDXClient
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class PlatformYearHarvestIntegrationTests(unittest.IsolatedAsyncioTestCase):
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
            seed = control.enqueue_platform_year_harvest(
                provider="wayback",
                subject="example.com",
                target_year=1997,
                request_template_hash=template_hash,
                policy_version="platform-v1",
            )
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
            task = control.enqueue_platform_year_harvest(
                provider="wayback",
                subject="example.com",
                target_year=1997,
                request_template_hash=template_hash,
                policy_version="platform-v1",
            )
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

            await client.aclose()
            evidence.close()
            control.close()


if __name__ == "__main__":
    unittest.main()
