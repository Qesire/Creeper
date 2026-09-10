from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.coordinator import ScoutDisposition
from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.scrapy_scout import ScrapyStructuralScoutExecutor
from creeper.source_discovery.scrapy_sidecar import ScrapyScoutRun, ScrapyScoutSpec


class _ReplayLauncher:
    def __init__(self, *, mode: str = "success") -> None:
        self.mode = mode
        self.specs: list[ScrapyScoutSpec] = []

    @staticmethod
    def _row(spec: ScrapyScoutSpec, page_url: str) -> bytes:
        payload = {
            "record_type": "LINK_DISCOVERY",
            "source_key": spec.source_key,
            "page_url": page_url,
            "discovered_url": "https://resources.example/catalog/",
            "anchor_text": "historical archive catalog",
            "depth": 1,
            "same_site": False,
        }
        return (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")

    async def run_async(self, spec: ScrapyScoutSpec) -> ScrapyScoutRun:
        self.specs.append(spec)
        if self.mode == "timeout":
            return ScrapyScoutRun(
                returncode=None,
                elapsed_seconds=10.0,
                timed_out=True,
                spool_path=spec.spool_path,
                jobdir=spec.jobdir,
            )
        if self.mode == "failure":
            return ScrapyScoutRun(
                returncode=17,
                elapsed_seconds=0.2,
                timed_out=False,
                spool_path=spec.spool_path,
                jobdir=spec.jobdir,
            )

        spec.spool_path.parent.mkdir(parents=True, exist_ok=True)
        previous = self._row(spec, "https://example.com/archive/page-a.html")
        latest = self._row(spec, "https://example.com/archive/page-b.html")
        spec.spool_path.write_bytes(previous + latest)
        return ScrapyScoutRun(
            returncode=0,
            elapsed_seconds=0.4,
            timed_out=False,
            spool_path=spec.spool_path,
            jobdir=spec.jobdir,
            # Pretend page-a was committed by an earlier attempt. Generic
            # resource promotion needs two distinct referrers, so parsing only
            # [start_offset, end_offset) would incorrectly yield no child.
            spool_start_offset=len(previous),
            spool_end_offset=len(previous) + len(latest),
        )


class ScrapyStructuralScoutExecutorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.candidate = SourceCandidate(
            canonical_entrypoint="https://example.com/archive/",
            source_family="RESOURCE_DIRECTORY",
            level=SourceLevel.COLLECTION,
            discovered_by="test",
            discovery_strategy="META_SOURCE_SEARCH",
            expected_volume=1000,
            confidence=0.7,
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    async def test_success_replays_full_committed_prefix_for_cross_retry_corroboration(self) -> None:
        launcher = _ReplayLauncher()
        executor = ScrapyStructuralScoutExecutor(launcher, self.root / "work")

        result = await executor(self.candidate)

        self.assertEqual(result.disposition, ScoutDisposition.HOLD)
        self.assertEqual(result.edge_relation, "links_to_resource")
        self.assertEqual(len(result.discovered_candidates), 1)
        child = result.discovered_candidates[0]
        self.assertEqual(child.canonical_entrypoint, "https://resources.example/catalog/")
        self.assertEqual(child.source_family, "RESOURCE_DIRECTORY")
        self.assertIn("inspected 2 committed links", result.reason)
        self.assertEqual(len(launcher.specs), 1)

    async def test_timeout_is_retryable_executor_failure_not_partial_result(self) -> None:
        executor = ScrapyStructuralScoutExecutor(
            _ReplayLauncher(mode="timeout"),
            self.root / "work-timeout",
        )

        with self.assertRaisesRegex(TimeoutError, "hard timeout"):
            await executor(self.candidate)

    async def test_nonzero_sidecar_exit_is_retryable_executor_failure(self) -> None:
        executor = ScrapyStructuralScoutExecutor(
            _ReplayLauncher(mode="failure"),
            self.root / "work-failure",
        )

        with self.assertRaisesRegex(RuntimeError, "rc=17"):
            await executor(self.candidate)


if __name__ == "__main__":
    unittest.main()
