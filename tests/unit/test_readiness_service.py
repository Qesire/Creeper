from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceCapsule
from creeper.readiness_cli import run_service
from creeper.storage.evidence_store import EvidenceStore


class ReadinessServiceTests(unittest.TestCase):
    @staticmethod
    def _build_baseline(
        root: Path,
        *,
        annual: dict[int, str] | None = None,
        output_name: str = "baseline.sqlite3",
    ) -> Path:
        task = root / ("task-" + output_name.replace(".", "-"))
        baseline_dir = task / "merged260912-3"
        baseline_dir.mkdir(parents=True)
        annual = annual or {}
        for year in range(1996, 2002):
            (baseline_dir / f"{year}.txt").write_text(
                annual.get(year, ""),
                encoding="utf-8",
            )
        (baseline_dir / "candidate_pool.txt").write_text("", encoding="utf-8")
        output = root / output_name
        BaselineIndex.build(task, output).close()
        return output

    @staticmethod
    def _model(root: Path) -> Path:
        path = root / "eed-model.json"
        path.write_text(
            json.dumps(
                {
                    "tld": ["org"],
                    "lang": ["eng"],
                    "perc_of_tld": ["100"],
                }
            ),
            encoding="utf-8",
        )
        return path

    @staticmethod
    def _capsule() -> EvidenceCapsule:
        return EvidenceCapsule(
            "novel.org",
            1997,
            "wayback",
            "capture_timestamp_year",
            "19970101000000",
            "http://novel.org/",
            "a" * 64,
            "evidence-v1",
        )

    def test_once_publishes_readiness_and_gate_markers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = root / "runtime"
            runtime.mkdir()
            baseline = self._build_baseline(root)
            model = self._model(root)
            evidence = EvidenceStore(runtime / "evidence.sqlite3")
            evidence.put(self._capsule())
            evidence.close()
            emitted: list[str] = []

            report = run_service(
                runtime,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
                once=True,
                emit=emitted.append,
            )

            readiness_root = runtime / "readiness"
            self.assertTrue(report.formal_gate_reached)
            self.assertTrue((readiness_root / "readiness.json").is_file())
            self.assertTrue((readiness_root / "prewarm-ready.json").is_file())
            self.assertTrue((readiness_root / "formal-gate-ready.json").is_file())
            payload = json.loads(
                (readiness_root / "readiness.json").read_text(encoding="utf-8")
            )
            self.assertEqual(payload["novel_eed"], "1")
            self.assertEqual(payload["growth_rate"], "0.05")
            self.assertEqual(len(emitted), 1)

    def test_baseline_rebase_removes_stale_gate_markers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = root / "runtime"
            runtime.mkdir()
            baseline = self._build_baseline(root)
            model = self._model(root)
            evidence = EvidenceStore(runtime / "evidence.sqlite3")
            evidence.put(self._capsule())
            evidence.close()

            first = run_service(
                runtime,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
                once=True,
                emit=lambda _payload: None,
            )
            marker = runtime / "readiness" / "formal-gate-ready.json"
            self.assertTrue(first.formal_gate_reached)
            self.assertTrue(marker.exists())

            replacement = self._build_baseline(
                root,
                annual={1997: "novel.org\n"},
                output_name="replacement.sqlite3",
            )
            os.replace(replacement, baseline)
            os.utime(baseline, None)

            second = run_service(
                runtime,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
                once=True,
                emit=lambda _payload: None,
            )

            self.assertFalse(second.formal_gate_reached)
            self.assertEqual(second.novel_eed, "0")
            self.assertFalse(marker.exists())
            self.assertFalse(
                (runtime / "readiness" / "prewarm-ready.json").exists()
            )


if __name__ == "__main__":
    unittest.main()
