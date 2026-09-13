import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import authority_digest
from creeper.storage.telemetry_store import RuntimeTelemetryStore
from scripts.run_v5_full_loop_canary import (
    CanaryCheckpointError,
    run_canary,
    validate_canary_checkpoints,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class V5FullLoopClosureCanaryTests(unittest.TestCase):
    def _fixture(self, root: Path) -> dict[str, Path]:
        baseline_dir = root / "baseline" / "merged-v5-canary"
        baseline_dir.mkdir(parents=True)
        annual_hashes = {}
        for year in range(1996, 2002):
            path = baseline_dir / f"{year}.txt"
            path.write_text("", encoding="utf-8")
            annual_hashes[path.name] = _sha256(path)
        candidate_pool = baseline_dir / "candidate_pool.txt"
        candidate_pool.write_text("", encoding="utf-8")

        model = root / "eed-model.json"
        model.write_text(
            json.dumps(
                {"tld": ["uk"], "lang": ["eng"], "perc_of_tld": [1]},
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        authority = {
            "baseline_id": baseline_dir.name,
            "annual_file_hashes": annual_hashes,
            "candidate_file_hash": _sha256(candidate_pool),
            "model_hash": _sha256(model),
            "baseline_eed": "1",
        }
        authority["authority_digest"] = authority_digest(**authority)
        authority_path = root / "baseline-manifest.json"
        authority_path.write_text(json.dumps(authority, sort_keys=True), encoding="utf-8")
        index_path = root / "baseline.sqlite3"
        BaselineIndex.build(
            baseline_dir=baseline_dir,
            output_path=index_path,
            authority_manifest=authority_path,
        ).close()

        documentation = root / "documentation.docx"
        documentation.write_bytes(b"Synthetic V5 canary; no network result.")
        return {
            "runtime_root": root / "runtime",
            "baseline_manifest": authority_path,
            "baseline_index": index_path,
            "eed_model": model,
            "documentation": documentation,
            "output_dir": root / "output",
        }

    def test_deterministic_full_loop_reaches_final_reward_and_verifier(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._fixture(Path(tmp))
            result = run_canary(**paths)

            self.assertEqual(
                result.stages,
                (
                    "discovery",
                    "measurement",
                    "activation",
                    "production",
                    "final_reward",
                    "allocation_ranking_change",
                    "formal_export",
                    "independent_verifier",
                ),
            )
            self.assertTrue(result.archive_path.is_file())
            self.assertGreater(result.final_reward_eed, 0)
            self.assertNotEqual(result.ranking_before, result.ranking_after)
            self.assertTrue(result.verifier_ready)
            self.assertTrue(result.report_path.is_file())

            readiness = paths["runtime_root"] / "readiness.json"
            self.assertTrue(readiness.is_file())
            with RuntimeTelemetryStore(paths["runtime_root"] / "telemetry.sqlite3") as telemetry:
                snapshot = telemetry.snapshot()
            self.assertEqual(snapshot.counters["v5_canary_runs"], 1)
            self.assertEqual(snapshot.gauges["v5_canary_independent_verifier_pass"], 1.0)

    def test_missing_readiness_or_telemetry_checkpoint_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._fixture(Path(tmp))
            paths["runtime_root"].mkdir(parents=True)
            with self.assertRaises(CanaryCheckpointError):
                validate_canary_checkpoints(**paths)

            (paths["runtime_root"] / "readiness.json").write_text("{}", encoding="utf-8")
            with self.assertRaises(CanaryCheckpointError):
                validate_canary_checkpoints(**paths)


if __name__ == "__main__":
    unittest.main()
