import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from creeper.metrics.readiness import build_readiness_report


class EEDReadinessMetricTests(unittest.TestCase):
    def _model(self, root: Path) -> Path:
        path = root / "model.json"
        path.write_text(
            json.dumps(
                {
                    "tld": ["com", "org", "net"],
                    "lang": ["eng", "eng", "eng"],
                    "perc_of_tld": ["50", "100", "75"],
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_report_excludes_baseline_per_year_and_sums_annual_eed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline"
            accepted = root / "accepted"
            baseline.mkdir()
            accepted.mkdir()
            (baseline / "1996.txt").write_text("known.com\n", encoding="utf-8")
            (baseline / "1997.txt").write_text("baseline.org\n", encoding="utf-8")
            (accepted / "1996.txt").write_text(
                "known.com\nnovel.com\n", encoding="utf-8"
            )
            (accepted / "1997.txt").write_text(
                "baseline.org\nnovel.org\n", encoding="utf-8"
            )
            (accepted / "1998.txt").write_text(
                "novel.com\nnovel.com\n", encoding="utf-8"
            )

            report = build_readiness_report(
                accepted_dir=accepted,
                baseline_dir=baseline,
                model_path=self._model(root),
                baseline_eed="100",
                elapsed_seconds="86400",
                run_id="fixture",
            )

            self.assertEqual(report["annual_novel_eed"], "2.0000000000")
            self.assertEqual(report["annual_eed_per_day"], "2.0000000000")
            self.assertEqual(report["five_percent_delta"], "5.00")
            self.assertEqual(report["eta_to_five_percent_days"], "2.5000000000")
            self.assertEqual(report["annual"]["1996"]["novel_pairs"], 1)
            self.assertEqual(report["annual"]["1997"]["novel_pairs"], 1)
            self.assertEqual(report["annual"]["1998"]["novel_pairs"], 1)

    def test_zero_eed_has_no_finite_eta(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline"
            accepted = root / "accepted"
            baseline.mkdir()
            accepted.mkdir()
            (baseline / "1996.txt").write_text("known.com\n", encoding="utf-8")
            (accepted / "1996.txt").write_text("known.com\n", encoding="utf-8")

            report = build_readiness_report(
                accepted_dir=accepted,
                baseline_dir=baseline,
                model_path=self._model(root),
                baseline_eed="100",
                elapsed_seconds="60",
                run_id="zero",
            )

            self.assertEqual(report["annual_novel_eed"], "0.0000000000")
            self.assertIsNone(report["eta_to_five_percent_days"])
            self.assertEqual(report["confirmed_fraction_of_five_percent"], "0")

    def test_cli_writes_machine_readable_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline"
            accepted = root / "accepted"
            output = root / "report"
            baseline.mkdir()
            accepted.mkdir()
            (baseline / "1996.txt").write_text("known.com\n", encoding="utf-8")
            (accepted / "1996.txt").write_text("novel.com\n", encoding="utf-8")
            model = self._model(root)

            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/run_eed_readiness.py",
                    str(output),
                    "--accepted-dir",
                    str(accepted),
                    "--baseline-dir",
                    str(baseline),
                    "--model",
                    str(model),
                    "--baseline-eed",
                    "100",
                    "--elapsed-seconds",
                    "86400",
                    "--run-id",
                    "cli-fixture",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((output / "run.json").exists())
            self.assertTrue((output / "eed-report.json").exists())
            payload = json.loads((output / "eed-report.json").read_text())
            self.assertEqual(payload["annual_novel_eed"], "0.5000000000")


if __name__ == "__main__":
    unittest.main()
