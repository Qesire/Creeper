import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from creeper.submission_cli import run_export


class SubmissionCliTests(unittest.TestCase):
    def test_declared_reviewed_registry_is_added_to_formal_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = root / "runtime"
            production_config = root / "production.toml"
            registry = root / "reviewed-contracts.json"
            readiness = root / "readiness.json"
            manifest = root / "authority.json"
            production_config.write_text(
                'source_mode = "activated"\n'
                'evidence_contract_registry = "reviewed-contracts.json"\n',
                encoding="utf-8",
            )
            registry.write_text(
                json.dumps(
                    {
                        "registry_version": "reviewed-source-contract-registry-v1",
                        "entries": [],
                    }
                ),
                encoding="utf-8",
            )
            readiness.write_text("{}", encoding="utf-8")

            with (
                patch(
                    "creeper.submission_cli.AuthoritySnapshot.from_manifest_path",
                    return_value=object(),
                ),
                patch(
                    "creeper.submission_cli._build_snapshot_from_readiness",
                    return_value=object(),
                ),
                patch(
                    "creeper.submission_cli.export_runtime_submission",
                    return_value=(root / "submission.zip", object()),
                ) as export,
            ):
                run_export(
                    runtime_data_root=runtime,
                    readiness_report=readiness,
                    baseline_manifest=manifest,
                    baseline_index=root / "baseline.sqlite3",
                    eed_model=root / "model.json",
                    source_root=root,
                    documentation=root / "methods.docx",
                    production_config=production_config,
                    source_reports=(root / "source-report.json",),
                    cdx_audits=(root / "cdx-audit.json",),
                    output_dir=root / "out",
                    name="formal",
                    snapshot_id="snapshot",
                    code_revision="a" * 64,
                )

            kwargs = export.call_args.kwargs
            specs = kwargs["artifact_specs"]
            self.assertEqual(
                [spec.archive_path for spec in specs if spec.logical_role == "reviewed_contract_registry"],
                ["artifacts/runtime/reviewed_contract_registry.json"],
            )
            self.assertEqual(kwargs["telemetry_path"], runtime / "telemetry.sqlite3")


if __name__ == "__main__":
    unittest.main()
