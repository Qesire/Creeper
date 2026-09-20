from __future__ import annotations

from datetime import datetime
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from creeper.distributed.config import load_email_report_config
from creeper.distributed.email_reporter import collect_snapshot,render_report
from creeper.storage.candidate_store import CandidateStore
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class FabricEmailReporterTests(unittest.TestCase):
    def _config(self, root: Path) -> Path:
        config=root/"fabric.toml"
        config.write_text(
            f"""
[authority]
database = "fabric.sqlite3"
credentials_file = "workers.json"
host = "127.0.0.1"
port = 8088

[email_report]
runtime_data_root = "{root.as_posix()}"
smtp_host = "smtp.gmail.com"
smtp_port = 587
starttls = true
timezone = "Asia/Singapore"
""",
            encoding="utf-8",
        )
        return config

    def test_email_config_keeps_delivery_secrets_in_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config=load_email_report_config(self._config(Path(tmp)))
            self.assertEqual(config.smtp_host,"smtp.gmail.com")
            self.assertEqual(config.smtp_port,587)
            self.assertEqual(
                config.resolved_state_file,
                Path(tmp)/"reporting"/"last-email-snapshot.json",
            )
            values={
                "CREEPER_REPORT_FROM":"sender@example.test",
                "CREEPER_REPORT_TO":"recipient@example.test",
                "CREEPER_SMTP_USERNAME":"sender@example.test",
                "CREEPER_SMTP_APP_PASSWORD":"secret",
            }
            with patch.dict("os.environ",values,clear=False):
                self.assertEqual(
                    config.load_delivery_environment(),
                    (
                        "sender@example.test",
                        "recipient@example.test",
                        "sender@example.test",
                        "secret",
                    ),
                )

    def test_collect_snapshot_reads_authority_and_domain_stores(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            config=self._config(root)
            ControlStore(root/"control.sqlite3").close()
            EvidenceStore(root/"evidence.sqlite3").close()
            CandidateStore(root/"candidates.sqlite3").close()

            snapshot=collect_snapshot(config)

            self.assertEqual(snapshot["fabric"]["workers"],0)
            self.assertEqual(snapshot["fabric"]["dead"],0)
            self.assertEqual(snapshot["evidence"]["host_years"],0)
            self.assertEqual(snapshot["evidence"]["capsules"],0)
            self.assertEqual(snapshot["candidates"]["records"],0)
            self.assertIn("disk_free_bytes",snapshot["host"])

    def test_render_report_includes_delta_since_previous_summary(self) -> None:
        current={
            "fabric":{"complete":8,"dead":0},
            "control":{},
            "evidence":{"host_years":12},
            "candidates":{"records":20},
            "host":{"disk_free_bytes":100},
        }
        previous={
            "fabric":{"complete":5,"dead":0},
            "control":{},
            "evidence":{"host_years":10},
            "candidates":{"records":18},
            "host":{"disk_free_bytes":120},
        }
        report=render_report(
            current,
            generated_at=datetime(2026,9,20,20,0,tzinfo=ZoneInfo("Asia/Singapore")),
            previous=previous,
        )
        self.assertIn('"fabric.complete": 3.0',report)
        self.assertIn('"evidence.host_years": 2.0',report)
        self.assertIn('"host.disk_free_bytes": -20.0',report)


if __name__=="__main__":
    unittest.main()
