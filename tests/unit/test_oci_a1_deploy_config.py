from __future__ import annotations

import hashlib
import json
import tempfile
import tomllib
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import authority_digest
from creeper.autopilot import load_autopilot_config
from creeper.distributed.config import (
    load_authority_config,
    load_email_report_config,
    load_evidence_bridge_config,
    load_worker_config,
)
from creeper.source_discovery_service import load_source_discovery_config


class OciA1DeploymentConfigTests(unittest.TestCase):
    @property
    def repo_root(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @property
    def root(self) -> Path:
        return self.repo_root/"deploy"/"oci-a1"

    def _materialized_profile(self, root: Path) -> Path:
        etc=root/"etc"
        srv=root/"srv"
        etc.mkdir()
        (srv/"data"/"indexes").mkdir(parents=True)
        (srv/"reference").mkdir(parents=True)
        baseline=srv/"baseline"/"test-baseline"
        baseline.mkdir(parents=True)

        for year in range(1996,2002):
            (baseline/f"{year}.txt").write_text(
                f"year{year}.example\n",
                encoding="utf-8",
            )
        (baseline/"candidate_pool.txt").write_text(
            "candidate.example\n",
            encoding="utf-8",
        )
        model=srv/"reference"/"equivalent_english_domain.json"
        model.write_text("{}\n",encoding="utf-8")
        annual={
            f"{year}.txt":hashlib.sha256(
                (baseline/f"{year}.txt").read_bytes()
            ).hexdigest()
            for year in range(1996,2002)
        }
        candidate_hash=hashlib.sha256(
            (baseline/"candidate_pool.txt").read_bytes()
        ).hexdigest()
        model_hash=hashlib.sha256(model.read_bytes()).hexdigest()
        baseline_eed="0"
        manifest={
            "baseline_id":baseline.name,
            "annual_file_hashes":annual,
            "candidate_file_hash":candidate_hash,
            "model_hash":model_hash,
            "baseline_eed":baseline_eed,
            "authority_digest":authority_digest(
                baseline_id=baseline.name,
                annual_file_hashes=annual,
                candidate_file_hash=candidate_hash,
                model_hash=model_hash,
                baseline_eed=baseline_eed,
            ),
        }
        manifest_path=baseline/"authority-manifest.json"
        manifest_path.write_text(
            json.dumps(manifest)+"\n",
            encoding="utf-8",
        )
        (srv/"baseline"/"current").symlink_to("test-baseline")
        BaselineIndex.build(
            baseline_dir=baseline,
            output_path=srv/"data"/"indexes"/"baseline-fast.sqlite3",
            authority_manifest=manifest_path,
        ).close()

        replacements={
            "/srv/creeper":str(srv),
            "/etc/creeper":str(etc),
            "/opt/creeper":str(self.repo_root),
        }
        for source in self.root.glob("*.toml"):
            text=source.read_text(encoding="utf-8")
            for old,new in replacements.items():
                text=text.replace(old,new)
            (etc/source.name).write_text(text,encoding="utf-8")
        return etc

    def test_fabric_profile_is_loopback_postgres_and_email_only(self) -> None:
        path=self.root/"fabric.toml"
        authority=load_authority_config(path)
        bridge=load_evidence_bridge_config(path)
        report=load_email_report_config(path)

        self.assertEqual(authority.database,"postgresql:///creeper")
        self.assertEqual(authority.host,"127.0.0.1")
        self.assertEqual(authority.port,8088)
        self.assertFalse(authority.outbox_enabled)
        self.assertTrue(authority.provider_budgets)
        self.assertTrue(
            all(not item.require_qualified_region for item in authority.provider_budgets)
        )
        self.assertEqual(bridge.runtime_data_root,Path("/srv/creeper/data"))
        self.assertEqual(report.timezone,"Asia/Singapore")
        self.assertTrue(report.starttls)

        raw=tomllib.loads(
            (self.root/"source-discovery.toml").read_text(encoding="utf-8")
        )
        self.assertTrue(raw["fabric"]["enabled"])
        self.assertFalse(raw["fabric"]["outbox_enabled"])
        self.assertEqual(raw["pool"]["max_search_directives"],0)
        self.assertEqual(
            raw["measurement"]["authority_manifest"],
            "/srv/creeper/baseline/current/authority-manifest.json",
        )

    def test_materialized_profile_loaders_bind_same_server_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            etc=self._materialized_profile(Path(tmp))
            discovery=load_source_discovery_config(
                etc/"source-discovery.toml"
            )
            autopilot=load_autopilot_config(etc/"autopilot.toml")

            self.assertIsNotNone(discovery.measurement)
            assert discovery.measurement is not None
            self.assertEqual(
                discovery.measurement.authority_manifest,
                Path(tmp)/"srv"/"baseline"/"current"/"authority-manifest.json",
            )
            self.assertIsNotNone(autopilot.readiness)
            assert autopilot.readiness is not None
            self.assertEqual(autopilot.readiness.baseline_eed,"0")
            self.assertEqual(
                autopilot.readiness.authority_manifest,
                Path(tmp)/"srv"/"baseline"/"current"/"authority-manifest.json",
            )

    def test_workers_are_colocated_and_have_disjoint_execution_roles(self) -> None:
        query=load_worker_config(self.root/"worker-query.toml")
        evidence=load_worker_config(self.root/"worker-evidence.toml")

        self.assertEqual(query.coordinator_url,"http://127.0.0.1:8088")
        self.assertEqual(evidence.coordinator_url,"http://127.0.0.1:8088")
        self.assertEqual(query.descriptor.architecture,"aarch64")
        self.assertEqual(evidence.descriptor.architecture,"aarch64")
        self.assertEqual(query.descriptor.producers,("ResidualQueryProducer",))
        self.assertEqual(evidence.descriptor.producers,("EvidenceQueryProducer",))
        self.assertNotEqual(query.descriptor.worker_id,evidence.descriptor.worker_id)

    def test_autopilot_resource_envelope_precedes_systemd_ceiling(self) -> None:
        raw=tomllib.loads(
            (self.root/"autopilot.toml").read_text(encoding="utf-8")
        )
        self.assertFalse(raw["evidence"]["enabled"])
        self.assertFalse(raw["evidence"]["platform_harvest_enabled"])
        self.assertEqual(raw["resource_governor"]["rss_throttle_gib"],5)
        self.assertEqual(raw["resource_governor"]["rss_stop_gib"],6)
        self.assertNotIn("baseline_eed",raw["readiness"])
        self.assertEqual(
            raw["readiness"]["authority_manifest"],
            "/srv/creeper/baseline/current/authority-manifest.json",
        )

        unit=(self.root/"systemd"/"creeper-autopilot.service").read_text(
            encoding="utf-8"
        )
        self.assertIn("MemoryHigh=5G",unit)
        self.assertIn("MemoryMax=7G",unit)
        for name in (
            "creeper-fabric-authority.service",
            "creeper-fabric-worker@.service",
            "creeper-fabric-evidence-bridge.service",
        ):
            with self.subTest(unit=name):
                text=(self.root/"systemd"/name).read_text(encoding="utf-8")
                self.assertIn("MemoryMax=",text)
                self.assertIn("OOMPolicy=stop",text)

    def test_server_baseline_bootstrap_is_required_before_production(self) -> None:
        bootstrap=(self.root/"bootstrap-baseline.sh").read_text(encoding="utf-8")
        verify=(self.root/"verify-baseline.sh").read_text(encoding="utf-8")
        install=(self.root/"install.sh").read_text(encoding="utf-8")
        start=(self.root/"start-production.sh").read_text(encoding="utf-8")

        self.assertIn("/srv/creeper/baseline",bootstrap)
        self.assertIn("authority-manifest.json",bootstrap)
        self.assertIn("scripts/build_baseline.py",bootstrap)
        self.assertIn("baseline-fast.sqlite3",bootstrap)
        self.assertIn("AuthoritySnapshot.from_manifest_path",verify)
        self.assertIn("authority.verify_baseline_dir",verify)
        self.assertIn("authority.verify_model",verify)
        self.assertIn("BaselineIndex(index_path,authority=authority)",verify)
        self.assertIn("verify-baseline.sh",install)
        self.assertIn("verify-baseline.sh",start)

    def test_transient_gc_is_scheduled_and_alerted(self) -> None:
        service=(self.root/"systemd"/"creeper-fabric-gc.service").read_text(
            encoding="utf-8"
        )
        timer=(self.root/"systemd"/"creeper-fabric-gc.timer").read_text(
            encoding="utf-8"
        )
        install=(self.root/"install.sh").read_text(encoding="utf-8")
        self.assertIn("gc --retention-hours 24 --limit 50000",service)
        self.assertIn("OnFailure=creeper-email-alert@%n.service",service)
        self.assertIn("OnUnitActiveSec=1h",timer)
        self.assertIn("Persistent=true",timer)
        self.assertIn("enable --now creeper-fabric-gc.timer",install)

    def test_all_deployment_toml_files_parse(self) -> None:
        for path in self.root.glob("*.toml"):
            with self.subTest(path=path.name):
                with path.open("rb") as source:
                    value=tomllib.load(source)
                self.assertIsInstance(value,dict)


if __name__=="__main__":
    unittest.main()
