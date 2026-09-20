from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


class FabricRemoteDeploymentTests(unittest.TestCase):
    @property
    def repo_root(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @property
    def root(self) -> Path:
        return self.repo_root/"deploy"/"fabric-worker"

    def test_remote_worker_is_outbound_only_and_does_not_require_local_authority(self) -> None:
        unit=(self.root/"systemd"/"creeper-fabric-remote-worker.service").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("Requires=creeper-fabric-authority.service",unit)
        self.assertIn("ExecStartPre=/opt/creeper/deploy/fabric-worker/preflight.sh",unit)
        self.assertIn("EnvironmentFile=/etc/creeper/remote-worker.env",unit)

    def test_installer_requires_explicit_identity_and_upload_budget_is_separate(self) -> None:
        install=(self.root/"install.sh").read_text(encoding="utf-8")
        self.assertIn("CREEPER_WORKER_ID",install)
        self.assertIn("CREEPER_WORKER_REGION",install)
        self.assertIn("CREEPER_WORKER_SECRET",install)
        self.assertIn("coordinator_upload_budget_bytes_per_month",install)
        self.assertIn("daily_egress_budget_bytes = 0",install)
        self.assertIn("heartbeat_seconds = 120",install)
        self.assertIn("claim_wait_seconds = 25",install)

    def test_preflight_requires_ntp_wireguard_and_private_coordinator(self) -> None:
        preflight=(self.root/"preflight.sh").read_text(encoding="utf-8")
        self.assertIn("NTPSynchronized",preflight)
        self.assertIn("/sys/class/net/creeper",preflight)
        self.assertNotIn("wg show creeper",preflight)
        self.assertIn("/healthz",preflight)
        self.assertIn("http://10.*",preflight)

    def test_shell_scripts_pass_bash_syntax_check(self) -> None:
        for path in self.root.glob("*.sh"):
            with self.subTest(path=path.name):
                subprocess.run(
                    ["bash","-n",str(path)],
                    check=True,
                    capture_output=True,
                    text=True,
                )


if __name__=="__main__":
    unittest.main()
