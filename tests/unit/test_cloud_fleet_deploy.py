from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


class CloudFleetDeployTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root=Path(__file__).resolve().parents[2]
        cls.cloud=cls.root/"deploy"/"cloud-fleet"

    def test_all_cloud_fleet_shell_scripts_parse(self) -> None:
        scripts=sorted(self.cloud.glob("*.sh"))
        self.assertGreaterEqual(len(scripts),6)
        for script in scripts:
            with self.subTest(script=script.name):
                subprocess.run(
                    ["bash","-n",str(script)],
                    check=True,
                    cwd=self.root,
                )

    def test_provider_provisioners_are_dry_run_by_default(self) -> None:
        for name in (
            "provision-gcp-e2-micro.sh",
            "provision-azure-free-worker.sh",
            "provision-aws-credit-worker.sh",
            "provision-oci-free-worker.sh",
        ):
            with self.subTest(script=name):
                text=(self.cloud/name).read_text(encoding="utf-8")
                self.assertIn('APPLY="${CREEPER_APPLY:-0}"',text)
                self.assertIn('if [[ "$APPLY" == "1" ]]',text)

    def test_non_oci_public_network_apply_requires_explicit_cost_opt_in(self) -> None:
        expected={
            "provision-gcp-e2-micro.sh":"CREEPER_ALLOW_BILLABLE_IPV4",
            "provision-azure-free-worker.sh":"CREEPER_ALLOW_BILLABLE_PUBLIC_IP",
            "provision-aws-credit-worker.sh":"CREEPER_ALLOW_AWS_CREDIT_SPEND",
        }
        for name,guard in expected.items():
            with self.subTest(script=name):
                text=(self.cloud/name).read_text(encoding="utf-8")
                self.assertIn(guard,text)

    def test_wireguard_worker_is_prepare_then_enroll(self) -> None:
        prepare=(self.cloud/"prepare-worker-wireguard.sh").read_text(
            encoding="utf-8"
        )
        enroll=(self.cloud/"authority-enroll-worker.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("10.77.0.0/24",prepare)
        self.assertIn("PersistentKeepalive = 25",prepare)
        self.assertIn("not started",prepare)
        self.assertIn("add-remote-worker.sh",enroll)
        self.assertIn("wg set creeper peer",enroll)
        self.assertIn("AllowedIPs",enroll)

    def test_command_runbook_uses_current_fabric_control_cli_order(self) -> None:
        commands=(self.cloud/"COMMANDS.md").read_text(encoding="utf-8")
        self.assertIn(
            "creeper-fabric-control --config /etc/creeper/fabric.toml status",
            commands,
        )
        self.assertNotIn(
            "creeper-fabric-control status --config",
            commands,
        )
        self.assertIn(
            "tests.integration.test_fabric_runtime_smoke",
            commands,
        )

    def test_provider_matrix_does_not_claim_gcp_is_zero_cost(self) -> None:
        readme=(self.cloud/"README.md").read_text(encoding="utf-8")
        self.assertIn("external IPv4 is billed separately",readme)
        self.assertIn("Do not deploy Creeper here",readme)
        self.assertIn("10 TB/month outbound",readme)


if __name__=="__main__":
    unittest.main()
