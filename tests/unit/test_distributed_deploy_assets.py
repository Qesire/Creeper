from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


class DistributedDeployAssetTests(unittest.TestCase):
    def test_shell_deployment_assets_parse(self) -> None:
        root = Path(__file__).resolve().parents[2]
        scripts = (
            root / "deploy/fabric/local-authority/install.sh",
            root / "deploy/fabric/local-authority/install-cloudflared.sh",
            root / "deploy/fabric/local-authority/provision-worker-secret.sh",
            root / "deploy/fabric/local-authority/smoke.sh",
            root / "deploy/fabric/vm-worker/install.sh",
            root / "deploy/fabric/vm-worker/smoke.sh",
            root / "deploy/fabric/oci/install.sh",
            root / "deploy/fabric/gcp/install.sh",
            root / "deploy/fabric/cloudflare-worker/deploy.sh",
        )
        for script in scripts:
            self.assertTrue(script.is_file(), script)
            completed = subprocess.run(
                ["bash", "-n", str(script)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                completed.returncode,
                0,
                f"{script}: {completed.stderr}",
            )

    def test_cloud_provider_region_bootstrap_is_stable(self) -> None:
        root = Path(__file__).resolve().parents[2]
        oci = (root / "deploy/fabric/oci/install.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("Authorization: Bearer Oracle", oci)
        self.assertIn("/opc/v2/instance/canonicalRegionName", oci)
        self.assertIn('FABRIC_REGION="oci-${OCI_REGION}"', oci)

        gcp = (root / "deploy/fabric/gcp/install.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("metadata.google.internal", gcp)
        self.assertIn('REGION_NAME="${ZONE_NAME%-*}"', gcp)
        self.assertIn('FABRIC_REGION="gcp-${REGION_NAME}"', gcp)

    def test_cloudflare_declares_required_hmac_secret(self) -> None:
        root = Path(__file__).resolve().parents[2]
        config = (
            root / "deploy/fabric/cloudflare-worker/wrangler.jsonc.example"
        ).read_text(encoding="utf-8")
        self.assertIn('"required": ["CREEPER_WORKER_SECRET"]', config)

    def test_production_worker_profiles_do_not_embed_secrets(self) -> None:
        root = Path(__file__).resolve().parents[2]
        for path in (
            root / "deploy/fabric/vm-worker/install.sh",
            root / "deploy/fabric/cloudflare-worker/wrangler.jsonc.example",
        ):
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("CREEPER_WORKER_SECRET =", text)
            self.assertNotIn('"CREEPER_WORKER_SECRET":', text)


if __name__ == "__main__":
    unittest.main()
