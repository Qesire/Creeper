import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from creeper.cli import main


class EvidenceWorkerCliTests(unittest.TestCase):
    def test_evidence_worker_once_opens_durable_stores_and_exits_idle(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                rc = main([
                    "evidence-worker",
                    "--once",
                    str(Path(tmp)),
                    "--max-retries",
                    "0",
                ])

        self.assertEqual(rc, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["claimed"], 0)
        self.assertEqual(payload["terminal"], 0)
        self.assertEqual(payload["retryable"], 0)


if __name__ == "__main__":
    unittest.main()
