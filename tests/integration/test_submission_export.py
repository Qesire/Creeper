import unittest
from pathlib import Path
from types import SimpleNamespace

from creeper.submission.export import EXPORTER_STATUS, export_submission


class SubmissionExportTests(unittest.TestCase):
    def test_legacy_exporter_is_explicitly_non_formal_and_rejected(self):
        self.assertEqual(EXPORTER_STATUS, "INTERNAL/LEGACY/NON-FORMAL")
        with self.assertRaisesRegex(RuntimeError, "build_submission_zip"):
            export_submission([], SimpleNamespace(path=Path("unused")), Path("unused"))


if __name__ == "__main__":
    unittest.main()
