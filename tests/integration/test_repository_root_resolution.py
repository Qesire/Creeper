import unittest

from creeper.source_research.adapters.base import ArtifactLead
from creeper.source_research.adapters.dataverse import DataverseAdapter


class RepositoryRootResolutionTests(unittest.TestCase):
    def test_artifact_lead_is_not_annual_evidence(self):
        lead = ArtifactLead("dv", "file:7", "https://dv/api/access/datafile/7", "application/gzip", 10, "md5:x", "doi:10/x", "doi:10/d")
        self.assertIsNone(getattr(lead, "evidence_year", None))
        self.assertEqual(lead.kind, "ARTIFACT_LEAD")

if __name__ == "__main__":
    unittest.main()
