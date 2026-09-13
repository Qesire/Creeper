from __future__ import annotations

import unittest

from creeper.source_research.adapters.base import ArtifactLead, SearchHit
from creeper.source_research.adapters.datacite import DataCiteAdapter
from creeper.source_research.adapters.dataverse import DataverseAdapter


class EmptyTransport:
    async def __call__(self, url, params=None, headers=None):
        raise AssertionError("resolution must not scrape a landing page")


class RepositoryRootResolutionTests(unittest.IsolatedAsyncioTestCase):
    def test_artifact_lead_is_never_annual_evidence(self):
        lead = ArtifactLead(
            "dv",
            "file:7",
            "https://dv.example/api/access/datafile/7",
            "application/gzip",
            10,
            "md5:x",
            "doi:10/x",
            "doi:10/d",
        )
        self.assertIsNone(lead.evidence_year)
        self.assertEqual(lead.kind, "ARTIFACT_LEAD")

    async def test_datacite_content_url_resolves_without_landing_scrape(self):
        hit = SearchHit(
            root_id="datacite",
            query_id="q",
            provider_native_id="10.1/x",
            provider_url="https://doi.org/10.1/x",
            provider_type="DOI",
            metadata={"content_urls": ("https://objects.example/a.cdx.gz",)},
        )
        leads = await DataCiteAdapter(transport=EmptyTransport()).resolve(hit)
        self.assertEqual([lead.locator for lead in leads], ["https://objects.example/a.cdx.gz"])

    async def test_dataverse_dataset_resolution_requires_file_search_not_landing_scrape(self):
        hit = SearchHit(
            root_id="dataverse:dv.example",
            query_id="q",
            provider_native_id="doi:10/d",
            provider_url="https://dv.example/dataset.xhtml?persistentId=doi:10/d",
            provider_type="DATASET",
            metadata={"publication_date": "1999-01-01"},
        )
        leads = await DataverseAdapter("https://dv.example", transport=EmptyTransport()).resolve(hit)
        self.assertEqual(leads, ())


if __name__ == "__main__":
    unittest.main()
