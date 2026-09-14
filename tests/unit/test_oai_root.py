import unittest

from creeper.source_research.adapters.base import RootQuery, SearchCheckpoint
from creeper.source_research.adapters.oai import OAIAdapter, parse_listfriends_baseurls

OAI = "http://www.openarchives.org/OAI/2.0/"


def envelope(body):
    return f'<OAI-PMH xmlns="{OAI}">{body}</OAI-PMH>'


class Response:
    def __init__(self, status, text="", headers=None):
        self.status_code = status
        self.text = text
        self.headers = headers or {}


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def __call__(self, url, params=None, headers=None):
        self.calls.append((url, params or {}, headers or {}))
        return self.responses.pop(0)


class OAIRootTests(unittest.IsolatedAsyncioTestCase):
    def query(self):
        return RootQuery("o1", "oai", "historical web", 10, 60.0, 100)

    def test_historical_listfriends_baseurl_seed_extraction(self):
        dump = """<friends>
          <friend><baseURL>https://a.example/oai/</baseURL></friend>
          <friend><baseURL>https://b.example/oai</baseURL></friend>
          <friend><baseURL>https://a.example/oai/</baseURL></friend>
        </friends>"""
        self.assertEqual(
            parse_listfriends_baseurls(dump),
            ("https://a.example/oai", "https://b.example/oai"),
        )

    async def test_pipeline_and_resumption_token_resume(self):
        identify = envelope('<Identify><repositoryName>Repo</repositoryName><baseURL>https://oai.example</baseURL><earliestDatestamp>1999-01-01</earliestDatestamp></Identify>')
        formats = envelope('<ListMetadataFormats><metadataFormat><metadataPrefix>oai_dc</metadataPrefix><schema>schema</schema><metadataNamespace>ns</metadataNamespace></metadataFormat></ListMetadataFormats>')
        sets1 = envelope('<ListSets><set><setSpec>web</setSpec><setName>Web</setName></set><resumptionToken>SET-TOKEN</resumptionToken></ListSets>')
        sets2 = envelope('<ListSets><set><setSpec>history</setSpec><setName>History</setName></set><resumptionToken></resumptionToken></ListSets>')
        records1 = envelope('<ListRecords><record><header><identifier>oai:x:1</identifier><datestamp>2000-02-03</datestamp></header><metadata><dc xmlns="urn:dc"><title>Dataset</title><identifier>https://example.org/file.tar.gz</identifier></dc></metadata></record><resumptionToken>REC-TOKEN</resumptionToken></ListRecords>')
        records2 = envelope('<ListRecords><resumptionToken></resumptionToken></ListRecords>')
        transport = FakeTransport([Response(200, identify), Response(200, formats), Response(200, sets1), Response(200, sets2), Response(200, records1), Response(200, records2)])
        adapter = OAIAdapter("https://oai.example", transport=transport)

        page = await adapter.search(self.query(), None)
        self.assertEqual(page.next_checkpoint.query_variant, "formats")
        page = await adapter.search(self.query(), page.next_checkpoint)
        self.assertEqual(page.next_checkpoint.query_variant, "sets")
        page = await adapter.search(self.query(), page.next_checkpoint)
        self.assertEqual(page.next_checkpoint.cursor, "SET-TOKEN")
        page = await adapter.search(self.query(), page.next_checkpoint)
        self.assertEqual(transport.calls[3][1], {"verb": "ListSets", "resumptionToken": "SET-TOKEN"})
        self.assertEqual(page.next_checkpoint.query_variant, "records")
        page = await adapter.search(self.query(), page.next_checkpoint)
        self.assertEqual(page.next_checkpoint.cursor, "REC-TOKEN")
        self.assertEqual(page.hits[0].metadata["pivot_urls"], ("https://example.org/file.tar.gz",))
        self.assertFalse(page.hits[0].metadata["annual_evidence_authority"])
        self.assertEqual(page.artifact_leads, ())
        page = await adapter.search(self.query(), page.next_checkpoint)
        self.assertTrue(page.terminal)

    async def test_dead_endpoint_bad_xml_and_503_are_isolated(self):
        retry = OAIAdapter("https://retry.example", transport=FakeTransport([Response(503, headers={"Retry-After": "4"})]))
        page = await retry.search(self.query(), SearchCheckpoint(query_variant="identify"))
        self.assertFalse(page.terminal)
        self.assertEqual(page.retry_after, 4.0)

        retry_default = OAIAdapter("https://retry-default.example", transport=FakeTransport([Response(503)]))
        page = await retry_default.search(self.query(), SearchCheckpoint(query_variant="identify"))
        self.assertFalse(page.terminal)
        self.assertEqual(page.retry_after, 5.0)

        bad = OAIAdapter("https://bad.example", transport=FakeTransport([Response(200, "<broken")]))
        page = await bad.search(self.query(), None)
        self.assertTrue(page.terminal)
        self.assertTrue(bad.dead_reason.startswith("BAD_XML:"))

        good = OAIAdapter("https://good.example", transport=FakeTransport([Response(200, envelope('<Identify><repositoryName>Good</repositoryName><baseURL>https://good.example</baseURL></Identify>'))]))
        page = await good.search(self.query(), None)
        self.assertFalse(page.terminal)
        self.assertIsNone(good.dead_reason)


if __name__ == "__main__":
    unittest.main()
