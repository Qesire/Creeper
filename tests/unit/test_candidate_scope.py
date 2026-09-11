import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.records.candidates import (
    CandidateRecord,
    CandidateSourceScope,
    classify_candidate_source,
    reconcile_active_candidates,
)


class CandidateScopeTests(unittest.TestCase):
    def test_candidate_provenance_is_required(self):
        with self.assertRaises(ValueError):
            CandidateRecord("example.com", "", CandidateSourceScope.LOCAL_DISCOVERY)

    def test_common_crawl_is_not_an_active_source(self):
        self.assertEqual(
            classify_candidate_source("common-crawl-corpus"),
            CandidateSourceScope.COMMON_CRAWL_CORPUS_EXCLUDED,
        )
        self.assertEqual(
            classify_candidate_source("commoncrawl"),
            CandidateSourceScope.COMMON_CRAWL_CORPUS_EXCLUDED,
        )

    def test_source_name_overrides_mislabelled_common_crawl_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            annual = root / "merged260909-3"
            annual.mkdir()
            for year in range(1996, 2002):
                (annual / f"{year}.txt").write_text("", encoding="utf-8")
            (annual / "candidate_pool.txt").write_text("", encoding="utf-8")
            index = BaselineIndex.build(root, root / "index.sqlite3")
            result = reconcile_active_candidates(
                [
                    CandidateRecord(
                        "cc.example",
                        "commoncrawl-index",
                        CandidateSourceScope.LOCAL_DISCOVERY,
                    )
                ],
                index,
            )
            self.assertEqual(result.active, ())
            self.assertEqual([item.hostname for item in result.excluded], ["cc.example"])
            index.close()

    def test_network_wizards_is_reference_only(self):
        self.assertEqual(
            classify_candidate_source("network_wizards:1997"),
            CandidateSourceScope.ISC_REFERENCE,
        )

    def test_reconciliation_separates_isc_and_removes_annual_hosts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            annual = root / "merged260909-3"
            annual.mkdir()
            for year in range(1996, 2002):
                (annual / f"{year}.txt").write_text(
                    "annual.example\n" if year == 1996 else "", encoding="utf-8"
                )
            (annual / "candidate_pool.txt").write_text("", encoding="utf-8")
            index = BaselineIndex.build(root, root / "index.sqlite3")
            result = reconcile_active_candidates(
                [
                    CandidateRecord(
                        "new.example", "local", CandidateSourceScope.LOCAL_DISCOVERY
                    ),
                    CandidateRecord(
                        "annual.example", "official", CandidateSourceScope.OFFICIAL_POOL
                    ),
                    CandidateRecord(
                        "cc.example",
                        "common-crawl",
                        CandidateSourceScope.COMMON_CRAWL_CORPUS_EXCLUDED,
                    ),
                    CandidateRecord(
                        "isc.example", "isc-1997", CandidateSourceScope.ISC_REFERENCE
                    ),
                    CandidateRecord("not a hostname", "local", CandidateSourceScope.LOCAL_DISCOVERY),
                ],
                index,
            )
            self.assertEqual([x.hostname for x in result.active], ["new.example"])
            self.assertEqual([x.hostname for x in result.isc_reference], ["isc.example"])
            self.assertEqual([x.hostname for x in result.excluded], ["cc.example"])
            self.assertEqual(result.unparsed, ("not a hostname",))
            index.close()


if __name__ == "__main__":
    unittest.main()
