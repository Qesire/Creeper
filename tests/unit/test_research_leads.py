from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.deterministic_search import (
    DeterministicSearchPolicy,
    classify_result,
)
from creeper.source_discovery.research_leads import (
    CURATED_RESEARCH_LEADS,
    ResearchLeadKind,
    ResearchLeadLedger,
)
from creeper.source_discovery.residual_search import (
    ResidualSearchLedger,
    SearchCell,
    SearchCellScheduler,
)
from creeper.source_discovery.search_identity import (
    RawSearchResult,
    canonicalize_search_result,
)
from creeper.storage.control_store import ControlStore


class ResearchLeadLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.residual = ResidualSearchLedger(self.control.connection)
        self.ledger = ResearchLeadLedger(self.control.connection)

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def test_curated_seed_is_durable_idempotent_and_bounded(self) -> None:
        self.assertEqual(self.ledger.seed_curated(self.residual), 8)
        self.assertEqual(self.ledger.seed_curated(self.residual), 0)

        rows = self.ledger.rows()
        self.assertEqual(len(rows), 8)
        kinds = [str(row["kind"]) for row in rows]
        self.assertEqual(kinds.count(ResearchLeadKind.HARD_NEGATIVE.value), 2)
        self.assertEqual(kinds.count(ResearchLeadKind.EXACT_RECOVERY.value), 5)
        self.assertEqual(kinds.count(ResearchLeadKind.PROVENANCE_HOLD.value), 1)
        self.assertEqual(len(self.ledger.recovery_cell_keys()), 5)

    def test_recovery_cells_are_prioritized_over_generic_unseen_cells(self) -> None:
        self.ledger.seed_curated(self.residual)
        generic = SearchCell(
            mechanism="proxy_access",
            institution="university",
            period="1998",
            artifact="trace",
        )
        self.residual.ensure_cell(generic)
        scheduler = SearchCellScheduler(
            self.residual,
            priority_cell_keys=self.ledger.recovery_cell_keys(),
        )

        plan = scheduler.next_plans(limit=1)[0]

        self.assertIn(plan.cell.key, self.ledger.recovery_cell_keys())
        self.assertTrue(plan.cell.mechanism.startswith("recover_"))

    def test_matchers_separate_hard_negative_and_provenance_hold(self) -> None:
        self.ledger.seed_curated(self.residual)
        ucb = canonicalize_search_result(
            RawSearchResult(
                provider="fixture",
                provider_result_id="ucb",
                url="https://repo.example/ucb-home-ip-trace.dat",
                title="UCB Home IP trace 1996",
                description="Berkeley public client trace",
            ),
            relevance_score=1.0,
            qualified=True,
        )
        nus = canonicalize_search_result(
            RawSearchResult(
                provider="fixture",
                provider_result_id="nus",
                url="https://www.comp.nus.edu.sg/research/nlanr-sample.zip",
                title="NUS NLANR teaching sample",
            ),
            relevance_score=1.0,
            qualified=True,
        )

        hard = self.ledger.hard_negative_match(ucb)
        hold = self.ledger.provenance_hold_match(nus)

        self.assertIsNotNone(hard)
        self.assertEqual(hard.lead_id, "ucb-home-ip-1996-public-trace")
        self.assertIsNone(self.ledger.provenance_hold_match(ucb))
        self.assertIsNotNone(hold)
        self.assertEqual(hold.lead_id, "nus-nlanr-sample")
        self.assertIsNone(self.ledger.hard_negative_match(nus))
        forged = canonicalize_search_result(
            RawSearchResult(
                provider="fixture",
                provider_result_id="forged-nus",
                url="https://example.test/paper.pdf",
                title="NUS NLANR teaching sample",
            ),
            relevance_score=1.0,
            qualified=True,
        )
        self.assertIsNone(self.ledger.provenance_hold_match(forged))

    def test_exact_recovery_filename_in_url_is_relevance_evidence(self) -> None:
        lead = next(
            item
            for item in CURATED_RESEARCH_LEADS
            if item.lead_id == "nlanr-uc-20000714"
        )
        assert lead.recovery_cell is not None
        raw = RawSearchResult(
            provider="fixture",
            provider_result_id="nlanr",
            url=(
                "https://mirror.example/nlanr/"
                "uc.sanitized-access.20000714.gz"
            ),
            title="historical proxy data",
            resource_type="file",
        )
        result = classify_result(
            type(
                "_Plan",
                (),
                {"cell": lead.recovery_cell},
            )(),
            raw,
            policy=DeterministicSearchPolicy(min_relevance_score=0.55),
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertTrue(result.qualified)
        self.assertGreaterEqual(result.relevance_score, 0.65)


if __name__ == "__main__":
    unittest.main()
