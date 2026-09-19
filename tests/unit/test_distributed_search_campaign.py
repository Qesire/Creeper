from __future__ import annotations

import unittest

from creeper.distributed.search_campaign import SearchCampaign


class DistributedSearchCampaignTests(unittest.TestCase):
    def campaign(self) -> SearchCampaign:
        return SearchCampaign(
            name="historical-web-v1",
            templates=(
                "{operator} {anchor} {source} {era} {topology} {language}",
                "{anchor} {topology} {source} {operator} {era} {language}",
            ),
            anchors=(
                "personal homepage",
                "company profile",
                "web directory",
            ),
            source_terms=(
                "links",
                "members",
                "archive",
            ),
            era_terms=(
                "1998",
                "1999",
                "2000",
            ),
            topology_terms=(
                "guestbook",
                "resources",
                "webring",
            ),
            language_terms=("", "english"),
            operators=("", "intitle:links", "inurl:members"),
        )

    def test_same_campaign_seed_and_slot_are_replay_stable(self) -> None:
        campaign = self.campaign()
        first = campaign.render_slice(seed=12345, slot_start=10, slot_count=8)
        second = campaign.render_slice(seed=12345, slot_start=10, slot_count=8)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 8)
        self.assertTrue(all(query.strip() for query in first))

    def test_seed_changes_exploration_direction(self) -> None:
        campaign = self.campaign()
        first = campaign.render_slice(seed=1, slot_start=0, slot_count=12)
        second = campaign.render_slice(seed=2, slot_start=0, slot_count=12)

        self.assertNotEqual(first, second)
        self.assertGreater(len(set(first) | set(second)), len(set(first)))

    def test_campaign_identity_changes_when_calibration_changes(self) -> None:
        original = self.campaign()
        changed = SearchCampaign(
            **(
                original.as_dict()
                | {
                    "anchors": list(original.anchors)
                    + ["faculty homepage"],
                }
            )
        )

        self.assertNotEqual(original.campaign_id, changed.campaign_id)

    def test_mapping_round_trip_preserves_identity(self) -> None:
        original = self.campaign()
        restored = SearchCampaign.from_mapping(original.as_dict())

        self.assertEqual(restored.campaign_id, original.campaign_id)
        self.assertEqual(
            restored.render(seed=99, slot=3),
            original.render(seed=99, slot=3),
        )


if __name__ == "__main__":
    unittest.main()
