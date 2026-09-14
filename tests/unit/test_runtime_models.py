import unittest
from dataclasses import FrozenInstanceError

from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord, SourceStats
from creeper.scheduler.leases import LeaseState, StateTransitionError
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirEstimate, ReservoirState


class RuntimeModelTests(unittest.TestCase):
    def test_source_domain_is_immutable_and_has_guarded_transitions(self):
        domain = SourceDomain(
            domain_id="archive",
            family="NATIONAL_WEB_ARCHIVE",
            discovery_mechanism="catalog",
            temporal_scope=(1996, 2001),
        )

        with self.assertRaises(FrozenInstanceError):
            domain.state = DomainState.EXPLORING

        exploring = domain.transition(DomainState.EXPLORING)
        productive = exploring.transition(DomainState.PRODUCTIVE)
        self.assertEqual(productive.state, DomainState.PRODUCTIVE)
        with self.assertRaises(StateTransitionError):
            domain.transition(DomainState.PRODUCTIVE)

    def test_reservoir_rejects_lossy_capacities_and_bad_identity(self):
        with self.assertRaisesRegex(ValueError, "capacity_lower"):
            ReservoirEstimate(capacity_lower=1.5)
        with self.assertRaisesRegex(ValueError, "capacity_lower"):
            ReservoirEstimate(capacity_lower=True)
        with self.assertRaisesRegex(ValueError, "reservoir_id"):
            Reservoir(
                reservoir_id=1,
                domain_id="archive",
                adapter_id="demo",
                root_locator="fixture://demo",
                enumeration_kind="finite_list",
                capacity_lower=1,
            )

    def test_source_record_models_reject_lossy_years_masks_and_stats(self):
        scope = CandidateSourceScope.LOCAL_DISCOVERY
        with self.assertRaisesRegex(ValueError, "source_year"):
            SourceRecord("source", "line:1", "payload", scope, 1998.5)
        with self.assertRaisesRegex(ValueError, "direct_year_mask"):
            HostObservation(
                "example.com",
                "source",
                "line:1",
                scope,
                direct_year_mask=True,
            )
        with self.assertRaisesRegex(ValueError, "elapsed_seconds"):
            SourceStats("source", 1, 1, 1, float("nan"))

    def test_reservoir_estimate_and_running_guard(self):
        estimate = ReservoirEstimate(
            capacity_lower=100,
            capacity_upper=200,
            sampled_records=32,
        )
        self.assertEqual(estimate.capacity_lower, 100)
        reservoir = Reservoir(
            reservoir_id="archive:demo",
            domain_id="archive",
            adapter_id="demo",
            root_locator="https://example.test/catalog",
            enumeration_kind="pagination",
            capacity_lower=estimate.capacity_lower,
            capacity_upper=estimate.capacity_upper,
            evidence_mode="direct_year",
        )
        with self.assertRaises(StateTransitionError):
            reservoir.transition(ReservoirState.RUNNING)

    def test_legacy_source_record_and_observation_constructors_keep_defaults(self):
        scope = CandidateSourceScope.LOCAL_DISCOVERY
        record = SourceRecord("source", "line:1", "payload", scope, 1998)
        observation = HostObservation("example.com", "source", "line:1", scope, 1998)

        self.assertEqual(record.source_year, 1998)
        self.assertEqual(record.record_type, "")
        self.assertEqual(record.direct_year_mask, 0)
        self.assertEqual(observation.source_year, 1998)
        self.assertEqual(observation.artifact_ref, "")
        self.assertEqual(observation.year_hint_mask, 0)


if __name__ == "__main__":
    unittest.main()
