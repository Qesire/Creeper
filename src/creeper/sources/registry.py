"""Source registry facade for candidate provenance."""

from creeper.records.candidates import (
    CandidateSourceScope,
    classify_candidate_source,
    is_active_candidate_allowed,
)

__all__ = [
    "CandidateSourceScope",
    "classify_candidate_source",
    "is_active_candidate_allowed",
]
