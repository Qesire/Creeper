"""Deterministic structured-repository root adapters."""

from .base import (
    ArtifactLead,
    RootCapabilityReport,
    RootQuery,
    SearchCheckpoint,
    SearchHit,
    SearchPage,
)
from .datacite import DataCiteAdapter
from .dataverse import DataverseAdapter
from .zenodo import ZenodoAdapter

__all__ = [
    "ArtifactLead",
    "DataCiteAdapter",
    "DataverseAdapter",
    "RootCapabilityReport",
    "RootQuery",
    "SearchCheckpoint",
    "SearchHit",
    "SearchPage",
    "ZenodoAdapter",
]
