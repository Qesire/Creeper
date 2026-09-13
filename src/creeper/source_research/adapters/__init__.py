"""Structured V7.1 research-root adapters."""
from .base import ArtifactLead,RootCapabilityReport,RootQuery,SearchCheckpoint,SearchHit,SearchPage
from .datacite import DataCiteAdapter
from .zenodo import ZenodoAdapter
from .dataverse import DataverseAdapter
__all__=["ArtifactLead","RootCapabilityReport","RootQuery","SearchCheckpoint","SearchHit","SearchPage","DataCiteAdapter","ZenodoAdapter","DataverseAdapter"]
