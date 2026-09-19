"""Open the configured Fabric authority backend."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from creeper.distributed.authority_store import DistributedAuthorityStore


def open_authority_store(database: str, **kwargs: Any):
    value=database.strip()
    if value.startswith(("postgresql://","postgres://")):
        from creeper.distributed.postgres_store import PostgresAuthorityStore
        return PostgresAuthorityStore(value,**kwargs)
    if value.startswith("sqlite:///"):
        return DistributedAuthorityStore(Path(value[len("sqlite:///"):]),**kwargs)
    if "://" in value:
        raise ValueError(f"unsupported Fabric database URL: {value}")
    return DistributedAuthorityStore(Path(value).expanduser(),**kwargs)
