"""Durable Factory/Index/Region/Synopsis state for bulk historical sources."""

from __future__ import annotations

import json
import time
from collections.abc import Iterable

from creeper.source_discovery.index_space import (
    CompiledIndexSpace,
    HarvestRegion,
    RegionKind,
    RegionSynopsis,
)
from creeper.storage.control_store import ControlStore


class IndexSpaceRegistry:
    """Persist recomputable index-space planning state beside ControlStore.

    Source discovery authority remains in SourceDiscoveryRegistry.  This store
    only records how an admitted source is partitioned and what bounded probes
    measured.  Keeping the tables separate avoids coupling source-state
    transitions to later region-level optimization.
    """

    def __init__(self, control_store: ControlStore, *, clock=time.time) -> None:
        self.control_store = control_store
        self.connection = control_store.connection
        self.clock = clock
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS source_factories_v1 (
                factory_key TEXT PRIMARY KEY,
                canonical_root TEXT NOT NULL,
                source_family TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS source_indexes_v1 (
                index_key TEXT PRIMARY KEY,
                factory_key TEXT NOT NULL,
                source_key TEXT NOT NULL UNIQUE,
                locator TEXT NOT NULL,
                access_mode TEXT NOT NULL,
                source_format TEXT NOT NULL,
                hierarchical INTEGER NOT NULL,
                range_supported INTEGER NOT NULL,
                timestamp_bearing INTEGER NOT NULL,
                direct_evidence_authority INTEGER NOT NULL,
                sorted_keyspace TEXT,
                supports_query INTEGER NOT NULL,
                supports_prefix INTEGER NOT NULL,
                supports_domain INTEGER NOT NULL,
                supports_date_filter INTEGER NOT NULL,
                expected_year_from INTEGER,
                expected_year_to INTEGER,
                expected_volume INTEGER,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY(factory_key)
                    REFERENCES source_factories_v1(factory_key)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_source_indexes_factory_v1
                ON source_indexes_v1(factory_key, index_key);

            CREATE TABLE IF NOT EXISTS source_regions_v1 (
                region_key TEXT PRIMARY KEY,
                index_key TEXT NOT NULL,
                kind TEXT NOT NULL,
                locator TEXT NOT NULL,
                parent_region_key TEXT,
                depth INTEGER NOT NULL,
                byte_start INTEGER,
                byte_end INTEGER,
                key_prefix TEXT,
                year_from INTEGER,
                year_to INTEGER,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY(index_key)
                    REFERENCES source_indexes_v1(index_key),
                FOREIGN KEY(parent_region_key)
                    REFERENCES source_regions_v1(region_key)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_source_regions_index_v1
                ON source_regions_v1(index_key, depth, region_key);

            CREATE TABLE IF NOT EXISTS source_region_synopses_v1 (
                region_key TEXT PRIMARY KEY,
                sampled_records INTEGER NOT NULL,
                unique_hosts INTEGER NOT NULL,
                novel_hosts INTEGER NOT NULL,
                observed_host_year_pairs INTEGER NOT NULL,
                novel_host_year_pairs INTEGER NOT NULL,
                novel_eed REAL NOT NULL,
                bytes_read INTEGER NOT NULL,
                requests INTEGER NOT NULL,
                measurement_mode TEXT NOT NULL,
                observed_year_histogram_json TEXT NOT NULL,
                novel_year_histogram_json TEXT NOT NULL,
                tld_host_year_histogram_json TEXT NOT NULL,
                minhash_json TEXT NOT NULL,
                confidence REAL NOT NULL,
                complete INTEGER NOT NULL,
                observed_at REAL NOT NULL,
                FOREIGN KEY(region_key)
                    REFERENCES source_regions_v1(region_key)
            ) WITHOUT ROWID;
            """
        )
        self.connection.commit()

    def register_index_space(self, compiled: CompiledIndexSpace) -> None:
        now = float(self.clock())
        factory = compiled.factory
        index = compiled.index
        capabilities = index.capabilities
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_factories_v1(
                    factory_key, canonical_root, source_family,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(factory_key) DO UPDATE SET
                    canonical_root = excluded.canonical_root,
                    source_family = excluded.source_family,
                    updated_at = excluded.updated_at
                """,
                (
                    factory.factory_key,
                    factory.canonical_root,
                    factory.source_family,
                    now,
                    now,
                ),
            )
            self.connection.execute(
                """
                INSERT INTO source_indexes_v1(
                    index_key, factory_key, source_key, locator,
                    access_mode, source_format, hierarchical, range_supported,
                    timestamp_bearing, direct_evidence_authority,
                    sorted_keyspace, supports_query, supports_prefix,
                    supports_domain, supports_date_filter,
                    expected_year_from, expected_year_to, expected_volume,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(index_key) DO UPDATE SET
                    factory_key = excluded.factory_key,
                    source_key = excluded.source_key,
                    locator = excluded.locator,
                    access_mode = excluded.access_mode,
                    source_format = excluded.source_format,
                    hierarchical = excluded.hierarchical,
                    range_supported = excluded.range_supported,
                    timestamp_bearing = excluded.timestamp_bearing,
                    direct_evidence_authority = excluded.direct_evidence_authority,
                    sorted_keyspace = excluded.sorted_keyspace,
                    supports_query = excluded.supports_query,
                    supports_prefix = excluded.supports_prefix,
                    supports_domain = excluded.supports_domain,
                    supports_date_filter = excluded.supports_date_filter,
                    expected_year_from = excluded.expected_year_from,
                    expected_year_to = excluded.expected_year_to,
                    expected_volume = excluded.expected_volume,
                    updated_at = excluded.updated_at
                """,
                (
                    index.index_key,
                    index.factory_key,
                    index.source_key,
                    index.locator,
                    capabilities.access_mode.value,
                    capabilities.format,
                    int(capabilities.hierarchical),
                    int(capabilities.range_supported),
                    int(capabilities.timestamp_bearing),
                    int(capabilities.direct_evidence_authority),
                    capabilities.sorted_keyspace,
                    int(capabilities.supports_query),
                    int(capabilities.supports_prefix),
                    int(capabilities.supports_domain),
                    int(capabilities.supports_date_filter),
                    index.expected_year_from,
                    index.expected_year_to,
                    index.expected_volume,
                    now,
                    now,
                ),
            )
            self._put_region(compiled.root_region, now=now)

    def _put_region(self, region: HarvestRegion, *, now: float) -> None:
        self.connection.execute(
            """
            INSERT INTO source_regions_v1(
                region_key, index_key, kind, locator, parent_region_key,
                depth, byte_start, byte_end, key_prefix, year_from, year_to,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(region_key) DO UPDATE SET
                index_key = excluded.index_key,
                kind = excluded.kind,
                locator = excluded.locator,
                parent_region_key = excluded.parent_region_key,
                depth = excluded.depth,
                byte_start = excluded.byte_start,
                byte_end = excluded.byte_end,
                key_prefix = excluded.key_prefix,
                year_from = excluded.year_from,
                year_to = excluded.year_to,
                updated_at = excluded.updated_at
            """,
            (
                region.region_key,
                region.index_key,
                region.kind.value,
                region.locator,
                region.parent_region_key,
                region.depth,
                region.byte_start,
                region.byte_end,
                region.key_prefix,
                region.year_from,
                region.year_to,
                now,
                now,
            ),
        )

    def put_region(self, region: HarvestRegion) -> None:
        now = float(self.clock())
        with self.connection:
            exists = self.connection.execute(
                "SELECT 1 FROM source_indexes_v1 WHERE index_key = ?",
                (region.index_key,),
            ).fetchone()
            if exists is None:
                raise KeyError(f"unknown source index: {region.index_key}")
            self._put_region(region, now=now)

    def get_region(self, region_key: str) -> HarvestRegion | None:
        row = self.connection.execute(
            "SELECT * FROM source_regions_v1 WHERE region_key = ?",
            (region_key,),
        ).fetchone()
        if row is None:
            return None
        return HarvestRegion(
            region_key=str(row["region_key"]),
            index_key=str(row["index_key"]),
            kind=RegionKind(str(row["kind"])),
            locator=str(row["locator"]),
            parent_region_key=row["parent_region_key"],
            depth=int(row["depth"]),
            byte_start=row["byte_start"],
            byte_end=row["byte_end"],
            key_prefix=row["key_prefix"],
            year_from=row["year_from"],
            year_to=row["year_to"],
        )

    def list_regions(self, index_key: str) -> tuple[HarvestRegion, ...]:
        rows = self.connection.execute(
            """
            SELECT region_key
            FROM source_regions_v1
            WHERE index_key = ?
            ORDER BY depth, region_key
            """,
            (index_key,),
        ).fetchall()
        return tuple(
            region
            for row in rows
            if (region := self.get_region(str(row["region_key"]))) is not None
        )

    @staticmethod
    def _encode_histogram(values: Iterable[tuple[object, int]]) -> str:
        return json.dumps(
            [[key, int(count)] for key, count in values],
            separators=(",", ":"),
            ensure_ascii=False,
        )

    def record_synopsis(self, synopsis: RegionSynopsis) -> None:
        if self.get_region(synopsis.region_key) is None:
            raise KeyError(f"unknown source region: {synopsis.region_key}")
        now = float(self.clock())
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_region_synopses_v1(
                    region_key, sampled_records, unique_hosts, novel_hosts,
                    observed_host_year_pairs, novel_host_year_pairs,
                    novel_eed, bytes_read, requests, measurement_mode,
                    observed_year_histogram_json,
                    novel_year_histogram_json,
                    tld_host_year_histogram_json,
                    minhash_json, confidence, complete, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(region_key) DO UPDATE SET
                    sampled_records = excluded.sampled_records,
                    unique_hosts = excluded.unique_hosts,
                    novel_hosts = excluded.novel_hosts,
                    observed_host_year_pairs = excluded.observed_host_year_pairs,
                    novel_host_year_pairs = excluded.novel_host_year_pairs,
                    novel_eed = excluded.novel_eed,
                    bytes_read = excluded.bytes_read,
                    requests = excluded.requests,
                    measurement_mode = excluded.measurement_mode,
                    observed_year_histogram_json =
                        excluded.observed_year_histogram_json,
                    novel_year_histogram_json =
                        excluded.novel_year_histogram_json,
                    tld_host_year_histogram_json =
                        excluded.tld_host_year_histogram_json,
                    minhash_json = excluded.minhash_json,
                    confidence = excluded.confidence,
                    complete = excluded.complete,
                    observed_at = excluded.observed_at
                """,
                (
                    synopsis.region_key,
                    synopsis.sampled_records,
                    synopsis.unique_hosts,
                    synopsis.novel_hosts,
                    synopsis.observed_host_year_pairs,
                    synopsis.novel_host_year_pairs,
                    synopsis.novel_eed,
                    synopsis.bytes_read,
                    synopsis.requests,
                    synopsis.measurement_mode.value,
                    self._encode_histogram(synopsis.observed_year_histogram),
                    self._encode_histogram(synopsis.novel_year_histogram),
                    self._encode_histogram(synopsis.tld_host_year_histogram),
                    json.dumps(
                        list(synopsis.minhash_values),
                        separators=(",", ":"),
                    ),
                    synopsis.confidence,
                    int(synopsis.complete),
                    now,
                ),
            )

    def get_synopsis(self, region_key: str) -> RegionSynopsis | None:
        row = self.connection.execute(
            "SELECT * FROM source_region_synopses_v1 WHERE region_key = ?",
            (region_key,),
        ).fetchone()
        if row is None:
            return None

        def hist(name: str, *, integer_key: bool) -> tuple[tuple, ...]:
            values = json.loads(str(row[name]))
            if integer_key:
                return tuple((int(key), int(count)) for key, count in values)
            return tuple((str(key), int(count)) for key, count in values)

        return RegionSynopsis(
            region_key=str(row["region_key"]),
            sampled_records=int(row["sampled_records"]),
            unique_hosts=int(row["unique_hosts"]),
            novel_hosts=int(row["novel_hosts"]),
            observed_host_year_pairs=int(row["observed_host_year_pairs"]),
            novel_host_year_pairs=int(row["novel_host_year_pairs"]),
            novel_eed=float(row["novel_eed"]),
            bytes_read=int(row["bytes_read"]),
            requests=int(row["requests"]),
            measurement_mode=str(row["measurement_mode"]),
            observed_year_histogram=hist(
                "observed_year_histogram_json",
                integer_key=True,
            ),
            novel_year_histogram=hist(
                "novel_year_histogram_json",
                integer_key=True,
            ),
            tld_host_year_histogram=hist(
                "tld_host_year_histogram_json",
                integer_key=False,
            ),
            minhash_values=tuple(
                int(value) for value in json.loads(str(row["minhash_json"]))
            ),
            confidence=float(row["confidence"]),
            complete=bool(row["complete"]),
        )
