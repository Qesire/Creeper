"""Durable local worker outbox for result delivery."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from creeper.distributed.models import ArtifactRef, ResultBatch


class WorkerResultSpool:
    def __init__(self, path: Path, *, clock=time.time) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.connection = sqlite3.connect(self.path, timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS worker_spool_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_result_batches (
                batch_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                sequence_no INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL
            ) WITHOUT ROWID
            """
        )

    def close(self) -> None:
        self.connection.close()

    def pending_count(self) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) AS n FROM pending_result_batches"
        ).fetchone()
        return int(row["n"])

    def resolve_worker_instance_id(
        self,
        proposed_instance_id: str,
        *,
        auto: bool,
    ) -> str:
        """Resolve one process incarnation against the durable local outbox.

        If an automatically generated process restarts with unacked batches,
        reuse the prior incarnation so Authority may idempotently ACK those
        batches while the lease is still valid. If Authority has already
        fenced/re-leased the task, replay receives STALE_LEASE and the old
        generation is discarded. With an empty outbox, a new proposed
        incarnation becomes durable immediately.

        Explicit instance IDs are never rewritten.
        """

        proposed = str(proposed_instance_id).strip()
        if not proposed:
            raise ValueError("proposed_instance_id is required")
        if not auto:
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO worker_spool_meta(key,value)
                    VALUES ('worker_instance_id', ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value
                    """,
                    (proposed,),
                )
            return proposed

        row = self.connection.execute(
            "SELECT value FROM worker_spool_meta WHERE key='worker_instance_id'"
        ).fetchone()
        if row is not None and self.pending_count() > 0:
            prior = str(row["value"]).strip()
            if prior:
                return prior

        with self.connection:
            self.connection.execute(
                """
                INSERT INTO worker_spool_meta(key,value)
                VALUES ('worker_instance_id', ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (proposed,),
            )
        return proposed

    def clear(self) -> int:
        with self.connection:
            return self.connection.execute(
                "DELETE FROM pending_result_batches"
            ).rowcount

    @staticmethod
    def _payload(batch: ResultBatch) -> str:
        return json.dumps(
            {
                "task_id": batch.task_id,
                "generation": batch.generation,
                "sequence_no": batch.sequence_no,
                "results": [dict(item) for item in batch.results],
                "artifacts": [
                    {
                        "uri": item.uri,
                        "sha256": item.sha256,
                        "size_bytes": item.size_bytes,
                        "content_type": item.content_type,
                        "compression": item.compression,
                        "metadata": dict(item.metadata),
                    }
                    for item in batch.artifacts
                ],
                "cursor_after": batch.cursor_after,
                "final": batch.final,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    def put(self, batch: ResultBatch) -> None:
        payload = self._payload(batch)
        with self.connection:
            prior = self.connection.execute(
                "SELECT payload_json FROM pending_result_batches WHERE batch_id=?",
                (batch.batch_id,),
            ).fetchone()
            if prior is not None and str(prior["payload_json"]) != payload:
                raise ValueError("local batch replay changed payload")
            self.connection.execute(
                """
                INSERT OR IGNORE INTO pending_result_batches(
                    batch_id, task_id, generation, sequence_no,
                    payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    batch.batch_id,
                    batch.task_id,
                    batch.generation,
                    batch.sequence_no,
                    payload,
                    float(self.clock()),
                ),
            )

    def ack(self, batch_id: str) -> bool:
        with self.connection:
            return (
                self.connection.execute(
                    "DELETE FROM pending_result_batches WHERE batch_id=?",
                    (batch_id,),
                ).rowcount
                == 1
            )

    def discard_generation(self, task_id: str, generation: int) -> int:
        with self.connection:
            return self.connection.execute(
                """
                DELETE FROM pending_result_batches
                WHERE task_id=? AND generation=?
                """,
                (task_id, int(generation)),
            ).rowcount

    def pending(self) -> tuple[ResultBatch, ...]:
        rows = self.connection.execute(
            """
            SELECT payload_json FROM pending_result_batches
            ORDER BY created_at, task_id, sequence_no
            """
        ).fetchall()
        batches: list[ResultBatch] = []
        for row in rows:
            raw = json.loads(str(row["payload_json"]))
            batches.append(
                ResultBatch(
                    task_id=str(raw["task_id"]),
                    generation=int(raw["generation"]),
                    sequence_no=int(raw["sequence_no"]),
                    results=tuple(dict(item) for item in raw["results"]),
                    artifacts=tuple(
                        ArtifactRef(
                            uri=str(item["uri"]),
                            sha256=str(item["sha256"]),
                            size_bytes=int(item["size_bytes"]),
                            content_type=str(item["content_type"]),
                            compression=str(item["compression"]),
                            metadata=dict(item.get("metadata", {})),
                        )
                        for item in raw["artifacts"]
                    ),
                    cursor_after=raw.get("cursor_after"),
                    final=bool(raw.get("final", False)),
                )
            )
        return tuple(batches)
