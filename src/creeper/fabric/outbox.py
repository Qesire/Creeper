"""Transactional outbox relay for Fabric v2."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Protocol


class OutboxPublisher(Protocol):
    async def publish(
        self,
        *,
        topic: str,
        payload: Mapping[str, object],
        event_id: str,
    ) -> None: ...


def _event_payload(row) -> tuple[str, str, dict[str, object]]:
    event_id = str(row["event_id"])
    topic = str(row["topic"])
    if "payload_json" in row.keys():
        payload = json.loads(str(row["payload_json"]))
    else:
        payload = dict(row["payload"])
    if not isinstance(payload, dict):
        raise ValueError("fabric outbox payload must be an object")
    return event_id, topic, payload


class PollingOutboxRelay:
    """Publish committed outbox rows; duplicate publication is allowed."""

    def __init__(
        self,
        store,
        publisher: OutboxPublisher,
        *,
        batch_size: int = 100,
        idle_sleep_seconds: float = 0.5,
    ) -> None:
        if batch_size < 1 or idle_sleep_seconds <= 0:
            raise ValueError("invalid outbox relay policy")
        self.store = store
        self.publisher = publisher
        self.batch_size = int(batch_size)
        self.idle_sleep_seconds = float(idle_sleep_seconds)

    async def run_once(self) -> int:
        rows = self.store.unpublished_events(limit=self.batch_size)
        published = 0
        for row in rows:
            event_id, topic, payload = _event_payload(row)
            await self.publisher.publish(
                topic=topic,
                payload=payload,
                event_id=event_id,
            )
            self.store.mark_event_published(event_id)
            published += 1
        return published

    async def run_forever(self) -> None:
        while True:
            count = await self.run_once()
            if count == 0:
                await asyncio.sleep(self.idle_sleep_seconds)


class NatsJetStreamPublisher:
    """Optional JetStream wake-up publisher.

    NATS is not imported unless this publisher is used.
    """

    def __init__(
        self,
        servers: tuple[str, ...],
        *,
        name: str = "creeper-fabric-outbox",
    ) -> None:
        if not servers or any(not item.strip() for item in servers):
            raise ValueError("at least one NATS server is required")
        self.servers = tuple(servers)
        self.name = name
        self._nc = None
        self._js = None

    async def connect(self) -> None:
        try:
            import nats
        except ImportError as exc:  # pragma: no cover - optional extra
            raise RuntimeError(
                "NATS outbox relay requires the distributed optional dependency"
            ) from exc
        self._nc = await nats.connect(
            servers=list(self.servers),
            name=self.name,
        )
        self._js = self._nc.jetstream()

    async def close(self) -> None:
        if self._nc is not None:
            await self._nc.drain()
            self._nc = None
            self._js = None

    async def publish(
        self,
        *,
        topic: str,
        payload: Mapping[str, object],
        event_id: str,
    ) -> None:
        if self._js is None:
            await self.connect()
        assert self._js is not None
        body = json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        await self._js.publish(
            topic,
            body,
            headers={"Nats-Msg-Id": event_id},
        )
