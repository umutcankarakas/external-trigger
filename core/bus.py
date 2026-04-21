"""
Kafka-backed message bus.

Keeps the same publish() / subscribe() surface as the old in-memory bus so
service code is unchanged.  Each subscribe() call creates an independent
AIOKafkaConsumer with its own group_id, giving fan-out semantics when needed.

Serialization: JSON over UTF-8.  datetime fields are round-tripped as ISO-8601
strings; everything else is a native JSON type.

BatchComplete sentinel: can appear in any topic — detected by checking the
"type" discriminator field before topic-specific dispatch.
"""
import dataclasses
import json
import logging
from datetime import datetime
from typing import Any, List, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from core.models import BatchComplete, DeduppedEvent, NormalizedEvent, RawEvent, TrustedEvent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def _default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Not serializable: {type(obj)}")


def _to_bytes(message: Any) -> bytes:
    # ensure_ascii=False preserves non-ASCII characters (e.g. Turkish) in Kafka
    return json.dumps(dataclasses.asdict(message), default=_default, ensure_ascii=False).encode("utf-8")


def _dt(s: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(s) if s else None


def _from_bytes(data: bytes, topic: str) -> Any:
    d = json.loads(data.decode("utf-8"))

    # BatchComplete sentinel can appear in any topic — check before topic dispatch
    if d.get("type") == "batch_complete":
        return BatchComplete(batch_id=d["batch_id"])

    if topic == "raw_events":
        d["fetched_at"] = _dt(d["fetched_at"])
        return RawEvent(**d)
    if topic == "normalized_events":
        d["date"] = _dt(d["date"])
        return NormalizedEvent(**d)
    if topic == "dedupped_events":
        d["date"] = _dt(d["date"])
        d["last_updated"] = _dt(d["last_updated"])
        return DeduppedEvent(**d)
    if topic == "trusted_events":
        d["date"] = _dt(d["date"])
        d["last_calculated_at"] = _dt(d["last_calculated_at"])
        return TrustedEvent(**d)
    raise ValueError(f"Unknown topic: {topic}")


# ---------------------------------------------------------------------------
# Inbox (consumer wrapper)
# ---------------------------------------------------------------------------

class KafkaInbox:
    """Wraps AIOKafkaConsumer with a simple .get() interface."""

    def __init__(self, topic: str, bootstrap_servers: str, group_id: str) -> None:
        self._topic = topic
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            # short enough to unblock quickly after a crash, long enough to
            # survive brief broker hiccups without triggering a rebalance
            session_timeout_ms=20000,
            heartbeat_interval_ms=3000,
        )
        self._started = False

    async def start(self) -> None:
        if not self._started:
            await self._consumer.start()
            self._started = True

    async def get(self) -> Any:
        await self.start()
        msg = await self._consumer.getone()
        return _from_bytes(msg.value, self._topic)

    async def stop(self) -> None:
        if self._started:
            await self._consumer.stop()


# ---------------------------------------------------------------------------
# Bus
# ---------------------------------------------------------------------------

class MessageBus:
    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        self._bootstrap_servers = bootstrap_servers
        self._producer: Optional[AIOKafkaProducer] = None
        self._inboxes: List[KafkaInbox] = []

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
        await self._producer.start()
        # Eagerly start all consumers so they are ready before the pipeline
        # tasks run. Services must call subscribe() before bus.start().
        for inbox in self._inboxes:
            await inbox.start()
        logger.info(f"[BUS] Connected @ {self._bootstrap_servers} ({len(self._inboxes)} consumers)")

    async def stop(self) -> None:
        for inbox in self._inboxes:
            await inbox.stop()
        if self._producer:
            await self._producer.stop()

    async def publish(self, topic: str, message: Any) -> None:
        await self._producer.send(topic, _to_bytes(message))

    def subscribe(self, topic: str, group_id: Optional[str] = None) -> KafkaInbox:
        # Each consumer gets its own group_id so all subscribers receive every message
        inbox = KafkaInbox(
            topic=topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=group_id or topic,
        )
        self._inboxes.append(inbox)
        return inbox
