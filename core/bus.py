"""
In-process asyncio message bus.

Replaces the Kafka-backed bus with plain asyncio.Queue so no external broker
is needed.  The publish() / subscribe() surface is identical — service code
is unchanged.

Fan-out: every subscriber on a topic receives every message independently,
which is the correct semantics (unlike the old Kafka impl that shared a
group_id and competed for messages when multiple subscribers existed).

No serialization needed — Python objects are passed by reference since
everything runs in the same process.
"""
import asyncio
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class MessageBus:
    def __init__(self) -> None:
        # topic → list of queues (one per subscriber)
        self._queues: Dict[str, List[asyncio.Queue]] = {}

    async def start(self) -> None:
        total = sum(len(qs) for qs in self._queues.values())
        logger.info(f"[BUS] Started ({total} subscribers)")

    async def stop(self) -> None:
        pass  # nothing to clean up — queues are garbage-collected with the process

    async def publish(self, topic: str, message: Any) -> None:
        """Fan out message to all subscribers on the topic."""
        for queue in self._queues.get(topic, []):
            await queue.put(message)

    def subscribe(self, topic: str, group_id: Optional[str] = None) -> asyncio.Queue:
        """Return a new queue that receives every message published to topic."""
        queue: asyncio.Queue = asyncio.Queue()
        self._queues.setdefault(topic, []).append(queue)
        return queue
