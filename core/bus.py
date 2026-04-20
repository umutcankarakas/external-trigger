"""
In-memory message bus simulating Kafka topics.

Each topic supports multiple subscribers (fan-out): every published message
is delivered to all subscriber queues independently.
"""
import asyncio
from typing import Any, Dict, List


class Topic:
    def __init__(self, name: str):
        self.name = name
        self._subscribers: List[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers.append(q)
        return q

    async def publish(self, message: Any) -> None:
        for q in self._subscribers:
            await q.put(message)


class MessageBus:
    def __init__(self):
        self._topics: Dict[str, Topic] = {}

    def topic(self, name: str) -> Topic:
        if name not in self._topics:
            self._topics[name] = Topic(name)
        return self._topics[name]

    async def publish(self, topic_name: str, message: Any) -> None:
        await self.topic(topic_name).publish(message)

    def subscribe(self, topic_name: str) -> asyncio.Queue:
        return self.topic(topic_name).subscribe()
