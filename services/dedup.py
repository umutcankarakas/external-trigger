"""
Dedup Service: consumes normalized_events and merges events that refer to the
same real-world occurrence (same name + date + location). Publishes to
dedupped_events after every upsert so downstream services stay current.

Deduplication key: SHA-256(name.lower() | date.date() | location.lower())[:16]
"""
import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Tuple

from core.bus import MessageBus
from core.models import DeduppedEvent, NormalizedEvent

logger = logging.getLogger(__name__)


def _event_key(name: str, date: datetime, location: str) -> str:
    raw = f"{name.lower().strip()}|{date.date().isoformat()}|{location.lower().strip()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class DedupStore:
    def __init__(self):
        self._events: Dict[str, DeduppedEvent] = {}

    def upsert(self, event: NormalizedEvent) -> Tuple[DeduppedEvent, bool]:
        """Returns (DeduppedEvent, is_new)."""
        key = _event_key(event.name, event.date, event.location)
        existing = self._events.get(key)

        if existing:
            existing.seen_count += 1
            if event.source_id not in existing.sources:
                existing.sources.append(event.source_id)
            # Keep the tightest price bounds seen across all sources
            if event.price_min is not None:
                existing.price_min = (
                    min(existing.price_min, event.price_min)
                    if existing.price_min is not None
                    else event.price_min
                )
            if event.price_max is not None:
                existing.price_max = (
                    max(existing.price_max, event.price_max)
                    if existing.price_max is not None
                    else event.price_max
                )
            # Union of tags
            existing.tags = list(set(existing.tags) | set(event.tags))
            existing.last_updated = datetime.utcnow()
            return existing, False

        new_event = DeduppedEvent(
            event_key=key,
            name=event.name,
            date=event.date,
            location=event.location,
            venue=event.venue,
            category=event.category,
            tags=event.tags,
            sources=[event.source_id],
            price_min=event.price_min,
            price_max=event.price_max,
            currency=event.currency,
            url=event.url,
            first_seen=datetime.utcnow(),
        )
        self._events[key] = new_event
        return new_event, True

    def remove(self, event_key: str) -> None:
        self._events.pop(event_key, None)

    def get_all(self) -> List[DeduppedEvent]:
        return list(self._events.values())


class DedupService:
    def __init__(self, bus: MessageBus):
        self.bus = bus
        self.store = DedupStore()
        self._inbox = bus.subscribe("normalized_events")

    async def start(self) -> None:
        logger.info("[DEDUP] Started")
        while True:
            event: NormalizedEvent = await self._inbox.get()
            dedupped, is_new = self.store.upsert(event)
            await self.bus.publish("dedupped_events", dedupped)
            if is_new:
                logger.info(
                    f"[DEDUP] New  → '{dedupped.name}' @ {dedupped.location}"
                )
            else:
                logger.debug(
                    f"[DEDUP] Merged '{dedupped.name}' — sources: {dedupped.sources}"
                )
