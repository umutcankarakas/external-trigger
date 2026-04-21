"""
Dedup Service: consumes normalized_events and merges events that refer to the
same real-world occurrence (same name + date + location).

Batch-flush design:
  Events are accumulated in _pending_keys as they arrive. When the BatchComplete
  sentinel arrives (signalling that all sources in a polling cycle have been
  processed), only the pending events are published to dedupped_events — not
  every normalized event individually. This means Kafka receives one message per
  unique event per batch, not one message per source per event.

Deduplication key: SHA-256(name.lower() | date.date() | location.lower())[:16]
"""
import asyncio
import hashlib
import logging
import re
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from core.bus import MessageBus
from core.models import BatchComplete, DeduppedEvent, NormalizedEvent

logger = logging.getLogger(__name__)


# Explicit map for Turkish characters that don't decompose via NFKD
_TR_MAP = str.maketrans({
    'ı': 'i', 'İ': 'i',
    'ğ': 'g', 'Ğ': 'g',
    'ş': 's', 'Ş': 's',
    'ü': 'u', 'Ü': 'u',
    'ö': 'o', 'Ö': 'o',
    'ç': 'c', 'Ç': 'c',
})


def _norm(s: str) -> str:
    """Normalize a string for key comparison.

    Handles: Turkish chars, other diacritics, punctuation, extra whitespace.
    'İzmir' == 'izmir' == 'IZMIR', 'Fazıl Say' == 'Fazil Say', etc.
    """
    s = s.lower().strip().translate(_TR_MAP)
    # Decompose remaining accented chars (e.g. é → e + combining accent)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # Replace punctuation/symbols with spaces, collapse whitespace
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _event_key(name: str, date: datetime, location: str) -> str:
    # Use only the city part of location so "İzmir" and "İzmir, Turkey" match
    city = location.split(",")[0]
    raw = f"{_norm(name)}|{date.date().isoformat()}|{_norm(city)}"
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
            # Union of tags so every source's classification contributes
            existing.tags = list(set(existing.tags) | set(event.tags))
            # Coordinates: keep the first non-null value; don't overwrite known data
            if existing.lat is None and event.lat is not None:
                existing.lat = event.lat
            if existing.lng is None and event.lng is not None:
                existing.lng = event.lng
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
            lat=event.lat,
            lng=event.lng,
        )
        self._events[key] = new_event
        return new_event, True

    def get(self, event_key: str) -> Optional[DeduppedEvent]:
        return self._events.get(event_key)

    def remove(self, event_key: str) -> None:
        self._events.pop(event_key, None)

    def get_all(self) -> List[DeduppedEvent]:
        return list(self._events.values())

    def __len__(self) -> int:
        return len(self._events)


class DedupService:
    def __init__(self, bus: MessageBus):
        self.bus = bus
        self.store = DedupStore()
        self._inbox = bus.subscribe("normalized_events")
        # Tracks event keys updated in the current batch (since last BatchComplete).
        # Only these events are published when the sentinel arrives.
        self._pending_keys: Set[str] = set()

    async def start(self) -> None:
        logger.info("[DEDUP] Started")
        self._new_count = self._merged_count = 0
        while True:
            message = await self._inbox.get()

            if isinstance(message, BatchComplete):
                await self._flush_batch(message)
                continue

            event: NormalizedEvent = message
            dedupped, is_new = self.store.upsert(event)
            # Mark this event as pending — it will be published on the next flush
            self._pending_keys.add(dedupped.event_key)
            if is_new:
                self._new_count += 1
            else:
                self._merged_count += 1

    async def _flush_batch(self, marker: BatchComplete) -> None:
        """Publish all events updated in this batch, then pass the sentinel downstream.

        Only events in _pending_keys are published — not the entire store.
        This means Kafka receives exactly one message per unique event per batch,
        regardless of how many sources reported it.
        """
        if self._pending_keys:
            for key in self._pending_keys:
                event = self.store.get(key)
                if event:
                    await self.bus.publish("dedupped_events", event)
            logger.info(
                f"[DEDUP] Flushed {len(self._pending_keys)} events "
                f"({self._new_count} new, {self._merged_count} merged) "
                f"| store: {len(self.store)}"
            )
            self._pending_keys.clear()
            self._new_count = self._merged_count = 0

        # Pass the sentinel downstream so trust score is aware the batch ended
        await self.bus.publish("dedupped_events", marker)
