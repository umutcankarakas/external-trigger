"""
Trust Score Service: assigns a confidence score to each deduplicated event.

Score formula:
  base       = max_weight if any source is "trusted", else avg_weight
  count_boost = 1.0 (1 source) | 1.2 (2 sources) | 1.4 (3+ sources)
  seen_boost  = capped at +10% for repeated sightings
  score       = min(base * count_boost * seen_boost, 1.0)

Lazy recalculation:
  A TrustedEvent stores last_calculated_at. When a DeduppedEvent arrives, the
  score is only recalculated if dedupped_event.last_updated > last_calculated_at.
  This prevents redundant work when an event hasn't changed since it was last scored.

Events above a configurable threshold are published to trusted_events and
stored in the TrustedEventsStore for downstream consumers.
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional

from core.bus import MessageBus
from core.models import BatchComplete, DeduppedEvent, TrustedEvent
from services.source_registry import SourceRegistry

logger = logging.getLogger(__name__)

# Multipliers for events confirmed by multiple independent sources
_COUNT_BOOST = {1: 1.0, 2: 1.2, 3: 1.4}

TRUST_THRESHOLD = 0.0        # accept everything in POC; raise in production
TRUSTED_SOURCE_MIN_WEIGHT = 0.85  # sources at or above this weight are "trusted"


class TrustedEventsStore:
    def __init__(self):
        self._events: Dict[str, TrustedEvent] = {}

    def upsert(self, event: TrustedEvent) -> None:
        self._events[event.event_key] = event

    def get(self, event_key: str) -> Optional[TrustedEvent]:
        return self._events.get(event_key)

    def remove(self, event_key: str) -> None:
        self._events.pop(event_key, None)

    def get_all(self) -> List[TrustedEvent]:
        return list(self._events.values())

    def __len__(self) -> int:
        return len(self._events)


class TrustScoreService:
    def __init__(self, bus: MessageBus, registry: SourceRegistry):
        self.bus = bus
        self.registry = registry
        self.store = TrustedEventsStore()
        self._inbox = bus.subscribe("dedupped_events")

    async def _flush_loop(self) -> None:
        """Logs a batch summary every 2 s instead of one line per event."""
        while True:
            await asyncio.sleep(2.0)
            if self._count:
                avg = self._score_sum / self._count
                logger.info(
                    f"[TRUST] {self._count} scored | avg: {avg:.3f} | store: {len(self.store)}"
                )
                self._count = 0
                self._score_sum = 0.0

    async def start(self) -> None:
        logger.info("[TRUST] Started")
        self._count = 0
        self._score_sum = 0.0
        asyncio.create_task(self._flush_loop())
        while True:
            message = await self._inbox.get()

            # BatchComplete is not meaningful for trust score — skip it
            if isinstance(message, BatchComplete):
                continue

            dedupped: DeduppedEvent = message

            # Skip recalculation if the score is still valid.
            # last_updated is set on every merge, so a stale score means
            # a new source was added or price/tags changed since last calculation.
            existing = self.store.get(dedupped.event_key)
            if existing and existing.last_calculated_at >= dedupped.last_updated:
                continue

            trusted = self._score(dedupped)
            self.store.upsert(trusted)
            await self.bus.publish("trusted_events", trusted)
            self._count += 1
            self._score_sum += trusted.trust_score

    def _score(self, event: DeduppedEvent) -> TrustedEvent:
        weights = [self.registry.get_weight(sid) for sid in event.sources]
        avg_weight = sum(weights) / len(weights) if weights else 0.5
        max_weight = max(weights) if weights else 0.5

        # If any source has high enough weight, use it as the base instead of
        # the average so a single trusted source can anchor the score
        has_trusted = max_weight >= TRUSTED_SOURCE_MIN_WEIGHT
        base = max_weight if has_trusted else avg_weight

        count_boost = _COUNT_BOOST.get(min(len(event.sources), 3), 1.4)

        # Small boost for repeated sightings (capped at +10%)
        seen_boost = min(1.0 + (event.seen_count - 1) * 0.01, 1.1)

        score = round(min(base * count_boost * seen_boost, 1.0), 4)

        return TrustedEvent(
            event_key=event.event_key,
            name=event.name,
            date=event.date,
            location=event.location,
            venue=event.venue,
            category=event.category,
            tags=event.tags,
            sources=list(event.sources),
            price_min=event.price_min,
            price_max=event.price_max,
            currency=event.currency,
            url=event.url,
            lat=event.lat,
            lng=event.lng,
            trust_score=score,
            source_count=len(event.sources),
            seen_count=event.seen_count,
            has_trusted_source=has_trusted,
            last_calculated_at=datetime.utcnow(),
        )
