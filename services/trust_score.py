"""
Trust Score Service: assigns a confidence score to each deduplicated event.

Score formula:
  avg_weight = mean of source_weight for each contributing source
  boost      = 1.0 (1 source) | 1.2 (2 sources) | 1.4 (3+ sources)
  score      = min(avg_weight * boost, 1.0)

Events above a configurable threshold are published to trusted_events and
stored in the TrustedEventsStore for downstream consumers.
"""
import logging
from datetime import datetime
from typing import Dict, List

from core.bus import MessageBus
from core.models import DeduppedEvent, TrustedEvent
from services.source_registry import SourceRegistry

logger = logging.getLogger(__name__)

_COUNT_BOOST = {1: 1.0, 2: 1.2, 3: 1.4}
TRUST_THRESHOLD = 0.0        # accept everything in POC; raise in production
TRUSTED_SOURCE_MIN_WEIGHT = 0.85  # sources at or above this weight are "trusted"


class TrustedEventsStore:
    def __init__(self):
        self._events: Dict[str, TrustedEvent] = {}

    def upsert(self, event: TrustedEvent) -> None:
        self._events[event.event_key] = event

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

    async def start(self) -> None:
        logger.info("[TRUST SCORE] Started")
        while True:
            dedupped: DeduppedEvent = await self._inbox.get()
            trusted = self._score(dedupped)
            self.store.upsert(trusted)
            await self.bus.publish("trusted_events", trusted)
            logger.info(
                f"[TRUST SCORE] '{trusted.name}' → score={trusted.trust_score:.3f} "
                f"seen={trusted.seen_count} trusted_src={trusted.has_trusted_source} "
                f"sources={trusted.sources}"
            )

    def _score(self, event: DeduppedEvent) -> TrustedEvent:
        weights = [self.registry.get_weight(sid) for sid in event.sources]
        avg_weight = sum(weights) / len(weights) if weights else 0.5
        max_weight = max(weights) if weights else 0.5

        # If any source is trusted, use its weight as the base (floor the score up)
        has_trusted = max_weight >= TRUSTED_SOURCE_MIN_WEIGHT
        base = max_weight if has_trusted else avg_weight

        # Boost for multiple distinct sources
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
            trust_score=score,
            source_count=len(event.sources),
            seen_count=event.seen_count,
            has_trusted_source=has_trusted,
            first_seen=event.first_seen,
        )
