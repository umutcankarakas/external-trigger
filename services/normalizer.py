"""
Normalizer: consumes raw_events, maps each source's schema to a common
NormalizedEvent shape, and publishes to normalized_events.

Add a new _normalize_<source_type> function and register it in
SOURCE_NORMALIZERS to support additional sources.
"""
import logging
from datetime import datetime
from typing import Callable, Dict, Optional

from core.bus import MessageBus
from core.models import NormalizedEvent, RawEvent
from services.source_registry import SourceRegistry

logger = logging.getLogger(__name__)


def _parse_dt(value: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return datetime.utcnow()


# ---------------------------------------------------------------------------
# Per-source normalizers
# ---------------------------------------------------------------------------

def _normalize_mock(raw: RawEvent) -> Optional[NormalizedEvent]:
    d = raw.raw_data
    price = d.get("price", {})
    return NormalizedEvent(
        event_id=f"{raw.source_id}_{d['id']}",
        name=d["title"],
        date=_parse_dt(d["start_date"]),
        location=f"{d['city']}, {d['country']}",
        venue=d.get("venue", ""),
        category=[d.get("category", ""), d.get("subcategory", "")],
        tags=d.get("tags", []),
        source_id=raw.source_id,
        url=d.get("url", ""),
        price_min=price.get("min"),
        price_max=price.get("max"),
        currency=price.get("currency", "TRY"),
        raw_data=d,
        fetched_at=raw.fetched_at,
    )


def _normalize_ticketmaster(raw: RawEvent) -> Optional[NormalizedEvent]:
    d = raw.raw_data
    dates = d.get("dates", {}).get("start", {})
    dt_str = dates.get("dateTime") or dates.get("localDate", "")
    venues = d.get("_embedded", {}).get("venues", [{}])
    venue = venues[0] if venues else {}
    city = venue.get("city", {}).get("name", "")
    country = venue.get("country", {}).get("name", "")
    price_ranges = d.get("priceRanges", [{}])
    pr = price_ranges[0] if price_ranges else {}
    cl = (d.get("classifications") or [{}])[0]
    genre = cl.get("genre", {}).get("name", "")
    segment = cl.get("segment", {}).get("name", "")
    return NormalizedEvent(
        event_id=f"ticketmaster_{d['id']}",
        name=d.get("name", ""),
        date=_parse_dt(dt_str),
        location=f"{city}, {country}",
        venue=venue.get("name", ""),
        category=[segment, genre],
        tags=[segment, genre, d.get("type", "event")],
        source_id=raw.source_id,
        url=d.get("url", ""),
        price_min=pr.get("min"),
        price_max=pr.get("max"),
        currency=pr.get("currency", "USD"),
        raw_data=d,
        fetched_at=raw.fetched_at,
    )


SOURCE_NORMALIZERS: Dict[str, Callable[[RawEvent], Optional[NormalizedEvent]]] = {
    "mock": _normalize_mock,
    "ticketmaster": _normalize_ticketmaster,
}


# ---------------------------------------------------------------------------
# Normalizer service
# ---------------------------------------------------------------------------

class Normalizer:
    def __init__(self, bus: MessageBus, registry: Optional[SourceRegistry] = None):
        self.bus = bus
        self.registry = registry
        self._inbox = bus.subscribe("raw_events")

    async def start(self) -> None:
        logger.info("[NORMALIZER] Started")
        while True:
            raw: RawEvent = await self._inbox.get()
            event = self._normalize(raw)
            if event:
                await self.bus.publish("normalized_events", event)
                logger.debug(f"[NORMALIZER] '{event.name}' from {event.source_id}")

    def _normalize(self, raw: RawEvent) -> Optional[NormalizedEvent]:
        source_type = "mock"
        if self.registry:
            src = self.registry.get_source(raw.source_id)
            if src:
                source_type = src.source_type

        fn = SOURCE_NORMALIZERS.get(source_type, _normalize_mock)
        try:
            return fn(raw)
        except Exception as exc:
            logger.warning(
                f"[NORMALIZER] Failed for {raw.source_id}/{raw.raw_data.get('id')}: {exc}"
            )
            return None
