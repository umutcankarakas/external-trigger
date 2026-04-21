"""
Normalizer: consumes raw_events, maps each source's schema to a common
NormalizedEvent shape, and publishes to normalized_events.

BatchComplete sentinels are passed through unchanged so the dedup service
knows when the full batch has been normalized.
"""
import asyncio
import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional

from dateutil import parser as _dateutil_parser

from core.bus import MessageBus
from core.models import BatchComplete, NormalizedEvent, RawEvent
from services.source_registry import SourceRegistry

logger = logging.getLogger(__name__)

# Ticketmaster populates these placeholder names when classification is unknown
_SKIP_CLASSIFICATION_NAMES = {"undefined", "other", "unclassified"}


def _parse_dt(value: str) -> Optional[datetime]:
    """Parse a date string in any reasonable format.

    Returns None if the value is empty or unparseable so the caller can
    drop the event rather than storing a corrupted timestamp.
    Timezone info is stripped — APIs targeting Turkish audiences return
    local time, which is what we want for the dedup date key.
    """
    if not value:
        return None
    try:
        return _dateutil_parser.parse(value).replace(tzinfo=None)
    except Exception:
        return None


def _classification_tags(classifications: list) -> List[str]:
    """Walk the full Ticketmaster classification hierarchy to collect tags."""
    tags: List[str] = []
    for cl in classifications:
        for key in ("segment", "genre", "subGenre", "type", "subType"):
            val = cl.get(key) or {}
            name = val.get("name", "")
            if name and name.lower() not in _SKIP_CLASSIFICATION_NAMES:
                tags.append(name)
    return list(dict.fromkeys(tags))  # deduplicate while preserving order


# ---------------------------------------------------------------------------
# Per-source normalizers
# ---------------------------------------------------------------------------

def _normalize_mock(raw: RawEvent) -> Optional[NormalizedEvent]:
    d = raw.raw_data
    price = d.get("price", {})
    date = _parse_dt(d.get("start_date", ""))
    if date is None:
        return None
    return NormalizedEvent(
        name=d["title"],
        date=date,
        location=f"{d['city']}, {d['country']}",
        venue=d.get("venue", ""),
        # Filter empty strings so category is consistent with the TM normalizer
        category=[n for n in [d.get("category", ""), d.get("subcategory", "")] if n],
        tags=d.get("tags", []),
        source_id=raw.source_id,
        url=d.get("url", ""),
        price_min=price.get("min"),
        price_max=price.get("max"),
        currency=price.get("currency", "TRY"),
        lat=None,
        lng=None,
    )


def _normalize_ticketmaster(raw: RawEvent) -> Optional[NormalizedEvent]:
    d = raw.raw_data
    dates = d.get("dates", {}).get("start", {})
    dt_str = dates.get("dateTime") or dates.get("localDate", "")
    date = _parse_dt(dt_str)
    if date is None:
        return None  # unparseable date — drop the event

    venues = (d.get("_embedded") or {}).get("venues", [{}])
    venue = venues[0] if venues else {}
    city = venue.get("city", {}).get("name", "")
    country = venue.get("country", {}).get("name", "")
    venue_name = venue.get("name", "")

    loc_data = venue.get("location") or {}
    lat = float(loc_data["latitude"]) if loc_data.get("latitude") else None
    lng = float(loc_data["longitude"]) if loc_data.get("longitude") else None

    price_ranges = d.get("priceRanges") or [{}]
    pr = price_ranges[0]

    classifications = d.get("classifications") or []
    cl_tags = _classification_tags(classifications)

    # Venue name appended as a tag so it is searchable downstream
    tags = list(dict.fromkeys(cl_tags + ([venue_name] if venue_name else [])))

    category = []
    if classifications:
        cl = classifications[0]
        segment = (cl.get("segment") or {}).get("name", "")
        genre = (cl.get("genre") or {}).get("name", "")
        category = [n for n in [segment, genre] if n]

    return NormalizedEvent(
        name=d.get("name", ""),
        date=date,
        location=f"{city}, {country}",
        venue=venue_name,
        category=category,
        tags=tags,
        source_id=raw.source_id,
        url=d.get("url", ""),
        price_min=pr.get("min"),
        price_max=pr.get("max"),
        currency=pr.get("currency", "USD"),
        lat=lat,
        lng=lng,
    )


SOURCE_NORMALIZERS: Dict[str, Callable[[RawEvent], Optional[NormalizedEvent]]] = {
    "mock": _normalize_mock,
    "ticketmaster": _normalize_ticketmaster
}


# ---------------------------------------------------------------------------
# Normalizer service
# ---------------------------------------------------------------------------

class Normalizer:
    def __init__(self, bus: MessageBus, registry: Optional[SourceRegistry] = None):
        self.bus = bus
        self.registry = registry
        self._inbox = bus.subscribe("raw_events")

    async def _flush_loop(self) -> None:
        """Logs a batch summary every 2 s instead of one line per event."""
        while True:
            await asyncio.sleep(2.0)
            if self._ok or self._fail:
                msg = f"[NORMALIZER] {self._ok} normalized"
                if self._fail:
                    msg += f", {self._fail} failed"
                logger.info(msg)
                self._ok = self._fail = 0

    async def start(self) -> None:
        logger.info("[NORMALIZER] Started")
        self._ok = self._fail = 0
        asyncio.create_task(self._flush_loop())
        while True:
            message = await self._inbox.get()

            # Pass the sentinel straight through so dedup can flush
            if isinstance(message, BatchComplete):
                await self.bus.publish("normalized_events", message)
                continue

            raw: RawEvent = message
            event = self._normalize(raw)
            if event:
                await self.bus.publish("normalized_events", event)
                self._ok += 1
            else:
                self._fail += 1

    def _normalize(self, raw: RawEvent) -> Optional[NormalizedEvent]:
        source_type = "mock"
        if self.registry:
            src = self.registry.get_source(raw.source_id)
            if src:
                source_type = src.source_type

        fn = SOURCE_NORMALIZERS.get(source_type)
        if fn is None:
            logger.warning(
                f"[NORMALIZER] Unknown source_type '{source_type}' for {raw.source_id} — skipping"
            )
            return None
        try:
            return fn(raw)
        except Exception as exc:
            logger.warning(
                f"[NORMALIZER] Failed for {raw.source_id}: {exc}"
            )
            return None
