"""
Fetcher: reads active sources from the Source Registry, fetches each one
asynchronously on its configured schedule, and publishes RawEvents to the
raw_events topic. Mock sources (endpoint starts with mock://) are handled
by in-process generators; real HTTP sources use aiohttp.

Retry strategy: exponential backoff — 2 s → 4 s → 8 s (max 3 attempts).
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import aiohttp

from core.bus import MessageBus
from core.models import RawEvent
from services.source_registry import Source, SourceRegistry
from services.vault import Vault

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mock data generators
# ---------------------------------------------------------------------------

def _gen_concerts(_source: Source, _query: dict) -> List[Dict]:
    base = datetime.utcnow()
    return [
        {
            "id": "concert_001",
            "title": "Jazz Night at Babylon",
            "start_date": (base + timedelta(days=6)).strftime("%Y-%m-%dT21:00:00"),
            "city": "Istanbul", "country": "Turkey",
            "venue": "Babylon",
            "category": "concert", "subcategory": "jazz",
            "tags": ["jazz", "live music", "nightlife", "entertainment"],
            "price": {"min": 150, "max": 300, "currency": "TRY"},
            "url": "https://biletix.com/event/concert_001",
        },
        {
            "id": "concert_002",
            "title": "Indie Rock Istanbul Festival",
            "start_date": (base + timedelta(days=11)).strftime("%Y-%m-%dT18:00:00"),
            "city": "Istanbul", "country": "Turkey",
            "venue": "KüçükÇiftlik Park",
            "category": "concert", "subcategory": "indie rock",
            "tags": ["indie", "rock", "festival", "live music", "outdoor"],
            "price": {"min": 500, "max": 800, "currency": "TRY"},
            "url": "https://biletix.com/event/concert_002",
        },
        {
            "id": "concert_003",
            "title": "Coldplay World Tour – Istanbul",
            "start_date": (base + timedelta(days=31)).strftime("%Y-%m-%dT20:00:00"),
            "city": "Istanbul", "country": "Turkey",
            "venue": "Atatürk Olympic Stadium",
            "category": "concert", "subcategory": "pop rock",
            "tags": ["pop", "rock", "stadium", "international", "live music"],
            "price": {"min": 1500, "max": 5000, "currency": "TRY"},
            "url": "https://biletix.com/event/concert_003",
        },
        {
            "id": "concert_004",
            "title": "Classical Symphony Night",
            "start_date": (base + timedelta(days=17)).strftime("%Y-%m-%dT20:30:00"),
            "city": "Istanbul", "country": "Turkey",
            "venue": "Cemal Reşit Rey Concert Hall",
            "category": "concert", "subcategory": "classical",
            "tags": ["classical", "symphony", "orchestra", "culture"],
            "price": {"min": 100, "max": 250, "currency": "TRY"},
            "url": "https://biletix.com/event/concert_004",
        },
        {
            "id": "concert_005",
            "title": "Electronic Music Night – Ankara",
            "start_date": (base + timedelta(days=14)).strftime("%Y-%m-%dT23:00:00"),
            "city": "Ankara", "country": "Turkey",
            "venue": "Congresium",
            "category": "concert", "subcategory": "electronic",
            "tags": ["electronic", "dj", "dance", "nightlife"],
            "price": {"min": 200, "max": 400, "currency": "TRY"},
            "url": "https://biletix.com/event/concert_005",
        },
    ]


def _gen_sports(_source: Source, _query: dict) -> List[Dict]:
    base = datetime.utcnow()
    return [
        {
            "id": "sports_001",
            "title": "Galatasaray vs Fenerbahçe – Süper Lig Derby",
            "start_date": (base + timedelta(days=5)).strftime("%Y-%m-%dT19:00:00"),
            "city": "Istanbul", "country": "Turkey",
            "venue": "RAMS Park",
            "category": "sports", "subcategory": "football",
            "tags": ["football", "derby", "süper lig", "live sports"],
            "price": {"min": 300, "max": 2000, "currency": "TRY"},
            "url": "https://biletmaster.com/event/sports_001",
        },
        {
            "id": "sports_002",
            "title": "EuroLeague – Anadolu Efes vs Real Madrid",
            "start_date": (base + timedelta(days=3)).strftime("%Y-%m-%dT20:00:00"),
            "city": "Istanbul", "country": "Turkey",
            "venue": "Sinan Erdem Arena",
            "category": "sports", "subcategory": "basketball",
            "tags": ["basketball", "euroleague", "live sports"],
            "price": {"min": 200, "max": 800, "currency": "TRY"},
            "url": "https://biletmaster.com/event/sports_002",
        },
        {
            "id": "sports_003",
            "title": "Champions League Final",
            "start_date": (base + timedelta(days=44)).strftime("%Y-%m-%dT21:00:00"),
            "city": "London", "country": "UK",
            "venue": "Wembley Stadium",
            "category": "sports", "subcategory": "football",
            "tags": ["football", "champions league", "final", "international"],
            "price": {"min": 200, "max": 2000, "currency": "EUR"},
            "url": "https://uefa.com/event/sports_003",
        },
        {
            "id": "sports_004",
            "title": "Istanbul Marathon 2026",
            "start_date": (base + timedelta(days=21)).strftime("%Y-%m-%dT08:00:00"),
            "city": "Istanbul", "country": "Turkey",
            "venue": "Bosphorus Bridge",
            "category": "sports", "subcategory": "running",
            "tags": ["marathon", "running", "outdoor", "fitness"],
            "price": {"min": 100, "max": 200, "currency": "TRY"},
            "url": "https://istanbulmarathon.com/event/sports_004",
        },
    ]


def _gen_cultural(_source: Source, _query: dict) -> List[Dict]:
    base = datetime.utcnow()
    return [
        {
            "id": "cultural_001",
            "title": "Modern Art Exhibition – Istanbul Modern",
            "start_date": (base + timedelta(days=4)).strftime("%Y-%m-%dT10:00:00"),
            "city": "Istanbul", "country": "Turkey",
            "venue": "Istanbul Modern",
            "category": "culture", "subcategory": "art exhibition",
            "tags": ["art", "modern art", "exhibition", "culture", "gallery"],
            "price": {"min": 80, "max": 80, "currency": "TRY"},
            "url": "https://istanbulmodern.org/event/cultural_001",
        },
        {
            "id": "cultural_002",
            "title": "Hamlet – Zorlu PSM Theater",
            "start_date": (base + timedelta(days=7)).strftime("%Y-%m-%dT20:00:00"),
            "city": "Istanbul", "country": "Turkey",
            "venue": "Zorlu PSM",
            "category": "culture", "subcategory": "theater",
            "tags": ["theater", "drama", "shakespeare", "culture", "arts"],
            "price": {"min": 200, "max": 600, "currency": "TRY"},
            "url": "https://zorlupsm.com/event/cultural_002",
        },
        {
            "id": "cultural_003",
            "title": "Istanbul Food & Taste Festival",
            "start_date": (base + timedelta(days=12)).strftime("%Y-%m-%dT12:00:00"),
            "city": "Istanbul", "country": "Turkey",
            "venue": "Harbiye Open Air Theater",
            "category": "culture", "subcategory": "food",
            "tags": ["food", "dining", "festival", "gastronomy", "outdoor"],
            "price": {"min": 50, "max": 200, "currency": "TRY"},
            "url": "https://foodfest.istanbul/event/cultural_003",
        },
        {
            "id": "cultural_004",
            "title": "TEDxIstanbul 2026",
            "start_date": (base + timedelta(days=19)).strftime("%Y-%m-%dT09:00:00"),
            "city": "Istanbul", "country": "Turkey",
            "venue": "Sabancı Center",
            "category": "culture", "subcategory": "conference",
            "tags": ["ted", "ideas", "technology", "culture", "conference", "innovation"],
            "price": {"min": 500, "max": 500, "currency": "TRY"},
            "url": "https://tedxistanbul.com/event/cultural_004",
        },
        {
            "id": "cultural_005",
            "title": "Photography Exhibition – Faces of Anatolia",
            "start_date": (base + timedelta(days=5)).strftime("%Y-%m-%dT10:00:00"),
            "city": "Ankara", "country": "Turkey",
            "venue": "CerModern",
            "category": "culture", "subcategory": "photography",
            "tags": ["photography", "exhibition", "art", "culture", "gallery"],
            "price": {"min": 30, "max": 30, "currency": "TRY"},
            "url": "https://cermodern.org/event/cultural_005",
        },
    ]


MOCK_HANDLERS: Dict[str, Callable] = {
    "mock_concerts": _gen_concerts,
    "mock_sports": _gen_sports,
    "mock_cultural": _gen_cultural,
}


# ---------------------------------------------------------------------------
# Fetcher service
# ---------------------------------------------------------------------------

class Fetcher:
    RETRY_DELAYS = [2, 4, 8]

    def __init__(self, registry: SourceRegistry, vault: Vault, bus: MessageBus):
        self.registry = registry
        self.vault = vault
        self.bus = bus
        self._last_fetched: Dict[str, datetime] = {}

    async def start(self) -> None:
        logger.info("[FETCHER] Started — polling every 5 s")
        while True:
            sources = self.registry.get_active_sources()
            await asyncio.gather(*[self._maybe_fetch(s) for s in sources])
            await asyncio.sleep(5)

    async def _maybe_fetch(self, source: Source) -> None:
        last = self._last_fetched.get(source.source_id)
        if last and (datetime.utcnow() - last).total_seconds() < source.fetch_frequency:
            return
        await self._fetch_with_retry(source)

    async def _fetch_with_retry(self, source: Source) -> None:
        for attempt, delay in enumerate(self.RETRY_DELAYS):
            try:
                events = await self._fetch(source)
                self._last_fetched[source.source_id] = datetime.utcnow()
                for event in events:
                    await self.bus.publish(
                        "raw_events",
                        RawEvent(source_id=source.source_id, raw_data=event),
                    )
                logger.info(
                    f"[FETCHER] {source.source_id}: fetched {len(events)} events"
                )
                return
            except Exception as exc:
                if attempt < len(self.RETRY_DELAYS) - 1:
                    logger.warning(
                        f"[FETCHER] {source.source_id}: attempt {attempt + 1} failed "
                        f"({exc}), retrying in {delay}s"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"[FETCHER] {source.source_id}: all retries exhausted — {exc}"
                    )

    async def _fetch(self, source: Source) -> List[Dict[str, Any]]:
        if source.endpoint.startswith("mock://"):
            handler = MOCK_HANDLERS.get(source.source_id)
            if not handler:
                raise ValueError(f"No mock handler registered for '{source.source_id}'")
            return handler(source, source.search_query)

        api_key: Optional[str] = None
        if source.auth_ref:
            api_key = self.vault.get(source.auth_ref)

        params = dict(source.search_query)
        if api_key:
            params["apikey"] = api_key

        async with aiohttp.ClientSession() as session:
            async with session.get(
                source.endpoint,
                params=params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

        # Unwrap common API envelopes
        if isinstance(data, dict):
            if "_embedded" in data:          # Ticketmaster
                return data["_embedded"].get("events", [])
            if "results" in data:            # PredictHQ
                return data["results"]
        return data if isinstance(data, list) else []
