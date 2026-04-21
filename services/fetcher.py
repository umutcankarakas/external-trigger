"""
Fetcher: reads active sources from the Source Registry, fetches each one
asynchronously on its configured schedule, and publishes RawEvents to the
raw_events topic.

After every polling cycle (all sources attempted), a BatchComplete sentinel is
published so downstream services know the batch is finished and can flush.

Retry strategy: exponential backoff - 2 s -> 4 s -> 8 s (max 3 attempts).
Auth types: "query_param" (e.g. Ticketmaster) or "bearer" (e.g. Eventbrite).

Dedup: each source maintains a set of seen event IDs. Duplicates within the
same batch and across repeated fetches are dropped before reaching Kafka.
"""
import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import aiohttp

from core.bus import MessageBus
from core.models import BatchComplete, RawEvent
from services.source_registry import Source, SourceRegistry
from services.vault import Vault

logger = logging.getLogger(__name__)

_SEEN_IDS_PATH = "config/seen_ids.json"
# IDs older than this are dropped — Ticketmaster only surfaces upcoming events
# so anything beyond this window will never reappear in the API.
_SEEN_IDS_TTL_DAYS = 60


def _load_seen_ids() -> tuple:
    """Load persisted seen-ID sets, pruning entries older than the TTL window.
    Returns (seen_ids, seen_ts) so both structures stay in sync."""
    if not os.path.exists(_SEEN_IDS_PATH):
        return {}, {}
    cutoff = (datetime.utcnow() - timedelta(days=_SEEN_IDS_TTL_DAYS)).isoformat()
    with open(_SEEN_IDS_PATH) as f:
        data = json.load(f)  # {source_id: {event_id: first_seen_iso}}
    seen_ids: Dict[str, Set[str]] = {}
    seen_ts: Dict[str, Dict[str, str]] = {}
    for source_id, entries in data.items():
        valid = {eid: ts for eid, ts in entries.items() if ts >= cutoff}
        seen_ids[source_id] = set(valid.keys())
        seen_ts[source_id] = valid  # preserve timestamps for next save
    return seen_ids, seen_ts


def _save_seen_ids(seen_ts: Dict[str, Dict[str, str]]) -> None:
    with open(_SEEN_IDS_PATH, "w") as f:
        json.dump(seen_ts, f)


def _event_id(event: Dict[str, Any]) -> Optional[str]:
    raw_id = event.get("id")
    return str(raw_id) if raw_id is not None else None


class Fetcher:
    RETRY_DELAYS = [2, 4, 8]  # seconds between attempts (exponential backoff)

    def __init__(self, registry: SourceRegistry, vault: Vault, bus: MessageBus):
        self.registry = registry
        self.vault = vault
        self.bus = bus
        self._last_fetched: Dict[str, datetime] = {}
        # Persisted across restarts so the same event is never published twice.
        # _seen_ids: fast lookup set; _seen_ts: {event_id: first_seen_iso} for TTL pruning.
        # Both are loaded together so timestamps are never lost on save.
        self._seen_ids, self._seen_ts = _load_seen_ids()

    async def start(self) -> None:
        sources = self.registry.get_active_sources()
        summary = ", ".join(
            f"{s.source_id} every {s.fetch_frequency // 60} min" for s in sources
        )
        logger.info(f"[FETCHER] Started  ({summary})")
        while True:
            # Fetch all sources concurrently; each returns the number of new events published
            counts = await asyncio.gather(*[self._maybe_fetch(s) for s in sources])
            # Only send BatchComplete when at least one source had new events.
            # An empty cycle (all sources skipped or all duplicates) produces no Kafka traffic.
            if any(counts):
                batch_id = datetime.utcnow().isoformat()
                await self.bus.publish("raw_events", BatchComplete(batch_id=batch_id))
            await asyncio.sleep(5)

    async def _maybe_fetch(self, source: Source) -> int:
        """Returns the number of new events published, or 0 if the source was not due."""
        last = self._last_fetched.get(source.source_id)
        if last and (datetime.utcnow() - last).total_seconds() < source.fetch_frequency:
            return 0
        return await self._fetch_with_retry(source)

    async def _fetch_with_retry(self, source: Source) -> int:
        """Returns the number of new events published, or 0 on total failure."""
        for attempt, delay in enumerate(self.RETRY_DELAYS):
            try:
                events = await self._fetch(source)
                self._last_fetched[source.source_id] = datetime.utcnow()

                seen = self._seen_ids.setdefault(source.source_id, set())
                ts_map = self._seen_ts.setdefault(source.source_id, {})
                now_iso = datetime.utcnow().isoformat()
                new_events = []
                for event in events:
                    eid = _event_id(event)
                    if eid is None or eid not in seen:
                        if eid:
                            seen.add(eid)
                            ts_map[eid] = now_iso  # record when we first saw it
                        new_events.append(event)

                skipped = len(events) - len(new_events)
                for event in new_events:
                    await self.bus.publish(
                        "raw_events",
                        RawEvent(source_id=source.source_id, raw_data=event),
                    )

                _save_seen_ids(self._seen_ts)

                if skipped:
                    logger.info(
                        f"[FETCHER] {source.source_id}: {len(new_events)} new, "
                        f"{skipped} duplicates skipped"
                    )
                else:
                    logger.info(
                        f"[FETCHER] {source.source_id}: fetched {len(new_events)} events"
                    )
                return len(new_events)
            except Exception as exc:
                if attempt < len(self.RETRY_DELAYS) - 1:
                    logger.warning(
                        f"[FETCHER] {source.source_id}: attempt {attempt + 1} failed "
                        f"({exc}), retrying in {delay}s"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"[FETCHER] {source.source_id}: all retries exhausted - {exc}"
                    )
        return 0

    async def _fetch(self, source: Source) -> List[Dict[str, Any]]:
        # File-based sources (used for mock/test data) — no HTTP needed
        if source.endpoint.startswith("file://"):
            path = source.endpoint[len("file://"):]
            with open(path, encoding="utf-8") as f:
                return json.load(f)

        api_key: Optional[str] = self.vault.get(source.auth_ref) if source.auth_ref else None

        params = dict(source.search_query)
        headers: Dict[str, str] = {}

        if api_key:
            if source.auth_type == "bearer":
                headers["Authorization"] = f"Bearer {api_key}"
            else:
                # query_param auth: key goes in the URL query string (e.g. Ticketmaster)
                params["apikey"] = api_key

        async with aiohttp.ClientSession() as session:
            async with session.get(
                source.endpoint,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

        # Each API wraps its event list differently; unwrap to a flat list
        if isinstance(data, dict):
            if "_embedded" in data:   # Ticketmaster
                return data["_embedded"].get("events", [])
            if "events" in data:      # Eventbrite
                return data["events"]
            if "results" in data:     # PredictHQ
                return data["results"]
        return data if isinstance(data, list) else []
