"""
Expiration Service: periodically scans the TrustedEventsStore and removes
events whose date has passed. Runs on a configurable interval (default 60 s).

Removes expired events from three places to keep everything in sync:
  1. TrustedEventsStore  — in-memory fast lookup
  2. DedupStore          — in-memory dedup state (so expired events don't
                           block new events with the same key from being
                           created fresh if the event is re-announced)
  3. PostgreSQL          — both dedupped_events and trusted_events tables
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional

from core.db import Database
from services.dedup import DedupStore
from services.trust_score import TrustedEventsStore

logger = logging.getLogger(__name__)


class ExpirationService:
    def __init__(
        self,
        store: TrustedEventsStore,
        check_interval: int = 60,
        db: Optional[Database] = None,
        dedup_store: Optional[DedupStore] = None,
    ):
        self.store = store
        self.check_interval = check_interval
        self.db = db
        self.dedup_store = dedup_store

    async def start(self) -> None:
        logger.info(f"[EXPIRATION] Started - checking every {self.check_interval}s")
        while True:
            await asyncio.sleep(self.check_interval)
            await self._expire()

    async def _expire(self) -> None:
        now = datetime.utcnow()
        expired = [e for e in self.store.get_all() if e.date < now]
        for event in expired:
            # Remove from both in-memory stores
            self.store.remove(event.event_key)
            if self.dedup_store:
                self.dedup_store.remove(event.event_key)
            # Remove from both database tables
            if self.db:
                await self.db.delete_trusted_event(event.event_key)
                await self.db.delete_dedupped_event(event.event_key)
            logger.info(
                f"[EXPIRATION] Removed past event: '{event.name}' ({event.date.date()})"
            )
        if expired:
            logger.info(
                f"[EXPIRATION] {len(expired)} expired | store: {len(self.store)}"
            )
