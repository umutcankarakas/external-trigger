"""
Database Writer: persists dedupped and trusted events to PostgreSQL.

Subscribes to two queues:
  dedupped_events → upserts into the dedupped_events table so DedupStore
                    state survives restarts and new sources correctly merge
                    with existing events.
  trusted_events  → upserts into the trusted_events table, the final
                    queryable and updatable output of the pipeline.

Both writes are idempotent (ON CONFLICT DO UPDATE) so replaying events
or recalculating scores never creates duplicates.
"""
import asyncio
import logging

from core.bus import MessageBus
from core.db import Database
from core.models import BatchComplete, DeduppedEvent, TrustedEvent

logger = logging.getLogger(__name__)


class DatabaseWriter:
    def __init__(self, bus: MessageBus, db: Database) -> None:
        self.db = db
        self._dedup_inbox = bus.subscribe("dedupped_events")
        self._trust_inbox = bus.subscribe("trusted_events")

    async def _flush_loop(self) -> None:
        """Logs a write summary every 2 s instead of one line per event."""
        while True:
            await asyncio.sleep(2.0)
            if self._dedup_count or self._trust_count:
                logger.info(
                    f"[DB] {self._dedup_count} dedupped, {self._trust_count} trusted upserted"
                )
                self._dedup_count = self._trust_count = 0

    async def _process_dedupped(self) -> None:
        """Consume dedupped_events queue and write to dedupped_events table."""
        while True:
            message = await self._dedup_inbox.get()
            if isinstance(message, BatchComplete):
                continue
            dedupped: DeduppedEvent = message
            await self.db.upsert_dedupped_event(dedupped)
            self._dedup_count += 1

    async def _process_trusted(self) -> None:
        """Consume trusted_events queue and write to trusted_events table."""
        while True:
            message = await self._trust_inbox.get()
            if isinstance(message, BatchComplete):
                continue
            trusted: TrustedEvent = message
            await self.db.upsert_trusted_event(trusted)
            self._trust_count += 1

    async def start(self) -> None:
        logger.info("[DB] Writer started")
        self._dedup_count = self._trust_count = 0
        asyncio.create_task(self._flush_loop())
        # Run both consumers concurrently
        await asyncio.gather(
            self._process_dedupped(),
            self._process_trusted(),
        )
