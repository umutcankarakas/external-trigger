"""
Expiration Service: periodically scans the TrustedEventsStore and removes
events whose date has passed. Runs on a configurable interval (default 60 s).

Removes expired events from every place they're tracked, to keep everything
in sync:
  1. TrustedEventsStore  — in-memory fast lookup
  2. DedupStore          — in-memory dedup state (so expired events don't
                           block new events with the same key from being
                           created fresh if the event is re-announced)
  3. PostgreSQL          — dedupped_events, trusted_events, and the
                           event_llm_tags row (no orphaned tag rows left behind)
"""
import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Optional

from core.db import Database
import core.event_log as elog
from core.event_log import T_EXPIRATION
from services.dedup import DedupStore
from services.trust_score import TrustedEventsStore

logger = logging.getLogger(__name__)

_EXPIRATION_LOG_PATH = "config/expiration_log.json"
_EXPIRATION_LOG_MAX  = 500


def _append_expiration_log(entry: dict) -> None:
    try:
        records = json.load(open(_EXPIRATION_LOG_PATH, encoding="utf-8")) if os.path.exists(_EXPIRATION_LOG_PATH) else []
        records.append(entry)
        records = records[-_EXPIRATION_LOG_MAX:]
        with open(_EXPIRATION_LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
    except Exception:
        pass


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
            # Remove from all three database tables (no orphaned rows)
            if self.db:
                await self.db.delete_trusted_event(event.event_key)
                await self.db.delete_dedupped_event(event.event_key)
                await self.db.delete_event_tags(event.event_key)
            _append_expiration_log({
                "ts": now.isoformat(timespec="seconds"),
                "event_key": event.event_key,
                "name": event.name,
                "date": event.date.isoformat() if event.date else None,
                "location": event.location,
                "sources": list(event.sources),
                "trust_score": event.trust_score,
            })
            logger.info(
                f"[EXPIRATION] Removed past event: '{event.name}' ({event.date.date()})"
            )
            elog.info(
                T_EXPIRATION,
                f"Expired: '{event.name}' ({event.date.date() if event.date else '?'})",
                {"event_key": event.event_key, "name": event.name,
                 "date": event.date.isoformat() if event.date else None,
                 "location": event.location, "sources": list(event.sources)},
            )
        if expired:
            logger.info(
                f"[EXPIRATION] {len(expired)} expired | store: {len(self.store)}"
            )
