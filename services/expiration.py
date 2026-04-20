"""
Expiration Service: periodically scans the TrustedEventsStore and removes
events whose date has passed. Runs on a configurable interval (default 60 s).
"""
import asyncio
import logging
from datetime import datetime

from services.trust_score import TrustedEventsStore

logger = logging.getLogger(__name__)


class ExpirationService:
    def __init__(self, store: TrustedEventsStore, check_interval: int = 60):
        self.store = store
        self.check_interval = check_interval

    async def start(self) -> None:
        logger.info(f"[EXPIRATION] Started — checking every {self.check_interval}s")
        while True:
            await asyncio.sleep(self.check_interval)
            self._expire()

    def _expire(self) -> None:
        now = datetime.utcnow()
        expired = [e for e in self.store.get_all() if e.date < now]
        for event in expired:
            self.store.remove(event.event_key)
            logger.info(
                f"[EXPIRATION] Removed past event: '{event.name}' ({event.date.date()})"
            )
        logger.debug(
            f"[EXPIRATION] Store size after cleanup: {len(self.store)} events"
        )
