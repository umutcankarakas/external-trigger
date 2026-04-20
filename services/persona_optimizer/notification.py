"""
Notification Service

Decides whether a user should be notified about an event based on:
  1. Score threshold  — event must meet minimum relevance score
  2. Deduplication   — same (user, event) pair is never notified twice

Emits a Notification dataclass on success; returns None otherwise.
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from core.models import Notification, TrustedEvent

logger = logging.getLogger(__name__)

DEFAULT_THRESHOLD = 60.0


class NotificationService:
    def __init__(self, threshold: float = DEFAULT_THRESHOLD):
        self.threshold = threshold
        # (user_id, event_key) → datetime of first notification
        self._sent: Dict[Tuple[str, str], datetime] = {}
        self._log: List[Notification] = []

    def evaluate(
        self,
        user_id: str,
        event: TrustedEvent,
        score: float,
        reasoning: str,
    ) -> Optional[Notification]:
        key = (user_id, event.event_key)
        if key in self._sent:
            logger.debug(f"[NOTIFICATION] Already sent '{event.name}' to {user_id}")
            return None
        if score < self.threshold:
            logger.debug(
                f"[NOTIFICATION] Below threshold for {user_id}: "
                f"'{event.name}' score={score:.1f} < {self.threshold}"
            )
            return None

        notif = Notification(
            user_id=user_id,
            event_key=event.event_key,
            event_name=event.name,
            event_date=event.date.strftime("%a %d %b %Y, %H:%M"),
            event_location=event.location,
            relevance_score=score,
            reasoning=reasoning,
        )
        self._sent[key] = datetime.utcnow()
        self._log.append(notif)
        return notif

    def history(self) -> List[Notification]:
        return list(self._log)
