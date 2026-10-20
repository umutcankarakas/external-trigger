"""
TrustedEventsStore: in-memory store of the final pipeline output.

Trust scoring has been removed — all deduped events are accepted as-is.
The store is kept as a thin wrapper used by ExpirationService.
"""
from datetime import datetime
from typing import Dict, List, Optional

from core.models import TrustedEvent


class TrustedEventsStore:
    def __init__(self):
        self._events: Dict[str, TrustedEvent] = {}

    def upsert(self, event: TrustedEvent) -> None:
        self._events[event.event_key] = event

    def get(self, event_key: str) -> Optional[TrustedEvent]:
        return self._events.get(event_key)

    def remove(self, event_key: str) -> None:
        self._events.pop(event_key, None)

    def get_all(self) -> List[TrustedEvent]:
        return list(self._events.values())

    def __len__(self) -> int:
        return len(self._events)
