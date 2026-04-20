"""
Stage 1 — Hard Filter

Removes events that are clearly irrelevant before any scoring.
Currently enforces location: events must be in the same city or country
as the user. All other events are immediately discarded.
"""
import logging
from typing import List

from core.models import TrustedEvent

logger = logging.getLogger(__name__)


def hard_filter(events: List[TrustedEvent], persona: dict) -> List[TrustedEvent]:
    bank = persona["bank_persona"]
    user_city = bank.get("location", "").lower().strip()
    user_country = bank.get("country", "").lower().strip()

    passed = []
    for event in events:
        event_location = event.location.lower()
        event_city = event_location.split(",")[0].strip()

        location_ok = (
            user_city and user_city in event_location
            or event_city == user_city
            or user_country and user_country in event_location
        )

        if location_ok:
            passed.append(event)

    logger.debug(
        f"[HARD FILTER] {persona['user_id']}: "
        f"{len(passed)}/{len(events)} events passed"
    )
    return passed
