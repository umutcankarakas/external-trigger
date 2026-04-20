"""
Stage 1 — Chat Persona / Qdrant Boost

POC stand-in for vector-similarity search against the user's chat persona
stored in Qdrant. Uses keyword overlap between event tags and the user's
declared interests/keywords to compute a boost score.

Each matching interest/keyword adds +8 pts to the existing bank score.
Result is the top-20 candidates sorted by boosted score.
"""
import logging
from typing import List, Tuple

from core.models import TrustedEvent

logger = logging.getLogger(__name__)

BOOST_PER_MATCH = 8.0


def chat_persona_boost(
    candidates: List[Tuple[TrustedEvent, float]],
    chat_persona: dict,
) -> List[Tuple[TrustedEvent, float]]:
    interests = {i.lower() for i in chat_persona.get("interests", [])}
    keywords = {k.lower() for k in chat_persona.get("keywords", [])}
    signal = interests | keywords

    boosted = []
    for event, score in candidates:
        event_signal = {t.lower() for t in event.tags + event.category}
        overlap = len(event_signal & signal)
        boosted.append((event, round(score + overlap * BOOST_PER_MATCH, 2)))

    boosted.sort(key=lambda x: x[1], reverse=True)
    top_20 = boosted[:20]

    if top_20:
        logger.debug(
            f"[CHAT BOOST] Applied boost — top score after boost: {top_20[0][1]}"
        )
    return top_20
