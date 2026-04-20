"""
Stage 1 — Bank Persona Scoring

Scores each event against the user's bank persona (spending categories,
income segment). Returns the top-50 candidates sorted by score descending.

Score breakdown (max 100):
  • Spending category alignment: up to 60 pts
  • Price affordability vs income segment: up to 20 pts
  • Trust score carry-over: up to 20 pts
"""
import logging
from typing import List, Tuple

from core.models import TrustedEvent

logger = logging.getLogger(__name__)

# Maps bank spending categories → event tag/category keywords
CATEGORY_KEYWORDS: dict = {
    "entertainment": ["concert", "music", "festival", "jazz", "rock", "pop",
                      "indie", "electronic", "classical", "live music"],
    "sports":        ["sports", "football", "basketball", "running", "marathon",
                      "live sports", "derby", "euroleague", "champions league"],
    "dining":        ["food", "dining", "gastronomy", "restaurant", "taste"],
    "culture":       ["culture", "art", "theater", "exhibition", "photography",
                      "conference", "ted", "drama", "orchestra", "symphony"],
    "travel":        ["international", "stadium", "festival", "outdoor"],
    "family":        ["festival", "outdoor", "culture", "food"],
    "education":     ["conference", "ted", "workshop", "innovation", "ideas"],
}

# Maximum TRY price considered affordable per income segment
INCOME_PRICE_CEILING: dict = {
    "high":   float("inf"),
    "medium": 2_000.0,
    "low":    500.0,
}


def _spending_score(event: TrustedEvent, spending_categories: List[str]) -> float:
    event_signals = {t.lower() for t in event.tags + event.category}
    score = 0.0
    for cat in spending_categories:
        for kw in CATEGORY_KEYWORDS.get(cat, []):
            if kw in event_signals:
                score += 10.0
    return min(score, 60.0)


def _price_score(event: TrustedEvent, income_segment: str) -> float:
    ceiling = INCOME_PRICE_CEILING.get(income_segment, 1_000.0)
    if event.price_max is None:
        return 20.0                  # unknown price — benefit of the doubt
    if event.price_max <= ceiling:
        return 20.0
    if event.price_min is not None and event.price_min <= ceiling:
        return 10.0                  # partially affordable
    return 0.0


def bank_persona_score(
    events: List[TrustedEvent],
    persona: dict,
) -> List[Tuple[TrustedEvent, float]]:
    bank = persona["bank_persona"]
    spending = bank.get("spending_categories", [])
    income = bank.get("income_segment", "medium")

    scored = [
        (event, round(
            _spending_score(event, spending)
            + _price_score(event, income)
            + event.trust_score * 20,
            2,
        ))
        for event in events
    ]
    scored.sort(key=lambda x: x[1], reverse=True)
    top_50 = scored[:50]

    logger.debug(
        f"[BANK SCORING] {persona['user_id']}: "
        f"top score={top_50[0][1] if top_50 else 0}"
    )
    return top_50
