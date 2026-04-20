"""
Stage 2 — LLM Reranker

Uses Claude (claude-haiku-4-5-20251001) to rerank the top-20 candidates from
Stage 1 and return relevance scores + reasoning for each event.

System prompt is cached with cache_control: ephemeral to reduce latency on
repeated calls across users in the same run.

Falls back gracefully to Stage 1 scores if:
  - anthropic package is not installed
  - API key is missing or invalid
  - The API call fails for any reason
"""
import asyncio
import json
import logging
from typing import List, Optional, Tuple

from core.models import TrustedEvent

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are an event relevance scorer for a personalized push-notification engine inside a banking app.

Given a user persona (derived from bank transaction history and chat activity) and a list of candidate events, score each event's relevance for that specific user on a scale of 0–100.

Scoring rubric:
  • Location match (user's city/country): up to 30 pts
  • Category & interest alignment: up to 40 pts
  • Price affordability relative to income segment: up to 20 pts
  • Event quality / uniqueness / excitement factor: up to 10 pts

Return ONLY a valid JSON array — no prose, no markdown fences.
Format: [{"index": 0, "score": 85, "reasoning": "one concise sentence"}, ...]
Preserve the original index values. Include every event."""


def _build_prompt(persona: dict, events: List[TrustedEvent]) -> str:
    bank = persona["bank_persona"]
    chat = persona.get("chat_persona") or {}

    lines = [
        "USER PERSONA:",
        f"  Income segment : {bank.get('income_segment')}",
        f"  Spending cats  : {', '.join(bank.get('spending_categories', []))}",
        f"  Location       : {bank.get('location')}, {bank.get('country')}",
        f"  Age range      : {bank.get('age_range')}",
    ]
    if chat:
        lines.append(f"  Chat interests : {', '.join(chat.get('interests', []))}")
        lines.append(f"  Chat keywords  : {', '.join(chat.get('keywords', []))}")

    lines.append("\nCANDIDATE EVENTS:")
    for i, ev in enumerate(events):
        price = (
            f"{ev.price_min}–{ev.price_max} {ev.currency}"
            if ev.price_min is not None
            else "price unknown"
        )
        lines.append(
            f"  [{i}] {ev.name} | {ev.location} | {ev.date.strftime('%Y-%m-%d %H:%M')} | "
            f"{ev.venue} | tags: {', '.join(ev.tags[:6])} | {price}"
        )

    return "\n".join(lines)


async def llm_rerank(
    persona: dict,
    candidates: List[Tuple[TrustedEvent, float]],
    api_key: Optional[str],
) -> List[Tuple[TrustedEvent, float, str]]:
    """
    Returns list of (event, score 0–100, reasoning) sorted by score descending.
    Scores are on a 0–100 scale (unlike Stage 1's raw points).
    """
    events = [ev for ev, _ in candidates]

    if not api_key:
        logger.warning("[LLM RERANKER] No API key — using Stage 1 scores as fallback")
        return [(ev, min(score, 100.0), "Stage 1 score (LLM unavailable)") for ev, score in candidates]

    try:
        import anthropic  # lazy import — optional dependency

        client = anthropic.AsyncAnthropic(api_key=api_key)
        user_prompt = _build_prompt(persona, events)

        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )

        raw = response.content[0].text.strip()
        # Strip accidental markdown fences
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1].lstrip("json").strip() if len(parts) > 1 else raw

        rankings = json.loads(raw)
        result = []
        for item in rankings:
            idx = item["index"]
            if 0 <= idx < len(events):
                result.append(
                    (events[idx], float(item["score"]), item.get("reasoning", ""))
                )

        result.sort(key=lambda x: x[1], reverse=True)
        logger.info(
            f"[LLM RERANKER] Reranked {len(result)} events for {persona['user_id']}"
        )
        return result

    except Exception as exc:
        logger.warning(f"[LLM RERANKER] Failed ({exc}) — falling back to Stage 1 scores")
        return [(ev, min(score, 100.0), f"Stage 1 score (LLM error: {exc})") for ev, score in candidates]
