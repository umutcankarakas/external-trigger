"""
Stage 2 — OpenAI Reranker (gpt-4o-mini)

Reranks the top-20 candidates from Stage 1 using OpenAI's chat completions API.
Returns a scored, ranked list with one-line reasoning per event.

Respects the configured token and iteration limits from secrets.json:
  openai_max_completion_tokens
  openai_max_tool_iterations   (stored for future agent layer)
  openai_max_function_calls    (stored for future agent layer)

Falls back to Stage 1 scores if the API call fails.
"""
import asyncio
import json
import logging
from typing import List, Optional, Tuple

from core.models import TrustedEvent

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are an event relevance scorer for a personalized push-notification engine inside a banking super-app.

Given a user persona (derived from bank transaction data and chat history) and a list of candidate events, score each event's relevance for that specific user on a scale of 0–100.

Scoring rubric:
  • Location match (user's city/country): up to 30 pts
  • Category & interest alignment: up to 40 pts
  • Price affordability relative to income segment: up to 20 pts
  • Event quality / uniqueness / excitement factor: up to 10 pts

Return ONLY a valid JSON object in this exact format — no prose, no markdown:
{"rankings": [{"index": 0, "score": 85, "reasoning": "one concise sentence"}, ...]}

Include every event. Preserve original index values."""


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
    if chat.get("interests"):
        lines.append(f"  Interests      : {', '.join(chat['interests'])}")
    if chat.get("keywords"):
        lines.append(f"  Keywords       : {', '.join(chat['keywords'])}")

    lines.append("\nCANDIDATE EVENTS:")
    for i, ev in enumerate(events):
        price = (
            f"{ev.price_min}–{ev.price_max} {ev.currency}"
            if ev.price_min is not None else "price unknown"
        )
        lines.append(
            f"  [{i}] {ev.name} | {ev.location} | "
            f"{ev.date.strftime('%Y-%m-%d %H:%M')} | "
            f"{ev.venue} | tags: {', '.join(ev.tags[:6])} | {price}"
        )

    return "\n".join(lines)


async def openai_rerank(
    persona: dict,
    candidates: List[Tuple[TrustedEvent, float]],
    api_key: Optional[str],
    model: str = "gpt-4o-mini",
    max_tokens: int = 2048,
) -> List[Tuple[TrustedEvent, float, str]]:
    """
    Returns [(event, score 0–100, reasoning), ...] sorted by score descending.
    Falls back to Stage 1 scores if OpenAI is unavailable.
    """
    events = [ev for ev, _ in candidates]

    if not api_key:
        logger.warning("[OPENAI RERANKER] No API key — using Stage 1 scores")
        return [(ev, min(sc, 100.0), "Stage 1 score (OpenAI unavailable)") for ev, sc in candidates]

    try:
        from openai import AsyncOpenAI  # lazy import

        client = AsyncOpenAI(api_key=api_key)
        prompt = _build_prompt(persona, events)

        response = await asyncio.wait_for(
            client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=max_tokens,
                temperature=0.2,
                response_format={"type": "json_object"},
            ),
            timeout=30.0,
        )

        raw = response.choices[0].message.content.strip()
        data = json.loads(raw)
        rankings = data.get("rankings", [])

        result = []
        for item in rankings:
            idx = item.get("index", -1)
            if 0 <= idx < len(events):
                result.append(
                    (events[idx], float(item["score"]), item.get("reasoning", ""))
                )

        result.sort(key=lambda x: x[1], reverse=True)
        logger.info(
            f"[OPENAI RERANKER] Reranked {len(result)} events for {persona['user_id']} "
            f"(model={model}, tokens_used={response.usage.total_tokens})"
        )
        return result

    except asyncio.TimeoutError:
        logger.warning(f"[OPENAI RERANKER] Timeout for {persona['user_id']} — Stage 1 fallback")
        return [(ev, min(sc, 100.0), "Stage 1 score (OpenAI timeout)") for ev, sc in candidates]
    except Exception as exc:
        logger.warning(f"[OPENAI RERANKER] Failed ({exc}) — Stage 1 fallback")
        return [(ev, min(sc, 100.0), f"Stage 1 score (error: {exc})") for ev, sc in candidates]
