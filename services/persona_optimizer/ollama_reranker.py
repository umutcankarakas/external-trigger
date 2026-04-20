"""
Stage 2 — Ollama Reranker (qwen2.5:7b via local Ollama)

Uses the OpenAI-compatible Ollama endpoint for reranking.
qwen2.5:7b does not support response_format:json_object, so JSON is
extracted from the raw text with a multi-pass parser.

Priority in optimizer: Ollama → OpenAI → Stage 1 scores.
"""
import asyncio
import json
import logging
import re
from typing import List, Optional, Tuple

from core.models import TrustedEvent

logger = logging.getLogger(__name__)

_TIMEOUT = 60.0   # local model can be slow on first token

_SYSTEM_PROMPT = """\
You are an event relevance scorer for a personalized push-notification engine.

Given a user persona and candidate events, score each event 0–100 for relevance.

Rubric:
  location match          → up to 30 pts
  category/interest match → up to 40 pts
  price vs income         → up to 20 pts
  quality / uniqueness    → up to 10 pts

You MUST return ONLY a JSON array. No prose, no markdown fences, no explanation.
Example output (follow this exact format):
[{"index":0,"score":85,"reasoning":"Short reason"},{"index":1,"score":40,"reasoning":"Short reason"}]"""


def _build_prompt(persona: dict, events: List[TrustedEvent]) -> str:
    bank = persona["bank_persona"]
    chat = persona.get("chat_persona") or {}

    lines = [
        "USER:",
        f"  location={bank.get('location')}, {bank.get('country')}",
        f"  income={bank.get('income_segment')}",
        f"  spending={', '.join(bank.get('spending_categories', []))}",
    ]
    if chat.get("interests"):
        lines.append(f"  interests={', '.join(chat['interests'][:6])}")
    if chat.get("keywords"):
        lines.append(f"  keywords={', '.join(chat['keywords'][:6])}")

    lines.append("EVENTS:")
    for i, ev in enumerate(events):
        price = (
            f"{ev.price_min}-{ev.price_max} {ev.currency}"
            if ev.price_min is not None else "price unknown"
        )
        lines.append(
            f"[{i}] {ev.name} | {ev.location} | "
            f"{ev.date.strftime('%Y-%m-%d')} | "
            f"tags:{','.join(ev.tags[:5])} | {price}"
        )
    lines.append("\nReturn the JSON array now:")
    return "\n".join(lines)


def _extract_json(text: str) -> Optional[list]:
    """Multi-pass JSON extraction from model output."""
    text = text.strip()

    # Pass 1: try parsing the whole thing
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
        if isinstance(result, dict) and "rankings" in result:
            return result["rankings"]
    except json.JSONDecodeError:
        pass

    # Pass 2: strip markdown fences
    fenced = re.sub(r"```(?:json)?", "", text).strip()
    try:
        result = json.loads(fenced)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass

    # Pass 3: find first [...] block
    match = re.search(r"\[.*?\]", text, re.DOTALL)
    if match:
        try:
            result = json.loads(match.group(0))
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

    return None


async def ollama_rerank(
    persona: dict,
    candidates: List[Tuple[TrustedEvent, float]],
    base_url: str = "http://localhost:11434/v1",
    api_key: str = "ollama",
    model: str = "qwen2.5:7b",
) -> List[Tuple[TrustedEvent, float, str]]:
    """
    Returns [(event, score 0-100, reasoning), ...] sorted by score desc.
    Returns empty list on failure so optimizer can fall back to OpenAI.
    """
    if not candidates:
        return []

    events = [ev for ev, _ in candidates]

    try:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(base_url=base_url, api_key=api_key)
        prompt = _build_prompt(persona, events)

        response = await asyncio.wait_for(
            client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=1024,
                temperature=0.0,
            ),
            timeout=_TIMEOUT,
        )

        raw = response.choices[0].message.content or ""
        rankings = _extract_json(raw)

        if not rankings:
            logger.warning(
                f"[OLLAMA RERANKER] Could not parse JSON from response for "
                f"{persona['user_id']} — raw: {raw[:200]!r}"
            )
            return []

        result = []
        for item in rankings:
            idx = item.get("index", -1)
            if 0 <= idx < len(events):
                result.append(
                    (events[idx], float(item.get("score", 0)), item.get("reasoning", ""))
                )

        result.sort(key=lambda x: x[1], reverse=True)
        logger.info(
            f"[OLLAMA RERANKER] {persona['user_id']}: ranked {len(result)} events "
            f"(model={model})"
        )
        return result

    except asyncio.TimeoutError:
        logger.warning(
            f"[OLLAMA RERANKER] Timeout ({_TIMEOUT}s) for {persona['user_id']}"
        )
        return []
    except Exception as exc:
        logger.warning(f"[OLLAMA RERANKER] Failed ({exc})")
        return []
