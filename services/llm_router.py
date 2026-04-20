"""
LLM Router — qwen2.5:7b via Ollama (OpenAI-compatible API)

Performs a fast, local relevance pre-screen before sending candidates to the
expensive OpenAI reranker. Sends the full candidate list in one batch call
and returns the set of indices worth reranking.

If Ollama is unreachable (or the model is slow), all candidates pass through
so downstream scoring is never blocked.

Env / secrets:
  routing_model    — default qwen2.5:7b
  routing_base_url — default http://localhost:11434/v1
  routing_api_key  — default "ollama"
"""
import asyncio
import json
import logging
from typing import List, Optional, Set, Tuple

from core.models import TrustedEvent

logger = logging.getLogger(__name__)

_ROUTER_TIMEOUT = 15.0   # seconds — local model, should be fast

_SYSTEM_PROMPT = (
    "You are a fast event relevance pre-screener. "
    "Given a user profile and a list of events, return a JSON object "
    '{"keep": [0, 2, 5, ...]} listing the indices of events that are '
    "potentially relevant to this user. Be inclusive — only exclude events "
    "that are obviously irrelevant (wrong location, wrong category, price "
    "completely out of reach). Return ONLY the JSON object, no other text."
)


def _build_routing_prompt(persona: dict, events: List[TrustedEvent]) -> str:
    bank = persona["bank_persona"]
    chat = persona.get("chat_persona") or {}
    lines = [
        f"User: {bank.get('location')}, {bank.get('income_segment')} income, "
        f"likes {', '.join(bank.get('spending_categories', []))}",
    ]
    if chat.get("interests"):
        lines.append(f"Chat interests: {', '.join(chat['interests'][:5])}")
    lines.append("Events:")
    for i, ev in enumerate(events):
        lines.append(
            f"  [{i}] {ev.name} | {ev.location} | "
            f"tags: {', '.join(ev.tags[:4])}"
        )
    return "\n".join(lines)


class LLMRouter:
    def __init__(
        self,
        model: str = "qwen2.5:7b",
        base_url: str = "http://localhost:11434/v1",
        api_key: str = "ollama",
    ):
        self.model = model
        self.base_url = base_url
        self.api_key = api_key
        self._client = None
        self._available: Optional[bool] = None   # None = not checked yet

    def _get_client(self):
        if self._client is None:
            from openai import AsyncOpenAI
            self._client = AsyncOpenAI(base_url=self.base_url, api_key=self.api_key)
        return self._client

    async def _check_available(self) -> bool:
        """One-time connectivity probe to Ollama."""
        if self._available is not None:
            return self._available
        try:
            client = self._get_client()
            await asyncio.wait_for(
                client.models.list(),
                timeout=5.0,
            )
            logger.info(f"[ROUTER] Ollama reachable — using {self.model}")
            self._available = True
        except Exception as exc:
            logger.warning(f"[ROUTER] Ollama not reachable ({exc}) — pre-screen disabled")
            self._available = False
        return self._available

    async def filter_candidates(
        self,
        persona: dict,
        candidates: List[Tuple[TrustedEvent, float]],
    ) -> List[Tuple[TrustedEvent, float]]:
        """
        Returns a filtered subset of candidates that the router deems relevant.
        Falls back to passing all candidates through if the router is unavailable.
        """
        if not candidates:
            return candidates

        if not await self._check_available():
            return candidates

        events = [ev for ev, _ in candidates]
        prompt = _build_routing_prompt(persona, events)

        try:
            client = self._get_client()
            response = await asyncio.wait_for(
                client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": _SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    max_tokens=256,
                    temperature=0.0,
                ),
                timeout=_ROUTER_TIMEOUT,
            )

            raw = response.choices[0].message.content.strip()
            # Strip accidental markdown fences
            if raw.startswith("```"):
                raw = raw.split("```")[1].lstrip("json").strip()

            data = json.loads(raw)
            keep_indices: Set[int] = set(data.get("keep", range(len(candidates))))

            filtered = [
                (ev, score)
                for i, (ev, score) in enumerate(candidates)
                if i in keep_indices
            ]
            logger.info(
                f"[ROUTER] {persona['user_id']}: "
                f"{len(filtered)}/{len(candidates)} candidates passed pre-screen"
            )
            return filtered or candidates   # never return empty — keep all if router blocks everything

        except asyncio.TimeoutError:
            logger.warning(f"[ROUTER] Timeout for {persona['user_id']} — passing all through")
            return candidates
        except Exception as exc:
            logger.warning(f"[ROUTER] Error ({exc}) — passing all through")
            return candidates
