"""
Persona Optimizer — orchestrates Stage 1 and Stage 2 per user persona.

Stage 1 (pre-filtering):
  Hard Filter
    → Bank Persona Scoring (Top 50)
    → [Router pre-screen via qwen2.5:7b — drops obviously irrelevant events]
    → Chat Persona Boost — Mem0 dynamic memories or static fallback (Top 20)

Stage 2 (LLM reranking) — tried in order, first success wins:
  1. Ollama  (qwen2.5:7b local — free, private)
  2. OpenAI  (gpt-4o-mini cloud — fallback)
  3. Claude  (haiku — fallback)
  4. Stage 1 raw scores (last resort)

Notification:
  NotificationService enforces score threshold + per-(user, event) dedup,
  then fires the on_notification callback.

The optimizer runs every `run_interval` seconds against a full snapshot of
the TrustedEventsStore. Mem0 chat persona is refreshed each cycle.
"""
import asyncio
import json
import logging
from typing import Callable, List, Optional

from core.models import Notification, TrustedEvent
from services.trust_score import TrustedEventsStore
from services.persona_optimizer.hard_filter import hard_filter
from services.persona_optimizer.bank_scoring import bank_persona_score
from services.persona_optimizer.chat_boost import chat_persona_boost
from services.persona_optimizer.ollama_reranker import ollama_rerank
from services.persona_optimizer.openai_reranker import openai_rerank
from services.persona_optimizer.llm_reranker import llm_rerank as claude_rerank
from services.persona_optimizer.notification import NotificationService

logger = logging.getLogger(__name__)


class PersonaOptimizer:
    def __init__(
        self,
        store: TrustedEventsStore,
        personas_path: str = "config/personas.json",
        # Ollama (primary Stage 2 reranker — local)
        ollama_base_url: str = "http://localhost:11434/v1",
        ollama_api_key: str = "ollama",
        ollama_model: str = "qwen2.5:7b",
        # OpenAI (Stage 2 fallback — cloud)
        openai_api_key: Optional[str] = None,
        openai_model: str = "gpt-4o-mini",
        openai_max_tokens: int = 2048,
        # Claude (Stage 2 last-resort fallback)
        anthropic_api_key: Optional[str] = None,
        # Router (qwen2.5 via Ollama — Stage 1 pre-screen)
        router=None,
        # Mem0 persona client
        mem0_client=None,
        # Bank user ID (maps Mem0 memories to one of the personas)
        bank_user_id: Optional[str] = None,
        run_interval: int = 30,
    ):
        self.store = store
        self.ollama_base_url = ollama_base_url
        self.ollama_api_key = ollama_api_key
        self.ollama_model = ollama_model
        self.openai_api_key = openai_api_key
        self.openai_model = openai_model
        self.openai_max_tokens = openai_max_tokens
        self.anthropic_api_key = anthropic_api_key
        self.router = router
        self.mem0_client = mem0_client
        self.bank_user_id = bank_user_id
        self.run_interval = run_interval
        self.notifier = NotificationService()
        self.on_notification: Optional[Callable[[Notification], None]] = None

        with open(personas_path) as f:
            self.personas: list = json.load(f)

    async def start(self) -> None:
        logger.info(
            f"[PERSONA OPTIMIZER] Started — "
            f"interval={self.run_interval}s, "
            f"personas={len(self.personas)}, "
            f"reranker=ollama→{'openai' if self.openai_api_key else 'claude' if self.anthropic_api_key else 'stage1'}, "
            f"router={'qwen2.5' if self.router else 'disabled'}, "
            f"mem0={'enabled' if self.mem0_client else 'disabled'}"
        )

        # Initialize Mem0 before the first cycle
        if self.mem0_client:
            await self.mem0_client.initialize()

        while True:
            await asyncio.sleep(self.run_interval)
            events = self.store.get_all()
            if not events:
                logger.debug("[PERSONA OPTIMIZER] Store empty, skipping this cycle")
                continue

            # Fetch Mem0 chat persona once per cycle (shared across personas if
            # they all map to the same bank user)
            mem0_chat_persona: Optional[dict] = None
            if self.mem0_client:
                mem0_chat_persona = await self.mem0_client.get_chat_persona()

            logger.info(
                f"[PERSONA OPTIMIZER] Cycle — {len(events)} trusted events, "
                f"Mem0 persona: {'yes' if mem0_chat_persona else 'no'}"
            )

            for persona in self.personas:
                await self._run_for_persona(persona, events, mem0_chat_persona)

    async def _run_for_persona(
        self,
        persona: dict,
        events: List[TrustedEvent],
        mem0_chat_persona: Optional[dict],
    ) -> None:
        user_id = persona["user_id"]

        # ── Build effective persona (Mem0 overrides static chat_persona) ─────
        effective_persona = dict(persona)
        if mem0_chat_persona and self.bank_user_id and user_id == self.bank_user_id:
            effective_persona = dict(persona)
            effective_persona["chat_persona"] = mem0_chat_persona
            logger.debug(f"[PERSONA OPTIMIZER] {user_id}: using Mem0 chat persona")
        # For other users, fall through to static chat_persona from JSON

        # ── Stage 1 — Hard filter ────────────────────────────────────────────
        filtered = hard_filter(events, effective_persona)
        if not filtered:
            logger.info(f"[PERSONA OPTIMIZER] {user_id}: 0 events passed hard filter")
            return

        # ── Stage 1 — Bank persona scoring ───────────────────────────────────
        scored = bank_persona_score(filtered, effective_persona)

        # ── Stage 1 — Router pre-screen (qwen2.5:7b) ─────────────────────────
        if self.router:
            scored = await self.router.filter_candidates(effective_persona, scored)

        # ── Stage 1 — Chat persona boost ─────────────────────────────────────
        chat = effective_persona.get("chat_persona")
        top_20 = chat_persona_boost(scored, chat) if chat else scored[:20]

        logger.info(
            f"[PERSONA OPTIMIZER] {user_id}: "
            f"{len(filtered)} hard-filtered → {len(scored)} bank-scored → "
            f"{len(top_20)} into Stage 2"
        )

        # ── Stage 2 — LLM Reranker (Ollama → OpenAI → Claude → Stage 1) ────────
        reranked = await ollama_rerank(
            effective_persona, top_20,
            base_url=self.ollama_base_url,
            api_key=self.ollama_api_key,
            model=self.ollama_model,
        )

        if not reranked:
            logger.info(f"[PERSONA OPTIMIZER] {user_id}: Ollama failed, trying OpenAI")
            if self.openai_api_key:
                reranked = await openai_rerank(
                    effective_persona, top_20,
                    self.openai_api_key, self.openai_model, self.openai_max_tokens,
                )

        if not reranked:
            logger.info(f"[PERSONA OPTIMIZER] {user_id}: OpenAI failed, trying Claude")
            reranked = await claude_rerank(effective_persona, top_20, self.anthropic_api_key)

        if not reranked:
            # Last resort: use Stage 1 scores directly
            logger.warning(f"[PERSONA OPTIMIZER] {user_id}: all rerankers failed, using Stage 1 scores")
            reranked = [(ev, min(sc, 100.0), "Stage 1 score") for ev, sc in top_20]

        if not reranked:
            return

        top_event, score, reasoning = reranked[0]

        # ── Notification ──────────────────────────────────────────────────────
        notif = self.notifier.evaluate(user_id, top_event, score, reasoning)
        if notif:
            # Write the notification decision back to Mem0 as a long-term memory
            if self.mem0_client and user_id == self.bank_user_id:
                await self.mem0_client.write_memory(
                    f"Was notified about '{top_event.name}' event on {top_event.date.strftime('%Y-%m-%d')} "
                    f"with relevance score {score:.1f}/100"
                )
            if self.on_notification:
                self.on_notification(notif)
