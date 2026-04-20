"""
Mem0 Persona Client

Manages dynamic user persona via Mem0 graph memory backed by Neo4j.
Replaces/augments the static chat_persona in personas.json with real memories
retrieved from the user's interaction history.

Config flags (from secrets.json / env):
  mem0_graph_url       — Neo4j bolt URL
  mem0_graph_username
  mem0_graph_password
  MEM0_FACT_TIMEOUT_S  — max seconds to wait for a memory retrieval
  MEM0_WRITE_TIMEOUT_S — max seconds to wait for a memory write
  MEM0_DEBUG_LONG_TERM_DECISION — if true, log every fact decision
"""
import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Seed memories written once at startup for the bank user (POC bootstrap).
# In production these would come from real user chat/transaction history.
SEED_MEMORIES = [
    "User loves jazz music and frequently buys tickets for jazz concerts",
    "User enjoys indie rock festivals and live outdoor events",
    "User is passionate about modern art exhibitions and gallery visits",
    "User watches football matches including Galatasaray and Champions League games",
    "User spends regularly on high-end dining and gastronomy experiences",
    "User frequently travels internationally for leisure",
    "User is interested in technology conferences and TED-style events",
    "User enjoys cultural activities like theater performances",
]

SEARCH_QUERIES = [
    "music concerts events interests",
    "sports activities preferences",
    "culture art hobbies",
    "dining travel lifestyle",
]


class Mem0PersonaClient:
    def __init__(
        self,
        openai_api_key: str,
        openai_model: str,
        graph_url: Optional[str] = None,
        graph_username: Optional[str] = None,
        graph_password: Optional[str] = None,
        bank_user_id: str = "user_default",
        fact_timeout: float = 5.0,
        write_timeout: float = 8.0,
        debug_decisions: bool = False,
    ):
        self.bank_user_id = bank_user_id
        self.fact_timeout = fact_timeout
        self.write_timeout = write_timeout
        self.debug_decisions = debug_decisions
        self._memory: Any = None
        self._available = False

        self._openai_api_key = openai_api_key
        self._openai_model = openai_model
        self._graph_url = graph_url
        self._graph_username = graph_username
        self._graph_password = graph_password

    def _build_config(self) -> dict:
        config: Dict[str, Any] = {
            "llm": {
                "provider": "openai",
                "config": {
                    "api_key": self._openai_api_key,
                    "model": self._openai_model,
                },
            },
        }
        if self._graph_url and self._graph_password:
            config["graph_store"] = {
                "provider": "neo4j",
                "config": {
                    "url": self._graph_url,
                    "username": self._graph_username or "neo4j",
                    "password": self._graph_password,
                },
            }
        return config

    async def initialize(self) -> None:
        """Initialize Mem0 and seed initial memories (idempotent)."""
        try:
            import os
            from mem0 import Memory  # lazy import

            # mem0ai internally creates an OpenAI client that checks OPENAI_API_KEY;
            # ensure it's set for the duration of this call.
            _prev = os.environ.get("OPENAI_API_KEY")
            os.environ["OPENAI_API_KEY"] = self._openai_api_key

            config = self._build_config()
            self._memory = await asyncio.to_thread(Memory.from_config, config)

            if _prev is None:
                os.environ.pop("OPENAI_API_KEY", None)
            else:
                os.environ["OPENAI_API_KEY"] = _prev
            self._available = True
            logger.info(
                f"[MEM0] Initialized for user '{self.bank_user_id}' "
                f"(graph={'enabled' if self._graph_url else 'disabled'})"
            )
            await self._seed_memories()
        except ImportError:
            logger.warning("[MEM0] mem0ai not installed — persona will use static fallback")
        except Exception as exc:
            logger.warning(f"[MEM0] Initialization failed ({exc}) — using static fallback")

    async def _seed_memories(self) -> None:
        """Write bootstrap memories if the user has none yet."""
        try:
            existing = await asyncio.wait_for(
                asyncio.to_thread(
                    self._memory.search,
                    "user preferences",
                    user_id=self.bank_user_id,
                    limit=1,
                ),
                timeout=self.fact_timeout,
            )
            if existing:
                logger.info(f"[MEM0] User '{self.bank_user_id}' already has memories, skipping seed")
                return
        except Exception:
            pass  # proceed to seed anyway

        logger.info(f"[MEM0] Seeding {len(SEED_MEMORIES)} memories for '{self.bank_user_id}'")
        for memory_text in SEED_MEMORIES:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(
                        self._memory.add,
                        memory_text,
                        user_id=self.bank_user_id,
                    ),
                    timeout=self.write_timeout,
                )
                if self.debug_decisions:
                    logger.debug(f"[MEM0] Wrote: {memory_text!r}")
            except asyncio.TimeoutError:
                logger.warning(f"[MEM0] Write timeout for: {memory_text!r}")
            except Exception as exc:
                logger.warning(f"[MEM0] Write failed ({exc}): {memory_text!r}")

    async def get_chat_persona(self) -> Dict[str, List[str]]:
        """
        Retrieve user memories and map them to the chat_persona schema:
          { "interests": [...], "keywords": [...] }
        Returns empty dict if Mem0 is unavailable.
        """
        if not self._available or self._memory is None:
            return {}

        all_memories: List[str] = []
        for query in SEARCH_QUERIES:
            try:
                results = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._memory.search,
                        query,
                        user_id=self.bank_user_id,
                        limit=10,
                    ),
                    timeout=self.fact_timeout,
                )
                for r in results:
                    mem = r.get("memory", "")
                    if mem and mem not in all_memories:
                        all_memories.append(mem)
                        if self.debug_decisions:
                            logger.debug(f"[MEM0] Retrieved fact: {mem!r}")
            except asyncio.TimeoutError:
                logger.warning(f"[MEM0] Retrieval timeout for query: {query!r}")
            except Exception as exc:
                logger.warning(f"[MEM0] Retrieval failed ({exc}) for: {query!r}")

        if not all_memories:
            return {}

        # Extract keywords from memory sentences (naive but effective for POC)
        interest_keywords = [
            "jazz", "indie rock", "classical", "pop", "rock", "electronic",
            "concert", "festival", "football", "basketball", "marathon",
            "art", "exhibition", "theater", "gallery", "food", "dining",
            "travel", "conference", "technology",
        ]
        found_interests = [kw for kw in interest_keywords if any(kw in m.lower() for m in all_memories)]
        found_keywords = [
            "live music", "match", "gallery", "ticket", "gastronomy",
            "culture", "outdoor", "stadium", "museum", "workshop",
        ]
        found_keywords = [kw for kw in found_keywords if any(kw in m.lower() for m in all_memories)]

        logger.info(
            f"[MEM0] Built chat persona for '{self.bank_user_id}': "
            f"{len(found_interests)} interests, {len(found_keywords)} keywords "
            f"from {len(all_memories)} memories"
        )
        return {"interests": found_interests, "keywords": found_keywords}

    async def write_memory(self, text: str) -> None:
        """Write a new fact to long-term memory."""
        if not self._available or self._memory is None:
            return
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self._memory.add, text, user_id=self.bank_user_id),
                timeout=self.write_timeout,
            )
            if self.debug_decisions:
                logger.debug(f"[MEM0] Long-term write: {text!r}")
        except asyncio.TimeoutError:
            logger.warning(f"[MEM0] Write timeout: {text!r}")
        except Exception as exc:
            logger.warning(f"[MEM0] Write failed ({exc}): {text!r}")
