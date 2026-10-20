"""
LLM tagging service — assigns controlled-vocabulary tags to events.

Runs inside the pipeline process (main.py) in two phases:

  Phase 1  bulk pass    — after the first fetch cycle drains to the DB, tag every
                          untagged event. The fetcher is paused until this finishes
                          (main.py gates it on `bulk_done`).
  Phase 2  steady state — every `watch_interval` seconds, tag any newly-arrived
                          untagged events. Usually a handful per cycle, often zero.

The expensive bulk pass happens once. event_llm_tags is keyed by the post-dedup
event_key, so each real-world event is tagged exactly once no matter how many
sources reported it — and the LLM sees the merged tags/category from all sources.

Ollama calls are blocking, so they run in a thread executor: the pipeline's
event loop (expiration, etc.) keeps ticking while a batch is being tagged.

The module-level helpers (load_vocab / build_prompt / call_ollama) are also
imported by the standalone tag_events.py CLI so there is one tagging
implementation, not two.
"""
import asyncio
import json
import logging

import requests

logger = logging.getLogger(__name__)

_OLLAMA_URL  = "http://localhost:11434/v1/chat/completions"
_OLLAMA_TAGS = "http://localhost:11434/api/tags"
_VOCAB_PATH  = "config/event_vocabulary.json"

_DEFAULT_MODEL = "qwen2.5:7b"


# ── shared helpers (also used by tag_events.py) ──────────────────────────────

def load_vocab() -> tuple:
    """Return (ordered slug list, slug set) from the controlled vocabulary file."""
    with open(_VOCAB_PATH, encoding="utf-8") as f:
        data = json.load(f)
    slugs = [t["slug"] for g in data["groups"] for t in g["tags"]]
    return slugs, set(slugs)


def build_prompt(events: list, vocab_slugs: list) -> str:
    """Build the classification prompt for one batch of events."""
    vocab_str = ", ".join(vocab_slugs)
    lines = []
    for i, e in enumerate(events):
        cat  = ", ".join(e.category or [])
        tags = ", ".join(e.tags or [])
        lines.append(
            f'{i}. name="{e.name}" | category="{cat}" | tags="{tags}" | venue="{e.venue or ""}"'
        )
    events_str = "\n".join(lines)
    return f"""You are an event classifier for a Turkish events platform. Many events are in Turkish.

VOCABULARY — you may ONLY use these exact slugs, nothing else:
{vocab_str}

RULES:
- Assign 1 to 4 slugs per event. Only slugs you are confident about — fewer is better than wrong.
- The `category` and `tags` fields are STRONG hints. Trust them:
    "tiyatro" / "Theatre" / "Arts & Theatre" / "performing-arts" -> theatre
    "muze" -> museum ;  "atolye" -> workshop ;  "sergi" -> exhibition
    "Football" / "futbol" -> football ;  "Basketball" / "basketbol" -> basketball
    "Music" / "concerts" -> concert PLUS one music genre
- "concert" is ONLY for live music performances. A theatre play, musical, exhibition,
  workshop, sports match, or museum show is NEVER a concert — do not add "concert" to them.
- Music genre slugs (pop, rock, jazz, classical, turkish_art_music, ...) apply ONLY to
  music events. A painting/art workshop is "workshop", NOT "turkish_art_music".
- If the event type is unclear, assign FEWER tags. Never pad with a guessed genre.

EXAMPLES:
  name="Satıcının Ölümü" -> ["theatre"]            (a stage play)
  name="7 Kocalı Hürmüz" category="Arts & Theatre" -> ["theatre","musical"]
  name="Türk Çini Resim Sanatı Atölyesi" -> ["workshop"]
  name="Piramitlerin Gizemi" tags="muze" -> ["museum","exhibition"]
  name="Galatasaray - Fenerbahçe" -> ["football"]
  name="Sezen Aksu Konseri" -> ["concert","turkish_pop"]
  name="Deborah De Luca" venue="nightclub" -> ["concert","electronic","nightlife"]

Output ONLY a JSON object mapping each event number (as a string) to its slug list:
{{"0": ["theatre"], "1": ["concert","rock"]}}

EVENTS:
{events_str}
"""


def call_ollama(prompt: str, model: str) -> dict:
    """Send one prompt to Ollama's OpenAI-compatible endpoint; return parsed JSON."""
    resp = requests.post(
        _OLLAMA_URL,
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        },
        timeout=300,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]
    return json.loads(content)


def ollama_reachable() -> bool:
    try:
        requests.get(_OLLAMA_TAGS, timeout=5).raise_for_status()
        return True
    except Exception:
        return False


# ── service ──────────────────────────────────────────────────────────────────

class TaggingService:
    """Runs the bulk pass then the steady-state incremental tagging loop."""

    def __init__(self, db, model: str = _DEFAULT_MODEL,
                 batch_size: int = 10, watch_interval: int = 600):
        self.db = db
        self.model = model
        self.batch_size = batch_size
        self.watch_interval = watch_interval
        self.vocab_slugs, self.vocab_set = load_vocab()
        # Set once the bulk pass finishes — main.py gates the fetcher on this so
        # no new events are pulled until the initial corpus is fully tagged.
        self.bulk_done: asyncio.Event = asyncio.Event()

    async def run(self, first_pull_issued: asyncio.Event) -> None:
        """Phase 1 (bulk) then Phase 2 (steady state). Never returns normally."""
        logger.info("[TAGGER] Started — waiting for the initial fetch cycle")
        try:
            await first_pull_issued.wait()
            await self._await_corpus_stable()
            await self._bulk_pass()
        finally:
            # Always release the fetcher, even if tagging failed (e.g. Ollama
            # down) — the pipeline must never deadlock on the tagger.
            self.bulk_done.set()
        await self._steady_state()

    async def _await_corpus_stable(self) -> None:
        """Wait until trusted_events count stops growing — the first fetch cycle
        has fully drained through Normalizer -> Dedup -> DB Writer."""
        prev, stable = -1, 0
        while stable < 3:
            await asyncio.sleep(8)
            count = await self.db.count_trusted_events()
            if count == prev:
                stable += 1
            else:
                prev, stable = count, 0
        logger.info(f"[TAGGER] corpus stable at {prev} events")

    async def _bulk_pass(self) -> None:
        events = await self.db.get_untagged_trusted_events()
        if not events:
            logger.info("[TAGGER] bulk pass — nothing untagged, skipping")
            return
        if not ollama_reachable():
            logger.warning(
                "[TAGGER] bulk pass — Ollama not reachable at localhost:11434; "
                "skipping. Events will be tagged once Ollama is up (steady state)."
            )
            return
        logger.info(f"[TAGGER] bulk pass — {len(events)} untagged events")
        await self._warmup()
        n = await self._tag_events(events)
        logger.info(f"[TAGGER] bulk pass complete — tagged {n}/{len(events)} events")

    async def _steady_state(self) -> None:
        logger.info(f"[TAGGER] steady state — checking for new events every {self.watch_interval}s")
        while True:
            await asyncio.sleep(self.watch_interval)
            events = await self.db.get_untagged_trusted_events()
            if not events:
                continue
            if not ollama_reachable():
                logger.warning("[TAGGER] steady state — Ollama down, will retry next cycle")
                continue
            logger.info(f"[TAGGER] steady state — {len(events)} new event(s) to tag")
            await self._tag_events(events)

    async def _warmup(self) -> None:
        """Load the model into memory before the timed loop (avoids a cold-start
        timeout on the first real batch)."""
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, call_ollama, 'Reply JSON only: {"ok":1}', self.model)
        except Exception as exc:
            logger.warning(f"[TAGGER] warmup failed ({exc}) — main loop will retry")

    async def _tag_events(self, events: list) -> int:
        """Tag a list of events in batches. Returns the count successfully tagged.
        Ollama calls run in an executor so the pipeline event loop stays responsive."""
        loop = asyncio.get_running_loop()
        tagged = 0
        for start in range(0, len(events), self.batch_size):
            batch = events[start : start + self.batch_size]
            prompt = build_prompt(batch, self.vocab_slugs)

            result = None
            for attempt in (1, 2):
                try:
                    result = await loop.run_in_executor(None, call_ollama, prompt, self.model)
                    break
                except Exception as exc:
                    logger.warning(f"[TAGGER] batch @{start} attempt {attempt} failed: {exc}")
            if result is None:
                continue

            for i, event in enumerate(batch):
                raw = result.get(str(i), result.get(i, []))
                tags = [t for t in raw if t in self.vocab_set] if isinstance(raw, list) else []
                await self.db.upsert_event_tags(event.event_key, tags)
                tagged += 1

            if tagged % 100 == 0 or start + self.batch_size >= len(events):
                logger.info(f"[TAGGER] tagged {tagged}/{len(events)}")
        return tagged
