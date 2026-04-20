# External Trigger

A personalized event push-notification pipeline (POC). Fetches real-world events from multiple sources, deduplicates and trust-scores them, then uses a two-stage LLM optimizer to surface the single most relevant event per user persona and fire a push notification.

---

## Architecture

```
Sources (Ticketmaster + 3 mock)
  └─ Fetcher (async poller, exponential backoff)
       └─ raw_events [bus]
            └─ Normalizer (per-source schema mapping)
                 └─ normalized_events [bus]
                      └─ Dedup Service (SHA-256 keyed, seen_count tracking)
                           └─ dedupped_events [bus]
                                └─ Trust Score Service → TrustedEventsStore
                                     ├─ Expiration Service (prunes past events, 60s)
                                     └─ Persona Optimizer (every 30s)
                                          Stage 1: Hard Filter
                                                → Bank Persona Scoring (top 50)
                                                → LLM Router / qwen2.5:7b pre-screen
                                                → Chat Persona Boost / Mem0 (top 20)
                                          Stage 2: Ollama reranker (qwen2.5:7b)
                                                → OpenAI gpt-4o-mini (fallback)
                                                → Claude haiku (fallback)
                                                → Stage 1 raw scores (last resort)
                                          └─ Notification Service → Rich console
```

The `MessageBus` is an in-memory Kafka simulation — each topic fan-outs to all subscribers via `asyncio.Queue`. All services run as concurrent `asyncio.create_task` coroutines.

---

## Project Structure

```
.
├── main.py                          # Entry point — wires all services, starts 6 tasks
├── requirements.txt
├── config/
│   ├── personas.json                # User personas (bank + chat)
│   ├── sources.json                 # Source registry (fetch frequency, weights, endpoints)
│   └── secrets.json                 # NOT in git — see Secrets section below
├── core/
│   ├── bus.py                       # In-memory pub/sub (Kafka simulation)
│   └── models.py                    # Dataclasses: RawEvent → NormalizedEvent → DeduppedEvent → TrustedEvent → Notification
└── services/
    ├── vault.py                     # Secret store (secrets.json + env var override)
    ├── source_registry.py           # Loads config/sources.json, exposes source metadata
    ├── fetcher.py                   # Polls each source; mock handlers for mock:// endpoints; real HTTP for live APIs
    ├── normalizer.py                # Maps raw source payloads to NormalizedEvent (mock + ticketmaster)
    ├── dedup.py                     # SHA-256 key dedup; merges sources, unions tags, tracks seen_count
    ├── trust_score.py               # Scores events: trusted-source detection, count boost, seen_count boost
    ├── expiration.py                # Periodic cleanup of past-dated events from TrustedEventsStore
    ├── llm_router.py                # qwen2.5:7b Stage 1 pre-screener (Ollama); returns keep indices
    ├── mem0_persona.py              # Mem0 + Neo4j graph memory; seeds bootstrap facts; maps to chat_persona
    └── persona_optimizer/
        ├── optimizer.py             # Main orchestrator: Stage 1 → Stage 2 → Notification → Mem0 write-back
        ├── hard_filter.py           # Drops events outside user's location
        ├── bank_scoring.py          # Scores on spending categories, price fit, trust score; top 50
        ├── chat_boost.py            # Keyword/interest overlap boost; top 20
        ├── ollama_reranker.py       # Primary Stage 2: qwen2.5:7b via Ollama, 60s timeout, multi-pass JSON parse
        ├── openai_reranker.py       # Stage 2 fallback: gpt-4o-mini
        ├── llm_reranker.py          # Stage 2 last resort: Claude haiku
        └── notification.py          # Score threshold + per-(user, event) dedup guard
```

---

## Secrets

Create `config/secrets.json` (excluded from git via `.gitignore`):

```json
{
  "openai_api_key": "sk-...",
  "openai_model": "gpt-4o-mini",
  "openai_max_completion_tokens": 2048,

  "anthropic_api_key": "sk-ant-...",

  "ticketmaster_api_key": "YOUR_KEY",

  "routing_model": "qwen2.5:7b",
  "routing_base_url": "http://localhost:11434/v1",
  "routing_api_key": "ollama",

  "mem0_graph_url": "bolt://localhost:7687",
  "mem0_graph_username": "neo4j",
  "mem0_graph_password": "YOUR_PASSWORD",
  "mem0_fact_timeout_s": 5.0,
  "mem0_write_timeout_s": 8.0,
  "mem0_debug_long_term_decision": "false",

  "bank_user_id": "user_001"
}
```

All keys can also be set as environment variables (uppercase, e.g. `OPENAI_API_KEY`). Env vars take precedence over the file.

---

## Prerequisites

| Dependency | Purpose | Required? |
|---|---|---|
| Python 3.11+ | Runtime | Yes |
| Ollama + qwen2.5:7b | Stage 1 router + Stage 2 primary reranker | Yes (falls back gracefully if missing) |
| OpenAI API key | Stage 2 fallback reranker | No |
| Anthropic API key | Stage 2 last-resort reranker | No |
| Ticketmaster API key | Real event data | No (mock sources still work) |
| Neo4j (local or Docker) | Mem0 graph memory backend | No (Mem0 disabled if missing) |

```bash
# Start Ollama with the routing model
ollama run qwen2.5:7b

# Start Neo4j (optional, for Mem0 graph)
docker run -p 7687:7687 -p 7474:7474 \
  -e NEO4J_AUTH=neo4j/YOUR_PASSWORD \
  neo4j:latest
```

---

## Running

```bash
pip install -r requirements.txt
python main.py
```

---

## Key Design Decisions

### Trust Scoring
- **Trusted source** = `source_weight >= 0.85` (currently: Ticketmaster at 0.90)
- If a trusted source contributed → use `max_weight` as base score instead of average
- **`seen_count`** — incremented every time an event arrives (even re-fetches of the same event). Adds up to +10% boost over time (`min(1 + (seen_count-1) * 0.01, 1.1)`)
- **`source_count` boost** — 1.0× (1 source) / 1.2× (2 sources) / 1.4× (3+ sources)

### Dedup
- Key: `SHA-256(name.lower() | date.date() | location.lower())[:16]`
- On merge: unions tags, keeps tightest price bounds, increments `seen_count`
- Dedup store is never pruned — expiration only applies to `TrustedEventsStore`

### LLM Router vs Ollama Reranker
- **Router** (`llm_router.py`): Stage 1 pre-screen. Fast batch call, returns `{"keep": [indices]}`. 15s timeout. Trims 32 → ~13 candidates before expensive reranking.
- **Ollama Reranker** (`ollama_reranker.py`): Stage 2. Scores each event 0–100 with reasoning. 60s timeout. Multi-pass JSON extraction (qwen2.5:7b doesn't support `response_format: json_object`).

### Mem0
- Only enabled for `bank_user_id` (default: `user_001`)
- Seeds 8 bootstrap memories on first run (idempotent)
- Retrieves memories via 4 semantic queries per cycle, maps to `{interests: [], keywords: []}`
- Writes notification decisions back as long-term memory

### Message Bus
- Simulates Kafka fan-out using `asyncio.Queue` per subscriber
- Topics: `raw_events` → `normalized_events` → `dedupped_events` → `trusted_events`
- Expiration and Optimizer bypass the bus and read `TrustedEventsStore` directly

---

## Personas (`config/personas.json`)

| user_id | Location | Income | Spending | Chat persona |
|---|---|---|---|---|
| user_001 | Istanbul | High | dining, entertainment, travel | jazz, indie music, art, football |
| user_002 | Istanbul | Medium | sports, family, dining | none (bank-only scoring) |
| user_003 | Ankara | Low | culture, education, dining | theater, classical music, art |

`user_001` is also the `bank_user_id` — Mem0 memories are written and read for this user.

---

## Sources (`config/sources.json`)

| source_id | Type | Weight | Frequency | Status |
|---|---|---|---|---|
| mock_concerts | mock | 0.75 | 30s | active |
| mock_sports | mock | 0.75 | 30s | active |
| mock_cultural | mock | 0.70 | 30s | active |
| ticketmaster | ticketmaster | 0.90 | 300s | active |
| predictiq | predictiq | 0.85 | 300s | **disabled** |

To add a new source: add an entry to `sources.json` and add a normalizer function to `services/normalizer.py` registered in `SOURCE_NORMALIZERS`.

---

## Known Limitations / Next Steps

- **Notification dedup is session-only** — the seen `(user_id, event_key)` set resets on restart. Persist to Redis or DB for production.
- **Dedup store never pruned** — expired events stay in `DedupStore` even after removal from `TrustedEventsStore`. Add coordinated cleanup.
- **Single Ticketmaster query** — currently fetches TR music events only. Parameterize per-persona or per-source.
- **Hard filter is location-exact** — matches on string contains. Could be improved with geo-distance or country-level fallback.
- **No retry on notification failure** — `on_notification` callback is fire-and-forget.
- **Mem0 is single-user** — all personas share the same `bank_user_id` memory. Extend to per-user memory stores.
