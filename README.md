# External Data Manager

A multi-source event aggregation system for Turkey, in two cooperating parts:

1. **Ingestion pipeline** — fetches real-world events from six APIs, deduplicates cross-source occurrences, and persists them to PostgreSQL.
2. **Event Recommender** — tags every event with a local LLM against a controlled vocabulary, then serves personalised, BM25-ranked recommendations through a survey-driven web app.

---

## Architecture

### 1. Ingestion pipeline (`main.py`)

```
Sources (Ticketmaster · PredictHQ · ESPN · TheSportsDB · Biletix · Passo)
  └─ Fetcher          async poller per source, exponential backoff
       └─ raw_events ──────────────────────────────── [MessageBus]
            └─ Normalizer    per-source schema mapping → NormalizedEvent
                 └─ normalized_events ───────────────── [MessageBus]
                      └─ Dedup Service    3-bucket fuzzy merge → DeduppedEvent
                           └─ dedupped_events ──────── [MessageBus]
                                └─ Passthrough         DeduppedEvent → TrustedEvent
                                     └─ trusted_events ── [MessageBus]
                                          └─ Database Writer    PostgreSQL upsert
                                               └─ Expiration Service   prunes past events
```

`MessageBus` is an in-memory pub/sub that simulates Kafka fan-out using `asyncio.Queue`. All services run as concurrent `asyncio.create_task` coroutines inside a single process.

### 2. Tagging + Recommender

```
TaggingService (inside main.py)
  Phase 1  bulk-tags every event via a local LLM — the Fetcher is paused
  Phase 2  tags newly-arrived events incrementally, every 10 min
       └─ event_llm_tags   controlled-vocabulary tags, keyed by event_key

Recommender API  (api/routers/recommender.py)
  └─ in-memory BM25 index over event_llm_tags, rebuilt per request
       └─ frontend-recommender/   survey + recommendations  (localhost:5174)
```

The REST API (`run_api.py`) is a separate FastAPI process serving both the admin dashboard and the recommender.

---

## Project Structure

```
.
├── main.py                      # Pipeline entry point — fetch, dedup, persist, tag
├── run_api.py                   # FastAPI server (localhost:8001)
├── tag_events.py                # CLI for manual/one-off LLM tagging runs
├── requirements.txt
├── docker-compose.yml           # PostgreSQL + Adminer
│
├── config/
│   ├── sources.json             # Source registry (endpoints, frequency, auth)
│   ├── secrets.json             # API keys — NOT committed to git
│   ├── event_vocabulary.json    # Controlled tag vocabulary (37 slugs, 4 groups)
│   ├── mock_events.json         # Static test events (mock source, disabled)
│   ├── seen_ids.json            # IDs already fetched — prevents re-publishing
│   ├── event_log.json           # Rolling pipeline activity log
│   └── merge_log.json           # Per-merge records (two sources, same event)
│
├── core/
│   ├── bus.py                   # In-memory pub/sub (Kafka simulation)
│   ├── db.py                    # asyncpg client — 4 tables (see Database below)
│   ├── models.py                # RawEvent → NormalizedEvent → DeduppedEvent → TrustedEvent
│   └── event_log.py             # Append-only structured logger
│
├── services/
│   ├── vault.py                 # Secret store (secrets.json + env var override)
│   ├── source_registry.py       # Loads config/sources.json
│   ├── fetcher.py               # Polls each source; phase-gate pauses after cycle 1
│   ├── normalizer.py            # Raw source payloads → NormalizedEvent (one fn per source)
│   ├── dedup.py                 # 3-bucket fuzzy dedup + token-set merge
│   ├── trust_score.py           # TrustedEventsStore (in-memory cache)
│   ├── db_writer.py             # Upserts dedupped_events + trusted_events
│   ├── expiration.py            # Prunes past events from memory + all 3 DB tables
│   ├── tagging_service.py       # LLM tagging — bulk + steady-state (see Recommender)
│   └── recommender_index.py     # BM25 index over event tags + slugify()
│
├── api/
│   ├── app.py                   # FastAPI app, CORS, router registration
│   └── routers/
│       ├── events.py            # GET /api/events, /api/events/{key}
│       ├── sources.py           # GET /api/sources
│       ├── stats.py             # GET /api/stats
│       ├── activity.py          # GET /api/activity (pipeline log)
│       └── recommender.py       # GET/POST /api/recommender/* (survey + ranking)
│
├── frontend/                    # Admin dashboard — React + Vite (localhost:5173)
│   └── src/pages/               # Stats · Events · ActivityLog · Sources
│
└── frontend-recommender/        # Recommender web app — React + Vite (localhost:5174)
    └── src/pages/               # Survey · Recommendations
```

---

## Data Sources

| source_id | Data | Auth | Frequency | Status |
|---|---|---|---|---|
| `ticketmaster` | Concerts, shows, live events in Turkey | API key (query param) | 3 600 s | **active** |
| `predicthq` | Concerts, sports, festivals, holidays | Bearer token | 1 800 s | **active** |
| `espn` | Turkish Süper Lig football fixtures | None (public) | 21 600 s | **active** |
| `thesportsdb_basketball` | Turkish Basketball Super League (BSL) | None (public) | 21 600 s | **active** |
| `biletix` | All Biletix listings (internal Solr index) | None | 3 600 s | **active** |
| `passo` | All Passo listings (internal JSON API) | None | 3 600 s | **active** |
| `apisports` | Turkish Süper Lig (api-sports.io) | API key (header) | 21 600 s | disabled |
| `mock` | Static local test events | None | 60 s | disabled |

Biletix and Passo are scraped from undocumented internal endpoints (browser-like headers, no official API). Passo's endpoint sits behind Cloudflare, so its fetch runs via `requests` in a thread executor rather than `aiohttp`.

---

## Deduplication

Events from different sources often describe the same real-world occurrence. The dedup service merges them into a single `DeduppedEvent` using a three-tier lookup.

### 1 — Hash key (exact match)

```
key = SHA-256( norm(name) | date.date() | norm(city) )[:16]
```

Identical entries (e.g. the same event fetched twice) merge immediately.

### 2 — Bucket candidates (narrow the search space)

| Bucket | Key | Used when |
|---|---|---|
| `_by_date_city` | `YYYY-MM-DD\|city` | Primary — most events have a city |
| `_by_date_venue` | `YYYY-MM-DD\|venue` | Fallback — catches city-name mismatches |
| `_by_date` | `YYYY-MM-DD` | Last resort — location is only a country code ("TR") |

All three buckets are unioned before name comparison.

### 3 — Name comparison (`_names_likely_same`)

Three independent rules — **any one is sufficient**:

1. **Exact** — identical after normalization (Turkish chars, punctuation, case).
2. **Fuzzy** — ≥ 85% string similarity (typos, minor wording differences).
3. **Token-set containment** — the *significant tokens* of one name are a subset of the other's. Handles template prefixes (`"%100 Müzik Sunar: Sagopa Kajmer"` ⊃ `"Sagopa Kajmer"`), abbreviations (`"Erkan Ogur"` ⊂ `"Erkan Oğur & İsmail Hakkı..."`), and league prefixes (`"BSL - Anadolu Efes"` ≡ `"Anadolu Efes SK"`).

Stopwords (EN/TR structural + event-generic + sports descriptors) are filtered before token comparison so generic words don't influence matching.

### Merge strategy

| Field | Rule |
|---|---|
| Text (name, venue, URL, currency) | keep first non-empty value |
| Tags | union of all sources |
| Price | tightest bounds — `min(price_min)`, `max(price_max)` |
| Coordinates | first non-null lat/lng |
| Sources | appended — e.g. `["ticketmaster", "biletix", "predicthq"]` |

Merges are recorded to `config/merge_log.json` and the Activity Log.

---

## Event Recommender

A separate web app (`localhost:5174`) that asks a visitor a 2-question survey and returns the top 20 events for them.

### Survey → preferences

1. **City** (required) — a hard filter; only events in that city are considered.
2. **Interests** — picked from the controlled vocabulary, grouped (music genres / event types / sports / vibe).

Preferences are stored in PostgreSQL keyed by an anonymous browser-generated UUID (`user_preferences` table), so they survive across sessions.

### Controlled vocabulary

`config/event_vocabulary.json` defines a fixed set of 37 tag slugs in 4 groups. Both the LLM tagger *and* the survey draw from this one list, so user preferences and event tags live in a single token space — exactly what BM25 needs.

### LLM tagging (the offline pass)

Source data carries messy, inconsistent tags (Ticketmaster classifications, PredictHQ labels, Biletix subcategories — mixed Turkish/English). The `TaggingService` normalises this onto the controlled vocabulary:

- A **local LLM** (`qwen2.5:7b` via [Ollama](https://ollama.com)) reads each event's name, category, tags, and venue and assigns 1–4 vocabulary slugs.
- Results are stored in the `event_llm_tags` table, keyed by `event_key`.
- Because tagging happens **after dedup**, each real-world event is tagged exactly once — the LLM sees the merged signal from every source.

The tagger runs in **two phases**, coordinated with the Fetcher:

| Phase | What happens |
|---|---|
| **1 — bulk** | After the first fetch cycle drains to the DB, the Fetcher *pauses* and every untagged event is tagged. |
| **2 — steady state** | The Fetcher resumes; every 10 min the tagger picks up only newly-arrived untagged events (`get_untagged_trusted_events()`). |

The expensive bulk pass happens once. On a warm restart it finds nothing untagged and is instant. `tag_events.py` is a CLI for manual/one-off runs (re-tag, city-scoped, model spot-check).

### BM25 ranking

At query time, `services/recommender_index.py` builds an in-memory **BM25 (Okapi)** index over `event_llm_tags` (cheap — rebuilt per request for ~5k events). The user's interest slugs are the query:

- **IDF weighting** — rare tag matches (`arabesque`) outrank common ones (`concert`) automatically.
- **Hard filters** (don't affect the score, only narrow candidates): event city == user city, event date in the future.
- Results are ranked by BM25 score, ties broken by soonest date.

Recommendations are instant — no LLM at query time, only the precomputed tag index.

---

## Event Lifecycle

```
RawEvent          raw JSON from the API, tagged with source_id
    ↓
NormalizedEvent   name, date, location, venue, category, tags, price, url, lat/lng
    ↓
DeduppedEvent     + event_key (hash), sources[], seen_count
    ↓
TrustedEvent      + trust_score (1.0), source_count        → trusted_events table
    ↓
event_llm_tags    controlled-vocabulary tags (offline LLM pass)
    ↓
BM25 index        built on demand for /api/recommender/recommendations
```

---

## Database

PostgreSQL, 4 tables (auto-created on connect):

| Table | Purpose |
|---|---|
| `dedupped_events` | Persistent mirror of the in-memory DedupStore |
| `trusted_events` | Final event output — queried by the API |
| `user_preferences` | Recommender survey answers, keyed by anonymous UUID |
| `event_llm_tags` | Controlled-vocabulary tags per `event_key` |

The Expiration Service removes past events from **all three** event-related tables together, so no orphaned rows are left behind.

---

## Setup

### Prerequisites

| Dependency | Version | Purpose |
|---|---|---|
| Python | 3.11+ | Pipeline + API runtime |
| Docker Desktop | any | PostgreSQL via docker-compose |
| Node.js | 18+ | Frontend dev servers |
| Ollama | any | Local LLM for event tagging |
| Ticketmaster + PredictHQ API keys | — | Live event data (ESPN/TheSportsDB/Biletix/Passo need no key) |

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure secrets

Create `config/secrets.json` (not committed to git):

```json
{
  "ticketmaster_api_key": "YOUR_TICKETMASTER_KEY",
  "predicthq_api_key":   "YOUR_PREDICTHQ_KEY"
}
```

Any value can also be supplied as an uppercase environment variable (takes precedence).

### 3. Start PostgreSQL

```bash
docker compose up -d
```

PostgreSQL on `localhost:5432`, Adminer (web DB browser) on `localhost:8080`.

### 4. Install Ollama + the tagging model

```bash
# install Ollama from https://ollama.com, then:
ollama pull qwen2.5:7b
```

Ollama serves an OpenAI-compatible API at `localhost:11434`. If it's not running, the pipeline still works — events just stay untagged until Ollama is back.

### 5. Start the pipeline

```bash
python main.py
```

On first run this fetches all sources, then bulk-tags every event (slow on CPU — a one-time cost; the Fetcher is paused meanwhile). Subsequent runs are fast.

### 6. Start the API server

```bash
python run_api.py
```

API at **http://localhost:8001** — interactive docs at `/docs`.

### 7. Start the frontends

```bash
cd frontend            && npm install && npm run dev   # admin dashboard → :5173
cd frontend-recommender && npm install && npm run dev   # recommender app → :5174
```

---

## API Endpoints

All under `http://localhost:8001`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/events` | List events (`?search=`, `?source=`, `?limit=`) |
| `GET` | `/api/events/{key}` | Single event by `event_key` |
| `GET` | `/api/stats` | Aggregate counts by source, category |
| `GET` | `/api/sources` | Active source configurations |
| `GET` | `/api/activity` | Rolling pipeline activity log |
| `GET` | `/api/recommender/taxonomy` | Survey options — cities + controlled vocabulary |
| `POST` | `/api/recommender/preferences` | Save a user's city + interests |
| `GET` | `/api/recommender/preferences/{user_id}` | Read stored preferences |
| `GET` | `/api/recommender/recommendations/{user_id}` | Top-N BM25-ranked events |

---

## Adding a New Source

1. **`config/sources.json`** — add an entry (`source_id`, `endpoint`, `search_query`, `fetch_frequency`, `is_active`, `auth_ref`, `auth_type`, `source_type`).
2. **`config/secrets.json`** — add the API key if the source needs one.
3. **`services/normalizer.py`** — add a `_normalize_<source>()` function and register it in `SOURCE_NORMALIZERS`.
4. For non-trivial fetching (pagination, POST, custom headers) add a `_fetch_all_<source>()` method in `services/fetcher.py`.

No changes are needed in dedup, tagging, BM25, or the recommender — they all key off `event_key` and are source-agnostic.

### Date placeholder tokens (for `search_query` values)

| Token | Example | Description |
|---|---|---|
| `$today` / `$today+Nd` | `2026-05-21` | Date, optionally offset N days |
| `$todayT` / `$today+NdT` | `2026-05-21T00:00:00Z` | …as an ISO datetime |
| `$today_compact` | `20260521` | …as `YYYYMMDD` |
| `$season` | `2025` | Turkish football season year (Aug–Jul) |

### Auth types

| `auth_type` | Behaviour |
|---|---|
| `query_param` | Appends `?apikey=...` to the URL |
| `bearer` | `Authorization: Bearer ...` header |
| `header:X-Name` | Sends the key in the named header |
| `none` | No authentication |

---

## Known Limitations

- **`DedupStore` is never pruned** — expired events are removed from `TrustedEventsStore` and all DB tables, but their dedup keys stay in the in-memory `DedupStore`. Grows slowly on long runs.
- **Tags aren't refreshed on re-merge** — if an already-tagged event later gains a new source with richer category data, it keeps its original tags (not re-tagged). Almost always fine, since event identity is stable.
- **CPU-only LLM tagging is slow** — the bulk pass on a CPU is a multi-hour, one-time cost (a GPU is ~10–30× faster). Steady-state incremental tagging is fast since it only handles new events.
- **Single geography** — all sources are configured for Turkey.
- **Scraped sources** — Biletix and Passo use undocumented internal endpoints that could change without notice.
