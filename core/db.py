"""
PostgreSQL database layer using asyncpg.

Owns two tables:
  dedupped_events  — persistent mirror of DedupStore; pre-warmed into memory
                     on startup so cross-source dedup survives restarts.
  trusted_events   — final scored output; queryable and updatable.

Both tables use event_key as the primary key with ON CONFLICT upsert so every
write is idempotent and always reflects the latest state.

Connection string is read from the vault (key: "db_url") and falls back
to the docker-compose default if not set.
"""
import json
import logging
from datetime import datetime
from typing import List, Optional

import asyncpg

from core.models import DeduppedEvent, TrustedEvent

logger = logging.getLogger(__name__)

_DEFAULT_DSN = "postgresql://edm:edm_password@localhost:5432/external_data_manager"

_CREATE_DEDUPPED = """
CREATE TABLE IF NOT EXISTS dedupped_events (
    event_key       TEXT        PRIMARY KEY,
    name            TEXT        NOT NULL,
    date            TIMESTAMP,
    location        TEXT,
    venue           TEXT,
    category        JSONB       DEFAULT '[]',
    tags            JSONB       DEFAULT '[]',
    sources         JSONB       DEFAULT '[]',
    price_min       FLOAT,
    price_max       FLOAT,
    currency        TEXT,
    url             TEXT,
    lat             FLOAT,
    lng             FLOAT,
    seen_count      INTEGER     DEFAULT 1,
    last_updated    TIMESTAMP
);
"""

_CREATE_USER_PREFERENCES = """
CREATE TABLE IF NOT EXISTS user_preferences (
    user_id     TEXT        PRIMARY KEY,
    city        TEXT,
    categories  JSONB       DEFAULT '[]',
    tags        JSONB       DEFAULT '[]',
    created_at  TIMESTAMP   DEFAULT NOW(),
    updated_at  TIMESTAMP   DEFAULT NOW()
);
"""

_CREATE_EVENT_LLM_TAGS = """
CREATE TABLE IF NOT EXISTS event_llm_tags (
    event_key   TEXT        PRIMARY KEY,
    tags        JSONB       DEFAULT '[]',
    tagged_at   TIMESTAMP   DEFAULT NOW()
);
"""

_CREATE_TRUSTED = """
CREATE TABLE IF NOT EXISTS trusted_events (
    event_key           TEXT        PRIMARY KEY,
    name                TEXT        NOT NULL,
    date                TIMESTAMP,
    location            TEXT,
    venue               TEXT,
    category            JSONB       DEFAULT '[]',
    tags                JSONB       DEFAULT '[]',
    sources             JSONB       DEFAULT '[]',
    price_min           FLOAT,
    price_max           FLOAT,
    currency            TEXT,
    url                 TEXT,
    lat                 FLOAT,
    lng                 FLOAT,
    trust_score         FLOAT,
    source_count        INTEGER,
    seen_count          INTEGER,
    has_trusted_source  BOOLEAN,
    last_calculated_at  TIMESTAMP
);
"""

_UPSERT_DEDUPPED = """
INSERT INTO dedupped_events (
    event_key, name, date, location, venue, category, tags, sources,
    price_min, price_max, currency, url, lat, lng, seen_count, last_updated
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT (event_key) DO UPDATE SET
    name         = EXCLUDED.name,
    date         = EXCLUDED.date,
    location     = EXCLUDED.location,
    venue        = EXCLUDED.venue,
    category     = EXCLUDED.category,
    tags         = EXCLUDED.tags,
    sources      = EXCLUDED.sources,
    price_min    = EXCLUDED.price_min,
    price_max    = EXCLUDED.price_max,
    currency     = EXCLUDED.currency,
    url          = EXCLUDED.url,
    lat          = EXCLUDED.lat,
    lng          = EXCLUDED.lng,
    seen_count   = EXCLUDED.seen_count,
    last_updated = EXCLUDED.last_updated;
"""

_UPSERT_TRUSTED = """
INSERT INTO trusted_events (
    event_key, name, date, location, venue, category, tags, sources,
    price_min, price_max, currency, url, lat, lng,
    trust_score, source_count, seen_count, has_trusted_source, last_calculated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14,
    $15, $16, $17, $18, $19
)
ON CONFLICT (event_key) DO UPDATE SET
    name                = EXCLUDED.name,
    date                = EXCLUDED.date,
    location            = EXCLUDED.location,
    venue               = EXCLUDED.venue,
    category            = EXCLUDED.category,
    tags                = EXCLUDED.tags,
    sources             = EXCLUDED.sources,
    price_min           = EXCLUDED.price_min,
    price_max           = EXCLUDED.price_max,
    currency            = EXCLUDED.currency,
    url                 = EXCLUDED.url,
    lat                 = EXCLUDED.lat,
    lng                 = EXCLUDED.lng,
    trust_score         = EXCLUDED.trust_score,
    source_count        = EXCLUDED.source_count,
    seen_count          = EXCLUDED.seen_count,
    has_trusted_source  = EXCLUDED.has_trusted_source,
    last_calculated_at  = EXCLUDED.last_calculated_at;
"""


class Database:
    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn or _DEFAULT_DSN
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn)
        async with self._pool.acquire() as conn:
            await conn.execute(_CREATE_DEDUPPED)
            await conn.execute(_CREATE_TRUSTED)
            await conn.execute(_CREATE_USER_PREFERENCES)
            await conn.execute(_CREATE_EVENT_LLM_TAGS)
        logger.info("[DB] Connected and schema ready")

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()

    # ------------------------------------------------------------------
    # dedupped_events
    # ------------------------------------------------------------------

    async def upsert_dedupped_event(self, event: DeduppedEvent) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                _UPSERT_DEDUPPED,
                event.event_key, event.name, event.date, event.location,
                event.venue, json.dumps(event.category), json.dumps(event.tags),
                json.dumps(event.sources), event.price_min, event.price_max,
                event.currency, event.url, event.lat, event.lng,
                event.seen_count, event.last_updated,
            )

    async def delete_dedupped_event(self, event_key: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM dedupped_events WHERE event_key = $1", event_key
            )

    async def get_all_dedupped_events(self) -> List[DeduppedEvent]:
        """Load all rows on startup to pre-warm DedupStore."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM dedupped_events")
        return [_row_to_dedupped(r) for r in rows]

    # ------------------------------------------------------------------
    # trusted_events
    # ------------------------------------------------------------------

    async def upsert_trusted_event(self, event: TrustedEvent) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                _UPSERT_TRUSTED,
                event.event_key, event.name, event.date, event.location,
                event.venue, json.dumps(event.category), json.dumps(event.tags),
                json.dumps(event.sources), event.price_min, event.price_max,
                event.currency, event.url, event.lat, event.lng,
                event.trust_score, event.source_count, event.seen_count,
                event.has_trusted_source, event.last_calculated_at,
            )

    async def delete_trusted_event(self, event_key: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM trusted_events WHERE event_key = $1", event_key
            )

    async def get_all_trusted_events(self) -> List[TrustedEvent]:
        """Load all rows on startup to pre-warm TrustedEventsStore."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM trusted_events")
        return [_row_to_trusted(r) for r in rows]

    async def count_trusted_events(self) -> int:
        """Cheap row count — used by the tagging service to detect when the
        initial fetch cycle has drained (count stops growing)."""
        async with self._pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM trusted_events")

    # ------------------------------------------------------------------
    # user_preferences
    # ------------------------------------------------------------------

    async def upsert_user_preferences(
        self, user_id: str, city: str, categories: list, tags: list
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO user_preferences (user_id, city, categories, tags, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    city = EXCLUDED.city,
                    categories = EXCLUDED.categories,
                    tags = EXCLUDED.tags,
                    updated_at = NOW();
                """,
                user_id, city, json.dumps(categories), json.dumps(tags),
            )

    async def get_user_preferences(self, user_id: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM user_preferences WHERE user_id = $1", user_id
            )
        if not row:
            return None
        return {
            "user_id": row["user_id"],
            "city": row["city"],
            "categories": json.loads(row["categories"] or "[]"),
            "tags": json.loads(row["tags"] or "[]"),
            "created_at": row["created_at"].isoformat() + "Z" if row["created_at"] else None,
            "updated_at": row["updated_at"].isoformat() + "Z" if row["updated_at"] else None,
        }

    # ------------------------------------------------------------------
    # event_llm_tags  (controlled-vocabulary tags from the offline LLM pass)
    # ------------------------------------------------------------------

    async def upsert_event_tags(self, event_key: str, tags: list) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO event_llm_tags (event_key, tags, tagged_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (event_key) DO UPDATE SET
                    tags = EXCLUDED.tags,
                    tagged_at = NOW();
                """,
                event_key, json.dumps(tags),
            )

    async def get_all_event_tags(self) -> dict:
        """Return {event_key: [tags]} for every tagged event."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT event_key, tags FROM event_llm_tags")
        return {r["event_key"]: json.loads(r["tags"] or "[]") for r in rows}

    async def get_untagged_trusted_events(self) -> List[TrustedEvent]:
        """Trusted events that don't yet have an LLM-tag row — the tagging job's worklist."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT t.* FROM trusted_events t
                LEFT JOIN event_llm_tags l ON t.event_key = l.event_key
                WHERE l.event_key IS NULL
                """
            )
        return [_row_to_trusted(r) for r in rows]

    async def delete_event_tags(self, event_key: str) -> None:
        """Drop an event's LLM-tag row. Called by the Expiration Service so a
        removed event never leaves an orphaned row in event_llm_tags."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM event_llm_tags WHERE event_key = $1", event_key
            )


# ------------------------------------------------------------------
# Row converters
# ------------------------------------------------------------------

def _row_to_dedupped(row: asyncpg.Record) -> DeduppedEvent:
    return DeduppedEvent(
        event_key=row["event_key"],
        name=row["name"],
        date=row["date"],
        location=row["location"],
        venue=row["venue"],
        category=json.loads(row["category"]),
        tags=json.loads(row["tags"]),
        sources=json.loads(row["sources"]),
        price_min=row["price_min"],
        price_max=row["price_max"],
        currency=row["currency"],
        url=row["url"],
        lat=row["lat"],
        lng=row["lng"],
        seen_count=row["seen_count"],
        last_updated=row["last_updated"],
    )


def _row_to_trusted(row: asyncpg.Record) -> TrustedEvent:
    return TrustedEvent(
        event_key=row["event_key"],
        name=row["name"],
        date=row["date"],
        location=row["location"],
        venue=row["venue"],
        category=json.loads(row["category"]),
        tags=json.loads(row["tags"]),
        sources=json.loads(row["sources"]),
        price_min=row["price_min"],
        price_max=row["price_max"],
        currency=row["currency"],
        url=row["url"],
        lat=row["lat"],
        lng=row["lng"],
        trust_score=row["trust_score"],
        source_count=row["source_count"],
        seen_count=row["seen_count"],
        has_trusted_source=row["has_trusted_source"],
        last_calculated_at=row["last_calculated_at"],
    )
