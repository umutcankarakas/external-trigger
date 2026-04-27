"""
External Data Manager — entry point.

Pipeline:
  Source Registry + Vault
    -> Fetcher  (checks seen_ids, publishes new RawEvents + BatchComplete)
    -> Normalizer
    -> Dedup Service  (merges by name+date+city, flushes on BatchComplete)
    -> Trust Score Service  (lazy recalc: only if last_updated > last_calculated_at)
    -> Database Writer  (upserts dedupped_events + trusted_events to PostgreSQL)
    -> Expiration Service  (periodic, cleans memory stores + both DB tables)

On startup both in-memory stores are pre-warmed from PostgreSQL so:
  - Cross-source dedup survives restarts (DedupStore loaded from dedupped_events)
  - Trust score skips recalculation for unchanged events (TrustedEventsStore
    loaded from trusted_events with their original last_calculated_at)

Run:
  pip install -r requirements.txt
  docker compose up -d        # starts PostgreSQL + Adminer (localhost:8080)
  python main.py
"""
import asyncio
import logging
import sys
from datetime import datetime

if sys.platform == "win32":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from core.bus import MessageBus
from core.db import Database
from services.db_writer import DatabaseWriter
from services.dedup import DedupService
from services.expiration import ExpirationService
from services.fetcher import Fetcher
from services.normalizer import Normalizer
from services.source_registry import SourceRegistry
from services.trust_score import TrustScoreService
from services.vault import Vault


class _Formatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        t = datetime.now().strftime("%H:%M:%S")
        return f"  {t}  {record.getMessage()}"


def _setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_Formatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def _banner(text: str) -> None:
    width = 72
    print(f"\n  {'─' * width}")
    print(f"  {text.center(width)}")
    print(f"  {'─' * width}\n")


async def main() -> None:
    _setup_logging()
    _banner("External Data Manager")

    vault    = Vault()
    registry = SourceRegistry()

    # Database — reads db_url from vault/env, falls back to docker-compose default
    db_url = vault.get("db_url")
    db = Database(dsn=db_url)
    await db.connect()

    active_sources = [s.source_id for s in registry.get_active_sources()]
    print(f"  Sources   {active_sources}")
    print(f"  Database  {db_url or 'postgresql://edm@localhost:5432/external_data_manager'}\n")

    bus = MessageBus()

    # Instantiate services before bus.start() so all subscribe() calls
    # register their queues before any messages are published.
    fetcher     = Fetcher(registry, vault, bus)
    normalizer  = Normalizer(bus, registry)
    dedup       = DedupService(bus)
    trust_score = TrustScoreService(bus, registry)
    db_writer   = DatabaseWriter(bus, db)
    expiration  = ExpirationService(
        store=trust_score.store,
        check_interval=60,
        db=db,
        dedup_store=dedup.store,
    )

    # Pre-warm DedupStore from PostgreSQL so cross-source dedup survives
    # restarts. A new source sending an already-known event will correctly
    # merge rather than create a duplicate entry.
    dedupped_events = await db.get_all_dedupped_events()
    for event in dedupped_events:
        dedup.store.load(event)

    # Pre-warm TrustedEventsStore so last_calculated_at is available from
    # the previous run — prevents unnecessary trust score recalculation.
    trusted_events = await db.get_all_trusted_events()
    for event in trusted_events:
        trust_score.store.upsert(event)

    if dedupped_events or trusted_events:
        print(
            f"  Loaded    {len(dedupped_events)} dedupped, "
            f"{len(trusted_events)} trusted events from database\n"
        )

    await bus.start()

    tasks = [
        asyncio.create_task(fetcher.start(),     name="fetcher"),
        asyncio.create_task(normalizer.start(),  name="normalizer"),
        asyncio.create_task(dedup.start(),       name="dedup"),
        asyncio.create_task(trust_score.start(), name="trust_score"),
        asyncio.create_task(db_writer.start(),   name="db_writer"),
        asyncio.create_task(expiration.start(),  name="expiration"),
    ]

    _banner("Pipeline running  —  Ctrl+C to stop")

    try:
        await asyncio.gather(*tasks)
    except (asyncio.CancelledError, KeyboardInterrupt):
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await bus.stop()
        await db.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n  Shut down.\n")
        sys.exit(0)
