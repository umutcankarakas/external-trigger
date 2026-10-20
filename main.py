"""
External Data Manager — entry point.

Pipeline:
  Source Registry + Vault
    -> Fetcher     (checks seen_ids, publishes new RawEvents + BatchComplete)
    -> Normalizer
    -> Dedup Service   (merges by name+date+city, flushes on BatchComplete)
    -> Passthrough     (DeduppedEvent → TrustedEvent, no scoring)
    -> Database Writer (upserts dedupped_events + trusted_events to PostgreSQL)
    -> Expiration Service (periodic, cleans memory stores + both DB tables)

Tagging (two phases, coordinated with the Fetcher via asyncio.Events):
  Phase 1  the Fetcher pulls one full cycle, then PAUSES. The TaggingService
           bulk-tags every untagged event with the local LLM.
  Phase 2  once the bulk pass finishes the Fetcher resumes; the TaggingService
           then tags only newly-arrived events on a steady-state loop.
  Events are only ever removed by the Expiration Service.

Run:
  pip install -r requirements.txt
  docker compose up -d        # starts PostgreSQL + Adminer (localhost:8080)
  python main.py
"""
import asyncio
import json
import logging
import os
import sys
from datetime import datetime

if sys.platform == "win32":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from core.bus import MessageBus
from core.db import Database
from core.models import BatchComplete, DeduppedEvent, TrustedEvent
from services.db_writer import DatabaseWriter
from services.dedup import DedupService
from services.expiration import ExpirationService
from services.fetcher import Fetcher
from services.normalizer import Normalizer
from services.source_registry import SourceRegistry
from services.tagging_service import TaggingService
from services.trust_score import TrustedEventsStore
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


def _clear_activity() -> None:
    """Reset pipeline activity and event logs on every startup."""
    for path in [
        "config/pipeline_activity.json",
        "config/raw_log.json",
        "config/merge_log.json",
        "config/expiration_log.json",
    ]:
        try:
            with open(path, "w") as f:
                json.dump([], f)
        except Exception:
            pass


async def _passthrough(bus: MessageBus, store: TrustedEventsStore) -> None:
    """Convert every DeduppedEvent → TrustedEvent and publish to trusted_events.

    Replaces TrustScoreService — no scoring logic, all events are accepted.
    """
    inbox = bus.subscribe("dedupped_events")
    while True:
        message = await inbox.get()
        if isinstance(message, BatchComplete):
            await bus.publish("trusted_events", message)
            continue
        e: DeduppedEvent = message
        trusted = TrustedEvent(
            event_key=e.event_key, name=e.name, date=e.date,
            location=e.location, venue=e.venue, category=e.category,
            tags=e.tags, sources=list(e.sources),
            price_min=e.price_min, price_max=e.price_max,
            currency=e.currency, url=e.url, lat=e.lat, lng=e.lng,
            trust_score=1.0, source_count=len(e.sources),
            seen_count=e.seen_count, has_trusted_source=True,
            last_calculated_at=datetime.utcnow(),
        )
        store.upsert(trusted)
        await bus.publish("trusted_events", trusted)


async def main() -> None:
    _setup_logging()
    _clear_activity()
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

    bus           = MessageBus()
    trusted_store = TrustedEventsStore()

    # Phase-gate events coordinating the Fetcher and the TaggingService:
    #   first_pull_issued — set by the Fetcher after its first full cycle
    #   tagging.bulk_done — set by the TaggingService after the bulk tag pass;
    #                       the Fetcher waits on it before pulling again.
    first_pull_issued = asyncio.Event()
    tagging = TaggingService(db)

    # Instantiate services before bus.start() so all subscribe() calls
    # register their queues before any messages are published.
    fetcher    = Fetcher(
        registry, vault, bus,
        first_cycle_signal=first_pull_issued,
        resume_gate=tagging.bulk_done,
    )
    normalizer = Normalizer(bus, registry)
    dedup      = DedupService(bus)
    db_writer  = DatabaseWriter(bus, db)
    expiration = ExpirationService(
        store=trusted_store,
        check_interval=60,
        db=db,
        dedup_store=dedup.store,
    )

    # Pre-warm stores from PostgreSQL so state survives restarts.
    dedupped_events = await db.get_all_dedupped_events()
    for event in dedupped_events:
        dedup.store.load(event)

    trusted_events = await db.get_all_trusted_events()
    for event in trusted_events:
        trusted_store.upsert(event)

    if dedupped_events or trusted_events:
        print(
            f"  Loaded    {len(dedupped_events)} dedupped, "
            f"{len(trusted_events)} trusted events from database\n"
        )

    await bus.start()

    tasks = [
        asyncio.create_task(fetcher.start(),                        name="fetcher"),
        asyncio.create_task(normalizer.start(),                     name="normalizer"),
        asyncio.create_task(dedup.start(),                          name="dedup"),
        asyncio.create_task(_passthrough(bus, trusted_store),       name="passthrough"),
        asyncio.create_task(db_writer.start(),                      name="db_writer"),
        asyncio.create_task(expiration.start(),                     name="expiration"),
        asyncio.create_task(tagging.run(first_pull_issued),         name="tagger"),
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
