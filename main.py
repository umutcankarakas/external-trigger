"""
External Data Manager — entry point.

Pipeline:
  Source Registry + Vault
    -> Fetcher (Ticketmaster)
    -> raw_events
    -> Normalizer
    -> normalized_events
    -> Dedup Service
    -> dedupped_events
    -> Trust Score Service -> TrustedEventsStore
    -> Expiration Service (periodic)

Run:
  pip install -r requirements.txt
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
    logging.getLogger("aiokafka").setLevel(logging.CRITICAL)


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

    kafka_servers = vault.get("kafka_bootstrap_servers") or "localhost:9092"
    bus = MessageBus(bootstrap_servers=kafka_servers)

    active_sources = [s.source_id for s in registry.get_active_sources()]
    print(f"  Sources  {active_sources}")
    print(f"  Kafka    {kafka_servers}\n")

    # Services must be instantiated before bus.start() so their subscribe()
    # calls register all consumers, which bus.start() then connects eagerly.
    fetcher     = Fetcher(registry, vault, bus)
    normalizer  = Normalizer(bus, registry)
    dedup       = DedupService(bus)
    trust_score = TrustScoreService(bus, registry)
    expiration  = ExpirationService(trust_score.store, check_interval=60)

    await bus.start()

    tasks = [
        asyncio.create_task(fetcher.start(),     name="fetcher"),
        asyncio.create_task(normalizer.start(),  name="normalizer"),
        asyncio.create_task(dedup.start(),       name="dedup"),
        asyncio.create_task(trust_score.start(), name="trust_score"),
        asyncio.create_task(expiration.start(),  name="expiration"),
    ]

    _banner("Pipeline running  —  Ctrl+C to stop")

    try:
        await asyncio.gather(*tasks)
    except (asyncio.CancelledError, KeyboardInterrupt):
        for task in tasks:
            task.cancel()
    finally:
        await bus.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n  Shut down.\n")
        sys.exit(0)
