"""
External Trigger — POC entry point.

Full pipeline:
  Source Registry + Vault
    → Fetcher (Ticketmaster + 3 mock sources)
    → raw_events
    → Normalizer
    → normalized_events
    → Dedup Service
    → dedupped_events
    → Trust Score Service → TrustedEventsStore
    → Expiration Service (periodic)
    → Persona Optimizer
        Stage 1: Hard Filter → Bank Scoring → qwen2.5 Router → Chat Boost (Mem0)
        Stage 2: OpenAI gpt-4o-mini Reranker
        → Notification Service → Rich console

Run:
  pip install -r requirements.txt
  python main.py

Secrets are read from config/secrets.json or environment variables
(env vars take precedence). See secrets.json for key names.

Neo4j for Mem0 graph: bolt://localhost:7687 (start with Docker or Neo4j Desktop)
Ollama for router: http://localhost:11434 (run: ollama run qwen2.5:7b)
"""
import asyncio
import logging
import sys

from rich import box
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table

from core.bus import MessageBus
from core.models import Notification
from services.dedup import DedupService
from services.expiration import ExpirationService
from services.fetcher import Fetcher
from services.llm_router import LLMRouter
from services.mem0_persona import Mem0PersonaClient
from services.normalizer import Normalizer
from services.persona_optimizer.optimizer import PersonaOptimizer
from services.source_registry import SourceRegistry
from services.trust_score import TrustScoreService
from services.vault import Vault

console = Console()


# ---------------------------------------------------------------------------
# Rich notification card
# ---------------------------------------------------------------------------

def _display_notification(notif: Notification) -> None:
    t = Table(box=box.SIMPLE, show_header=False, padding=(0, 1), expand=False)
    t.add_column(style="bold cyan", no_wrap=True)
    t.add_column()
    t.add_row("User",      notif.user_id)
    t.add_row("Event",     f"[bold white]{notif.event_name}[/]")
    t.add_row("Date",      notif.event_date)
    t.add_row("Location",  notif.event_location)
    t.add_row("Score",     f"[bold green]{notif.relevance_score:.1f} / 100[/]")
    t.add_row("Reasoning", f"[italic]{notif.reasoning}[/]")
    console.print(
        Panel(t, title="[bold yellow] PUSH NOTIFICATION [/]", border_style="yellow")
    )


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            RichHandler(
                console=console,
                rich_tracebacks=True,
                markup=True,
                show_path=False,
            )
        ],
        format="%(message)s",
        datefmt="[%X]",
    )
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("anthropic").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("mem0").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    _setup_logging()
    console.rule("[bold blue]External Trigger — POC[/bold blue]")
    console.print()

    # ── Secrets + Registry ───────────────────────────────────────────────────
    vault    = Vault()
    registry = SourceRegistry()
    bus      = MessageBus()

    openai_key      = vault.get("openai_api_key")
    openai_model    = vault.get("openai_model") or "gpt-4o-mini"
    openai_tokens   = int(vault.get("openai_max_completion_tokens") or 2048)
    anthropic_key   = vault.get("anthropic_api_key")
    routing_model   = vault.get("routing_model") or "qwen2.5:7b"
    routing_url     = vault.get("routing_base_url") or "http://localhost:11434/v1"
    routing_key     = vault.get("routing_api_key") or "ollama"
    mem0_graph_url  = vault.get("mem0_graph_url")
    mem0_graph_user = vault.get("mem0_graph_username") or "neo4j"
    mem0_graph_pass = vault.get("mem0_graph_password")
    bank_user_id    = vault.get("bank_user_id") or "user_001"

    active_sources = [s.source_id for s in registry.get_active_sources()]

    # ── Status banner ────────────────────────────────────────────────────────
    console.print(f"[green]Active sources[/]  {active_sources}")
    console.print(
        f"[green]Stage 2 reranker[/] "
        f"[bold green]{routing_model} (Ollama)[/] → "
        + (f"[green]OpenAI {openai_model}[/]" if openai_key else "[dim]OpenAI (no key)[/]")
        + " → "
        + ("[green]Claude[/]" if anthropic_key else "[dim]Claude (no key)[/]")
        + " → Stage 1"
    )
    console.print(
        f"[green]Router (Stage 1)[/] "
        + f"[bold green]{routing_model} @ {routing_url}[/]"
    )
    console.print(
        f"[green]Mem0 graph[/]      "
        + (f"[bold green]Neo4j @ {mem0_graph_url}[/]" if mem0_graph_url
           else "[yellow]disabled[/]")
    )
    console.print(f"[green]Bank user ID[/]    {bank_user_id}")
    console.print()

    # ── Pipeline services ────────────────────────────────────────────────────
    fetcher     = Fetcher(registry, vault, bus)
    normalizer  = Normalizer(bus, registry)
    dedup       = DedupService(bus)
    trust_score = TrustScoreService(bus, registry)
    expiration  = ExpirationService(trust_score.store, check_interval=60)

    # ── Router (qwen2.5:7b via Ollama) ───────────────────────────────────────
    router = LLMRouter(
        model=routing_model,
        base_url=routing_url,
        api_key=routing_key,
    )

    # ── Mem0 persona client ───────────────────────────────────────────────────
    mem0_client = None
    if openai_key:
        mem0_client = Mem0PersonaClient(
            openai_api_key=openai_key,
            openai_model=openai_model,
            graph_url=mem0_graph_url,
            graph_username=mem0_graph_user,
            graph_password=mem0_graph_pass,
            bank_user_id=bank_user_id,
            fact_timeout=float(vault.get("mem0_fact_timeout_s") or 5.0),
            write_timeout=float(vault.get("mem0_write_timeout_s") or 8.0),
            debug_decisions=str(vault.get("mem0_debug_long_term_decision") or "").lower() == "true",
        )

    # ── Persona Optimizer ────────────────────────────────────────────────────
    optimizer = PersonaOptimizer(
        store=trust_score.store,
        ollama_base_url=routing_url,
        ollama_api_key=routing_key,
        ollama_model=routing_model,
        openai_api_key=openai_key,
        openai_model=openai_model,
        openai_max_tokens=openai_tokens,
        anthropic_api_key=anthropic_key,
        router=router,
        mem0_client=mem0_client,
        bank_user_id=bank_user_id,
        run_interval=30,
    )
    optimizer.on_notification = _display_notification

    # ── Run all services ─────────────────────────────────────────────────────
    tasks = [
        asyncio.create_task(fetcher.start(),     name="fetcher"),
        asyncio.create_task(normalizer.start(),  name="normalizer"),
        asyncio.create_task(dedup.start(),       name="dedup"),
        asyncio.create_task(trust_score.start(), name="trust_score"),
        asyncio.create_task(expiration.start(),  name="expiration"),
        asyncio.create_task(optimizer.start(),   name="optimizer"),
    ]

    console.rule("[dim]Pipeline running — Ctrl+C to stop[/dim]")
    console.print()

    try:
        await asyncio.gather(*tasks)
    except (asyncio.CancelledError, KeyboardInterrupt):
        for task in tasks:
            task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Shut down.[/dim]")
        sys.exit(0)
