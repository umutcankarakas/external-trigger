"""
Offline LLM tagging CLI.

A thin command-line wrapper around services.tagging_service — useful for manual
one-off runs (initial bulk pass, re-tagging, city-scoped runs, model spot-checks).
In normal operation the pipeline (main.py) runs the TaggingService automatically;
this script exists for ad-hoc control.

The shared tagging logic (vocabulary, prompt, Ollama call) lives in
services/tagging_service.py so there is one implementation, not two.

Usage (run from the external-trigger/ directory):
  python tag_events.py                      # tag only events missing tags
  python tag_events.py --retag              # re-tag every event from scratch
  python tag_events.py --city İstanbul      # only events in one city
  python tag_events.py --limit 50           # cap (quality spot-check)
  python tag_events.py --model qwen2.5:3b   # use a different Ollama model
"""
import argparse
import asyncio
import sys
import unicodedata

from core.db import Database
from services.tagging_service import (
    build_prompt, call_ollama, load_vocab, ollama_reachable,
)
from services.vault import Vault

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

_DEFAULT_MODEL = "qwen2.5:7b"

_TR_MAP = str.maketrans({
    'ı': 'i', 'İ': 'i', 'ğ': 'g', 'Ğ': 'g',
    'ş': 's', 'Ş': 's', 'ü': 'u', 'Ü': 'u',
    'ö': 'o', 'Ö': 'o', 'ç': 'c', 'Ç': 'c',
})


def _norm_city(location: str) -> str:
    """City token of an event location, normalized for Turkish chars + case.
    'İstanbul, Turkey' -> 'istanbul'."""
    s = (location or "").split(",")[0].strip().lower().translate(_TR_MAP)
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))


def _warmup(model: str) -> None:
    """Load the model into memory before the timed loop (avoids a cold-start
    timeout on the first real batch)."""
    print("Warming up model (loading into memory)…")
    try:
        call_ollama('Reply with JSON only: {"ok": 1}', model)
        print("  model ready")
    except Exception as exc:
        print(f"  warmup call failed ({exc}) — the main loop will retry")


async def main() -> None:
    parser = argparse.ArgumentParser(description="LLM event tagging CLI")
    parser.add_argument("--retag", action="store_true", help="re-tag every event")
    parser.add_argument("--limit", type=int, default=0, help="cap number of events (0 = all)")
    parser.add_argument("--model", default=_DEFAULT_MODEL, help="Ollama model name")
    parser.add_argument("--batch", type=int, default=10, help="events per LLM call")
    parser.add_argument("--city", default="", help="only tag events in this city")
    args = parser.parse_args()

    vocab_slugs, vocab_set = load_vocab()
    print(f"Vocabulary: {len(vocab_slugs)} tags  |  model: {args.model}  |  batch: {args.batch}")

    if not ollama_reachable():
        print("ERROR: Ollama not reachable at localhost:11434.")
        print("Start it with 'ollama serve' and ensure the model is pulled.")
        sys.exit(1)

    vault = Vault()
    db = Database(dsn=vault.get("db_url"))
    await db.connect()

    if args.retag:
        events = await db.get_all_trusted_events()
    else:
        events = await db.get_untagged_trusted_events()

    if args.city:
        target = _norm_city(args.city)
        before = len(events)
        events = [e for e in events if _norm_city(e.location) == target]
        print(f"City filter '{args.city}': {len(events)}/{before} events match")

    if args.limit:
        events = events[: args.limit]

    print(f"Events to tag: {len(events)}")
    if not events:
        await db.disconnect()
        return

    _warmup(args.model)

    tagged = failed = 0
    for start in range(0, len(events), args.batch):
        batch = events[start : start + args.batch]
        prompt = build_prompt(batch, vocab_slugs)

        result = None
        for attempt in (1, 2):
            try:
                result = call_ollama(prompt, args.model)
                break
            except Exception as exc:
                if attempt == 1:
                    print(f"\n  batch @{start}: LLM error ({exc}) — retrying")
                else:
                    print(f"\n  batch @{start}: failed twice ({exc}) — skipping")

        if result is None:
            failed += len(batch)
            continue

        for i, event in enumerate(batch):
            raw = result.get(str(i), result.get(i, []))
            tags = [t for t in raw if t in vocab_set] if isinstance(raw, list) else []
            await db.upsert_event_tags(event.event_key, tags)
            tagged += 1

        print(f"  tagged {tagged}/{len(events)}  (failed {failed})", end="\r")

    print(f"\nDone — tagged {tagged}, failed {failed}.")
    await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
