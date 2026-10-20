"""
Dedup Service: consumes normalized_events and merges events that refer to the
same real-world occurrence (same name + date + location).

Batch-flush design:
  Events are accumulated in _pending_keys as they arrive. When the BatchComplete
  sentinel arrives (signalling that all sources in a polling cycle have been
  processed), only the pending events are published to dedupped_events — not
  every normalized event individually. This means Kafka receives one message per
  unique event per batch, not one message per source per event.

Deduplication key: SHA-256(name.lower() | date.date() | location.lower())[:16]
"""
import asyncio
import hashlib
import json
import logging
import os
import re
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Set, Tuple

from core.bus import MessageBus
import core.event_log as elog
from core.event_log import T_MERGE
from core.models import BatchComplete, DeduppedEvent, NormalizedEvent

logger = logging.getLogger(__name__)

_MERGE_LOG_PATH = "config/merge_log.json"
_MERGE_LOG_MAX  = 500


def _append_merge_log(entry: dict) -> None:
    try:
        records = json.load(open(_MERGE_LOG_PATH, encoding="utf-8")) if os.path.exists(_MERGE_LOG_PATH) else []
        records.append(entry)
        records = records[-_MERGE_LOG_MAX:]
        with open(_MERGE_LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
    except Exception:
        pass

# String-similarity threshold (handles typos / minor variations).
# Token-set logic below handles structurally different names that share an artist.
_SIM_THRESHOLD = 0.85

# Stopwords: structural words (articles, prepositions) and generic event terms
# that don't help distinguish two events. Filtered out before token comparison.
# Tokens are matched after _norm(), so all entries here must be in normalized form
# (lowercase, no diacritics, no punctuation).
_NAME_STOPWORDS = frozenset({
    # English structural
    "the", "a", "an", "and", "or", "with", "at", "in", "on", "of", "to", "for",
    "by", "vs", "feat", "ft",
    # English event-generic
    "concert", "concerts", "festival", "festivals", "tour", "tours", "show", "shows",
    "live", "presents", "presented", "presenting", "edition",
    # Turkish structural
    "ve", "ile", "ya", "veya", "icin",
    # Turkish event-generic
    "konser", "konseri", "konserleri", "festivali", "etkinlik", "etkinligi",
    "muzik", "sunar", "sunuyor", "sunuyoruz", "turnesi", "turne", "anadolu",
    # Sports club / league suffixes — universal descriptors, not unique IDs.
    # Allows "Anadolu Efes SK" ≡ "Anadolu Efes" and "BSL - Efes" ≡ "Efes".
    "sk", "fc", "fk", "as", "bb", "bk", "bsl", "tbl", "tbsl", "tff",
    "basketbol", "futbol", "basketball", "football", "soccer",
})


def _significant_tokens(name: str) -> Set[str]:
    """Extract identifying tokens from an event name.

    Filters out stopwords (articles, generic event terms in EN/TR), pure digits,
    and tokens shorter than 2 chars. The remaining set is what makes the event
    distinguishable — typically artist or production names.
    """
    if not name:
        return set()
    return {
        t for t in _norm(name).split()
        if len(t) >= 2 and not t.isdigit() and t not in _NAME_STOPWORDS
    }


def _names_likely_same(a: str, b: str) -> bool:
    """Unified name comparison. True when two names plausibly refer to the same event.

    Three independent rules — any one is sufficient:
      1. Identical after normalization (Turkish chars / punct / case).
      2. >= 85% string similarity (typo / minor wording tolerance).
      3. Significant-token containment: one's identifying tokens are a subset
         of the other's. Handles templates ("%100 Müzik Sunar: X" → {x}),
         abbreviations ("Erkan Ogur" ⊂ "Erkan Oğur - İsmail Hakkı..."),
         and venue suffixes ("Sagopa Kajmer at CerModern" ⊃ "Sagopa Kajmer").

    Caller is responsible for narrowing scope first (same date + city/venue),
    so the rule doesn't need to discriminate between unrelated events globally.
    """
    na = _norm(a)
    nb = _norm(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    if SequenceMatcher(None, na, nb).ratio() >= _SIM_THRESHOLD:
        return True
    ta = _significant_tokens(a)
    tb = _significant_tokens(b)
    return bool(ta and tb and (ta <= tb or tb <= ta))


def _prefer(existing: str, incoming: str) -> str:
    """Keep existing value; fill in if empty."""
    return existing if existing else incoming


def _prefer_list(existing: list, incoming: list) -> list:
    """Keep existing list; fill in if empty."""
    return existing if existing else incoming


# Explicit map for Turkish characters that don't decompose via NFKD
_TR_MAP = str.maketrans({
    'ı': 'i', 'İ': 'i',
    'ğ': 'g', 'Ğ': 'g',
    'ş': 's', 'Ş': 's',
    'ü': 'u', 'Ü': 'u',
    'ö': 'o', 'Ö': 'o',
    'ç': 'c', 'Ç': 'c',
})


def _norm(s: str) -> str:
    """Normalize a string for key comparison.

    Handles: Turkish chars, other diacritics, punctuation, extra whitespace.
    'İzmir' == 'izmir' == 'IZMIR', 'Fazıl Say' == 'Fazil Say', etc.
    """
    s = s.lower().strip().translate(_TR_MAP)
    # Decompose remaining accented chars (e.g. é → e + combining accent)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # Replace punctuation/symbols with spaces, collapse whitespace
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _event_key(name: str, date: datetime, location: str) -> str:
    """Hash key for fast exact-match lookup. Uses normalized full name + date + city.
    Variations (templates, abbreviations) are handled via the fuzzy lookup path."""
    city = location.split(",")[0]
    raw = f"{_norm(name)}|{date.date().isoformat()}|{_norm(city)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _is_country_code(location: str) -> bool:
    """True when location carries no city info — just a bare country code like 'TR'."""
    city = location.split(",")[0].strip()
    return len(city) <= 3 and city.upper() == city


class DedupStore:
    def __init__(self):
        self._events: Dict[str, DeduppedEvent] = {}
        # Index 1: "YYYY-MM-DD|city"  — primary fuzzy bucket
        self._by_date_city: Dict[str, List[str]] = {}
        # Index 2: "YYYY-MM-DD|venue" — catches city-name mismatches (e.g. İzmir vs Atakent)
        self._by_date_venue: Dict[str, List[str]] = {}
        # Index 3: "YYYY-MM-DD"       — last resort when location is only a country code (e.g. "TR")
        self._by_date: Dict[str, List[str]] = {}

    def _date_city_key(self, date: datetime, location: str) -> str:
        city = _norm(location.split(",")[0])
        return f"{date.date().isoformat()}|{city}"

    def _date_venue_key(self, date: datetime, venue: str) -> str:
        return f"{date.date().isoformat()}|{_norm(venue)}"

    def _fuzzy_lookup(self, event: NormalizedEvent) -> Optional[str]:
        """Find a matching existing event using a unified name matcher across narrowed scopes.

        Bucket strategy narrows the candidate set so that name comparison only competes
        against events sharing strong locality signals:

          1. Same date + same city   — typical case
          2. Same date + same venue  — catches city-name mismatches (İzmir vs Atakent)
          3. Same date (any location)— only used when location is a bare country code
                                       like "TR" (no city info to bucket on)

        Once narrowed, _names_likely_same handles templates, abbreviations, and typos
        with one rule. No per-bucket thresholds, no special-case suffix stripping.
        """
        candidate_keys: Set[str] = set()
        candidate_keys.update(
            self._by_date_city.get(self._date_city_key(event.date, event.location), [])
        )
        if event.venue:
            candidate_keys.update(
                self._by_date_venue.get(self._date_venue_key(event.date, event.venue), [])
            )
        if _is_country_code(event.location):
            candidate_keys.update(
                self._by_date.get(event.date.date().isoformat(), [])
            )

        for key in candidate_keys:
            candidate = self._events.get(key)
            if candidate and _names_likely_same(event.name, candidate.name):
                return key

        return None

    def upsert(self, event: NormalizedEvent) -> Tuple[DeduppedEvent, bool]:
        """Returns (DeduppedEvent, is_new)."""
        key = _event_key(event.name, event.date, event.location)
        existing = self._events.get(key)

        # Exact miss — try fuzzy match within same date+city or date+venue bucket
        if not existing:
            fuzzy_key = self._fuzzy_lookup(event)
            if fuzzy_key:
                existing = self._events[fuzzy_key]
                key = fuzzy_key

        if existing:
            existing.seen_count += 1

            if event.source_id not in existing.sources:
                existing.sources.append(event.source_id)
                date_iso = existing.date.isoformat() if existing.date else None
                _append_merge_log({
                    "ts": datetime.utcnow().isoformat(timespec="seconds"),
                    "event_key": key,
                    "name": existing.name,
                    "date": date_iso,
                    "location": existing.location,
                    "venue": existing.venue,
                    "new_source": event.source_id,
                    "all_sources": list(existing.sources),
                })
                # Compose a human-readable header — date + location distinguishes
                # different occurrences of the same tour ("Meksika Açmazı in
                # Ankara on 8 May" vs the same tour in İstanbul on 9 May).
                where = existing.location or "—"
                when = existing.date.strftime("%d %b %Y, %H:%M") if existing.date else "—"
                elog.info(
                    T_MERGE,
                    f"Merged '{existing.name}' @ {where} ({when}) — {' + '.join(existing.sources)}",
                    {
                        "event_key": key,
                        "name": existing.name,
                        "date": date_iso,
                        "location": existing.location,
                        "venue": existing.venue,
                        "new_source": event.source_id,
                        "all_sources": list(existing.sources),
                    },
                )

            # Text fields: keep first non-empty value
            existing.name     = _prefer(existing.name,     event.name)
            existing.url      = _prefer(existing.url,      event.url)
            existing.venue    = _prefer(existing.venue,    event.venue)
            existing.currency = _prefer(existing.currency, event.currency)
            existing.category = _prefer_list(existing.category, event.category)

            # Price: always keep the tightest bounds across all sources
            if event.price_min is not None:
                existing.price_min = (
                    min(existing.price_min, event.price_min)
                    if existing.price_min is not None else event.price_min
                )
            if event.price_max is not None:
                existing.price_max = (
                    max(existing.price_max, event.price_max)
                    if existing.price_max is not None else event.price_max
                )

            # Tags: always union so every source's classification contributes
            existing.tags = list(dict.fromkeys(existing.tags + event.tags))

            # Coordinates: fill in first non-null
            if existing.lat is None and event.lat is not None:
                existing.lat = event.lat
            if existing.lng is None and event.lng is not None:
                existing.lng = event.lng

            existing.last_updated = datetime.utcnow()
            return existing, False

        new_event = DeduppedEvent(
            event_key=key,
            name=event.name,
            date=event.date,
            location=event.location,
            venue=event.venue,
            category=event.category,
            tags=event.tags,
            sources=[event.source_id],
            price_min=event.price_min,
            price_max=event.price_max,
            currency=event.currency,
            url=event.url,
            lat=event.lat,
            lng=event.lng,
        )
        self._events[key] = new_event
        dc_key = self._date_city_key(event.date, event.location)
        self._by_date_city.setdefault(dc_key, []).append(key)
        if event.venue:
            dv_key = self._date_venue_key(event.date, event.venue)
            self._by_date_venue.setdefault(dv_key, []).append(key)
        # Always index by date so country-code-only events can find matches
        self._by_date.setdefault(event.date.date().isoformat(), []).append(key)
        return new_event, True

    def get(self, event_key: str) -> Optional[DeduppedEvent]:
        return self._events.get(event_key)

    def load(self, event: DeduppedEvent) -> None:
        """Pre-warm the store with an existing event loaded from the database.
        Used on startup so cross-source dedup survives pipeline restarts."""
        self._events[event.event_key] = event
        if event.date and event.location:
            dc_key = self._date_city_key(event.date, event.location)
            bucket = self._by_date_city.setdefault(dc_key, [])
            if event.event_key not in bucket:
                bucket.append(event.event_key)
        if event.date and event.venue:
            dv_key = self._date_venue_key(event.date, event.venue)
            bucket = self._by_date_venue.setdefault(dv_key, [])
            if event.event_key not in bucket:
                bucket.append(event.event_key)
        if event.date:
            bucket = self._by_date.setdefault(event.date.date().isoformat(), [])
            if event.event_key not in bucket:
                bucket.append(event.event_key)

    def remove(self, event_key: str) -> None:
        self._events.pop(event_key, None)

    def get_all(self) -> List[DeduppedEvent]:
        return list(self._events.values())

    def __len__(self) -> int:
        return len(self._events)


class DedupService:
    def __init__(self, bus: MessageBus):
        self.bus = bus
        self.store = DedupStore()
        self._inbox = bus.subscribe("normalized_events")
        # Tracks event keys updated in the current batch (since last BatchComplete).
        # Only these events are published when the sentinel arrives.
        self._pending_keys: Set[str] = set()

    async def start(self) -> None:
        logger.info("[DEDUP] Started")
        self._new_count = self._merged_count = 0
        while True:
            message = await self._inbox.get()

            if isinstance(message, BatchComplete):
                await self._flush_batch(message)
                continue

            event: NormalizedEvent = message
            dedupped, is_new = self.store.upsert(event)
            # Mark this event as pending — it will be published on the next flush
            self._pending_keys.add(dedupped.event_key)
            if is_new:
                self._new_count += 1
            else:
                self._merged_count += 1

    async def _flush_batch(self, marker: BatchComplete) -> None:
        """Publish all events updated in this batch, then pass the sentinel downstream.

        Only events in _pending_keys are published — not the entire store.
        This means Kafka receives exactly one message per unique event per batch,
        regardless of how many sources reported it.
        """
        if self._pending_keys:
            for key in self._pending_keys:
                event = self.store.get(key)
                if event:
                    await self.bus.publish("dedupped_events", event)
            logger.info(
                f"[DEDUP] Flushed {len(self._pending_keys)} events "
                f"({self._new_count} new, {self._merged_count} merged) "
                f"| store: {len(self.store)}"
            )
            self._pending_keys.clear()
            self._new_count = self._merged_count = 0

        # Pass the sentinel downstream so trust score is aware the batch ended
        await self.bus.publish("dedupped_events", marker)
