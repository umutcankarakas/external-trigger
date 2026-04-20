from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class RawEvent:
    source_id: str
    raw_data: Dict[str, Any]
    fetched_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class NormalizedEvent:
    event_id: str
    name: str
    date: datetime
    location: str      # "City, Country"
    venue: str
    category: List[str]
    tags: List[str]
    source_id: str
    url: str
    price_min: Optional[float]
    price_max: Optional[float]
    currency: str
    raw_data: Dict[str, Any]
    fetched_at: datetime
    normalized_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DeduppedEvent:
    event_key: str
    name: str
    date: datetime
    location: str
    venue: str
    category: List[str]
    tags: List[str]
    sources: List[str]   # all source_ids that reported this event
    price_min: Optional[float]
    price_max: Optional[float]
    currency: str
    url: str
    first_seen: datetime
    seen_count: int = 1  # total ingestion count across all sources and refreshes
    last_updated: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TrustedEvent:
    event_key: str
    name: str
    date: datetime
    location: str
    venue: str
    category: List[str]
    tags: List[str]
    sources: List[str]
    price_min: Optional[float]
    price_max: Optional[float]
    currency: str
    url: str
    trust_score: float
    source_count: int
    seen_count: int      # total ingestion count, passed through from dedup
    has_trusted_source: bool  # true if any source has weight >= TRUSTED_SOURCE_THRESHOLD
    first_seen: datetime
    scored_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Notification:
    user_id: str
    event_key: str
    event_name: str
    event_date: str
    event_location: str
    relevance_score: float
    reasoning: str
    notified_at: datetime = field(default_factory=datetime.utcnow)
