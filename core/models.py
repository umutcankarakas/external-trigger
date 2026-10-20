from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class BatchComplete:
    """Sentinel published by the Fetcher after every polling cycle.
    Flows through the pipeline so each service knows the batch is done:
      Fetcher → raw_events → Normalizer → normalized_events
             → Dedup (flush) → dedupped_events → TrustScore (skip)
    """
    batch_id: str                  # UTC ISO timestamp of the polling cycle
    type: str = "batch_complete"   # discriminator used by bus._from_bytes


@dataclass
class RawEvent:
    source_id: str
    raw_data: Dict[str, Any]
    fetched_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class NormalizedEvent:
    name: str
    date: datetime
    location: str           # "City, Country"
    venue: str
    category: List[str]
    tags: List[str]
    source_id: str
    url: str
    price_min: Optional[float]
    price_max: Optional[float]
    currency: str
    lat: Optional[float]
    lng: Optional[float]


@dataclass
class DeduppedEvent:
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
    lat: Optional[float]
    lng: Optional[float]
    seen_count: int = 1
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
    lat: Optional[float]
    lng: Optional[float]
    trust_score: float
    source_count: int
    seen_count: int
    has_trusted_source: bool
    # Updated every time the score is (re)calculated. Trust score compares this
    # against DeduppedEvent.last_updated to decide whether recalculation is needed.
    last_calculated_at: datetime = field(default_factory=datetime.utcnow)
