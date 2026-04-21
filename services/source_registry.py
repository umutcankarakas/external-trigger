import json
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class Source:
    source_id: str
    endpoint: str
    search_query: Dict
    fetch_frequency: int   # seconds between fetches
    last_fetched_at: Optional[str]
    is_active: bool
    auth_ref: Optional[str]
    category: List[str]
    source_weight: float
    source_type: str
    auth_type: str = "query_param"  # "query_param" | "bearer"


class SourceRegistry:
    def __init__(self, sources_path: str = "config/sources.json"):
        with open(sources_path) as f:
            data = json.load(f)
        self._sources: Dict[str, Source] = {s["source_id"]: Source(**s) for s in data}

    def get_active_sources(self) -> List[Source]:
        return [s for s in self._sources.values() if s.is_active]

    def get_source(self, source_id: str) -> Optional[Source]:
        return self._sources.get(source_id)

    def get_weight(self, source_id: str) -> float:
        src = self._sources.get(source_id)
        return src.source_weight if src else 0.5
