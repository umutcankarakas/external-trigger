"""
BM25-based event recommender.

Builds an in-memory BM25 index over events' controlled-vocabulary tags and
ranks them against a user's preference tags. For ~5k events this is the
guide's "lighter alternative" — no Elasticsearch, the index rebuilds in
milliseconds, so it's built fresh per recommendation request.

BM25 (Okapi) gives us the ranking behaviour we want for free:
  - term frequency: an event tagged "jazz" twice isn't really a thing here,
    but overlap count still matters across the query
  - IDF: rare tags ("arabesque") score much higher than common ones
    ("concert"), so niche matches rightly dominate
  - length normalisation: an event with 3 tight tags isn't penalised vs one
    with 8 loose tags
"""
import re
from typing import Dict, List

from rank_bm25 import BM25Okapi

# Placeholder token for events with no tags — keeps the BM25 corpus free of
# empty documents (which would skew average document length). It can never
# match a real query slug, so untagged events simply score 0.
_EMPTY_DOC_TOKEN = "__untagged__"


def slugify(tag: str) -> str:
    """Normalize a tag into a single BM25 token.

    Lowercase, trim, collapse internal whitespace to underscores. Applied
    identically to event tags and user query tags so multi-word tags like
    "live music" stay one token ("live_music") on both sides.
    """
    s = (tag or "").strip().lower()
    s = re.sub(r"\s+", "_", s)
    return s


class RecommenderIndex:
    """In-memory BM25 index over event controlled-vocabulary tags."""

    def __init__(self, event_tags: Dict[str, List[str]]):
        """event_tags: {event_key: [tag, ...]} from the event_llm_tags table."""
        self.event_keys: List[str] = list(event_tags.keys())
        corpus = [
            [slugify(t) for t in event_tags[k] if t] or [_EMPTY_DOC_TOKEN]
            for k in self.event_keys
        ]
        # BM25Okapi floors negative IDF (common terms) via its epsilon param,
        # so over-common tags can't drag a score below zero.
        self.bm25 = BM25Okapi(corpus)

    def score(self, query_tags: List[str]) -> Dict[str, float]:
        """Return {event_key: bm25_score} for the given query tags.

        Events not in the index (e.g. tagged after this index was built) are
        simply absent from the dict — callers should default them to 0.0.
        """
        query = [slugify(t) for t in query_tags if t]
        if not query or not self.event_keys:
            return {}
        scores = self.bm25.get_scores(query)
        return dict(zip(self.event_keys, scores))
