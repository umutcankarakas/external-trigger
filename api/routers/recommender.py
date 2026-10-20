"""
Recommender API — survey + BM25 personalised event recommendations.

Endpoints (all under /api/recommender):
  GET  /taxonomy                   — survey options: cities (from the event
                                     corpus) + the controlled vocabulary
                                     (config/event_vocabulary.json).
  POST /preferences                — create or update a user's preferences.
  GET  /preferences/{user_id}      — read a user's stored preferences.
  GET  /recommendations/{user_id}  — top N events for the user.

Ranking — BM25 over LLM-assigned controlled-vocabulary tags:
  - Events are tagged offline by tag_events.py → event_llm_tags table.
  - An in-memory BM25 index is built per request (cheap for ~5k events).
  - The user's interest slugs are the BM25 query; IDF means rare tag matches
    ("arabesque") outweigh common ones ("concert") automatically.

Hard filters (do not affect the BM25 score, only narrow candidates):
  - event city == user city
  - event date in the future
"""
import dataclasses
import json
import re
import unicodedata
from collections import Counter
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from services.recommender_index import RecommenderIndex

router = APIRouter()

_VOCAB_PATH = "config/event_vocabulary.json"


# ── helpers ──────────────────────────────────────────────────────────────────

_TR_MAP = str.maketrans({
    'ı': 'i', 'İ': 'i', 'ğ': 'g', 'Ğ': 'g',
    'ş': 's', 'Ş': 's', 'ü': 'u', 'Ü': 'u',
    'ö': 'o', 'Ö': 'o', 'ç': 'c', 'Ç': 'c',
})


def _norm(s: Optional[str]) -> str:
    """Normalize a string for city comparison (handles Turkish chars)."""
    if not s:
        return ""
    s = s.lower().strip().translate(_TR_MAP)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).strip()


def _event_city(event) -> str:
    """Extract the city (first comma-separated token) from event.location."""
    loc = getattr(event, "location", "") or ""
    return loc.split(",")[0].strip()


def _serialize_event(event) -> dict:
    """Convert a TrustedEvent dataclass to a JSON-safe dict."""
    d = dataclasses.asdict(event)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = (v.isoformat() + "Z") if v else None
    return d


def _load_vocabulary() -> dict:
    with open(_VOCAB_PATH, encoding="utf-8") as f:
        return json.load(f)


def _vocab_slugs() -> set:
    """All valid vocabulary slugs — used to validate user input."""
    data = _load_vocabulary()
    return {t["slug"] for g in data["groups"] for t in g["tags"]}


def _shape_prefs(prefs: dict) -> dict:
    """Collapse the stored categories+tags columns into one `interests` list.

    The recommender treats preferences as a single bag of vocabulary slugs.
    Legacy rows (pre-BM25) stored values across both columns; merging here
    keeps old rows working without a migration.
    """
    interests = list(dict.fromkeys(
        (prefs.get("categories") or []) + (prefs.get("tags") or [])
    ))
    return {
        "user_id": prefs["user_id"],
        "city": prefs["city"],
        "interests": interests,
        "created_at": prefs.get("created_at"),
        "updated_at": prefs.get("updated_at"),
    }


# ── input models ─────────────────────────────────────────────────────────────

class PreferencesIn(BaseModel):
    user_id: str
    city: str
    interests: List[str] = []


# ── routes ───────────────────────────────────────────────────────────────────

@router.get("/recommender/taxonomy")
async def taxonomy(request: Request):
    """Survey options: city list (from the corpus) + controlled vocabulary."""
    events = await request.app.state.db.get_all_trusted_events()
    cities: Counter = Counter()
    for e in events:
        city = _event_city(e)
        # Skip bare country codes ("TR") and obvious junk
        if city and len(city) > 2 and city.upper() != city:
            cities[city] += 1

    return {
        "cities": [c for c, _ in cities.most_common(50)],
        "vocabulary": _load_vocabulary(),
    }


@router.post("/recommender/preferences")
async def save_preferences(prefs: PreferencesIn, request: Request):
    # Keep only valid vocabulary slugs so the BM25 query space stays clean
    valid = _vocab_slugs()
    interests = [i for i in prefs.interests if i in valid]
    # Stored in the `tags` column; `categories` is left empty (vestigial).
    await request.app.state.db.upsert_user_preferences(
        prefs.user_id, prefs.city, [], interests
    )
    return {"ok": True}


@router.get("/recommender/preferences/{user_id}")
async def get_preferences(user_id: str, request: Request):
    prefs = await request.app.state.db.get_user_preferences(user_id)
    if not prefs:
        raise HTTPException(status_code=404, detail="Preferences not found")
    return _shape_prefs(prefs)


@router.get("/recommender/recommendations/{user_id}")
async def recommendations(
    user_id: str,
    request: Request,
    limit: int = Query(20, ge=1, le=100),
):
    prefs = await request.app.state.db.get_user_preferences(user_id)
    if not prefs:
        raise HTTPException(status_code=404, detail="Set preferences first")
    shaped = _shape_prefs(prefs)

    events = await request.app.state.db.get_all_trusted_events()
    event_tags = await request.app.state.db.get_all_event_tags()
    now = datetime.utcnow()
    user_city_norm = _norm(shaped["city"])

    # BM25 index over the whole tagged corpus — IDF reflects global tag rarity.
    index = RecommenderIndex(event_tags)
    scores = index.score(shaped["interests"])

    # Hard filters: same city + future date. These narrow candidates only;
    # they don't change the BM25 score (matches the guide's filter-clause model).
    results = []
    for e in events:
        if not e.date or e.date < now:
            continue
        if _norm(_event_city(e)) != user_city_norm:
            continue
        bm25 = scores.get(e.event_key, 0.0)
        results.append((bm25, e))

    # Rank by BM25 desc, then soonest-first within ties
    results.sort(key=lambda x: (-x[0], x[1].date))
    top = results[:limit]

    return {
        "user_preferences": shaped,
        "total_matching": len(results),
        "tagged_events": len(event_tags),
        "recommendations": [
            {
                "score": round(float(s), 2),
                "event": _serialize_event(e),
                "matched_tags": event_tags.get(e.event_key, []),
            }
            for s, e in top
        ],
    }
