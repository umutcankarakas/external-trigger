from collections import Counter

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/stats")
async def get_stats(request: Request):
    events = await request.app.state.db.get_all_trusted_events()
    dedupped = await request.app.state.db.get_all_dedupped_events()

    if not events:
        return {
            "total_trusted": 0,
            "total_dedupped": 0,
            "avg_trust_score": 0,
            "multi_source_count": 0,
            "sources_breakdown": {},
            "top_locations": [],
        }

    scores = [e.trust_score for e in events]
    source_counter = Counter(s for e in events for s in e.sources)
    location_counter = Counter(e.location.split(",")[0].strip() for e in events if e.location)

    return {
        "total_trusted": len(events),
        "total_dedupped": len(dedupped),
        "avg_trust_score": round(sum(scores) / len(scores), 4),
        "min_trust_score": round(min(scores), 4),
        "max_trust_score": round(max(scores), 4),
        "multi_source_count": sum(1 for e in events if e.source_count > 1),
        "sources_breakdown": dict(source_counter),
        "top_locations": [
            {"city": city, "count": count}
            for city, count in location_counter.most_common(10)
        ],
    }
