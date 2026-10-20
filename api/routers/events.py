import dataclasses
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


def _serialize(obj) -> dict:
    """Convert a dataclass to a JSON-safe dict.
    Datetime fields are serialised with a 'Z' suffix so browsers parse them
    as UTC and display them in the user's local timezone correctly.
    """
    d = dataclasses.asdict(obj)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = (v.isoformat() + "Z") if v else None
    return d


@router.get("/events")
async def list_events(
    request: Request,
    search: Optional[str] = Query(None, description="Search in name, venue, location"),
    location: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    min_trust: float = Query(0.0, ge=0.0, le=1.0),
    date_from: Optional[str] = Query(None, description="ISO date e.g. 2026-01-01"),
    date_to: Optional[str] = Query(None),
):
    events = await request.app.state.db.get_all_trusted_events()

    # Apply filters in Python (dataset is small for POC)
    if search:
        s = search.lower()
        events = [
            e for e in events
            if s in e.name.lower()
            or s in (e.venue or "").lower()
            or s in (e.location or "").lower()
        ]
    if location:
        events = [e for e in events if location.lower() in (e.location or "").lower()]
    if source:
        events = [e for e in events if source in e.sources]
    if min_trust:
        events = [e for e in events if e.trust_score >= min_trust]
    if date_from:
        dt = datetime.fromisoformat(date_from)
        events = [e for e in events if e.date and e.date >= dt]
    if date_to:
        dt = datetime.fromisoformat(date_to)
        events = [e for e in events if e.date and e.date <= dt]

    return [_serialize(e) for e in events]


@router.get("/events/{event_key}")
async def get_event(event_key: str, request: Request):
    events = await request.app.state.db.get_all_trusted_events()
    event = next((e for e in events if e.event_key == event_key), None)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return _serialize(event)
