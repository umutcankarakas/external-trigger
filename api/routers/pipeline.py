"""
Pipeline router — exposes per-stage data for the Pipeline dashboard.

Endpoints:
  GET /api/pipeline/dedupped      — full dedup store (all events before scoring)
  GET /api/pipeline/merge-log     — cross-source merge events (newest first)
  GET /api/pipeline/expiration-log — expired/removed events (newest first)
"""
import json
import os

from fastapi import APIRouter, Request

router = APIRouter()

_RAW_LOG_PATH        = "config/raw_log.json"
_MERGE_LOG_PATH      = "config/merge_log.json"
_EXPIRATION_LOG_PATH = "config/expiration_log.json"
_EVENT_LOG_PATH      = "config/event_log.json"


def _read_log(path: str) -> list:
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return list(reversed(json.load(f)))
    except Exception:
        return []


@router.get("/pipeline/raw-log")
async def get_raw_log():
    return _read_log(_RAW_LOG_PATH)


@router.get("/pipeline/dedupped")
async def get_dedupped(request: Request):
    events = await request.app.state.db.get_all_dedupped_events()
    return [
        {
            "event_key":   e.event_key,
            "name":        e.name,
            "date":        (e.date.isoformat() + "Z") if e.date else None,
            "location":    e.location,
            "venue":       e.venue,
            "sources":     e.sources,
            "source_count": len(e.sources),
            "tags":        e.tags,
            "seen_count":  e.seen_count,
            "last_updated": e.last_updated.isoformat() if e.last_updated else None,
        }
        for e in sorted(events, key=lambda x: x.date or "", reverse=False)
    ]


@router.get("/pipeline/merge-log")
async def get_merge_log():
    return _read_log(_MERGE_LOG_PATH)


@router.get("/pipeline/expiration-log")
async def get_expiration_log():
    return _read_log(_EXPIRATION_LOG_PATH)


@router.get("/logs")
async def get_event_log():
    return _read_log(_EVENT_LOG_PATH)
