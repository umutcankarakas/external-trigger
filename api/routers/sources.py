import json
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()

_SOURCES_PATH = "config/sources.json"


def _read_sources():
    with open(_SOURCES_PATH, encoding="utf-8") as f:
        return json.load(f)


def _write_sources(data):
    with open(_SOURCES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


class SourceUpdate(BaseModel):
    is_active: Optional[bool] = None
    source_weight: Optional[float] = Field(None, ge=0.0, le=1.0)
    fetch_frequency: Optional[int] = Field(None, ge=10, description="Seconds between fetches")


@router.get("/sources")
async def list_sources():
    return _read_sources()


@router.patch("/sources/{source_id}")
async def update_source(source_id: str, body: SourceUpdate):
    sources = _read_sources()
    source = next((s for s in sources if s["source_id"] == source_id), None)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    if body.is_active is not None:
        source["is_active"] = body.is_active
    if body.source_weight is not None:
        source["source_weight"] = body.source_weight
    if body.fetch_frequency is not None:
        source["fetch_frequency"] = body.fetch_frequency

    _write_sources(sources)
    return source
