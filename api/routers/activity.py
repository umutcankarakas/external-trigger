import json
import os

from fastapi import APIRouter

router = APIRouter()

_ACTIVITY_PATH = "config/pipeline_activity.json"


@router.get("/activity")
async def get_activity():
    if not os.path.exists(_ACTIVITY_PATH):
        return []
    with open(_ACTIVITY_PATH) as f:
        records = json.load(f)
    # newest first
    return list(reversed(records))
