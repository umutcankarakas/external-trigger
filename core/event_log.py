"""
Centralised pipeline event log — written to config/event_log.json.

Each entry:
  ts      – ISO-8601 UTC timestamp (no Z; frontend appends it)
  level   – "info" | "warn" | "error"
  type    – slug identifying the kind of event (see TYPE_* constants)
  message – human-readable one-liner
  meta    – optional dict with structured details

All writes are best-effort: exceptions are silently swallowed so a logging
failure can never crash the pipeline.
"""
import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

_LOG_PATH = "config/event_log.json"
_LOG_MAX  = 2000   # rolling window

# ── type constants ────────────────────────────────────────────────────────────
T_FETCH_OK    = "fetch_ok"
T_FETCH_ERR   = "fetch_error"
T_NEW_EVENTS  = "new_events"
T_MERGE       = "merge"
T_EXPIRATION  = "expiration"
T_NORM_FAIL   = "normalize_fail"
T_PIPELINE    = "pipeline"
T_ERROR       = "error"


def append(
    level: str,
    type_: str,
    message: str,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Append one log entry; never raises."""
    entry: Dict[str, Any] = {
        "ts":      datetime.utcnow().isoformat(timespec="seconds"),
        "level":   level,
        "type":    type_,
        "message": message,
    }
    if meta:
        entry["meta"] = meta
    try:
        records = json.load(open(_LOG_PATH, encoding="utf-8-sig")) if os.path.exists(_LOG_PATH) else []
        records.append(entry)
        records = records[-_LOG_MAX:]
        with open(_LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
    except Exception:
        pass


# ── convenience helpers ───────────────────────────────────────────────────────
def info(type_: str, message: str, meta: Optional[Dict[str, Any]] = None) -> None:
    append("info", type_, message, meta)

def warn(type_: str, message: str, meta: Optional[Dict[str, Any]] = None) -> None:
    append("warn", type_, message, meta)

def error(type_: str, message: str, meta: Optional[Dict[str, Any]] = None) -> None:
    append("error", type_, message, meta)
