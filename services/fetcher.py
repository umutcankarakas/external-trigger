"""
Fetcher: reads active sources from the Source Registry, fetches each one
asynchronously on its configured schedule, and publishes RawEvents to the
raw_events topic.

After every polling cycle (all sources attempted), a BatchComplete sentinel is
published so downstream services know the batch is finished and can flush.

Retry strategy: exponential backoff - 2 s -> 4 s -> 8 s (max 3 attempts).
Auth types: "query_param" (e.g. Ticketmaster) or "bearer" (e.g. Eventbrite).

Dedup: each source maintains a set of seen event IDs. Duplicates within the
same batch and across repeated fetches are dropped before reaching Kafka.
"""
import asyncio
import json
import logging
import os
import time as _time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import aiohttp
import requests as _sync_requests

from core.bus import MessageBus
from core.event_log import T_FETCH_OK, T_FETCH_ERR, T_NEW_EVENTS
import core.event_log as elog
from core.models import BatchComplete, RawEvent
from services.source_registry import Source, SourceRegistry
from services.vault import Vault

logger = logging.getLogger(__name__)

# Browser-like headers for unofficial scraping endpoints (Biletix Solr + Passo API).
# These are internal APIs — not official data products — so we mimic a browser
# to avoid basic bot-detection. No API key is required for either.
_BILETIX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://www.biletix.com/search/TURKIYE/tr",
    "Accept-Language": "tr-TR,tr;q=0.9",
}

_PASSO_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://www.passo.com.tr/",
    "Origin": "https://www.passo.com.tr",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "tr-TR,tr;q=0.9",
}

_SEEN_IDS_PATH = "config/seen_ids.json"
_ACTIVITY_PATH = "config/pipeline_activity.json"
_RAW_LOG_PATH  = "config/raw_log.json"
_ACTIVITY_MAX  = 200   # keep last N entries
_RAW_LOG_MAX   = 5000  # enough for several full fetch cycles
# IDs older than this are dropped — Ticketmaster only surfaces upcoming events
# so anything beyond this window will never reappear in the API.
_SEEN_IDS_TTL_DAYS = 60


def _append_raw_log(entries: list) -> None:
    """Append multiple raw-event summary entries to the rolling raw log."""
    try:
        records = json.load(open(_RAW_LOG_PATH, encoding="utf-8")) if os.path.exists(_RAW_LOG_PATH) else []
        records.extend(entries)
        records = records[-_RAW_LOG_MAX:]
        with open(_RAW_LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
    except Exception:
        pass


def _raw_summary(source_id: str, event: dict, status: str, now_iso: str) -> dict:
    """Extract key fields from a raw API event for the pipeline log."""
    name = event.get("name") or event.get("title") or event.get("strEvent") or ""
    eid  = event.get("id") or event.get("idEvent")
    # Ticketmaster: dates.start.{dateTime, localDate}
    dates = event.get("dates", {}).get("start", {})
    date = dates.get("dateTime") or dates.get("localDate", "")
    # PredictHQ: top-level "start"
    if not date:
        date = event.get("start", "")
    # ESPN: top-level "date"
    if not date:
        date = event.get("date", "")
    # TheSportsDB: strTimestamp (preferred) or dateEvent + strTime
    if not date:
        date = event.get("strTimestamp") or ""
        if not date and event.get("dateEvent"):
            date = f"{event['dateEvent']}T{event.get('strTime') or '00:00:00'}"
    # Biletix Solr: top-level "start" field (e.g. "2026-05-16T18:00:00Z")
    if not date:
        date = event.get("start", "")
    # api-sports.io: name from teams, date from fixture.date, id from fixture.id
    if not name and "teams" in event:
        teams = event.get("teams") or {}
        home = (teams.get("home") or {}).get("name", "")
        away = (teams.get("away") or {}).get("name", "")
        name = f"{home} vs {away}".strip(" vs ")
    if not date:
        fix = event.get("fixture") or {}
        date = fix.get("date", "")
        if eid is None:
            eid = fix.get("id")
    return {
        "ts":        now_iso,
        "source_id": source_id,
        "event_id":  str(eid) if eid is not None else "",
        "name":      name,
        "date":      date,
        "status":    status,
        "raw_data":  event,   # full API response for inspection
    }


def _append_activity(entry: dict) -> None:
    """Append a fetch activity record; keep only the last _ACTIVITY_MAX entries."""
    try:
        if os.path.exists(_ACTIVITY_PATH):
            with open(_ACTIVITY_PATH, encoding="utf-8") as f:
                records = json.load(f)
        else:
            records = []
        records.append(entry)
        records = records[-_ACTIVITY_MAX:]
        with open(_ACTIVITY_PATH, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
    except Exception:
        pass  # never let logging break the pipeline


def _load_seen_ids() -> tuple:
    """Load persisted seen-ID sets, pruning entries older than the TTL window.
    Returns (seen_ids, seen_ts) so both structures stay in sync."""
    if not os.path.exists(_SEEN_IDS_PATH):
        return {}, {}
    cutoff = (datetime.utcnow() - timedelta(days=_SEEN_IDS_TTL_DAYS)).isoformat()
    try:
        with open(_SEEN_IDS_PATH, encoding="utf-8-sig") as f:
            data = json.load(f)  # {source_id: {event_id: first_seen_iso}}
    except Exception:
        return {}, {}
    seen_ids: Dict[str, Set[str]] = {}
    seen_ts: Dict[str, Dict[str, str]] = {}
    for source_id, entries in data.items():
        valid = {eid: ts for eid, ts in entries.items() if ts >= cutoff}
        seen_ids[source_id] = set(valid.keys())
        seen_ts[source_id] = valid  # preserve timestamps for next save
    return seen_ids, seen_ts


def _save_seen_ids(seen_ts: Dict[str, Dict[str, str]]) -> None:
    with open(_SEEN_IDS_PATH, "w") as f:
        json.dump(seen_ts, f)


def _resolve_date_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Replace $today and $today+Nd / $today-Nd placeholders with real dates.

    Supported in search_query values:
      "$today"       → today's UTC date, e.g. "2026-05-04"
      "$today+30d"   → 30 days from today
      "$today+30dT"  → same but as ISO datetime with time "T00:00:00Z"
      "$todayT"      → today as "2026-05-04T00:00:00Z"

    Example sources.json usage:
      "startDateTime": "$todayT",  "endDateTime": "$today+30dT"
      "start.gte":     "$today",   "start.lte":   "$today+30d"
    """
    import re
    today = datetime.utcnow().date()

    # Match $today, $today+Nd / $today-Nd, with optional format suffix.
    # Suffixes: T (ISO datetime), _compact (YYYYMMDD, ESPN scoreboard).
    # Uses sub() rather than match() so values can embed multiple placeholders,
    # e.g. "$today_compact-$today+30d_compact" → "20260508-20260607".
    _today_pat = re.compile(r"\$today([+-]\d+d)?(T|_compact)?")

    def _replace_today(m):
        offset_str, fmt = m.group(1), m.group(2)
        days = int(offset_str[:-1]) if offset_str else 0
        d = today + timedelta(days=days)
        if fmt == "T":
            return d.strftime("%Y-%m-%dT23:59:59Z" if days > 0 else "%Y-%m-%dT00:00:00Z")
        if fmt == "_compact":
            return d.strftime("%Y%m%d")
        return d.isoformat()

    result = {}
    for k, v in params.items():
        if isinstance(v, str):
            v = _today_pat.sub(_replace_today, v)
            if v == "$season":
                # Turkish football season runs Aug→May. The "season" param in
                # api-sports.io is the year the season started.
                #   May 2026 → season 2025 (2025-2026 season)
                #   Sep 2026 → season 2026 (2026-2027 season)
                v = str(today.year if today.month >= 7 else today.year - 1)
        result[k] = v
    return result


def _event_id(event: Dict[str, Any]) -> Optional[str]:
    """Extract a stable per-source event ID. Different sources name it differently:
      - Ticketmaster / PredictHQ / ESPN: top-level "id"
      - api-sports.io:                   fixture.id (nested)
      - TheSportsDB:                     "idEvent"
    """
    raw_id = event.get("id") or event.get("idEvent")
    if raw_id is None:
        fix = event.get("fixture")
        if isinstance(fix, dict):
            raw_id = fix.get("id")
    return str(raw_id) if raw_id is not None else None


class Fetcher:
    RETRY_DELAYS = [2, 4, 8]  # seconds between attempts (exponential backoff)

    def __init__(
        self,
        registry: SourceRegistry,
        vault: Vault,
        bus: MessageBus,
        first_cycle_signal: Optional["asyncio.Event"] = None,
        resume_gate: Optional["asyncio.Event"] = None,
    ):
        self.registry = registry
        self.vault = vault
        self.bus = bus
        self._last_fetched: Dict[str, datetime] = {}
        # Persisted across restarts so the same event is never published twice.
        # _seen_ids: fast lookup set; _seen_ts: {event_id: first_seen_iso} for TTL pruning.
        # Both are loaded together so timestamps are never lost on save.
        self._seen_ids, self._seen_ts = _load_seen_ids()
        # Phase-gate coordination with the TaggingService:
        #   first_cycle_signal — set by the fetcher once the initial pull is issued
        #   resume_gate        — awaited by the fetcher after cycle 1; the tagger
        #                        sets it when the bulk tagging pass completes.
        # Pulling pauses during the bulk tag so the initial corpus is a fixed target.
        self._first_cycle_signal = first_cycle_signal
        self._resume_gate = resume_gate

    async def start(self) -> None:
        sources = self.registry.get_active_sources()
        summary = ", ".join(
            f"{s.source_id} every {s.fetch_frequency // 60} min" for s in sources
        )
        logger.info(f"[FETCHER] Started  ({summary})")
        first_cycle = True
        while True:
            # Fetch all sources concurrently; each returns the number of new events published
            counts = await asyncio.gather(*[self._maybe_fetch(s) for s in sources])
            # Only send BatchComplete when at least one source had new events.
            # An empty cycle (all sources skipped or all duplicates) produces no Kafka traffic.
            if any(counts):
                batch_id = datetime.utcnow().isoformat()
                await self.bus.publish("raw_events", BatchComplete(batch_id=batch_id))

            if first_cycle:
                first_cycle = False
                # Tell the tagger the initial pull has been issued
                if self._first_cycle_signal is not None:
                    self._first_cycle_signal.set()
                # Pause until the bulk tagging pass finishes (Phase 1 → Phase 2)
                if self._resume_gate is not None and not self._resume_gate.is_set():
                    logger.info("[FETCHER] initial pull issued — pausing until bulk tagging completes")
                    await self._resume_gate.wait()
                    logger.info("[FETCHER] bulk tagging complete — resuming periodic fetch")

            await asyncio.sleep(5)

    async def _maybe_fetch(self, source: Source) -> int:
        """Returns the number of new events published, or 0 if the source was not due."""
        last = self._last_fetched.get(source.source_id)
        if last and (datetime.utcnow() - last).total_seconds() < source.fetch_frequency:
            return 0
        return await self._fetch_with_retry(source)

    async def _fetch_with_retry(self, source: Source) -> int:
        """Returns the number of new events published, or 0 on total failure."""
        for attempt, delay in enumerate(self.RETRY_DELAYS):
            try:
                events = await self._fetch(source)
                self._last_fetched[source.source_id] = datetime.utcnow()

                seen = self._seen_ids.setdefault(source.source_id, set())
                ts_map = self._seen_ts.setdefault(source.source_id, {})
                now_iso = datetime.utcnow().isoformat()
                new_events = []
                raw_entries = []
                for event in events:
                    eid = _event_id(event)
                    if eid is None or eid not in seen:
                        if eid:
                            seen.add(eid)
                            ts_map[eid] = now_iso
                        new_events.append(event)
                        raw_entries.append(_raw_summary(source.source_id, event, "new", now_iso))
                    else:
                        raw_entries.append(_raw_summary(source.source_id, event, "seen", now_iso))

                _append_raw_log(raw_entries)
                skipped = len(events) - len(new_events)
                for event in new_events:
                    await self.bus.publish(
                        "raw_events",
                        RawEvent(source_id=source.source_id, raw_data=event),
                    )

                _save_seen_ids(self._seen_ts)
                _append_activity({
                    "ts": datetime.utcnow().isoformat(timespec="seconds"),
                    "source_id": source.source_id,
                    "fetched": len(events),
                    "new": len(new_events),
                    "skipped": skipped,
                    "status": "ok",
                })

                if skipped:
                    logger.info(
                        f"[FETCHER] {source.source_id}: {len(new_events)} new, "
                        f"{skipped} duplicates skipped"
                    )
                else:
                    logger.info(
                        f"[FETCHER] {source.source_id}: fetched {len(new_events)} events"
                    )
                elog.info(
                    T_FETCH_OK,
                    f"{source.source_id}: {len(new_events)} new, {skipped} skipped "
                    f"({len(events)} fetched total)",
                    {"source_id": source.source_id, "fetched": len(events),
                     "new": len(new_events), "skipped": skipped},
                )
                if len(new_events) > 0:
                    elog.info(
                        T_NEW_EVENTS,
                        f"{source.source_id}: {len(new_events)} new event(s) entered pipeline",
                        {"source_id": source.source_id, "count": len(new_events)},
                    )
                return len(new_events)
            except Exception as exc:
                if attempt < len(self.RETRY_DELAYS) - 1:
                    logger.warning(
                        f"[FETCHER] {source.source_id}: attempt {attempt + 1} failed "
                        f"({exc}), retrying in {delay}s"
                    )
                    elog.warn(
                        T_FETCH_ERR,
                        f"{source.source_id}: attempt {attempt + 1} failed — {exc}",
                        {"source_id": source.source_id, "attempt": attempt + 1, "error": str(exc)},
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"[FETCHER] {source.source_id}: all retries exhausted - {exc}"
                    )
                    elog.error(
                        T_FETCH_ERR,
                        f"{source.source_id}: all retries exhausted — {exc}",
                        {"source_id": source.source_id, "error": str(exc)},
                    )
                    _append_activity({
                        "ts": datetime.utcnow().isoformat(timespec="seconds"),
                        "source_id": source.source_id,
                        "fetched": 0,
                        "new": 0,
                        "skipped": 0,
                        "status": "error",
                        "error": str(exc),
                    })
        return 0

    async def _fetch(self, source: Source) -> List[Dict[str, Any]]:
        # File-based sources (used for mock/test data) — no HTTP needed
        if source.endpoint.startswith("file://"):
            path = source.endpoint[len("file://"):]
            with open(path, encoding="utf-8") as f:
                return json.load(f)

        api_key: Optional[str] = self.vault.get(source.auth_ref) if source.auth_ref else None

        params = _resolve_date_params(dict(source.search_query))
        headers: Dict[str, str] = {}

        if api_key:
            if source.auth_type == "bearer":
                headers["Authorization"] = f"Bearer {api_key}"
            elif source.auth_type.startswith("header:"):
                # Generic header auth — auth_type encodes the header name.
                # e.g. "header:x-apisports-key" → headers["x-apisports-key"] = api_key
                header_name = source.auth_type[len("header:"):]
                headers[header_name] = api_key
            else:  # "query_param"
                params["apikey"] = api_key

        if source.source_type == "ticketmaster":
            return await self._fetch_all_tm(source.endpoint, params, headers)
        if source.source_type == "predicthq":
            return await self._fetch_all_phq(source.endpoint, params, headers)
        if source.source_type == "apisports":
            return await self._fetch_all_apisports(source.endpoint, params, headers)
        if source.source_type == "biletix":
            return await self._fetch_all_biletix(source.endpoint)
        if source.source_type == "passo":
            return await self._fetch_all_passo(source.endpoint)

        # Generic single-page fallback
        async with aiohttp.ClientSession() as session:
            async with session.get(
                source.endpoint, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        if isinstance(data, dict):
            # Some APIs (e.g. TheSportsDB) return null instead of [] when empty
            if "events" in data:
                return data["events"] or []
            if "results" in data:
                return data["results"] or []
        return data if isinstance(data, list) else []

    async def _fetch_all_tm(
        self, endpoint: str, params: dict, headers: dict
    ) -> List[Dict[str, Any]]:
        """Paginate through all Ticketmaster results (max 200/page)."""
        params = {**params, "size": 200}
        all_events: List[Dict[str, Any]] = []
        page = 0
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession() as session:
            while True:
                params["page"] = page
                async with session.get(
                    endpoint, params=params, headers=headers, timeout=timeout
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                events = (data.get("_embedded") or {}).get("events", [])
                all_events.extend(events)
                page_info = data.get("page", {})
                total_pages = page_info.get("totalPages", 1)
                logger.info(
                    f"[FETCHER] TM page {page+1}/{total_pages} — {len(events)} events"
                )
                if page >= total_pages - 1 or not events:
                    break
                page += 1
        return all_events

    async def _fetch_all_phq(
        self, endpoint: str, params: dict, headers: dict
    ) -> List[Dict[str, Any]]:
        """Follow PredictHQ 'next' cursor until all results are fetched."""
        params = {**params, "limit": 500}
        all_events: List[Dict[str, Any]] = []
        url: Optional[str] = endpoint
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession() as session:
            while url:
                req_params = params if url == endpoint else {}
                async with session.get(
                    url, params=req_params, headers=headers, timeout=timeout
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                results = data.get("results", [])
                all_events.extend(results)
                url = data.get("next")  # full URL for next page, or None
                logger.info(
                    f"[FETCHER] PHQ batch {len(results)} events "
                    f"(total so far: {len(all_events)})"
                )
        return all_events

    async def _fetch_all_biletix(self, endpoint: str) -> List[Dict[str, Any]]:
        """Paginate Biletix's internal Solr index (500 docs/request).

        Filters in-process to on-sale events with a start date >= today because
        the Solr endpoint doesn't support date-range queries.
        Raw Solr fields used: id, name, status, start (date), venue, city, category, subcategory.
        """
        today_str = datetime.utcnow().date().isoformat()
        batch_size = 500
        all_events: List[Dict[str, Any]] = []
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession() as session:
            # Probe for total count with a 1-row request
            # content_type=None: Biletix Solr returns text/plain despite being JSON
            probe = {"q": "*:*", "wt": "json", "rows": 1, "start": 0}
            async with session.get(
                endpoint, params=probe, headers=_BILETIX_HEADERS, timeout=timeout
            ) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
            total = data["response"]["numFound"]
            logger.info(f"[FETCHER] Biletix: {total} total events in Solr index")

            start = 0
            while start < total:
                req = {"q": "*:*", "wt": "json", "rows": batch_size, "start": start}
                async with session.get(
                    endpoint, params=req, headers=_BILETIX_HEADERS, timeout=timeout
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                docs = data["response"]["docs"]
                for doc in docs:
                    # Only on-sale events
                    if doc.get("status") != "s01_onsale":
                        continue
                    # Only future events (Solr "start" field is UTC ISO e.g. "2026-05-16T18:00:00Z")
                    start_date = (doc.get("start") or "")[:10]
                    if start_date and start_date < today_str:
                        continue
                    all_events.append(doc)
                start += batch_size
                if start < total:
                    await asyncio.sleep(0.3)

        logger.info(f"[FETCHER] Biletix: {len(all_events)} on-sale upcoming events")
        return all_events

    async def _fetch_all_passo(self, endpoint: str) -> List[Dict[str, Any]]:
        """Paginate Passo's internal event listing API via a thread executor.

        Cloudflare on ticketingweb.passo.com.tr blocks aiohttp's TLS fingerprint
        but accepts the same headers from requests (urllib3). Running in an executor
        keeps the pipeline non-blocking while using the proven requests stack.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._fetch_passo_sync, endpoint)

    def _fetch_passo_sync(self, endpoint: str) -> List[Dict[str, Any]]:
        """Synchronous Passo paginator (runs in executor thread)."""
        today_str = datetime.utcnow().date().isoformat()
        batch_size = 100
        language_id = 118  # Turkish
        all_events: List[Dict[str, Any]] = []

        session = _sync_requests.Session()
        session.headers.update(_PASSO_HEADERS)

        # Probe for total count
        probe = session.post(endpoint, json={"LanguageId": language_id, "from": 0, "size": 1}, timeout=15)
        probe.raise_for_status()
        first = probe.json()
        if first.get("isError"):
            raise RuntimeError(f"Passo API error: {first}")
        total = first.get("totalItemCount", 0)
        logger.info(f"[FETCHER] Passo: {total} total events")

        offset = 0
        while offset < total:
            resp = session.post(endpoint, json={"LanguageId": language_id, "from": offset, "size": batch_size}, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data.get("isError"):
                logger.warning(f"[FETCHER] Passo: API error at offset {offset}, skipping batch")
                offset += batch_size
                continue
            items = data.get("valueList") or []
            for item in items:
                event_date = (item.get("date") or "")[:10]
                if not event_date or event_date < today_str:
                    continue
                all_events.append(item)
            offset += batch_size
            if offset < total:
                _time.sleep(0.3)

        # Deduplicate by ID (API can return the same event on multiple pages)
        seen: Set[str] = set()
        unique: List[Dict[str, Any]] = []
        for e in all_events:
            eid = str(e.get("id", ""))
            if eid not in seen:
                seen.add(eid)
                unique.append(e)

        logger.info(f"[FETCHER] Passo: {len(unique)} upcoming unique events")
        return unique

    async def _fetch_all_apisports(
        self, endpoint: str, params: dict, headers: dict
    ) -> List[Dict[str, Any]]:
        """Paginate through api-sports.io results.

        Response shape:
          {"response": [...fixtures...], "paging": {"current": 1, "total": N}}
        Free tier returns max 100 fixtures/page; for a 30-day window of one league
        this fits in a single page comfortably.
        """
        all_events: List[Dict[str, Any]] = []
        page = 1
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession() as session:
            while True:
                req = {**params, "page": page}
                async with session.get(
                    endpoint, params=req, headers=headers, timeout=timeout
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                results = data.get("response", [])
                all_events.extend(results)
                paging = data.get("paging") or {}
                current = paging.get("current", 1)
                total = paging.get("total", 1)
                logger.info(
                    f"[FETCHER] api-sports page {current}/{total} — {len(results)} fixtures"
                )
                if current >= total or not results:
                    break
                page = current + 1
        return all_events
