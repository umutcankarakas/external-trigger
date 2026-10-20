"""
Normalizer: consumes raw_events, maps each source's schema to a common
NormalizedEvent shape, and publishes to normalized_events.

BatchComplete sentinels are passed through unchanged so the dedup service
knows when the full batch has been normalized.
"""
import asyncio
import logging
import re
from datetime import datetime, timezone as _tz
from typing import Callable, Dict, List, Optional

from dateutil import parser as _dateutil_parser

from core.bus import MessageBus
import core.event_log as elog
from core.event_log import T_NORM_FAIL
from core.models import BatchComplete, NormalizedEvent, RawEvent
from services.source_registry import SourceRegistry

logger = logging.getLogger(__name__)

# Ticketmaster populates these placeholder names when classification is unknown
_SKIP_CLASSIFICATION_NAMES = {"undefined", "other", "unclassified"}


def _parse_dt(value: str) -> Optional[datetime]:
    """Parse a date string in any reasonable format, always returning naive UTC.

    Returns None if the value is empty or unparseable so the caller can
    drop the event rather than storing a corrupted timestamp.

    If the parsed datetime is timezone-aware (e.g. +03:00 from PredictHQ or
    Z from Ticketmaster), it is first converted to UTC and then the tzinfo is
    stripped so all stored datetimes are comparable with datetime.utcnow().
    If the value has no timezone (bare date or naive datetime), it is kept
    as-is (assumed to already be UTC or close enough).
    """
    if not value:
        return None
    try:
        dt = _dateutil_parser.parse(value)
        if dt.tzinfo is not None:
            dt = dt.astimezone(_tz.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _classification_tags(classifications: list) -> List[str]:
    """Walk the full Ticketmaster classification hierarchy to collect tags."""
    tags: List[str] = []
    for cl in classifications:
        for key in ("segment", "genre", "subGenre", "type", "subType"):
            val = cl.get(key) or {}
            name = val.get("name", "")
            if name and name.lower() not in _SKIP_CLASSIFICATION_NAMES:
                tags.append(name)
    return list(dict.fromkeys(tags))  # deduplicate while preserving order


# ---------------------------------------------------------------------------
# Per-source normalizers
# ---------------------------------------------------------------------------

def _normalize_mock(raw: RawEvent) -> Optional[NormalizedEvent]:
    d = raw.raw_data
    price = d.get("price", {})
    date = _parse_dt(d.get("start_date", ""))
    if date is None:
        return None
    return NormalizedEvent(
        name=d["title"],
        date=date,
        location=f"{d['city']}, {d['country']}",
        venue=d.get("venue", ""),
        # Filter empty strings so category is consistent with the TM normalizer
        category=[n for n in [d.get("category", ""), d.get("subcategory", "")] if n],
        tags=d.get("tags", []),
        source_id=raw.source_id,
        url=d.get("url", ""),
        price_min=price.get("min"),
        price_max=price.get("max"),
        currency=price.get("currency", "TRY"),
        lat=None,
        lng=None,
    )


def _normalize_ticketmaster(raw: RawEvent) -> Optional[NormalizedEvent]:
    d = raw.raw_data
    dates = d.get("dates", {}).get("start", {})
    dt_str = dates.get("dateTime") or dates.get("localDate", "")
    date = _parse_dt(dt_str)
    if date is None:
        return None  # unparseable date — drop the event

    venues = (d.get("_embedded") or {}).get("venues", [{}])
    venue = venues[0] if venues else {}
    city = venue.get("city", {}).get("name", "")
    country = venue.get("country", {}).get("name", "")
    venue_name = venue.get("name", "")

    loc_data = venue.get("location") or {}
    lat = float(loc_data["latitude"]) if loc_data.get("latitude") else None
    lng = float(loc_data["longitude"]) if loc_data.get("longitude") else None

    price_ranges = d.get("priceRanges") or [{}]
    pr = price_ranges[0]

    classifications = d.get("classifications") or []
    cl_tags = _classification_tags(classifications)

    tags = list(dict.fromkeys(cl_tags))

    category = []
    if classifications:
        cl = classifications[0]
        segment = (cl.get("segment") or {}).get("name", "")
        genre = (cl.get("genre") or {}).get("name", "")
        category = [n for n in [segment, genre] if n]

    return NormalizedEvent(
        name=d.get("name", ""),
        date=date,
        location=f"{city}, {country}",
        venue=venue_name,
        category=category,
        tags=tags,
        source_id=raw.source_id,
        url=d.get("url", ""),
        price_min=pr.get("min"),
        price_max=pr.get("max"),
        currency=pr.get("currency", "USD"),
        lat=lat,
        lng=lng,
    )


_COUNTRY_WORDS = {"turkey", "turkiye", "türkiye"}

# Matches a Turkish 5-digit postal code followed by "City" or "District/City"
_POSTAL_RE = re.compile(r'\b(\d{5})\s+([\wÀ-ɏİı]+(?:/[\wÀ-ɏİı]+)?)')


def _city_from_address(address: str) -> str:
    """Extract city name from a PredictHQ formatted_address.

    Examples:
      "Harbiye, Kadırgalar Yokuşu No:4, 34367 Maçka/İstanbul, Turkey" → "İstanbul"
      "Oran, Zülfü Tiğrel Cd. No:3, 06450 Çankaya/Ankara, Türkiye"    → "Ankara"
      "34146 Istanbul, Turkey"                                          → "Istanbul"
      "Yeşilevler Mah. İvedik Cad. 06190 Ankara Turkey"                → "Ankara"
      "D: 90 Hüseyinağa Mh. İstanbul, Turkey"                         → "İstanbul"
    """
    if not address:
        return ""

    # Method 1: Turkish 5-digit postal code — most reliable
    # e.g. "06190 Ankara" → "Ankara", "34367 Maçka/İstanbul" → "İstanbul"
    m = _POSTAL_RE.search(address)
    if m:
        return m.group(2).split("/")[-1].strip()

    # Method 2: strip country suffix, take last comma-delimited segment
    for suffix in (", Turkey", ", Türkiye"):
        if address.endswith(suffix):
            address = address[: -len(suffix)]
            break
    last = address.split(",")[-1].strip()
    if "/" in last:
        last = last.split("/")[-1].strip()
    words = last.split()
    if words and words[0].isdigit():
        words = words[1:]
    while words and words[-1].lower().rstrip(".,") in _COUNTRY_WORDS:
        words = words[:-1]
    last = " ".join(words).strip()

    # If the segment is a single clean word it's the city — return it
    if last and " " not in last:
        return last

    # Method 3: last resort — in Turkish addresses the city is ALWAYS the last
    # word before the country, e.g. "D: 90 Hüseyinağa Mh. İstanbul" → "İstanbul"
    all_words = address.split()
    while all_words and all_words[-1].lower().rstrip(".,") in _COUNTRY_WORDS:
        all_words = all_words[:-1]
    if all_words:
        last_word = all_words[-1].rstrip(".,;:")
        if "/" in last_word:
            last_word = last_word.split("/")[-1]
        return last_word

    return last


def _normalize_predicthq(raw: RawEvent) -> Optional[NormalizedEvent]:
    d = raw.raw_data
    date = _parse_dt(d.get("start", ""))
    if date is None:
        return None

    # Location: PredictHQ uses GeoJSON [lng, lat]
    geo = d.get("location") or []
    lat = float(geo[1]) if len(geo) >= 2 else None
    lng = float(geo[0]) if len(geo) >= 2 else None

    # Venue: first entity of type "venue"
    entities = d.get("entities") or []
    venue_entity = next((e for e in entities if e.get("type") == "venue"), {})
    venue_name = venue_entity.get("name", "")
    address = venue_entity.get("formatted_address", "")

    # Extract city from full address so dedup keys match Ticketmaster's format.
    # e.g. "Harbiye, Kadırgalar Yokuşu No:4, 34367 Maçka/İstanbul, Turkey" → "İstanbul, Turkey"
    country_str = d.get("country", "TR")
    city = _city_from_address(address)
    if city:
        location = f"{city}, Turkey"
    elif address:
        location = address
    else:
        location = country_str

    # Category and tags
    # PredictHQ tag sources:
    #   category  → top-level type e.g. "concerts", "sports"
    #   labels    → granular descriptors e.g. ["music", "rock", "festival"]
    #   phq_labels → PHQ-specific enrichment labels (present on some plans)
    cat_str = d.get("category", "")
    category = [cat_str] if cat_str else []
    labels = [t for t in (d.get("labels") or []) if isinstance(t, str)]
    phq_labels = [t for t in (d.get("phq_labels") or []) if isinstance(t, str)]
    tag_parts = ([cat_str] if cat_str else []) + labels + phq_labels
    tags = list(dict.fromkeys(t for t in tag_parts if t))

    return NormalizedEvent(
        name=d.get("title", ""),
        date=date,
        location=location,
        venue=venue_name,
        category=category,
        tags=tags,
        source_id=raw.source_id,
        url=d.get("url", ""),
        price_min=None,
        price_max=None,
        currency=None,
        lat=lat,
        lng=lng,
    )


def _normalize_apisports(raw: RawEvent) -> Optional[NormalizedEvent]:
    """Normalize an api-sports.io football fixture into a common event shape.

    Sample shape:
      { "fixture": {"id": ..., "date": "2026-05-08T16:00:00+00:00",
                    "venue": {"name": "...", "city": "..."}},
        "league":  {"name": "Süper Lig", "country": "Turkey", "round": "..."},
        "teams":   {"home": {"name": "..."}, "away": {"name": "..."}} }
    """
    d = raw.raw_data
    fix = d.get("fixture") or {}
    league = d.get("league") or {}
    teams = d.get("teams") or {}

    home = (teams.get("home") or {}).get("name", "")
    away = (teams.get("away") or {}).get("name", "")
    if not home or not away:
        return None

    date = _parse_dt(fix.get("date", ""))
    if date is None:
        return None

    venue_obj = fix.get("venue") or {}
    venue_name = venue_obj.get("name", "")
    city = venue_obj.get("city", "")
    country = league.get("country", "Turkey")
    location = f"{city}, {country}" if city else country

    league_name = league.get("name", "")
    round_name = league.get("round", "")
    # Tags: keep both league and round so it's filterable in the UI
    tags = [t for t in ["football", league_name, round_name] if t]

    return NormalizedEvent(
        name=f"{home} vs {away}",
        date=date,
        location=location,
        venue=venue_name,
        category=["Sports", "Football"],
        tags=tags,
        source_id=raw.source_id,
        url="",  # api-sports doesn't surface ticket URLs
        price_min=None,
        price_max=None,
        currency=None,
        lat=None,
        lng=None,
    )


def _normalize_espn(raw: RawEvent) -> Optional[NormalizedEvent]:
    """Normalize an ESPN scoreboard event (soccer fixture) into a common shape.

    Sample shape:
      { "id": "...", "date": "2026-05-09T17:00Z",
        "name": "Kayserispor at Alanyaspor",
        "competitions": [{
          "venue": {"fullName": "...", "address": {"city": "..."}},
          "competitors": [
            {"team": {"displayName": "..."}, "homeAway": "home"},
            {"team": {"displayName": "..."}, "homeAway": "away"}
          ]
        }],
        "season": {"slug": "..."} }
    """
    d = raw.raw_data
    date = _parse_dt(d.get("date", ""))
    if date is None:
        return None

    comps = d.get("competitions") or []
    comp = comps[0] if comps else {}

    venue_obj = comp.get("venue") or {}
    venue_name = venue_obj.get("fullName", "")
    city = (venue_obj.get("address") or {}).get("city", "")
    location = f"{city}, Turkey" if city else "Turkey"

    # Build "Home vs Away" from competitors so naming matches Ticketmaster /
    # api-sports conventions (ESPN's default name is "Away at Home").
    competitors = comp.get("competitors") or []
    home = next((c for c in competitors if c.get("homeAway") == "home"), {})
    away = next((c for c in competitors if c.get("homeAway") == "away"), {})
    home_name = (home.get("team") or {}).get("displayName", "")
    away_name = (away.get("team") or {}).get("displayName", "")
    if home_name and away_name:
        name = f"{home_name} vs {away_name}"
    else:
        name = d.get("name", "")  # fall back to ESPN's "Away at Home" string

    return NormalizedEvent(
        name=name,
        date=date,
        location=location,
        venue=venue_name,
        category=["Sports", "Football"],
        tags=["football", "Süper Lig"],
        source_id=raw.source_id,
        url="",
        price_min=None,
        price_max=None,
        currency=None,
        lat=None,
        lng=None,
    )


def _normalize_thesportsdb(raw: RawEvent) -> Optional[NormalizedEvent]:
    """Normalize a TheSportsDB event (used for sports leagues ESPN doesn't cover,
    e.g. Türkiye Basketbol Süper Ligi).

    Sample shape:
      { "idEvent": "...", "strEvent": "Anadolu Efes vs ...",
        "strHomeTeam": "...", "strAwayTeam": "...",
        "strTimestamp": "2026-05-08T16:00:00",
        "strVenue": "...", "strCountry": "Turkey",
        "strSport": "Basketball", "strLeague": "..." }

    Note: TheSportsDB doesn't surface a city. We set location to "TR" so the
    dedup pipeline's date-only fallback can still match cross-source events
    by name+date when other sources (e.g. PredictHQ) have the same fixture.
    """
    d = raw.raw_data
    # Prefer the unified timestamp; fallback to date+time pieces
    ts = d.get("strTimestamp")
    if not ts and d.get("dateEvent"):
        ts = f"{d['dateEvent']}T{d.get('strTime') or '00:00:00'}"
    date = _parse_dt(ts) if ts else None
    if date is None:
        return None

    home = d.get("strHomeTeam", "")
    away = d.get("strAwayTeam", "")
    if not home or not away:
        return None

    sport = d.get("strSport") or "Sports"
    league = d.get("strLeague") or ""
    venue = d.get("strVenue") or ""
    # No city in payload — use "TR" so _is_country_code triggers date-only bucket
    location = "TR" if (d.get("strCountry") or "").lower() in ("turkey", "türkiye") else (d.get("strCountry") or "TR")

    tags = [t for t in [sport.lower(), league] if t]

    return NormalizedEvent(
        name=f"{home} vs {away}",
        date=date,
        location=location,
        venue=venue,
        category=["Sports", sport],
        tags=tags,
        source_id=raw.source_id,
        url="",
        price_min=None,
        price_max=None,
        currency=None,
        lat=None,
        lng=None,
    )


_BILETIX_CATEGORY_MAP = {
    "MUSIC":   ["Music", "Concert"],
    "ART":     ["Arts & Theatre"],
    "SPORTS":  ["Sports"],
    "FAMILY":  ["Family"],
    "COMEDY":  ["Comedy"],
    "FILM":    ["Film"],
    "OTHER":   ["Other"],
}

_PASSO_EVENT_BASE = "https://www.passo.com.tr/tr/etkinlik"


def _normalize_biletix(raw: RawEvent) -> Optional[NormalizedEvent]:
    """Normalize a raw Biletix Solr document.

    Key Solr fields:
      start      – UTC ISO date  e.g. "2026-05-16T18:00:00Z"
      name       – event name
      city       – Turkish city name  e.g. "İstanbul"
      venue      – venue name
      category   – MUSIC | ART | SPORTS | FAMILY | COMEDY | FILM | OTHER
      subcategory – "pop$MUSIC" → tags as "pop"
      status     – filtered to "s01_onsale" in the fetcher
    """
    d = raw.raw_data

    # Solr may return text fields as multi-valued lists (e.g. name: ["X"]) — take the first value
    def _str(key: str) -> str:
        v = d.get(key)
        if isinstance(v, list):
            v = v[0] if v else ""
        return v or ""

    name = _str("name").strip()
    if not name:
        return None

    # Solr date field is "start" (not "date" or "start_date")
    date = _parse_dt(_str("start"))
    if date is None or date.year < 2000:
        return None

    city = _str("city")
    venue = _str("venue")
    location = f"{city}, Turkey" if city else "Turkey"

    raw_cat = (_str("category") or "OTHER").upper()
    category = _BILETIX_CATEGORY_MAP.get(raw_cat, ["Other"])

    # Subcategory format: "pop$MUSIC", "tiyatro$ART", "atolye$OTHER"
    # Extract the descriptive part before "$" as a tag
    subcat_raw = _str("subcategory")
    subcat = subcat_raw.split("$")[0].replace("_", " ").strip() if subcat_raw else ""
    tags = [subcat] if subcat and subcat.lower() not in {"other", "diğer"} else []

    return NormalizedEvent(
        name=name,
        date=date,
        location=location,
        venue=venue,
        category=category,
        tags=tags,
        source_id=raw.source_id,
        url=_str("url"),
        price_min=None,
        price_max=None,
        currency=None,
        lat=None,
        lng=None,
    )


def _normalize_passo(raw: RawEvent) -> Optional[NormalizedEvent]:
    """Normalize a raw Passo API item.

    Key fields (camelCase in the raw API response):
      id            – integer event ID
      name          – event name
      date          – local Turkey time, naive ISO  e.g. "2026-05-16T21:00:00"
      venueName     – venue name (no city field in the API)
      seoUrl        – SEO slug for building the ticket URL

    No city is available so location is set to "TR" — this triggers the dedup
    service's date-only fallback bucket. The venue bucket (_by_date_venue) is
    the primary merge path when another source reports the same venue.
    """
    d = raw.raw_data
    name = (d.get("name") or "").strip()
    if not name:
        return None

    # Passo dates are local Turkey time (UTC+3), stored as naive ISO strings.
    # _parse_dt keeps them naive — this is consistent with how TheSportsDB
    # timestamps are handled across the pipeline.
    # Guard against Passo's promo-listing entries that carry date="0001-01-01T00:00:00".
    date = _parse_dt(d.get("date") or "")
    if date is None or date.year < 2000:
        return None

    # Handle both raw camelCase and scraper-renamed snake_case keys
    venue_name = d.get("venueName") or d.get("venue_name") or ""
    seo_url = d.get("seoUrl") or d.get("seo_url") or ""
    event_id = d.get("id", "")
    url = f"{_PASSO_EVENT_BASE}/{seo_url}/{event_id}" if seo_url and event_id else ""

    return NormalizedEvent(
        name=name,
        date=date,
        location="TR",
        venue=venue_name,
        category=["event"],
        tags=[],
        source_id=raw.source_id,
        url=url,
        price_min=None,
        price_max=None,
        currency=None,
        lat=None,
        lng=None,
    )


SOURCE_NORMALIZERS: Dict[str, Callable[[RawEvent], Optional[NormalizedEvent]]] = {
    "mock": _normalize_mock,
    "ticketmaster": _normalize_ticketmaster,
    "predicthq": _normalize_predicthq,
    "apisports": _normalize_apisports,
    "espn": _normalize_espn,
    "thesportsdb": _normalize_thesportsdb,
    "biletix": _normalize_biletix,
    "passo": _normalize_passo,
}


# ---------------------------------------------------------------------------
# Normalizer service
# ---------------------------------------------------------------------------

class Normalizer:
    def __init__(self, bus: MessageBus, registry: Optional[SourceRegistry] = None):
        self.bus = bus
        self.registry = registry
        self._inbox = bus.subscribe("raw_events")

    async def _flush_loop(self) -> None:
        """Logs a batch summary every 2 s instead of one line per event."""
        while True:
            await asyncio.sleep(2.0)
            if self._ok or self._fail:
                msg = f"[NORMALIZER] {self._ok} normalized"
                if self._fail:
                    msg += f", {self._fail} failed"
                logger.info(msg)
                self._ok = self._fail = 0

    async def start(self) -> None:
        logger.info("[NORMALIZER] Started")
        self._ok = self._fail = 0
        asyncio.create_task(self._flush_loop())
        while True:
            message = await self._inbox.get()

            # Pass the sentinel straight through so dedup can flush
            if isinstance(message, BatchComplete):
                await self.bus.publish("normalized_events", message)
                continue

            raw: RawEvent = message
            event = self._normalize(raw)
            if event:
                await self.bus.publish("normalized_events", event)
                self._ok += 1
            else:
                self._fail += 1

    def _normalize(self, raw: RawEvent) -> Optional[NormalizedEvent]:
        source_type = "mock"
        if self.registry:
            src = self.registry.get_source(raw.source_id)
            if src:
                source_type = src.source_type

        fn = SOURCE_NORMALIZERS.get(source_type)
        if fn is None:
            logger.warning(
                f"[NORMALIZER] Unknown source_type '{source_type}' for {raw.source_id} — skipping"
            )
            return None
        try:
            return fn(raw)
        except Exception as exc:
            logger.warning(f"[NORMALIZER] Failed for {raw.source_id}: {exc}")
            elog.warn(
                T_NORM_FAIL,
                f"{raw.source_id}: normalization failed — {exc}",
                {"source_id": raw.source_id, "error": str(exc)},
            )
            return None
