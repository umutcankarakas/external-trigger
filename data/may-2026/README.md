# May 2026 event snapshot

Processed output of the pipeline's last May run (14 May 2026): every upcoming event in Turkey as of that day, normalized, de-duplicated across sources, and partly LLM-tagged. Exported from the `trusted_events` and `event_llm_tags` tables.

| File | Format |
|---|---|
| `events.json` | One object per event; list fields are JSON arrays |
| `events.csv` | Same rows; list fields joined with `; ` (UTF-8 with BOM, opens cleanly in Excel) |

## Contents

- **4,420 events**, 1,294 of them reported by two or more sources
- **Events per source:** Biletix 3,437 · Passo 1,152 · PredictHQ 953 · Ticketmaster 938 · ESPN 9 · TheSportsDB 1
- **LLM tags:** 2,049 events (46%); the bulk tagging pass never finished, so `llm_tags` is empty for the rest
- **Top locations:** İstanbul 1,756 · TR (no city) 610 · Ankara 388 · İzmir 283 · Antalya 235

## Columns

| Column | Meaning |
|---|---|
| `event_key` | Stable id: hash of normalized name, day and city |
| `name`, `venue` | As reported by the first source that listed the event |
| `date` | Start time; see the note on time zones below |
| `location` | `City, Turkey`, or `TR` when the source has no city (Passo, TheSportsDB) |
| `category`, `source_tags` | The sources' own categories and tags, before LLM tagging |
| `llm_tags` | Tags from the 37-slug vocabulary in `config/event_vocabulary.json`; empty = not tagged yet |
| `sources`, `source_count` | Which sources reported the event |
| `seen_count` | Number of raw records merged into this event |
| `price_min`, `price_max`, `currency` | Rarely filled; most sources give no price |
| `url` | Ticket link, when the source gives one |
| `lat`, `lng` | Coordinates (Ticketmaster and PredictHQ only) |

## Caveats

- **Time zones are mixed.** Ticketmaster, Biletix, PredictHQ and ESPN dates are UTC; Passo and TheSportsDB dates are Turkish local time (UTC+3). A merged event keeps the date of the source that reported it first.
- **City spelling** varies: 17 events say "Istanbul, Turkey" instead of "İstanbul, Turkey".
- **Placeholder dates:** 8 Biletix listings are dated 2030–2036: test entries ("test", "TEST WEBBOX DENEME 1"), a gift package, and open-ended listings with a dummy date.
