import { useEffect, useState, useCallback } from "react";

function trustBadge(score) {
  if (score >= 0.75) return <span className="badge badge-green">{score.toFixed(3)}</span>;
  if (score >= 0.45) return <span className="badge badge-yellow">{score.toFixed(3)}</span>;
  return <span className="badge badge-red">{score.toFixed(3)}</span>;
}

function fmtDate(iso) {
  if (!iso) return <span className="muted">—</span>;
  const d = new Date(iso);
  return d.toLocaleString("en-GB", { dateStyle: "medium", timeStyle: "short" });
}

export default function Events() {
  const [events, setEvents]     = useState([]);
  const [loading, setLoading]   = useState(false);
  const [selected, setSelected] = useState(null);

  // filter state
  const [search,   setSearch]   = useState("");
  const [location, setLocation] = useState("");
  const [source,   setSource]   = useState("");
  const [minTrust, setMinTrust] = useState("0");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo,   setDateTo]   = useState("");

  const load = useCallback(() => {
    setLoading(true);
    const params = new URLSearchParams();
    if (search)   params.set("search",    search);
    if (location) params.set("location",  location);
    if (source)   params.set("source",    source);
    if (minTrust && minTrust !== "0") params.set("min_trust", minTrust);
    if (dateFrom) params.set("date_from", dateFrom);
    if (dateTo)   params.set("date_to",   dateTo);

    fetch(`/api/events?${params}`)
      .then((r) => r.json())
      .then(setEvents)
      .finally(() => setLoading(false));
  }, [search, location, source, minTrust, dateFrom, dateTo]);

  useEffect(() => { load(); }, [load]);

  return (
    <div>
      <div className="page-header">
        <h1>Events</h1>
        <p>{events.length} trusted events</p>
      </div>

      <div className="filters">
        <input
          type="text"
          placeholder="Search name / venue / location…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <input
          type="text"
          placeholder="City / location"
          value={location}
          onChange={(e) => setLocation(e.target.value)}
          style={{ minWidth: 140 }}
        />
        <input
          type="text"
          placeholder="Source ID"
          value={source}
          onChange={(e) => setSource(e.target.value)}
          style={{ minWidth: 130 }}
        />
        <select value={minTrust} onChange={(e) => setMinTrust(e.target.value)}>
          <option value="0">Min trust: any</option>
          <option value="0.3">≥ 0.30</option>
          <option value="0.5">≥ 0.50</option>
          <option value="0.7">≥ 0.70</option>
          <option value="0.9">≥ 0.90</option>
        </select>
        <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} title="From date" />
        <input type="date" value={dateTo}   onChange={(e) => setDateTo(e.target.value)}   title="To date"   />
      </div>

      {loading && <p className="muted" style={{ marginBottom: 10 }}>Loading…</p>}

      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Date</th>
              <th>Location</th>
              <th>Venue</th>
              <th>Trust</th>
              <th>Sources</th>
            </tr>
          </thead>
          <tbody>
            {events.length === 0 && !loading && (
              <tr><td colSpan={6} style={{ textAlign: "center", padding: 32, color: "var(--muted)" }}>No events found.</td></tr>
            )}
            {events.map((e) => (
              <tr key={e.event_key} style={{ cursor: "pointer" }} onClick={() => setSelected(e)}>
                <td style={{ maxWidth: 240, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{e.name}</td>
                <td style={{ whiteSpace: "nowrap" }}>{fmtDate(e.date)}</td>
                <td>{e.location || <span className="muted">—</span>}</td>
                <td>{e.venue   || <span className="muted">—</span>}</td>
                <td>{trustBadge(e.trust_score)}</td>
                <td>
                  {(e.sources || []).map((s) => (
                    <span key={s} className="badge badge-blue" style={{ marginRight: 4 }}>{s}</span>
                  ))}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {selected && (
        <EventModal event={selected} onClose={() => setSelected(null)} />
      )}
    </div>
  );
}

function EventModal({ event: e, onClose }) {
  return (
    <div
      style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,.6)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 1000,
      }}
      onClick={onClose}
    >
      <div
        className="card"
        style={{ width: 560, maxHeight: "80vh", overflowY: "auto" }}
        onClick={(ev) => ev.stopPropagation()}
      >
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 16 }}>
          <h2 style={{ fontSize: "1.1rem" }}>{e.name}</h2>
          <button onClick={onClose} style={{ background: "none", border: "none", color: "var(--muted)", fontSize: "1.2rem" }}>✕</button>
        </div>

        <Row label="Key"         value={e.event_key} mono />
        <Row label="Date"        value={e.date ? new Date(e.date).toLocaleString("en-GB", { dateStyle: "full", timeStyle: "short" }) : "—"} />
        <Row label="Location"    value={e.location} />
        <Row label="Venue"       value={e.venue} />
        <Row label="URL"         value={e.url} link />
        <Row label="Trust Score" value={e.trust_score?.toFixed(4)} />
        <Row label="Source count" value={e.source_count} />
        <Row label="Sources"     value={(e.sources || []).join(", ")} />
        <Row label="Category"    value={(e.category || []).join(", ")} />
        <Row label="Tags"        value={(e.tags || []).join(", ")} />
        {e.min_price != null && (
          <Row label="Price" value={`${e.min_price}${e.max_price && e.max_price !== e.min_price ? ` – ${e.max_price}` : ""} ${e.currency || ""}`.trim()} />
        )}
        <Row label="Last updated" value={e.last_updated ? new Date(e.last_updated).toLocaleString("en-GB") : "—"} />
      </div>
    </div>
  );
}

function Row({ label, value, mono, link }) {
  if (!value) return null;
  return (
    <div style={{ display: "flex", gap: 12, marginBottom: 8, fontSize: "0.875rem" }}>
      <span style={{ width: 110, flexShrink: 0, color: "var(--muted)" }}>{label}</span>
      {link
        ? <a href={value} target="_blank" rel="noreferrer" style={{ color: "var(--accent)", wordBreak: "break-all" }}>{value}</a>
        : <span style={{ fontFamily: mono ? "monospace" : "inherit", wordBreak: "break-all" }}>{value}</span>
      }
    </div>
  );
}
