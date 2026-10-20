import { useEffect, useState, useCallback, useRef } from "react";

const TYPE_LABELS = {
  fetch_ok:       "Fetch",
  fetch_error:    "Fetch Error",
  new_events:     "New Events",
  merge:          "Merge",
  expiration:     "Expiration",
  normalize_fail: "Norm Fail",
  pipeline:       "Pipeline",
  error:          "Error",
};

const TYPE_COLORS = {
  fetch_ok:       "badge-blue",
  fetch_error:    "badge-red",
  new_events:     "badge-green",
  merge:          "badge-purple",
  expiration:     "badge-yellow",
  normalize_fail: "badge-red",
  pipeline:       "badge-gray",
  error:          "badge-red",
};

const LEVEL_COLORS = {
  info:  "var(--green)",
  warn:  "#f59e0b",
  error: "var(--red)",
};

function fmtTs(iso) {
  if (!iso) return "—";
  const utc = iso.endsWith("Z") ? iso : iso + "Z";
  return new Date(utc).toLocaleString("en-GB", {
    dateStyle: "short", timeStyle: "medium",
  });
}

export default function ActivityLog() {
  const [logs,    setLogs]    = useState([]);
  const [search,  setSearch]  = useState("");
  const [typeFilter, setTypeFilter] = useState("all");
  const [levelFilter, setLevelFilter] = useState("all");
  const [paused,  setPaused]  = useState(false);
  const pausedRef = useRef(false);

  const refresh = useCallback(() => {
    if (pausedRef.current) return;
    fetch("/api/logs")
      .then(r => r.json())
      .then(setLogs)
      .catch(() => {});
  }, []);

  useEffect(() => {
    pausedRef.current = paused;
  }, [paused]);

  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 3000);
    return () => clearInterval(t);
  }, [refresh]);

  const allTypes = [...new Set(logs.map(l => l.type))];

  const filtered = logs.filter(l => {
    if (typeFilter  !== "all" && l.type  !== typeFilter)  return false;
    if (levelFilter !== "all" && l.level !== levelFilter) return false;
    if (search) {
      const q = search.toLowerCase();
      const inMsg  = l.message?.toLowerCase().includes(q);
      const inMeta = l.meta ? JSON.stringify(l.meta).toLowerCase().includes(q) : false;
      const inType = l.type?.toLowerCase().includes(q);
      if (!inMsg && !inMeta && !inType) return false;
    }
    return true;
  });

  const counts = { info: 0, warn: 0, error: 0 };
  logs.forEach(l => { if (counts[l.level] !== undefined) counts[l.level]++; });

  return (
    <div>
      <div className="page-header">
        <h1>Activity Log</h1>
        <p>Pipeline'daki tüm önemli olaylar — gerçek zamanlı</p>
      </div>

      {/* Summary chips */}
      <div style={{ display: "flex", gap: 10, marginBottom: 20, flexWrap: "wrap" }}>
        <Chip label="Toplam" value={logs.length} />
        <Chip label="Info"  value={counts.info}  color="var(--green)" />
        <Chip label="Warn"  value={counts.warn}  color="#f59e0b" />
        <Chip label="Error" value={counts.error} color="var(--red)" />
        <div style={{ flex: 1 }} />
        <button
          onClick={() => setPaused(p => !p)}
          style={{
            padding: "6px 14px", borderRadius: 6, border: "1px solid var(--border)",
            background: paused ? "var(--accent)" : "var(--surface)",
            color: paused ? "#fff" : "var(--muted)",
            cursor: "pointer", fontSize: "0.82rem",
          }}
        >
          {paused ? "▶ Resume" : "⏸ Pause"}
        </button>
      </div>

      {/* Filters */}
      <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap", alignItems: "center" }}>
        <input
          type="text"
          placeholder="Search message, source, event name…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{
            flex: "1 1 260px", padding: "7px 12px", borderRadius: 6,
            border: "1px solid var(--border)", background: "var(--surface)",
            color: "var(--text)", fontSize: "0.85rem",
          }}
        />

        <select
          value={levelFilter}
          onChange={e => setLevelFilter(e.target.value)}
          style={{
            padding: "7px 10px", borderRadius: 6,
            border: "1px solid var(--border)", background: "var(--surface)",
            color: "var(--text)", fontSize: "0.85rem", cursor: "pointer",
          }}
        >
          <option value="all">All levels</option>
          <option value="info">Info</option>
          <option value="warn">Warn</option>
          <option value="error">Error</option>
        </select>

        <select
          value={typeFilter}
          onChange={e => setTypeFilter(e.target.value)}
          style={{
            padding: "7px 10px", borderRadius: 6,
            border: "1px solid var(--border)", background: "var(--surface)",
            color: "var(--text)", fontSize: "0.85rem", cursor: "pointer",
          }}
        >
          <option value="all">All types</option>
          {allTypes.map(t => (
            <option key={t} value={t}>{TYPE_LABELS[t] ?? t}</option>
          ))}
        </select>

        {(search || typeFilter !== "all" || levelFilter !== "all") && (
          <button
            onClick={() => { setSearch(""); setTypeFilter("all"); setLevelFilter("all"); }}
            style={{
              padding: "6px 12px", borderRadius: 6, border: "1px solid var(--border)",
              background: "var(--surface)", color: "var(--muted)",
              cursor: "pointer", fontSize: "0.82rem",
            }}
          >
            ✕ Clear
          </button>
        )}

        <span style={{ color: "var(--muted)", fontSize: "0.78rem", marginLeft: 4 }}>
          {filtered.length} / {logs.length} entries
        </span>
      </div>

      {/* Log list */}
      <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
        {filtered.length === 0 && (
          <div style={{ textAlign: "center", padding: 48, color: "var(--muted)" }}>
            {logs.length === 0 ? "Henüz log yok — pipeline çalışınca dolacak." : "Filtrelerle eşleşen log yok."}
          </div>
        )}
        {filtered.map((l, i) => (
          <LogRow key={i} entry={l} search={search} />
        ))}
      </div>
    </div>
  );
}

function LogRow({ entry, search }) {
  const [open, setOpen] = useState(false);
  const levelColor = LEVEL_COLORS[entry.level] ?? "var(--muted)";
  const typeClass  = TYPE_COLORS[entry.type] ?? "badge-gray";
  const hasMeta    = entry.meta && Object.keys(entry.meta).length > 0;

  return (
    <div
      style={{
        borderLeft: `3px solid ${levelColor}`,
        background: "var(--surface)",
        borderRadius: "0 6px 6px 0",
        padding: "7px 12px",
        cursor: hasMeta ? "pointer" : "default",
      }}
      onClick={() => hasMeta && setOpen(o => !o)}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
        <span style={{
          fontSize: "0.72rem", color: "var(--muted)",
          whiteSpace: "nowrap", minWidth: 130, fontVariantNumeric: "tabular-nums",
        }}>
          {fmtTs(entry.ts)}
        </span>

        <span style={{
          fontSize: "0.7rem", fontWeight: 700, textTransform: "uppercase",
          color: levelColor, minWidth: 36,
        }}>
          {entry.level}
        </span>

        <span className={`badge ${typeClass}`} style={{ fontSize: "0.7rem" }}>
          {TYPE_LABELS[entry.type] ?? entry.type}
        </span>

        <span style={{ fontSize: "0.83rem", flex: 1 }}>
          {highlight(entry.message, search)}
        </span>

        {hasMeta && (
          <span style={{ fontSize: "0.7rem", color: "var(--muted)" }}>
            {open ? "▲" : "▼"}
          </span>
        )}
      </div>

      {open && hasMeta && (
        <pre style={{
          marginTop: 8, padding: "8px 12px",
          background: "var(--surface2)", borderRadius: 4,
          fontSize: "0.72rem", color: "var(--muted)",
          lineHeight: 1.6, overflowX: "auto", maxHeight: 300,
        }}>
          {JSON.stringify(entry.meta, null, 2)}
        </pre>
      )}
    </div>
  );
}

function highlight(text, search) {
  if (!search || !text) return text;
  const idx = text.toLowerCase().indexOf(search.toLowerCase());
  if (idx === -1) return text;
  return (
    <>
      {text.slice(0, idx)}
      <mark style={{ background: "rgba(99,102,241,0.35)", borderRadius: 2, padding: "0 1px" }}>
        {text.slice(idx, idx + search.length)}
      </mark>
      {text.slice(idx + search.length)}
    </>
  );
}

function Chip({ label, value, color }) {
  return (
    <div className="card" style={{ padding: "8px 16px", textAlign: "center", minWidth: 70 }}>
      <div style={{ fontSize: "1.2rem", fontWeight: 700, color: color ?? undefined }}>{value}</div>
      <div style={{ fontSize: "0.72rem", color: "var(--muted)" }}>{label}</div>
    </div>
  );
}
