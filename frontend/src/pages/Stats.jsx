import { useEffect, useState } from "react";

function StatCard({ label, value, sub }) {
  return (
    <div className="stat-card">
      <div className="label">{label}</div>
      <div className="value">{value ?? "—"}</div>
      {sub && <div className="sub">{sub}</div>}
    </div>
  );
}

function BarChart({ data, max }) {
  return (
    <div>
      {data.map(({ label, count }) => (
        <div className="bar-row" key={label}>
          <span className="bar-label" title={label}>{label}</span>
          <div className="bar-track">
            <div className="bar-fill" style={{ width: `${(count / max) * 100}%` }} />
          </div>
          <span className="bar-count">{count}</span>
        </div>
      ))}
    </div>
  );
}

function ActivityFeed({ records }) {
  if (!records.length) return <p className="muted">No activity yet.</p>;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {records.map((r, i) => {
        const isError = r.status === "error";
        const utc  = r.ts.endsWith("Z") ? r.ts : r.ts + "Z";
        const time = new Date(utc).toLocaleString("en-GB", { dateStyle: "short", timeStyle: "medium" });
        return (
          <div
            key={i}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 10,
              fontSize: "0.82rem",
              padding: "7px 10px",
              borderRadius: 6,
              background: "var(--surface2)",
              borderLeft: `3px solid ${isError ? "var(--red)" : r.new > 0 ? "var(--green)" : "var(--border)"}`,
            }}
          >
            <span style={{ color: "var(--muted)", fontVariantNumeric: "tabular-nums", whiteSpace: "nowrap" }}>
              {time}
            </span>
            <span className={`badge ${isError ? "badge-red" : "badge-blue"}`}>
              {r.source_id}
            </span>
            {isError ? (
              <span style={{ color: "var(--red)" }}>error — {r.error}</span>
            ) : (
              <span style={{ color: "var(--muted)" }}>
                {r.fetched} fetched
                {r.new > 0 && (
                  <span style={{ color: "var(--green)", marginLeft: 6 }}>+{r.new} new</span>
                )}
                {r.skipped > 0 && (
                  <span style={{ marginLeft: 6 }}>{r.skipped} skipped</span>
                )}
              </span>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default function Stats() {
  const [stats,    setStats]    = useState(null);
  const [activity, setActivity] = useState([]);
  const [loading,  setLoading]  = useState(true);

  const loadAll = () => {
    fetch("/api/stats")
      .then((r) => r.json())
      .then(setStats)
      .finally(() => setLoading(false));
    fetch("/api/activity")
      .then((r) => r.json())
      .then(setActivity)
      .catch(() => {});
  };

  useEffect(() => {
    loadAll();
    const id = setInterval(loadAll, 5_000);
    return () => clearInterval(id);
  }, []);

  if (loading && !stats) return <div className="loading">Loading…</div>;

  const sources = stats
    ? Object.entries(stats.sources_breakdown).map(([label, count]) => ({ label, count }))
    : [];
  const maxSrc = sources.reduce((m, s) => Math.max(m, s.count), 1);

  const topLoc = stats?.top_locations ?? [];
  const maxLoc = topLoc.reduce((m, l) => Math.max(m, l.count), 1);

  return (
    <div>
      <div className="page-header">
        <h1>Pipeline Dashboard</h1>
        <p>Refreshes every 5 seconds.</p>
      </div>

      <div className="stat-grid">
        <StatCard label="Trusted Events" value={stats?.total_trusted} />
        <StatCard label="Multi-source"   value={stats?.multi_source_count} sub="events seen from 2+ sources" />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 20 }}>
        <div className="card">
          <h2 style={{ marginBottom: 16, fontSize: "1rem" }}>Events by Source</h2>
          {sources.length === 0
            ? <p className="muted">No data yet.</p>
            : <BarChart data={sources} max={maxSrc} />}
        </div>

        <div className="card">
          <h2 style={{ marginBottom: 16, fontSize: "1rem" }}>Top Cities</h2>
          {topLoc.length === 0
            ? <p className="muted">No data yet.</p>
            : <BarChart data={topLoc.map(l => ({ label: l.city, count: l.count }))} max={maxLoc} />}
        </div>
      </div>

      <div className="card">
        <h2 style={{ marginBottom: 16, fontSize: "1rem" }}>
          Fetch Activity
          <span style={{ marginLeft: 8, fontSize: "0.75rem", color: "var(--muted)", fontWeight: 400 }}>
            last {activity.length} entries
          </span>
        </h2>
        <ActivityFeed records={activity} />
      </div>
    </div>
  );
}
