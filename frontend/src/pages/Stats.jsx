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

export default function Stats() {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);

  const load = () => {
    setLoading(true);
    fetch("/api/stats")
      .then((r) => r.json())
      .then(setStats)
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
    const id = setInterval(load, 10_000);
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
        <p>Refreshes every 10 seconds.</p>
      </div>

      <div className="stat-grid">
        <StatCard label="Trusted Events"   value={stats?.total_trusted}   />
        <StatCard label="Dedupped Events"  value={stats?.total_dedupped}  />
        <StatCard
          label="Avg Trust Score"
          value={stats?.avg_trust_score != null ? stats.avg_trust_score.toFixed(4) : "—"}
          sub={`min ${stats?.min_trust_score ?? "—"} · max ${stats?.max_trust_score ?? "—"}`}
        />
        <StatCard label="Multi-source"     value={stats?.multi_source_count} sub="events seen from 2+ sources" />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
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
    </div>
  );
}
