import { useEffect, useState } from "react";

export default function Sources() {
  const [sources, setSources] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/sources")
      .then((r) => r.json())
      .then(setSources)
      .finally(() => setLoading(false));
  }, []);

  const handleSave = (updatedSource) => {
    setSources((prev) =>
      prev.map((s) => (s.source_id === updatedSource.source_id ? updatedSource : s))
    );
  };

  if (loading) return <div className="loading">Loading…</div>;

  return (
    <div>
      <div className="page-header">
        <h1>Sources</h1>
        <p>Manage data source configuration. Changes take effect on the next fetch cycle.</p>
      </div>

      <div className="sources-grid">
        {sources.map((src) => (
          <SourceCard key={src.source_id} source={src} onSave={handleSave} />
        ))}
      </div>
    </div>
  );
}

function SourceCard({ source, onSave }) {
  const [isActive,       setIsActive]       = useState(source.is_active);
  const [fetchFrequency, setFetchFrequency] = useState(source.fetch_frequency);
  const [saving,         setSaving]         = useState(false);
  const [saved,          setSaved]          = useState(false);

  const dirty =
    isActive !== source.is_active ||
    Number(fetchFrequency) !== source.fetch_frequency;

  const save = async () => {
    setSaving(true);
    try {
      const res = await fetch(`/api/sources/${source.source_id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          is_active:       isActive,
          fetch_frequency: Number(fetchFrequency),
        }),
      });
      if (!res.ok) throw new Error("Save failed");
      const updated = await res.json();
      onSave(updated);
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (err) {
      alert("Failed to save: " + err.message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="source-card">
      <div className="source-card-header">
        <div>
          <h3>{source.name || source.source_id}</h3>
          <div className="source-id">{source.source_id} · {source.source_type}</div>
        </div>
        <label className="toggle" title={isActive ? "Active" : "Inactive"}>
          <input
            type="checkbox"
            checked={isActive}
            onChange={(e) => setIsActive(e.target.checked)}
          />
          <span className="toggle-slider" />
        </label>
      </div>

      <div className="source-fields">
        <div className="source-field">
          <label>Frequency (s)</label>
          <input
            type="number"
            min={10}
            step={10}
            value={fetchFrequency}
            onChange={(e) => setFetchFrequency(e.target.value)}
          />
        </div>

        {source.base_url && (
          <div className="source-field">
            <label>Base URL</label>
            <span style={{ fontSize: "0.78rem", color: "var(--muted)", wordBreak: "break-all" }}>
              {source.base_url}
            </span>
          </div>
        )}
      </div>

      <button
        className="btn-save"
        onClick={save}
        disabled={!dirty || saving}
      >
        {saving ? "Saving…" : saved ? "✓ Saved" : "Save changes"}
      </button>
    </div>
  );
}
