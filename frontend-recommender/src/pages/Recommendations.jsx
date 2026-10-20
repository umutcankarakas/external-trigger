import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { api, getUserId, resetUser } from "../api.js";

/** Main page: user's interests + top-20 BM25-ranked events for their city. */
export default function Recommendations() {
  const [data, setData] = useState(null);
  const [labels, setLabels] = useState({}); // slug -> display label
  const [error, setError] = useState(null);
  const navigate = useNavigate();

  useEffect(() => {
    Promise.all([
      api.recommendations(getUserId(), 20),
      api.taxonomy().catch(() => null),
    ]).then(([rec, tax]) => {
      setData(rec);
      if (tax && tax.vocabulary) {
        const map = {};
        tax.vocabulary.groups.forEach(g =>
          g.tags.forEach(t => { map[t.slug] = t.label; })
        );
        setLabels(map);
      }
    }).catch(e => setError(String(e)));
  }, []);

  const label = (slug) => labels[slug] || slug.replace(/_/g, " ");

  if (error) {
    return (
      <div className="page">
        <div className="error">{error}</div>
        <button className="primary" onClick={() => navigate("/survey")}>Back to survey</button>
      </div>
    );
  }
  if (!data) return <div className="centered muted">Loading recommendations…</div>;

  const { user_preferences: prefs, recommendations, total_matching, tagged_events } = data;

  return (
    <div className="page">
      <header className="topbar">
        <h1>Your event recommendations</h1>
        <div className="topbar-actions">
          <button className="secondary" onClick={() => navigate("/survey")}>Edit preferences</button>
          <button className="ghost" onClick={() => { resetUser(); navigate("/survey"); }}>Start over</button>
        </div>
      </header>

      <div className="layout">
        {/* Left: preferences */}
        <aside className="prefs-panel">
          <h3>Your preferences</h3>

          <div className="pref-block">
            <div className="pref-label">City</div>
            <div className="pref-value city-value">📍 {prefs.city || "—"}</div>
          </div>

          <div className="pref-block">
            <div className="pref-label">Interests</div>
            <div className="chips small">
              {(prefs.interests || []).length === 0
                ? <span className="muted small">none</span>
                : prefs.interests.map(s => (
                    <span key={s} className="chip chip-on small">{label(s)}</span>
                  ))
              }
            </div>
          </div>

          <div className="match-summary">
            {total_matching} matching events in {prefs.city}
            <br />
            <span className="muted small">ranked by BM25 over {tagged_events} tagged events</span>
          </div>
        </aside>

        {/* Right: recommendations */}
        <main className="results">
          {recommendations.length === 0 ? (
            <div className="empty card">
              <h3>No events found in {prefs.city}</h3>
              <p className="muted">Try a different city, or add more interests.</p>
              <button className="primary" onClick={() => navigate("/survey")}>Edit preferences</button>
            </div>
          ) : (
            <div className="event-grid">
              {recommendations.map(({ event, score, matched_tags }, i) => (
                <EventCard
                  key={event.event_key}
                  event={event}
                  score={score}
                  matchedTags={matched_tags}
                  rank={i + 1}
                  label={label}
                />
              ))}
            </div>
          )}
        </main>
      </div>
    </div>
  );
}

function EventCard({ event, score, matchedTags, rank, label }) {
  const date = event.date ? new Date(event.date) : null;
  const dateStr = date ? date.toLocaleDateString("en-GB", {
    day: "numeric", month: "short", year: "numeric"
  }) : "—";
  const timeStr = date ? date.toLocaleTimeString("en-GB", {
    hour: "2-digit", minute: "2-digit"
  }) : "";

  return (
    <article className="event-card">
      <div className="event-rank">#{rank}</div>
      <div className="event-body">
        <h4 className="event-name">{event.name}</h4>
        <div className="event-meta">
          <span>📅 {dateStr}{timeStr && ` · ${timeStr}`}</span>
          {event.venue && <span>📍 {event.venue}</span>}
        </div>
        <div className="event-tags">
          {(matchedTags || []).map(t => (
            <span key={t} className="tag tag-cat">{label(t)}</span>
          ))}
          {(matchedTags || []).length === 0 && (
            <span className="muted small">not yet tagged</span>
          )}
        </div>
        <div className="event-footer">
          <span className="sources">{(event.sources || []).join(" · ")}</span>
          {event.url && (
            <a href={event.url} target="_blank" rel="noopener noreferrer" className="link">
              View tickets →
            </a>
          )}
        </div>
      </div>
      <div className="score-badge" title="BM25 relevance score">{score}</div>
    </article>
  );
}
