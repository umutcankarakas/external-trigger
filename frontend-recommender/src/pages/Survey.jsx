import React, { useEffect, useState } from "react";
import { api, getUserId } from "../api.js";

/**
 * Survey: city + interests picked from the controlled vocabulary.
 * The vocabulary groups (music genres / event types / sports / vibe) are
 * rendered as labeled chip sections. Pre-fills with existing prefs for editing.
 */
export default function Survey({ onSaved }) {
  const [cities, setCities] = useState([]);
  const [vocabulary, setVocabulary] = useState(null);
  const [city, setCity] = useState("");
  const [interests, setInterests] = useState(new Set());
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);

  const userId = getUserId();

  useEffect(() => {
    Promise.all([
      api.taxonomy(),
      api.getPreferences(userId).catch(() => null),
    ]).then(([tax, prefs]) => {
      setCities(tax.cities || []);
      setVocabulary(tax.vocabulary);
      if (prefs) {
        setCity(prefs.city || "");
        setInterests(new Set(prefs.interests || []));
      }
    }).catch(e => setError(String(e)));
  }, []);

  const toggle = (slug) => {
    const next = new Set(interests);
    if (next.has(slug)) next.delete(slug);
    else next.add(slug);
    setInterests(next);
  };

  const submit = async () => {
    if (!city) {
      setError("Please choose a city");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await api.savePreferences({
        user_id: userId,
        city,
        interests: [...interests],
      });
      onSaved && onSaved();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (!vocabulary) {
    return <div className="centered muted">Loading survey…</div>;
  }

  return (
    <div className="page">
      <header className="hero">
        <h1>Find events you'll love</h1>
        <p className="muted">Tell us where you are and what you're into.</p>
      </header>

      {/* Q1: City */}
      <section className="card">
        <h2><span className="step">1</span>Which city are you in?</h2>
        <select className="select" value={city} onChange={e => setCity(e.target.value)}>
          <option value="">— select a city —</option>
          {cities.map(c => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
      </section>

      {/* Q2: Interests, grouped by vocabulary section */}
      <section className="card">
        <h2><span className="step">2</span>What are you into?</h2>
        <p className="muted small">Pick anything that fits — the more you pick, the sharper the match.</p>
        {vocabulary.groups.map(group => (
          <div key={group.id} className="vocab-group">
            <div className="vocab-group-label">{group.label}</div>
            <div className="chips">
              {group.tags.map(t => (
                <button
                  key={t.slug}
                  type="button"
                  className={`chip ${interests.has(t.slug) ? "chip-on" : ""}`}
                  onClick={() => toggle(t.slug)}
                >{t.label}</button>
              ))}
            </div>
          </div>
        ))}
      </section>

      {error && <div className="error">{error}</div>}

      <div className="submit-row">
        <button className="primary" onClick={submit} disabled={saving || !city}>
          {saving
            ? "Saving…"
            : `Show me events${interests.size ? ` (${interests.size} selected)` : ""}`}
        </button>
      </div>
    </div>
  );
}
