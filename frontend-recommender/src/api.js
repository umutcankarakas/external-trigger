// Thin wrapper around fetch — all calls go through the Vite proxy to /api → :8001.

async function request(path, opts = {}) {
  const res = await fetch(`/api${path}`, {
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
    ...opts,
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${err}`);
  }
  return res.json();
}

export const api = {
  taxonomy:        ()           => request("/recommender/taxonomy"),
  getPreferences:  (userId)     => request(`/recommender/preferences/${userId}`),
  savePreferences: (prefs)      => request("/recommender/preferences", { method: "POST", body: JSON.stringify(prefs) }),
  recommendations: (userId, n=20) => request(`/recommender/recommendations/${userId}?limit=${n}`),
};

// User ID helpers — stored in localStorage so the same browser keeps its prefs.
const USER_ID_KEY = "edm_recommender_user_id";

function uuid() {
  // RFC4122 v4-ish; good enough for an anonymous user key.
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, c => {
    const r = Math.random() * 16 | 0;
    const v = c === "x" ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

export function getUserId() {
  let id = localStorage.getItem(USER_ID_KEY);
  if (!id) {
    id = uuid();
    localStorage.setItem(USER_ID_KEY, id);
  }
  return id;
}

export function resetUser() {
  localStorage.removeItem(USER_ID_KEY);
}
