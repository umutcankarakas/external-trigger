import React, { useEffect, useState } from "react";
import { Routes, Route, Navigate, useNavigate } from "react-router-dom";
import Survey from "./pages/Survey.jsx";
import Recommendations from "./pages/Recommendations.jsx";
import { api, getUserId } from "./api.js";

/** Routes traffic between Survey (first-time) and Recommendations (returning). */
export default function App() {
  const [status, setStatus] = useState("loading"); // "loading" | "survey" | "ready"
  const navigate = useNavigate();

  useEffect(() => {
    const userId = getUserId();
    api.getPreferences(userId)
      .then(() => setStatus("ready"))
      .catch(() => setStatus("survey"));
  }, []);

  if (status === "loading") {
    return <div className="centered muted">Loading…</div>;
  }

  return (
    <Routes>
      <Route
        path="/"
        element={
          status === "ready"
            ? <Navigate to="/recommendations" replace />
            : <Navigate to="/survey" replace />
        }
      />
      <Route path="/survey" element={<Survey onSaved={() => navigate("/recommendations")} />} />
      <Route path="/recommendations" element={<Recommendations />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
