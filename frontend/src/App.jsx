import { Routes, Route, NavLink, Navigate } from "react-router-dom";
import Events from "./pages/Events";
import Sources from "./pages/Sources";
import Stats from "./pages/Stats";
import ActivityLog from "./pages/ActivityLog";
import "./App.css";

const NAV = [
  { to: "/stats",       label: "📊 Dashboard" },
  { to: "/events",      label: "🗓️  Events"    },
  { to: "/logs",        label: "📋 Activity Log"},
  { to: "/sources",     label: "⚙️  Sources"   },
];

export default function App() {
  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="sidebar-brand">EDM</div>
        <nav>
          {NAV.map(({ to, label }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) => "nav-item" + (isActive ? " active" : "")}
            >
              {label}
            </NavLink>
          ))}
        </nav>
      </aside>

      <main className="content">
        <Routes>
          <Route path="/" element={<Navigate to="/stats" replace />} />
          <Route path="/stats"    element={<Stats />}    />
          <Route path="/events"   element={<Events />}   />
          <Route path="/sources"  element={<Sources />}     />
          <Route path="/logs"     element={<ActivityLog />} />
        </Routes>
      </main>
    </div>
  );
}
