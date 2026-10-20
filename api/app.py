"""
FastAPI application — read-only + config API for the External Data Manager.

Runs as a separate process alongside the pipeline:
  python run_api.py          (default: http://localhost:8000)

The React frontend proxies /api/* here via Vite dev server.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import events, sources, stats, activity, pipeline, recommender
from core.db import Database
from services.vault import Vault


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Connect to PostgreSQL on startup, disconnect on shutdown."""
    vault = Vault()
    app.state.db = Database(dsn=vault.get("db_url"))
    await app.state.db.connect()
    yield
    await app.state.db.disconnect()


app = FastAPI(title="External Data Manager API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174"],  # Vite dev servers (admin + recommender)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(events.router,   prefix="/api")
app.include_router(sources.router,  prefix="/api")
app.include_router(stats.router,    prefix="/api")
app.include_router(activity.router,  prefix="/api")
app.include_router(pipeline.router,  prefix="/api")
app.include_router(recommender.router, prefix="/api")
