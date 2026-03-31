"""
core/config.py
==============
All environment-driven settings in one place.
Import the `settings` singleton anywhere:

    from app.core.config import settings
    print(settings.DATABASE_URL)
"""

import os


class Settings:
    # ── Database ───────────────────────────────────────────────────────────────
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql://sca:sca@postgres:5432/sca"
    )

    # ── API metadata ───────────────────────────────────────────────────────────
    APP_TITLE: str = "South Cinema Analytics API"
    APP_DESCRIPTION: str = (
        "Analytics API for South Indian cinema. "
        "Actor profiles, filmographies, collaboration graphs, "
        "and stats powered by precomputed analytics tables."
    )
    APP_VERSION: str = "3.0.0"

    # ── CORS ───────────────────────────────────────────────────────────────────
    # Comma-separated origins, e.g. "http://localhost:3000,https://myapp.com"
    CORS_ORIGINS: list[str] = os.getenv(
        "CORS_ORIGINS", "http://localhost:3000,http://localhost:3001"
    ).split(",")

    # ── Graph versioning ───────────────────────────────────────────────────────
    # Bump this value (e.g. "2", "3") whenever you ingest new data and want
    # every Gunicorn worker to rebuild its in-memory graph on the next request.
    # Workers compare their built version to this on each request (O(1) string
    # compare) and rebuild only on mismatch — no rebuild on every request.
    GRAPH_VERSION: str = os.getenv("GRAPH_VERSION", "1")

    # ── Redis cache ────────────────────────────────────────────────────────────
    # Optional — leave unset to disable Redis caching (app falls back gracefully).
    REDIS_URL: str | None = os.getenv("REDIS_URL")          # e.g. redis://redis:6379/0
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", "300"))     # seconds (default 5 min)

    # ── Sentry ─────────────────────────────────────────────────────────────────
    # Leave unset in dev/CI — Sentry is fully disabled when DSN is absent.
    SENTRY_DSN: str | None = os.getenv("SENTRY_DSN")

    # ── Admin ──────────────────────────────────────────────────────────────────
    # Required to call POST /admin/rebuild-graph.
    # Set via ADMIN_API_KEY env var — no default, unset means endpoint is locked.
    ADMIN_API_KEY: str | None = os.getenv("ADMIN_API_KEY")

    # ── Graph cache (in-memory) ────────────────────────────────────────────────
    # TTL in seconds for BFS result cache entries.
    # The adjacency list itself is permanent (built once at startup).
    GRAPH_RESULT_TTL: int = int(os.getenv("GRAPH_RESULT_TTL", "300"))   # 5 min
    GRAVITY_RESULT_TTL: int = int(os.getenv("GRAVITY_RESULT_TTL", "600"))  # 10 min

    # Max number of cached BFS results before oldest is evicted.
    GRAPH_CACHE_MAXSIZE: int = int(os.getenv("GRAPH_CACHE_MAXSIZE", "500"))


# Module-level singleton — import this, not the class.
settings = Settings()
