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
        "DATABASE_URL", "postgresql://sca:sca@localhost:5432/sca"
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

    # ── Graph cache (in-memory) ────────────────────────────────────────────────
    # TTL in seconds for BFS result cache entries.
    # The adjacency list itself is permanent (built once at startup).
    GRAPH_RESULT_TTL: int = int(os.getenv("GRAPH_RESULT_TTL", "300"))   # 5 min
    GRAVITY_RESULT_TTL: int = int(os.getenv("GRAVITY_RESULT_TTL", "600"))  # 10 min

    # Max number of cached BFS results before oldest is evicted.
    GRAPH_CACHE_MAXSIZE: int = int(os.getenv("GRAPH_CACHE_MAXSIZE", "500"))


# Module-level singleton — import this, not the class.
settings = Settings()
