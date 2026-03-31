"""
core/limiter.py
===============
Shared slowapi Limiter instance.

When REDIS_URL is set and reachable, rate limit counters are stored in Redis
and shared across all Gunicorn workers. If Redis is unavailable at startup,
the limiter falls back to in-memory storage (per-worker limits, same behaviour
as before Redis was introduced).

Import this singleton into routers that need rate limiting:

    from app.core.limiter import limiter

Register it on the app in main.py:
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
"""

import logging

import redis as redis_client
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings

logger = logging.getLogger(__name__)


def _build_limiter() -> Limiter:
    if settings.REDIS_URL:
        try:
            # Probe Redis before handing the URI to limits — avoids an
            # opaque error at the first rate-limited request.
            probe = redis_client.from_url(settings.REDIS_URL, socket_connect_timeout=1)
            probe.ping()
            lim = Limiter(key_func=get_remote_address, storage_uri=settings.REDIS_URL)
            logger.info("rate limiter: Redis storage (%s)", settings.REDIS_URL)
            return lim
        except Exception as exc:
            logger.warning(
                "rate limiter: Redis unavailable (%s) — falling back to in-memory (per-worker) storage",
                exc,
            )

    logger.info("rate limiter: in-memory storage (per-worker)")
    return Limiter(key_func=get_remote_address, storage_uri="memory://")


limiter = _build_limiter()
