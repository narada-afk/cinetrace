"""
core/cache.py
=============
Thin Redis wrapper used for response caching.

All public methods silently return None / False on any Redis error so the
application degrades gracefully when Redis is unavailable.

Usage:
    from app.core.cache import cache

    cached = cache.get("my-key")
    if cached is None:
        result = expensive_operation()
        cache.set("my-key", result)   # TTL from settings.CACHE_TTL
"""

import json
import logging
from typing import Any

import redis

from .config import settings

logger = logging.getLogger(__name__)

_client: redis.Redis | None = None


def _get_client() -> redis.Redis | None:
    global _client
    if _client is not None:
        return _client
    if not settings.REDIS_URL:
        return None
    try:
        _client = redis.from_url(settings.REDIS_URL, socket_connect_timeout=1, decode_responses=True)
        _client.ping()
        logger.info("Redis cache connected: %s", settings.REDIS_URL)
    except Exception as exc:
        logger.warning("Redis unavailable, caching disabled: %s", exc)
        _client = None
    return _client


class _Cache:
    def get(self, key: str) -> Any | None:
        client = _get_client()
        if client is None:
            return None
        try:
            raw = client.get(key)
            return json.loads(raw) if raw is not None else None
        except Exception as exc:
            logger.warning("cache.get failed: %s", exc)
            return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        client = _get_client()
        if client is None:
            return
        try:
            client.setex(key, ttl or settings.CACHE_TTL, json.dumps(value))
        except Exception as exc:
            logger.warning("cache.set failed: %s", exc)

    def delete(self, key: str) -> None:
        client = _get_client()
        if client is None:
            return
        try:
            client.delete(key)
        except Exception as exc:
            logger.warning("cache.delete failed: %s", exc)

    def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching a glob pattern (e.g. "actor:*", "compare:*").
        Uses SCAN to avoid blocking Redis on large key sets.
        Returns the number of keys deleted, or 0 on error / no Redis.
        """
        client = _get_client()
        if client is None:
            return 0
        try:
            deleted = 0
            for key in client.scan_iter(match=pattern, count=100):
                client.delete(key)
                deleted += 1
            if deleted:
                logger.info("cache.delete_pattern %r: removed %d keys", pattern, deleted)
            return deleted
        except Exception as exc:
            logger.warning("cache.delete_pattern failed: %s", exc)
            return 0


cache = _Cache()
