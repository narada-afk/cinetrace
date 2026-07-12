"""
Content generator interface + registry.

A generator converts a structured Insight into platform copy. It receives
ONLY the insight (plus tone/char limit) — generators never query the database.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from engine.models import ContentItem, Insight, Platform

_REGISTRY: dict[Platform, "ContentGenerator"] = {}


def register(gen_cls: type["ContentGenerator"]) -> type["ContentGenerator"]:
    instance = gen_cls()
    _REGISTRY[instance.platform] = instance
    return gen_cls


def get_generator(platform: Platform) -> "ContentGenerator":
    return _REGISTRY[platform]


def all_generators() -> dict[Platform, "ContentGenerator"]:
    return dict(_REGISTRY)


class ContentGenerator(ABC):
    platform: Platform
    default_char_limit: int = 2000
    default_tone: str = "sharp, understated, film-analyst"

    @abstractmethod
    async def generate(self, insight: Insight, insight_id: int,
                       tone: str | None = None,
                       char_limit: int | None = None) -> ContentItem | None:
        """Return a ContentItem, or None if generation/validation failed."""
