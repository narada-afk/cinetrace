"""
TrendProvider — where "what's trending right now" comes from.

GoogleTrendsProvider reads Google's public daily-trends RSS feed
(https://trends.google.com/trending/rss?geo=IN). No API key, no scraping of
trends.google.com's UI, no third-party trend database. Google's own feed
already groups each trend with a short set of related news items, which we
reuse as "why is this trending" context (see TrendCandidate.news_snippets) —
we never build our own trend-context lookup.

A second source (e.g. vidIQ) can be added later as another TrendProvider
implementation without changing anything downstream — the resolver and
reasoning step only depend on this module's TrendCandidate shape.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import httpx

from engine.config import TrendConfig
from engine.shared.logging import get_logger

log = get_logger("trends.provider")

# Namespace Google's daily-trends RSS feed declares for its trend-specific
# fields (approx_traffic, news_item, …). Fixed by the feed format itself.
_HT_NS = "https://trends.google.com/trending/rss"


@dataclass
class TrendCandidate:
    """One entry from the trends feed, with whatever "why is this
    trending" context the feed itself provides — nothing we computed."""

    title: str
    approx_traffic: str | None = None
    news_snippets: list[str] = field(default_factory=list)

    @property
    def context_text(self) -> str:
        """Best-effort human-readable context for the reasoning step."""
        parts = []
        if self.approx_traffic:
            parts.append(f"search volume: {self.approx_traffic}")
        parts.extend(self.news_snippets[:5])
        return " | ".join(parts) if parts else "(no additional context from feed)"


class TrendProvider(ABC):
    @abstractmethod
    async def get_trends(self) -> list[TrendCandidate]:
        """Return current trends, most prominent first. Empty list on any
        failure — callers must treat that as "nothing to do today", never
        raise to force a fallback."""


class GoogleTrendsProvider(TrendProvider):
    def __init__(self, config: TrendConfig):
        self._config = config

    async def get_trends(self) -> list[TrendCandidate]:
        url = self._config.rss_url_template.format(geo=self._config.geo)
        try:
            async with httpx.AsyncClient(timeout=self._config.request_timeout_seconds) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                body = resp.text
        except Exception as e:
            log.warning("fetch failed (%s): %s", url, e)
            return []

        try:
            candidates = _parse_rss(body)
        except ET.ParseError as e:
            log.warning("feed did not parse as XML: %s", e)
            return []

        log.info("fetched %d trend(s) for geo=%s", len(candidates), self._config.geo)
        return candidates


def _parse_rss(body: str) -> list[TrendCandidate]:
    root = ET.fromstring(body)
    candidates: list[TrendCandidate] = []
    for item in root.iter("item"):
        title_el = item.find("title")
        if title_el is None or not (title_el.text or "").strip():
            continue

        traffic_el = item.find(f"{{{_HT_NS}}}approx_traffic")
        traffic = traffic_el.text.strip() if traffic_el is not None and traffic_el.text else None

        snippets: list[str] = []
        for news_item in item.findall(f"{{{_HT_NS}}}news_item"):
            snippet_el = news_item.find(f"{{{_HT_NS}}}news_item_title")
            if snippet_el is not None and snippet_el.text:
                snippets.append(snippet_el.text.strip())

        candidates.append(TrendCandidate(
            title=title_el.text.strip(),
            approx_traffic=traffic,
            news_snippets=snippets,
        ))
    return candidates
