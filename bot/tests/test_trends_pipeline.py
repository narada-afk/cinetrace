"""
Decision boundaries for discover_trending_insight() (provider -> resolver
-> reasoning): a usable trend produces a correctly-shaped Insight; anything
short of that is a SKIP (None) — never a forced/fallback result. Exercised
with a fully mocked provider so no network call happens.

Wiring that Insight into the EXISTING TwitterGenerator, the daily post cap,
and immediate-post-on-approval live in test_trend_realtime.py.
"""

from unittest.mock import AsyncMock, patch

import pytest

from engine.trends.pipeline import discover_trending_insight
from engine.trends.provider import TrendCandidate
from engine.trends.resolver import ResolvedEntity


class _FakeProvider:
    def __init__(self, candidates):
        self._candidates = candidates

    async def get_trends(self):
        return self._candidates


# ── discover_trending_insight: provider -> resolver -> reasoning ───────────

@pytest.mark.asyncio
async def test_google_trends_unavailable_is_a_graceful_skip():
    """Empty feed (fetch failed / nothing returned) — no exception, no tweet."""
    insight = await discover_trending_insight(provider=_FakeProvider([]))
    assert insight is None


@pytest.mark.asyncio
async def test_first_candidate_unresolved_second_succeeds():
    """A non-cinema trend ahead of a cinema one in the feed must not abort
    the whole run — the pipeline keeps trying candidates in order."""
    candidates = [
        TrendCandidate(title="Cricket Match Result"),
        TrendCandidate(title="Test Actor"),
    ]
    resolved = ResolvedEntity(actor_id=1, name="Test Actor", slug="test-actor", industry="Tamil")

    async def fake_resolve(title):
        return resolved if title == "Test Actor" else None

    class _Conn:
        fact_key = "total_films"
        metric_value = 42
        metric_unit = "films"
        why_trending = "trending now"
        tweet_angle = "angle"
        confidence = 0.75

    with patch("engine.trends.pipeline.resolve_trend_to_actor", side_effect=fake_resolve), \
         patch("engine.trends.pipeline.stats_client.get_full_profile",
               new=AsyncMock(return_value={"movies": [{"release_year": 2020}],
                                           "collaborators": [], "directors": []})), \
         patch("engine.trends.pipeline.find_trend_connection", new=AsyncMock(return_value=_Conn())):
        insight = await discover_trending_insight(provider=_FakeProvider(candidates))

    assert insight is not None
    assert insight.rule == "trending_now"
    assert insight.entities[0].name == "Test Actor"
    assert insight.metrics[0].value == 42
    assert insight.facts["trend_term"] == "Test Actor"


@pytest.mark.asyncio
async def test_resolved_actor_but_no_profile_skips():
    candidates = [TrendCandidate(title="Test Actor")]
    resolved = ResolvedEntity(actor_id=1, name="Test Actor", slug="test-actor", industry="Tamil")
    with patch("engine.trends.pipeline.resolve_trend_to_actor", new=AsyncMock(return_value=resolved)), \
         patch("engine.trends.pipeline.stats_client.get_full_profile", new=AsyncMock(return_value=None)):
        insight = await discover_trending_insight(provider=_FakeProvider(candidates))

    assert insight is None


@pytest.mark.asyncio
async def test_resolved_actor_but_no_interesting_angle_skips():
    candidates = [TrendCandidate(title="Test Actor")]
    resolved = ResolvedEntity(actor_id=1, name="Test Actor", slug="test-actor", industry="Tamil")
    with patch("engine.trends.pipeline.resolve_trend_to_actor", new=AsyncMock(return_value=resolved)), \
         patch("engine.trends.pipeline.stats_client.get_full_profile",
               new=AsyncMock(return_value={"movies": [{"release_year": 2020}],
                                           "collaborators": [], "directors": []})), \
         patch("engine.trends.pipeline.find_trend_connection", new=AsyncMock(return_value=None)):
        insight = await discover_trending_insight(provider=_FakeProvider(candidates))

    assert insight is None
