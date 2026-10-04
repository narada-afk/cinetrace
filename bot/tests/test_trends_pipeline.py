"""
End-to-end decision boundaries for trend-aware entity selection:
  - a usable trend reaches the EXISTING TwitterGenerator unchanged
  - anything short of that is a SKIP — never a forced/fallback tweet

discover_trending_insight() itself (provider -> resolver -> reasoning) is
exercised with a fully mocked provider so no network call happens.
"""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from engine.models import ContentItem, Entity, Insight, Metric, Platform
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


# ── broadcaster wiring: Insight -> EXISTING TwitterGenerator, nothing else ──

def _sample_insight() -> Insight:
    return Insight(
        rule="trending_now",
        entities=[Entity(kind="actor", id=1, name="Test Actor", slug="test-actor")],
        metrics=[Metric(key="total_films", value=42, unit="films")],
        facts={"trend_term": "Test Actor", "why_trending": "why", "tweet_angle": "angle"},
        confidence=0.8,
    )


@pytest.mark.asyncio
async def test_valid_trend_reaches_existing_tweet_generator():
    import broadcaster

    fake_generator = MagicMock()
    fake_generator.generate = AsyncMock(return_value=ContentItem(
        insight_id=1, platform=Platform.TWITTER, text="tweet text", media_ref="test-actor"))

    with patch("engine.db.slot_content_exists", return_value=False), \
         patch("engine.db.insert_insight", return_value=101), \
         patch("engine.db.recent_posted_texts", return_value=[]), \
         patch("engine.db.insert_content_item", return_value=202) as insert_item, \
         patch("engine.db.set_content_telegram_id"), \
         patch("engine.generators.get_generator", return_value=fake_generator), \
         patch("engine.trends.pipeline.discover_trending_insight",
               new=AsyncMock(return_value=_sample_insight())), \
         patch("telegram_handler.send_insight_for_review", new=AsyncMock(return_value=999)), \
         patch("screenshot.capture_section_snapshot", new=AsyncMock(return_value=None)):
        handled = await broadcaster._generate_daily_schedule_trend_aware(date(2026, 1, 2))

    assert handled is True
    fake_generator.generate.assert_awaited_once()
    called_insight = fake_generator.generate.call_args.args[0]
    assert called_insight.entities[0].name == "Test Actor"
    insert_item.assert_called_once()


@pytest.mark.asyncio
async def test_no_trend_today_is_a_skip_with_no_db_write():
    import broadcaster

    with patch("engine.db.slot_content_exists", return_value=False), \
         patch("engine.db.insert_content_item") as insert_item, \
         patch("engine.trends.pipeline.discover_trending_insight", new=AsyncMock(return_value=None)):
        handled = await broadcaster._generate_daily_schedule_trend_aware(date(2026, 1, 2))

    assert handled is False
    insert_item.assert_not_called()


@pytest.mark.asyncio
async def test_generation_failure_is_a_skip_not_a_fallback():
    """Insight found, but the existing TwitterGenerator's own validator
    rejects every attempt — must SKIP, not post something unchecked."""
    import broadcaster

    fake_generator = MagicMock()
    fake_generator.generate = AsyncMock(return_value=None)

    with patch("engine.db.slot_content_exists", return_value=False), \
         patch("engine.db.insert_insight", return_value=101), \
         patch("engine.db.recent_posted_texts", return_value=[]), \
         patch("engine.db.insert_content_item") as insert_item, \
         patch("engine.generators.get_generator", return_value=fake_generator), \
         patch("engine.trends.pipeline.discover_trending_insight",
               new=AsyncMock(return_value=_sample_insight())):
        handled = await broadcaster._generate_daily_schedule_trend_aware(date(2026, 1, 2))

    assert handled is False
    insert_item.assert_not_called()
