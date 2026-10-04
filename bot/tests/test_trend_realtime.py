"""
Real-time trend polling/posting:
  - the daily post cap is checked BEFORE the Trends fetch or any LLM call
  - a usable trend reaches the EXISTING TwitterGenerator, unchanged
  - generation failure is a SKIP, never a forced/fallback post
  - approval posts IMMEDIATELY (not at a fixed slot)
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from engine.models import ContentItem, Entity, Insight, Metric, Platform


def _sample_insight() -> Insight:
    return Insight(
        rule="trending_now",
        entities=[Entity(kind="actor", id=1, name="Test Actor", slug="test-actor")],
        metrics=[Metric(key="total_films", value=42, unit="films")],
        facts={"trend_term": "Test Actor", "why_trending": "why", "tweet_angle": "angle"},
        confidence=0.8,
    )


# ── Daily cap: checked before the Trends fetch or either LLM call ──────────

@pytest.mark.asyncio
async def test_daily_cap_reached_skips_before_any_trend_fetch():
    import trend_realtime

    with patch("engine.db.count_trend_posts_today", return_value=3), \
         patch("engine.trends.pipeline.discover_trending_insight") as discover:
        await trend_realtime.poll_and_maybe_post()

    discover.assert_not_called()


@pytest.mark.asyncio
async def test_under_cap_proceeds_to_discovery():
    import trend_realtime

    with patch("engine.db.count_trend_posts_today", return_value=1), \
         patch("engine.trends.pipeline.discover_trending_insight",
               new=AsyncMock(return_value=None)) as discover:
        await trend_realtime.poll_and_maybe_post()

    discover.assert_awaited_once()


# ── Happy path: Insight -> EXISTING TwitterGenerator, sent for review ──────

@pytest.mark.asyncio
async def test_valid_trend_reaches_existing_tweet_generator_and_is_sent_for_review():
    import trend_realtime

    fake_generator = MagicMock()
    fake_generator.generate = AsyncMock(return_value=ContentItem(
        insight_id=1, platform=Platform.TWITTER, text="tweet text", media_ref="test-actor"))

    with patch("engine.db.count_trend_posts_today", return_value=0), \
         patch("engine.db.insert_insight", return_value=101), \
         patch("engine.db.recent_posted_texts", return_value=[]), \
         patch("engine.db.insert_content_item", return_value=202) as insert_item, \
         patch("engine.db.set_content_telegram_id"), \
         patch("engine.generators.get_generator", return_value=fake_generator), \
         patch("engine.trends.pipeline.discover_trending_insight",
               new=AsyncMock(return_value=_sample_insight())), \
         patch("telegram_handler.send_trend_insight_for_review",
               new=AsyncMock(return_value=999)) as send_review, \
         patch("screenshot.capture_section_snapshot", new=AsyncMock(return_value=None)):
        await trend_realtime.poll_and_maybe_post()

    fake_generator.generate.assert_awaited_once()
    called_insight = fake_generator.generate.call_args.args[0]
    assert called_insight.entities[0].name == "Test Actor"
    insert_item.assert_called_once()
    # Real-time items carry no scheduled_date/slot_hour — posting happens on
    # approval, not at a fixed cron time.
    assert insert_item.call_args.args == (fake_generator.generate.return_value,)
    assert insert_item.call_args.kwargs == {}
    send_review.assert_awaited_once()


@pytest.mark.asyncio
async def test_generation_failure_is_a_skip_with_no_db_write():
    """Insight found, but the existing TwitterGenerator's own validator
    rejects every attempt — must SKIP, not post something unchecked."""
    import trend_realtime

    fake_generator = MagicMock()
    fake_generator.generate = AsyncMock(return_value=None)

    with patch("engine.db.count_trend_posts_today", return_value=0), \
         patch("engine.db.insert_insight", return_value=101), \
         patch("engine.db.recent_posted_texts", return_value=[]), \
         patch("engine.db.insert_content_item") as insert_item, \
         patch("engine.generators.get_generator", return_value=fake_generator), \
         patch("engine.trends.pipeline.discover_trending_insight",
               new=AsyncMock(return_value=_sample_insight())):
        await trend_realtime.poll_and_maybe_post()

    insert_item.assert_not_called()


@pytest.mark.asyncio
async def test_no_trend_this_poll_writes_nothing():
    import trend_realtime

    with patch("engine.db.count_trend_posts_today", return_value=0), \
         patch("engine.db.insert_insight") as insert_insight, \
         patch("engine.trends.pipeline.discover_trending_insight",
               new=AsyncMock(return_value=None)):
        await trend_realtime.poll_and_maybe_post()

    insert_insight.assert_not_called()


# ── Approval posts immediately ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_approved_trend_tweet_posts_immediately():
    import trend_realtime

    row = {"id": 202, "insight_id": 101, "text": "tweet text", "media_ref": None}

    with patch.object(trend_realtime, "_twitter") as twitter_client, \
         patch("engine.db.mark_content_posted") as mark_posted, \
         patch("telegram_handler.send_posted_notification",
               new=AsyncMock()) as send_posted, \
         patch("engine.db.get_insight", return_value=None):
        resp = MagicMock()
        resp.data = {"id": "999888777"}
        twitter_client.create_tweet.return_value = resp

        await trend_realtime.post_approved_trend_tweet(row)

    twitter_client.create_tweet.assert_called_once_with(text="tweet text")
    mark_posted.assert_called_once_with(202, "999888777")
    send_posted.assert_awaited_once()


@pytest.mark.asyncio
async def test_post_failure_marks_failed_and_alerts_not_a_silent_drop():
    import trend_realtime

    row = {"id": 202, "insight_id": None, "text": "tweet text", "media_ref": None}

    with patch.object(trend_realtime, "_twitter") as twitter_client, \
         patch("engine.db.mark_content_failed") as mark_failed, \
         patch("telegram_handler.send_alert", new=AsyncMock()) as send_alert:
        twitter_client.create_tweet.side_effect = Exception("rate limited")

        await trend_realtime.post_approved_trend_tweet(row)

    mark_failed.assert_called_once()
    send_alert.assert_awaited_once()
