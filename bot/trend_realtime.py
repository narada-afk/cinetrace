"""
trend_realtime.py
==================
Real-time trend-driven tweets — independent of the nightly broadcaster
schedule (broadcaster.py). Polled on an interval by main.py's scheduler
(TREND_POLL_INTERVAL_SECONDS, config.py; default 1800s / 30 min).

On a genuine Cinetrace connection, sends ONE Telegram review card that
posts IMMEDIATELY on approval — there is no fixed slot to wait for, unlike
the nightly broadcaster's approved items. A daily post cap
(TREND_DAILY_POST_CAP, engine/config.py; default 3) is checked BEFORE the
Trends fetch or either LLM call, so polling frequency alone never drives
cost or posting volume past the cap.

See docs/trend-awareness.md for the full flow and rationale.
"""

from __future__ import annotations

import io
from datetime import date, datetime
from zoneinfo import ZoneInfo

import tweepy

import screenshot as ss
from config import (
    TWITTER_API_KEY, TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET, TWITTER_BEARER_TOKEN,
)
from engine.config import get_trend_config
from engine.models import Platform, RankedInsight, Score
from engine.shared.fingerprint import canonical_fingerprint
from engine.shared.logging import get_logger

log = get_logger("trend_realtime")

IST = ZoneInfo("Asia/Kolkata")

_twitter = tweepy.Client(
    bearer_token=TWITTER_BEARER_TOKEN,
    consumer_key=TWITTER_API_KEY,
    consumer_secret=TWITTER_API_SECRET,
    access_token=TWITTER_ACCESS_TOKEN,
    access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    wait_on_rate_limit=True,
)
_auth_v1 = tweepy.OAuth1UserHandler(
    TWITTER_API_KEY, TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET,
)
_api_v1 = tweepy.API(_auth_v1)


def _today_ist() -> date:
    return datetime.now(IST).date()


# ── Poll: discover -> generate -> send for review ────────────────────────────

async def poll_and_maybe_post() -> None:
    """Called on a recurring interval by main.py's scheduler."""
    from engine import db as engine_db
    from engine.generators import get_generator
    from engine.trends.pipeline import discover_trending_insight

    config = get_trend_config()
    if not config.enabled:
        return

    posted_today = engine_db.count_trend_posts_today(_today_ist())
    if posted_today >= config.daily_post_cap:
        log.info("daily trend-post cap reached (%d/%d) — skipping this poll",
                 posted_today, config.daily_post_cap)
        return

    insight = await discover_trending_insight(config=config)
    if insight is None:
        log.info("no usable trend this poll — skip")
        return

    ranked = RankedInsight(
        insight=insight,
        score=Score(total=insight.confidence, components={}, weights_version="trend-realtime"),
        fingerprint=canonical_fingerprint(insight),
    )
    ranked.db_id = engine_db.insert_insight(ranked)

    twitter = get_generator(Platform.TWITTER)
    avoid   = engine_db.recent_posted_texts(days=14)
    item    = await twitter.generate(ranked.insight, insight_id=ranked.db_id, avoid_texts=avoid)
    if item is None:
        trend_term = insight.facts.get("trend_term", "?")
        log.info("copywriting/validation failed for %s (trend=%r) — skip",
                 insight.entities[0].name, trend_term)
        return

    # No scheduled_date/slot_hour — this posts on approval, whenever that
    # happens, not at a fixed cron time.
    item_id = engine_db.insert_content_item(item)

    png = None
    if item.media_ref:
        try:
            png = await ss.capture_section_snapshot(item.media_ref, "stat-card")
        except Exception as e:
            log.warning("stat-card capture failed for %s: %s", item.media_ref, e)

    from telegram_handler import send_trend_insight_for_review
    header = (
        f"🎬 {insight.entities[0].name} — {insight.facts.get('why_trending', '')}\n"
        f"🔎 trend: {insight.facts.get('trend_term', '?')}\n"
        f"📊 today: {posted_today}/{config.daily_post_cap} trend posts so far"
    )
    msg_id = await send_trend_insight_for_review(
        item_id=item_id, header=header, text=item.text, screenshot=png)
    if msg_id:
        engine_db.set_content_telegram_id(item_id, msg_id)

    log.info("sent for review: %s (trend=%r, item_id=%d)",
             insight.entities[0].name, insight.facts.get("trend_term", "?"), item_id)


# ── Post on Telegram approval ─────────────────────────────────────────────────

async def post_approved_trend_tweet(row: dict) -> None:
    """Registered with telegram_handler.set_trend_post_callback. Posts
    IMMEDIATELY on approval — mirrors broadcaster.py's slot-posting
    mechanics (v1 media upload, v2 create_tweet) without a slot to wait for.
    """
    from engine import db as engine_db
    from telegram_handler import send_alert, send_posted_notification

    actor_name = row.get("media_ref") or "trend tweet"
    if row.get("insight_id"):
        insight = engine_db.get_insight(row["insight_id"])
        if insight and insight.entities:
            actor_name = insight.entities[0].name

    media_ids: list[str] | None = None
    if row.get("media_ref"):
        try:
            png = await ss.capture_section_snapshot(row["media_ref"], "stat-card")
            if png:
                media = _api_v1.media_upload(filename="snapshot.png", file=io.BytesIO(png))
                media_ids = [str(media.media_id)]
        except Exception as e:
            log.warning("media upload failed for %s: %s, posting text-only", row["media_ref"], e)

    try:
        kwargs: dict = {"text": row["text"]}
        if media_ids:
            kwargs["media_ids"] = media_ids
        resp = _twitter.create_tweet(**kwargs)
        tweet_id = str(resp.data["id"])
        engine_db.mark_content_posted(row["id"], tweet_id)
        log.info("posted trend tweet -> %s", tweet_id)
        await send_posted_notification(actor_name, tweet_id, label="Trend tweet")
    except Exception as e:
        engine_db.mark_content_failed(row["id"], str(e))
        log.error("failed to post trend tweet: %s", e)
        try:
            await send_alert(f"Trend tweet failed to post: {e}")
        except Exception:
            pass
