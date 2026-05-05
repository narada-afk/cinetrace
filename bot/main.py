import asyncio
import tweepy
from datetime import datetime, timedelta

import db
import stats_client
import intelligence
import crafter
import validator
import screenshot as screenshotter
import telegram_handler
import stream_listener
import trends_poller
from actors import BY_HANDLE
from config import (
    TWITTER_API_KEY, TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET, TWITTER_BEARER_TOKEN,
    MIN_HOURS_BETWEEN_POSTS, MAX_REPLIES_PER_ACTOR_PER_DAY,
)

_twitter = tweepy.Client(
    bearer_token=TWITTER_BEARER_TOKEN,
    consumer_key=TWITTER_API_KEY,
    consumer_secret=TWITTER_API_SECRET,
    access_token=TWITTER_ACCESS_TOKEN,
    access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    wait_on_rate_limit=True,
)

# ── Rate guard ────────────────────────────────────────────────────────────────

def _rate_ok(actor_handle: str) -> tuple[bool, str]:
    count = db.actor_reply_count_today(actor_handle)
    if count >= MAX_REPLIES_PER_ACTOR_PER_DAY:
        return False, f"daily limit reached ({count})"

    last = db.last_post_time()
    if last and datetime.utcnow() - last < timedelta(hours=MIN_HOURS_BETWEEN_POSTS):
        wait = (last + timedelta(hours=MIN_HOURS_BETWEEN_POSTS) - datetime.utcnow())
        return False, f"too soon — wait {int(wait.seconds/60)}m"

    return True, ""

# ── Core pipeline ─────────────────────────────────────────────────────────────

async def _pipeline(tweet_id: str, tweet_text: str, actor: dict,
                    trigger_type: str = "tweet", trend_context: str = ""):
    handle   = actor["handle"]
    db_name  = actor["db_name"]

    # Dedup
    if db.already_replied(tweet_id):
        return

    # Rate guard
    ok, reason = _rate_ok(handle)
    if not ok:
        print(f"[pipeline] skipped @{handle}: {reason}")
        return

    print(f"[pipeline] processing tweet {tweet_id} from @{handle}")

    # 1. Intelligence — should we engage?
    analysis = await intelligence.analyse_tweet(
        actor["name"], handle, tweet_text, trend_context
    )
    if not analysis.get("should_engage"):
        print(f"[pipeline] no engage: {analysis.get('reason')}")
        return

    stat_angle = analysis.get("stat_angle", "career overview")
    print(f"[pipeline] engaging — stat angle: {stat_angle}")

    # 2. Fetch stats from cinetrace
    profile = await stats_client.get_full_profile(db_name)
    if not profile:
        print(f"[pipeline] no profile data for {db_name}")
        return

    # 3. Craft reply — up to 2 retries
    crafted = None
    for attempt in range(3):
        result = await crafter.craft_reply(actor, profile, tweet_text, stat_angle)
        reply_text = result.get("reply_text")
        confidence = result.get("confidence", 0)

        if not reply_text:
            continue

        # 4. Validate
        passed, final_conf, failures = await validator.validate(
            reply_text, result.get("stat_used", ""),
            confidence, profile["profile_url"]
        )

        if passed:
            crafted = {"text": reply_text, "confidence": final_conf}
            break
        else:
            print(f"[pipeline] validation failed (attempt {attempt+1}): {failures}")
            stat_angle = f"{stat_angle} — fix issues: {'; '.join(failures)}"

    if not crafted:
        print(f"[pipeline] dropped after 3 failed attempts for @{handle}")
        row_id = db.insert_pending(tweet_id, handle, db_name, "", 0, trigger_type)
        db.mark_dropped(row_id, "validation failed after 3 attempts")
        return

    # 5. Screenshot
    slug       = screenshotter.actor_slug(db_name)
    screenshot = await screenshotter.capture_actor_page(slug)

    # 6. Store + send to Telegram for review
    row_id = db.insert_pending(
        tweet_id, handle, db_name,
        crafted["text"], crafted["confidence"], trigger_type
    )

    msg_id = await telegram_handler.send_for_review(
        row_id      = row_id,
        actor_name  = actor["name"],
        handle      = handle,
        reply_text  = crafted["text"],
        confidence  = crafted["confidence"],
        trigger     = trigger_type,
        screenshot  = screenshot,
    )

    if msg_id:
        db.set_telegram_message_id(row_id, msg_id)
        print(f"[pipeline] sent to Telegram for review (row {row_id})")
    else:
        db.mark_dropped(row_id, "telegram send failed")

# ── Post to Twitter (called from Telegram callback) ───────────────────────────

async def post_approved(row: dict):
    try:
        resp = _twitter.create_tweet(
            text=row["draft_reply"],
            in_reply_to_tweet_id=row["tweet_id"] if row["trigger_type"] == "tweet" else None,
        )
        reply_tweet_id = str(resp.data["id"])
        db.mark_posted(row["id"], reply_tweet_id)
        print(f"[post] posted tweet {reply_tweet_id}")
    except Exception as e:
        print(f"[post] failed: {e}")

# ── Tweet stream handler ──────────────────────────────────────────────────────

async def on_actor_tweet(tweet, actor: dict):
    await _pipeline(
        tweet_id     = str(tweet.id),
        tweet_text   = tweet.text,
        actor        = actor,
        trigger_type = "tweet",
    )

# ── Trend handler ─────────────────────────────────────────────────────────────

async def on_trend(trend_name: str, actor: dict, volume: int):
    fake_tweet_id = f"trend_{trend_name.replace(' ', '_')}_{int(datetime.utcnow().timestamp())}"
    await _pipeline(
        tweet_id     = fake_tweet_id,
        tweet_text   = f"[trending] {trend_name} (volume: {volume})",
        actor        = actor,
        trigger_type = "trend",
        trend_context= trend_name,
    )

# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    print("[main] CineTrace Stats Bot starting...")

    db.run_migrations()
    print("[main] DB ready")

    telegram_handler.set_post_callback(post_approved)
    tg_app = telegram_handler.build_app()
    await tg_app.initialize()
    await tg_app.start()
    await tg_app.updater.start_polling()
    print("[main] Telegram polling started")

    await stream_listener.start_stream(on_actor_tweet)
    print("[main] Twitter stream started")

    asyncio.create_task(trends_poller.poll_trends(on_trend))
    print("[main] Trends poller started")

    print("[main] All systems running ✓")
    await asyncio.Event().wait()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
