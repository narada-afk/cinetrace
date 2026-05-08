import asyncio
import tweepy
import asyncpraw
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler

import db
import stats_client
import intelligence
import crafter
import reddit_crafter
import validator
import screenshot as screenshotter
import telegram_handler
import broadcaster
import stream_listener
import trends_poller
import reddit_monitor
from actors import BY_HANDLE, ACTORS
from inventory import SLOT_HOURS, GENERATION_HOUR
from config import (
    TWITTER_API_KEY, TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET, TWITTER_BEARER_TOKEN,
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD,
    MIN_HOURS_BETWEEN_POSTS, MAX_REPLIES_PER_ACTOR_PER_DAY,
)

IST = ZoneInfo("Asia/Kolkata")

SIGNAL_RECENCY_HOURS = 48  # only reply to actor tweets posted within this window

_twitter = tweepy.Client(
    bearer_token=TWITTER_BEARER_TOKEN,
    consumer_key=TWITTER_API_KEY,
    consumer_secret=TWITTER_API_SECRET,
    access_token=TWITTER_ACCESS_TOKEN,
    access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    wait_on_rate_limit=True,
)

# ── Amplification cluster guard ───────────────────────────────────────────────
# Track (actor_handle, signal_tweet_keyword) → first seen time.
# If 2+ signal accounts mention the same actor within 1 hour, treat as a cluster
# and only fire once.
_signal_cluster: dict[str, datetime] = {}  # key: "{actor_handle}:{normalised_topic}"

def _cluster_key(actor_handle: str, signal_text: str) -> str:
    import re
    words = re.findall(r"\b\w{5,}\b", signal_text.lower())
    topic = "_".join(sorted(set(words))[:3])  # top-3 long words as fingerprint
    return f"{actor_handle}:{topic}"

def _cluster_ok(actor_handle: str, signal_text: str) -> bool:
    key = _cluster_key(actor_handle, signal_text)
    now = datetime.now(timezone.utc)
    # Purge entries older than 2 hours to keep dict small
    stale = [k for k, t in _signal_cluster.items() if now - t > timedelta(hours=2)]
    for k in stale:
        del _signal_cluster[k]
    last = _signal_cluster.get(key)
    if last and now - last < timedelta(hours=1):
        print(f"[cluster] suppressed duplicate signal for @{actor_handle}")
        return False
    _signal_cluster[key] = now
    return True

# ── Rate guard ────────────────────────────────────────────────────────────────

def _rate_ok(actor_handle: str) -> tuple[bool, str]:
    count = db.actor_reply_count_today(actor_handle)
    if count >= MAX_REPLIES_PER_ACTOR_PER_DAY:
        return False, f"daily limit reached ({count})"

    last = db.last_post_time_for_actor(actor_handle)
    if last and datetime.utcnow() - last < timedelta(hours=MIN_HOURS_BETWEEN_POSTS):
        wait = (last + timedelta(hours=MIN_HOURS_BETWEEN_POSTS) - datetime.utcnow())
        return False, f"too soon for @{actor_handle} — wait {int(wait.seconds/60)}m"

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
        result = await crafter.craft_reply(
            actor, profile, tweet_text, stat_angle, trend_context
        )
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
    screenshot = await screenshotter.capture_section_snapshot(slug, "overview")

    # 6. Store + send to Telegram for review
    row_id = db.insert_pending(
        tweet_id, handle, db_name,
        crafted["text"], crafted["confidence"], trigger_type
    )

    msg_id = await telegram_handler.send_for_review(
        row_id          = row_id,
        actor_name      = actor["name"],
        handle          = handle,
        reply_text      = crafted["text"],
        confidence      = crafted["confidence"],
        trigger         = trigger_type,
        screenshot      = screenshot,
        original_tweet  = tweet_text,
        engage_reason   = analysis.get("reason", ""),
        stat_angle      = stat_angle,
        trigger_context = trend_context,
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

# ── Signal account handler (Tier 2) ──────────────────────────────────────────

async def on_signal_tweet(tweet, signal: dict):
    signal_text = tweet.text
    signal_name = signal["name"]
    signal_role = signal["role"]

    # Detect which actor this signal is talking about
    actor = intelligence.detect_actor_in_text(signal_text)
    if not actor:
        print(f"[signal] @{signal['handle']} tweet — no actor detected, skipping")
        return

    # Amplification cluster guard
    if not _cluster_ok(actor["handle"], signal_text):
        return

    print(f"[signal] @{signal['handle']} ({signal_role}) → actor: {actor['name']}")

    # Find actor's most recent tweet within the recency window
    cutoff = datetime.now(timezone.utc) - timedelta(hours=SIGNAL_RECENCY_HOURS)
    loop = asyncio.get_running_loop()
    try:
        # Resolve actor user ID then fetch recent tweets
        user_resp = await loop.run_in_executor(
            None,
            lambda: _twitter.get_users(usernames=[actor["handle"]], user_fields=["id"])
        )
        if not user_resp.data:
            print(f"[signal] could not resolve @{actor['handle']}")
            return
        uid = user_resp.data[0].id

        tweets_resp = await loop.run_in_executor(
            None,
            lambda: _twitter.get_users_tweets(
                uid, max_results=5,
                tweet_fields=["created_at", "text"],
                exclude=["retweets", "replies"],
            )
        )
        recent_tweet = None
        if tweets_resp.data:
            for t in tweets_resp.data:
                created = t.created_at
                if created and created.replace(tzinfo=timezone.utc) >= cutoff:
                    recent_tweet = t
                    break

        if not recent_tweet:
            print(f"[signal] no tweet from @{actor['handle']} in last {SIGNAL_RECENCY_HOURS}h")
            return
    except Exception as e:
        print(f"[signal] fetch error: {e}")
        return

    stat_angle = intelligence.classify_signal_angle(signal_role, signal_text)
    trigger_context = f"{signal_name} ({signal_role}) tweeted: \"{signal_text[:120]}\""

    await _pipeline(
        tweet_id        = str(recent_tweet.id),
        tweet_text      = recent_tweet.text,
        actor           = actor,
        trigger_type    = "signal",
        trend_context   = trigger_context,
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

# ── Reddit pipeline ───────────────────────────────────────────────────────────

_reddit_client: asyncpraw.Reddit | None = None

def _get_reddit() -> asyncpraw.Reddit | None:
    global _reddit_client
    if not all([REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD]):
        return None
    if not _reddit_client:
        _reddit_client = asyncpraw.Reddit(
            client_id     = REDDIT_CLIENT_ID,
            client_secret = REDDIT_CLIENT_SECRET,
            username      = REDDIT_USERNAME,
            password      = REDDIT_PASSWORD,
            user_agent    = f"CineTrace Stats Bot v1.0 by /u/{REDDIT_USERNAME}",
        )
    return _reddit_client

async def on_reddit_post(submission, actor: dict):
    post_id  = f"reddit_{submission.id}"
    handle   = actor["handle"]
    db_name  = actor["db_name"]

    if db.already_replied(post_id):
        return

    ok, reason = _rate_ok(handle)
    if not ok:
        print(f"[reddit] skipped @{handle}: {reason}")
        return

    analysis = await intelligence.analyse_tweet(
        actor["name"], handle,
        f"{submission.title} {submission.selftext or ''}",
        trend_context=f"reddit post in r/{submission.subreddit}",
    )
    if not analysis.get("should_engage"):
        print(f"[reddit] no engage: {analysis.get('reason')}")
        return

    stat_angle = analysis.get("stat_angle", "box_office_avg")
    profile    = await stats_client.get_full_profile(db_name)
    if not profile:
        return

    result = await reddit_crafter.craft_reddit_comment(
        actor, profile,
        post_title = submission.title,
        post_body  = submission.selftext or "",
        subreddit  = str(submission.subreddit),
        stat_angle = stat_angle,
    )

    comment_text = result.get("comment_text")
    confidence   = result.get("confidence", 0)
    if not comment_text:
        return

    post_url = f"https://reddit.com{submission.permalink}"
    row_id   = db.insert_pending(
        post_id, handle, db_name, comment_text, confidence,
        trigger_type="reddit_post", platform="reddit", source_url=post_url,
    )

    msg_id = await telegram_handler.send_reddit_for_review(
        row_id       = row_id,
        actor_name   = actor["name"],
        handle       = handle,
        comment_text = comment_text,
        confidence   = confidence,
        subreddit    = str(submission.subreddit),
        post_title   = submission.title,
        post_url     = post_url,
    )
    if msg_id:
        db.set_telegram_message_id(row_id, msg_id)
        print(f"[reddit] sent to Telegram for review (row {row_id})")
    else:
        db.mark_dropped(row_id, "telegram send failed")

async def post_reddit_approved(row: dict):
    reddit = _get_reddit()
    if not reddit:
        print("[reddit] no credentials — cannot post")
        return
    try:
        source_url = row.get("source_url", "")
        submission = await reddit.submission(url=source_url)
        comment    = await submission.reply(row["draft_reply"])
        db.mark_posted(row["id"], comment.id)
        print(f"[reddit] posted comment {comment.id}")
    except Exception as e:
        print(f"[reddit] post failed: {e}")

async def format_for_reddit(row_id: int):
    import psycopg2.extras
    from db import get_conn
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM bot_tweet_log WHERE id = %s", (row_id,))
            row = cur.fetchone()
    if not row:
        print(f"[reddit] row {row_id} not found for Reddit formatting")
        return

    db_name = row["actor_db_name"]
    actor   = next((a for a in ACTORS if a["db_name"] == db_name), None)
    if not actor:
        print(f"[reddit] actor not found for db_name={db_name}")
        return

    profile = await stats_client.get_full_profile(db_name)
    if not profile:
        return

    result = await reddit_crafter.craft_reddit_comment(
        actor, profile,
        post_title = row["draft_reply"][:100],  # use Twitter draft as context
        post_body  = "",
        subreddit  = "tollywood",  # default — user will paste in the right thread
        stat_angle = "box_office_avg",
    )

    comment_text = result.get("comment_text")
    confidence   = result.get("confidence", 0)
    if not comment_text:
        print(f"[reddit] Reddit formatting failed for row {row_id}")
        return

    # Store as a new pending Reddit row
    new_row_id = db.insert_pending(
        f"reddit_fmt_{row_id}", actor["handle"], db_name,
        comment_text, confidence,
        trigger_type="reddit_format", platform="reddit",
    )

    msg_id = await telegram_handler.send_reddit_for_review(
        row_id       = new_row_id,
        actor_name   = actor["name"],
        handle       = actor["handle"],
        comment_text = comment_text,
        confidence   = confidence,
        subreddit    = "relevant subreddit",
        post_title   = "Copy-paste into relevant Reddit thread",
        post_url     = "",
    )
    if msg_id:
        db.set_telegram_message_id(new_row_id, msg_id)

# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    print("[main] CineTrace Stats Bot starting...")

    db.run_migrations()
    print("[main] DB ready")

    telegram_handler.set_post_callback(post_approved)
    telegram_handler.set_reddit_post_callback(post_reddit_approved)
    telegram_handler.set_reddit_format_callback(format_for_reddit)
    tg_app = telegram_handler.build_app()
    await tg_app.initialize()
    await tg_app.start()
    await tg_app.updater.start_polling()
    print("[main] Telegram polling started")

    # ── Broadcaster scheduler (IST timezone) ─────────────────────────────────
    scheduler = AsyncIOScheduler(timezone=IST)

    # 9 PM nightly — generate and send next day's schedule to Telegram
    # 9:30 PM — retry (no-op if already generated)
    for _gen_min in (0, 30):
        scheduler.add_job(
            lambda: asyncio.create_task(
                broadcaster.generate_daily_schedule(telegram_handler.send_scheduled_for_review)
            ),
            trigger="cron", hour=GENERATION_HOUR, minute=_gen_min,
            id=f"daily_schedule_gen_{_gen_min}", replace_existing=True,
        )

    # Slot posters — 7am, 10am, 1pm, 4pm, 7pm, 10pm IST
    for slot_h in SLOT_HOURS:
        scheduler.add_job(
            lambda h=slot_h: asyncio.create_task(broadcaster.post_scheduled_slot(h)),
            trigger="cron", hour=slot_h, minute=0,
            id=f"slot_{slot_h}h", replace_existing=True,
        )

    scheduler.start()
    print(f"[main] Broadcaster scheduler started (slots: {SLOT_HOURS}, generation: {GENERATION_HOUR}:00 IST)")

    _stream_handle = await stream_listener.start_stream(on_actor_tweet, on_signal_tweet)
    print("[main] Twitter stream started (Tier 1 actors + Tier 2 signals)")

    # Watchdog — restart stream thread if it dies (only if stream started successfully)
    if _stream_handle:
        async def _stream_watchdog():
            import threading
            nonlocal _stream_handle
            while True:
                await asyncio.sleep(300)
                threads = {t.name for t in threading.enumerate()}
                live = any("stream" in t.lower() or "tweepy" in t.lower() for t in threads)
                if not live:
                    print("[watchdog] stream thread dead — restarting")
                    try:
                        _stream_handle = await stream_listener.start_stream(on_actor_tweet, on_signal_tweet)
                        if _stream_handle:
                            print("[watchdog] stream restarted")
                    except Exception as e:
                        print(f"[watchdog] restart failed: {e}")
        asyncio.create_task(_stream_watchdog())

    asyncio.create_task(trends_poller.poll_trends(on_trend))
    print("[main] Trends poller started")

    asyncio.create_task(reddit_monitor.monitor_subreddits(on_reddit_post))
    print("[main] Reddit monitor started")

    print("[main] All systems running ✓")
    await asyncio.Event().wait()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
