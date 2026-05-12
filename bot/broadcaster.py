"""
broadcaster.py
==============
Scheduled tweet system — stat-based original tweets posted every 3 hours.

Flow:
  • 9 PM nightly → generate next day's 6 slots → capture share snapshot →
                   send to Telegram for approval (with image preview)
  • 7am / 10am / 1pm / 4pm / 7pm / 10pm → post approved slot tweet to Twitter
                                           (with share snapshot as attached image)
"""

from __future__ import annotations

import io
import random
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import tweepy

import db
import screenshot as ss
from config import (
    TWITTER_API_KEY, TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET, TWITTER_BEARER_TOKEN,
    CINETRACE_BASE_URL,
)
from inventory import FACTS, SLOT_HOURS, all_actors_with_facts, get_enriched_fact
from actors import ACTORS

IST = ZoneInfo("Asia/Kolkata")

# v2 client — used for create_tweet
_twitter = tweepy.Client(
    bearer_token=TWITTER_BEARER_TOKEN,
    consumer_key=TWITTER_API_KEY,
    consumer_secret=TWITTER_API_SECRET,
    access_token=TWITTER_ACCESS_TOKEN,
    access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    wait_on_rate_limit=True,
)

# v1.1 client — used for media_upload (only v1 supports this)
_auth_v1 = tweepy.OAuth1UserHandler(
    TWITTER_API_KEY, TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET,
)
_api_v1 = tweepy.API(_auth_v1)

# actor db_name → handle (for profile URL)
_DB_NAME_TO_HANDLE: dict[str, str] = {a["db_name"]: a["handle"] for a in ACTORS}

# stat_key → section (for snapshot lookup)
_KEY_TO_SECTION: dict[str, str] = {
    f["key"]: f.get("section", "overview")
    for facts in FACTS.values()
    for f in facts
}

# stat_key → compare_with slug (only for "compare" section facts)
_KEY_TO_COMPARE_WITH: dict[str, str] = {
    f["key"]: f.get("compare_with", "")
    for facts in FACTS.values()
    for f in facts
    if f.get("compare_with")
}

# ── Tweet formatter ───────────────────────────────────────────────────────────

def _format_tweet(actor_db_name: str, fact: dict) -> str:
    slug = ss.actor_slug(actor_db_name)
    url  = f"{CINETRACE_BASE_URL}/actors/{slug}"

    parts = [
        fact["hook"],
        "",
        fact["body"],
        "",
        f"📊 {url}",
        fact["hashtags"],
    ]
    text = "\n".join(parts)

    if len(text) > 278:
        max_body = 278 - len(fact["hook"]) - len(url) - len(fact["hashtags"]) - 8
        body_trimmed = fact["body"][:max_body].rsplit(" ", 1)[0] + "…"
        parts[2] = body_trimmed
        text = "\n".join(parts)

    return text

# ── Industry picker — ensure variety across 6 slots ─────────────────────────

def _pick_actors_for_day(used_keys_last_7_days: set[str]) -> list[str]:
    by_industry: dict[str, list[str]] = {
        "Telugu":    [],
        "Tamil":     [],
        "Malayalam": [],
        "Kannada":   [],
    }
    for a in ACTORS:
        name = a["db_name"]
        industry = a.get("industry", "")
        if name in FACTS and FACTS[name]:
            fresh = [f for f in FACTS[name] if f["key"] not in used_keys_last_7_days]
            if fresh:
                by_industry.setdefault(industry, []).append(name)

    chosen: list[str] = []
    for ind in ["Telugu", "Tamil", "Malayalam", "Kannada"]:
        pool = by_industry.get(ind, [])
        if pool:
            chosen.append(random.choice(pool))

    all_eligible = [
        name for names in by_industry.values() for name in names
        if name not in chosen
    ]
    random.shuffle(all_eligible)
    chosen += all_eligible[: max(0, 6 - len(chosen))]

    random.shuffle(chosen)
    return chosen[:6]

# ── Slot-aware fact picker ────────────────────────────────────────────────────

def _pick_fact_for_slot(actor_db_name: str, slot_hour: int,
                        used_keys: set[str]) -> dict | None:
    """Return the best fact for this actor at this slot hour.

    Selection priority:
      1. Fresh facts (not in used_keys) that list this slot_hour in preferred_window
      2. Any fresh fact (no preferred_window preference)
      3. All facts (recycled) — same priority order as above
    """
    facts = FACTS.get(actor_db_name, [])
    if not facts:
        return None

    fresh = [f for f in facts if f["key"] not in used_keys]
    pool  = fresh if fresh else facts  # fall back to recycled if exhausted

    # Prefer facts whose preferred_window includes this slot
    preferred = [
        f for f in pool
        if (pw := get_enriched_fact(f).get("preferred_window")) is not None
        and slot_hour in pw
    ]

    return random.choice(preferred) if preferred else random.choice(pool)

# ── Snapshot helper ───────────────────────────────────────────────────────────

async def _capture(actor_db_name: str, section: str,
                   compare_with: str = "") -> bytes | None:
    slug = ss.actor_slug(actor_db_name)
    try:
        return await ss.capture_section_snapshot(slug, section,
                                                  compare_with=compare_with)
    except Exception as e:
        print(f"[broadcaster] snapshot failed for {actor_db_name}/{section}: {e}")
        return None

# ── Nightly generation ────────────────────────────────────────────────────────

async def generate_daily_schedule(send_for_review_fn) -> None:
    """
    Called at 9 PM IST.
    Generates 6 tweet drafts for tomorrow, stores them as 'pending',
    captures share snapshot, sends each to Telegram for approval with image preview.
    """
    tomorrow = date.today() + timedelta(days=1)

    if db.scheduled_slots_exist(tomorrow):
        print(f"[broadcaster] schedule already exists for {tomorrow}, skipping")
        return

    used_keys = db.get_used_fact_keys(days=7)
    actors = _pick_actors_for_day(used_keys)

    if len(actors) < 6:
        all_names = all_actors_with_facts()
        while len(actors) < 6:
            actors.append(random.choice(all_names))

    print(f"[broadcaster] generating schedule for {tomorrow}: {actors}")

    for i, slot_hour in enumerate(SLOT_HOURS):
        actor_db_name = actors[i] if i < len(actors) else random.choice(all_actors_with_facts())

        fact = _pick_fact_for_slot(actor_db_name, slot_hour, used_keys)
        if not fact:
            print(f"[broadcaster] no facts for {actor_db_name}, skipping slot {slot_hour}")
            continue
        section      = fact.get("section", "overview")
        compare_with = fact.get("compare_with", "")
        tweet_text   = _format_tweet(actor_db_name, fact)

        row_id = db.insert_scheduled_tweet(
            scheduled_date = tomorrow,
            slot_hour      = slot_hour,
            actor_db_name  = actor_db_name,
            tweet_text     = tweet_text,
            stat_key       = fact["key"],
            section        = section,
        )

        # Capture share snapshot for Telegram preview
        png = await _capture(actor_db_name, section, compare_with=compare_with)

        slot_label = datetime(tomorrow.year, tomorrow.month, tomorrow.day,
                              slot_hour, 0, tzinfo=IST).strftime("%-I:%M %p IST")
        msg_id = await send_for_review_fn(
            row_id     = row_id,
            slot_label = slot_label,
            actor_name = actor_db_name,
            tweet_text = tweet_text,
            screenshot = png,
        )
        if msg_id:
            db.set_scheduled_telegram_id(row_id, msg_id)

        used_keys.add(fact["key"])

    print(f"[broadcaster] {len(SLOT_HOURS)} drafts sent to Telegram for {tomorrow}")

# ── Slot poster ───────────────────────────────────────────────────────────────

async def post_scheduled_slot(slot_hour: int) -> None:
    """
    Called at each slot time (7, 10, 13, 16, 19, 22 IST).
    Posts the approved tweet for today's slot with the share snapshot image.
    """
    today = date.today()
    row   = db.get_scheduled_tweet(today, slot_hour)

    if not row:
        print(f"[broadcaster] no scheduled tweet for {today} slot {slot_hour}h")
        return

    if row["status"] != "approved":
        print(f"[broadcaster] slot {slot_hour}h not approved (status={row['status']}), skipping")
        return

    actor_db_name = row["actor_db_name"]
    section       = row.get("section") or _KEY_TO_SECTION.get(row.get("stat_key", ""), "overview")
    compare_with  = _KEY_TO_COMPARE_WITH.get(row.get("stat_key", ""), "")

    # Capture share snapshot to attach to tweet
    media_ids: list[str] | None = None
    png = await _capture(actor_db_name, section, compare_with=compare_with)
    if png:
        try:
            media = _api_v1.media_upload(filename="snapshot.png", file=io.BytesIO(png))
            media_ids = [str(media.media_id)]
            print(f"[broadcaster] uploaded media {media_ids[0]} for slot {slot_hour}h")
        except Exception as e:
            print(f"[broadcaster] media upload failed for slot {slot_hour}h: {e}, posting text-only")

    try:
        kwargs: dict = {"text": row["tweet_text"]}
        if media_ids:
            kwargs["media_ids"] = media_ids
        resp = _twitter.create_tweet(**kwargs)
        tweet_id = str(resp.data["id"])
        db.mark_scheduled_posted(row["id"], tweet_id)
        print(f"[broadcaster] posted slot {slot_hour}h tweet → {tweet_id}")
    except Exception as e:
        print(f"[broadcaster] failed to post slot {slot_hour}h: {e}")
