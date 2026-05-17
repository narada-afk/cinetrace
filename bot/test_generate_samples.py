#!/usr/bin/env python3
"""
test_generate_samples.py
========================
Runs the full generator → scorer → screenshot → Telegram pipeline
for a handful of actors and sends the results to your Telegram chat
as real scheduled-tweet review cards (with Approve / Skip buttons).

Usage (inside the bot container):
    python test_generate_samples.py
    python test_generate_samples.py rajinikanth "kamal haasan" prabhas

Entries ARE written to the database so Approve/Skip buttons work.
Scheduled for today's date, slot 0 (no conflict with real slots).
"""

import asyncio
import random
import sys
from datetime import date

import db
import stats_client
import generator as gen
import scorer as sc
import screenshot as ss
import broadcaster as bc
from telegram_handler import send_scheduled_for_review
from actors import ACTORS

# Default test actors if none passed on CLI
DEFAULT_ACTORS = ["Rajinikanth", "Kamal Haasan", "Mohanlal"]

# Fake slot hours that don't collide with real slots (0, 1, 2 …)
_FAKE_SLOT_BASE = 0


async def test_actor(actor_db_name: str, fake_slot: int) -> None:
    print(f"\n[test] ── {actor_db_name} ──")

    industry = bc._ACTOR_INDUSTRY.get(actor_db_name, "")

    # 1. Generate
    profile = await stats_client.get_full_profile(actor_db_name)
    if not profile:
        print(f"[test] no profile data for {actor_db_name}, skipping")
        return

    recent_tweets = db.get_recent_tweet_texts_for_actor(actor_db_name, days=30)
    fact = await gen.generate_fact(actor_db_name, industry, profile, recent_tweets)

    if not fact:
        print(f"[test] generator returned nothing for {actor_db_name}")
        return

    # 2. Score
    score = await sc.score_fact(actor_db_name, industry, fact)
    total = score.get("total", 0)
    verdict = score.get("verdict", "?")
    print(f"[test] score {total}/60 ({verdict}) — {fact['section']} / {fact.get('stat_key','')}")

    # 3. Format tweet
    tweet_text = bc._format_tweet(actor_db_name, fact)
    print(f"[test] tweet:\n{tweet_text}\n")

    # 4. Screenshot
    png = await bc._capture(
        actor_db_name,
        fact["section"],
        compare_with=fact.get("compare_with", ""),
        chart_mode=fact.get("chart_mode", "rating"),
        director_name=fact.get("director_name", ""),
    )
    print(f"[test] screenshot: {'✅ captured' if png else '❌ failed'}")

    # 5. Insert into DB so approve/skip buttons work
    stat_key = fact.get("stat_key", "test")
    row_id = db.insert_scheduled_tweet(
        scheduled_date = date.today(),
        slot_hour      = fake_slot,
        actor_db_name  = actor_db_name,
        tweet_text     = tweet_text,
        stat_key       = stat_key,
        section        = fact["section"],
        chart_mode     = fact.get("chart_mode", "rating"),
        director_name  = fact.get("director_name", ""),
    )

    # 6. Send to Telegram
    slot_label = f"TEST slot {fake_slot}"
    msg_id = await send_scheduled_for_review(
        row_id     = row_id,
        slot_label = slot_label,
        actor_name = actor_db_name,
        tweet_text = tweet_text,
        screenshot = png,
        score      = score,
    )
    if msg_id:
        db.set_scheduled_telegram_id(row_id, msg_id)
        print(f"[test] ✅ sent to Telegram (row={row_id}, msg={msg_id})")
    else:
        print(f"[test] ❌ Telegram send failed")


async def main():
    db.run_migrations()

    actor_names = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_ACTORS

    # Resolve CLI names against ACTORS list (case-insensitive)
    actor_map = {a["db_name"].lower(): a["db_name"] for a in ACTORS}
    resolved = []
    for name in actor_names:
        key = name.lower()
        if key in actor_map:
            resolved.append(actor_map[key])
        else:
            # Partial match
            matches = [v for k, v in actor_map.items() if key in k]
            if matches:
                resolved.append(matches[0])
            else:
                print(f"[test] '{name}' not found in ACTORS — skipping")

    if not resolved:
        print("[test] no valid actors found, exiting")
        return

    print(f"[test] generating samples for: {resolved}")
    for i, actor in enumerate(resolved):
        await test_actor(actor, fake_slot=_FAKE_SLOT_BASE + i)

    print("\n[test] done — check your Telegram")


if __name__ == "__main__":
    asyncio.run(main())
