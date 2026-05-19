"""
daily_enrichment.py
===================
Runs nightly to keep the DB accurate and send a Telegram digest.

Steps:
  1. Enrich missing box office data from TMDB
  2. Re-enrich films released in the last 6 months (BO data still coming in)
  3. Report what was filled, what's still missing, and flag notable gaps

Usage (from backend container):
    python -m data_pipeline.daily_enrichment

Environment:
    DATABASE_URL        — Postgres connection string
    TMDB_API_KEY        — TMDB v3 API key
    TELEGRAM_BOT_TOKEN  — Telegram bot token
    TELEGRAM_CHAT_ID    — Telegram chat ID to send digest to
"""

import os
import sys
import time
import requests
from datetime import date, timedelta

from sqlalchemy import text
from app.database import SessionLocal
from app.models import Movie
from data_pipeline.enrich_box_office import enrich_box_office
from data_pipeline.tmdb_client import fetch_movie_details

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
TMDB_API_KEY       = os.getenv("TMDB_API_KEY", "")
USD_TO_INR         = 84.0
RECENT_MONTHS      = 6   # re-enrich films released within this window
NOTABLE_THRESHOLD  = 20  # ₹Cr — flag missing BO if film looks notable (has imdb_rating)


def _send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[enrichment] Telegram not configured — skipping notification")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        print(f"[enrichment] Telegram send failed: {e}")


def _count_missing(db) -> int:
    return db.query(Movie).filter(
        Movie.tmdb_id.isnot(None),
        Movie.box_office.is_(None),
        Movie.release_year > 0,
    ).count()


def _reenrich_recent(db) -> int:
    """Re-fetch BO for films released in the last RECENT_MONTHS months — data is still being updated."""
    cutoff_year = date.today().year - 1
    cutoff_month = date.today().month
    recent_films = db.query(Movie).filter(
        Movie.tmdb_id.isnot(None),
        Movie.release_year >= cutoff_year,
    ).all()

    updated = 0
    for movie in recent_films:
        try:
            details = fetch_movie_details(movie.tmdb_id)
            if not details or not details.get("revenue"):
                continue
            crore = round(details["revenue"] * USD_TO_INR / 10_000_000, 2)
            if crore < 0.5:
                continue
            if movie.box_office != crore:
                movie.box_office = crore
                db.commit()
                print(f"[enrichment] updated {movie.title} ({movie.release_year}) → ₹{crore}Cr")
                updated += 1
        except Exception as e:
            print(f"[enrichment] error re-enriching {movie.title}: {e}")
    return updated


def _find_notable_gaps(db) -> list[dict]:
    """Films with no BO data that look notable — imdb_rating >= 6.5, released 2018+."""
    rows = db.execute(text("""
        SELECT title, release_year, imdb_rating, tmdb_id
        FROM movies
        WHERE box_office IS NULL
          AND release_year >= 2018
          AND release_year > 0
          AND imdb_rating >= 6.5
          AND (is_documentary IS NULL OR is_documentary = FALSE)
        ORDER BY release_year DESC
        LIMIT 20
    """)).fetchall()
    return [
        {"title": r[0], "year": r[1], "rating": r[2], "has_tmdb": r[3] is not None}
        for r in rows
    ]


def run():
    t_start = time.monotonic()
    db = SessionLocal()
    has_tmdb = bool(TMDB_API_KEY.strip())

    try:
        filled = 0
        re_updated = 0

        if has_tmdb:
            missing_before = _count_missing(db)
            print(f"[enrichment] {missing_before} films missing BO data before enrichment")

            # Step 1: Fill missing BO from TMDB
            enrich_box_office(batch_size=500, min_crore=0.5)

            missing_after = _count_missing(db)
            filled = missing_before - missing_after
            print(f"[enrichment] filled {filled} films, {missing_after} still missing")

            # Step 2: Re-enrich recent films
            db.expire_all()
            re_updated = _reenrich_recent(db)
            print(f"[enrichment] re-enriched {re_updated} recent films")
        else:
            print("[enrichment] TMDB_API_KEY not set — skipping enrichment, running gap audit only")

        # Step 3: Find notable gaps (always runs)
        db.expire_all()
        gaps = _find_notable_gaps(db)

        elapsed = round(time.monotonic() - t_start)

        # Build Telegram digest
        lines = [f"📊 *Daily DB Enrichment — {date.today()}*\n"]

        if has_tmdb:
            lines.append(f"✅ BO data filled: *{filled}* films")
            lines.append(f"🔄 Recent films re-synced: *{re_updated}* films")
        else:
            lines.append("⚠️ TMDB_API_KEY not set — enrichment skipped")
        lines.append(f"⏱ Time: {elapsed}s\n")

        if gaps:
            lines.append(f"⚠️ *{len(gaps)} notable films still missing BO data:*")
            for g in gaps:
                tmdb_flag = "" if g["has_tmdb"] else " _(no TMDB ID)_"
                lines.append(f"  • {g['title']} ({g['year']}) — IMDb {g['rating']}{tmdb_flag}")
        else:
            lines.append("✅ No notable BO data gaps found")

        message = "\n".join(lines)
        print(message)
        _send_telegram(message)

    finally:
        db.close()


if __name__ == "__main__":
    run()
