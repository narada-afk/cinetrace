"""
ingest_network_actors.py
========================
Fetches full TMDB filmographies for every actor marked actor_tier='network'
and upserts missing movies + actor_movies rows.

Network actors are the top-40 supporting connectors ranked by:
  score = unique_costars × 3 + film_count + industry_coverage × 15

This script is safe to re-run (ON CONFLICT DO NOTHING throughout).

Usage
-----
    export TMDB_API_KEY=<your_key>
    cd /Users/macmini/south-cinema-analytics/backend
    python -m data_pipeline.ingest_network_actors
    python -m data_pipeline.ingest_network_actors --dry-run
"""

import argparse
import os
import sys
import time

# ── path so we can import app.* ──────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.database import SessionLocal
from app import models
from sqlalchemy.orm import Session
from sqlalchemy import func

from data_pipeline.tmdb_client import fetch_person_movie_credits
from data_pipeline.audit_and_fix_filmographies import upsert_missing_films

# ── constants ─────────────────────────────────────────────────────────────────
RATE_LIMIT_SLEEP = 0.26   # seconds between TMDB requests (~4 req/s)
LANG_FILTER      = {"ta", "te", "ml", "kn"}

LANG_TO_INDUSTRY = {
    "ta": "Tamil",
    "te": "Telugu",
    "ml": "Malayalam",
    "kn": "Kannada",
}


def get_network_actors(db: Session) -> list:
    """Return all actors with actor_tier='network' that have a TMDB person ID."""
    return (
        db.query(
            models.Actor.id,
            models.Actor.name,
            models.Actor.industry,
            models.Actor.tmdb_person_id,
        )
        .filter(
            models.Actor.actor_tier == "network",
            models.Actor.tmdb_person_id != None,   # noqa: E711
        )
        .order_by(models.Actor.name)
        .all()
    )


def db_film_count(db: Session, actor_id: int) -> int:
    return (
        db.query(func.count(models.ActorMovie.movie_id))
        .filter(models.ActorMovie.actor_id == actor_id)
        .scalar()
        or 0
    )


def derive_industry(films: list[dict]) -> str:
    """Pick the most common South Indian industry from a TMDB credit list."""
    counts: dict[str, int] = {}
    for f in films:
        lang = f.get("original_language", "")
        ind  = LANG_TO_INDUSTRY.get(lang)
        if ind:
            counts[ind] = counts.get(ind, 0) + 1
    return max(counts, key=counts.get) if counts else "Unknown"


def run(dry_run: bool = False) -> None:
    tag = " [DRY RUN]" if dry_run else ""
    print(f"\n{'='*65}")
    print(f"  South Cinema — Network Actor Ingestion{tag}")
    print(f"{'='*65}\n")

    db = SessionLocal()
    actors = get_network_actors(db)
    db.close()

    print(f"Found {len(actors)} network actors to process.\n")

    total_new_movies = 0
    total_new_rels   = 0
    fixed            = []
    already_full     = []

    for i, actor in enumerate(actors, 1):
        actor_id, name, _, tmdb_pid = actor

        # Derive best industry from their TMDB credits
        films = fetch_person_movie_credits(tmdb_pid)
        time.sleep(RATE_LIMIT_SLEEP)

        industry = derive_industry(films)

        south_count = len([f for f in films if f.get("original_language") in LANG_FILTER])

        db2     = SessionLocal()
        db_cnt  = db_film_count(db2, actor_id)

        print(f"[{i:02d}/{len(actors)}] {name:<28}  DB={db_cnt:>4}  TMDB≈{south_count:>4}  ({industry})")

        result = upsert_missing_films(
            db=db2,
            actor_id=actor_id,
            actor_name=name,
            actor_industry=industry,
            tmdb_person_id=tmdb_pid,
            films=films,
            dry_run=dry_run,
        )
        db2.close()

        nm = result["inserted_movies"]
        nr = result["inserted_rels"]
        if nm or nr:
            print(f"           → +{nm} new movies, +{nr} new links")
            fixed.append((name, db_cnt, nm, nr))
        else:
            print(f"           → already complete")
            already_full.append(name)

        total_new_movies += nm
        total_new_rels   += nr

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  SUMMARY{tag}")
    print(f"{'='*65}")
    print(f"  Network actors processed  : {len(actors)}")
    print(f"  Already complete          : {len(already_full)}")
    print(f"  Updated with new data     : {len(fixed)}")
    print(f"  Total new movies inserted : {total_new_movies}")
    print(f"  Total new links inserted  : {total_new_rels}")

    if fixed:
        print(f"\n  Updated actors:")
        for name, was, nm, nr in sorted(fixed, key=lambda x: -x[2]):
            print(f"    {name:<28}  was {was:>4} films  +{nm} movies  +{nr} links")

    if dry_run:
        print("\n  ⚠  Dry-run mode — no changes were committed.")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest full filmographies for all network-tier actors"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing to DB")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
