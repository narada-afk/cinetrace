"""
audit_and_fix_filmographies.py
==============================
Audits every primary actor against TMDB and re-ingests missing films.

Steps
-----
1. Pull all is_primary_actor=True rows from the DB (with tmdb_person_id).
2. Call TMDB /person/{id}/movie_credits for each.
3. Compare TMDB count vs DB count.
4. For any actor with gap >= GAP_THRESHOLD, upsert the missing movies +
   actor_movies rows using the same helpers as ingest_primary_actors.py.
5. Print a full report.

Usage
-----
    export TMDB_API_KEY=<your_key>
    cd /Users/macmini/south-cinema-analytics/backend
    python -m data_pipeline.audit_and_fix_filmographies
    python -m data_pipeline.audit_and_fix_filmographies --dry-run
    python -m data_pipeline.audit_and_fix_filmographies --gap 5
"""

import argparse
import os
import sys
import time

from sqlalchemy.dialects.postgresql import insert as pg_insert

# ── path so we can import app.* ──────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.database import SessionLocal
from app import models
from sqlalchemy.orm import Session
from sqlalchemy import func

from data_pipeline.tmdb_client import fetch_person_movie_credits
from data_pipeline.ingest_primary_actors import (
    _get_or_insert_movie,
    _upsert_primary_actor,
)

# ── constants ─────────────────────────────────────────────────────────────────
GAP_THRESHOLD_DEFAULT = 3   # re-ingest if TMDB has this many more films than DB
RATE_LIMIT_SLEEP      = 0.26  # seconds between TMDB requests (~4 req/s, well under 40/10s)

LANG_FILTER = {"ta", "te", "ml", "kn"}   # South Indian languages to count on TMDB side


def get_primary_actors(db: Session) -> list:
    return (
        db.query(
            models.Actor.id,
            models.Actor.name,
            models.Actor.industry,
            models.Actor.tmdb_person_id,
        )
        .filter(
            models.Actor.is_primary_actor == True,  # noqa: E712
            models.Actor.tmdb_person_id != None,    # noqa: E711
        )
        .order_by(models.Actor.name)
        .all()
    )


def db_movie_count(db: Session, actor_id: int) -> int:
    return (
        db.query(func.count(models.ActorMovie.movie_id))
        .filter(models.ActorMovie.actor_id == actor_id)
        .scalar()
        or 0
    )


def upsert_missing_films(
    db: Session,
    actor_id: int,
    actor_name: str,
    actor_industry: str,
    tmdb_person_id: int,
    films: list[dict],
    dry_run: bool,
) -> dict:
    """Insert films from TMDB that are not yet in the DB for this actor."""
    inserted_movies  = 0
    inserted_rels    = 0
    skipped          = 0

    # Deduplicate by tmdb_id — TMDB sometimes returns the same film twice
    # (e.g. actor listed under different character names in the same movie).
    seen_tmdb: set[int] = set()
    unique_films = []
    for f in films:
        tid = f.get("tmdb_id")
        if tid and tid not in seen_tmdb:
            seen_tmdb.add(tid)
            unique_films.append(f)

    for film in unique_films:
        try:
            movie_id, is_new = _get_or_insert_movie(
                db=db,
                tmdb_id=film["tmdb_id"],
                title=film["title"],
                release_year=film["release_year"] or 0,
                original_language=film["original_language"],
                vote_average=film["vote_average"],
                popularity=film["popularity"],
                poster_url=film["poster_url"],
                backdrop_url=film["backdrop_url"],
                actor_industry=actor_industry,
                dry_run=dry_run,
            )
            if is_new:
                inserted_movies += 1
            else:
                skipped += 1

            if movie_id is None or actor_id is None:
                continue

            # Use ON CONFLICT DO NOTHING so re-runs are always safe
            if not dry_run:
                stmt = (
                    pg_insert(models.ActorMovie)
                    .values(actor_id=actor_id, movie_id=movie_id)
                    .on_conflict_do_nothing(index_elements=["actor_id", "movie_id"])
                )
                result = db.execute(stmt)
                if result.rowcount:
                    inserted_rels += 1
            else:
                # dry-run: count as new if not already in DB
                exists = (
                    db.query(models.ActorMovie)
                    .filter_by(actor_id=actor_id, movie_id=movie_id)
                    .first()
                )
                if not exists:
                    inserted_rels += 1

        except Exception as exc:
            print(f"    ⚠  Error on film '{film.get('title')}': {exc}")

    if not dry_run:
        db.commit()

    return {
        "inserted_movies": inserted_movies,
        "inserted_rels":   inserted_rels,
        "skipped":         skipped,
    }


def run(gap_threshold: int = GAP_THRESHOLD_DEFAULT, dry_run: bool = False) -> None:
    tag = " [DRY RUN]" if dry_run else ""
    print(f"\n{'='*65}")
    print(f"  South Cinema — Filmography Audit + Fix{tag}")
    print(f"  Gap threshold : {gap_threshold}  (re-ingest if TMDB − DB ≥ {gap_threshold})")
    print(f"{'='*65}\n")

    db = SessionLocal()
    actors = get_primary_actors(db)
    db.close()

    print(f"Found {len(actors)} primary actors with TMDB IDs.\n")

    results   = []
    fixed     = []
    clean     = []
    no_change = []

    for i, actor in enumerate(actors, 1):
        actor_id, name, industry, tmdb_pid = actor

        # ── TMDB fetch ───────────────────────────────────────────────────────
        films = fetch_person_movie_credits(tmdb_pid)
        time.sleep(RATE_LIMIT_SLEEP)

        # Count only South Indian language films on TMDB side
        south_films = [f for f in films if f.get("original_language") in LANG_FILTER]
        tmdb_count  = len(south_films)

        db2 = SessionLocal()
        db_count = db_movie_count(db2, actor_id)
        gap      = tmdb_count - db_count

        status_icon = "✅" if gap < gap_threshold else "⚠️ "
        print(f"[{i:02d}/{len(actors)}] {name:<25}  DB={db_count:>4}  TMDB={tmdb_count:>4}  gap={gap:>+4}  {status_icon}")

        row = {
            "name":       name,
            "industry":   industry,
            "db_count":   db_count,
            "tmdb_count": tmdb_count,
            "gap":        gap,
            "fixed":      False,
            "new_movies": 0,
            "new_rels":   0,
        }

        if gap >= gap_threshold:
            print(f"         → Re-ingesting {len(films)} TMDB films …")
            r = upsert_missing_films(
                db=db2,
                actor_id=actor_id,
                actor_name=name,
                actor_industry=industry,
                tmdb_person_id=tmdb_pid,
                films=films,          # pass ALL films (helper filters duplicates)
                dry_run=dry_run,
            )
            row["fixed"]      = True
            row["new_movies"] = r["inserted_movies"]
            row["new_rels"]   = r["inserted_rels"]
            fixed.append(row)
            print(f"         → +{r['inserted_movies']} new movies, +{r['inserted_rels']} new links")
        else:
            no_change.append(row)

        db2.close()
        results.append(row)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  SUMMARY{tag}")
    print(f"{'='*65}")
    print(f"  Total actors checked : {len(results)}")
    print(f"  Already complete     : {len(no_change)}")
    print(f"  Re-ingested          : {len(fixed)}")

    if fixed:
        print(f"\n  Actors fixed:")
        total_movies = 0
        total_rels   = 0
        for r in fixed:
            print(f"    {r['name']:<25}  DB was {r['db_count']:>4} → TMDB {r['tmdb_count']:>4}  "
                  f"+{r['new_movies']} movies  +{r['new_rels']} links")
            total_movies += r["new_movies"]
            total_rels   += r["new_rels"]
        print(f"\n  Total new movies inserted : {total_movies}")
        print(f"  Total new links inserted  : {total_rels}")
    else:
        print("\n  No actors needed re-ingestion — database looks complete!")

    if dry_run:
        print("\n  ⚠  Dry-run mode — no changes were committed to the database.")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audit and fix actor filmographies against TMDB")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    parser.add_argument("--gap",     type=int, default=GAP_THRESHOLD_DEFAULT,
                        help=f"Minimum gap to trigger re-ingestion (default {GAP_THRESHOLD_DEFAULT})")
    args = parser.parse_args()
    run(gap_threshold=args.gap, dry_run=args.dry_run)
