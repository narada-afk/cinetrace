"""
ingest_supporting_actors.py
===========================
Fetches cast data from TMDB for every movie that has a tmdb_id, then
inserts supporting actors and their actor↔movie relationships into the
database.

Pipeline position
-----------------
    ingest_all_actors
        → enrich_movies
            → enrich_tmdb_movies
                → ingest_supporting_actors   ← this script
                    → build_analytics_tables

How it works
------------
For each movie where movies.tmdb_id IS NOT NULL:
  1. Call TMDB GET /movie/{tmdb_id}/credits
  2. Take the top 10 billed cast members.
  3. For each cast member:
       a. Look up the actor by tmdb_person_id.
       b. If not found, look up by name (handles primary actors whose
          tmdb_person_id was unknown before this sprint).
       c. If still not found, insert a new actor row (is_primary_actor=FALSE).
       d. If found by name but tmdb_person_id is not yet set, backfill it.
  4. Upsert a row in actor_movies (ON CONFLICT DO NOTHING for idempotency).
  5. Commit per movie so progress survives interruptions.

Idempotency
-----------
All inserts use ON CONFLICT DO NOTHING.  Re-running the script is safe;
it simply finds everything already inserted and skips it.

Prerequisites
-------------
  1. Apply the migration:
       psql ... -f backend/migrations/sprint8_supporting_actor_schema.sql
  2. Run the TMDB movie enrichment first:
       python -m data_pipeline.enrich_tmdb_movies
  3. Set environment variable:
       export TMDB_API_KEY=your_key_here

Usage
-----
    # From the backend/ directory (preferred):
    python -m data_pipeline.ingest_supporting_actors
    python -m data_pipeline.ingest_supporting_actors --dry-run
    python -m data_pipeline.ingest_supporting_actors --batch-size 100
    python -m data_pipeline.ingest_supporting_actors --limit 50 --dry-run

Flags
-----
    --batch-size N   Fetch TMDB credits N movies at a time before committing
                     (default: 50; does not limit total movies processed).
    --limit N        Process at most N movies total (default: 0 = no limit).
    --dry-run        Print all actions without writing to the database.

Environment
-----------
    DATABASE_URL   PostgreSQL DSN (default: postgresql://sca:sca@postgres:5432/sca)
    TMDB_API_KEY   Your TMDB v3 API key (required)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Path bootstrap — ensures `app` and `data_pipeline` are importable when the
# script is run directly (python data_pipeline/ingest_supporting_actors.py)
# ---------------------------------------------------------------------------
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Movie
from data_pipeline.tmdb_client import fetch_movie_credits


# ---------------------------------------------------------------------------
# Console formatting
# ---------------------------------------------------------------------------

_SEP_THIN = "-" * 60
_SEP_BOLD = "=" * 60


def _print_header(total: int, dry_run: bool) -> None:
    mode = "  [DRY RUN — no DB writes]" if dry_run else ""
    print(f"\n{_SEP_BOLD}")
    print(f"  Supporting Actor Ingestion{mode}")
    print(f"  Movies to process : {total}")
    print(f"{_SEP_BOLD}\n")


def _print_movie(index: int, total: int, title: str, year: int) -> None:
    print(f"\n[{index}/{total}] Processing movie: {title} ({year})")
    print(_SEP_THIN)


def _print_summary(
    movies_processed: int,
    actors_inserted: int,
    actors_backfilled: int,
    relationships_inserted: int,
    relationships_skipped: int,
    errors: int,
    elapsed: float,
    dry_run: bool,
) -> None:
    mode = "  [DRY RUN]" if dry_run else ""
    print(f"\n{_SEP_BOLD}")
    print(f"  Supporting Actor Ingestion complete{mode}")
    print(_SEP_THIN)
    print(f"  Movies processed       : {movies_processed}")
    print(f"  Actors inserted        : {actors_inserted}")
    print(f"  Actors backfilled      : {actors_backfilled}  (tmdb_person_id added to existing actor)")
    print(f"  Relationships inserted : {relationships_inserted}")
    print(f"  Relationships skipped  : {relationships_skipped}  (already existed)")
    print(f"  Errors                 : {errors}")
    print(f"  Elapsed                : {elapsed:.1f} s")
    print(f"{_SEP_BOLD}\n")


# ---------------------------------------------------------------------------
# Pipeline run tracking
# ---------------------------------------------------------------------------

def _start_pipeline_run(run_type: str) -> Optional[int]:
    """Insert a pipeline_runs row with status='running'. Returns id or None."""
    try:
        from app.models import PipelineRun
        db: Session = SessionLocal()
        try:
            run = PipelineRun(
                run_type=run_type,
                started_at=datetime.now(timezone.utc),
                status="running",
            )
            db.add(run)
            db.commit()
            db.refresh(run)
            return run.id
        finally:
            db.close()
    except Exception as exc:
        print(f"  [pipeline_runs] Warning: could not record run start — {exc}")
        return None


def _finish_pipeline_run(run_id: Optional[int], status: str, details: dict) -> None:
    """Update the pipeline_runs row to success or failed."""
    if run_id is None:
        return
    try:
        from app.models import PipelineRun
        db: Session = SessionLocal()
        try:
            run = db.query(PipelineRun).filter(PipelineRun.id == run_id).first()
            if run:
                run.finished_at = datetime.now(timezone.utc)
                run.status      = status
                run.details     = json.dumps(details)
                db.commit()
        finally:
            db.close()
    except Exception as exc:
        print(f"  [pipeline_runs] Warning: could not update run record — {exc}")


# ---------------------------------------------------------------------------
# Database helpers — all use raw SQL for clarity and ON CONFLICT support
# ---------------------------------------------------------------------------

def _find_actor_by_tmdb_person_id(db: Session, tmdb_person_id: int) -> Optional[int]:
    """Return actors.id for the given TMDB person ID, or None."""
    row = db.execute(
        text("SELECT id FROM actors WHERE tmdb_person_id = :pid"),
        {"pid": tmdb_person_id},
    ).fetchone()
    return row[0] if row else None


def _find_actor_by_name(db: Session, name: str) -> Optional[tuple[int, Optional[int]]]:
    """
    Return (actors.id, tmdb_person_id) for a case-insensitive name match,
    or None if not found.
    """
    row = db.execute(
        text("SELECT id, tmdb_person_id FROM actors WHERE lower(name) = lower(:name)"),
        {"name": name},
    ).fetchone()
    return (row[0], row[1]) if row else None


def _insert_supporting_actor(
    db: Session, name: str, tmdb_person_id: int, dry_run: bool
) -> Optional[int]:
    """
    Insert a new supporting actor.  Returns the new actors.id, or None on
    conflict (actor already exists — should not happen if callers check first).
    """
    if dry_run:
        print(f"  + [DRY RUN] Would insert actor: {name} (TMDB person {tmdb_person_id})")
        return None

    result = db.execute(
        text("""
            INSERT INTO actors (name, industry, is_primary_actor, tmdb_person_id, created_at)
            VALUES (:name, 'Unknown', FALSE, :tmdb_person_id, NOW())
            ON CONFLICT (name) DO NOTHING
            RETURNING id
        """),
        {"name": name, "tmdb_person_id": tmdb_person_id},
    ).fetchone()

    if result:
        print(f"  + Actor inserted: {name}")
        return result[0]

    # name conflict — try to fetch the existing id
    existing = _find_actor_by_name(db, name)
    return existing[0] if existing else None


def _backfill_tmdb_person_id(
    db: Session, actor_id: int, tmdb_person_id: int, name: str, dry_run: bool
) -> None:
    """Set tmdb_person_id on an existing actor row that didn't have it yet."""
    if dry_run:
        print(f"  ~ [DRY RUN] Would backfill tmdb_person_id={tmdb_person_id} → {name}")
        return
    db.execute(
        text("""
            UPDATE actors
            SET    tmdb_person_id = :tmdb_person_id
            WHERE  id = :actor_id
              AND  tmdb_person_id IS NULL
        """),
        {"actor_id": actor_id, "tmdb_person_id": tmdb_person_id},
    )
    print(f"  ~ Backfilled tmdb_person_id for existing actor: {name}")


def _upsert_actor_movie(
    db: Session,
    actor_id: int,
    movie_id: int,
    character_name: Optional[str],
    billing_order: int,
    role_type: str,
    dry_run: bool,
) -> bool:
    """
    Insert a row into actor_movies.  Returns True if a new row was inserted,
    False if it already existed (ON CONFLICT DO NOTHING).
    """
    if dry_run:
        label = f"actor {actor_id}" if actor_id is not None else "actor (new)"
        print(f"  → [DRY RUN] Would create relationship: {label} ↔ movie {movie_id}")
        return True

    result = db.execute(
        text("""
            INSERT INTO actor_movies
                (actor_id, movie_id, character_name, billing_order, role_type)
            VALUES
                (:actor_id, :movie_id, :character_name, :billing_order, :role_type)
            ON CONFLICT (actor_id, movie_id) DO NOTHING
            RETURNING actor_id
        """),
        {
            "actor_id":       actor_id,
            "movie_id":       movie_id,
            "character_name": character_name,
            "billing_order":  billing_order,
            "role_type":      role_type,
        },
    ).fetchone()

    if result:
        print(f"  → Relationship created")
        return True
    return False


# ---------------------------------------------------------------------------
# Core per-movie processing
# ---------------------------------------------------------------------------

def _resolve_actor(
    db: Session,
    member: dict,
    dry_run: bool,
) -> tuple[Optional[int], str, int]:
    """
    Resolve a TMDB cast member to an actors.id.

    Lookup order:
      1. Find by tmdb_person_id (fastest, most accurate).
      2. Find by name (handles primary actors without tmdb_person_id yet).
         If found and tmdb_person_id is missing, backfill it.
      3. Insert a new supporting actor row.

    Returns (actor_id, action, backfilled_count) where action is one of
    'found', 'inserted', 'error'.
    """
    tmdb_pid = member["tmdb_person_id"]
    name     = member["name"]

    # 1. Lookup by TMDB person ID
    actor_id = _find_actor_by_tmdb_person_id(db, tmdb_pid)
    if actor_id:
        return actor_id, "found", 0

    # 2. Lookup by name
    name_match = _find_actor_by_name(db, name)
    if name_match:
        actor_id, existing_tmdb_pid = name_match
        if existing_tmdb_pid is None:
            _backfill_tmdb_person_id(db, actor_id, tmdb_pid, name, dry_run)
            return actor_id, "backfilled", 1
        return actor_id, "found", 0

    # 3. Insert new supporting actor
    actor_id = _insert_supporting_actor(db, name, tmdb_pid, dry_run)
    if actor_id is not None or dry_run:
        # dry_run: _insert_supporting_actor returns None (no DB write), but
        # that is expected — treat as a successful "would insert" action.
        return actor_id, "inserted", 0

    return None, "error", 0


def _process_movie(
    movie: Movie,
    index: int,
    total: int,
    dry_run: bool,
) -> dict:
    """
    Fetch TMDB credits for one movie, resolve actors, and insert relationships.

    Returns a summary dict with counts for the caller to aggregate.
    """
    _print_movie(index, total, movie.title, movie.release_year)

    credits = fetch_movie_credits(movie.tmdb_id, top_n=10)
    if not credits:
        print("  ✗ No cast data returned by TMDB — skipped.")
        return {"actors_inserted": 0, "actors_backfilled": 0,
                "rels_inserted": 0, "rels_skipped": 0, "error": False}

    print(f"  Cast members found: {len(credits)}")

    actors_inserted   = 0
    actors_backfilled = 0
    rels_inserted     = 0
    rels_skipped      = 0
    error             = False

    db: Session = SessionLocal()
    try:
        for member in credits:
            try:
                actor_id, action, backfilled = _resolve_actor(db, member, dry_run)

                if action == "error":
                    print(f"  ✗ Could not resolve actor: {member['name']}")
                    error = True
                    continue

                if action == "inserted":
                    actors_inserted += 1
                elif action == "backfilled":
                    actors_backfilled += backfilled

                # Determine role_type from billing_order, not from is_primary_actor.
                # billing_order 0-2 (top 3 billed) → 'primary' (lead / co-lead).
                # billing_order 3+               → 'supporting'.
                # This correctly marks cameos and guest appearances even when the
                # actor is a primary actor in the system (e.g. Adivi Sesh billing=12
                # in HIT 3 → supporting, even though he is a primary actor).
                cast_order = member.get("cast_order", 0) or 0
                role_type  = "primary" if cast_order <= 2 else "supporting"

                inserted = _upsert_actor_movie(
                    db=db,
                    actor_id=actor_id,
                    movie_id=movie.id,
                    character_name=member.get("character"),
                    billing_order=cast_order,
                    role_type=role_type,
                    dry_run=dry_run,
                )
                if inserted:
                    rels_inserted += 1
                else:
                    rels_skipped += 1

            except Exception as exc:
                print(f"  ✗ Error processing {member.get('name', '?')}: {exc}")
                error = True

        if not dry_run:
            db.commit()

    except Exception as exc:
        db.rollback()
        print(f"  ✗ DB commit failed for {movie.title}: {exc}")
        error = True
    finally:
        db.close()

    return {
        "actors_inserted":   actors_inserted,
        "actors_backfilled": actors_backfilled,
        "rels_inserted":     rels_inserted,
        "rels_skipped":      rels_skipped,
        "error":             error,
    }


def _role_type_from_billing(cast_order: int) -> str:
    """
    Return role_type based on billing position in the film's cast list.

    billing_order 0-2 (top 3 billed) → 'primary'  (lead / co-lead)
    billing_order 3+                  → 'supporting'

    Using billing_order as the source of truth instead of the actor's
    is_primary_actor flag ensures correctness for cameos and guest
    appearances: e.g. Adivi Sesh at billing=12 in HIT 3 is correctly
    marked 'supporting', even though he is a primary actor in the system.
    """
    return "primary" if cast_order <= 2 else "supporting"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def ingest_supporting_actors(
    batch_size: int  = 50,
    limit:      int  = 0,
    dry_run:    bool = False,
) -> int:
    """
    Main ingestion function.

    Parameters
    ----------
    batch_size : int   Number of movies to process before printing a progress
                       checkpoint (does not affect commit frequency — each
                       movie is committed individually).
    limit      : int   Cap on total movies to process (0 = no limit).
    dry_run    : bool  Print all actions without writing to the database.

    Returns
    -------
    0 on success, 1 on fatal error.
    """
    t_start = time.monotonic()

    # Validate API key before touching the DB
    if not os.getenv("TMDB_API_KEY", "").strip():
        print(
            "\n✗ TMDB_API_KEY is not set.\n"
            "  Get a free key at https://www.themoviedb.org/settings/api\n"
            "  Then run:  export TMDB_API_KEY=your_key_here\n"
        )
        return 1

    # Load movies with a known TMDB ID
    db: Session = SessionLocal()
    try:
        q = db.query(Movie).filter(Movie.tmdb_id != None).order_by(Movie.release_year.desc())  # noqa: E711
        if limit and limit > 0:
            q = q.limit(limit)
        movies = q.all()
    finally:
        db.close()

    total = len(movies)
    if total == 0:
        print("\n✓ No movies with tmdb_id found. Run enrich_tmdb_movies first.\n")
        return 0

    _print_header(total=total, dry_run=dry_run)

    run_id = _start_pipeline_run("supporting_actor_ingestion")

    # Aggregate counters
    n_actors_inserted   = 0
    n_actors_backfilled = 0
    n_rels_inserted     = 0
    n_rels_skipped      = 0
    n_errors            = 0

    for idx, movie in enumerate(movies, start=1):
        result = _process_movie(
            movie=movie,
            index=idx,
            total=total,
            dry_run=dry_run,
        )
        n_actors_inserted   += result["actors_inserted"]
        n_actors_backfilled += result["actors_backfilled"]
        n_rels_inserted     += result["rels_inserted"]
        n_rels_skipped      += result["rels_skipped"]
        if result["error"]:
            n_errors += 1

        # Batch checkpoint line
        if batch_size and idx % batch_size == 0:
            print(f"\n  ── Batch checkpoint: {idx}/{total} movies processed ──\n")

    elapsed = time.monotonic() - t_start

    _print_summary(
        movies_processed=total,
        actors_inserted=n_actors_inserted,
        actors_backfilled=n_actors_backfilled,
        relationships_inserted=n_rels_inserted,
        relationships_skipped=n_rels_skipped,
        errors=n_errors,
        elapsed=elapsed,
        dry_run=dry_run,
    )

    final_status = "success" if n_errors == 0 else "failed"
    _finish_pipeline_run(
        run_id,
        final_status,
        {
            "movies_processed":      total,
            "actors_inserted":       n_actors_inserted,
            "actors_backfilled":     n_actors_backfilled,
            "relationships_inserted": n_rels_inserted,
            "relationships_skipped": n_rels_skipped,
            "errors":                n_errors,
            "elapsed_s":             round(elapsed, 1),
            "dry_run":               dry_run,
        },
    )

    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest supporting actors from TMDB cast credits.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python -m data_pipeline.ingest_supporting_actors\n"
            "  python -m data_pipeline.ingest_supporting_actors --dry-run\n"
            "  python -m data_pipeline.ingest_supporting_actors --limit 50\n"
            "  python -m data_pipeline.ingest_supporting_actors --batch-size 25 --dry-run\n"
        ),
    )
    p.add_argument(
        "--batch-size", "-b",
        type=int,
        default=50,
        metavar="N",
        help="Print a checkpoint line every N movies (default: 50).",
    )
    p.add_argument(
        "--limit", "-n",
        type=int,
        default=0,
        metavar="N",
        help="Process at most N movies (default: 0 = no limit).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print all actions without writing to the database.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(
        ingest_supporting_actors(
            batch_size=args.batch_size,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    )
