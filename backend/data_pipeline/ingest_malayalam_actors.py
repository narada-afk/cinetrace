"""
ingest_malayalam_actors.py
==========================
Sprint 9 — Ingest the 6 major Malayalam primary actors and their complete
filmographies from TMDB, expanding the dataset into Malayalam cinema.

Pipeline position
-----------------
    ingest_all_actors   (Wikidata — Telugu / Tamil / Kannada actors)
         │
         ├── ingest_malayalam_actors   ← this script (TMDB — Malayalam actors)
         │
         → enrich_tmdb_movies     (enriches movies WHERE tmdb_id IS NULL)
             → ingest_supporting_actors   (top-10 cast for every tmdb_id movie)
                 → build_analytics_tables

Actors ingested
---------------
    Mohanlal, Mammootty, Fahadh Faasil,
    Dulquer Salmaan, Prithviraj Sukumaran, Tovino Thomas

Why TMDB instead of Wikidata?
------------------------------
The existing Wikidata pipeline (ingest_all_actors.py) works well for actors
whose QIDs are already in actor_registry.  For Sprint 9 we use TMDB directly
because:
  • TMDB person credits include rich metadata (poster, rating, language).
  • TMDB is already integrated (Sprint 7/8), no extra dependencies.
  • Movie rows created here have tmdb_id set, so enrich_tmdb_movies will skip
    them and ingest_supporting_actors will process them immediately.

How it works
------------
For each of the 6 primary actors:
  1. Call search_person_tmdb(name) to resolve the TMDB person ID.
  2. Upsert the actor row with is_primary_actor=TRUE:
       • If already in actors (by tmdb_person_id or name) → promote + backfill.
       • Otherwise → insert new row (industry='Malayalam').
  3. Call fetch_person_movie_credits(person_id) for their full filmography.
  4. For each film credit:
       a. Check if the movie already exists by tmdb_id — skip if so.
       b. Insert new movie (industry from original_language mapping).
       c. Insert actor_movies row (role_type='primary').
  5. Commit once per actor for atomicity.

Industry mapping (TMDB original_language → movies.industry)
------------------------------------------------------------
    ml → Malayalam     ta → Tamil     te → Telugu
    kn → Kannada       hi → Hindi     en → English
    other → 'Malayalam' (actor's home industry as default)

Idempotency
-----------
All inserts use ON CONFLICT DO NOTHING.  Re-running the script is safe;
existing rows are detected by tmdb_id (movies) or tmdb_person_id/name (actors).

Prerequisites
-------------
  1. Apply the migration:
       psql ... -f backend/migrations/sprint9_add_movie_industry.sql
  2. Set environment variable:
       export TMDB_API_KEY=your_key_here

Usage
-----
    # From the backend/ directory:
    python -m data_pipeline.ingest_malayalam_actors
    python -m data_pipeline.ingest_malayalam_actors --dry-run
    python -m data_pipeline.ingest_malayalam_actors --limit 2
    python -m data_pipeline.ingest_malayalam_actors --actor "Mohanlal" --dry-run

Flags
-----
    --dry-run        Print all planned actions without writing to the database.
    --limit N        Process at most N actors (default: 0 = all 6).
    --actor NAME     Process only this actor (case-insensitive name match).

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
# Path bootstrap
# ---------------------------------------------------------------------------
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from data_pipeline.tmdb_client import (
    fetch_person_movie_credits,
    search_person_tmdb,
)


# ---------------------------------------------------------------------------
# Actor definitions
# ---------------------------------------------------------------------------

#: Canonical names used to search TMDB.  These must match the actor's TMDB
#: profile name closely enough for the top search result to be correct.
MALAYALAM_PRIMARY_ACTORS: list[str] = [
    "Mohanlal",
    "Mammootty",
    "Fahadh Faasil",
    "Dulquer Salmaan",
    "Prithviraj Sukumaran",
    "Tovino Thomas",
]

#: Maps TMDB original_language ISO 639-1 codes to the movies.industry values
#: used throughout this database.  Consistent with existing Wikidata-ingested
#: industry strings (title-case).
_LANG_TO_INDUSTRY: dict[str, str] = {
    "ml": "Malayalam",
    "ta": "Tamil",
    "te": "Telugu",
    "kn": "Kannada",
    "hi": "Hindi",
    "en": "English",
}

_DEFAULT_INDUSTRY = "Malayalam"   # fallback for unmapped languages


# ---------------------------------------------------------------------------
# Console formatting helpers
# ---------------------------------------------------------------------------

_SEP_BOLD = "=" * 64
_SEP_THIN = "-" * 64


def _print_header(actors: list[str], dry_run: bool) -> None:
    mode = "  [DRY RUN — no DB writes]" if dry_run else ""
    print(f"\n{_SEP_BOLD}")
    print(f"  Sprint 9 — Malayalam Actor Ingestion{mode}")
    print(f"  Actors to process : {len(actors)}")
    for name in actors:
        print(f"    • {name}")
    print(f"{_SEP_BOLD}\n")


def _print_actor_result(
    index: int,
    total: int,
    name: str,
    tmdb_person_id: Optional[int],
    movies_discovered: int,
    movies_inserted: int,
    movies_skipped: int,
    rels_inserted: int,
    rels_skipped: int,
) -> None:
    print(f"  ✓ [{index}/{total}] {name}")
    if tmdb_person_id:
        print(f"      TMDB person ID   : {tmdb_person_id}")
    print(f"      Movies discovered: {movies_discovered}")
    print(f"      Movies inserted  : {movies_inserted}")
    print(f"      Movies skipped   : {movies_skipped}  (already in DB)")
    print(f"      Rels inserted    : {rels_inserted}")
    print(f"      Rels skipped     : {rels_skipped}  (already existed)")


def _print_summary(
    actors_processed: int,
    actors_failed: int,
    total_movies_inserted: int,
    total_rels_inserted: int,
    elapsed: float,
    dry_run: bool,
) -> None:
    mode = "  [DRY RUN]" if dry_run else ""
    print(f"\n{_SEP_BOLD}")
    print(f"  Sprint 9 summary{mode}")
    print(_SEP_THIN)
    print(f"  Actors processed      : {actors_processed}")
    if actors_failed:
        print(f"  Actors failed         : {actors_failed}")
    print(f"  Movies inserted       : {total_movies_inserted}")
    print(f"  Relationships inserted: {total_rels_inserted}")
    print(f"  Elapsed               : {elapsed:.1f} s")
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
    """Update the pipeline_runs row to success or failed with stats JSON."""
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
# Database helpers — raw SQL for ON CONFLICT support
# ---------------------------------------------------------------------------

def _upsert_primary_actor(
    db: Session,
    name: str,
    tmdb_person_id: int,
    dry_run: bool,
) -> Optional[int]:
    """
    Ensure the actor exists in actors with is_primary_actor=TRUE.

    Resolution order (same as ingest_supporting_actors.py, reversed):
      1. Find by tmdb_person_id — update is_primary_actor=TRUE.
      2. Find by name (case-insensitive) — promote + backfill tmdb_person_id.
      3. Insert new row (industry='Malayalam', is_primary_actor=TRUE).

    In dry-run mode: performs the SELECT lookups (read-only) and prints what
    would change, but skips all writes.  Returns the existing actor_id when
    found, or None when the actor would need to be inserted.

    Returns actors.id on success, None on unrecoverable error.
    """
    # 1. Lookup by TMDB person ID
    row = db.execute(
        text("SELECT id FROM actors WHERE tmdb_person_id = :pid"),
        {"pid": tmdb_person_id},
    ).fetchone()
    if row:
        actor_id = row[0]
        if not dry_run:
            db.execute(
                text("UPDATE actors SET is_primary_actor = TRUE WHERE id = :id"),
                {"id": actor_id},
            )
            print(f"  ↑ Promoted (tmdb_person_id match): {name} → id={actor_id}")
        else:
            print(f"  ~ [DRY RUN] Would promote: {name} (id={actor_id}) → is_primary_actor=TRUE")
        return actor_id

    # 2. Lookup by name (catches actors discovered in Sprint 8 as supporting)
    row = db.execute(
        text("SELECT id, tmdb_person_id FROM actors WHERE lower(name) = lower(:n)"),
        {"n": name},
    ).fetchone()
    if row:
        actor_id, existing_pid = row[0], row[1]
        if not dry_run:
            if existing_pid is None:
                db.execute(
                    text("""
                        UPDATE actors
                        SET    is_primary_actor = TRUE,
                               tmdb_person_id   = :pid
                        WHERE  id = :id
                    """),
                    {"pid": tmdb_person_id, "id": actor_id},
                )
                print(f"  ↑ Promoted + backfilled: {name} → id={actor_id}")
            else:
                db.execute(
                    text("UPDATE actors SET is_primary_actor = TRUE WHERE id = :id"),
                    {"id": actor_id},
                )
                print(f"  ↑ Promoted (name match): {name} → id={actor_id}")
        else:
            action = "promote + backfill tmdb_person_id" if existing_pid is None else "promote to primary"
            print(f"  ~ [DRY RUN] Would {action}: {name} (id={actor_id})")
        return actor_id

    # 3. Insert new primary actor
    if dry_run:
        print(f"  + [DRY RUN] Would insert actor: {name} (TMDB person {tmdb_person_id})")
        return None  # no real id available in dry-run for new actors

    result = db.execute(
        text("""
            INSERT INTO actors
                (name, industry, is_primary_actor, tmdb_person_id, created_at)
            VALUES
                (:name, 'Malayalam', TRUE, :pid, NOW())
            ON CONFLICT (name) DO NOTHING
            RETURNING id
        """),
        {"name": name, "pid": tmdb_person_id},
    ).fetchone()

    if result:
        print(f"  + Actor inserted: {name} → id={result[0]}")
        return result[0]

    # Name conflict from a concurrent insert — re-query
    row = db.execute(
        text("SELECT id FROM actors WHERE lower(name) = lower(:n)"),
        {"n": name},
    ).fetchone()
    if row:
        print(f"  ↑ Actor found after conflict: {name} → id={row[0]}")
        return row[0]

    return None


def _get_or_insert_movie(
    db: Session,
    tmdb_id: int,
    title: str,
    release_year: Optional[int],
    original_language: Optional[str],
    vote_average: Optional[float],
    popularity: Optional[float],
    poster_url: Optional[str],
    backdrop_url: Optional[str],
    dry_run: bool,
) -> tuple[Optional[int], bool]:
    """
    Return (movie_id, is_new).

    Checks the movies table by tmdb_id first; inserts a new row only when the
    movie does not yet exist.  Never overwrites existing movie metadata.

    In dry-run mode, returns (None, True) for movies that would be inserted and
    (existing_id, False) for movies already in the DB.
    """
    # Check if movie already exists
    row = db.execute(
        text("SELECT id FROM movies WHERE tmdb_id = :tid"),
        {"tid": tmdb_id},
    ).fetchone()
    if row:
        return row[0], False

    # Determine industry from TMDB language code
    industry = _LANG_TO_INDUSTRY.get(original_language or "", _DEFAULT_INDUSTRY)
    year     = release_year or 0   # 0 = unknown (sentinel used across the pipeline)

    if dry_run:
        return None, True   # pretend inserted; no id available

    result = db.execute(
        text("""
            INSERT INTO movies
                (title, release_year, industry, language,
                 tmdb_id, vote_average, popularity, poster_url, backdrop_url)
            VALUES
                (:title, :year, :industry, :language,
                 :tmdb_id, :vote_average, :popularity, :poster_url, :backdrop_url)
            ON CONFLICT DO NOTHING
            RETURNING id
        """),
        {
            "title":        title,
            "year":         year,
            "industry":     industry,
            "language":     industry,   # consistent with existing pipeline convention
            "tmdb_id":      tmdb_id,
            "vote_average": vote_average,
            "popularity":   popularity,
            "poster_url":   poster_url,
            "backdrop_url": backdrop_url,
        },
    ).fetchone()

    if result:
        return result[0], True

    # ON CONFLICT fired (race condition) — re-query for the existing id
    row = db.execute(
        text("SELECT id FROM movies WHERE tmdb_id = :tid"),
        {"tid": tmdb_id},
    ).fetchone()
    return (row[0], False) if row else (None, False)


def _upsert_actor_movie(
    db: Session,
    actor_id: int,
    movie_id: int,
    character_name: Optional[str],
    billing_order: int,
    dry_run: bool,
) -> bool:
    """
    Insert an actor_movies row with role_type derived from billing_order.

    billing_order 0-2 (top 3 billed) → 'primary'  (lead / co-lead)
    billing_order 3+                  → 'supporting'

    This correctly handles cases where a primary actor has a cameo in another
    actor's film (e.g. billing=17 → 'supporting'), rather than hardcoding
    'primary' for every film in an actor's TMDB filmography.

    Returns True if a new row was created, False if it already existed.
    """
    if dry_run:
        return True   # pretend inserted

    role_type = "primary" if billing_order <= 2 else "supporting"

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

    return result is not None


# ---------------------------------------------------------------------------
# Per-actor processing
# ---------------------------------------------------------------------------

def _process_actor(
    name: str,
    index: int,
    total: int,
    dry_run: bool,
) -> dict:
    """
    Process one Malayalam primary actor end-to-end.

    Searches TMDB for the actor, fetches their filmography, upserts the actor
    row, inserts new movies, and creates actor_movies relationships — all in
    one DB session committed atomically per actor.

    Returns a summary dict:
        {
            "name":              str,
            "tmdb_person_id":    int | None,
            "movies_discovered": int,
            "movies_inserted":   int,
            "movies_skipped":    int,
            "rels_inserted":     int,
            "rels_skipped":      int,
            "error":             str | None,
        }
    """
    print(f"\n{_SEP_THIN}")
    print(f"[{index}/{total}] Processing actor: {name}")

    # ── Step 1: Resolve TMDB person ID ───────────────────────────────────────
    person = search_person_tmdb(name)
    if not person or not person.get("tmdb_person_id"):
        print(f"  ✗ TMDB search returned no results for '{name}' — skipped.")
        return {
            "name": name, "tmdb_person_id": None,
            "movies_discovered": 0, "movies_inserted": 0,
            "movies_skipped": 0, "rels_inserted": 0, "rels_skipped": 0,
            "error": "Not found on TMDB",
        }

    tmdb_person_id = person["tmdb_person_id"]
    tmdb_name      = person.get("name", name)
    print(f"  TMDB: {tmdb_name!r}  (person_id={tmdb_person_id})")

    # ── Step 2: Fetch filmography ─────────────────────────────────────────────
    films = fetch_person_movie_credits(tmdb_person_id)
    print(f"  Movies discovered: {len(films)}")

    if not films:
        print(f"  ✗ TMDB returned empty filmography for {name} — skipped.")
        return {
            "name": name, "tmdb_person_id": tmdb_person_id,
            "movies_discovered": 0, "movies_inserted": 0,
            "movies_skipped": 0, "rels_inserted": 0, "rels_skipped": 0,
            "error": "Empty filmography",
        }

    # ── Step 3: DB writes (single session, commit per actor) ──────────────────
    movies_inserted = 0
    movies_skipped  = 0
    rels_inserted   = 0
    rels_skipped    = 0
    error: Optional[str] = None

    db: Session = SessionLocal()
    try:
        # Upsert actor — get/create/promote the primary actor row
        actor_id = _upsert_primary_actor(db, name, tmdb_person_id, dry_run)

        for film in films:
            try:
                movie_id, is_new = _get_or_insert_movie(
                    db=db,
                    tmdb_id=film["tmdb_id"],
                    title=film["title"],
                    release_year=film["release_year"],
                    original_language=film["original_language"],
                    vote_average=film["vote_average"],
                    popularity=film["popularity"],
                    poster_url=film["poster_url"],
                    backdrop_url=film["backdrop_url"],
                    dry_run=dry_run,
                )

                if is_new:
                    movies_inserted += 1
                else:
                    movies_skipped += 1

                # Skip relationship row if we have no real IDs (dry-run new actor)
                if actor_id is None or movie_id is None:
                    if not dry_run:
                        # Real run: actor_id None means upsert failed — count as error
                        error = f"actor_id is None for {name}"
                        break
                    # dry-run: new actor, new movie — still count the relationship
                    rels_inserted += 1
                    continue

                inserted = _upsert_actor_movie(
                    db=db,
                    actor_id=actor_id,
                    movie_id=movie_id,
                    character_name=film["character"],
                    billing_order=film["cast_order"],
                    dry_run=dry_run,
                )
                if inserted:
                    rels_inserted += 1
                else:
                    rels_skipped += 1

            except Exception as exc:
                print(f"  ✗ Error on '{film.get('title', '?')}': {exc}")
                error = str(exc)

        if not dry_run:
            db.commit()

    except Exception as exc:
        db.rollback()
        error = str(exc)
        print(f"  ✗ DB commit failed for {name}: {exc}")
    finally:
        db.close()

    _print_actor_result(
        index=index,
        total=total,
        name=name,
        tmdb_person_id=tmdb_person_id,
        movies_discovered=len(films),
        movies_inserted=movies_inserted,
        movies_skipped=movies_skipped,
        rels_inserted=rels_inserted,
        rels_skipped=rels_skipped,
    )

    return {
        "name":              name,
        "tmdb_person_id":    tmdb_person_id,
        "movies_discovered": len(films),
        "movies_inserted":   movies_inserted,
        "movies_skipped":    movies_skipped,
        "rels_inserted":     rels_inserted,
        "rels_skipped":      rels_skipped,
        "error":             error,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def ingest_malayalam_actors(
    limit:   int  = 0,
    dry_run: bool = False,
    actor:   str  = "",
) -> int:
    """
    Ingest all 6 Malayalam primary actors and their TMDB filmographies.

    Parameters
    ----------
    limit   : int   Cap on total actors to process (0 = all 6).
    dry_run : bool  Print planned actions without any DB writes.
    actor   : str   If non-empty, process only this actor (name match).

    Returns
    -------
    0 on success, 1 if any actor failed.
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

    # Build the actor list for this run
    actors = list(MALAYALAM_PRIMARY_ACTORS)

    if actor:
        actors = [a for a in actors if a.lower() == actor.strip().lower()]
        if not actors:
            print(
                f"\n✗ Actor '{actor}' is not in the Malayalam primary actor list.\n"
                f"  Valid names: {', '.join(MALAYALAM_PRIMARY_ACTORS)}\n"
            )
            return 1

    if limit and limit > 0:
        actors = actors[:limit]

    _print_header(actors, dry_run)

    run_id  = _start_pipeline_run("malayalam_actor_ingestion")
    results: list[dict] = []

    for idx, name in enumerate(actors, start=1):
        result = _process_actor(name, idx, len(actors), dry_run)
        results.append(result)

    elapsed = time.monotonic() - t_start

    errors = [(r["name"], r["error"]) for r in results if r["error"]]

    _print_summary(
        actors_processed=len(results),
        actors_failed=len(errors),
        total_movies_inserted=sum(r["movies_inserted"] for r in results),
        total_rels_inserted=sum(r["rels_inserted"]   for r in results),
        elapsed=elapsed,
        dry_run=dry_run,
    )

    final_status = "success" if not errors else "failed"
    _finish_pipeline_run(
        run_id,
        final_status,
        {
            "actors_processed":   len(results),
            "actors_failed":      len(errors),
            "movies_inserted":    sum(r["movies_inserted"] for r in results),
            "rels_inserted":      sum(r["rels_inserted"]   for r in results),
            "elapsed_s":          round(elapsed, 1),
            "dry_run":            dry_run,
        },
    )

    return 1 if errors else 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest Malayalam primary actors and their TMDB filmographies.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python -m data_pipeline.ingest_malayalam_actors\n"
            "  python -m data_pipeline.ingest_malayalam_actors --dry-run\n"
            "  python -m data_pipeline.ingest_malayalam_actors --limit 2\n"
            "  python -m data_pipeline.ingest_malayalam_actors --actor Mohanlal\n"
            "  python -m data_pipeline.ingest_malayalam_actors --actor Mohanlal --dry-run\n"
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print all planned actions without writing to the database.",
    )
    p.add_argument(
        "--limit", "-n",
        type=int,
        default=0,
        metavar="N",
        help="Process at most N actors (default: 0 = all 6).",
    )
    p.add_argument(
        "--actor",
        type=str,
        default="",
        metavar="NAME",
        help="Process only this actor (case-insensitive name match against the 6 defined actors).",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(
        ingest_malayalam_actors(
            limit=args.limit,
            dry_run=args.dry_run,
            actor=args.actor,
        )
    )
