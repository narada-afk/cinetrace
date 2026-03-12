"""
ingest_primary_actors.py
========================
Sprint 18 — Expand the South Cinema Analytics database with Phase-1 primary
actors from Telugu, Tamil, Malayalam, and Kannada industries.

Four tasks in one run
----------------------
  TASK 1  Insert Phase-1 actors into actors (is_primary_actor=TRUE, correct industry).
          Actors already in the DB are promoted rather than re-inserted.

  TASK 2  Fix industry='Unknown' for existing actors who were previously ingested as
          supporting cast but whose correct industry is now known.  Actors that appear
          in both the UNKNOWN_INDUSTRY_FIXES list AND PHASE1_ACTORS are handled during
          the Phase-1 upsert (task 1) — no double-update needed.

  TASK 3  Fetch TMDB filmographies for Phase-1 actors and upsert movies +
          actor_movies rows (same pipeline pattern as ingest_malayalam_actors.py).

  TASK 4  Print a comprehensive summary:
          - actors_processed
          - industry_fixes_applied (task 2)
          - new_movies_added
          - new_cast_rows_added
          - actors_updated_from_unknown_industry
          - failed actors

Phase-1 actors (15 actors)
---------------------------
  Telugu   : Nani, Vijay Deverakonda, Ram Pothineni, Naga Chaitanya, Adivi Sesh
  Tamil    : Sivakarthikeyan, Vijay Sethupathi, Jayam Ravi, Vishal
  Malayalam: Nivin Pauly, Kunchacko Boban
  Kannada  : Yash, Rakshit Shetty, Rishab Shetty, Sudeep

Unknown-industry fixes (TASK 2 only — not in Phase-1)
------------------------------------------------------
  Telugu  : Satyadev Kancharana
  Tamil   : Arya, Jiiva
  Malayalam: Asif Ali, Unni Mukundan
  Kannada : Shiva Rajkumar, Dhananjay

  NOTE: Nani, Adivi Sesh, Sivakarthikeyan, Vijay Sethupathi, Rishab Shetty appear
  in BOTH lists — their industry is fixed automatically during the Phase-1 upsert.

Idempotency
-----------
All DB writes use ON CONFLICT DO NOTHING or UPDATE-by-id, so re-running the
script is safe.  Existing movies are detected by tmdb_id; existing actors are
detected by tmdb_person_id (first), then by name (case-insensitive).

Prerequisites
-------------
  export TMDB_API_KEY=your_key_here

Usage
-----
  # From the backend/ directory:
  python -m data_pipeline.ingest_primary_actors
  python -m data_pipeline.ingest_primary_actors --dry-run
  python -m data_pipeline.ingest_primary_actors --limit 3
  python -m data_pipeline.ingest_primary_actors --actor "Nani"
  python -m data_pipeline.ingest_primary_actors --actor "Nani" --dry-run
  python -m data_pipeline.ingest_primary_actors --skip-industry-fix

Flags
-----
  --dry-run            Print all planned actions without writing to the database.
  --limit N            Process at most N Phase-1 actors (default: 0 = all 15).
  --actor NAME         Process only this actor (case-insensitive name match).
  --skip-industry-fix  Skip TASK 2 (industry=Unknown fixes) and only do ingestion.

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

#: Phase-1 primary actors.  Each tuple is (canonical_tmdb_search_name, industry).
#: Industry must exactly match the values used in the movies table
#: ("Telugu", "Tamil", "Malayalam", "Kannada").
PHASE1_ACTORS: list[tuple[str, str]] = [
    # Telugu
    ("Nani",               "Telugu"),
    ("Vijay Deverakonda",  "Telugu"),
    ("Ram Pothineni",      "Telugu"),
    ("Naga Chaitanya",     "Telugu"),
    ("Adivi Sesh",         "Telugu"),
    # Tamil
    ("Sivakarthikeyan",    "Tamil"),
    ("Vijay Sethupathi",   "Tamil"),
    ("Jayam Ravi",         "Tamil"),
    ("Vishal",             "Tamil"),
    # Malayalam
    ("Nivin Pauly",        "Malayalam"),
    ("Kunchacko Boban",    "Malayalam"),
    # Kannada
    ("Yash",               "Kannada"),
    ("Rakshit Shetty",     "Kannada"),
    ("Rishab Shetty",      "Kannada"),
    ("Sudeep",             "Kannada"),
]

#: TASK 2: Actors already in the DB with industry='Unknown' who are NOT in
#: Phase-1.  Their industry is corrected with a targeted SQL UPDATE.
#: Phase-1 actors whose industry is also 'Unknown' are fixed automatically
#: during the _upsert_primary_actor step — no need to list them here.
UNKNOWN_INDUSTRY_FIXES: list[tuple[str, str]] = [
    # (exact name as stored in actors.name, correct_industry)
    ("Satyadev Kancharana", "Telugu"),
    ("Arya",                "Tamil"),
    ("Jiiva",               "Tamil"),
    ("Asif Ali",            "Malayalam"),
    ("Unni Mukundan",       "Malayalam"),
    ("Shiva Rajkumar",      "Kannada"),
    ("Dhananjay",           "Kannada"),
]

#: Maps TMDB original_language ISO 639-1 code → movies.industry string.
_LANG_TO_INDUSTRY: dict[str, str] = {
    "ml": "Malayalam",
    "ta": "Tamil",
    "te": "Telugu",
    "kn": "Kannada",
    "hi": "Hindi",
    "en": "English",
}


# ---------------------------------------------------------------------------
# Console formatting helpers
# ---------------------------------------------------------------------------

_SEP_BOLD = "=" * 64
_SEP_THIN = "-" * 64


def _print_header(actors: list[tuple[str, str]], dry_run: bool) -> None:
    mode = "  [DRY RUN — no DB writes]" if dry_run else ""
    print(f"\n{_SEP_BOLD}")
    print(f"  Sprint 18 — Phase-1 Primary Actor Ingestion{mode}")
    print(f"  Actors to process : {len(actors)}")
    for name, industry in actors:
        print(f"    • {name:<22} ({industry})")
    print(f"{_SEP_BOLD}\n")


def _print_summary(
    industry_fixes_applied: int,
    industry_fixes_not_found: list[str],
    actors_processed: int,
    actors_failed: int,
    failed_names: list[str],
    total_movies_inserted: int,
    total_rels_inserted: int,
    elapsed: float,
    dry_run: bool,
) -> None:
    mode = "  [DRY RUN]" if dry_run else ""
    print(f"\n{_SEP_BOLD}")
    print(f"  Sprint 18 summary{mode}")
    print(_SEP_THIN)
    print(f"  TASK 2 — Industry fixes")
    print(f"    actors_updated_from_unknown_industry : {industry_fixes_applied}")
    if industry_fixes_not_found:
        print(f"    not_found_in_db                      : {', '.join(industry_fixes_not_found)}")
    print(_SEP_THIN)
    print(f"  TASK 1+3 — Phase-1 ingestion")
    print(f"    actors_processed    : {actors_processed}")
    if actors_failed:
        print(f"    actors_failed       : {actors_failed}  ({', '.join(failed_names)})")
    print(f"    new_movies_added    : {total_movies_inserted}")
    print(f"    new_cast_rows_added : {total_rels_inserted}")
    print(f"    elapsed             : {elapsed:.1f} s")
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
# TASK 2 — Fix industry='Unknown' for non-Phase-1 actors
# ---------------------------------------------------------------------------

def fix_unknown_industries(dry_run: bool) -> dict:
    """
    TASK 2: Update actors.industry for the UNKNOWN_INDUSTRY_FIXES list.

    Skips actors whose industry is already correct (idempotent).
    Phase-1 actors that overlap with the Unknown list are intentionally
    excluded here — they are handled by _upsert_primary_actor with
    is_primary_actor=TRUE promotion.

    Returns
    -------
    {"updated": int, "not_found": list[str]}
    """
    print(f"\n{_SEP_BOLD}")
    print("  TASK 2 — Fixing industry='Unknown' actors")
    print(_SEP_BOLD)

    # Names that are Phase-1 actors — skip them here, they'll be handled later
    phase1_lower = {name.lower() for name, _ in PHASE1_ACTORS}

    updated   = 0
    not_found: list[str] = []

    db: Session = SessionLocal()
    try:
        for name, correct_industry in UNKNOWN_INDUSTRY_FIXES:
            if name.lower() in phase1_lower:
                print(f"  → Skipping {name!r} (handled by Phase-1 upsert)")
                continue

            row = db.execute(
                text("""
                    SELECT id, industry
                    FROM   actors
                    WHERE  lower(trim(name)) = lower(trim(:n))
                """),
                {"n": name},
            ).fetchone()

            if not row:
                not_found.append(name)
                print(f"  ✗ Not found in DB: {name!r}")
                continue

            actor_id, current_industry = row[0], row[1]

            if current_industry == correct_industry:
                print(f"  = Already correct: {name!r} ({current_industry})")
                continue

            if not dry_run:
                db.execute(
                    text("UPDATE actors SET industry = :ind WHERE id = :id"),
                    {"ind": correct_industry, "id": actor_id},
                )
            updated += 1
            action = "[DRY RUN] Would update" if dry_run else "Updated"
            print(f"  ✓ {action}: {name!r}  {current_industry!r} → {correct_industry!r}")

        if not dry_run:
            db.commit()
            print(f"\n  Committed {updated} industry update(s).")

    except Exception as exc:
        if not dry_run:
            db.rollback()
        print(f"  ✗ Error during industry fix: {exc}")
    finally:
        db.close()

    return {"updated": updated, "not_found": not_found}


# ---------------------------------------------------------------------------
# Database helpers — raw SQL (mirrors ingest_malayalam_actors.py)
# ---------------------------------------------------------------------------

def _upsert_primary_actor(
    db: Session,
    name: str,
    tmdb_person_id: int,
    industry: str,
    dry_run: bool,
) -> Optional[int]:
    """
    Ensure the actor exists in actors with is_primary_actor=TRUE and the
    correct industry.  Mirrors the logic in ingest_malayalam_actors.py but
    accepts an explicit industry parameter instead of hardcoding 'Malayalam'.

    Resolution order:
      1. Find by tmdb_person_id → promote + fix industry.
      2. Find by name (case-insensitive) → promote + backfill tmdb_person_id + fix industry.
      3. Insert new row.

    Returns actors.id on success, None in dry-run for brand-new actors.
    """
    # 1. Lookup by TMDB person ID
    row = db.execute(
        text("SELECT id, industry FROM actors WHERE tmdb_person_id = :pid"),
        {"pid": tmdb_person_id},
    ).fetchone()
    if row:
        actor_id, current_industry = row[0], row[1]
        if not dry_run:
            db.execute(
                text("""
                    UPDATE actors
                    SET    is_primary_actor = TRUE,
                           industry         = :ind
                    WHERE  id = :id
                """),
                {"ind": industry, "id": actor_id},
            )
            print(f"  ↑ Promoted (tmdb_person_id match): {name!r} → id={actor_id}"
                  + (f", industry: {current_industry!r} → {industry!r}" if current_industry != industry else ""))
        else:
            print(f"  ~ [DRY RUN] Would promote: {name!r} (id={actor_id}) → is_primary_actor=TRUE, industry={industry!r}")
        return actor_id

    # 2. Lookup by name (catches actors discovered in Sprint 8 as supporting)
    row = db.execute(
        text("SELECT id, tmdb_person_id, industry FROM actors WHERE lower(name) = lower(:n)"),
        {"n": name},
    ).fetchone()
    if row:
        actor_id, existing_pid, current_industry = row[0], row[1], row[2]
        if not dry_run:
            db.execute(
                text("""
                    UPDATE actors
                    SET    is_primary_actor = TRUE,
                           industry         = :ind,
                           tmdb_person_id   = COALESCE(tmdb_person_id, :pid)
                    WHERE  id = :id
                """),
                {"ind": industry, "pid": tmdb_person_id, "id": actor_id},
            )
            backfill = " + backfilled tmdb_person_id" if existing_pid is None else ""
            ind_note = f", industry: {current_industry!r} → {industry!r}" if current_industry != industry else ""
            print(f"  ↑ Promoted (name match): {name!r} → id={actor_id}{backfill}{ind_note}")
        else:
            action_parts = ["promote to primary", f"set industry={industry!r}"]
            if existing_pid is None:
                action_parts.append("backfill tmdb_person_id")
            print(f"  ~ [DRY RUN] Would {', '.join(action_parts)}: {name!r} (id={actor_id})")
        return actor_id

    # 3. Insert new primary actor
    if dry_run:
        print(f"  + [DRY RUN] Would insert actor: {name!r} (TMDB person {tmdb_person_id}, {industry})")
        return None

    result = db.execute(
        text("""
            INSERT INTO actors
                (name, industry, is_primary_actor, tmdb_person_id, created_at)
            VALUES
                (:name, :industry, TRUE, :pid, NOW())
            ON CONFLICT (name) DO NOTHING
            RETURNING id
        """),
        {"name": name, "industry": industry, "pid": tmdb_person_id},
    ).fetchone()

    if result:
        print(f"  + Actor inserted: {name!r} ({industry}) → id={result[0]}")
        return result[0]

    # Name conflict from a concurrent insert — re-query
    row = db.execute(
        text("SELECT id FROM actors WHERE lower(name) = lower(:n)"),
        {"n": name},
    ).fetchone()
    if row:
        print(f"  ↑ Actor found after conflict: {name!r} → id={row[0]}")
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
    actor_industry: str,
    dry_run: bool,
) -> tuple[Optional[int], bool]:
    """
    Return (movie_id, is_new).

    Checks by tmdb_id first; inserts only when the movie does not yet exist.
    Uses actor_industry as the fallback industry when the TMDB language code
    is not in _LANG_TO_INDUSTRY (e.g. for dubbed films).
    """
    row = db.execute(
        text("SELECT id FROM movies WHERE tmdb_id = :tid"),
        {"tid": tmdb_id},
    ).fetchone()
    if row:
        return row[0], False

    industry = _LANG_TO_INDUSTRY.get(original_language or "", actor_industry)
    year     = release_year or 0

    if dry_run:
        return None, True

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
            "language":     industry,
            "tmdb_id":      tmdb_id,
            "vote_average": vote_average,
            "popularity":   popularity,
            "poster_url":   poster_url,
            "backdrop_url": backdrop_url,
        },
    ).fetchone()

    if result:
        return result[0], True

    # ON CONFLICT — re-query for the existing id
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
    Insert actor_movies row with role_type='primary'.
    Returns True if a new row was created.
    """
    if dry_run:
        return True

    result = db.execute(
        text("""
            INSERT INTO actor_movies
                (actor_id, movie_id, character_name, billing_order, role_type)
            VALUES
                (:actor_id, :movie_id, :character_name, :billing_order, 'primary')
            ON CONFLICT (actor_id, movie_id) DO NOTHING
            RETURNING actor_id
        """),
        {
            "actor_id":       actor_id,
            "movie_id":       movie_id,
            "character_name": character_name,
            "billing_order":  billing_order,
        },
    ).fetchone()

    return result is not None


# ---------------------------------------------------------------------------
# Per-actor processing (TASK 1 + TASK 3)
# ---------------------------------------------------------------------------

def _process_actor(
    name: str,
    industry: str,
    index: int,
    total: int,
    dry_run: bool,
) -> dict:
    """
    Process one Phase-1 primary actor end-to-end:
      1. Search TMDB for the actor's person ID.
      2. Upsert the actor row (promote to primary + fix industry).
      3. Fetch their full filmography from TMDB.
      4. Insert new movies and actor_movies rows.
      5. Commit per actor for atomicity.

    Returns a summary dict.
    """
    print(f"\n{_SEP_THIN}")
    print(f"[{index}/{total}] Processing: {name!r}  ({industry})")

    # Step 1: Resolve TMDB person ID
    person = search_person_tmdb(name)
    if not person or not person.get("tmdb_person_id"):
        print(f"  ✗ TMDB search returned no results for {name!r} — skipped.")
        return {
            "name": name, "industry": industry, "tmdb_person_id": None,
            "movies_discovered": 0, "movies_inserted": 0,
            "movies_skipped": 0, "rels_inserted": 0, "rels_skipped": 0,
            "error": "Not found on TMDB",
        }

    tmdb_person_id = person["tmdb_person_id"]
    tmdb_name      = person.get("name", name)
    print(f"  TMDB: {tmdb_name!r}  (person_id={tmdb_person_id})")

    # Step 2: Fetch filmography
    films = fetch_person_movie_credits(tmdb_person_id)
    print(f"  Movies discovered: {len(films)}")

    if not films:
        print(f"  ✗ TMDB returned empty filmography for {name!r} — skipped.")
        return {
            "name": name, "industry": industry, "tmdb_person_id": tmdb_person_id,
            "movies_discovered": 0, "movies_inserted": 0,
            "movies_skipped": 0, "rels_inserted": 0, "rels_skipped": 0,
            "error": "Empty filmography",
        }

    # Step 3: DB writes (single session, commit per actor)
    movies_inserted = 0
    movies_skipped  = 0
    rels_inserted   = 0
    rels_skipped    = 0
    error: Optional[str] = None

    db: Session = SessionLocal()
    try:
        actor_id = _upsert_primary_actor(db, name, tmdb_person_id, industry, dry_run)

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
                    actor_industry=industry,
                    dry_run=dry_run,
                )

                if is_new:
                    movies_inserted += 1
                else:
                    movies_skipped += 1

                if actor_id is None or movie_id is None:
                    if not dry_run:
                        error = f"actor_id is None for {name!r}"
                        break
                    # dry-run: new actor + new movie — still count the relationship
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
                print(f"  ✗ Error on {film.get('title', '?')!r}: {exc}")
                error = str(exc)

        if not dry_run:
            db.commit()

    except Exception as exc:
        if not dry_run:
            db.rollback()
        error = str(exc)
        print(f"  ✗ DB commit failed for {name!r}: {exc}")
    finally:
        db.close()

    # Per-actor summary line
    print(f"  ✓ Done: movies_inserted={movies_inserted}, movies_skipped={movies_skipped}, "
          f"rels_inserted={rels_inserted}, rels_skipped={rels_skipped}")
    if error:
        print(f"  ✗ Error: {error}")

    return {
        "name":              name,
        "industry":          industry,
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

def ingest_primary_actors(
    limit:              int  = 0,
    dry_run:            bool = False,
    actor:              str  = "",
    skip_industry_fix:  bool = False,
) -> int:
    """
    Run all 4 tasks for Phase-1 primary actor expansion.

    Parameters
    ----------
    limit             : Cap on total actors to process (0 = all 15).
    dry_run           : Print planned actions without any DB writes.
    actor             : If non-empty, process only this actor (name match).
    skip_industry_fix : Skip TASK 2 industry fixes.

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

    # ── TASK 2: Fix Unknown industries ───────────────────────────────────────
    industry_fix_result = {"updated": 0, "not_found": []}
    if not skip_industry_fix and not actor:
        # Only run the blanket industry fix when doing a full run (not single-actor)
        industry_fix_result = fix_unknown_industries(dry_run)

    # ── Build Phase-1 actor list for this run ─────────────────────────────────
    actors = list(PHASE1_ACTORS)

    if actor:
        actors = [(n, ind) for n, ind in actors if n.lower() == actor.strip().lower()]
        if not actors:
            valid = ", ".join(n for n, _ in PHASE1_ACTORS)
            print(
                f"\n✗ Actor {actor!r} is not in the Phase-1 list.\n"
                f"  Valid names: {valid}\n"
            )
            return 1

    if limit and limit > 0:
        actors = actors[:limit]

    _print_header(actors, dry_run)

    run_id  = _start_pipeline_run("phase1_primary_actor_ingestion")
    results: list[dict] = []

    # ── TASK 1 + TASK 3: Ingest each actor ───────────────────────────────────
    for idx, (name, industry) in enumerate(actors, start=1):
        result = _process_actor(name, industry, idx, len(actors), dry_run)
        results.append(result)

    elapsed = time.monotonic() - t_start

    errors      = [(r["name"], r["error"]) for r in results if r["error"]]
    failed_names = [r["name"] for r in results if r["error"]]

    # ── TASK 4: Print summary ─────────────────────────────────────────────────
    _print_summary(
        industry_fixes_applied=industry_fix_result["updated"],
        industry_fixes_not_found=industry_fix_result["not_found"],
        actors_processed=len(results),
        actors_failed=len(errors),
        failed_names=failed_names,
        total_movies_inserted=sum(r["movies_inserted"] for r in results),
        total_rels_inserted=sum(r["rels_inserted"]    for r in results),
        elapsed=elapsed,
        dry_run=dry_run,
    )

    final_status = "success" if not errors else "partial"
    _finish_pipeline_run(
        run_id,
        final_status,
        {
            "industry_fixes_applied":              industry_fix_result["updated"],
            "actors_updated_from_unknown_industry": industry_fix_result["updated"],
            "actors_processed":                    len(results),
            "actors_failed":                       len(errors),
            "new_movies_added":                    sum(r["movies_inserted"] for r in results),
            "new_cast_rows_added":                 sum(r["rels_inserted"]   for r in results),
            "elapsed_s":                           round(elapsed, 1),
            "dry_run":                             dry_run,
        },
    )

    return 1 if errors else 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest Phase-1 primary actors and fix Unknown industries.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python -m data_pipeline.ingest_primary_actors\n"
            "  python -m data_pipeline.ingest_primary_actors --dry-run\n"
            "  python -m data_pipeline.ingest_primary_actors --limit 3\n"
            "  python -m data_pipeline.ingest_primary_actors --actor Nani\n"
            "  python -m data_pipeline.ingest_primary_actors --actor Nani --dry-run\n"
            "  python -m data_pipeline.ingest_primary_actors --skip-industry-fix\n"
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
        help="Process at most N Phase-1 actors (default: 0 = all 15).",
    )
    p.add_argument(
        "--actor",
        type=str,
        default="",
        metavar="NAME",
        help="Process only this actor (case-insensitive name match).",
    )
    p.add_argument(
        "--skip-industry-fix",
        action="store_true",
        help="Skip TASK 2 (industry=Unknown fixes).",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(
        ingest_primary_actors(
            limit=args.limit,
            dry_run=args.dry_run,
            actor=args.actor,
            skip_industry_fix=args.skip_industry_fix,
        )
    )
