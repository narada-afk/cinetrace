"""
enrich_tmdb_movies.py
=====================
Enriches movie records in the database with metadata from TMDB
(The Movie Database).

For every movie where tmdb_id IS NULL the script:
  1. Calls search_movie_tmdb(title, release_year) to query TMDB.
  2. Updates only columns that are currently NULL (never overwrites
     existing data — not even poster_url / backdrop_url if already set).
  3. Commits each movie individually so progress is preserved if the
     script is interrupted.

Fields updated (only when currently NULL):
    tmdb_id        — written even in conservative mode (it's the sentinel)
    poster_url     — TMDB w500 poster image URL
    backdrop_url   — TMDB w780 backdrop image URL
    vote_average   — TMDB community vote average (0.0–10.0)
    popularity     — TMDB popularity score

Prerequisites:
  1. Apply the migration:
       psql ... -f backend/migrations/sprint7_tmdb_columns.sql
  2. Set environment variable:
       export TMDB_API_KEY=your_key_here

Usage:
    # From the backend/ directory (preferred):
    python -m data_pipeline.enrich_tmdb_movies
    python -m data_pipeline.enrich_tmdb_movies --dry-run
    python -m data_pipeline.enrich_tmdb_movies --batch-size 50
    python -m data_pipeline.enrich_tmdb_movies --industry Telugu --dry-run

    # Or directly:
    python data_pipeline/enrich_tmdb_movies.py --batch-size 20

Flags:
    --dry-run        Print what would be updated without writing to DB.
    --batch-size N   Process at most N movies per run (default: unlimited).
    --industry X     Filter to movies whose industry = X (e.g. "Telugu").

Environment:
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
# Path bootstrap — same pattern used across the data_pipeline package
# ---------------------------------------------------------------------------
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Movie
from data_pipeline.tmdb_client import search_movie_tmdb


# ---------------------------------------------------------------------------
# Console formatting helpers
# ---------------------------------------------------------------------------

_SEP_THIN = "-" * 56
_SEP_BOLD = "=" * 56


def _print_header(total: int, dry_run: bool, industry: str) -> None:
    mode = "  [DRY RUN — no DB writes]" if dry_run else ""
    print(f"\n{_SEP_BOLD}")
    print(f"  TMDB Enrichment{mode}")
    print(f"  Movies to process : {total}")
    if industry:
        print(f"  Industry filter   : {industry}")
    print(f"{_SEP_BOLD}\n")


def _print_movie_block(
    index: int,
    total: int,
    title: str,
    year: int,
    result: Optional[dict],
    updates: dict,
    dry_run: bool,
    error: Optional[str] = None,
) -> None:
    """Print a formatted block for one processed movie."""
    print(f"[{index}/{total}] Processing: {title} ({year})")
    print(_SEP_THIN)

    if error:
        print(f"  ✗ Error: {error}")
        print()
        return

    if result is None:
        print("  ✗ Not found on TMDB — skipped.")
        print()
        return

    # Per-field status lines
    _print_field("TMDB ID",       result.get("tmdb_id"),      "tmdb_id"      in updates, dry_run)
    _print_field("Poster",        result.get("poster_url"),   "poster_url"   in updates, dry_run)
    _print_field("Backdrop",      result.get("backdrop_url"), "backdrop_url" in updates, dry_run)
    _print_field("Vote average",  result.get("vote_average"), "vote_average" in updates, dry_run)
    _print_field("Popularity",    result.get("popularity"),   "popularity"   in updates, dry_run)

    if updates:
        verb = "Would save" if dry_run else "Saved successfully"
        print(f"  → {verb} ({len(updates)} field(s) updated).")
    else:
        print("  → All fields already populated — skipped.")
    print()


def _print_field(label: str, value, will_update: bool, dry_run: bool) -> None:
    if value is None:
        print(f"  {'  ' + label:<22}: —  (not returned by TMDB)")
        return

    if will_update:
        status = "  (would set)" if dry_run else "  ✓"
        # Truncate long URLs for readability
        display = str(value)
        if len(display) > 55:
            display = display[:52] + "..."
        print(f"  {'  ' + label:<22}: {display}{status}")
    else:
        print(f"  {'  ' + label:<22}: already set — skipped")


def _print_summary(
    processed: int,
    updated: int,
    not_found: int,
    skipped: int,
    errors: int,
    elapsed: float,
    dry_run: bool,
) -> None:
    mode = "  [DRY RUN]" if dry_run else ""
    print(f"\n{_SEP_BOLD}")
    print(f"  TMDB Enrichment complete{mode}")
    print(_SEP_THIN)
    print(f"  Processed  : {processed}")
    print(f"  Updated    : {updated}")
    print(f"  Not found  : {not_found}")
    print(f"  Skipped    : {skipped}  (all fields already set)")
    print(f"  Errors     : {errors}")
    print(f"  Elapsed    : {elapsed:.1f} s")
    print(f"{_SEP_BOLD}\n")


# ---------------------------------------------------------------------------
# Pipeline run tracking (mirrors the pattern in enrich_movies.py)
# ---------------------------------------------------------------------------

def _start_pipeline_run(run_type: str) -> Optional[int]:
    """Insert a pipeline_runs row with status='running'. Returns row id or None."""
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
# Core per-movie logic
# ---------------------------------------------------------------------------

def _compute_updates(movie: Movie, result: dict) -> dict:
    """
    Return a dict of {column: value} for fields that are currently NULL
    and have a non-None value from TMDB.

    Never overwrites a field that already has a value.
    """
    updates: dict = {}

    field_map = {
        "tmdb_id":      result.get("tmdb_id"),
        "poster_url":   result.get("poster_url"),
        "backdrop_url": result.get("backdrop_url"),
        "vote_average": result.get("vote_average"),
        "popularity":   result.get("popularity"),
    }

    for col, new_val in field_map.items():
        if new_val is not None and getattr(movie, col, None) is None:
            updates[col] = new_val

    return updates


def _process_one_movie(
    movie: Movie,
    dry_run: bool,
    index: int,
    total: int,
) -> dict:
    """
    Fetch TMDB data for one movie, apply updates, and print a log block.

    Returns a summary dict:
        updated   : bool
        not_found : bool
        error     : str | None
    """
    title = movie.title
    year  = movie.release_year

    result: Optional[dict] = None
    updates: dict          = {}
    error: Optional[str]   = None

    try:
        result = search_movie_tmdb(title, year)
    except RuntimeError as exc:
        # TMDB_API_KEY missing — fatal, re-raise so main loop can abort
        raise
    except Exception as exc:
        error = str(exc)

    if result and not error:
        updates = _compute_updates(movie, result)

        if updates and not dry_run:
            try:
                db: Session = SessionLocal()
                try:
                    db_movie = db.query(Movie).filter(Movie.id == movie.id).first()
                    if db_movie:
                        for col, val in updates.items():
                            setattr(db_movie, col, val)
                        db.commit()
                finally:
                    db.close()
            except Exception as exc:
                error = f"DB write failed: {exc}"
                updates = {}

    _print_movie_block(
        index=index,
        total=total,
        title=title,
        year=year,
        result=result,
        updates=updates,
        dry_run=dry_run,
        error=error,
    )

    return {
        "updated":   bool(updates and not error),
        "not_found": result is None and not error,
        "error":     error,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def enrich_tmdb_movies(
    batch_size: int  = 0,
    dry_run:    bool = False,
    industry:   str  = "",
) -> int:
    """
    Fetch TMDB metadata for all movies where tmdb_id IS NULL.

    Parameters
    ----------
    batch_size : int   Max movies to process (0 = no limit).
    dry_run    : bool  If True, print results but make no DB writes.
    industry   : str   If non-empty, restrict to movies.industry = industry.

    Returns
    -------
    0 on success, 1 on fatal error.
    """
    t_start = time.monotonic()

    # -- Validate API key early so we fail fast before touching the DB -------
    api_key = os.getenv("TMDB_API_KEY", "").strip()
    if not api_key:
        print(
            "\n✗ TMDB_API_KEY is not set.\n"
            "  Get a free key at https://www.themoviedb.org/settings/api\n"
            "  Then run:  export TMDB_API_KEY=your_key_here\n"
        )
        return 1

    # -- Query: movies where tmdb_id IS NULL ---------------------------------
    db: Session = SessionLocal()
    try:
        q = db.query(Movie).filter(Movie.tmdb_id == None)  # noqa: E711
        if industry:
            q = q.filter(Movie.industry == industry)
        q = q.order_by(Movie.release_year.desc())
        if batch_size and batch_size > 0:
            q = q.limit(batch_size)
        movies = q.all()
    finally:
        db.close()

    total = len(movies)
    if total == 0:
        print("\n✓ No movies need TMDB enrichment — all records already have a tmdb_id.\n")
        return 0

    _print_header(total=total, dry_run=dry_run, industry=industry)

    # -- Pipeline run tracking -----------------------------------------------
    run_id = _start_pipeline_run("tmdb_enrichment")

    # -- Process movies -------------------------------------------------------
    n_updated   = 0
    n_not_found = 0
    n_skipped   = 0
    n_errors    = 0

    for idx, movie in enumerate(movies, start=1):
        try:
            outcome = _process_one_movie(
                movie=movie,
                dry_run=dry_run,
                index=idx,
                total=total,
            )
        except RuntimeError:
            # API key missing mid-run — abort
            _finish_pipeline_run(run_id, "failed", {"error": "TMDB_API_KEY missing"})
            return 1

        if outcome["error"]:
            n_errors += 1
        elif outcome["not_found"]:
            n_not_found += 1
        elif outcome["updated"]:
            n_updated += 1
        else:
            n_skipped += 1

    elapsed = time.monotonic() - t_start

    _print_summary(
        processed=total,
        updated=n_updated,
        not_found=n_not_found,
        skipped=n_skipped,
        errors=n_errors,
        elapsed=elapsed,
        dry_run=dry_run,
    )

    final_status = "success" if n_errors == 0 else "failed"
    _finish_pipeline_run(
        run_id,
        final_status,
        {
            "total":     total,
            "updated":   n_updated,
            "not_found": n_not_found,
            "skipped":   n_skipped,
            "errors":    n_errors,
            "elapsed_s": round(elapsed, 1),
            "dry_run":   dry_run,
        },
    )

    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Enrich South Cinema Analytics movies with TMDB metadata.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python -m data_pipeline.enrich_tmdb_movies\n"
            "  python -m data_pipeline.enrich_tmdb_movies --dry-run\n"
            "  python -m data_pipeline.enrich_tmdb_movies --batch-size 50\n"
            "  python -m data_pipeline.enrich_tmdb_movies --industry Telugu --dry-run\n"
        ),
    )
    p.add_argument(
        "--batch-size", "-n",
        type=int,
        default=0,
        metavar="N",
        help="Process at most N movies per run (default: 0 = no limit).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be updated without writing to the database.",
    )
    p.add_argument(
        "--industry",
        type=str,
        default="",
        metavar="INDUSTRY",
        help='Restrict to movies with this industry value (e.g. "Telugu", "Tamil").',
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(
        enrich_tmdb_movies(
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            industry=args.industry,
        )
    )
