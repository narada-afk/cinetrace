"""
enrich_movies.py
================
Enriches existing movie records in the database with metadata from Wikipedia.

For every movie that has runtime = NULL, the script:
  1. Calls fetch_movie_metadata(title) to retrieve runtime, production_company,
     and language from the Wikipedia film infobox.
  2. Updates *only* the columns that are currently NULL (never overwrites
     existing data).
  3. Commits the update for that movie immediately so progress is preserved
     even if the script is interrupted mid-run.

Sprint 4 additions
------------------
  Parallel workers (Task 5):
    Uses concurrent.futures.ThreadPoolExecutor to process multiple movies at
    the same time.  Each worker creates its own DB session to avoid sharing
    state across threads.  Set --workers N to control parallelism (default 1,
    meaning sequential — the Sprint 2 behaviour is preserved by default).

    Wikipedia rate-limiting note: each worker sleeps REQUEST_DELAY (1 s) before
    every live HTTP call.  With 5 workers running concurrently the effective
    request rate is ~5 req/s toward Wikipedia.  Wikipedia's bot policy allows
    up to 200 req/min (~3.3 req/s) for registered bots; anonymous scripts
    should stay lower.  Keep --workers ≤ 3 for production runs; use --workers 5
    only for one-off backfill operations where you have pre-warmed the cache.

  Pipeline run tracking (Task 6):
    Creates a pipeline_runs row at the start of every run and updates it with
    final stats when the run finishes.  Requires the sprint4_pipeline_runs.sql
    migration to have been applied first.  If the table does not exist the
    script still runs; the tracking step is skipped with a warning.

The script is intentionally conservative:
  - It does NOT touch movies.director or any other column.
  - It does NOT modify existing (non-NULL) values.
  - It does NOT delete or recreate any rows.

Usage:
    # From the backend/ directory (preferred):
    python -m data_pipeline.enrich_movies
    python -m data_pipeline.enrich_movies --dry-run
    python -m data_pipeline.enrich_movies --batch-size 50 --workers 3
    python -m data_pipeline.enrich_movies --batch-size 5 --workers 5 --dry-run

    # Or directly:
    python data_pipeline/enrich_movies.py --workers 3

Flags:
    --dry-run        Print what would be updated without writing to the DB.
    --batch-size N   Process at most N movies per run (default: unlimited).
    --industry X     Filter to movies with industry = X (e.g. "Telugu").
    --workers N      Number of parallel Wikipedia fetch workers (default: 1).

Environment:
    DATABASE_URL  – PostgreSQL DSN
                    Default: postgresql://sca:sca@postgres:5432/sca
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Path bootstrap — same pattern as ingest_actor.py
# ---------------------------------------------------------------------------
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Movie
from data_pipeline.wikipedia_client import fetch_movie_metadata


# ---------------------------------------------------------------------------
# Pretty-print helpers
# ---------------------------------------------------------------------------

_SEP_THIN = "-" * 52
_SEP_BOLD = "=" * 52


def _print_header(total: int, dry_run: bool, workers: int) -> None:
    mode = "  [DRY RUN — no DB writes]" if dry_run else ""
    print(f"\n{_SEP_BOLD}")
    print(f"  Wikipedia Enrichment{mode}")
    print(f"  Movies to process : {total}")
    if workers > 1:
        print(f"  Parallel workers  : {workers}")
    print(f"{_SEP_BOLD}\n")


def _print_movie_result(
    index: int,
    total: int,
    result: dict,
    dry_run: bool,
) -> None:
    """Print a single worker result in the standard format."""
    title = result["title"]
    year  = result["year"]
    meta  = result.get("meta", {})
    updates = result.get("updates", {})
    error   = result.get("error")
    changed = result.get("changed", False)

    print(f"[{index}/{total}] Processing: {title} ({year})")
    print(_SEP_THIN)

    if error:
        print(f"  ✗ Error: {error}")
        print()
        return

    # Print per-field results.
    runtime_val = meta.get("runtime")
    company_val = meta.get("production_company")
    language_val = meta.get("language")

    _print_field("Runtime",            runtime_val,  "runtime"            in updates, dry_run)
    _print_field("Production company", company_val,  "production_company" in updates, dry_run)
    _print_field("Language",           language_val, "language"           in updates, dry_run)

    if changed:
        action = "Would update" if dry_run else "Saved"
        print(f"  → {action} successfully.")
    else:
        print("  → No new data found, skipped.")
    print()


def _print_field(label: str, value, found: bool, dry_run: bool) -> None:
    """Print a single enriched field with consistent formatting."""
    if found:
        status = "  (would set)" if dry_run else "  ✓"
    else:
        status = "  (not found)"

    padded  = f"  {label:<22}"
    val_str = str(value) if value is not None else "—"
    print(f"{padded}: {val_str}{status}")


# ---------------------------------------------------------------------------
# Pipeline run tracking helpers (Task 6)
# ---------------------------------------------------------------------------

def _start_pipeline_run(run_type: str) -> Optional[int]:
    """
    Insert a new pipeline_runs row with status='running'.

    Returns the new row's id, or None if the table doesn't exist yet
    (migration not applied).
    """
    try:
        from app.models import PipelineRun  # imported here to avoid circular issues
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
    """Update an existing pipeline_runs row with the final status and stats."""
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
# Worker function (Task 5 — parallel enrichment)
# ---------------------------------------------------------------------------

def _enrich_movie_worker(movie_data: dict, dry_run: bool) -> dict:
    """
    Thread worker: fetch Wikipedia metadata for one movie and optionally
    write changes to the DB.

    Each worker creates its own SQLAlchemy session so threads never share
    database connections.

    Args:
        movie_data: Plain dict snapshot of the movie row with keys:
                    id, title, year, runtime, production_company, language.
        dry_run:    If True, compute changes but do not commit to DB.

    Returns:
        Result dict::

            {
                "id":       int,
                "title":    str,
                "year":     int,
                "changed":  bool,
                "error":    str | None,
                "meta":     {"runtime": …, "production_company": …, "language": …},
                "updates":  {"runtime": …},   # only keys that will change
            }
    """
    result: dict = {
        "id":      movie_data["id"],
        "title":   movie_data["title"],
        "year":    movie_data["year"],
        "changed": False,
        "error":   None,
        "meta":    {},
        "updates": {},
    }

    try:
        meta = fetch_movie_metadata(movie_data["title"])
    except Exception as exc:
        result["error"] = str(exc)
        return result

    result["meta"] = meta

    runtime_val  = meta.get("runtime")
    company_val  = meta.get("production_company")
    language_val = meta.get("language")

    # Build the dict of fields that would change (never overwrite non-NULL).
    updates: dict = {}
    if runtime_val  is not None and movie_data["runtime"]            is None:
        updates["runtime"]            = runtime_val
    if company_val  and not movie_data["production_company"]:
        updates["production_company"] = company_val
    if language_val and not movie_data["language"]:
        updates["language"]           = language_val

    result["updates"] = updates
    result["changed"] = bool(updates)

    if updates and not dry_run:
        # Each worker opens its own session — never share across threads.
        db: Session = SessionLocal()
        try:
            movie = db.query(Movie).filter(Movie.id == movie_data["id"]).first()
            if movie:
                for col, val in updates.items():
                    setattr(movie, col, val)
                db.commit()
            else:
                result["error"]   = f"Movie id={movie_data['id']} not found in DB"
                result["changed"] = False
        except Exception as exc:
            db.rollback()
            result["error"]   = str(exc)
            result["changed"] = False
        finally:
            db.close()

    return result


# ---------------------------------------------------------------------------
# Sequential fallback (workers=1) — keeps original behaviour
# ---------------------------------------------------------------------------

def _enrich_one_movie(db: Session, movie: Movie, dry_run: bool) -> dict:
    """
    Fetch Wikipedia metadata for *movie* and apply missing fields.
    Used in single-worker (sequential) mode only.

    Args:
        db:      Active SQLAlchemy session.
        movie:   Movie ORM instance to enrich.
        dry_run: If True, compute and log changes but do not commit.

    Returns:
        Worker-compatible result dict.
    """
    movie_data = {
        "id":                movie.id,
        "title":             movie.title,
        "year":              movie.release_year,
        "runtime":           movie.runtime,
        "production_company": movie.production_company,
        "language":          movie.language,
    }

    # Use the worker function logic but pass dry_run=True to skip its DB write;
    # we commit via the shared session below instead.
    result = _enrich_movie_worker(movie_data, dry_run=True)

    if result["updates"] and not dry_run:
        for col, val in result["updates"].items():
            setattr(movie, col, val)
        try:
            db.commit()
            result["changed"] = True
        except Exception as exc:
            db.rollback()
            result["error"]   = str(exc)
            result["changed"] = False

    return result


# ---------------------------------------------------------------------------
# Main enrichment driver
# ---------------------------------------------------------------------------

def enrich_movies(
    batch_size: int  = 0,
    dry_run:    bool = False,
    industry:   str  = "",
    workers:    int  = 1,
) -> None:
    """
    Query the database for movies with NULL runtime and enrich them
    using Wikipedia metadata.

    With workers=1 (default) the behaviour is identical to Sprint 2:
    movies are processed one at a time in a single shared session.

    With workers>1 a ThreadPoolExecutor processes multiple movies in
    parallel.  Each worker manages its own DB session.

    Args:
        batch_size: Maximum number of movies to process.  0 = no limit.
        dry_run:    If True, log what would change without writing to DB.
        industry:   Optional filter, e.g. "Telugu".  Empty = all industries.
        workers:    Number of parallel Wikipedia fetch threads (default: 1).
    """
    start_time = time.monotonic()

    # ── Pipeline run tracking ─────────────────────────────────────────────
    run_id = _start_pipeline_run("wikipedia_enrichment")

    # ── Load movies to enrich ─────────────────────────────────────────────
    db: Session = SessionLocal()
    try:
        q = db.query(Movie).filter(Movie.runtime.is_(None))

        if industry:
            q = q.filter(Movie.industry == industry)

        # Prefer recently released movies (more likely to have Wikipedia pages).
        q = q.order_by(Movie.release_year.desc(), Movie.title)

        if batch_size > 0:
            q = q.limit(batch_size)

        movies = q.all()

        if not movies:
            print("\nNothing to enrich — all movies already have a runtime value.\n")
            _finish_pipeline_run(run_id, "success", {"processed": 0, "updated": 0, "skipped": 0})
            return

        # Snapshot plain dicts so ORM objects are not shared across threads.
        movie_snapshots: list[dict] = [
            {
                "id":                 m.id,
                "title":              m.title,
                "year":               m.release_year,
                "runtime":            m.runtime,
                "production_company": m.production_company,
                "language":           m.language,
            }
            for m in movies
        ]

        # Hold the shared session for sequential mode; close before parallel.
        if workers > 1:
            db.close()
            db = None  # type: ignore[assignment]

    except Exception as exc:
        db.rollback()
        db.close()
        _finish_pipeline_run(run_id, "failed", {"error": str(exc)})
        print(f"\nFATAL: {exc}\n")
        raise

    _print_header(total=len(movie_snapshots), dry_run=dry_run, workers=workers)

    updated = 0
    skipped = 0
    errored = 0
    results_ordered: list[dict] = []

    # ── PARALLEL mode ─────────────────────────────────────────────────────
    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_snap = {
                executor.submit(_enrich_movie_worker, snap, dry_run): snap
                for snap in movie_snapshots
            }

            # Collect results in completion order; print them sequentially.
            completed_results: dict[int, dict] = {}
            for future in as_completed(future_to_snap):
                result = future.result()
                completed_results[result["id"]] = result

        # Re-sort to original query order for consistent output.
        for snap in movie_snapshots:
            results_ordered.append(completed_results[snap["id"]])

    # ── SEQUENTIAL mode (workers=1) ───────────────────────────────────────
    else:
        if db is None:
            db = SessionLocal()

        for snap in movie_snapshots:
            movie_orm = db.query(Movie).filter(Movie.id == snap["id"]).first()
            if movie_orm is None:
                results_ordered.append({
                    "id": snap["id"], "title": snap["title"], "year": snap["year"],
                    "changed": False, "error": "not found in DB", "meta": {}, "updates": {},
                })
                continue

            try:
                result = _enrich_one_movie(db, movie_orm, dry_run)
            except Exception as exc:
                db.rollback()
                result = {
                    "id": snap["id"], "title": snap["title"], "year": snap["year"],
                    "changed": False, "error": str(exc), "meta": {}, "updates": {},
                }
            results_ordered.append(result)

        db.close()

    # ── Print results and tally ───────────────────────────────────────────
    for i, result in enumerate(results_ordered, start=1):
        _print_movie_result(i, len(results_ordered), result, dry_run)

        if result.get("error"):
            errored += 1
        elif result.get("changed"):
            updated += 1
        else:
            skipped += 1

    elapsed = time.monotonic() - start_time

    # ── Final summary ─────────────────────────────────────────────────────
    print(_SEP_BOLD)
    mode = "DRY RUN — " if dry_run else ""
    print(f"  {mode}Enrichment complete")
    print(f"  Updated  : {updated}")
    print(f"  Skipped  : {skipped}  (no new data found on Wikipedia)")
    if errored:
        print(f"  Errors   : {errored}  (check output above)")
    print(f"  Total    : {len(results_ordered)}")
    print(f"  Elapsed  : {elapsed:.1f} s")
    print(f"{_SEP_BOLD}\n")

    # ── Update pipeline_runs record ───────────────────────────────────────
    final_status = "failed" if errored and (updated + skipped) == 0 else "success"
    _finish_pipeline_run(run_id, final_status, {
        "processed": len(results_ordered),
        "updated":   updated,
        "skipped":   skipped,
        "errors":    errored,
        "elapsed_s": round(elapsed, 1),
        "workers":   workers,
        "dry_run":   dry_run,
    })


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="enrich_movies",
        description=(
            "Enrich movies.runtime / production_company / language "
            "from Wikipedia infoboxes."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print what would be updated without writing to the database.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        metavar="N",
        help="Process at most N movies (default: 0 = no limit).",
    )
    parser.add_argument(
        "--industry",
        type=str,
        default="",
        metavar="INDUSTRY",
        help='Filter to a specific industry, e.g. "Telugu" or "Tamil".',
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help=(
            "Number of parallel Wikipedia fetch workers (default: 1). "
            "With workers=1 the script runs sequentially (Sprint 2 behaviour). "
            "Recommended: ≤ 3 for production; use 5 only for one-off backfills "
            "with a pre-warmed cache."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    enrich_movies(
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        industry=args.industry,
        workers=max(1, args.workers),
    )
