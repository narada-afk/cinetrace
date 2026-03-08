"""
ingest_all_actors.py
====================
Batch ingestion script: reads every actor from the ``actor_registry`` table
and ingests their full Wikidata filmographies into the database using batched
SPARQL queries.

Why batching?
-------------
The Sprint 3 version issued one Wikidata SPARQL request per actor — 13 actors
meant 13 sequential HTTP calls and 13+ seconds of rate-limit delay.

This version groups actors into batches of up to BATCH_SIZE (default 20) and
issues ONE SPARQL query per batch via a ``VALUES`` clause, collapsing 13 calls
into a single round-trip.  Wall-clock time drops from ~15 s to ~2 s for all
13 actors.

How it works
------------
1. Load all ActorRegistry rows from the DB (optionally filtered/limited).
2. Split them into batches of ``batch_size`` actors.
3. For each batch:
   a. Call ``fetch_filmography_batch(qids)`` — one SPARQL request for all actors.
   b. Group the flat result list by ``actor_qid``.
   c. For each actor in the batch, write movies/directors/cast to the DB,
      committing once per actor so a single failure never rolls back others.
4. Sleep 1 second between batches (polite rate limiting).
5. Print a cumulative summary at the end.

``ingest_actor.py`` remains available for one-off / manual ingestion of a
single actor.  Its private upsert helpers are imported and reused here so
the DB logic lives in exactly one place.

Sprint 4 addition — Pipeline run tracking (Task 6)
---------------------------------------------------
Creates a pipeline_runs row at the start of every run and updates it with
the final actor/film statistics when the run finishes.  Requires the
sprint4_pipeline_runs.sql migration.  If the table does not exist the
script continues normally and the tracking step is silently skipped.

Flags
-----
    --batch-size N       Actors per SPARQL query (default: 20, max recommended).
    --industry INDUSTRY  Process only actors from a specific industry.
    --dry-run            Perform Wikidata queries but skip all DB writes.
    --limit N            Process at most N actors (useful for testing).

Usage
-----
    # From the backend/ directory:
    python -m data_pipeline.ingest_all_actors
    python -m data_pipeline.ingest_all_actors --industry Telugu
    python -m data_pipeline.ingest_all_actors --limit 3 --dry-run
    python -m data_pipeline.ingest_all_actors --batch-size 5

Environment
-----------
    DATABASE_URL  – PostgreSQL DSN (default: postgresql://sca:sca@postgres:5432/sca)
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import ActorRegistry
from data_pipeline.wikidata_batch_client import BATCH_SIZE, fetch_filmography_batch


# ---------------------------------------------------------------------------
# Pipeline run tracking helpers (Task 6)
# ---------------------------------------------------------------------------

def _start_pipeline_run(run_type: str) -> Optional[int]:
    """
    Insert a pipeline_runs row with status='running'.

    Returns the new row id, or None if the table does not exist yet
    (migration not applied — script continues without tracking).
    """
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

# Reuse the private upsert helpers from ingest_actor so DB logic stays DRY.
from data_pipeline.ingest_actor import (
    _get_or_create_actor,
    _get_or_create_cast,
    _get_or_create_director,
    _get_or_create_movie,
    _get_or_create_movie_director,
)


# ---------------------------------------------------------------------------
# Pretty-print helpers
# ---------------------------------------------------------------------------

_BOLD = "=" * 64
_THIN = "-" * 64


def _print_run_header(
    total: int,
    batch_size: int,
    industry_filter: str,
    dry_run: bool,
) -> None:
    mode = "  *** DRY RUN — no DB writes ***" if dry_run else ""
    n_batches = (total + batch_size - 1) // batch_size
    print(f"\n{_BOLD}")
    print(f"  South Cinema Analytics — Batched Wikidata Ingestion{mode}")
    print(f"  Actors to process : {total}")
    print(f"  Batch size        : {batch_size} actors per SPARQL query")
    print(f"  Total batches     : {n_batches}")
    if industry_filter:
        print(f"  Industry filter   : {industry_filter}")
    print(f"{_BOLD}\n")


def _print_batch_header(batch_num: int, total_batches: int, names: list[str]) -> None:
    print(f"\n{'─' * 64}")
    print(
        f"  Batch {batch_num}/{total_batches}  "
        f"({len(names)} actor{'s' if len(names) != 1 else ''})"
    )
    for name in names:
        print(f"    • {name}")
    print(f"{'─' * 64}")


def _print_final_summary(
    results: list[dict],
    errors: list[tuple[str, str]],
    elapsed: float,
) -> None:
    total_films    = sum(r["total"]    for r in results)
    total_inserted = sum(r["inserted"] for r in results)
    total_skipped  = sum(r["skipped"]  for r in results)

    print(f"\n{_BOLD}")
    print("  Ingestion complete")
    print(f"{_THIN}")
    print(f"  Actors processed : {len(results)}")
    print(f"  Films found      : {total_films}")
    print(f"  Movies inserted  : {total_inserted}")
    print(f"  Movies skipped   : {total_skipped}  (already existed)")
    if errors:
        print(f"  Errors           : {len(errors)}")
        for actor_name, err_msg in errors:
            print(f"    ✗ {actor_name}: {err_msg}")
    print(f"  Elapsed          : {elapsed:.1f} s")
    print(f"{_BOLD}\n")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _group_rows_by_actor(rows: list[dict]) -> dict[str, list[dict]]:
    """
    Group a flat list of (actor, film) row dicts by actor QID.

    Args:
        rows: Flat list returned by ``fetch_filmography_batch``.
              Each dict has at minimum: ``actor_qid``, ``actor_name``,
              ``film_title``, ``release_year``, ``director``.

    Returns:
        Dict mapping ``actor_qid`` → list of row dicts for that actor.
    """
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["actor_qid"]].append(row)
    return dict(grouped)


def _write_actor_films(
    db:       Session,
    name:     str,
    qid:      str,
    industry: str,
    films:    list[dict],
) -> dict:
    """
    Upsert one actor and all their films into the DB within a single transaction.

    Commits after all of the actor's films are written.  A rollback on error
    only affects this actor, not the whole batch.

    Args:
        db:       Active SQLAlchemy session.
        name:     Actor display name (from Wikidata label or registry).
        qid:      Wikidata QID, e.g. ``"Q352416"``.
        industry: Industry label inherited from actor_registry, e.g. ``"Telugu"``.
        films:    List of row dicts from ``fetch_filmography_batch`` for this actor.
                  Each dict: ``{"film_title", "release_year", "director", ...}``.

    Returns:
        Stats dict::

            {
                "actor":    "Allu Arjun",
                "qid":      "Q352416",
                "total":    24,
                "inserted": 2,
                "skipped":  22,
            }

    Raises:
        Exception: re-raises any DB error after rolling back this actor's writes.
    """
    inserted = 0
    skipped  = 0

    try:
        # Step 1 — Upsert actor
        actor = _get_or_create_actor(db, name, industry)

        # Steps 2-4 — Per-film upserts
        for row in films:
            title    = row["film_title"]
            year: Optional[int] = row.get("release_year")
            dir_name: Optional[str] = row.get("director")

            # Step 2: upsert movie (keeps legacy director TEXT in sync)
            movie, is_new = _get_or_create_movie(
                db,
                title=title,
                year=year,
                director=dir_name,
                industry=industry,
            )
            if is_new:
                inserted += 1
            else:
                skipped += 1

            # Step 3: upsert director + join row (when known)
            if dir_name:
                director_obj = _get_or_create_director(db, dir_name)
                _get_or_create_movie_director(db, movie.id, director_obj.id)

            # Step 4: upsert cast link (actor ↔ movie)
            _get_or_create_cast(db, actor.id, movie.id)

        db.commit()

    except Exception:
        db.rollback()
        raise

    return {
        "actor":    name,
        "qid":      qid,
        "total":    len(films),
        "inserted": inserted,
        "skipped":  skipped,
    }


def _process_batch(
    batch:         list[tuple[str, str, str]],
    batch_num:     int,
    total_batches: int,
    dry_run:       bool,
) -> tuple[list[dict], list[tuple[str, str]]]:
    """
    Fetch and ingest one batch of actors.

    Issues a SINGLE Wikidata SPARQL request for all actors in *batch*, then
    writes each actor's films to the DB individually (so one DB failure never
    rolls back another actor's data).

    Args:
        batch:         List of ``(name, qid, industry)`` triples.
        batch_num:     1-based batch index (for logging).
        total_batches: Total number of batches (for logging).
        dry_run:       If True, skip all DB writes.

    Returns:
        Tuple of (results, errors):
          - results: list of stats dicts (one per successfully processed actor)
          - errors:  list of (actor_name, error_message) for failed actors
    """
    names    = [t[0] for t in batch]
    qids     = [t[1] for t in batch]

    _print_batch_header(batch_num, total_batches, names)

    label = f"batch {batch_num}/{total_batches}, {len(batch)} actors"

    # ── ONE Wikidata request for the whole batch ──────────────────────────
    rows = fetch_filmography_batch(qids, label=label)
    print(f"  → Fetched {len(rows)} film row(s) across {len(batch)} actor(s).")

    if dry_run:
        print("  [DRY RUN] Skipping all DB writes.")
        results = []
        for name, qid, _industry in batch:
            actor_rows = [r for r in rows if r["actor_qid"] == qid]
            results.append({
                "actor":    name,
                "qid":      qid,
                "total":    len(actor_rows),
                "inserted": 0,
                "skipped":  len(actor_rows),
            })
        return results, []

    # ── Group rows by actor QID ───────────────────────────────────────────
    grouped = _group_rows_by_actor(rows)

    results: list[dict]            = []
    errors:  list[tuple[str, str]] = []

    # ── Per-actor DB writes ───────────────────────────────────────────────
    for name, qid, industry in batch:
        actor_rows = grouped.get(qid, [])

        if not actor_rows:
            print(f"\n  ⚠  No films found for {name} ({qid}) in batch results.")
            print(f"     Verify QID at: https://www.wikidata.org/wiki/{qid}")
            results.append({
                "actor":    name,
                "qid":      qid,
                "total":    0,
                "inserted": 0,
                "skipped":  0,
            })
            continue

        # Use Wikidata actor_name label when available; fall back to registry name.
        wikidata_name = actor_rows[0].get("actor_name") or name
        display_name  = wikidata_name if wikidata_name and wikidata_name != qid else name

        print(f"\n  Processing : {display_name} ({qid})  [{industry}]")
        print(f"    {len(actor_rows)} film row(s) to process.")

        db: Session = SessionLocal()
        try:
            stats = _write_actor_films(db, display_name, qid, industry, actor_rows)
            results.append(stats)
            print(
                f"  ✓ {display_name}: "
                f"{stats['total']} films | "
                f"{stats['inserted']} inserted | "
                f"{stats['skipped']} skipped"
            )

        except Exception as exc:
            err_msg = str(exc).splitlines()[0]
            print(f"  ✗ FAILED: {display_name} ({qid}) — {err_msg}")
            errors.append((display_name, err_msg))

        finally:
            db.close()

    return results, errors


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------

def ingest_all_actors(
    batch_size:      int  = BATCH_SIZE,
    industry_filter: str  = "",
    limit:           int  = 0,
    dry_run:         bool = False,
) -> int:
    """
    Load actor_registry, split into batches, and ingest each batch from Wikidata.

    Args:
        batch_size:      Number of actors per SPARQL query (default: BATCH_SIZE=20).
        industry_filter: If non-empty, only process actors whose industry matches
                         this string exactly (e.g. ``"Telugu"``).
        limit:           Max number of actors to process (0 = no limit).
        dry_run:         If True, query Wikidata but do not write to DB.

    Returns:
        Exit code — 0 if all actors succeeded, 1 if any errors occurred.
    """
    # ── Load actor registry ────────────────────────────────────────────────
    db = SessionLocal()
    try:
        q = db.query(ActorRegistry).order_by(
            ActorRegistry.industry, ActorRegistry.name
        )
        if industry_filter:
            q = q.filter(ActorRegistry.industry == industry_filter)
        if limit > 0:
            q = q.limit(limit)
        registry_entries = q.all()

        # Detach before closing so we don't hold an idle connection during
        # the long-running ingestion loop.
        actors: list[tuple[str, str, str]] = [
            (entry.name, entry.wikidata_id, entry.industry)
            for entry in registry_entries
        ]
    finally:
        db.close()

    if not actors:
        msg = "actor_registry"
        if industry_filter:
            msg += f" (industry={industry_filter!r})"
        print(
            f"\nNo actors found in {msg}.  "
            f"Run the SQL migration to populate it.\n"
        )
        return 0

    # ── Start pipeline run tracking ────────────────────────────────────────
    run_id = _start_pipeline_run("wikidata_ingestion")

    effective_batch_size = max(1, batch_size)
    batches = [
        actors[i : i + effective_batch_size]
        for i in range(0, len(actors), effective_batch_size)
    ]

    _print_run_header(
        total=len(actors),
        batch_size=effective_batch_size,
        industry_filter=industry_filter,
        dry_run=dry_run,
    )

    # ── Batch ingestion loop ───────────────────────────────────────────────
    all_results: list[dict]            = []
    all_errors:  list[tuple[str, str]] = []
    start = time.monotonic()

    for batch_num, batch in enumerate(batches, start=1):
        batch_results, batch_errors = _process_batch(
            batch=batch,
            batch_num=batch_num,
            total_batches=len(batches),
            dry_run=dry_run,
        )
        all_results.extend(batch_results)
        all_errors.extend(batch_errors)

        # Rate-limit: sleep between batches (not before the first one —
        # fetch_filmography_batch already sleeps REQUEST_DELAY internally).
        if batch_num < len(batches):
            print(f"\n  ⏳ Sleeping 1 s before next batch...")
            time.sleep(1)

    elapsed = time.monotonic() - start
    _print_final_summary(all_results, all_errors, elapsed)

    # ── Finish pipeline run tracking ───────────────────────────────────────
    final_status = "failed" if all_errors and not all_results else "success"
    _finish_pipeline_run(run_id, final_status, {
        "actors_processed": len(all_results),
        "actors_failed":    len(all_errors),
        "films_found":      sum(r.get("total",    0) for r in all_results),
        "movies_inserted":  sum(r.get("inserted", 0) for r in all_results),
        "movies_skipped":   sum(r.get("skipped",  0) for r in all_results),
        "elapsed_s":        round(elapsed, 1),
        "dry_run":          dry_run,
        "batch_size":       effective_batch_size,
    })

    return 1 if all_errors else 0


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="ingest_all_actors",
        description=(
            "Batch-ingest South Indian actor filmographies from Wikidata "
            "using QIDs stored in actor_registry.  Issues ONE SPARQL query "
            "per batch of --batch-size actors (default 20) instead of one "
            "query per actor."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        metavar="N",
        help=(
            f"Actors per SPARQL query (default: {BATCH_SIZE}).  "
            "Reduce if you hit Wikidata query timeouts."
        ),
    )
    parser.add_argument(
        "--industry",
        type=str,
        default="",
        metavar="INDUSTRY",
        help='Only process actors from this industry (e.g. "Telugu", "Tamil").',
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Query Wikidata and print results without writing to the database.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Process at most N actors (default: 0 = no limit).  Useful for testing.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    rc   = ingest_all_actors(
        batch_size=args.batch_size,
        industry_filter=args.industry,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    sys.exit(rc)
