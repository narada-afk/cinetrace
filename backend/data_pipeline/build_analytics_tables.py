"""
build_analytics_tables.py
=========================
Rebuilds four precomputed analytics tables from the live cinema data.

Why precomputed tables?
-----------------------
Dashboard analytics queries require expensive multi-table joins:

    SELECT a.name, COUNT(*)
    FROM   actors a
    JOIN   "cast" c ON a.id = c.actor_id
    JOIN   movies m ON m.id = c.movie_id
    GROUP  BY a.name;

As the dataset grows, these joins become the performance bottleneck.
This script precomputes those aggregates once and stores them in four
flat tables that dashboards can query with sub-millisecond O(1) lookups.

Tables built
------------
  actor_stats
      One row per actor: film_count, first_film_year, last_film_year,
      avg_runtime.  Powers actor profile pages and career-span analytics.

  actor_collaborations
      One row per ordered (actor1, actor2) pair: collaboration_count.
      Both directions (A→B and B→A) are stored so queries never need OR.
      Powers "actors who worked together" features.

  actor_director_stats
      One row per (actor_id, director) pair: film_count.
      Sourced from the legacy movies.director TEXT column.
      Powers "Prabhas worked with Rajamouli X times" queries.

  actor_production_stats
      One row per (actor_id, production_company) pair: film_count.
      Sourced from movies.production_company (populated by enrich_movies).
      Powers "Vijay worked with Sun Pictures X times" queries.

Idempotency (Task 4)
--------------------
Each table is TRUNCATED inside a transaction before fresh data is inserted.
Running this script multiple times produces identical results with zero
duplicate rows.

Pipeline order (Task 6)
-----------------------
Run this script AFTER ingest_all_actors and (optionally) enrich_movies:

    python -m data_pipeline.ingest_all_actors
    python -m data_pipeline.enrich_movies        # optional — fills avg_runtime
    python -m data_pipeline.build_analytics_tables

Usage (Task 5)
--------------
    python -m data_pipeline.build_analytics_tables

Environment
-----------
    DATABASE_URL  – PostgreSQL DSN
                    Default: postgresql://sca:sca@postgres:5432/sca
"""

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

from app.database import engine, SessionLocal

# ---------------------------------------------------------------------------
# Pretty-print helpers
# ---------------------------------------------------------------------------

_BOLD = "=" * 60
_THIN = "-" * 60


def _log(msg: str) -> None:
    """Print a timestamped progress line."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}]  {msg}")


# ---------------------------------------------------------------------------
# Pipeline run tracking (reuses the PipelineRun model from Sprint 4)
# ---------------------------------------------------------------------------

def _start_run() -> Optional[int]:
    """Insert a pipeline_runs row.  Returns id or None if table missing."""
    try:
        from app.models import PipelineRun
        db = SessionLocal()
        try:
            run = PipelineRun(
                run_type="analytics_build",
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


def _finish_run(run_id: Optional[int], status: str, details: dict) -> None:
    if run_id is None:
        return
    try:
        from app.models import PipelineRun
        db = SessionLocal()
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
# Individual table builders
# All use INSERT INTO ... SELECT ... so computation happens entirely in the DB.
# Each is wrapped in the same transaction as the TRUNCATE so a mid-run failure
# leaves the table with its previous data (the TRUNCATE is rolled back too).
# ---------------------------------------------------------------------------

_SQL_ACTOR_STATS = text("""
    WITH all_credits AS (
        -- Wikidata-sourced cast links (Sprints 1-5, original 13 actors)
        SELECT actor_id, movie_id FROM "cast"
        UNION
        -- TMDB-sourced cast links (Sprints 8-9, supporting + Malayalam actors)
        SELECT actor_id, movie_id FROM actor_movies
    )
    INSERT INTO actor_stats
        (actor_id, film_count, first_film_year, last_film_year, avg_runtime)
    SELECT
        a.id                                                   AS actor_id,
        COUNT(DISTINCT ac.movie_id)                            AS film_count,
        MIN(CASE WHEN m.release_year > 0
                 THEN m.release_year END)                      AS first_film_year,
        MAX(CASE WHEN m.release_year > 0
                 THEN m.release_year END)                      AS last_film_year,
        AVG(m.runtime)                                         AS avg_runtime
    FROM   actors a
    JOIN   all_credits ac ON a.id  = ac.actor_id
    JOIN   movies m       ON m.id  = ac.movie_id
    GROUP  BY a.id
""")

# Stores BOTH (A→B) and (B→A) so queries can use a simple WHERE actor1_id = ?
# without needing to know which id is numerically smaller.
_SQL_ACTOR_COLLABORATIONS = text("""
    WITH all_credits AS (
        SELECT actor_id, movie_id FROM "cast"
        UNION
        SELECT actor_id, movie_id FROM actor_movies
    )
    INSERT INTO actor_collaborations
        (actor1_id, actor2_id, collaboration_count)
    SELECT
        c1.actor_id                      AS actor1_id,
        c2.actor_id                      AS actor2_id,
        COUNT(DISTINCT c1.movie_id)      AS collaboration_count
    FROM   all_credits c1
    JOIN   all_credits c2
           ON  c1.movie_id = c2.movie_id
           AND c1.actor_id != c2.actor_id
    GROUP  BY c1.actor_id, c2.actor_id
""")

_SQL_ACTOR_DIRECTOR_STATS = text("""
    WITH all_credits AS (
        SELECT actor_id, movie_id FROM "cast"
        UNION
        SELECT actor_id, movie_id FROM actor_movies
    )
    INSERT INTO actor_director_stats
        (actor_id, director, film_count)
    SELECT
        ac.actor_id                          AS actor_id,
        m.director                           AS director,
        COUNT(DISTINCT ac.movie_id)          AS film_count
    FROM   all_credits ac
    JOIN   movies m ON m.id = ac.movie_id
    WHERE  m.director IS NOT NULL
      AND  m.director <> ''
    GROUP  BY ac.actor_id, m.director
""")

_SQL_ACTOR_PRODUCTION_STATS = text("""
    WITH all_credits AS (
        SELECT actor_id, movie_id FROM "cast"
        UNION
        SELECT actor_id, movie_id FROM actor_movies
    )
    INSERT INTO actor_production_stats
        (actor_id, production_company, film_count)
    SELECT
        ac.actor_id                          AS actor_id,
        m.production_company                 AS production_company,
        COUNT(DISTINCT ac.movie_id)          AS film_count
    FROM   all_credits ac
    JOIN   movies m ON m.id = ac.movie_id
    WHERE  m.production_company IS NOT NULL
      AND  m.production_company <> ''
    GROUP  BY ac.actor_id, m.production_company
""")


def _build_table(
    conn,
    table_name: str,
    insert_sql,
) -> int:
    """
    TRUNCATE *table_name*, then run *insert_sql* inside the current transaction.

    Args:
        conn:       Active SQLAlchemy connection (inside an open transaction).
        table_name: Table to clear before inserting.
        insert_sql: SQLAlchemy text() INSERT INTO … SELECT statement.

    Returns:
        Number of rows inserted.
    """
    conn.execute(text(f"TRUNCATE {table_name}"))
    result = conn.execute(insert_sql)
    return result.rowcount if result.rowcount >= 0 else 0


# ---------------------------------------------------------------------------
# Main entry-point
# ---------------------------------------------------------------------------

def build_analytics_tables() -> int:
    """
    Rebuild all four analytics tables inside a single transaction.

    If any step fails the transaction is rolled back, leaving every table
    in its previous state.  No partial updates are committed.

    Returns:
        Exit code — 0 on success, 1 on error.
    """
    print(f"\n{_BOLD}")
    print("  South Cinema Analytics — Build Analytics Tables")
    print(f"{_BOLD}\n")

    start_time = time.monotonic()
    run_id     = _start_run()

    stats: dict[str, int] = {}

    try:
        # Use a raw engine connection so we can control the transaction boundary
        # explicitly and run TRUNCATE + INSERT as a single atomic unit.
        with engine.connect() as conn:
            with conn.begin():

                # ── Table 1: actor_stats ───────────────────────────────────
                _log("Building actor_stats ...")
                n = _build_table(conn, "actor_stats", _SQL_ACTOR_STATS)
                stats["actor_stats"] = n
                _log(f"  actor_stats          → {n} row(s)")

                # ── Table 2: actor_collaborations ──────────────────────────
                _log("Building actor_collaborations ...")
                n = _build_table(conn, "actor_collaborations", _SQL_ACTOR_COLLABORATIONS)
                stats["actor_collaborations"] = n
                _log(f"  actor_collaborations → {n} row(s)")

                # ── Table 3: actor_director_stats ──────────────────────────
                _log("Building actor_director_stats ...")
                n = _build_table(conn, "actor_director_stats", _SQL_ACTOR_DIRECTOR_STATS)
                stats["actor_director_stats"] = n
                _log(f"  actor_director_stats → {n} row(s)")

                # ── Table 4: actor_production_stats ────────────────────────
                _log("Building actor_production_stats ...")
                n = _build_table(conn, "actor_production_stats", _SQL_ACTOR_PRODUCTION_STATS)
                stats["actor_production_stats"] = n
                _log(f"  actor_production_stats → {n} row(s)")

        # ── Summary ──────────────────────────────────────────────────────────
        elapsed = time.monotonic() - start_time
        print(f"\n{_THIN}")
        print("  Analytics tables built successfully ✓")
        print(f"{_THIN}")
        for tbl, rows in stats.items():
            print(f"    {tbl:<30} {rows:>6} row(s)")
        print(f"  Elapsed : {elapsed:.1f} s")
        print(f"{_BOLD}\n")

        _finish_run(run_id, "success", {**stats, "elapsed_s": round(elapsed, 1)})
        return 0

    except Exception as exc:
        elapsed = time.monotonic() - start_time
        print(f"\n  ✗ Build failed: {exc}")
        print(f"    (transaction rolled back — all analytics tables unchanged)\n")
        _finish_run(run_id, "failed", {"error": str(exc), "elapsed_s": round(elapsed, 1)})
        return 1


# ---------------------------------------------------------------------------
# CLI entry-point (Task 5)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sys.exit(build_analytics_tables())
