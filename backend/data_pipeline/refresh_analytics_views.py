"""
refresh_analytics_views.py
===========================
Refreshes the Sprint 4 analytics materialized views after data ingestion or
enrichment runs.

Why refresh?
------------
PostgreSQL materialized views cache query results at creation time and do NOT
update automatically when the underlying tables change.  After every Wikidata
ingestion or Wikipedia enrichment run, this script must be executed to bring
the views up to date.

Views refreshed
---------------
  actor_film_counts
      One row per actor: actor_id, actor_name, industry, film_count.
      Used by leaderboard / "top actors" dashboard queries.

  actor_director_collaborations
      One row per (actor, director) pair: actor, industry, director,
      collaborations.
      Used by collaboration charts.

Refresh strategy
----------------
Both views have a UNIQUE index on their primary key column, which allows
``REFRESH MATERIALIZED VIEW CONCURRENTLY``.  Concurrent refresh means the
old view data remains readable while the new data is being computed — no
downtime for dashboard users.

Pipeline run tracking
---------------------
Records the refresh in the pipeline_runs table (run_type = "view_refresh").
If the table does not exist (migration not applied) the script still runs;
only the tracking step is skipped with a warning.

Requires
--------
  sprint4_materialized_views.sql must be applied before this script is run.

Usage
-----
    # From the backend/ directory:
    python -m data_pipeline.refresh_analytics_views

    # Or directly:
    python data_pipeline/refresh_analytics_views.py

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

from app.database import SessionLocal

# ---------------------------------------------------------------------------
# Views to refresh, in dependency order.
# Each entry: (view_name, use_concurrent_refresh)
# CONCURRENTLY requires a UNIQUE index on the view — both views have one.
# ---------------------------------------------------------------------------

VIEWS: list[tuple[str, bool]] = [
    ("actor_film_counts",           True),
    ("actor_director_collaborations", False),  # no unique index → standard refresh
]

_BOLD = "=" * 56
_THIN = "-" * 56


# ---------------------------------------------------------------------------
# Pipeline run tracking helpers
# ---------------------------------------------------------------------------

def _start_run() -> Optional[int]:
    """Insert a pipeline_runs row for this refresh.  Returns id or None."""
    try:
        from app.models import PipelineRun
        db = SessionLocal()
        try:
            run = PipelineRun(
                run_type="view_refresh",
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
    """Update the pipeline_runs row with final status and stats."""
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
# Main refresh logic
# ---------------------------------------------------------------------------

def refresh_analytics_views() -> int:
    """
    Refresh all analytics materialized views.

    Returns:
        Exit code — 0 if all views refreshed successfully, 1 if any errors.
    """
    print(f"\n{_BOLD}")
    print("  South Cinema Analytics — Refresh Analytics Views")
    print(f"{_BOLD}\n")

    start_time = time.monotonic()
    run_id     = _start_run()

    refreshed: list[str] = []
    failed:    list[str] = []

    # Use a raw connection for DDL statements; autocommit=True is required for
    # REFRESH MATERIALIZED VIEW CONCURRENTLY (cannot run inside a transaction).
    with SessionLocal().connection() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")

        for view_name, concurrent in VIEWS:
            keyword = "CONCURRENTLY" if concurrent else ""
            sql     = f"REFRESH MATERIALIZED VIEW {keyword} {view_name}"

            print(f"  Refreshing : {view_name} ...", end=" ", flush=True)
            t0 = time.monotonic()

            try:
                conn.execute(text(sql))
                elapsed_ms = int((time.monotonic() - t0) * 1000)
                print(f"✓  ({elapsed_ms} ms)")
                refreshed.append(view_name)

            except Exception as exc:
                print(f"✗")
                print(f"    Error: {exc}")
                failed.append(view_name)

    total_elapsed = time.monotonic() - start_time

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{_THIN}")
    print(f"  Refreshed : {len(refreshed)}  view(s)")
    if failed:
        print(f"  Failed    : {len(failed)}  view(s): {', '.join(failed)}")
    print(f"  Elapsed   : {total_elapsed:.1f} s")
    print(f"{_BOLD}\n")

    final_status = "success" if not failed else "failed"
    _finish_run(run_id, final_status, {
        "refreshed": refreshed,
        "failed":    failed,
        "elapsed_s": round(total_elapsed, 1),
    })

    return 0 if not failed else 1


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sys.exit(refresh_analytics_views())
