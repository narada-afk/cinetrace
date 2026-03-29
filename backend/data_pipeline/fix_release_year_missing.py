"""
fix_release_year_missing.py
============================
Fixes release_year:missing for BROKEN movies where release_year = 0.

For each movie:
  A) Has tmdb_id → fetch release_date from TMDB and update movies.release_year
  B) TMDB also has no release date (unreleased/upcoming) → set
     validation_overrides.release_year = 'confirmed_none'
  C) No tmdb_id → set confirmed_none immediately

Usage
-----
    cd backend
    export TMDB_API_KEY=your_key
    python -m data_pipeline.fix_release_year_missing
    python -m data_pipeline.fix_release_year_missing --dry-run
"""

from __future__ import annotations

import logging
import sys
import time
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from data_pipeline.tmdb_client import _TMDB_BASE, _api_get, _get_api_key

logger = logging.getLogger(__name__)

RATE_LIMIT_S = 0.26


def _set_override(movie_id: int, field: str, value: str, db: Session) -> None:
    db.execute(
        text("""
            UPDATE movie_validation_results
            SET validation_overrides = jsonb_set(validation_overrides, :path, :value)
            WHERE movie_id = :mid
        """),
        {"mid": movie_id, "path": f'{{{field}}}', "value": f'"{value}"'},
    )


def _fetch_tmdb_release_year(tmdb_id: int) -> Optional[int]:
    """
    Returns release year from TMDB, or None if not yet set (upcoming film).
    Returns -1 on API failure.
    """
    api_key = _get_api_key()
    try:
        data = _api_get(
            f"{_TMDB_BASE}/movie/{tmdb_id}",
            {"api_key": api_key, "language": "en-US"},
        )
    except Exception as exc:
        logger.warning("TMDB fetch failed for tmdb_id=%s: %s", tmdb_id, exc)
        return -1

    release_date = data.get("release_date") or ""
    if release_date and len(release_date) >= 4:
        try:
            return int(release_date[:4])
        except ValueError:
            return None
    return None  # No release date on TMDB — upcoming film


def fix_release_year_missing(
    db: Session,
    *,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> dict:

    q = """
        SELECT DISTINCT m.id AS movie_id, m.tmdb_id, m.title, m.industry
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
          jsonb_array_elements_text(mvr.issues) AS issue
        WHERE mvr.status = 'BROKEN'
          AND issue LIKE 'release_year:missing%'
          AND m.release_year = 0
          AND (mvr.validation_overrides->>'release_year') IS NULL
        ORDER BY m.id
    """
    if limit:
        q += f" LIMIT {limit}"

    rows  = db.execute(text(q)).fetchall()
    total = len(rows)
    logger.info("[fix_release_year_missing] %d movies to process", total)

    fixed       = 0  # year updated from TMDB
    placeholded = 0  # confirmed_none (upcoming or no tmdb_id)
    skipped     = 0  # API failure
    errors      = 0

    for i, row in enumerate(rows, 1):
        movie_id = row.movie_id
        tmdb_id  = row.tmdb_id
        title    = row.title

        try:
            # ── No tmdb_id — can't look up, set placeholder ──────────────
            if not tmdb_id:
                logger.info("  [%d/%d] no tmdb_id → confirmed_none (%s)", i, total, title)
                if not dry_run:
                    _set_override(movie_id, "release_year", "confirmed_none", db)
                placeholded += 1
                continue

            # ── Fetch from TMDB ───────────────────────────────────────────
            year = _fetch_tmdb_release_year(tmdb_id)
            time.sleep(RATE_LIMIT_S)

            if year == -1:
                logger.warning("  [%d/%d] API fail — skipping (%s)", i, total, title)
                skipped += 1
                continue

            if year is None:
                # TMDB has no release date yet — upcoming film
                logger.info("  [%d/%d] upcoming → confirmed_none (%s)", i, total, title)
                if not dry_run:
                    _set_override(movie_id, "release_year", "confirmed_none", db)
                placeholded += 1
                continue

            # ── Got a real year — update movies table ─────────────────────
            logger.info("  [%d/%d] year=%d → %s", i, total, year, title)
            if not dry_run:
                db.execute(
                    text("UPDATE movies SET release_year = :yr WHERE id = :mid"),
                    {"yr": year, "mid": movie_id},
                )
            fixed += 1

        except Exception as exc:
            logger.error("  [%d/%d] error for movie %d: %s", i, total, movie_id, exc)
            db.rollback()
            errors += 1

        if not dry_run and i % 25 == 0:
            db.commit()
            logger.info("  [%d/%d] batch committed", i, total)

    if not dry_run:
        db.commit()

    summary = {
        "total":       total,
        "fixed":       fixed,
        "placeholded": placeholded,
        "skipped":     skipped,
        "errors":      errors,
        "dry_run":     dry_run,
    }
    logger.info(
        "[fix_release_year_missing] Done — fixed=%d placeholded=%d skipped=%d errors=%d",
        fixed, placeholded, skipped, errors,
    )
    return summary


if __name__ == "__main__":
    import argparse
    from app.database import SessionLocal

    parser = argparse.ArgumentParser(description="Fix missing release years from TMDB")
    parser.add_argument("--limit",   type=int,            help="Max movies to process")
    parser.add_argument("--dry-run", action="store_true", help="No DB writes")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )

    db = SessionLocal()
    try:
        summary = fix_release_year_missing(db, dry_run=args.dry_run, limit=args.limit)
        print("\n" + "─" * 50)
        print(f"  Total processed : {summary['total']}")
        print(f"  Fixed (year set): {summary['fixed']}")
        print(f"  Placeholded     : {summary['placeholded']}  (confirmed_none)")
        print(f"  Skipped         : {summary['skipped']}  (API failures)")
        print(f"  Errors          : {summary['errors']}")
        print(f"  Dry run         : {summary['dry_run']}")
        print("─" * 50)
    finally:
        db.close()
