"""
fix_pipeline.py
===============
Safe, automated fix pipeline for South Cinema Analytics movie data.

Root causes addressed
---------------------
  1. Director normalisation (9 963 movies)
     movies.director TEXT exists but directors / movie_directors tables empty.
     Fix: pure SQL migration — zero TMDB calls needed.

  2. Primary-cast role mislabelling (6 165 movies)
     actor_movies rows exist with billing_order 0-2 but role_type = 'supporting'.
     Fix: SQL UPDATE based on billing_order threshold.

  3. Truly missing cast (24 movies)
     No actor_movies rows at all.
     Fix: fetch TMDB credits, match against actors table, insert.

  4. Re-validate & update movie_validation_results.

Usage
-----
  # Run all fixes + re-validate affected movies:
  python -m data_pipeline.fix_pipeline

  # Dry-run (shows what would change, writes nothing):
  python -m data_pipeline.fix_pipeline --dry-run

  # Only director migration:
  python -m data_pipeline.fix_pipeline --directors-only

  # Only cast fixes:
  python -m data_pipeline.fix_pipeline --cast-only

  # Limit TMDB-assisted cast enrichment to N movies:
  python -m data_pipeline.fix_pipeline --cast-enrich-limit 200
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("DATABASE_URL", "postgresql://sca:sca@localhost:5432/sca")

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from data_pipeline.tmdb_client import _TMDB_BASE, _api_get, _get_api_key

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

# TMDB billing positions (0-indexed): 0-2 → primary, 3-9 → supporting
PRIMARY_BILLING_MAX    = 2
SUPPORTING_BILLING_MAX = 9

# Validate re-validation in batches of this size
REVALIDATE_BATCH = 200

# ─── Simple in-process TMDB cache ────────────────────────────────────────────

_tmdb_credits_cache: dict[int, dict] = {}   # tmdb_id → raw TMDB credits payload
_tmdb_details_cache: dict[int, dict] = {}   # tmdb_id → raw TMDB details payload


def _fetch_tmdb_credits(tmdb_id: int, api_key: str) -> Optional[dict]:
    """Fetch and cache TMDB /movie/{id}/credits."""
    if tmdb_id in _tmdb_credits_cache:
        return _tmdb_credits_cache[tmdb_id]
    try:
        data = _api_get(
            f"{_TMDB_BASE}/movie/{tmdb_id}/credits",
            {"api_key": api_key, "language": "en-US"},
        )
        _tmdb_credits_cache[tmdb_id] = data
        return data
    except Exception as exc:
        logger.warning("TMDB credits fetch failed tmdb_id=%s: %s", tmdb_id, exc)
        return None


# ─── Dataclass for fix report ─────────────────────────────────────────────────

@dataclass
class FixReport:
    directors_inserted:     int = 0
    director_links_created: int = 0
    roles_promoted:         int = 0   # supporting → primary promotion
    cast_rows_inserted:     int = 0   # new actor_movies rows from TMDB
    movies_revalidated:     int = 0
    errors:                 list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            "─" * 58,
            "FIX PIPELINE SUMMARY",
            "─" * 58,
            f"  Directors inserted:            {self.directors_inserted:>6}",
            f"  Director links created:        {self.director_links_created:>6}",
            f"  Roles promoted to primary:     {self.roles_promoted:>6}",
            f"  Cast rows inserted (TMDB):     {self.cast_rows_inserted:>6}",
            f"  Movies re-validated:           {self.movies_revalidated:>6}",
            f"  Errors:                        {len(self.errors):>6}",
            "─" * 58,
        ]
        if self.errors:
            lines.append("Errors (first 10):")
            for e in self.errors[:10]:
                lines.append(f"  • {e}")
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# FIX 1 — DIRECTOR MIGRATION
# ─────────────────────────────────────────────────────────────────────────────

def _parse_director_names(raw: str) -> list[str]:
    """
    Split a director string that may contain multiple names.

    Handles:
      "Mani Ratnam"                    → ["Mani Ratnam"]
      "Anas Khan, Akhil Paul"          → ["Anas Khan", "Akhil Paul"]
      "Diphan, Vinod Vijayan, Padmakumar M" → ["Diphan", "Vinod Vijayan", "Padmakumar M"]

    Note: comma is the only reliable separator used by backfill_directors.py.
    """
    if not raw or not raw.strip():
        return []
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts


def migrate_directors(db: Session, dry_run: bool = False) -> tuple[int, int]:
    """
    One-time migration: movies.director (TEXT) → directors + movie_directors.

    Returns (directors_inserted, links_created).
    Safe to run multiple times — uses INSERT ... ON CONFLICT DO NOTHING.
    """
    logger.info("Starting director migration …")

    # ── Load all movies that have a director text value ───────────────────────
    rows = db.execute(text("""
        SELECT id, title, director
        FROM   movies
        WHERE  director IS NOT NULL
          AND  director != ''
        ORDER  BY id
    """)).fetchall()

    logger.info("  Movies with director text: %d", len(rows))

    directors_inserted = 0
    links_created      = 0

    for movie_id, title, director_raw in rows:
        names = _parse_director_names(director_raw)
        if not names:
            continue

        for name in names:
            # ── Upsert director ───────────────────────────────────────────────
            existing = db.execute(
                text("SELECT id FROM directors WHERE LOWER(name) = LOWER(:name)"),
                {"name": name},
            ).fetchone()

            if existing:
                director_id = existing.id
            else:
                if not dry_run:
                    result = db.execute(
                        text("INSERT INTO directors (name) VALUES (:name) RETURNING id"),
                        {"name": name},
                    )
                    director_id = result.fetchone().id
                    directors_inserted += 1
                else:
                    directors_inserted += 1
                    director_id = -1

            # ── Link movie → director ─────────────────────────────────────────
            if director_id != -1:
                already_linked = db.execute(text("""
                    SELECT 1 FROM movie_directors
                    WHERE movie_id = :mid AND director_id = :did
                """), {"mid": movie_id, "did": director_id}).fetchone()

                if not already_linked:
                    if not dry_run:
                        db.execute(text("""
                            INSERT INTO movie_directors (movie_id, director_id)
                            VALUES (:mid, :did)
                            ON CONFLICT DO NOTHING
                        """), {"mid": movie_id, "did": director_id})
                    links_created += 1

    if not dry_run:
        db.commit()

    logger.info(
        "  Director migration complete: %d inserted, %d links created (dry_run=%s)",
        directors_inserted, links_created, dry_run,
    )
    return directors_inserted, links_created


# ─────────────────────────────────────────────────────────────────────────────
# FIX 2 — PRIMARY CAST ROLE PROMOTION
# ─────────────────────────────────────────────────────────────────────────────

def fix_primary_cast_roles(db: Session, dry_run: bool = False) -> int:
    """
    Promote actor_movies rows to role_type='primary' where:
      - billing_order <= PRIMARY_BILLING_MAX (i.e. top 3 billed)
      - current role_type = 'supporting'

    Returns count of rows updated.
    """
    logger.info("Fixing primary cast role labels …")

    if dry_run:
        count = db.execute(text("""
            SELECT COUNT(*) FROM actor_movies
            WHERE billing_order <= :cutoff
              AND role_type = 'supporting'
        """), {"cutoff": PRIMARY_BILLING_MAX}).scalar()
        logger.info("  [DRY RUN] Would promote %d rows to primary", count)
        return count

    result = db.execute(text("""
        UPDATE actor_movies
        SET    role_type = 'primary'
        WHERE  billing_order <= :cutoff
          AND  role_type = 'supporting'
    """), {"cutoff": PRIMARY_BILLING_MAX})
    db.commit()

    promoted = result.rowcount
    logger.info("  Promoted %d actor_movies rows to primary", promoted)
    return promoted


# ─────────────────────────────────────────────────────────────────────────────
# FIX 3 — TMDB CAST ENRICHMENT (for movies with no cast at all)
# ─────────────────────────────────────────────────────────────────────────────

def _get_movies_with_no_cast(db: Session, limit: Optional[int] = None) -> list[tuple]:
    """Return (movie_id, title, tmdb_id) for movies with zero actor_movies rows."""
    q = """
        SELECT m.id, m.title, m.tmdb_id
        FROM   movies m
        WHERE  m.tmdb_id IS NOT NULL
          AND  NOT EXISTS (
              SELECT 1 FROM actor_movies am WHERE am.movie_id = m.id
          )
        ORDER  BY m.id
    """
    if limit:
        q += f" LIMIT {int(limit)}"
    return db.execute(text(q)).fetchall()


def _build_actor_name_index(db: Session) -> dict[str, int]:
    """
    Return {normalised_name: actor_id} for all actors in our DB.
    Used for fast local matching against TMDB cast names.
    """
    rows = db.execute(text("SELECT id, name FROM actors")).fetchall()
    index: dict[str, int] = {}
    for actor_id, name in rows:
        index[name.lower().strip()] = actor_id
    return index


def enrich_cast_from_tmdb(
    db: Session,
    api_key: str,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> int:
    """
    For movies with no cast rows, fetch TMDB credits and insert matched actors.
    Only inserts actors that already exist in our actors table (no new actors created).

    Returns total cast rows inserted.
    """
    movies = _get_movies_with_no_cast(db, limit=limit)
    if not movies:
        logger.info("No movies with missing cast found.")
        return 0

    logger.info("Cast enrichment — movies with no cast: %d", len(movies))

    actor_index = _build_actor_name_index(db)
    total_inserted = 0

    for i, (movie_id, title, tmdb_id) in enumerate(movies, 1):
        prefix = f"[{i}/{len(movies)}]"

        credits = _fetch_tmdb_credits(tmdb_id, api_key)
        if not credits:
            logger.warning("  %s %s — TMDB credits unavailable", prefix, title)
            continue

        cast_members = sorted(
            credits.get("cast") or [],
            key=lambda x: x.get("order", 999),
        )

        inserted = 0
        for member in cast_members[:SUPPORTING_BILLING_MAX + 1]:
            order     = member.get("order", 999)
            tmdb_name = (member.get("name") or "").strip()
            character = (member.get("character") or "").strip() or None
            role_type = "primary" if order <= PRIMARY_BILLING_MAX else "supporting"

            # Match by normalised name
            actor_id = actor_index.get(tmdb_name.lower())
            if actor_id is None:
                continue  # actor not in our DB — skip, don't create new records

            if not dry_run:
                db.execute(text("""
                    INSERT INTO actor_movies
                        (actor_id, movie_id, character_name, billing_order, role_type)
                    VALUES
                        (:aid, :mid, :char, :order, :role)
                    ON CONFLICT (actor_id, movie_id) DO UPDATE
                        SET character_name = EXCLUDED.character_name,
                            billing_order  = EXCLUDED.billing_order,
                            role_type      = EXCLUDED.role_type
                """), {
                    "aid":   actor_id,
                    "mid":   movie_id,
                    "char":  character,
                    "order": order,
                    "role":  role_type,
                })
            inserted += 1

        if inserted and not dry_run:
            db.commit()

        total_inserted += inserted
        if inserted:
            logger.info("  %s %s — inserted %d cast rows", prefix, title, inserted)
        else:
            logger.debug("  %s %s — no matching actors found", prefix, title)

    logger.info("Cast enrichment complete — %d rows inserted", total_inserted)
    return total_inserted


# ─────────────────────────────────────────────────────────────────────────────
# FIX 4 — RE-VALIDATE AFFECTED MOVIES
# ─────────────────────────────────────────────────────────────────────────────

def revalidate_affected_movies(db: Session, api_key: str) -> int:
    """
    Re-run validation on all movies that currently have issues in the DB.
    Updates movie_validation_results in place.
    Returns count re-validated.
    """
    # Lazy import to avoid circular dependency
    from data_pipeline.validate_movies import validate_movie, _upsert_result

    movie_ids = db.execute(text("""
        SELECT movie_id FROM movie_validation_results
        WHERE  issues != '[]'::jsonb
        ORDER  BY confidence_score ASC
    """)).scalars().all()

    total   = len(movie_ids)
    updated = 0
    logger.info("Re-validating %d affected movies …", total)

    for i, movie_id in enumerate(movie_ids, 1):
        try:
            result = validate_movie(
                movie_id, db,
                tmdb_api_key=api_key,
                fetch_tmdb=True,
            )
            _upsert_result(result, db)
            updated += 1

            if i % REVALIDATE_BATCH == 0:
                db.commit()
                logger.info("  [%d/%d] batch committed", i, total)

        except Exception as exc:
            logger.warning("  revalidate error movie_id=%s: %s", movie_id, exc)

    db.commit()
    logger.info("Re-validation complete: %d/%d updated", updated, total)
    return updated


# ─────────────────────────────────────────────────────────────────────────────
# ISSUE ANALYSIS QUERIES
# ─────────────────────────────────────────────────────────────────────────────

def print_issue_analysis(db: Session) -> None:
    """Print top issues from movie_validation_results to stdout."""
    rows = db.execute(text("""
        SELECT issue, COUNT(*) AS freq
        FROM   movie_validation_results,
               jsonb_array_elements_text(issues) AS issue
        GROUP  BY issue
        ORDER  BY freq DESC
        LIMIT  20
    """)).fetchall()

    print("\n" + "─" * 60)
    print(f"{'ISSUE':<50} {'COUNT':>8}")
    print("─" * 60)
    for issue, freq in rows:
        print(f"  {issue:<48} {freq:>8}")
    print("─" * 60)

    status = db.execute(text("""
        SELECT status, COUNT(*) as cnt,
               ROUND(AVG(confidence_score)::numeric, 3) as avg_score
        FROM   movie_validation_results
        GROUP  BY status
        ORDER  BY status
    """)).fetchall()

    print("\nCURRENT STATUS BREAKDOWN")
    print("─" * 40)
    for s, cnt, avg in status:
        bar = "█" * int(avg * 20)
        print(f"  {s:<10} {cnt:>6}  {avg}  {bar}")
    print("─" * 40 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# BULK FIX RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run_fix_pipeline(
    directors: bool = True,
    cast_roles: bool = True,
    cast_enrich: bool = True,
    cast_enrich_limit: Optional[int] = None,
    revalidate: bool = True,
    dry_run: bool = False,
) -> FixReport:
    """
    Master fix runner.  Applies all safe fixes in order:
      1. Director migration   (SQL only, instant)
      2. Cast role promotion  (SQL only, instant)
      3. Cast enrichment      (TMDB API, rate-limited)
      4. Re-validation        (TMDB API, rate-limited)

    Parameters
    ----------
    directors          : run director migration
    cast_roles         : run primary-cast role promotion
    cast_enrich        : run TMDB cast enrichment for empty-cast movies
    cast_enrich_limit  : cap TMDB cast enrichment to N movies
    revalidate         : re-validate affected movies after fixes
    dry_run            : log what would change but write nothing
    """
    api_key = _get_api_key()
    db      = SessionLocal()
    report  = FixReport()

    try:
        print("\n" + "═" * 58)
        print("  SOUTH CINEMA ANALYTICS — FIX PIPELINE")
        print(f"  dry_run={dry_run}")
        print("═" * 58)

        # ── Pre-fix issue analysis ────────────────────────────────────────────
        print("\n[BEFORE] Issue analysis:")
        print_issue_analysis(db)

        # ── Fix 1: Director migration ─────────────────────────────────────────
        if directors:
            logger.info("=== FIX 1: Director migration ===")
            ins, links = migrate_directors(db, dry_run=dry_run)
            report.directors_inserted     = ins
            report.director_links_created = links

        # ── Fix 2: Cast role promotion ────────────────────────────────────────
        if cast_roles:
            logger.info("=== FIX 2: Primary cast role promotion ===")
            promoted = fix_primary_cast_roles(db, dry_run=dry_run)
            report.roles_promoted = promoted

        # ── Fix 3: TMDB cast enrichment ───────────────────────────────────────
        if cast_enrich and not dry_run:
            logger.info("=== FIX 3: TMDB cast enrichment ===")
            inserted = enrich_cast_from_tmdb(
                db, api_key,
                limit=cast_enrich_limit,
                dry_run=dry_run,
            )
            report.cast_rows_inserted = inserted

        # ── Fix 4: Re-validate ────────────────────────────────────────────────
        if revalidate and not dry_run:
            logger.info("=== FIX 4: Re-validation ===")
            count = revalidate_affected_movies(db, api_key)
            report.movies_revalidated = count

        # ── Post-fix issue analysis ───────────────────────────────────────────
        if not dry_run:
            print("\n[AFTER] Issue analysis:")
            print_issue_analysis(db)

    except Exception as exc:
        report.errors.append(str(exc))
        logger.error("Fix pipeline error: %s", exc, exc_info=True)
    finally:
        db.close()

    print(report.summary())
    return report


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="South Cinema Analytics — Fix Pipeline")
    p.add_argument("--dry-run",            action="store_true",
                   help="Show what would change without writing to DB")
    p.add_argument("--directors-only",     action="store_true",
                   help="Only run director migration")
    p.add_argument("--cast-only",          action="store_true",
                   help="Only run cast fixes")
    p.add_argument("--no-revalidate",      action="store_true",
                   help="Skip re-validation step")
    p.add_argument("--cast-enrich-limit",  type=int, default=None,
                   help="Limit TMDB cast enrichment to N movies")
    p.add_argument("--analysis-only",      action="store_true",
                   help="Print issue analysis and exit")
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    args = _parse_args()

    if args.analysis_only:
        db = SessionLocal()
        print_issue_analysis(db)
        db.close()
        sys.exit(0)

    run_directors = not args.cast_only
    run_cast      = not args.directors_only

    run_fix_pipeline(
        directors         = run_directors,
        cast_roles        = run_cast,
        cast_enrich       = run_cast,
        cast_enrich_limit = args.cast_enrich_limit,
        revalidate        = not args.no_revalidate,
        dry_run           = args.dry_run,
    )
