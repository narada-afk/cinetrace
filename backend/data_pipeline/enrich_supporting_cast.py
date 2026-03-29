"""
enrich_supporting_cast.py
=========================
Fetches supporting cast (TMDB billing positions 3–12) for movies flagged
with supporting_cast:no_cast_data_at_all, then either:

  A) Inserts actors into actor_movies as role_type='supporting'
  B) Sets validation_overrides.supporting_cast = 'confirmed_none' when
     TMDB also has no cast for that movie (legitimately empty)

Usage
-----
    cd backend
    export TMDB_API_KEY=your_key
    python -m data_pipeline.enrich_supporting_cast             # full run
    python -m data_pipeline.enrich_supporting_cast --limit 10  # test run
    python -m data_pipeline.enrich_supporting_cast --dry-run   # no writes
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

SUPPORTING_BILLING_START = 3    # billing positions 3–12
SUPPORTING_BILLING_END   = 12
RATE_LIMIT_S             = 0.26


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_or_create_actor(name: str, tmdb_person_id: int, industry: str, db: Session) -> Optional[int]:
    """Return actor.id — creating a minimal record if not present. Never duplicates."""
    row = db.execute(
        text("SELECT id FROM actors WHERE tmdb_person_id = :tid"),
        {"tid": tmdb_person_id},
    ).fetchone()
    if row:
        return row.id

    row = db.execute(
        text("SELECT id FROM actors WHERE LOWER(TRIM(name)) = LOWER(TRIM(:n))"),
        {"n": name},
    ).fetchone()
    if row:
        db.execute(
            text("UPDATE actors SET tmdb_person_id = :tid WHERE id = :aid AND tmdb_person_id IS NULL"),
            {"tid": tmdb_person_id, "aid": row.id},
        )
        return row.id

    result = db.execute(
        text("""
            INSERT INTO actors (name, tmdb_person_id, industry)
            VALUES (:name, :tid, :industry)
            ON CONFLICT DO NOTHING
            RETURNING id
        """),
        {"name": name.strip(), "tid": tmdb_person_id, "industry": industry},
    ).fetchone()

    if result:
        return result.id

    row = db.execute(
        text("SELECT id FROM actors WHERE tmdb_person_id = :tid"),
        {"tid": tmdb_person_id},
    ).fetchone()
    return row.id if row else None


def _set_override(movie_id: int, field: str, value: str, db: Session) -> None:
    db.execute(
        text("""
            UPDATE movie_validation_results
            SET validation_overrides = jsonb_set(validation_overrides, :path, :value)
            WHERE movie_id = :mid
        """),
        {"mid": movie_id, "path": f'{{{field}}}', "value": f'"{value}"'},
    )


def _fetch_tmdb_supporting_cast(tmdb_id: int) -> Optional[list[dict]]:
    """
    Fetch supporting cast from TMDB (billing positions 3–12).
    Returns None on API failure, [] if TMDB genuinely has no cast.
    """
    api_key = _get_api_key()
    try:
        data = _api_get(
            f"{_TMDB_BASE}/movie/{tmdb_id}/credits",
            {"api_key": api_key, "language": "en-US"},
        )
    except Exception as exc:
        logger.warning("TMDB credits failed for tmdb_id=%s: %s", tmdb_id, exc)
        return None

    raw_cast = sorted(
        (data.get("cast") or []),
        key=lambda c: c.get("order", 999),
    )

    if not raw_cast:
        return []

    supporting = [
        {
            "name":           c["name"].strip(),
            "tmdb_person_id": c["id"],
            "billing_order":  c.get("order", 0),
            "character_name": (c.get("character") or "").strip(),
            "gender":         {1: "F", 2: "M"}.get(c.get("gender"), None),
        }
        for c in raw_cast
        if SUPPORTING_BILLING_START <= c.get("order", 999) <= SUPPORTING_BILLING_END
        and c.get("name") and c.get("id")
    ]
    return supporting


# ─── Main enrichment function ─────────────────────────────────────────────────

def enrich_supporting_cast(
    db: Session,
    *,
    dry_run: bool = False,
    limit:   Optional[int] = None,
) -> dict:
    """
    Enrich supporting cast for BROKEN movies with supporting_cast:no_cast_data_at_all.
    """
    q = """
        SELECT m.id AS movie_id, m.tmdb_id, m.title, m.industry
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id
        WHERE mvr.status = 'BROKEN'
          AND mvr.issues::text LIKE '%supporting_cast:no_cast_data_at_all%'
          AND m.tmdb_id IS NOT NULL
          AND (mvr.validation_overrides->>'supporting_cast') IS NULL
        ORDER BY m.id
    """
    if limit:
        q += f" LIMIT {limit}"

    rows  = db.execute(text(q)).fetchall()
    total = len(rows)
    logger.info("[enrich_supporting_cast] %d movies to process", total)

    enriched    = 0
    placeholded = 0
    skipped     = 0
    errors      = 0

    for i, row in enumerate(rows, 1):
        movie_id = row.movie_id
        tmdb_id  = row.tmdb_id
        title    = row.title
        industry = row.industry or "Unknown"

        try:
            supporting = _fetch_tmdb_supporting_cast(tmdb_id)
            time.sleep(RATE_LIMIT_S)

            if supporting is None:
                logger.warning("  [%d/%d] API fail — skipping (%s)", i, total, title)
                skipped += 1
                continue

            if len(supporting) == 0:
                logger.info("  [%d/%d] confirmed_none → %s", i, total, title)
                if not dry_run:
                    _set_override(movie_id, "supporting_cast", "confirmed_none", db)
                placeholded += 1
                continue

            inserted = 0
            for actor in supporting:
                actor_id = _get_or_create_actor(
                    actor["name"], actor["tmdb_person_id"], industry, db
                )
                if not actor_id:
                    continue

                if actor["gender"] and not dry_run:
                    db.execute(
                        text("UPDATE actors SET gender = :g WHERE id = :aid AND gender IS NULL"),
                        {"g": actor["gender"], "aid": actor_id},
                    )

                if not dry_run:
                    db.execute(
                        text("""
                            INSERT INTO actor_movies
                                (actor_id, movie_id, role_type, billing_order, character_name)
                            VALUES
                                (:aid, :mid, 'supporting', :order, :char)
                            ON CONFLICT (actor_id, movie_id) DO UPDATE
                                SET role_type      = 'supporting',
                                    billing_order  = EXCLUDED.billing_order,
                                    character_name = EXCLUDED.character_name
                        """),
                        {
                            "aid":   actor_id,
                            "mid":   movie_id,
                            "order": actor["billing_order"],
                            "char":  actor["character_name"],
                        },
                    )
                inserted += 1

            logger.info("  [%d/%d] enriched %d supporting actor(s) → %s", i, total, inserted, title)
            enriched += 1

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
        "enriched":    enriched,
        "placeholded": placeholded,
        "skipped":     skipped,
        "errors":      errors,
        "dry_run":     dry_run,
    }
    logger.info(
        "[enrich_supporting_cast] Done — enriched=%d placeholded=%d skipped=%d errors=%d",
        enriched, placeholded, skipped, errors,
    )
    return summary


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from app.database import SessionLocal

    parser = argparse.ArgumentParser(description="Enrich supporting cast from TMDB")
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
        summary = enrich_supporting_cast(db, dry_run=args.dry_run, limit=args.limit)
        print("\n" + "─" * 50)
        print(f"  Total processed : {summary['total']}")
        print(f"  Enriched        : {summary['enriched']}")
        print(f"  Placeholded     : {summary['placeholded']}  (confirmed_none)")
        print(f"  Skipped         : {summary['skipped']}  (API failures)")
        print(f"  Errors          : {summary['errors']}")
        print(f"  Dry run         : {summary['dry_run']}")
        print("─" * 50)
    finally:
        db.close()
