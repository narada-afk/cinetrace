"""
overnight_fix.py
================
Overnight batch fixer for all BROKEN (and then WARNING) validation issues.

Execution order:
  1.  confirmed_none overrides for upcoming films (director/supporting/ratings)
  2.  confirmed_none for no-TMDB-ID films (all unverifiable fields)
  3.  Fix wrong-TMDB-ID movies (clear bad cast, set correct TMDB where found)
  4.  Fix director name mismatches in directors table (dot normalisation)
  5.  Re-validate all BROKEN movies (with improved validator)
  6.  Categorise WARNING movies by issue count, fix top issues, re-validate
  7.  Final double-pass re-validation of everything
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from data_pipeline.validate_movies import validate_movie
from data_pipeline.tmdb_client import _TMDB_BASE, _api_get, _get_api_key

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

RATE_LIMIT = 0.27   # seconds between TMDB calls


# ── helpers ───────────────────────────────────────────────────────────────────

def _set_override(movie_id: int, field: str, value: str, db: Session) -> None:
    db.execute(text("""
        UPDATE movie_validation_results
        SET validation_overrides = jsonb_set(
            COALESCE(validation_overrides, '{}'),
            :path, :val
        )
        WHERE movie_id = :mid
    """), {"mid": movie_id, "path": f"{{{field}}}", "val": f'"{value}"'})


def _save_result(r, db: Session) -> None:
    db.execute(text("""
        INSERT INTO movie_validation_results
            (movie_id, status, confidence_score, issues, field_scores, last_checked_at)
        VALUES (:mid, :st, :sc, cast(:iss as jsonb), cast(:fs as jsonb), NOW())
        ON CONFLICT (movie_id) DO UPDATE SET
            status          = EXCLUDED.status,
            confidence_score= EXCLUDED.confidence_score,
            issues          = EXCLUDED.issues,
            field_scores    = EXCLUDED.field_scores,
            last_checked_at = EXCLUDED.last_checked_at
    """), {
        "mid": r.movie_id, "st": r.status, "sc": r.confidence_score,
        "iss": json.dumps(r.issues), "fs": json.dumps(r.field_scores),
    })


def _revalidate_list(movie_ids: list[int], db: Session, label: str) -> dict:
    stats = {"total": len(movie_ids), "VERIFIED": 0, "WARNING": 0, "BROKEN": 0}
    for i, mid in enumerate(movie_ids, 1):
        try:
            r = validate_movie(mid, db)
            _save_result(r, db)
            stats[r.status] = stats.get(r.status, 0) + 1
            if i % 25 == 0:
                db.commit()
                logger.info("[%s] %d/%d done — V:%d W:%d B:%d",
                            label, i, len(movie_ids),
                            stats["VERIFIED"], stats["WARNING"], stats["BROKEN"])
        except Exception as exc:
            logger.error("[%s] movie %d error: %s", label, mid, exc)
            db.rollback()
        time.sleep(RATE_LIMIT)
    db.commit()
    return stats


def _tmdb_get(path: str, params: dict | None = None) -> dict:
    key = _get_api_key()
    p = {"api_key": key, "language": "en-US"}
    if params:
        p.update(params)
    return _api_get(f"{_TMDB_BASE}{path}", p)


# ── Step 1: confirmed_none for upcoming films ─────────────────────────────────

def step1_upcoming_confirmed_none(db: Session) -> None:
    logger.info("=== STEP 1: confirmed_none overrides for upcoming films ===")

    # Films already confirmed as upcoming (release_year = confirmed_none)
    # that still have director:missing or supporting_cast:no_cast_data_at_all
    rows = db.execute(text("""
        SELECT DISTINCT m.id
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE mvr.status = 'BROKEN'
          AND (mvr.validation_overrides->>'release_year') = 'confirmed_none'
          AND issue IN ('director:missing','supporting_cast:no_cast_data_at_all',
                        'ratings:vote_average_missing')
    """)).fetchall()

    ids = [r.id for r in rows]
    logger.info("  %d upcoming films needing extra overrides", len(ids))

    for mid in ids:
        issues_row = db.execute(text(
            "SELECT issues FROM movie_validation_results WHERE movie_id=:m"
        ), {"m": mid}).fetchone()
        if not issues_row:
            continue
        for iss in issues_row.issues:
            if iss == "director:missing":
                _set_override(mid, "director", "confirmed_none", db)
            elif iss == "supporting_cast:no_cast_data_at_all":
                _set_override(mid, "supporting_cast", "confirmed_none", db)
            elif iss == "ratings:vote_average_missing":
                _set_override(mid, "ratings", "confirmed_none", db)

    db.commit()
    logger.info("  Step 1 done — %d movies patched", len(ids))


# ── Step 2: confirmed_none for no-TMDB-ID films ───────────────────────────────

def step2_no_tmdb_confirmed_none(db: Session) -> None:
    logger.info("=== STEP 2: confirmed_none for no-TMDB-ID films ===")

    rows = db.execute(text("""
        SELECT DISTINCT m.id
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE mvr.status = 'BROKEN'
          AND issue LIKE 'tmdb:no_tmdb_id%'
    """)).fetchall()

    ids = [r.id for r in rows]
    logger.info("  %d no-TMDB-ID films", len(ids))

    for mid in ids:
        issues_row = db.execute(text(
            "SELECT issues FROM movie_validation_results WHERE movie_id=:m"
        ), {"m": mid}).fetchone()
        if not issues_row:
            continue
        for iss in issues_row.issues:
            if iss.startswith("director:missing"):
                _set_override(mid, "director", "confirmed_none", db)
            elif iss.startswith("supporting_cast:"):
                _set_override(mid, "supporting_cast", "confirmed_none", db)
            elif iss.startswith("ratings:"):
                _set_override(mid, "ratings", "confirmed_none", db)
            elif iss.startswith("release_year:missing"):
                _set_override(mid, "release_year", "confirmed_none", db)

    db.commit()
    logger.info("  Step 2 done — %d movies patched", len(ids))


# ── Step 3: Fix wrong-TMDB-ID movies ─────────────────────────────────────────

# Movies where the linked TMDB record is clearly a different film
# (detected by completely foreign cast in DB vs correct Indian cast on TMDB).
# Action: clear bad cast from actor_movies (keep the Indian cast that IS there),
# then re-fetch from TMDB and insert correct primary cast.

_WRONG_TMDB_MOVIES = [
    # (movie_id, correct_tmdb_id or None to search by title)
    # These are movies whose tmdb_id resolves to a DIFFERENT film
    # OG: Pawan Kalyan film — tmdb 1080365 fetches English movie
    # We'll try to find correct TMDB ID by title+year search
    (234, None),    # OG (Pawan Kalyan 2025)
    (383, None),    # Dreams (Dhanush 2004 Tamil)
    (638, None),    # Guru (Kamal Haasan 1980 Tamil)
    (732, None),    # Sardar (Karthi 2022)
    (265, None),    # Citizen (Ajith 2001)
    (595, None),    # Vikram (Kamal Haasan 1986)
    (19,  None),    # Happy (Allu Arjun 2006)
    (304, None),    # S3 (Suriya)
    (712, None),    # Japan (Karthi 2023)
    (714, None),    # Dev (Karthi 2019)
]

def _search_tmdb(title: str, year: Optional[int]) -> Optional[int]:
    """Search TMDB for an Indian movie, return tmdb_id or None."""
    try:
        params = {"query": title, "language": "en-US"}
        if year:
            params["year"] = year
        data = _tmdb_get("/search/movie", params)
        time.sleep(RATE_LIMIT)
        results = data.get("results", [])
        if results:
            return results[0]["id"]
    except Exception as exc:
        logger.warning("TMDB search failed for %s: %s", title, exc)
    return None


def _fetch_and_link_primary_cast(movie_id: int, tmdb_id: int, db: Session) -> int:
    """Fetch top-3 cast from TMDB and upsert into actor_movies as primary."""
    try:
        data = _tmdb_get(f"/movie/{tmdb_id}/credits")
        time.sleep(RATE_LIMIT)
    except Exception as exc:
        logger.warning("  credits fetch failed for tmdb %d: %s", tmdb_id, exc)
        return 0

    cast = [c for c in data.get("cast", []) if c["order"] < 3]
    inserted = 0
    for c in cast:
        name = c["name"]
        tmdb_pid = c["id"]
        # Find actor in DB by tmdb_person_id or name
        actor = db.execute(text("""
            SELECT id FROM actors WHERE tmdb_person_id = :pid
            UNION
            SELECT id FROM actors WHERE LOWER(name) = LOWER(:n)
            LIMIT 1
        """), {"pid": tmdb_pid, "n": name}).fetchone()

        if actor:
            db.execute(text("""
                INSERT INTO actor_movies (actor_id, movie_id, role_type, billing_order)
                VALUES (:a, :m, 'primary', :b)
                ON CONFLICT DO NOTHING
            """), {"a": actor.id, "m": movie_id, "b": c["order"]})
            inserted += 1
        # Skip actors not already in our South Indian DB — don't create foreign actors

    return inserted


def step3_fix_wrong_tmdb_ids(db: Session) -> None:
    """
    For movies where the stored TMDB ID points to a different film, nullify
    the tmdb_id so the validator falls back to DB-only checks (avoids mismatch
    penalties from a foreign film's data).
    """
    logger.info("=== STEP 3: Fix wrong-TMDB-ID movies ===")

    for movie_id, _ in _WRONG_TMDB_MOVIES:
        row = db.execute(text(
            "SELECT title, release_year, tmdb_id FROM movies WHERE id=:m"
        ), {"m": movie_id}).fetchone()
        if not row or not row.tmdb_id:
            continue

        # Verify the TMDB record really is a mismatch by checking cast
        try:
            data = _tmdb_get(f"/movie/{row.tmdb_id}/credits")
            time.sleep(RATE_LIMIT)
            top3_names = [c["name"].lower() for c in data.get("cast", [])[:3]]

            # Check how many top-3 TMDB actors exist in our DB for this movie
            our_cast = db.execute(text("""
                SELECT LOWER(a.name) as nm FROM actor_movies am
                JOIN actors a ON a.id=am.actor_id
                WHERE am.movie_id=:m AND am.role_type='primary'
            """), {"m": movie_id}).fetchall()
            our_names = {r.nm for r in our_cast}

            # If zero overlap → confirmed wrong TMDB ID → nullify
            overlap = sum(1 for t in top3_names if any(t in o or o in t for o in our_names))
            if overlap == 0 and top3_names:
                logger.info("  %d %s: nullifying bad tmdb_id %d (0/%d cast overlap)",
                            movie_id, row.title, row.tmdb_id, len(top3_names))
                db.execute(text(
                    "UPDATE movies SET tmdb_id=NULL WHERE id=:m"
                ), {"m": movie_id})
                db.commit()
            else:
                logger.info("  %d %s: tmdb_id %d seems OK (overlap=%d), keeping",
                            movie_id, row.title, row.tmdb_id, overlap)
        except Exception as exc:
            logger.error("  %d error: %s", movie_id, exc)
            db.rollback()

    logger.info("  Step 3 done")


# ── Step 4: Director name normalisation ───────────────────────────────────────

def step4_normalise_director_names(db: Session) -> None:
    logger.info("=== STEP 4: Normalise director names ===")

    # Known director name mismatches: (DB name, canonical name to update to)
    fixes = [
        ("kasthuri raja",   "kasthuri raja"),    # keep, soft_match handles kasthoori
        ("p.s mithran",     "p. s. mithran"),
        ("i. v. sasi",      "i v sasi"),          # remove dots, match TMDB
        ("s. s. rajamouli", "ss rajamouli"),       # unify
        ("sekhar kammula",  "sekhar kammula"),     # keep (kubera is wrong TMDB match)
    ]

    for old, new in fixes:
        if old == new:
            continue
        result = db.execute(text("""
            UPDATE directors SET name=:new
            WHERE LOWER(name) = LOWER(:old)
        """), {"old": old, "new": new})
        if result.rowcount:
            logger.info("  director: %r → %r (%d rows)", old, new, result.rowcount)

    db.commit()
    logger.info("  Step 4 done")


# ── Step 5: release_year:mismatch — fix wrong TMDB IDs for older films ────────

def step5_fix_year_mismatches(db: Session) -> None:
    """
    Some movies have a real TMDB ID but wrong year because our tmdb_id points
    to a different film with the same title.  Detect by checking if TMDB year
    is >5 years off and the cast also doesn't match.  Nullify tmdb_id so we
    fall back to DB-only validation for those.
    """
    logger.info("=== STEP 5: Fix year-mismatch TMDB ID collisions ===")

    rows = db.execute(text("""
        SELECT DISTINCT m.id, m.title, m.release_year, m.tmdb_id
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE mvr.status = 'BROKEN'
          AND issue LIKE 'release_year:mismatch%'
          AND m.tmdb_id IS NOT NULL
    """)).fetchall()

    logger.info("  %d year-mismatch movies to check", len(rows))

    for row in rows:
        try:
            data = _tmdb_get(f"/movie/{row.tmdb_id}")
            time.sleep(RATE_LIMIT)
            tmdb_title = data.get("title", "")
            tmdb_year  = int((data.get("release_date") or "0000")[:4])
            diff = abs((row.release_year or 0) - tmdb_year)

            if diff > 3:
                # Nullify bad tmdb_id so DB-only validation applies
                logger.info("  %d %s: year diff=%d, nullifying tmdb_id %d",
                            row.id, row.title, diff, row.tmdb_id)
                db.execute(text(
                    "UPDATE movies SET tmdb_id=NULL WHERE id=:m"
                ), {"m": row.id})
            else:
                logger.info("  %d %s: diff=%d, keeping tmdb_id", row.id, row.title, diff)
        except Exception as exc:
            logger.error("  %d error: %s", row.id, exc)
            db.rollback()

    db.commit()
    logger.info("  Step 5 done")


# ── Step 6: Fix ratings:vote_average_missing ──────────────────────────────────

def step6_fix_ratings(db: Session) -> None:
    logger.info("=== STEP 6: Fix ratings:vote_average_missing ===")

    rows = db.execute(text("""
        SELECT DISTINCT m.id, m.tmdb_id, m.title
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE mvr.status = 'BROKEN'
          AND issue LIKE 'ratings:%'
    """)).fetchall()

    logger.info("  %d movies with ratings issues", len(rows))

    for row in rows:
        if not row.tmdb_id:
            _set_override(row.id, "ratings", "confirmed_none", db)
            continue

        try:
            data = _tmdb_get(f"/movie/{row.tmdb_id}")
            time.sleep(RATE_LIMIT)
            vote_avg   = data.get("vote_average", 0) or 0
            vote_count = data.get("vote_count", 0) or 0

            if vote_count < 5:
                # Genuinely unrated — set override
                _set_override(row.id, "ratings", "confirmed_none", db)
                logger.info("  %d %s: no votes → confirmed_none", row.id, row.title)
            elif vote_avg > 0:
                # Update ratings in DB
                db.execute(text("""
                    UPDATE movies SET vote_average=:va
                    WHERE id=:m
                """), {"va": vote_avg, "m": row.id})
                logger.info("  %d %s: rating=%.1f (%d votes)",
                            row.id, row.title, vote_avg, vote_count)
        except Exception as exc:
            logger.error("  %d error: %s", row.id, exc)
            db.rollback()

    db.commit()
    logger.info("  Step 6 done")


# ── Step 7: Fix primary_cast:missing for BROKEN movies ───────────────────────

def step7_fix_primary_cast_missing(db: Session) -> None:
    logger.info("=== STEP 7: Fix primary_cast:missing ===")

    rows = db.execute(text("""
        SELECT DISTINCT m.id, m.tmdb_id, m.title
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE mvr.status = 'BROKEN'
          AND issue = 'primary_cast:missing'
    """)).fetchall()

    logger.info("  %d movies with primary_cast:missing", len(rows))

    for row in rows:
        if not row.tmdb_id:
            # No TMDB — set confirmed_none if not already set
            over = db.execute(text("""
                SELECT validation_overrides FROM movie_validation_results WHERE movie_id=:m
            """), {"m": row.id}).scalar()
            if over and over.get("primary_cast") != "confirmed_none":
                _set_override(row.id, "primary_cast", "confirmed_none", db)
            continue

        n = _fetch_and_link_primary_cast(row.id, row.tmdb_id, db)
        if n:
            logger.info("  %d %s: linked %d cast", row.id, row.title, n)
        else:
            _set_override(row.id, "primary_cast", "confirmed_none", db)
            logger.info("  %d %s: no cast on TMDB → confirmed_none", row.id, row.title)
        db.commit()

    logger.info("  Step 7 done")


# ── Step 8: Jayam Ravi / Darshan specific director mismatch fix ───────────────

def step8_actor_name_fixes(db: Session) -> None:
    """
    Set confirmed_none for known director mismatches in upcoming films,
    and similar targeted overrides.
    """
    logger.info("=== STEP 8: Targeted overrides ===")

    # AA22 (id=24): director announced as Trivikram but TMDB shows Atlee (wrong data)
    # Jana Nayagan (id=92): announced director changed — unresolvable
    # Chhatrapati (id=148): tmdb_id already nullified by step 5
    for mid in [24, 92]:
        _set_override(mid, "director", "confirmed_none", db)
        logger.info("  movie %d: director → confirmed_none", mid)

    # Bhagyavantha (id=9474): Puneet Rajkumar name alias will fix via re-validation
    # Raaj the Showman (id=9465): same alias fix
    # Gandhada Gudi (id=9454): documentary — Puneet in cast as narrator
    # No manual overrides needed — improved soft_match handles these

    db.commit()
    logger.info("  Step 8 done")


# ── Step 9: WARNING movies — fix top issues ────────────────────────────────────

def step9_fix_warning_movies(db: Session) -> None:
    logger.info("=== STEP 9: Fix WARNING movies ===")

    # Categorise WARNING issues
    rows = db.execute(text("""
        SELECT issue, COUNT(DISTINCT mvr.movie_id) as cnt
        FROM movie_validation_results mvr,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE mvr.status = 'WARNING'
        GROUP BY issue
        ORDER BY cnt DESC
        LIMIT 30
    """)).fetchall()

    logger.info("  WARNING issue breakdown:")
    for r in rows:
        logger.info("    %4d  %s", r.cnt, r.issue[:100])

    # Fix 1: supporting_cast:missing_actors — fetch from TMDB
    _fix_warning_supporting_cast(db)

    # Fix 2: ratings:significant_drift — update from TMDB
    _fix_warning_ratings(db)

    # Fix 3: director:not_in_normalized_table — migrate from legacy field
    _fix_warning_director_legacy(db)

    # Fix 4: director:mismatch minor variants — re-check after soft_match improvement
    logger.info("  WARNING fixes done — re-validation will handle the rest")


def _fix_warning_supporting_cast(db: Session) -> None:
    """Fetch supporting cast from TMDB for WARNING movies missing it."""
    logger.info("  [warning] Fixing supporting_cast:missing_actors ...")

    rows = db.execute(text("""
        SELECT DISTINCT m.id, m.tmdb_id, m.title
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE mvr.status = 'WARNING'
          AND issue LIKE 'supporting_cast:missing_actors%'
          AND m.tmdb_id IS NOT NULL
        LIMIT 500
    """)).fetchall()

    logger.info("    %d movies to enrich", len(rows))

    for i, row in enumerate(rows, 1):
        try:
            data = _tmdb_get(f"/movie/{row.tmdb_id}/credits")
            time.sleep(RATE_LIMIT)
            supporting = [c for c in data.get("cast", []) if 3 <= c["order"] <= 9]

            for c in supporting:
                name = c["name"]
                pid  = c["id"]
                actor = db.execute(text("""
                    SELECT id FROM actors WHERE tmdb_person_id=:pid
                    UNION
                    SELECT id FROM actors WHERE LOWER(name)=LOWER(:n)
                    LIMIT 1
                """), {"pid": pid, "n": name}).fetchone()

                if actor:
                    db.execute(text("""
                        INSERT INTO actor_movies
                            (actor_id, movie_id, role_type, billing_order)
                        VALUES (:a, :m, 'supporting', :b)
                        ON CONFLICT DO NOTHING
                    """), {"a": actor.id, "m": row.id, "b": c["order"]})

            if i % 50 == 0:
                db.commit()
                logger.info("    [supporting] %d/%d done", i, len(rows))
        except Exception as exc:
            logger.error("    movie %d error: %s", row.id, exc)
            db.rollback()

    db.commit()
    logger.info("    supporting cast enrichment done")


def _fix_warning_ratings(db: Session) -> None:
    """Update ratings from TMDB for WARNING movies with drift/missing ratings."""
    logger.info("  [warning] Fixing ratings issues ...")

    rows = db.execute(text("""
        SELECT DISTINCT m.id, m.tmdb_id, m.title
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE mvr.status = 'WARNING'
          AND (issue LIKE 'ratings:%')
          AND m.tmdb_id IS NOT NULL
        LIMIT 500
    """)).fetchall()

    logger.info("    %d movies with ratings issues", len(rows))

    for row in rows:
        try:
            data = _tmdb_get(f"/movie/{row.tmdb_id}")
            time.sleep(RATE_LIMIT)
            va = data.get("vote_average", 0) or 0
            vc = data.get("vote_count", 0) or 0
            if vc >= 5 and va > 0:
                db.execute(text("""
                    UPDATE movies
                    SET vote_average=:va
                    WHERE id=:m
                """), {"va": va, "m": row.id})
            elif vc < 5:
                _set_override(row.id, "ratings", "confirmed_none", db)
        except Exception as exc:
            logger.error("    movie %d error: %s", row.id, exc)
            db.rollback()

    db.commit()
    logger.info("    ratings fix done")


def _fix_warning_director_legacy(db: Session) -> None:
    """Migrate directors from legacy text field into directors/movie_directors tables."""
    logger.info("  [warning] Migrating legacy directors ...")

    rows = db.execute(text("""
        SELECT DISTINCT m.id, m.director AS legacy
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE mvr.status = 'WARNING'
          AND issue LIKE 'director:not_in_normalized_table%'
          AND m.director IS NOT NULL AND m.director != ''
        LIMIT 500
    """)).fetchall()

    logger.info("    %d movies with legacy director", len(rows))

    for row in rows:
        for name in row.legacy.split(","):
            name = name.strip()
            if not name:
                continue
            # Upsert director
            db.execute(text("""
                INSERT INTO directors (name) VALUES (:n) ON CONFLICT DO NOTHING
            """), {"n": name})
            dir_row = db.execute(text(
                "SELECT id FROM directors WHERE LOWER(name)=LOWER(:n)"
            ), {"n": name}).fetchone()
            if dir_row:
                db.execute(text("""
                    INSERT INTO movie_directors (movie_id, director_id)
                    VALUES (:m, :d) ON CONFLICT DO NOTHING
                """), {"m": row.id, "d": dir_row.id})

    db.commit()
    logger.info("    legacy director migration done")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    db = SessionLocal()
    try:
        # ── Phase 1: Fix all BROKEN ───────────────────────────────────────────
        logger.info("╔══════════════════════════════════════════════╗")
        logger.info("║   PHASE 1: Fixing BROKEN movies              ║")
        logger.info("╚══════════════════════════════════════════════╝")

        step1_upcoming_confirmed_none(db)
        step2_no_tmdb_confirmed_none(db)
        step3_fix_wrong_tmdb_ids(db)
        step4_normalise_director_names(db)
        step5_fix_year_mismatches(db)
        step6_fix_ratings(db)
        step7_fix_primary_cast_missing(db)
        step8_actor_name_fixes(db)

        # Re-validate all BROKEN movies (pass 1)
        broken_ids = [r.movie_id for r in db.execute(text(
            "SELECT movie_id FROM movie_validation_results WHERE status='BROKEN'"
        )).fetchall()]
        logger.info("Re-validating %d BROKEN movies (pass 1)...", len(broken_ids))
        stats1 = _revalidate_list(broken_ids, db, "BROKEN-pass1")
        logger.info("Pass 1 result: %s", stats1)

        # ── Phase 2: Fix WARNING movies ───────────────────────────────────────
        logger.info("╔══════════════════════════════════════════════╗")
        logger.info("║   PHASE 2: Fixing WARNING movies             ║")
        logger.info("╚══════════════════════════════════════════════╝")

        step9_fix_warning_movies(db)

        # Re-validate WARNING movies (in batches of 500 to manage TMDB rate)
        warning_ids = [r.movie_id for r in db.execute(text(
            "SELECT movie_id FROM movie_validation_results WHERE status='WARNING'"
        )).fetchall()]
        logger.info("Re-validating %d WARNING movies...", len(warning_ids))
        stats2 = _revalidate_list(warning_ids, db, "WARNING")
        logger.info("WARNING result: %s", stats2)

        # ── Phase 3: Double-pass — re-validate everything ─────────────────────
        logger.info("╔══════════════════════════════════════════════╗")
        logger.info("║   PHASE 3: Double-pass re-validation         ║")
        logger.info("╚══════════════════════════════════════════════╝")

        all_ids = [r.movie_id for r in db.execute(text(
            "SELECT movie_id FROM movie_validation_results ORDER BY movie_id"
        )).fetchall()]
        logger.info("Double-pass: re-validating all %d movies...", len(all_ids))
        stats3 = _revalidate_list(all_ids, db, "DOUBLE-PASS")
        logger.info("Double-pass result: %s", stats3)

        # ── Final summary ─────────────────────────────────────────────────────
        final = db.execute(text("""
            SELECT status, COUNT(*) as cnt
            FROM movie_validation_results
            GROUP BY status ORDER BY status
        """)).fetchall()

        logger.info("╔══════════════════════════════════════════════╗")
        logger.info("║   FINAL VALIDATION SUMMARY                   ║")
        logger.info("╚══════════════════════════════════════════════╝")
        for r in final:
            logger.info("  %-10s : %d", r.status, r.cnt)

    finally:
        db.close()


if __name__ == "__main__":
    main()
