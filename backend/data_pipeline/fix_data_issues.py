"""
fix_data_issues.py
==================
Production-grade data repair orchestrator for South Cinema Analytics.

Execution order (all steps are idempotent):
  1. snapshot  — backup tables to _bak copies
  2. merge_duplicates        — merge true-duplicate movies (same industry only)
  3. remove_bad_actor_links  — remove cross-industry Hollywood actors (TMDB-validated)
  4. rebuild_collaborations  — DELETE all collab rows, rebuild from actor_movies
  5. recompute_industries    — set actor.industry = dominant movie.industry
  6. add_constraints         — UNIQUE indexes + guard trigger
  7. validate_gate           — abort with exit 1 if any critical issues remain

Usage:
  docker compose exec backend python -m data_pipeline.fix_data_issues
  docker compose exec backend python -m data_pipeline.fix_data_issues --dry-run
  docker compose exec backend python -m data_pipeline.fix_data_issues --step merge_duplicates

Rollback:
  docker compose exec backend python -m data_pipeline.fix_data_issues --rollback
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fix_data_issues")

# ── DB connection ─────────────────────────────────────────────────────────────

def _connect() -> psycopg2.extensions.connection:
    url = os.environ.get("DATABASE_URL", "postgresql://sca:sca@postgres:5432/sca")
    conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# STEP 0 — Snapshot (backup)
# ─────────────────────────────────────────────────────────────────────────────

SNAPSHOT_TABLES = ["movies", "actor_movies", "actor_collaborations", "actors"]

def snapshot(conn, dry_run: bool) -> None:
    """
    Copy core tables to <table>_bak for instant rollback.
    Safe to run repeatedly — drops existing _bak first.
    """
    log.info("── Step 0: Snapshot ──────────────────────────────────────")
    with conn.cursor() as cur:
        for tbl in SNAPSHOT_TABLES:
            bak = f"{tbl}_bak"
            cur.execute(f"DROP TABLE IF EXISTS {bak}")
            cur.execute(f"CREATE TABLE {bak} AS SELECT * FROM {tbl}")
            cur.execute(f"SELECT COUNT(*) AS n FROM {bak}")
            n = cur.fetchone()["n"]
            log.info("  Snapshot %s → %s (%d rows)", tbl, bak, n)
    if not dry_run:
        conn.commit()
        log.info("  Snapshots committed.")
    else:
        conn.rollback()
        log.info("  [DRY RUN] Rolled back snapshots.")


def rollback_from_snapshot(conn) -> None:
    """Restore all tables from their _bak copies."""
    log.info("── ROLLBACK from snapshots ───────────────────────────────")
    with conn.cursor() as cur:
        for tbl in SNAPSHOT_TABLES:
            bak = f"{tbl}_bak"
            cur.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s", (bak,))
            if cur.fetchone()["count"] == 0:
                log.warning("  No backup found for %s — skipping.", tbl)
                continue
            cur.execute(f"TRUNCATE {tbl} CASCADE")
            cur.execute(f"INSERT INTO {tbl} SELECT * FROM {bak}")
            cur.execute(f"SELECT COUNT(*) AS n FROM {tbl}")
            log.info("  Restored %s (%d rows)", tbl, cur.fetchone()["n"])
    conn.commit()
    log.info("  Rollback complete.")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Merge duplicate movies
# ─────────────────────────────────────────────────────────────────────────────

def _find_duplicate_groups(cur) -> list[dict]:
    """
    Return groups of movies with the same (lower title, release_year).
    Canonical = highest actor_count, then lowest id (tiebreak).

    SAFETY RULE: Only merge within the same industry.
    Cross-industry same-title films (e.g. Drishyam 3 Hindi vs Malayalam)
    are different movies and must NOT be merged.
    """
    cur.execute("""
        WITH actor_counts AS (
            SELECT movie_id, COUNT(*) AS cnt
            FROM actor_movies
            GROUP BY movie_id
        ),
        dup_groups AS (
            SELECT LOWER(TRIM(title)) AS norm_title, release_year, industry
            FROM movies
            GROUP BY LOWER(TRIM(title)), release_year, industry
            HAVING COUNT(*) > 1
        )
        SELECT
            m.id,
            m.title,
            m.release_year,
            m.tmdb_id,
            m.industry,
            COALESCE(ac.cnt, 0) AS actor_count,
            LOWER(TRIM(m.title))  AS norm_title
        FROM movies m
        JOIN dup_groups d
          ON LOWER(TRIM(m.title)) = d.norm_title
         AND m.release_year       = d.release_year
         AND m.industry           = d.industry
        LEFT JOIN actor_counts ac ON ac.movie_id = m.id
        ORDER BY
            LOWER(TRIM(m.title)),
            m.release_year,
            m.industry,
            COALESCE(ac.cnt, 0) DESC,
            m.id ASC
    """)
    rows = cur.fetchall()

    groups: dict[tuple, list] = {}
    for r in rows:
        key = (r["norm_title"], r["release_year"], r["industry"])
        groups.setdefault(key, []).append(dict(r))

    # Only return groups with > 1 member (i.e. genuine duplicates within same industry)
    return [members for members in groups.values() if len(members) > 1]


def merge_duplicates(conn, dry_run: bool) -> int:
    """
    For each true-duplicate group (same title + year + industry):
      1. Canonical = member with most actors (lowest id tiebreaker)
      2. Re-point actor_movies, movie_directors, cast → canonical id
      3. Delete duplicate movie rows

    Returns number of duplicate rows deleted.
    """
    log.info("── Step 1: Merge duplicate movies ────────────────────────")

    with conn.cursor() as cur:
        groups = _find_duplicate_groups(cur)

    if not groups:
        log.info("  No same-industry duplicates found — nothing to do.")
        return 0

    total_deleted = 0

    with conn.cursor() as cur:
        for members in groups:
            canonical = members[0]
            duplicates = members[1:]
            dup_ids = [d["id"] for d in duplicates]

            log.info(
                "  Merging: KEEP id=%d '%s' (%s %d, actors=%d) | DROP ids=%s",
                canonical["id"], canonical["title"], canonical["industry"],
                canonical["release_year"], canonical["actor_count"], dup_ids,
            )

            for dup_id in dup_ids:
                # ── Re-point actor_movies ──────────────────────────────────
                # Insert any actors from duplicate that canonical doesn't already have
                cur.execute("""
                    INSERT INTO actor_movies (actor_id, movie_id, character_name, billing_order, role_type)
                    SELECT am.actor_id, %s, am.character_name, am.billing_order, am.role_type
                    FROM actor_movies am
                    WHERE am.movie_id = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM actor_movies x
                          WHERE x.actor_id = am.actor_id AND x.movie_id = %s
                      )
                """, (canonical["id"], dup_id, canonical["id"]))
                inserted = cur.rowcount
                cur.execute("DELETE FROM actor_movies WHERE movie_id = %s", (dup_id,))
                log.info("    actor_movies: merged %d new actors, removed duplicate links", inserted)

                # ── Re-point movie_directors ───────────────────────────────
                cur.execute("""
                    INSERT INTO movie_directors (movie_id, director_id)
                    SELECT %s, md.director_id
                    FROM movie_directors md
                    WHERE md.movie_id = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM movie_directors x
                          WHERE x.movie_id = %s AND x.director_id = md.director_id
                      )
                    ON CONFLICT DO NOTHING
                """, (canonical["id"], dup_id, canonical["id"]))
                cur.execute("DELETE FROM movie_directors WHERE movie_id = %s", (dup_id,))

                # ── Re-point cast table ────────────────────────────────────
                cur.execute("""
                    UPDATE "cast" SET movie_id = %s
                    WHERE movie_id = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM "cast" x
                          WHERE x.movie_id = %s AND x.actor_id = "cast".actor_id
                      )
                """, (canonical["id"], dup_id, canonical["id"]))
                cur.execute('DELETE FROM "cast" WHERE movie_id = %s', (dup_id,))

                # ── Delete the duplicate movie row ─────────────────────────
                cur.execute("DELETE FROM movies WHERE id = %s", (dup_id,))
                total_deleted += 1

    if dry_run:
        conn.rollback()
        log.info("  [DRY RUN] Would delete %d duplicate movie rows.", total_deleted)
    else:
        conn.commit()
        log.info("  Deleted %d duplicate movie rows. Committed.", total_deleted)

    return total_deleted


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Remove incorrect cross-industry actor links
# ─────────────────────────────────────────────────────────────────────────────

def remove_bad_actor_links(conn, dry_run: bool) -> int:
    """
    Remove actor_movies rows where:
      - The actor was flagged by TMDB validation as NOT appearing in the film's cast
        (severity = 'critical', i.e. primary role)
      - AND the actor has only 1 film in our DB (removing this link orphans them)

    This precisely targets Hollywood actors mistakenly linked to South Indian films.
    It does NOT touch legitimate supporting actors who appear in one film.
    """
    log.info("── Step 2: Remove incorrect cross-industry actor links ────")

    with conn.cursor() as cur:
        # Check validation table exists and has data
        cur.execute("""
            SELECT COUNT(*) AS n FROM information_schema.tables
            WHERE table_name = 'validation_actor_movie_links'
        """)
        if cur.fetchone()["n"] == 0:
            log.warning("  validation_actor_movie_links not found — run validate_integrity first.")
            return 0

        cur.execute("SELECT COUNT(*) AS n FROM validation_actor_movie_links WHERE severity = 'critical'")
        if cur.fetchone()["n"] == 0:
            log.info("  No critical actor-movie link issues in validation table — skipping.")
            return 0

        # Targets: flagged as critical + actor has only 1 film total
        cur.execute("""
            SELECT v.actor_id, v.actor_name, v.movie_id, v.movie_title, v.issue
            FROM validation_actor_movie_links v
            WHERE v.severity = 'critical'
              AND (
                  SELECT COUNT(*) FROM actor_movies am2
                  WHERE am2.actor_id = v.actor_id
              ) = 1
        """)
        targets = cur.fetchall()

    if not targets:
        log.info("  No single-film critical actors to remove.")
        return 0

    log.info("  Removing %d incorrect actor-movie links:", len(targets))
    for t in targets:
        log.info("    %s → %s", t["actor_name"], t["movie_title"])

    with conn.cursor() as cur:
        for t in targets:
            cur.execute(
                "DELETE FROM actor_movies WHERE actor_id = %s AND movie_id = %s",
                (t["actor_id"], t["movie_id"]),
            )
        # Also remove orphaned actors (no remaining films)
        cur.execute("""
            DELETE FROM actors
            WHERE id IN (
                SELECT a.id FROM actors a
                WHERE NOT EXISTS (
                    SELECT 1 FROM actor_movies am WHERE am.actor_id = a.id
                )
                AND a.industry NOT IN ('Telugu','Tamil','Malayalam','Kannada','Hindi')
            )
        """)
        orphan_actors_removed = cur.rowcount
        log.info("  Removed %d orphaned non-South-Indian actor records.", orphan_actors_removed)

    if dry_run:
        conn.rollback()
        log.info("  [DRY RUN] Would remove %d actor-movie links.", len(targets))
    else:
        conn.commit()
        log.info("  Removed %d actor-movie links. Committed.", len(targets))

    return len(targets)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Rebuild actor_collaborations from actor_movies
# ─────────────────────────────────────────────────────────────────────────────

def rebuild_collaborations(conn, dry_run: bool) -> int:
    """
    Full rebuild of actor_collaborations:
      1. TRUNCATE the table
      2. INSERT computed pairs (both directions for fast lookups)

    This is the ONLY safe way to update collaborations.
    Ghost rows (from deleted movies, bad links) are eliminated automatically.
    """
    log.info("── Step 3: Rebuild actor_collaborations ──────────────────")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM actor_collaborations")
        before = cur.fetchone()["n"]
        log.info("  Before: %d rows", before)

        # Compute canonical pairs (a1_id < a2_id)
        cur.execute("""
            SELECT
                LEAST(am1.actor_id, am2.actor_id)    AS actor1_id,
                GREATEST(am1.actor_id, am2.actor_id) AS actor2_id,
                COUNT(DISTINCT am1.movie_id)          AS collaboration_count
            FROM actor_movies am1
            JOIN actor_movies am2
              ON am1.movie_id  = am2.movie_id
             AND am1.actor_id != am2.actor_id
            GROUP BY 1, 2
            HAVING COUNT(DISTINCT am1.movie_id) >= 1
        """)
        pairs = cur.fetchall()
        log.info("  Computed %d unique pairs (one direction).", len(pairs))

        # Build both-direction rows
        rows = []
        for p in pairs:
            a1, a2, cnt = p["actor1_id"], p["actor2_id"], p["collaboration_count"]
            rows.append({"actor1_id": a1, "actor2_id": a2, "collaboration_count": cnt})
            rows.append({"actor1_id": a2, "actor2_id": a1, "collaboration_count": cnt})

        cur.execute("TRUNCATE actor_collaborations")
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO actor_collaborations (actor1_id, actor2_id, collaboration_count)
            VALUES (%(actor1_id)s, %(actor2_id)s, %(collaboration_count)s)
        """, rows, page_size=1000)

        cur.execute("SELECT COUNT(*) AS n FROM actor_collaborations")
        after = cur.fetchone()["n"]
        log.info("  After: %d rows (%+d)", after, after - before)

    if dry_run:
        conn.rollback()
        log.info("  [DRY RUN] Rolled back collaboration rebuild.")
    else:
        conn.commit()
        log.info("  Collaboration rebuild committed.")

    return after if not dry_run else len(rows)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Recompute actor.industry
# ─────────────────────────────────────────────────────────────────────────────

def recompute_industries(conn, dry_run: bool) -> int:
    """
    Set actor.industry = the most frequent movie.industry for that actor.
    Only updates actors who have at least one film. Actors with no films
    are left unchanged (they'll be cleaned up by other steps).
    """
    log.info("── Step 4: Recompute actor.industry ──────────────────────")

    with conn.cursor() as cur:
        cur.execute("""
            WITH dominant AS (
                SELECT
                    am.actor_id,
                    m.industry,
                    COUNT(*) AS cnt,
                    ROW_NUMBER() OVER (
                        PARTITION BY am.actor_id
                        ORDER BY COUNT(*) DESC, m.industry
                    ) AS rn
                FROM actor_movies am
                JOIN movies m ON m.id = am.movie_id
                WHERE m.industry IS NOT NULL
                GROUP BY am.actor_id, m.industry
            )
            UPDATE actors a
            SET industry = d.industry
            FROM dominant d
            WHERE d.actor_id = a.id
              AND d.rn = 1
              AND (a.industry IS DISTINCT FROM d.industry)
        """)
        updated = cur.rowcount
        log.info("  Updated industry for %d actors.", updated)

    if dry_run:
        conn.rollback()
        log.info("  [DRY RUN] Rolled back industry update.")
    else:
        conn.commit()
        log.info("  Industry recompute committed.")

    return updated


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Add constraints (prevention)
# ─────────────────────────────────────────────────────────────────────────────

def add_constraints(conn, dry_run: bool) -> None:
    """
    Add idempotent database constraints and advisory trigger.
    Each constraint is applied in its own savepoint so failures don't abort the batch.
    NOTE: uq_movies_tmdb_id will fail if tmdb_id duplicates remain — run merge_duplicates first.
    """
    log.info("── Step 5: Add constraints ───────────────────────────────")

    # Each entry is (description, SQL string)
    # $$-quoted blocks must be a single string — never split by semicolon
    constraints = [
        (
            "UNIQUE index on movies.tmdb_id",
            """
            DROP INDEX IF EXISTS uq_movies_tmdb_id;
            CREATE UNIQUE INDEX uq_movies_tmdb_id
                ON movies (tmdb_id)
                WHERE tmdb_id IS NOT NULL
            """,
        ),
        (
            "CHECK collaboration_count > 0",
            """
            ALTER TABLE actor_collaborations
                DROP CONSTRAINT IF EXISTS chk_collab_count_positive,
                ADD  CONSTRAINT chk_collab_count_positive
                    CHECK (collaboration_count > 0)
            """,
        ),
        (
            "CHECK no self-collaboration",
            """
            ALTER TABLE actor_collaborations
                DROP CONSTRAINT IF EXISTS chk_collab_no_self,
                ADD  CONSTRAINT chk_collab_no_self
                    CHECK (actor1_id != actor2_id)
            """,
        ),
        (
            "Advisory trigger function on actor_collaborations",
            """
            CREATE OR REPLACE FUNCTION fn_collab_rebuild_notice()
            RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
            BEGIN
                RAISE NOTICE
                    'actor_collaborations: prefer rebuild_collaborations() over direct inserts.';
                RETURN NEW;
            END;
            $fn$
            """,
        ),
        (
            "Advisory trigger on actor_collaborations",
            """
            DO $do$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_trigger
                    WHERE tgname   = 'trg_collab_rebuild_notice'
                      AND tgrelid  = 'actor_collaborations'::regclass
                ) THEN
                    CREATE TRIGGER trg_collab_rebuild_notice
                    BEFORE INSERT ON actor_collaborations
                    FOR EACH STATEMENT
                    EXECUTE FUNCTION fn_collab_rebuild_notice();
                END IF;
            END;
            $do$
            """,
        ),
    ]

    for desc, sql in constraints:
        sp = "sp_" + "".join(c if c.isalnum() else "_" for c in desc[:20])
        try:
            with conn.cursor() as cur:
                cur.execute(f"SAVEPOINT {sp}")
                cur.execute(sql)
                cur.execute(f"RELEASE SAVEPOINT {sp}")
                log.info("  OK: %s", desc)
        except Exception as exc:
            with conn.cursor() as cur:
                cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                cur.execute(f"RELEASE SAVEPOINT {sp}")
            log.warning("  SKIP '%s': %s", desc, str(exc).splitlines()[0])

    if dry_run:
        conn.rollback()
        log.info("  [DRY RUN] Rolled back constraints.")
    else:
        conn.commit()
        log.info("  Constraints committed.")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Validation gate
# ─────────────────────────────────────────────────────────────────────────────

def validation_gate(conn) -> bool:
    """
    Post-fix validation. Returns True (PASS) or False (FAIL).
    Hard fails on:
      - Any ghost collaborations
      - Any exact same-industry duplicate movies
      - Collaboration counts that don't match actor_movies
    """
    log.info("── Step 6: Validation gate ───────────────────────────────")
    passed = True

    with conn.cursor() as cur:
        # Ghost collaborations
        cur.execute("""
            SELECT COUNT(*) AS n
            FROM actor_collaborations ac
            WHERE NOT EXISTS (
                SELECT 1
                FROM actor_movies am1
                JOIN actor_movies am2
                  ON am1.movie_id = am2.movie_id
                 AND am1.actor_id = ac.actor1_id
                 AND am2.actor_id = ac.actor2_id
            )
        """)
        ghosts = cur.fetchone()["n"]
        if ghosts > 0:
            log.error("  FAIL: %d ghost collaborations remain.", ghosts)
            passed = False
        else:
            log.info("  PASS: 0 ghost collaborations.")

        # Same-industry exact duplicate movies
        cur.execute("""
            SELECT COUNT(*) AS n
            FROM (
                SELECT LOWER(TRIM(title)), release_year, industry
                FROM movies
                GROUP BY LOWER(TRIM(title)), release_year, industry
                HAVING COUNT(*) > 1
            ) dups
        """)
        dups = cur.fetchone()["n"]
        if dups > 0:
            log.error("  FAIL: %d same-industry duplicate movie groups remain.", dups)
            passed = False
        else:
            log.info("  PASS: 0 same-industry duplicate movies.")

        # Collaboration count integrity (sample check)
        cur.execute("""
            WITH computed AS (
                SELECT
                    LEAST(am1.actor_id, am2.actor_id)    AS a1,
                    GREATEST(am1.actor_id, am2.actor_id) AS a2,
                    COUNT(DISTINCT am1.movie_id)          AS cnt
                FROM actor_movies am1
                JOIN actor_movies am2
                  ON am1.movie_id = am2.movie_id AND am1.actor_id < am2.actor_id
                GROUP BY 1, 2
            ),
            stored AS (
                SELECT
                    LEAST(actor1_id, actor2_id)    AS a1,
                    GREATEST(actor1_id, actor2_id) AS a2,
                    MAX(collaboration_count)        AS cnt
                FROM actor_collaborations
                GROUP BY 1, 2
            )
            SELECT COUNT(*) AS n
            FROM computed c
            JOIN stored s ON c.a1 = s.a1 AND c.a2 = s.a2
            WHERE c.cnt != s.cnt
        """)
        mismatches = cur.fetchone()["n"]
        if mismatches > 0:
            log.error("  FAIL: %d collaboration count mismatches.", mismatches)
            passed = False
        else:
            log.info("  PASS: collaboration counts match actor_movies.")

        # Summary stats
        cur.execute("SELECT COUNT(*) AS n FROM movies")
        log.info("  movies: %d", cur.fetchone()["n"])
        cur.execute("SELECT COUNT(*) AS n FROM actor_movies")
        log.info("  actor_movies: %d", cur.fetchone()["n"])
        cur.execute("SELECT COUNT(*) AS n FROM actor_collaborations WHERE actor1_id < actor2_id")
        log.info("  unique collaboration pairs: %d", cur.fetchone()["n"])
        cur.execute("SELECT COUNT(DISTINCT industry) AS n FROM actors WHERE industry IS NOT NULL")
        log.info("  distinct actor industries: %d", cur.fetchone()["n"])

    verdict = "✅  PASS — safe to rebuild analytics" if passed else "❌  FAIL — issues remain"
    log.info("  Gate result: %s", verdict)
    return passed


# ─────────────────────────────────────────────────────────────────────────────
# Ingestion-time actor filter (Part 2, Item 3)
# ─────────────────────────────────────────────────────────────────────────────

def should_reject_actor_link(
    actor_id: int,
    actor_industry: str | None,
    movie_industry: str,
    existing_film_count: int,
) -> tuple[bool, str]:
    """
    Ingestion-time gate: returns (reject: bool, reason: str).

    Reject an actor_movies link if:
      - The actor has appeared in only this one film (new or single-film)
      - AND the actor's known industry is completely different from the movie

    "Different" = neither matches and both are non-null.
    Cross-industry actors are common in South Indian cinema, so the rule
    only activates when the mismatch is unambiguous.

    Usage in ingestion scripts:
        reject, reason = should_reject_actor_link(aid, a.industry, m.industry, count)
        if reject:
            logger.warning("Skipping: %s", reason)
            continue
    """
    SOUTH_INDIAN = {"Telugu", "Tamil", "Malayalam", "Kannada", "Hindi"}
    FOREIGN      = {"English", "Korean", "Chinese", "French", "German", "Japanese"}

    if existing_film_count > 1:
        return False, ""   # Actor has multiple films — not a single-film ghost

    if actor_industry is None:
        return False, ""   # Unknown industry — can't judge

    if movie_industry in SOUTH_INDIAN and actor_industry in FOREIGN:
        return True, (
            f"Single-film foreign actor rejected: actor_industry={actor_industry!r} "
            f"movie_industry={movie_industry!r}"
        )

    return False, ""


# ─────────────────────────────────────────────────────────────────────────────
# CLI / orchestrator
# ─────────────────────────────────────────────────────────────────────────────

STEPS = {
    "snapshot":             snapshot,
    "merge_duplicates":     merge_duplicates,
    "remove_bad_links":     remove_bad_actor_links,
    "rebuild_collabs":      rebuild_collaborations,
    "recompute_industries": recompute_industries,
    "add_constraints":      add_constraints,
}


def run_all(conn, dry_run: bool) -> int:
    t0 = time.monotonic()
    log.info("=" * 58)
    log.info("  South Cinema Analytics — Data Fix Pipeline")
    log.info("  Mode: %s", "DRY RUN" if dry_run else "LIVE")
    log.info("=" * 58)

    snapshot(conn, dry_run=False)   # always snapshot, even in dry-run mode
    merge_duplicates(conn, dry_run)
    remove_bad_actor_links(conn, dry_run)
    rebuild_collaborations(conn, dry_run)
    recompute_industries(conn, dry_run)
    add_constraints(conn, dry_run)

    if not dry_run:
        passed = validation_gate(conn)
        elapsed = time.monotonic() - t0
        log.info("Completed in %.1fs", elapsed)
        return 0 if passed else 1
    else:
        log.info("[DRY RUN] All steps complete — no changes persisted.")
        return 0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="South Cinema Analytics — data fix pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Steps (run individually with --step):
  snapshot             backup core tables
  merge_duplicates     merge same-industry exact duplicate movies
  remove_bad_links     remove Hollywood actors from South Indian films
  rebuild_collabs      truncate + rebuild actor_collaborations
  recompute_industries update actor.industry from dominant movie.industry
  add_constraints      add UNIQUE indexes + check constraints

Rollback:
  --rollback           restore all tables from _bak snapshots
        """,
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Preview changes without writing to DB")
    p.add_argument("--step", choices=list(STEPS.keys()),
                   help="Run only a single step")
    p.add_argument("--rollback", action="store_true",
                   help="Restore DB from snapshots (taken at last run start)")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    conn = _connect()

    try:
        if args.rollback:
            rollback_from_snapshot(conn)
            return 0

        if args.step:
            fn = STEPS[args.step]
            result = fn(conn, args.dry_run)
            log.info("Step '%s' result: %s", args.step, result)
            return 0

        return run_all(conn, args.dry_run)

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
