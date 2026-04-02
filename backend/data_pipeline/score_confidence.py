"""
score_confidence.py
===================
Compute and persist confidence scores for actors, movies, and the overall system.

Scoring model
─────────────
Actor score (0–100):
  +20  tmdb_person_id IS NOT NULL          (verified identity)
  +20  film count tier: ≥5→20  ≥3→15  ≥2→10  1→5  0→0
  +30  no invalid actor_movies (is_valid=FALSE → deduct 10 per bad link, min 0)
  +15  actor.industry matches dominant movie.industry
  +15  at least one collaboration exists

Movie score (0–100):
  +20  tmdb_id IS NOT NULL
  +15  poster_url IS NOT NULL
  +15  runtime IS NOT NULL AND runtime > 0
  +15  vote_average IS NOT NULL
  +15  director IS NOT NULL AND director != ''
  +20  actor count tier: ≥5→20  ≥2→15  1→8  0→0

System score (0–100):
  avg_actor_score * 0.35 + avg_movie_score * 0.35 + collab_integrity * 0.30

  collab_integrity = 100 if ghost_count = 0 else max(0, 100 − ghost_count × 2)

Usage:
  docker compose exec backend python -m data_pipeline.score_confidence
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

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
log = logging.getLogger("score_confidence")


# ── Connection ────────────────────────────────────────────────────────────────

def _connect() -> psycopg2.extensions.connection:
    url = os.environ.get("DATABASE_URL", "postgresql://sca:sca@postgres:5432/sca")
    conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    return conn


# ── Column existence guard ────────────────────────────────────────────────────

def _col_exists(cur, table: str, col: str) -> bool:
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s AND table_schema = 'public'
    """, (table, col))
    return cur.fetchone() is not None


def _table_exists(cur, table: str) -> bool:
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = %s AND table_schema = 'public'
    """, (table,))
    return cur.fetchone() is not None


# ── Output dataclass ──────────────────────────────────────────────────────────

@dataclass
class SystemHealthSnapshot:
    system_score:       float
    avg_actor_score:    float
    avg_movie_score:    float
    collab_integrity:   float
    ghost_collab_count: int
    duplicate_count:    int
    invalid_link_count: int
    total_actors:       int
    total_movies:       int
    total_collab_pairs: int
    validation_passed:  bool
    sources_used:       list[str]
    computed_at:        datetime


# ── Ghost collab count ────────────────────────────────────────────────────────

def _count_ghosts(cur) -> int:
    cur.execute("""
        SELECT COUNT(*) AS n
        FROM actor_collaborations ac
        WHERE NOT EXISTS (
            SELECT 1
            FROM actor_movies am1
            JOIN actor_movies am2
              ON am1.movie_id  = am2.movie_id
             AND am1.actor_id  = ac.actor1_id
             AND am2.actor_id  = ac.actor2_id
        )
    """)
    return cur.fetchone()["n"]


def _count_same_industry_duplicates(cur) -> int:
    cur.execute("""
        SELECT COUNT(*) AS n FROM (
            SELECT LOWER(TRIM(title)), release_year, industry
            FROM movies
            GROUP BY 1, 2, 3
            HAVING COUNT(*) > 1
        ) dups
    """)
    return cur.fetchone()["n"]


# ── Actor scoring ─────────────────────────────────────────────────────────────

def _score_actors(cur) -> tuple[list[dict], float]:
    """
    Compute score for every actor in a single SQL pass.
    Returns (rows_for_batch_update, avg_score).
    """
    has_is_valid = _col_exists(cur, "actor_movies", "is_valid")
    invalid_filter = "AND am.is_valid = FALSE" if has_is_valid else "AND FALSE"

    cur.execute(f"""
        WITH film_counts AS (
            SELECT actor_id, COUNT(*) AS cnt
            FROM actor_movies
            GROUP BY actor_id
        ),
        invalid_counts AS (
            SELECT actor_id, COUNT(*) AS bad
            FROM actor_movies am
            WHERE 1=1 {invalid_filter}
            GROUP BY actor_id
        ),
        dominant_industry AS (
            SELECT am.actor_id, m.industry,
                   ROW_NUMBER() OVER (
                       PARTITION BY am.actor_id
                       ORDER BY COUNT(*) DESC, m.industry
                   ) AS rn
            FROM actor_movies am
            JOIN movies m ON m.id = am.movie_id
            WHERE m.industry IS NOT NULL
            GROUP BY am.actor_id, m.industry
        ),
        has_collab AS (
            SELECT DISTINCT actor1_id AS actor_id FROM actor_collaborations
        )
        SELECT
            a.id,
            a.name,
            a.industry,
            a.tmdb_person_id,
            COALESCE(fc.cnt,  0) AS film_count,
            COALESCE(ic.bad,  0) AS invalid_count,
            di.industry          AS dominant_industry,
            (hc.actor_id IS NOT NULL) AS has_collab,

            -- Identity score
            CASE WHEN a.tmdb_person_id IS NOT NULL THEN 20 ELSE 0 END

            -- Film count score
            + CASE
                WHEN COALESCE(fc.cnt, 0) >= 5 THEN 20
                WHEN COALESCE(fc.cnt, 0) >= 3 THEN 15
                WHEN COALESCE(fc.cnt, 0) >= 2 THEN 10
                WHEN COALESCE(fc.cnt, 0) =  1 THEN  5
                ELSE 0
              END

            -- Invalid link deduction (max 30 pts, -10 per bad link)
            + GREATEST(0, 30 - COALESCE(ic.bad, 0) * 10)

            -- Industry consistency
            + CASE
                WHEN a.industry IS NOT NULL AND di.industry IS NOT NULL
                     AND a.industry = di.industry THEN 15
                ELSE 0
              END

            -- Has collaborations
            + CASE WHEN hc.actor_id IS NOT NULL THEN 15 ELSE 0 END

            AS score

        FROM actors a
        LEFT JOIN film_counts        fc ON fc.actor_id = a.id
        LEFT JOIN invalid_counts     ic ON ic.actor_id = a.id
        LEFT JOIN dominant_industry  di ON di.actor_id = a.id AND di.rn = 1
        LEFT JOIN has_collab         hc ON hc.actor_id = a.id
    """)

    rows = cur.fetchall()
    result = [{"id": r["id"], "score": float(r["score"])} for r in rows]
    avg = sum(r["score"] for r in result) / len(result) if result else 0.0
    return result, round(avg, 2)


# ── Movie scoring ─────────────────────────────────────────────────────────────

def _score_movies(cur) -> tuple[list[dict], float]:
    """
    Compute score for every movie in a single SQL pass.
    """
    cur.execute("""
        WITH actor_counts AS (
            SELECT movie_id, COUNT(*) AS cnt
            FROM actor_movies
            GROUP BY movie_id
        )
        SELECT
            m.id,
            m.title,

            -- TMDB ID
            CASE WHEN m.tmdb_id IS NOT NULL THEN 20 ELSE 0 END

            -- Poster
            + CASE WHEN m.poster_url IS NOT NULL THEN 15 ELSE 0 END

            -- Runtime
            + CASE WHEN m.runtime IS NOT NULL AND m.runtime > 0 THEN 15 ELSE 0 END

            -- Vote average
            + CASE WHEN m.vote_average IS NOT NULL THEN 15 ELSE 0 END

            -- Director
            + CASE WHEN m.director IS NOT NULL AND m.director != '' THEN 15 ELSE 0 END

            -- Actor count tier
            + CASE
                WHEN COALESCE(ac.cnt, 0) >= 5 THEN 20
                WHEN COALESCE(ac.cnt, 0) >= 2 THEN 15
                WHEN COALESCE(ac.cnt, 0) =  1 THEN  8
                ELSE 0
              END

            AS score

        FROM movies m
        LEFT JOIN actor_counts ac ON ac.movie_id = m.id
    """)

    rows = cur.fetchall()
    result = [{"id": r["id"], "score": float(r["score"])} for r in rows]
    avg = sum(r["score"] for r in result) / len(result) if result else 0.0
    return result, round(avg, 2)


# ── Persist scores ────────────────────────────────────────────────────────────

def _persist_actor_scores(conn, rows: list[dict]) -> None:
    with conn.cursor() as cur:
        if not _col_exists(cur, "actors", "data_confidence_score"):
            log.warning("actors.data_confidence_score column missing — skipping persist")
            return
    psycopg2.extras.execute_batch(
        conn.cursor(),
        "UPDATE actors SET data_confidence_score = %(score)s WHERE id = %(id)s",
        rows,
        page_size=500,
    )
    conn.commit()
    log.info("  Persisted scores for %d actors.", len(rows))


def _persist_movie_scores(conn, rows: list[dict]) -> None:
    with conn.cursor() as cur:
        if not _col_exists(cur, "movies", "data_confidence_score"):
            log.warning("movies.data_confidence_score column missing — skipping persist")
            return
    psycopg2.extras.execute_batch(
        conn.cursor(),
        "UPDATE movies SET data_confidence_score = %(score)s WHERE id = %(id)s",
        rows,
        page_size=500,
    )
    conn.commit()
    log.info("  Persisted scores for %d movies.", len(rows))


def _persist_system_health(conn, snap: SystemHealthSnapshot) -> None:
    with conn.cursor() as cur:
        if not _table_exists(cur, "system_health"):
            log.warning("system_health table missing — run migration 0002 first")
            return
        cur.execute("""
            INSERT INTO system_health (
                id, data_confidence_score, avg_actor_score, avg_movie_score,
                collab_integrity, ghost_collab_count, duplicate_count,
                invalid_link_count, total_actors, total_movies,
                total_collab_pairs, validation_passed, sources_used,
                last_scored_at
            ) VALUES (
                1, %(sys)s, %(actor)s, %(movie)s,
                %(collab)s, %(ghosts)s, %(dups)s,
                %(invalid)s, %(ta)s, %(tm)s,
                %(tcp)s, %(vp)s, %(src)s,
                %(now)s
            )
            ON CONFLICT (id) DO UPDATE SET
                data_confidence_score = EXCLUDED.data_confidence_score,
                avg_actor_score       = EXCLUDED.avg_actor_score,
                avg_movie_score       = EXCLUDED.avg_movie_score,
                collab_integrity      = EXCLUDED.collab_integrity,
                ghost_collab_count    = EXCLUDED.ghost_collab_count,
                duplicate_count       = EXCLUDED.duplicate_count,
                invalid_link_count    = EXCLUDED.invalid_link_count,
                total_actors          = EXCLUDED.total_actors,
                total_movies          = EXCLUDED.total_movies,
                total_collab_pairs    = EXCLUDED.total_collab_pairs,
                validation_passed     = EXCLUDED.validation_passed,
                sources_used          = EXCLUDED.sources_used,
                last_scored_at        = EXCLUDED.last_scored_at
        """, {
            "sys":     snap.system_score,
            "actor":   snap.avg_actor_score,
            "movie":   snap.avg_movie_score,
            "collab":  snap.collab_integrity,
            "ghosts":  snap.ghost_collab_count,
            "dups":    snap.duplicate_count,
            "invalid": snap.invalid_link_count,
            "ta":      snap.total_actors,
            "tm":      snap.total_movies,
            "tcp":     snap.total_collab_pairs,
            "vp":      snap.validation_passed,
            "src":     snap.sources_used,
            "now":     snap.computed_at,
        })
    conn.commit()
    log.info("  system_health updated.")


def _log_run(conn, snap: SystemHealthSnapshot, run_id: str) -> None:
    with conn.cursor() as cur:
        if not _table_exists(cur, "data_fix_log"):
            return
        cur.execute("""
            INSERT INTO data_fix_log (action, entity_type, entity_id, entity_label, reason, run_id)
            VALUES ('score_confidence', 'system', 1,
                    'system_health',
                    %s,
                    %s)
        """, (
            f"system_score={snap.system_score:.1f} actors={snap.avg_actor_score:.1f} "
            f"movies={snap.avg_movie_score:.1f} integrity={snap.collab_integrity:.1f} "
            f"ghosts={snap.ghost_collab_count} dups={snap.duplicate_count}",
            run_id,
        ))
    conn.commit()


# ── Main entry ────────────────────────────────────────────────────────────────

def compute_and_persist() -> SystemHealthSnapshot:
    conn = _connect()
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    try:
        with conn.cursor() as cur:
            log.info("Scoring actors…")
            actor_rows, avg_actor = _score_actors(cur)

            log.info("Scoring movies…")
            movie_rows, avg_movie = _score_movies(cur)

            log.info("Computing system metrics…")
            ghost_count = _count_ghosts(cur)
            dup_count   = _count_same_industry_duplicates(cur)

            # Invalid links
            has_is_valid = _col_exists(cur, "actor_movies", "is_valid")
            if has_is_valid:
                cur.execute("SELECT COUNT(*) AS n FROM actor_movies WHERE is_valid = FALSE")
                invalid_count = cur.fetchone()["n"]
            else:
                invalid_count = 0

            cur.execute("SELECT COUNT(*) AS n FROM actors")
            total_actors = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) AS n FROM movies")
            total_movies = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) AS n FROM actor_collaborations WHERE actor1_id < actor2_id")
            total_pairs = cur.fetchone()["n"]

            # Detect sources
            cur.execute("SELECT COUNT(*) AS n FROM actors WHERE tmdb_person_id IS NOT NULL")
            tmdb_actors = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) AS n FROM movies WHERE tmdb_id IS NOT NULL")
            tmdb_movies = cur.fetchone()["n"]
            sources = []
            if tmdb_actors > 0 or tmdb_movies > 0:
                sources.append("TMDB")
            sources.append("Wikidata")
            sources.append("Derived")

        collab_integrity = max(0.0, 100.0 - ghost_count * 2)
        system_score = round(
            avg_actor * 0.35 + avg_movie * 0.35 + collab_integrity * 0.30,
            2,
        )
        validation_passed = ghost_count == 0 and dup_count == 0

        snap = SystemHealthSnapshot(
            system_score       = system_score,
            avg_actor_score    = avg_actor,
            avg_movie_score    = avg_movie,
            collab_integrity   = collab_integrity,
            ghost_collab_count = ghost_count,
            duplicate_count    = dup_count,
            invalid_link_count = invalid_count,
            total_actors       = total_actors,
            total_movies       = total_movies,
            total_collab_pairs = total_pairs,
            validation_passed  = validation_passed,
            sources_used       = sources,
            computed_at        = now,
        )

        log.info("Persisting actor scores…")
        _persist_actor_scores(conn, actor_rows)

        log.info("Persisting movie scores…")
        _persist_movie_scores(conn, movie_rows)

        log.info("Persisting system health…")
        _persist_system_health(conn, snap)

        _log_run(conn, snap, run_id)

        return snap

    finally:
        conn.close()


def main() -> None:
    snap = compute_and_persist()
    print()
    print("══════════════════════════════════════════════")
    print("  CONFIDENCE SCORES")
    print("══════════════════════════════════════════════")
    print(f"  System score     : {snap.system_score:.1f} / 100")
    print(f"  Avg actor score  : {snap.avg_actor_score:.1f} / 100")
    print(f"  Avg movie score  : {snap.avg_movie_score:.1f} / 100")
    print(f"  Collab integrity : {snap.collab_integrity:.1f} / 100")
    print(f"  Ghost collabs    : {snap.ghost_collab_count}")
    print(f"  Duplicates       : {snap.duplicate_count}")
    print(f"  Invalid links    : {snap.invalid_link_count}")
    print(f"  Total actors     : {snap.total_actors}")
    print(f"  Total movies     : {snap.total_movies}")
    print(f"  Collab pairs     : {snap.total_collab_pairs}")
    print(f"  Validation passed: {'✅' if snap.validation_passed else '❌'}")
    print(f"  Sources          : {', '.join(snap.sources_used)}")
    print(f"  Computed at      : {snap.computed_at.strftime('%Y-%m-%d %H:%M UTC')}")
    print("══════════════════════════════════════════════")
    print()


if __name__ == "__main__":
    main()
