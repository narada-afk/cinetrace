"""
validate_integrity.py
=====================
Production-grade data validation system for the South Cinema Analytics database.

Validates CORRECTNESS and INTEGRITY — not completeness (already handled).

Checks:
  1. Actor–Movie mapping validity (role anomalies + TMDB spot-check)
  2. Collaboration accuracy (recompute from scratch vs stored)
  3. Duplicate movie detection (exact + fuzzy)
  4. TMDB integrity (title / year cross-check)
  5. Industry consistency (actor vs movie industry)
  6. Orphan / suspicious records

Outputs:
  - Six validation tables written to PostgreSQL
  - Per-actor and per-movie confidence scores
  - Summary report with severity classification and final verdict

Usage:
  docker compose exec backend python -m data_pipeline.validate_integrity
  docker compose exec -e TMDB_API_KEY=xxx backend python -m data_pipeline.validate_integrity --tmdb-sample 300
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import os
import ssl
import sys
import time
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

# ── Bootstrap path so we can run as a module ─────────────────────────────────

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("validate_integrity")

# ── Constants ─────────────────────────────────────────────────────────────────

TMDB_BASE   = "https://api.themoviedb.org/3"
TMDB_DELAY  = 0.26          # seconds between API calls (~4 req/s)
FUZZY_THRESHOLD = 0.85      # SequenceMatcher ratio for duplicate detection
OUTLIER_COLLAB  = 50        # collaboration_count above this is flagged for review
INDUSTRY_DRIFT  = 0.35      # actor flagged if >35% of movies are in a different industry
BILLING_ANOMALY = 5         # 'primary' role but billing_order > this is suspicious

# Severity weights for final confidence score (must sum to 1.0)
SEVERITY_WEIGHTS = {
    "actor_movie_links":      0.25,
    "collaborations":         0.20,
    "duplicates":             0.20,
    "tmdb_integrity":         0.15,
    "industry_consistency":   0.10,
    "orphans":                0.10,
}

# ── SSL context (same pattern used across the pipeline) ───────────────────────

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode    = ssl.CERT_NONE

# ── TMDB helpers ──────────────────────────────────────────────────────────────

_tmdb_cache: dict[str, dict] = {}


def _tmdb_get(path: str, api_key: str) -> Optional[dict]:
    """GET a TMDB endpoint; returns parsed JSON or None on error. Results cached in-process."""
    if path in _tmdb_cache:
        return _tmdb_cache[path]
    url = f"{TMDB_BASE}{path}?api_key={api_key}"
    req = urllib.request.Request(url, headers={"User-Agent": "SCA-Validator/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10, context=_ssl_ctx) as r:
            data = json.loads(r.read())
            _tmdb_cache[path] = data
            return data
    except Exception as exc:
        logger.debug("TMDB %s failed: %s", path, exc)
        return None
    finally:
        time.sleep(TMDB_DELAY)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _connect() -> psycopg2.extensions.connection:
    url = os.environ.get("DATABASE_URL", "postgresql://sca:sca@postgres:5432/sca")
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)


def _setup_output_tables(conn) -> None:
    """Create validation result tables if they don't exist, or truncate for a fresh run."""
    ddl = """
        CREATE TABLE IF NOT EXISTS validation_actor_movie_links (
            run_at          TIMESTAMPTZ DEFAULT NOW(),
            actor_id        INTEGER,
            actor_name      TEXT,
            movie_id        INTEGER,
            movie_title     TEXT,
            issue           TEXT,
            severity        TEXT
        );

        CREATE TABLE IF NOT EXISTS validation_collaboration_mismatches (
            run_at              TIMESTAMPTZ DEFAULT NOW(),
            actor1_id           INTEGER,
            actor1_name         TEXT,
            actor2_id           INTEGER,
            actor2_name         TEXT,
            stored_count        INTEGER,
            computed_count      INTEGER,
            delta               INTEGER,
            issue               TEXT,
            severity            TEXT
        );

        CREATE TABLE IF NOT EXISTS validation_duplicate_movies (
            run_at          TIMESTAMPTZ DEFAULT NOW(),
            movie_id_a      INTEGER,
            title_a         TEXT,
            year_a          INTEGER,
            movie_id_b      INTEGER,
            title_b         TEXT,
            year_b          INTEGER,
            match_type      TEXT,
            similarity      REAL,
            severity        TEXT
        );

        CREATE TABLE IF NOT EXISTS validation_tmdb_mismatches (
            run_at          TIMESTAMPTZ DEFAULT NOW(),
            movie_id        INTEGER,
            db_title        TEXT,
            db_year         INTEGER,
            tmdb_title      TEXT,
            tmdb_year       INTEGER,
            issue           TEXT,
            severity        TEXT
        );

        CREATE TABLE IF NOT EXISTS validation_industry_anomalies (
            run_at              TIMESTAMPTZ DEFAULT NOW(),
            actor_id            INTEGER,
            actor_name          TEXT,
            stated_industry     TEXT,
            dominant_industry   TEXT,
            movie_count         INTEGER,
            cross_industry_pct  REAL,
            issue               TEXT,
            severity            TEXT
        );

        CREATE TABLE IF NOT EXISTS validation_orphan_records (
            run_at          TIMESTAMPTZ DEFAULT NOW(),
            record_type     TEXT,
            record_id       INTEGER,
            record_label    TEXT,
            issue           TEXT,
            severity        TEXT
        );

        CREATE TABLE IF NOT EXISTS validation_confidence_scores (
            run_at          TIMESTAMPTZ DEFAULT NOW(),
            entity_type     TEXT,
            entity_id       INTEGER,
            entity_label    TEXT,
            score           REAL,
            flags           TEXT
        );

        -- Truncate so each run is a clean slate
        TRUNCATE validation_actor_movie_links;
        TRUNCATE validation_collaboration_mismatches;
        TRUNCATE validation_duplicate_movies;
        TRUNCATE validation_tmdb_mismatches;
        TRUNCATE validation_industry_anomalies;
        TRUNCATE validation_orphan_records;
        TRUNCATE validation_confidence_scores;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logger.info("Output tables ready.")


# ── Check 1: Actor–Movie mapping validity ─────────────────────────────────────

@dataclass
class ActorMovieResult:
    issues: list[dict] = field(default_factory=list)
    checked: int = 0
    tmdb_spot_checked: int = 0
    tmdb_mislinked: int = 0


def check_actor_movie_links(conn, api_key: Optional[str], tmdb_sample: int) -> ActorMovieResult:
    """
    Two sub-checks:
      A) SQL: billing_order anomalies (primary role but high billing order)
      B) TMDB spot-check: for a sample of movies, verify actor appears in TMDB cast
    """
    result = ActorMovieResult()
    issues = []

    with conn.cursor() as cur:
        # ── A: Billing order anomaly ──────────────────────────────────────────
        logger.info("Check 1A: billing order anomalies…")
        cur.execute("""
            SELECT
                am.actor_id,
                a.name        AS actor_name,
                am.movie_id,
                m.title       AS movie_title,
                am.role_type,
                am.billing_order
            FROM actor_movies am
            JOIN actors a ON a.id = am.actor_id
            JOIN movies  m ON m.id = am.movie_id
            WHERE am.role_type = 'primary'
              AND am.billing_order IS NOT NULL
              AND am.billing_order > %s
            ORDER BY am.billing_order DESC
        """, (BILLING_ANOMALY,))
        rows = cur.fetchall()
        result.checked += cur.rowcount if cur.rowcount >= 0 else 0

        for r in rows:
            issues.append({
                "actor_id":    r["actor_id"],
                "actor_name":  r["actor_name"],
                "movie_id":    r["movie_id"],
                "movie_title": r["movie_title"],
                "issue":       f"primary role but billing_order={r['billing_order']} (threshold {BILLING_ANOMALY})",
                "severity":    "warning",
            })

        logger.info("  %d billing anomalies found.", len(rows))

        # ── B: TMDB spot-check ────────────────────────────────────────────────
        if not api_key:
            logger.info("Check 1B: TMDB_API_KEY not set — skipping TMDB spot-check.")
        else:
            logger.info("Check 1B: TMDB cast spot-check (sample=%d)…", tmdb_sample)

            # Pick movies that have tmdb_id + at least one actor with tmdb_person_id
            cur.execute("""
                SELECT DISTINCT
                    m.id          AS movie_id,
                    m.title,
                    m.tmdb_id
                FROM movies m
                JOIN actor_movies am ON am.movie_id = m.id
                JOIN actors a        ON a.id = am.actor_id
                WHERE m.tmdb_id IS NOT NULL
                  AND a.tmdb_person_id IS NOT NULL
                ORDER BY m.id
                LIMIT %s
            """, (tmdb_sample,))
            sample_movies = cur.fetchall()

            for movie in sample_movies:
                data = _tmdb_get(f"/movie/{movie['tmdb_id']}/credits", api_key)
                if not data:
                    continue

                result.tmdb_spot_checked += 1
                cast_ids = {c["id"] for c in data.get("cast", [])}

                # Get all actors linked to this movie who have tmdb_person_id
                cur.execute("""
                    SELECT a.id AS actor_id, a.name, a.tmdb_person_id, am.role_type
                    FROM actor_movies am
                    JOIN actors a ON a.id = am.actor_id
                    WHERE am.movie_id = %s AND a.tmdb_person_id IS NOT NULL
                """, (movie["movie_id"],))
                linked = cur.fetchall()

                for actor in linked:
                    if actor["tmdb_person_id"] not in cast_ids:
                        result.tmdb_mislinked += 1
                        severity = "critical" if actor["role_type"] == "primary" else "warning"
                        issues.append({
                            "actor_id":    actor["actor_id"],
                            "actor_name":  actor["name"],
                            "movie_id":    movie["movie_id"],
                            "movie_title": movie["title"],
                            "issue":       f"actor (tmdb_person_id={actor['tmdb_person_id']}) not found in TMDB cast for movie tmdb_id={movie['tmdb_id']}",
                            "severity":    severity,
                        })

            logger.info(
                "  Spot-checked %d movies; %d actor-movie links not in TMDB cast.",
                result.tmdb_spot_checked, result.tmdb_mislinked,
            )

    # Write to DB
    if issues:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO validation_actor_movie_links
                    (actor_id, actor_name, movie_id, movie_title, issue, severity)
                VALUES (%(actor_id)s, %(actor_name)s, %(movie_id)s, %(movie_title)s,
                        %(issue)s, %(severity)s)
            """, issues)
        conn.commit()

    result.issues = issues
    return result


# ── Check 2: Collaboration accuracy ──────────────────────────────────────────

@dataclass
class CollaborationResult:
    issues: list[dict] = field(default_factory=list)
    pairs_checked: int = 0
    missing_pairs: int = 0
    ghost_pairs: int = 0
    count_mismatches: int = 0
    outliers: int = 0


def check_collaborations(conn) -> CollaborationResult:
    """
    Recompute collaborations from actor_movies and compare against stored counts.
    Detects: wrong counts, ghost collaborations, missing collaborations, extreme outliers.
    """
    result = CollaborationResult()
    issues = []

    logger.info("Check 2: Recomputing collaborations from actor_movies…")

    with conn.cursor() as cur:
        # Recompute: count shared movies for each unique pair (normalised: a1 < a2)
        cur.execute("""
            SELECT
                LEAST(am1.actor_id, am2.actor_id)    AS actor1_id,
                GREATEST(am1.actor_id, am2.actor_id) AS actor2_id,
                COUNT(DISTINCT am1.movie_id)          AS computed_count
            FROM actor_movies am1
            JOIN actor_movies am2 ON am1.movie_id = am2.movie_id
                                 AND am1.actor_id  < am2.actor_id
            GROUP BY 1, 2
        """)
        computed_pairs = {(r["actor1_id"], r["actor2_id"]): r["computed_count"]
                          for r in cur.fetchall()}

        # Load stored (normalised to a1 < a2)
        cur.execute("""
            SELECT
                LEAST(actor1_id, actor2_id)    AS actor1_id,
                GREATEST(actor1_id, actor2_id) AS actor2_id,
                MAX(collaboration_count)        AS stored_count
            FROM actor_collaborations
            GROUP BY 1, 2
        """)
        stored_pairs = {(r["actor1_id"], r["actor2_id"]): r["stored_count"]
                        for r in cur.fetchall()}

        result.pairs_checked = len(computed_pairs)

        # Build actor name lookup
        cur.execute("SELECT id, name FROM actors")
        actor_names = {r["id"]: r["name"] for r in cur.fetchall()}

        # Ghost pairs: stored but not in computed (shouldn't exist)
        for (a1, a2), stored in stored_pairs.items():
            if (a1, a2) not in computed_pairs:
                result.ghost_pairs += 1
                issues.append({
                    "actor1_id":      a1,
                    "actor1_name":    actor_names.get(a1, "?"),
                    "actor2_id":      a2,
                    "actor2_name":    actor_names.get(a2, "?"),
                    "stored_count":   stored,
                    "computed_count": 0,
                    "delta":          -stored,
                    "issue":          "stored collaboration has no shared movies in actor_movies (ghost)",
                    "severity":       "critical",
                })

        # Mismatches + outliers
        for (a1, a2), computed in computed_pairs.items():
            stored = stored_pairs.get((a1, a2))

            if stored is None:
                result.missing_pairs += 1
                issues.append({
                    "actor1_id":      a1,
                    "actor1_name":    actor_names.get(a1, "?"),
                    "actor2_id":      a2,
                    "actor2_name":    actor_names.get(a2, "?"),
                    "stored_count":   0,
                    "computed_count": computed,
                    "delta":          computed,
                    "issue":          f"collaboration exists in actor_movies ({computed} films) but missing from actor_collaborations",
                    "severity":       "warning",
                })
            elif stored != computed:
                result.count_mismatches += 1
                severity = "critical" if abs(stored - computed) > 5 else "warning"
                issues.append({
                    "actor1_id":      a1,
                    "actor1_name":    actor_names.get(a1, "?"),
                    "actor2_id":      a2,
                    "actor2_name":    actor_names.get(a2, "?"),
                    "stored_count":   stored,
                    "computed_count": computed,
                    "delta":          stored - computed,
                    "issue":          f"count mismatch: stored={stored} computed={computed}",
                    "severity":       severity,
                })

            if computed > OUTLIER_COLLAB:
                result.outliers += 1
                issues.append({
                    "actor1_id":      a1,
                    "actor1_name":    actor_names.get(a1, "?"),
                    "actor2_id":      a2,
                    "actor2_name":    actor_names.get(a2, "?"),
                    "stored_count":   stored or 0,
                    "computed_count": computed,
                    "delta":          0,
                    "issue":          f"outlier: {computed} shared films (>{OUTLIER_COLLAB}) — verify intentional",
                    "severity":       "warning",
                })

    logger.info(
        "  Pairs checked: %d | ghost: %d | missing: %d | mismatches: %d | outliers: %d",
        result.pairs_checked, result.ghost_pairs, result.missing_pairs,
        result.count_mismatches, result.outliers,
    )

    if issues:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO validation_collaboration_mismatches
                    (actor1_id, actor1_name, actor2_id, actor2_name,
                     stored_count, computed_count, delta, issue, severity)
                VALUES (%(actor1_id)s, %(actor1_name)s, %(actor2_id)s, %(actor2_name)s,
                        %(stored_count)s, %(computed_count)s, %(delta)s,
                        %(issue)s, %(severity)s)
            """, issues)
        conn.commit()

    result.issues = issues
    return result


# ── Check 3: Duplicate movie detection ───────────────────────────────────────

@dataclass
class DuplicateResult:
    issues: list[dict] = field(default_factory=list)
    exact_pairs: int = 0
    fuzzy_pairs: int = 0
    same_tmdb: int = 0


def check_duplicates(conn) -> DuplicateResult:
    """
    Detect duplicate movies via:
      A) Exact: same title + same release_year
      B) Same tmdb_id on multiple rows
      C) Fuzzy: SequenceMatcher ratio >= FUZZY_THRESHOLD within same year
    """
    result = DuplicateResult()
    issues = []

    with conn.cursor() as cur:
        # ── A: Exact duplicates ───────────────────────────────────────────────
        logger.info("Check 3A: exact duplicate detection…")
        cur.execute("""
            SELECT
                m1.id    AS id_a, m1.title AS title_a, m1.release_year AS year_a,
                m2.id    AS id_b, m2.title AS title_b, m2.release_year AS year_b
            FROM movies m1
            JOIN movies m2 ON m1.id < m2.id
                           AND lower(trim(m1.title)) = lower(trim(m2.title))
                           AND m1.release_year = m2.release_year
            ORDER BY m1.title, m1.release_year
        """)
        for r in cur.fetchall():
            result.exact_pairs += 1
            issues.append({
                "movie_id_a": r["id_a"],   "title_a": r["title_a"], "year_a": r["year_a"],
                "movie_id_b": r["id_b"],   "title_b": r["title_b"], "year_b": r["year_b"],
                "match_type": "exact",     "similarity": 1.0,
                "severity":   "critical",
            })
        logger.info("  %d exact duplicate pairs.", result.exact_pairs)

        # ── B: Same TMDB ID on multiple movies ────────────────────────────────
        logger.info("Check 3B: duplicate tmdb_id detection…")
        cur.execute("""
            SELECT tmdb_id, array_agg(id ORDER BY id) AS ids,
                   array_agg(title ORDER BY id)       AS titles
            FROM movies
            WHERE tmdb_id IS NOT NULL
            GROUP BY tmdb_id
            HAVING COUNT(*) > 1
        """)
        for r in cur.fetchall():
            ids    = r["ids"]
            titles = r["titles"]
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    result.same_tmdb += 1
                    issues.append({
                        "movie_id_a": ids[i],    "title_a": titles[i], "year_a": None,
                        "movie_id_b": ids[j],    "title_b": titles[j], "year_b": None,
                        "match_type": "same_tmdb_id",
                        "similarity": 1.0,
                        "severity":   "critical",
                    })
        logger.info("  %d same-TMDB-ID pairs.", result.same_tmdb)

        # ── C: Fuzzy match within same year ───────────────────────────────────
        logger.info("Check 3C: fuzzy duplicate detection…")
        cur.execute("""
            SELECT id, title, release_year
            FROM movies
            WHERE title IS NOT NULL AND release_year IS NOT NULL
            ORDER BY release_year, title
        """)
        movies = cur.fetchall()

    # Group by year, compare titles within each group
    by_year: dict[int, list] = defaultdict(list)
    for m in movies:
        if m["release_year"]:
            by_year[m["release_year"]].append(m)

    seen_fuzzy = set()
    for year, group in by_year.items():
        for i, ma in enumerate(group):
            for mb in group[i + 1:]:
                key = (min(ma["id"], mb["id"]), max(ma["id"], mb["id"]))
                if key in seen_fuzzy:
                    continue
                ratio = difflib.SequenceMatcher(
                    None,
                    ma["title"].lower().strip(),
                    mb["title"].lower().strip(),
                ).ratio()
                if ratio >= FUZZY_THRESHOLD and ratio < 1.0:   # 1.0 already caught by exact
                    seen_fuzzy.add(key)
                    result.fuzzy_pairs += 1
                    issues.append({
                        "movie_id_a": ma["id"],   "title_a": ma["title"], "year_a": year,
                        "movie_id_b": mb["id"],   "title_b": mb["title"], "year_b": year,
                        "match_type": "fuzzy",
                        "similarity": round(ratio, 3),
                        "severity":   "warning",
                    })

    logger.info("  %d fuzzy duplicate pairs (threshold=%.2f).", result.fuzzy_pairs, FUZZY_THRESHOLD)

    if issues:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO validation_duplicate_movies
                    (movie_id_a, title_a, year_a, movie_id_b, title_b, year_b,
                     match_type, similarity, severity)
                VALUES (%(movie_id_a)s, %(title_a)s, %(year_a)s,
                        %(movie_id_b)s, %(title_b)s, %(year_b)s,
                        %(match_type)s, %(similarity)s, %(severity)s)
            """, issues)
        conn.commit()

    result.issues = issues
    return result


# ── Check 4: TMDB integrity (title / year cross-check) ───────────────────────

@dataclass
class TmdbIntegrityResult:
    issues: list[dict] = field(default_factory=list)
    checked: int = 0
    title_mismatches: int = 0
    year_mismatches: int = 0


def check_tmdb_integrity(conn, api_key: Optional[str], tmdb_sample: int) -> TmdbIntegrityResult:
    """
    For a sample of movies with tmdb_id, fetch /movie/{id} and compare
    title and release_year against our DB values.
    """
    result = TmdbIntegrityResult()

    if not api_key:
        logger.info("Check 4: TMDB_API_KEY not set — skipping TMDB integrity check.")
        return result

    logger.info("Check 4: TMDB title/year integrity (sample=%d)…", tmdb_sample)
    issues = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, title, release_year, tmdb_id
            FROM movies
            WHERE tmdb_id IS NOT NULL
            ORDER BY RANDOM()
            LIMIT %s
        """, (tmdb_sample,))
        sample = cur.fetchall()

    for movie in sample:
        # Reuse cache if already fetched during Check 1
        data = _tmdb_cache.get(f"/movie/{movie['tmdb_id']}")
        if data is None:
            data = _tmdb_get(f"/movie/{movie['tmdb_id']}", api_key)
        if data is None:
            continue

        result.checked += 1
        tmdb_title = data.get("title") or data.get("original_title") or ""
        tmdb_year_raw = (data.get("release_date") or "")[:4]
        tmdb_year = int(tmdb_year_raw) if tmdb_year_raw.isdigit() else None

        db_title = movie["title"] or ""
        db_year  = movie["release_year"]

        title_ratio = difflib.SequenceMatcher(
            None, db_title.lower().strip(), tmdb_title.lower().strip()
        ).ratio()

        if title_ratio < 0.60:
            result.title_mismatches += 1
            severity = "critical" if title_ratio < 0.40 else "warning"
            issues.append({
                "movie_id":   movie["id"],
                "db_title":   db_title,
                "db_year":    db_year,
                "tmdb_title": tmdb_title,
                "tmdb_year":  tmdb_year,
                "issue":      f"title mismatch: similarity={title_ratio:.2f}",
                "severity":   severity,
            })
        elif tmdb_year and db_year and abs(tmdb_year - db_year) > 1:
            result.year_mismatches += 1
            severity = "critical" if abs(tmdb_year - db_year) > 3 else "warning"
            issues.append({
                "movie_id":   movie["id"],
                "db_title":   db_title,
                "db_year":    db_year,
                "tmdb_title": tmdb_title,
                "tmdb_year":  tmdb_year,
                "issue":      f"year mismatch: db={db_year} tmdb={tmdb_year}",
                "severity":   severity,
            })

    logger.info(
        "  Checked %d; title mismatches: %d; year mismatches: %d",
        result.checked, result.title_mismatches, result.year_mismatches,
    )

    if issues:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO validation_tmdb_mismatches
                    (movie_id, db_title, db_year, tmdb_title, tmdb_year, issue, severity)
                VALUES (%(movie_id)s, %(db_title)s, %(db_year)s,
                        %(tmdb_title)s, %(tmdb_year)s, %(issue)s, %(severity)s)
            """, issues)
        conn.commit()

    result.issues = issues
    return result


# ── Check 5: Industry consistency ─────────────────────────────────────────────

@dataclass
class IndustryResult:
    issues: list[dict] = field(default_factory=list)
    actors_checked: int = 0
    drifted: int = 0


def check_industry_consistency(conn) -> IndustryResult:
    """
    For each actor, find the dominant industry from their movies.
    Flag actors where:
      - stated industry != dominant industry, AND
      - cross-industry ratio > INDUSTRY_DRIFT threshold
    """
    result = IndustryResult()
    issues = []

    logger.info("Check 5: Industry consistency…")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                a.id            AS actor_id,
                a.name          AS actor_name,
                a.industry      AS stated_industry,
                m.industry      AS movie_industry,
                COUNT(*)        AS cnt
            FROM actors a
            JOIN actor_movies am ON am.actor_id = a.id
            JOIN movies m        ON m.id = am.movie_id
            WHERE m.industry IS NOT NULL
            GROUP BY a.id, a.name, a.industry, m.industry
        """)
        rows = cur.fetchall()

    # Aggregate per actor
    by_actor: dict[int, dict] = {}
    for r in rows:
        aid = r["actor_id"]
        if aid not in by_actor:
            by_actor[aid] = {
                "actor_id":        aid,
                "actor_name":      r["actor_name"],
                "stated_industry": r["stated_industry"],
                "industry_counts": defaultdict(int),
            }
        by_actor[aid]["industry_counts"][r["movie_industry"]] += r["cnt"]

    result.actors_checked = len(by_actor)

    for aid, info in by_actor.items():
        counts = info["industry_counts"]
        total  = sum(counts.values())
        if total == 0:
            continue

        dominant = max(counts, key=counts.get)
        dominant_pct = counts[dominant] / total

        stated = info["stated_industry"]
        # Cross-industry ratio = fraction of movies NOT in dominant industry
        cross_pct = 1.0 - dominant_pct

        if cross_pct > INDUSTRY_DRIFT and stated and stated != dominant:
            result.drifted += 1
            severity = "critical" if cross_pct > 0.60 else "warning"
            issues.append({
                "actor_id":           aid,
                "actor_name":         info["actor_name"],
                "stated_industry":    stated,
                "dominant_industry":  dominant,
                "movie_count":        total,
                "cross_industry_pct": round(cross_pct * 100, 1),
                "issue": (
                    f"stated={stated!r} but dominant={dominant!r} "
                    f"({cross_pct*100:.0f}% of {total} movies are cross-industry)"
                ),
                "severity": severity,
            })

    logger.info("  Actors checked: %d | industry drift flags: %d", result.actors_checked, result.drifted)

    if issues:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO validation_industry_anomalies
                    (actor_id, actor_name, stated_industry, dominant_industry,
                     movie_count, cross_industry_pct, issue, severity)
                VALUES (%(actor_id)s, %(actor_name)s, %(stated_industry)s,
                        %(dominant_industry)s, %(movie_count)s, %(cross_industry_pct)s,
                        %(issue)s, %(severity)s)
            """, issues)
        conn.commit()

    result.issues = issues
    return result


# ── Check 6: Orphan / suspicious records ─────────────────────────────────────

@dataclass
class OrphanResult:
    issues: list[dict] = field(default_factory=list)
    movies_no_actors: int = 0
    actors_one_film: int = 0
    ghost_collabs: int = 0


def check_orphans(conn) -> OrphanResult:
    """
    Detect:
      - Movies with zero actors in actor_movies
      - Actors with only 1 film (possible ingestion artefact)
      - Collaborations whose combined shared-movie evidence doesn't add up
    """
    result = OrphanResult()
    issues = []

    with conn.cursor() as cur:
        # ── Movies with no actors ─────────────────────────────────────────────
        logger.info("Check 6A: movies with no actor links…")
        cur.execute("""
            SELECT m.id, m.title, m.release_year, m.industry
            FROM movies m
            WHERE NOT EXISTS (
                SELECT 1 FROM actor_movies am WHERE am.movie_id = m.id
            )
        """)
        for r in cur.fetchall():
            result.movies_no_actors += 1
            issues.append({
                "record_type":  "movie",
                "record_id":    r["id"],
                "record_label": f"{r['title']} ({r['release_year']}) [{r['industry']}]",
                "issue":        "movie has no actor_movies entries",
                "severity":     "warning",
            })
        logger.info("  %d movies with no actors.", result.movies_no_actors)

        # ── Actors with only 1 film ───────────────────────────────────────────
        logger.info("Check 6B: actors with only one film…")
        cur.execute("""
            SELECT a.id, a.name, a.industry, COUNT(am.movie_id) AS film_count
            FROM actors a
            JOIN actor_movies am ON am.actor_id = a.id
            GROUP BY a.id, a.name, a.industry
            HAVING COUNT(am.movie_id) = 1
        """)
        for r in cur.fetchall():
            result.actors_one_film += 1
            issues.append({
                "record_type":  "actor",
                "record_id":    r["id"],
                "record_label": f"{r['name']} [{r['industry']}]",
                "issue":        "actor has only 1 film — possible incomplete ingestion",
                "severity":     "acceptable",
            })
        logger.info("  %d actors with only 1 film.", result.actors_one_film)

        # ── Collaborations referencing non-existent actors ────────────────────
        logger.info("Check 6C: collaborations with missing actors…")
        cur.execute("""
            SELECT DISTINCT ac.actor1_id, ac.actor2_id
            FROM actor_collaborations ac
            WHERE NOT EXISTS (SELECT 1 FROM actors a WHERE a.id = ac.actor1_id)
               OR NOT EXISTS (SELECT 1 FROM actors a WHERE a.id = ac.actor2_id)
        """)
        for r in cur.fetchall():
            result.ghost_collabs += 1
            issues.append({
                "record_type":  "collaboration",
                "record_id":    r["actor1_id"],
                "record_label": f"actor1_id={r['actor1_id']} ↔ actor2_id={r['actor2_id']}",
                "issue":        "collaboration references actor not in actors table",
                "severity":     "critical",
            })
        logger.info("  %d collaborations with missing actors.", result.ghost_collabs)

    if issues:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO validation_orphan_records
                    (record_type, record_id, record_label, issue, severity)
                VALUES (%(record_type)s, %(record_id)s, %(record_label)s,
                        %(issue)s, %(severity)s)
            """, issues)
        conn.commit()

    result.issues = issues
    return result


# ── Anomaly scoring ───────────────────────────────────────────────────────────

def compute_confidence_scores(conn,
                               link_res: ActorMovieResult,
                               collab_res: CollaborationResult,
                               dup_res: DuplicateResult,
                               tmdb_res: TmdbIntegrityResult,
                               ind_res: IndustryResult,
                               orphan_res: OrphanResult) -> None:
    """
    Assign a 0–100 confidence score to each actor and each movie based on
    which validation checks they failed and severity.
    """
    logger.info("Computing per-entity confidence scores…")

    deductions: dict[tuple[str, int], list[tuple[float, str]]] = defaultdict(list)

    # Actor-movie link issues → deduct from both actor and movie
    for issue in link_res.issues:
        sev = issue["severity"]
        amt = 25.0 if sev == "critical" else 10.0
        deductions[("actor", issue["actor_id"])].append((amt, issue["issue"]))
        deductions[("movie", issue["movie_id"])].append((amt, issue["issue"]))

    # Collaboration mismatches → deduct from both actors
    for issue in collab_res.issues:
        if issue["issue"].startswith("outlier"):
            amt = 5.0
        elif issue["severity"] == "critical":
            amt = 20.0
        else:
            amt = 8.0
        deductions[("actor", issue["actor1_id"])].append((amt, issue["issue"]))
        deductions[("actor", issue["actor2_id"])].append((amt, issue["issue"]))

    # Duplicate movies → deduct from both movies
    for issue in dup_res.issues:
        amt = 30.0 if issue["severity"] == "critical" else 12.0
        flag = f"duplicate ({issue['match_type']}, similarity={issue['similarity']})"
        deductions[("movie", issue["movie_id_a"])].append((amt, flag))
        deductions[("movie", issue["movie_id_b"])].append((amt, flag))

    # TMDB mismatches → deduct from movie
    for issue in tmdb_res.issues:
        amt = 25.0 if issue["severity"] == "critical" else 10.0
        deductions[("movie", issue["movie_id"])].append((amt, issue["issue"]))

    # Industry anomalies → deduct from actor
    for issue in ind_res.issues:
        amt = 20.0 if issue["severity"] == "critical" else 8.0
        deductions[("actor", issue["actor_id"])].append((amt, issue["issue"]))

    # Orphan records
    for issue in orphan_res.issues:
        etype = issue["record_type"]
        if etype not in ("actor", "movie"):
            continue
        amt = 15.0 if issue["severity"] == "critical" else 5.0
        deductions[(etype, issue["record_id"])].append((amt, issue["issue"]))

    # Build score rows
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM actors")
        actors = {r["id"]: r["name"] for r in cur.fetchall()}
        cur.execute("SELECT id, title FROM movies")
        movies = {r["id"]: r["title"] for r in cur.fetchall()}

    rows = []

    for aid, name in actors.items():
        deduct_list = deductions.get(("actor", aid), [])
        total_deduct = min(sum(d for d, _ in deduct_list), 100.0)
        score = round(100.0 - total_deduct, 1)
        flags = "; ".join(f[:80] for _, f in deduct_list) if deduct_list else ""
        rows.append({
            "entity_type":  "actor",
            "entity_id":    aid,
            "entity_label": name,
            "score":        score,
            "flags":        flags[:500],
        })

    for mid, title in movies.items():
        deduct_list = deductions.get(("movie", mid), [])
        total_deduct = min(sum(d for d, _ in deduct_list), 100.0)
        score = round(100.0 - total_deduct, 1)
        flags = "; ".join(f[:80] for _, f in deduct_list) if deduct_list else ""
        rows.append({
            "entity_type":  "movie",
            "entity_id":    mid,
            "entity_label": title,
            "score":        score,
            "flags":        flags[:500],
        })

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO validation_confidence_scores
                (entity_type, entity_id, entity_label, score, flags)
            VALUES (%(entity_type)s, %(entity_id)s, %(entity_label)s,
                    %(score)s, %(flags)s)
        """, rows, page_size=500)
    conn.commit()
    logger.info("  Scored %d entities.", len(rows))


# ── Final report ──────────────────────────────────────────────────────────────

def _severity_counts(issues: list[dict], key: str = "severity") -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for i in issues:
        counts[i.get(key, "unknown")] += 1
    return dict(counts)


def generate_report(conn,
                    link_res: ActorMovieResult,
                    collab_res: CollaborationResult,
                    dup_res: DuplicateResult,
                    tmdb_res: TmdbIntegrityResult,
                    ind_res: IndustryResult,
                    orphan_res: OrphanResult) -> None:
    """Print the final summary report and compute an overall confidence score."""

    SEP  = "═" * 60
    sep  = "─" * 60

    print(f"\n{SEP}")
    print("  SOUTH CINEMA ANALYTICS — DATA INTEGRITY REPORT")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(SEP)

    def _print_check(name: str, issues: list[dict], checked: int = 0):
        sevs  = _severity_counts(issues)
        total = len(issues)
        crit  = sevs.get("critical", 0)
        warn  = sevs.get("warning", 0)
        ok    = sevs.get("acceptable", 0)
        label = f"{'CRITICAL' if crit else ('WARNING' if warn else 'PASS')}"
        extra = f"  (checked {checked})" if checked else ""
        print(f"\n  {name}{extra}")
        print(f"    Issues total : {total}")
        print(f"    Critical     : {crit}")
        print(f"    Warning      : {warn}")
        print(f"    Acceptable   : {ok}")
        print(f"    Status       : {label}")

    _print_check("1. Actor–Movie Links",
                 link_res.issues,
                 link_res.tmdb_spot_checked)
    _print_check("2. Collaboration Accuracy",
                 [i for i in collab_res.issues if not i["issue"].startswith("outlier")])
    if collab_res.outliers:
        print(f"    Outlier pairs (>{OUTLIER_COLLAB} shared films): {collab_res.outliers}")
    _print_check("3. Duplicate Movies", dup_res.issues)
    print(f"    Exact: {dup_res.exact_pairs}  |  Same-TMDB-ID: {dup_res.same_tmdb}  |  Fuzzy: {dup_res.fuzzy_pairs}")
    _print_check("4. TMDB Integrity", tmdb_res.issues, tmdb_res.checked)
    _print_check("5. Industry Consistency", ind_res.issues)
    _print_check("6. Orphan Records", orphan_res.issues)

    # Overall confidence
    print(f"\n{sep}")
    print("  CONFIDENCE SCORES (from validation_confidence_scores table)")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                entity_type,
                ROUND(AVG(score)::numeric, 1)  AS avg_score,
                MIN(score)                     AS min_score,
                COUNT(*) FILTER (WHERE score < 70) AS low_confidence
            FROM validation_confidence_scores
            GROUP BY entity_type
        """)
        for r in cur.fetchall():
            print(f"    {r['entity_type']:<8}  avg={r['avg_score']}  "
                  f"min={r['min_score']}  low-confidence={r['low_confidence']}")

    # Overall score: weighted by check severity
    total_issues = (
        sum(1 for i in link_res.issues   if i["severity"] == "critical") * 3 +
        sum(1 for i in link_res.issues   if i["severity"] == "warning")  * 1 +
        sum(1 for i in collab_res.issues if i["severity"] == "critical") * 3 +
        sum(1 for i in collab_res.issues if i["severity"] == "warning")  * 1 +
        dup_res.exact_pairs   * 5 +
        dup_res.same_tmdb     * 5 +
        dup_res.fuzzy_pairs   * 1 +
        sum(1 for i in tmdb_res.issues   if i["severity"] == "critical") * 3 +
        sum(1 for i in ind_res.issues    if i["severity"] == "critical") * 3 +
        sum(1 for i in ind_res.issues    if i["severity"] == "warning")  * 1 +
        orphan_res.ghost_collabs * 5
    )

    # Each 10 weighted issues = -1% confidence
    raw_confidence = max(0.0, 100.0 - total_issues * 0.1)
    confidence = round(raw_confidence, 1)

    critical_total = (
        sum(1 for i in link_res.issues   if i["severity"] == "critical") +
        sum(1 for i in collab_res.issues if i["severity"] == "critical") +
        dup_res.exact_pairs + dup_res.same_tmdb +
        sum(1 for i in tmdb_res.issues   if i["severity"] == "critical") +
        sum(1 for i in ind_res.issues    if i["severity"] == "critical") +
        orphan_res.ghost_collabs
    )

    safe = critical_total == 0 and confidence >= 85.0

    print(f"\n{SEP}")
    print(f"  FINAL VERDICT")
    print(sep)
    print(f"  Critical issues  : {critical_total}")
    print(f"  Confidence score : {confidence}%")
    print(f"  Safe for prod    : {'✅  YES' if safe else '❌  NO'}")
    print(SEP)
    print()

    if not safe:
        print("  ACTION REQUIRED:")
        if critical_total:
            print(f"    • {critical_total} critical issue(s) must be resolved before production.")
        if confidence < 85.0:
            print(f"    • Confidence {confidence}% is below the 85% production threshold.")
        print()


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="South Cinema Analytics — data integrity validator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--tmdb-sample", type=int, default=200, metavar="N",
                   help="Number of movies to spot-check via TMDB API (default 200; 0 to skip)")
    p.add_argument("--skip-tmdb", action="store_true",
                   help="Skip all TMDB API calls entirely")
    p.add_argument("--report-file", metavar="PATH",
                   help="Write a JSON summary report to this path (for CI artifact upload)")
    return p.parse_args()


def _write_report_file(path: str,
                       link_res, collab_res, dup_res, tmdb_res, ind_res, orphan_res,
                       elapsed: float, critical: int) -> None:
    """Write a machine-readable JSON report for CI consumption."""
    import json
    from datetime import datetime, timezone

    report = {
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "elapsed_s":         round(elapsed, 1),
        "validation_passed": critical == 0,
        "critical_total":    critical,
        "checks": {
            "actor_movie_links": {
                "total_issues": len(link_res.issues),
                "critical":     sum(1 for i in link_res.issues if i["severity"] == "critical"),
                "warning":      sum(1 for i in link_res.issues if i["severity"] == "warning"),
                "tmdb_spot_checked": link_res.tmdb_spot_checked,
            },
            "collaborations": {
                "pairs_checked":   collab_res.pairs_checked,
                "ghost_pairs":     collab_res.ghost_pairs,
                "missing_pairs":   collab_res.missing_pairs,
                "count_mismatches": collab_res.count_mismatches,
                "outliers":        collab_res.outliers,
                "critical":        sum(1 for i in collab_res.issues if i["severity"] == "critical"),
            },
            "duplicates": {
                "exact_pairs":   dup_res.exact_pairs,
                "same_tmdb":     dup_res.same_tmdb,
                "fuzzy_pairs":   dup_res.fuzzy_pairs,
                "critical":      dup_res.exact_pairs + dup_res.same_tmdb,
            },
            "tmdb_integrity": {
                "checked":          tmdb_res.checked,
                "title_mismatches": tmdb_res.title_mismatches,
                "year_mismatches":  tmdb_res.year_mismatches,
                "critical":         sum(1 for i in tmdb_res.issues if i["severity"] == "critical"),
            },
            "industry_consistency": {
                "actors_checked": ind_res.actors_checked,
                "drifted":        ind_res.drifted,
                "critical":       sum(1 for i in ind_res.issues if i["severity"] == "critical"),
            },
            "orphans": {
                "movies_no_actors": orphan_res.movies_no_actors,
                "actors_one_film":  orphan_res.actors_one_film,
                "ghost_collabs":    orphan_res.ghost_collabs,
            },
        },
    }
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info("Report written to %s", path)


def main() -> int:
    args = _parse_args()
    api_key = None if args.skip_tmdb else os.getenv("TMDB_API_KEY", "").strip() or None
    tmdb_sample = 0 if args.skip_tmdb else args.tmdb_sample

    logger.info("Connecting to database…")
    conn = _connect()

    try:
        _setup_output_tables(conn)

        logger.info("=" * 50)
        logger.info("Starting validation run")
        logger.info("=" * 50)

        t0 = time.monotonic()

        link_res   = check_actor_movie_links(conn, api_key, tmdb_sample)
        collab_res = check_collaborations(conn)
        dup_res    = check_duplicates(conn)
        tmdb_res   = check_tmdb_integrity(conn, api_key, tmdb_sample)
        ind_res    = check_industry_consistency(conn)
        orphan_res = check_orphans(conn)

        compute_confidence_scores(
            conn, link_res, collab_res, dup_res, tmdb_res, ind_res, orphan_res
        )

        elapsed = time.monotonic() - t0
        logger.info("Validation completed in %.1fs", elapsed)

        generate_report(conn, link_res, collab_res, dup_res, tmdb_res, ind_res, orphan_res)

        critical = (
            sum(1 for i in link_res.issues   if i["severity"] == "critical") +
            sum(1 for i in collab_res.issues if i["severity"] == "critical") +
            dup_res.exact_pairs + dup_res.same_tmdb +
            sum(1 for i in tmdb_res.issues   if i["severity"] == "critical") +
            sum(1 for i in ind_res.issues    if i["severity"] == "critical") +
            orphan_res.ghost_collabs
        )

        if args.report_file:
            _write_report_file(
                args.report_file,
                link_res, collab_res, dup_res, tmdb_res, ind_res, orphan_res,
                elapsed, critical,
            )

        return 1 if critical > 0 else 0

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
