"""
insight_engine.py — WOW Insight Engine for South Cinema Analytics

Generates surprising, story-driven insights for the homepage.
Each pattern returns a single insight dict (or None if the condition
is not met by the current dataset).

The engine collects all candidates, scores them by impact, and
returns the top 3 diverse insights — at most 1 per type.

Fail-safe: a broken pattern is silently skipped so one bad query
never crashes the homepage.

v2 additions (backward-compatible):
  • In-memory TTL cache (10 min) — no DB hit on every request
  • Weighted log-based scoring — balances large numbers vs rare patterns
  • confidence field (0–1) — simple score normalisation
  • Logging — candidates generated, scores, final selection

v3 additions (backward-compatible):
  • Thread-safe cache — Lock protects reads/writes; compute runs outside lock
  • Smoother confidence curve — score / 100 instead of score / 50
  • title field — replaces headline for consistent API contract
  • category field — "collaboration" | "network" | "career" | "industry"
"""

import math
import re
import time
from threading import Lock
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

CURRENT_YEAR = 2026


# ── Module-level TTL cache ────────────────────────────────────────────────────
# Shared across all requests in the same process.  Rebuilt once every 10 min.
# _cache_lock guards both reads and writes; compute runs OUTSIDE the lock so
# a slow DB query on cache-miss doesn't block other in-flight requests.

_cache_lock: Lock = Lock()
_cache_data: Optional[list] = None
_cache_expiry: float = 0.0


# ── Pattern 1: Collaboration Shock ────────────────────────────────────────────

def _collaboration_shock(db: Session) -> Optional[dict]:
    """
    A legendary duo that worked together many times but hasn't shared
    the screen in 8+ years.  The gap is what makes it surprising.
    """
    row = db.execute(text("""
        WITH shared_years AS (
            -- TMDB pipeline
            SELECT
                LEAST(am1.actor_id, am2.actor_id)    AS a1_id,
                GREATEST(am1.actor_id, am2.actor_id) AS a2_id,
                MAX(m.release_year)                  AS last_year
            FROM   actor_movies am1
            JOIN   actor_movies am2 ON am2.movie_id = am1.movie_id
                                   AND am2.actor_id != am1.actor_id
            JOIN   movies m ON m.id = am1.movie_id
            WHERE  m.release_year IS NOT NULL
            GROUP  BY LEAST(am1.actor_id, am2.actor_id),
                      GREATEST(am1.actor_id, am2.actor_id)

            UNION ALL

            -- Wikidata pipeline
            SELECT
                LEAST(c1.actor_id, c2.actor_id)    AS a1_id,
                GREATEST(c1.actor_id, c2.actor_id) AS a2_id,
                MAX(m.release_year)                AS last_year
            FROM   cast c1
            JOIN   cast c2 ON c2.movie_id = c1.movie_id
                           AND c2.actor_id != c1.actor_id
            JOIN   movies m ON m.id = c1.movie_id
            WHERE  m.release_year IS NOT NULL
            GROUP  BY LEAST(c1.actor_id, c2.actor_id),
                      GREATEST(c1.actor_id, c2.actor_id)
        ),
        best_last AS (
            SELECT a1_id, a2_id, MAX(last_year) AS last_year
            FROM   shared_years
            GROUP  BY a1_id, a2_id
        )
        SELECT
            a1.id   AS actor1_id,
            a1.name AS actor1_name,
            a2.id   AS actor2_id,
            a2.name AS actor2_name,
            ac.collaboration_count AS films,
            bl.last_year
        FROM   actor_collaborations ac
        JOIN   best_last bl ON bl.a1_id = ac.actor1_id
                            AND bl.a2_id = ac.actor2_id
        JOIN   actors a1 ON a1.id = ac.actor1_id
        JOIN   actors a2 ON a2.id = ac.actor2_id
        WHERE  ac.actor1_id < ac.actor2_id
          AND  ac.collaboration_count >= 10
          AND  bl.last_year <= :cutoff
        ORDER  BY ac.collaboration_count DESC, bl.last_year ASC
        LIMIT  1
    """), {"cutoff": CURRENT_YEAR - 8}).fetchone()

    if not row:
        return None

    gap = CURRENT_YEAR - row.last_year
    return {
        "type":      "collab_shock",
        "category":  "collaboration",
        "title":     f"{row.actor1_name} & {row.actor2_name}",
        "value":     row.films,
        "unit":      "films together",
        "actors":    [row.actor1_name, row.actor2_name],
        "actor_ids": [row.actor1_id, row.actor2_id],
        "subtext":   (
            f"Worked together {row.films} times — but haven't shared "
            f"the screen in {gap}+ years."
        ),
    }


# ── Pattern 2: Hidden Dominance ───────────────────────────────────────────────

def _hidden_dominance(db: Session) -> Optional[dict]:
    """
    A supporting actor whose total film count rivals most lead actors —
    always in the background, never in the spotlight.
    """
    row = db.execute(text("""
        SELECT
            a.id,
            a.name,
            COUNT(am.movie_id) AS film_count
        FROM   actors a
        JOIN   actor_movies am ON am.actor_id = a.id
        WHERE  am.role_type = 'supporting'
          AND  a.is_primary_actor = FALSE
        GROUP  BY a.id, a.name
        HAVING COUNT(am.movie_id) >= 20
        ORDER  BY film_count DESC
        LIMIT  1
    """)).fetchone()

    if not row:
        return None

    # Average film count among primary actors for context
    avg_row = db.execute(text("""
        SELECT AVG(ast.film_count) AS avg_films
        FROM   actor_stats ast
        JOIN   actors a ON a.id = ast.actor_id
        WHERE  a.is_primary_actor = TRUE
    """)).fetchone()
    avg = int(avg_row.avg_films or 0) if avg_row else 0

    return {
        "type":      "hidden_dominance",
        "category":  "career",
        "title":     row.name,
        "value":     row.film_count,
        "unit":      "films",
        "actors":    [row.name],
        "actor_ids": [row.id],
        "subtext":   (
            f"Always in the background — {row.film_count} films, "
            f"rivalling most lead actors (avg {avg})."
        ),
    }


# ── Pattern 3: Cross-Industry Reach ──────────────────────────────────────────

def _cross_industry_reach(db: Session) -> Optional[dict]:
    """
    A primary actor who crossed language barriers to work in 3+ industries.
    """
    row = db.execute(text("""
        SELECT
            a.id,
            a.name,
            COUNT(DISTINCT LOWER(m.industry))                 AS ind_count,
            COUNT(DISTINCT am.movie_id)                       AS film_count,
            STRING_AGG(DISTINCT m.industry, ' · '
                       ORDER BY m.industry)                   AS industries
        FROM   actors a
        JOIN   actor_movies am ON am.actor_id = a.id
        JOIN   movies m        ON m.id = am.movie_id
        WHERE  m.industry IS NOT NULL
          AND  m.industry <> ''
          AND  a.is_primary_actor = TRUE
        GROUP  BY a.id, a.name
        HAVING COUNT(DISTINCT LOWER(m.industry)) >= 3
        ORDER  BY ind_count DESC, film_count DESC
        LIMIT  1
    """)).fetchone()

    if not row:
        return None

    return {
        "type":      "cross_industry",
        "category":  "industry",
        "title":     row.name,
        "value":     row.ind_count,
        "unit":      "industries",
        "actors":    [row.name],
        "actor_ids": [row.id],
        "subtext":   (
            f"Crossed language barriers across {row.ind_count} industries "
            f"in {row.film_count}+ films: {row.industries}."
        ),
    }


# ── Pattern 4: Career Peak Window ────────────────────────────────────────────

def _career_peak_window(db: Session) -> Optional[dict]:
    """
    The single densest 5-year window in an actor's career — their golden era —
    where they accounted for ≥35% of their total output.
    """
    row = db.execute(text("""
        WITH yearly AS (
            SELECT
                am.actor_id,
                m.release_year,
                COUNT(*)  AS films_in_year
            FROM   actor_movies am
            JOIN   movies  m ON m.id = am.movie_id
            JOIN   actors  a ON a.id = am.actor_id
            WHERE  m.release_year IS NOT NULL
              AND  a.is_primary_actor = TRUE
            GROUP  BY am.actor_id, m.release_year
        ),
        windows AS (
            SELECT
                y1.actor_id,
                y1.release_year                          AS win_start,
                SUM(y2.films_in_year)                    AS win_films
            FROM   yearly y1
            JOIN   yearly y2 ON  y2.actor_id    = y1.actor_id
                              AND y2.release_year BETWEEN y1.release_year
                                                      AND y1.release_year + 4
            GROUP  BY y1.actor_id, y1.release_year
        ),
        best_window AS (
            SELECT DISTINCT ON (actor_id)
                actor_id, win_start, win_films
            FROM   windows
            ORDER  BY actor_id, win_films DESC
        )
        SELECT
            a.id, a.name,
            bw.win_start                                AS peak_start,
            bw.win_start + 4                            AS peak_end,
            bw.win_films,
            ast.film_count                              AS total_films
        FROM   best_window bw
        JOIN   actors      a   ON a.id         = bw.actor_id
        JOIN   actor_stats ast ON ast.actor_id = bw.actor_id
        WHERE  bw.win_films >= 8
          AND  bw.win_films::float / NULLIF(ast.film_count, 0) >= 0.35
        ORDER  BY bw.win_films DESC
        LIMIT  1
    """)).fetchone()

    if not row:
        return None

    return {
        "type":      "career_peak",
        "category":  "career",
        "title":     row.name,
        "value":     f"{row.peak_start}–{row.peak_end}",   # string range — kept as-is
        "unit":      "peak years",
        "actors":    [row.name],
        "actor_ids": [row.id],
        "subtext":   (
            f"{row.win_films} of their {row.total_films} films packed into "
            f"just 5 years ({row.peak_start}–{row.peak_end}) — their golden era."
        ),
    }


# ── Pattern 5: Network Power ──────────────────────────────────────────────────

def _network_power(db: Session) -> Optional[dict]:
    """
    The actor connected to the most unique co-stars across all industries —
    the ultimate bridge builder.
    """
    row = db.execute(text("""
        SELECT
            a.id, a.name,
            COUNT(DISTINCT ac.actor2_id) AS costar_count,
            ast.film_count
        FROM   actors a
        JOIN   actor_stats          ast ON ast.actor_id = a.id
        JOIN   actor_collaborations ac  ON ac.actor1_id = a.id
        WHERE  a.is_primary_actor = TRUE
        GROUP  BY a.id, a.name, ast.film_count
        HAVING COUNT(DISTINCT ac.actor2_id) >= 30
        ORDER  BY costar_count DESC
        LIMIT  1
    """)).fetchone()

    if not row:
        return None

    return {
        "type":      "network_power",
        "category":  "network",
        "title":     row.name,
        "value":     row.costar_count,
        "unit":      "connections",
        "actors":    [row.name],
        "actor_ids": [row.id],
        "subtext":   (
            f"Connected to {row.costar_count} unique actors across all industries "
            f"— the ultimate bridge builder in South Indian cinema."
        ),
    }


# ── Pattern 6: Director Loyalty ───────────────────────────────────────────────

def _director_loyalty(db: Session) -> Optional[dict]:
    """
    An actor who spent ≥30% of their career with a single director —
    a defining creative partnership.
    """
    row = db.execute(text("""
        SELECT
            a.id                                                     AS actor_id,
            ads.actor_name,
            ads.director_name,
            ads.film_count                                           AS dir_films,
            ast.film_count                                           AS total_films,
            ROUND(ads.film_count * 100.0 / NULLIF(ast.film_count, 0)) AS pct
        FROM   actor_director_stats ads
        JOIN   actor_stats ast ON ast.actor_id = ads.actor_id
        JOIN   actors      a   ON a.id         = ads.actor_id
        WHERE  a.is_primary_actor = TRUE
          AND  ads.film_count >= 8
          AND  ads.film_count * 100.0 / NULLIF(ast.film_count, 0) >= 30
        ORDER  BY pct DESC, ads.film_count DESC
        LIMIT  1
    """)).fetchone()

    if not row:
        return None

    return {
        "type":      "director_loyalty",
        "category":  "collaboration",
        "title":     row.actor_name,
        "value":     row.dir_films,
        "unit":      f"films with {row.director_name}",
        "actors":    [row.actor_name],
        "actor_ids": [row.actor_id],
        "subtext":   (
            f"{int(row.pct)}% of their career alongside director "
            f"{row.director_name} — a defining creative partnership."
        ),
    }


# ── Scoring helpers ───────────────────────────────────────────────────────────

def _extract_number(value) -> Optional[float]:
    """
    Safely pull a numeric value from an insight's 'value' field.
    Handles int, float, and strings like '2015–2020' (returns first number found).
    """
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        match = re.search(r'\d+', value)
        if match:
            return float(match.group())
    return None


def _score(insight: dict) -> float:
    """
    Weighted log-based scoring.

    Formula:
      base  = log(value + 1) × 10        — magnitude (log-scaled, avoids large-number bias)
      base += type_weight[type]           — category importance
      base += 15 if insight has 'rarity'  — bonus for rare patterns

    Returns a float; higher = more impressive / surprising.
    """
    base = 0.0

    numeric = _extract_number(insight.get("value"))
    # Skip year-shaped numbers (≥ 1000) — they are labels, not magnitudes.
    # e.g. career_peak stores "2010–2015" in value; scoring on the year itself
    # would inflate its score by ~76 points, drowning out film/costar counts.
    if numeric is not None and numeric < 1000:
        base += math.log(numeric + 1) * 10

    # Per-category importance weights
    type_weight: dict = {
        # WOW engine types
        "collab_shock":     20,
        "network_power":    18,
        "hidden_dominance": 17,
        "career_peak":      15,
        "cross_industry":   15,
        "director_loyalty": 12,
        # Legacy types from crud.get_insights() — kept for backward compat
        "collaboration":    20,
        "director":         12,
        "supporting":       15,
    }
    base += type_weight.get(insight.get("type", ""), 10)

    # Optional rarity bonus — set insight["rarity"] = True in any pattern to boost it
    if insight.get("rarity"):
        base += 15

    return base


# ── Diversity picker ──────────────────────────────────────────────────────────

def _pick_diverse(candidates: list, n: int = 3) -> list:
    """
    Return top-n insights with at most 1 per type, ranked by score.
    Attaches 'confidence' (0–1) to each candidate and logs progress.
    """
    print(f"[INSIGHT] {len(candidates)} candidates generated")

    # Score every candidate, attach confidence, log
    scored: list = []
    for ins in candidates:
        s = _score(ins)
        # Divide by 100 (not 50) so confidence stays spread across the 0–1 range
        # and doesn't saturate prematurely on common high-scoring patterns.
        ins["confidence"] = round(min(1.0, s / 100), 3)
        scored.append((s, ins))
        print(
            f"[INSIGHT] type={ins['type']:<20} "
            f"score={s:5.1f}  confidence={ins['confidence']:.3f}  "
            f"title={ins['title']!r}"
        )

    # Pick top-N with type diversity
    seen: set = set()
    result: list = []
    for _, insight in sorted(scored, key=lambda x: x[0], reverse=True):
        if insight["type"] not in seen:
            seen.add(insight["type"])
            result.append(insight)
        if len(result) >= n:
            break

    print(f"[INSIGHT] selected {len(result)}: {[i['type'] for i in result]}")
    return result


# ── Core computation (no cache) ───────────────────────────────────────────────

def compute_wow_insights(db: Session) -> list:
    """
    Run all WOW patterns, score candidates, return top 6 diverse insights
    (one per type — there are 6 patterns so up to 6 unique cards).

    Fail-safe: a broken pattern is silently skipped — one bad SQL query
    must not crash the entire homepage.

    Returns an empty list if no patterns fire (triggers frontend fallback).
    """
    patterns = [
        _collaboration_shock,
        _hidden_dominance,
        _cross_industry_reach,
        _career_peak_window,
        _network_power,
        _director_loyalty,
    ]

    candidates = []
    for pattern in patterns:
        try:
            result = pattern(db)
            if result:
                candidates.append(result)
        except Exception:
            pass   # one broken pattern must not crash the whole page

    return _pick_diverse(candidates, n=6)  # one per type — up to all 6 patterns


# ── Public entry point (thread-safe TTL cache) ────────────────────────────────

def get_wow_insights(db: Session) -> list:
    """
    Returns top 3 WOW insights, cached for 10 minutes.

    Thread-safety design:
      1. Check cache under lock — return immediately on hit (no contention).
      2. Compute OUTSIDE the lock — DB queries don't block other requests.
      3. Write result under lock — last writer wins (acceptable: results are
         identical across concurrent cache-miss computations).

    To force a refresh after new data ingestion:
        from app.insight_engine import _invalidate_cache; _invalidate_cache()
    """
    global _cache_data, _cache_expiry

    now = time.time()

    with _cache_lock:
        if _cache_data is not None and now < _cache_expiry:
            return _cache_data

    # Compute outside the lock — this is the slow part (6 SQL queries).
    # Multiple threads may enter here on concurrent cache-miss; that is fine —
    # the work is idempotent and the last write wins.
    insights = compute_wow_insights(db)

    with _cache_lock:
        _cache_data = insights
        _cache_expiry = now + 600   # 10 minutes

    return insights


def _invalidate_cache() -> None:
    """Force the next call to get_wow_insights() to recompute. Useful after ingestion."""
    global _cache_data, _cache_expiry
    with _cache_lock:
        _cache_data = None
        _cache_expiry = 0.0
