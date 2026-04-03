"""
insight_engine.py — WOW Insight Engine for South Cinema Analytics

Generates surprising, story-driven insights for the homepage.
Each pattern returns a list of candidates; the engine scores and
selects the best 3–4 diverse insights for display.

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

v4 additions:
  • collab_shock — requires BOTH actors to be primary (filters obscure pairs)
  • career_peak  — value is now win_films (int), not a year-range string;
                   unit = "films in 5 years"; year range kept in subtext only
  • director_loyalty — director name added to actors[] so card renders as duo;
                       unit = "films together" (consistent with collab_shock)
  • hidden_dominance — minimum raised from 20 → 100 films (filters obscure actors)
  • _score()    — four components: magnitude, type weight, primary bonus, clarity
  • _pick_diverse() — one insight per category max; min score floor; hard cap of 4
"""

import logging
import math
import re
import time
from threading import Lock
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

CURRENT_YEAR = 2026

# Minimum score an insight must reach to be eligible for selection.
# Prevents low-magnitude or obscure-actor cards from appearing.
_MIN_SCORE = 45.0

# Maximum insights returned to the carousel.
_MAX_INSIGHTS = 4


# ── Module-level TTL cache ────────────────────────────────────────────────────
# Shared across all requests in the same process.  Rebuilt once every 10 min.
# _cache_lock guards both reads and writes; compute runs OUTSIDE the lock so
# a slow DB query on cache-miss doesn't block other in-flight requests.

_cache_lock: Lock = Lock()
_cache_data: Optional[list] = None
_cache_expiry: float = 0.0


# ── Pattern 1: Collaboration Shock ────────────────────────────────────────────

def _collaboration_shock(db: Session, limit: int = 50) -> list:
    """
    Legendary duos that worked together many times but haven't shared
    the screen in 8+ years.

    v4 change: both actors must be is_primary_actor = TRUE so only
    well-known lead pairings surface (filters character-actor pairs like
    Ali Basha + M. S. Narayana that score low on recognisability).
    """
    rows = db.execute(text("""
        WITH shared_years AS (
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

            SELECT
                LEAST(c1.actor_id, c2.actor_id)    AS a1_id,
                GREATEST(c1.actor_id, c2.actor_id) AS a2_id,
                MAX(m.release_year)                AS last_year
            FROM   "cast" c1
            JOIN   "cast" c2 ON c2.movie_id = c1.movie_id
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
          AND  a1.is_primary_actor = TRUE
          AND  a2.is_primary_actor = TRUE
        ORDER  BY ac.collaboration_count DESC, bl.last_year ASC
        LIMIT  :limit
    """), {"cutoff": CURRENT_YEAR - 8, "limit": limit}).fetchall()

    results = []
    for row in rows:
        gap = CURRENT_YEAR - row.last_year
        results.append({
            "type":       "collab_shock",
            "category":   "collaboration",
            "headline":   f"{row.actor1_name} & {row.actor2_name}",
            "value":      row.films,
            "unit":       "films together",
            "actors":     [row.actor1_name, row.actor2_name],
            "actor_ids":  [row.actor1_id, row.actor2_id],
            "is_primary": True,   # both actors guaranteed primary by SQL filter
            "subtext":    (
                f"Worked together {row.films} times — but haven't shared "
                f"the screen in {gap}+ years."
            ),
        })
    return results


# ── Pattern 2: Hidden Dominance ───────────────────────────────────────────────

def _hidden_dominance(db: Session, limit: int = 50) -> list:
    """
    Supporting actors whose total film count rivals most lead actors.

    v4 change: minimum raised from 20 → 100 films.  This filters actors with
    a modest filmography (20–99 films) who produce uninteresting, low-magnitude
    cards.  Only actors with genuinely extraordinary output (Brahmanandam 522,
    Sukumari 460, etc.) remain eligible.
    """
    rows = db.execute(text("""
        SELECT
            a.id,
            a.name,
            COUNT(am.movie_id) AS film_count
        FROM   actors a
        JOIN   actor_movies am ON am.actor_id = a.id
        WHERE  am.role_type = 'supporting'
          AND  a.is_primary_actor = FALSE
        GROUP  BY a.id, a.name
        HAVING COUNT(am.movie_id) >= 100
        ORDER  BY film_count DESC
        LIMIT  :limit
    """), {"limit": limit}).fetchall()

    if not rows:
        return []

    avg_row = db.execute(text("""
        SELECT AVG(ast.film_count) AS avg_films
        FROM   actor_stats ast
        JOIN   actors a ON a.id = ast.actor_id
        WHERE  a.is_primary_actor = TRUE
    """)).fetchone()
    avg = int(avg_row.avg_films or 0) if avg_row else 0

    results = []
    for row in rows:
        results.append({
            "type":       "hidden_dominance",
            "category":   "career",
            "headline":   row.name,
            "value":      row.film_count,
            "unit":       "films",
            "actors":     [row.name],
            "actor_ids":  [row.id],
            "is_primary": False,   # these actors are explicitly non-primary
            "subtext":    (
                f"Always in the background — {row.film_count} films, "
                f"rivalling most lead actors (avg {avg})."
            ),
        })
    return results


# ── Pattern 3: Cross-Industry Reach ──────────────────────────────────────────

def _cross_industry_reach(db: Session, limit: int = 50) -> list:
    """
    Primary actors who crossed language barriers to work in 3+ industries.
    """
    rows = db.execute(text("""
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
        LIMIT  :limit
    """), {"limit": limit}).fetchall()

    return [
        {
            "type":       "cross_industry",
            "category":   "industry",
            "headline":   row.name,
            "value":      row.ind_count,
            "unit":       "industries",
            "actors":     [row.name],
            "actor_ids":  [row.id],
            "is_primary": True,
            "subtext":    (
                f"Crossed language barriers across {row.ind_count} industries "
                f"in {row.film_count}+ films: {row.industries}."
            ),
        }
        for row in rows
    ]


# ── Pattern 4: Career Peak Window ────────────────────────────────────────────

def _career_peak_window(db: Session, limit: int = 50) -> list:
    """
    The densest 5-year career windows — one per actor (their golden era).

    v4 change: value is now win_films (integer) instead of the year-range
    string "YYYY–YYYY".  This lets the card render a big, punchy number
    ("160 films in 5 years") rather than a year that looks like data.
    The year range is preserved in subtext for context.
    """
    rows = db.execute(text("""
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
        LIMIT  :limit
    """), {"limit": limit}).fetchall()

    return [
        {
            "type":       "career_peak",
            "category":   "career",
            "headline":   row.name,
            # Integer now — renders as a large number on the card ("160")
            "value":      int(row.win_films),
            "unit":       "films in 5 years",
            "actors":     [row.name],
            "actor_ids":  [row.id],
            "is_primary": True,
            "subtext":    (
                f"{row.win_films} films in just 5 years "
                f"({row.peak_start}–{row.peak_end}) — their golden era."
            ),
        }
        for row in rows
    ]


# ── Pattern 5: Network Power ──────────────────────────────────────────────────

def _network_power(db: Session, limit: int = 50) -> list:
    """
    Actors connected to the most unique co-stars across all industries.
    """
    rows = db.execute(text("""
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
        LIMIT  :limit
    """), {"limit": limit}).fetchall()

    return [
        {
            "type":       "network_power",
            "category":   "network",
            "headline":   row.name,
            "value":      row.costar_count,
            "unit":       "connections",
            "actors":     [row.name],
            "actor_ids":  [row.id],
            "is_primary": True,
            "subtext":    (
                f"Connected to {row.costar_count} unique actors across all industries "
                f"— the ultimate bridge builder in South Indian cinema."
            ),
        }
        for row in rows
    ]


# ── Pattern 6: Director Loyalty ───────────────────────────────────────────────

def _director_loyalty(db: Session, limit: int = 50) -> list:
    """
    Actors who spent ≥30% of their career with a single director.

    v4 change: director_name is now included in actors[] so the card
    renders as a duo (actor portrait + director name visible).
    unit changed to "films together" for consistency with collab_shock.
    """
    rows = db.execute(text("""
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
        LIMIT  :limit
    """), {"limit": limit}).fetchall()

    return [
        {
            "type":       "director_loyalty",
            "category":   "collaboration",
            "headline":   f"{row.actor_name} & {row.director_name}",
            # Duo: actor first, director second.  Director has no actor_id
            # so actor_ids carries only the actor's DB id.
            "value":      row.dir_films,
            "unit":       "films together",
            "actors":     [row.actor_name, row.director_name],
            "actor_ids":  [row.actor_id],
            "is_primary": True,
            "subtext":    (
                f"{int(row.pct)}% of {row.actor_name}'s career alongside "
                f"director {row.director_name} — a defining creative partnership."
            ),
        }
        for row in rows
    ]


# ── Scoring ───────────────────────────────────────────────────────────────────

def _extract_number(value) -> Optional[float]:
    """
    Safely pull a numeric value from an insight's 'value' field.
    Handles int, float, and strings (returns first digit sequence found).
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
    Four-component scoring.  Higher = more impressive and shareable.

    1. Magnitude  — log-scaled film/connection count.  career_peak now
                    contributes meaningfully because value is an integer.
    2. Type weight — curiosity and shareability ranking per insight type.
    3. Primary bonus — +20 when both/all actors are primary lead actors.
                       Suppresses obscure supporting-only cards without
                       hard-removing them (hidden_dominance still competes
                       when its film count is extraordinary).
    4. Clarity bonus — +8 when value is already an integer (no string parsing
                       needed; card renders a clean large number).
    """
    score = 0.0

    # 1. Magnitude
    numeric = _extract_number(insight.get("value"))
    if numeric is not None:
        score += math.log(numeric + 1) * 12

    # 2. Type weight
    type_weight: dict = {
        "collab_shock":     25,   # two names + big number = most shareable
        "network_power":    22,   # enormous connection count = jaw-dropping
        "hidden_dominance": 20,   # shocking film count for a background actor
        "cross_industry":   18,   # clear, visual, relatable across audiences
        "career_peak":      16,   # now numeric — direct comparison possible
        "director_loyalty": 14,   # duo but director name less recognised
        # Legacy types — kept for backward compatibility
        "collaboration":    20,
        "director":         12,
        "supporting":       15,
    }
    score += type_weight.get(insight.get("type", ""), 10)

    # 3. Primary actor bonus
    if insight.get("is_primary"):
        score += 20

    # 4. Clarity bonus — value is a plain integer (not a string range)
    if isinstance(insight.get("value"), int):
        score += 8

    # Optional rarity flag — set insight["rarity"] = True in any pattern to boost
    if insight.get("rarity"):
        score += 10

    return score


# ── Diversity picker ──────────────────────────────────────────────────────────

def _pick_diverse(candidates: list) -> list:
    """
    Select the best 3–4 insights with strict diversity enforcement.

    Algorithm:
      1. Score every candidate; discard those below _MIN_SCORE.
      2. Sort survivors highest-score first.
      3. Pick greedily: add a candidate only if its category has not yet
         appeared in the result set.
      4. Stop when _MAX_INSIGHTS is reached or candidates are exhausted.

    Category map (4 categories → natural 1-per-category diversity):
      collaboration → collab_shock, director_loyalty
      network       → network_power
      career        → hidden_dominance, career_peak
      industry      → cross_industry

    If fewer than _MAX_INSIGHTS candidates survive the category filter
    (e.g. only 2 categories have qualifying insights), the result list
    is shorter rather than duplicating a category.
    """
    logger.info("insight candidates before scoring: %d", len(candidates))

    # Score everything
    for ins in candidates:
        s = _score(ins)
        ins["_score"] = s
        ins["confidence"] = round(min(1.0, s / 100), 3)
        logger.debug(
            "insight type=%-20s score=%5.1f primary=%-5s value=%-8s headline=%r",
            ins["type"], s, ins.get("is_primary"), ins.get("value"), ins.get("headline"),
        )

    # Apply minimum score floor
    eligible = [ins for ins in candidates if ins["_score"] >= _MIN_SCORE]
    logger.info("insight candidates after score floor (>= %.0f): %d", _MIN_SCORE, len(eligible))

    # Sort highest score first
    eligible.sort(key=lambda x: x["_score"], reverse=True)

    # Greedy one-per-category selection
    seen_categories: set = set()
    result: list = []

    for ins in eligible:
        cat = ins.get("category", "")
        if cat in seen_categories:
            continue
        seen_categories.add(cat)
        result.append(ins)
        if len(result) >= _MAX_INSIGHTS:
            break

    logger.info(
        "insights selected: %d  types=%s",
        len(result),
        [f"{i['type']}({i['_score']:.0f})" for i in result],
    )
    return result


# ── Core computation (no cache) ───────────────────────────────────────────────

def compute_wow_insights(db: Session) -> list:
    """
    Run all WOW patterns, score candidates, return top 3–4 diverse insights.

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
            results = pattern(db)
            if results:
                candidates.extend(results)
        except Exception as e:
            db.rollback()
            logger.warning("insight pattern %s failed: %s", pattern.__name__, e)

    return _pick_diverse(candidates)


# ── Public entry point (thread-safe TTL cache) ────────────────────────────────

def get_wow_insights(db: Session) -> list:
    """
    Returns top 3–4 WOW insights, cached for 10 minutes.

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
