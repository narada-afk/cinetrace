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

v5 additions:
  • _enrich_with_fame() — single bulk SQL stamps film_count + costar_count onto
                          every candidate after collection (no per-insight queries)
  • _fame_score()       — tiered points for film_count / costar_count / is_primary
  • _relatability_score() — rewards integer values and clear count-based units
  • _wow_score()        — tiered magnitude bonuses + pattern-specific surprises
  • _hard_filter()      — removes insights below per-type value floors and
                          collab_shock pairs where either actor has < 50 films
  • _score()            — now composes the three sub-scores + a type base weight
  • _MIN_SCORE raised to 55 — ensures only high-fame or high-wow cards survive

v6 additions (extend & refine only — no existing logic removed):
  • _fame_score()       — recognition boost: +10 if is_primary AND film_count >= 150
                          (globally recognisable tier); fame cap raised 40 → 50
  • _wow_score()        — shock override: +15 for value >= 500, +10 for value >= 400
                          (stacked on top of existing magnitude tiers); cap raised 30 → 50
  • _duo_wow_boost()    — new helper: +10 for collab_shock where both actors score
                          is_primary=True AND film_count >= 150
  • _headline_readiness() — new helper: -8 penalty when value/unit combo won't render
                            cleanly as a large-number card
  • _score()            — adds duo_wow and headline to composite; breakdown updated
  • _hard_filter()      — Rule 6: non-hidden_dominance types require at least one
                          primary actor; hidden_dominance now requires film_count >= 200
  • _pick_diverse()     — 3-pass algorithm: (1) one-per-category, (2) fallback fill
                          if score > 75, (3) supporting cap (max 1 hidden_dominance)
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

# Minimum composite score an insight must reach to be eligible for selection.
# 40 is the practical floor — below this, either the actor is obscure or
# the stat is too small to be interesting. Still filters junk while
# allowing career_peak / director_loyalty / supporting industry cards through.
_MIN_SCORE = 25.0   # Lowered from 40 — lets more Telugu/Kannada candidates through

# No hard cap — return every eligible card so the carousel is limitless.
# The score floor (_MIN_SCORE) and per-industry diversity caps are the only
# constraints; all passing candidates are included and ordered.
_MAX_INSIGHTS = 10_000  # effectively unlimited


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
            "category":   "supporting",
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
        HAVING COUNT(DISTINCT LOWER(m.industry)) >= 2
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
        WHERE  bw.win_films >= 6
          AND  bw.win_films::float / NULLIF(ast.film_count, 0) >= 0.25
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
            "unit":       f"films  ·  {row.peak_start}–{row.peak_end}",
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
            a.name                                                   AS actor_name,
            ads.director                                             AS director_name,
            ads.film_count                                           AS dir_films,
            ast.film_count                                           AS total_films,
            ROUND(ads.film_count * 100.0 / NULLIF(ast.film_count, 0)) AS pct
        FROM   actor_director_stats ads
        JOIN   actor_stats ast ON ast.actor_id = ads.actor_id
        JOIN   actors      a   ON a.id         = ads.actor_id
        WHERE  a.is_primary_actor = TRUE
          AND  ads.film_count >= 15
          AND  LOWER(a.name) != LOWER(ads.director)
        ORDER  BY ads.film_count DESC, pct DESC
        LIMIT  :limit
    """), {"limit": limit}).fetchall()

    return [
        {
            "type":       "director_loyalty",
            "category":   "director",
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


# ── Pattern 7: Director Box Office ────────────────────────────────────────────

def _director_box_office(db: Session, limit: int = 50) -> list:
    """
    Directors whose total box office across all films exceeds 1000 Cr.

    Value  = total box office (integer Cr).
    Subtext calls out the biggest single hit by name and figure.
    actor_ids is empty (directors are not in the actors table);
    hard_filter rule 1 is bypassed for this type.
    """
    rows = db.execute(text("""
        SELECT
            m.director,
            ROUND(SUM(m.box_office))                              AS total_cr,
            ROUND(MAX(m.box_office))                              AS biggest_cr,
            (
                SELECT m2.title
                FROM   movies m2
                WHERE  m2.director = m.director
                  AND  m2.box_office IS NOT NULL
                ORDER  BY m2.box_office DESC
                LIMIT  1
            )                                                     AS biggest_title
        FROM   movies m
        WHERE  m.box_office  IS NOT NULL
          AND  m.director    IS NOT NULL
        GROUP  BY m.director
        HAVING SUM(m.box_office) >= 1000
        ORDER  BY total_cr DESC
        LIMIT  :limit
    """), {"limit": limit}).fetchall()

    return [
        {
            "type":       "director_box_office",
            "category":   "blockbuster",
            "headline":   row.director,
            "value":      int(row.total_cr),
            "unit":       "Cr box office",
            "actors":     [row.director],
            "actor_ids":  [],          # directors not in actors table
            "is_primary": False,
            "subtext":    (
                f"Biggest hit: {row.biggest_title} — ₹{int(row.biggest_cr)} Cr. "
                f"₹{int(row.total_cr)} Cr in total box office."
            ),
        }
        for row in rows
    ]


# ── Fame enrichment ───────────────────────────────────────────────────────────

def _enrich_with_fame(candidates: list, db: Session) -> None:
    """
    Stamp actor career stats onto every candidate via ONE bulk SQL query.

    After this runs, each insight dict carries:
      _actor_stats: list of {film_count, costar_count, is_primary} dicts
                    — one entry per actor_id in actor_ids[].

    Directors are not in the actors table so they won't appear in the result;
    director_loyalty insights will have _actor_stats with 1 entry (the actor).
    """
    all_ids: set = set()
    for ins in candidates:
        for aid in (ins.get("actor_ids") or []):
            if aid:
                all_ids.add(int(aid))

    if not all_ids:
        for ins in candidates:
            ins["_actor_stats"] = []
        return

    rows = db.execute(text("""
        SELECT
            a.id,
            a.industry,
            ast.film_count,
            COUNT(DISTINCT ac.actor2_id) AS costar_count,
            a.is_primary_actor
        FROM   actors a
        JOIN   actor_stats ast ON ast.actor_id = a.id
        LEFT JOIN actor_collaborations ac ON ac.actor1_id = a.id
        WHERE  a.id = ANY(:ids)
        GROUP  BY a.id, a.industry, ast.film_count, a.is_primary_actor
    """), {"ids": list(all_ids)}).fetchall()

    # Build lookup: actor_id → stats dict
    lookup: dict = {
        r.id: {
            "film_count":   r.film_count   or 0,
            "costar_count": r.costar_count or 0,
            "is_primary":   r.is_primary_actor,
            "industry":     r.industry or "Unknown",
        }
        for r in rows
    }

    for ins in candidates:
        ins["_actor_stats"] = [
            lookup[aid]
            for aid in (ins.get("actor_ids") or [])
            if aid in lookup
        ]
        # Stamp industry on the insight itself — use the first actor's industry.
        # For duo cards, prefer the actor with more films (already first by SQL sort).
        if ins["_actor_stats"]:
            ins["industry"] = ins["_actor_stats"][0]["industry"]


# ── Scoring helpers ───────────────────────────────────────────────────────────

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


def _fame_score(insight: dict) -> float:
    """
    0–50 points.  Rewards well-known, high-volume actors.

    Uses tiered thresholds rather than log-scaling so that the difference
    between a 60-film actor and a 200-film actor is meaningfully large.

    film_count tiers   (max 25 pts):  200+ → 25, 100+ → 18, 50+ → 10, 20+ → 4
    costar_count tiers (max 15 pts):  200+ → 15, 100+ → 10, 50+ →  5
    is_primary bonus                : +8 per primary actor
    recognition boost [v6]          : +10 if is_primary AND film_count >= 150
                                      (globally recognisable tier; applied per actor,
                                       averaged for duo insights so both must qualify)

    For multi-actor insights (collab_shock, director_loyalty) the score is
    the average across all actor entries — both actors must be famous for
    the insight to score high, not just one of them.
    """
    stats_list = insight.get("_actor_stats", [])
    if not stats_list:
        return 0.0

    total = 0.0
    for s in stats_list:
        fc = s["film_count"]
        cc = s["costar_count"]

        # Film count — how prolific is this actor?
        if fc >= 200:
            total += 25
        elif fc >= 100:
            total += 18
        elif fc >= 50:
            total += 10
        elif fc >= 20:
            total += 4

        # Co-star count — how well-connected / recognised by collaborators?
        if cc >= 200:
            total += 15
        elif cc >= 100:
            total += 10
        elif cc >= 50:
            total += 5

        # Primary lead actor bonus
        if s["is_primary"]:
            total += 8

        # v6: Recognition boost — globally recognisable tier
        if s["is_primary"] and fc >= 150:
            total += 10

    avg = total / len(stats_list)
    return min(50.0, avg)


def _relatability_score(insight: dict) -> float:
    """
    0–18 points.  Rewards stats that are immediately understandable on a card.

    Integer value  → +8  (renders as clean large number, no parsing needed)
    Count-based unit → +10  (films, connections, industries are universally legible)
    Abstract unit  → -10  (percentages, ratios, peak years confuse casual readers)
    """
    score = 0.0
    value = insight.get("value")
    unit  = insight.get("unit", "")

    if isinstance(value, int):
        score += 8

    RELATABLE_UNITS = {"films", "films together", "connections", "films in 5 years", "industries", "Cr box office"}
    ABSTRACT_UNITS  = {"peak years", "pct", "ratio", "%"}

    if unit in RELATABLE_UNITS:
        score += 10
    elif unit in ABSTRACT_UNITS:
        score -= 10

    return max(0.0, score)


def _wow_score(insight: dict) -> float:
    """
    0–50 points.  Rewards extreme magnitudes and genuinely surprising patterns.

    Magnitude tiers (the raw numeric value):
      500+  → 30    jaw-dropping (Brahmanandam 522 films, Kamal 557 connections)
      200+  → 22
      100+  → 16
       50+  → 10
       30+  →  7
       15+  →  4
        6+  →  2

    Pattern-specific bonuses (stacked on top of magnitude):
      collab_shock:     gap >= 20 years → +10,  gap >= 10 → +5
      cross_industry:   value >= 5 industries → +8   (genuinely rare)
      hidden_dominance: value >= 400 films    → +10  (extraordinary output)

    v6 shock overrides (stacked on top of all other bonuses — cap raised to 50):
      value >= 500 → +15   extreme outlier; always deserves prominence
      value >= 400 → +10   (only one tier fires per insight)
    """
    score = 0.0
    value = insight.get("value")
    itype = insight.get("type", "")

    numeric = _extract_number(value)
    if numeric is not None:
        if numeric >= 500:
            score += 30
        elif numeric >= 200:
            score += 22
        elif numeric >= 100:
            score += 16
        elif numeric >= 50:
            score += 10
        elif numeric >= 30:
            score += 7
        elif numeric >= 15:
            score += 4
        elif numeric >= 6:
            score += 2

    # Pattern-specific surprise bonuses
    if itype == "collab_shock":
        gap_match = re.search(r"(\d+)\+ years", insight.get("subtext", ""))
        if gap_match:
            gap = int(gap_match.group(1))
            if gap >= 20:
                score += 10
            elif gap >= 10:
                score += 5

    if itype == "cross_industry" and isinstance(value, int) and value >= 5:
        score += 8

    if itype == "hidden_dominance" and isinstance(value, int) and value >= 400:
        score += 10

    # v6: shock overrides — applied after pattern bonuses, before cap
    if numeric is not None:
        if numeric >= 500:
            score += 15
        elif numeric >= 400:
            score += 10

    return min(50.0, score)


def _duo_wow_boost(insight: dict) -> float:
    """
    v6: +10 for collab_shock when BOTH actors are top-tier (primary + 150+ films).

    Rationale: a pairing of two globally recognisable lead actors is far more
    shareable than a pairing where only one is famous.  This bonus only fires
    when _actor_stats has at least 2 entries and every entry qualifies.
    """
    if insight.get("type") != "collab_shock":
        return 0.0
    stats = insight.get("_actor_stats", [])
    if len(stats) < 2:
        return 0.0
    both_top_tier = all(s["is_primary"] and s["film_count"] >= 150 for s in stats)
    return 10.0 if both_top_tier else 0.0


def _headline_readiness(insight: dict) -> float:
    """
    v6: Checks whether the insight produces a clean, punchy card.

    Rules:
      value is int/float AND unit is in SIMPLE_UNITS → no adjustment (card is ready)
      Otherwise → -8 penalty (stat is confusing or abstract on a glance-read card)

    SIMPLE_UNITS matches the set already used by _relatability_score so that
    the two functions are consistent — this function exists as a hard structural
    check separate from the graduated relatability score.
    """
    SIMPLE_UNITS = {"films", "films together", "connections", "films in 5 years", "industries", "Cr box office"}
    value = insight.get("value")
    unit  = insight.get("unit", "")
    if isinstance(value, (int, float)) and unit in SIMPLE_UNITS:
        return 0.0
    return -8.0


def _score(insight: dict) -> float:
    """
    Composite score = type_base + fame + relatability + wow + duo_wow + headline.

    Theoretical range: ~5–133.  Minimum for selection: _MIN_SCORE = 55.

    type_base     — fixed per-type weight (curiosity value of the insight category)
    fame          — 0–50: how well-known and prolific are the actors?
                          (raised from 40 in v6 to accommodate recognition boost)
    relatability  — 0–18: how immediately understandable is the stat?
    wow           — 0–50: how extreme or surprising is the number/pattern?
                          (raised from 30 in v6 to accommodate shock override)
    duo_wow [v6]  — 0 or +10: collab_shock where both actors are globally famous
    headline [v6] — 0 or -8: penalty when value/unit combo won't render cleanly

    Sub-scores are stored in insight["_score_breakdown"] for debug logging.
    """
    TYPE_BASE: dict = {
        "collab_shock":        15,
        "network_power":       12,
        "hidden_dominance":    10,
        "cross_industry":      12,
        "career_peak":         10,
        "director_loyalty":     8,
        "director_box_office": 14,
        # Legacy backward-compat
        "collaboration":       12,
        "director":             8,
        "supporting":          10,
    }
    base     = TYPE_BASE.get(insight.get("type", ""), 5)
    fame     = _fame_score(insight)
    relate   = _relatability_score(insight)
    wow      = _wow_score(insight)
    duo      = _duo_wow_boost(insight)
    headline = _headline_readiness(insight)
    total    = base + fame + relate + wow + duo + headline

    insight["_score_breakdown"] = {
        "base":          round(base,     1),
        "fame":          round(fame,     1),
        "relatability":  round(relate,   1),
        "wow":           round(wow,      1),
        "duo_wow":       round(duo,      1),
        "headline":      round(headline, 1),
        "total":         round(total,    1),
    }
    return total


# ── Hard filter ───────────────────────────────────────────────────────────────

def _hard_filter(candidates: list) -> list:
    """
    Remove insights that fail minimum quality bars regardless of score.

    Rules applied in order:
    1. Must have at least one actor_id — no ID means no URL, no portrait.
    2. Value must be a numeric type (int or float).  String values (old
       career_peak year ranges) render poorly and are dropped as a safety net.
    3. Per-type minimum value thresholds — removes low-magnitude candidates
       before scoring to avoid polluting the pool.
    4. collab_shock: both actors must have film_count >= 50 from _actor_stats.
       This filters primary-actor pairs who technically qualify but have thin
       filmographies that make the insight unimpressive.
    5. hidden_dominance with no _actor_stats: actor not in actor_stats table,
       skip rather than crash.
    """
    # Per-type minimum value to even enter scoring
    MIN_VALUE: dict = {
        "hidden_dominance":    150,   # 150+ supporting films (obscure actors below this)
        "collab_shock":         10,   # 10+ films together for a primary pair
        "network_power":       100,   # 100+ unique co-stars
        "cross_industry":        3,   # 3+ industries
        "career_peak":           6,   # 6+ films in the 5-year window
        "director_loyalty":     15,   # 15+ films with that director
        "director_box_office": 1000,  # 1000+ Cr total box office
    }

    filtered = []
    for ins in candidates:
        itype = ins.get("type", "")
        value = ins.get("value")

        # Rule 1: must have actor_id (director_box_office exempt — no actor in DB)
        if itype != "director_box_office" and not ins.get("actor_ids"):
            logger.debug("hard_filter: drop %s — no actor_ids", itype)
            continue

        # Rule 2: value must be numeric
        if not isinstance(value, (int, float)):
            logger.debug("hard_filter: drop %s — non-numeric value %r", itype, value)
            continue

        # Rule 3: per-type value floor
        floor = MIN_VALUE.get(itype, 0)
        if value < floor:
            logger.debug("hard_filter: drop %s value=%s < floor %s", itype, value, floor)
            continue

        # Rule 4: collab_shock — both actors need a substantial filmography
        if itype == "collab_shock":
            stats = ins.get("_actor_stats", [])
            if len(stats) >= 2 and any(s["film_count"] < 50 for s in stats):
                logger.debug(
                    "hard_filter: drop collab_shock %r — thin actor filmography %s",
                    ins.get("headline"), [s["film_count"] for s in stats],
                )
                continue

        # Rule 5: hidden_dominance must have resolved actor stats
        if itype == "hidden_dominance" and not ins.get("_actor_stats"):
            logger.debug("hard_filter: drop hidden_dominance %r — no actor_stats", ins.get("headline"))
            continue

        # Rule 6 [v6]: supporting actor exception.
        #   hidden_dominance with film_count >= 200 is allowed (shock value).
        #   All other insight types must have at least one primary actor.
        #   This prevents obscure non-primary actors from leaking into
        #   types (e.g. collab_shock) where they produce unrecognisable cards.
        if itype not in ("hidden_dominance", "director_box_office"):
            stats = ins.get("_actor_stats", [])
            if stats and not any(s["is_primary"] for s in stats):
                logger.debug(
                    "hard_filter: drop %s %r — no primary actor",
                    itype, ins.get("headline"),
                )
                continue
        else:
            # hidden_dominance: require film_count >= 200 for the shock to land
            stats = ins.get("_actor_stats", [])
            if stats and stats[0]["film_count"] < 200:
                logger.debug(
                    "hard_filter: drop hidden_dominance %r — film_count %d < 200",
                    ins.get("headline"), stats[0]["film_count"],
                )
                continue

        filtered.append(ins)

    logger.info("hard_filter: %d → %d candidates", len(candidates), len(filtered))
    return filtered


# ── Diversity picker ──────────────────────────────────────────────────────────

def _pick_diverse(candidates: list) -> list:
    """
    Select up to _MAX_INSIGHTS insights with equal industry representation.

    Algorithm:

    Pass 1 — Industry quota (Tamil / Telugu / Malayalam equal; Kannada best-effort):
      Score all candidates; filter by _MIN_SCORE floor.
      Target = _MAX_INSIGHTS // 3  cards per primary industry (Tamil/Telugu/Malayalam).
      Within each industry's pool: enforce category diversity (max 4 per category)
      so no single industry is all-network or all-collab.
      Kannada and Unknown (directors) fill up to their natural availability.

    Pass 2 — Gap fill:
      If total < _MAX_INSIGHTS after quotas, fill from any remaining eligible
      candidates (highest score first) until the cap is reached.

    Pass 3 — Supporting cap:
      At most 1 hidden_dominance insight in final output.
    """
    logger.info("_pick_diverse: %d candidates entering", len(candidates))

    for ins in candidates:
        s = _score(ins)
        ins["_score"] = s
        ins["confidence"] = round(min(1.0, s / 100), 3)
        bd = ins["_score_breakdown"]
        logger.debug(
            "score %-20s  total=%5.1f  fame=%4.1f  relate=%4.1f  wow=%4.1f  | %r",
            ins["type"], s, bd["fame"], bd["relatability"], bd["wow"],
            ins.get("headline"),
        )

    eligible = [ins for ins in candidates if ins["_score"] >= _MIN_SCORE]
    logger.info("_pick_diverse: %d above score floor %.0f", len(eligible), _MIN_SCORE)
    eligible.sort(key=lambda x: x["_score"], reverse=True)

    from collections import defaultdict

    # ── Pass 1: industry-quota selection ─────────────────────────────────────
    # Tamil, Telugu, Malayalam get equal slots. Kannada + Unknown fill naturally.
    PRIMARY_INDUSTRIES   = {"Tamil", "Telugu", "Malayalam"}
    SECONDARY_INDUSTRIES = {"Kannada"}
    # Per-category diversity caps within each industry pool.
    # With no hard total cap, these just prevent one category from
    # monopolising a single industry (e.g. 200 career_peak cards for Tamil).
    # The carousel round-robin interleave below further ensures variety.
    #
    # hidden_dominance (supporting) cards are capped separately so supporting
    # actors don't overwhelm the feed; Pass 3 enforces the same budget globally.
    MAX_CAT_PER_INDUSTRY        = 50   # generous — take almost everything per category
    MAX_SUPPORTING_PER_INDUSTRY =  6   # hidden_dominance per industry

    # No artificial quota — take everything available from each industry pool.
    per_industry_target = _MAX_INSIGHTS  # i.e. no per-industry cap either

    # Group eligible candidates by industry
    ind_pools: dict = defaultdict(list)
    for ins in eligible:
        ind = ins.get("industry") or "Unknown"
        ind_pools[ind].append(ins)

    result: list = []
    used_ids: set = set()

    def _fill_from_pool(pool: list, target: int) -> list:
        """Pick up to `target` cards from pool with per-category diversity cap."""
        cat_count: dict = {}
        picked = []
        for ins in pool:   # already score-sorted
            if len(picked) >= target:
                break
            cat = ins.get("category", "")
            cap = MAX_SUPPORTING_PER_INDUSTRY if cat == "supporting" else MAX_CAT_PER_INDUSTRY
            if cat_count.get(cat, 0) < cap:
                cat_count[cat] = cat_count.get(cat, 0) + 1
                picked.append(ins)
        return picked

    # Primary industries — equal quota
    for ind in PRIMARY_INDUSTRIES:
        picked = _fill_from_pool(ind_pools.get(ind, []), per_industry_target)
        result.extend(picked)
        used_ids.update(id(i) for i in picked)

    # Secondary (Kannada) — take all available
    for ind in SECONDARY_INDUSTRIES:
        picked = _fill_from_pool(ind_pools.get(ind, []), len(ind_pools.get(ind, [])))
        result.extend(picked)
        used_ids.update(id(i) for i in picked)

    # Unknown (directors, no industry) — take all available
    for ins in ind_pools.get("Unknown", []):
        if id(ins) not in used_ids:
            result.append(ins)
            used_ids.add(id(ins))

    # ── Pass 2: gap fill ──────────────────────────────────────────────────────
    if len(result) < _MAX_INSIGHTS:
        for ins in eligible:
            if len(result) >= _MAX_INSIGHTS:
                break
            if id(ins) not in used_ids:
                result.append(ins)
                used_ids.add(id(ins))

    result = result[:_MAX_INSIGHTS]

    # ── Pass 3: supporting cap ────────────────────────────────────────────────
    # Keep at most MAX_HIDDEN_DOMINANCE hidden_dominance cards total so supporting
    # actors don't crowd the feed.  Budget = 6 per primary industry × 3 = 18.
    # _fill_from_pool already capped each industry at MAX_SUPPORTING_PER_INDUSTRY=6.
    MAX_HIDDEN_DOMINANCE = 18
    supporting = [i for i in result if i.get("type") == "hidden_dominance"]
    if len(supporting) > MAX_HIDDEN_DOMINANCE:
        supporting.sort(key=lambda x: x["_score"], reverse=True)
        to_remove = set(id(i) for i in supporting[MAX_HIDDEN_DOMINANCE:])
        result = [i for i in result if id(i) not in to_remove]

    # ── Carousel ordering ─────────────────────────────────────────────────────
    #
    # Rules (applied in order):
    #   1. Single-avatar categories lead each round (one person = cleaner focus).
    #      Multi-avatar categories (pairs/duos) trail within every round.
    #   2. Within each avatar-group, order by top-card score so the strongest
    #      category of that group leads.
    #   3. Round-robin across categories — guarantees no same category back-to-back.
    #   4. Final dedup pass — if two same-category cards are still adjacent
    #      (can happen at round boundaries), swap the later one forward.
    #
    # Single-avatar categories: network, career, supporting, blockbuster, industry
    # Multi-avatar categories : collaboration, director

    _SINGLE_AVATAR_CATS = {"network", "career", "supporting", "blockbuster", "industry"}

    from collections import defaultdict
    buckets: dict = defaultdict(list)
    for ins in sorted(result, key=lambda x: x["_score"], reverse=True):
        buckets[ins.get("category", "other")].append(ins)

    # ── Sub-interleave each category bucket by industry ───────────────────────
    # Within each category bucket (e.g. all network_power cards), cards are
    # currently sorted by score — which front-loads one industry.
    # Re-order each bucket so industries alternate: Tamil → Malayalam → Telugu
    # → Kannada → Tamil → Malayalam → … (by each industry's best score).
    for cat in list(buckets.keys()):
        bucket = buckets[cat]   # already score-sorted
        ind_groups: dict = defaultdict(list)
        for ins in bucket:
            ind_groups[ins.get("industry") or "Unknown"].append(ins)

        # Industry order: highest-scoring industry first (ensures quality leads)
        ind_order = sorted(
            ind_groups.keys(),
            key=lambda i: ind_groups[i][0]["_score"], reverse=True,
        )

        # Round-robin across industries within this bucket
        new_bucket: list = []
        idx = 0
        while len(new_bucket) < len(bucket):
            added = False
            for ind in ind_order:
                if idx < len(ind_groups[ind]):
                    new_bucket.append(ind_groups[ind][idx])
                    added = True
            if not added:
                break
            idx += 1
        buckets[cat] = new_bucket

    # Sort categories: single-avatar first (by best score), then multi-avatar (by best score)
    single_cats = sorted(
        [c for c in buckets if c in _SINGLE_AVATAR_CATS],
        key=lambda c: buckets[c][0]["_score"], reverse=True,
    )
    multi_cats = sorted(
        [c for c in buckets if c not in _SINGLE_AVATAR_CATS],
        key=lambda c: buckets[c][0]["_score"], reverse=True,
    )
    cat_order = single_cats + multi_cats

    # Round-robin fill across categories
    interleaved: list = []
    round_idx = 0
    while len(interleaved) < len(result):
        added_this_round = 0
        for cat in cat_order:
            if round_idx < len(buckets[cat]):
                interleaved.append(buckets[cat][round_idx])
                added_this_round += 1
        if added_this_round == 0:
            break
        round_idx += 1

    # ── Safety pass: no same-category back-to-back ────────────────────────────
    # At round boundaries the last card of round N and first of round N+1 can
    # share a category if one bucket is larger.  Swap offenders forward.
    for i in range(1, len(interleaved)):
        if interleaved[i].get("category") == interleaved[i - 1].get("category"):
            # Find the next card with a different category and swap it here
            for j in range(i + 1, len(interleaved)):
                if interleaved[j].get("category") != interleaved[i - 1].get("category"):
                    interleaved[i], interleaved[j] = interleaved[j], interleaved[i]
                    break

    logger.info(
        "insights selected: %d  single-avatar-cats=%s  multi-avatar-cats=%s",
        len(interleaved), single_cats, multi_cats,
    )
    return interleaved


# ── Core computation (no cache) ───────────────────────────────────────────────

def compute_wow_insights(db: Session) -> list:
    """
    Run all WOW patterns → enrich with actor fame → hard-filter → score & pick.

    Pipeline:
      1. Run 6 patterns (fail-safe: broken pattern is skipped).
      2. _enrich_with_fame() — single bulk SQL, stamps _actor_stats on every candidate.
      3. _hard_filter()      — removes non-numeric values, thin filmographies, floors.
      4. _pick_diverse()     — scores (fame + relatability + wow), enforces
                               one-per-category, caps at _MAX_INSIGHTS.

    Returns an empty list if no patterns fire (triggers frontend fallback).
    """
    patterns = [
        _collaboration_shock,
        _hidden_dominance,
        _cross_industry_reach,
        _career_peak_window,
        _network_power,
        _director_loyalty,
        _director_box_office,
    ]

    candidates = []
    for pattern in patterns:
        try:
            results = pattern(db, limit=200)
            if results:
                candidates.extend(results)
        except Exception as e:
            db.rollback()
            logger.warning("insight pattern %s failed: %s", pattern.__name__, e)

    # Enrich all candidates with actor fame stats in one query
    try:
        _enrich_with_fame(candidates, db)
    except Exception as e:
        db.rollback()
        logger.warning("_enrich_with_fame failed: %s — proceeding without fame data", e)
        for ins in candidates:
            ins.setdefault("_actor_stats", [])

    # Hard-filter before scoring to keep the scoring pool clean
    candidates = _hard_filter(candidates)

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
