"""
Shared SQL fragments used by discovery rules.

The credits graph has TWO sources that must be UNIONed for completeness:
  - "cast"        (Wikidata, curated lead roles)
  - actor_movies  (TMDB, includes supporting roles + billing_order)

Movies flagged BROKEN by the validation pipeline are excluded everywhere.
"""

# All credits for primary actors (lead roles only) — the canonical idiom
# ported from backend/app/insight_engine.py::_career_peak_window.
PRIMARY_CREDITS_CTE = """
    all_credits AS (
        SELECT c.actor_id, c.movie_id
        FROM   "cast" c
        JOIN   actors a ON a.id = c.actor_id
        WHERE  a.is_primary_actor = TRUE
        UNION
        SELECT am.actor_id, am.movie_id
        FROM   actor_movies am
        JOIN   actors a ON a.id = am.actor_id
        WHERE  a.is_primary_actor = TRUE
          AND  am.role_type = 'primary'
    )
"""

# All credits for all actors (any role)
ALL_CREDITS_CTE = """
    all_credits AS (
        SELECT c.actor_id, c.movie_id FROM "cast" c
        UNION
        SELECT am.actor_id, am.movie_id FROM actor_movies am
    )
"""

# Guard: exclude movies the validation pipeline marked BROKEN
NOT_BROKEN = """
    NOT EXISTS (
        SELECT 1 FROM movie_validation_results mvr
        WHERE mvr.movie_id = m.id AND mvr.status = 'BROKEN'
    )
"""

SOUTH_INDUSTRIES = "('Telugu', 'Tamil', 'Malayalam', 'Kannada')"

# ── Year sanity ───────────────────────────────────────────────────────────────
# The movies table contains rows with release_year = 0 (463 at last audit) and
# other junk values. Every rule that touches years MUST use sane_year() instead
# of a bare IS NOT NULL — this is the single place the valid range is defined.

MIN_SANE_YEAR = 1900


def sane_year(column: str) -> str:
    """SQL predicate: `column` is a plausible historical release year.

    Rejects NULL, 0, negatives, pre-cinema years and future years
    (next year allowed — release calendars run ahead).
    """
    return (f"({column} IS NOT NULL AND {column} >= {MIN_SANE_YEAR} "
            f"AND {column} <= EXTRACT(YEAR FROM now())::int + 1)")
