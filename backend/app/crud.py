# crud.py
# Contains all database query logic (Create, Read, Update, Delete).
# The routes in main.py call these functions — keeping routes clean and simple.
#
# Sprint history:
#   Sprint 1-2 : get_all_actors, get_actor_by_name, get_movies_by_actor,
#                get_actor_stats (legacy — computes from movies table)
#   Sprint 6   : search_actors, get_actor_with_stats,
#                get_actor_movies_enriched, get_actor_collaborators,
#                get_actor_directors, get_actor_production,
#                get_actor_compare_stats, get_health_counts
#   Sprint 10  : get_top_collaborations
#   Sprint 15  : get_insights
#   Sprint 19  : get_top_directors, get_top_production_houses

from sqlalchemy.orm import Session
from sqlalchemy import func, text, case
from typing import Optional

from . import models, schemas


# ===========================================================================
# Core actor helpers  (used by multiple endpoints)
# ===========================================================================

def get_all_actors(
    db: Session,
    primary_only: bool = False,
    gender: Optional[str] = None,
):
    """Return actors from the database (used by GET /actors).

    primary_only=True  → only is_primary_actor=TRUE actors.
    gender='M'         → lead actors only.
    gender='F'         → lead actresses only.
    """
    query = db.query(models.Actor)
    if primary_only:
        query = query.filter(models.Actor.is_primary_actor == True)  # noqa: E712
    if gender is not None:
        query = query.filter(models.Actor.gender == gender)
    return query.all()


def get_actor_by_name(db: Session, name: str) -> Optional[models.Actor]:
    """
    Find a single actor by their exact name (case-insensitive).
    Uses the functional index idx_actors_name_lower for an indexed scan.
    """
    return (
        db.query(models.Actor)
        .filter(func.lower(models.Actor.name) == name.lower())
        .first()
    )


def get_actor_by_id(db: Session, actor_id: int) -> Optional[models.Actor]:
    """Return the Actor row for a given primary key, or None."""
    return db.query(models.Actor).filter(models.Actor.id == actor_id).first()


# ===========================================================================
# Search  (GET /actors/search?q=)
# ===========================================================================

def search_actors(db: Session, q: str, limit: int = 20, lead_only: bool = False) -> list:
    """
    Case-insensitive partial-match search on actors.name.

    Uses ILIKE '%q%' — with a leading wildcard a btree index cannot be used,
    but the table is small enough that a sequential scan completes in < 1 ms.
    Returns at most `limit` results (default 20).

    Ordering priority
    -----------------
    1. Exact name match (case-insensitive) — prevents "Aishwarya Rajinikanth"
       ranking above "Rajinikanth" when the query is "rajinikanth".
    2. Primary actors (is_primary_actor = True) — heroes before supporting cast.
    3. Alphabetical tiebreaker.

    When lead_only=True only actors with is_primary_actor=True are returned,
    filtering out supporting cast entirely.
    """
    exact_first = case(
        (func.lower(models.Actor.name) == q.lower(), 0),
        else_=1,
    )
    query = (
        db.query(models.Actor.id, models.Actor.name)
        .filter(models.Actor.name.ilike(f"%{q}%"))
    )
    if lead_only:
        query = query.filter(models.Actor.is_primary_actor == True)  # noqa: E712
    return (
        query
        .order_by(
            exact_first,
            models.Actor.is_primary_actor.desc(),
            models.Actor.name,
        )
        .limit(limit)
        .all()
    )


# ===========================================================================
# Actor profile  (GET /actors/{actor_id})
# ===========================================================================

def get_actor_with_stats(
    db: Session, actor_id: int
) -> Optional[tuple[models.Actor, Optional[models.ActorStats]]]:
    """
    Return (Actor, ActorStats | None) for the given actor_id.
    ActorStats is read from the precomputed actor_stats table (O(1) lookup).
    Returns None if the actor does not exist.
    """
    actor = get_actor_by_id(db, actor_id)
    if not actor:
        return None

    stats = (
        db.query(models.ActorStats)
        .filter(models.ActorStats.actor_id == actor_id)
        .first()
    )
    return actor, stats


# ===========================================================================
# Actor movies  (GET /actors/{actor_id}/movies)
# ===========================================================================

def get_movies_by_actor(db: Session, actor_id: int):
    """
    Return all movies an actor appeared in (legacy — used by seed_data
    and older tooling).  Not ordered; use get_actor_movies_enriched for
    the Sprint 6 endpoint.
    """
    return (
        db.query(models.Movie)
        .join(models.Cast, models.Cast.movie_id == models.Movie.id)
        .filter(models.Cast.actor_id == actor_id)
        .all()
    )


def get_actor_movies_enriched(db: Session, actor_id: int) -> list:
    """
    Return all movies for an actor, ordered by release_year DESC.

    Unions two sources so every pipeline path is covered:
      • "cast"        — Wikidata-sourced links (Sprints 1-5, original 13 actors)
      • actor_movies  — TMDB-sourced links   (Sprints 8-9, supporting + Malayalam)

    Returns full Movie objects so Pydantic can read enriched fields
    (runtime, production_company, language, poster_url, etc.).
    """
    from sqlalchemy import union, select

    cast_ids        = select(models.Cast.movie_id).where(
        models.Cast.actor_id == actor_id
    )
    actor_movie_ids = select(models.ActorMovie.movie_id).where(
        models.ActorMovie.actor_id == actor_id
    )
    all_movie_ids   = union(cast_ids, actor_movie_ids).scalar_subquery()

    return (
        db.query(models.Movie)
        .filter(models.Movie.id.in_(all_movie_ids))
        .order_by(models.Movie.release_year.desc())
        .all()
    )


# ===========================================================================
# Actor collaborators  (GET /actors/{actor_id}/collaborators)
# ===========================================================================

def get_actor_collaborators(db: Session, actor_id: int) -> list:
    """
    Return top co-stars for an actor, ordered by collaboration_count DESC.
    Reads from the precomputed actor_collaborations table (O(1) scan per actor).

    Returns a list of (name: str, collaboration_count: int) named tuples.
    Because both directions are stored in actor_collaborations, querying
    WHERE actor1_id = ? always returns the full collaborator set.

    Uses the idx_collab_actor1 index.
    """
    return (
        db.query(
            models.Actor.name,
            models.ActorCollaboration.collaboration_count,
        )
        .join(
            models.ActorCollaboration,
            models.ActorCollaboration.actor2_id == models.Actor.id,
        )
        .filter(models.ActorCollaboration.actor1_id == actor_id)
        .order_by(models.ActorCollaboration.collaboration_count.desc())
        .all()
    )


# ===========================================================================
# Actor directors  (GET /actors/{actor_id}/directors)
# ===========================================================================

def get_actor_directors(db: Session, actor_id: int) -> list:
    """
    Return all directors an actor has worked with, ordered by film_count DESC.
    Reads from the precomputed actor_director_stats table.
    Uses the idx_director_actor index.
    """
    return (
        db.query(models.ActorDirectorStat)
        .filter(models.ActorDirectorStat.actor_id == actor_id)
        .order_by(models.ActorDirectorStat.film_count.desc())
        .all()
    )


# ===========================================================================
# Actor production companies  (GET /actors/{actor_id}/production)
# ===========================================================================

def get_actor_production(db: Session, actor_id: int) -> list:
    """
    Return production companies an actor has worked with, ordered by
    film_count DESC. Reads from the precomputed actor_production_stats table.
    Uses the idx_production_actor index.
    """
    return (
        db.query(models.ActorProductionStat)
        .filter(models.ActorProductionStat.actor_id == actor_id)
        .order_by(models.ActorProductionStat.film_count.desc())
        .all()
    )


# ===========================================================================
# Actor comparison  (GET /compare?actor1=...&actor2=...)
# ===========================================================================

def get_actor_compare_stats(
    db: Session, actor_name: str
) -> Optional[tuple[models.Actor, models.ActorStats]]:
    """
    Look up an actor by name and return their precomputed stats.
    Both the actor row and the actor_stats row are required; if either is
    missing (e.g. the analytics tables haven't been built yet) returns None.

    This is the Sprint 6 compare helper — reads only two indexed rows per
    actor (actors PK + actor_stats PK), so the response is O(1).
    """
    actor = get_actor_by_name(db, actor_name)
    if not actor:
        return None

    stats = (
        db.query(models.ActorStats)
        .filter(models.ActorStats.actor_id == actor.id)
        .first()
    )
    if not stats:
        return None

    return actor, stats


# ===========================================================================
# Health  (GET /health)
# ===========================================================================

def get_health_counts(db: Session) -> tuple[int, int]:
    """
    Return (actor_count, movie_count) for the /health endpoint.
    Uses COUNT aggregates — fast even without dedicated indexes since
    PostgreSQL tracks table sizes in pg_class.
    """
    actor_count = db.query(func.count(models.Actor.id)).scalar() or 0
    movie_count = db.query(func.count(models.Movie.id)).scalar() or 0
    return actor_count, movie_count


# ===========================================================================
# Top collaborations  (GET /analytics/top-collaborations)
# ===========================================================================

def get_top_collaborations(db: Session, limit: int = 20) -> list:
    """
    Return the actor pairs with the most shared films, ranked descending.

    Data source
    -----------
    Reads from the **actor_collaborations** precomputed table, which is built
    by `build_analytics_tables.py` from the union of both ingestion pipelines:
        • "cast"        — Wikidata-sourced links (Sprints 1-5, 13 original actors)
        • actor_movies  — TMDB-sourced links   (Sprints 8-9, supporting + Malayalam)

    Using the precomputed table (O(1) scan) instead of re-joining actor_movies
    at query time keeps response latency sub-millisecond regardless of dataset size.

    The table stores both directions (A→B and B→A).  The WHERE clause
    ``actor1_id < actor2_id`` selects only one canonical direction per pair,
    eliminating duplicates without a DISTINCT pass.

    Parameters
    ----------
    db    : Active SQLAlchemy session.
    limit : Maximum rows to return (default 20, passed as query parameter).

    Returns
    -------
    List of Row objects, each with fields: actor_1 (str), actor_2 (str), films (int).
    Ordered by films DESC.

    Example
    -------
    >>> rows = get_top_collaborations(db, limit=5)
    >>> rows[0]
    ('Mohanlal', 'Jagathy Sreekumar', 60)
    """
    sql = text("""
        SELECT
            a1.name                          AS actor_1,
            a2.name                          AS actor_2,
            ac.collaboration_count           AS films
        FROM   actor_collaborations ac
        JOIN   actors a1 ON ac.actor1_id = a1.id
        JOIN   actors a2 ON ac.actor2_id = a2.id
        WHERE  ac.actor1_id < ac.actor2_id
        ORDER  BY ac.collaboration_count DESC
        LIMIT  :lim
    """)
    result = db.execute(sql, {"lim": limit})
    return result.fetchall()


# ===========================================================================
# Insight engine  (GET /analytics/insights)
# ===========================================================================

def get_insights(db: Session, industry: Optional[str] = None) -> list:
    """
    Return WOW insight cards for the homepage.

    Delegates entirely to insight_engine.get_wow_insights(), which runs six
    story-driven patterns (collaboration shock, hidden dominance, cross-industry
    reach, career peak window, network power, director loyalty) and returns the
    top 3 diverse, high-impact insights.

    The ``industry`` parameter is accepted for API compatibility but the WOW
    engine intentionally operates on the full cross-industry dataset — the most
    surprising insights emerge from unexpected connections across languages.

    Returns
    -------
    list of plain dicts — converted to Insight Pydantic models in the route.
    Empty list triggers the frontend's static fallback cards.
    """
    from .insight_engine import get_wow_insights
    return get_wow_insights(db)


# ===========================================================================
# Shared films  (GET /actors/{actor1_id}/shared/{actor2_id})
# ===========================================================================

def get_shared_films(db: Session, actor1_id: int, actor2_id: int) -> list:
    """
    Return movies that both actors have appeared in together, ordered newest-first.

    Searches across both ingestion pipelines:
      • "cast"        — Wikidata-sourced links (original 13 actors);
                        has role_type but no character_name
      • actor_movies  — TMDB-sourced links (supporting + Malayalam);
                        has role_type AND character_name

    Uses LEFT JOINs on both tables for each actor so we can collect their
    role_type and character_name in the same row.  COALESCE prefers the
    TMDB data (actor_movies) since it has richer character info.

    A movie qualifies only if at least one link for each actor exists —
    it does not matter which pipeline produced each link.
    """
    sql = text("""
        SELECT
            m.title,
            m.release_year,
            m.director,
            m.poster_url,
            m.vote_average,
            m.popularity,
            -- Actor 1: prefer TMDB character name; fall back to Wikidata role_type
            COALESCE(am1.character_name)                        AS actor1_character,
            COALESCE(am1.role_type, c1.role_type)               AS actor1_role,
            -- Actor 2: same
            COALESCE(am2.character_name)                        AS actor2_character,
            COALESCE(am2.role_type, c2.role_type)               AS actor2_role
        FROM   movies m
        LEFT JOIN actor_movies am1 ON am1.movie_id = m.id AND am1.actor_id = :a1
        LEFT JOIN "cast"       c1  ON c1.movie_id  = m.id AND c1.actor_id  = :a1
        LEFT JOIN actor_movies am2 ON am2.movie_id = m.id AND am2.actor_id = :a2
        LEFT JOIN "cast"       c2  ON c2.movie_id  = m.id AND c2.actor_id  = :a2
        WHERE (am1.actor_id IS NOT NULL OR c1.actor_id IS NOT NULL)
          AND (am2.actor_id IS NOT NULL OR c2.actor_id IS NOT NULL)
        ORDER BY m.release_year DESC
    """)
    return db.execute(sql, {"a1": actor1_id, "a2": actor2_id}).fetchall()


# ===========================================================================
# Legacy helpers  (retained for backward compatibility)
# ===========================================================================

def get_actor_stats(db: Session, actor_name: str) -> Optional[schemas.LegacyActorStats]:
    """
    Compute analytics for a single actor on the fly from the movies table.

    LEGACY — originally used by /compare (Sprint 1-2). Kept so any tooling
    that imports this function still works. New endpoints should call
    get_actor_compare_stats() which reads from the precomputed actor_stats
    table and is significantly faster.
    """
    actor = get_actor_by_name(db, actor_name)
    if not actor:
        return None

    movies = get_movies_by_actor(db, actor.id)
    total_movies = len(movies)

    ratings = [m.imdb_rating for m in movies if m.imdb_rating is not None]
    avg_rating = round(sum(ratings) / len(ratings), 2) if ratings else None

    movies_after_2015 = sum(1 for m in movies if m.release_year > 2015)

    box_offices = [m.box_office for m in movies if m.box_office is not None]
    avg_box_office = round(sum(box_offices) / len(box_offices), 2) if box_offices else None

    return schemas.LegacyActorStats(
        name=actor.name,
        total_movies=total_movies,
        avg_rating=avg_rating,
        movies_after_2015=movies_after_2015,
        avg_box_office=avg_box_office,
    )


# ===========================================================================
# Top directors  (GET /analytics/directors)  Sprint 19
# ===========================================================================

def get_top_directors(
    db: Session, industry: Optional[str] = None, limit: int = 30
) -> list:
    """
    Return directors ranked by number of films in the database.

    Data source
    -----------
    Reads from the legacy ``movies.director`` TEXT column — the denormalized
    field that is always populated.  The normalised ``movie_directors`` join
    table is more correct but only covers a subset of movies; this query uses
    the legacy column for maximum coverage.

    Filtering
    ---------
    industry : optional case-insensitive match against ``movies.industry``.
               Pass None / "all" / "explore" for the cross-industry view.

    Deduplication
    -------------
    Only directors with ≥ 2 films are included — eliminates one-off credits
    that pollute the leaderboard with unique names.

    Returns
    -------
    List of Row objects with fields: name (str), film_count (int),
    industries (str — comma-separated, e.g. "Telugu, Tamil").
    Ordered by film_count DESC.
    """
    ind = (
        industry.lower()
        if industry and industry.lower() not in ("all", "explore")
        else None
    )
    sql = text("""
        SELECT
            m.director                               AS name,
            COUNT(*)                                 AS film_count,
            STRING_AGG(DISTINCT m.industry, ', ')    AS industries
        FROM   movies m
        WHERE  m.director IS NOT NULL
          AND  m.director <> ''
          AND  (:ind IS NULL OR LOWER(m.industry) = :ind)
        GROUP  BY m.director
        HAVING COUNT(*) >= 2
        ORDER  BY film_count DESC
        LIMIT  :lim
    """)
    return db.execute(sql, {"ind": ind, "lim": limit}).fetchall()


# ===========================================================================
# Top production houses  (GET /analytics/production-houses)  Sprint 19
# ===========================================================================

def get_top_production_houses(
    db: Session, industry: Optional[str] = None, limit: int = 20
) -> list:
    """
    Return production companies ranked by number of films in the database.

    Data source
    -----------
    Reads from ``movies.production_company`` — populated by enrich_movies.py
    via Wikipedia infobox scraping.  Rows where the column is NULL or empty
    are excluded.

    Filtering
    ---------
    industry : optional case-insensitive match against ``movies.industry``.

    Deduplication
    -------------
    Only companies with ≥ 2 films are included.

    Returns
    -------
    List of Row objects with fields: name (str), film_count (int),
    industries (str — comma-separated).
    Ordered by film_count DESC.
    """
    ind = (
        industry.lower()
        if industry and industry.lower() not in ("all", "explore")
        else None
    )
    sql = text("""
        SELECT
            m.production_company                     AS name,
            COUNT(*)                                 AS film_count,
            STRING_AGG(DISTINCT m.industry, ', ')    AS industries
        FROM   movies m
        WHERE  m.production_company IS NOT NULL
          AND  m.production_company <> ''
          AND  (:ind IS NULL OR LOWER(m.industry) = :ind)
        GROUP  BY m.production_company
        HAVING COUNT(*) >= 2
        ORDER  BY film_count DESC
        LIMIT  :lim
    """)
    return db.execute(sql, {"ind": ind, "lim": limit}).fetchall()


# ===========================================================================
# Sprint 21 — Stats for Nerds  (GET /stats/*)
# ===========================================================================

def get_stats_overview(db: Session) -> dict:
    """Global DB snapshot: movies, actors, links, industries."""
    total_movies = db.execute(text(
        "SELECT COUNT(*) FROM movies WHERE industry IN ('Tamil','Telugu','Malayalam','Kannada')"
    )).scalar()
    total_actors = db.execute(text(
        "SELECT COUNT(*) FROM actors WHERE actor_tier IN ('primary','network')"
    )).scalar()
    # Count distinct (actor, movie) pairs across both ingestion pipelines
    total_links = db.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT actor_id, movie_id FROM "cast"
            UNION
            SELECT actor_id, movie_id FROM actor_movies
        ) all_credits
    """)).scalar()
    return {
        "total_movies":  total_movies,
        "total_actors":  total_actors,
        "total_links":   total_links,
        "industries":    4,
    }


def get_most_connected_actors(db: Session, limit: int = 25) -> list:
    """Actors ranked by number of unique co-stars (primary + network tier only)."""
    rows = db.execute(text("""
        WITH all_credits AS (
            SELECT actor_id, movie_id FROM "cast"
            UNION
            SELECT actor_id, movie_id FROM actor_movies
        ),
        costar_counts AS (
            SELECT ac1.actor_id,
                   COUNT(DISTINCT ac2.actor_id) AS unique_costars
            FROM all_credits ac1
            JOIN all_credits ac2
              ON ac1.movie_id = ac2.movie_id AND ac1.actor_id != ac2.actor_id
            GROUP BY ac1.actor_id
        ),
        film_counts AS (
            SELECT actor_id, COUNT(DISTINCT movie_id) AS film_count
            FROM all_credits
            GROUP BY actor_id
        )
        SELECT a.id, a.name, a.industry, a.actor_tier,
               COALESCE(cc.unique_costars, 0) AS unique_costars,
               COALESCE(fc.film_count, 0)     AS film_count
        FROM actors a
        LEFT JOIN costar_counts cc ON cc.actor_id = a.id
        LEFT JOIN film_counts   fc ON fc.actor_id = a.id
        WHERE a.actor_tier IN ('primary','network')
        ORDER BY unique_costars DESC
        LIMIT :lim
    """), {"lim": limit}).fetchall()
    return [
        {"id": r[0], "name": r[1], "industry": r[2], "tier": r[3],
         "unique_costars": r[4], "film_count": r[5]}
        for r in rows
    ]


def get_industry_distribution(db: Session) -> list:
    """Film counts per South Indian industry, including per-decade breakdown."""
    rows = db.execute(text("""
        SELECT industry,
               COUNT(*)                                      AS total,
               COUNT(*) FILTER (WHERE release_year < 1980)  AS pre_1980,
               COUNT(*) FILTER (WHERE release_year BETWEEN 1980 AND 1999) AS s1980s,
               COUNT(*) FILTER (WHERE release_year BETWEEN 2000 AND 2009) AS s2000s,
               COUNT(*) FILTER (WHERE release_year BETWEEN 2010 AND 2019) AS s2010s,
               COUNT(*) FILTER (WHERE release_year >= 2020)               AS s2020s
        FROM movies
        WHERE industry IN ('Tamil','Telugu','Malayalam','Kannada')
        GROUP BY industry
        ORDER BY total DESC
    """)).fetchall()
    return [
        {"industry": r[0], "total": r[1],
         "pre_1980": r[2], "s1980s": r[3], "s2000s": r[4],
         "s2010s": r[5], "s2020s": r[6]}
        for r in rows
    ]


def get_top_director_partnerships(db: Session, limit: int = 15) -> list:
    """Most prolific actor–director pairs (requires movies.director column)."""
    rows = db.execute(text("""
        WITH all_credits AS (
            SELECT actor_id, movie_id FROM "cast"
            UNION
            SELECT actor_id, movie_id FROM actor_movies
        )
        SELECT a.name                               AS actor_name,
               m.director,
               COUNT(*)                            AS film_count,
               MAX(m.industry)                     AS industry,
               array_agg(m.title ORDER BY m.release_year DESC) AS films
        FROM all_credits ac
        JOIN actors a ON a.id = ac.actor_id
        JOIN movies m  ON m.id = ac.movie_id
        WHERE m.director IS NOT NULL
          AND m.director != ''
          AND a.actor_tier IN ('primary','network')
        GROUP BY a.name, m.director
        HAVING COUNT(*) >= 3
        ORDER BY film_count DESC
        LIMIT :lim
    """), {"lim": limit}).fetchall()
    return [
        {"actor": r[0], "director": r[1], "film_count": r[2],
         "industry": r[3], "films": list(r[4])[:5]}
        for r in rows
    ]


def get_career_timeline(db: Session, actor_id: int) -> list:
    """Films per year for a given actor (unions Wikidata cast + TMDB actor_movies)."""
    rows = db.execute(text("""
        WITH all_credits AS (
            SELECT actor_id, movie_id FROM "cast"
            UNION
            SELECT actor_id, movie_id FROM actor_movies
        )
        SELECT m.release_year AS year, COUNT(*) AS count
        FROM movies m
        JOIN all_credits ac ON ac.movie_id = m.id
        WHERE ac.actor_id = :aid
          AND m.release_year > 1950
          AND m.release_year <= EXTRACT(YEAR FROM NOW())::int
        GROUP BY m.release_year
        ORDER BY m.release_year
    """), {"aid": actor_id}).fetchall()
    return [{"year": r[0], "count": r[1]} for r in rows]


def get_top_costars(db: Session, limit: int = 15) -> list:
    """Actors with the highest unique co-star counts across all tiers."""
    rows = db.execute(text("""
        WITH all_credits AS (
            SELECT actor_id, movie_id FROM "cast"
            UNION
            SELECT actor_id, movie_id FROM actor_movies
        ),
        costar_counts AS (
            SELECT ac1.actor_id,
                   COUNT(DISTINCT ac2.actor_id) AS unique_costars,
                   COUNT(DISTINCT ac1.movie_id) AS film_count
            FROM all_credits ac1
            JOIN all_credits ac2
              ON ac1.movie_id = ac2.movie_id AND ac1.actor_id != ac2.actor_id
            GROUP BY ac1.actor_id
        )
        SELECT a.id, a.name, a.industry, cc.unique_costars, cc.film_count
        FROM actors a
        JOIN costar_counts cc ON cc.actor_id = a.id
        WHERE a.actor_tier IN ('primary','network')
        ORDER BY cc.unique_costars DESC
        LIMIT :lim
    """), {"lim": limit}).fetchall()
    return [
        {"id": r[0], "name": r[1], "industry": r[2],
         "unique_costars": r[3], "film_count": r[4]}
        for r in rows
    ]


def find_actor_connection(db: Session, actor1_id: int, actor2_id: int,
                          max_depth: int = 6) -> dict:
    """
    BFS shortest path between two actors through the collaboration graph.
    Returns path (list of actors) + connecting movies for each edge.
    """
    from collections import defaultdict, deque

    # Build adjacency list: actor_id -> {neighbor_id: (movie_id, movie_title)}
    # One movie per pair, chosen by highest popularity.
    # Unions both Wikidata (cast) and TMDB (actor_movies) pipelines so the
    # original 13 seed actors' early-career films are included in the graph.
    rows = db.execute(text("""
        WITH all_credits AS (
            SELECT actor_id, movie_id FROM "cast"
            UNION
            SELECT actor_id, movie_id FROM actor_movies
        )
        SELECT DISTINCT ON (ac1.actor_id, ac2.actor_id)
            ac1.actor_id, ac2.actor_id, m.id, m.title
        FROM all_credits ac1
        JOIN all_credits ac2
          ON ac1.movie_id = ac2.movie_id AND ac1.actor_id < ac2.actor_id
        JOIN movies m ON m.id = ac1.movie_id
        ORDER BY ac1.actor_id, ac2.actor_id, m.popularity DESC NULLS LAST
    """)).fetchall()

    graph: dict[int, dict[int, tuple]] = defaultdict(dict)
    for a, b, mid, mtitle in rows:
        graph[a][b] = (mid, mtitle or "Unknown")
        graph[b][a] = (mid, mtitle or "Unknown")

    if actor1_id == actor2_id:
        row = db.execute(text("SELECT id, name FROM actors WHERE id=:id"),
                         {"id": actor1_id}).fetchone()
        return {"found": True, "depth": 0,
                "path": [{"id": row[0], "name": row[1]}], "connections": []}

    # BFS
    visited = {actor1_id}
    prev: dict[int, tuple] = {}   # neighbor -> (from_actor_id, movie_id, movie_title)
    queue = deque([actor1_id])
    found = False

    while queue and not found:
        current = queue.popleft()
        if len(prev) > 500_000:   # safety cap
            break
        for neighbor, (mid, mtitle) in graph[current].items():
            if neighbor not in visited:
                visited.add(neighbor)
                prev[neighbor] = (current, mid, mtitle)
                if neighbor == actor2_id:
                    found = True
                    break
                queue.append(neighbor)

    if not found:
        return {"found": False, "depth": -1, "path": [], "connections": []}

    # Reconstruct path
    path_ids, connections = [], []
    cur = actor2_id
    while cur in prev:
        prev_actor, movie_id, movie_title = prev[cur]
        path_ids.insert(0, cur)
        connections.insert(0, {"movie_id": movie_id, "movie_title": movie_title})
        cur = prev_actor
    path_ids.insert(0, actor1_id)

    # Fetch actor names
    actor_rows = db.execute(
        text("SELECT id, name FROM actors WHERE id = ANY(:ids)"),
        {"ids": path_ids}
    ).fetchall()
    name_map = {r[0]: r[1] for r in actor_rows}

    return {
        "found": True,
        "depth": len(path_ids) - 1,
        "path": [{"id": aid, "name": name_map.get(aid, "?")} for aid in path_ids],
        "connections": connections,
    }


# ===========================================================================
# Sprint 22 — Build Your Own Chart / Cinema Universe / Gravity Center
# ===========================================================================

def get_chart_data(
    db: Session,
    x_axis: str,
    y_axis: str,
    actor_ids: list[int],
    industry: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
) -> dict:
    """
    Flexible chart data for the Build Your Own Chart feature.

    x_axis: 'year' | 'decade' | 'actor' | 'industry' | 'director'
    y_axis: 'film_count' | 'avg_rating' | 'unique_costars'
            | 'director_collaborations' | 'total_collaborations'

    Returns {"chart_type": "line"|"bar", "series": [...]}
    """
    # ── helpers ──────────────────────────────────────────────────────────────
    industry_filter = "AND m.industry = :industry" if industry else ""
    year_from_filter = "AND m.release_year >= :year_from" if year_from else ""
    year_to_filter   = "AND m.release_year <= :year_to"   if year_to   else ""
    year_filters = f"{year_from_filter} {year_to_filter}"

    params: dict = {"actor_ids": actor_ids}
    if industry:   params["industry"]  = industry
    if year_from:  params["year_from"] = year_from
    if year_to:    params["year_to"]   = year_to

    def actor_name_map() -> dict:
        rows = db.execute(
            text("SELECT id, name FROM actors WHERE id = ANY(:ids)"),
            {"ids": actor_ids}
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    # ── y-axis SQL fragments ──────────────────────────────────────────────────
    Y_SQL = {
        "film_count":               "COUNT(DISTINCT am.movie_id)",
        "avg_rating":               "ROUND((AVG(m.vote_average) FILTER (WHERE m.vote_average > 0))::numeric, 2)",
        "director_collaborations":  "COUNT(DISTINCT m.director) FILTER (WHERE m.director IS NOT NULL)",
        # Box office / budget
        "avg_box_office":           "ROUND((AVG(m.box_office) FILTER (WHERE m.box_office IS NOT NULL AND m.box_office > 0))::numeric, 1)",
        "total_box_office":         "ROUND((SUM(m.box_office) FILTER (WHERE m.box_office IS NOT NULL AND m.box_office > 0))::numeric, 1)",
        "avg_budget":               "ROUND((AVG(m.budget_crore) FILTER (WHERE m.budget_crore IS NOT NULL AND m.budget_crore > 0))::numeric, 1)",
        # Quality / reach
        "avg_runtime":              "ROUND((AVG(m.runtime) FILTER (WHERE m.runtime IS NOT NULL AND m.runtime > 0))::numeric, 1)",
        "avg_popularity":           "ROUND((AVG(m.popularity) FILTER (WHERE m.popularity > 0))::numeric, 2)",
        "hit_rate":                 "ROUND(100.0 * COUNT(*) FILTER (WHERE m.vote_average >= 7.0) / NULLIF(COUNT(*) FILTER (WHERE m.vote_average > 0), 0), 1)",
        "unique_directors":         "COUNT(DISTINCT m.director) FILTER (WHERE m.director IS NOT NULL)",
    }
    # unique_costars and total_collaborations need a different join
    COSTAR_Y = y_axis in ("unique_costars", "total_collaborations")

    # Shared CTE snippet used in every branch — unions Wikidata (cast) +
    # TMDB (actor_movies) so original seed actors' early films are included.
    ALL_CREDITS_CTE = """
        WITH all_credits AS (
            SELECT actor_id, movie_id FROM "cast"
            UNION
            SELECT actor_id, movie_id FROM actor_movies
        )
    """

    # ── x = year → LINE chart ────────────────────────────────────────────────
    if x_axis == "year":
        name_map = actor_name_map()
        series = []
        for aid in actor_ids:
            p = {"aid": aid, **params}
            if COSTAR_Y:
                agg = "COUNT(DISTINCT am2.actor_id)" if y_axis == "unique_costars" else "COUNT(am2.actor_id)"
                rows = db.execute(text(f"""
                    {ALL_CREDITS_CTE}
                    SELECT m.release_year, {agg}
                    FROM all_credits am
                    JOIN movies m ON m.id = am.movie_id
                    JOIN all_credits am2 ON am2.movie_id = am.movie_id AND am2.actor_id != am.actor_id
                    WHERE am.actor_id = :aid
                    {industry_filter} {year_filters}
                    AND m.release_year BETWEEN 1950 AND EXTRACT(YEAR FROM NOW())::int
                    GROUP BY m.release_year ORDER BY m.release_year
                """), p).fetchall()
            else:
                agg = Y_SQL[y_axis]
                rows = db.execute(text(f"""
                    {ALL_CREDITS_CTE}
                    SELECT m.release_year, {agg}
                    FROM all_credits am
                    JOIN movies m ON m.id = am.movie_id
                    WHERE am.actor_id = :aid
                    {industry_filter} {year_filters}
                    AND m.release_year BETWEEN 1950 AND EXTRACT(YEAR FROM NOW())::int
                    GROUP BY m.release_year ORDER BY m.release_year
                """), p).fetchall()
            series.append({
                "actor_id": aid,
                "actor_name": name_map.get(aid, "?"),
                "points": [{"x": r[0], "y": float(r[1]) if r[1] else 0} for r in rows],
            })
        return {"chart_type": "line", "series": series}

    # ── x = decade → BAR chart ───────────────────────────────────────────────
    if x_axis == "decade":
        DECADE_LABELS = ["Pre-1980","1980s","1990s","2000s","2010s","2020s"]
        DECADE_CASE = """
            CASE WHEN m.release_year < 1980 THEN 'Pre-1980'
                 WHEN m.release_year < 1990 THEN '1980s'
                 WHEN m.release_year < 2000 THEN '1990s'
                 WHEN m.release_year < 2010 THEN '2000s'
                 WHEN m.release_year < 2020 THEN '2010s'
                 ELSE '2020s' END
        """
        name_map = actor_name_map()
        series = []
        for aid in actor_ids:
            p = {"aid": aid, **params}
            if COSTAR_Y:
                agg = "COUNT(DISTINCT am2.actor_id)" if y_axis == "unique_costars" else "COUNT(am2.actor_id)"
                rows = db.execute(text(f"""
                    {ALL_CREDITS_CTE}
                    SELECT {DECADE_CASE} AS decade, {agg}
                    FROM all_credits am
                    JOIN movies m ON m.id = am.movie_id
                    JOIN all_credits am2 ON am2.movie_id = am.movie_id AND am2.actor_id != am.actor_id
                    WHERE am.actor_id = :aid {industry_filter}
                    GROUP BY decade
                """), p).fetchall()
            else:
                agg = Y_SQL[y_axis]
                rows = db.execute(text(f"""
                    {ALL_CREDITS_CTE}
                    SELECT {DECADE_CASE} AS decade, {agg}
                    FROM all_credits am
                    JOIN movies m ON m.id = am.movie_id
                    WHERE am.actor_id = :aid {industry_filter}
                    GROUP BY decade
                """), p).fetchall()
            d = {r[0]: float(r[1]) if r[1] else 0 for r in rows}
            series.append({
                "actor_id": aid,
                "actor_name": name_map.get(aid, "?"),
                "points": [{"x": lbl, "y": d.get(lbl, 0)} for lbl in DECADE_LABELS],
            })
        return {"chart_type": "bar", "series": series}

    # ── x = actor → single-series BAR chart ──────────────────────────────────
    if x_axis == "actor":
        name_map = actor_name_map()
        series = []
        for aid in actor_ids:
            p = {"aid": aid, **params}
            if COSTAR_Y:
                agg = "COUNT(DISTINCT am2.actor_id)" if y_axis == "unique_costars" else "COUNT(am2.actor_id)"
                row = db.execute(text(f"""
                    {ALL_CREDITS_CTE}
                    SELECT {agg}
                    FROM all_credits am
                    JOIN movies m ON m.id = am.movie_id
                    JOIN all_credits am2 ON am2.movie_id = am.movie_id AND am2.actor_id != am.actor_id
                    WHERE am.actor_id = :aid {industry_filter} {year_filters}
                """), p).fetchone()
            else:
                agg = Y_SQL[y_axis]
                row = db.execute(text(f"""
                    {ALL_CREDITS_CTE}
                    SELECT {agg}
                    FROM all_credits am
                    JOIN movies m ON m.id = am.movie_id
                    WHERE am.actor_id = :aid {industry_filter} {year_filters}
                """), p).fetchone()
            series.append({
                "actor_id": aid,
                "actor_name": name_map.get(aid, "?"),
                "points": [{"x": name_map.get(aid, "?"), "y": float(row[0]) if row and row[0] else 0}],
            })
        return {"chart_type": "bar", "series": series}

    # ── x = industry → per-industry BAR chart ────────────────────────────────
    if x_axis == "industry":
        INDUSTRIES = ["Tamil", "Telugu", "Malayalam", "Kannada"]
        name_map = actor_name_map()
        series = []
        for aid in actor_ids:
            p = {"aid": aid, **params}
            if COSTAR_Y:
                agg = "COUNT(DISTINCT am2.actor_id)" if y_axis == "unique_costars" else "COUNT(am2.actor_id)"
                rows = db.execute(text(f"""
                    {ALL_CREDITS_CTE}
                    SELECT m.industry, {agg}
                    FROM all_credits am
                    JOIN movies m ON m.id = am.movie_id
                    JOIN all_credits am2 ON am2.movie_id = am.movie_id AND am2.actor_id != am.actor_id
                    WHERE am.actor_id = :aid AND m.industry = ANY(:inds) {year_filters}
                    GROUP BY m.industry ORDER BY m.industry
                """), {**p, "inds": INDUSTRIES}).fetchall()
            else:
                agg = Y_SQL[y_axis]
                rows = db.execute(text(f"""
                    {ALL_CREDITS_CTE}
                    SELECT m.industry, {agg}
                    FROM all_credits am
                    JOIN movies m ON m.id = am.movie_id
                    WHERE am.actor_id = :aid AND m.industry = ANY(:inds) {year_filters}
                    GROUP BY m.industry ORDER BY m.industry
                """), {**p, "inds": INDUSTRIES}).fetchall()
            d = {r[0]: float(r[1]) if r[1] else 0 for r in rows}
            series.append({
                "actor_id": aid,
                "actor_name": name_map.get(aid, "?"),
                "points": [{"x": ind, "y": d.get(ind, 0)} for ind in INDUSTRIES],
            })
        return {"chart_type": "bar", "series": series}

    # ── x = director → top directors by Y, for selected actors ───────────────
    if x_axis == "director":
        # director_collaborations per-director is always 1 — fallback to film_count
        if COSTAR_Y or y_axis == "director_collaborations":
            y_axis = "film_count"
        agg = Y_SQL.get(y_axis, "COUNT(DISTINCT am.movie_id)")
        rows = db.execute(text(f"""
            {ALL_CREDITS_CTE}
            SELECT m.director, a.id, a.name, {agg}
            FROM all_credits am
            JOIN movies m ON m.id = am.movie_id
            JOIN actors a ON a.id = am.actor_id
            WHERE am.actor_id = ANY(:actor_ids)
              AND m.director IS NOT NULL AND m.director != ''
              {industry_filter} {year_filters}
            GROUP BY m.director, a.id, a.name
            ORDER BY 4 DESC
            LIMIT 60
        """), params).fetchall()
        # Pivot: group by director across actors
        from collections import defaultdict
        dir_data: dict = defaultdict(dict)
        for director, aid, aname, val in rows:
            dir_data[director][aid] = float(val) if val else 0
        # Top 15 directors by sum of all actors
        top_dirs = sorted(dir_data.items(), key=lambda kv: sum(kv[1].values()), reverse=True)[:15]
        name_map = actor_name_map()
        series = []
        for aid in actor_ids:
            series.append({
                "actor_id": aid,
                "actor_name": name_map.get(aid, "?"),
                "points": [{"x": d, "y": dir_data[d].get(aid, 0)} for d, _ in top_dirs],
            })
        return {"chart_type": "bar", "series": series}

    return {"chart_type": "bar", "series": []}


def get_cinema_universe(db: Session, min_shared_films: int = 2) -> dict:
    """
    Returns nodes (primary actors) and edges (shared-film pairs) for the
    force-directed Cinema Universe graph.
    Scoped to primary actors only to keep the graph fast and readable.
    """
    # Nodes: primary actors with costar counts
    node_rows = db.execute(text("""
        WITH all_credits AS (
            SELECT actor_id, movie_id FROM "cast"
            UNION
            SELECT actor_id, movie_id FROM actor_movies
        )
        SELECT a.id, a.name, a.industry,
               COUNT(DISTINCT ac.movie_id)  AS film_count,
               COUNT(DISTINCT ac2.actor_id) AS costar_count
        FROM actors a
        JOIN all_credits ac  ON ac.actor_id  = a.id
        JOIN all_credits ac2 ON ac2.movie_id = ac.movie_id
                             AND ac2.actor_id != a.id
        WHERE a.actor_tier = 'primary'
        GROUP BY a.id, a.name, a.industry
        ORDER BY costar_count DESC
    """)).fetchall()
    nodes = [
        {"id": r[0], "name": r[1], "industry": r[2] or "Unknown",
         "film_count": r[3], "costar_count": r[4]}
        for r in node_rows
    ]
    node_ids = {r[0] for r in node_rows}

    # Edges: pairs of primary actors with ≥ min_shared_films together
    edge_rows = db.execute(text("""
        WITH all_credits AS (
            SELECT actor_id, movie_id FROM "cast"
            UNION
            SELECT actor_id, movie_id FROM actor_movies
        )
        SELECT ac1.actor_id AS source,
               ac2.actor_id AS target,
               COUNT(*)     AS weight
        FROM all_credits ac1
        JOIN all_credits ac2
          ON ac1.movie_id = ac2.movie_id AND ac1.actor_id < ac2.actor_id
        JOIN actors a1 ON a1.id = ac1.actor_id AND a1.actor_tier = 'primary'
        JOIN actors a2 ON a2.id = ac2.actor_id AND a2.actor_tier = 'primary'
        GROUP BY ac1.actor_id, ac2.actor_id
        HAVING COUNT(*) >= :min_films
        ORDER BY weight DESC
    """), {"min_films": min_shared_films}).fetchall()
    edges = [{"source": r[0], "target": r[1], "weight": r[2]} for r in edge_rows]

    return {"nodes": nodes, "edges": edges}


def get_gravity_center(db: Session, limit: int = 25) -> list:
    """
    Compute approximate betweenness centrality (Brandes algorithm) on the
    collaboration graph of ingested actors, and return the top `limit` actors
    ranked by centrality score.
    """
    from collections import deque, defaultdict

    # Build adjacency list for primary actors only (keeps graph small and fast)
    # Unions both Wikidata (cast) and TMDB (actor_movies) pipelines.
    rows = db.execute(text("""
        WITH all_credits AS (
            SELECT actor_id, movie_id FROM "cast"
            UNION
            SELECT actor_id, movie_id FROM actor_movies
        )
        SELECT DISTINCT ac1.actor_id, ac2.actor_id
        FROM all_credits ac1
        JOIN all_credits ac2
          ON ac1.movie_id = ac2.movie_id AND ac1.actor_id < ac2.actor_id
        JOIN actors a1 ON a1.id = ac1.actor_id AND a1.actor_tier = 'primary'
        JOIN actors a2 ON a2.id = ac2.actor_id AND a2.actor_tier = 'primary'
    """)).fetchall()

    graph: dict[int, set] = defaultdict(set)
    for a, b in rows:
        graph[a].add(b)
        graph[b].add(a)
    V = list(graph.keys())

    # Brandes betweenness centrality
    centrality: dict[int, float] = {v: 0.0 for v in V}
    for s in V:
        stack: list[int] = []
        pred: dict[int, list] = {v: [] for v in V}
        sigma = dict.fromkeys(V, 0.0); sigma[s] = 1.0
        dist  = dict.fromkeys(V, -1);  dist[s]  = 0
        q: deque = deque([s])
        while q:
            v = q.popleft()
            stack.append(v)
            for w in graph[v]:
                if dist[w] < 0:
                    q.append(w); dist[w] = dist[v] + 1
                if dist[w] == dist[v] + 1:
                    sigma[w] += sigma[v]; pred[w].append(v)
        delta = dict.fromkeys(V, 0.0)
        while stack:
            w = stack.pop()
            for v in pred[w]:
                delta[v] += (sigma[v] / sigma[w]) * (1 + delta[w])
            if w != s:
                centrality[w] += delta[w]

    # Normalize
    n = len(V)
    norm = (n - 1) * (n - 2) if n > 2 else 1
    for v in centrality:
        centrality[v] /= norm

    # Fetch actor metadata
    top_ids = sorted(centrality, key=lambda v: centrality[v], reverse=True)[:limit]
    actor_rows = db.execute(
        text("SELECT id, name, industry FROM actors WHERE id = ANY(:ids)"),
        {"ids": top_ids}
    ).fetchall()
    meta = {r[0]: {"name": r[1], "industry": r[2]} for r in actor_rows}

    # Film + costar counts
    counts = db.execute(text("""
        SELECT am.actor_id,
               COUNT(DISTINCT am.movie_id)  AS film_count,
               COUNT(DISTINCT am2.actor_id) AS costar_count
        FROM actor_movies am
        JOIN actor_movies am2 ON am2.movie_id = am.movie_id AND am2.actor_id != am.actor_id
        WHERE am.actor_id = ANY(:ids)
        GROUP BY am.actor_id
    """), {"ids": top_ids}).fetchall()
    cnt_map = {r[0]: {"film_count": r[1], "costar_count": r[2]} for r in counts}

    return [
        {
            "id": aid,
            "name": meta.get(aid, {}).get("name", "?"),
            "industry": meta.get(aid, {}).get("industry", "Unknown"),
            "centrality": round(centrality[aid], 6),
            "film_count": cnt_map.get(aid, {}).get("film_count", 0),
            "costar_count": cnt_map.get(aid, {}).get("costar_count", 0),
        }
        for aid in top_ids
    ]


# ===========================================================================
# Top box office  (GET /analytics/top-box-office)  Sprint 23
# ===========================================================================

def get_top_box_office(
    db: Session,
    industry: Optional[str] = None,
    limit: int = 10,
) -> list[dict]:
    """
    Return the top-grossing films ranked by box_office (INR crore).

    Data source
    -----------
    Reads from ``movies.box_office`` (populated by enrich_box_office.py).
    Only rows where box_office IS NOT NULL are included.

    Actor association
    -----------------
    Left-joins ``actor_movies`` + ``actors`` to attach the primary actors
    (is_primary_actor = TRUE) who appeared in each film.  Returns their
    names and DB ids as parallel lists so the frontend can render avatars
    and build profile links without additional requests.

    Filtering
    ---------
    industry : case-insensitive match against ``movies.industry``.
               Pass None / "all" / "explore" for the South-Indian-only view
               (Telugu + Tamil + Malayalam + Kannada).
    limit    : max rows to return (default 10).

    Sanity cap
    ----------
    Films with box_office > 2 500 crore are excluded — a generous ceiling
    that keeps genuine record-breakers (Baahubali 2 ≈ 2 358 Cr) while
    silently dropping obvious TMDB data-entry errors.

    Returns
    -------
    List of dicts with keys matching the BoxOfficeEntry schema.
    Ordered by box_office DESC.
    """
    ind = (
        industry.lower()
        if industry and industry.lower() not in ("all", "explore")
        else None
    )

    south_indian_filter = (
        "AND LOWER(m.industry) = :ind"
        if ind
        else "AND m.industry IN ('Telugu', 'Tamil', 'Malayalam', 'Kannada')"
    )

    sql = text(f"""
        SELECT
            m.id,
            m.title,
            m.release_year,
            m.industry,
            ROUND(m.box_office::numeric, 1)         AS box_office_crore,
            m.poster_url,
            -- Order by billing_order so the top-billed primary actor is first.
            -- actor_movies PK is (actor_id, movie_id) so no duplicates per film.
            STRING_AGG(a.name,      ', ' ORDER BY am.billing_order NULLS LAST) AS actor_names,
            STRING_AGG(a.id::text,  ','  ORDER BY am.billing_order NULLS LAST) AS actor_id_list
        FROM   movies m
        LEFT JOIN actor_movies am ON am.movie_id = m.id
        LEFT JOIN actors a
               ON a.id = am.actor_id
              AND a.is_primary_actor = TRUE
        WHERE  m.box_office IS NOT NULL
          AND  m.box_office >    0
          AND  m.box_office <= 2500
          {south_indian_filter}
        GROUP  BY m.id, m.title, m.release_year, m.industry,
                  m.box_office, m.poster_url
        ORDER  BY m.box_office DESC
        LIMIT  :lim
    """)

    params: dict = {"lim": limit}
    if ind:
        params["ind"] = ind

    rows = db.execute(sql, params).fetchall()

    result = []
    for row in rows:
        # Parse comma-separated actor names / ids (may be empty when no
        # primary actor is linked to this film in actor_movies)
        names = (
            [n.strip() for n in row.actor_names.split(",") if n.strip()]
            if row.actor_names
            else []
        )
        ids = (
            [int(x) for x in row.actor_id_list.split(",") if x.strip().isdigit()]
            if row.actor_id_list
            else []
        )
        result.append(
            {
                "title":            row.title,
                "release_year":     row.release_year,
                "industry":         row.industry,
                "box_office_crore": float(row.box_office_crore),
                "actor_names":      names,
                "actor_ids":        ids,
                "poster_url":       row.poster_url,
            }
        )
    return result
