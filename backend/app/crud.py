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

from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional

from . import models, schemas


# ===========================================================================
# Core actor helpers  (used by multiple endpoints)
# ===========================================================================

def get_all_actors(db: Session):
    """Return all actors from the database (used by GET /actors)."""
    return db.query(models.Actor).all()


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

def search_actors(db: Session, q: str, limit: int = 20) -> list:
    """
    Case-insensitive partial-match search on actors.name.

    Uses ILIKE '%q%' — with a leading wildcard a btree index cannot be used,
    but with only ~13 actors in the table a sequential scan completes in < 1 ms.
    Returns at most `limit` results (default 20).
    """
    return (
        db.query(models.Actor.id, models.Actor.name)
        .filter(models.Actor.name.ilike(f"%{q}%"))
        .order_by(models.Actor.name)
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
    Joins cast → movies and returns full Movie objects so Pydantic can
    read the enriched fields (runtime, production_company, language).

    Uses the idx_cast_actor index for an efficient cast scan.
    """
    return (
        db.query(models.Movie)
        .join(models.Cast, models.Cast.movie_id == models.Movie.id)
        .filter(models.Cast.actor_id == actor_id)
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
