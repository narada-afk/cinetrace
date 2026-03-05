# crud.py
# Contains all database query logic (Create, Read, Update, Delete).
# The routes in main.py call these functions — keeping routes clean and simple.

from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional

from . import models, schemas


def get_all_actors(db: Session):
    """Return all actors from the database."""
    return db.query(models.Actor).all()


def get_actor_by_name(db: Session, name: str) -> Optional[models.Actor]:
    """Find a single actor by their exact name (case-insensitive)."""
    return (
        db.query(models.Actor)
        .filter(func.lower(models.Actor.name) == name.lower())
        .first()
    )


def get_movies_by_actor(db: Session, actor_id: int):
    """
    Return all movies an actor appeared in.
    We join through the Cast table to find movies linked to this actor.
    """
    return (
        db.query(models.Movie)
        .join(models.Cast, models.Cast.movie_id == models.Movie.id)
        .filter(models.Cast.actor_id == actor_id)
        .all()
    )


def get_actor_stats(db: Session, actor_name: str) -> Optional[schemas.ActorStats]:
    """
    Compute analytics for a single actor:
    - Total number of movies
    - Average IMDb rating
    - Number of movies released after 2015
    - Average box office

    Returns None if the actor is not found.
    """
    actor = get_actor_by_name(db, actor_name)
    if not actor:
        return None

    movies = get_movies_by_actor(db, actor.id)

    total_movies = len(movies)

    # Average rating — filter out movies where rating is missing
    ratings = [m.imdb_rating for m in movies if m.imdb_rating is not None]
    avg_rating = round(sum(ratings) / len(ratings), 2) if ratings else None

    # Count movies released after 2015
    movies_after_2015 = sum(1 for m in movies if m.release_year > 2015)

    # Average box office — filter out movies where box office is missing
    box_offices = [m.box_office for m in movies if m.box_office is not None]
    avg_box_office = round(sum(box_offices) / len(box_offices), 2) if box_offices else None

    return schemas.ActorStats(
        name=actor.name,
        total_movies=total_movies,
        avg_rating=avg_rating,
        movies_after_2015=movies_after_2015,
        avg_box_office=avg_box_office,
    )
