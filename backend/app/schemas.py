# schemas.py
# Pydantic models define the shape of data coming IN and going OUT of the API.
# These are separate from SQLAlchemy models (models.py = database, schemas.py = API).
#
# Sprint history:
#   Sprint 1-2 : ActorBase, ActorOut, MovieBase, MovieOut, LegacyActorStats,
#                LegacyCompareResponse
#   Sprint 6   : ActorSearchResult, ActorProfile, ActorMovieOut,
#                CollaboratorOut, DirectorCollabOut, ProductionOut,
#                ActorCompareStats, CompareResponse, HealthOut

from pydantic import BaseModel
from typing import Optional, List


# ===========================================================================
# Actor Schemas
# ===========================================================================

class ActorBase(BaseModel):
    """Fields shared between actor requests and responses."""
    name: str
    industry: str
    debut_year: Optional[int] = None


class ActorOut(ActorBase):
    """What the API returns when listing all actors (GET /actors)."""
    id: int

    class Config:
        from_attributes = True  # Allows converting SQLAlchemy objects to Pydantic


class ActorSearchResult(BaseModel):
    """
    Lightweight actor object returned by GET /actors/search?q=.
    Intentionally minimal — the frontend needs only id + name to render
    a search-results dropdown or list.
    """
    id: int
    name: str

    class Config:
        from_attributes = True


class ActorProfile(BaseModel):
    """
    Full actor profile returned by GET /actors/{actor_id}.
    Combines the actors row with the precomputed actor_stats row so the
    frontend never needs to fire two separate requests.
    """
    id: int
    name: str
    industry: str
    film_count: int
    first_film_year: Optional[int] = None   # None if all years are unknown
    last_film_year: Optional[int] = None    # None if all years are unknown
    avg_runtime: Optional[float] = None     # None until Wikipedia enrichment runs


# ===========================================================================
# Movie Schemas
# ===========================================================================

class MovieBase(BaseModel):
    """Fields shared between movie requests and responses."""
    title: str
    release_year: int
    imdb_rating: Optional[float] = None
    box_office: Optional[float] = None
    industry: str


class MovieOut(MovieBase):
    """What the API returns when listing movies (legacy endpoint)."""
    id: int

    class Config:
        from_attributes = True


class ActorMovieOut(BaseModel):
    """
    Enriched movie row returned by GET /actors/{actor_id}/movies.
    Uses the richer fields added by the Wikipedia enrichment pipeline.
    Ordered by release_year DESC on the server so no client-side sorting needed.
    """
    title: str
    release_year: int
    director: Optional[str] = None
    runtime: Optional[int] = None               # minutes
    production_company: Optional[str] = None
    language: Optional[str] = None

    class Config:
        from_attributes = True


# ===========================================================================
# Analytics / Collaboration Schemas
# ===========================================================================

class CollaboratorOut(BaseModel):
    """
    One row in GET /actors/{actor_id}/collaborators.
    Sourced from the actor_collaborations precomputed table.
    """
    actor: str      # co-star's name
    films: int      # number of shared films


class DirectorCollabOut(BaseModel):
    """
    One row in GET /actors/{actor_id}/directors.
    Sourced from the actor_director_stats precomputed table.
    """
    director: str
    films: int


class ProductionOut(BaseModel):
    """
    One row in GET /actors/{actor_id}/production.
    Sourced from the actor_production_stats precomputed table.
    """
    company: str
    films: int


# ===========================================================================
# Comparison Schemas  (Sprint 6 — uses analytics tables, O(1) reads)
# ===========================================================================

class ActorCompareStats(BaseModel):
    """
    Analytics snapshot for one actor, returned inside CompareResponse.
    Read from the actor_stats precomputed table — no heavy joins needed.

    Fields
    ------
    name        : canonical actor name
    films       : total distinct films in the database
    avg_runtime : average film runtime in minutes (null until enrichment runs)
    first_film  : earliest known release year (null if all years are unknown)
    last_film   : most recent known release year (null if all years are unknown)
    """
    name: str
    films: int
    avg_runtime: Optional[float] = None
    first_film: Optional[int] = None
    last_film: Optional[int] = None


class CompareResponse(BaseModel):
    """
    Full response for GET /compare?actor1=...&actor2=...
    Both actor slots use ActorCompareStats drawn from the actor_stats table.
    """
    actor1: ActorCompareStats
    actor2: ActorCompareStats


# ===========================================================================
# Health Schema
# ===========================================================================

class HealthOut(BaseModel):
    """Response shape for GET /health."""
    status: str
    actors: int
    movies: int


# ===========================================================================
# Legacy Schemas  (kept for backward compatibility — do not remove)
# ===========================================================================

class LegacyActorStats(BaseModel):
    """
    Original analytics summary for a single actor (Sprint 1-2).
    Computed on the fly from the movies table — heavier than ActorCompareStats.
    Retained so existing tooling that calls crud.get_actor_stats() still works.

    NOTE: New code should use ActorCompareStats (reads from actor_stats table).
    """
    name: str
    total_movies: int
    avg_rating: Optional[float]       # Average IMDb rating across all movies
    movies_after_2015: int            # Count of movies released after 2015
    avg_box_office: Optional[float]   # Average box office in crores


class LegacyCompareResponse(BaseModel):
    """Original compare response shape (Sprint 1-2). Kept for reference."""
    actor1: LegacyActorStats
    actor2: LegacyActorStats
