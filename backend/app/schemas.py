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
#   Sprint 10  : Collaboration (GET /analytics/top-collaborations)
#   Sprint 15  : Insight, InsightsOut (GET /analytics/insights)

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
    Combines Wikipedia fields (runtime, production_company, language) with
    TMDB fields (poster_url, backdrop_url, vote_average, popularity, tmdb_id).
    Ordered by release_year DESC on the server so no client-side sorting needed.
    """
    title: str
    release_year: int
    director: Optional[str] = None
    runtime: Optional[int] = None               # minutes
    production_company: Optional[str] = None
    language: Optional[str] = None
    # TMDB fields (Sprint 7)
    tmdb_id: Optional[int] = None
    poster_url: Optional[str] = None
    backdrop_url: Optional[str] = None
    vote_average: Optional[float] = None
    popularity: Optional[float] = None

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


class Collaboration(BaseModel):
    """
    One actor pair row returned by GET /analytics/top-collaborations.
    Sourced from the precomputed actor_collaborations table (O(1) per pair).

    Fields
    ------
    actor_1 : name of the first actor in the pair (lower primary-key id)
    actor_2 : name of the second actor in the pair (higher primary-key id)
    films   : number of movies the two actors appeared in together
    """
    actor_1: str
    actor_2: str
    films: int


class Insight(BaseModel):
    """
    One dynamic cinema fact for GET /analytics/insights.

    Fields
    ------
    type      : category — "collaboration" | "director" | "supporting"
    headline  : sentence fragment describing the fact (value + unit appended by UI)
    value     : the numeric stat (film count, collaboration count, etc.)
    unit      : label for the value — always "films" in Sprint 15
    actors    : names involved; 2 entries for collaborations/directors, 1 for supporting
    actor_ids : database IDs for the actors in `actors` (same order).
                For director insights only the actor's ID is included (directors
                are not in the actors table).  Use these IDs to build compare or
                actor-profile URLs without name→slug conversion.
    """
    type: str
    headline: str
    value: int
    unit: str
    actors: List[str]
    actor_ids: List[int] = []


class InsightsOut(BaseModel):
    """Response envelope for GET /analytics/insights."""
    insights: List[Insight]


class SharedFilmOut(BaseModel):
    """
    One movie that two actors both appeared in.
    Returned by GET /actors/{actor1_id}/shared/{actor2_id}.

    Fields
    ------
    title            : movie title
    release_year     : release year (0 = unknown)
    director         : director name (legacy TEXT column)
    poster_url       : TMDB poster URL (null until TMDB enrichment runs)
    vote_average     : TMDB vote average 0–10 (null until enrichment)
    popularity       : TMDB popularity score (null until enrichment)
    actor1_character : character name actor 1 played (from actor_movies; null for cast-only rows)
    actor1_role      : role type for actor 1 — "Lead" / "Supporting" / null
    actor2_character : character name actor 2 played (from actor_movies; null for cast-only rows)
    actor2_role      : role type for actor 2 — "Lead" / "Supporting" / null
    """
    title: str
    release_year: int
    director: Optional[str] = None
    poster_url: Optional[str] = None
    vote_average: Optional[float] = None
    popularity: Optional[float] = None
    actor1_character: Optional[str] = None
    actor1_role: Optional[str] = None
    actor2_character: Optional[str] = None
    actor2_role: Optional[str] = None

    class Config:
        from_attributes = True


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
