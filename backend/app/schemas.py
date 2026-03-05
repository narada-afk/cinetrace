# schemas.py
# Pydantic models define the shape of data coming IN and going OUT of the API.
# These are separate from SQLAlchemy models (models.py = database, schemas.py = API).

from pydantic import BaseModel
from typing import Optional, List


# --- Actor Schemas ---

class ActorBase(BaseModel):
    """Fields shared between actor requests and responses."""
    name: str
    industry: str
    debut_year: Optional[int] = None


class ActorOut(ActorBase):
    """What the API returns when listing actors."""
    id: int

    class Config:
        from_attributes = True  # Allows converting SQLAlchemy objects to Pydantic


# --- Movie Schemas ---

class MovieBase(BaseModel):
    """Fields shared between movie requests and responses."""
    title: str
    release_year: int
    imdb_rating: Optional[float] = None
    box_office: Optional[float] = None
    industry: str


class MovieOut(MovieBase):
    """What the API returns when listing movies for an actor."""
    id: int

    class Config:
        from_attributes = True


# --- Comparison Schemas ---

class ActorStats(BaseModel):
    """
    Analytics summary for a single actor.
    Returned as part of the /compare endpoint response.
    """
    name: str
    total_movies: int
    avg_rating: Optional[float]       # Average IMDb rating across all movies
    movies_after_2015: int            # Count of movies released after 2015
    avg_box_office: Optional[float]   # Average box office in crores


class CompareResponse(BaseModel):
    """The full response for GET /compare?actor1=...&actor2=..."""
    actor1: ActorStats
    actor2: ActorStats
