# main.py
# Entry point for the FastAPI application.
# Defines all API routes and starts the server.
#
# Run with:
#   uvicorn app.main:app --reload
#
# Sprint history:
#   Sprint 1-2 : /health, /actors, /actors/{id}/movies, /compare
#   Sprint 6   : /health (enriched), /actors/search, /actors/{id} (profile),
#                /actors/{id}/movies (enriched + ordered), /actors/{id}/collaborators,
#                /actors/{id}/directors, /actors/{id}/production,
#                /compare (analytics-backed, O(1))

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List

from .database import engine, get_db
from . import models, crud, schemas

# Create all database tables on startup (safe to run multiple times).
models.Base.metadata.create_all(bind=engine)

# ---------------------------------------------------------------------------
# App instance
# ---------------------------------------------------------------------------

app = FastAPI(
    title="South Cinema Analytics API",
    description=(
        "Analytics API for South Indian cinema. "
        "Query actor profiles, filmographies, collaborations, and "
        "side-by-side comparisons powered by precomputed analytics tables."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Allow the frontend (localhost:3000) to call the backend (localhost:8000).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===========================================================================
# Health
# ===========================================================================

@app.get(
    "/health",
    response_model=schemas.HealthOut,
    summary="API health check",
    tags=["Health"],
)
def health_check(db: Session = Depends(get_db)):
    """
    Returns the service status plus live row counts.

    Example response:
    ```json
    { "status": "ok", "actors": 13, "movies": 734 }
    ```
    """
    actor_count, movie_count = crud.get_health_counts(db)
    return schemas.HealthOut(status="ok", actors=actor_count, movies=movie_count)


# ===========================================================================
# Actor endpoints
# ===========================================================================

@app.get(
    "/actors/search",
    response_model=List[schemas.ActorSearchResult],
    summary="Search actors by name",
    tags=["Actors"],
)
def search_actors(
    q: str = Query(..., min_length=1, description="Partial actor name to search for"),
    db: Session = Depends(get_db),
):
    """
    Case-insensitive partial-match search on actor names. Returns at most 20 results.

    Example:
    ```
    GET /actors/search?q=vij
    ```
    ```json
    [
      { "id": 7, "name": "Vijay" }
    ]
    ```
    """
    results = crud.search_actors(db, q)
    return [schemas.ActorSearchResult(id=row.id, name=row.name) for row in results]


@app.get(
    "/actors",
    response_model=List[schemas.ActorOut],
    summary="List all actors",
    tags=["Actors"],
)
def list_actors(db: Session = Depends(get_db)):
    """
    Returns every actor in the database (id, name, industry, debut_year).

    Example:
    ```
    GET /actors
    ```
    """
    return crud.get_all_actors(db)


@app.get(
    "/actors/{actor_id}",
    response_model=schemas.ActorProfile,
    summary="Actor profile with career stats",
    tags=["Actors"],
)
def get_actor_profile(actor_id: int, db: Session = Depends(get_db)):
    """
    Returns an actor's profile enriched with precomputed career statistics.
    Stats are read from the **actor_stats** analytics table — O(1) lookup.

    Example:
    ```
    GET /actors/12
    ```
    ```json
    {
      "id": 12,
      "name": "Rajinikanth",
      "industry": "Tamil",
      "film_count": 157,
      "first_film_year": 1957,
      "last_film_year": 2025,
      "avg_runtime": 170.0
    }
    ```

    Run `python -m data_pipeline.build_analytics_tables` if `film_count` is 0.
    """
    result = crud.get_actor_with_stats(db, actor_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Actor not found")

    actor, stats = result
    return schemas.ActorProfile(
        id=actor.id,
        name=actor.name,
        industry=actor.industry,
        film_count=stats.film_count if stats else 0,
        first_film_year=stats.first_film_year if stats else None,
        last_film_year=stats.last_film_year if stats else None,
        avg_runtime=round(stats.avg_runtime, 1) if stats and stats.avg_runtime else None,
    )


@app.get(
    "/actors/{actor_id}/movies",
    response_model=List[schemas.ActorMovieOut],
    summary="Movies an actor appeared in",
    tags=["Actors"],
)
def get_actor_movies(actor_id: int, db: Session = Depends(get_db)):
    """
    Returns all movies the actor has appeared in, ordered newest-first.
    Enriched fields (runtime, production_company, language) are populated by
    `python -m data_pipeline.enrich_movies`.

    Example:
    ```
    GET /actors/12/movies
    ```
    ```json
    [
      {
        "title": "Jailer",
        "release_year": 2023,
        "director": "Nelson Dilipkumar",
        "runtime": 168,
        "production_company": "Sun Pictures",
        "language": "Tamil"
      },
      ...
    ]
    ```
    """
    if not crud.get_actor_by_id(db, actor_id):
        raise HTTPException(status_code=404, detail="Actor not found")

    movies = crud.get_actor_movies_enriched(db, actor_id)
    return movies


@app.get(
    "/actors/{actor_id}/collaborators",
    response_model=List[schemas.CollaboratorOut],
    summary="Top co-stars for an actor",
    tags=["Actors"],
)
def get_actor_collaborators(actor_id: int, db: Session = Depends(get_db)):
    """
    Returns actors who have appeared in the most films alongside this actor,
    sorted by collaboration count (descending).
    Reads from the **actor_collaborations** precomputed table — O(1) per actor.

    Example:
    ```
    GET /actors/12/collaborators
    ```
    ```json
    [
      { "actor": "Kamal Haasan", "films": 12 },
      { "actor": "Sridevi", "films": 8 }
    ]
    ```
    """
    if not crud.get_actor_by_id(db, actor_id):
        raise HTTPException(status_code=404, detail="Actor not found")

    rows = crud.get_actor_collaborators(db, actor_id)
    return [
        schemas.CollaboratorOut(actor=name, films=count)
        for name, count in rows
    ]


@app.get(
    "/actors/{actor_id}/directors",
    response_model=List[schemas.DirectorCollabOut],
    summary="Directors an actor has worked with",
    tags=["Actors"],
)
def get_actor_directors(actor_id: int, db: Session = Depends(get_db)):
    """
    Returns directors sorted by number of films made with this actor.
    Reads from the **actor_director_stats** precomputed table.

    Example:
    ```
    GET /actors/12/directors
    ```
    ```json
    [
      { "director": "S. P. Muthuraman", "films": 23 },
      { "director": "Shankar", "films": 4 }
    ]
    ```
    """
    if not crud.get_actor_by_id(db, actor_id):
        raise HTTPException(status_code=404, detail="Actor not found")

    rows = crud.get_actor_directors(db, actor_id)
    return [
        schemas.DirectorCollabOut(director=row.director, films=row.film_count)
        for row in rows
    ]


@app.get(
    "/actors/{actor_id}/production",
    response_model=List[schemas.ProductionOut],
    summary="Production companies an actor has worked with",
    tags=["Actors"],
)
def get_actor_production(actor_id: int, db: Session = Depends(get_db)):
    """
    Returns production companies sorted by number of films made with this actor.
    Populated only after running `python -m data_pipeline.enrich_movies`.
    Reads from the **actor_production_stats** precomputed table.

    Example:
    ```
    GET /actors/12/production
    ```
    ```json
    [
      { "company": "Sun Pictures", "films": 2 }
    ]
    ```
    """
    if not crud.get_actor_by_id(db, actor_id):
        raise HTTPException(status_code=404, detail="Actor not found")

    rows = crud.get_actor_production(db, actor_id)
    return [
        schemas.ProductionOut(company=row.production_company, films=row.film_count)
        for row in rows
    ]


# ===========================================================================
# Comparison endpoint
# ===========================================================================

@app.get(
    "/compare",
    response_model=schemas.CompareResponse,
    summary="Side-by-side actor comparison",
    tags=["Compare"],
)
def compare_actors(
    actor1: str = Query(..., description="Full name of the first actor"),
    actor2: str = Query(..., description="Full name of the second actor"),
    db: Session = Depends(get_db),
):
    """
    Compares two actors side by side.
    Stats are read from the **actor_stats** precomputed table — O(1) per actor.

    Example:
    ```
    GET /compare?actor1=Rajinikanth&actor2=Kamal Haasan
    ```
    ```json
    {
      "actor1": {
        "name": "Rajinikanth",
        "films": 157,
        "avg_runtime": 170.0,
        "first_film": 1957,
        "last_film": 2025
      },
      "actor2": {
        "name": "Kamal Haasan",
        "films": 174,
        "avg_runtime": 182.0,
        "first_film": 1957,
        "last_film": 2024
      }
    }
    ```

    Returns HTTP 404 if either actor is not found, or if the analytics tables
    have not been built yet (`python -m data_pipeline.build_analytics_tables`).
    """
    result1 = crud.get_actor_compare_stats(db, actor1)
    if not result1:
        raise HTTPException(
            status_code=404,
            detail=f"Actor '{actor1}' not found or analytics not built yet.",
        )

    result2 = crud.get_actor_compare_stats(db, actor2)
    if not result2:
        raise HTTPException(
            status_code=404,
            detail=f"Actor '{actor2}' not found or analytics not built yet.",
        )

    a1, s1 = result1
    a2, s2 = result2

    return schemas.CompareResponse(
        actor1=schemas.ActorCompareStats(
            name=a1.name,
            films=s1.film_count,
            avg_runtime=round(s1.avg_runtime, 1) if s1.avg_runtime else None,
            first_film=s1.first_film_year,
            last_film=s1.last_film_year,
        ),
        actor2=schemas.ActorCompareStats(
            name=a2.name,
            films=s2.film_count,
            avg_runtime=round(s2.avg_runtime, 1) if s2.avg_runtime else None,
            first_film=s2.first_film_year,
            last_film=s2.last_film_year,
        ),
    )
