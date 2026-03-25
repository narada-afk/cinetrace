"""
routers/actors.py
=================
Actor-domain endpoints: list, search, profile, filmography,
collaborators, directors, production companies, shared films, compare.

All actor DB queries go through actor_repo
(repositories/actor_repository.py).  Pydantic schema construction
stays here in the router — not inside the repository.

Pattern note
------------
This file is the pattern for the rest of the routers:

  1. Import the repository (or service) singleton.
  2. Raise HTTPException here if a record isn't found.
  3. Map raw ORM rows → Pydantic schemas in the router function body.
  4. Never import crud.py here — all SQL is in the repository.
     When the router grows a service layer, the call chain becomes:
       router  →  service  →  actor_repo.method(db, ...)
     No further changes needed inside actor_repo.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db
from app import schemas
from app.repositories.actor_repository import actor_repo


router = APIRouter()


# ── Search ───────────────────────────────────────────────────────────────────

@router.get(
    "/actors/search",
    response_model=List[schemas.ActorSearchResult],
    summary="Search actors by name",
    tags=["Actors"],
)
def search_actors(
    q: str = Query(..., min_length=1, description="Partial actor name to search for"),
    lead_only: bool = Query(False, description="If true, return only lead/primary actors"),
    db: Session = Depends(get_db),
):
    """
    Case-insensitive partial-match search on actor names. Returns at most 20 results.

    Pass `lead_only=true` to exclude supporting actors from results.

    Example:
    ```
    GET /actors/search?q=vij&lead_only=true
    ```
    ```json
    [
      { "id": 7, "name": "Vijay" }
    ]
    ```
    """
    rows = actor_repo.search(db, q, lead_only=lead_only)
    return [schemas.ActorSearchResult(id=row.id, name=row.name) for row in rows]


# ── List ─────────────────────────────────────────────────────────────────────

@router.get(
    "/actors",
    response_model=List[schemas.ActorOut],
    summary="List all actors",
    tags=["Actors"],
)
def list_actors(
    primary_only: bool = Query(False, description="If true, return only primary actors"),
    gender: Optional[str] = Query(
        None, description="Filter by gender: 'M' (lead actors) or 'F' (lead actresses)"
    ),
    db: Session = Depends(get_db),
):
    """
    Returns actors in the database (id, name, industry, debut_year, gender).

    Filters:
    - `primary_only=true`  → lead actors + lead actresses (all is_primary_actor=TRUE)
    - `gender=M`           → lead actors only
    - `gender=F`           → lead actresses only
    - both combined        → e.g. ?primary_only=true&gender=F

    Examples:
    ```
    GET /actors?primary_only=true
    GET /actors?gender=F
    GET /actors?gender=M&primary_only=true
    ```
    """
    return actor_repo.get_all(db, primary_only=primary_only, gender=gender)


# ── Profile ──────────────────────────────────────────────────────────────────

@router.get(
    "/actors/{actor_id}",
    response_model=schemas.ActorProfile,
    summary="Actor profile with career stats",
    tags=["Actors"],
)
def get_actor_profile(actor_id: int, db: Session = Depends(get_db)):
    """
    Returns an actor's profile enriched with precomputed career statistics.
    Stats are read from the **actor_stats** analytics table — O(1) lookup.

    Run `python -m data_pipeline.build_analytics_tables` if `film_count` is 0.
    """
    result = actor_repo.get_with_stats(db, actor_id)
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


# ── Filmography ───────────────────────────────────────────────────────────────

@router.get(
    "/actors/{actor_id}/movies",
    response_model=List[schemas.ActorMovieOut],
    summary="Movies an actor appeared in",
    tags=["Actors"],
)
def get_actor_movies(actor_id: int, db: Session = Depends(get_db)):
    """
    Returns all movies the actor has appeared in, ordered newest-first.
    Covers both ingestion pipelines (Wikidata cast table + TMDB actor_movies table).
    Enriched fields (runtime, production_company, language) are populated by
    `python -m data_pipeline.enrich_movies`.
    """
    if not actor_repo.get_by_id(db, actor_id):
        raise HTTPException(status_code=404, detail="Actor not found")

    return actor_repo.get_movies(db, actor_id)


# ── Collaborators ─────────────────────────────────────────────────────────────

@router.get(
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
    """
    if not actor_repo.get_by_id(db, actor_id):
        raise HTTPException(status_code=404, detail="Actor not found")

    rows = actor_repo.get_collaborators(db, actor_id)
    return [
        schemas.CollaboratorOut(actor=name, films=count)
        for name, count in rows
    ]


# ── Lead collaborators (primary-role only) ────────────────────────────────────

@router.get(
    "/actors/{actor_id}/lead-collaborators",
    response_model=List[schemas.CollaboratorOut],
    summary="Co-stars who shared a primary (lead) role in the same films",
    tags=["Actors"],
)
def get_lead_collaborators(actor_id: int, db: Session = Depends(get_db)):
    """
    Returns actors who appeared in a **primary** role in the same films as this actor
    (where this actor also had a primary role). Supporting/background actors are excluded.
    Useful for building accurate "Lead Actresses" sections.
    """
    if not actor_repo.get_by_id(db, actor_id):
        raise HTTPException(status_code=404, detail="Actor not found")

    rows = actor_repo.get_lead_collaborators(db, actor_id)
    return [
        schemas.CollaboratorOut(actor=name, films=count)
        for name, count in rows
    ]


# ── Directors ─────────────────────────────────────────────────────────────────

@router.get(
    "/actors/{actor_id}/directors",
    response_model=List[schemas.DirectorCollabOut],
    summary="Directors an actor has worked with",
    tags=["Actors"],
)
def get_actor_directors(actor_id: int, db: Session = Depends(get_db)):
    """
    Returns directors sorted by number of films made with this actor.
    Reads from the **actor_director_stats** precomputed table.
    """
    if not actor_repo.get_by_id(db, actor_id):
        raise HTTPException(status_code=404, detail="Actor not found")

    rows = actor_repo.get_directors(db, actor_id)
    return [
        schemas.DirectorCollabOut(director=row.director, films=row.film_count)
        for row in rows
    ]


# ── Production companies ──────────────────────────────────────────────────────

@router.get(
    "/actors/{actor_id}/production",
    response_model=List[schemas.ProductionOut],
    summary="Production companies an actor has worked with",
    tags=["Actors"],
)
def get_actor_production(actor_id: int, db: Session = Depends(get_db)):
    """
    Returns production companies sorted by number of films made with this actor.
    Reads from the **actor_production_stats** precomputed table.
    Populated only after running `python -m data_pipeline.enrich_movies`.
    """
    if not actor_repo.get_by_id(db, actor_id):
        raise HTTPException(status_code=404, detail="Actor not found")

    rows = actor_repo.get_production_companies(db, actor_id)
    return [
        schemas.ProductionOut(company=row.production_company, films=row.film_count)
        for row in rows
    ]


# ── Blockbusters ──────────────────────────────────────────────────────────────

@router.get(
    "/actors/{actor_id}/blockbusters",
    response_model=List[schemas.BlockbusterOut],
    summary="Actor's top 10 films by box office, highest first",
    tags=["Actors"],
)
def get_actor_blockbusters(actor_id: int, db: Session = Depends(get_db)):
    if not actor_repo.get_by_id(db, actor_id):
        raise HTTPException(status_code=404, detail="Actor not found")
    rows = db.execute(text("""
        SELECT m.title, m.release_year, m.poster_url,
               m.box_office   AS box_office_crore,
               m.budget_crore AS budget_crore
        FROM movies m
        JOIN actor_movies am ON am.movie_id = m.id
        WHERE am.actor_id = :actor_id
          AND m.box_office IS NOT NULL
          AND m.box_office > 0
        ORDER BY m.box_office DESC
        LIMIT 10
    """), {"actor_id": actor_id}).fetchall()
    return [
        schemas.BlockbusterOut(
            title=r.title,
            release_year=r.release_year,
            poster_url=r.poster_url,
            box_office_crore=r.box_office_crore,
            budget_crore=r.budget_crore,
            box_office_source="TMDB",
            budget_source="TMDB / Wikipedia" if r.budget_crore else None,
        )
        for r in rows
    ]


# ── Shared films ──────────────────────────────────────────────────────────────

@router.get(
    "/actors/{actor1_id}/shared/{actor2_id}",
    response_model=List[schemas.SharedFilmOut],
    summary="Films two actors have appeared in together",
    tags=["Actors"],
)
def get_shared_films(
    actor1_id: int,
    actor2_id: int,
    db: Session = Depends(get_db),
):
    """
    Returns all movies that both actors appeared in, ordered newest-first.

    Each row includes:
    * `title`, `release_year`, `director`, `poster_url`, `vote_average`
    * `actor1_character` / `actor1_role` — character name and role type for actor 1
    * `actor2_character` / `actor2_role` — character name and role type for actor 2
    """
    if not actor_repo.get_by_id(db, actor1_id):
        raise HTTPException(status_code=404, detail="Actor not found")
    if not actor_repo.get_by_id(db, actor2_id):
        raise HTTPException(status_code=404, detail="Actor not found")

    rows = actor_repo.get_shared_films(db, actor1_id, actor2_id)
    return [
        schemas.SharedFilmOut(
            title=row.title,
            release_year=row.release_year,
            director=row.director,
            poster_url=row.poster_url,
            vote_average=row.vote_average,
            popularity=row.popularity,
            actor1_character=row.actor1_character,
            actor1_role=row.actor1_role,
            actor2_character=row.actor2_character,
            actor2_role=row.actor2_role,
        )
        for row in rows
    ]


# ── Compare ───────────────────────────────────────────────────────────────────

@router.get(
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

    Returns HTTP 404 if either actor is not found, or if the analytics tables
    have not been built yet (`python -m data_pipeline.build_analytics_tables`).
    """
    result1 = actor_repo.get_with_stats_by_name(db, actor1)
    if not result1:
        raise HTTPException(
            status_code=404,
            detail=f"Actor '{actor1}' not found or analytics not built yet.",
        )

    result2 = actor_repo.get_with_stats_by_name(db, actor2)
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
