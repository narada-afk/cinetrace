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
#   Sprint 10  : /analytics/top-collaborations
#   Sprint 15  : /analytics/insights
#   Sprint 19  : /analytics/directors, /analytics/production-houses

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional

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
    results = crud.search_actors(db, q, lead_only=lead_only)
    return [schemas.ActorSearchResult(id=row.id, name=row.name) for row in results]


@app.get(
    "/actors",
    response_model=List[schemas.ActorOut],
    summary="List all actors",
    tags=["Actors"],
)
def list_actors(
    primary_only: bool = Query(False, description="If true, return only primary actors"),
    gender: Optional[str] = Query(None, description="Filter by gender: 'M' (lead actors) or 'F' (lead actresses)"),
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
    return crud.get_all_actors(db, primary_only=primary_only, gender=gender)


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
# Shared films endpoint
# ===========================================================================

@app.get(
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

    Character names come from the **actor_movies** table (TMDB pipeline).
    Role types come from both **actor_movies** and **cast** (Wikidata pipeline).
    Either field may be null if the enrichment pipelines have not yet run.

    Example:
    ```
    GET /actors/3/shared/5
    ```
    ```json
    [
      {
        "title": "Acharya",
        "release_year": 2022,
        "director": "Koratala Siva",
        "poster_url": "https://image.tmdb.org/...",
        "vote_average": 5.3,
        "actor1_character": "Acharya",
        "actor1_role": "Lead",
        "actor2_character": "Siddha",
        "actor2_role": "Lead"
      }
    ]
    ```
    """
    if not crud.get_actor_by_id(db, actor1_id):
        raise HTTPException(status_code=404, detail="Actor not found")
    if not crud.get_actor_by_id(db, actor2_id):
        raise HTTPException(status_code=404, detail="Actor not found")

    rows = crud.get_shared_films(db, actor1_id, actor2_id)
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


# ===========================================================================
# Analytics endpoints
# ===========================================================================

@app.get(
    "/analytics/insights",
    response_model=schemas.InsightsOut,
    summary="Dynamic cinema insight cards for the homepage",
    tags=["Analytics"],
)
def get_insights(
    industry: Optional[str] = Query(
        default=None,
        description=(
            "Filter insights by industry — 'telugu', 'tamil', 'malayalam', "
            "'kannada'.  Omit or pass 'all' for the global cross-industry view."
        ),
    ),
    db: Session = Depends(get_db),
):
    """
    Returns 6–8 dynamic cinema facts drawn from three categories:

    * **collaboration** — actor pairs with the most shared films (from the
      precomputed `actor_collaborations` table).
    * **director** — actor + director duos with ≥ 4 films together (from
      `actor_movies ⋈ movies`).
    * **supporting** — the most prolific supporting performers (from
      `actor_movies` where `role_type = 'supporting'`).

    Pass `?industry=telugu` (or tamil / malayalam / kannada) to scope all
    three queries to a single industry.  Omit the param for the global view.

    Results are interleaved across the three types for homepage variety.
    Capped at 8 items — enough to fill two rows of four insight cards.

    Example response
    ----------------
    ```json
    {
      "insights": [
        {
          "type": "collaboration",
          "headline": "Mohanlal and Mammootty have appeared together in",
          "value": 60,
          "unit": "films",
          "actors": ["Mohanlal", "Mammootty"]
        },
        {
          "type": "director",
          "headline": "Mohanlal's most frequent director is",
          "value": 36,
          "unit": "films",
          "actors": ["Mohanlal", "Priyadarshan"]
        }
      ]
    }
    ```
    """
    insights = crud.get_insights(db, industry=industry)
    return schemas.InsightsOut(
        insights=[schemas.Insight(**i) for i in insights]
    )


@app.get(
    "/analytics/top-collaborations",
    response_model=List[schemas.Collaboration],
    summary="Actor pairs with the most shared films",
    tags=["Analytics"],
)
def top_collaborations(
    limit: int = Query(
        default=20,
        ge=1,
        le=100,
        description="Maximum number of pairs to return (1–100, default 20)",
    ),
    db: Session = Depends(get_db),
):
    """
    Returns the top actor pairs ranked by how many films they appeared in together.

    Data is read from the **actor_collaborations** precomputed table, which
    covers both the Wikidata pipeline (original 13 actors) and the TMDB pipeline
    (supporting actors + Malayalam stars).  Run
    `python -m data_pipeline.build_analytics_tables` to refresh the table after
    new actors are ingested.

    Query parameters
    ----------------
    * **limit** — number of pairs to return (default 20, max 100)

    Example requests
    ----------------
    ```
    GET /analytics/top-collaborations
    GET /analytics/top-collaborations?limit=10
    ```

    Example response
    ----------------
    ```json
    [
      {"actor_1": "Mohanlal", "actor_2": "Mammootty",          "films": 60},
      {"actor_1": "Rajinikanth", "actor_2": "Nassar",          "films": 18}
    ]
    ```
    """
    rows = crud.get_top_collaborations(db, limit=limit)
    return [
        schemas.Collaboration(actor_1=row.actor_1, actor_2=row.actor_2, films=row.films)
        for row in rows
    ]


@app.get(
    "/analytics/directors",
    response_model=List[schemas.DirectorStat],
    summary="Top directors by number of films",
    tags=["Analytics"],
)
def get_top_directors(
    industry: Optional[str] = Query(
        default=None,
        description=(
            "Filter by industry — 'telugu', 'tamil', 'malayalam', 'kannada'. "
            "Omit or pass 'all' for the cross-industry global view."
        ),
    ),
    limit: int = Query(
        default=30,
        ge=1,
        le=100,
        description="Maximum number of directors to return (1–100, default 30)",
    ),
    db: Session = Depends(get_db),
):
    """
    Returns directors ranked by how many films they have directed in the database.

    Data is read from the ``movies.director`` TEXT column — the denormalized
    legacy column which is populated for nearly every film.  Only directors
    with ≥ 2 films are included to eliminate one-off credits.

    The ``industries`` field is a comma-separated string of distinct industries
    for all films by this director (e.g. ``"Telugu, Tamil"``).

    Example response
    ----------------
    ```json
    [
      { "name": "S. P. Muthuraman", "film_count": 28, "industries": "Tamil" },
      { "name": "Priyadarshan",      "film_count": 22, "industries": "Malayalam, Tamil" }
    ]
    ```
    """
    rows = crud.get_top_directors(db, industry=industry, limit=limit)
    return [
        schemas.DirectorStat(
            name=r.name,
            film_count=r.film_count,
            industries=r.industries,
        )
        for r in rows
    ]


@app.get(
    "/analytics/production-houses",
    response_model=List[schemas.ProductionHouseStat],
    summary="Top production houses by number of films",
    tags=["Analytics"],
)
def get_top_production_houses(
    industry: Optional[str] = Query(
        default=None,
        description=(
            "Filter by industry — 'telugu', 'tamil', 'malayalam', 'kannada'. "
            "Omit or pass 'all' for the cross-industry global view."
        ),
    ),
    limit: int = Query(
        default=20,
        ge=1,
        le=100,
        description="Maximum number of production houses to return (1–100, default 20)",
    ),
    db: Session = Depends(get_db),
):
    """
    Returns production companies ranked by how many films they have produced.

    Data is read from ``movies.production_company`` — populated by
    ``enrich_movies.py`` via Wikipedia infobox scraping.  Only companies with
    ≥ 2 films are included.

    Example response
    ----------------
    ```json
    [
      { "name": "Sun Pictures",        "film_count": 14, "industries": "Tamil" },
      { "name": "Mythri Movie Makers", "film_count": 10, "industries": "Telugu" }
    ]
    ```
    """
    rows = crud.get_top_production_houses(db, industry=industry, limit=limit)
    return [
        schemas.ProductionHouseStat(
            name=r.name,
            film_count=r.film_count,
            industries=r.industries,
        )
        for r in rows
    ]


# ===========================================================================
# Sprint 21 — Stats for Nerds  (GET /stats/*)
# ===========================================================================

@app.get(
    "/analytics/top-box-office",
    response_model=List[schemas.BoxOfficeEntry],
    summary="Top-grossing South Indian films",
    tags=["Analytics"],
)
def get_top_box_office(
    industry: Optional[str] = Query(
        default=None,
        description=(
            "Filter by industry — 'telugu', 'tamil', 'malayalam', 'kannada'. "
            "Omit or pass 'all' for all South Indian industries combined."
        ),
    ),
    limit: int = Query(
        default=10,
        ge=1,
        le=50,
        description="Max films to return (1–50, default 10)",
    ),
    db: Session = Depends(get_db),
):
    """
    Returns the highest-grossing South Indian films ranked by worldwide box
    office revenue.

    Revenue is stored in INR crore (converted from TMDB USD figures at
    84.0 INR/USD by ``enrich_box_office.py``).  Films with no revenue data
    on TMDB are excluded.  An upper sanity cap of 2 500 crore filters out
    obvious TMDB data-entry errors.

    Each entry includes the primary actors (``is_primary_actor = TRUE``) who
    appeared in the film, so the frontend can render avatars and build profile
    links without extra round-trips.

    Example requests
    ----------------
    ```
    GET /analytics/top-box-office
    GET /analytics/top-box-office?industry=tamil&limit=5
    ```

    Example response
    ----------------
    ```json
    [
      {
        "title": "Baahubali 2: The Conclusion",
        "release_year": 2017,
        "industry": "Telugu",
        "box_office_crore": 2357.9,
        "actor_names": ["Prabhas", "Rana Daggubati"],
        "actor_ids": [3, 14],
        "poster_url": "https://image.tmdb.org/t/p/w500/..."
      }
    ]
    ```
    """
    rows = crud.get_top_box_office(db, industry=industry, limit=limit)
    return [schemas.BoxOfficeEntry(**row) for row in rows]


@app.get("/stats/overview", tags=["Stats"])
def stats_overview(db: Session = Depends(get_db)):
    """Global counts: movies, ingested actors, actor→movie links, industries."""
    return crud.get_stats_overview(db)


@app.get("/stats/most-connected", tags=["Stats"])
def stats_most_connected(
    limit: int = Query(25, le=50, description="Max actors to return"),
    db: Session = Depends(get_db),
):
    """Primary + network actors ranked by number of unique co-stars."""
    return crud.get_most_connected_actors(db, limit)


@app.get("/stats/industry-distribution", tags=["Stats"])
def stats_industry_distribution(db: Session = Depends(get_db)):
    """Film counts per South Indian industry with per-decade breakdown."""
    return crud.get_industry_distribution(db)


@app.get("/stats/top-partnerships", tags=["Stats"])
def stats_top_partnerships(
    limit: int = Query(15, le=30),
    db: Session = Depends(get_db),
):
    """Most prolific actor–director partnerships (≥3 films together)."""
    return crud.get_top_director_partnerships(db, limit)


@app.get("/stats/career-timeline", tags=["Stats"])
def stats_career_timeline(
    actor_id: int = Query(..., description="Actor DB id"),
    db: Session = Depends(get_db),
):
    """Films per year for the given actor."""
    actor = crud.get_actor_by_id(db, actor_id)
    if not actor:
        raise HTTPException(status_code=404, detail="Actor not found")
    data = crud.get_career_timeline(db, actor_id)
    return {"actor_id": actor_id, "actor_name": actor.name, "data": data}


@app.get("/stats/top-costars", tags=["Stats"])
def stats_top_costars(
    limit: int = Query(15, le=30),
    db: Session = Depends(get_db),
):
    """Actors with the most unique co-stars (highest network centrality)."""
    return crud.get_top_costars(db, limit)


@app.get("/stats/connection", tags=["Stats"])
def stats_connection(
    actor1_id: int = Query(..., description="Start actor DB id"),
    actor2_id: int = Query(..., description="End actor DB id"),
    db: Session = Depends(get_db),
):
    """
    BFS shortest collaboration path between two actors.
    Returns path nodes and the connecting film at each step.
    """
    for aid in (actor1_id, actor2_id):
        if not crud.get_actor_by_id(db, aid):
            raise HTTPException(status_code=404, detail=f"Actor {aid} not found")
    return crud.find_actor_connection(db, actor1_id, actor2_id)


# ===========================================================================
# Sprint 22 — Build Your Own Chart / Cinema Universe / Gravity Center
# ===========================================================================

@app.get("/stats/chart-data", tags=["Stats"])
def stats_chart_data(
    x_axis:   str = Query(..., description="year|decade|actor|industry|director"),
    y_axis:   str = Query(..., description="film_count|avg_rating|unique_costars|director_collaborations|total_collaborations"),
    actors:   str = Query("", description="Comma-separated actor IDs"),
    industry: str | None = Query(None),
    year_from: int | None = Query(None),
    year_to:   int | None = Query(None),
    db: Session = Depends(get_db),
):
    """Dynamic chart data for the Build Your Own Chart playground."""
    actor_ids = [int(a) for a in actors.split(",") if a.strip().isdigit()]
    if not actor_ids and x_axis not in ("industry",):
        raise HTTPException(status_code=400, detail="Select at least one actor")
    return crud.get_chart_data(db, x_axis, y_axis, actor_ids, industry, year_from, year_to)


@app.get("/stats/cinema-universe", tags=["Stats"])
def stats_cinema_universe(
    min_films: int = Query(2, ge=1, le=10,
                           description="Min shared films for an edge"),
    db: Session = Depends(get_db),
):
    """Force-directed graph data: nodes (ingested actors) + edges (shared films)."""
    return crud.get_cinema_universe(db, min_films)


@app.get("/stats/gravity-center", tags=["Stats"])
def stats_gravity_center(
    limit: int = Query(25, le=50),
    db: Session = Depends(get_db),
):
    """
    Betweenness centrality leaderboard — actors who bridge the most paths
    in the South Indian cinema collaboration network.
    """
    return crud.get_gravity_center(db, limit)
