"""
routers/analytics.py
====================
Analytics endpoints: insights, top-collaborations, directors,
production houses, box office.

All DB queries are still delegated to crud.py — these endpoints are
straightforward aggregations that don't (yet) warrant their own
repository class.  When this grows, extract an AnalyticsRepository
following the same pattern as repositories/actor_repository.py.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app import schemas, crud


router = APIRouter(prefix="/analytics", tags=["Analytics"])


# ── Insights ──────────────────────────────────────────────────────────────────

@router.get(
    "/insights",
    response_model=schemas.InsightsOut,
    summary="Dynamic cinema insight cards for the homepage",
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
    collaboration, director, and supporting.

    Pass `?industry=telugu` (or tamil / malayalam / kannada) to scope all
    queries to a single industry.  Omit the param for the global view.
    """
    insights = crud.get_insights(db, industry=industry)
    return schemas.InsightsOut(
        insights=[schemas.Insight(**i) for i in insights]
    )


# ── Top collaborations ────────────────────────────────────────────────────────

@router.get(
    "/top-collaborations",
    response_model=List[schemas.Collaboration],
    summary="Actor pairs with the most shared films",
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
    Data is read from the **actor_collaborations** precomputed table.
    Run `python -m data_pipeline.build_analytics_tables` to refresh after new ingestion.
    """
    rows = crud.get_top_collaborations(db, limit=limit)
    return [
        schemas.Collaboration(actor_1=row.actor_1, actor_2=row.actor_2, films=row.films)
        for row in rows
    ]


# ── Directors ─────────────────────────────────────────────────────────────────

@router.get(
    "/directors",
    response_model=List[schemas.DirectorStat],
    summary="Top directors by number of films",
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
    Only directors with ≥ 2 films are included.
    The `industries` field is a comma-separated string (e.g. ``"Telugu, Tamil"``).
    """
    rows = crud.get_top_directors(db, industry=industry, limit=limit)
    return [
        schemas.DirectorStat(name=r.name, film_count=r.film_count, industries=r.industries)
        for r in rows
    ]


# ── Production houses ─────────────────────────────────────────────────────────

@router.get(
    "/production-houses",
    response_model=List[schemas.ProductionHouseStat],
    summary="Top production houses by number of films",
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
    Data is read from ``movies.production_company`` (populated by enrich_movies.py).
    Only companies with ≥ 2 films are included.
    """
    rows = crud.get_top_production_houses(db, industry=industry, limit=limit)
    return [
        schemas.ProductionHouseStat(
            name=r.name, film_count=r.film_count, industries=r.industries
        )
        for r in rows
    ]


# ── Box office ────────────────────────────────────────────────────────────────

@router.get(
    "/top-box-office",
    response_model=List[schemas.BoxOfficeEntry],
    summary="Top-grossing South Indian films",
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
    office revenue (stored in INR crore, converted from TMDB USD at 84.0 INR/USD).
    Films with no revenue data on TMDB are excluded.
    """
    rows = crud.get_top_box_office(db, industry=industry, limit=limit)
    return [schemas.BoxOfficeEntry(**row) for row in rows]
