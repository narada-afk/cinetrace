"""
routers/stats.py
================
"Stats for Nerds" endpoints: overview counts, most-connected actors,
industry distribution, career timeline, director partnerships,
chart data, cinema universe graph, gravity center, and actor connection BFS.

Two data sources:
  • crud.py        — aggregation queries (all endpoints except connection
                     and gravity-center)
  • graph_service  — pure in-memory BFS + Brandes centrality
                     (connection, gravity-center); no DB calls during
                     traversal itself.

Upgrade path
------------
Heavy aggregations (overview, most-connected, etc.) can be extracted
into a StatsRepository following the same pattern as actor_repository.py
when the crud.py function count grows further.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app import crud
from app.repositories.actor_repository import actor_repo
from app.services.graph_service import graph_service


router = APIRouter(prefix="/stats", tags=["Stats"])


# ── Overview ──────────────────────────────────────────────────────────────────

@router.get("/overview")
def stats_overview(db: Session = Depends(get_db)):
    """Global counts: movies, ingested actors, actor→movie links, industries."""
    return crud.get_stats_overview(db)


# ── Most connected ────────────────────────────────────────────────────────────

@router.get("/most-connected")
def stats_most_connected(
    limit: int = Query(25, le=50, description="Max actors to return"),
    db: Session = Depends(get_db),
):
    """Primary + network actors ranked by number of unique co-stars."""
    return crud.get_most_connected_actors(db, limit)


# ── Industry distribution ─────────────────────────────────────────────────────

@router.get("/industry-distribution")
def stats_industry_distribution(db: Session = Depends(get_db)):
    """Film counts per South Indian industry with per-decade breakdown."""
    return crud.get_industry_distribution(db)


# ── Director partnerships ─────────────────────────────────────────────────────

@router.get("/top-partnerships")
def stats_top_partnerships(
    limit: int = Query(15, le=30),
    db: Session = Depends(get_db),
):
    """Most prolific actor–director partnerships (≥3 films together)."""
    return crud.get_top_director_partnerships(db, limit)


# ── Career timeline ───────────────────────────────────────────────────────────

@router.get("/career-timeline")
def stats_career_timeline(
    actor_id: int = Query(..., description="Actor DB id"),
    db: Session = Depends(get_db),
):
    """Films per year for the given actor."""
    actor = actor_repo.get_by_id(db, actor_id)
    if not actor:
        raise HTTPException(status_code=404, detail="Actor not found")
    data = crud.get_career_timeline(db, actor_id)
    return {"actor_id": actor_id, "actor_name": actor.name, "data": data}


# ── Top co-stars ──────────────────────────────────────────────────────────────

@router.get("/top-costars")
def stats_top_costars(
    limit: int = Query(15, le=30),
    db: Session = Depends(get_db),
):
    """Actors with the most unique co-stars (highest network centrality)."""
    return crud.get_top_costars(db, limit)


# ── Actor connection (BFS) ────────────────────────────────────────────────────

@router.get("/connection")
def stats_connection(
    actor1_id: int = Query(..., description="Start actor DB id"),
    actor2_id: int = Query(..., description="End actor DB id"),
    db: Session = Depends(get_db),
):
    """
    BFS shortest collaboration path between two actors.
    Returns path nodes and the connecting film at each step.

    Traversal is purely in-memory — graph_service holds a pre-built
    adjacency list loaded once at startup, so this endpoint makes
    zero DB calls during BFS itself.
    """
    for aid in (actor1_id, actor2_id):
        if not actor_repo.get_by_id(db, aid):
            raise HTTPException(status_code=404, detail=f"Actor {aid} not found")

    return graph_service.find_connection(actor1_id, actor2_id)


# ── Chart data ────────────────────────────────────────────────────────────────

@router.get("/chart-data")
def stats_chart_data(
    x_axis: str = Query(
        ..., description="year|decade|actor|industry|director"
    ),
    y_axis: str = Query(
        ...,
        description=(
            "film_count|avg_rating|unique_costars|"
            "director_collaborations|total_collaborations"
        ),
    ),
    actors: str = Query("", description="Comma-separated actor IDs"),
    industry: Optional[str] = Query(None),
    year_from: Optional[int] = Query(None),
    year_to: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """Dynamic chart data for the Build Your Own Chart playground."""
    actor_ids = [int(a) for a in actors.split(",") if a.strip().isdigit()]
    if not actor_ids and x_axis not in ("industry",):
        raise HTTPException(status_code=400, detail="Select at least one actor")
    return crud.get_chart_data(db, x_axis, y_axis, actor_ids, industry, year_from, year_to)


# ── Cinema universe ───────────────────────────────────────────────────────────

@router.get("/cinema-universe")
def stats_cinema_universe(
    min_films: int = Query(
        2, ge=1, le=10, description="Min shared films for an edge"
    ),
    db: Session = Depends(get_db),
):
    """Force-directed graph data: nodes (primary actors) + edges (shared films)."""
    return crud.get_cinema_universe(db, min_films)


# ── Gravity center (Brandes betweenness centrality) ──────────────────────────

@router.get("/gravity-center")
def stats_gravity_center(
    limit: int = Query(25, le=50),
    db: Session = Depends(get_db),
):
    """
    Betweenness centrality leaderboard — actors who bridge the most paths
    in the South Indian cinema collaboration network.

    Brandes algorithm runs on the primary-actor subgraph (≤85 nodes) held
    entirely in memory by graph_service.  Results are cached for 10 minutes.
    One DB call is made after the algorithm to fetch film and co-star counts.
    """
    return graph_service.get_gravity_center(db, limit=limit)
