# main.py
# Entry point for the FastAPI application.
# Defines all API routes and starts the server.
#
# Run with:
#   uvicorn app.main:app --reload

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List

from .database import engine, get_db
from . import models, crud, schemas

# Create all database tables on startup (safe to run multiple times).
# This uses the models defined in models.py to create the tables.
models.Base.metadata.create_all(bind=engine)

# Create the FastAPI app instance.
app = FastAPI(
    title="South Cinema Analytics API",
    description="Compare South Indian actors by movies, ratings, and box office performance.",
    version="1.0.0",
)

# Allow the frontend (localhost:3000) to call the backend (localhost:8000).
# Without this, the browser blocks cross-origin requests (CORS policy).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------

@app.get("/health")
def health_check():
    """
    Simple health check endpoint.
    Returns { "status": "ok" } if the server is running.
    """
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Actor Endpoints
# ---------------------------------------------------------------------------

@app.get("/actors", response_model=List[schemas.ActorOut])
def list_actors(db: Session = Depends(get_db)):
    """
    Returns a list of all actors in the database.
    Example: GET /actors
    """
    actors = crud.get_all_actors(db)
    return actors


@app.get("/actors/{actor_id}/movies", response_model=List[schemas.MovieOut])
def get_actor_movies(actor_id: int, db: Session = Depends(get_db)):
    """
    Returns all movies for a specific actor by their ID.
    Example: GET /actors/1/movies
    """
    # Check the actor exists first
    actor = db.query(models.Actor).filter(models.Actor.id == actor_id).first()
    if not actor:
        raise HTTPException(status_code=404, detail="Actor not found")

    movies = crud.get_movies_by_actor(db, actor_id)
    return movies


# ---------------------------------------------------------------------------
# Comparison Endpoint
# ---------------------------------------------------------------------------

@app.get("/compare", response_model=schemas.CompareResponse)
def compare_actors(
    actor1: str = Query(..., description="Full name of the first actor"),
    actor2: str = Query(..., description="Full name of the second actor"),
    db: Session = Depends(get_db),
):
    """
    Compares two actors side by side using analytics.
    Example: GET /compare?actor1=Allu Arjun&actor2=Vijay

    Returns stats for each actor:
    - Total movies
    - Average IMDb rating
    - Movies released after 2015
    - Average box office (in crores)
    """
    stats1 = crud.get_actor_stats(db, actor1)
    if not stats1:
        raise HTTPException(status_code=404, detail=f"Actor '{actor1}' not found")

    stats2 = crud.get_actor_stats(db, actor2)
    if not stats2:
        raise HTTPException(status_code=404, detail=f"Actor '{actor2}' not found")

    return schemas.CompareResponse(actor1=stats1, actor2=stats2)
