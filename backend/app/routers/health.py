"""
routers/health.py
=================
GET /health — service liveness + live row counts.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app import schemas
from app.repositories.actor_repository import actor_repo


router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    response_model=schemas.HealthOut,
    summary="API health check",
)
def health_check(db: Session = Depends(get_db)):
    """
    Returns the service status plus live row counts.

    Example response:
    ```json
    { "status": "ok", "actors": 6691, "movies": 4809 }
    ```
    """
    actor_count, movie_count = actor_repo.get_counts(db)
    return schemas.HealthOut(status="ok", actors=actor_count, movies=movie_count)
