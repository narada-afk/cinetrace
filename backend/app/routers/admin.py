"""
routers/admin.py
================
Admin-only endpoints. All routes require the X-Admin-Key header to match
the ADMIN_API_KEY environment variable.
"""

from fastapi import APIRouter, Depends, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel

from ..core.cache import cache
from ..core.config import settings
from ..database import SessionLocal
from ..services.graph_service import graph_service

router = APIRouter(prefix="/admin", tags=["admin"])

_api_key_header = APIKeyHeader(name="X-Admin-Key", auto_error=False)


def _require_admin_key(key: str | None = Security(_api_key_header)) -> None:
    """Dependency: reject request if key is missing or doesn't match."""
    if not settings.ADMIN_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin API key not configured on this server.",
        )
    if key != settings.ADMIN_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-Admin-Key header.",
        )


class RebuildResponse(BaseModel):
    status: str
    node_count: int
    edge_count: int


@router.post(
    "/rebuild-graph",
    response_model=RebuildResponse,
    summary="Rebuild the in-memory collaboration graph",
    dependencies=[Depends(_require_admin_key)],
)
def rebuild_graph() -> RebuildResponse:
    """
    Re-runs the graph build against the current database state.
    Use after ingesting new actor/movie data without restarting the container.
    """
    db = SessionLocal()
    try:
        graph_service.rebuild(db)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Graph rebuild failed: {exc}",
        ) from exc
    finally:
        db.close()

    # Invalidate cached actor and compare responses so the next request
    # fetches fresh data. Silent no-op if Redis is unavailable.
    cache.delete_pattern("actor:*")
    cache.delete_pattern("compare:*")

    return RebuildResponse(
        status="ok",
        node_count=graph_service.node_count,
        edge_count=graph_service.edge_count,
    )
