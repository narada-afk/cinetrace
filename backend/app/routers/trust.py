"""
routers/trust.py
================
GET /trust  — expose data confidence signals to the frontend.

Returns the current system_health singleton row, enriched with a
human-readable "last verified" label. Falls back gracefully when the
migration hasn't run yet (system_health table not present).
"""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app import schemas

router = APIRouter(tags=["Trust"])


def _human_delta(dt: Optional[datetime]) -> Optional[str]:
    """Return a friendly relative label: '3 minutes ago', '2 days ago', etc."""
    if dt is None:
        return None
    now   = datetime.now(timezone.utc)
    delta = now - dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else now - dt
    secs  = int(delta.total_seconds())

    if secs < 60:
        return "just now"
    if secs < 3600:
        m = secs // 60
        return f"{m} minute{'s' if m != 1 else ''} ago"
    if secs < 86400:
        h = secs // 3600
        return f"{h} hour{'s' if h != 1 else ''} ago"
    d = secs // 86400
    return f"{d} day{'s' if d != 1 else ''} ago"


@router.get(
    "/trust",
    response_model=schemas.TrustSignalOut,
    summary="Data confidence trust signals",
)
def get_trust_signals():
    """
    Returns the latest data confidence snapshot from the system_health table.

    Used by the frontend to display:
    - "Data Confidence: XX.X%"
    - "Last Verified: 3 hours ago"
    - "Source: TMDB + Wikidata"

    If the system_health table doesn't exist yet (migration not applied),
    returns a zero-state response rather than a 500 error.
    """
    db: Session = SessionLocal()
    try:
        # Guard: table may not exist if migration 0002 hasn't run
        result = db.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'system_health' AND table_schema = 'public'
            ) AS exists
        """)).fetchone()

        if not result or not result[0]:
            return schemas.TrustSignalOut()

        row = db.execute(text("""
            SELECT
                data_confidence_score,
                avg_actor_score,
                avg_movie_score,
                collab_integrity,
                validation_passed,
                ghost_collab_count,
                duplicate_count,
                invalid_link_count,
                total_actors,
                total_movies,
                total_collab_pairs,
                sources_used,
                last_scored_at
            FROM system_health
            WHERE id = 1
        """)).fetchone()

        if not row:
            return schemas.TrustSignalOut()

        last_dt: Optional[datetime] = row[12]
        last_iso = last_dt.isoformat() if last_dt else None

        return schemas.TrustSignalOut(
            data_confidence_score = row[0],
            avg_actor_score       = row[1],
            avg_movie_score       = row[2],
            collab_integrity      = row[3],
            validation_passed     = row[4],
            ghost_collab_count    = row[5] or 0,
            duplicate_count       = row[6] or 0,
            invalid_link_count    = row[7] or 0,
            total_actors          = row[8] or 0,
            total_movies          = row[9] or 0,
            total_collab_pairs    = row[10] or 0,
            sources_used          = list(row[11]) if row[11] else [],
            last_verified         = last_iso,
            last_verified_human   = _human_delta(last_dt),
        )

    finally:
        db.close()
