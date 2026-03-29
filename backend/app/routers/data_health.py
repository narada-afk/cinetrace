"""
routers/data_health.py
======================
FastAPI endpoints for the movie data validation system.

Routes
------
    GET  /data-health                  Overview: score distribution + worst movies
    GET  /data-health/{movie_id}       Single movie validation result (cached)
    POST /data-health/validate/{id}    Re-validate one movie now (writes result)
    POST /data-health/fix/{id}         Dry-run auto-fix for one movie
    POST /data-health/fix/{id}?apply=1 Apply safe fixes and write to DB
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..database import get_db

router = APIRouter(prefix="/data-health", tags=["data-health"])


# ─── Response schemas ─────────────────────────────────────────────────────────

class ValidationSummary(BaseModel):
    total_validated:  int
    verified:         int
    warning:          int
    broken:           int
    avg_confidence:   float
    coverage_pct:     float   # % of all movies with a validation result

class MovieValidationResult(BaseModel):
    movie_id:         int
    title:            str
    tmdb_id:          Optional[int]
    confidence_score: float
    status:           str
    issues:           list[str]
    field_scores:     dict[str, float]
    last_checked_at:  str

class FixResult(BaseModel):
    movie_id:      int
    title:         str
    dry_run:       bool
    fixes_applied: list[str]
    fixes_skipped: list[str]


# ─── GET /data-health ─────────────────────────────────────────────────────────

@router.get("", response_model=dict, summary="Data quality overview")
def data_health_overview(
    limit: int = Query(20, ge=1, le=100, description="Worst movies to include"),
    db: Session = Depends(get_db),
):
    """
    Returns a health dashboard:
    - Score distribution (VERIFIED / WARNING / BROKEN counts + %)
    - Average confidence across all validated movies
    - Coverage % (how many movies have been validated)
    - `worst` list: lowest-confidence movies needing attention
    - `most_common_issues`: top 10 issue types by frequency
    """
    # ── Summary counts ────────────────────────────────────────────────────────
    summary = db.execute(
        text("""
            SELECT
                COUNT(*)                                               AS total,
                COUNT(*) FILTER (WHERE status = 'VERIFIED')           AS verified,
                COUNT(*) FILTER (WHERE status = 'WARNING')            AS warning,
                COUNT(*) FILTER (WHERE status = 'BROKEN')             AS broken,
                COALESCE(AVG(confidence_score), 0)::FLOAT             AS avg_confidence
            FROM movie_validation_results
        """)
    ).fetchone()

    total_movies = db.execute(
        text("SELECT COUNT(*) FROM movies WHERE tmdb_id IS NOT NULL")
    ).scalar() or 1  # guard against division by zero

    validated   = summary.total or 0
    coverage    = round(validated / total_movies * 100, 1)

    # ── Worst movies ──────────────────────────────────────────────────────────
    worst_rows = db.execute(
        text("""
            SELECT mvr.movie_id, m.title, mvr.confidence_score,
                   mvr.status, mvr.issues, mvr.last_checked_at
            FROM   movie_validation_results mvr
            JOIN   movies m ON m.id = mvr.movie_id
            WHERE  mvr.status != 'VERIFIED'
            ORDER  BY mvr.confidence_score ASC
            LIMIT  :lim
        """),
        {"lim": limit},
    ).fetchall()

    worst = [
        {
            "movie_id":        r.movie_id,
            "title":           r.title,
            "confidence_score":float(r.confidence_score),
            "status":          r.status,
            "issues":          r.issues,
            "last_checked_at": str(r.last_checked_at),
        }
        for r in worst_rows
    ]

    # ── Issue frequency (unnest the JSONB arrays) ─────────────────────────────
    issue_rows = db.execute(
        text("""
            SELECT   issue, COUNT(*) AS cnt
            FROM     movie_validation_results,
                     jsonb_array_elements_text(issues) AS issue
            GROUP BY issue
            ORDER BY cnt DESC
            LIMIT    10
        """)
    ).fetchall()

    most_common_issues = [
        {"issue": r.issue, "count": r.cnt}
        for r in issue_rows
    ]

    return {
        "summary": {
            "total_validated":  validated,
            "verified":         summary.verified or 0,
            "warning":          summary.warning  or 0,
            "broken":           summary.broken   or 0,
            "avg_confidence":   round(float(summary.avg_confidence), 3),
            "coverage_pct":     coverage,
        },
        "most_common_issues": most_common_issues,
        "worst_movies":        worst,
    }


# ─── GET /data-health/{movie_id} ─────────────────────────────────────────────

@router.get("/{movie_id}", response_model=MovieValidationResult)
def get_movie_validation(movie_id: int, db: Session = Depends(get_db)):
    """Return the stored validation result for one movie (from last run)."""
    row = db.execute(
        text("""
            SELECT mvr.movie_id, m.title, m.tmdb_id,
                   mvr.confidence_score, mvr.status,
                   mvr.issues, mvr.field_scores, mvr.last_checked_at
            FROM   movie_validation_results mvr
            JOIN   movies m ON m.id = mvr.movie_id
            WHERE  mvr.movie_id = :id
        """),
        {"id": movie_id},
    ).fetchone()

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No validation result for movie {movie_id}. Run POST /data-health/validate/{movie_id} first.",
        )

    return MovieValidationResult(
        movie_id=row.movie_id,
        title=row.title,
        tmdb_id=row.tmdb_id,
        confidence_score=float(row.confidence_score),
        status=row.status,
        issues=row.issues or [],
        field_scores=row.field_scores or {},
        last_checked_at=str(row.last_checked_at),
    )


# ─── POST /data-health/validate/{movie_id} ───────────────────────────────────

@router.post("/validate/{movie_id}", response_model=MovieValidationResult)
def validate_one_movie(
    movie_id: int,
    skip_tmdb: bool = Query(False, description="Skip TMDB API, DB checks only"),
    db: Session = Depends(get_db),
):
    """
    Re-validate one movie right now against TMDB and store the result.
    Use `skip_tmdb=true` for a fast, offline consistency check.
    """
    # Import here to avoid circular deps + keep startup fast
    from data_pipeline.validate_movies import validate_movie, _upsert_result

    result = validate_movie(movie_id, db, skip_tmdb=skip_tmdb)

    if result.issues == ["movie:not_found_in_db"]:
        raise HTTPException(status_code=404, detail=f"Movie {movie_id} not found")

    _upsert_result(result, db)
    db.commit()

    return MovieValidationResult(
        movie_id=result.movie_id,
        title=result.title,
        tmdb_id=result.tmdb_id,
        confidence_score=result.confidence_score,
        status=result.status,
        issues=result.issues,
        field_scores=result.field_scores,
        last_checked_at=result.checked_at,
    )


# ─── POST /data-health/fix/{movie_id} ────────────────────────────────────────

@router.post("/fix/{movie_id}", response_model=FixResult)
def fix_one_movie(
    movie_id: int,
    apply: bool = Query(False, description="Set to true to actually write fixes"),
    db: Session = Depends(get_db),
):
    """
    Attempt safe auto-fixes for one movie.

    By default this is a **dry-run** — it shows what WOULD be changed.
    Pass `?apply=true` to write the fixes to the database.

    Safe fixes only:
    - Missing `tmdb_id` → resolved via TMDB title search
    - Missing director  → added from TMDB crew (only if exactly 1 director)
    """
    from data_pipeline.validate_movies import fix_movie_data

    result = fix_movie_data(movie_id, db, dry_run=not apply)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return FixResult(**result)
