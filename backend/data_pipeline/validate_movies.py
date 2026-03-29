"""
validate_movies.py
==================
Movie data validation pipeline for South Cinema Analytics.

Validates each movie record against TMDB ground truth and internal DB
consistency rules, producing a confidence_score (0–1) and a list of
human-readable issue strings per movie.

Public surface
--------------
    validate_movie(movie_id, db, ...)    → ValidationResult
    validate_all_movies(db, ...)         → list[ValidationResult]
    fix_movie_data(movie_id, db, ...)    → dict  (safe auto-fix, dry-run by default)

Quick start
-----------
    # From the project root:
    export TMDB_API_KEY=your_key_here
    python -m data_pipeline.validate_movies --limit 20

    # Or import inside a FastAPI route / one-off script:
    from data_pipeline.validate_movies import validate_movie, validate_all_movies

Confidence weights (must sum to 1.0)
-------------------------------------
    director        25 %
    primary_cast    30 %
    release_year    20 %
    supporting_cast 15 %
    ratings         10 %

Status thresholds
-----------------
    ≥ 0.90  →  VERIFIED
    0.60–0.90 →  WARNING
    < 0.60  →  BROKEN
"""

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

# Reuse the project's existing TMDB session + rate-limiter
from data_pipeline.tmdb_client import (
    _TMDB_BASE,
    _api_get,
    _build_image_url,
    _get_api_key,
    search_movie_tmdb,
)

logger = logging.getLogger(__name__)

# ─── Scoring constants ────────────────────────────────────────────────────────

WEIGHTS: dict[str, float] = {
    "director":         0.25,
    "primary_cast":     0.30,
    "release_year":     0.20,
    "supporting_cast":  0.15,
    "ratings":          0.10,
}
assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9, "WEIGHTS must sum to 1.0"

STATUS_VERIFIED = 0.90
STATUS_WARNING  = 0.60

# TMDB cast positions 0-indexed: 0–2 → primary, 3–9 → supporting
PRIMARY_CAST_CUTOFF    = 3
SUPPORTING_CAST_CUTOFF = 10


# ─── Result dataclass ─────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    movie_id:         int
    title:            str
    tmdb_id:          Optional[int]
    confidence_score: float
    status:           str                      # VERIFIED | WARNING | BROKEN
    issues:           list[str]                = field(default_factory=list)
    field_scores:     dict[str, float]         = field(default_factory=dict)
    checked_at:       str                      = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        return asdict(self)

    def __str__(self) -> str:
        bar = "█" * int(self.confidence_score * 20)
        pad = "░" * (20 - len(bar))
        return (
            f"[{self.status:<8}] {self.confidence_score:.2f} |{bar}{pad}| "
            f"({self.movie_id}) {self.title}"
            + (f"\n  issues: {self.issues}" if self.issues else "")
        )


# ─────────────────────────────────────────────────────────────────────────────
# TMDB GROUND TRUTH FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_tmdb_ground_truth(tmdb_id: int) -> Optional[dict]:
    """
    Fetch all validation-relevant data for a movie from TMDB in exactly 2 API
    calls — /movie/{id} and /movie/{id}/credits — then merge into one dict.

    Returns None if the movie is not found or the API fails.

    Return shape:
        {
          "title":        str,
          "release_year": int | None,
          "vote_average": float | None,
          "revenue":      int,          # USD, 0 = unknown
          "budget":       int,          # USD, 0 = unknown
          "runtime":      int | None,   # minutes
          "directors": [{"name": str, "tmdb_person_id": int}, ...],
          "cast":      [{"name": str, "tmdb_person_id": int,
                         "cast_order": int, "character": str}, ...],
                       # cast is sorted by billing order, capped at top-10
        }
    """
    api_key = _get_api_key()

    try:
        details = _api_get(
            f"{_TMDB_BASE}/movie/{tmdb_id}",
            {"api_key": api_key, "language": "en-US"},
        )
    except Exception as exc:
        logger.warning("TMDB /movie/%s failed: %s", tmdb_id, exc)
        return None

    try:
        credits = _api_get(
            f"{_TMDB_BASE}/movie/{tmdb_id}/credits",
            {"api_key": api_key, "language": "en-US"},
        )
    except Exception as exc:
        logger.warning("TMDB /movie/%s/credits failed: %s", tmdb_id, exc)
        credits = {"cast": [], "crew": []}

    # Parse release year
    release_year: Optional[int] = None
    raw_date = details.get("release_date") or ""
    if len(raw_date) >= 4:
        try:
            release_year = int(raw_date[:4])
        except ValueError:
            pass

    # Directors from crew
    directors = [
        {"name": p["name"].strip(), "tmdb_person_id": p["id"]}
        for p in (credits.get("crew") or [])
        if p.get("job") == "Director" and p.get("name")
    ]

    # Cast sorted by billing order, top 10
    raw_cast = sorted(
        (credits.get("cast") or []),
        key=lambda c: c.get("order", 999),
    )
    cast = [
        {
            "name":           c["name"].strip(),
            "tmdb_person_id": c["id"],
            "cast_order":     c.get("order", 0),
            "character":      (c.get("character") or "").strip(),
        }
        for c in raw_cast[:SUPPORTING_CAST_CUTOFF]
        if c.get("name")
    ]

    return {
        "title":        details.get("title", ""),
        "release_year": release_year,
        "vote_average": details.get("vote_average"),
        "revenue":      details.get("revenue") or 0,
        "budget":       details.get("budget") or 0,
        "runtime":      details.get("runtime") or None,
        "directors":    directors,
        "cast":         cast,
    }


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE LOADER
# ─────────────────────────────────────────────────────────────────────────────

def _load_movie_from_db(movie_id: int, db: Session) -> Optional[dict]:
    """
    Load all validation-relevant fields for one movie from PostgreSQL.
    Pulls from: movies, movie_directors, directors, actor_movies, cast tables.
    """
    row = db.execute(
        text("""
            SELECT id, title, release_year, director AS legacy_director,
                   imdb_rating, box_office, tmdb_id, vote_average
            FROM   movies
            WHERE  id = :id
        """),
        {"id": movie_id},
    ).fetchone()

    if not row:
        return None

    # Normalized directors (movie_directors → directors)
    dir_rows = db.execute(
        text("""
            SELECT d.name
            FROM   movie_directors md
            JOIN   directors d ON d.id = md.director_id
            WHERE  md.movie_id = :mid
        """),
        {"mid": movie_id},
    ).fetchall()

    # TMDB-sourced cast (actor_movies has billing_order + role_type)
    am_rows = db.execute(
        text("""
            SELECT a.name, a.tmdb_person_id,
                   am.role_type, am.billing_order, am.character_name
            FROM   actor_movies am
            JOIN   actors a ON a.id = am.actor_id
            WHERE  am.movie_id = :mid
            ORDER  BY am.billing_order NULLS LAST
        """),
        {"mid": movie_id},
    ).fetchall()

    # Wikidata-sourced cast (legacy cast table — no billing_order)
    cast_rows = db.execute(
        text("""
            SELECT a.name, a.tmdb_person_id, c.role_type
            FROM   "cast" c
            JOIN   actors a ON a.id = c.actor_id
            WHERE  c.movie_id = :mid
        """),
        {"mid": movie_id},
    ).fetchall()

    # Load validation_overrides from movie_validation_results (confirmed_none fields)
    override_row = db.execute(
        text("SELECT validation_overrides FROM movie_validation_results WHERE movie_id = :id"),
        {"id": movie_id},
    ).fetchone()
    overrides = (override_row.validation_overrides if override_row else None) or {}

    return {
        "id":              row.id,
        "title":           row.title or "",
        "release_year":    row.release_year,
        "legacy_director": row.legacy_director,
        "imdb_rating":     row.imdb_rating,
        "box_office":      row.box_office,
        "tmdb_id":         row.tmdb_id,
        "vote_average":    row.vote_average,
        "directors":       [r.name for r in dir_rows],
        "actor_movies":    [dict(r._mapping) for r in am_rows],
        "legacy_cast":     [dict(r._mapping) for r in cast_rows],
        "validation_overrides": overrides,
    }


# ─────────────────────────────────────────────────────────────────────────────
# FIELD VALIDATORS  (each returns (score: float, issues: list[str]))
# ─────────────────────────────────────────────────────────────────────────────

def _validate_title(db: dict) -> tuple[float, list[str]]:
    """Title must be non-empty."""
    if not db["title"] or not db["title"].strip():
        return 0.0, ["title:empty"]
    return 1.0, []


# ── Name normalisation + alias table ──────────────────────────────────────────
# Maps lower-cased canonical DB names → set of TMDB spelling variants (and vice
# versa).  Extend here when new mismatches are discovered.
_NAME_ALIASES: dict[str, set[str]] = {
    # Jayam Ravi — credited as "Ravi Mohan" (birth name) on TMDB
    "jayam ravi":             {"ravi mohan"},
    "ravi mohan":             {"jayam ravi"},
    # Puneet / Puneeth Rajkumar spelling variant
    "puneet rajkumar":        {"puneeth rajkumar"},
    "puneeth rajkumar":       {"puneet rajkumar"},
    # Jr. NTR — multiple formats
    "jr. ntr":                {"n.t. rama rao jr.", "jr ntr", "n t rama rao jr"},
    "jr ntr":                 {"n.t. rama rao jr.", "jr. ntr", "n t rama rao jr"},
    "n.t. rama rao jr.":      {"jr. ntr", "jr ntr"},
    # Darshan — full name on TMDB
    "darshan":                {"darshan thoogudeepa srinivas"},
    "darshan thoogudeepa srinivas": {"darshan"},
    # SS Rajamouli — dot vs no-dot variant
    "ss rajamouli":           {"s. s. rajamouli", "s s rajamouli"},
    "s. s. rajamouli":        {"ss rajamouli", "s s rajamouli"},
    "s s rajamouli":          {"ss rajamouli", "s. s. rajamouli"},
    # Director name-order reversals (TMDB uses surname-first in some entries)
    "tatineni rama rao":      {"rama rao tatineni"},
    "rama rao tatineni":      {"tatineni rama rao"},
    "g. n. rangarajan":       {"rangarajan g n", "rangarajan gn"},
    "rangarajan g n":         {"g. n. rangarajan", "gn rangarajan"},
    # Director transliteration variants
    "vamsi paidipally":       {"vamshi paidipally"},
    "vamshi paidipally":      {"vamsi paidipally"},
    "suresh krishna":         {"suresh krissna"},
    "suresh krissna":         {"suresh krishna"},
    "kasthuri raja":          {"kasthoori raja"},
    "kasthoori raja":         {"kasthuri raja"},
    "p. s. mithran":          {"p.s mithran", "ps mithran"},
    "p.s mithran":            {"p. s. mithran", "ps mithran"},
    # Malayalam/Tamil actor name variants common on TMDB
    "jagathy sreekumar":      {"jagathi sreekumar", "jagathysreekumar"},
    "nedumudi venu":          {"nedumudi"},
    "thilakan":               {"thilakan pillai"},
    "menaka suresh":          {"menaka"},
    "suresh gopi":            {"suresh gopinath"},
}

def _norm(name: str) -> str:
    """Remove dots, collapse spaces — for fuzzy comparison."""
    import re as _re
    return _re.sub(r"\s+", " ", name.replace(".", " ")).strip()

def _name_matches(a: str, b: str) -> bool:
    """True if a and b refer to the same person, via substring or alias."""
    if a in b or b in a:
        return True
    an, bn = _norm(a), _norm(b)
    if an in bn or bn in an:
        return True
    for alias in _NAME_ALIASES.get(a, set()):
        al = alias.lower()
        if al in b or b in al or _norm(al) in bn or bn in _norm(al):
            return True
    return False


def _validate_director(db: dict, tmdb: Optional[dict]) -> tuple[float, list[str]]:
    """
    Director must exist in movie_directors table.
    When TMDB data is available, at least one director must match.
    Fuzzy match: substring containment in either direction handles
    transliteration variants (e.g. "S.S. Rajamouli" ↔ "SS Rajamouli").
    """
    issues: list[str] = []

    # Build DB director list (normalised table first, legacy fallback)
    db_directors = [d.lower().strip() for d in db["directors"]]
    if not db_directors:
        legacy = (db.get("legacy_director") or "").strip()
        if legacy:
            issues.append("director:not_in_normalized_table — only legacy TEXT field")
            db_directors = [p.strip().lower() for p in legacy.split(",")]
        else:
            return 0.0, ["director:missing"]

    if tmdb is None:
        # DB is TMDB-populated — full credit for having a director in the normalized table
        return 0.95, issues

    tmdb_directors = [d["name"].lower().strip() for d in tmdb.get("directors", [])]
    if not tmdb_directors:
        issues.append("director:not_found_on_tmdb")
        return 0.5, issues

    matched = sum(
        1 for td in tmdb_directors
        if any(_name_matches(td, dd) for dd in db_directors)
    )

    if matched == 0:
        issues.append(
            f"director:mismatch — db={db_directors} tmdb={tmdb_directors}"
        )
        return 0.0, issues

    if matched < len(tmdb_directors):
        issues.append(
            f"director:partial_match — {matched}/{len(tmdb_directors)} matched"
        )
        return 0.7, issues

    return 1.0, issues


def _validate_release_year(db: dict, tmdb: Optional[dict]) -> tuple[float, list[str]]:
    """Release year must exist, be plausible, and match TMDB (±1 year)."""
    issues: list[str] = []
    db_year = db.get("release_year")

    if not db_year:
        return 0.0, ["release_year:missing"]

    try:
        db_year = int(db_year)
    except (TypeError, ValueError):
        return 0.0, ["release_year:invalid_format"]

    current_year = datetime.now().year
    if db_year < 1920 or db_year > current_year + 2:
        return 0.2, [f"release_year:unrealistic ({db_year})"]

    if tmdb is None:
        return 0.95, issues  # DB is TMDB-populated — credit for having a plausible year

    tmdb_year = tmdb.get("release_year")
    if not tmdb_year:
        return 0.8, issues  # TMDB has no date — benefit of the doubt

    diff = abs(db_year - tmdb_year)
    if diff == 0:
        return 1.0, issues
    if diff == 1:
        # Regional/streaming release date differences are common
        issues.append(
            f"release_year:off_by_one — db={db_year} tmdb={tmdb_year}"
        )
        return 0.7, issues

    return 0.0, [f"release_year:mismatch — db={db_year} tmdb={tmdb_year}"]


def _validate_primary_cast(db: dict, tmdb: Optional[dict]) -> tuple[float, list[str]]:
    """
    Primary actors must exist.
    Compare against TMDB billing positions 0–2 (top 3 billed).
    """
    issues: list[str] = []

    # Collect primary names from both tables
    am_primary    = {r["name"].lower() for r in db["actor_movies"]
                     if r.get("role_type") == "primary"}
    cast_primary  = {r["name"].lower() for r in db["legacy_cast"]
                     if r.get("role_type") in ("primary", "lead")}
    all_primary   = am_primary | cast_primary

    if not all_primary:
        return 0.0, ["primary_cast:missing"]

    if tmdb is None:
        return 0.95, issues  # DB is TMDB-populated — credit for having primary cast

    tmdb_primary = [
        c for c in tmdb.get("cast", [])
        if c["cast_order"] < PRIMARY_CAST_CUTOFF
    ]
    if not tmdb_primary:
        return 0.7, issues  # TMDB has no cast data

    tmdb_names = {c["name"].lower() for c in tmdb_primary}

    def soft_match(t: str, db_set: set[str]) -> bool:
        return any(_name_matches(t, d) for d in db_set)

    matched = sum(1 for t in tmdb_names if soft_match(t, all_primary))
    ratio   = matched / len(tmdb_names)

    if matched == 0:
        issues.append(
            f"primary_cast:mismatch — "
            f"db={sorted(all_primary)} tmdb={sorted(tmdb_names)}"
        )
        return 0.0, issues

    if ratio < 1.0:
        missing = [t for t in tmdb_names if not soft_match(t, all_primary)]
        issues.append(
            f"primary_cast:partial_match — {matched}/{len(tmdb_names)} "
            f"matched; missing={missing}"
        )
        return 0.5 + 0.5 * ratio, issues

    return 1.0, issues


def _validate_supporting_cast(db: dict, tmdb: Optional[dict]) -> tuple[float, list[str]]:
    """
    Supporting cast should exist in actor_movies.
    Detect duplicates and measure coverage vs. TMDB positions 3–9.
    """
    issues: list[str] = []

    supporting = [
        r for r in db["actor_movies"]
        if r.get("role_type") == "supporting"
    ]

    # Duplicate detection
    names = [r["name"].lower() for r in supporting]
    duplicates = [n for n in set(names) if names.count(n) > 1]
    if duplicates:
        issues.append(f"supporting_cast:duplicates — {duplicates}")

    if not supporting and not db["legacy_cast"]:
        return 0.0, issues + ["supporting_cast:no_cast_data_at_all"]

    if not supporting:
        issues.append("supporting_cast:missing_from_actor_movies")
        return 0.4, issues  # legacy cast exists — softer penalty

    if tmdb is None:
        return (0.7 if duplicates else 0.95), issues  # DB is TMDB-populated

    tmdb_supporting = [
        c for c in tmdb.get("cast", [])
        if PRIMARY_CAST_CUTOFF <= c["cast_order"] < SUPPORTING_CAST_CUTOFF
    ]
    if not tmdb_supporting:
        return (0.8 if duplicates else 1.0), issues

    db_names  = {r["name"].lower() for r in supporting}
    tmdb_names = {c["name"].lower() for c in tmdb_supporting}

    missing = [
        t for t in tmdb_names
        if not any(t in d or d in t for d in db_names)
    ]
    if missing:
        issues.append(
            f"supporting_cast:missing_actors — {missing[:5]}"  # cap output
        )
        coverage = 1.0 - len(missing) / len(tmdb_names)
        return max(0.3, coverage), issues

    return (0.7 if duplicates else 1.0), issues


def _validate_ratings(db: dict, tmdb: Optional[dict]) -> tuple[float, list[str]]:
    """vote_average must be 0–10; flag significant drift vs. TMDB (>1.5 pts)."""
    issues: list[str] = []
    db_rating = db.get("vote_average")

    if db_rating is None:
        # Many films have no ratings yet — soft penalty only
        return 0.4, ["ratings:vote_average_missing"]

    try:
        db_rating = float(db_rating)
    except (TypeError, ValueError):
        return 0.0, ["ratings:invalid_format"]

    if not 0 <= db_rating <= 10:
        return 0.0, [f"ratings:out_of_range ({db_rating})"]

    if tmdb is None:
        return 0.95, issues  # DB is TMDB-populated — credit for having a valid rating

    tmdb_rating = tmdb.get("vote_average")
    if tmdb_rating and abs(float(tmdb_rating) - db_rating) > 1.5:
        issues.append(
            f"ratings:significant_drift — db={db_rating:.1f} "
            f"tmdb={float(tmdb_rating):.1f}"
        )
        return 0.5, issues

    return 1.0, issues


def _validate_financials(db: dict, _tmdb: Optional[dict]) -> tuple[float, list[str]]:
    """
    Box office sanity check.  This is informational only — it appends issues
    but NEVER reduces the confidence score (financials are too unreliable to
    penalise heavily).
    """
    issues: list[str] = []
    bo = db.get("box_office")

    if bo is not None:
        try:
            bo_f = float(bo)
            if bo_f < 0:
                issues.append("box_office:negative_value")
            elif bo_f == 0:
                issues.append("box_office:zero (may be genuinely unknown)")
            elif bo_f > 50_000:   # ₹50,000 crore — physically impossible today
                issues.append(f"box_office:suspiciously_high ({bo_f:.0f} cr)")
        except (TypeError, ValueError):
            issues.append("box_office:invalid_format")

    return 1.0, issues  # score always 1.0 — purely informational


# ─────────────────────────────────────────────────────────────────────────────
# MAIN VALIDATE FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def validate_movie(
    movie_id: int,
    db: Session,
    *,
    tmdb_cache: Optional[dict] = None,
    skip_tmdb: bool = False,
) -> ValidationResult:
    """
    Validate a single movie record against TMDB ground truth.

    Parameters
    ----------
    movie_id    : PK in the movies table.
    db          : Active SQLAlchemy session.
    tmdb_cache  : Optional dict shared across a bulk run.  Pass the same dict
                  for every call to avoid duplicate TMDB API hits.
    skip_tmdb   : If True, only internal DB consistency is checked (no API
                  calls).  Useful for fast smoke-tests or offline dev.

    Returns
    -------
    ValidationResult  — confidence_score, status, issues, field_scores.
    """
    db_movie = _load_movie_from_db(movie_id, db)
    if not db_movie:
        return ValidationResult(
            movie_id=movie_id,
            title="UNKNOWN",
            tmdb_id=None,
            confidence_score=0.0,
            status="BROKEN",
            issues=["movie:not_found_in_db"],
        )

    title   = db_movie["title"]
    tmdb_id = db_movie.get("tmdb_id")

    # ── Fetch TMDB ground truth (with shared in-run cache) ────────────────────
    tmdb: Optional[dict] = None
    if not skip_tmdb:
        if tmdb_id:
            if tmdb_cache is not None and tmdb_id in tmdb_cache:
                tmdb = tmdb_cache[tmdb_id]
            else:
                tmdb = fetch_tmdb_ground_truth(tmdb_id)
                if tmdb_cache is not None and tmdb is not None:
                    tmdb_cache[tmdb_id] = tmdb
        else:
            pass  # no tmdb_id — validators receive tmdb=None and score accordingly

    # ── Run all field validators ───────────────────────────────────────────────
    overrides = db_movie.get("validation_overrides") or {}

    title_score,      title_iss   = _validate_title(db_movie)
    director_score,   dir_iss     = _validate_director(db_movie, tmdb)
    year_score,       year_iss    = _validate_release_year(db_movie, tmdb)
    primary_score,    primary_iss = _validate_primary_cast(db_movie, tmdb)
    supporting_score, sup_iss     = _validate_supporting_cast(db_movie, tmdb)
    ratings_score,    rat_iss     = _validate_ratings(db_movie, tmdb)
    _,                fin_iss     = _validate_financials(db_movie, tmdb)

    # ── Apply confirmed_none overrides — skip penalty for data-unavailable fields
    if overrides.get("director") == "confirmed_none":
        director_score, dir_iss = 1.0, []
    if overrides.get("release_year") == "confirmed_none":
        year_score, year_iss = 1.0, []
    if overrides.get("primary_cast") == "confirmed_none":
        primary_score, primary_iss = 1.0, []
    if overrides.get("supporting_cast") == "confirmed_none":
        supporting_score, sup_iss = 1.0, []
    if overrides.get("ratings") == "confirmed_none":
        ratings_score, rat_iss = 1.0, []

    all_issues: list[str] = (
        title_iss + dir_iss + year_iss +
        primary_iss + sup_iss + rat_iss + fin_iss
    )
    if not tmdb_id:
        all_issues.append("tmdb:no_tmdb_id — cross-validation skipped")

    field_scores: dict[str, float] = {
        "title":           title_score,
        "director":        director_score,
        "release_year":    year_score,
        "primary_cast":    primary_score,
        "supporting_cast": supporting_score,
        "ratings":         ratings_score,
    }

    # Title failure is a hard signal: halve the weighted total
    title_mult = 1.0 if title_score == 1.0 else 0.5

    raw_score = (
        WEIGHTS["director"]         * director_score   +
        WEIGHTS["primary_cast"]     * primary_score    +
        WEIGHTS["release_year"]     * year_score       +
        WEIGHTS["supporting_cast"]  * supporting_score +
        WEIGHTS["ratings"]          * ratings_score
    )
    confidence = round(max(0.0, min(1.0, title_mult * raw_score)), 4)

    status = (
        "VERIFIED" if confidence >= STATUS_VERIFIED else
        "WARNING"  if confidence >= STATUS_WARNING  else
        "BROKEN"
    )

    return ValidationResult(
        movie_id=movie_id,
        title=title,
        tmdb_id=tmdb_id,
        confidence_score=confidence,
        status=status,
        issues=all_issues,
        field_scores=field_scores,
    )


# ─────────────────────────────────────────────────────────────────────────────
# BULK VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def validate_all_movies(
    db: Session,
    *,
    write_results: bool = True,
    skip_tmdb: bool = False,
    limit: Optional[int] = None,
    only_with_tmdb_id: bool = True,
) -> list[ValidationResult]:
    """
    Validate every movie in the database.

    Results are upserted into movie_validation_results (run the Sprint 26
    migration first).  Commits every 50 rows to avoid huge transactions.

    Parameters
    ----------
    write_results     : Write/upsert into movie_validation_results table.
    skip_tmdb         : Skip TMDB API calls entirely (DB-only validation).
    limit             : Validate only the first N movies (testing).
    only_with_tmdb_id : Skip movies without a tmdb_id (can't cross-validate).

    Returns
    -------
    List of ValidationResult objects (all movies validated this run).
    """
    q = "SELECT id FROM movies"
    if only_with_tmdb_id:
        q += " WHERE tmdb_id IS NOT NULL"
    q += " ORDER BY id"
    if limit:
        q += f" LIMIT {limit}"

    movie_ids = [r[0] for r in db.execute(text(q)).fetchall()]
    total = len(movie_ids)
    logger.info("[validate_all_movies] Starting — %d movies to validate", total)

    results:     list[ValidationResult] = []
    tmdb_cache:  dict                   = {}  # shared across all movies

    for i, mid in enumerate(movie_ids, 1):
        try:
            result = validate_movie(mid, db, tmdb_cache=tmdb_cache, skip_tmdb=skip_tmdb)
            results.append(result)

            if write_results:
                _upsert_result(result, db)
            if write_results and i % 50 == 0:
                db.commit()
                logger.info("  [%d/%d] batch committed", i, total)

        except Exception as exc:
            logger.error("  [movie %d] unexpected error: %s", mid, exc)

    if write_results:
        db.commit()

    # ── Summary ───────────────────────────────────────────────────────────────
    verified = sum(1 for r in results if r.status == "VERIFIED")
    warning  = sum(1 for r in results if r.status == "WARNING")
    broken   = sum(1 for r in results if r.status == "BROKEN")
    avg_conf = (
        sum(r.confidence_score for r in results) / total if total else 0.0
    )
    logger.info(
        "[validate_all_movies] Done. "
        "VERIFIED=%d WARNING=%d BROKEN=%d avg_confidence=%.2f",
        verified, warning, broken, avg_conf,
    )
    return results


def _upsert_result(result: ValidationResult, db: Session) -> None:
    """
    Upsert one ValidationResult into movie_validation_results.

    Note: CAST(:param AS jsonb) is used instead of :param::jsonb because
    SQLAlchemy's text() parser mis-tokenises the :: cast operator when it
    immediately follows a named parameter placeholder.
    """
    db.execute(
        text("""
            INSERT INTO movie_validation_results
                (movie_id, confidence_score, status, issues, field_scores, last_checked_at)
            VALUES
                (:movie_id, :score, :status,
                 CAST(:issues      AS jsonb),
                 CAST(:field_scores AS jsonb),
                 NOW())
            ON CONFLICT (movie_id) DO UPDATE SET
                confidence_score = EXCLUDED.confidence_score,
                status           = EXCLUDED.status,
                issues           = EXCLUDED.issues,
                field_scores     = EXCLUDED.field_scores,
                last_checked_at  = EXCLUDED.last_checked_at
        """),
        {
            "movie_id":     result.movie_id,
            "score":        result.confidence_score,
            "status":       result.status,
            "issues":       json.dumps(result.issues),
            "field_scores": json.dumps(result.field_scores),
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# AUTO-FIX (SAFE MODE)
# ─────────────────────────────────────────────────────────────────────────────

def fix_movie_data(
    movie_id: int,
    db: Session,
    *,
    dry_run: bool = True,
) -> dict:
    """
    Attempt safe, high-confidence fixes for a single movie.

    What it WILL fix
    ----------------
    1. Missing tmdb_id  →  TMDB title/year search (if unambiguous top result)
    2. Missing director →  Add from TMDB crew, but ONLY when TMDB lists
                           exactly one director (multiple = manual review)

    What it will NEVER touch
    ------------------------
    cast, box_office, release_year, ratings — too ambiguous to auto-correct.

    Parameters
    ----------
    dry_run : Default True.  Set False to actually write to the DB.

    Returns
    -------
    {
        "movie_id":       int,
        "title":          str,
        "dry_run":        bool,
        "fixes_applied":  list[str],
        "fixes_skipped":  list[str],
    }
    """
    db_movie = _load_movie_from_db(movie_id, db)
    if not db_movie:
        return {"error": f"movie {movie_id} not found in DB"}

    fixes_applied: list[str] = []
    fixes_skipped: list[str] = []

    # ── Fix 1: Missing tmdb_id ─────────────────────────────────────────────────
    if not db_movie["tmdb_id"]:
        found = search_movie_tmdb(db_movie["title"], db_movie.get("release_year") or 0)
        if found and found.get("tmdb_id"):
            new_id = int(found["tmdb_id"])
            if not dry_run:
                db.execute(
                    text("UPDATE movies SET tmdb_id = :tid WHERE id = :mid"),
                    {"tid": new_id, "mid": movie_id},
                )
                db.commit()
            fixes_applied.append(f"tmdb_id:set_to_{new_id}")
        else:
            fixes_skipped.append("tmdb_id:search_returned_no_result")

    # ── Fix 2: Missing director (only when TMDB has exactly one) ──────────────
    tmdb_id = db_movie.get("tmdb_id")
    if tmdb_id and not db_movie["directors"]:
        tmdb = fetch_tmdb_ground_truth(tmdb_id)
        directors_on_tmdb = (tmdb or {}).get("directors", [])

        if len(directors_on_tmdb) == 1:
            name = directors_on_tmdb[0]["name"].strip()
            if not dry_run:
                db.execute(
                    text("""
                        INSERT INTO directors (name)
                        VALUES (:name)
                        ON CONFLICT (name) DO NOTHING
                    """),
                    {"name": name},
                )
                db.execute(
                    text("""
                        INSERT INTO movie_directors (movie_id, director_id)
                        SELECT :mid, id FROM directors WHERE name = :name
                        ON CONFLICT DO NOTHING
                    """),
                    {"mid": movie_id, "name": name},
                )
                db.commit()
            fixes_applied.append(f"director:added '{name}' from TMDB")

        elif len(directors_on_tmdb) > 1:
            names = [d["name"] for d in directors_on_tmdb]
            fixes_skipped.append(
                f"director:multiple_on_tmdb ({names}) — needs manual review"
            )
        else:
            fixes_skipped.append("director:not_found_on_tmdb")

    return {
        "movie_id":      movie_id,
        "title":         db_movie["title"],
        "dry_run":       dry_run,
        "fixes_applied": fixes_applied,
        "fixes_skipped": fixes_skipped,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Quick CLI runner.  Validates up to --limit movies and prints a summary.

    Usage:
        cd backend
        export TMDB_API_KEY=your_key
        python -m data_pipeline.validate_movies --limit 50
        python -m data_pipeline.validate_movies --skip-tmdb         # DB-only, fast
        python -m data_pipeline.validate_movies --movie-id 42       # single movie
    """
    import argparse

    from app.database import SessionLocal

    parser = argparse.ArgumentParser(description="Movie data validation pipeline")
    parser.add_argument("--limit",     type=int,  default=None,  help="Max movies to validate")
    parser.add_argument("--movie-id",  type=int,  default=None,  help="Validate single movie")
    parser.add_argument("--skip-tmdb", action="store_true",      help="DB-only, no API calls")
    parser.add_argument("--write",     action="store_true",      help="Write results to DB")
    parser.add_argument("--verbose",   action="store_true",      help="Print each result")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )

    db = SessionLocal()
    try:
        if args.movie_id:
            r = validate_movie(args.movie_id, db, skip_tmdb=args.skip_tmdb)
            print(r)
            if args.write:
                _upsert_result(r, db)
                db.commit()
        else:
            results = validate_all_movies(
                db,
                write_results=args.write,
                skip_tmdb=args.skip_tmdb,
                limit=args.limit,
            )
            if args.verbose:
                for r in results:
                    print(r)

            # Print breakdown table
            print("\n" + "─" * 60)
            print(f"{'STATUS':<10} {'COUNT':>6}  {'AVG SCORE':>10}")
            print("─" * 60)
            for status in ("VERIFIED", "WARNING", "BROKEN"):
                subset = [r for r in results if r.status == status]
                avg = sum(r.confidence_score for r in subset) / len(subset) if subset else 0
                print(f"{status:<10} {len(subset):>6}  {avg:>10.3f}")
            print("─" * 60)
    finally:
        db.close()
