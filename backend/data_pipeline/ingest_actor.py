"""
ingest_actor.py
===============
CLI script that runs the full data ingestion pipeline for a single actor.

Sprint 3 change — QID-based lookups
-------------------------------------
The Wikidata query now resolves the actor via their canonical QID rather than
by name, eliminating ambiguity for common names such as "Vijay".

``ingest_actor()`` now accepts a ``wikidata_id`` parameter.  When called from
the CLI without a QID, the script looks it up automatically from the
``actor_registry`` table so existing command-line usage still works:

    python -m data_pipeline.ingest_actor "Allu Arjun"
    # ^ looks up Q352416 from actor_registry automatically

Providing the QID explicitly is faster (skips the DB lookup) and works even
for actors not yet in the registry:

    python -m data_pipeline.ingest_actor "Allu Arjun" Telugu Q352416

Pipeline steps (unchanged from Sprint 2):
  1. Query Wikidata for the actor's filmography (now via QID).
  2. Upsert the actor record into `actors`.
  3. Upsert each movie record into `movies`.
  4. Upsert each director into `directors` + `movie_directors` join table.
  5. Create cast relationships in `cast`.

Every step remains idempotent.

Usage:
    python -m data_pipeline.ingest_actor "Allu Arjun"
    python -m data_pipeline.ingest_actor "Allu Arjun" Telugu Q352416
    python -m data_pipeline.ingest_actor "Vijay"      Tamil  Q536725

Environment:
    DATABASE_URL – PostgreSQL DSN (default: postgresql://sca:sca@postgres:5432/sca)
"""

import os
import sys
from typing import Optional

# ---------------------------------------------------------------------------
# Path bootstrap — makes `app` importable when run directly.
# ---------------------------------------------------------------------------
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Actor, ActorRegistry, Cast, Director, Movie, MovieDirector
from data_pipeline.wikidata_client import fetch_actor_filmography


# ---------------------------------------------------------------------------
# Upsert helpers
# All helpers share the same contract:
#   • Operate within the caller's session / transaction.
#   • Never commit — the pipeline commits once at the very end.
#   • Flush after INSERT so the PK is available immediately.
# ---------------------------------------------------------------------------

def _get_or_create_actor(db: Session, name: str, industry: str) -> Actor:
    """
    Upsert an actor row.

    Lookup key: actors.name  (UNIQUE constraint in DB)

    Args:
        db:       Active SQLAlchemy session.
        name:     Actor's full English name, e.g. "Allu Arjun".
        industry: Film industry label, e.g. "Telugu".

    Returns:
        Actor ORM instance with populated .id.
    """
    actor = db.query(Actor).filter(Actor.name == name).first()
    if actor:
        print(f"  [~] Actor already exists : {name!r} (id={actor.id})")
        return actor

    actor = Actor(name=name, industry=industry)
    db.add(actor)
    db.flush()
    print(f"  [+] Created actor        : {name!r} (id={actor.id})")
    return actor


def _get_or_create_movie(
    db: Session,
    title: str,
    year: Optional[int],
    director: Optional[str],
    industry: str,
) -> tuple["Movie", bool]:
    """
    Upsert a movie row.

    Lookup key: (movies.title, movies.release_year).
    When *year* is unknown, release_year is stored as 0 (sentinel) and
    matching falls back to title-only.

    The legacy ``movies.director`` TEXT column is kept in sync for backward
    compatibility.  New analytics code should use the movie_directors table.

    Args:
        db:       Active SQLAlchemy session.
        title:    Film title from Wikidata.
        year:     Release year, or None.
        director: Director name for the legacy TEXT column, or None.
        industry: Industry label inherited from the ingested actor.

    Returns:
        Tuple of (Movie ORM instance, is_new: bool).
        is_new is True if the row was inserted, False if it already existed.
    """
    release_year = year if year is not None else 0

    q = db.query(Movie).filter(Movie.title == title)
    if year is not None:
        q = q.filter(Movie.release_year == year)
    movie = q.first()

    if movie:
        # Back-fill the legacy director text if the existing row is missing it.
        if director and not movie.director:
            movie.director = director
        print(f"    [~] Movie already exists : {title!r} ({release_year})")
        return movie, False

    movie = Movie(
        title=title,
        release_year=release_year,
        industry=industry,
        director=director,   # legacy TEXT — kept for backward compat
    )
    db.add(movie)
    db.flush()
    print(f"    [+] Created movie        : {title!r} ({release_year})")
    return movie, True


def _get_or_create_director(db: Session, name: str) -> Director:
    """
    Upsert a director row in the normalized `directors` table.

    Lookup key: directors.name  (UNIQUE constraint in DB)
    """
    director = db.query(Director).filter(Director.name == name).first()
    if director:
        return director   # silent — director rows are high-volume noise otherwise

    director = Director(name=name)
    db.add(director)
    db.flush()
    print(f"      [+] Created director     : {name!r} (id={director.id})")
    return director


def _get_or_create_movie_director(
    db: Session, movie_id: int, director_id: int
) -> MovieDirector:
    """
    Upsert a row in the `movie_directors` join table.

    The composite PK (movie_id, director_id) guarantees DB-level uniqueness.
    We pre-check to avoid an IntegrityError on duplicate insert.
    """
    link = (
        db.query(MovieDirector)
        .filter(
            MovieDirector.movie_id    == movie_id,
            MovieDirector.director_id == director_id,
        )
        .first()
    )
    if link:
        return link   # already linked — idempotent

    link = MovieDirector(movie_id=movie_id, director_id=director_id)
    db.add(link)
    print(f"      [+] Linked movie {movie_id} ↔ director {director_id}")
    return link


def _get_or_create_cast(db: Session, actor_id: int, movie_id: int) -> Cast:
    """
    Upsert a row in the `cast` join table.
    """
    cast = (
        db.query(Cast)
        .filter(Cast.actor_id == actor_id, Cast.movie_id == movie_id)
        .first()
    )
    if cast:
        return cast   # already linked — idempotent

    cast = Cast(actor_id=actor_id, movie_id=movie_id, role_type="Lead")
    db.add(cast)
    print(f"      [+] Linked actor {actor_id} ↔ movie  {movie_id}")
    return cast


def _resolve_qid_from_registry(db: Session, actor_name: str) -> Optional[str]:
    """
    Look up an actor's Wikidata QID from the actor_registry table by name.

    Used as a convenience fallback when the CLI caller does not supply a QID
    explicitly — so ``python -m data_pipeline.ingest_actor "Allu Arjun"``
    still works as long as the actor exists in actor_registry.

    Args:
        db:         Active SQLAlchemy session.
        actor_name: Display name to search, e.g. "Allu Arjun".

    Returns:
        QID string (e.g. "Q352416") or None if not found in registry.
    """
    entry = (
        db.query(ActorRegistry)
        .filter(ActorRegistry.name == actor_name)
        .first()
    )
    return entry.wikidata_id if entry else None


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def ingest_actor(
    actor_name:  str,
    industry:    str = "Telugu",
    wikidata_id: str = "",
) -> dict:
    """
    Run the full five-step ingestion pipeline for one actor.

    All DB writes (steps 2-5) happen in a single transaction.  The transaction
    is rolled back cleanly on any error.

    Args:
        actor_name:  Full English name of the actor, e.g. "Allu Arjun".
        industry:    Film industry label for new records (default "Telugu").
        wikidata_id: Wikidata QID, e.g. "Q352416".  If empty, the QID is
                     looked up from the actor_registry table by name.  An
                     error is raised if the QID cannot be found either way.

    Returns:
        Stats dict:
            {
                "actor":    "Allu Arjun",
                "qid":      "Q352416",
                "total":    24,   # films returned by Wikidata
                "inserted": 2,    # new movie rows created
                "skipped":  22,   # movie rows that already existed
            }

    Raises:
        ValueError:  if no QID is provided and the actor is not in the registry.
        Exception:   re-raises any DB or network error after rolling back.
    """
    sep = "=" * 62
    print(f"\n{sep}")
    print(f"  Ingesting : {actor_name}  [{industry}]")
    print(sep)

    # ------------------------------------------------------------------
    # Step 0 — Resolve QID (new in Sprint 3)
    # ------------------------------------------------------------------
    db_for_lookup: Session = SessionLocal()
    try:
        qid = wikidata_id.strip() if wikidata_id.strip() else None
        if not qid:
            print("\n[0/5] Resolving QID from actor_registry...")
            qid = _resolve_qid_from_registry(db_for_lookup, actor_name)
            if qid:
                print(f"      Found QID : {qid}")
            else:
                raise ValueError(
                    f"No Wikidata QID found for {actor_name!r}.\n"
                    f"  Either:\n"
                    f"    1. Add the actor to actor_registry, or\n"
                    f"    2. Pass the QID explicitly:\n"
                    f"       python -m data_pipeline.ingest_actor "
                    f'"{actor_name}" {industry} Q<ID>'
                )
    finally:
        db_for_lookup.close()

    # ------------------------------------------------------------------
    # Step 1 — Fetch filmography from Wikidata via QID
    # ------------------------------------------------------------------
    print(f"\n[1/5] Querying Wikidata for {actor_name} ({qid})...")
    data   = fetch_actor_filmography(wikidata_id=qid, actor_name=actor_name)
    movies = data.get("movies", [])

    if not movies:
        print("      ⚠  No films found on Wikidata.")
        print(f"         Verify QID at: https://www.wikidata.org/wiki/{qid}")
        return {"actor": actor_name, "qid": qid, "total": 0, "inserted": 0, "skipped": 0}

    directors_found = sum(1 for m in movies if m.get("director"))
    print(f"      Found {len(movies)} film(s), "
          f"{directors_found} with a known director.")

    # ------------------------------------------------------------------
    # Steps 2-5 — Write to PostgreSQL inside one transaction
    # ------------------------------------------------------------------
    inserted = 0
    skipped  = 0

    db: Session = SessionLocal()
    try:
        # Step 2 — Upsert actor
        print("\n[2/5] Upserting actor...")
        actor = _get_or_create_actor(db, actor_name, industry)

        # Steps 3-5 — Per-film loop
        print("\n[3/5] Upserting movies...")
        print("[4/5] Upserting directors + movie_directors links...")
        print("[5/5] Creating cast relationships...")
        print()

        for movie_data in movies:
            title    = movie_data["title"]
            year     = movie_data.get("year")
            dir_name = movie_data.get("director")

            # Step 3: upsert movie (also keeps legacy director TEXT in sync).
            movie, is_new = _get_or_create_movie(
                db,
                title=title,
                year=year,
                director=dir_name,
                industry=industry,
            )
            if is_new:
                inserted += 1
            else:
                skipped += 1

            # Step 4: upsert director + join-table row (if known).
            if dir_name:
                director_obj = _get_or_create_director(db, dir_name)
                _get_or_create_movie_director(db, movie.id, director_obj.id)

            # Step 5: upsert cast link (actor ↔ movie).
            _get_or_create_cast(db, actor.id, movie.id)

        db.commit()

        print(f"\n  ✓ Done! {actor_name} ({qid})")
        print(f"    Total films  : {len(movies)}")
        print(f"    Inserted     : {inserted} new movie(s)")
        print(f"    Skipped      : {skipped} already in DB")

    except Exception as exc:
        db.rollback()
        print(f"\n  ✗ Ingestion failed — transaction rolled back.")
        print(f"    Error: {exc}")
        raise

    finally:
        db.close()

    return {
        "actor":    actor_name,
        "qid":      qid,
        "total":    len(movies),
        "inserted": inserted,
        "skipped":  skipped,
    }


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage  : python -m data_pipeline.ingest_actor <actor_name> [industry] [QID]")
        print()
        print("Examples:")
        print('  python -m data_pipeline.ingest_actor "Allu Arjun"')
        print('  python -m data_pipeline.ingest_actor "Allu Arjun" Telugu Q352416')
        print('  python -m data_pipeline.ingest_actor "Vijay"      Tamil  Q536725')
        print()
        print("If QID is omitted, it is looked up from the actor_registry table.")
        sys.exit(1)

    _name     = sys.argv[1]
    _industry = sys.argv[2] if len(sys.argv) > 2 else "Telugu"
    _qid      = sys.argv[3] if len(sys.argv) > 3 else ""

    ingest_actor(_name, _industry, _qid)
