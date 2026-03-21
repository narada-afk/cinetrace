"""
ingest_bo_leads.py
==================
One-shot script to close the lead-actor gap found in the box-office audit
(Sprint 24).  Three groups of actors are handled in a single pipeline run:

  Group A — NEW actors: completely absent from the actors table.
             Inserted with is_primary_actor=TRUE and the correct industry.

  Group B — PROMOTE: already in the DB but is_primary_actor=FALSE
             (or wrong industry).  Promoted + industry fixed in-place.

  Group C — ALREADY PRIMARY: Rajinikanth & Sivakarthikeyan were primary but
             had one or two films not yet linked via actor_movies.
             Re-fetching their filmography fills the gap idempotently.

For every actor (all three groups) the script also fetches their complete
TMDB filmography and upserts actor_movies rows so the box-office leaderboard
links to the correct lead.

After all actor runs the script rebuilds the analytics tables
(actor_collaborations, actor_stats) so the homepage reflects the new data
immediately.

Idempotency
-----------
All DB writes use ON CONFLICT DO NOTHING or UPDATE-by-id, so the script can
be re-run safely.

Usage
-----
  # From the backend/ directory:
  export TMDB_API_KEY=your_key_here
  python -m data_pipeline.ingest_bo_leads
  python -m data_pipeline.ingest_bo_leads --dry-run
  python -m data_pipeline.ingest_bo_leads --actor "Ravi Teja"
  python -m data_pipeline.ingest_bo_leads --skip-analytics

Flags
-----
  --dry-run          Print all planned actions without writing to the database.
  --actor NAME       Process only this actor (case-insensitive name match).
  --skip-analytics   Skip the final analytics-table rebuild.

Environment
-----------
  DATABASE_URL   PostgreSQL DSN (default: postgresql://sca:sca@postgres:5432/sca)
  TMDB_API_KEY   Your TMDB v3 API key (required)
"""

import argparse
import os
import sys
import time
from typing import Optional

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from data_pipeline.tmdb_client import fetch_person_movie_credits, search_person_tmdb


# ---------------------------------------------------------------------------
# Actor registry
# ---------------------------------------------------------------------------
# Each tuple: (display_name, industry, tmdb_person_id)
# All TMDB person IDs are hardcoded to prevent search-name ambiguity.
# Industries follow the movies.industry convention: Telugu/Tamil/Malayalam/Kannada.

ACTORS_TO_PROCESS: list[tuple[str, str, int]] = [
    # ── Group A: New actors ─────────────────────────────────────────────────
    # Telugu
    ("Ravi Teja",              "Telugu",    146935),    # verified: 81 Telugu films
    ("Venkatesh Daggubati",    "Telugu",    88166),     # verified: 94 Telugu films
    ("Varun Tej",              "Telugu",    1407562),   # verified: 16 Telugu films
    ("Sharwanand",             "Telugu",    588007),    # verified: 37 Telugu films
    ("Siddhu Jonnalagadda",    "Telugu",    931864),    # verified: 21 Telugu films incl (Tillu)²
    ("Vishwak Sen",            "Telugu",    1893251),   # verified: 21 Telugu films incl Gaami
    ("Nandamuri Balakrishna",  "Telugu",    150529),
    # Tamil
    ("Vijay Antony",           "Tamil",     237610),    # verified: 27 Tamil films
    ("Arjun Sarja",            "Tamil",     544977),
    # Kannada
    ("Ganesh",                 "Kannada",   141837),
    # Malayalam
    ("Mukesh",                 "Malayalam", 82733),

    # ── Group B: Promote to primary + fix industry where needed ────────────
    # Telugu (Nagarjuna was misclassified as Tamil)
    ("Chiranjeevi",            "Telugu",    147079),
    ("Nagarjuna Akkineni",     "Telugu",    149958),   # was: industry=Tamil
    ("Rana Daggubati",         "Telugu",    215910),
    ("Varun Sandesh",          "Telugu",    824710),
    # Tamil
    ("Vikram",                 "Tamil",     93191),
    ("R. Madhavan",            "Tamil",     85519),
    ("Siddharth",              "Tamil",     108216),
    ("Arvind Swamy",           "Tamil",     560056),
    ("Santhanam",              "Tamil",     141076),
    ("Vimal",                  "Tamil",     1020064),
    # Malayalam (Biju Menon was misclassified as Tamil)
    ("Dileep",                 "Malayalam", 930728),
    ("Jayaram",                "Malayalam", 141704),
    ("Biju Menon",             "Malayalam", 584764),   # was: industry=Tamil
    # Kannada
    ("Vishnuvardhan",          "Kannada",   1179741),

    # ── Group C: Already primary — re-fetch filmography for missing links ──
    ("Rajinikanth",            "Tamil",     91555),    # missing: Kaala
    ("Sivakarthikeyan",        "Tamil",     587982),   # missing: Vanakkam Chennai
]

# Maps TMDB original_language ISO code → movies.industry label
_LANG_TO_INDUSTRY: dict[str, str] = {
    "ml": "Malayalam",
    "ta": "Tamil",
    "te": "Telugu",
    "kn": "Kannada",
    "hi": "Hindi",
    "en": "English",
}

_SEP_BOLD = "=" * 64
_SEP_THIN = "-" * 64


# ---------------------------------------------------------------------------
# DB helpers (mirrors ingest_primary_actors.py)
# ---------------------------------------------------------------------------

def _upsert_primary_actor(
    db: Session,
    name: str,
    tmdb_person_id: int,
    industry: str,
    dry_run: bool,
) -> Optional[int]:
    """
    Ensure the actor exists with is_primary_actor=TRUE and correct industry.

    Resolution order:
      1. Find by tmdb_person_id  → promote + fix industry.
      2. Find by name (case-insensitive) → promote + backfill tmdb_person_id.
      3. Insert new row.

    Returns actors.id, or None in dry-run for brand-new actors.
    """
    # 1. Lookup by TMDB person ID
    row = db.execute(
        text("SELECT id, industry, is_primary_actor FROM actors WHERE tmdb_person_id = :pid"),
        {"pid": tmdb_person_id},
    ).fetchone()
    if row:
        actor_id, current_ind, is_primary = row[0], row[1], row[2]
        ind_changed = current_ind != industry
        if not dry_run:
            db.execute(
                text("""
                    UPDATE actors
                    SET    is_primary_actor = TRUE,
                           industry         = :ind
                    WHERE  id = :id
                """),
                {"ind": industry, "id": actor_id},
            )
        action = "~" if is_primary and not ind_changed else "↑ Promoted"
        ind_note = f", industry: {current_ind!r} → {industry!r}" if ind_changed else ""
        tag = " [DRY RUN]" if dry_run else ""
        print(f"  {action}: {name!r} (id={actor_id}){ind_note}{tag}")
        return actor_id

    # 2. Lookup by name
    row = db.execute(
        text("SELECT id, tmdb_person_id, industry, is_primary_actor FROM actors WHERE lower(name) = lower(:n)"),
        {"n": name},
    ).fetchone()
    if row:
        actor_id, existing_pid, current_ind, is_primary = row
        if not dry_run:
            db.execute(
                text("""
                    UPDATE actors
                    SET    is_primary_actor = TRUE,
                           industry         = :ind,
                           tmdb_person_id   = COALESCE(tmdb_person_id, :pid)
                    WHERE  id = :id
                """),
                {"ind": industry, "pid": tmdb_person_id, "id": actor_id},
            )
        backfill = " + backfilled tmdb_person_id" if existing_pid is None else ""
        ind_note = f", industry: {current_ind!r} → {industry!r}" if current_ind != industry else ""
        tag = " [DRY RUN]" if dry_run else ""
        print(f"  ↑ Promoted (name match): {name!r} (id={actor_id}){backfill}{ind_note}{tag}")
        return actor_id

    # 3. Insert new
    if dry_run:
        print(f"  + [DRY RUN] Would insert: {name!r} ({industry}, tmdb_person_id={tmdb_person_id})")
        return None

    result = db.execute(
        text("""
            INSERT INTO actors (name, industry, is_primary_actor, tmdb_person_id, created_at)
            VALUES (:name, :industry, TRUE, :pid, NOW())
            ON CONFLICT (name) DO NOTHING
            RETURNING id
        """),
        {"name": name, "industry": industry, "pid": tmdb_person_id},
    ).fetchone()

    if result:
        print(f"  + Inserted: {name!r} ({industry}) → id={result[0]}")
        return result[0]

    # Conflict — re-query
    row = db.execute(
        text("SELECT id FROM actors WHERE lower(name) = lower(:n)"),
        {"n": name},
    ).fetchone()
    if row:
        print(f"  ↑ Post-conflict find: {name!r} → id={row[0]}")
        return row[0]

    return None


def _get_or_insert_movie(
    db: Session,
    tmdb_id: int,
    title: str,
    release_year: Optional[int],
    original_language: Optional[str],
    vote_average: Optional[float],
    popularity: Optional[float],
    poster_url: Optional[str],
    backdrop_url: Optional[str],
    actor_industry: str,
    dry_run: bool,
) -> tuple[Optional[int], bool]:
    """Return (movie_id, is_new). Inserts only when the movie is absent."""
    row = db.execute(
        text("SELECT id FROM movies WHERE tmdb_id = :tid"),
        {"tid": tmdb_id},
    ).fetchone()
    if row:
        return row[0], False

    industry = _LANG_TO_INDUSTRY.get(original_language or "", actor_industry)
    year = release_year or 0

    if dry_run:
        return None, True

    result = db.execute(
        text("""
            INSERT INTO movies
                (title, release_year, industry, language,
                 tmdb_id, vote_average, popularity, poster_url, backdrop_url)
            VALUES
                (:title, :year, :industry, :language,
                 :tmdb_id, :vote_average, :popularity, :poster_url, :backdrop_url)
            ON CONFLICT DO NOTHING
            RETURNING id
        """),
        {
            "title":        title,
            "year":         year,
            "industry":     industry,
            "language":     industry,
            "tmdb_id":      tmdb_id,
            "vote_average": vote_average,
            "popularity":   popularity,
            "poster_url":   poster_url,
            "backdrop_url": backdrop_url,
        },
    ).fetchone()

    if result:
        return result[0], True

    # ON CONFLICT — re-query
    row = db.execute(
        text("SELECT id FROM movies WHERE tmdb_id = :tid"),
        {"tid": tmdb_id},
    ).fetchone()
    return (row[0], False) if row else (None, False)


def _upsert_actor_movie(
    db: Session,
    actor_id: int,
    movie_id: int,
    character_name: Optional[str],
    billing_order: int,
    dry_run: bool,
) -> bool:
    """
    Insert actor_movies row.  billing_order 0-2 → role_type='primary',
    3+ → 'supporting'.  Returns True if a new row was created.
    """
    if dry_run:
        return True

    role_type = "primary" if billing_order <= 2 else "supporting"

    result = db.execute(
        text("""
            INSERT INTO actor_movies
                (actor_id, movie_id, character_name, billing_order, role_type)
            VALUES
                (:actor_id, :movie_id, :character_name, :billing_order, :role_type)
            ON CONFLICT (actor_id, movie_id) DO NOTHING
            RETURNING actor_id
        """),
        {
            "actor_id":       actor_id,
            "movie_id":       movie_id,
            "character_name": character_name,
            "billing_order":  billing_order,
            "role_type":      role_type,
        },
    ).fetchone()

    return result is not None


# ---------------------------------------------------------------------------
# Per-actor pipeline
# ---------------------------------------------------------------------------

def _process_actor(
    name: str,
    industry: str,
    tmdb_person_id: int,
    index: int,
    total: int,
    dry_run: bool,
) -> dict:
    """
    End-to-end for one actor:
      1. Upsert actor (promote to primary + fix industry).
      2. Fetch TMDB filmography using the hardcoded tmdb_person_id.
      3. Upsert movies + actor_movies rows.
      4. Commit per actor.
    """
    print(f"\n{_SEP_THIN}")
    print(f"[{index}/{total}] {name!r}  ({industry})  tmdb_person_id={tmdb_person_id}")

    # Fetch filmography (no TMDB search — we have the person ID already)
    films = fetch_person_movie_credits(tmdb_person_id)
    print(f"  TMDB filmography: {len(films)} film(s)")

    if not films:
        print(f"  ✗ Empty filmography — skipped.")
        return _error_result(name, industry, tmdb_person_id, "Empty filmography")

    movies_inserted = 0
    movies_skipped  = 0
    rels_inserted   = 0
    rels_skipped    = 0
    error: Optional[str] = None

    db: Session = SessionLocal()
    try:
        actor_id = _upsert_primary_actor(db, name, tmdb_person_id, industry, dry_run)

        for film in films:
            try:
                movie_id, is_new = _get_or_insert_movie(
                    db=db,
                    tmdb_id=film["tmdb_id"],
                    title=film["title"],
                    release_year=film["release_year"],
                    original_language=film["original_language"],
                    vote_average=film["vote_average"],
                    popularity=film["popularity"],
                    poster_url=film["poster_url"],
                    backdrop_url=film["backdrop_url"],
                    actor_industry=industry,
                    dry_run=dry_run,
                )
                if is_new:
                    movies_inserted += 1
                else:
                    movies_skipped += 1

                if actor_id is None or movie_id is None:
                    if not dry_run:
                        error = "actor_id or movie_id is None"
                        break
                    rels_inserted += 1
                    continue

                inserted = _upsert_actor_movie(
                    db=db,
                    actor_id=actor_id,
                    movie_id=movie_id,
                    character_name=film["character"],
                    billing_order=film["cast_order"],
                    dry_run=dry_run,
                )
                if inserted:
                    rels_inserted += 1
                else:
                    rels_skipped += 1

            except Exception as exc:
                print(f"  ✗ Error on {film.get('title', '?')!r}: {exc}")
                error = str(exc)

        if not dry_run:
            db.commit()

    except Exception as exc:
        if not dry_run:
            db.rollback()
        error = str(exc)
        print(f"  ✗ DB error for {name!r}: {exc}")
    finally:
        db.close()

    print(f"  ✓ movies_inserted={movies_inserted}  movies_skipped={movies_skipped}  "
          f"rels_inserted={rels_inserted}  rels_skipped={rels_skipped}")
    if error:
        print(f"  ✗ {error}")

    return {
        "name":              name,
        "industry":          industry,
        "tmdb_person_id":    tmdb_person_id,
        "movies_discovered": len(films),
        "movies_inserted":   movies_inserted,
        "movies_skipped":    movies_skipped,
        "rels_inserted":     rels_inserted,
        "rels_skipped":      rels_skipped,
        "error":             error,
    }


def _error_result(name: str, industry: str, tmdb_person_id: int, msg: str) -> dict:
    return {
        "name": name, "industry": industry, "tmdb_person_id": tmdb_person_id,
        "movies_discovered": 0, "movies_inserted": 0, "movies_skipped": 0,
        "rels_inserted": 0, "rels_skipped": 0, "error": msg,
    }


# ---------------------------------------------------------------------------
# Analytics rebuild
# ---------------------------------------------------------------------------

def _rebuild_analytics(dry_run: bool) -> None:
    """Rebuild actor_stats and actor_collaborations via build_analytics_tables.py."""
    if dry_run:
        print("\n  [DRY RUN] Would rebuild analytics tables.")
        return

    print(f"\n{_SEP_BOLD}")
    print("  Rebuilding analytics tables...")
    print(_SEP_BOLD)

    try:
        from data_pipeline.build_analytics_tables import build_analytics_tables
        build_analytics_tables()
        print("  ✓ Analytics tables rebuilt.")
    except Exception as exc:
        print(f"  ✗ Analytics rebuild failed: {exc}")
        print("    Run manually: python -m data_pipeline.build_analytics_tables")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def ingest_bo_leads(
    dry_run:         bool = False,
    actor:           str  = "",
    skip_analytics:  bool = False,
) -> int:
    t_start = time.monotonic()

    if not os.getenv("TMDB_API_KEY", "").strip():
        print(
            "\n✗ TMDB_API_KEY is not set.\n"
            "  Get a free key at https://www.themoviedb.org/settings/api\n"
            "  Then run:  export TMDB_API_KEY=your_key_here\n"
        )
        return 1

    # Build actor list for this run
    actors = list(ACTORS_TO_PROCESS)
    if actor:
        actors = [(n, ind, pid) for n, ind, pid in actors if n.lower() == actor.strip().lower()]
        if not actors:
            valid = ", ".join(n for n, _, _ in ACTORS_TO_PROCESS)
            print(f"\n✗ {actor!r} not found in actor list.\n  Valid names:\n  {valid}\n")
            return 1

    mode = "  [DRY RUN — no DB writes]" if dry_run else ""
    print(f"\n{_SEP_BOLD}")
    print(f"  Box-Office Lead Actor Ingestion{mode}")
    print(f"  Actors to process: {len(actors)}")
    for n, ind, pid in actors:
        print(f"    • {n:<28} ({ind:<10}) tmdb={pid}")
    print(f"{_SEP_BOLD}\n")

    results: list[dict] = []
    for idx, (name, industry, tmdb_person_id) in enumerate(actors, start=1):
        result = _process_actor(name, industry, tmdb_person_id, idx, len(actors), dry_run)
        results.append(result)

    elapsed = time.monotonic() - t_start
    errors  = [r for r in results if r["error"]]

    # Summary
    print(f"\n{_SEP_BOLD}")
    print(f"  Summary{mode}")
    print(_SEP_THIN)
    print(f"  actors_processed    : {len(results)}")
    print(f"  actors_failed       : {len(errors)}")
    print(f"  new_movies_added    : {sum(r['movies_inserted'] for r in results)}")
    print(f"  new_cast_rows_added : {sum(r['rels_inserted']   for r in results)}")
    print(f"  elapsed             : {elapsed:.1f} s")
    if errors:
        print(_SEP_THIN)
        print("  Failed actors:")
        for r in errors:
            print(f"    ✗ {r['name']}: {r['error']}")
    print(_SEP_BOLD)

    # Rebuild analytics
    if not skip_analytics:
        _rebuild_analytics(dry_run)

    return 1 if errors else 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest box-office lead actors (Group A/B/C from Sprint 24 audit).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python -m data_pipeline.ingest_bo_leads\n"
            "  python -m data_pipeline.ingest_bo_leads --dry-run\n"
            '  python -m data_pipeline.ingest_bo_leads --actor "Ravi Teja"\n'
            "  python -m data_pipeline.ingest_bo_leads --skip-analytics\n"
        ),
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Print planned actions without writing to the DB.")
    p.add_argument("--actor", type=str, default="",
                   help="Process only this actor (case-insensitive).")
    p.add_argument("--skip-analytics", action="store_true",
                   help="Skip the final analytics-table rebuild.")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(ingest_bo_leads(
        dry_run=args.dry_run,
        actor=args.actor,
        skip_analytics=args.skip_analytics,
    ))
