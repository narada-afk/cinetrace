"""
ingest_lead_actresses_v2.py
===========================
Sprint 24b — Expand lead actress coverage with 20 additional actresses.

Adds gender='F' + is_primary_actor=TRUE for actresses missing from the
initial Sprint 24 run, completing broader representation across all four
South Indian industries.

Three sub-groups are handled:

  Group A — In DB as supporting, with correct industry:
             Promoted to is_primary_actor=TRUE, gender set to 'F'.

  Group B — In DB as supporting, but wrong industry (Tamil misclassification):
             Promoted + industry corrected to Telugu/Kannada as appropriate.

  Group C — Not yet in DB at all:
             Inserted fresh with is_primary_actor=TRUE and gender='F'.

For every actress the script fetches her complete TMDB filmography and
upserts actor_movies rows for all films that already exist in the movies
table.

After all actresses are processed the analytics tables are rebuilt.

Idempotency
-----------
All DB writes use ON CONFLICT DO NOTHING or UPDATE-by-id so the script is
safe to re-run.

Usage
-----
  # From the backend/ directory:
  export TMDB_API_KEY=your_key_here
  python -m data_pipeline.ingest_lead_actresses_v2
  python -m data_pipeline.ingest_lead_actresses_v2 --dry-run
  python -m data_pipeline.ingest_lead_actresses_v2 --actor "Kajal Aggarwal"
  python -m data_pipeline.ingest_lead_actresses_v2 --skip-analytics

Flags
-----
  --dry-run          Print planned actions without writing to the DB.
  --actor NAME       Process only this actress (case-insensitive).
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

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from data_pipeline.tmdb_client import fetch_person_movie_credits


# ---------------------------------------------------------------------------
# Actress registry
# ---------------------------------------------------------------------------
# Each tuple: (display_name, industry, tmdb_person_id)
# All TMDB person IDs verified against live TMDB search.
# Industries follow the movies.industry convention: Telugu/Tamil/Malayalam/Kannada.
#
# Notes on industry corrections (actresses misclassified as Tamil in DB):
#   Ritu Varma       → Telugu  (primarily Telugu industry)
#   Rachita Ram      → Kannada (Sandalwood's leading actress)
#   Amala Akkineni   → Telugu  (Tollywood veteran)
#   Soundarya        → Telugu  (Tollywood legend, 1972–2004)

ACTRESSES_TO_PROCESS: list[tuple[str, str, int]] = [
    # ── Group A: In DB as supporting, industry correct ────────────────────
    # Tamil
    ("Kajal Aggarwal",         "Telugu",    113809),   # primarily Telugu + Tamil
    ("Simran",                 "Tamil",     141705),   # 98 Tamil films
    ("Sneha",                  "Tamil",     141083),
    ("Nithya Menen",           "Tamil",     225388),
    ("Asin",                   "Tamil",     81092),
    ("Amala Paul",             "Tamil",     223164),
    ("Devayani",               "Tamil",     286670),
    ("Meera Jasmine",          "Tamil",     237624),
    ("Bhanupriya",             "Tamil",     540530),
    ("Suhasini Maniratnam",    "Tamil",     584464),
    # Telugu
    ("Ramya Krishnan",         "Telugu",    141701),   # 179 films, Baahubali queen
    ("Sreeleela",              "Telugu",    2476557),  # current breakout star

    # ── Group B: In DB as supporting, industry wrong ──────────────────────
    # (was: Tamil in DB — corrected below)
    ("Ritu Varma",             "Telugu",    1608576),  # was: industry=Tamil
    ("Rachita Ram",            "Kannada",   1840670),  # was: industry=Tamil
    ("Amala Akkineni",         "Telugu",    150369),   # was: industry=Tamil
    ("Soundarya",              "Telugu",    288587),   # was: industry=Tamil (Telugu legend)

    # ── Group C: Not in DB at all ─────────────────────────────────────────
    ("Khushbu",                "Tamil",     82078),
    ("Kavya Madhavan",         "Malayalam", 584761),
    ("Bhavana",                "Malayalam", 238006),
    ("Priyamani",              "Tamil",     1107197),
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
# DB helpers
# ---------------------------------------------------------------------------

def _upsert_primary_actress(
    db: Session,
    name: str,
    tmdb_person_id: int,
    industry: str,
    dry_run: bool,
) -> Optional[int]:
    """
    Ensure the actress exists with is_primary_actor=TRUE, gender='F', and the
    correct industry.

    Resolution order:
      1. Find by tmdb_person_id  → promote + set gender + fix industry.
      2. Find by name (case-insensitive) → same + backfill tmdb_person_id.
      3. Insert new row.

    Returns actors.id, or None in dry-run for brand-new actresses.
    """
    # 1. Lookup by TMDB person ID
    row = db.execute(
        text("""
            SELECT id, industry, is_primary_actor, gender
            FROM   actors
            WHERE  tmdb_person_id = :pid
        """),
        {"pid": tmdb_person_id},
    ).fetchone()

    if row:
        actor_id, cur_ind, is_primary, cur_gender = row
        ind_changed    = cur_ind    != industry
        gender_changed = cur_gender != 'F'
        if not dry_run:
            db.execute(
                text("""
                    UPDATE actors
                    SET    is_primary_actor = TRUE,
                           gender           = 'F',
                           industry         = :ind
                    WHERE  id = :id
                """),
                {"ind": industry, "id": actor_id},
            )
        changes = []
        if not is_primary:    changes.append("promoted to primary")
        if gender_changed:    changes.append("gender→F")
        if ind_changed:       changes.append(f"industry: {cur_ind!r}→{industry!r}")
        tag  = " [DRY RUN]" if dry_run else ""
        verb = "~" if not changes else "↑"
        note = f" ({', '.join(changes)})" if changes else ""
        print(f"  {verb} {name!r} id={actor_id}{note}{tag}")
        return actor_id

    # 2. Lookup by name
    row = db.execute(
        text("""
            SELECT id, tmdb_person_id, industry, is_primary_actor, gender
            FROM   actors
            WHERE  lower(name) = lower(:n)
        """),
        {"n": name},
    ).fetchone()

    if row:
        actor_id, existing_pid, cur_ind, is_primary, cur_gender = row
        if not dry_run:
            db.execute(
                text("""
                    UPDATE actors
                    SET    is_primary_actor = TRUE,
                           gender           = 'F',
                           industry         = :ind,
                           tmdb_person_id   = COALESCE(tmdb_person_id, :pid)
                    WHERE  id = :id
                """),
                {"ind": industry, "pid": tmdb_person_id, "id": actor_id},
            )
        changes = []
        if not is_primary:          changes.append("promoted to primary")
        if cur_gender != 'F':       changes.append("gender→F")
        if cur_ind != industry:     changes.append(f"industry: {cur_ind!r}→{industry!r}")
        if existing_pid is None:    changes.append("backfilled tmdb_person_id")
        tag  = " [DRY RUN]" if dry_run else ""
        note = f" ({', '.join(changes)})" if changes else ""
        print(f"  ↑ {name!r} id={actor_id}{note}{tag}")
        return actor_id

    # 3. Insert new actress
    if dry_run:
        print(f"  + [DRY RUN] Would insert: {name!r} ({industry}, gender=F, tmdb={tmdb_person_id})")
        return None

    result = db.execute(
        text("""
            INSERT INTO actors
                (name, industry, is_primary_actor, gender, tmdb_person_id, created_at)
            VALUES
                (:name, :industry, TRUE, 'F', :pid, NOW())
            ON CONFLICT (name) DO NOTHING
            RETURNING id
        """),
        {"name": name, "industry": industry, "pid": tmdb_person_id},
    ).fetchone()

    if result:
        print(f"  + Inserted: {name!r} ({industry}, gender=F) → id={result[0]}")
        return result[0]

    # Name conflict — re-query
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
    """Return (movie_id, is_new). Insert only when the movie is absent."""
    row = db.execute(
        text("SELECT id FROM movies WHERE tmdb_id = :tid"),
        {"tid": tmdb_id},
    ).fetchone()
    if row:
        return row[0], False

    industry = _LANG_TO_INDUSTRY.get(original_language or "", actor_industry)
    year     = release_year or 0

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
    """Insert actor_movies row; role_type derived from billing_order (0-2 → primary)."""
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
# Per-actress pipeline
# ---------------------------------------------------------------------------

def _process_actress(
    name: str,
    industry: str,
    tmdb_person_id: int,
    index: int,
    total: int,
    dry_run: bool,
) -> dict:
    """End-to-end ingestion for one actress."""
    print(f"\n{_SEP_THIN}")
    print(f"[{index}/{total}] {name!r}  ({industry})  tmdb_person_id={tmdb_person_id}")

    films = fetch_person_movie_credits(tmdb_person_id)
    print(f"  TMDB filmography: {len(films)} film(s)")

    if not films:
        print(f"  ✗ Empty filmography — skipped.")
        return {
            "name": name, "industry": industry, "tmdb_person_id": tmdb_person_id,
            "movies_discovered": 0, "movies_inserted": 0, "movies_skipped": 0,
            "rels_inserted": 0, "rels_skipped": 0, "error": "Empty filmography",
        }

    movies_inserted = movies_skipped = rels_inserted = rels_skipped = 0
    error: Optional[str] = None

    db: Session = SessionLocal()
    try:
        actor_id = _upsert_primary_actress(db, name, tmdb_person_id, industry, dry_run)

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
        print(f"  ✗ DB error: {exc}")
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


# ---------------------------------------------------------------------------
# Analytics rebuild
# ---------------------------------------------------------------------------

def _rebuild_analytics(dry_run: bool) -> None:
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

def ingest_lead_actresses_v2(
    dry_run:        bool = False,
    actor:          str  = "",
    skip_analytics: bool = False,
) -> int:
    t_start = time.monotonic()

    if not os.getenv("TMDB_API_KEY", "").strip():
        print(
            "\n✗ TMDB_API_KEY is not set.\n"
            "  Get a free key at https://www.themoviedb.org/settings/api\n"
            "  Then run:  export TMDB_API_KEY=your_key_here\n"
        )
        return 1

    actresses = list(ACTRESSES_TO_PROCESS)
    if actor:
        actresses = [(n, ind, pid) for n, ind, pid in actresses
                     if n.lower() == actor.strip().lower()]
        if not actresses:
            valid = ", ".join(n for n, _, _ in ACTRESSES_TO_PROCESS)
            print(f"\n✗ {actor!r} not found.\n  Valid names:\n  {valid}\n")
            return 1

    mode = "  [DRY RUN — no DB writes]" if dry_run else ""
    print(f"\n{_SEP_BOLD}")
    print(f"  Sprint 24b — Lead Actress Expansion{mode}")
    print(f"  Actresses to process: {len(actresses)}")
    for n, ind, pid in actresses:
        print(f"    • {n:<28} ({ind:<10}) tmdb={pid}")
    print(_SEP_BOLD)

    results: list[dict] = []
    for idx, (name, industry, tmdb_person_id) in enumerate(actresses, start=1):
        result = _process_actress(name, industry, tmdb_person_id, idx, len(actresses), dry_run)
        results.append(result)

    elapsed = time.monotonic() - t_start
    errors  = [r for r in results if r["error"]]

    print(f"\n{_SEP_BOLD}")
    print(f"  Summary{mode}")
    print(_SEP_THIN)
    print(f"  actresses_processed : {len(results)}")
    print(f"  actresses_failed    : {len(errors)}")
    print(f"  new_movies_added    : {sum(r['movies_inserted'] for r in results)}")
    print(f"  new_cast_rows_added : {sum(r['rels_inserted']   for r in results)}")
    print(f"  elapsed             : {elapsed:.1f} s")
    if errors:
        print(_SEP_THIN)
        for r in errors:
            print(f"    ✗ {r['name']}: {r['error']}")
    print(_SEP_BOLD)

    if not skip_analytics:
        _rebuild_analytics(dry_run)

    return 1 if errors else 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Expand South Indian lead actress coverage (Sprint 24b).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python -m data_pipeline.ingest_lead_actresses_v2\n"
            "  python -m data_pipeline.ingest_lead_actresses_v2 --dry-run\n"
            '  python -m data_pipeline.ingest_lead_actresses_v2 --actor "Kajal Aggarwal"\n'
        ),
    )
    p.add_argument("--dry-run",        action="store_true")
    p.add_argument("--actor",          type=str, default="")
    p.add_argument("--skip-analytics", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(ingest_lead_actresses_v2(
        dry_run=args.dry_run,
        actor=args.actor,
        skip_analytics=args.skip_analytics,
    ))
