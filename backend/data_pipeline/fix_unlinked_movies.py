"""
fix_unlinked_movies.py
----------------------
For movies with no actor_movies associations:

Phase A — movies that HAVE a tmdb_id:
  Fetch TMDB credits, find cast members whose tmdb_person_id exists in our actors table,
  create actor_movies rows.

Phase B — movies that have NO tmdb_id:
  Search TMDB by title+year+industry, pick the best language-matching result,
  update movies.tmdb_id/poster_url/release_year, then link actors from Phase A logic.

Phase C — re-link 4 orphaned supporting actors (Bellamkonda Srinivas et al.)
  via their TMDB person credits → match to existing movies in DB.
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("DATABASE_URL", "postgresql://sca:sca@localhost:5432/sca")

from sqlalchemy import text
from app.database import SessionLocal
from data_pipeline.tmdb_client import (
    _api_get, _get_api_key, _build_image_url,
)

_DETAIL_URL  = "https://api.themoviedb.org/3/movie/{tmdb_id}"
_CREDITS_URL = "https://api.themoviedb.org/3/movie/{tmdb_id}/credits"
_SEARCH_URL  = "https://api.themoviedb.org/3/search/movie"
_PERSON_URL  = "https://api.themoviedb.org/3/person/{person_id}/movie_credits"

INDUSTRY_LANG = {
    "Tamil": "ta", "Telugu": "te", "Malayalam": "ml",
    "Kannada": "kn", "Hindi": "hi", "English": "en",
}

# ── helpers ──────────────────────────────────────────────────────────────────

def link_tmdb_cast(db, movie_db_id, tmdb_id, api_key):
    """Fetch TMDB credits and create actor_movies rows for known actors."""
    try:
        data = _api_get(_CREDITS_URL.format(tmdb_id=tmdb_id),
                        {"api_key": api_key})
        cast = data.get("cast", [])
    except Exception as e:
        print(f"    credits error: {e}")
        return 0

    linked = 0
    for order, member in enumerate(cast[:20]):  # top-20 billed
        person_id = member.get("id")
        if not person_id:
            continue
        actor = db.execute(text(
            "SELECT id FROM actors WHERE tmdb_person_id = :pid"
        ), {"pid": person_id}).fetchone()
        if not actor:
            continue
        exists = db.execute(text(
            "SELECT 1 FROM actor_movies WHERE actor_id=:aid AND movie_id=:mid"
        ), {"aid": actor[0], "mid": movie_db_id}).fetchone()
        if not exists:
            role = "primary" if order == 0 else "supporting"
            db.execute(text("""
                INSERT INTO actor_movies (actor_id, movie_id, character_name, billing_order, role_type)
                VALUES (:aid, :mid, :char, :bill, :role)
            """), {"aid": actor[0], "mid": movie_db_id,
                   "char": member.get("character"), "bill": order, "role": role})
            linked += 1
    db.commit()
    return linked


def search_tmdb(title, year, lang_code, api_key):
    """Search TMDB, return best match whose original_language == lang_code."""
    try:
        data = _api_get(_SEARCH_URL, {
            "api_key": api_key, "query": title,
            "year": year if year > 0 else None,
            "language": "en-US",
        })
    except Exception:
        return None
    results = data.get("results", [])
    # prefer exact language match, then any match
    matches = [r for r in results if r.get("original_language") == lang_code]
    if not matches:
        # broaden: any language result whose title is close
        matches = results
    if not matches:
        return None
    return matches[0]


# ── Phase A: link movies that already have tmdb_id ───────────────────────────

def phase_a(db, api_key):
    print("\n── Phase A: link movies WITH tmdb_id ──")
    rows = db.execute(text("""
        SELECT m.id, m.title, m.release_year, m.industry, m.tmdb_id
        FROM movies m
        WHERE m.tmdb_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM actor_movies WHERE movie_id = m.id)
        ORDER BY m.id
    """)).fetchall()
    print(f"  Movies: {len(rows)}")

    for db_id, title, year, industry, tmdb_id in rows:
        linked = link_tmdb_cast(db, db_id, tmdb_id, api_key)
        print(f"  [{db_id}] {title} ({year}) — linked {linked} actors")
        time.sleep(0.1)


# ── Phase B: assign tmdb_id to movies with none, then link ──────────────────

def phase_b(db, api_key):
    print("\n── Phase B: assign tmdb_id + link actors for movies WITHOUT tmdb_id ──")
    rows = db.execute(text("""
        SELECT m.id, m.title, m.release_year, m.industry
        FROM movies m
        WHERE m.tmdb_id IS NULL
          AND NOT EXISTS (SELECT 1 FROM actor_movies WHERE movie_id = m.id)
        ORDER BY m.id
    """)).fetchall()
    print(f"  Movies: {len(rows)}")

    assigned = linked_total = 0
    for db_id, title, year, industry in rows:
        lang_code = INDUSTRY_LANG.get(industry, "ta")

        # clean title: strip "(YYYY film)" suffix if present
        clean_title = title.split("(")[0].strip()

        match = search_tmdb(clean_title, year, lang_code, api_key)
        if not match:
            print(f"  [{db_id}] {title} — not found on TMDB")
            time.sleep(0.1)
            continue

        matched_lang = match.get("original_language")
        release_date = match.get("release_date", "")
        matched_year = int(release_date[:4]) if release_date and len(release_date) >= 4 else 0

        # Reject if year mismatch > 2 AND the movie has a year
        if year > 0 and matched_year > 0 and abs(matched_year - year) > 2:
            print(f"  [{db_id}] {title} — year mismatch ({year} vs {matched_year}), skip")
            time.sleep(0.1)
            continue

        tmdb_id    = match.get("id")
        poster_url = _build_image_url(match.get("poster_path"), "w500") if match.get("poster_path") else None
        backdrop_url = _build_image_url(match.get("backdrop_path"), "w780") if match.get("backdrop_path") else None

        # Check unique constraint
        conflict = db.execute(text(
            "SELECT id, title FROM movies WHERE tmdb_id = :tid AND id != :mid"
        ), {"tid": tmdb_id, "mid": db_id}).fetchone()
        if conflict:
            print(f"  [{db_id}] {title} — tmdb_id {tmdb_id} already used by '{conflict[1]}'")
            time.sleep(0.1)
            continue

        # Update the movie row
        update_year = matched_year if matched_year > 0 else year
        db.execute(text("""
            UPDATE movies
            SET tmdb_id=:tid, poster_url=:poster, backdrop_url=:backdrop,
                release_year=:yr
            WHERE id=:id
        """), {"tid": tmdb_id, "poster": poster_url, "backdrop": backdrop_url,
               "yr": update_year, "id": db_id})
        db.commit()
        assigned += 1

        # Now link actors
        linked = link_tmdb_cast(db, db_id, tmdb_id, api_key)
        print(f"  [{db_id}] {title} → tmdb {tmdb_id} ({matched_lang},{matched_year}) | linked {linked} actors")
        linked_total += linked
        time.sleep(0.15)

    print(f"  Assigned {assigned} tmdb_ids, linked {linked_total} actors total")


# ── Phase C: re-link 4 orphaned supporting actors ───────────────────────────

def phase_c(db, api_key):
    print("\n── Phase C: re-link orphaned supporting actors via TMDB person credits ──")
    orphans = db.execute(text("""
        SELECT id, name, tmdb_person_id FROM actors
        WHERE NOT EXISTS (SELECT 1 FROM actor_movies WHERE actor_id=actors.id)
          AND tmdb_person_id IS NOT NULL
    """)).fetchall()
    print(f"  Orphaned actors: {len(orphans)}")

    for actor_id, name, person_id in orphans:
        try:
            data = _api_get(_PERSON_URL.format(person_id=person_id),
                            {"api_key": api_key, "language": "en-US"})
        except Exception as e:
            print(f"  {name}: person credits error: {e}")
            continue

        credits = data.get("cast", [])
        linked = 0
        for credit in credits:
            film_tmdb_id = credit.get("id")
            if not film_tmdb_id:
                continue
            movie = db.execute(text(
                "SELECT id FROM movies WHERE tmdb_id = :tid"
            ), {"tid": film_tmdb_id}).fetchone()
            if not movie:
                continue
            exists = db.execute(text(
                "SELECT 1 FROM actor_movies WHERE actor_id=:aid AND movie_id=:mid"
            ), {"aid": actor_id, "mid": movie[0]}).fetchone()
            if not exists:
                db.execute(text("""
                    INSERT INTO actor_movies (actor_id, movie_id, character_name, billing_order, role_type)
                    VALUES (:aid, :mid, :char, :bill, :role)
                """), {"aid": actor_id, "mid": movie[0],
                       "char": credit.get("character"), "bill": credit.get("order", 99),
                       "role": "supporting"})
                linked += 1

        if linked > 0:
            db.commit()
            # Update industry to Telugu since these are Telugu supporting actors
            db.execute(text("UPDATE actors SET industry='Telugu' WHERE id=:id"), {"id": actor_id})
            db.commit()

        print(f"  {name} (person={person_id}): linked {linked} films")
        time.sleep(0.1)


# ── main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    api_key = _get_api_key()
    db = SessionLocal()

    phase_a(db, api_key)
    phase_b(db, api_key)
    phase_c(db, api_key)

    # Final tally
    print("\n── Final counts ──")
    total = db.execute(text("SELECT COUNT(*) FROM movies")).scalar()
    unlinked = db.execute(text("""
        SELECT COUNT(*) FROM movies m
        WHERE NOT EXISTS (SELECT 1 FROM actor_movies WHERE movie_id = m.id)
    """)).scalar()
    orphan_actors = db.execute(text("""
        SELECT COUNT(*) FROM actors
        WHERE NOT EXISTS (SELECT 1 FROM actor_movies WHERE actor_id = actors.id)
    """)).scalar()
    print(f"  Movies total: {total} | Unlinked: {unlinked}")
    print(f"  Actors with no films: {orphan_actors}")

    db.close()
