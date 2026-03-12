"""
fix_release_years.py
--------------------
For movies with release_year=0 that have a tmdb_id, fetch the real release date
from TMDB and update the DB.
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("DATABASE_URL", "postgresql://sca:sca@localhost:5432/sca")

from sqlalchemy import text
from app.database import SessionLocal
from data_pipeline.tmdb_client import _api_get, _get_api_key

_DETAIL_URL = "https://api.themoviedb.org/3/movie/{tmdb_id}"

def run():
    api_key = _get_api_key()
    db = SessionLocal()

    rows = db.execute(text("""
        SELECT id, title, tmdb_id FROM movies
        WHERE release_year = 0 AND tmdb_id IS NOT NULL
        ORDER BY id
    """)).fetchall()

    print(f"Movies to fix: {len(rows)}")
    fixed = updated = skipped = errors = 0

    for i, (db_id, title, tmdb_id) in enumerate(rows, 1):
        try:
            data = _api_get(_DETAIL_URL.format(tmdb_id=tmdb_id),
                            {"api_key": api_key, "language": "en-US"})
            release_date = data.get("release_date", "")
            year = int(release_date[:4]) if release_date and len(release_date) >= 4 else 0

            if year > 0:
                db.execute(text("UPDATE movies SET release_year=:y WHERE id=:id"),
                           {"y": year, "id": db_id})
                db.commit()
                print(f"  [{i}/{len(rows)}] ✓ {title} → {year}")
                fixed += 1
            else:
                print(f"  [{i}/{len(rows)}] – {title}: TMDB has no release date yet")
                skipped += 1

        except Exception as e:
            print(f"  [{i}/{len(rows)}] ERROR {title} (tmdb={tmdb_id}): {e}")
            errors += 1

        time.sleep(0.05)

    db.close()
    print(f"\nFixed: {fixed} | Still 0 (not released): {skipped} | Errors: {errors}")

if __name__ == "__main__":
    run()
