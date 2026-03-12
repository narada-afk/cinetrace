"""
fix_missing_posters.py
----------------------
Fetch poster_url / backdrop_url for movies that have a tmdb_id but no poster.
Runs a targeted GET /movie/{id} per row and updates only the image fields.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("DATABASE_URL", "postgresql://sca:sca@localhost:5432/sca")

from sqlalchemy import text
from app.database import SessionLocal
from data_pipeline.tmdb_client import _api_get, _get_api_key, _build_image_url

_DETAIL_URL = "https://api.themoviedb.org/3/movie/{tmdb_id}"
_POSTER_SIZE = "w500"
_BACKDROP_SIZE = "w780"


def run():
    api_key = _get_api_key()
    db = SessionLocal()

    rows = db.execute(text("""
        SELECT id, title, tmdb_id
        FROM movies
        WHERE tmdb_id IS NOT NULL AND poster_url IS NULL
        ORDER BY id
    """)).fetchall()

    total = len(rows)
    print(f"Movies to fix: {total}")

    fixed = 0
    no_poster = 0
    errors = 0

    for i, row in enumerate(rows, 1):
        db_id, title, tmdb_id = row

        try:
            data = _api_get(
                _DETAIL_URL.format(tmdb_id=tmdb_id),
                {"api_key": api_key, "language": "en-US"},
            )

            poster_path   = data.get("poster_path")
            backdrop_path = data.get("backdrop_path")

            if not poster_path:
                print(f"  [{i}/{total}] {title} (tmdb={tmdb_id}) — no poster on TMDB")
                no_poster += 1
                continue

            poster_url   = _build_image_url(poster_path,   _POSTER_SIZE)
            backdrop_url = _build_image_url(backdrop_path, _BACKDROP_SIZE) if backdrop_path else None

            db.execute(text("""
                UPDATE movies
                SET poster_url = :poster_url,
                    backdrop_url = :backdrop_url
                WHERE id = :id
            """), {"poster_url": poster_url, "backdrop_url": backdrop_url, "id": db_id})
            db.commit()

            print(f"  [{i}/{total}] ✓ {title} (tmdb={tmdb_id}) → {poster_url[:60]}...")
            fixed += 1

        except Exception as e:
            print(f"  [{i}/{total}] ERROR {title} (tmdb={tmdb_id}): {e}")
            errors += 1

        # Polite rate limit
        time.sleep(0.05)

    db.close()

    print()
    print(f"=== Done ===")
    print(f"  Fixed   : {fixed}")
    print(f"  No poster on TMDB: {no_poster}")
    print(f"  Errors  : {errors}")
    print(f"  Total   : {total}")


if __name__ == "__main__":
    run()
