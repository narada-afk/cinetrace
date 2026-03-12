"""
backfill_directors.py
---------------------
Populate the `director` column for every movie that has a tmdb_id but is
missing director data.

Strategy
--------
  GET /movie/{tmdb_id}/credits
  Filter crew where job == "Director".
  Join multiple directors with ", " and write to movies.director.

Usage
-----
  python -m data_pipeline.backfill_directors

Requires TMDB_API_KEY to be set in the environment (or .env).
Rate-limit: 0.25 s between requests (~4 req/s, well within TMDB free tier).

Expected impact
---------------
  Up to 1,080 movies currently have director IS NULL or ''.
  Most blockbusters and recent releases have director data on TMDB,
  so the realistic fill rate is 70-90 %.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("DATABASE_URL", "postgresql://sca:sca@localhost:5432/sca")

from sqlalchemy import text
from app.database import SessionLocal
from data_pipeline.tmdb_client import _api_get, _get_api_key

# TMDB endpoint — defined locally, same pattern as fix_release_years.py
_CREDITS_URL = "https://api.themoviedb.org/3/movie/{tmdb_id}/credits"


def run() -> None:
    api_key = _get_api_key()
    db = SessionLocal()

    # ── 1. Identify movies that need a director ───────────────────
    rows = db.execute(text("""
        SELECT id, title, tmdb_id
        FROM   movies
        WHERE  tmdb_id IS NOT NULL
          AND  (director IS NULL OR director = '')
        ORDER  BY id
    """)).fetchall()

    total   = len(rows)
    added   = 0
    skipped = 0
    errors  = 0

    print(f"Movies missing director: {total}")
    print("─" * 56)

    for i, (db_id, title, tmdb_id) in enumerate(rows, 1):
        prefix = f"[{i:>4}/{total}]"

        try:
            # ── 2. Fetch TMDB credits ─────────────────────────────
            data = _api_get(
                _CREDITS_URL.format(tmdb_id=tmdb_id),
                {"api_key": api_key, "language": "en-US"},
            )

            # ── 3. Extract directors from crew ────────────────────
            crew = data.get("crew") or []
            directors = [
                member["name"].strip()
                for member in crew
                if member.get("job") == "Director" and member.get("name")
            ]

            if not directors:
                print(f"  {prefix} – {title}: no director on TMDB")
                skipped += 1
            else:
                director_str = ", ".join(directors)

                # ── 4. Write to database ──────────────────────────
                db.execute(
                    text("UPDATE movies SET director = :director WHERE id = :id"),
                    {"director": director_str, "id": db_id},
                )
                db.commit()

                tag = "✓" if len(directors) == 1 else f"✓ ({len(directors)} directors)"
                print(f"  {prefix} {tag} {title} → {director_str}")
                added += 1

        except Exception as exc:
            print(f"  {prefix} ERROR {title} (tmdb={tmdb_id}): {exc}")
            errors += 1

        # ── 5. Rate limiting ──────────────────────────────────────
        time.sleep(0.25)

    db.close()

    # ── 6. Final summary ──────────────────────────────────────────
    print("\n" + "─" * 56)
    print(f"Directors added:            {added:>5}")
    print(f"Movies skipped (no TMDB):   {skipped:>5}")
    print(f"Errors:                     {errors:>5}")
    print(f"Total processed:            {total:>5}")


if __name__ == "__main__":
    run()
