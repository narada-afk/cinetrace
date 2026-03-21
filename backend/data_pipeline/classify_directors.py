"""
classify_directors.py
=====================
Checks every supporting actor (is_primary_actor=FALSE) against the TMDB
/person/{id} endpoint and marks those whose known_for_department == "Directing"
as is_director=TRUE.

Why this matters
----------------
The supporting-actor ingestion pulls the top-10 cast members for every movie.
Some of those cast members are primarily directors who appeared in cameos
(e.g. S. S. Rajamouli, Shankar, Anurag Kashyap).  We want them classified
as directors, not as supporting actors.

The is_director column was added in Sprint 24c.

How it works
------------
For each actor with is_primary_actor=FALSE and a known tmdb_person_id:
  1. GET /person/{tmdb_person_id}
  2. If known_for_department == "Directing" → set is_director = TRUE
  3. Commit in batches of 50.

Safety
------
- Primary actors (is_primary_actor=TRUE) are never touched.
- Already-marked directors (is_director=TRUE) are skipped.
- All inserts are idempotent (UPDATE ... WHERE is_director=FALSE).

Usage
-----
    python -m data_pipeline.classify_directors
    # or inside the Docker container:
    docker compose exec backend python -m data_pipeline.classify_directors

Environment
-----------
    DATABASE_URL   PostgreSQL DSN  (default: postgresql://sca:sca@postgres:5432/sca)
    TMDB_API_KEY   Your TMDB v3 API key (required)
"""

import os
import sys
import time

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

import requests
from sqlalchemy import text
from app.database import SessionLocal

# ── Config ────────────────────────────────────────────────────────────────────
TMDB_BASE     = "https://api.themoviedb.org/3"
SLEEP_SEC     = 0.26    # ~3.8 req/s — under the 40 req/10 s free-tier cap
RETRY_SLEEP   = 12.0    # back off on 429
MAX_RETRIES   = 3
BATCH_SIZE    = 50


def _get_api_key() -> str:
    key = os.getenv("TMDB_API_KEY", "").strip()
    if not key:
        sys.exit(
            "ERROR: TMDB_API_KEY environment variable is not set.\n"
            "Set it and re-run:  export TMDB_API_KEY=your_key_here"
        )
    return key


def _tmdb_person(tmdb_person_id: int, api_key: str) -> dict | None:
    """Fetch /person/{id} from TMDB and return parsed JSON, or None on error."""
    url = f"{TMDB_BASE}/person/{tmdb_person_id}"
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params={"api_key": api_key}, timeout=10)
            if resp.status_code == 429:
                print(f"    [rate-limit] sleeping {RETRY_SLEEP}s …")
                time.sleep(RETRY_SLEEP)
                continue
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            print(f"    [error] attempt {attempt+1}/{MAX_RETRIES}: {exc}")
    return None


def run() -> None:
    api_key = _get_api_key()
    db = SessionLocal()

    # ── 1. Load all supporting actors with a TMDB person ID ──────────────────
    rows = db.execute(text("""
        SELECT id, name, tmdb_person_id
        FROM   actors
        WHERE  is_primary_actor = FALSE
          AND  is_director      = FALSE
          AND  tmdb_person_id IS NOT NULL
        ORDER  BY id
    """)).fetchall()

    total      = len(rows)
    directors  = 0
    skipped    = 0
    errors     = 0
    batch_n    = 0

    print("=" * 60)
    print("  Director Classification")
    print(f"  Actors to check : {total}")
    print("=" * 60)
    print()

    for i, (actor_id, name, tmdb_person_id) in enumerate(rows, 1):
        data = _tmdb_person(tmdb_person_id, api_key)

        if data is None:
            skipped += 1
        else:
            dept = data.get("known_for_department", "")
            if dept == "Directing":
                db.execute(text("""
                    UPDATE actors
                    SET    is_director = TRUE
                    WHERE  id = :id
                """), {"id": actor_id})
                directors += 1
                print(f"  [{i:>5}/{total}] 🎬 Director: {name} (TMDB {tmdb_person_id})")
            # else: actor — no action needed

        batch_n += 1
        if batch_n >= BATCH_SIZE:
            db.commit()
            batch_n = 0
            pct = i / total * 100
            print(f"  ── checkpoint {i}/{total} ({pct:.1f}%) — {directors} directors found so far ──")

        time.sleep(SLEEP_SEC)

    # Final commit
    if batch_n > 0:
        db.commit()

    db.close()

    print()
    print("=" * 60)
    print("  Classification complete")
    print(f"  Directors marked  : {directors}")
    print(f"  Actors confirmed  : {total - directors - skipped}")
    print(f"  Skipped (no TMDB) : {skipped}")
    print(f"  Errors            : {errors}")
    print("=" * 60)


if __name__ == "__main__":
    run()
