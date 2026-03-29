"""
enrich_documentary_flag.py
==========================
Fetches TMDB genre data for every movie that has a tmdb_id and sets
is_documentary = TRUE for any film where TMDB genre 99 (Documentary)
is present.

Usage
-----
    cd backend
    TMDB_API_KEY=your_key \
    DATABASE_URL=postgresql://sca:sca@localhost:5432/sca \
    python3 data_pipeline/enrich_documentary_flag.py

What it does
------------
1.  Loads all movies that have a tmdb_id from the DB.
2.  For each movie, calls GET /movie/{tmdb_id} on TMDB to fetch genre list.
3.  If genre_id 99 (Documentary) is found → sets is_documentary = TRUE.
4.  Commits in batches of 100 to avoid long transactions.
5.  Prints a summary at the end.

TMDB Documentary genre ID = 99 (universal across all languages).

Rate limiting
-------------
Respects the same 0.25 s delay used by the rest of the pipeline (~4 req/s).
~10 000 movies ≈ 40 minutes.  Run in the background or overnight.
"""

import os
import sys
import time

import psycopg2

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "")
DATABASE_URL  = os.environ.get("DATABASE_URL", "postgresql://sca:sca@localhost:5432/sca")
TMDB_BASE     = "https://api.themoviedb.org/3"
DELAY         = 0.26          # seconds between TMDB calls
BATCH_SIZE    = 100           # commit every N rows
DOCUMENTARY_GENRE_ID = 99

if not TMDB_API_KEY:
    print("ERROR: TMDB_API_KEY environment variable is not set.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def _build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s

SESSION = _build_session()
_last_req: float = 0.0

def _get(url: str, params: dict) -> dict | None:
    global _last_req
    elapsed = time.monotonic() - _last_req
    if elapsed < DELAY:
        time.sleep(DELAY - elapsed)
    try:
        r = SESSION.get(url, params=params, timeout=10)
        _last_req = time.monotonic()
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            return None   # movie not found on TMDB — skip
        print(f"  WARN HTTP {r.status_code} for {url}")
        return None
    except Exception as e:
        print(f"  WARN request failed: {e}")
        return None

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def is_documentary_on_tmdb(tmdb_id: int) -> bool:
    url  = f"{TMDB_BASE}/movie/{tmdb_id}"
    data = _get(url, {"api_key": TMDB_API_KEY, "language": "en-US"})
    if not data:
        return False
    genres = data.get("genre_ids") or [g["id"] for g in data.get("genres", [])]
    return DOCUMENTARY_GENRE_ID in genres

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    # Ensure column exists
    cur.execute("""
        ALTER TABLE movies
        ADD COLUMN IF NOT EXISTS is_documentary BOOLEAN NOT NULL DEFAULT FALSE
    """)
    conn.commit()

    # Load all movies with a tmdb_id
    cur.execute("""
        SELECT id, tmdb_id, title, is_documentary
        FROM movies
        WHERE tmdb_id IS NOT NULL
        ORDER BY id
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"Checking {total} movies with TMDB IDs…\n")

    newly_flagged  = []
    already_flagged = 0
    checked        = 0
    errors         = 0

    for i, (movie_id, tmdb_id, title, already_doc) in enumerate(rows, 1):
        if already_doc:
            already_flagged += 1
            if i % 500 == 0:
                print(f"  [{i}/{total}] skipping already-flagged docs…")
            continue

        is_doc = is_documentary_on_tmdb(tmdb_id)
        checked += 1

        if is_doc:
            cur.execute(
                "UPDATE movies SET is_documentary = TRUE WHERE id = %s",
                (movie_id,)
            )
            newly_flagged.append((movie_id, title))
            print(f"  [{i}/{total}] 🎥 DOCUMENTARY: {title} (tmdb={tmdb_id})")

        # Batch commit
        if checked % BATCH_SIZE == 0:
            conn.commit()
            print(f"  [{i}/{total}] committed batch — {len(newly_flagged)} flagged so far")

        # Progress every 200
        elif i % 200 == 0:
            print(f"  [{i}/{total}] …{len(newly_flagged)} documentaries found so far")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n{'='*55}")
    print(f"Done.")
    print(f"  Movies checked    : {checked}")
    print(f"  Already flagged   : {already_flagged}")
    print(f"  Newly flagged     : {len(newly_flagged)}")
    if newly_flagged:
        print(f"\nNewly flagged documentaries:")
        for mid, t in newly_flagged:
            print(f"  [{mid}] {t}")
    print(f"{'='*55}")
    print("\nRemember to restart the backend so the graph rebuilds without documentaries.")

if __name__ == "__main__":
    main()
