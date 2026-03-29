"""
enrich_directors.py
====================
Fetches director (and crew) data from TMDB for all movies that have
a tmdb_id but no director field populated.

Run:
  DATABASE_URL=postgresql://sca:sca@localhost:5432/sca \
  TMDB_API_KEY=25c74a6fc22333d38c72470ec59ee0b5 \
  python3 data_pipeline/enrich_directors.py
"""

import os, time, ssl, urllib.request, json

DATABASE_URL = os.environ["DATABASE_URL"]
API_KEY      = os.environ["TMDB_API_KEY"]
BATCH        = 50   # commit every N updates
DELAY        = 0.26 # ~4 req/s TMDB rate limit

import psycopg2

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode    = ssl.CERT_NONE

def fetch_director(tmdb_id: int) -> str | None:
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/credits?api_key={API_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "SCA/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=8, context=ctx) as r:
            data = json.loads(r.read())
            crew = data.get("crew", [])
            directors = [c["name"] for c in crew if c.get("job") == "Director"]
            return directors[0] if directors else None
    except Exception:
        return None

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

cur.execute("""
    SELECT id, tmdb_id FROM movies
    WHERE (director IS NULL OR trim(director) = '')
      AND tmdb_id IS NOT NULL
    ORDER BY id
""")
rows = cur.fetchall()
print(f"Movies to enrich: {len(rows)}")

updated = 0
skipped = 0

for i, (movie_id, tmdb_id) in enumerate(rows):
    director = fetch_director(tmdb_id)
    if director:
        cur.execute("UPDATE movies SET director = %s WHERE id = %s", (director, movie_id))
        updated += 1
    else:
        skipped += 1

    if (i + 1) % BATCH == 0:
        conn.commit()
        print(f"  [{i+1}/{len(rows)}] updated={updated} skipped={skipped}")

    time.sleep(DELAY)

conn.commit()
cur.close()
conn.close()
print(f"\nDone. updated={updated}, skipped={skipped}")
