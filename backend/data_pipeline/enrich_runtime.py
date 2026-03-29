"""
enrich_runtime.py
==================
Fetches runtime from TMDB for all movies that have a tmdb_id but no runtime.

TMDB's /movie/{id} endpoint returns runtime in minutes for almost all films.

Run:
  DATABASE_URL=postgresql://sca:sca@localhost:5432/sca \
  TMDB_API_KEY=25c74a6fc22333d38c72470ec59ee0b5 \
  python3 data_pipeline/enrich_runtime.py
"""

import os, time, ssl, urllib.request, json, sys

DATABASE_URL = os.environ["DATABASE_URL"]
API_KEY      = os.environ["TMDB_API_KEY"]
BATCH        = 100  # commit every N updates
DELAY        = 0.26 # ~4 req/s — stay within TMDB rate limit

import psycopg2

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode    = ssl.CERT_NONE

def fetch_runtime(tmdb_id: int) -> int | None:
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={API_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "SCA/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=8, context=ctx) as r:
            data = json.loads(r.read())
            rt = data.get("runtime")
            return int(rt) if rt and int(rt) > 0 else None
    except Exception:
        return None

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

cur.execute("""
    SELECT id, tmdb_id, title FROM movies
    WHERE (runtime IS NULL OR runtime = 0)
      AND tmdb_id IS NOT NULL
    ORDER BY id
""")
rows = cur.fetchall()
print(f"Movies missing runtime: {len(rows)}")

updated = 0
skipped = 0

for i, (movie_id, tmdb_id, title) in enumerate(rows):
    runtime = fetch_runtime(tmdb_id)
    if runtime:
        cur.execute("UPDATE movies SET runtime = %s WHERE id = %s", (runtime, movie_id))
        updated += 1
    else:
        skipped += 1

    if (i + 1) % BATCH == 0:
        conn.commit()
        pct = (i + 1) / len(rows) * 100
        print(f"  [{i+1}/{len(rows)} {pct:.0f}%] updated={updated} skipped={skipped}", flush=True)

    time.sleep(DELAY)

conn.commit()
print(f"\nDone. updated={updated} skipped={skipped} total={len(rows)}")
cur.close()
conn.close()
