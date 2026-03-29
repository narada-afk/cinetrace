"""
enrich_directors_v2.py
====================
Re-runs director enrichment with better logging and retry logic.
Focuses on ALL movies that still have no director.

Run:
  DATABASE_URL=postgresql://sca:sca@localhost:5432/sca \
  TMDB_API_KEY=25c74a6fc22333d38c72470ec59ee0b5 \
  python3 data_pipeline/enrich_directors_v2.py
"""

import os, time, ssl, urllib.request, json, sys

DATABASE_URL = os.environ["DATABASE_URL"]
API_KEY      = os.environ["TMDB_API_KEY"]
BATCH        = 100   # commit every N updates
DELAY        = 0.26  # ~4 req/s TMDB rate limit

import psycopg2

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode    = ssl.CERT_NONE

def fetch_director(tmdb_id: int) -> tuple[str | None, str]:
    """Returns (director_name_or_None, status_string)."""
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/credits?api_key={API_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "SCA/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10, context=ctx) as r:
            data = json.loads(r.read())
            if "success" in data and not data["success"]:
                return None, f"TMDB_ERROR:{data.get('status_message','?')}"
            crew = data.get("crew", [])
            directors = [c["name"] for c in crew if c.get("job") == "Director"]
            if directors:
                return directors[0], "ok"
            else:
                return None, "no_director_in_crew"
    except urllib.error.HTTPError as e:
        return None, f"HTTP_{e.code}"
    except Exception as e:
        return None, f"ERR:{type(e).__name__}"

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

cur.execute("""
    SELECT id, tmdb_id, title FROM movies
    WHERE (director IS NULL OR trim(director) = '')
      AND tmdb_id IS NOT NULL
    ORDER BY id
""")
rows = cur.fetchall()
print(f"Movies to enrich: {len(rows)}", flush=True)

updated   = 0
skipped   = 0
errors    = {}

for i, (movie_id, tmdb_id, title) in enumerate(rows):
    director, status = fetch_director(tmdb_id)
    if director:
        cur.execute("UPDATE movies SET director = %s WHERE id = %s", (director, movie_id))
        updated += 1
    else:
        skipped += 1
        errors[status] = errors.get(status, 0) + 1

    if (i + 1) % BATCH == 0:
        conn.commit()
        pct = (i + 1) / len(rows) * 100
        print(f"  [{i+1}/{len(rows)} {pct:.1f}%] updated={updated} skipped={skipped}", flush=True)

    time.sleep(DELAY)

conn.commit()
cur.close()
conn.close()

print(f"\nDone. updated={updated}, skipped={skipped}")
print("Skip reasons:")
for reason, count in sorted(errors.items(), key=lambda x: -x[1]):
    print(f"  {reason}: {count}")
