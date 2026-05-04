"""
prod_data_fixes.py — idempotent data corrections run after every deploy.

Add fixes here when bad data is found in production. Each fix must be safe
to run multiple times (DELETE WHERE, ON CONFLICT DO UPDATE, etc.).
"""
import os, psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://sca:sca@/sca?host=/tmp")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# ── Fix 1: Remove spurious Jack & Daniel credit for Rajinikanth ──────────────
# Incorrectly linked Rajinikanth to Jack & Daniel (movie_id=132, tmdb_id=635233).
# Check both pipelines (actor_movies + cast table).
cur.execute("""
    DELETE FROM actor_movies
    WHERE actor_id = (SELECT id FROM actors WHERE name='Rajinikanth' LIMIT 1)
      AND movie_id = (SELECT id FROM movies WHERE tmdb_id=635233 LIMIT 1)
""")
rows_am = cur.rowcount
cur.execute("""
    DELETE FROM "cast"
    WHERE actor_id = (SELECT id FROM actors WHERE name='Rajinikanth' LIMIT 1)
      AND movie_id = (SELECT id FROM movies WHERE tmdb_id=635233 LIMIT 1)
""")
rows_cast = cur.rowcount
print(f"Fix 1 (Raj/Jack&Daniel): deleted {rows_am} actor_movies rows, {rows_cast} cast rows")

# ── Fix 2: Rebuild actor_collaborations from source data ─────────────────────
# Ensures collaboration counts match actual actor_movies entries.
# Fixes Malayalam pairs (e.g. Dileep+Jayaram) that had stale counts.
cur.execute("""
    INSERT INTO actor_collaborations (actor1_id, actor2_id, collaboration_count)
    SELECT am1.actor_id, am2.actor_id, COUNT(*)
    FROM actor_movies am1
    JOIN actor_movies am2
      ON am1.movie_id = am2.movie_id AND am1.actor_id != am2.actor_id
    GROUP BY am1.actor_id, am2.actor_id
    ON CONFLICT (actor1_id, actor2_id)
    DO UPDATE SET collaboration_count = EXCLUDED.collaboration_count
""")
print(f"Fix 2 (actor_collaborations rebuild): upserted {cur.rowcount} rows")

# ── Fix 3: Remove spurious Jack & Daniel credit for Prabhas ──────────────────
# Same bad TMDB/Wikidata link as Rajinikanth — Prabhas incorrectly linked to
# Jack & Daniel (movie_id=132, tmdb_id=635233). Check both pipelines.
cur.execute("""
    DELETE FROM actor_movies
    WHERE actor_id = (SELECT id FROM actors WHERE name='Prabhas' LIMIT 1)
      AND movie_id = (SELECT id FROM movies WHERE tmdb_id=635233 LIMIT 1)
""")
rows_am = cur.rowcount
cur.execute("""
    DELETE FROM "cast"
    WHERE actor_id = (SELECT id FROM actors WHERE name='Prabhas' LIMIT 1)
      AND movie_id = (SELECT id FROM movies WHERE tmdb_id=635233 LIMIT 1)
""")
rows_cast = cur.rowcount
print(f"Fix 3 (Prabhas/Jack&Daniel): deleted {rows_am} actor_movies rows, {rows_cast} cast rows")

# ── Fix 4: Rename Vijay → CM Vijay ───────────────────────────────────────────
# Actor id=2 (Tamil, is_primary_actor=True) — reflects his current public name.
# UPDATE is idempotent: re-running after the rename is a no-op (0 rows affected).
cur.execute("""
    UPDATE actors
    SET name = 'CM Vijay'
    WHERE name = 'Vijay'
      AND industry = 'Tamil'
      AND is_primary_actor = TRUE
""")
print(f"Fix 4 (Vijay → CM Vijay): updated {cur.rowcount} row(s)")

conn.commit()
conn.close()
print("All data fixes complete.")
