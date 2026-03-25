"""
Enrich movies.budget_crore from TMDB API.
Converts USD → INR crore at 83 INR/USD.
Skips films where budget_crore is already set or TMDB returns 0.
"""

import os, sys, time, ssl, urllib.request, json
import psycopg2

API_KEY   = os.getenv("TMDB_API_KEY", "25c74a6fc22333d38c72470ec59ee0b5")
DB_URL    = os.getenv("DATABASE_URL", "postgresql://sca:sca@localhost:5432/sca")
USD_TO_CR = 83 / 1e7          # 1 USD → INR crore  (83 INR / 1 crore)
RATE_SLEEP = 0.26              # ~3.8 req/s — safely under TMDB's 40/10s limit
BATCH_LOG  = 100               # print progress every N films

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode    = ssl.CERT_NONE


def tmdb_budget(tmdb_id: int) -> float | None:
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={API_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "SouthCinemaAnalytics/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=8, context=ctx) as r:
            d = json.loads(r.read())
            usd = d.get("budget", 0)
            return round(usd * USD_TO_CR, 2) if usd and usd > 0 else None
    except Exception:
        return None


def main():
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()

    # Fetch all TMDB-linked films that don't have budget yet
    cur.execute("""
        SELECT id, title, tmdb_id
        FROM movies
        WHERE tmdb_id IS NOT NULL
          AND budget_crore IS NULL
        ORDER BY id
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"Movies to enrich: {total}")

    updated = 0
    zero    = 0

    for i, (movie_id, title, tmdb_id) in enumerate(rows, 1):
        budget = tmdb_budget(tmdb_id)
        time.sleep(RATE_SLEEP)

        if budget:
            cur.execute(
                "UPDATE movies SET budget_crore = %s WHERE id = %s",
                (budget, movie_id)
            )
            updated += 1
            if updated % BATCH_LOG == 0:
                conn.commit()
                print(f"  [{i}/{total}] updated {updated} so far | latest: {title} → ₹{budget} Cr")
        else:
            zero += 1

        if i % BATCH_LOG == 0 and updated % BATCH_LOG != 0:
            conn.commit()
            print(f"  [{i}/{total}] updated={updated} zero/missing={zero}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nDone. Updated: {updated} | TMDB had no data: {zero} | Total processed: {total}")


if __name__ == "__main__":
    main()
