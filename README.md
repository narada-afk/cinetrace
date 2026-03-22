# South Cinema Analytics

A cinema curiosity engine for South Indian films. Explore the collaboration networks, career arcs, and box-office records of actors across Telugu, Tamil, Malayalam, and Kannada cinema.

**Dataset:** ~10,000+ movies · 6,700+ actors · 4 industries

---

## What it does

- **Connection Finder** — animated BFS shortest path between any two actors through shared films
- **Cinema Universe** — force-directed collaboration graph of primary actors
- **Gravity Center** — Brandes betweenness-centrality leaderboard (in-memory, sub-second)
- **WOW Insights** — story-driven cinema facts (collaboration shock, career peaks, network power, director loyalty) with TTL-cached scoring
- **Actor Profiles** — filmography, co-stars, director partnerships, production companies
- **Side-by-side Compare** — career stats from precomputed analytics tables (O(1))
- **Stats for Nerds** — industry distribution, top partnerships, career timelines, chart builder

---

## Tech stack

| Layer | Technology |
|---|---|
| Frontend | Next.js 14 (App Router) · TypeScript · Tailwind CSS |
| Backend | FastAPI · Uvicorn |
| ORM | SQLAlchemy |
| Database | PostgreSQL 15 |
| Containerisation | Docker Compose |
| Data sources | TMDB API · Wikidata SPARQL · Wikipedia |

---

## Running with Docker

```bash
docker compose up --build
```

| Service | URL |
|---|---|
| Frontend | http://localhost:3000 |
| Backend API | http://localhost:8000 |
| Swagger docs | http://localhost:8000/docs |

The backend connects to Postgres inside the same Compose network.
The frontend calls the backend at `http://backend:8000` server-side (SSR/RSC) and `http://localhost:8000` client-side (browser).

---

## Project structure

```
south-cinema-analytics/
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI app, CORS, lifespan (graph build on startup)
│   │   ├── crud.py              # All database query functions
│   │   ├── insight_engine.py    # WOW insight patterns + thread-safe TTL cache
│   │   ├── models.py            # SQLAlchemy ORM models
│   │   ├── schemas.py           # Pydantic request / response schemas
│   │   └── database.py          # DB engine + session factory
│   ├── data_pipeline/
│   │   ├── ingest_all_actors.py          # Wikidata batch ingestion (13 primary actors)
│   │   ├── ingest_supporting_actors.py   # TMDB supporting actor ingestion
│   │   ├── ingest_malayalam_actors.py    # TMDB Malayalam actor expansion
│   │   ├── enrich_movies.py              # Wikipedia runtime / production enrichment
│   │   ├── enrich_box_office.py          # TMDB box-office revenue enrichment
│   │   ├── backfill_directors.py         # TMDB director credit backfill
│   │   ├── build_analytics_tables.py     # Precompute all analytics tables
│   │   └── classify_directors.py         # Set actor_tier (primary / network / null)
│   ├── migrations/               # Schema migrations — sprint-tagged SQL files
│   └── requirements.txt
├── frontend/
│   ├── app/
│   │   ├── layout.tsx            # Root layout, global footer, dark theme (#0a0a0f)
│   │   ├── page.tsx              # Homepage: Hero, Connection Finder, Graph, Insights
│   │   ├── actors/[id]/          # Actor profile page
│   │   ├── compare/              # Side-by-side compare page
│   │   └── stats/                # Stats for Nerds page
│   ├── components/
│   │   ├── HeroSearch.tsx        # Hero search with trending actor chips
│   │   ├── ConnectionFinder.tsx  # Search form wired to /stats/connection
│   │   ├── ConnectionResult.tsx  # BFS path animation — entry fade, pause/resume on hover, payoff glow, share
│   │   ├── GraphPreview.tsx      # Interactive collaboration network graph + share button
│   │   ├── InsightsCarousel.tsx  # Infinite-scroll insight cards (RAF, visibility + intersection pause, edge fade)
│   │   ├── InsightCard.tsx       # Gradient card — single or paired actor avatars
│   │   ├── ActorAvatar.tsx       # Avatar image with deterministic initials fallback
│   │   └── stats/                # Stats page sub-components (charts, panels)
│   └── lib/
│       └── api.ts                # Typed fetch helpers for every backend endpoint
├── docker-compose.yml
└── README.md
```

---

## Backend API

Full interactive docs at `/docs` (Swagger UI) and `/redoc`.

### Actors

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Service status + live row counts |
| GET | `/actors` | List actors (`?primary_only=true`, `?gender=M\|F`) |
| GET | `/actors/search?q=` | Partial name search — max 20 results |
| GET | `/actors/{id}` | Profile with precomputed career stats |
| GET | `/actors/{id}/movies` | Filmography, newest-first |
| GET | `/actors/{id}/collaborators` | Top co-stars by shared film count |
| GET | `/actors/{id}/directors` | Directors sorted by collaboration count |
| GET | `/actors/{id}/production` | Production companies by film count |
| GET | `/actors/{id1}/shared/{id2}` | Films two actors appeared in together |
| GET | `/compare?actor1=&actor2=` | Side-by-side career comparison |

### Analytics

| Method | Endpoint | Description |
|---|---|---|
| GET | `/analytics/insights` | WOW insight cards (`?industry=telugu\|tamil\|…`) |
| GET | `/analytics/top-collaborations` | Actor pairs by shared film count |
| GET | `/analytics/directors` | Top directors by film count |
| GET | `/analytics/production-houses` | Top production companies by film count |
| GET | `/analytics/top-box-office` | Highest-grossing films in INR crore |

### Stats

| Method | Endpoint | Description |
|---|---|---|
| GET | `/stats/overview` | Global counts: movies, actors, links, industries |
| GET | `/stats/most-connected` | Actors ranked by unique co-star count |
| GET | `/stats/industry-distribution` | Film counts per industry with per-decade breakdown |
| GET | `/stats/top-partnerships` | Top actor–director partnerships (≥ 3 films) |
| GET | `/stats/career-timeline?actor_id=` | Films per year for one actor |
| GET | `/stats/top-costars` | Highest network-centrality actors |
| GET | `/stats/connection?actor1_id=&actor2_id=` | BFS shortest collaboration path (in-memory graph) |
| GET | `/stats/chart-data` | Dynamic chart data for the chart builder |
| GET | `/stats/cinema-universe` | Force-directed graph: nodes + edges |
| GET | `/stats/gravity-center` | Brandes betweenness centrality leaderboard |

---

## WOW Insight Engine

`backend/app/insight_engine.py` runs 6 pattern queries on startup and returns the 3 highest-scoring, most diverse insights.

| Pattern | Category | What it surfaces |
|---|---|---|
| `collab_shock` | collaboration | A legendary duo whose last shared film was 8+ years ago |
| `hidden_dominance` | career | A supporting actor with more films than most lead actors |
| `cross_industry` | industry | A primary actor who worked across 3+ language industries |
| `career_peak` | career | The densest 5-year window (≥ 35% of an actor's total output) |
| `network_power` | network | The actor connected to the most unique co-stars |
| `director_loyalty` | collaboration | An actor who spent ≥ 30% of their career with one director |

**Scoring:** log-scaled magnitude + per-type importance weight + optional rarity bonus.
**Confidence:** `min(1.0, score / 100)` — normalised 0–1, exposed to the frontend.
**Cache:** module-level TTL (10 min), protected by `threading.Lock`. Bust with `_invalidate_cache()` after ingestion.

Each insight emits: `type`, `category`, `title`, `value`, `unit`, `actors`, `actor_ids`, `subtext`, `confidence`.

---

## Data pipeline

Run in order after a fresh database:

```bash
cd backend

# 1. Wikidata — 13 primary South Indian actors
DATABASE_URL=postgresql://sca:sca@localhost:5432/sca python -m data_pipeline.ingest_all_actors

# 2. TMDB — supporting actors from primary actors' film credits
DATABASE_URL=postgresql://sca:sca@localhost:5432/sca python -m data_pipeline.ingest_supporting_actors

# 3. TMDB — Malayalam actor expansion
DATABASE_URL=postgresql://sca:sca@localhost:5432/sca python -m data_pipeline.ingest_malayalam_actors

# 4. Wikipedia — enrich movies with runtime, production company, language
DATABASE_URL=postgresql://sca:sca@localhost:5432/sca python -m data_pipeline.enrich_movies

# 5. TMDB — backfill director credits
DATABASE_URL=postgresql://sca:sca@localhost:5432/sca python -m data_pipeline.backfill_directors

# 6. TMDB — box-office revenue (INR crore at 84 INR/USD)
DATABASE_URL=postgresql://sca:sca@localhost:5432/sca python -m data_pipeline.enrich_box_office

# 7. Classify director-only entries; set actor_tier
DATABASE_URL=postgresql://sca:sca@localhost:5432/sca python -m data_pipeline.classify_directors

# 8. Build precomputed analytics tables (re-run after any ingestion step)
DATABASE_URL=postgresql://sca:sca@localhost:5432/sca python -m data_pipeline.build_analytics_tables
```

---

## Database schema

```
actors                  actor records — actor_tier: 'primary' | 'network' | null (directors)
movies                  film records — enriched with runtime, language, box_office_crore
cast                    actor ↔ movie join (Wikidata pipeline)
actor_movies            actor ↔ movie join (TMDB pipeline — includes character + role_type)

actor_stats             precomputed: film count, career span, avg runtime per actor
actor_collaborations    precomputed: co-star pair counts (bidirectional)
actor_director_stats    precomputed: actor × director film counts
actor_production_stats  precomputed: actor × production company film counts

actor_registry          seed catalog — Wikidata QIDs for primary actor ingestion
pipeline_runs           audit log for every pipeline execution
```

---

## Local development (without Docker)

```bash
# Backend
cd backend
pip install -r requirements.txt
DATABASE_URL=postgresql://sca:sca@localhost:5432/sca uvicorn app.main:app --reload --port 8000

# Frontend (separate terminal)
cd frontend
npm install
NEXT_PUBLIC_API_URL=http://localhost:8000 npm run dev
```

Apply all migrations before the first run:

```bash
for f in backend/migrations/sprint*.sql; do
  psql -h localhost -U sca -d sca -f "$f"
done
```

All migrations use `IF NOT EXISTS` — safe to re-run.

---

## Sprint history

| Sprint | What shipped |
|---|---|
| 1–6 | Core backend · Wikidata ingestion · analytics tables · actor + compare API |
| 7–8 | TMDB columns · supporting actor ingestion |
| 9 | Movie industry field · Malayalam actor expansion |
| 10 | `/analytics/top-collaborations` |
| 11 | Next.js 14 frontend: homepage · Header · InsightCard · TrendingActors |
| 15 | `/analytics/insights` — dynamic insight cards |
| 19 | `/analytics/directors` · `/analytics/production-houses` |
| 21 | Stats for Nerds: full `/stats/*` suite |
| 22 | Cinema Universe graph · Gravity Center (Brandes) · Build Your Own Chart |
| 23 | Box-office enrichment · `/analytics/top-box-office` |
| 24 | Gender column · lead actresses · director classification · actor tiers |
| 25 | Backend refactor: routers / repositories / services · in-memory graph singleton · lifespan startup |
| 26 | Homepage redesign: HeroSearch · ConnectionFinder + animated path · GraphPreview · InsightsCarousel · WOW insight engine v3 (thread-safe cache · confidence · category field) |
| 27 | ConnectionResult premium animation: entry fade + scale bump · hover/touch pause (auto-resume 1.8 s) · payoff scale-bump + expanding glow ring · variable timing (actor 500 ms / movie 350 ms) · edge fade · Replay re-trigger · Share (Web Share API + clipboard) · GraphPreview share button · InsightsCarousel visibility + IntersectionObserver pause · `?actor=` URL param for deep-linking network center |

---

## Data attribution

Movie metadata provided by [The Movie Database (TMDB)](https://www.themoviedb.org/).
This product uses the TMDB API but is not endorsed or certified by TMDB.
Additional data from [Wikidata](https://www.wikidata.org/) and [Wikipedia](https://www.wikipedia.org/).
