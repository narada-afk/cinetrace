# CineTrace

**South Indian Cinema… traced.**

A cinema curiosity engine for exploring the collaboration networks, career arcs, and hidden connections between actors across Telugu, Tamil, Malayalam, and Kannada films.

🌐 **Live:** [cinetrace.in](https://cinetrace.in)

---

## What it does

| Feature | Description |
|---|---|
| **Connection Finder** | Animated BFS shortest path between any two actors through shared films |
| **Cinema Universe** | Force-directed collaboration graph of all primary actors |
| **Gravity Center** | Brandes betweenness-centrality leaderboard — who is the most connected? |
| **WOW Insights** | Story-driven cinema facts: collaboration shocks, career peaks, director loyalty, network power |
| **Actor Profiles** | Filmography, co-stars, director partnerships, production companies, career stats |
| **Side-by-side Compare** | Career stats from precomputed analytics tables |
| **Stats for Nerds** | Industry distribution, top partnerships, career timelines, chart builder |
| **Chrome Extension** | Highlight any actor name on any page → instant collaboration popup |

**Dataset:** 10,000+ movies · 8,000+ actors · 4 industries

---

## Tech stack

| Layer | Technology |
|---|---|
| Frontend | Next.js 14 (App Router) · TypeScript · Tailwind CSS |
| Backend | FastAPI · Uvicorn · Gunicorn |
| ORM / Migrations | SQLAlchemy 2.0 · Alembic |
| Database | PostgreSQL 15 |
| Cache | Redis 7 |
| Reverse Proxy | Caddy (automatic HTTPS via Let's Encrypt) |
| CDN | Cloudflare (Mumbai edge, DDoS protection) |
| Containerisation | Docker Compose (dev + prod configs) |
| Analytics | Google Analytics 4 · Microsoft Clarity · PostHog |
| Error Tracking | Sentry (backend + frontend) |
| Data Sources | TMDB API · Wikidata SPARQL · Wikipedia |

---

## Quick start (Docker)

```bash
# 1. Clone and configure
git clone https://github.com/narada-afk/south-cinema-analytics.git
cd south-cinema-analytics
cp .env.example .env          # fill in POSTGRES_PASSWORD at minimum

# 2. Start all services
docker compose up --build
```

| Service | URL |
|---|---|
| Frontend | http://localhost:3000 |
| Backend API | http://localhost:8000 |
| Swagger docs | http://localhost:8000/docs |

The backend connects to Postgres inside the Compose network.
The frontend calls the backend at `http://backend:8000` server-side (SSR/RSC) and `http://localhost:8000` client-side (browser).

---

## Production deployment

### Infrastructure

| Component | Choice | Cost |
|---|---|---|
| Server | Hetzner CX22 (2 vCPU / 4 GB / 40 GB SSD) | €4.51/mo |
| CDN + DNS | Cloudflare Free (Mumbai edge) | Free |
| SSL | Caddy via Let's Encrypt (automatic renewal) | Free |
| Monitoring | UptimeRobot + Sentry | Free tier |

### Traffic flow

```
User (India)
    ↓
Cloudflare Mumbai Edge  ← caches static assets, ~20-40ms
    ↓
Hetzner Falkenstein (Germany)
    ↓
Caddy :443  →  Next.js :3000  or  FastAPI :8000
    ↓
PostgreSQL / Redis  (internal Docker network, not exposed)
```

### Deploying

```bash
# On the server — clone and configure
git clone https://github.com/narada-afk/south-cinema-analytics.git /opt/cinetrace
cd /opt/cinetrace
cp .env.example .env
# Edit .env — fill POSTGRES_PASSWORD, DATABASE_URL, ADMIN_API_KEY, analytics IDs

# Build and start all services
docker compose -f docker-compose.prod.yml up -d --build

# Copy Caddyfile and start Caddy
cp Caddyfile /etc/caddy/Caddyfile
systemctl enable --now caddy
```

### Database migration (first deploy)

```bash
# On your local machine
docker compose exec postgres pg_dump -U cinescope cinescope > backup.sql
scp backup.sql deploy@<server-ip>:/opt/cinetrace/

# On the server
docker compose -f docker-compose.prod.yml exec -T postgres \
  psql -U cinescope cinescope < backup.sql
```

### Cloudflare DNS

| Type | Name | Content | Proxy |
|---|---|---|---|
| A | `@` | `<server-ip>` | ✅ Proxied |
| A | `www` | `<server-ip>` | ✅ Proxied |
| A | `api` | `<server-ip>` | ✅ Proxied |

Set SSL/TLS mode to **Full (strict)**.

---

## Project structure

```
south-cinema-analytics/
├── backend/
│   ├── app/
│   │   ├── main.py                  # FastAPI app, CORS, lifespan (graph build on startup)
│   │   ├── models.py                # SQLAlchemy ORM models
│   │   ├── schemas.py               # Pydantic request / response schemas
│   │   ├── crud.py                  # Database query functions
│   │   ├── insight_engine.py        # WOW insight patterns + thread-safe TTL cache
│   │   ├── database.py              # DB engine + session factory
│   │   ├── routers/                 # Domain-separated route handlers
│   │   │   ├── actors.py            # /actors endpoints
│   │   │   ├── analytics.py         # /analytics endpoints
│   │   │   ├── stats.py             # /stats endpoints (BFS, Brandes centrality)
│   │   │   ├── health.py            # /health
│   │   │   └── admin.py             # /admin (API-key protected)
│   │   ├── repositories/
│   │   │   └── actor_repository.py  # Actor data access layer
│   │   ├── services/
│   │   │   └── graph_service.py     # In-memory collaboration graph (BFS, Brandes)
│   │   └── core/
│   │       ├── config.py            # Settings from env vars
│   │       ├── cache.py             # Redis caching decorator
│   │       ├── limiter.py           # Rate limiting (slowapi)
│   │       └── logging.py           # Structured request logging
│   ├── data_pipeline/               # 48 ingestion + enrichment scripts
│   │   ├── ingest_all_actors.py     # Wikidata: 13 primary actors
│   │   ├── ingest_supporting_actors.py
│   │   ├── ingest_malayalam_actors.py
│   │   ├── enrich_movies.py         # Wikipedia: runtime, production company
│   │   ├── enrich_box_office.py     # TMDB: revenue in INR crore
│   │   ├── backfill_directors.py    # TMDB: director credits
│   │   ├── classify_directors.py    # Set actor_tier
│   │   └── build_analytics_tables.py # Precompute all analytics tables
│   ├── migrations/                  # Sprint-tagged schema SQL (14 files, IF NOT EXISTS)
│   ├── alembic/                     # Alembic migration config
│   ├── requirements.txt
│   └── Dockerfile                   # Python 3.11-slim + gunicorn
│
├── frontend/
│   ├── app/
│   │   ├── layout.tsx               # Root layout, GA4 + Clarity scripts, global footer
│   │   ├── page.tsx                 # Homepage: Hero, Connection Finder, Graph, Insights
│   │   ├── actors/[id]/             # Actor profile
│   │   ├── compare/                 # Side-by-side compare
│   │   ├── stats/                   # Stats for Nerds
│   │   └── not-found.tsx            # 404 page
│   ├── components/
│   │   ├── HeroSearch.tsx           # Hero search with rotating headlines + trending chips
│   │   ├── ConnectionFinder.tsx     # BFS path search form
│   │   ├── ConnectionResult.tsx     # Animated path — entry fade, hover pause, payoff glow, share
│   │   ├── GraphPreview.tsx         # Interactive force-directed collaboration graph
│   │   ├── InsightsCarousel.tsx     # Infinite-scroll insight cards
│   │   ├── InsightCard.tsx          # Gradient insight card with actor avatars
│   │   ├── ActorAvatar.tsx          # Avatar image + deterministic initials fallback
│   │   ├── Header.tsx               # Sticky nav header with gradient logo
│   │   ├── SearchBar.tsx            # Autocomplete search input
│   │   ├── StarBackground.tsx       # Animated canvas starfield with constellation lines
│   │   ├── ShareButton.tsx          # Web Share API + clipboard fallback
│   │   ├── PostHogProvider.tsx      # PostHog + GA4 SPA pageview tracking
│   │   └── stats/                   # ChartBuilder, CinemaUniverse, GravityCenter, etc.
│   ├── lib/
│   │   ├── api.ts                   # Typed fetch helpers for all endpoints
│   │   ├── analytics.ts             # GA4 gtag wrapper (SSR-safe)
│   │   └── shareCard.ts             # OG image generation (canvas)
│   ├── public/
│   │   ├── narada.png               # Site logo
│   │   └── avatars/                 # Actor avatar PNGs ({slug}.png)
│   ├── Dockerfile                   # Node 20-alpine, npm ci + build, npm start
│   └── .env.local                   # Local dev overrides (gitignored)
│
├── chrome-extension/
│   ├── manifest.json                # Manifest V3, content script permissions
│   ├── content.js                   # Selection handler → API call → popup overlay
│   ├── config.js                    # API endpoint config
│   └── popup.css                    # Floating popup styles
│
├── qa/
│   ├── api_tests.py                 # 26 pytest API tests (schema, data integrity, perf)
│   ├── e2e/                         # 15 Playwright browser tests
│   ├── playwright.config.ts
│   └── QA_REPORT.md                 # Latest test results
│
├── docker-compose.yml               # Dev: all services, ports exposed
├── docker-compose.prod.yml          # Prod: localhost-bound ports, gunicorn, restart policies
├── Caddyfile                        # cinetrace.in + api.cinetrace.in reverse proxy
├── .env.example                     # All env vars with descriptions
└── ecosystem.config.js              # PM2 config for local dev
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
| GET | `/stats/connection?actor1_id=&actor2_id=` | BFS shortest collaboration path |
| GET | `/stats/chart-data` | Dynamic chart data for the chart builder |
| GET | `/stats/cinema-universe` | Force-directed graph: nodes + edges |
| GET | `/stats/gravity-center` | Brandes betweenness centrality leaderboard |

### Admin

| Method | Endpoint | Description |
|---|---|---|
| POST | `/admin/rebuild-graph` | Force in-memory graph rebuild (requires `ADMIN_API_KEY`) |

---

## WOW Insight Engine

`backend/app/insight_engine.py` runs 6 pattern queries and returns the highest-scoring, most diverse insights.

| Pattern | Category | What it surfaces |
|---|---|---|
| `collab_shock` | collaboration | A legendary duo whose last shared film was 8+ years ago |
| `hidden_dominance` | career | A supporting actor with more films than most lead actors |
| `cross_industry` | industry | A primary actor who worked across 3+ language industries |
| `career_peak` | career | The densest 5-year window (≥ 35% of an actor's total output) |
| `network_power` | network | The actor connected to the most unique co-stars |
| `director_loyalty` | collaboration | An actor who spent ≥ 30% of their career with one director |

**Scoring:** log-scaled magnitude + per-type importance weight + optional rarity bonus.
**Confidence:** `min(1.0, score / 100)` — normalised 0–1.
**Cache:** module-level TTL (10 min), protected by `threading.Lock`. Bust with `POST /admin/rebuild-graph` after ingestion.

Each insight emits: `type`, `category`, `title`, `value`, `unit`, `actors`, `actor_ids`, `subtext`, `confidence`.

---

## In-memory collaboration graph

`backend/app/services/graph_service.py` builds an adjacency list at startup via the FastAPI lifespan hook.

- **BFS** (`/stats/connection`) — shortest path between any two actors in sub-100ms
- **Brandes betweenness centrality** (`/stats/gravity-center`) — ranks actors by network influence
- **Version-aware reload** — bump `GRAPH_VERSION` in `.env` to force rebuild without a container restart

---

## Data pipeline

Run in order after a fresh database:

```bash
cd backend

DATABASE_URL=postgresql://cinescope:<password>@localhost:5432/cinescope

# 1. Wikidata — 13 primary South Indian actors (seed)
python -m data_pipeline.ingest_all_actors

# 2. TMDB — supporting actors from primary actors' film credits
python -m data_pipeline.ingest_supporting_actors

# 3. TMDB — Malayalam actor expansion
python -m data_pipeline.ingest_malayalam_actors

# 4. Wikipedia — enrich movies with runtime, production company, language
python -m data_pipeline.enrich_movies

# 5. TMDB — backfill director credits
python -m data_pipeline.backfill_directors

# 6. TMDB — box-office revenue (INR crore at 84 INR/USD)
python -m data_pipeline.enrich_box_office

# 7. Classify director-only entries; set actor_tier (primary / network / null)
python -m data_pipeline.classify_directors

# 8. Build precomputed analytics tables — re-run after any ingestion step
python -m data_pipeline.build_analytics_tables
```

All scripts are idempotent (`INSERT … ON CONFLICT`, update checks). Every execution is logged to the `pipeline_runs` table.

---

## Database schema

```
actors                  Actor records — name, industry, debut_year, tmdb_person_id,
                        actor_tier (primary | network | null), gender (M | F)

movies                  Film records — title, release_year, industry, director, poster_url,
                        runtime, language, box_office_crore, tmdb_id, vote_average

cast                    actor ↔ movie join (Wikidata pipeline)
actor_movies            actor ↔ movie join (TMDB pipeline — includes character + role_type)
movie_directors         movie ↔ director join

actor_stats             Precomputed: film count, career span, avg runtime per actor
actor_collaborations    Precomputed: co-star pair counts (bidirectional)
actor_director_stats    Precomputed: actor × director film counts
actor_production_stats  Precomputed: actor × production company film counts

actor_registry          Seed catalog — Wikidata QIDs for primary actors
pipeline_runs           Audit log for every pipeline execution
validation_metrics      Data health tracking
```

---

## Environment variables

Copy `.env.example` to `.env` and fill in all values before running.

```bash
# ── Database ──────────────────────────────────────────────────────────────────
POSTGRES_USER=cinescope
POSTGRES_PASSWORD=                     # generate: openssl rand -hex 32
POSTGRES_DB=cinescope
DATABASE_URL=postgresql://cinescope:<password>@postgres:5432/cinescope

# ── Frontend (baked into JS bundle at build time) ─────────────────────────────
NEXT_PUBLIC_API_URL=https://api.cinetrace.in   # what the browser uses
API_URL=http://backend:8000                    # server-side (Docker internal)

# ── Analytics ─────────────────────────────────────────────────────────────────
NEXT_PUBLIC_GA_ID=G-XXXXXXXXXX                 # Google Analytics 4 Measurement ID
NEXT_PUBLIC_CLARITY_ID=xxxxxxxxxx              # Microsoft Clarity Project ID
NEXT_PUBLIC_POSTHOG_KEY=                       # PostHog API key (leave blank to disable)

# ── Cache ─────────────────────────────────────────────────────────────────────
REDIS_URL=                                     # handled internally by docker-compose

# ── Graph ─────────────────────────────────────────────────────────────────────
GRAPH_VERSION=1                                # bump to trigger in-memory graph rebuild

# ── Error tracking ────────────────────────────────────────────────────────────
SENTRY_DSN=                                    # server-side (optional)
NEXT_PUBLIC_SENTRY_DSN=                        # client-side (optional)

# ── Security ──────────────────────────────────────────────────────────────────
ADMIN_API_KEY=                                 # required for POST /admin/rebuild-graph
```

> `NEXT_PUBLIC_*` variables are baked into the Next.js JS bundle at build time.
> In Docker production they must be passed as build args (already wired in `docker-compose.prod.yml`).

---

## QA & Testing

### API tests (Python / pytest)

```bash
cd qa
pip install pytest requests
BASE_URL=http://localhost:8000 pytest api_tests.py -v
```

26 tests covering: endpoint health, response schemas, data integrity, performance baselines.

### E2E tests (Playwright)

```bash
cd qa
npm install
npx playwright install chromium
npx playwright test
```

15 browser tests: homepage load, actor profile, search flow, connection finder.

### Latest results

| Layer | Tests | Pass | Fail | Flaky |
|---|---|---|---|---|
| Backend API | 26 | 26 ✅ | 0 | 0 |
| E2E (Playwright) | 15 | 11 | 2 | 2 |

See `qa/QA_REPORT.md` for the full breakdown.

---

## Chrome Extension

The `chrome-extension/` directory contains a Manifest V3 browser extension.

**How it works:**
1. Select any actor name on any webpage
2. A popup appears with their CineTrace profile — film count, top co-stars, career span
3. Click the popup to open their full profile page

**Installing locally (unpacked):**
1. Open `chrome://extensions`
2. Enable **Developer mode**
3. Click **Load unpacked** → select the `chrome-extension/` folder
4. Update `config.js` to point at your API URL

---

## Monitoring

| Layer | Tool | Purpose |
|---|---|---|
| Container restart | Docker `restart: unless-stopped` | Auto-recover from crashes |
| Uptime alerts | UptimeRobot (free) | Email/Telegram alert if site goes down |
| App errors | Sentry | Backend exceptions + frontend JS errors |
| Server metrics | Netdata (optional) | Real-time CPU/RAM/disk dashboard |
| Self-healing | `watchdog.sh` cron (every 5 min) | Health check → auto-restart Compose if down |

---

## Local development (without Docker)

```bash
# Backend
cd backend
pip install -r requirements.txt
DATABASE_URL=postgresql://cinescope:<password>@localhost:5432/cinescope \
  uvicorn app.main:app --reload --port 8000

# Frontend (separate terminal)
cd frontend
npm install
NEXT_PUBLIC_API_URL=http://localhost:8000 npm run dev
```

Apply all migrations before first run:

```bash
for f in backend/migrations/sprint*.sql; do
  psql -h localhost -U cinescope -d cinescope -f "$f"
done
```

All migrations use `IF NOT EXISTS` — safe to re-run.

---

## Data attribution

Movie metadata provided by [The Movie Database (TMDB)](https://www.themoviedb.org/).
This product uses the TMDB API but is not endorsed or certified by TMDB.

Additional data sourced from [Wikidata](https://www.wikidata.org/) and [Wikipedia](https://www.wikipedia.org/),
both published under open licenses (CC0 / CC BY-SA).
