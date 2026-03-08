-- =============================================================================
-- Migration: Sprint 5 — Precomputed analytics tables
-- File     : backend/migrations/sprint5_analytics_tables.sql
-- Target   : PostgreSQL 14+
--
-- What this migration does:
--   Task 1. Creates four denormalised analytics tables that store precomputed
--           aggregates, eliminating expensive multi-table joins at query time.
--   Task 2. Adds covering indexes so every analytics lookup is an index scan.
--
-- Design notes:
--   • These tables are populated (and refreshed) entirely by:
--       python -m data_pipeline.build_analytics_tables
--     They are NEVER written to by the ingestion pipeline; they are read-only
--     from the application's perspective.
--
--   • actor_id columns are plain INTs (no FOREIGN KEY constraint).
--     Denormalised analytics tables intentionally omit FK constraints so
--     TRUNCATE + re-INSERT can run without disabling triggers.
--
--   • actor_collaborations stores BOTH directions (A→B and B→A) so dashboard
--     queries can use a simple WHERE actor1_id = ? without needing an OR.
--
-- Safe to re-run: all CREATE TABLE / CREATE INDEX use IF NOT EXISTS.
--
-- Run order: after sprint4_*.sql migrations.
-- =============================================================================

BEGIN;


-- ---------------------------------------------------------------------------
-- Table 1: actor_stats
-- ---------------------------------------------------------------------------
-- One row per actor — precomputed career summary.
-- Answers: "How many films has Rajinikanth made? When was his first film?"

CREATE TABLE IF NOT EXISTS actor_stats (
    actor_id        INT     PRIMARY KEY,            -- maps to actors.id
    film_count      INT     NOT NULL DEFAULT 0,     -- total distinct films
    first_film_year INT,                            -- earliest release_year (>0)
    last_film_year  INT,                            -- latest  release_year (>0)
    avg_runtime     FLOAT                           -- average runtime in minutes
);

COMMENT ON TABLE  actor_stats              IS 'Precomputed career stats per actor. Rebuilt by build_analytics_tables.py.';
COMMENT ON COLUMN actor_stats.first_film_year IS 'Excludes sentinel year 0 (used when year is unknown).';
COMMENT ON COLUMN actor_stats.avg_runtime     IS 'NULL if no enriched movies exist yet for this actor.';


-- ---------------------------------------------------------------------------
-- Table 2: actor_collaborations
-- ---------------------------------------------------------------------------
-- One row per ordered (actor1, actor2) pair — how often they share a film.
-- Both directions stored: (A,B) AND (B,A) with the same count.
-- Answers: "How many films did Prabhas and Ram Charan do together?"

CREATE TABLE IF NOT EXISTS actor_collaborations (
    actor1_id          INT NOT NULL,    -- maps to actors.id
    actor2_id          INT NOT NULL,    -- maps to actors.id  (actor1_id != actor2_id)
    collaboration_count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (actor1_id, actor2_id)
);

COMMENT ON TABLE actor_collaborations IS
    'Precomputed co-occurrence counts. Both directions (A→B and B→A) are stored.';


-- ---------------------------------------------------------------------------
-- Table 3: actor_director_stats
-- ---------------------------------------------------------------------------
-- One row per (actor, director) pair — how many films they made together.
-- Sourced from movies.director (the legacy denormalised TEXT column).
-- Answers: "How many films did Prabhas do with S.S. Rajamouli?"

CREATE TABLE IF NOT EXISTS actor_director_stats (
    actor_id   INT  NOT NULL,    -- maps to actors.id
    director   TEXT NOT NULL,    -- director name from movies.director
    film_count INT  NOT NULL DEFAULT 0,
    PRIMARY KEY (actor_id, director)
);

COMMENT ON TABLE actor_director_stats IS
    'Precomputed actor-director collaboration counts from movies.director.';


-- ---------------------------------------------------------------------------
-- Table 4: actor_production_stats
-- ---------------------------------------------------------------------------
-- One row per (actor, production_company) pair — how many films together.
-- Sourced from movies.production_company (populated by Wikipedia enrichment).
-- Answers: "How many films did Vijay do under Sun Pictures?"

CREATE TABLE IF NOT EXISTS actor_production_stats (
    actor_id           INT  NOT NULL,    -- maps to actors.id
    production_company TEXT NOT NULL,   -- from movies.production_company
    film_count         INT  NOT NULL DEFAULT 0,
    PRIMARY KEY (actor_id, production_company)
);

COMMENT ON TABLE actor_production_stats IS
    'Precomputed actor-production-company collaboration counts.';


-- ---------------------------------------------------------------------------
-- Task 2 — Indexes
-- ---------------------------------------------------------------------------
-- The composite PKs already cover (actor_id, …) prefix lookups.
-- These single-column indexes support queries that filter on actor_id alone,
-- and help the query planner choose index scans over sequential scans.

-- actor_stats: PK is actor_id; this index is for explicit O(1) lookup.
CREATE INDEX IF NOT EXISTS idx_actor_stats_actor
    ON actor_stats (actor_id);

-- actor_collaborations: PK is (actor1_id, actor2_id).
-- Single-column index on actor1_id lets us list all collaborators of one actor.
CREATE INDEX IF NOT EXISTS idx_collab_actor1
    ON actor_collaborations (actor1_id);

-- actor_director_stats: PK is (actor_id, director).
-- Filter by actor_id to get all directors an actor worked with.
CREATE INDEX IF NOT EXISTS idx_director_actor
    ON actor_director_stats (actor_id);

-- actor_production_stats: PK is (actor_id, production_company).
-- Filter by actor_id to get all production companies an actor worked with.
CREATE INDEX IF NOT EXISTS idx_production_actor
    ON actor_production_stats (actor_id);

-- Additional reverse-direction index: "which actors worked with director X?"
CREATE INDEX IF NOT EXISTS idx_director_name
    ON actor_director_stats (director);

-- Additional reverse-direction index: "which actors worked with company X?"
CREATE INDEX IF NOT EXISTS idx_production_company
    ON actor_production_stats (production_company);


-- ---------------------------------------------------------------------------
-- Verification (uncomment to inspect after migration)
-- ---------------------------------------------------------------------------

-- Confirm tables were created:
-- SELECT tablename FROM pg_tables
-- WHERE tablename IN ('actor_stats','actor_collaborations',
--                     'actor_director_stats','actor_production_stats')
-- ORDER BY tablename;

-- Confirm indexes were created:
-- SELECT indexname, indexdef FROM pg_indexes
-- WHERE tablename IN ('actor_stats','actor_collaborations',
--                     'actor_director_stats','actor_production_stats')
-- ORDER BY tablename, indexname;


COMMIT;
